// copilot_retry.go — transient-error retry and CLI-child recovery for SDK sends (CRI-272).
//
// A provider 5xx (or a CLI-child death) surfaced through the SDK used to be a
// turn error: with one copilot client per adapter lifetime, the engine saw the
// stdio EOF and failed the step after a single attempt. This file bounds the
// blast radius: provider 500/502/503/504/429 and transport-level failures are
// retried with a capped exponential backoff, a dead CLI child is restarted,
// and the SDK session is re-opened (resume-or-create) before the next attempt.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
	copilot "github.com/github/copilot-sdk/go"
)

// Retry policy: 1s → 4s → 16s backoff, 4 attempts total (1 initial + 3
// retries), so the longest possible retry window is ~21s of sleep plus
// recovery time — a step-level timeout stays in charge because every sleep is
// additionally capped by the remaining context deadline. The vars are
// overridable in tests; the shape (4× growth, capped) is fixed.
var (
	retryBaseDelay   = 1 * time.Second
	retryMaxDelay    = 16 * time.Second
	retryMaxAttempts = 4
)

// retrySleep suspends the caller for d or until ctx is done. It is a variable
// so tests can record the backoff sequence instead of sleeping.
var retrySleep = func(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// retryNow is the clock used to compute the remaining context deadline when
// clamping a backoff sleep; overridable in tests so deadline behaviour is
// asserted deterministically instead of against wall-clock margins.
var retryNow = time.Now

// backoffDelay returns the wait preceding the retry after the failed attempt
// (0-based): 1s → 4s → 16s, capped at retryMaxDelay.
func backoffDelay(attempt int) time.Duration {
	d := retryBaseDelay
	for i := 0; i < attempt; i++ {
		d *= 4
		if d > retryMaxDelay {
			return retryMaxDelay
		}
	}
	return d
}

// transportDeathRe matches errors meaning the CLI child process or its stdio
// connection is gone (as opposed to a provider-side HTTP error the CLI
// survived): EOF, connection reset, broken pipe, process exit, stopped client.
// The SDK's Session.Send wrapper text ("failed to send message") wraps every
// Send failure and is deliberately NOT a death marker; "failed to send
// request" (a write to a dead pipe) is.
var transportDeathRe = regexp.MustCompile(
	`\b(?:eof|unexpected eof|econnreset|connection reset|broken pipe|connection refused|` +
		`connection closed|unavailable|process exited|exited unexpectedly|client stopped|` +
		`failed to send request|message write error|not connected)\b`)

// retryableStatusRe matches a mention of one of the transient provider HTTP
// statuses (500/502/503/504/429) in forms like "HTTP 502", "status code 502",
// "status: 502", "http=502".
var retryableStatusRe = regexp.MustCompile(
	`(?i)\b(?:http|status)(?:\s*code)?\s*[-:=/]?\s*(?:500|502|503|504|429)\b`)

// retryableErrorPhrases are provider-side failure markers that indicate a
// transient 5xx/429 even when the raw status code is not echoed back.
var retryableErrorPhrases = []string{
	"bad gateway",
	"service unavailable",
	"gateway timeout",
	"internal server error",
	"too many requests",
	"rate limit",
	"rate-limit",
	"ratelimit",
}

// isTransportDeathError reports whether err means the CLI child process or its
// stdio connection is gone.
func isTransportDeathError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return transportDeathRe.MatchString(strings.ToLower(err.Error()))
}

// isRetryableSendError reports whether a Send failure is transient and worth a
// bounded retry: provider HTTP 500/502/503/504/429 (surfaced by the CLI as a
// JSON-RPC error whose message carries the status), rate-limit/overload
// markers, and transport-level failures on the CLI stdio. Everything else —
// including other 4xx — is surfaced as-is.
func isRetryableSendError(err error) bool {
	if err == nil {
		return false
	}
	if isTransportDeathError(err) {
		return true
	}
	text := strings.ToLower(err.Error())
	if retryableStatusRe.MatchString(text) {
		return true
	}
	for _, phrase := range retryableErrorPhrases {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

// sendWithRetry sends a prompt through the SDK session with bounded
// exponential backoff. Provider 5xx/429 and transport-level failures are
// retried (backoff 1s → 4s → 16s, retryMaxAttempts total); when the CLI child
// died (EOF on stdio), the owning adapter restarts the client and re-opens the
// SDK session — resume via the persisted ID, fallback CreateSession — before
// the next attempt. Every sleep is capped by the remaining context deadline so
// a step-level timeout still applies; exhaustion surfaces the last error.
func (s *sessionState) sendWithRetry(ctx context.Context, opts *copilot.MessageOptions) (string, error) {
	var lastErr error
	for attempt := 0; attempt < retryMaxAttempts; attempt++ {
		// Re-read the session every attempt: a CLI-child restart mid-retry
		// swaps in a re-opened session (recoverTransport), and the next
		// attempt must target the new one. The accessor shares swapSession's
		// lock, so the swap cannot race this read.
		msgID, err := s.currentSession().Send(ctx, opts)
		if err == nil {
			return msgID, nil
		}
		lastErr = err
		if !isRetryableSendError(err) {
			return "", err
		}
		if attempt == retryMaxAttempts-1 {
			break
		}
		if err := ctx.Err(); err != nil {
			return "", lastErr
		}
		if s.owner != nil {
			// The child may have died instead of returning a provider
			// error; restore the transport (no-op when it is still alive)
			// before giving the provider time to recover.
			s.owner.recoverTransport(ctx, s)
		}
		sleep := backoffDelay(attempt)
		if deadline, ok := ctx.Deadline(); ok {
			remaining := deadline.Sub(retryNow())
			if remaining <= 0 {
				return "", lastErr
			}
			if sleep > remaining {
				sleep = remaining
			}
		}
		if err := retrySleep(ctx, sleep); err != nil {
			return "", lastErr
		}
	}
	return "", fmt.Errorf("copilot: send retries exhausted after %d attempts: %w", retryMaxAttempts, lastErr)
}

// startClientWithRetry starts the CLI child with the same bounded backoff used
// for provider errors, absorbing transient spawn/transport failures at startup
// instead of failing the first caller outright.
func startClientWithRetry(ctx context.Context, client copilotClient) error {
	var lastErr error
	for attempt := 0; attempt < retryMaxAttempts; attempt++ {
		err := client.Start(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryableSendError(err) {
			return err
		}
		if attempt == retryMaxAttempts-1 {
			break
		}
		if ctx.Err() != nil {
			return lastErr
		}
		if err := retrySleep(ctx, backoffDelay(attempt)); err != nil {
			return lastErr
		}
	}
	return fmt.Errorf("copilot: start client retries exhausted after %d attempts: %w", retryMaxAttempts, lastErr)
}

// recoverTransport restores the adapter's CLI child after a send failed on a
// dead transport (CRI-272). The current client is probed and restarted only
// when actually dead; every session still bound to a superseded runtime is
// then re-opened — including sessions whose Execute is in flight: with the
// guarded session accessor a swap is safe, and the in-flight retry loop picks
// the re-opened session up on its next attempt. The triggering session is
// handled last (its own goroutine is waiting inside sendWithRetry). A restart
// already performed by a peer goroutine does not skip the sweep: the caller's
// session may still be bound to the superseded runtime, and reopenSession's
// staleness check keeps sessions a peer already re-opened from churning twice.
// Errors are logged, not returned: the retry loop continues and surfaces the
// last Send error if recovery fails.
func (p *copilotAdapter) recoverTransport(ctx context.Context, trigger *sessionState) {
	if _, _, err := p.restartClient(ctx); err != nil {
		slog.Warn("copilot: cli child restart failed", "err", err)
		return
	}
	p.mu.Lock()
	live := make([]*sessionState, 0, len(p.sessions))
	for _, s := range p.sessions {
		live = append(live, s)
	}
	p.mu.Unlock()
	for _, s := range live {
		if s == trigger {
			continue
		}
		p.reopenSession(ctx, s)
	}
	if trigger != nil {
		p.reopenSession(ctx, trigger)
	}
}

// restartClient probes the CLI child and restarts it when dead. Returns the
// (possibly unchanged) client and whether a restart happened. Serialized with
// ensureClient on clientMu.
func (p *copilotAdapter) restartClient(ctx context.Context) (copilotClient, bool, error) {
	p.clientMu.Lock()
	defer p.clientMu.Unlock()
	if p.client != nil {
		if err := p.client.Ping(ctx, "liveness"); err == nil {
			return p.client, false, nil
		}
		_ = p.client.Stop()
		p.client = nil
	}
	client, err := p.startClientLocked(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	return client, true, nil
}

// clientEpoch identifies the current CLI child runtime: it is bumped on every
// successful (re)start, so sessions can tell whether they are still bound to
// the runtime they were opened on or to one that has since been superseded by
// a restart (possibly by a peer goroutine).
func (p *copilotAdapter) currentClientAndEpoch() (copilotClient, int) {
	p.clientMu.Lock()
	defer p.clientMu.Unlock()
	return p.client, p.clientEpoch
}

// currentClientEpoch reports the current CLI runtime epoch.
func (p *copilotAdapter) currentClientEpoch() int {
	p.clientMu.Lock()
	defer p.clientMu.Unlock()
	return p.clientEpoch
}

// reopenSession re-opens the SDK session for an adapter session that is still
// bound to a superseded CLI runtime (CRI-272): resume via the persisted SDK
// session ID when possible, falling back to a fresh CreateSession; the
// persisted ID is updated when a new session had to be created. Sessions
// already on the current runtime epoch — including ones a peer goroutine
// re-opened first — are left untouched, so concurrent recovery sweeps churn a
// session at most once, and sessions removed from the adapter (CloseSession)
// are skipped. Serialized per session on reopenMu; errors are logged — the
// subsequent retry will surface the failure if the transport stayed broken.
func (p *copilotAdapter) reopenSession(ctx context.Context, s *sessionState) {
	if s == nil {
		return
	}
	s.reopenMu.Lock()
	defer s.reopenMu.Unlock()

	p.mu.Lock()
	_, stillOpen := p.sessions[s.adapterSessionID]
	p.mu.Unlock()
	if !stillOpen {
		return
	}
	client, epoch := p.currentClientAndEpoch()
	if client == nil || s.boundClientEpoch == epoch {
		return
	}
	sess, resumed, err := p.openSDKSession(ctx, client, s.adapterSessionID, s.sessionConfig, s.resumeConfig)
	if err != nil {
		slog.Warn("copilot: reopen sdk session failed", "adapterSession", s.adapterSessionID, "err", err)
		return
	}
	s.swapSession(sess)
	s.boundClientEpoch = epoch
	slog.Info("copilot: reopened sdk session after cli restart",
		"adapterSession", s.adapterSessionID, "sdkSession", sess.SessionID(), "resumed", resumed)
}

// newClientFn builds the SDK client; overridable in tests.
var newClientFn = func(options *copilot.ClientOptions) copilotClient {
	return &sdkClient{inner: copilot.NewClient(options)}
}

// cliPath resolves the Copilot CLI binary from the environment or the default.
func cliPath() string {
	if p := os.Getenv(defaultBinEnv); strings.TrimSpace(p) != "" {
		return p
	}
	return defaultBin
}

// ensureClient returns the adapter's connected Copilot client, starting (or
// restarting) the CLI child as needed. A dead CLI child is a restartable
// condition, not a terminal error: the child is stopped and started again with
// the same bounded backoff used for provider errors (CRI-272). Serialized on
// clientMu; startClientLocked reuses the options captured from the first
// build, so restarts need no fresh secrets.
func (p *copilotAdapter) ensureClient(ctx context.Context, secrets *adapterhost.Secrets) (copilotClient, error) {
	p.clientMu.Lock()
	defer p.clientMu.Unlock()

	if p.client != nil {
		if err := p.client.Ping(ctx, "liveness"); err == nil {
			return p.client, nil
		}
		slog.Warn("copilot: cli child is dead; restarting")
		_ = p.client.Stop()
		p.client = nil
	}
	return p.startClientLocked(ctx, secrets)
}

// startClientLocked builds and starts the CLI child. Callers must hold
// clientMu. When clientOptions has not been captured yet, secrets is required
// to build it; restarts (options already captured) pass nil.
func (p *copilotAdapter) startClientLocked(ctx context.Context, secrets *adapterhost.Secrets) (copilotClient, error) {
	if p.clientOptions == nil {
		if secrets == nil {
			return nil, errors.New("copilot: no captured client options and no secrets to build them")
		}
		options := &copilot.ClientOptions{
			Connection: copilot.StdioConnection{Path: cliPath()},
			LogLevel:   "info",
		}
		if err := applyAuthOptions(options, secrets); err != nil {
			return nil, fmt.Errorf("copilot: apply auth options: %w", err)
		}
		p.clientOptions = options
	}

	client := newClientFn(p.clientOptions)
	if err := startClientWithRetry(ctx, client); err != nil {
		return nil, err
	}
	p.client = client
	p.clientEpoch++
	return p.client, nil
}
