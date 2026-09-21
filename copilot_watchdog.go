// copilot_watchdog.go — provider-call watchdog for SDK sends and streaming
// turns (CRI-274).
//
// The adapter owns first-line monitoring of its own provider calls: a normal
// streaming response produces SDK session events continuously, so silence on
// an in-flight call is the signature of a wedged provider request (observed
// live: a turn sat silent 51+ minutes with zero provider requests after the
// stream hung). The watchdog bounds that silence:
//
//   - sendRPCTimeboxed bounds the session.send RPC itself. The SDK's jsonrpc2
//     Request has no context or timeout of its own — it unblocks only on a
//     response, client stop, or process exit — so an unresponsive CLI parks
//     Send forever without adapter-level timeboxing.
//   - waitTurnSignal bounds the inter-event silence of a streaming turn: a
//     stream that opens and then goes silent for watchdogWindow is failed.
//   - Adapter-side waits that legitimately suspend provider activity (a host
//     permission decision, an adapter tool call, a native CLI tool execution)
//     hold a gate that swaps the silence window for watchdogGateWindow, so
//     long-but-healthy tool runs are not misread as stalls while a stuck gate
//     still cannot recreate an indefinite hang.
//
// A watchdog stall is a retryable call failure: executeTurn and sendWithRetry
// classify it via isProviderStallError and route it through the CRI-272
// backoff path. Because an alive-but-wedged CLI child passes the Ping
// liveness probe, stall recovery forces the restart (recoverTransport force).
// The forced teardown itself is bounded: SDK Stop can wedge on the per-session
// disconnect RPCs it issues before killing the child, so past stopGrace the
// child is killed via ForceStop (stopClientBounded) — failing the pending
// RPCs, which unblocks both the wedged Stop and the abandoned in-flight Send
// goroutine, whose result lands in a buffered channel (no leak beyond that).

//
// CRI-277: both windows are configurable per session through the agent-level
// `watchdog_window` / `watchdog_gate_window` config keys (see
// parseWatchdogSettings); the package vars remain the shipped defaults, the
// test overrides, and the fallback for sessions opened without the keys.

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	copilot "github.com/github/copilot-sdk/go"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// Watchdog policy: a provider call that produces no response bytes or session
// events for watchdogWindow is failed as a stall; a gate-held wait (host
// decision, adapter/native tool execution) may be silent for up to
// watchdogGateWindow. An Execute re-sends the prompt at most
// watchdogMaxCallAttempts times after stalls, so the worst-case wall time per
// call is bounded (~3 × 90s of silence plus backoff and recovery time) instead
// of the unbounded wedge this replaces. The vars are the shipped defaults,
// overridable in tests, and the fallback for sessions opened without the
// CRI-277 config keys (see parseWatchdogSettings).
var (
	watchdogWindow     = 90 * time.Second
	watchdogGateWindow = 10 * time.Minute
	// watchdogNow is the clock used for watchdog deadlines (tests).
	watchdogNow = time.Now
)

// CRI-277 agent-level config keys for the watchdog windows. Values are Go
// duration strings (time.ParseDuration; e.g. "90s", "10m").
const (
	watchdogWindowCfgKey     = "watchdog_window"
	watchdogGateWindowCfgKey = "watchdog_gate_window"
)

// Config guardrails, derived from the CRI-277 healthy-gap data (15 runs,
// ~15K events: 12 healthy turns, 3 death turns): the worst inter-event gap on
// a healthy turn was 217s (a gate-held build/test phase), the worst gate-held
// wait reached ~10m, and the smallest silent gap on a death run was 994s.
const (
	// maxWatchdogWindow is the ceiling for the configured base window: a base
	// window at or above the smallest observed death-run silence would catch
	// every documented death mode only slower than the runs that died.
	maxWatchdogWindow = 994 * time.Second
	// minWatchdogGateWindow is the floor for the configured gate ceiling:
	// gate-held waits legitimately reach ~10m on healthy build/test-heavy
	// turns, so a tighter ceiling false-stalls healthy turns.
	minWatchdogGateWindow = 10 * time.Minute
)

// watchdogSettings is the per-session watchdog window configuration parsed
// from the agent-level config (CRI-277). Zero fields fall back to the package
// defaults (watchdogWindow / watchdogGateWindow) so sessions opened without
// the config keys — and bare unit-test states — keep the shipped behavior.
// Written once at OpenSession and read-only afterwards.
type watchdogSettings struct {
	window     time.Duration
	gateWindow time.Duration
}

// resolved fills unset (non-positive) fields with the current package
// defaults.
func (w watchdogSettings) resolved() watchdogSettings {
	if w.window <= 0 {
		w.window = watchdogWindow
	}
	if w.gateWindow <= 0 {
		w.gateWindow = watchdogGateWindow
	}
	return w
}

// parseWatchdogSettings parses and validates the CRI-277 watchdog config keys
// (agent-level, the OpenSession config map). Unset keys yield zero fields.
// Validation against the healthy-gap data: both windows must be positive Go
// durations, the base window must stay below the smallest death-run silence
// gap (maxWatchdogWindow) so the watchdog keeps catching the documented death
// modes, and the gate ceiling must stay at or above the worst observed
// healthy gate-held wait (minWatchdogGateWindow) and never below the base
// window — a gate ceiling tighter than the base window would protect nothing.
func parseWatchdogSettings(cfg map[string]string) (watchdogSettings, error) {
	var w watchdogSettings
	for _, spec := range []struct {
		key string
		dst *time.Duration
	}{
		{watchdogWindowCfgKey, &w.window},
		{watchdogGateWindowCfgKey, &w.gateWindow},
	} {
		raw := strings.TrimSpace(cfg[spec.key])
		if raw == "" {
			continue
		}
		d, err := time.ParseDuration(raw)
		if err != nil {
			return watchdogSettings{}, fmt.Errorf("copilot: config %s: %q is not a valid duration (e.g. 90s, 10m): %w", spec.key, raw, err)
		}
		if d <= 0 {
			return watchdogSettings{}, fmt.Errorf("copilot: config %s: %q must be positive", spec.key, raw)
		}
		*spec.dst = d
	}

	w = w.resolved()
	if w.window >= maxWatchdogWindow {
		return watchdogSettings{}, fmt.Errorf("copilot: config %s: %s must stay below %s — the smallest silent gap observed on a death run (CRI-277 healthy-gap data); a window that large would catch the documented death modes later than every run that actually died", watchdogWindowCfgKey, w.window, maxWatchdogWindow)
	}
	if w.gateWindow < minWatchdogGateWindow {
		return watchdogSettings{}, fmt.Errorf("copilot: config %s: %s must stay at or above %s — the worst gate-held wait observed on a healthy turn (CRI-277 healthy-gap data); a tighter ceiling false-stalls healthy build/test-heavy turns", watchdogGateWindowCfgKey, w.gateWindow, minWatchdogGateWindow)
	}
	if w.gateWindow < w.window {
		return watchdogSettings{}, fmt.Errorf("copilot: config %s: %s must not be below %s (%s)", watchdogGateWindowCfgKey, w.gateWindow, watchdogWindowCfgKey, w.window)
	}
	return w, nil
}

// watchdogWindow returns the session's send/stream silence window: the
// session-configured value (CRI-277) or the package default when unset. Safe
// on bare unit-test states (zero settings fall back to the default).
func (s *sessionState) watchdogWindow() time.Duration {
	if w := s.watchdog.window; w > 0 {
		return w
	}
	return watchdogWindow
}

// watchdogGateWindow returns the session's gate ceiling, falling back to the
// package default when unset.
func (s *sessionState) watchdogGateWindow() time.Duration {
	if w := s.watchdog.gateWindow; w > 0 {
		return w
	}
	return watchdogGateWindow
}

// stopGrace bounds the graceful Stop of the previous CLI child during forced
// stall recovery (restartClient with force): SDK Stop disconnects every
// session — an unbounded session.destroy RPC per session — before killing the
// child, so a child that stays alive but no longer services RPCs wedges Stop
// itself (observed against copilot-sdk/go v1.0.0). Past the grace,
// stopClientBounded kills the child via ForceStop instead; the kill fails the
// pending RPCs, which unblocks Stop and any Send parked on it.
var stopGrace = 5 * time.Second

// stopClientBounded stops c, killing the CLI child via ForceStop when the
// graceful stop does not finish within stopGrace. The kill does not
// deterministically unblock a wedged Stop (the goroutine is abandoned after a
// second grace if it still has not returned), but the killed child can no
// longer wedge the replacement client started afterwards.
func stopClientBounded(c copilotClient) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Stop()
	}()
	select {
	case <-done:
		return
	case <-time.After(stopGrace):
		c.ForceStop()
		select {
		case <-done:
		case <-time.After(stopGrace):
			// Stop is still wedged past the kill; abandon its goroutine.
			// It exits once the killed child's pipes error out.
		}
	}
}

// watchdogMaxCallAttempts bounds how many whole provider calls (send + await
// outcome) one Execute makes after watchdog stalls: 1 initial + 2 retries.
const watchdogMaxCallAttempts = 3

// errProviderStall is the sentinel every watchdog failure wraps. Classification
// goes through errors.Is so wrapper chains (retry-exhaustion, prompt context)
// stay stall-typed.
var errProviderStall = errors.New("provider call stalled")

// stallKind labels which watchdog fired, for actionable diagnostics.
type stallKind string

const (
	// stallSendRPC: the session.send RPC produced no response in time.
	stallSendRPC stallKind = "send_no_response"
	// stallStream: the streaming turn produced no session events in time.
	stallStream stallKind = "stream_silent"
	// stallGateCeiling: a held gate stayed silent for watchdogGateWindow.
	stallGateCeiling stallKind = "gate_silent"
)

// stallError builds a stall error naming the watchdog that fired, how long the
// call was silent, and when the last activity was observed.
func stallError(kind stallKind, lastActivity time.Time, silentFor time.Duration) error {
	return fmt.Errorf("copilot: watchdog: %s: no provider activity for %s (last activity %s): %w",
		kind, silentFor, lastActivity.UTC().Format(time.RFC3339Nano), errProviderStall)
}

// isProviderStallError reports whether err is a watchdog stall (CRI-274).
func isProviderStallError(err error) bool {
	return errors.Is(err, errProviderStall)
}

// sendRPCTimeboxed runs sess.Send under the send-RPC watchdog: a Send that
// produces no JSON-RPC response within window (the session's watchdog window,
// CRI-277) is failed as a stall.
// The abandoned goroutine parks until the response arrives or the transport is
// torn down (a forced recovery stops the CLI child, which unblocks pending
// jsonrpc2 requests); its result lands in the buffered channel and is dropped.
func sendRPCTimeboxed(ctx context.Context, sess copilotSession, opts *copilot.MessageOptions, window time.Duration) (string, error) {
	type sendResult struct {
		msgID string
		err   error
	}
	start := watchdogNow()
	ch := make(chan sendResult, 1)
	go func() {
		msgID, err := sess.Send(ctx, opts)
		ch <- sendResult{msgID, err}
	}()
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res.msgID, res.err
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
		return "", stallError(stallSendRPC, start, watchdogNow().Sub(start))
	}
}

// markActivity records provider-side progress (any SDK session event, a send
// attempt, a gate transition) and wakes a parked waitTurnSignal so it can
// re-baseline. Safe on bare unit-test states (nil stallNotify).
func (s *sessionState) markActivity() {
	s.lastActivityNs.Store(watchdogNow().UnixNano())
	s.notifyWatchdog()
}

// notifyWatchdog pings the parked waiter without blocking; the channel holds
// one token because every recompute re-reads fresh state, so a coalesced
// wakeup can never lose a transition.
func (s *sessionState) notifyWatchdog() {
	if s.stallNotify == nil {
		return
	}
	select {
	case s.stallNotify <- struct{}{}:
	default:
	}
}

// beginWatchdogGate marks the start of an adapter-side wait that legitimately
// suspends provider-side activity (host permission decision, adapter or native
// tool execution). While a gate is held, waitTurnSignal swaps the silence
// window for watchdogGateWindow. The returned release func must be called
// exactly once (defer) when the wait ends; release re-baselines activity so
// the wait after the gate closes gets a fresh window.
func (s *sessionState) beginWatchdogGate() func() {
	s.gatedWaits.Add(1)
	s.markActivity()
	return func() {
		s.gatedWaits.Add(-1)
		s.markActivity()
	}
}

// openToolGate opens the watchdog gate for a native CLI tool execution keyed
// by its tool call ID; re-opening an already-open gate is a no-op. Gates are
// keyed so concurrent tool executions close independently, and
// drainStaleSignals closes every gate left open by an abandoned call.
func (s *sessionState) openToolGate(toolCallID string) {
	if toolCallID == "" {
		return
	}
	s.toolGateMu.Lock()
	defer s.toolGateMu.Unlock()
	if s.toolGates == nil {
		s.toolGates = map[string]func(){}
	}
	if _, open := s.toolGates[toolCallID]; open {
		return
	}
	s.toolGates[toolCallID] = s.beginWatchdogGate()
}

// closeToolGate closes the gate opened for the tool execution with this ID.
func (s *sessionState) closeToolGate(toolCallID string) {
	if toolCallID == "" {
		return
	}
	s.toolGateMu.Lock()
	release, open := s.toolGates[toolCallID]
	delete(s.toolGates, toolCallID)
	s.toolGateMu.Unlock()
	if open {
		release()
	}
}

// closeAllToolGates force-closes every open tool gate. Called when a call is
// abandoned (stall retry): the wedged child will never emit the matching
// tool.execution_complete, so its gate must not leak into the next attempt.
func (s *sessionState) closeAllToolGates() {
	s.toolGateMu.Lock()
	gates := s.toolGates
	s.toolGates = nil
	s.toolGateMu.Unlock()
	for _, release := range gates {
		release()
	}
}

// turnSignal classifies why waitTurnSignal returned.
type turnSignal int

const (
	turnSignalCtx   turnSignal = iota // ctx done; err = ctx.Err()
	turnSignalErr                     // handler error; err = the handler error
	turnSignalIdle                    // session.idle; err = nil
	turnSignalStall                   // watchdog stall; err = stall error
)

// waitTurnSignal blocks until the turn ends (session.idle), a handler error
// surfaces, ctx is done, or the watchdog fires. The silence window is measured
// from the last observed activity (any SDK session event or send); a gate-held
// wait uses watchdogGateWindow instead. Every wakeup re-reads fresh activity,
// so continuous event flow keeps re-arming the window and a timer racing a
// fresh event cannot produce a false stall.
func (ts *turnState) waitTurnSignal(ctx context.Context, s *sessionState) (turnSignal, error) {
	for {
		if ctx.Err() != nil {
			return turnSignalCtx, ctx.Err()
		}
		if s.gatedWaits.Load() > 0 {
			// Gate held: provider silence is expected. Bound the total wait
			// with the gate ceiling so a stuck gate (e.g. the CLI died mid
			// native-tool without emitting completion) still fails the call.
			// The ceiling is the session-configured watchdogGateWindow
			// (CRI-277).
			gate := s.watchdogGateWindow()
			timer := time.NewTimer(gate)
			select {
			case <-ctx.Done():
				timer.Stop()
				return turnSignalCtx, ctx.Err()
			case err := <-ts.errCh:
				timer.Stop()
				return turnSignalErr, err
			case <-ts.turnDone:
				timer.Stop()
				return turnSignalIdle, nil
			case <-s.stallNotify:
				timer.Stop()
				continue
			case <-timer.C:
				return turnSignalStall, stallError(stallGateCeiling, time.Unix(0, s.lastActivityNs.Load()), gate)
			}
		}
		last := time.Unix(0, s.lastActivityNs.Load())
		window := s.watchdogWindow()
		delay := last.Add(window).Sub(watchdogNow())
		if delay <= 0 {
			return turnSignalStall, stallError(stallStream, last, watchdogNow().Sub(last))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return turnSignalCtx, ctx.Err()
		case err := <-ts.errCh:
			timer.Stop()
			return turnSignalErr, err
		case <-ts.turnDone:
			timer.Stop()
			return turnSignalIdle, nil
		case <-s.stallNotify:
			timer.Stop()
			// Fresh activity or a gate transition: re-read state and re-arm.
			continue
		case <-timer.C:
			// The baseline deadline elapsed; the re-read below decides whether
			// activity since arming re-arms the wait or the call has stalled.
			continue
		}
	}
}

// drainStaleSignals clears signals an abandoned provider call left behind —
// a late session.idle in turnDone, a late handler error, tool gates never
// closed by a wedged child — so they cannot be mistaken for the outcome of
// the next attempt (CRI-274). Anything buffered before a re-send is stale by
// definition: it was produced by the abandoned call.
func (ts *turnState) drainStaleSignals(s *sessionState) {
	for {
		select {
		case <-ts.turnDone:
		case <-ts.errCh:
		default:
			s.closeAllToolGates()
			return
		}
	}
}

// executeTurn sends the prompt and drives the turn to an outcome, retrying
// whole provider calls when the watchdog fails one (CRI-274). A send failure
// is surfaced as-is (sendWithRetry already retried it); an outcome-stage stall
// triggers a forced transport recovery — the stalled call may still be wedged
// inside an alive CLI child, which the Ping probe cannot detect — followed by
// the CRI-272 backoff and a re-send. A turn that stalled after successfully
// finalizing is not retried: the recorded outcome is returned directly, since
// re-prompting would risk a duplicate-finalize failure on the resumed
// conversation.
func (ts *turnState) executeTurn(ctx context.Context, s *sessionState, opts *copilot.MessageOptions, sink adapterhost.ExecuteEventSender) error {
	for attempt := 0; ; attempt++ {
		ts.drainStaleSignals(s)

		if _, err := s.sendWithRetry(ctx, opts); err != nil {
			return fmt.Errorf("copilot: send prompt: %w", err)
		}
		err := ts.awaitOutcome(ctx, s, sink)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isProviderStallError(err) {
			return err
		}

		s.mu.Lock()
		finalized := s.finalizedOutcome
		reason := s.finalizedReason
		secrets := s.heldSecrets
		s.mu.Unlock()
		if finalized != "" {
			return sink.Send(resultEvent(finalized, reason, secrets...))
		}

		if attempt+1 >= watchdogMaxCallAttempts {
			return fmt.Errorf("copilot: provider call stalled %d times without recovery: %w", attempt+1, err)
		}
		if s.owner != nil {
			s.owner.recoverTransport(ctx, s, true)
		}
		if err := retrySleep(ctx, backoffDelay(attempt)); err != nil {
			return err
		}
	}
}
