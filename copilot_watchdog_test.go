// copilot_watchdog_test.go — CRI-274: provider-call watchdog coverage.
//
// Covers both acceptance cases: a hung provider request (send RPC that never
// answers) and a stream that opens and then goes silent are failed by the
// watchdog and retried through the CRI-272 backoff path instead of wedging the
// turn. Windows are shrunk to milliseconds via withFastWatchdog so the tests
// stay fast and deterministic; margins between "must not stall" and "must
// stall" checks are ≥3× the window.

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// withFastWatchdog shrinks the watchdog windows to milliseconds; both are
// restored on test cleanup.
func withFastWatchdog(t *testing.T, window, gate time.Duration) {
	t.Helper()
	origWindow, origGate := watchdogWindow, watchdogGateWindow
	watchdogWindow = window
	watchdogGateWindow = gate
	t.Cleanup(func() { watchdogWindow, watchdogGateWindow = origWindow, origGate })
}

// withFastStop shrinks the forced-stop grace (stopGrace) for bounded-stop
// tests; the real grace is 5s.
func withFastStop(t *testing.T, grace time.Duration) {
	t.Helper()
	orig := stopGrace
	stopGrace = grace
	t.Cleanup(func() { stopGrace = orig })
}

// hungSendSession blocks its first hangFirst Send calls until release is
// closed, then delegates to the wrapped fake session. Used to script a provider
// request that never responds (CRI-274): the abandoned sendRPCTimeboxed
// goroutine parks until cleanup releases it.
type hungSendSession struct {
	*fakeSession
	release   chan struct{}
	hangFirst int

	mu  sync.Mutex
	snt int
}

func (h *hungSendSession) Send(ctx context.Context, opts *copilot.MessageOptions) (string, error) {
	h.mu.Lock()
	call := h.snt
	h.snt++
	h.mu.Unlock()
	if call < h.hangFirst {
		<-h.release
		return "", errors.New("client stopped")
	}
	return h.fakeSession.Send(ctx, opts)
}

func (h *hungSendSession) attempts() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snt
}

// withRecoverableClient builds an adapter with a fake CLI client whose restart
// path is fully faked, so stall recovery (forced restart + session re-open) is
// observable without spawning a real CLI.
func withRecoverableClient(t *testing.T, fc *fakeClient) *copilotAdapter {
	t.Helper()
	t.Setenv("CRITERIA_HOME", t.TempDir())
	p := newCopilotAdapter()
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })
	return p
}

// newWatchdogSession builds a real (non-bare) sessionState bound to p, with
// fanout, stallNotify, and owner wired, exercising the production shapes.
func newWatchdogSession(t *testing.T, p *copilotAdapter, id string, sess copilotSession) *sessionState {
	t.Helper()
	sc := &copilot.SessionConfig{Model: "m"}
	secrets := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil)
	s := newSessionState(id, sess, sc, buildResumeConfig(sc), secrets, p)
	p.sessions[id] = s
	return s
}

// ── send-RPC watchdog (acceptance 1: hung provider request) ──────────────────

func TestSendRPCTimeboxedFailsHungSend(t *testing.T) {
	withFastWatchdog(t, 30*time.Millisecond, time.Minute)
	release := make(chan struct{})
	hung := &hungSendSession{fakeSession: &fakeSession{sessionID: "sdk-hung"}, release: release, hangFirst: 1 << 30}
	t.Cleanup(func() { close(release) })

	_, err := sendRPCTimeboxed(context.Background(), hung, &copilot.MessageOptions{Prompt: "hi"})
	if !isProviderStallError(err) {
		t.Fatalf("sendRPCTimeboxed err = %v, want a provider stall", err)
	}
	if !strings.Contains(err.Error(), string(stallSendRPC)) {
		t.Fatalf("stall error = %v, want kind %q in the message", err, stallSendRPC)
	}
	if !strings.Contains(err.Error(), "no provider activity for") {
		t.Fatalf("stall error = %v, want the silence duration in the message", err)
	}
}

func TestSendRPCTimeboxedPassesThroughSuccessfulSend(t *testing.T) {
	withFastWatchdog(t, 30*time.Millisecond, time.Minute)
	fake := &fakeSession{sessionID: "sdk-ok"}

	msgID, err := sendRPCTimeboxed(context.Background(), fake, &copilot.MessageOptions{Prompt: "hi"})
	if err != nil || msgID != "msg-1" {
		t.Fatalf("sendRPCTimeboxed = (%q, %v), want (msg-1, nil)", msgID, err)
	}
}

func TestSendRPCTimeboxedHonorsContextCancellation(t *testing.T) {
	withFastWatchdog(t, time.Minute, time.Minute)
	release := make(chan struct{})
	hung := &hungSendSession{fakeSession: &fakeSession{sessionID: "sdk-hung"}, release: release, hangFirst: 1 << 30}
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := sendRPCTimeboxed(ctx, hung, &copilot.MessageOptions{Prompt: "hi"})
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sendRPCTimeboxed ignored context cancellation")
	}
}

// TestSendWithRetryRecoversHungSendWithForcedRestart: a provider request that
// never responds is failed by the send-RPC watchdog and retried per the
// CRI-272 backoff path (acceptance 1). Recovery must FORCE the restart — an
// alive-but-wedged CLI child answers the Ping liveness probe, so the gated
// probe would never restart it — and the second attempt must succeed on the
// re-opened session.
func TestSendWithRetryRecoversHungSendWithForcedRestart(t *testing.T) {
	withFastWatchdog(t, 30*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil} // deliberately healthy: force must not consult it
	p := withRecoverableClient(t, fc)

	release := make(chan struct{})
	hung := &hungSendSession{fakeSession: &fakeSession{sessionID: "sdk-hung"}, release: release, hangFirst: 1}
	t.Cleanup(func() { close(release) })
	fc.handOut = hung
	s := newWatchdogSession(t, p, "adapter-hung", hung)

	msgID, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"})
	if err != nil {
		t.Fatalf("sendWithRetry returned error: %v", err)
	}
	if msgID != "msg-1" {
		t.Fatalf("msgID = %q, want msg-1 from the second attempt", msgID)
	}
	if got := hung.attempts(); got != 2 {
		t.Fatalf("send attempts = %d, want 2 (hung attempt + retry)", got)
	}
	if got := *sleeps; len(got) != 1 || got[0] != time.Second {
		t.Fatalf("backoff sequence = %v, want [1s] (CRI-272 path)", got)
	}
	if fc.stopCount != 1 || fc.startCount != 1 {
		t.Fatalf("stop/start = %d/%d, want 1/1 (forced CLI-child restart)", fc.stopCount, fc.startCount)
	}
	if fc.pingCount != 0 {
		t.Fatalf("ping count = %d, want 0 (stall recovery must force, skipping the probe)", fc.pingCount)
	}
	if fc.createCount != 1 {
		t.Fatalf("create calls = %d, want 1 (session re-opened on the new runtime)", fc.createCount)
	}
}

// TestSendWithRetryExhaustsOnPersistentStall: a provider that hangs on every
// attempt must not wedge the call forever — the CRI-272 bound (retryMaxAttempts)
// applies to stalls too, and the surfaced error stays stall-typed.
func TestSendWithRetryExhaustsOnPersistentStall(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	release := make(chan struct{})
	hung := &hungSendSession{fakeSession: &fakeSession{sessionID: "sdk-hung"}, release: release, hangFirst: 1 << 30}
	t.Cleanup(func() { close(release) })
	fc.handOut = hung
	s := newWatchdogSession(t, p, "adapter-hung", hung)

	_, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"})
	if err == nil {
		t.Fatal("sendWithRetry must fail after exhausting retries on persistent stalls")
	}
	if !isProviderStallError(err) {
		t.Fatalf("exhaustion error = %v, want it to stay stall-typed", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("retries exhausted after %d attempts", retryMaxAttempts)) {
		t.Fatalf("exhaustion error = %v, want the attempts summary", err)
	}
	if got := hung.attempts(); got != retryMaxAttempts {
		t.Fatalf("send attempts = %d, want %d", got, retryMaxAttempts)
	}
	if fc.stopCount != retryMaxAttempts-1 {
		t.Fatalf("stop count = %d, want %d (forced restart before each retry)", fc.stopCount, retryMaxAttempts-1)
	}
	if got := *sleeps; len(got) != retryMaxAttempts-1 || got[0] != time.Second || got[1] != 4*time.Second || got[2] != 16*time.Second {
		t.Fatalf("backoff sequence = %v, want [1s 4s 16s]", got)
	}
}

// ── stream watchdog (acceptance 2: stream opens then goes silent) ────────────

func TestWaitTurnSignalFailsSilentStream(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fake := &fakeSession{sessionID: "sdk-silent"}
	p := newCopilotAdapter()
	s := newWatchdogSession(t, p, "adapter-silent", fake)
	ts := newTurnState(0)

	s.markActivity()
	signal, err := ts.waitTurnSignal(context.Background(), s)
	if signal != turnSignalStall {
		t.Fatalf("turn signal = %v, want turnSignalStall", signal)
	}
	if !isProviderStallError(err) {
		t.Fatalf("stall err = %v, want a provider stall", err)
	}
	if !strings.Contains(err.Error(), string(stallStream)) {
		t.Fatalf("stall error = %v, want kind %q", err, stallStream)
	}
}

// stallSignal carries a waitTurnSignal result out of a goroutine under test.
type stallSignal struct {
	signal turnSignal
	err    error
}

// TestWaitTurnSignalContinuousActivityPreventsStall: a healthy streaming
// response emits events continuously; the window must re-arm on every event
// and never fire while the stream is alive.
func TestWaitTurnSignalContinuousActivityPreventsStall(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	s := &sessionState{session: &fakeSession{}, stallNotify: make(chan struct{}, 1)}
	ts := newTurnState(0)
	s.markActivity()

	results := make(chan stallSignal, 1)
	go func() {
		signal, err := ts.waitTurnSignal(context.Background(), s)
		results <- stallSignal{signal, err}
	}()

	// Feed activity for 10 × window/2 = 5 windows: well past any single window.
	for i := 0; i < 10; i++ {
		s.markActivity()
		time.Sleep(watchdogWindow / 2)
	}
	select {
	case sig := <-results:
		t.Fatalf("waitTurnSignal returned during continuous activity: %v, %v", sig.signal, sig.err)
	default:
	}

	// Once the activity stops, the next window must expire into a stall.
	deadline := time.After(2 * time.Second)
	select {
	case sig := <-results:
		if sig.signal != turnSignalStall || !isProviderStallError(sig.err) {
			t.Fatalf("post-activity result = (%v, %v), want a stall", sig.signal, sig.err)
		}
	case <-deadline:
		t.Fatal("waitTurnSignal did not stall after activity stopped")
	}
}

// ── watchdog gates: adapter-side waits that suspend provider activity ────────

// TestWaitTurnSignalGateDefersStallUntilRelease: while a gate is held (host
// permission decision, adapter/native tool run), provider silence is expected
// and must not stall the turn; once the gate is released the silence window
// re-arms from the fresh baseline and a still-silent provider stalls normally.
func TestWaitTurnSignalGateDefersStallUntilRelease(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	s := &sessionState{session: &fakeSession{}, stallNotify: make(chan struct{}, 1)}
	ts := newTurnState(0)
	s.markActivity()

	release := s.beginWatchdogGate()
	results := make(chan stallSignal, 1)
	go func() {
		signal, err := ts.waitTurnSignal(context.Background(), s)
		results <- stallSignal{signal, err}
	}()

	// 3× the silence window with the gate held: must still be waiting.
	time.Sleep(3 * watchdogWindow)
	select {
	case sig := <-results:
		t.Fatalf("gate did not disarm the watchdog: (%v, %v)", sig.signal, sig.err)
	default:
	}

	release()
	select {
	case sig := <-results:
		if sig.signal != turnSignalStall || !isProviderStallError(sig.err) {
			t.Fatalf("post-release result = (%v, %v), want a stream stall", sig.signal, sig.err)
		}
		if !strings.Contains(sig.err.Error(), string(stallStream)) {
			t.Fatalf("post-release stall = %v, want kind %q", sig.err, stallStream)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitTurnSignal did not stall after the gate was released")
	}
}

// TestWaitTurnSignalGateCeilingFires: a gate that never closes (e.g. the CLI
// died mid native-tool without emitting completion) must still fail the call
// after watchdogGateWindow, so a stuck gate cannot recreate the indefinite
// hang this watchdog exists to prevent.
func TestWaitTurnSignalGateCeilingFires(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, 60*time.Millisecond)
	s := &sessionState{session: &fakeSession{}, stallNotify: make(chan struct{}, 1)}
	ts := newTurnState(0)
	s.markActivity()

	release := s.beginWatchdogGate()
	defer release()
	results := make(chan stallSignal, 1)
	go func() {
		signal, err := ts.waitTurnSignal(context.Background(), s)
		results <- stallSignal{signal, err}
	}()

	select {
	case sig := <-results:
		if sig.signal != turnSignalStall || !isProviderStallError(sig.err) {
			t.Fatalf("result = (%v, %v), want a gate-ceiling stall", sig.signal, sig.err)
		}
		if !strings.Contains(sig.err.Error(), string(stallGateCeiling)) {
			t.Fatalf("stall = %v, want kind %q", sig.err, stallGateCeiling)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stuck gate did not trip the watchdog ceiling")
	}
}

// TestToolGatesKeyedByToolCallID: native tool gates are opened on
// tool.execution_start and closed on the matching completion; completions for
// unknown calls are no-ops, and drainStaleSignals force-closes gates left open
// by an abandoned call so they cannot leak into the retried attempt.
func TestToolGatesKeyedByToolCallID(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	p := newCopilotAdapter()
	s := newWatchdogSession(t, p, "adapter-gates", &fakeSession{sessionID: "sdk-gates"})
	ts := newTurnState(0)
	handler := ts.handleEvent(s, &recordingSender{})

	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionStartData{ToolCallID: "tc-1"}})
	if s.gatedWaits.Load() != 1 {
		t.Fatalf("gatedWaits = %d, want 1 while the native tool runs", s.gatedWaits.Load())
	}
	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionCompleteData{ToolCallID: "tc-1"}})
	if s.gatedWaits.Load() != 0 {
		t.Fatalf("gatedWaits = %d, want 0 after the tool completed", s.gatedWaits.Load())
	}

	// A completion without a matching start must not underflow the counter.
	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionCompleteData{ToolCallID: "tc-unknown"}})
	if s.gatedWaits.Load() != 0 {
		t.Fatalf("gatedWaits = %d, want 0 (unmatched completion is a no-op)", s.gatedWaits.Load())
	}

	// An abandoned call's never-completed tool gate must be force-closed.
	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionStartData{ToolCallID: "tc-stuck"}})
	ts.drainStaleSignals(s)
	if s.gatedWaits.Load() != 0 {
		t.Fatalf("gatedWaits = %d, want 0 after drainStaleSignals", s.gatedWaits.Load())
	}

	// drainStaleSignals closes EVERY open gate (it runs only when a call is
	// abandoned and the wedged child will never emit completions), so a gate
	// opened even after the first drain is closed by the next one.
	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionStartData{ToolCallID: "tc-fresh"}})
	ts.drainStaleSignals(s)
	if s.gatedWaits.Load() != 0 {
		t.Fatalf("gatedWaits = %d, want 0 (drain closes every open gate)", s.gatedWaits.Load())
	}
}

// TestDrainStaleSignalsClearsLateSignals: a late session.idle or handler error
// from an abandoned provider call must be dropped before a re-send, so the
// retried attempt cannot mistake them for its own outcome.
func TestDrainStaleSignalsClearsLateSignals(t *testing.T) {
	p := newCopilotAdapter()
	s := newWatchdogSession(t, p, "adapter-drain", &fakeSession{sessionID: "sdk-drain"})
	ts := newTurnState(0)

	ts.turnDone <- struct{}{} // late session.idle
	ts.sendErr(errors.New("late handler error"))
	release := s.beginWatchdogGate() // abandoned native-tool gate
	ts.drainStaleSignals(s)
	release()

	select {
	case <-ts.turnDone:
		t.Fatal("stale session.idle survived drainStaleSignals")
	default:
	}
	select {
	case err := <-ts.errCh:
		t.Fatalf("stale handler error survived drainStaleSignals: %v", err)
	default:
	}
	if s.gatedWaits.Load() != 0 {
		t.Fatalf("gatedWaits = %d, want 0 after the drain", s.gatedWaits.Load())
	}
}

// ── stall classification ─────────────────────────────────────────────────────

func TestStallErrorClassification(t *testing.T) {
	stall := stallError(stallStream, time.Unix(0, 0), 90*time.Second)
	if !isProviderStallError(stall) {
		t.Fatal("stallError must classify as a provider stall")
	}
	if !isProviderStallError(fmt.Errorf("copilot: send prompt: %w", stall)) {
		t.Fatal("wrapped stall must stay stall-typed through errors.Is")
	}
	if !isProviderStallError(fmt.Errorf("copilot: provider call stalled 3 times without recovery: %w", stall)) {
		t.Fatal("retry-exhaustion-wrapped stall must stay stall-typed")
	}
	for name, err := range map[string]error{
		"nil":             nil,
		"context":         context.Canceled,
		"provider 5xx":    fmt.Errorf("failed to send message: HTTP 502"),
		"max turns":       errMaxTurnsReached,
		"transport death": fmt.Errorf("failed to send message: CLI process exited: EOF"),
	} {
		if isProviderStallError(err) {
			t.Errorf("%s must not classify as a provider stall", name)
		}
	}
}

// ── forced restart semantics ─────────────────────────────────────────────────

// TestRestartClientForceSemantics: without force, a healthy CLI child is left
// alone (the gated Ping probe governs); with force, a live child is stopped
// unconditionally — watchdog recovery must not be fooled by a child that
// answers liveness probes while its provider stream is wedged.
func TestRestartClientForceSemantics(t *testing.T) {
	t.Setenv("CRITERIA_HOME", t.TempDir())
	p := newCopilotAdapter()
	fc := &fakeClient{pingErr: nil} // healthy
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })

	_, restarted, err := p.restartClient(context.Background(), false)
	if err != nil || restarted {
		t.Fatalf("healthy client restart (force=false) = (%v, %v), want no restart", restarted, err)
	}
	if fc.stopCount != 0 || fc.pingCount != 1 {
		t.Fatalf("stop=%d ping=%d, want stop=0 ping=1 (probe consulted)", fc.stopCount, fc.pingCount)
	}

	_, restarted, err = p.restartClient(context.Background(), true)
	if err != nil || !restarted {
		t.Fatalf("forced restart = (restarted=%v, err=%v), want a restart", restarted, err)
	}
	if fc.stopCount != 1 || fc.pingCount != 1 || fc.forceStopCount != 0 {
		t.Fatalf("stop=%d ping=%d forceStop=%d, want stop=1 ping=1 forceStop=0 (force must skip the probe; a prompt Stop needs no kill)",
			fc.stopCount, fc.pingCount, fc.forceStopCount)
	}
}

// TestRestartClientBoundsWedgedStopOnForce: SDK Stop disconnects every session
// before killing the child, and those disconnect RPCs are unbounded — a child
// that stays alive but stops servicing RPCs wedges Stop itself (observed
// against copilot-sdk/go v1.0.0). Forced stall recovery must still return
// within the stopGrace bound, kill the child via ForceStop, and replace the
// client (CRI-274).
func TestRestartClientBoundsWedgedStopOnForce(t *testing.T) {
	withFastStop(t, 30*time.Millisecond)
	fc := &fakeClient{stopBlock: make(chan struct{})}
	t.Cleanup(func() { close(fc.stopBlock) }) // let the abandoned Stop goroutine exit
	p := withRecoverableClient(t, fc)

	bounded := time.After(2 * time.Second)
	client, restarted, err := p.restartClient(context.Background(), true)
	if err != nil || !restarted {
		t.Fatalf("forced restart with wedged Stop = (restarted=%v, err=%v), want a restart", restarted, err)
	}
	select {
	case <-bounded:
		t.Fatalf("restartClient did not return within the bound")
	default:
	}
	if fc.stopCount != 1 || fc.forceStopCount != 1 {
		t.Fatalf("stop=%d forceStop=%d, want the wedged client stopped once and the child killed past the grace",
			fc.stopCount, fc.forceStopCount)
	}
	if client == nil || fc.startCount == 0 {
		t.Fatalf("startCount=%d, want the replacement client started", fc.startCount)
	}
}

// TestRestartClientUnforcedStopStaysFast: the non-force path keeps its
// original semantics — a child that fails Ping has a broken connection, so
// Stop returns promptly and ForceStop is never reached.
func TestRestartClientUnforcedStopStaysFast(t *testing.T) {
	fc := &fakeClient{pingErr: errors.New("client not connected")}
	p := withRecoverableClient(t, fc)

	_, restarted, err := p.restartClient(context.Background(), false)
	if err != nil || !restarted {
		t.Fatalf("restart of dead child = (restarted=%v, err=%v), want a restart", restarted, err)
	}
	if fc.stopCount != 1 || fc.forceStopCount != 0 {
		t.Fatalf("stop=%d forceStop=%d, want stop=1 forceStop=0 (non-force keeps the fast graceful stop)",
			fc.stopCount, fc.forceStopCount)
	}
}

// TestRecoverTransportBoundsWedgedStop: the full stall-recovery sweep —
// restartClient(force) plus reopenSession for every live session — completes
// within the stopGrace bound even when the previous client's Stop never
// returns, so a stalled turn can proceed to its retry (CRI-274).
func TestRecoverTransportBoundsWedgedStop(t *testing.T) {
	withFastStop(t, 30*time.Millisecond)
	fc := &fakeClient{stopBlock: make(chan struct{})}
	t.Cleanup(func() { close(fc.stopBlock) })
	p := withRecoverableClient(t, fc)
	s := newWatchdogSession(t, p, "sess-wedge", &fakeSession{sessionID: "sdk-orig"})
	before := s.currentSession().SessionID()

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		p.recoverTransport(context.Background(), s, true)
	}()
	select {
	case <-swept:
	case <-time.After(2 * time.Second):
		t.Fatalf("recoverTransport did not complete within the bound")
	}
	if fc.forceStopCount != 1 {
		t.Fatalf("forceStop=%d, want the wedged child killed past the grace", fc.forceStopCount)
	}
	if after := s.currentSession().SessionID(); after == before {
		t.Fatalf("trigger session not re-opened after forced recovery")
	}
}

// ── end-to-end Execute coverage ──────────────────────────────────────────────

// TestExecuteStalledTurnIsRetriedThenSucceeds: the stream opens (one delta)
// and then goes silent — the watchdog fails the whole call, recovery force-
// restarts the CLI child and re-opens the session, and the re-sent prompt
// completes with the declared outcome (acceptance 1 + 2 through Execute).
func TestExecuteStalledTurnIsRetriedThenSucceeds(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	// Attempt 1: a delta event, then silence → stream stall.
	silent := &fakeSession{
		sessionID: "sdk-silent",
		emitOnSend: []copilot.SessionEvent{
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: "partial"}},
		},
	}
	// Attempt 2 (after recovery): finalize + assistant message + idle.
	done := &fakeSession{sessionID: "sdk-done"}
	done.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := p.handleSubmitOutcome("adapter-x", SubmitOutcomeArgs{Outcome: "success", Reason: "ok"}); err != nil {
			t.Errorf("handleSubmitOutcome on retry: %v", err)
		}
	}
	done.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m2", Content: "done"}},
		{Data: &copilot.SessionIdleData{}},
	}
	fc.handOut = done
	newWatchdogSession(t, p, "adapter-x", silent)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-x",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")
	if got := silent.sendCount; got != 1 {
		t.Fatalf("silent session sends = %d, want 1", got)
	}
	if got := done.sendCount; got != 1 {
		t.Fatalf("re-opened session sends = %d, want 1 (the retried prompt)", got)
	}
	if got := *sleeps; len(got) != 1 || got[0] != time.Second {
		t.Fatalf("backoff sequence = %v, want [1s]", got)
	}
	if fc.stopCount != 1 || fc.startCount != 1 || fc.createCount != 1 {
		t.Fatalf("stop/start/create = %d/%d/%d, want 1/1/1 (forced restart + re-open)", fc.stopCount, fc.startCount, fc.createCount)
	}
}

// TestExecuteStallExhaustionFailsCall: a provider that stalls on every attempt
// must fail the Execute with a bounded, stall-typed error after
// watchdogMaxCallAttempts instead of wedging the turn.
func TestExecuteStallExhaustionFailsCall(t *testing.T) {
	withFastWatchdog(t, 15*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	silent := &fakeSession{
		sessionID: "sdk-silent",
		emitOnSend: []copilot.SessionEvent{
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: "partial"}},
		},
	}
	fc.handOut = silent
	newWatchdogSession(t, p, "adapter-stuck", silent)

	sender := &recordingSender{}
	err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-stuck",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender)
	if err == nil {
		t.Fatal("Execute must fail after watchdogMaxCallAttempts stalls")
	}
	if !isProviderStallError(err) {
		t.Fatalf("Execute error = %v, want it to stay stall-typed", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("stalled %d times", watchdogMaxCallAttempts)) {
		t.Fatalf("Execute error = %v, want the stall-attempts summary", err)
	}
	if got := silent.sendCount; got != watchdogMaxCallAttempts {
		t.Fatalf("send count = %d, want %d (1 initial + %d retries)", got, watchdogMaxCallAttempts, watchdogMaxCallAttempts-1)
	}
	if got := *sleeps; len(got) != 2 || got[0] != time.Second || got[1] != 4*time.Second {
		t.Fatalf("backoff sequence = %v, want [1s 4s]", got)
	}
	if fc.stopCount != watchdogMaxCallAttempts-1 {
		t.Fatalf("stop count = %d, want %d (forced restart before each retry)", fc.stopCount, watchdogMaxCallAttempts-1)
	}
}

// TestExecutePreservesFinalizedOutcomeAcrossStall: a turn that finalized
// successfully and THEN went silent must not be re-prompted — the recorded
// outcome is returned directly, because re-prompting risks a duplicate-
// finalize failure on the resumed conversation.
func TestExecutePreservesFinalizedOutcomeAcrossStall(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	finalized := &fakeSession{sessionID: "sdk-finalized"}
	pHandle := p
	finalized.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := pHandle.handleSubmitOutcome("adapter-fin", SubmitOutcomeArgs{Outcome: "success", Reason: "done"}); err != nil {
			t.Errorf("handleSubmitOutcome: %v", err)
		}
	}
	// The assistant message arrives, but the stream then goes silent before
	// session.idle — the turn stalls AFTER the finalize.
	finalized.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
	}
	newWatchdogSession(t, p, "adapter-fin", finalized)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-fin",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")
	if got := finalized.sendCount; got != 1 {
		t.Fatalf("send count = %d, want 1 (a finalized turn must not be re-prompted)", got)
	}
	if got := *sleeps; len(got) != 0 {
		t.Fatalf("backoff sleeps = %v, want none (no retry after finalize)", got)
	}
	if fc.stopCount != 0 {
		t.Fatalf("stop count = %d, want 0 (no recovery once the outcome is recorded)", fc.stopCount)
	}
}