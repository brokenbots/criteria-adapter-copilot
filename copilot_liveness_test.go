// copilot_liveness_test.go — CRI-288: forwarding-stall seam and liveness tick.

package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// withFastLiveness shrinks the liveness windows so ticks fire on test scale;
// production is 5min tick / 30s check.
func withFastLiveness(t *testing.T, interval, check time.Duration) {
	t.Helper()
	origInterval, origCheck := livenessTickInterval, livenessCheckInterval
	livenessTickInterval = interval
	livenessCheckInterval = check
	t.Cleanup(func() { livenessTickInterval, livenessCheckInterval = origInterval, origCheck })
}

// emitToHandlers drives the handlers registered on f (the fanout subscribed by
// Execute), mirroring how a real SDK session delivers session events.
func (f *fakeSession) emitToHandlers(ev copilot.SessionEvent) {
	f.mu.Lock()
	handlers := make([]copilot.SessionEventHandler, 0, len(f.handlers))
	for _, h := range f.handlers {
		if h != nil {
			handlers = append(handlers, h)
		}
	}
	f.mu.Unlock()
	for _, h := range handlers {
		h(ev)
	}
}

// livenessTicks returns the liveness.tick adapter events recorded by sender.
func livenessTicks(sender *recordingSender) []*v2.AdapterEvent {
	ticks := []*v2.AdapterEvent{}
	for _, ev := range sender.snapshot() {
		if a := ev.GetAdapter(); a != nil && a.GetEventKind() == livenessTickEventKind {
			ticks = append(ticks, a)
		}
	}
	return ticks
}

// errSinkSender always fails Send; used to script a rejecting host stream.
type errSinkSender struct{ err error }

func (e errSinkSender) Send(event *v2.ExecuteEvent) error { return e.err }

var errTestSink = errors.New("liveness tick send rejected")

// TestNativeToolEventsNotForwarded pins the CRI-288 forwarding gap at the
// adapter seam: native CLI tool executions are consumed as CRI-274 watchdog
// bookkeeping (gates + activity marks) and produce ZERO host-visible events —
// tool.execution_start / _complete open and close the gate, and
// tool.execution_progress / _partial_result hit the switch default. A turn
// that runs one long native tool is therefore host-silent for its whole run
// while the session keeps working (live run 74cf49b7).
func TestNativeToolEventsNotForwarded(t *testing.T) {
	p := newCopilotAdapter()
	s := newWatchdogSession(t, p, "adapter-gap", &fakeSession{sessionID: "sdk-gap"})
	ts := newTurnState(0)
	rec := &recordingSender{}
	handler := ts.handleEvent(s, rec)

	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionStartData{ToolCallID: "tc-1"}})
	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionProgressData{ToolCallID: "tc-1", ProgressMessage: "running"}})
	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionPartialResultData{ToolCallID: "tc-1", PartialOutput: "out"}})
	handler(copilot.SessionEvent{Data: &copilot.ToolExecutionCompleteData{ToolCallID: "tc-1"}})

	if events := rec.snapshot(); len(events) != 0 {
		t.Fatalf("native tool run forwarded %d events, want 0 (the CRI-288 forwarding gap)", len(events))
	}
	if s.gatedWaits.Load() != 0 {
		t.Fatalf("gatedWaits = %d, want 0 after the tool completed", s.gatedWaits.Load())
	}
	if s.lastActivityNs.Load() == 0 {
		t.Fatal("tool events must mark provider activity (watchdog stays disarmed while the host stream is silent)")
	}
}

// TestSendLivenessTickSilenceWindows: the tick fires only after
// livenessTickInterval of host-visible silence, re-baselines the clock after a
// successful send (no tick bursts), and stops the ticker on a send error.
func TestSendLivenessTickSilenceWindows(t *testing.T) {
	base := time.Unix(1700000000, 0)
	origNow := watchdogNow
	watchdogNow = func() time.Time { return base }
	t.Cleanup(func() { watchdogNow = origNow })

	p := newCopilotAdapter()
	s := newWatchdogSession(t, p, "adapter-lv", &fakeSession{sessionID: "sdk-lv"})
	rec := &recordingSender{}
	wrapped := forwardTrackingSink{inner: rec, s: s}
	cleanup := s.beginExecution(wrapped)
	t.Cleanup(cleanup)

	// Fresh turn stamp: silence 0 → no tick.
	if s.sendLivenessTick(context.Background(), wrapped) {
		t.Fatal("ticker must keep running before the silence interval elapses")
	}
	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("emitted %d events on a fresh turn, want 0", got)
	}

	// A forwarded event stamps the clock.
	if err := wrapped.Send(adapterEvent("agent.message", map[string]any{"content": "hi"})); err != nil {
		t.Fatalf("forwarding event: %v", err)
	}
	if got := s.lastForwardNs.Load(); got != base.UnixNano() {
		t.Fatalf("lastForwardNs = %d, want %d after a forwarded event", got, base.UnixNano())
	}

	// Silence >= interval: exactly one tick, and it re-baselines.
	after := base.Add(livenessTickInterval + time.Second)
	watchdogNow = func() time.Time { return after }
	if s.sendLivenessTick(context.Background(), wrapped) {
		t.Fatal("ticker must keep running after a successful tick")
	}
	ticks := livenessTicks(rec)
	if len(ticks) != 1 {
		t.Fatalf("liveness ticks = %d, want 1", len(ticks))
	}
	tick := ticks[0]
	payload := tick.GetPayload().AsMap()
	if got := payload["silent_for"]; got == "" {
		t.Fatalf("payload missing silent_for: %v", payload)
	}
	lastAt, err := time.Parse(time.RFC3339Nano, payload["last_forward_at"].(string))
	if err != nil {
		t.Fatalf("last_forward_at %v is not RFC3339Nano: %v", payload["last_forward_at"], err)
	}
	if !lastAt.Equal(base) {
		t.Fatalf("last_forward_at = %v, want the stamp of the last forwarded event %v", lastAt, base)
	}
	if got := payload["gated_tools"]; got != "0" {
		t.Fatalf("gated_tools = %v, want %q", got, "0")
	}
	if got := s.lastForwardNs.Load(); got != after.UnixNano() {
		t.Fatalf("tick did not re-baseline lastForwardNs: %d, want %d", got, after.UnixNano())
	}

	// Re-baselined clock: an immediate second call emits nothing.
	if s.sendLivenessTick(context.Background(), wrapped) {
		t.Fatal("ticker must keep running after the re-baseline")
	}
	if got := len(livenessTicks(rec)); got != 1 {
		t.Fatalf("liveness ticks = %d after re-baseline, want 1 (no burst)", got)
	}

	// A sink error must stop the ticker (the next handler forward surfaces
	// the same error; the tick must not double-report).
	watchdogNow = func() time.Time { return after.Add(livenessTickInterval) }
	if !s.sendLivenessTick(context.Background(), errSinkSender{err: errTestSink}) {
		t.Fatal("ticker must stop after a send error")
	}
}

// TestLivenessTickDoesNotTouchWatchdog: a tick is host-visible liveness, not
// provider activity — it must never advance the CRI-274 activity clock.
func TestLivenessTickDoesNotTouchWatchdog(t *testing.T) {
	base := time.Unix(1700000000, 0)
	origNow := watchdogNow
	watchdogNow = func() time.Time { return base }
	t.Cleanup(func() { watchdogNow = origNow })

	p := newCopilotAdapter()
	s := newWatchdogSession(t, p, "adapter-lv2", &fakeSession{sessionID: "sdk-lv2"})
	rec := &recordingSender{}
	wrapped := forwardTrackingSink{inner: rec, s: s}
	cleanup := s.beginExecution(wrapped)
	t.Cleanup(cleanup)

	watchdogNow = func() time.Time { return base.Add(livenessTickInterval) }
	s.sendLivenessTick(context.Background(), wrapped)
	if got := s.lastActivityNs.Load(); got != 0 {
		t.Fatalf("lastActivityNs = %d after a tick, want 0 (ticks must not re-arm the watchdog)", got)
	}
}

// TestLivenessTickerRunsAndStops: the ticker emits ticks while the clock is
// silent, the returned stop func is idempotent, and no tick fires after the
// stop.
func TestLivenessTickerRunsAndStops(t *testing.T) {
	withFastLiveness(t, 10*time.Millisecond, 2*time.Millisecond)
	base := time.Unix(1700000000, 0)
	origNow := watchdogNow
	t.Cleanup(func() { watchdogNow = origNow })

	p := newCopilotAdapter()
	s := newWatchdogSession(t, p, "adapter-lv3", &fakeSession{sessionID: "sdk-lv3"})
	rec := &recordingSender{}
	wrapped := forwardTrackingSink{inner: rec, s: s}
	cleanup := s.beginExecution(wrapped)
	t.Cleanup(cleanup)

	// Anchor the liveness clock at `base`, then hold the fake watchdog clock
	// beyond the interval: the ticker ticks on every check. After the first
	// successful tick the re-baselined stamp equals the constant fake now, so
	// silence is 0 and no further ticks fire — exactly one tick, deterministically.
	s.lastForwardNs.Store(base.UnixNano())
	watchdogNow = func() time.Time { return base.Add(livenessTickInterval + time.Second) }
	stop := s.startLivenessTicker(context.Background(), wrapped)
	time.Sleep(20 * time.Millisecond)
	before := len(livenessTicks(rec))
	if before == 0 {
		t.Fatal("no ticks fired while the clock was silent")
	}

	// Stop twice (idempotent): no further ticks reach the sink.
	stop()
	stop()

	time.Sleep(20 * time.Millisecond)
	if got := len(livenessTicks(rec)); got != before {
		t.Fatalf("ticks after stop = %d (before %d), want unchanged", got, before)
	}
}

// TestExecuteSilentNativeToolRunEmitsLivenessTicks is the CRI-288 regression
// test at the adapter seam: a turn whose provider runs one long native tool
// (start + continuous progress, no forwardable events) must still produce
// periodic liveness.tick events on the Execute stream instead of the observed
// 60-minute silence, and the turn must complete normally.
func TestExecuteSilentNativeToolRunEmitsLivenessTicks(t *testing.T) {
	withFastLiveness(t, 30*time.Millisecond, 5*time.Millisecond)
	withFastWatchdog(t, time.Second, time.Second)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	var once sync.Once
	silent := &fakeSession{sessionID: "sdk-silent"}
	silent.onSend = func(_ int, _ copilot.MessageOptions) {
		once.Do(func() {
			go func() {
				emit := func(ev copilot.SessionEvent) { silent.emitToHandlers(ev) }
				emit(copilot.SessionEvent{Data: &copilot.ToolExecutionStartData{ToolCallID: "tc-1"}})
				// ~120ms of native tool progress with no forwards.
				for i := 0; i < 12; i++ {
					time.Sleep(10 * time.Millisecond)
					emit(copilot.SessionEvent{Data: &copilot.ToolExecutionProgressData{ToolCallID: "tc-1", ProgressMessage: "working"}})
				}
				emit(copilot.SessionEvent{Data: &copilot.ToolExecutionCompleteData{ToolCallID: "tc-1"}})
				if _, err := p.handleSubmitOutcome("adapter-lvt", SubmitOutcomeArgs{Outcome: "success", Reason: "done"}); err != nil {
					t.Errorf("handleSubmitOutcome: %v", err)
				}
				emit(copilot.SessionEvent{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}})
				emit(copilot.SessionEvent{Data: &copilot.SessionIdleData{}})
			}()
		})
	}
	newWatchdogSession(t, p, "adapter-lvt", silent)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-lvt",
		Input:           map[string]string{"prompt": "run a long tool"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")
	ticks := livenessTicks(sender)
	if len(ticks) == 0 {
		t.Fatal("no liveness.tick events during a host-silent native tool run (CRI-288 regression)")
	}
	// Every tick must re-baseline: the silence measured at one tick
	// (last_forward_at spacing) is at least one full interval apart.
	var stamps []time.Time
	for _, tick := range ticks {
		payload := tick.GetPayload().AsMap()
		at, err := time.Parse(time.RFC3339Nano, payload["last_forward_at"].(string))
		if err != nil {
			t.Fatalf("tick last_forward_at %v: %v", payload["last_forward_at"], err)
		}
		stamps = append(stamps, at)
	}
	for i := 1; i < len(stamps); i++ {
		if gap := stamps[i].Sub(stamps[i-1]); gap < livenessTickInterval {
			t.Fatalf("tick gap = %v, want >= %v (tick must re-baseline after each send)", gap, livenessTickInterval)
		}
	}
	if len(stamps) > 12 {
		t.Fatalf("got %d ticks over a 120ms window with a 30ms interval, want a handful (bursty ticking)", len(stamps))
	}
}

// TestExecuteForwardingTurnEmitsNoLivenessTicks guards the healthy path: while
// assistant deltas keep flowing (every one forwarded), the liveness ticker
// must stay silent.
func TestExecuteForwardingTurnEmitsNoLivenessTicks(t *testing.T) {
	withFastLiveness(t, 20*time.Millisecond, 5*time.Millisecond)
	withFastWatchdog(t, time.Second, time.Second)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	var once sync.Once
	streaming := &fakeSession{sessionID: "sdk-streaming"}
	streaming.onSend = func(_ int, _ copilot.MessageOptions) {
		once.Do(func() {
			go func() {
				emit := func(ev copilot.SessionEvent) { streaming.emitToHandlers(ev) }
				// ~90ms of streaming deltas, each one forwarded.
				for i := 0; i < 9; i++ {
					time.Sleep(10 * time.Millisecond)
					emit(copilot.SessionEvent{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: "part"}})
				}
				if _, err := p.handleSubmitOutcome("adapter-lvf", SubmitOutcomeArgs{Outcome: "success", Reason: "done"}); err != nil {
					t.Errorf("handleSubmitOutcome: %v", err)
				}
				emit(copilot.SessionEvent{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}})
				emit(copilot.SessionEvent{Data: &copilot.SessionIdleData{}})
			}()
		})
	}
	newWatchdogSession(t, p, "adapter-lvf", streaming)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-lvf",
		Input:           map[string]string{"prompt": "stream"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")
	if got := len(livenessTicks(sender)); got != 0 {
		t.Fatalf("liveness ticks = %d on a continuously forwarding turn, want 0", got)
	}
	forwarded := 0
	for _, ev := range sender.snapshot() {
		if a := ev.GetAdapter(); a != nil && strings.HasPrefix(a.GetEventKind(), "agent.") {
			forwarded++
		}
	}
	if forwarded == 0 {
		t.Fatal("test setup: no agent.message events were forwarded")
	}
}