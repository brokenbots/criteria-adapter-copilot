// copilot_liveness.go — CRI-288: host-visible liveness ticks for turns that
// stall to zero forwarded events while the Copilot session keeps working.
//
// Root cause of the forwarding stall observed in live run 74cf49b7 (CRI-279
// develop, 2026-09-21): the adapter forwards assistant messages, adapter-tool
// invocations/results, and permission events, but native CLI tool executions
// are consumed purely as watchdog bookkeeping — tool.execution_start /
// tool.execution_complete open and close CRI-274 gates, and
// tool.execution_progress / tool.execution_partial_result are not handled at
// all. During a single long native tool (the observed run: one hour-long bash
// invocation that pushed six commits), the session keeps emitting such events,
// every one of which marks provider activity and re-arms the gate ceiling, so
// the watchdog stays disarmed while the Execute stream to the host emits
// nothing for the whole run.
//
// The liveness tick closes that gap at the adapter seam: while a turn is
// executing, a ticker checks the host-visible silence (time since the last
// event actually forwarded through the sink) and emits an adapter
// "liveness.tick" event whenever livenessTickInterval elapsed with no
// forwarded events. Ticks prove the adapter and session are alive; they are
// NOT provider activity and must never re-arm the watchdog.

package main

import (
	"context"
	"strconv"
	"sync"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// adapterStart anchors the lastForward fallback for states that never
// forwarded an event and never ran beginExecution (bare unit-test states); in
// production beginExecution stamps the liveness clock before the ticker runs.
var adapterStart = time.Now()

const (
	// livenessTickEventKind is the AdapterEvent.EventKind of a liveness tick.
	livenessTickEventKind = "liveness.tick"
)

var (
	// livenessTickInterval is the host-visible silence after which a turn
	// must emit a liveness.tick event. 5 minutes sits inside the CRI-288
	// acceptance window (a tick after 5-10 minutes of turn time with no
	// forwarded events) and well below the watchdog gate ceiling, so a turn
	// cannot be host-silent for longer than one tick interval.
	// Test-overridable.
	livenessTickInterval = 5 * time.Minute

	// livenessCheckInterval is how often the ticker evaluates the silence
	// clock; worst-case tick latency is livenessTickInterval plus this
	// cadence. Test-overridable.
	livenessCheckInterval = 30 * time.Second
)

// forwardTrackingSink wraps the Execute sink of the active turn and records
// the unix-nano timestamp of the last event the adapter forwarded to the host
// into the session's liveness clock. Every forward path (handler forwards,
// permission bridge, finalize/reprompt diagnostics, result events) sends
// through the sink captured in Execute, so wrapping there covers them all.
type forwardTrackingSink struct {
	inner adapterhost.ExecuteEventSender
	s     *sessionState
}

func (w forwardTrackingSink) Send(event *v2.ExecuteEvent) error {
	if err := w.inner.Send(event); err != nil {
		return err
	}
	w.s.lastForwardNs.Store(watchdogNow().UnixNano())
	return nil
}

// lastForward returns the wall time of the last host-visible forwarded event.
// A never-forwarded (zero) value falls back to adapterStart so a state that
// never began execution still measures from a sane base (production states
// always stamp the clock in beginExecution before the ticker starts).
func (s *sessionState) lastForward() time.Time {
	if t := s.lastForwardNs.Load(); t != 0 {
		return time.Unix(0, t)
	}
	return adapterStart
}

// sendLivenessTick emits one liveness.tick event if host-visible silence has
// reached livenessTickInterval. It returns false to keep the ticker running
// and true when the ticker must stop: the turn context is done, or the sink
// rejected the event (a dead host stream — the next handler forward will
// surface the same error through sendErr, so the tick must not double-report).
func (s *sessionState) sendLivenessTick(ctx context.Context, sink adapterhost.ExecuteEventSender) bool {
	if ctx.Err() != nil {
		return true
	}
	now := watchdogNow()
	silence := now.Sub(s.lastForward())
	if silence < livenessTickInterval {
		return false
	}
	payload := map[string]any{
		"silent_for":      silence.Truncate(time.Second).String(),
		"last_forward_at": now.Add(-silence).UTC().Format(time.RFC3339Nano),
		// Diagnostic context for the observed failure mode: how many native
		// tool gates are currently held (progress keeps them open while the
		// host stream is silent).
		"gated_tools": strconv.FormatInt(s.gatedWaits.Load(), 10),
	}
	if err := sink.Send(adapterEvent(livenessTickEventKind, payload)); err != nil {
		return true
	}
	// Re-baseline only after a successful send, so the next tick fires a full
	// interval after the event the host actually received.
	s.lastForwardNs.Store(now.UnixNano())
	return false
}

// startLivenessTicker runs the CRI-288 liveness tick for one Execute call and
// returns its idempotent stop func. Ticks are host-visible liveness only:
// they never call markActivity, so a tick (nor its cadence) can re-arm the
// CRI-274 provider watchdog.
func (s *sessionState) startLivenessTicker(ctx context.Context, sink adapterhost.ExecuteEventSender) func() {
	stop := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(livenessCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				if s.sendLivenessTick(ctx, sink) {
					return
				}
			}
		}
	}()
	return func() {
		once.Do(func() { close(stop) })
	}
}