// copilot_turn_test.go — KB-42 regression coverage: a developer turn that
// ends without submitting the step outcome must fail the Execute loudly
// instead of waiting on a session.idle that will never arrive, and the last
// agent message must be preserved as evidence wherever the failure is
// recorded.

package main

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// lossScenario builds the scripted fake session for a forced outcome-less
// turn: the model produces its final message and then a turn-terminal session
// event fires, with no session.idle and no submit_outcome call. Returns the
// adapter actually wired to the session.
func lossScenario(t *testing.T, id string, terminalEvents []copilot.SessionEvent) (*copilotAdapter, *fakeSession, *fakeClient, *recordingSender) {
	t.Helper()
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)
	fake := &fakeSession{
		sessionID: "sdk-loss-" + id,
		emitOnSend: append([]copilot.SessionEvent{
			{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "Workstream CRI-201 is complete. The adapter changes are implemented and validated."}},
		}, terminalEvents...),
	}
	newWatchdogSession(t, p, id, fake)
	return p, fake, fc, &recordingSender{}
}

// findAdapterEvent returns the payload map of the first adapter event with
// the given kind, or nil.
func findAdapterEvent(t *testing.T, sender *recordingSender, kind string) map[string]any {
	t.Helper()
	for _, ev := range sender.snapshot() {
		a := ev.GetAdapter()
		if a != nil && a.GetEventKind() == kind {
			return a.GetPayload().AsMap()
		}
	}
	return nil
}

// hasResult reports whether the sender received a result event at all.
func hasResult(sender *recordingSender) bool {
	for _, ev := range sender.snapshot() {
		if ev.GetResult() != nil {
			return true
		}
	}
	return false
}

// TestExecuteSessionErrorEndsTurnLoudly: the turn's final agent message is
// followed by a turn-terminal session.error and no session.idle — the Execute
// must fail promptly with the session-loss sentinel (no forced stall-recovery
// cycles, no re-sends), with the last agent message preserved as evidence.
func TestExecuteSessionErrorEndsTurnLoudly(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)

	p, fake, fc, sender := lossScenario(t, "adapter-loss-err", []copilot.SessionEvent{
		{Data: &copilot.SessionErrorData{ErrorType: "authentication", Message: "token expired"}},
	})

	start := time.Now()
	err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-loss-err",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Execute must fail when the turn ends with a session error")
	}
	if !isSessionLossError(err) {
		t.Fatalf("Execute error = %v, want it session-loss-typed", err)
	}
	if !strings.Contains(err.Error(), "session error (authentication)") {
		t.Fatalf("Execute error = %v, want the provider error cause", err)
	}
	if !strings.Contains(err.Error(), "Workstream CRI-201 is complete") {
		t.Fatalf("Execute error = %v, want the last agent message as evidence", err)
	}
	// The loss must end the wait long before the watchdog stall machinery
	// gets involved: no re-sends, no forced child restarts, no backoff.
	if elapsed > 5*time.Second {
		t.Fatalf("Execute took %s, want an immediate loud failure (not a stall cycle)", elapsed)
	}
	if got := fake.sendCount; got != 1 {
		t.Fatalf("send count = %d, want 1 (a lost turn must not be re-sent)", got)
	}
	if got := *sleeps; len(got) != 0 {
		t.Fatalf("backoff sleeps = %v, want none (session loss is not a stall)", got)
	}
	if fc.stopCount != 0 {
		t.Fatalf("stop count = %d, want 0 (the CLI reported the error; it is alive)", fc.stopCount)
	}
	// The diagnostic evidence event must be forwarded with the last agent
	// message and the provider error type.
	payload := findAdapterEvent(t, sender, "session.error")
	if payload == nil {
		t.Fatal("expected a session.error diagnostic event")
	}
	if payload["error_type"] != "authentication" {
		t.Errorf("session.error payload error_type = %v, want authentication", payload["error_type"])
	}
	if msg, _ := payload["last_agent_message"].(string); !strings.Contains(msg, "Workstream CRI-201 is complete") {
		t.Errorf("session.error payload last_agent_message = %q, want the final model message", msg)
	}
	// No result event: the Execute failed, so the engine owns the retry.
	if hasResult(sender) {
		t.Error("a session loss must surface as an Execute error, not a result event")
	}
}

// TestExecuteSessionShutdownEndsTurnLoudly: the CLI tears the session down
// mid-turn (no session.idle will ever arrive) — the Execute must fail
// promptly with the session-loss sentinel and the last agent message
// preserved as evidence.
func TestExecuteSessionShutdownEndsTurnLoudly(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)

	errorReason := "fatal provider stream error"
	p, _, fc, sender := lossScenario(t, "adapter-loss-shutdown", []copilot.SessionEvent{
		{Data: &copilot.SessionShutdownData{ShutdownType: "error", ErrorReason: &errorReason}},
	})

	err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-loss-shutdown",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender)
	if err == nil {
		t.Fatal("Execute must fail when the session shuts down mid-turn")
	}
	if !isSessionLossError(err) {
		t.Fatalf("Execute error = %v, want it session-loss-typed", err)
	}
	if !strings.Contains(err.Error(), "session shutdown (error)") {
		t.Fatalf("Execute error = %v, want the shutdown type", err)
	}
	if !strings.Contains(err.Error(), "Workstream CRI-201 is complete") {
		t.Fatalf("Execute error = %v, want the last agent message as evidence", err)
	}
	if got := *sleeps; len(got) != 0 {
		t.Fatalf("backoff sleeps = %v, want none", got)
	}
	payload := findAdapterEvent(t, sender, "session.shutdown")
	if payload == nil {
		t.Fatal("expected a session.shutdown diagnostic event")
	}
	if payload["shutdown_type"] != "error" {
		t.Errorf("session.shutdown payload shutdown_type = %v, want error", payload["shutdown_type"])
	}
	if msg, _ := payload["last_agent_message"].(string); !strings.Contains(msg, "Workstream CRI-201 is complete") {
		t.Errorf("session.shutdown payload last_agent_message = %q, want the final model message", msg)
	}
	if fc.stopCount != 0 {
		t.Fatalf("stop count = %d, want 0", fc.stopCount)
	}
}

// TestExecuteSessionErrorAutoSwitchContinuesTurn: an auto-switch-eligible
// rate limit is NOT turn-terminal — the runtime switches models and the turn
// continues to its idle, so the Execute must still succeed.
func TestExecuteSessionErrorAutoSwitchContinuesTurn(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	eligible := true
	fake := &fakeSession{sessionID: "sdk-autoswitch"}
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := p.handleSubmitOutcome("adapter-autoswitch", SubmitOutcomeArgs{Outcome: "success", Reason: "ok"}); err != nil {
			t.Errorf("handleSubmitOutcome: %v", err)
		}
	}
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "working"}},
		{Data: &copilot.SessionErrorData{ErrorType: "rate_limit", Message: "user rate limited", EligibleForAutoSwitch: &eligible}},
		{Data: &copilot.SessionIdleData{}},
	}
	newWatchdogSession(t, p, "adapter-autoswitch", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-autoswitch",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	assertOutcome(t, sender, "success")
	if got := fake.sendCount; got != 1 {
		t.Fatalf("send count = %d, want 1 (the auto-switched turn must not be re-sent)", got)
	}
	if payload := findAdapterEvent(t, sender, "session.error"); payload == nil {
		t.Error("expected the auto-switched rate limit to be forwarded as a diagnostic")
	}
}

// TestExecuteAbortIsNotTurnTerminal: an abort event is followed by the idle
// that closes the turn; the finalize loop's reprompt path must still work and
// the Execute must succeed, with the abort forwarded as a diagnostic.
func TestExecuteAbortIsNotTurnTerminal(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	fake := &fakeSession{sessionID: "sdk-abort"}
	// Turn 1 (call 0): abort + idle without an outcome → reprompt. Turn 2
	// (call 1): the developer finalizes and the turn idles.
	fake.onSend = func(callIndex int, _ copilot.MessageOptions) {
		if callIndex != 1 {
			return
		}
		if _, err := p.handleSubmitOutcome("adapter-abort", SubmitOutcomeArgs{Outcome: "success", Reason: "done after reprompt"}); err != nil {
			t.Errorf("handleSubmitOutcome: %v", err)
		}
	}
	fake.sendSequence = [][]copilot.SessionEvent{
		{
			{Data: &copilot.AbortData{Reason: "user_abort"}},
			{Data: &copilot.SessionIdleData{}},
		},
		{
			{Data: &copilot.AssistantMessageData{MessageID: "m2", Content: "done"}},
			{Data: &copilot.SessionIdleData{}},
		},
	}
	newWatchdogSession(t, p, "adapter-abort", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-abort",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	assertOutcome(t, sender, "success")
	if got := fake.sendCount; got != 2 {
		t.Fatalf("send count = %d, want 2 (initial + reprompt)", got)
	}
	payload := findAdapterEvent(t, sender, "turn.aborted")
	if payload == nil {
		t.Fatal("expected a turn.aborted diagnostic event")
	}
	if payload["reason"] != "user_abort" {
		t.Errorf("turn.aborted payload reason = %v, want user_abort", payload["reason"])
	}
}

// TestExecuteLateSessionErrorDoesNotKillNextReprompt: a session error that
// trails the idle of a completed turn is stale — the finalize loop must drop
// it and keep reprompting instead of failing the turn (KB-42).
func TestExecuteLateSessionErrorDoesNotKillNextReprompt(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	fake := &fakeSession{sessionID: "sdk-late"}
	fake.onSend = func(callIndex int, _ copilot.MessageOptions) {
		if callIndex != 1 {
			return
		}
		if _, err := p.handleSubmitOutcome("adapter-late", SubmitOutcomeArgs{Outcome: "success", Reason: "finalized on reprompt"}); err != nil {
			t.Errorf("handleSubmitOutcome: %v", err)
		}
	}
	// Turn 1 idles without an outcome, and a turn-terminal session error
	// trails the idle (the CLI reports it as the turn is already closing).
	// The stale error must be dropped, not fail the reprompt wait.
	fake.sendSequence = [][]copilot.SessionEvent{
		{
			{Data: &copilot.SessionIdleData{}},
			{Data: &copilot.SessionErrorData{ErrorType: "query", Message: "upstream hiccup"}},
		},
		{
			{Data: &copilot.AssistantMessageData{MessageID: "m2", Content: "done"}},
			{Data: &copilot.SessionIdleData{}},
		},
	}
	newWatchdogSession(t, p, "adapter-late", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-late",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	assertOutcome(t, sender, "success")
	if got := fake.sendCount; got != 2 {
		t.Fatalf("send count = %d, want 2 (initial + reprompt)", got)
	}
}

// TestExecuteFinalizedOutcomeBeatsSessionLoss: an outcome submitted before
// the turn died must still be returned — a trailing session loss must not
// discard an already-finalized deliverable (CRI-274 principle, KB-42).
func TestExecuteFinalizedOutcomeBeatsSessionLoss(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	fake := &fakeSession{sessionID: "sdk-fin-loss"}
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := p.handleSubmitOutcome("adapter-fin-loss", SubmitOutcomeArgs{Outcome: "success", Reason: "done"}); err != nil {
			t.Errorf("handleSubmitOutcome: %v", err)
		}
	}
	// The finalize lands, then the turn dies with a session error and no
	// idle. The outcome was submitted, so the step must succeed.
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
		{Data: &copilot.SessionErrorData{ErrorType: "query", Message: "upstream closed"}},
	}
	newWatchdogSession(t, p, "adapter-fin-loss", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-fin-loss",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	assertOutcome(t, sender, "success")
	if got := fake.sendCount; got != 1 {
		t.Fatalf("send count = %d, want 1 (a finalized turn must not be re-prompted)", got)
	}
	if got := *sleeps; len(got) != 0 {
		t.Fatalf("backoff sleeps = %v, want none (no retry after finalize)", got)
	}
}

// TestExecuteExhaustionPreservesLastAgentMessage: the failure result reason
// and the structured outcome.failure event must carry the last agent message
// when the finalize loop exhausts (KB-42 acceptance: evidence preserved on
// run failure).
func TestExecuteExhaustionPreservesLastAgentMessage(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	const finalMsg = "Workstream CRI-201 is complete. All workstreams are validated."
	fake := &fakeSession{sessionID: "sdk-exhaust"}
	fake.sendSequence = [][]copilot.SessionEvent{
		{
			{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: finalMsg}},
			{Data: &copilot.SessionIdleData{}},
		},
		{{Data: &copilot.SessionIdleData{}}},
		{{Data: &copilot.SessionIdleData{}}},
	}
	newWatchdogSession(t, p, "adapter-exhaust", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-exhaust",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"success", "failure"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "failure")
	result := resultFromSender(sender)
	if result == nil {
		t.Fatal("no result event")
	}
	reason := outputsOf(t, result)["reason"]
	if !strings.Contains(reason, finalMsg) {
		t.Errorf("failure result reason = %q, want the last agent message as evidence", reason)
	}
	payload := findAdapterEvent(t, sender, "outcome.failure")
	if payload == nil {
		t.Fatal("expected an outcome.failure event")
	}
	if msg, _ := payload["last_agent_message"].(string); msg != finalMsg {
		t.Errorf("outcome.failure last_agent_message = %q, want %q", msg, finalMsg)
	}
}

// TestEvidenceSnippet covers the truncation used for all failure evidence:
// byte-budgeted, never splitting a rune, always marked when truncated.
func TestEvidenceSnippet(t *testing.T) {
	if got := evidenceSnippet(""); got != "" {
		t.Errorf("evidenceSnippet(\"\") = %q, want \"\"", got)
	}
	short := "Workstream CRI-201 is complete."
	if got := evidenceSnippet(short); got != short {
		t.Errorf("evidenceSnippet(short) = %q, want the input unchanged", got)
	}
	const marker = "…[truncated]"
	// Multi-byte runes: the cut must land on a rune boundary.
	long := strings.Repeat("ä", maxEvidenceLen+50)
	got := evidenceSnippet(long)
	if !utf8.ValidString(got) {
		t.Errorf("evidenceSnippet(long) = %q, want valid UTF-8", got)
	}
	if !strings.HasSuffix(got, marker) {
		t.Errorf("evidenceSnippet(long) = %q, want the %q marker", got, marker)
	}
	prefix := strings.TrimSuffix(got, marker)
	if len(prefix) > maxEvidenceLen {
		t.Errorf("evidenceSnippet(long) body = %d bytes, want ≤ %d", len(prefix), maxEvidenceLen)
	}
	if !strings.HasPrefix(long, prefix) {
		t.Error("evidenceSnippet(long) body is not a prefix of the input")
	}
	if maxEvidenceLen <= len(marker) {
		t.Fatalf("maxEvidenceLen = %d, want > %d so long inputs are marked", maxEvidenceLen, len(marker))
	}
}