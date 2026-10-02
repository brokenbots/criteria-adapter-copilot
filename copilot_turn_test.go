// copilot_turn_test.go — KB-42 regression coverage: a developer turn that
// ends without submitting the step outcome must fail the Execute loudly
// instead of waiting on a session.idle that will never arrive, and the last
// agent message must be preserved as evidence wherever the failure is
// recorded.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
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

// TestExecuteContractFinalizeBeatsSessionLoss: the same trailing session-loss
// scenario in contract mode (KB-47 review defect #2). The contract payload and
// comment recorded by handleSubmitOutcome are forwarded verbatim through
// ExecuteResult.outputs_json/comment; a legacy session-state outputs
// assembly (holding only outcome/reason) must not replace them.
func TestExecuteContractFinalizeBeatsSessionLoss(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	fake := &fakeSession{sessionID: "sdk-contract-loss"}
	pHandle := p
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := pHandle.handleSubmitOutcome("adapter-contract-loss", SubmitOutcomeArgs{
			Outcome: "approved",
			Payload: json.RawMessage(`{"verdict":"approved","notes":"final"}`),
			Comment: "all gates green",
		}); err != nil {
			t.Errorf("handleSubmitOutcome: %v", err)
		}
	}
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
		{Data: &copilot.SessionErrorData{ErrorType: "query", Message: "upstream closed"}},
	}
	newWatchdogSession(t, p, "adapter-contract-loss", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:        "adapter-contract-loss",
		Input:            map[string]string{"prompt": "do work"},
		AllowedOutcomes:  []string{"approved", "needs_review"},
		OutcomeContracts: []*v2.OutcomeContract{contractTestContract(t, "approved", `{"type":"object"}`, false, false)},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "approved")
	result := resultFromSender(sender)
	if got := string(result.GetOutputsJson()); got != `{"verdict":"approved","notes":"final"}` {
		t.Fatalf("outputs_json = %s, want the submitted payload verbatim", got)
	}
	if got := result.GetComment(); got != "all gates green" {
		t.Fatalf("comment = %q, want the submitted comment verbatim", got)
	}
	if got := fake.sendCount; got != 1 {
		t.Fatalf("send count = %d, want 1 (a finalized turn must not be re-prompted)", got)
	}
	if got := *sleeps; len(got) != 0 {
		t.Fatalf("backoff sleeps = %v, want none (no retry after finalize)", got)
	}
	if ev := findAdapterEvent(t, sender, "outcome.recovered"); ev != nil {
		t.Fatalf("outcome.recovered emitted for a non-repair finalize: %v", ev)
	}
}

// reopenSimSession simulates the KB-47 recreate-during-repair ordering: the
// first Send of a repair Execute dies with a transport error (the way a dead
// CLI child is detected inside sendWithRetry), which drives recoverTransport →
// reopenSession → a fresh SDK session create (resumed=false → createdEpoch
// bumped). The wrapper stands in for the reopen's epoch bump so the test
// observes composition against the POST-reopen generation.
type reopenSimSession struct {
	*fakeSession
	state *sessionState
	mu    sync.Mutex
	calls int
}

func (w *reopenSimSession) Send(ctx context.Context, opts *copilot.MessageOptions) (string, error) {
	w.mu.Lock()
	call := w.calls
	w.calls++
	w.mu.Unlock()
	if call == 0 {
		// Transport death before the prompt reached a live conversation; the
		// reopen's fresh create bumps the creation epoch (KB-47).
		w.state.mu.Lock()
		w.state.createdEpoch++
		w.state.mu.Unlock()
		return "", errors.New("connection closed")
	}
	return w.fakeSession.Send(ctx, opts)
}

// TestExecuteRepairAfterRecreateDeliversDegradedPrompt: a repair Execute whose
// first send dies with a transport error gets its SDK session re-created
// before the retry. The delivered (retry) prompt must be the DEGRADED
// re-execute variant — full step prompt plus the rejection note — composed
// against the bumped epoch, not the minimal repair variant that assumes the
// rejected finalize is still in the conversation (KB-47 defect #3: the
// in-session decision must be made after the lazy reopen, not when Execute
// populates session state).
func TestExecuteRepairAfterRecreateDeliversDegradedPrompt(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	// Epochs (1,1): Execute #1 finalized in the live SDK session, so a repair
	// WOULD be in-session — unless the session is re-created first.
	fake := &fakeSession{sessionID: "sdk-reopen-repair"}
	wrapped := &reopenSimSession{fakeSession: fake}
	s := newWatchdogSession(t, p, "adapter-reopen-repair", wrapped)
	wrapped.state = s
	s.mu.Lock()
	s.createdEpoch = 1
	s.finalizeSessionEpoch = 1
	s.mu.Unlock()

	pHandle := p
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := pHandle.handleSubmitOutcome("adapter-reopen-repair", SubmitOutcomeArgs{
			Outcome: "approved",
			Payload: json.RawMessage(`{"verdict":"approved"}`),
			Comment: "repaired after recreate",
		}); err != nil {
			t.Errorf("handleSubmitOutcome: %v", err)
		}
	}
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
		{Data: &copilot.SessionIdleData{}},
	}

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:        "adapter-reopen-repair",
		Input:            map[string]string{"prompt": "do the full step"},
		AllowedOutcomes:  []string{"approved", "needs_review"},
		OutcomeContracts: []*v2.OutcomeContract{contractTestContract(t, "approved", verbatimSchema, false, false)},
		Rejection:        &v2.ExecutionRejection{Outcome: "approved", Attempt: 2, Issues: "payload missing verdict"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "approved")
	if got := len(fake.sentOpts); got != 1 {
		t.Fatalf("delivered prompts = %d, want 1 (only the retry send reaches the live conversation)", got)
	}
	prompt := fake.sentOpts[0].Prompt
	for _, want := range []string{
		"do the full step",
		`(outcome "approved", attempt 2)`,
		"payload missing verdict",
		"Address these issues in your finalization",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("delivered prompt missing degraded re-execute content %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "Resubmit the finalize now") {
		t.Errorf("delivered prompt must NOT be the minimal repair variant (the recreate dropped the rejected finalize):\n%s", prompt)
	}
	if got := fake.sendCount; got != 1 {
		t.Fatalf("fake send count = %d, want 1", got)
	}
	if got := *sleeps; len(got) != 1 || got[0] != time.Second {
		t.Fatalf("backoff sleeps = %v, want [1s] (one transport retry)", got)
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

// TestExecuteExhaustionCarriesDeltaEvidence (KB-69 regression): a streaming
// review leg emits its prose as assistant.message_delta chunks, and the CLI
// emits a complete assistant.message with EMPTY content for every requested
// tool. When the finalize loop exhausts, the evidence (last_agent_message in
// outcome.failure and the failure result) must carry the accumulated delta
// text — an empty tool-request message must not erase it — and the
// turn.finalize_exhausted diagnostic must report the loop's signal
// accounting (observed runs d3f8c225/9b7495f1 reported
// last_agent_message="" with attempts=0).
func TestExecuteExhaustionCarriesDeltaEvidence(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	const deltaText = "Reviewing the diff for regressions."
	fake := &fakeSession{sessionID: "sdk-delta-exhaust"}
	fake.sendSequence = [][]copilot.SessionEvent{
		{
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: "Reviewing "}},
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: "the diff "}},
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: "for regressions."}},
			// A requested tool renders as a complete message with empty
			// content plus a ToolRequests entry (CLI behavior). It must
			// forward the tool invocation and keep the delta evidence.
			{Data: &copilot.AssistantMessageData{
				MessageID: "m2",
				Content:   "",
				ToolRequests: []copilot.AssistantMessageToolRequest{{
					ToolCallID: "t1",
					Name:       "bash",
					Arguments:  map[string]any{"command": "grep -rn TODO ."},
				}},
			}},
			{Data: &copilot.SessionIdleData{}},
		},
		{{Data: &copilot.SessionIdleData{}}},
		{{Data: &copilot.SessionIdleData{}}},
	}
	newWatchdogSession(t, p, "adapter-delta-exhaust", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-delta-exhaust",
		Input:           map[string]string{"prompt": "review the change"},
		AllowedOutcomes: []string{"approved", "changes_requested", "failure", "need_help"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "failure")
	payload := findAdapterEvent(t, sender, "outcome.failure")
	if payload == nil {
		t.Fatal("expected an outcome.failure event")
	}
	if msg, _ := payload["last_agent_message"].(string); msg != deltaText {
		t.Errorf("outcome.failure last_agent_message = %q, want the accumulated delta text %q", msg, deltaText)
	}
	if attempts, _ := payload["attempts"].(float64); attempts != 0 {
		t.Errorf("outcome.failure attempts = %v, want 0 (no submit_outcome call ever landed)", attempts)
	}
	if toolPayload := findAdapterEvent(t, sender, "tool.invocation"); toolPayload == nil || toolPayload["tool_call_id"] != "t1" {
		t.Errorf("tool.invocation event = %v, want the tool request forwarded from the empty-content message", toolPayload)
	}

	// The turn-level diagnostic must describe the exhausted loop: one
	// assistant turn produced, three idle signals consumed, no leftover or
	// drained signals, zero finalize attempts.
	dbg := findAdapterEvent(t, sender, "turn.finalize_exhausted")
	if dbg == nil {
		t.Fatal("expected a turn.finalize_exhausted diagnostic event")
	}
	want := map[string]float64{
		"attempts":             0,
		"assistant_turns":      1,
		"idle_signals":         3,
		"err_signals":          0,
		"drained_idle_signals": 0,
		"drained_err_signals":  0,
	}
	for k, v := range want {
		if got, _ := dbg[k].(float64); got != v {
			t.Errorf("turn.finalize_exhausted %s = %v, want %v", k, got, v)
		}
	}
	if leftover, _ := dbg["leftover_idle_signal"].(bool); leftover {
		t.Errorf("turn.finalize_exhausted leftover_idle_signal = true, want false")
	}
	if leftover, _ := dbg["leftover_err_signal"].(bool); leftover {
		t.Errorf("turn.finalize_exhausted leftover_err_signal = true, want false")
	}
	if dbg["finalize_kind"] != "missing" {
		t.Errorf("turn.finalize_exhausted finalize_kind = %v, want missing", dbg["finalize_kind"])
	}
	if msg, _ := dbg["last_agent_message"].(string); msg != deltaText {
		t.Errorf("turn.finalize_exhausted last_agent_message = %q, want %q", msg, deltaText)
	}
	// KB-71: turns 2 and 3 here are message-less reprompted turns, so the
	// per-reprompt evidence must carry one empty entry per reprompt —
	// positional proof of "reprompted turn sent no message" as opposed to an
	// answered-prose refusal.
	replies, _ := dbg["reprompt_replies"].([]any)
	if len(replies) != 2 {
		t.Fatalf("turn.finalize_exhausted reprompt_replies = %#v, want 2 entries (one per reprompt)", dbg["reprompt_replies"])
	}
	for i, r := range replies {
		if s, _ := r.(string); s != "" {
			t.Errorf("turn.finalize_exhausted reprompt_replies[%d] = %q, want empty (turn carried no message)", i, s)
		}
	}
}

// TestExecuteExhaustionCarriesPerRepromptReplyText (KB-71 regression): when
// the reviewer answers the corrective reprompts with prose and still never
// calls submit_outcome (the review-leg failure shape: attempts=0,
// idle_signals=3, three finished turns), each reprompted turn's reply text
// must be recorded in turn.finalize_exhausted so operators can tell
// "answered prose" from "empty reprompt turn" without re-running the leg.
func TestExecuteExhaustionCarriesPerRepromptReplyText(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	const initialReply = "I probed the diff and the policy bands."
	const reprompt1Reply = "I looked again; the evidence is complete."
	const reprompt2Reply = "I decline to submit without re-checking the upstream diff."
	fake := &fakeSession{sessionID: "sdk-reprompt-refusal"}
	fake.sendSequence = [][]copilot.SessionEvent{
		{ // Turn 1: reply to the initial prompt, then idle with no finalize.
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: initialReply}},
			{Data: &copilot.SessionIdleData{}},
		},
		{ // Turn 2: reply to corrective reprompt 1 — prose, still no finalize.
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m2", DeltaContent: reprompt1Reply}},
			{Data: &copilot.SessionIdleData{}},
		},
		{ // Turn 3: reply to corrective reprompt 2 — prose, still no finalize.
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m3", DeltaContent: reprompt2Reply}},
			{Data: &copilot.SessionIdleData{}},
		},
	}
	newWatchdogSession(t, p, "adapter-reprompt-refusal", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-reprompt-refusal",
		Input:           map[string]string{"prompt": "review the change"},
		AllowedOutcomes: []string{"approved", "changes_requested", "failure", "need_help"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "failure")
	dbg := findAdapterEvent(t, sender, "turn.finalize_exhausted")
	if dbg == nil {
		t.Fatal("expected a turn.finalize_exhausted diagnostic event")
	}
	replies, _ := dbg["reprompt_replies"].([]any)
	if len(replies) != 2 {
		t.Fatalf("turn.finalize_exhausted reprompt_replies = %#v, want 2 entries (one per reprompt)", dbg["reprompt_replies"])
	}
	if s, _ := replies[0].(string); s != reprompt1Reply {
		t.Errorf("turn.finalize_exhausted reprompt_replies[0] = %q, want the reply to reprompt 1 %q", s, reprompt1Reply)
	}
	if s, _ := replies[1].(string); s != reprompt2Reply {
		t.Errorf("turn.finalize_exhausted reprompt_replies[1] = %q, want the reply to reprompt 2 %q", s, reprompt2Reply)
	}
	// The final turn's reply doubles as last_agent_message; attempts and idle
	// accounting match the observed failure runs.
	if msg, _ := dbg["last_agent_message"].(string); msg != reprompt2Reply {
		t.Errorf("turn.finalize_exhausted last_agent_message = %q, want the final turn's reply %q", msg, reprompt2Reply)
	}
	if attempts, _ := dbg["attempts"].(float64); attempts != 0 {
		t.Errorf("turn.finalize_exhausted attempts = %v, want 0 (no submit_outcome call landed)", attempts)
	}
	if idles, _ := dbg["idle_signals"].(float64); idles != 3 {
		t.Errorf("turn.finalize_exhausted idle_signals = %v, want 3 (1 per finalize attempt)", idles)
	}
}

// TestExecuteRepromptAfterIdleConvergesOnFinalize (KB-69 regression): a
// review leg that idles without submit_outcome — the shape of the failing
// runs (evidence loop, denied probes, then a turn end with no finalize) —
// must produce a REAL reprompt send (a corrective finalize instruction, not
// a silently consumed attempt), and a later turn that finalizes must
// converge the step rather than failing.
func TestExecuteRepromptAfterIdleConvergesOnFinalize(t *testing.T) {
	withFastWatchdog(t, 20*time.Millisecond, time.Minute)
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	fake := &fakeSession{sessionID: "sdk-reprompt"}
	var reprompts []string
	fake.onSend = func(callIndex int, opts copilot.MessageOptions) {
		if callIndex == 0 {
			return
		}
		if !strings.Contains(opts.Prompt, "submit_outcome") {
			t.Errorf("reprompt send %d prompt = %q, want a corrective submit_outcome instruction", callIndex+1, opts.Prompt)
		}
		reprompts = append(reprompts, opts.Prompt)
		if callIndex == 2 {
			if _, err := p.handleSubmitOutcome("adapter-reprompt", SubmitOutcomeArgs{Outcome: "approved", Reason: "finalize on final attempt"}); err != nil {
				t.Errorf("handleSubmitOutcome: %v", err)
			}
		}
	}
	fake.sendSequence = [][]copilot.SessionEvent{
		{ // Turn 1: evidence loop with tool activity, no finalize, then idle.
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m1", DeltaContent: "Starting the review."}},
			{Data: &copilot.AssistantMessageData{
				MessageID: "m2",
				Content:   "",
				ToolRequests: []copilot.AssistantMessageToolRequest{{
					ToolCallID: "t1",
					Name:       "bash",
					Arguments:  map[string]any{"command": "git diff --stat"},
				}},
			}},
			{Data: &copilot.SessionIdleData{}},
		},
		{ // Turn 2 (after reprompt 1): model activity again, still no finalize.
			{Data: &copilot.AssistantMessageDeltaData{MessageID: "m3", DeltaContent: "Continuing the review."}},
			{Data: &copilot.SessionIdleData{}},
		},
		{ // Turn 3 (after reprompt 2, final attempt): the reviewer finalizes.
			{Data: &copilot.AssistantMessageData{MessageID: "m4", Content: "Review complete."}},
			{Data: &copilot.SessionIdleData{}},
		},
	}
	newWatchdogSession(t, p, "adapter-reprompt", fake)

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "adapter-reprompt",
		Input:           map[string]string{"prompt": "review the change"},
		AllowedOutcomes: []string{"approved", "changes_requested", "failure", "need_help"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "approved")
	if got := fake.sendCount; got != 3 {
		t.Fatalf("send count = %d, want 3 (initial prompt + 2 real reprompt sends)", got)
	}
	if len(reprompts) != 2 {
		t.Fatalf("reprompt count = %d, want 2 (one per idle without finalize)", len(reprompts))
	}
	if findAdapterEvent(t, sender, "outcome.failure") != nil {
		t.Fatal("a converged turn must not emit outcome.failure")
	}
	if dbg := findAdapterEvent(t, sender, "turn.finalize_exhausted"); dbg != nil {
		t.Fatal("a converged turn must not emit turn.finalize_exhausted")
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

// ── KB-47 contract-mode turn-loop tests ──────────────────────────────────────

// composeExecutePrompt: four shapes — legacy (no contracts, no rejection),
// contract conveyance appended, minimal in-session repair, degraded
// re-execute with the rejection note. The in-session decision is a property
// of the session state passed in (KB-47): epochs (1,1) mean the finalize
// landed in the live conversation, (2,1) means a fresh SDK create predates
// the finalize (degraded).
func TestComposeExecutePromptShapes(t *testing.T) {
	const stepPrompt = "review the worktree diff"
	legacy := composeExecutePrompt(stepPrompt, []string{"approved", "failure"}, nil, nil, &sessionState{}, nil)
	wantLegacy := "You must finalize the outcome for this step by calling the `submit_outcome` tool exactly once before ending the turn. The allowed outcomes are: approved, failure. If you do not call the tool with a valid outcome, the step will fail.\n\nreview the worktree diff"
	if legacy != wantLegacy {
		t.Errorf("legacy shape drift:\n got %q\nwant %q", legacy, wantLegacy)
	}
	if strings.Contains(legacy, "Finalization contracts") || strings.Contains(legacy, "rejected by the workflow") {
		t.Errorf("legacy shape must not carry contract/repair content: %q", legacy)
	}

	contracts := []*v2.OutcomeContract{
		{Name: "approved", SchemaJson: []byte(`{"type":"object"}`), RequireComment: true},
		{Name: "stalled", Fallback: true},
	}
	withContract := composeExecutePrompt(stepPrompt, []string{"approved", "stalled"}, contracts, nil, &sessionState{}, nil)
	for _, want := range []string{"Finalization contracts", `"approved"`, `"stalled"`, "require_comment", "fallback contract"} {
		if !strings.Contains(withContract, want) {
			t.Errorf("contract shape missing %q:\n%s", want, withContract)
		}
	}

	rejection := &v2.ExecutionRejection{Outcome: "approved", Issues: "the payload misses the verdict", Attempt: 2}
	liveSession := &sessionState{createdEpoch: 1, finalizeSessionEpoch: 1}
	reopenedSession := &sessionState{createdEpoch: 2, finalizeSessionEpoch: 1}
	repair := composeExecutePrompt(stepPrompt, []string{"approved"}, nil, rejection, liveSession, nil)
	for _, want := range []string{`Rejected outcome: "approved"`, "Issues reported by the workflow", "Resubmit the finalize now"} {
		if !strings.Contains(repair, want) {
			t.Errorf("in-session repair shape missing %q:\n%s", want, repair)
		}
	}
	if strings.Contains(repair, stepPrompt) {
		t.Errorf("in-session repair shape must stay minimal, got the full step prompt:\n%s", repair)
	}

	degraded := composeExecutePrompt(stepPrompt, []string{"approved"}, nil, rejection, reopenedSession, nil)
	for _, want := range []string{stepPrompt, `(outcome "approved", attempt 2)`, "Its reported issues were", "Address these issues"} {
		if !strings.Contains(degraded, want) {
			t.Errorf("degraded shape missing %q:\n%s", want, degraded)
		}
	}

	// Issue text quotes model-generated content: it must not leak a held
	// secret through either repair shape.
	leaky := &v2.ExecutionRejection{Outcome: "approved", Issues: "verdict swordfish was not in the enum", Attempt: 1}
	leakyRepair := composeExecutePrompt("step", []string{"approved"}, nil, leaky, liveSession, []string{"swordfish"})
	leakyDegraded := composeExecutePrompt("step", []string{"approved"}, nil, leaky, reopenedSession, []string{"swordfish"})
	if strings.Contains(leakyRepair, "swordfish") || strings.Contains(leakyDegraded, "swordfish") {
		t.Errorf("repair prompts leak held secrets:\n%s\n%s", leakyRepair, leakyDegraded)
	}
}

func TestContractConveyanceRendering(t *testing.T) {
	if got := contractConveyance(nil); got != "" {
		t.Errorf("no contracts should render empty, got %q", got)
	}
	contracts := []*v2.OutcomeContract{
		{Name: "approved", SchemaJson: []byte(`{"type":"object"}`)},
		{Name: "unspecified"},
		{Name: "stalled", Fallback: true},
	}
	out := contractConveyance(contracts)
	for _, want := range []string{
		"Finalization contracts for this step.",
		`Outcome "approved": when finalizing with this outcome, the submit_outcome call must include a ` + "`payload`",
		"JSON Schema: {\"type\":\"object\"}",
		`Outcome "unspecified": no additional requirements.`,
		"fallback contract: if no other outcome can be finalized",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("conveyance missing %q:\n%s", want, out)
		}
	}
}

func TestRepairInSessionEpochMatrix(t *testing.T) {
	cases := []struct {
		finalizeEpoch, createdEpoch uint32
		want                        bool
	}{
		{3, 3, true},  // finalize landed in the current session generation
		{4, 3, true},  // newer-than-current: still the live conversation
		{2, 3, false}, // finalize predates a fresh SDK create (CRI-272)
		{1, 1, true},  // first-generation finalize
		{0, 1, true},  // no finalize epoch yet; process attached the checkpointed conversation
		{0, 0, true},  // bare unit-test state
		{0, 2, false}, // bookkeeping lost + fresh create: degrade to re-execute
		{1, 2, false},
	}
	for _, tc := range cases {
		s := &sessionState{createdEpoch: tc.createdEpoch, finalizeSessionEpoch: tc.finalizeEpoch}
		if got := s.repairInSession(); got != tc.want {
			t.Errorf("repairInSession(finalize=%d created=%d) = %v, want %v", tc.finalizeEpoch, tc.createdEpoch, got, tc.want)
		}
	}
}

// handleIdleTurn (contract mode): terminal result carries the payload verbatim
// and the comment, both secret-redacted, preceded by outcome.recovered only
// for a host-rejection repair. No legacy outputs assembly.
func TestHandleIdleTurnContractEmission(t *testing.T) {
	newContractSession := func(t *testing.T) (*sessionState, *recordingSender) {
		t.Helper()
		s := contractTestState([]string{"approved"}, contractTestContract(t, "approved", `{"type":"object"}`, true, false))
		sink := &recordingSender{}
		s.active, s.sink = true, sink
		s.heldSecrets = []string{"swordfish"}
		s.finalizedOutcome = "approved"
		s.finalizedComment = "note swordfish"
		s.finalizedPayload = json.RawMessage(`{"verdict":"swordfish","score":2}`)
		return s, sink
	}
	ctx := context.Background()

	t.Run("recovered + payloads", func(t *testing.T) {
		s, sink := newContractSession(t)
		s.mu.Lock()
		s.inRepairMode, s.inRepairAttempt = true, 2
		s.mu.Unlock()
		ts := newTurnState(3)
		done, err := ts.handleIdleTurn(ctx, s, sink, 1)
		if err != nil || !done {
			t.Fatalf("done/err = %v/%v, want true/nil", done, err)
		}
		evs := sink.snapshot()
		var recovered map[string]any
		for _, ev := range evs {
			if a := ev.GetAdapter(); a != nil && a.GetEventKind() == "outcome.recovered" {
				recovered = a.GetPayload().AsMap()
			}
		}
		if recovered == nil {
			t.Fatal("expected an outcome.recovered event for a repaired finalize")
		}
		if recovered["outcome"] != "approved" || recovered["repair_attempt"] != float64(2) {
			t.Errorf("outcome.recovered = %v, want outcome approved repair_attempt 2", recovered)
		}
		r := resultFromSender(sink)
		if r.GetOutcome() != "approved" {
			t.Fatalf("result outcome = %q, want approved", r.GetOutcome())
		}
		if got := string(r.GetOutputsJson()); strings.Contains(got, "swordfish") || !strings.Contains(got, "[REDACTED]") {
			t.Errorf("outputs_json not redacted: %s", got)
		}
		if r.GetComment() != "note [REDACTED]" {
			t.Errorf("comment = %q, want %q", r.GetComment(), "note [REDACTED]")
		}
		if bytes.Contains(s.finalizedPayload, []byte("[REDACTED]")) {
			t.Errorf("stored payload must stay verbatim, got %s", s.finalizedPayload)
		}
	})

	t.Run("no recovered without repair", func(t *testing.T) {
		s, sink := newContractSession(t)
		s.mu.Lock()
		s.inRepairMode = false
		s.mu.Unlock()
		ts := newTurnState(3)
		if done, err := ts.handleIdleTurn(ctx, s, sink, 1); err != nil || !done {
			t.Fatalf("done/err = %v/%v, want true/nil", done, err)
		}
		for _, ev := range sink.snapshot() {
			if a := ev.GetAdapter(); a != nil && a.GetEventKind() == "outcome.recovered" {
				t.Fatal("outcome.recovered must not fire without repair mode")
			}
		}
		if r := resultFromSender(sink); r.GetComment() == "" {
			t.Error("contract result should carry the finalize comment")
		}
	})

	t.Run("legacy shape byte-for-byte", func(t *testing.T) {
		s, sink := newContractSession(t)
		s.mu.Lock()
		s.contractMode = false
		s.finalizedReason = "looks good"
		s.mu.Unlock()
		ts := newTurnState(3)
		if done, err := ts.handleIdleTurn(ctx, s, sink, 1); err != nil || !done {
			t.Fatalf("done/err = %v/%v, want true/nil", done, err)
		}
		r := resultFromSender(sink)
		out := outputsOf(t, r)
		if len(out) != 2 || out["outcome"] != "approved" || out["reason"] != "looks good" {
			t.Errorf("legacy outputs = %v, want exactly outcome+reason", out)
		}
		if r.GetComment() != "" {
			t.Errorf("legacy result must not carry comment, got %q", r.GetComment())
		}
	})
}

// Contract-mode finalize loop, end to end through Execute: a rejected
// payload submission (invalid type) triggers an outcome.payload_invalid event
// and a reprompt carrying the structured issues; the repaired second
// submission forwards its payload verbatim and the comment.
func TestExecuteContractRejectionRepairsInTurn(t *testing.T) {
	contracts := []*v2.OutcomeContract{
		contractTestContract(t, "approved", `{"type":"object","required":["verdict"],"properties":{"verdict":{"type":"string"}}}`, true, false),
	}
	s := contractTestState([]string{"approved"}, contracts...)
	p := outcomeAdapter(s)
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	calls := 0
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		calls++
		var err error
		var res copilot.ToolResult
		if calls == 1 {
			res, err = p.handleSubmitOutcome("s1", SubmitOutcomeArgs{
				Outcome: "approved", Comment: "note", Payload: json.RawMessage(`{"verdict": 9}`),
			})
		} else {
			res, err = p.handleSubmitOutcome("s1", SubmitOutcomeArgs{
				Outcome: "approved", Comment: "note", Payload: json.RawMessage(`{"verdict":"approved","n":1}`),
			})
		}
		if err != nil {
			t.Errorf("handler call %d: err=%v", calls, err)
		}
		wantType := "success"
		if calls == 1 {
			wantType = "failure" // stage 1 is deliberately invalid
		}
		if res.ResultType != wantType {
			t.Errorf("handler call %d: resultType = %q, want %q (%s)", calls, res.ResultType, wantType, res.TextResultForLLM)
		}
	}
	sender := &recordingSender{}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:        "s1",
		Input:            map[string]string{"prompt": "do work"},
		AllowedOutcomes:  []string{"approved"},
		OutcomeContracts: contracts,
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if fake.sendCount != 2 {
		t.Errorf("sendCount = %d, want 2 (initial + one repair reprompt)", fake.sendCount)
	}
	// The reprompt must quote the structured issues.
	if opts := fake.getSentOpts(); len(opts) < 2 || !strings.Contains(opts[1].Prompt, "Your last submission was rejected") || !strings.Contains(opts[1].Prompt, "verdict") {
		t.Errorf("reprompt lacks issue list:\n%v", opts)
	}
	// The rejection was surfaced to the operator.
	found := false
	for _, ev := range sender.snapshot() {
		if a := ev.GetAdapter(); a != nil && a.GetEventKind() == "outcome.payload_invalid" {
			found = true
			d := a.GetPayload().AsMap()
			if d["kind"] != "invalid_payload" {
				t.Errorf("payload_invalid kind = %v, want invalid_payload", d["kind"])
			}
		}
	}
	if !found {
		t.Fatal("expected an outcome.payload_invalid event")
	}
	r := resultFromSender(sender)
	if r.GetOutcome() != "approved" {
		t.Fatalf("result outcome = %q, want approved", r.GetOutcome())
	}
	if got := string(r.GetOutputsJson()); got != `{"verdict":"approved","n":1}` {
		t.Errorf("outputs_json = %s, want the repaired payload verbatim", got)
	}
	if r.GetComment() != "note" {
		t.Errorf("comment = %q, want note", r.GetComment())
	}
}

// Exhaustion under contracts with a fallback: turn.finalize_exhausted is kept
// as operator evidence, the result finalizes with the fallback outcome (no
// outputs, no comment), and the legacy outcome.failure event is skipped.
func TestExecuteContractFallbackOnExhaustion(t *testing.T) {
	contracts := []*v2.OutcomeContract{
		contractTestContract(t, "approved", `{"type":"object"}`, false, false),
		contractTestContract(t, "failure", "", false, true),
	}
	s := contractTestState([]string{"approved", "failure"}, contracts...)
	p := outcomeAdapter(s)
	fake := s.session.(*fakeSession)
	idle := []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	fake.sendSequence = [][]copilot.SessionEvent{idle, idle, idle}
	sender := &recordingSender{}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:        "s1",
		Input:            map[string]string{"prompt": "do work"},
		AllowedOutcomes:  []string{"approved", "failure"},
		OutcomeContracts: contracts,
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	hasExhausted := false
	for _, ev := range sender.snapshot() {
		a := ev.GetAdapter()
		if a == nil {
			continue
		}
		switch a.GetEventKind() {
		case "turn.finalize_exhausted":
			hasExhausted = true
		case "outcome.failure":
			t.Error("outcome.failure must be skipped when a fallback contract fires")
		}
	}
	if !hasExhausted {
		t.Error("turn.finalize_exhausted must be kept as operator evidence")
	}
	r := resultFromSender(sender)
	if r.GetOutcome() != "failure" {
		t.Fatalf("result outcome = %q, want fallback failure", r.GetOutcome())
	}
	if len(r.GetOutputsJson()) != 0 || r.GetComment() != "" {
		t.Errorf("fallback result must carry no outputs/comment, got %s/%q", r.GetOutputsJson(), r.GetComment())
	}
}

// Max-turns reached under contracts with a fallback finalizes the fallback
// outcome instead of the legacy failure/needs_review result.
func TestExecuteContractFallbackOnMaxTurns(t *testing.T) {
	contracts := []*v2.OutcomeContract{
		contractTestContract(t, "approved", "", false, false),
		contractTestContract(t, "stalled", "", false, true),
	}
	s := contractTestState([]string{"approved", "stalled"}, contracts...)
	p := outcomeAdapter(s)
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	sender := &recordingSender{}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:        "s1",
		Input:            map[string]string{"prompt": "do work", "max_turns": "1"},
		AllowedOutcomes:  []string{"approved", "stalled"},
		OutcomeContracts: contracts,
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	r := resultFromSender(sender)
	if r.GetOutcome() != "stalled" {
		t.Fatalf("result outcome = %q, want fallback stalled", r.GetOutcome())
	}
	if len(r.GetOutputsJson()) != 0 || r.GetComment() != "" {
		t.Errorf("fallback result must carry no outputs/comment, got %s/%q", r.GetOutputsJson(), r.GetComment())
	}
}

// The corrective reprompt carries the rejected finalize's issue list,
// truncated and secret-redacted.
func TestRepromptCarriesContractIssues(t *testing.T) {
	s := stateWithOutcomes("approved")
	s.heldSecrets = []string{"swordfish"}
	s.finalizeFailureIssues = []string{
		"validating /properties/verdict: type: swordfish has type \"string\", want \"number\"",
	}
	fake := s.session.(*fakeSession)
	ts := newTurnState(3)
	if err := ts.reprompt(context.Background(), s); err != nil {
		t.Fatalf("reprompt returned error: %v", err)
	}
	if opts := fake.getSentOpts(); len(opts) != 1 {
		t.Fatalf("sentOpts = %d calls, want 1", len(opts))
	}
	prompt := fake.getSentOpts()[0].Prompt
	if !strings.Contains(prompt, "Your last submission was rejected") {
		t.Errorf("reprompt missing rejection marker:\n%s", prompt)
	}
	if !strings.Contains(prompt, "/properties/verdict") {
		t.Errorf("reprompt missing the structured issue:\n%s", prompt)
	}
	if strings.Contains(prompt, "swordfish") {
		t.Errorf("reprompt leaks the held secret:\n%s", prompt)
	}
	if !strings.Contains(prompt, "[REDACTED]") {
		t.Errorf("reprompt should show the redaction placeholder:\n%s", prompt)
	}
}
