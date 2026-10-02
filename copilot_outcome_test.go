// copilot_outcome_test.go — unit tests for handleSubmitOutcome, reprompt loop,
// and supporting helpers.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func outcomeAdapter(s *sessionState) *copilotAdapter {
	return &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}
}

func stateWithOutcomes(allowed ...string) *sessionState {
	set := make(map[string]struct{}, len(allowed))
	for _, o := range allowed {
		set[o] = struct{}{}
	}
	return &sessionState{
		session:               &fakeSession{},
		activeAllowedOutcomes: set,
	}
}

func stateWithOutcomesAndSecrets(allowed []string, secrets []string) *sessionState {
	s := stateWithOutcomes(allowed...)
	s.heldSecrets = secrets
	return s
}

// ── handleSubmitOutcome unit tests ───────────────────────────────────────────

// Test 5.1: Valid outcome sets finalizedOutcome, returns success ToolResult.
func TestHandleSubmitOutcomeSuccess(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	p := outcomeAdapter(s)

	res, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success", Reason: "looks good"})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "success" {
		t.Errorf("ResultType = %q, want %q", res.ResultType, "success")
	}
	if !strings.Contains(res.TextResultForLLM, "success") {
		t.Errorf("TextResultForLLM %q should mention the outcome", res.TextResultForLLM)
	}

	s.mu.Lock()
	outcome, reason := s.finalizedOutcome, s.finalizedReason
	attempts := s.finalizeAttempts
	s.mu.Unlock()

	if outcome != "success" {
		t.Errorf("finalizedOutcome = %q, want %q", outcome, "success")
	}
	if reason != "looks good" {
		t.Errorf("finalizedReason = %q, want %q", reason, "looks good")
	}
	if attempts != 1 {
		t.Errorf("finalizeAttempts = %d, want 1", attempts)
	}
}

// Test 5.2: Invalid outcome not in allowed set returns failure ToolResult.
func TestHandleSubmitOutcomeInvalidOutcome(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	p := outcomeAdapter(s)

	res, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "unknown"})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" {
		t.Errorf("ResultType = %q, want %q", res.ResultType, "failure")
	}
	if !strings.Contains(res.TextResultForLLM, "not in the allowed set") {
		t.Errorf("TextResultForLLM %q should mention allowed set constraint", res.TextResultForLLM)
	}
	if !strings.Contains(res.TextResultForLLM, "failure") || !strings.Contains(res.TextResultForLLM, "success") {
		t.Errorf("TextResultForLLM %q should list allowed outcomes", res.TextResultForLLM)
	}

	s.mu.Lock()
	outcome := s.finalizedOutcome
	attempts := s.finalizeAttempts
	kind := s.finalizeFailureKind
	s.mu.Unlock()

	if outcome != "" {
		t.Errorf("finalizedOutcome should be empty on invalid call, got %q", outcome)
	}
	if attempts != 1 {
		t.Errorf("finalizeAttempts = %d, want 1 (invalid attempts still count)", attempts)
	}
	if kind != "invalid_outcome" {
		t.Errorf("finalizeFailureKind = %q, want %q", kind, "invalid_outcome")
	}
}

// Test 5.2b: submit_outcome on a step with no declared outcomes returns a clear
// error with kind="no_outcomes" — not "invalid_outcome" — so the failure event
// accurately reflects a misconfigured step rather than a model error.
func TestHandleSubmitOutcomeNoOutcomesDeclared(t *testing.T) {
	s := stateWithOutcomes() // empty allowed set
	p := outcomeAdapter(s)

	res, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success"})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" {
		t.Errorf("ResultType = %q, want %q", res.ResultType, "failure")
	}
	if !strings.Contains(res.TextResultForLLM, "no outcomes are declared") {
		t.Errorf("TextResultForLLM %q should mention no declared outcomes", res.TextResultForLLM)
	}

	s.mu.Lock()
	kind := s.finalizeFailureKind
	s.mu.Unlock()

	if kind != "no_outcomes" {
		t.Errorf("finalizeFailureKind = %q, want %q", kind, "no_outcomes")
	}
}

// Test 5.3: Empty outcome string returns failure ToolResult.
func TestHandleSubmitOutcomeEmptyOutcome(t *testing.T) {
	s := stateWithOutcomes("success")
	p := outcomeAdapter(s)

	res, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "   "})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" {
		t.Errorf("ResultType = %q, want %q", res.ResultType, "failure")
	}
	if !strings.Contains(res.TextResultForLLM, "outcome is required") {
		t.Errorf("TextResultForLLM %q should mention outcome is required", res.TextResultForLLM)
	}
	// Empty-string case: attempts still incremented.
	s.mu.Lock()
	attempts := s.finalizeAttempts
	kind := s.finalizeFailureKind
	s.mu.Unlock()
	if attempts != 1 {
		t.Errorf("finalizeAttempts = %d, want 1", attempts)
	}
	if kind != "missing" {
		t.Errorf("finalizeFailureKind = %q, want %q", kind, "missing")
	}
}

// Test 5.4: Duplicate call in same turn returns failure ToolResult preserving
// the first outcome.
func TestHandleSubmitOutcomeDuplicate(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	p := outcomeAdapter(s)

	if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success"}); err != nil {
		t.Fatalf("first call: unexpected Go error: %v", err)
	}

	res, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "failure"})
	if err != nil {
		t.Fatalf("second call: unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" {
		t.Errorf("ResultType = %q, want %q (duplicate must be rejected)", res.ResultType, "failure")
	}
	if !strings.Contains(res.TextResultForLLM, "already finalized") {
		t.Errorf("TextResultForLLM %q should mention already finalized", res.TextResultForLLM)
	}

	// The first outcome must be preserved.
	s.mu.Lock()
	outcome := s.finalizedOutcome
	kind := s.finalizeFailureKind
	s.mu.Unlock()
	if outcome != "success" {
		t.Errorf("finalizedOutcome = %q, want %q (first call wins)", outcome, "success")
	}
	// The second (duplicate) call must record the failure kind.
	if kind != "duplicate" {
		t.Errorf("finalizeFailureKind = %q, want %q (second call is a duplicate)", kind, "duplicate")
	}
}

// Test 5.4b: Duplicate call with an invalid/empty outcome is still classified as
// "duplicate" — not "invalid_outcome" or "missing" — because the first valid
// call already finalized the step. The check order must be: duplicate → missing
// → invalid, not the reverse.
func TestHandleSubmitOutcomeDuplicateInvalidArgs(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	p := outcomeAdapter(s)

	if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success"}); err != nil {
		t.Fatalf("first call: unexpected Go error: %v", err)
	}

	// Second call with an out-of-set outcome: must be "duplicate", not "invalid_outcome".
	res, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "not-valid"})
	if err != nil {
		t.Fatalf("second call: unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" {
		t.Errorf("ResultType = %q, want %q", res.ResultType, "failure")
	}
	if !strings.Contains(res.TextResultForLLM, "already finalized") {
		t.Errorf("TextResultForLLM %q should mention already finalized", res.TextResultForLLM)
	}

	s.mu.Lock()
	kind := s.finalizeFailureKind
	s.mu.Unlock()
	if kind != "duplicate" {
		t.Errorf("finalizeFailureKind = %q, want %q (duplicate check precedes set-membership check)", kind, "duplicate")
	}
}

// Test 5.5: Unknown session ID returns failure ToolResult (not a Go error).
func TestHandleSubmitOutcomeUnknownSession(t *testing.T) {
	p := &copilotAdapter{sessions: map[string]*sessionState{}}
	res, err := p.handleSubmitOutcome("no-such-session", SubmitOutcomeArgs{Outcome: "success"})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" {
		t.Errorf("ResultType = %q, want %q", res.ResultType, "failure")
	}
}

// Test 5.6: outcome.finalized event is emitted to the sink on success.
func TestHandleSubmitOutcomeEmitsEvent(t *testing.T) {
	s := stateWithOutcomes("success")
	sink := &recordingSender{}
	s.mu.Lock()
	s.active = true
	s.sink = sink
	s.mu.Unlock()
	p := outcomeAdapter(s)

	if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success", Reason: "done"}); err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}

	found := false
	for _, ev := range sink.snapshot() {
		if a := ev.GetAdapter(); a != nil && a.GetEventKind() == "outcome.finalized" {
			found = true
			d := a.GetPayload().AsMap()
			if d["outcome"] != "success" {
				t.Errorf("outcome.finalized event outcome = %v, want %q", d["outcome"], "success")
			}
		}
	}
	if !found {
		t.Fatal("expected outcome.finalized adapter event")
	}
}

// ── sortedAllowedOutcomes helper ─────────────────────────────────────────────

// Test 5.7: sortedAllowedOutcomes returns deterministically sorted slice.
func TestSortedAllowedOutcomes(t *testing.T) {
	set := map[string]struct{}{
		"success":      {},
		"needs_review": {},
		"failure":      {},
	}
	got := sortedAllowedOutcomes(set)
	want := []string{"failure", "needs_review", "success"}
	if len(got) != len(want) {
		t.Fatalf("sortedAllowedOutcomes len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sortedAllowedOutcomes[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// ── reprompt loop integration tests ──────────────────────────────────────────

// Test 5.8: Submit on first turn → success result, single Send call.
func TestAwaitOutcomeSuccessOnFirstTurn(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
		{Data: &copilot.SessionIdleData{}},
	}
	// Simulate tool call via onSend hook.
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		s.mu.Lock()
		s.finalizedOutcome = "success"
		s.mu.Unlock()
	}
	p := outcomeAdapter(s)
	sender := &recordingSender{}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")
	if fake.sendCount != 1 {
		t.Errorf("sendCount = %d, want 1 (no reprompts)", fake.sendCount)
	}
}

// Test 5.9: Missing submit on first turn → reprompt → submit on second turn → success.
func TestAwaitOutcomeSuccessAfterOneReprompt(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	// Turn 1: no tool call (session goes idle without finalize).
	// Turn 2: tool call simulated, then idle.
	fake.sendSequence = [][]copilot.SessionEvent{
		{ // turn 1: just idle, no outcome
			{Data: &copilot.SessionIdleData{}},
		},
		{ // turn 2: message + idle
			{Data: &copilot.AssistantMessageData{MessageID: "m2", Content: "ok"}},
			{Data: &copilot.SessionIdleData{}},
		},
	}
	// Simulate submit_outcome on second Send call.
	fake.onSend = func(callIndex int, _ copilot.MessageOptions) {
		if callIndex == 1 {
			s.mu.Lock()
			s.finalizedOutcome = "success"
			s.mu.Unlock()
		}
	}
	p := outcomeAdapter(s)
	sender := &recordingSender{}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")
	if fake.sendCount != 2 {
		t.Errorf("sendCount = %d, want 2 (1 initial + 1 reprompt)", fake.sendCount)
	}
	// Second Send must contain the reprompt wording.
	opts := fake.getSentOpts()
	if !strings.Contains(opts[1].Prompt, "submit_outcome") {
		t.Errorf("reprompt message should mention submit_outcome, got: %q", opts[1].Prompt)
	}
	if !strings.Contains(opts[1].Prompt, "success") {
		t.Errorf("reprompt message should list allowed outcomes, got: %q", opts[1].Prompt)
	}
}

// Test 5.10: All 3 turns exhausted without submit → failure result.
func TestAwaitOutcomeExhaustedReturnsFailure(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	// All 3 turns: idle without any submit_outcome call.
	idle := []copilot.SessionEvent{
		{Data: &copilot.SessionIdleData{}},
	}
	fake.sendSequence = [][]copilot.SessionEvent{idle, idle, idle}
	p := outcomeAdapter(s)
	sender := &recordingSender{}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "failure")
	if fake.sendCount != maxFinalizeAttempts {
		t.Errorf("sendCount = %d, want %d (1 initial + 2 reprompts)", fake.sendCount, maxFinalizeAttempts)
	}

	// A structured outcome.failure adapter event should be present with
	// required diagnostic fields.
	hasFailureEvent := false
	for _, ev := range sender.snapshot() {
		a := ev.GetAdapter()
		if a == nil || a.GetEventKind() != "outcome.failure" {
			continue
		}
		hasFailureEvent = true
		d := a.GetPayload().AsMap()
		// Verify the structured fields are present.
		if _, ok := d["kind"]; !ok {
			t.Error("outcome.failure event missing 'kind' field")
		}
		if _, ok := d["allowed_outcomes"]; !ok {
			t.Error("outcome.failure event missing 'allowed_outcomes' field")
		}
		if _, ok := d["attempts"]; !ok {
			t.Error("outcome.failure event missing 'attempts' field")
		}
		if _, ok := d["reason"]; !ok {
			t.Error("outcome.failure event missing 'reason' field")
		}
	}
	if !hasFailureEvent {
		t.Fatal("expected outcome.failure adapter event on exhaustion")
	}
}

// Test 5.11: max_turns reached with needs_review in allowed set → needs_review result.
func TestMaxTurnsWithNeedsReviewAllowed(t *testing.T) {
	s := stateWithOutcomes("success", "failure", "needs_review")
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "thinking"}},
	}
	p := outcomeAdapter(s)
	sender := &recordingSender{}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work", "max_turns": "1"},
		AllowedOutcomes: []string{"failure", "needs_review", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "needs_review")
}

// ── assertion helper ──────────────────────────────────────────────────────────

func assertOutcome(t *testing.T, sender *recordingSender, want string) {
	t.Helper()
	for _, ev := range sender.snapshot() {
		if r := ev.GetResult(); r != nil {
			if r.GetOutcome() == want {
				return
			}
			t.Fatalf("outcome = %q, want %q", r.GetOutcome(), want)
		}
	}
	t.Fatalf("no result event found; want outcome %q", want)
}

// resultFromSender returns the ExecuteResult from the last result event, or
// nil if none was emitted.
func resultFromSender(sender *recordingSender) *v2.ExecuteResult {
	for _, ev := range sender.snapshot() {
		if r := ev.GetResult(); r != nil {
			return r
		}
	}
	return nil
}

// outputsOf decodes the native outputs_json channel into a string map for
// assertions. The copilot adapter emits only string-valued outputs (outcome,
// reason), so map[string]string is sufficient.
func outputsOf(t *testing.T, result *v2.ExecuteResult) map[string]string {
	t.Helper()
	oj := result.GetOutputsJson()
	if len(oj) == 0 {
		return map[string]string{}
	}
	var m map[string]string
	if err := json.Unmarshal(oj, &m); err != nil {
		t.Fatalf("decode outputs_json: %v", err)
	}
	return m
}

// Successful finalization must surface both the outcome and the model-supplied
// reason as step outputs so downstream workflow expressions can reference
// steps.<name>.outcome and steps.<name>.reason.
func TestAwaitOutcome_OutcomeAndReasonInOutputs(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
		{Data: &copilot.SessionIdleData{}},
	}
	p := outcomeAdapter(s)
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success", Reason: "all checks passed"}); err != nil {
			t.Errorf("handleSubmitOutcome: unexpected error: %v", err)
		}
	}

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	result := resultFromSender(sender)
	if result == nil {
		t.Fatal("no result event emitted")
	}
	outputs := outputsOf(t, result)
	if got := outputs["outcome"]; got != "success" {
		t.Errorf("outputs[outcome] = %q, want %q", got, "success")
	}
	if got := outputs["reason"]; got != "all checks passed" {
		t.Errorf("outputs[reason] = %q, want %q", got, "all checks passed")
	}
}

// Failure paths must still emit outputs with the outcome key populated. The
// reason carries the KB-42 failure evidence: why no outcome was submitted,
// plus the last agent message when one exists (KB-42).
func TestAwaitOutcome_FailurePathPopulatesOutcomeOutput(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	idle := []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	fake.sendSequence = [][]copilot.SessionEvent{idle, idle, idle}

	sender := &recordingSender{}
	if err := outcomeAdapter(s).Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	result := resultFromSender(sender)
	if result == nil {
		t.Fatal("no result event emitted")
	}
	outputs := outputsOf(t, result)
	if got := outputs["outcome"]; got != "failure" {
		t.Errorf("outputs[outcome] = %q, want %q", got, "failure")
	}
	if got, ok := outputs["reason"]; !ok || got == "" {
		t.Errorf("outputs[reason] = (%q, present=%v), want the KB-42 failure evidence", got, ok)
	}
}

// A secret delivered over the secret channel must not appear verbatim in the
// returned `reason` output; it is replaced with a visible placeholder.
func TestAwaitOutcome_RedactsHeldSecretFromReason(t *testing.T) {
	secret := "ghp_delivered_over_secret_channel_12345"
	s := stateWithOutcomesAndSecrets([]string{"success", "failure"}, []string{secret})
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
		{Data: &copilot.SessionIdleData{}},
	}
	p := outcomeAdapter(s)
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		reason := fmt.Sprintf("I used token %s to complete the task", secret)
		if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success", Reason: reason}); err != nil {
			t.Errorf("handleSubmitOutcome: unexpected error: %v", err)
		}
	}

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	result := resultFromSender(sender)
	if result == nil {
		t.Fatal("no result event emitted")
	}
	outputs := outputsOf(t, result)
	if got := outputs["outcome"]; got != "success" {
		t.Errorf("outputs[outcome] = %q, want %q", got, "success")
	}
	if strings.Contains(outputs["reason"], secret) {
		t.Errorf("outputs[reason] still contains secret; got %q", outputs["reason"])
	}
	want := fmt.Sprintf("I used token %s to complete the task", redactedPlaceholder)
	if outputs["reason"] != want {
		t.Errorf("outputs[reason] = %q, want %q", outputs["reason"], want)
	}
}

// When `reason` contains no held secret, redaction must not alter it at all.
func TestAwaitOutcome_ReasonPassThroughWithoutSecret(t *testing.T) {
	reason := "all checks passed"
	s := stateWithOutcomesAndSecrets([]string{"success", "failure"}, []string{"ghp_some_secret_value"})
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "done"}},
		{Data: &copilot.SessionIdleData{}},
	}
	p := outcomeAdapter(s)
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success", Reason: reason}); err != nil {
			t.Errorf("handleSubmitOutcome: unexpected error: %v", err)
		}
	}

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	result := resultFromSender(sender)
	if result == nil {
		t.Fatal("no result event emitted")
	}
	outputs := outputsOf(t, result)
	if got := outputs["reason"]; got != reason {
		t.Errorf("outputs[reason] = %q, want %q", got, reason)
	}
}

// ── additional Step 5.1 matrix tests ─────────────────────────────────────────

// Test 5.12: Success on 3rd attempt (2 reprompts before success).
func TestSubmitOutcome_RepromptTwice(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	idle := []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	withIdle := []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m3", Content: "ok"}},
		{Data: &copilot.SessionIdleData{}},
	}
	// Turn 1, 2: no outcome. Turn 3: outcome submitted.
	fake.sendSequence = [][]copilot.SessionEvent{idle, idle, withIdle}
	fake.onSend = func(callIndex int, _ copilot.MessageOptions) {
		if callIndex == 2 {
			s.mu.Lock()
			s.finalizedOutcome = "success"
			s.mu.Unlock()
		}
	}

	sender := &recordingSender{}
	if err := outcomeAdapter(s).Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")
	if fake.sendCount != 3 {
		t.Errorf("sendCount = %d, want 3 (1 initial + 2 reprompts)", fake.sendCount)
	}
}

// Test 5.13: Invalid outcome on turn 1 (handler rejects it), success on turn 2.
// This validates that invalid enum rejections don't prevent eventual success.
// The real handleSubmitOutcome is called directly from onSend so the actual
// handler validation path (not manual state mutation) is exercised.
func TestSubmitOutcome_InvalidEnumThenSuccess(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	idle := []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	withIdle := []copilot.SessionEvent{
		{Data: &copilot.SessionIdleData{}},
	}
	fake.sendSequence = [][]copilot.SessionEvent{idle, withIdle}

	p := outcomeAdapter(s)
	fake.onSend = func(callIndex int, _ copilot.MessageOptions) {
		switch callIndex {
		case 0:
			// Turn 1: model calls submit_outcome with a value outside the allowed
			// set. The real handler rejects it, increments finalizeAttempts, and
			// sets finalizeFailureKind = "invalid_outcome".
			if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "not-valid"}); err != nil {
				t.Errorf("handleSubmitOutcome turn 1: unexpected error: %v", err)
			}
		case 1:
			// Turn 2: model corrects the call; handler accepts and sets
			// finalizedOutcome = "success".
			if _, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "success"}); err != nil {
				t.Errorf("handleSubmitOutcome turn 2: unexpected error: %v", err)
			}
		}
	}

	sender := &recordingSender{}
	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "success")

	s.mu.Lock()
	kind := s.finalizeFailureKind
	s.mu.Unlock()
	// After a successful outcome, finalizeFailureKind retains the category of
	// the last rejection ("invalid_outcome" from the first call). This is
	// correct: the field is only reset by beginExecution at the start of each
	// step and set on failure paths — successful calls do not clear it.
	if kind != "invalid_outcome" {
		t.Errorf("finalizeFailureKind = %q after invalid-then-success, want %q (last rejection category preserved)", kind, "invalid_outcome")
	}
}

// Test 5.14: No submit_outcome called after idle turns → "failure" outcome.
// WS03: permissions are auto-approved; this test verifies the reprompt-exhaustion
// path that results in "failure" when the model never calls submit_outcome.
func TestSubmitOutcome_PermissionDeniedFailure(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	// All turns: session idles without the model calling submit_outcome.
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.SessionIdleData{}},
	}

	sender := &recordingSender{}
	if err := outcomeAdapter(s).Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "failure")
}

// Test 5.15: max_turns hit without "needs_review" in allowed set → "failure".
// Regression guard for the handleMaxTurnsReached path; distinct from
// TestMaxTurnsWithNeedsReviewAllowed (Test 5.11) which expects "needs_review".
func TestSubmitOutcome_MaxTurnsReached_NoNeedsReviewInAllowed(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "thinking..."}},
	}

	sender := &recordingSender{}
	if err := outcomeAdapter(s).Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do work", "max_turns": "1"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "failure")
}

// Test 5.16: Empty allowed set → fails immediately on the first idle turn with
// kind="no_outcomes", without spending reprompt turns. Validates that a
// misconfigured step (no outcome blocks declared) produces a clear, immediate
// failure rather than burning 2 extra reprompt turns that can never succeed.
func TestSubmitOutcome_EmptyAllowedSetFailsClosed(t *testing.T) {
	s := stateWithOutcomes() // no outcomes declared
	fake := s.session.(*fakeSession)
	idle := []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	// Provide multiple idles so the test catches any unwanted reprompt turns.
	fake.sendSequence = [][]copilot.SessionEvent{idle, idle, idle}

	sender := &recordingSender{}
	if err := outcomeAdapter(s).Execute(context.Background(), &v2.ExecuteRequest{
		SessionId: "s1",
		Input:     map[string]string{"prompt": "do work"},
		// No AllowedOutcomes: empty set.
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertOutcome(t, sender, "failure")

	// Must fail on the first turn: only 1 Send call (the initial prompt); no
	// reprompts because the step can never succeed with an empty outcome set.
	if fake.sendCount != 1 {
		t.Errorf("sendCount = %d, want 1 (immediate failure, no reprompts for empty allowed set)", fake.sendCount)
	}

	// outcome.failure event must be present with kind="no_outcomes" so operators
	// can distinguish a misconfigured step from a model that failed to finalize.
	hasFailureEvent := false
	for _, ev := range sender.snapshot() {
		a := ev.GetAdapter()
		if a == nil || a.GetEventKind() != "outcome.failure" {
			continue
		}
		hasFailureEvent = true
		d := a.GetPayload().AsMap()
		if kind, _ := d["kind"].(string); kind != "no_outcomes" {
			t.Errorf("outcome.failure kind = %q, want %q", kind, "no_outcomes")
		}
		if reason, _ := d["reason"].(string); reason != "step has no declared outcomes" {
			t.Errorf("outcome.failure reason = %q, want %q", reason, "step has no declared outcomes")
		}
	}
	if !hasFailureEvent {
		t.Fatal("expected outcome.failure adapter event on empty allowed set")
	}
}

// Test 5.17: Initial Send contains a preamble listing the allowed outcomes.
// Verifies that the model receives the submit_outcome instruction on the first
// turn, not just in reprompts.
func TestSubmitOutcome_PreamblePresentInPrompt(t *testing.T) {
	s := stateWithOutcomes("success", "failure")
	fake := s.session.(*fakeSession)
	// Use a single idle turn; we only care about the sent prompt, not the result.
	fake.emitOnSend = []copilot.SessionEvent{
		{Data: &copilot.SessionIdleData{}},
	}
	// Turn 2 and 3 produce idle too (for exhaustion path).
	idle := []copilot.SessionEvent{{Data: &copilot.SessionIdleData{}}}
	fake.sendSequence = [][]copilot.SessionEvent{idle, idle, idle}

	sender := &recordingSender{}
	_ = outcomeAdapter(s).Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "do the task"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender)

	opts := fake.getSentOpts()
	if len(opts) == 0 {
		t.Fatal("expected at least one Send call")
	}
	firstPrompt := opts[0].Prompt
	if !strings.Contains(firstPrompt, "allowed outcomes are") {
		t.Errorf("first prompt should contain allowed-outcomes preamble, got: %q", firstPrompt)
	}
	if !strings.Contains(firstPrompt, "success") {
		t.Errorf("first prompt should list 'success' in allowed outcomes, got: %q", firstPrompt)
	}
	if !strings.Contains(firstPrompt, "failure") {
		t.Errorf("first prompt should list 'failure' in allowed outcomes, got: %q", firstPrompt)
	}
	if !strings.Contains(firstPrompt, "do the task") {
		t.Errorf("first prompt should include the step prompt, got: %q", firstPrompt)
	}
}

// ── contract-mode submit_outcome tests (KB-47) ───────────────────────────────

// contractTestContract builds an OutcomeContract for handler tests.
func contractTestContract(t *testing.T, name string, schema string, requireComment, fallback bool) *v2.OutcomeContract {
	t.Helper()
	c := &v2.OutcomeContract{Name: name, RequireComment: requireComment, Fallback: fallback}
	if schema != "" {
		c.SchemaJson = []byte(schema)
	}
	return c
}

// contractTestState returns a sessionState in contract mode with the given
// allowed outcomes and contracts; a fallback-marked contract also becomes
// fallbackContractName.
func contractTestState(allowed []string, contracts ...*v2.OutcomeContract) *sessionState {
	s := stateWithOutcomes(allowed...)
	s.contractMode = true
	// OpenSession sets createdEpoch = 1; every accepted contract finalize
	// stamps finalizeSessionEpoch with it (repair eligibility).
	s.createdEpoch = 1
	s.activeContracts = make(map[string]*v2.OutcomeContract, len(contracts))
	for _, c := range contracts {
		s.activeContracts[c.GetName()] = c
		if c.GetFallback() {
			s.fallbackContractName = c.GetName()
		}
	}
	return s
}

// verbatimSchema: object with a required string verdict property.
const verbatimSchema = `{"type":"object","required":["verdict"],"properties":{"verdict":{"type":"string"}}}`

// Contract-mode finalize accepted: outcome, verbatim payload, and trimmed
// comment all land on sessionState, and the finalize session epoch is set.
func TestContractSubmitAcceptedStoresPayloadAndComment(t *testing.T) {
	s := contractTestState([]string{"approved", "failure"},
		contractTestContract(t, "approved", `{"type":"object"}`, false, false))
	p := outcomeAdapter(s)

	res, err := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{
		Outcome: "approved",
		Reason:  " ship it ",
		Comment: " LGTM ",
		Payload: json.RawMessage(`{"b":1,"a":2}`),
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "success" {
		t.Fatalf("ResultType = %q, want success (%s)", res.ResultType, res.TextResultForLLM)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalizedOutcome != "approved" || s.finalizedReason != "ship it" || s.finalizedComment != "LGTM" {
		t.Errorf("finalized = %q/%q/%q, want approved/ship it/LGTM", s.finalizedOutcome, s.finalizedReason, s.finalizedComment)
	}
	if string(s.finalizedPayload) != `{"b":1,"a":2}` {
		t.Errorf("payload stored as %s, want the submitted bytes verbatim", s.finalizedPayload)
	}
	if s.finalizeSessionEpoch == 0 {
		t.Error("finalizeSessionEpoch not set on accepted contract finalize")
	}
}

// Contract-mode accepted finalize with no schema and no payload forwards an
// empty-object payload (proto: absent/empty/null payload = empty object).
func TestContractSubmitNoSchemaNoPayloadYieldsEmptyObject(t *testing.T) {
	s := contractTestState([]string{"approved"},
		contractTestContract(t, "approved", "", false, false))
	p := outcomeAdapter(s)

	res, _ := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "approved"})
	if res.ResultType != "success" {
		t.Fatalf("ResultType = %q, want success (%s)", res.ResultType, res.TextResultForLLM)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if string(s.finalizedPayload) != "{}" {
		t.Errorf("finalizedPayload = %s, want {}", s.finalizedPayload)
	}
}

// Uncontracted allowed outcome in contract mode: the adapter does not reject
// it (host owns outcome_uncontracted post-finalize); the submission finalizes.
func TestContractSubmitUncontractedOutcomeIsForwarded(t *testing.T) {
	s := contractTestState([]string{"approved", "failure"},
		contractTestContract(t, "approved", verbatimSchema, false, false))
	p := outcomeAdapter(s)

	res, _ := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "failure"})
	if res.ResultType != "success" {
		t.Fatalf("ResultType = %q, want success (adapter must not reject uncontracted outcomes): %s", res.ResultType, res.TextResultForLLM)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalizedOutcome != "failure" {
		t.Errorf("finalizedOutcome = %q, want failure", s.finalizedOutcome)
	}
	if s.finalizeFailureIssues != nil {
		t.Errorf("finalizeFailureIssues = %v, want none", s.finalizeFailureIssues)
	}
}

// require_comment without a comment → comment_missing rejection that does not
// finalize, and reports the requirement in the ToolResult text.
func TestContractSubmitCommentMissing(t *testing.T) {
	s := contractTestState([]string{"approved"},
		contractTestContract(t, "approved", verbatimSchema, true, false))
	p := outcomeAdapter(s)

	s.mu.Lock()
	s.finalizeAttempts = 0
	s.mu.Unlock()
	for _, comment := range []string{"", "   "} {
		res, _ := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "approved", Comment: comment, Payload: json.RawMessage(`{"verdict":"ok"}`)})
		if res.ResultType != "failure" {
			t.Fatalf("comment %q: ResultType = %q, want failure", comment, res.ResultType)
		}
		if !strings.Contains(res.TextResultForLLM, "requires a finalize comment") || !strings.Contains(res.TextResultForLLM, "Fix the listed issues") {
			t.Errorf("comment %q: message %q should state the requirement and invite repair", comment, res.TextResultForLLM)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalizedOutcome != "" || s.finalizeFailureKind != "comment_missing" {
		t.Errorf("state = outcome %q kind %q, want unfinalized comment_missing", s.finalizedOutcome, s.finalizeFailureKind)
	}
	if s.finalizeAttempts != 2 {
		t.Errorf("finalizeAttempts = %d, want 2 (one per rejected call)", s.finalizeAttempts)
	}
}

// Schema violation → invalid_payload rejection with the validator's structured
// issue text so the model can repair in-turn.
func TestContractSubmitInvalidPayloadStructuredIssues(t *testing.T) {
	s := contractTestState([]string{"approved"},
		contractTestContract(t, "approved", verbatimSchema, false, false))
	p := outcomeAdapter(s)

	res, _ := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{Outcome: "approved", Payload: json.RawMessage(`{"verdict": 9}`)})
	if res.ResultType != "failure" {
		t.Fatalf("ResultType = %q, want failure", res.ResultType)
	}
	s.mu.Lock()
	kind, issues := s.finalizeFailureKind, s.finalizeFailureIssues
	s.mu.Unlock()
	if kind != "invalid_payload" {
		t.Errorf("finalizeFailureKind = %q, want invalid_payload", kind)
	}
	if len(issues) == 0 || !strings.Contains(strings.Join(issues, "; "), "properties/verdict") {
		t.Errorf("issues = %v, want a structured validator issue naming the failing path", issues)
	}
}

// A payload that breaks a schema whose messages quote the submitted value: the
// ToolResult keeps the raw issue (same session, model repairs from it) while
// the emitted outcome.payload_invalid event is secret-redacted.
func TestContractSubmitInvalidPayloadRedactsEventIssuesButKeepsToolResultRaw(t *testing.T) {
	s := contractTestState([]string{"approved"},
		contractTestContract(t, "approved", `{"type":"object","required":["note"],"properties":{"note":{"type":"array"}}}`, false, false))
	s.heldSecrets = []string{"swordfish"}
	sink := &recordingSender{}
	s.mu.Lock()
	s.active, s.sink = true, sink
	s.mu.Unlock()
	p := outcomeAdapter(s)

	res, _ := p.handleSubmitOutcome("s1", SubmitOutcomeArgs{
		Outcome: "approved",
		Payload: json.RawMessage(`{"note": "swordfish"}`),
	})
	if res.ResultType != "failure" {
		t.Fatalf("ResultType = %q, want failure", res.ResultType)
	}

	s.mu.Lock()
	kind := s.finalizeFailureKind
	s.mu.Unlock()
	if kind != "invalid_payload" {
		t.Fatalf("finalizeFailureKind = %q, want invalid_payload", kind)
	}

	// The ToolResult goes only to the model in the live session.
	if !strings.Contains(res.TextResultForLLM, "swordfish") {
		t.Errorf("ToolResult should carry the validator issue verbatim for in-session repair, got %q", res.TextResultForLLM)
	}

	found := false
	for _, ev := range sink.snapshot() {
		a := ev.GetAdapter()
		if a == nil || a.GetEventKind() != "outcome.payload_invalid" {
			continue
		}
		found = true
		d := a.GetPayload().AsMap()
		if d["outcome"] != "approved" || d["kind"] != "invalid_payload" {
			t.Errorf("event = %v, want outcome approved kind invalid_payload", d)
		}
		issues, ok := d["issues"].([]any)
		if !ok || len(issues) == 0 {
			t.Fatalf("event issues = %#v, want a non-empty list", d["issues"])
		}
		for _, i := range issues {
			if txt, ok := i.(string); ok && strings.Contains(txt, "swordfish") {
				t.Errorf("event issue %q leaks the held secret", txt)
			}
		}
	}
	if !found {
		t.Fatal("expected an outcome.payload_invalid adapter event")
	}
}

// Undecodable invocation envelopes (hand-built tools bypass reflection
// decoding) are classified invalid_outcome and consume the finalize budget.
func TestSubmitOutcomeRawDecodeEnvelopeFailures(t *testing.T) {
	s := contractTestState([]string{"approved"},
		contractTestContract(t, "approved", verbatimSchema, false, false))
	p := outcomeAdapter(s)

	res, err := p.handleSubmitOutcomeRaw("s1", copilot.ToolInvocation{ToolName: "submit_outcome", Arguments: "not-an-object"})
	if err != nil {
		t.Fatalf("raw handler must return ToolResult, not Go error: %v", err)
	}
	if res.ResultType != "failure" || !strings.Contains(res.TextResultForLLM, "JSON object") {
		t.Errorf("string envelope: %q / %q, want failure with decode guidance", res.ResultType, res.TextResultForLLM)
	}
	s.mu.Lock()
	kind, attempts := s.finalizeFailureKind, s.finalizeAttempts
	s.mu.Unlock()
	if kind != "invalid_outcome" || attempts != 1 {
		t.Errorf("kind/attempts = %q/%d, want invalid_outcome/1", kind, attempts)
	}

	res, err = p.handleSubmitOutcomeRaw("s1", copilot.ToolInvocation{ToolName: "submit_outcome", Arguments: "{nope"})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" {
		t.Errorf("malformed envelope: ResultType = %q, want failure", res.ResultType)
	}

	// A good round-trip still succeeds through the raw handler.
	res, err = p.handleSubmitOutcomeRaw("s1", copilot.ToolInvocation{
		ToolName:  "submit_outcome",
		Arguments: map[string]any{"outcome": "approved", "payload": map[string]any{"verdict": "ok"}},
	})
	if err != nil || res.ResultType != "success" {
		t.Fatalf("map envelope: err=%v result=%q, want success", err, res.ResultType)
	}
}

// validateContractPayload table: proto absent/empty/null = empty-object
// semantics, structured issue text, and invalid-instance classification.
func TestValidateContractPayload(t *testing.T) {
	schema := []byte(verbatimSchema)
	cases := []struct {
		name    string
		payload string
		want    []string // nil = valid; substrings expected in issues otherwise
	}{
		{"valid object", `{"verdict":"approved"}`, nil},
		{"null payload is empty object", "null", []string{"required"}},
		{"missing required", `{}`, []string{"required"}},
		{"wrong type", `{"verdict": 5}`, []string{"verdict"}},
		{"instance not object", `[1,2]`, []string{"type"}},
		{"undecodable bytes", `{"a":`, []string{"payload does not decode to a JSON object"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := validateContractPayload(json.RawMessage(tc.payload), schema)
			if tc.want == nil {
				if len(issues) != 0 {
					t.Fatalf("issues = %v, want none", issues)
				}
				return
			}
			joined := strings.Join(issues, "; ")
			for _, fragment := range tc.want {
				if !strings.Contains(joined, fragment) {
					t.Errorf("issues %q missing fragment %q", joined, fragment)
				}
			}
		})
	}
	if issues := validateContractPayload(json.RawMessage(`{"x":1}`), nil); len(issues) != 0 {
		t.Errorf("nil schema should validate anything, got %v", issues)
	}
}

// normalizedFinalizePayload: only the degenerate shapes rewrite to {}; the
// whitespace-trimmed original bytes are otherwise preserved.
func TestNormalizedFinalizePayload(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "{}"},
		{"   ", "{}"},
		{"null", "{}"},
		{"  null  ", "{}"},
		{`{}`, `{}`},
		{`  {"b":1,"a":2}  `, `{"b":1,"a":2}`},
		{`{"a":1}`, `{"a":1}`},
	}
	for _, tc := range cases {
		if got := string(normalizedFinalizePayload(json.RawMessage(tc.in))); got != tc.want {
			t.Errorf("normalizedFinalizePayload(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// checkContractFinalize: comment-missing class only when the payload is
// otherwise clean; combined violations classify invalid_payload.
func TestCheckContractFinalizeClassification(t *testing.T) {
	s := contractTestState([]string{"approved"},
		contractTestContract(t, "approved", verbatimSchema, true, false))
	if r := s.checkContractFinalize("approved", json.RawMessage(`{"verdict":"ok"}`), ""); r == nil || r.kind != "comment_missing" {
		t.Errorf("clean payload + no comment: %+v, want comment_missing", r)
	}
	if r := s.checkContractFinalize("approved", json.RawMessage(`{"verdict":9}`), ""); r == nil || r.kind != "invalid_payload" {
		t.Errorf("bad payload + no comment: %+v, want invalid_payload", r)
	}
	if r := s.checkContractFinalize("approved", json.RawMessage(`{"verdict":"ok"}`), "note"); r != nil {
		t.Errorf("clean payload + comment: %+v, want nil", r)
	}
}
