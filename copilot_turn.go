// copilot_turn.go — per-Execute turn execution: state machine, event handling, and request config.

package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

const maxFinalizeAttempts = 3

// turnState tracks per-Execute state: final content, turn count, and channels
// for coordinating the event handler goroutine with the wait loop.
type turnState struct {
	finalContent   string
	assistantTurns int
	turnDone       chan struct{}
	errCh          chan error
	maxTurns       int
}

func newTurnState(maxTurns int) *turnState {
	return &turnState{
		turnDone: make(chan struct{}, 1),
		errCh:    make(chan error, 1),
		maxTurns: maxTurns,
	}
}

// sendErr non-blockingly forwards a non-nil error to the error channel.
func (ts *turnState) sendErr(err error) {
	if err == nil {
		return
	}
	select {
	case ts.errCh <- err:
	default:
	}
}

// handleEvent returns a SessionEventHandler that dispatches SDK events to the
// appropriate per-event-type methods on ts.
func (ts *turnState) handleEvent(sink adapterhost.ExecuteEventSender) func(copilot.SessionEvent) {
	return func(event copilot.SessionEvent) {
		switch d := event.Data.(type) {
		case *copilot.AssistantMessageDeltaData:
			ts.handleAssistantDelta(sink, event.Type(), d)
		case *copilot.AssistantMessageData:
			ts.handleAssistantMessage(sink, event.Type(), d)
		case *copilot.ExternalToolRequestedData:
			ts.sendErr(sink.Send(adapterEvent("tool.invocation", map[string]any{
				"request_id":   d.RequestID,
				"tool_call_id": d.ToolCallID,
				"name":         d.ToolName,
				"arguments":    stringifyAny(d.Arguments),
				"event_type":   string(event.Type()),
			})))
		case *copilot.ExternalToolCompletedData:
			ts.sendErr(sink.Send(adapterEvent("tool.result", map[string]any{
				"request_id": d.RequestID,
				"event_type": string(event.Type()),
			})))
		case *copilot.SessionIdleData:
			select {
			case ts.turnDone <- struct{}{}:
			default:
			}
		}
	}
}

// handleAssistantDelta forwards a streaming delta event.
func (ts *turnState) handleAssistantDelta(sink adapterhost.ExecuteEventSender, eventType copilot.SessionEventType, d *copilot.AssistantMessageDeltaData) {
	if d.DeltaContent == "" {
		return
	}
	ts.sendErr(sink.Send(adapterEvent("agent.message", map[string]any{
		"message_id": d.MessageID,
		"delta":      d.DeltaContent,
		"event_type": string(eventType),
	})))
}

// handleAssistantMessage processes a complete assistant turn, forwarding
// content and tool invocations, then enforcing the max_turns limit.
func (ts *turnState) handleAssistantMessage(sink adapterhost.ExecuteEventSender, eventType copilot.SessionEventType, d *copilot.AssistantMessageData) {
	ts.finalContent = d.Content
	ts.sendErr(sink.Send(adapterEvent("agent.message", map[string]any{
		"message_id": d.MessageID,
		"content":    d.Content,
		"event_type": string(eventType),
	})))
	for _, tr := range d.ToolRequests {
		ts.sendErr(sink.Send(adapterEvent("tool.invocation", map[string]any{
			"tool_call_id": tr.ToolCallID,
			"name":         tr.Name,
			"arguments":    stringifyAny(tr.Arguments),
			"event_type":   string(eventType),
		})))
	}
	ts.assistantTurns++
	if ts.maxTurns > 0 && ts.assistantTurns >= ts.maxTurns {
		ts.sendErr(sink.Send(adapterEvent("limit.reached", map[string]any{
			"max_turns": strconv.Itoa(ts.maxTurns),
		})))
		ts.sendErr(errMaxTurnsReached)
	}
}

// awaitOutcome blocks until the session idles with a valid finalized outcome,
// a reprompt-exhaustion failure, a context cancellation, or an error. It runs
// up to maxFinalizeAttempts (1 initial + 2 reprompts) before returning failure.
func (ts *turnState) awaitOutcome(ctx context.Context, s *sessionState, sink adapterhost.ExecuteEventSender) error {
	for attempt := 1; attempt <= maxFinalizeAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ts.errCh:
			if errors.Is(err, errMaxTurnsReached) {
				return ts.handleMaxTurnsReached(s, sink)
			}
			return err
		case <-ts.turnDone:
			done, err := ts.handleIdleTurn(ctx, s, sink, attempt)
			if done || err != nil {
				return err
			}
			// Not done: reprompt was sent; loop and wait for the next SessionIdle.
		}
	}
	return ts.failExhausted(s, sink)
}

// handleIdleTurn processes a SessionIdle event during the awaitOutcome loop.
// Returns (done=true, err) when execution should end; (done=false, nil) when a
// reprompt was sent and the loop should continue to the next turn.
func (ts *turnState) handleIdleTurn(ctx context.Context, s *sessionState, sink adapterhost.ExecuteEventSender, attempt int) (done bool, err error) {
	s.mu.Lock()
	outcome := s.finalizedOutcome
	reason := s.finalizedReason
	s.mu.Unlock()

	if outcome != "" {
		return true, sink.Send(resultEvent(outcome, reason))
	}

	// No valid finalize this turn. Fail if exhausted.
	if attempt == maxFinalizeAttempts {
		return true, ts.failExhausted(s, sink)
	}
	// Short-circuit when no outcomes are declared: the model can never succeed
	// regardless of how many reprompts we send. Fail immediately with a clear
	// reason so the operator can fix the misconfigured step. Unconditionally
	// override the kind so a prior "invalid_outcome" from a model that called
	// submit_outcome on an empty set doesn't mask the real root cause.
	s.mu.Lock()
	noOutcomes := len(s.activeAllowedOutcomes) == 0
	if noOutcomes {
		s.finalizeFailureKind = "no_outcomes"
	}
	s.mu.Unlock()
	if noOutcomes {
		return true, ts.failExhausted(s, sink)
	}
	return false, ts.reprompt(ctx, s)
}

// reprompt sends a corrective message instructing the model to call submit_outcome.
func (ts *turnState) reprompt(ctx context.Context, s *sessionState) error {
	s.mu.Lock()
	allowedList := sortedAllowedOutcomes(s.activeAllowedOutcomes)
	s.mu.Unlock()

	list := strings.Join(allowedList, ", ")
	msg := fmt.Sprintf(
		"You must call the `submit_outcome` tool with one of the allowed outcomes: %s. Do not return a final answer without calling the tool. Allowed outcomes: %s. Failure to call the tool will fail the step.",
		list, list,
	)
	if _, err := s.session.Send(ctx, &copilot.MessageOptions{Prompt: msg}); err != nil {
		return fmt.Errorf("copilot: reprompt: %w", err)
	}
	return nil
}

// failExhausted emits a structured failure event and returns a failure result.
// The event payload includes:
//   - reason: human-readable category ("missing finalize", "invalid outcome", "duplicate finalize", "step has no declared outcomes")
//   - kind:   machine-readable category ("missing", "invalid_outcome", "duplicate", "no_outcomes")
//   - allowed_outcomes: sorted list of the step's declared outcomes (for operator alerting)
//   - attempts: how many tool-call attempts were made
func (ts *turnState) failExhausted(s *sessionState, sink adapterhost.ExecuteEventSender) error {
	s.mu.Lock()
	attempts := s.finalizeAttempts
	kind := s.finalizeFailureKind
	allowedList := sortedAllowedOutcomes(s.activeAllowedOutcomes)
	s.mu.Unlock()

	if kind == "" {
		kind = "missing"
	}
	reasonLabels := map[string]string{
		"missing":         "missing finalize",
		"invalid_outcome": "invalid outcome",
		"duplicate":       "duplicate finalize",
		"no_outcomes":     "step has no declared outcomes",
	}
	reason, ok := reasonLabels[kind]
	if !ok {
		reason = "missing finalize"
	}
	// Convert []string to []any for structpb.NewStruct compatibility.
	allowedAny := make([]any, len(allowedList))
	for i, v := range allowedList {
		allowedAny[i] = v
	}
	_ = sink.Send(adapterEvent("outcome.failure", map[string]any{
		"reason":           reason,
		"kind":             kind,
		"allowed_outcomes": allowedAny,
		"attempts":         attempts,
	}))
	return sink.Send(resultEvent("failure", ""))
}

// handleMaxTurnsReached returns failure unless "needs_review" is in the
// allowed set, in which case it preserves the historical max-turns behavior.
func (ts *turnState) handleMaxTurnsReached(s *sessionState, sink adapterhost.ExecuteEventSender) error {
	s.mu.Lock()
	_, needsReviewAllowed := s.activeAllowedOutcomes["needs_review"]
	s.mu.Unlock()
	if needsReviewAllowed {
		return sink.Send(resultEvent("needs_review", ""))
	}
	return sink.Send(resultEvent("failure", ""))
}

func (p *copilotAdapter) Execute(ctx context.Context, req *v2.ExecuteRequest, sink adapterhost.ExecuteEventSender) error {
	s, prompt, maxTurns, err := p.prepareExecute(req)
	if err != nil {
		return err
	}

	s.execMu.Lock()
	defer s.execMu.Unlock()

	cleanup := s.beginExecution(sink)
	defer cleanup()

	// Populate allowed set before the prompt is sent so the tool handler can
	// validate on the very first turn.
	allowed := req.GetAllowedOutcomes()
	s.mu.Lock()
	s.activeAllowedOutcomes = make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		s.activeAllowedOutcomes[name] = struct{}{}
	}
	s.mu.Unlock()

	// Prepend the allowed-outcomes preamble so the model knows what to call.
	if len(allowed) > 0 {
		outcomeList := strings.Join(allowed, ", ") // already sorted ascending by W14 loader
		prompt = fmt.Sprintf(
			"You must finalize the outcome for this step by calling the `submit_outcome` tool exactly once before ending the turn. The allowed outcomes are: %s. If you do not call the tool with a valid outcome, the step will fail.\n\n%s",
			outcomeList, prompt,
		)
	}

	state := newTurnState(maxTurns)
	unsubscribe := s.session.On(state.handleEvent(sink))
	defer unsubscribe()

	restoreEffort, err := applyRequestEffort(ctx, s, s.session, req.GetInput())
	if err != nil {
		return err
	}
	defer restoreEffort()

	if err := applyRequestModel(ctx, s.session, req.GetInput()); err != nil {
		return err
	}

	if _, err := s.session.Send(ctx, &copilot.MessageOptions{Prompt: prompt}); err != nil {
		return fmt.Errorf("copilot: send prompt: %w", err)
	}

	return state.awaitOutcome(ctx, s, sink)
}

// prepareExecute validates the request and returns the session state, prompt,
// and max_turns limit. Returns an error when any required field is missing or
// the session is unknown.
func (p *copilotAdapter) prepareExecute(req *v2.ExecuteRequest) (s *sessionState, prompt string, maxTurns int, err error) {
	s = p.getSession(req.GetSessionId())
	if s == nil {
		return nil, "", 0, fmt.Errorf("copilot: unknown session %q", req.GetSessionId())
	}

	prompt = strings.TrimSpace(req.GetInput()["prompt"])
	if prompt == "" {
		return nil, "", 0, fmt.Errorf("copilot: config.prompt is required")
	}

	if raw := strings.TrimSpace(req.GetInput()["max_turns"]); raw != "" {
		n, parseErr := strconv.Atoi(raw)
		if parseErr != nil || n < 0 {
			return nil, "", 0, fmt.Errorf("copilot: invalid max_turns %q", raw)
		}
		maxTurns = n
	}
	return s, prompt, maxTurns, nil
}

// beginExecution marks the session active and wires up the event sink.
// The returned cleanup function must be deferred by the caller.
func (s *sessionState) beginExecution(sink adapterhost.ExecuteEventSender) func() {
	execDone := make(chan struct{})
	s.mu.Lock()
	s.active = true
	s.activeCh = execDone
	s.sink = sink

	// W15: reset per-execute finalize state. activeAllowedOutcomes is set by
	// Execute *after* this returns; do not reset it here.
	s.finalizedOutcome = ""
	s.finalizedReason = ""
	s.finalizeAttempts = 0
	s.finalizeFailureKind = ""
	s.mu.Unlock()

	return func() {
		s.mu.Lock()
		s.active = false
		s.sink = nil
		if s.activeCh != nil {
			close(s.activeCh)
			s.activeCh = nil
		}
		s.mu.Unlock()
	}
}
