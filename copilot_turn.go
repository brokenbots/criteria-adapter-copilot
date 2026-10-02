// copilot_turn.go — per-Execute turn execution: state machine, event handling, and request config.

package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

const maxFinalizeAttempts = 3

// errSessionLoss is the sentinel for a developer turn that died without a
// chance to submit an outcome: the CLI reported a turn-terminal session event
// (session.error, session.shutdown) that ends the agentic loop, so no
// session.idle — and therefore no outcome — will ever arrive for it (KB-42).
// Waiting it out previously fell through to the CRI-274 silence watchdog and
// burned three whole-call stall attempts (forced child restarts included)
// before surfacing anything. The sentinel is deliberately NOT
// provider-stall-typed: a reported session error means the CLI is alive and
// its session object exists, so executeTurn must return the error without a
// forced transport restart or a re-send into the dead turn (CRI-272 policy).
// Recovery belongs to the engine's bounded step retry: the Execute error is
// what the engine records, and the last agent message rides along as
// evidence.
var errSessionLoss = errors.New("copilot developer turn lost before submitting an outcome")

// isSessionLossError reports whether err is a turn-terminal session loss
// (KB-42).
func isSessionLossError(err error) bool {
	return errors.Is(err, errSessionLoss)
}

// maxEvidenceLen bounds the last-agent-message evidence attached to turn
// failures and the errors they surface. Long enough for an operator to
// recognize the workstream message that ended the run, short enough to keep
// error payloads reviewable.
const maxEvidenceLen = 2000

// evidenceSnippet truncates model-produced text for failure evidence (KB-42).
// A rune-boundary cut keeps multi-byte content from splitting mid-character.
// Empty input yields an empty snippet, so callers can decide to omit the
// evidence entirely.
func evidenceSnippet(content string) string {
	if content == "" {
		return ""
	}
	if len(content) <= maxEvidenceLen {
		return content
	}
	cut := maxEvidenceLen
	for cut > 0 && !utf8.RuneStart(content[cut]) {
		cut--
	}
	return content[:cut] + "…[truncated]"
}

// turnState tracks per-Execute state: final content, turn count, and channels
// for coordinating the event handler goroutine with the wait loop.
type turnState struct {
	finalContent string
	// lastMessageID tracks the assistant message id the evidence text was
	// last accumulated from (KB-69): a delta stream with a new id starts a
	// new turn, so the previous turn's evidence must not bleed into it.
	lastMessageID  string
	assistantTurns int
	turnDone       chan struct{}
	errCh          chan error
	maxTurns       int

	// Turn-diagnostic counters for the missing-finalize failure path
	// (KB-69): how many idle/error signals the wait loop consumed from the
	// session event fanout, and how many buffered signals the inter-turn
	// drains dropped. All four are written only from the goroutine running
	// awaitOutcome/waitTurnSignal/drainStaleSignals, and failExhausted reads
	// them on that same goroutine, so no extra synchronization is needed.
	idleSignals  int
	errSignals   int
	drainedIdles int
	drainedErrs  int
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
// appropriate per-event-type methods on ts. Every event is recorded as
// provider-side activity first (CRI-274): a streaming response produces
// events continuously, so event flow is what keeps the inter-event watchdog
// disarmed.
func (ts *turnState) handleEvent(s *sessionState, sink adapterhost.ExecuteEventSender) func(copilot.SessionEvent) {
	return func(event copilot.SessionEvent) {
		s.markActivity()
		switch d := event.Data.(type) {
		case *copilot.AssistantMessageDeltaData:
			ts.handleAssistantDelta(sink, event.Type(), d)
		case *copilot.AssistantMessageData:
			// KB-57: record raw commands for bash tool requests BEFORE the
			// CLI's permission gate fires for these ToolCallIDs - the
			// permission request payload itself only carries identifiers.
			for _, tr := range d.ToolRequests {
				if tr.Name == "bash" && tr.Arguments != nil {
					if m, ok := tr.Arguments.(map[string]any); ok {
						if cmd, ok := m["command"].(string); ok && cmd != "" {
							s.recordToolCommand(tr.ToolCallID, cmd)
						}
					}
				}
			}
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
		case *copilot.ToolExecutionStartData:
			// A native CLI tool execution (shell, read, ...) can run far
			// longer than the silence window with only its start event
			// emitted. Hold the watchdog gate for the run, keyed by tool call
			// ID, so a long-but-healthy tool is not misread as a stall
			// (CRI-274); the gate is bounded by watchdogGateWindow and closed
			// on the matching completion event.
			s.openToolGate(d.ToolCallID)
			// KB-57: record the raw command for native tool calls so the
			// permission request the CLI sends for this call can carry a
			// full-text fingerprint (the SDK request itself carries only
			// parsed identifiers). Only shell-ish calls carry a command.
			if cmd := toolCommandFromStartArgs(d); cmd != "" {
				s.recordToolCommand(d.ToolCallID, cmd)
			}
		case *copilot.ToolExecutionCompleteData:
			s.closeToolGate(d.ToolCallID)
			s.forgetToolCommand(d.ToolCallID)
		case *copilot.SessionIdleData:
			select {
			case ts.turnDone <- struct{}{}:
			default:
			}
		case *copilot.SessionErrorData:
			// KB-42: a reported session error ends the agentic turn (SDK
			// SendAndWait treats it the same way). Forward the diagnostic as
			// evidence, then end the wait instead of letting it ride the
			// silence watchdog through three forced stall-recovery cycles.
			ts.handleSessionError(s, sink, event.Type(), d)
		case *copilot.SessionShutdownData:
			// KB-42: the CLI session is being torn down — an idle for the
			// in-flight turn will never arrive. Same treatment: evidence +
			// immediate turn-terminal error.
			ts.handleSessionShutdown(s, sink, event.Type(), d)
		case *copilot.AbortData:
			// Evidence only, not turn-terminal: the SDK follows an abort
			// with session.idle (Aborted=true), which the finalize loop
			// already handles via the reprompt path. Making the abort itself
			// terminal would break that recovery. If the idle never comes,
			// the silence watchdog bounds the wait (CRI-274).
			ts.sendErr(sink.Send(adapterEvent("turn.aborted", map[string]any{
				"reason":             string(d.Reason),
				"last_agent_message": evidenceSnippet(ts.finalContent),
				"event_type":         string(event.Type()),
			})))
		}
	}
}

// handleSessionError processes a session.error event (KB-42). The error is
// forwarded as a diagnostic event carrying the last agent message as
// evidence; when the turn cannot continue (every errorType except an
// auto-switch-eligible rate limit) the wait is ended immediately with a
// session-loss error so the Execute fails loudly instead of stalling.
func (ts *turnState) handleSessionError(s *sessionState, sink adapterhost.ExecuteEventSender, eventType copilot.SessionEventType, d *copilot.SessionErrorData) {
	payload := map[string]any{
		"error_type":         d.ErrorType,
		"message":            redactSecrets(d.Message, s.heldSecrets),
		"last_agent_message": evidenceSnippet(ts.finalContent),
		"event_type":         string(eventType),
	}
	if d.ErrorCode != nil && *d.ErrorCode != "" {
		payload["error_code"] = *d.ErrorCode
	}
	if d.StatusCode != nil {
		payload["status_code"] = *d.StatusCode
	}
	autoSwitch := d.EligibleForAutoSwitch != nil && *d.EligibleForAutoSwitch
	if autoSwitch {
		payload["eligible_for_auto_switch"] = true
	}
	ts.sendErr(sink.Send(adapterEvent("session.error", payload)))
	if autoSwitch {
		// SDK contract: the runtime follows this error with an
		// auto_mode_switch.requested event and continues the agentic loop,
		// so an idle for this turn still arrives. Not turn-terminal.
		return
	}
	ts.sendErr(sessionLossError(s, fmt.Sprintf("session error (%s): %s", d.ErrorType, d.Message), ts.finalContent))
}

// handleSessionShutdown processes a session.shutdown event (KB-42): the CLI
// session is gone (ShutdownType "routine" or "error"), so the in-flight turn
// can never idle. Forward the diagnostic and end the wait immediately.
func (ts *turnState) handleSessionShutdown(s *sessionState, sink adapterhost.ExecuteEventSender, eventType copilot.SessionEventType, d *copilot.SessionShutdownData) {
	detail := ""
	if d.ErrorReason != nil {
		detail = *d.ErrorReason
	}
	ts.sendErr(sink.Send(adapterEvent("session.shutdown", map[string]any{
		"shutdown_type":      string(d.ShutdownType),
		"error_reason":       redactSecrets(detail, s.heldSecrets),
		"last_agent_message": evidenceSnippet(ts.finalContent),
		"event_type":         string(eventType),
	})))
	label := fmt.Sprintf("session shutdown (%s)", d.ShutdownType)
	if detail != "" {
		label += ": " + detail
	}
	ts.sendErr(sessionLossError(s, label, ts.finalContent))
}

// sessionLossError builds the turn-terminal session-loss error: the reason
// text, the truncated last agent message as evidence, and the sentinel the
// classification checks.
func sessionLossError(s *sessionState, cause string, finalContent string) error {
	msg := "copilot: developer turn ended without submitting the outcome: " + redactSecrets(cause, s.heldSecrets)
	if ev := evidenceSnippet(finalContent); ev != "" {
		msg += "; last agent message: " + ev
	}
	return fmt.Errorf("%s: %w", msg, errSessionLoss)
}

// handleAssistantDelta forwards a streaming delta event and accumulates the
// turn's evidence text (KB-69). On a streaming turn the model prose arrives
// only as assistant.message_delta chunks; the complete assistant.message
// events a requested tool produces carry EMPTY content (verified against the
// CLI's event pipeline), so dropping the deltas leaves failure evidence
// empty (observed runs d3f8c225/9b7495f1: last_agent_message="").
func (ts *turnState) handleAssistantDelta(sink adapterhost.ExecuteEventSender, eventType copilot.SessionEventType, d *copilot.AssistantMessageDeltaData) {
	if d.DeltaContent == "" {
		return
	}
	if d.MessageID != ts.lastMessageID {
		// A new message id starts a new turn's evidence. The previous
		// message has ended (its complete content, when any, already
		// replaced the accumulation), so it must not bleed into this one.
		ts.finalContent = ""
		ts.lastMessageID = d.MessageID
	}
	ts.finalContent += d.DeltaContent
	ts.sendErr(sink.Send(adapterEvent("agent.message", map[string]any{
		"message_id": d.MessageID,
		"delta":      d.DeltaContent,
		"event_type": string(eventType),
	})))
}

// handleAssistantMessage processes a complete assistant turn, forwarding
// content and tool invocations, then enforcing the max_turns limit.
func (ts *turnState) handleAssistantMessage(sink adapterhost.ExecuteEventSender, eventType copilot.SessionEventType, d *copilot.AssistantMessageData) {
	// Only a complete message with non-empty content replaces the evidence
	// (KB-69): the CLI emits a complete assistant.message with empty content
	// for every requested tool before the final content-bearing message, and
	// overwriting on those erased the delta text already accumulated.
	if d.Content != "" {
		ts.finalContent = d.Content
		ts.lastMessageID = d.MessageID
	}
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
// a reprompt-exhaustion failure, a context cancellation, an error, or a
// turn-terminal session loss (KB-42). It runs up to maxFinalizeAttempts
// (1 initial + 2 reprompts) before returning failure. The wait itself is
// watchdog-supervised (CRI-274): waitTurnSignal fails the call with a stall
// error when the provider stream goes silent, which executeTurn translates
// into a whole-call retry.
func (ts *turnState) awaitOutcome(ctx context.Context, s *sessionState, sink adapterhost.ExecuteEventSender) error {
	for attempt := 1; attempt <= maxFinalizeAttempts; attempt++ {
		signal, err := ts.waitTurnSignal(ctx, s)
		switch signal {
		case turnSignalCtx:
			return err
		case turnSignalErr:
			if errors.Is(err, errMaxTurnsReached) {
				return ts.handleMaxTurnsReached(s, sink)
			}
			// A session loss can race a completed turn: the CLI may report
			// the error event and only then deliver the idle that closes the
			// turn. If the idle is already buffered, the turn survived the
			// error — prefer it and let the finalize loop decide (KB-42).
			select {
			case <-ts.turnDone:
				// The idle was buffered before the error was consumed:
				// the turn survived the error (KB-42).
				ts.idleSignals++
				ts.drainStaleSignals(s)
				done, idleErr := ts.handleIdleTurn(ctx, s, sink, attempt)
				if done || idleErr != nil {
					return idleErr
				}
			default:
				// A session loss that trails a submitted outcome must not
				// fail the step: the deliverable was already produced, the
				// same way the stall path returns a finalized outcome
				// directly instead of retrying (CRI-274, KB-42).
				if errors.Is(err, errSessionLoss) {
					s.mu.Lock()
					outcome := s.finalizedOutcome
					reason := s.finalizedReason
					s.mu.Unlock()
					if outcome != "" {
						return sink.Send(resultEvent(outcome, reason, s.heldSecrets...))
					}
				}
				return err
			}
		case turnSignalIdle:
			// The completed turn may leave a late turn-terminal signal
			// buffered behind its idle (e.g. a session error that trailed
			// it). Drop everything buffered BEFORE the reprompt send: at
			// this point it all belongs to the turn that just ended, and a
			// stale session error must not kill the next reprompt wait
			// (KB-42). Signals the reprompt itself produces are buffered
			// after this drain and stay intact.
			ts.drainStaleSignals(s)
			done, idleErr := ts.handleIdleTurn(ctx, s, sink, attempt)
			if done || idleErr != nil {
				return idleErr
			}
		case turnSignalStall:
			return err
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
		return true, sink.Send(resultEvent(outcome, reason, s.heldSecrets...))
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
	if _, err := s.sendWithRetry(ctx, &copilot.MessageOptions{Prompt: msg}); err != nil {
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
//   - last_agent_message: truncated final model output as evidence (KB-42)
//
// The failure result reason carries the same evidence: the engine records it
// as the step failure reason, which is how a run that failed on a
// missing-outcome turn keeps its last agent message visible (KB-42).
func (ts *turnState) failExhausted(s *sessionState, sink adapterhost.ExecuteEventSender) error {
	s.mu.Lock()
	attempts := s.finalizeAttempts
	kind := s.finalizeFailureKind
	allowedList := sortedAllowedOutcomes(s.activeAllowedOutcomes)
	secrets := s.heldSecrets
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
	// ts.finalContent is written by the event-handler goroutine; every path
	// into failExhausted has received turnDone from that goroutine (the
	// channel send happens after the last assistant message was recorded),
	// so the read is synchronized (KB-42).
	evidence := evidenceSnippet(ts.finalContent)
	// KB-69: turn-level fail-path diagnostics. The outcome counts alone
	// cannot distinguish a finalize loop whose reprompts carried real
	// provider turns from one that consumed stale or duplicated idle
	// signals instantly (observed runs d3f8c225/9b7495f1: attempts=0,
	// zero reprompt turns). Report the loop's signal accounting.
	_ = sink.Send(adapterEvent("turn.finalize_exhausted", map[string]any{
		"attempts":             attempts,
		"assistant_turns":      ts.assistantTurns,
		"idle_signals":         ts.idleSignals,
		"err_signals":          ts.errSignals,
		"drained_idle_signals": ts.drainedIdles,
		"drained_err_signals":  ts.drainedErrs,
		"leftover_idle_signal": ts.probeIdleSignal(),
		"leftover_err_signal":  ts.probeErrSignal(),
		"finalize_kind":        kind,
		"last_agent_message":   evidence,
	}))
	_ = sink.Send(adapterEvent("outcome.failure", map[string]any{
		"reason":             reason,
		"kind":               kind,
		"allowed_outcomes":   allowedAny,
		"attempts":           attempts,
		"last_agent_message": evidence,
	}))
	resultReason := "step failed: no outcome submitted (" + reason + ")"
	if evidence != "" {
		resultReason += "; last agent message: " + evidence
	}
	return sink.Send(resultEvent("failure", resultReason, secrets...))
}

// probeIdleSignal reports (and consumes) whether an idle signal is still
// buffered in turnDone. Only called after the finalize loop has decided to
// fail, where consuming a leftover token cannot change the result; it keeps
// the drain-state report from racing the loop's bookkeeping.
func (ts *turnState) probeIdleSignal() bool {
	select {
	case <-ts.turnDone:
		return true
	default:
		return false
	}
}

// probeErrSignal is probeIdleSignal for the error channel.
func (ts *turnState) probeErrSignal() bool {
	select {
	case <-ts.errCh:
		return true
	default:
		return false
	}
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

	// CRI-288: wrap the sink so every host-visible forward (handler forwards,
	// permission bridge, finalize/reprompt diagnostics, result events) stamps
	// the liveness clock, then run the liveness ticker for the turn. The
	// wrapper must be in place before beginExecution stores s.sink so the
	// permission bridge tracks forwards too. LIFO defers stop the ticker
	// before cleanup closes the active-execution channel.
	sink = forwardTrackingSink{inner: sink, s: s}

	cleanup := s.beginExecution(sink)
	defer cleanup()

	stopLiveness := s.startLivenessTicker(ctx, sink)
	defer stopLiveness()

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
	// Route the handler through the session's event fanout (CRI-272): if the
	// CLI child dies mid-turn and the session is re-opened, the swapped-in SDK
	// session re-registers the same fanout and this turn keeps its events.
	// The handler also feeds the CRI-274 watchdog: every event marks activity,
	// and native tool executions hold the watchdog gate.
	unsubscribe := s.subscribeEvents(state.handleEvent(s, sink))
	defer unsubscribe()

	// Snapshot the SDK session via the guarded accessor for the model/effort
	// apply calls below; a concurrent reopenSession may swap the session
	// mid-setup, and the bare field read would be a data race (CRI-272).
	sess := s.currentSession()
	restoreEffort, err := applyRequestEffort(ctx, s, sess, req.GetInput())
	if err != nil {
		return err
	}
	defer restoreEffort()

	if err := applyRequestModel(ctx, sess, req.GetInput()); err != nil {
		return err
	}

	// The whole provider call (send + await outcome) is watchdog-supervised
	// and retried by executeTurn: a hung send or a stream that opens and goes
	// silent is failed and re-sent per the CRI-272 backoff path instead of
	// wedging the turn (CRI-274).
	return state.executeTurn(ctx, s, &copilot.MessageOptions{Prompt: prompt}, sink)
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

	// CRI-288: turn time starts here — the liveness ticker measures
	// host-visible silence from this stamp until the first forwarded event.
	s.lastForwardNs.Store(watchdogNow().UnixNano())

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

// toolCommandFromStartArgs extracts the raw shell command from a native
// ToolExecutionStart event's arguments (KB-57). The CLI delivers arguments as
// a decoded JSON object; the shell tool carries its command under the
// "command" key. Returns "" for every other tool or a command-less call so
// non-shell tools never pollute the fingerprint map.
func toolCommandFromStartArgs(d *copilot.ToolExecutionStartData) string {
	if d == nil || d.ToolName != "shell" || d.Arguments == nil {
		return ""
	}
	m, ok := d.Arguments.(map[string]any)
	if !ok {
		return ""
	}
	if raw, ok := m["command"].(string); ok {
		return raw
	}
	// Some CLI versions nest the payload under "args" or "input"; unwrap one
	// level so the fingerprint survives those shapes too.
	if inner, ok := m["args"].(map[string]any); ok {
		if raw, ok := inner["command"].(string); ok {
			return raw
		}
	}
	return ""
}
