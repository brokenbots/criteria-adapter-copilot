// copilot_turn.go — per-Execute turn execution: state machine, event handling, and request config.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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

// recordedFinalize captures an accepted submit_outcome whose terminal
// ExecuteEvent must be emitted outside the turn's idle path: a provider stall
// that struck after the finalize (CRI-274) or a session loss that trailed the
// submit (KB-42). Emission mirrors handleIdleTurn exactly — an
// outcome.recovered event precedes the terminal result for a repair finalize,
// and contract mode forwards the schema-validated payload and comment
// verbatim (through the secret-hygiene path) instead of the legacy
// session-state outputs assembly.
type recordedFinalize struct {
	outcome       string
	reason        string
	contractMode  bool
	inRepair      bool
	repairAttempt uint32
	payload       json.RawMessage
	comment       string
	secrets       []string
}

// snapshotRecordedFinalize captures the recorded finalize state under s.mu.
// ok is false when no finalize was accepted, so the caller falls back to its
// own error handling.
func (s *sessionState) snapshotRecordedFinalize() (recordedFinalize, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalizedOutcome == "" {
		return recordedFinalize{}, false
	}
	return recordedFinalize{
		outcome:       s.finalizedOutcome,
		reason:        s.finalizedReason,
		contractMode:  s.contractMode,
		inRepair:      s.inRepairMode,
		repairAttempt: s.inRepairAttempt,
		payload:       append(json.RawMessage(nil), s.finalizedPayload...),
		comment:       s.finalizedComment,
		secrets:       s.heldSecrets,
	}, true
}

// send emits the captured terminal result (and the outcome.recovered event
// ahead of it for a repair finalize).
func (f recordedFinalize) send(sink adapterhost.ExecuteEventSender) error {
	if f.inRepair {
		// KB-47: a rejected finalize that the model repaired and resubmitted
		// successfully — surface the recovery once, before the terminal result.
		_ = sink.Send(adapterEvent("outcome.recovered", map[string]any{
			"outcome":        f.outcome,
			"repair_attempt": f.repairAttempt,
		}))
	}
	if f.contractMode {
		// Contract mode: outputs_json is exactly the model-submitted,
		// schema-validated payload — never a session-state assembly — and the
		// finalize comment rides ExecuteResult.comment. Both pass through the
		// secret-hygiene path.
		return sink.Send(contractResultEvent(f.outcome, f.payload, f.comment, f.secrets...))
	}
	return sink.Send(resultEvent(f.outcome, f.reason, f.secrets...))
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

	// repromptReplies records the evidence snippet of each reprompted turn's
	// own reply text (KB-71): repromptReplies[i] is the model's reply to
	// corrective reprompt i+1, "" when that turn carried no assistant
	// message. The initial prompt's reply is NOT here (it is
	// last_agent_message). Captured per idle dispatch so an exhausted loop
	// distinguishes "the model answered prose to every reprompt" (deliberate
	// refusal; charter or convergence problem) from "the reprompted turn
	// sent no message" (signal-level failure, KB-69). lastCapturedReplyID is
	// the assistant-message id whose content the last capture drew from, so
	// a turn without a fresh message cannot leak an earlier turn's text.
	// Same goroutine discipline as the counters below: written from the
	// awaitOutcome goroutine, read by failExhausted on that same goroutine.
	repromptReplies     []string
	lastCapturedReplyID string

	// execPrompt holds the composition inputs for this Execute's prompt: the
	// step prompt, allowed outcomes, contracts, and an optional rejection.
	// The prompt is composed at send time — not when Execute populates
	// session state — because a repair Execute must observe the session
	// generation AFTER sendWithRetry's lazy reopen has had its chance to
	// re-create the SDK session (KB-47): a fresh create bumps createdEpoch
	// and degrades the repair to the full re-execute prompt.
	execPrompt executePrompt

	// announcedTools tracks the tool_call_ids already rendered as a
	// tool.invocation row for this Execute (KB-263). The CLI announces a
	// submit via the complete assistant.message's ToolRequests AND echoes it
	// as external_tool.requested when the adapter handles the call — two
	// events for one call that previously rendered as two identical-looking
	// tool.invocation rows (seq 1196/1197, call_e4aoan72). The
	// assistant-message row is the canonical invocation render; the echo of
	// an already-announced call degrades to a thin, unmistakable
	// tool.requested row carrying the request_id (the anchor tool.result
	// correlates with). Guarded by announcedMu because events arrive from
	// the SDK's goroutine, not the awaitOutcome goroutine.
	announcedTools map[string]struct{}
	announcedMu    sync.Mutex
}

func newTurnState(maxTurns int) *turnState {
	return &turnState{
		turnDone:       make(chan struct{}, 1),
		errCh:          make(chan error, 1),
		maxTurns:       maxTurns,
		announcedTools: make(map[string]struct{}),
	}
}

// announceToolCall marks a tool_call_id as rendered as a tool.invocation row
// and reports whether this Execute had already announced it (KB-263).
func (ts *turnState) announceToolCall(toolCallID string) bool {
	ts.announcedMu.Lock()
	defer ts.announcedMu.Unlock()
	if _, seen := ts.announcedTools[toolCallID]; seen {
		return true
	}
	ts.announcedTools[toolCallID] = struct{}{}
	return false
}

// executePrompt carries the composition inputs for an Execute's prompt. The
// prompt is composed lazily — at each send attempt — so a repair Execute can
// observe the session generation after any lazy reopen (KB-47).
type executePrompt struct {
	stepPrompt string
	allowed    []string
	contracts  []*v2.OutcomeContract
	rejection  *v2.ExecutionRejection
	// descriptions carries the per-outcome presentation description from the
	// Execute handshake (KB-260): splitAllowedOutcomeProse unwraps the
	// host-supplied "name — description" prompt-prose carrier that rides the
	// AllowedOutcomes entries (the additive criteria-adapter-proto
	// ExecuteRequest field remains the preferred future carrier), and
	// composeExecutePrompt renders the "name — description" menu when it is
	// populated instead of the flat name list.
	descriptions map[string]string
}

// messageOptions composes the prompt for the current session state. Called
// outside s.mu: composeExecutePrompt reads session state (repair eligibility)
// under s.mu itself.
func (ep executePrompt) messageOptions(s *sessionState) *copilot.MessageOptions {
	return &copilot.MessageOptions{
		Prompt: composeExecutePrompt(ep.stepPrompt, ep.allowed, ep.contracts, ep.rejection, s, s.heldSecrets, ep.descriptions),
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
			// KB-263: the CLI announces a submit via the complete
			// assistant.message's ToolRequests (rendered by
			// handleAssistantMessage as the canonical tool.invocation row)
			// AND echoes it here when the adapter handles the call. Announce
			// only the row whose kind is not already rendered: the echo of an
			// already-announced call degrades to a thin tool.requested row
			// carrying the request_id — the anchor tool.result correlates
			// with — so one submit renders one invocation-class row, and the
			// two kinds are unmistakable. The echo of an unannounced call
			// (fixture path: the child announced the call without a
			// first-class assistant message) still renders the invocation
			// row, so no call goes unannounced.
			if ts.announceToolCall(d.ToolCallID) {
				ts.sendErr(sink.Send(adapterEvent("tool.requested", map[string]any{
					"request_id":   d.RequestID,
					"tool_call_id": d.ToolCallID,
					"name":         d.ToolName,
					"event_type":   string(event.Type()),
				})))
				break
			}
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
		// KB-263: mark the call announced before rendering; the assistant
		// message row is the canonical tool.invocation render for the call.
		ts.announceToolCall(tr.ToolCallID)
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
				// directly instead of retrying (CRI-274, KB-42). In contract
				// mode the recorded payload/comment ride the dedicated
				// result fields, not the legacy outputs assembly (KB-47).
				if errors.Is(err, errSessionLoss) {
					if snap, ok := s.snapshotRecordedFinalize(); ok {
						return snap.send(sink)
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

	// KB-71: from the second attempt on, the turn that just ended is the
	// model's reply to a corrective reprompt. Capture the text produced by
	// THIS turn only: a fresh assistant-message id marks a turn that carried
	// a message (the reply text is the evidence), while a turn with no
	// message leaves ts.finalContent stale from an earlier turn — that case
	// records an empty entry instead, so per-reprompt positions stay
	// one-per-reprompt and a future failure distinguishes "answered prose"
	// from "empty reprompted turn". The finalContent/lastMessageID reads are
	// synchronized the same way as in failExhausted: waitTurnSignal has
	// received turnDone from the event-handler goroutine before this runs.
	if ts.lastMessageID != ts.lastCapturedReplyID {
		ts.lastCapturedReplyID = ts.lastMessageID
		if attempt > 1 {
			ts.repromptReplies = append(ts.repromptReplies, evidenceSnippet(ts.finalContent))
		}
	} else if attempt > 1 {
		// This reprompted turn carried no message of its own.
		ts.repromptReplies = append(ts.repromptReplies, "")
	}

	if outcome != "" {
		s.mu.Lock()
		contractMode := s.contractMode
		inRepair := s.inRepairMode
		repairAttempt := s.inRepairAttempt
		payload := append([]byte(nil), s.finalizedPayload...)
		comment := s.finalizedComment
		s.mu.Unlock()
		if inRepair {
			// KB-47: a rejected finalize that the model repaired and
			// resubmitted successfully — surface the recovery once, before the
			// terminal result.
			_ = sink.Send(adapterEvent("outcome.recovered", map[string]any{
				"outcome":        outcome,
				"repair_attempt": repairAttempt,
			}))
		}
		if contractMode {
			// Contract mode: outputs_json is exactly the model-submitted,
			// schema-validated payload — never a session-state assembly — and
			// the finalize comment rides ExecuteResult.comment. Both pass
			// through the secret-hygiene path.
			return true, sink.Send(contractResultEvent(outcome, payload, comment, s.heldSecrets...))
		}
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
	issues := append([]string(nil), s.finalizeFailureIssues...)
	secrets := s.heldSecrets
	s.mu.Unlock()

	list := strings.Join(allowedList, ", ")
	msg := fmt.Sprintf(
		"You must call the `submit_outcome` tool with one of the allowed outcomes: %s. Do not return a final answer without calling the tool. Allowed outcomes: %s. Failure to call the tool will fail the step.",
		list, list,
	)
	if len(issues) > 0 {
		// KB-47: the prior finalize was rejected by the step's outcome
		// contract; surface the structured issue list so the model repairs
		// in-turn. Issue text is truncated and secret-redacted before echoing
		// (it can quote model-generated content).
		var b strings.Builder
		b.WriteString(msg)
		b.WriteString("\n\nYour last submission was rejected; fix the issues below and resubmit:")
		for _, issue := range issues {
			fmt.Fprintf(&b, "\n- %s", truncateText(redactSecrets(issue, secrets), maxContractIssueLen))
		}
		msg = b.String()
	}
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
//
// Additionally, reprompt_replies carries the model's reply text for each
// reprompted turn (KB-71): reprompt_replies[i] is the reply to corrective
// reprompt i+1 (the initial-prompt reply is last_agent_message). A missing
// finalize with answered prose in reprompt_replies points at the reviewer
// charter / convergence failure (the model deliberately refuses), while
// empty entries point at a signal-level failure like the KB-69 drained-idle
// defect.
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
		"invalid_payload": "invalid payload",
		"comment_missing": "missing comment",
		"invalid_params":  "invalid parameters",
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
	// repromptReplies is []string for the same structpb reason.
	repromptAny := make([]any, len(ts.repromptReplies))
	for i, v := range ts.repromptReplies {
		repromptAny[i] = v
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
		"reprompt_replies":     repromptAny,
	}))
	// KB-47: under outcome contracts a fallback contract is the step's
	// guaranteed-finalize exit — a finalize loop that ran out of budget is
	// exactly the condition the fallback declares, so the loop's loss
	// finalizes with the fallback outcome (no payload, no comment; proto
	// v0.7.0) instead of the legacy failure. turn.finalize_exhausted above is
	// kept (it is the operator evidence); only the legacy outcome.failure +
	// failure result are skipped. Without a fallback contract — or with no
	// contracts at all — today's legacy path is byte-for-byte unchanged.
	s.mu.Lock()
	fallbackName := s.fallbackContractName
	s.mu.Unlock()
	if fallbackName != "" {
		return sink.Send(fallbackResultEvent(fallbackName))
	}
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

// handleMaxTurnsReached returns the fallback-contract outcome when the step
// carries outcome contracts with a fallback (KB-47); otherwise it returns
// failure unless "needs_review" is in the allowed set, preserving the
// historical max-turns behavior (the no-contract legacy path).
func (ts *turnState) handleMaxTurnsReached(s *sessionState, sink adapterhost.ExecuteEventSender) error {
	s.mu.Lock()
	_, needsReviewAllowed := s.activeAllowedOutcomes["needs_review"]
	fallbackName := s.fallbackContractName
	s.mu.Unlock()
	if fallbackName != "" {
		return sink.Send(fallbackResultEvent(fallbackName))
	}
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
	// validate on the very first turn. Contract mode (v0.7.0 OutcomeContracts)
	// and repair-mode state ride the same lock.
	// KB-260: the allowed-outcome prose carrier. A host supplies an
	// outcome's presentation description inside its AllowedOutcomes entry as
	// "name — description" prompt-prose until the additive
	// criteria-adapter-proto ExecuteRequest field lands; the bare name
	// remains the submission/validation surface, so the split feeds both the
	// allowed set and the prompt's verdict menu.
	allowed, outcomeDescriptions := splitAllowedOutcomeProse(req.GetAllowedOutcomes())
	contracts := req.GetOutcomeContracts()
	rejection := req.GetRejection()
	s.mu.Lock()
	s.activeAllowedOutcomes = make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		s.activeAllowedOutcomes[name] = struct{}{}
	}
	s.contractMode = len(contracts) > 0
	if s.contractMode {
		s.activeContracts = make(map[string]*v2.OutcomeContract, len(contracts))
		for _, contract := range contracts {
			s.activeContracts[contract.GetName()] = contract
			if contract.GetFallback() {
				s.fallbackContractName = contract.GetName()
			}
		}
	} else {
		s.activeContracts = nil
		s.fallbackContractName = ""
	}
	s.inRepairMode = rejection != nil && rejection.GetOutcome() != ""
	s.inRepairAttempt = 0
	if s.inRepairMode {
		s.inRepairAttempt = rejection.GetAttempt()
	}
	s.mu.Unlock()

	state := newTurnState(maxTurns)
	state.execPrompt = executePrompt{
		stepPrompt:   prompt,
		allowed:      allowed,
		contracts:    contracts,
		rejection:    rejection,
		descriptions: outcomeDescriptions,
	}
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
	// wedging the turn (CRI-274). The prompt is composed at send time —
	// inside executeTurn, per attempt (KB-47) — so a repair Execute observes
	// the session generation after sendWithRetry's lazy reopen: a fresh SDK
	// create bumps createdEpoch and degrades the repair to the full
	// re-execute prompt instead of a minimal prompt into an empty
	// conversation.
	return state.executeTurn(ctx, s, sink)
}

// repairInSession reports whether a rejected finalize can be repaired with a
// minimal prompt into the live SDK session (KB-47): the rejected finalize must
// still be in the model's conversation. That holds when the finalize landed in
// the current SDK session generation (finalizeSessionEpoch >= createdEpoch).
// Both epochs zero — bare unit-test states, and process restarts whose in-memory
// bookkeeping was lost while the host reattached the checkpointed conversation
// (Restore resumes the exact conversation) — also count as in-session. A fresh
// SDK session create after a CLI-child death (createdEpoch bumped by
// reopenSession) starts an empty conversation and degrades the repair to a
// full re-execute.
//
// Takes s.mu. Call it OUTSIDE s.mu — from prompt composition, which runs at
// send time, never from code already holding the lock (KB-47).
func (s *sessionState) repairInSession() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalizeSessionEpoch >= s.createdEpoch && s.finalizeSessionEpoch > 0 {
		return true
	}
	return s.finalizeSessionEpoch == 0 && s.createdEpoch <= 1
}

// composeExecutePrompt assembles the model prompt for an Execute: the
// allowed-outcomes preamble, the contract-conveyance block (contract mode),
// and — for a host-rejected repair — either the minimal repair prompt
// (in-session) or the full step prompt with the rejection note appended
// (degraded re-execute). The no-contract, no-rejection result is byte-for-byte
// today's preamble-wrapped step prompt.
//
// Takes s *sessionState instead of a pre-decided inSession flag (KB-47): the
// in-session decision is made HERE, at composition time (under s.mu), so a
// lazy reopen that re-created the SDK session before the send is reflected.
// s must not be nil; bare (&sessionState{}) works for stateless tests.
func composeExecutePrompt(stepPrompt string, allowed []string, contracts []*v2.OutcomeContract, rejection *v2.ExecutionRejection, s *sessionState, secrets []string, descriptions map[string]string) string {
	// Prepend the allowed-outcomes preamble so the model knows what to call.
	// KB-260: when per-outcome descriptions travel (the proto carrier), the
	// menu renders one "name — description" line per outcome; with no
	// descriptions the flat name list stays byte-for-byte the legacy
	// preamble.
	if len(allowed) > 0 {
		if menu := outcomeMenu(allowed, descriptions); menu != "" {
			stepPrompt = fmt.Sprintf(
				"You must finalize the outcome for this step by calling the `submit_outcome` tool exactly once before ending the turn. The allowed outcomes are:\n%s\n\nIf you do not call the tool with a valid outcome, the step will fail.\n\n%s",
				menu, stepPrompt,
			)
		} else {
			outcomeList := strings.Join(allowed, ", ") // already sorted ascending by W14 loader
			stepPrompt = fmt.Sprintf(
				"You must finalize the outcome for this step by calling the `submit_outcome` tool exactly once before ending the turn. The allowed outcomes are: %s. If you do not call the tool with a valid outcome, the step will fail.\n\n%s",
				outcomeList, stepPrompt,
			)
		}
	}

	if contracts := contractConveyance(contracts); contracts != "" {
		stepPrompt = stepPrompt + "\n\n" + contracts
	}

	if rejection != nil && rejection.GetOutcome() != "" {
		// The model must repair its rejected finalize. Issue text is
		// host-provided diagnostic quoting model-generated content: truncate
		// and secret-redact before echoing it back.
		issues := truncateText(redactSecrets(rejection.GetIssues(), secrets), maxContractIssueLen)
		// In-session decision at composition time (KB-47): a reopen that
		// created a fresh SDK session before this send bumped createdEpoch,
		// degrading the repair to the full re-execute.
		inSession := s.repairInSession()
		if inSession {
			// Minimal repair prompt (KB-47): the live conversation already
			// holds the full step context and the rejected finalize; resending
			// the full step prompt would re-run the discovery work.
			var b strings.Builder
			fmt.Fprintf(&b, "Your previous finalize submission was rejected by the workflow and must be repaired in this session with the `submit_outcome` tool. Rejected outcome: %q.", rejection.GetOutcome())
			if issues != "" {
				fmt.Fprintf(&b, "\nIssues reported by the workflow:\n%s", issues)
			}
			fmt.Fprintf(&b, "\nResubmit the finalize now: call `submit_outcome` with a corrected submission for this outcome before ending the turn.")
			return b.String()
		}
		// Degraded re-execute: the conversation was recreated (CRI-272 fresh
		// create), so the full step prompt runs again — with the rejection
		// note attached so the resubmission addresses the reported issues.
		var b strings.Builder
		b.WriteString(stepPrompt)
		fmt.Fprintf(&b, "\n\nNote: a previous finalize submission for this step was rejected by the workflow (outcome %q, attempt %d).", rejection.GetOutcome(), rejection.GetAttempt())
		if issues != "" {
			fmt.Fprintf(&b, " Its reported issues were:\n%s", issues)
		}
		b.WriteString("\nAddress these issues in your finalization.")
		return b.String()
	}

	return stepPrompt
}

// outcomeProseSeparator separates an allowed-outcome entry's name from the
// description prose riding its AllowedOutcomes entry (KB-260: the
// host-supplied prompt-prose carrier; the additive proto field on
// ExecuteRequest remains the preferred future carrier). Outcome names are
// identifier-like, so a space-em-dash-space separator never appears in a
// bare name.
const outcomeProseSeparator = " — "

// splitAllowedOutcomeProse splits each AllowedOutcomes entry into its bare
// outcome name and optional description prose (KB-260). When NO entry carries
// describable prose, the input slice is returned as-is with a nil map, so
// hosts that travel no descriptions keep every legacy path byte-for-byte
// (allowed set, error lists, prompt preamble). Otherwise the returned names
// are TrimSpace'd split results (an entry whose name part is empty keeps its
// whole trimmed text as the name) and the map carries a non-empty description
// per named outcome; the map is the only render input, so a description for
// an outcome that is not in the allowed set cannot surface.
func splitAllowedOutcomeProse(entries []string) ([]string, map[string]string) {
	descriptions := map[string]string{}
	sawProse := false
	names := make([]string, len(entries))
	for i, entry := range entries {
		name, description := strings.TrimSpace(entry), ""
		if before, after, ok := strings.Cut(entry, outcomeProseSeparator); ok {
			trimmedName := strings.TrimSpace(before)
			if trimmedName != "" {
				name, description, sawProse = trimmedName, strings.TrimSpace(after), true
			}
		}
		names[i] = name
		if description != "" {
			descriptions[name] = description
		}
	}
	if !sawProse {
		return entries, nil
	}
	return names, descriptions
}

// outcomeMenu renders the allowed-outcomes menu for the Execute preamble
// (KB-260). When at least one allowed outcome carries a description, the menu
// is one line per outcome — "- name — description" (bare "- name" when an
// individual description is empty) — so the verdict menu is prompt-prose
// visible at the step where the model finalizes ("stop Remain swallowing it"
// — KB-259 acceptance: names compiled with descriptions must render both).
// When no description travels, the flat legacy name list renders instead
// (composeExecutePrompt keeps the legacy preamble byte-for-byte). Callers
// keep the loader's sorted order; the menu must not re-sort.
func outcomeMenu(allowed []string, descriptions map[string]string) string {
	hasDescriptions := false
	for _, name := range allowed {
		if strings.TrimSpace(descriptions[name]) != "" {
			hasDescriptions = true
			break
		}
	}
	if !hasDescriptions {
		return ""
	}
	lines := make([]string, 0, len(allowed))
	for _, name := range allowed {
		if d := strings.TrimSpace(descriptions[name]); d != "" {
			lines = append(lines, fmt.Sprintf("- %s — %s", name, d))
		} else {
			lines = append(lines, fmt.Sprintf("- %s", name))
		}
	}
	return strings.Join(lines, "\n")
}

// contractConveyance renders the per-step contract block the model sees: what
// each contracted outcome requires of the payload and comment, and which
// outcome is the fallback. Empty when there are no contracts. Typed
// (schema-declaring) contracts also convey the payload-as-output doctrine:
// the payload is the output and a top-level reason is a wrong-lane
// submission (KB-263; the static schema's docs teach the same lane split).
func contractConveyance(contracts []*v2.OutcomeContract) string {
	if len(contracts) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Finalization contracts for this step.")
	for _, contract := range contracts {
		name := contract.GetName()
		var reqs []string
		if schema := string(contract.GetSchemaJson()); strings.TrimSpace(schema) != "" {
			reqs = append(reqs, fmt.Sprintf(
				"when finalizing with this outcome, the submit_outcome call must include a `payload` argument — a JSON object matching this JSON Schema: %s", schema))
			reqs = append(reqs, "the payload is the output — findings belong under the payload's declared fields and the finalization must not carry a top-level `reason`")
		}
		if contract.GetRequireComment() {
			reqs = append(reqs, "when finalizing with this outcome, the submit_outcome call must include a non-empty `comment` argument (require_comment)")
		}
		if contract.GetFallback() {
			reqs = append(reqs, "fallback contract: if no other outcome can be finalized, finalize with this outcome (no payload or comment required)")
		}
		if len(reqs) == 0 {
			fmt.Fprintf(&b, "\n- Outcome %q: no additional requirements.", name)
			continue
		}
		fmt.Fprintf(&b, "\n- Outcome %q: %s.", name, strings.Join(reqs, "; and "))
	}
	return b.String()
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

	// W15: reset per-execute finalize state. activeAllowedOutcomes and the
	// KB-47 contract-mode fields (contractMode, activeContracts,
	// fallbackContractName) are set by Execute *after* this returns; do not
	// reset them here.
	s.finalizedOutcome = ""
	s.finalizedReason = ""
	s.finalizeAttempts = 0
	s.finalizeFailureKind = ""
	s.finalizeFailureIssues = nil
	s.finalizedPayload = nil
	s.finalizedComment = ""
	s.inRepairMode = false
	s.inRepairAttempt = 0
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
