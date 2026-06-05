// copilot_outcome.go — submit_outcome tool: parameter struct, handler, and helpers.

package main

import (
	"fmt"
	"sort"
	"strings"

	copilot "github.com/github/copilot-sdk/go"
)

// SubmitOutcomeArgs is the typed parameter struct for the `submit_outcome` tool.
// The schema deliberately does NOT encode an enum for Outcome — the Copilot Go
// SDK v0.3.0 has no public live tool-mutation API, and refreshing the enum would
// require ResumeSessionWithOptions per step, which the design explicitly rejects.
// Validation runs in the tool handler against the active step's allowed_outcomes
// set carried on sessionState.
type SubmitOutcomeArgs struct {
	Outcome string `json:"outcome"`          // required; must be a member of the active allowed set
	Reason  string `json:"reason,omitempty"` // optional; surfaced in events for operator visibility
}

// handleSubmitOutcome is the tool handler for submit_outcome. It is goroutine-safe:
// the SDK dispatches tool handlers from its own goroutines.
func (p *copilotAdapter) handleSubmitOutcome(adapterSessionID string, args SubmitOutcomeArgs) (copilot.ToolResult, error) {
	s := p.getSession(adapterSessionID)
	if s == nil {
		return submitOutcomeError("unknown session"), nil
	}

	s.mu.Lock()
	s.finalizeAttempts++
	outcome := strings.TrimSpace(args.Outcome)
	if s.finalizedOutcome != "" {
		// Duplicate finalize: the model called us again after a successful call.
		// Check this before any other validation so subsequent calls — whether
		// valid, invalid, or empty — are consistently classified as "duplicate"
		// rather than "missing" or "invalid_outcome".
		existing := s.finalizedOutcome
		s.finalizeFailureKind = "duplicate"
		s.mu.Unlock()
		return submitOutcomeError(fmt.Sprintf(
			"outcome already finalized as %q in this turn; do not call submit_outcome again",
			existing,
		)), nil
	}
	if outcome == "" {
		s.finalizeFailureKind = "missing"
		s.mu.Unlock()
		return submitOutcomeError("outcome is required"), nil
	}
	if _, ok := s.activeAllowedOutcomes[outcome]; !ok {
		if len(s.activeAllowedOutcomes) == 0 {
			s.finalizeFailureKind = "no_outcomes"
			s.mu.Unlock()
			return submitOutcomeError("no outcomes are declared for this step; it cannot be finalized via submit_outcome"), nil
		}
		allowedList := sortedAllowedOutcomes(s.activeAllowedOutcomes)
		s.finalizeFailureKind = "invalid_outcome"
		s.mu.Unlock()
		return submitOutcomeError(fmt.Sprintf(
			"outcome %q is not in the allowed set; choose one of: %s",
			outcome, strings.Join(allowedList, ", "),
		)), nil
	}
	trimmedReason := strings.TrimSpace(args.Reason)
	s.finalizedOutcome = outcome
	s.finalizedReason = trimmedReason
	sink := s.sink
	s.mu.Unlock()

	// Forward an adapter event so operators see the finalize call in the event
	// stream. Use the active sink captured in beginExecution.
	if sink != nil {
		_ = sink.Send(adapterEvent("outcome.finalized", map[string]any{
			"outcome": outcome,
			"reason":  trimmedReason,
		}))
	}

	return submitOutcomeSuccess(outcome), nil
}

// submitOutcomeSuccess returns the ToolResult for a valid finalize call.
func submitOutcomeSuccess(outcome string) copilot.ToolResult {
	return copilot.ToolResult{
		TextResultForLLM: fmt.Sprintf("Outcome %q recorded successfully.", outcome),
		ResultType:       "success",
	}
}

// submitOutcomeError returns the ToolResult for an invalid finalize call.
// Using a ToolResult (not a Go error) so the model can retry within the same
// turn; returning a Go error ends the turn unrecoverably.
func submitOutcomeError(msg string) copilot.ToolResult {
	return copilot.ToolResult{
		TextResultForLLM: msg,
		ResultType:       "failure",
		Error:            msg,
	}
}

// sortedAllowedOutcomes returns the active allowed-outcomes set as a sorted
// slice for deterministic error messages.
func sortedAllowedOutcomes(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
