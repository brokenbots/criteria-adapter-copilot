// copilot_outcome.go — submit_outcome tool: parameter struct, handler, static
// tool schema, and contract-validation helpers.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	copilot "github.com/github/copilot-sdk/go"

	"github.com/google/jsonschema-go/jsonschema"
)

// maxContractIssueLen bounds each issue echoed back to the model in in-turn
// rejection messages and repair prompts. Issue text can quote model-generated
// content (validator paths echo instance values), so it must stay bounded and
// secret-redacted before it reaches any prompt.
const maxContractIssueLen = 2000

// SubmitOutcomeArgs is the typed parameter struct for the `submit_outcome` tool.
// The tool is registered as a hand-built SDK Tool (static structural schema via
// Tool.Parameters), not via DefineTool reflection, so the shape is
// adapter-controlled. The schema deliberately does NOT encode an enum for
// Outcome — the Copilot Go SDK (v1.0.0) binds tools only at
// CreateSession/ResumeSessionWithOptions with no Session-level tool-mutation
// API, so a per-step enum would require resuming the session once per step,
// which the design explicitly rejects. Validation runs in the tool handler
// against the active step's allowed_outcomes set and, when the step carries
// outcome contracts (v0.7.0 contract mode), the matched contract's schema —
// both carried on sessionState.
type SubmitOutcomeArgs struct {
	Outcome string          `json:"outcome"`                   // required; must be a member of the active allowed set
	Reason  string          `json:"reason,omitempty"`          // optional prose; surfaced in events for operator visibility
	Comment string          `json:"comment,omitempty"`         // optional finalize comment; mandatory when the matched contract sets require_comment
	Payload json.RawMessage `json:"payload,omitempty"`         // optional JSON object payload, kept verbatim; validated against the matched contract's schema_json when contract mode is active
}

// submitOutcomeToolParameters is the static structural JSON Schema the
// hand-built submit_outcome tool presents to the model. It is deliberately
// structural only — no per-step enum on outcome (SDK tools bind per session)
// and no property-level detail beyond the three fields: the enforceable
// specifics (allowed set, payload schema, require_comment) are prompt-conveyed
// and handler-validated, which is what makes per-step contracts possible with
// a session-bound tool.
func submitOutcomeToolParameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"outcome": map[string]any{
				"type":        "string",
				"description": "Required. The step's final outcome; must be one of the allowed outcomes conveyed in the step prompt.",
			},
			"comment": map[string]any{
				"type":        "string",
				"description": "Optional finalize comment. Mandatory when the step's contract for the chosen outcome sets require_comment (the prompt says when).",
			},
			"payload": map[string]any{
				"type":        "object",
				"description": "Optional JSON object payload. When the step carries outcome contracts, the payload must satisfy the contract's schema for the chosen outcome and is forwarded to the workflow verbatim.",
			},
		},
		"required": []string{"outcome"},
	}
}

// decodeSubmitOutcomeArgs converts the raw hand-built-tool invocation into
// typed SubmitOutcomeArgs. Hand-built tools bypass DefineTool's reflection
// decoding, so the arguments — already-decoded JSON carried as `any` — are
// re-encoded and unmarshaled here. A nil envelope decodes to zero args (the
// handler classifies the missing outcome); a non-object envelope is an error.
func decodeSubmitOutcomeArgs(invocation copilot.ToolInvocation) (SubmitOutcomeArgs, error) {
	if invocation.Arguments == nil {
		return SubmitOutcomeArgs{}, nil
	}
	raw, err := json.Marshal(invocation.Arguments)
	if err != nil {
		return SubmitOutcomeArgs{}, fmt.Errorf("submit_outcome arguments could not be encoded: %w", err)
	}
	var args SubmitOutcomeArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return SubmitOutcomeArgs{}, fmt.Errorf("submit_outcome arguments must be a JSON object with outcome, comment, and payload fields: %w", err)
	}
	return args, nil
}

// handleSubmitOutcomeRaw is the raw handler the SDK dispatches for the
// hand-built submit_outcome tool: it decodes the invocation envelope and
// delegates to handleSubmitOutcome. An undecodable envelope is classified as
// an invalid_outcome rejection (consuming the finalize budget) with a precise
// message; a ToolResult — not a Go error — is returned in every case so the
// model can retry within the same turn.
func (p *copilotAdapter) handleSubmitOutcomeRaw(adapterSessionID string, invocation copilot.ToolInvocation) (copilot.ToolResult, error) {
	args, err := decodeSubmitOutcomeArgs(invocation)
	if err != nil {
		if s := p.getSession(adapterSessionID); s != nil {
			s.mu.Lock()
			s.finalizeAttempts++
			s.finalizeFailureKind = "invalid_outcome"
			s.mu.Unlock()
		}
		return submitOutcomeError(err.Error()), nil
	}
	return p.handleSubmitOutcome(adapterSessionID, args)
}

// contractRejection is an in-turn finalize rejection produced by the step's
// outcome contract: the finalizeFailureKind to record and the structured issue
// list the model repairs from (also echoed by the corrective reprompt).
type contractRejection struct {
	kind   string
	issues []string
}

// messages renders the rejection's ToolResult text: the structured issue list
// (one truncated bullet per issue) after a one-line cause.
func (r *contractRejection) message(outcome string) string {
	var b strings.Builder
	if r.kind == "comment_missing" {
		fmt.Fprintf(&b, "finalize rejected for outcome %q: a comment is required and none was supplied.\n", outcome)
	} else {
		fmt.Fprintf(&b, "finalize rejected for outcome %q: the submitted payload does not satisfy the step contract.\n", outcome)
	}
	for _, issue := range r.issues {
		fmt.Fprintf(&b, "- %s\n", truncateText(issue, maxContractIssueLen))
	}
	b.WriteString("Fix the listed issues and call submit_outcome again with a corrected submission.")
	return b.String()
}

// checkContractFinalize enforces the matched outcome contract on a finalize
// call in contract mode. The caller holds s.mu. Returns nil when the outcome
// carries no contract entry (the host's post-finalize validation owns
// outcome_uncontracted; the adapter only enforces the matched contract's
// schema and require_comment per the v0.7.0 split) or when the submission
// satisfies it; otherwise a contractRejection with the issue list.
func (s *sessionState) checkContractFinalize(outcome string, payload json.RawMessage, comment string) *contractRejection {
	contract := s.activeContracts[outcome]
	if contract == nil {
		return nil
	}
	schema := contract.GetSchemaJson()
	requireComment := contract.GetRequireComment()
	commentMissing := requireComment && strings.TrimSpace(comment) == ""
	var issues []string
	if commentMissing {
		issues = append(issues, fmt.Sprintf(
			"the outcome %q requires a finalize comment (require_comment); none was supplied", outcome))
	}
	if len(schema) > 0 {
		issues = append(issues, validateContractPayload(payload, schema)...)
	}
	if len(issues) == 0 {
		return nil
	}
	// Comment-missing is its own in-turn class, but only when the payload is
	// otherwise clean; when both are wrong the payload issues dominate the
	// rejection message and the classification.
	kind := "invalid_payload"
	if commentMissing && len(issues) == 1 {
		kind = "comment_missing"
	}
	return &contractRejection{kind: kind, issues: issues}
}

// validateContractPayload validates the model-submitted payload against one
// contract's schema_json using github.com/google/jsonschema-go (already in the
// module graph via the Copilot SDK). Per the proto's pinned semantics, a
// missing, empty, or literal-null payload is treated as an empty object, so a
// schema with required properties reports them as missing instead of waving
// the empty payload through. Returns the validator's issue list (empty =
// valid); issue text is the validator's structured messages, truncated and
// never nil-safe-guessed.
func validateContractPayload(payload json.RawMessage, schemaJSON []byte) []string {
	if len(schemaJSON) == 0 {
		// No payload contract: everything valid (proto: empty schema_json =
		// outputs_json forwarded verbatim).
		return nil
	}
	instance, instanceIssue := payloadInstance(payload)
	if instanceIssue != "" {
		return []string{instanceIssue}
	}

	var schema jsonschema.Schema
	if err := schema.UnmarshalJSON(schemaJSON); err != nil {
		return []string{fmt.Sprintf("the step contract's schema is not a valid JSON Schema: %s",
			truncateText(err.Error(), maxContractIssueLen))}
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		return []string{fmt.Sprintf("the step contract's schema could not be resolved: %s",
			truncateText(err.Error(), maxContractIssueLen))}
	}
	if err := resolved.Validate(instance); err != nil {
		return splitIssueLines(err.Error())
	}
	return nil
}

// payloadInstance decodes the submitted payload into the instance value the
// validator sees, applying the proto's absent/empty/null = empty-object
// semantics and normalizing those cases to `{}` for verbatim storage. The
// second return is an issue string when the payload bytes do not decode to a
// single JSON value.
func payloadInstance(payload json.RawMessage) (any, string) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return map[string]any{}, ""
	}
	var instance any
	if err := json.Unmarshal(trimmed, &instance); err != nil {
		return nil, fmt.Sprintf("the submitted payload does not decode to a JSON object: %s",
			truncateText(err.Error(), maxContractIssueLen))
	}
	return instance, ""
}

// splitIssueLines splits a validator error into one structured issue per line,
// truncating each. jsonschema-go reports the first failing rule per instance
// as a single error string, so in practice this yields one entry.
func splitIssueLines(errText string) []string {
	var issues []string
	for _, line := range strings.Split(errText, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			issues = append(issues, line)
		}
	}
	if len(issues) == 0 {
		issues = []string{errText}
	}
	return issues
}

// normalizedFinalizePayload applies the proto's absent/empty/null = empty-object
// normalization to the raw bytes stored on sessionState for verbatim
// forwarding (contract mode): only those degenerate shapes are rewritten to
// `{}`; everything else is forwarded byte-for-byte as submitted.
func normalizedFinalizePayload(payload json.RawMessage) json.RawMessage {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return json.RawMessage([]byte("{}"))
	}
	return json.RawMessage(trimmed)
}

// handleSubmitOutcome is the tool handler for submit_outcome. It is goroutine-safe:
// the SDK dispatches tool handlers from its own goroutines.
func (p *copilotAdapter) handleSubmitOutcome(adapterSessionID string, args SubmitOutcomeArgs) (copilot.ToolResult, error) {
	s := p.getSession(adapterSessionID)
	if s == nil {
		return submitOutcomeError("unknown session"), nil
	}

	s.mu.Lock()

	// Duplicate finalize: the model called us again after a successful call.
	// Check this before any other validation so subsequent calls — whether
	// valid, invalid, or empty — are consistently classified as "duplicate"
	// rather than "missing" or "invalid_outcome". It never consumes the
	// finalize budget.
	if s.finalizedOutcome != "" {
		existing := s.finalizedOutcome
		s.finalizeFailureKind = "duplicate"
		s.mu.Unlock()
		return submitOutcomeError(fmt.Sprintf(
			"outcome already finalized as %q in this turn; do not call submit_outcome again",
			existing,
		)), nil
	}

	// Budget accounting happens up front: every classify-and-reject path below
	// has consumed one of the step's allowed finalize attempts.
	s.finalizeAttempts++
	outcome := strings.TrimSpace(args.Outcome)
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
	trimmedComment := strings.TrimSpace(args.Comment)
	canonicalPayload := normalizedFinalizePayload(args.Payload)

	// Contract mode (v0.7.0 OutcomeContracts present): enforce the matched
	// contract's comment requirement and payload schema in-turn so the model
	// can repair-and-resubmit within the same finalize budget.
	if s.contractMode {
		if rejection := s.checkContractFinalize(outcome, args.Payload, trimmedComment); rejection != nil {
			s.finalizeFailureKind = rejection.kind
			s.finalizeFailureIssues = append([]string(nil), rejection.issues...)
			s.mu.Unlock()
			p.emitContractRejectionEvent(s, adapterSessionID, outcome, rejection)
			return submitOutcomeError(rejection.message(outcome)), nil
		}
	}

	// Accept the finalize: record outcome, reason, and — contract mode only —
	// the normalized verbatim payload and comment, plus the session epoch the
	// finalize landed in (drives in-session repair eligibility). The turn loop
	// (handleIdleTurn) owns the terminal ExecuteEvent emission.
	s.finalizedOutcome = outcome
	s.finalizedReason = trimmedReason
	if s.contractMode {
		s.finalizedPayload = canonicalPayload
		s.finalizedComment = trimmedComment
		s.finalizeSessionEpoch = s.createdEpoch
	}
	sink := s.sink
	heldSecrets := s.heldSecrets
	s.mu.Unlock()

	// Forward an adapter event so operators see the finalize call in the event
	// stream. Use the active sink captured in beginExecution. Redact any
	// adapter-held secrets from the event payload; this is best-effort hygiene,
	// not a safety guarantee. (The validated payload itself is forwarded
	// verbatim by contractResultEvent with redaction applied there.)
	if sink != nil {
		_ = sink.Send(adapterEvent("outcome.finalized", map[string]any{
			"outcome": outcome,
			"reason":  redactSecrets(trimmedReason, heldSecrets),
		}))
	}

	return submitOutcomeSuccess(outcome), nil
}

// emitContractRejectionEvent forwards the outcome.payload_invalid adapter event
// for an in-turn contract rejection, carrying the failed outcome, the rejection
// class, and the truncated structured issue list (secret-redacted — validator
// messages can echo model-generated content). Fetches the sink and held
// secrets from session state after the caller released s.mu.
func (p *copilotAdapter) emitContractRejectionEvent(s *sessionState, adapterSessionID, outcome string, rejection *contractRejection) {
	s.mu.Lock()
	sink, heldSecrets := s.sink, s.heldSecrets
	s.mu.Unlock()
	if sink == nil {
		return
	}
	issueList := make([]string, 0, len(rejection.issues))
	for _, issue := range rejection.issues {
		issueList = append(issueList, truncateText(issue, maxContractIssueLen))
	}
	issueJSON, err := json.Marshal(issueList)
	if err != nil {
		issueJSON = []byte("[]")
	}
	// proto Struct encoding cannot carry json.RawMessage: decode the redacted
	// JSON array into concrete values before building the event payload.
	var redactedIssues []any
	if err := json.Unmarshal([]byte(redactSecrets(string(issueJSON), heldSecrets)), &redactedIssues); err != nil {
		redactedIssues = []any{}
	}
	_ = sink.Send(adapterEvent("outcome.payload_invalid", map[string]any{
		"outcome": outcome,
		"kind":    rejection.kind,
		"issues":  redactedIssues,
	}))
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
