// copilot_outcome.go — submit_outcome tool: parameter struct, handler, static
// tool schema, and contract-validation helpers.

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"

	copilot "github.com/github/copilot-sdk/go"
)

// maxContractIssueLen bounds each issue echoed back to the model in in-turn
// rejection messages and repair prompts. Issue text can quote model-generated
// content (validator paths echo instance values), so it must stay bounded and
// secret-redacted before it reaches any prompt.
const maxContractIssueLen = 2000

// SubmitOutcomeArgs is the typed parameter struct for the `submit_outcome` tool.
// The tool is registered as a hand-built SDK Tool (static parameter schema via
// Tool.Parameters), not via DefineTool reflection, so the shape is
// adapter-controlled. The schema deliberately does NOT encode an enum for
// Outcome — the Copilot Go SDK (v1.0.0) binds tools only at
// CreateSession/ResumeSessionWithOptions with no Session-level tool-mutation
// API, so a per-step enum would require resuming the session once per step,
// which the design explicitly rejects. Validation runs in the tool handler
// against the active step's allowed_outcomes set and, when the step carries
// outcome contracts (v0.7.0 contract mode), the matched contract's schema —
// both carried on sessionState.
//
// KB-216: the parameter contract is DECLARED, not implicit. outcome is the
// required member of the schema the model sees (the only per-lane
// universal); reason is the findings surface of the contract-less legacy
// lane — its requirement is lane-specific; comment and payload are
// the optional contract-mode members; and a call carrying keys outside the
// declared set fails loudly (decodeSubmitOutcomeArgs) instead of Go's default
// unknown-key tolerance silently dropping the text — the observed failure had
// all findings under an undeclared parameter name evaporate with no retry
// signal. There is deliberately NO comment→reason alias (dave ruling
// 2026-10-08): reason is the only findings surface of the legacy lane, and
// the schema says so.
//
// KB-263 ruling (dave, 10-14) with the KB-388 refinement: reason is the
// findings surface for UNTYPED outcomes only. On a typed step (the chosen
// outcome's contract declares a payload schema) the payload IS the output —
// a top-level reason the fold consumes (the schema requires a "reason"
// member the payload lacks) lands in the payload and finalizes accepted,
// while one the fold refuses (no such required member, or the payload
// supplies its own) is a wrong-lane submission rejected with the same typed
// not-consumed shape. The outcome.finalized event renders the operative
// prose from the payload — for a fold-consumed reason that is the exact
// top-level prose.
//
// KB-260 re-bind (dave, 10-14): the static schema previously advertised
// top-level reason as uniformly required, mis-teaching typed steps (the
// model paid a reason onto a lane whose output is the payload). The schema's
// docs now teach the per-step lanes — reason required on contract-less
// steps, payload-as-output on schema-declaring contracts — while the
// per-step truth stays prompt-conveyed and handler-enforced: SDK tools bind
// per session, so no per-step required list is expressible on the static
// schema itself, and the legacy lane's reason requirement is enforced in
// turn (an empty/absent reason on a step without outcome contracts is
// rejected — KB-260 review B1).
type SubmitOutcomeArgs struct {
	Outcome string `json:"outcome"`          // required; must be a member of the active allowed set
	Reason  string `json:"reason,omitempty"` // required-and-enforced on contract-less steps; on typed steps folded into the payload when the contract requires it, wrong-lane-rejected when the fold refuses it (KB-263/KB-388)

	// Comment and Payload are the optional contract-mode members: consumed only
	// when the step carries outcome contracts (comment is mandatory when the
	// matched contract sets require_comment). On contract-less steps the
	// handler rejects a submission carrying either rather than silently
	// dropping the bytes.
	Comment string          `json:"comment,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// declaredSubmitOutcomeParams is the declared submit_outcome parameter set
// (KB-216): the keys the tool's schema advertises to the model and exactly the
// keys strict decoding accepts. It is one sorted source of truth on purpose —
// the schema builder, the unknown-parameter rejection, and the retry guidance
// all name the same set, so the declared contract cannot drift apart.
var declaredSubmitOutcomeParams = []string{"comment", "outcome", "payload", "reason"}

// submitOutcomeParamDocs is the parameter documentation the schema presents to
// the model: type plus a description that says what belongs in each parameter
// (the docs are the contract — the model cannot claim a parameter was
// undeclared or that text belongs somewhere else).
var submitOutcomeParamDocs = map[string][2]string{
	"outcome": {"string", "Required. The step's final outcome; must be one of the allowed outcomes conveyed in the step prompt."},
	"reason":  {"string", "Required on steps without outcome contracts (an empty reason is rejected in-turn on such steps): ALL of the step's findings, review feedback, and prose justification belong here as the value of reason, and the workflow renders the step's findings from reason. On steps with a typed outcome contract requiring reason, the adapter folds this top-level value into the contract payload automatically. Never submit findings text under comment or any other parameter."},
	"comment": {"string", "Optional finalize comment; consumed only when the step carries outcome contracts. Mandatory when the step's contract for the chosen outcome sets require_comment (the prompt says when). Findings text still belongs in reason — do not submit feedback as the comment."},
	"payload": {"object", "Optional JSON object payload; consumed only when the step carries outcome contracts. When the contract for the chosen outcome declares a payload schema, the payload IS the output — the declared schema's fields are the submission surface; the payload must satisfy that schema and is forwarded to the workflow verbatim. A top-level reason is folded into the payload when the contract's schema requires a `reason` member that the submitted payload does not supply; a top-level reason is wrong-lane-rejected only when there is no such lane (the schema declares no required `reason` member, or the payload supplies its own reason)."},
}

// submitOutcomeToolParameters is the static parameter JSON Schema the
// hand-built submit_outcome tool presents to the model. KB-216: it declares
// the full parameter contract — additionalProperties false and per-parameter
// descriptions that say what belongs where — so a call that does not match is
// a loud rejection instead of a silently-accepted shape. KB-260 re-bind:
// `outcome` is the only required member (the per-lane universal); reason's
// requirement is lane-specific (required on contract-less steps — enforced
// in-turn — and wrong-lane-rejected on typed steps). The enforceable
// per-step specifics (allowed set, payload schema, require_comment) remain
// prompt-conveyed and handler-validated, and there is no per-step enum or
// required list on outcome (SDK tools bind per session), which is what makes
// per-step contracts possible with a session-bound tool.
func submitOutcomeToolParameters() map[string]any {
	properties := make(map[string]any, len(declaredSubmitOutcomeParams))
	for _, param := range declaredSubmitOutcomeParams {
		docs := submitOutcomeParamDocs[param]
		properties[param] = map[string]any{
			"type":        docs[0],
			"description": docs[1],
		}
	}
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             []string{"outcome"},
		"additionalProperties": false,
	}
}

// submitArgsDecodeError is a typed failure to decode the submit_outcome
// invocation envelope against the declared parameter contract (KB-216). kind
// selects the finalize-failure classification the raw handler records:
// "invalid_params" for calls that carry keys outside the declared set or
// contract-mode parameters on a contract-less step (a typed rejection the
// model repairs from by retrying with the declared names), "invalid_outcome"
// for structurally undecodable envelopes.
type submitArgsDecodeError struct {
	kind    string
	message string
}

// Error returns the rejection text the model sees in the ToolResult.
func (e *submitArgsDecodeError) Error() string { return e.message }

// decodeSubmitOutcomeArgs converts the raw hand-built-tool invocation into
// typed SubmitOutcomeArgs. Hand-built tools bypass DefineTool's reflection
// decoding, so the arguments — already-decoded JSON carried as `any` — are
// re-encoded and unmarshaled here. A nil envelope decodes to zero args (the
// handler classifies the missing outcome); a non-object envelope is an error.
//
// KB-216: decoding is STRICT. Keys outside the declared parameter set are
// rejected BEFORE the struct decode — encoding/json tolerates unknown fields
// by silently dropping their values, which is exactly how the observed calls
// lost the reviewer's findings text with no error and no retry signal — and
// the rejection names the unexpected keys plus the declared set so the model
// retries with the correct names inside the existing finalize budget.
func decodeSubmitOutcomeArgs(invocation copilot.ToolInvocation, surface *stepToolSurface) (SubmitOutcomeArgs, error) {
	if invocation.Arguments == nil {
		return SubmitOutcomeArgs{}, nil
	}
	raw, err := json.Marshal(invocation.Arguments)
	if err != nil {
		return SubmitOutcomeArgs{}, fmt.Errorf("submit_outcome arguments could not be encoded: %w", err)
	}
	declared := declaredSubmitParamsFor(surface)
	if unexpected := unexpectedSubmitArgsKeys(raw, declared); len(unexpected) > 0 {
		// Retry guidance is surface-aware: on a lane whose surface still
		// declares reason (static or untyped step) the findings text belongs
		// in reason; on a typed step the payload schema's declared members
		// are the submission surface, and there is no top-level reason at all
		// (KB-391: the reason rejection is schema-level, at this decode).
		guidance := "the step's findings text belongs in the required \"reason\" parameter."
		if surface != nil && !slices.Contains(declared, "reason") {
			guidance = "the payload is this step's output: findings and structured results belong under the declared payload schema members, and no top-level reason parameter exists."
		}
		return SubmitOutcomeArgs{}, &submitArgsDecodeError{
			kind: "invalid_params",
			message: fmt.Sprintf(
				"submit_outcome rejected: unknown parameter(s) %s; the declared parameters are %s. Retry with the declared names — %s",
				quoteJoin(unexpected), quoteJoin(declared), guidance,
			),
		}
	}
	var args SubmitOutcomeArgs
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&args); err != nil {
		return SubmitOutcomeArgs{}, &submitArgsDecodeError{
			kind:    "invalid_outcome",
			message: fmt.Sprintf("submit_outcome arguments must be a JSON object with the declared fields: %v", err),
		}
	}
	return args, nil
}

// unexpectedSubmitArgsKeys returns the envelope keys outside the declared
// parameter set (caller-provided: the static declared set or the bound step
// surface's rendered property names, KB-391), sorted for a deterministic
// rejection message. A non-object envelope contributes no names — it is
// rejected by the struct decode with its own guidance instead.
func unexpectedSubmitArgsKeys(raw []byte, declaredParams []string) []string {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope) == 0 {
		return nil
	}
	declared := make(map[string]struct{}, len(declaredParams))
	for _, key := range declaredParams {
		declared[key] = struct{}{}
	}
	var unexpected []string
	for key := range envelope {
		if _, ok := declared[key]; !ok {
			unexpected = append(unexpected, key)
		}
	}
	sort.Strings(unexpected)
	return unexpected
}

// quoteJoin renders string list items as quoted, comma-separated text for
// rejection messages.
func quoteJoin(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = strconv.Quote(item)
	}
	return strings.Join(quoted, ", ")
}

// handleSubmitOutcomeRaw is the raw handler the SDK dispatches for the
// hand-built submit_outcome tool: it decodes the invocation envelope and
// delegates to handleSubmitOutcome. An undecodable envelope is classified as
// an invalid_outcome rejection (consuming the finalize budget) with a precise
// message; a ToolResult — not a Go error — is returned in every case so the
// model can retry within the same turn.
func (p *copilotAdapter) handleSubmitOutcomeRaw(adapterSessionID string, invocation copilot.ToolInvocation) (copilot.ToolResult, error) {
	// The strict decode allowlist is the surface the SDK session is currently
	// bound to (KB-391): a typed step rejects a top-level reason key here —
	// schema-level, decode-layer — rather than at the handler's wrong-lane
	// fold, which remains authoritative only on lanes whose surface declares
	// reason (static, untyped step).
	surface := p.getSession(adapterSessionID).boundSubmitSurface()
	args, err := decodeSubmitOutcomeArgs(invocation, surface)
	if err != nil {
		// A rejected call consumed one finalize attempt; the classification is
		// the typed decode error's kind so operators can distinguish
		// parameter-contract violations from undecodable envelopes.
		kind := "invalid_outcome"
		var decodeErr *submitArgsDecodeError
		if errors.As(err, &decodeErr) {
			kind = decodeErr.kind
		}
		if s := p.getSession(adapterSessionID); s != nil {
			s.mu.Lock()
			s.finalizeAttempts++
			s.finalizeFailureKind = kind
			s.mu.Unlock()
		}
		return submitOutcomeError(err.Error()), nil
	}
	return p.handleSubmitOutcome(adapterSessionID, args)
}

// typedContractSchema returns the matched contract's schema_json for outcome
// when the contract is present and declares a non-empty schema; nil otherwise.
// A non-nil result marks the outcome TYPED (KB-263): the payload is the
// output, the top-level reason lane does not exist, and the display lane
// derives from the payload. Caller holds s.mu.
func (s *sessionState) typedContractSchema(outcome string) []byte {
	contract := s.activeContracts[outcome]
	if contract == nil || len(contract.GetSchemaJson()) == 0 {
		return nil
	}
	return contract.GetSchemaJson()
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
	switch r.kind {
	case "comment_missing":
		fmt.Fprintf(&b, "finalize rejected for outcome %q: a comment is required and none was supplied.\n", outcome)
	case "invalid_outcome":
		fmt.Fprintf(&b, "finalize rejected for outcome %q: no outcome contract governs that outcome on this step's contract set.\n", outcome)
	default:
		fmt.Fprintf(&b, "finalize rejected for outcome %q: the submitted payload does not satisfy the step contract.\n", outcome)
	}
	for _, issue := range r.issues {
		fmt.Fprintf(&b, "- %s\n", truncateText(issue, maxContractIssueLen))
	}
	b.WriteString("Fix the listed issues and call submit_outcome again with a corrected submission.")
	return b.String()
}

// checkContractFinalize pre-flights a finalize call against the step's
// outcome contracts with the SDK's pinned evaluator wrapper (KB-391: the
// adapter validates with the SDK contract helpers BEFORE pushing the
// ExecuteResult; the engine re-validates as the backstop through the existing
// attempt loop). The caller holds s.mu. Returns nil when the submission
// satisfies the contracts; otherwise a contractRejection carrying the pinned,
// ordered issue list, classified into the rejection lane:
//
//   - "comment_missing": every issue comments on the require_comment lane
//     (the payload being otherwise clean);
//   - "invalid_payload": payload-schema issues (a missing comment rides along
//     when both lanes are wrong — the payload issues dominate);
//   - "invalid_outcome": the finalized outcome has no contract entry. KB-391
//     moves this host-owned post-finalize rejection into the in-turn
//     pre-flight, so the model can repair within the finalize budget; the
//     engine still re-validates.
//
// The candidate is the canonical finalize wire shape: the normalized payload
// as outputs_json and the trimmed comment (whitespace-only comments are
// missing, matching the trimmed comment the finalize stores). Typed-lane
// parameter reconciliation: the submit_outcome tool schema documents "reason"
// as a required TOP-LEVEL parameter, while typed contracts can require a
// member of the same name INSIDE the payload — that reconcile happens in
// handleSubmitOutcome before this check (foldDeclaredPayloadParams), the same
// shape the host performs at the verdict funnel. Since KB-388 the fold's
// consumption is also honored at the wrong-lane gate in handleSubmitOutcome,
// so a fold-consumed reason finalizes instead of being discarded by the
// rejection one call after the fold computed it.
func (s *sessionState) checkContractFinalize(outcome string, payload json.RawMessage, comment string) *contractRejection {
	if len(s.activeContracts) == 0 {
		return nil
	}
	issues := evaluateFinalizeCandidate(sortedContractList(s.activeContracts), outcome, payload, comment)
	if len(issues) == 0 {
		return nil
	}
	return &contractRejection{kind: contractRejectionKind(issues), issues: issues}
}

// evaluateFinalizeCandidate pre-flights one finalize candidate over a contract
// set with the SDK's pinned evaluator wrapper (KB-391): nil issues means the
// result is safe to push across the Execute stream as-is. The payload is
// normalized to the finalize wire shape (absent/empty/null = {}) and the
// comment is the trimmed finalize comment the result will carry.
func evaluateFinalizeCandidate(contracts []*v2.OutcomeContract, outcome string, payload json.RawMessage, comment string) []string {
	candidate := &v2.ExecuteResult{
		Outcome:     outcome,
		Comment:     comment,
		OutputsJson: normalizedFinalizePayload(payload),
	}
	return adapterhost.ValidateOutcomeResult(contracts, candidate)
}

// pushBoundaryIssuesLocked re-runs the pinned evaluator over an already
// recorded finalize at the terminal push boundary (KB-391 defensive
// pre-flight): the finalize passed the in-turn checks at submit, this re-checks
// that the contract set on file still accepts the candidate before the result
// rides the Execute stream — catching contract drift between accept and push
// (an Execute re-handshake on the session, a repair path recording outside the
// handler). Caller holds s.mu. Legacy mode (no contracts) is always clean.
func (s *sessionState) pushBoundaryIssuesLocked(outcome string, payload json.RawMessage, comment string) []string {
	if !s.contractMode || len(s.activeContracts) == 0 {
		return nil
	}
	return evaluateFinalizeCandidate(sortedContractList(s.activeContracts), outcome, payload, comment)
}

// contractRejectionKind classifies the pinned issue list into the rejection
// lane: a require_comment issue keeps its dedicated class when the payload is
// otherwise clean; payload-schema issues dominate a multi-lane rejection; the
// remaining issues are the outcome-level contract-coverage lane.
func contractRejectionKind(issues []string) string {
	commentOnly := true
	payloadIssue := false
	for _, issue := range issues {
		if strings.HasPrefix(issue, "missing_comment: ") {
			continue
		}
		commentOnly = false
		if strings.HasPrefix(issue, "payload_schema: ") {
			payloadIssue = true
		}
	}
	switch {
	case commentOnly:
		return "comment_missing"
	case payloadIssue:
		return "invalid_payload"
	default:
		return "invalid_outcome"
	}
}

// foldDeclaredPayloadParams folds documented top-level submit parameters into
// the payload when the contract schema requires those names and the payload
// itself lacks them. Returns the (possibly rewritten) payload, whether a fold
// happened, and whether that fold consumed the top-level reason — true only
// when the schema requires a "reason" member, the payload itself did not
// already carry one, and the submitted top-level reason is non-empty prose.
// The reason-specific signal (not the any-fold flag) is what gates the KB-263
// wrong-lane rejection: a fold that consumed only the comment still leaves
// the top-level reason unconsumed (KB-388).
func foldDeclaredPayloadParams(reason, comment string, payload json.RawMessage, schemaJSON []byte) (json.RawMessage, bool, bool) {
	required := schemaRequiredMembers(schemaJSON)
	if len(required) == 0 {
		return payload, false, false
	}
	obj := map[string]json.RawMessage{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &obj); err != nil {
			return payload, false, false
		}
		// encoding/json unmarshals the literal `null` document into a NIL
		// map with no error — Payload carries the submitted bytes verbatim,
		// so a wire shape like payload: null reaches the fold unchanged —
		// and the required-member loop below would panic on assignment to
		// a nil map. A null payload folds exactly like an absent one: re-arm
		// the map; when the fold does not fire, the original bytes still
		// flow on to normalizedFinalizePayload's null→{} normalization.
		if obj == nil {
			obj = map[string]json.RawMessage{}
		}
	}
	folded := false
	reasonFolded := false
	for _, name := range required {
		if _, present := obj[name]; present {
			continue
		}
		switch name {
		case "reason":
			if strings.TrimSpace(reason) != "" {
				obj[name] = json.RawMessage(mustJSONString(reason))
				folded = true
				reasonFolded = true
			}
		case "comment":
			if strings.TrimSpace(comment) != "" {
				obj[name] = json.RawMessage(mustJSONString(comment))
				folded = true
			}
		}
	}
	if !folded {
		return payload, false, false
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return payload, false, false
	}
	return out, true, reasonFolded
}

// schemaRequiredMembers extracts the "required" array from a schema document.
func schemaRequiredMembers(schemaJSON []byte) []string {
	var doc struct {
		Required []string `json:"required"`
	}
	if len(schemaJSON) == 0 {
		return nil
	}
	if err := json.Unmarshal(schemaJSON, &doc); err != nil {
		return nil
	}
	return doc.Required
}

func mustJSONString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// activeContractSchemaFor returns the declared schema for the outcome, empty
// when no contract is active (the fold helper treats that as no-op).
func (s *sessionState) activeContractSchemaFor(outcome string) []byte {
	if contract := s.activeContracts[outcome]; contract != nil {
		return contract.GetSchemaJson()
	}
	return nil
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
	foldedPayload, _, reasonFolded := foldDeclaredPayloadParams(trimmedReason, trimmedComment, args.Payload, s.activeContractSchemaFor(outcome))
	canonicalPayload := normalizedFinalizePayload(foldedPayload)

	// KB-216 wire-call strictness: comment and payload are contract-mode
	// members, and on a step without outcome contracts both would be silently
	// dropped — the exact class the observed calls failed with (findings
	// submitted as a comment evaporating while reason was left empty). Reject
	// them as a typed error pointing at the required reason instead of ever
	// silently reinterpreting them.
	if !s.contractMode {
		var notConsumed []string
		if trimmedComment != "" {
			notConsumed = append(notConsumed, "\"comment\"")
		}
		if trimmedPayload := bytes.TrimSpace(args.Payload); len(trimmedPayload) > 0 && !bytes.Equal(trimmedPayload, []byte("null")) {
			notConsumed = append(notConsumed, "\"payload\"")
		}
		if len(notConsumed) > 0 {
			subject := strings.Join(notConsumed, " and ")
			verb := "is"
			if len(notConsumed) > 1 {
				verb = "are"
			}
			s.finalizeFailureKind = "invalid_params"
			s.mu.Unlock()
			return submitOutcomeError(fmt.Sprintf(
				"submit_outcome rejected: %s %s not consumed by this step (no outcome contracts declared), so the value would be silently dropped. This step's findings text belongs in the required \"reason\" parameter. Call submit_outcome again with the declared parameters %s.",
				subject, verb, quoteJoin(declaredSubmitOutcomeParams),
			)), nil
		}
		// KB-260 (review B1): the legacy lane still requires the reason —
		// enforced here, not merely doc-taught, because the session-bound
		// static schema cannot carry a per-step required list. A
		// contract-less finalize without reason is the KB-216
		// silent-findings-evaporation class: reject in-turn,
		// budget-consuming and repairable, mirroring the not-consumed shape
		// above. Contract-mode steps keep their own lanes: typed outcomes
		// reject reason as wrong-lane (KB-263), and schemaless contracts
		// surface findings through the comment lane.
		if trimmedReason == "" {
			s.finalizeFailureKind = "invalid_params"
			s.mu.Unlock()
			return submitOutcomeError(legacyReasonRequiredMessage()), nil
		}
	}

	// Contract mode (v0.7.0 OutcomeContracts present): enforce the typed-lane
	// rule, then the matched contract's comment requirement and payload
	// schema, in-turn so the model can repair-and-resubmit within the same
	// finalize budget.
	if s.contractMode {
		// KB-263 ruling (dave, 10-14): on a typed outcome (schema-declaring
		// contract) the payload is the output and there is no top-level
		// reason lane. A finalize carrying findings prose under reason is a
		// wrong-lane submission — reject it in-turn with the KB-216
		// not-consumed shape, the exact symmetry of the comment/payload
		// rejection applied to untyped steps above. The rejection consumes
		// one finalize attempt and is repairable: a payload-only resubmit
		// within the budget finalizes once.
		//
		// KB-388: when the fold consumed the top-level reason (the contract
		// schema requires a "reason" member and the submitted payload lacked
		// one), the reason IS the payload lane — canonicalPayload carries the
		// prose and the rejection below would discard the fold's work one
		// line after computing it, the observed in-turn burn (castle
		// 361b3d39: 29 rejected submissions, zero payload_invalid events,
		// blind shape-probing against a contradictory instruction pair).
		// Keep the rejection for the true wrong-lane shape: non-empty
		// top-level prose the fold refused — a reason member the schema does
		// not require, or a payload that already supplies its own — which
		// cannot land in the payload and must be rejected per the KB-263
		// ruling.
		if trimmedReason != "" && !reasonFolded && len(s.typedContractSchema(outcome)) > 0 {
			s.finalizeFailureKind = "invalid_params"
			s.mu.Unlock()
			return submitOutcomeError(typedReasonNotConsumedMessage()), nil
		}
		if rejection := s.checkContractFinalize(outcome, canonicalPayload, trimmedComment); rejection != nil {
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
	// KB-263: the display lane resolves under the pre-unlock snapshot — typed
	// steps render the payload's operative prose (schema reason, else digest);
	// untyped steps keep the legacy finalize-reason render.
	typedSchema := s.typedContractSchema(outcome)
	s.mu.Unlock()

	// Forward an adapter event so operators see the finalize call in the event
	// stream. Use the active sink captured in beginExecution. The reason field
	// is the event's DISPLAY lane (KB-263): on typed steps it renders the
	// operative prose from the validated payload — the schema's reason field
	// when the contract declares one, else a digest of the payload — so an
	// emitted event is never the empty string merely because the legacy
	// parameter was absent. Redact any adapter-held secrets from the rendered
	// prose; this is best-effort hygiene, not a safety guarantee. (The
	// validated payload itself is forwarded verbatim by contractResultEvent
	// with redaction applied there.)
	if sink != nil {
		_ = sink.Send(adapterEvent("outcome.finalized", map[string]any{
			"outcome": outcome,
			"reason":  finalizedDisplayReason(typedSchema, trimmedReason, canonicalPayload, heldSecrets),
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

// typedReasonNotConsumedMessage is the KB-216 not-consumed rejection text for
// a wrong-lane finalize on a typed step (KB-263): the outcome contract
// declares a payload schema, so reason is not consumed — the payload is the
// output and the step's findings belong under the payload's declared fields.
// Repairable in-turn within the finalize budget.
func typedReasonNotConsumedMessage() string {
	return `submit_outcome rejected: "reason" is not consumed by this typed outcome (its contract declares a payload schema, so the payload is the output). The step's findings belong under the payload's declared fields — the schema's reason field when the contract declares one. Call submit_outcome again without the "reason" parameter.`
}

// legacyReasonRequiredMessage is the KB-216 rejection text for a
// contract-less finalize whose top-level reason is empty or absent (KB-260
// review B1): the workflow renders the step's findings from reason, so an
// empty reason would silently drop the findings. Repairable in-turn within
// the finalize budget.
func legacyReasonRequiredMessage() string {
	return fmt.Sprintf(`submit_outcome rejected: "reason" is required by this step (no outcome contracts declared): the workflow renders the step's findings from reason, so an empty reason would silently drop the findings. Call submit_outcome again with the declared parameters %s.`, quoteJoin(declaredSubmitOutcomeParams))
}

// finalizedDisplayReason resolves the reason field of the outcome.finalized
// adapter event (KB-263). Untyped steps keep the legacy render: the
// finalize's top-level reason. Typed steps (a schema-declaring contract
// matched) render the operative prose from the validated payload — the
// schema's reason field when present — falling back to a digest of the
// payload when the schema declares no reason field, so the display lane is
// never empty on typed steps merely because the legacy parameter was absent.
func finalizedDisplayReason(typedSchema []byte, legacyReason string, payload json.RawMessage, secrets []string) string {
	if len(typedSchema) == 0 {
		return redactSecrets(legacyReason, secrets)
	}
	return typedFinalizedDisplayReason(typedSchema, payload, secrets)
}

// typedFinalizedDisplayReason renders the display prose for a typed finalize:
// the payload's "reason" property when the contract schema declares one and
// the submitted payload carries non-empty prose, else a digest of the
// secret-redacted payload. The digest path is the payload's fingerprint — the
// full typed output travels on the result lane (contractResultEvent forwards
// OutputsJson verbatim), so the display lane stays cheap and non-empty.
func typedFinalizedDisplayReason(schemaJSON []byte, payload json.RawMessage, secrets []string) string {
	if schemaDeclaresReason(schemaJSON) {
		if prose := payloadStringField(payload, "reason"); prose != "" {
			return redactSecrets(prose, secrets)
		}
	}
	sum := sha256.Sum256([]byte(redactSecrets(string(payload), secrets)))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// schemaDeclaresReason reports whether the contract schema declares a
// top-level "reason" property — the payload field whose prose the
// outcome.finalized display lane mirrors on typed steps (KB-263).
func schemaDeclaresReason(schemaJSON []byte) bool {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return false
	}
	_, ok := schema.Properties["reason"]
	return ok
}

// payloadStringField extracts a top-level string property from the submitted
// payload; empty when the payload carries no such string.
func payloadStringField(payload json.RawMessage, field string) string {
	var top map[string]any
	if err := json.Unmarshal(payload, &top); err != nil {
		return ""
	}
	prose, _ := top[field].(string)
	return prose
}
