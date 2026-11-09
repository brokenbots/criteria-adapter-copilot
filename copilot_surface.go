// copilot_surface.go — the per-step submit_outcome tool surface (KB-391).
//
// How the adapter presents tools to its model, what the model calls them, and
// how model shape translates back to wire shape is ADAPTER-INTERNAL (dave
// ruling 2026-11-08): the engine enforces the step's declared outcome/schema on
// results, and the SDK (criteria-go-adapter-sdk v0.7.0) hands the contracts
// over — everything between receipt and translation is this adapter's
// implementation detail. This file builds that translation: it re-declares the
// submit_outcome tool FROM the outcome contracts delivered on the Execute
// request. The tool NAME stays submit_outcome only because the served-tools
// probe and compat machinery key on that exact name (verifySubmitOutcomePresented).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// surfaceDigestPrefix versions the digest input so a change to the surface
// mapping policy re-binds every step surface rather than silently keeping the
// old presentation.
const surfaceDigestPrefix = "submit-surface-v1"

// stepToolSurface is one step's submit_outcome presentation: the tool schema
// the model sees, the description that teaches it, and the declared parameter
// set the strict decode allowlists for this step. Built from the step's
// allowed outcomes and outcome contracts; the digest keys the currently bound
// surface so consecutive same-shape steps cost no session re-open.
//
// The session starts bound to the static surface (nil *stepToolSurface). The
// static lane — pre-KB-391 session-lifetime schema plus its prompts and
// handler behavior — must stay byte-identical.
type stepToolSurface struct {
	digest      string
	description string
	params      map[string]any
	// declared is the sorted declared parameter key set — the same set the
	// re-declared schema advertises and the strict decode accepts.
	declared []string
}

// declaredSubmitParamsFor returns the declared parameter key set in force for
// the given surface: a step surface declares exactly its schema's keys; the
// static surface declares the shared declaredSubmitOutcomeParams set. The
// strict decode rejects everything outside the returned set.
func declaredSubmitParamsFor(surface *stepToolSurface) []string {
	if surface == nil {
		return declaredSubmitOutcomeParams
	}
	return surface.declared
}

// submitSurfaceFromStep derives the step's submit_outcome surface from the
// step's allowed outcomes and outcome contracts (KB-391). A nil return is the
// static surface: contract-less steps leave the session-bound schema untouched.
//
// Mapping policy (adapter-internal, per the layering ruling):
//   - outcome enum: the sorted union of the allowed set and the contract
//     names — the handler's allowed-set check stays authoritative; the schema
//     teaches the step's declared submission surface from the same wire inputs.
//   - schema-carrying contracts (non-empty schema_json's pinned subset): the
//     payload member embeds the contract schema wholesale and there is NO
//     top-level reason member — the payload IS the output. The reason lane is
//     derived from the kept contracts themselves: a contract is schemaless
//     iff its schema_json is EMPTY. The number of DISTINCT payload schemas is
//     the payload member's shape (uniform collapse vs diverging anyOf), never
//     an untyped-lane signal — two contracts sharing one schema_json are two
//     typed contracts, not a step with a reason lane.
//   - a non-empty but unparseable schema_json counts as schema-carrying (no
//     top-level reason member) and contributes no payload branch: the surface
//     cannot present what it cannot parse, and the malformed schema is
//     flagged at finalize time by the pinned contract gate
//     (v2.ValidatePayloadSchema -> parseOutcomeSchema returns a pinned issue
//     that checkContractFinalize rejects in turn).
//   - untyped-contract steps (at least one empty schema_json) keep the
//     top-level reason member with a doc that
//     teaches the real lanes (reason prose is display-only; durable findings
//     ride comment/payload) — matching the handler, and keeping the KB-263
//     fold/wrong-lane machinery reachable where it applies.
//   - comment becomes a required member only when every contract sets
//     require_comment; mixed steps keep it optional and the enum description
//     names the outcomes that require it (the contract gate enforces the
//     per-outcome rule at finalize).
func submitSurfaceFromStep(allowed []string, contracts []*v2.OutcomeContract) *stepToolSurface {
	names := make(map[string]struct{}, len(allowed)+len(contracts))
	for _, name := range allowed {
		if name = strings.TrimSpace(name); name != "" {
			names[name] = struct{}{}
		}
	}
	kept := make([]*v2.OutcomeContract, 0, len(contracts))
	for _, contract := range contracts {
		if contract == nil || strings.TrimSpace(contract.GetName()) == "" {
			continue
		}
		names[contract.GetName()] = struct{}{}
		kept = append(kept, contract)
	}
	if len(kept) == 0 {
		// No contracts: the step is a legacy/untyped-lane step and stays on
		// the static surface byte-identically.
		return nil
	}

	enum := make([]string, 0, len(names))
	for name := range names {
		enum = append(enum, name)
	}
	sort.Strings(enum)

	// The reason lane and the typed-lane wording are derived from the kept
	// CONTRACTS, not from the distinct-schema count: fewer branches than
	// contracts happens whenever two contracts share one schema_json (or a
	// typed contract's schema_json does not parse), and reading that as an
	// untyped lane wrongly declared top-level `reason` on fully schema-
	// carrying steps (review 5821523125). Unparseable schema_json is decided
	// as schema-carrying: it contributes no payload branch (the surface
	// cannot present what it cannot parse) but forbids the reason lane, and
	// the pinned contract gate flags the malformed schema at finalize time.
	hasSchemalessContract := false
	hasSchemaCarryingContract := false
	for _, contract := range kept {
		if len(contract.GetSchemaJson()) == 0 {
			hasSchemalessContract = true
		} else {
			hasSchemaCarryingContract = true
		}
	}
	branches := distinctPayloadSchemas(kept)
	var requiresComment []string
	allRequireComment := len(kept) > 0
	for _, contract := range kept {
		if contract.GetRequireComment() {
			requiresComment = append(requiresComment, contract.GetName())
		} else {
			allRequireComment = false
		}
	}
	sort.Strings(requiresComment)

	properties := map[string]any{
		"outcome": map[string]any{
			"type":        "string",
			"description": outcomeEnumDescription(requiresComment),
			"enum":        enum,
		},
		"comment": map[string]any{
			"type":        "string",
			"description": commentParamDescription(allRequireComment, requiresComment),
		},
		"payload": payloadParamDescription(branches),
	}
	if hasSchemalessContract {
		// At least one contract's schema_json is empty: the legacy reason lane
		// stays a declared member — carrying reason prose on a schemaless contract's
		// outcome is a correct submission (KB-260) and must never be taught
		// away. On mixed steps the doc also names the typed lane: there the
		// payload is the output and a top-level reason is folded in when the
		// contract's schema requires one, wrong-lane-rejected otherwise.
		properties["reason"] = map[string]any{
			"type":        "string",
			"description": reasonParamDescription(hasSchemaCarryingContract),
		}
	}

	required := []string{"outcome"}
	if allRequireComment {
		required = append(required, "comment")
	}

	declared := make([]string, 0, len(properties))
	for key := range properties {
		declared = append(declared, key)
	}
	sort.Strings(declared)

	description := stepSurfaceToolDescription(hasSchemaCarryingContract, hasSchemalessContract)
	params := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
	return &stepToolSurface{
		digest:      surfaceDigest(description, params),
		description: description,
		params:      params,
		declared:    declared,
	}
}

// surfaceSchemaBranch is one payload-schema branch of the re-declared payload
// member: the canonicalized schema document plus the outcomes whose contracts
// declare it.
type surfaceSchemaBranch struct {
	schema   map[string]any
	outcomes []string
}

// distinctPayloadSchemas collects the distinct payload-schema documents among
// the step's contracts, canonicalized (key-order normalized) so two byte
// layouts of one schema collapse into one branch, and returns them in a
// deterministic order. Contracts whose schema_json does not parse are skipped
// here — the surface cannot present what it cannot parse, and the contract
// gate flags the malformed schema at finalize time.
func distinctPayloadSchemas(contracts []*v2.OutcomeContract) []surfaceSchemaBranch {
	branches := make([]surfaceSchemaBranch, 0, len(contracts))
	seen := make(map[string]int, len(contracts))
	for _, contract := range contracts {
		if len(contract.GetSchemaJson()) == 0 {
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal(contract.GetSchemaJson(), &doc); err != nil {
			continue
		}
		canonical, err := json.Marshal(doc)
		if err != nil {
			continue
		}
		if idx, ok := seen[string(canonical)]; ok {
			branches[idx].outcomes = append(branches[idx].outcomes, contract.GetName())
			continue
		}
		seen[string(canonical)] = len(branches)
		branches = append(branches, surfaceSchemaBranch{
			schema:   doc,
			outcomes: []string{contract.GetName()},
		})
	}
	sort.Slice(branches, func(i, j int) bool {
		return branches[i].outcomes[0] < branches[j].outcomes[0]
	})
	for i := range branches {
		sort.Strings(branches[i].outcomes)
	}
	return branches
}

// outcomeEnumDescription teaches the outcome member: the required choice and,
// when some outcomes demand one, the per-outcome comment requirement (the
// contract gate enforces it at finalize; the schema says so up front).
func outcomeEnumDescription(requiresComment []string) string {
	var b strings.Builder
	b.WriteString("Required. The step's final outcome; must be one of the outcomes declared in this step's submission schema, which mirror the step's allowed outcomes.")
	if len(requiresComment) > 0 {
		fmt.Fprintf(&b, " Finalizing with outcome %s requires a non-empty comment (require_comment).",
			quoteJoin(requiresComment))
	}
	return b.String()
}

// commentParamDescription teaches the comment member: required when every
// outcome sets require_comment (the schema makes it a required member), else
// optional with the requiring outcomes named. The comment forwards to the
// workflow with the result.
func commentParamDescription(required bool, requiresComment []string) string {
	if required {
		return "Required. The step's finalize comment (require_comment): every outcome of this step requires a non-empty comment, which is forwarded to the workflow with the result."
	}
	var b strings.Builder
	b.WriteString("Optional finalize comment, forwarded to the workflow with the result.")
	if len(requiresComment) > 0 {
		fmt.Fprintf(&b, " Required (non-empty) when finalizing with outcome %s.", quoteJoin(requiresComment))
	}
	return b.String()
}

// reasonParamDescription teaches the reason member on steps with at least one
// schemaless outcome (a contract whose schema_json is empty). On untyped-contract
// steps reason prose is display-only —
// the durable surfaces are comment and payload. On mixed steps (some outcomes
// schema-carrying) the doc must also name the typed lane there: the payload
// is the output for the typed outcomes, and a top-level reason is folded in
// when the contract's schema requires one, wrong-lane-rejected otherwise.
// A typed contract whose schema_json does not parse still counts as
// schema-carrying, so its coexistence with a schemaless lane uses the mixed
// wording. Neither form may be taught as the
// workflow-rendered findings lane — that is
// the contract-less lane's truth, not a contract-mode step's.
func reasonParamDescription(mixed bool) string {
	if mixed {
		return "Optional prose; only the step's schemaless outcomes consume it (display-only for operators, shown on the outcome.finalized event of those outcomes). Finalizing with one of this step's typed outcomes instead: the payload IS the output — put the findings under the payload's declared fields; a top-level reason is folded in only when the contract's schema declares a reason field, and is rejected otherwise."
	}
	return "Optional display prose: it is shown to operators on the outcome.finalized event but is NOT part of the step's forwarded output. Durable findings text belongs under comment — or, when the chosen outcome declares a payload schema, under the payload's declared fields."
}

// payloadParamDescription builds the payload member. Typed surfaces (one or
// more schema-carrying contracts) embed the contract schema wholesale with a
// description that teaches the payload-is-output rule; a single distinct
// schema collapses to the shape, a diverging set branches per distinct schema
// (anyOf, so overlapping schemas always leave at least one matching branch).
func payloadParamDescription(branches []surfaceSchemaBranch) map[string]any {
	if len(branches) == 0 {
		return map[string]any{
			"type":        "object",
			"description": "Optional JSON object payload, forwarded to the workflow verbatim.",
		}
	}
	if len(branches) == 1 {
		prop := cloneSchemaMap(branches[0].schema)
		prop["description"] = payloadSchemaDescription(branches[0].outcomes)
		return prop
	}
	members := make([]any, len(branches))
	for i, branch := range branches {
		member := cloneSchemaMap(branch.schema)
		member["description"] = payloadBranchDescription(branch.outcomes)
		members[i] = member
	}
	return map[string]any{
		"type":        "object",
		"description": "Required when the chosen outcome declares it (see the branches): the payload IS the step's output — the findings belong under the payload's declared fields, which are forwarded to the workflow verbatim.",
		"anyOf":       members,
	}
}

// payloadSchemaDescription teaches the uniform typed payload member: the
// payload IS the output, the schema's fields are the submission surface, and
// the payload forwards verbatim.
func payloadSchemaDescription(outcomes []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Required when finalizing with outcome %s: the payload IS the step's output — the declared schema's fields are the submission surface; the payload must satisfy this schema and is forwarded to the workflow verbatim. Put the step's findings under these declared fields.",
		quoteJoin(outcomes))
	return b.String()
}

// payloadBranchDescription teaches one diverging-schema branch: it applies
// only to the outcomes whose contract declares it.
func payloadBranchDescription(outcomes []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Use only when finalizing with outcome %s: the payload IS the step's output — the declared schema's fields are the submission surface, forwarded to the workflow verbatim. Put the step's findings under these declared fields.",
		quoteJoin(outcomes))
	return b.String()
}

// cloneSchemaMap copies a schema document so embedding can override the
// description without mutating the contract's parsed form.
func cloneSchemaMap(schema map[string]any) map[string]any {
	clone := make(map[string]any, len(schema)+1)
	for key, value := range schema {
		clone[key] = value
	}
	return clone
}

// stepSurfaceToolDescription is the description surfaced to the model for a
// re-declared submit_outcome tool. It teaches the step-declared submission
// lanes instead of the static description's four-parameter prose: the surface
// schema (enum, payload shape, comment rule) is the contract for THIS step.
// Both booleans derive from the step's contracts (not from branch counts): a
// contract with a non-empty schema_json is typed — even when its schema does
// not parse and the payload member can therefore not present it.
func stepSurfaceToolDescription(typed, reasonDeclared bool) string {
	var b strings.Builder
	b.WriteString("Finalize the outcome for the current step. Call this exactly once with one of the outcomes in the schema's `outcome` enum below: this step's submission schema is re-declared from the step's finalization contracts, so it declares the required shape — the outcome choice, the `payload` schema per outcome (when any contract declares one, the payload IS the step's output and the findings belong under its declared fields), and the `comment` requirement per outcome. Submit exactly the declared shape; parameters outside this step's declared set are rejected, and failure to call this tool with a valid finalize for the step will fail it.")
	if reasonDeclared {
		if typed {
			b.WriteString(" On this step `reason` prose belongs only to the schemaless outcomes (shown on their outcome.finalized event); for the typed outcomes the payload is the output and the findings belong under the payload's declared fields.")
		} else {
			b.WriteString(" On this step the durable findings surface is `comment` (and the payload when an outcome declares a payload schema); `reason` is display-only prose for operators.")
		}
	}
	return b.String()
}

// surfaceDigest fingerprints one surface's presentation (description + params)
// from canonical JSON, so equal inputs yield an equal digest and a changed
// mapping policy version triggers a re-bind of every step surface.
func surfaceDigest(description string, params map[string]any) string {
	canonical, err := json.Marshal(params)
	if err != nil {
		// json.Marshal of a schema built from maps/slices/strings cannot fail;
		// keep the fallback deterministic anyway.
		canonical = []byte(fmt.Sprintf("%v", params))
	}
	sum := sha256.Sum256([]byte(surfaceDigestPrefix + "\n" + description + "\n" + string(canonical)))
	return hex.EncodeToString(sum[:])
}

// sortedContractList returns the active contracts deterministically ordered by
// outcome name (map iteration order must not leak into the pinned evaluator's
// issue ordering or the surface building). Nil-safe; empty/nil input yields nil.
func sortedContractList(contracts map[string]*v2.OutcomeContract) []*v2.OutcomeContract {
	if len(contracts) == 0 {
		return nil
	}
	ordered := make([]*v2.OutcomeContract, 0, len(contracts))
	for _, contract := range contracts {
		ordered = append(ordered, contract)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].GetName() < ordered[j].GetName()
	})
	return ordered
}

// submitToolFor builds the submit_outcome SDK tool bound to one surface: the
// static surface (nil) presents the session-lifetime schema; a step surface
// presents the step-declared one. The tool NAME stays submit_outcome in both
// cases — the served-tools probe and compat machinery key on that exact name
// (verifySubmitOutcomePresented), while wording and schema phrasing are this
// adapter's prerogative.
func (p *copilotAdapter) submitToolFor(adapterSessionID string, surface *stepToolSurface) copilot.Tool {
	name := submitOutcomeToolName
	description := submitOutcomeToolDescription
	params := submitOutcomeToolParameters()
	if surface != nil {
		description = surface.description
		params = surface.params
	}
	return copilot.Tool{
		Name:           name,
		Description:    description,
		Parameters:     params,
		SkipPermission: true,
		Handler: func(invocation copilot.ToolInvocation) (copilot.ToolResult, error) {
			return p.handleSubmitOutcomeRaw(adapterSessionID, invocation)
		},
	}
}

// surfaceDigestOf fingerprints the surface a session is bound to, with the
// static lane digesting as "" so the digest comparison covers both
// directions of a static-to-step or step-to-static transition.
func surfaceDigestOf(surface *stepToolSurface) string {
	if surface == nil {
		return ""
	}
	return surface.digest
}

// withSubmitToolSurface clones the session config with submit_outcome
// re-rendered for the given step surface; adapter_tool, the permission hook,
// model, streaming and config-discovery posture carry over unchanged. Returns
// nil when there is no static config to derive from or the base carries no
// submit_outcome binding to replace (KB-43: never invent tool surface).
func withSubmitToolSurface(base *copilot.SessionConfig, submitTool copilot.Tool) *copilot.SessionConfig {
	if base == nil {
		return nil
	}
	replaced := false
	tools := make([]copilot.Tool, 0, len(base.Tools))
	for _, t := range base.Tools {
		if t.Name == submitOutcomeToolName {
			t = submitTool
			replaced = true
		}
		tools = append(tools, t)
	}
	if !replaced {
		return nil
	}
	cfg := *base
	cfg.Tools = tools
	return &cfg
}

// surfaceSessionConfigs returns the session/resume configs to open the SDK
// session with: the step-bound pair while a step surface is bound, the static
// originals otherwise (KB-391). Caller must hold reopenMu (or otherwise be
// serialized against surface re-binds) for the pair to stay consistent.
func (s *sessionState) surfaceSessionConfigs() (*copilot.SessionConfig, *copilot.ResumeSessionConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stepSurface != nil && s.stepSessionConfig != nil {
		// ensureStepSurface binds the pair together (stepResumeConfig is
		// built from the same session config), so a nil stepResumeConfig
		// alongside a non-nil stepSessionConfig cannot happen; never hand a
		// nil resume config to a resume call regardless.
		if s.stepResumeConfig != nil {
			return s.stepSessionConfig, s.stepResumeConfig
		}
	}
	return s.sessionConfig, s.resumeConfig
}

// ensureStepSurface re-declares submit_outcome for the executing step (KB-391)
// when the step's derived surface differs from the one the SDK session is
// currently bound to. Contract-less steps (nil derived surface on an
// unbound/static session) and consecutive same-shape steps cost zero re-opens:
// the decision is keyed on the surface digest. Repair-mode executes never
// re-open — the rejection loop re-executes the same step, so the bound surface
// stays in force. When a re-open is needed, the session is resumed via the
// persisted SDK session ID so the conversation history survives the surface
// change, falling back to a fresh create (which bumps createdEpoch, mirroring
// reopenSession, so repair logic cannot assume the prior finalize is still
// in-conversation). Open failures degrade to the previously bound surface with
// a loud warning: the decode allowlist and handler stay keyed on session
// state, which still reflects the in-force surface.
//
// Returns the surface in force for this execute — the newly declared one on a
// successful re-open, the previously bound one otherwise (nil = static).
func (p *copilotAdapter) ensureStepSurface(ctx context.Context, s *sessionState, allowed []string, contracts []*v2.OutcomeContract) *stepToolSurface {
	if s == nil {
		return nil
	}
	want := submitSurfaceFromStep(allowed, contracts)

	s.mu.Lock()
	bound := s.stepSurface
	inRepair := s.inRepairMode
	s.mu.Unlock()
	if inRepair || want != nil && bound != nil && want.digest == bound.digest {
		return bound
	}
	if want == nil && bound == nil {
		return nil
	}

	sessionCfg, resumeCfg := s.sessionConfig, s.resumeConfig
	if want != nil {
		sessionCfg = withSubmitToolSurface(s.sessionConfig, p.submitToolFor(s.adapterSessionID, want))
		if sessionCfg == nil {
			// No static config to derive from (bare unit-test state without a
			// completed open): keep the previous binding rather than open a
			// session that would not present the step's surface.
			slog.Warn("copilot: step surface re-declaration skipped; no session config to derive from",
				"adapterSession", s.adapterSessionID, "surfaceDigest", surfaceDigestOf(want))
			return bound
		}
		resumeCfg = buildResumeConfig(sessionCfg)
	}

	s.reopenMu.Lock()
	defer s.reopenMu.Unlock()

	p.mu.Lock()
	_, stillOpen := p.sessions[s.adapterSessionID]
	p.mu.Unlock()
	if !stillOpen {
		return bound
	}
	// A peer execute may have re-bound the surface while we were between
	// locks: re-check the digest before paying for a session open.
	s.mu.Lock()
	bound = s.stepSurface
	s.mu.Unlock()
	if inRepair || want != nil && bound != nil && want.digest == bound.digest {
		return bound
	}
	if want == nil && bound == nil {
		return nil
	}

	client, epoch := p.currentClientAndEpoch()
	if client == nil {
		slog.Warn("copilot: step surface re-declaration skipped; no connected cli client",
			"adapterSession", s.adapterSessionID, "surfaceDigest", surfaceDigestOf(want))
		return bound
	}
	sess, resumed, err := p.openSDKSession(ctx, client, s.adapterSessionID, sessionCfg, resumeCfg)
	if err != nil {
		slog.Warn("copilot: step surface re-open failed; keeping previously bound submit_outcome surface",
			"adapterSession", s.adapterSessionID, "surfaceDigest", surfaceDigestOf(want), "err", err)
		return bound
	}
	s.swapSession(sess)
	s.boundClientEpoch = epoch
	s.mu.Lock()
	s.stepSurface = want
	if want == nil {
		// Back to the static lane: the step-derived configs are retired so
		// surfaceSessionConfigs returns the static originals again.
		s.stepSessionConfig, s.stepResumeConfig = nil, nil
	} else {
		s.stepSessionConfig, s.stepResumeConfig = sessionCfg, resumeCfg
	}
	// A fresh create starts a new, empty conversation: bump the creation epoch
	// so a later repair degrades to a full re-execute instead of a minimal
	// repair prompt into a conversation that no longer contains the finalize.
	// A resume keeps the conversation and the epoch unchanged.
	if !resumed {
		s.createdEpoch++
	}
	s.mu.Unlock()
	slog.Info("copilot: re-declared submit_outcome for step surface",
		"adapterSession", s.adapterSessionID, "sdkSession", sess.SessionID(),
		"surfaceDigest", surfaceDigestOf(want), "resumed", resumed)
	return want
}
