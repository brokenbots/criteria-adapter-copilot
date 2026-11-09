// copilot_surface_test.go — unit tests for the per-step submit_outcome tool
// surface construction (KB-391).

package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

func TestSubmitSurfaceFromStepContractlessIsStatic(t *testing.T) {
	if got := submitSurfaceFromStep([]string{"approved"}, nil); got != nil {
		t.Fatalf("contract-less step must stay static (nil surface), got %+v", got)
	}
	if got := submitSurfaceFromStep(nil, []*v2.OutcomeContract{}); got != nil {
		t.Fatalf("no allowed outcomes and no contracts must stay static, got %+v", got)
	}
}

// Typed uniform: one schema-carrying contract collapses the payload member to
// that schema, declares no top-level reason member, and the enum unions the
// allowed set with the contract names.
func TestSubmitSurfaceTypedUniform(t *testing.T) {
	surf := submitSurfaceFromStep(
		[]string{"approved", "failure"},
		[]*v2.OutcomeContract{contractTestContract(t, "approved", reasonRequiredSchema, false, false)},
	)
	if surf == nil {
		t.Fatal("expected a step surface")
	}
	props := surf.params["properties"].(map[string]any)
	if _, has := props["reason"]; has {
		t.Fatalf("typed step must not declare a top-level reason member, props: %v", props)
	}
	payload, ok := props["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload member must be the embedded schema, got %T", props["payload"])
	}
	var want map[string]any
	if err := json.Unmarshal([]byte(reasonRequiredSchema), &want); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"type", "required", "properties"} {
		if !reflect.DeepEqual(payload[key], want[key]) {
			t.Errorf("payload member key %q = %v, want %v", key, payload[key], want[key])
		}
	}
	if desc, _ := payload["description"].(string); desc == "" || !containsAll(desc, "approved", "output") {
		t.Errorf("payload description must teach the payload-is-output rule for the outcome, got %q", desc)
	}
	outcome := props["outcome"].(map[string]any)
	enum := outcome["enum"].([]string)
	if !reflect.DeepEqual(enum, []string{"approved", "failure"}) {
		t.Errorf("outcome enum = %v, want the allowed-set union", enum)
	}
	if required := surf.params["required"].([]string); !reflect.DeepEqual(required, []string{"outcome"}) {
		t.Errorf("required = %v, want [outcome]", required)
	}
	if got := declaredSubmitParamsFor(surf); !reflect.DeepEqual(got, []string{"comment", "outcome", "payload"}) {
		t.Errorf("declared set = %v", got)
	}
}

// Diverging schemas branch per distinct schema, branch descriptions name their
// outcomes, and equivalent schemas with different key layouts collapse into
// one branch.
func TestSubmitSurfaceDivergingBranches(t *testing.T) {
	other := `{"required":["verdict"],"properties":{"verdict":{"type":"string"}},"type":"object"}`
	surf := submitSurfaceFromStep(
		[]string{"approved"},
		[]*v2.OutcomeContract{
			contractTestContract(t, "approved", reasonRequiredSchema, false, false),
			contractTestContract(t, "failure", other, false, false),
			contractTestContract(t, "needs_review", other, false, false),
		},
	)
	if surf == nil {
		t.Fatal("expected a step surface")
	}
	payload := surf.params["properties"].(map[string]any)["payload"].(map[string]any)
	members, ok := payload["anyOf"].([]any)
	if !ok || len(members) != 2 {
		t.Fatalf("payload must branch per distinct schema (2), got %v", payload["anyOf"])
	}
	first := members[0].(map[string]any)
	desc, _ := first["description"].(string)
	if !containsAll(desc, "approved") {
		t.Errorf("branch description must name its outcomes, got %q", desc)
	}
}

// Untyped-contract step: reason stays declared with display-truth docs,
// uniform require_comment makes comment a required member, and mixed
// require_comment keeps it optional while the enum description names the
// requiring outcomes.
func TestSubmitSurfaceUntypedContracts(t *testing.T) {
	surf := submitSurfaceFromStep(
		[]string{"approved"},
		[]*v2.OutcomeContract{
			contractTestContract(t, "approved", "", false, false),
			contractTestContract(t, "failure", "", true, false),
		},
	)
	if surf == nil {
		t.Fatal("expected a step surface")
	}
	props := surf.params["properties"].(map[string]any)
	if _, has := props["reason"]; !has {
		t.Fatal("untyped-contract step keeps the reason member")
	}
	if reasonDesc, _ := props["reason"].(map[string]any)["description"].(string); !containsAll(reasonDesc, "comment") {
		t.Errorf("reason doc must teach the durable lanes, got %q", reasonDesc)
	}
	if required := surf.params["required"].([]string); !reflect.DeepEqual(required, []string{"outcome"}) {
		t.Errorf("mixed require_comment keeps comment optional, required = %v", required)
	}
	enumDesc := props["outcome"].(map[string]any)["description"].(string)
	if !containsAll(enumDesc, "failure") {
		t.Errorf("enum description must name the require_comment outcomes, got %q", enumDesc)
	}

	uniform := submitSurfaceFromStep([]string{"approved"},
		[]*v2.OutcomeContract{contractTestContract(t, "approved", "", true, false)})
	if required := uniform.params["required"].([]string); !reflect.DeepEqual(required, []string{"outcome", "comment"}) {
		t.Fatalf("uniform require_comment makes comment required, required = %v", required)
	}
	commentDesc := uniform.params["properties"].(map[string]any)["comment"].(map[string]any)["description"].(string)
	if !containsAll(commentDesc, "Required") {
		t.Errorf("comment doc must teach the requirement, got %q", commentDesc)
	}
}

// The digest is deterministic for equal inputs, insensitive to schema key
// layout, and sensitive to allowed set and require_comment changes.
func TestSubmitSurfaceDigest(t *testing.T) {
	build := func(allowed []string, schema string, requireComment bool) string {
		return submitSurfaceFromStep(allowed,
			[]*v2.OutcomeContract{contractTestContract(t, "approved", schema, requireComment, false)}).digest
	}
	base := build([]string{"approved"}, reasonRequiredSchema, false)
	rebuilt := build([]string{"approved"}, reasonRequiredSchema, false)
	if base != rebuilt {
		t.Fatalf("equal inputs must produce an equal digest: %s vs %s", base, rebuilt)
	}
	reordered := build([]string{"approved"}, `{"properties":{"reason":{"type":"string"}},"required":["reason"],"type":"object"}`, false)
	if reordered != base {
		t.Errorf("schema key layout must not change the digest: %s vs %s", base, reordered)
	}
	if growAllowed := build([]string{"approved", "failure"}, reasonRequiredSchema, false); growAllowed == base {
		t.Error("a changed allowed set must change the digest")
	}
	if requireComment := build([]string{"approved"}, reasonRequiredSchema, true); requireComment == base {
		t.Error("a changed require_comment must change the digest")
	}
}

// sortedContractList orders a contract map deterministically (map order must
// not leak into the pinned evaluator's issue ordering).
func TestSortedContractList(t *testing.T) {
	in := map[string]*v2.OutcomeContract{
		"failure":    contractTestContract(t, "failure", "", false, false),
		"approved":   contractTestContract(t, "approved", "", false, false),
		"needs_look": contractTestContract(t, "needs_look", "", false, false),
	}
	got := sortedContractList(in)
	if len(got) != 3 || got[0].GetName() != "approved" || got[1].GetName() != "failure" || got[2].GetName() != "needs_look" {
		names := make([]string, 0, len(got))
		for _, c := range got {
			names = append(names, c.GetName())
		}
		t.Fatalf("deterministic order expected, got %v", names)
	}
	if sortedContractList(nil) != nil {
		t.Fatalf("nil map must return nil")
	}
}

// submitToolFor binds the static presentation when the surface is nil and the
// step presentation when one is bound; the tool NAME must stay submit_outcome
// in both cases (the served-tools probe and compat machinery key on it).
func TestSubmitToolForSurface(t *testing.T) {
	p := outcomeAdapter(contractTestState([]string{"approved"},
		contractTestContract(t, "approved", reasonRequiredSchema, false, false)))

	static := p.submitToolFor("s1", nil)
	if static.Name != submitOutcomeToolName {
		t.Fatalf("static tool name = %q", static.Name)
	}
	if static.Description != submitOutcomeToolDescription {
		t.Fatal("static tool must keep the session-lifetime description")
	}
	if !reflect.DeepEqual(static.Parameters, submitOutcomeToolParameters()) {
		t.Fatal("static tool must keep the session-lifetime schema")
	}

	surf := submitSurfaceFromStep([]string{"approved"},
		[]*v2.OutcomeContract{contractTestContract(t, "approved", reasonRequiredSchema, false, false)})
	stepTool := p.submitToolFor("s1", surf)
	if stepTool.Name != submitOutcomeToolName {
		t.Fatalf("step tool name must stay submit_outcome, got %q", stepTool.Name)
	}
	if stepTool.Description != surf.description || !reflect.DeepEqual(stepTool.Parameters, surf.params) {
		t.Fatal("step tool must present the bound surface")
	}
	if stepTool.Handler == nil {
		t.Fatal("step tool must carry the submit_outcome handler")
	}
}

// containsAll reports whether s contains every needles substring.
func containsAll(s string, needles ...string) bool {
	for _, n := range needles {
		if n == "" {
			continue
		}
		if !strings.Contains(s, n) {
			return false
		}
	}
	return true
}

// rebindFixture builds an adapter + session fixture wired to a fake client, as
// the reopenSession tests do.
func rebindFixture(t *testing.T) (*copilotAdapter, *fakeClient, *sessionState) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-surface", "sdk-surface")

	p := newCopilotAdapter()
	fc := &fakeClient{}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	sc := p.buildSessionConfig(map[string]string{"model": "m"}, "adapter-surface")
	live := &fakeSession{sessionID: "sdk-surface"}
	s := newSessionState("adapter-surface", live, sc, buildResumeConfig(sc), adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil), p)
	p.sessions["adapter-surface"] = s
	return p, fc, s
}

func approvedStep(t *testing.T, allowed ...string) ([]string, []*v2.OutcomeContract) {
	t.Helper()
	if allowed == nil {
		allowed = []string{"approved"}
	}
	return allowed, []*v2.OutcomeContract{contractTestContract(t, "approved", reasonRequiredSchema, false, false)}
}

// toolArgSet returns the flattened parameter properties of the submit_outcome
// tool in the given tool list, or nil when absent.
func toolArgSet(t *testing.T, tools []copilot.Tool) map[string]any {
	t.Helper()
	for _, tool := range tools {
		if tool.Name != submitOutcomeToolName {
			continue
		}
		props, ok := tool.Parameters["properties"].(map[string]any)
		if !ok {
			t.Fatalf("submit_outcome parameters lack a properties object: %v", tool.Parameters)
		}
		return props
	}
	t.Fatalf("no submit_outcome tool in %d tools", len(tools))
	return nil
}

func TestEnsureStepSurfaceRebindsTypedStepViaResume(t *testing.T) {
	p, fc, s := rebindFixture(t)
	origSession := s.currentSession()
	allowed, contracts := approvedStep(t)

	surf := p.ensureStepSurface(context.Background(), s, allowed, contracts)
	if surf == nil {
		t.Fatal("typed step must bind a step surface")
	}
	if s.stepSurface != surf {
		t.Fatal("the bound surface must be recorded on session state")
	}
	if s.currentSession() == origSession {
		t.Fatal("the SDK session must be swapped to the re-opened one")
	}
	if len(fc.resumeIDs) != 1 || fc.resumeIDs[0] != "sdk-surface" || fc.createCount != 0 {
		t.Fatalf("open = resume %v / creates %d, want resume [sdk-surface] / 0 creates (history preserved)",
			fc.resumeIDs, fc.createCount)
	}
	if s.createdEpoch != 0 {
		t.Fatalf("createdEpoch = %d, want 0 (resume keeps the conversation epoch; fixture starts bare)",
			s.createdEpoch)
	}

	// The resumed session's tool set carries the step surface instead of the
	// static reason member.
	props := toolArgSet(t, fc.resumeConfigs[0].Tools)
	if _, has := props["reason"]; has {
		t.Fatal("resumed tool set must not carry the static reason member on a typed step")
	}
	if _, has := props["payload"]; !has {
		t.Fatal("resumed tool set must carry the step's payload schema")
	}
	if s.stepSessionConfig == nil || len(s.stepSessionConfig.Tools) != 2 {
		t.Fatalf("step session config must record the step-bound tool pair, got %+v", s.stepSessionConfig)
	}
	stepProps := toolArgSet(t, s.stepSessionConfig.Tools)
	if !reflect.DeepEqual(stepProps, surf.params["properties"]) {
		t.Fatal("recorded step config must carry the bound surface's properties")
	}
}

func TestEnsureStepSurfaceConsecutiveSameShapeCostsNothing(t *testing.T) {
	p, fc, s := rebindFixture(t)

	allowed, contracts := approvedStep(t)
	first := p.ensureStepSurface(context.Background(), s, allowed, contracts)
	if first == nil {
		t.Fatal("first typed step must bind")
	}
	opens, creates, resumes := 0, fc.createCount, len(fc.resumeIDs)

	for i := 0; i < 3; i++ {
		again := p.ensureStepSurface(context.Background(), s, allowed, contracts)
		if again != first {
			t.Fatalf("step %d: surface %v, want the unchanged binding", i+1, again)
		}
	}
	if fc.createCount != creates || len(fc.resumeIDs) != resumes {
		t.Fatalf("same-shape steps re-opened: creates %d→%d resumes %d→%d, want zero churn",
			creates, fc.createCount, resumes, len(fc.resumeIDs))
	}
	if opens != 0 {
		t.Fatal("opens must stay zero")
	}
}

func TestEnsureStepSurfaceRepairModeDoesNotReopen(t *testing.T) {
	p, fc, s := rebindFixture(t)

	allowed, contracts := approvedStep(t)
	bound := p.ensureStepSurface(context.Background(), s, allowed, contracts)
	if bound == nil {
		t.Fatal("typed step must bind")
	}
	s.mu.Lock()
	s.inRepairMode = true
	s.mu.Unlock()

	// A same-shaped (or any) repair re-execute must not touch the session.
	got := p.ensureStepSurface(context.Background(), s, allowed, contracts)
	if got != bound {
		t.Fatal("repair execute must keep the bound surface")
	}
	if fc.createCount != 0 || len(fc.resumeIDs) != 1 {
		t.Fatalf("repair re-open churned: creates %d resumes %d, want 0/1", fc.createCount, len(fc.resumeIDs))
	}
	// A different-shape contract during repair also degrades to the bound one.
	other := []*v2.OutcomeContract{contractTestContract(t, "needs_look", reasonRequiredSchema, true, false)}
	got = p.ensureStepSurface(context.Background(), s, []string{"needs_look"}, other)
	if got != bound {
		t.Fatal("repair execute must keep the bound surface even across shapes")
	}
	if fc.createCount != 0 || len(fc.resumeIDs) != 1 {
		t.Fatalf("repair across shapes churned: creates %d resumes %d, want 0/1", fc.createCount, len(fc.resumeIDs))
	}
}

func TestEnsureStepSurfaceStaticTransitionRebindsBack(t *testing.T) {
	p, fc, s := rebindFixture(t)

	allowed, contracts := approvedStep(t)
	if got := p.ensureStepSurface(context.Background(), s, allowed, contracts); got == nil {
		t.Fatal("typed step must bind")
	}
	static := p.ensureStepSurface(context.Background(), s, []string{"approved"}, nil)
	if static != nil {
		t.Fatal("contract-less follow-up must rebind to the static lane")
	}
	if s.stepSurface != nil || s.stepSessionConfig != nil {
		t.Fatal("session state must return to the static binding")
	}
	if len(fc.resumeIDs) != 2 || fc.createCount != 0 {
		t.Fatalf("static transition = resume %v / creates %d, want 2 resumes / 0 creates", fc.resumeIDs, fc.createCount)
	}
	// The resumed tool set is the static surface again (includes reason).
	props := toolArgSet(t, fc.resumeConfigs[1].Tools)
	if _, has := props["reason"]; !has {
		t.Fatal("static rebind must restore the reason member")
	}
}

func TestEnsureStepSurfaceOpenFailureDegradesLoudly(t *testing.T) {
	p, fc, s := rebindFixture(t)

	p.mu.Lock()
	delete(p.sessions, "adapter-surface") // membership guard: not open
	p.mu.Unlock()
	allowed, contracts := approvedStep(t)
	got := p.ensureStepSurface(context.Background(), s, allowed, contracts)
	if got != nil {
		t.Fatal("a removed session must degrade to the static surface")
	}
	p.sessions["adapter-surface"] = s
	if got = p.ensureStepSurface(context.Background(), s, allowed, contracts); got == nil {
		t.Fatalf("bound after re-open, err trace: creates=%d resumes=%v", fc.createCount, fc.resumeIDs)
	}

	// Next surface change: both resume and create fail — the binding stays.
	fc.mu.Lock()
	fc.resumeErr = context.DeadlineExceeded
	fc.createErr = context.DeadlineExceeded
	fc.mu.Unlock()
	bound := s.stepSurface
	got = p.ensureStepSurface(context.Background(), s, []string{"needs_look"},
		[]*v2.OutcomeContract{contractTestContract(t, "needs_look", reasonRequiredSchema, true, false)})
	if got != bound {
		t.Fatal("failed re-open must degrade to the previously bound surface")
	}
	if s.stepSurface != bound {
		t.Fatal("failed re-open must not touch the recorded binding")
	}
}

func TestEnsureStepSurfaceFreshCreateBumpsEpoch(t *testing.T) {
	p, fc, s := rebindFixture(t)
	// Resume is impossible: open falls through to a fresh create.
	fc.mu.Lock()
	fc.resumeErr = errors.New("resume unavailable")
	fc.mu.Unlock()

	allowed, contracts := approvedStep(t)
	got := p.ensureStepSurface(context.Background(), s, allowed, contracts)
	if got == nil {
		t.Fatal("typed step must bind on the fresh-create path too")
	}
	if fc.createCount != 1 {
		t.Fatalf("creates = %d, want 1", fc.createCount)
	}
	if s.createdEpoch != 1 {
		t.Fatalf("createdEpoch = %d, want 1 (fresh create bumps the bare fixture's epoch, mirroring reopenSession)",
			s.createdEpoch)
	}
}

func TestReopenSessionKeepsStepSurfaceBinding(t *testing.T) {
	p, fc, s := rebindFixture(t)

	allowed, contracts := approvedStep(t)
	surf := p.ensureStepSurface(context.Background(), s, allowed, contracts)
	if surf == nil {
		t.Fatal("typed step must bind")
	}
	if err := fc.Ping(context.Background(), "liveness"); err != nil {
		t.Fatal(err)
	}
	// Force a transport-recovery reopen: the session is stale by epoch.
	s.boundClientEpoch = -1
	p.reopenSession(context.Background(), s)

	if len(fc.resumeIDs) != 2 || fc.createCount != 0 {
		t.Fatalf("reopen = resume %v / creates %d, want the second resume / no create",
			fc.resumeIDs, fc.createCount)
	}
	props := toolArgSet(t, fc.resumeConfigs[1].Tools)
	if _, has := props["reason"]; has {
		t.Fatal("transport reopen on a bound step must keep the step surface, not the static one")
	}
	if _, has := props["payload"]; !has {
		t.Fatal("transport reopen must keep the step's payload schema")
	}
	if s.stepSurface != surf {
		t.Fatal("reopen must preserve the bound surface record")
	}
}

// KB-391 scope item 6: on a typed step (payload-schema contract, no top-level
// reason member on the bound surface) a reason key is rejected at the decode
// layer as an unknown parameter — schema-level, not handler-level.
func TestDecodeSubmitOutcomeArgsTypedStepRejectsTopLevelReason(t *testing.T) {
	allowed, contracts := approvedStep(t)
	surf := submitSurfaceFromStep(allowed, contracts)
	if declaredSubmitParamsFor(surf) == nil || slices.Contains(declaredSubmitParamsFor(surf), "reason") {
		t.Fatalf("typed surface must not declare reason: %v", declaredSubmitParamsFor(surf))
	}

	args, err := decodeSubmitOutcomeArgs(copilot.ToolInvocation{ToolName: submitOutcomeToolName,
		Arguments: map[string]any{"outcome": "approved", "reason": "all findings here"}}, surf)
	if err == nil {
		t.Fatal("top-level reason on a typed step must be rejected at the decode layer")
	}
	var typed *submitArgsDecodeError
	if !errors.As(err, &typed) {
		t.Fatalf("err = %T, want *submitArgsDecodeError", err)
	}
	if typed.kind != "invalid_params" {
		t.Errorf("kind = %q, want invalid_params (schema-level, not the handler wrong-lane)", typed.kind)
	}
	if !strings.Contains(typed.message, `"reason"`) || !strings.Contains(typed.message, `"payload"`) {
		t.Errorf("message must name reason as unknown and payload as declared, got %q", typed.message)
	}
	if !strings.Contains(typed.message, "payload") || strings.Contains(typed.message, `required \"reason\"`) {
		t.Errorf("retry guidance must be payload-shape, got %q", typed.message)
	}
	if args.Outcome != "" {
		t.Errorf("no partial decode expected, got %+v", args)
	}

	// A payload-shaped call decodes cleanly against the surface.
	if _, err := decodeSubmitOutcomeArgs(copilot.ToolInvocation{ToolName: submitOutcomeToolName,
		Arguments: map[string]any{"outcome": "approved", "payload": map[string]any{"reason": "findings"}}}, surf); err != nil {
		t.Errorf("payload-shaped call rejected: %v", err)
	}
}

// An untyped-contract step surface still declares reason, so the retry
// guidance stays the legacy findings-in-reason text.
func TestDecodeSubmitOutcomeArgsUntypedSurfaceKeepsReasonGuidance(t *testing.T) {
	surf := submitSurfaceFromStep([]string{"approved"},
		[]*v2.OutcomeContract{contractTestContract(t, "approved", "", false, false)})
	if surf == nil || !slices.Contains(declaredSubmitParamsFor(surf), "reason") {
		t.Fatalf("untyped step surface must declare reason: %+v", surf)
	}
	_, err := decodeSubmitOutcomeArgs(copilot.ToolInvocation{ToolName: submitOutcomeToolName,
		Arguments: map[string]any{"outcome": "approved", "notes": "x"}}, surf)
	var typed *submitArgsDecodeError
	if !errors.As(err, &typed) || typed.kind != "invalid_params" {
		t.Fatalf("err = %v, want invalid_params decode error", err)
	}
	if !strings.Contains(typed.message, `"notes"`) || !strings.Contains(typed.message, `"reason"`) {
		t.Errorf("message must teach the reason member, got %q", typed.message)
	}
	if strings.Contains(typed.message, "payload schema members") {
		t.Errorf("untyped surface guidance must stay reason-shaped, got %q", typed.message)
	}
}

// End-to-end: with a typed surface bound to the session, the raw handler
// reports the reason key as an unknown parameter, records invalid_params, and
// consumes a finalize attempt; the same session with the static lane accepts
// the identical call structurally (legacy lane byte-identical).
func TestHandleSubmitOutcomeRawSurfaceAwareDecode(t *testing.T) {
	p, fc, s := rebindFixture(t)
	allowed, contracts := approvedStep(t)
	if p.ensureStepSurface(context.Background(), s, allowed, contracts) == nil {
		t.Fatal("typed step must bind")
	}
	if fc.createCount != 0 {
		t.Fatalf("bind must resume (creates=%d)", fc.createCount)
	}

	res, err := p.handleSubmitOutcomeRaw("adapter-surface", copilot.ToolInvocation{ToolName: submitOutcomeToolName,
		Arguments: map[string]any{"outcome": "approved", "reason": "findings"}})
	if err != nil {
		t.Fatalf("raw handler must return ToolResult, not Go error: %v", err)
	}
	if res.ResultType == "success" {
		t.Fatalf("reason call must fail, got %+v", res)
	}
	if !strings.Contains(res.TextResultForLLM, `"reason"`) {
		t.Errorf("rejection must name the unknown reason param, got %q", res.TextResultForLLM)
	}
	s.mu.Lock()
	kind, attempts := s.finalizeFailureKind, s.finalizeAttempts
	s.mu.Unlock()
	if kind != "invalid_params" || attempts != 1 {
		t.Fatalf("recorded kind/attempts = %q/%d, want invalid_params/1", kind, attempts)
	}
}

func TestDeclaredSubmitParamsFor(t *testing.T) {
	if got := declaredSubmitParamsFor(nil); !reflect.DeepEqual(got, declaredSubmitOutcomeParams) {
		t.Fatalf("nil surface = %v, want the static declared set", got)
	}
	// Same shape, different required-comment config: declared set is stable.
	surf := submitSurfaceFromStep([]string{"approved"},
		[]*v2.OutcomeContract{contractTestContract(t, "approved", reasonRequiredSchema, true, false)})
	if !reflect.DeepEqual(declaredSubmitParamsFor(surf), []string{"comment", "outcome", "payload"}) {
		t.Fatalf("typed surface declared set = %v", declaredSubmitParamsFor(surf))
	}
}
