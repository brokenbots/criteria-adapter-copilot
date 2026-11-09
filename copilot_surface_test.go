// copilot_surface_test.go — unit tests for the per-step submit_outcome tool
// surface construction (KB-391).

package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

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
