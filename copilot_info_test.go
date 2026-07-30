// copilot_info_test.go — tests for the adapter's declared Info schema.

package main

import (
	"context"
	"encoding/json"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// TestInfo_OutputSchemaMatchesRuntimeOutputs asserts that Info declares an
// OutputSchema whose key set exactly matches the keys the adapter populates in
// its step output map (built by resultEvent in copilot_util.go). It also
// checks that every declared field has the correct type and a non-empty
// description, and that sensitivity flags match the data the adapter emits.
func TestInfo_OutputSchemaMatchesRuntimeOutputs(t *testing.T) {
	var a copilotAdapter
	info, err := a.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	out := info.GetOutputSchema()
	if out == nil {
		t.Fatal("Info OutputSchema is nil")
	}
	schema := out.GetFields()
	if len(schema) == 0 {
		t.Fatal("Info OutputSchema has no fields")
	}

	// The runtime step output map is the only source of truth for what keys
	// the adapter can ever populate. Reconstruct it from resultEvent, which is
	// the single code site that builds ExecuteResult outputs (copilot_util.go).
	ev := resultEvent("success", "all checks passed")
	res := ev.GetResult()
	if res == nil {
		t.Fatal("resultEvent did not produce an ExecuteResult")
	}
	var runtime map[string]any
	if err := json.Unmarshal(res.GetOutputsJson(), &runtime); err != nil {
		t.Fatalf("decode runtime outputs_json: %v", err)
	}

	wantKeys := keySetAny(runtime)
	gotKeys := keySet(schema)
	if !reflect.DeepEqual(wantKeys, gotKeys) {
		t.Errorf("OutputSchema keys do not match runtime output keys:\n  runtime: %v\n  schema:  %v",
			sortedStrings(wantKeys), sortedStrings(gotKeys))
	}

	for name, field := range schema {
		if field == nil {
			t.Errorf("schema field %q is nil", name)
			continue
		}
		if field.GetType() != "string" {
			t.Errorf("schema[%q].Type = %q, want %q", name, field.GetType(), "string")
		}
		if field.GetDescription() == "" {
			t.Errorf("schema[%q].Description is empty", name)
		}
	}

	// reason is agent-authored prose and is the adapter's primary human-readable
	// output; it must NOT be marked sensitive so workflows can log it, feed it
	// to prompts, and post it to review comments without taint errors.
	if schema["reason"].GetSensitive() {
		t.Errorf("schema[reason].Sensitive = true, want false")
	}
	// outcome is a controlled vocabulary token, not sensitive data.
	if schema["outcome"].GetSensitive() {
		t.Errorf("schema[outcome].Sensitive = true, want false")
	}
}

// TestEmitManifest_OutputSchemaMatchesInfo runs the adapter binary with the
// --emit-manifest flag and verifies that the emitted manifest's output_schema
// contains the same fields as Info's OutputSchema. This is the repository's
// manifest/schema verification: it confirms that the statically declared
// schema survives the SDK's manifest conversion and matches what the adapter
// reports at runtime.
func TestEmitManifest_OutputSchemaMatchesInfo(t *testing.T) {
	root, err := moduleRoot()
	if err != nil {
		t.Fatalf("module root: %v", err)
	}

	cmd := exec.Command("go", "run", ".", "--emit-manifest")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go run . --emit-manifest: %v\n%s", err, out)
	}

	var manifest map[string]any
	if err := json.Unmarshal(out, &manifest); err != nil {
		t.Fatalf("parse emitted manifest: %v", err)
	}

	schema, ok := manifest["output_schema"].(map[string]any)
	if !ok {
		t.Fatalf("manifest output_schema missing or not an object: %v", manifest["output_schema"])
	}
	fields, ok := schema["fields"].(map[string]any)
	if !ok {
		t.Fatalf("manifest output_schema.fields missing or not an object: %v", schema["fields"])
	}

	want := map[string]struct{}{"outcome": {}, "reason": {}}
	got := keySetAny(fields)
	if !reflect.DeepEqual(want, got) {
		t.Errorf("manifest output_schema fields = %v, want %v", sortedStrings(got), sortedStrings(want))
	}

	if outcome, ok := fields["outcome"].(map[string]any); ok {
		if outcome["type"] != "string" {
			t.Errorf("manifest outcome.type = %v, want string", outcome["type"])
		}
		if outcome["description"] == "" {
			t.Error("manifest outcome.description is empty")
		}
		if s, _ := outcome["sensitive"].(bool); s {
			t.Error("manifest outcome.sensitive = true, want false")
		}
	} else {
		t.Errorf("manifest outcome field missing or malformed: %v", fields["outcome"])
	}

	if reason, ok := fields["reason"].(map[string]any); ok {
		if reason["type"] != "string" {
			t.Errorf("manifest reason.type = %v, want string", reason["type"])
		}
		if reason["description"] == "" {
			t.Error("manifest reason.description is empty")
		}
		if s, _ := reason["sensitive"].(bool); s {
			t.Error("manifest reason.sensitive = true, want false")
		}
	} else {
		t.Errorf("manifest reason field missing or malformed: %v", fields["reason"])
	}
}

// moduleRoot returns the module's root directory using `go list -m -f '{{.Dir}}'`.
func moduleRoot() (string, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func keySet(m map[string]*v2.ConfigFieldProto) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

func keySetAny(m map[string]any) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

func sortedStrings(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
