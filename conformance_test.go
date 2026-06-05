package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brokenbots/criteria/internal/adapter/conformance"
	"github.com/brokenbots/criteria/internal/adapterhost"
	"github.com/brokenbots/criteria/workflow"
)

var (
	testAdapterBin string
	testFakeBin    string
)

func TestMain(m *testing.M) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("resolve caller path")
	}
	moduleRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	testAdapterBin = buildBinary(moduleRoot, "./cmd/criteria-adapter-copilot", "criteria-adapter-copilot-test")
	testFakeBin = buildBinary(moduleRoot, "./cmd/criteria-adapter-copilot/testfixtures/fake-copilot", "fake-copilot-test")
	os.Exit(m.Run())
}

// TestCopilotAdapterConformance runs the full conformance suite against the
// copilot adapter binary.
//
// By default it uses the deterministic fake-copilot stub so no real copilot
// CLI or network access is required. Set COPILOT_E2E=1 to use the real copilot
// CLI instead (requires copilot CLI on PATH or CRITERIA_COPILOT_BIN set):
//
//	COPILOT_E2E=1 go test ./cmd/criteria-adapter-copilot/... -run Conformance
func TestCopilotAdapterConformance(t *testing.T) {
	opts := conformance.Options{
		Secrets: fixtureSecrets(),
		StepConfig: map[string]string{
			"prompt": "Reply with only: RESULT: success",
		},
		PermissionConfig: map[string]string{
			// Force a fetch tool invocation so the fake emits a permission
			// request. The host deny policy drives the outcome to failure (W15:
			// copilot returns failure for all permission-denied cases).
			"prompt": "Use the fetch tool (web/URL fetcher) to retrieve the contents of https://api.github.com/zen — you must invoke the tool to fetch the URL, not guess. After the fetch completes, end your response with `RESULT: success`.",
		},
		AllowedOutcomes:         []string{"success", "failure", "needs_review"},
		Streaming:               true,
		PermissionDenialOutcome: "failure",
	}

	applyFakeIfNeeded(t)
	conformance.RunAdapter(t, "copilot", testAdapterBin, opts)
}

// applyFakeIfNeeded sets CRITERIA_COPILOT_BIN to the deterministic fake
// binary unless COPILOT_E2E=1 is set, in which case whatever binary is at
// CRITERIA_COPILOT_BIN (or "copilot" on PATH) is used instead.
func applyFakeIfNeeded(t *testing.T) {
	t.Helper()
	if os.Getenv("COPILOT_E2E") != "1" {
		t.Setenv("CRITERIA_COPILOT_BIN", testFakeBin)
	}
}

// TestCopilotE2ERouting verifies the routing invariant: when COPILOT_E2E=1 is
// set, applyFakeIfNeeded must NOT override CRITERIA_COPILOT_BIN to the fake
// binary, so a real CLI (or any caller-supplied binary) is used instead.
//
// This test protects against a future refactor accidentally inverting or
// removing the guard so the fake always runs regardless of the env var.
func TestCopilotE2ERouting(t *testing.T) {
	t.Run("fake_used_when_e2e_unset", func(t *testing.T) {
		t.Setenv("COPILOT_E2E", "")
		t.Setenv("CRITERIA_COPILOT_BIN", "/some/other/binary")
		applyFakeIfNeeded(t)
		if got := os.Getenv("CRITERIA_COPILOT_BIN"); got != testFakeBin {
			t.Fatalf("expected CRITERIA_COPILOT_BIN=%q (fake), got %q", testFakeBin, got)
		}
	})

	t.Run("fake_not_used_when_e2e_set", func(t *testing.T) {
		sentinel := testAdapterBin + "-real"
		t.Setenv("COPILOT_E2E", "1")
		t.Setenv("CRITERIA_COPILOT_BIN", sentinel)
		applyFakeIfNeeded(t)
		if got := os.Getenv("CRITERIA_COPILOT_BIN"); got != sentinel {
			t.Fatalf("COPILOT_E2E=1 must not override CRITERIA_COPILOT_BIN: got %q, want %q (fake is %q)", got, sentinel, testFakeBin)
		}
	})
}

// TestCopilotAdapterBuilds verifies the adapter binary exists and is executable.
func TestCopilotAdapterBuilds(t *testing.T) {
	if _, err := os.Stat(testAdapterBin); err != nil {
		t.Fatalf("adapter binary not found at %q: %v", testAdapterBin, err)
	}
}

// TestCopilotReasoningEffortOverride (test 6.8) exercises the full
// agent-open → per-step reasoning_effort override → restore flow end-to-end
// against the fake copilot binary. It validates that:
//   - Opening a session with reasoning_effort succeeds.
//   - Executing a step with a per-step reasoning_effort override succeeds
//     and returns a valid outcome.
//   - Executing a follow-up step without per-step effort also succeeds
//     (verifying the restore did not break session state).
//
// Run by make test-conformance.
func TestCopilotReasoningEffortOverride(t *testing.T) {
	applyFakeIfNeeded(t)

	loader := adapterhost.NewLoaderWithDiscovery(func(requested string) (string, error) {
		if requested != "copilot" {
			return "", fmt.Errorf("unexpected adapter %q", requested)
		}
		return testAdapterBin, nil
	})
	t.Cleanup(func() { _ = loader.Shutdown(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plug, err := loader.Resolve(ctx, "copilot")
	if err != nil {
		t.Fatalf("resolve adapter: %v", err)
	}

	sessionID := "effort-override-test-session"

	// Open with agent-level reasoning_effort = "medium".
	if err := plug.OpenSession(ctx, sessionID, map[string]string{
		"reasoning_effort": "medium",
	}, fixtureSecrets()); err != nil {
		t.Fatalf("OpenSession with reasoning_effort=medium: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = plug.CloseSession(closeCtx, sessionID)
		cancel()
		plug.Kill()
	})

	// Step 1: execute with per-step reasoning_effort override "high".
	step1 := &workflow.StepNode{
		Name:       "planning",
		TargetKind: workflow.StepTargetAdapter,
		AdapterRef: "bot",
		Input: map[string]string{
			"prompt":           "Reply with only: RESULT: success",
			"reasoning_effort": "high",
		},
	}
	result1, err := plug.Execute(ctx, sessionID, step1, &discardSink{})
	if err != nil {
		t.Fatalf("Execute step1 (per-step effort override): %v", err)
	}
	if result1.Outcome == "" {
		t.Fatal("step1 returned empty outcome")
	}

	// Step 2: execute without per-step effort (inherits agent default after restore).
	step2 := &workflow.StepNode{
		Name:       "execution",
		TargetKind: workflow.StepTargetAdapter,
		AdapterRef: "bot",
		Input: map[string]string{
			"prompt": "Reply with only: RESULT: success",
		},
	}
	result2, err := plug.Execute(ctx, sessionID, step2, &discardSink{})
	if err != nil {
		t.Fatalf("Execute step2 (after effort restore): %v", err)
	}
	if result2.Outcome == "" {
		t.Fatal("step2 returned empty outcome after effort restore")
	}
}

// TestConformance_AllowedOutcomesPropagation (W15 Step 5.2) verifies that
// AllowedOutcomes derived from a step's declared outcome set are propagated
// end-to-end through the adapter loader to the copilot adapter, and that the
// adapter returns an outcome that is a member of the declared set.
//
// The loader's collectAllowedOutcomes helper converts step.Outcomes into
// the AllowedOutcomes field of the ExecuteRequest proto; this test exercises
// the whole pipe from StepNode declaration to Execute result.
func TestConformance_AllowedOutcomesPropagation(t *testing.T) {
	if os.Getenv("COPILOT_E2E") == "1" {
		t.Skip("skipping fake-copilot default-scenario test in COPILOT_E2E mode")
	}
	applyFakeIfNeeded(t)

	loader := adapterhost.NewLoaderWithDiscovery(func(requested string) (string, error) {
		if requested != "copilot" {
			return "", fmt.Errorf("unexpected adapter %q", requested)
		}
		return testAdapterBin, nil
	})
	t.Cleanup(func() { _ = loader.Shutdown(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plug, err := loader.Resolve(ctx, "copilot")
	if err != nil {
		t.Fatalf("resolve adapter: %v", err)
	}

	sessionID := "allowed-outcomes-propagation-test"
	if err := plug.OpenSession(ctx, sessionID, nil, fixtureSecrets()); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = plug.CloseSession(closeCtx, sessionID)
		closeCancel()
		plug.Kill()
	})

	// Declare exactly two outcomes. The loader populates AllowedOutcomes from
	// step.Outcomes so the adapter receives ["failure", "success"] (sorted).
	// The fake's default scenario submits outcome "success".
	step := &workflow.StepNode{
		Name:       "propagation-step",
		TargetKind: workflow.StepTargetAdapter,
		AdapterRef: "bot",
		Input:      map[string]string{"prompt": "test AllowedOutcomes propagation"},
		Outcomes: map[string]*workflow.CompiledOutcome{
			"success": {Next: "done"},
			"failure": {Next: "done"},
		},
	}

	result, err := plug.Execute(ctx, sessionID, step, &discardSink{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Assert exactly "success" (not just in-set).
	//
	// Rationale: the fake's default scenario calls submit_outcome("success"). With
	// correct AllowedOutcomes propagation (allowed = {"success","failure"}), the
	// tool handler accepts "success" and the step returns "success". If propagation
	// breaks (empty set), the handler rejects "success" on every attempt, exhaustion
	// returns "failure", and this assertion fails — exposing the regression.
	if result.Outcome != "success" {
		t.Fatalf("Execute returned outcome %q, want %q (AllowedOutcomes propagation may be broken)", result.Outcome, "success")
	}
}

// TestConformance_AllowedOutcomesPropagation_SetProof (W15) directly validates
// that the exact declared AllowedOutcomes set reaches the adapter, not just
// that the eventual outcome is in-set. It drives the "missing" scenario (fake
// never calls submit_outcome) so the adapter exhausts all attempts and emits
// an outcome.failure event that carries the allowed_outcomes it received. The
// test captures that event and asserts the exact set matches what was declared
// — any forwarding bug (e.g., wrong set, wrong sort, dropped entries) fails here.
func TestConformance_AllowedOutcomesPropagation_SetProof(t *testing.T) {
	if os.Getenv("COPILOT_E2E") == "1" {
		t.Skip("skipping fake-copilot scenario test in COPILOT_E2E mode")
	}
	t.Setenv("FAKE_COPILOT_SCENARIO", "missing")
	applyFakeIfNeeded(t)

	loader := adapterhost.NewLoaderWithDiscovery(func(requested string) (string, error) {
		if requested != "copilot" {
			return "", fmt.Errorf("unexpected adapter %q", requested)
		}
		return testAdapterBin, nil
	})
	t.Cleanup(func() { _ = loader.Shutdown(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plug, err := loader.Resolve(ctx, "copilot")
	if err != nil {
		t.Fatalf("resolve adapter: %v", err)
	}

	sessionID := "allowed-outcomes-setproof-test"
	if err := plug.OpenSession(ctx, sessionID, nil, fixtureSecrets()); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = plug.CloseSession(closeCtx, sessionID)
		closeCancel()
		plug.Kill()
	})

	// Declare two narrow canary outcomes. The loader converts step.Outcomes
	// into AllowedOutcomes=["canary-a","canary-b"] (sorted). The fake never
	// calls submit_outcome (missing scenario) so the adapter exhausts and emits
	// outcome.failure carrying exactly the set it received.
	step := &workflow.StepNode{
		Name:       "setproof-step",
		TargetKind: workflow.StepTargetAdapter,
		AdapterRef: "bot",
		Input:      map[string]string{"prompt": "test exact AllowedOutcomes propagation"},
		Outcomes: map[string]*workflow.CompiledOutcome{
			"canary-a": {Next: "done"},
			"canary-b": {Next: "done"},
		},
	}

	capSink := newCapturingSink()
	if _, err := plug.Execute(ctx, sessionID, step, capSink); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// The outcome.failure event's allowed_outcomes must exactly match the
	// canary set — this is the direct boundary proof.
	events := capSink.adapterEvents("outcome.failure")
	if len(events) == 0 {
		t.Fatal("expected outcome.failure adapter event from exhaustion")
	}
	data := events[0]
	raw, ok := data["allowed_outcomes"]
	if !ok {
		t.Fatal("outcome.failure event missing 'allowed_outcomes' field")
	}
	list, ok := raw.([]interface{})
	if !ok {
		t.Fatalf("allowed_outcomes type = %T, want []interface{}", raw)
	}
	want := []string{"canary-a", "canary-b"} // sorted
	if len(list) != len(want) {
		t.Fatalf("allowed_outcomes len = %d, want %d; got %v", len(list), len(want), list)
	}
	for i, w := range want {
		if s, _ := list[i].(string); s != w {
			t.Errorf("allowed_outcomes[%d] = %q, want %q", i, s, w)
		}
	}
}

// newFixtureHandle returns a Handle loaded from testAdapterBin for use in
// scenario fixture tests. The binary inherits the current process environment,
// so set FAKE_COPILOT_SCENARIO before calling this.
func newFixtureHandle(t *testing.T) adapterhost.Handle {
	t.Helper()
	loader := adapterhost.NewLoaderWithDiscovery(func(requested string) (string, error) {
		if requested != "copilot" {
			return "", fmt.Errorf("unexpected adapter %q", requested)
		}
		return testAdapterBin, nil
	})
	t.Cleanup(func() { _ = loader.Shutdown(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	plug, err := loader.Resolve(ctx, "copilot")
	if err != nil {
		t.Fatalf("resolve adapter: %v", err)
	}
	return plug
}

// fixtureSecrets is the secret map the conformance fixtures deliver on
// OpenSession. Copilot resolves its GitHub token from the secret channel
// (D69/WS45) and fails closed without one, so every fixture session supplies a
// stub token; the fake CLI does not validate it.
func fixtureSecrets() map[string]string {
	return map[string]string{"GITHUB_TOKEN": "fixture-token"}
}

// openFixtureSession opens a session on plug and registers cleanup. Returns
// a context with a 30-second deadline.
func openFixtureSession(t *testing.T, plug adapterhost.Handle, sessionID string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := plug.OpenSession(ctx, sessionID, nil, fixtureSecrets()); err != nil {
		t.Fatalf("OpenSession %q: %v", sessionID, err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = plug.CloseSession(closeCtx, sessionID)
		closeCancel()
		plug.Kill()
	})
	return ctx
}

// TestConformance_InvalidOutcomeScenario_Fixture (W15 Blocker-1a) drives the
// "invalid-outcome" fake scenario end-to-end through the real adapter binary.
//
// The fake submits "not-a-real-outcome" on turn 1 (rejected by the handler) and
// "success" on turn 2 (accepted). This validates:
//   - Both tool invocations are observable at the adapter boundary as
//     tool.invocation events, with the exact outcome argument on each call —
//     proving the invalid call was not silently swallowed by the handler.
//   - Both calls produced tool.result completion events (full SDK-boundary
//     lifecycle visibility for rejected and accepted calls alike).
//   - Exactly one outcome.finalized event is emitted with outcome="success".
//   - No outcome.failure event is emitted (step succeeded without exhausting).
func TestConformance_InvalidOutcomeScenario_Fixture(t *testing.T) {
	if os.Getenv("COPILOT_E2E") == "1" {
		t.Skip("skipping fake-copilot fixture test in COPILOT_E2E mode")
	}
	t.Setenv("FAKE_COPILOT_SCENARIO", "invalid-outcome")
	applyFakeIfNeeded(t)

	plug := newFixtureHandle(t)
	ctx := openFixtureSession(t, plug, "invalid-outcome-fixture-test")

	step := &workflow.StepNode{
		Name:       "invalid-outcome-step",
		TargetKind: workflow.StepTargetAdapter,
		AdapterRef: "bot",
		Input:      map[string]string{"prompt": "test invalid-outcome scenario"},
		Outcomes: map[string]*workflow.CompiledOutcome{
			"success": {Next: "done"},
			"failure": {Next: "done"},
		},
	}

	capSink := newCapturingSink()
	result, err := plug.Execute(ctx, "invalid-outcome-fixture-test", step, capSink)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// First call ("not-a-real-outcome") rejected; second call ("success") accepted.
	if result.Outcome != "success" {
		t.Errorf("outcome = %q, want %q", result.Outcome, "success")
	}

	// Assert both tool invocations are observable at the boundary with their
	// exact argument payloads. This is the contract-visible proof that the
	// invalid call was observed and rejected (not silently swallowed) before
	// the valid call succeeded on turn 2.
	invocations := capSink.invocationsForTool("submit_outcome")
	if len(invocations) != 2 {
		t.Fatalf("submit_outcome tool.invocation count = %d, want 2 (one per turn)", len(invocations))
	}
	// Turn 1: invalid outcome argument must be present on the first invocation.
	if args, _ := invocations[0]["arguments"].(string); !strings.Contains(args, "not-a-real-outcome") {
		t.Errorf("invocation[0] arguments = %q, want to contain %q (invalid outcome boundary-visible)", args, "not-a-real-outcome")
	}
	// Turn 2: accepted outcome argument must be present on the second invocation.
	if args, _ := invocations[1]["arguments"].(string); !strings.Contains(args, "success") {
		t.Errorf("invocation[1] arguments = %q, want to contain %q (valid outcome boundary-visible)", args, "success")
	}
	// Both calls produced completion events — the full tool-call lifecycle
	// (request → handler → result) ran for both the rejected and accepted calls.
	if n := len(capSink.adapterEvents("tool.result")); n < 2 {
		t.Errorf("tool.result event count = %d, want at least 2 (one per invocation)", n)
	}

	// Exactly one outcome.finalized event — only the valid call on turn 2.
	finalized := capSink.adapterEvents("outcome.finalized")
	if len(finalized) != 1 {
		t.Fatalf("outcome.finalized event count = %d, want 1; events: %v", len(finalized), finalized)
	}
	if got, _ := finalized[0]["outcome"].(string); got != "success" {
		t.Errorf("outcome.finalized outcome = %q, want %q", got, "success")
	}

	// No outcome.failure event (step did not exhaust).
	if failures := capSink.adapterEvents("outcome.failure"); len(failures) != 0 {
		t.Errorf("unexpected outcome.failure events: %v", failures)
	}
}

// TestConformance_DuplicateCallScenario_Fixture (W15 Blocker-1b) drives the
// "duplicate-call" fake scenario end-to-end through the real adapter binary.
//
// The fake submits submit_outcome("success") and submit_outcome("failure") in
// the same turn (no idle between them). This validates:
//   - Both tool invocations are observable at the adapter boundary as
//     tool.invocation events — proving the duplicate call was not silently
//     swallowed and its arguments ("failure") are boundary-visible.
//   - Both calls produced tool.result completion events.
//   - First call wins: outcome = "success".
//   - Exactly one outcome.finalized event (second call rejected — its rejection
//     is evidenced by the absence of a second outcome.finalized event).
func TestConformance_DuplicateCallScenario_Fixture(t *testing.T) {
	if os.Getenv("COPILOT_E2E") == "1" {
		t.Skip("duplicate-call fixture test requires deterministic fake copilot; skip in E2E mode")
	}
	t.Setenv("FAKE_COPILOT_SCENARIO", "duplicate-call")
	applyFakeIfNeeded(t)

	plug := newFixtureHandle(t)
	ctx := openFixtureSession(t, plug, "duplicate-call-fixture-test")

	step := &workflow.StepNode{
		Name:       "duplicate-call-step",
		TargetKind: workflow.StepTargetAdapter,
		AdapterRef: "bot",
		Input:      map[string]string{"prompt": "test duplicate-call scenario"},
		Outcomes: map[string]*workflow.CompiledOutcome{
			"success": {Next: "done"},
			"failure": {Next: "done"},
		},
	}

	capSink := newCapturingSink()
	result, err := plug.Execute(ctx, "duplicate-call-fixture-test", step, capSink)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// First call wins: outcome must be "success".
	if result.Outcome != "success" {
		t.Errorf("outcome = %q, want %q (first call must win)", result.Outcome, "success")
	}

	// Assert both tool invocations are observable at the boundary — proves the
	// duplicate call was not silently swallowed and its arguments are visible
	// as boundary evidence of the rejected attempt.
	invocations := capSink.invocationsForTool("submit_outcome")
	if len(invocations) != 2 {
		t.Fatalf("submit_outcome tool.invocation count = %d, want 2 (both calls boundary-visible)", len(invocations))
	}
	// First call carried "success".
	if args, _ := invocations[0]["arguments"].(string); !strings.Contains(args, "success") {
		t.Errorf("invocation[0] arguments = %q, want to contain %q (first call)", args, "success")
	}
	// Second (duplicate) call carried "failure" — its argument is the boundary
	// proof that the call arrived and was processed (not silently dropped).
	if args, _ := invocations[1]["arguments"].(string); !strings.Contains(args, "failure") {
		t.Errorf("invocation[1] arguments = %q, want to contain %q (duplicate call boundary-visible)", args, "failure")
	}
	// Both calls completed the full tool-call lifecycle at the boundary level.
	if n := len(capSink.adapterEvents("tool.result")); n < 2 {
		t.Errorf("tool.result event count = %d, want at least 2 (one per invocation)", n)
	}

	// Exactly ONE outcome.finalized event — the duplicate call was rejected,
	// evidenced by the absence of a second outcome.finalized event.
	finalized := capSink.adapterEvents("outcome.finalized")
	if len(finalized) != 1 {
		t.Fatalf("outcome.finalized event count = %d, want 1 (duplicate rejected); events: %v", len(finalized), finalized)
	}
	if got, _ := finalized[0]["outcome"].(string); got != "success" {
		t.Errorf("outcome.finalized outcome = %q, want %q", got, "success")
	}
}

// discardSink is a no-op adapter.EventSink used in conformance tests that only
// need to verify outcomes and errors rather than individual events.
type discardSink struct{}

func (*discardSink) Log(_ string, _ []byte)  {}
func (*discardSink) Adapter(_ string, _ any) {}

// capturingSink is a thread-safe adapter.EventSink that records all adapter
// events for later inspection by fixture tests.
type capturingSink struct {
	mu     sync.Mutex
	events []capturedAdapterEvent
}

type capturedAdapterEvent struct {
	kind string
	data map[string]any
}

func newCapturingSink() *capturingSink { return &capturingSink{} }

func (*capturingSink) Log(_ string, _ []byte) {}

func (c *capturingSink) Adapter(kind string, data any) {
	var m map[string]any
	if data != nil {
		m, _ = data.(map[string]any)
	}
	c.mu.Lock()
	c.events = append(c.events, capturedAdapterEvent{kind: kind, data: m})
	c.mu.Unlock()
}

// adapterEvents returns the data payloads for all captured events of the given kind.
func (c *capturingSink) adapterEvents(kind string) []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []map[string]any
	for _, ev := range c.events {
		if ev.kind == kind {
			out = append(out, ev.data)
		}
	}
	return out
}

// invocationsForTool returns tool.invocation events whose "name" field matches
// toolName. The adapter emits one tool.invocation per external tool call, so
// this gives the ordered list of calls made to that tool during the step.
func (c *capturingSink) invocationsForTool(toolName string) []map[string]any {
	var out []map[string]any
	for _, ev := range c.adapterEvents("tool.invocation") {
		if name, _ := ev["name"].(string); name == toolName {
			out = append(out, ev)
		}
	}
	return out
}

// buildBinary compiles the package at pkgPath in moduleRoot and writes the
// binary to os.TempDir(). It panics if compilation fails.
func buildBinary(moduleRoot, pkgPath, binName string) string {
	out := filepath.Join(os.TempDir(), binName)
	cmd := exec.Command("go", "build", "-o", out, pkgPath)
	cmd.Dir = moduleRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		panic("build " + pkgPath + ": " + err.Error() + "\n" + string(output))
	}
	return out
}
