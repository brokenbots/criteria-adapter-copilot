package main

// copilot_conformance_test.go drives the adapter end-to-end against a real
// child process running the scripted testfixtures/fake-copilot binary
// (KB-71). Unlike the fakeClient/fakeSession unit tests, these exercise the
// full Execute path: SDK wire protocol, session transport, the
// submit_outcome tool contract, and the finalize-reprompt loop.
//
// Coverage contract (workstream KB-71):
//  1. submit_outcome is offered in the session's served tool set for the
//     review-step session shape (presentation probe agrees with the child).
//  2. A no-submit turn followed by one corrective reprompt converges into a
//     submitted outcome on attempt 2.
//  3. When the reviewer answers every reprompt with prose and never
//     submits, turn.finalize_exhausted carries the per-reprompt reply text
//     so a future failure distinguishes "answered prose" from "empty turn".
//  4. A session that provably does NOT serve submit_outcome fails the open
//     loudly instead of burning the finalize budget.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

var fakeCopilotBinPath string

// TestMain builds the scripted fake-copilot fixture binary once for the
// whole package so conformance tests spawn it as a genuine child CLI.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "fake-copilot-bin-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "conformance: tempdir: %v\n", err)
		os.Exit(1)
	}
	fakeCopilotBinPath = filepath.Join(dir, "fake-copilot")
	goTool, err := goToolPath()
	if err == nil {
		build := exec.Command(goTool, "build", "-o", fakeCopilotBinPath, "./testfixtures/fake-copilot")
		build.Stderr = os.Stderr
		err = build.Run()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "conformance: build testfixtures/fake-copilot: %v\n", err)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// goToolPath locates a go toolchain for building the fixture: PATH first,
// then the GOROOT of the toolchain that compiled this test binary.
func goToolPath() (string, error) {
	if p, err := exec.LookPath("go"); err == nil {
		return p, nil
	}
	p := filepath.Join(runtime.GOROOT(), "bin", "go")
	if st, err := os.Stat(p); err == nil && !st.IsDir() {
		return p, nil
	}
	return "", fmt.Errorf("no go toolchain on PATH and %s does not exist", p)
}

// newConformanceAdapter returns a fresh adapter pointed at the fake-copilot
// child, with per-test env (scenario overrides) inherited by the child via
// os.Environ(). Serial only: tests mutate the process environment.
func newConformanceAdapter(t *testing.T, env map[string]string) *copilotAdapter {
	t.Helper()
	t.Setenv("CRITERIA_COPILOT_BIN", fakeCopilotBinPath)
	t.Setenv("CRITERIA_HOME", t.TempDir())
	for k, v := range env {
		t.Setenv(k, v)
	}
	p := newCopilotAdapter()
	t.Cleanup(func() {
		p.clientMu.Lock()
		client := p.client
		p.clientMu.Unlock()
		if client != nil {
			_ = client.Stop()
		}
	})
	return p
}

func mustOpenConformanceSession(t *testing.T, p *copilotAdapter, adapterSessionID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := p.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: adapterSessionID}); err != nil {
		t.Fatalf("OpenSession(%s) returned error: %v", adapterSessionID, err)
	}
}

// conformanceSessionTools reads the served tool names directly off the live
// sdkSession the adapter stored.
func conformanceSessionTools(t *testing.T, p *copilotAdapter, adapterSessionID string) []string {
	t.Helper()
	st, ok := p.sessions[adapterSessionID]
	if !ok || st == nil {
		t.Fatalf("adapter has no session state for %q", adapterSessionID)
	}
	src, ok := st.session.(interface {
		CurrentToolMetadata(context.Context) ([]string, error)
	})
	if !ok {
		t.Fatalf("session for %q does not expose CurrentToolMetadata", adapterSessionID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	names, err := src.CurrentToolMetadata(ctx)
	if err != nil {
		t.Fatalf("CurrentToolMetadata: %v", err)
	}
	return names
}

// reviewStepAllowedOutcomes is the outcome contract a review step presents
// (develop steps use success/failure instead).
var reviewStepAllowedOutcomes = []string{"approved", "changes_requested", "failure", "need_help"}

func runConformanceExecute(t *testing.T, p *copilotAdapter, adapterSessionID string) *recordingSender {
	t.Helper()
	sender := &recordingSender{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := p.Execute(ctx, &v2.ExecuteRequest{
		SessionId:       adapterSessionID,
		Input:           map[string]string{"prompt": "review the worktree diff and submit your verdict"},
		AllowedOutcomes: reviewStepAllowedOutcomes,
	}, sender)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	return sender
}

// TestConformanceReviewSessionServesSubmitOutcome: workstream item (1) — for
// the review-step session shape, the open succeeds and the session's served
// tool set includes submit_outcome (the adapter binds it) alongside the
// adapter_tool cross-adapter bridge, so the corrective reprompt never
// instructs an action the model cannot take.
func TestConformanceReviewSessionServesSubmitOutcome(t *testing.T) {
	p := newConformanceAdapter(t, nil)
	mustOpenConformanceSession(t, p, "conf-served")

	names := conformanceSessionTools(t, p, "conf-served")
	for _, want := range []string{submitOutcomeToolName, adapterToolToolName} {
		if !slices.Contains(names, want) {
			t.Errorf("served tools %v missing %q", names, want)
		}
	}
}

// TestConformanceReviewVerdictAfterReprompt: workstream item (2) — a review
// turn that idles without submitting, followed by the corrective reprompt,
// converges: the reviewer submits an allowed review outcome on attempt 2.
func TestConformanceReviewVerdictAfterReprompt(t *testing.T) {
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_SCENARIO": "success-after-reprompt-1",
		"FAKE_COPILOT_OUTCOME":  "approved",
	})
	mustOpenConformanceSession(t, p, "conf-verdict")

	sender := runConformanceExecute(t, p, "conf-verdict")
	assertOutcome(t, sender, "approved")
	if dbg := findAdapterEvent(t, sender, "turn.finalize_exhausted"); dbg != nil {
		t.Errorf("turn.finalize_exhausted fired on a converging leg: %#v", dbg)
	}
}

// TestConformanceReviewAnswersProseCarriesRepromptReplies: workstream item
// (3) — the review-leg refusal shape (every turn answered in prose, zero
// submit_outcome attempts) must end in a failure result whose
// turn.finalize_exhausted diagnostics include the per-reprompt reply text.
func TestConformanceReviewAnswersProseCarriesRepromptReplies(t *testing.T) {
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_SCENARIO": "review-answers-prose",
	})
	mustOpenConformanceSession(t, p, "conf-prose")

	sender := runConformanceExecute(t, p, "conf-prose")
	assertOutcome(t, sender, "failure")

	dbg := findAdapterEvent(t, sender, "turn.finalize_exhausted")
	if dbg == nil {
		t.Fatalf("no turn.finalize_exhausted event in payload")
	}
	if got, _ := dbg["finalize_kind"].(string); got != "missing" {
		t.Errorf("finalize_kind = %v, want missing", got)
	}
	if got, _ := dbg["attempts"].(float64); got != 0 {
		t.Errorf("attempts = %v, want 0 (the reviewer never submitted)", got)
	}
	if got, _ := dbg["idle_signals"].(float64); got != 3 {
		t.Errorf("idle_signals = %v, want 3 (bounded finalize loop)", got)
	}
	replies, _ := dbg["reprompt_replies"].([]any)
	if len(replies) != 2 {
		t.Fatalf("reprompt_replies = %#v, want 2 entries (one per reprompt)", dbg["reprompt_replies"])
	}
	// The fixture answers every reprompted turn in prose with a fresh
	// message id, so both entries carry that turn's reply text.
	want2 := "prose answer turn 2: still examining the diff, no outcome yet"
	want3 := "prose answer turn 3: still examining the diff, no outcome yet"
	if got, _ := replies[0].(string); got != want2 {
		t.Errorf("reprompt_replies[0] = %q, want %q", got, want2)
	}
	if got, _ := replies[1].(string); got != want3 {
		t.Errorf("reprompt_replies[1] = %q, want %q", got, want3)
	}
	if msg, _ := dbg["last_agent_message"].(string); msg != want3 {
		t.Errorf("last_agent_message = %q, want %q (the prose answer)", msg, want3)
	}
}

// TestConformanceMissingSubmitOutcomeFailsOpenLoudly: the structural probe
// must fail the open LOUDLY when a session provably does not serve
// submit_outcome (fixture serving a non-empty list with the tool omitted),
// naming the missing tool and the served set, and persisting no SDK session
// id for a later silent resume.
func TestConformanceMissingSubmitOutcomeFailsOpenLoudly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CRITERIA_COPILOT_BIN", fakeCopilotBinPath)
	t.Setenv("CRITERIA_HOME", home)
	t.Setenv("FAKE_COPILOT_OMIT_TOOLS", submitOutcomeToolName)
	p := &copilotAdapter{}
	t.Cleanup(func() {
		p.clientMu.Lock()
		client := p.client
		p.clientMu.Unlock()
		if client != nil {
			_ = client.Stop()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := p.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: "conf-omit"})
	if err == nil {
		t.Fatalf("OpenSession succeeded without submit_outcome in the served tool set")
	}
	for _, want := range []string{submitOutcomeToolName, "bash"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("OpenSession error %q does not mention %q", err, want)
		}
	}
	if _, statErr := os.Stat(sdkSessionIDPath("conf-omit")); statErr == nil {
		t.Errorf("SDK session id persisted despite the failed open (silent-resume risk)")
	}
}
