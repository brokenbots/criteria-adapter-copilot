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

// ── KB-47 acceptance conformance tests ───────────────────────────────────────

// conformanceApprovedSchema is the pinned-subset payload schema the contract
// conformance tests attach to the "approved" outcome (verdict string required;
// extra properties untouched, mirroring host-authored contracts).
const conformanceApprovedSchema = `{"type":"object","required":["verdict"],"properties":{"verdict":{"type":"string"}}}`

// conformanceContract builds an OutcomeContract for the execute request.
func conformanceContract(name string, schema string, requireComment, fallback bool) *v2.OutcomeContract {
	c := &v2.OutcomeContract{Name: name, RequireComment: requireComment, Fallback: fallback}
	if schema != "" {
		c.SchemaJson = []byte(schema)
	}
	return c
}

// conformanceExecuteOpts shapes the ExecuteRequest the KB-47 tests vary.
type conformanceExecuteOpts struct {
	prompt    string
	maxTurns  string
	allowed   []string // nil = reviewStepAllowedOutcomes
	contracts []*v2.OutcomeContract
	rejection *v2.ExecutionRejection
}

// runConformanceExecuteWith issues one Execute with the given request shape
// against the live fake-copilot child.
func runConformanceExecuteWith(t *testing.T, p *copilotAdapter, adapterSessionID string, opts conformanceExecuteOpts) *recordingSender {
	t.Helper()
	allowed := opts.allowed
	if allowed == nil {
		allowed = reviewStepAllowedOutcomes
	}
	prompt := opts.prompt
	if prompt == "" {
		prompt = "review the worktree diff and submit your verdict"
	}
	input := map[string]string{"prompt": prompt}
	if opts.maxTurns != "" {
		input["max_turns"] = opts.maxTurns
	}
	sender := &recordingSender{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := p.Execute(ctx, &v2.ExecuteRequest{
		SessionId:        adapterSessionID,
		Input:            input,
		AllowedOutcomes:  allowed,
		OutcomeContracts: opts.contracts,
		Rejection:        opts.rejection,
	}, sender)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	return sender
}

// adapterEventPayload returns the payload map for the first adapter event with
// the given kind (nil when absent).
func adapterEventPayload(sender *recordingSender, kind string) map[string]any {
	for _, ev := range sender.snapshot() {
		if a := ev.GetAdapter(); a != nil && a.GetEventKind() == kind {
			return a.GetPayload().AsMap()
		}
	}
	return nil
}

// mustOpenConformanceSessionWith opens a session carrying secrets (the secret
// channel values to redact from echoed text).
func mustOpenConformanceSessionWith(t *testing.T, p *copilotAdapter, adapterSessionID string, secrets map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := p.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: adapterSessionID, Secrets: secrets}); err != nil {
		t.Fatalf("OpenSession(%s) returned error: %v", adapterSessionID, err)
	}
}

// Acceptance "contract mode round-trip" + "verbatim payload forwarding": the
// approved finalize's payload reaches the host byte-for-byte (fixture payloads
// use alphabetically-sorted keys — the JSON-RPC decode/re-encode boundary
// canonicalizes key order) with the comment riding ExecuteResult.comment.
func TestConformanceContractRoundTripVerbatim(t *testing.T) {
	for _, payload := range []string{
		`{"note":"first-stage note","verdict":"approved"}`,
		`{"meta":{"branch":"main","deep":true},"verdict":"approved"}`,
	} {
		t.Run(payload, func(t *testing.T) {
			p := newConformanceAdapter(t, map[string]string{
				"FAKE_COPILOT_SCENARIO": "contract-payload-ok",
				"FAKE_COPILOT_OUTCOME":  "approved",
				"FAKE_COPILOT_PAYLOAD":  payload,
				"FAKE_COPILOT_COMMENT":  "LGTM",
			})
			mustOpenConformanceSession(t, p, "kb47-roundtrip")
			sender := runConformanceExecuteWith(t, p, "kb47-roundtrip", conformanceExecuteOpts{
				contracts: []*v2.OutcomeContract{conformanceContract("approved", conformanceApprovedSchema, true, false)},
			})
			r := resultFromSender(sender)
			if r.GetOutcome() != "approved" {
				t.Fatalf("outcome = %q, want approved", r.GetOutcome())
			}
			if got := string(r.GetOutputsJson()); got != payload {
				t.Errorf("outputs_json = %s, want the submitted payload verbatim: %s", got, payload)
			}
			if r.GetComment() != "LGTM" {
				t.Errorf("comment = %q, want LGTM", r.GetComment())
			}
			if a := adapterEventPayload(sender, "outcome.payload_invalid"); a != nil {
				t.Errorf("unexpected payload_invalid event: %v", a)
			}
		})
	}
}

// Acceptance "invalid_payload classification + reprompt-with-issues": the
// schema-violating first submission is rejected in-turn (event with the
// structured issues), the corrective reprompt reaches the fixture (it only
// resubmits stage 2 on a rejection-marker prompt), and the repaired payload is
// forwarded verbatim. Exhaustion diagnostics must stay silent.
func TestConformanceContractInvalidPayloadRepromptWithIssues(t *testing.T) {
	const payload2 = `{"note":"second-stage","verdict":"approved"}`
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_SCENARIO": "contract-payload-repair",
		"FAKE_COPILOT_OUTCOME":  "approved",
		"FAKE_COPILOT_PAYLOAD":  `{"verdict": 9}`,
		"FAKE_COPILOT_PAYLOAD2": payload2,
		"FAKE_COPILOT_COMMENT":  "note",
		"FAKE_COPILOT_COMMENT2": "fixed now",
	})
	mustOpenConformanceSession(t, p, "kb47-repair")
	sender := runConformanceExecuteWith(t, p, "kb47-repair", conformanceExecuteOpts{
		contracts: []*v2.OutcomeContract{conformanceContract("approved", conformanceApprovedSchema, true, false)},
	})

	a := adapterEventPayload(sender, "outcome.payload_invalid")
	if a == nil {
		t.Fatal("expected an outcome.payload_invalid adapter event")
	}
	if a["kind"] != "invalid_payload" {
		t.Errorf("kind = %v, want invalid_payload", a["kind"])
	}
	if issues, ok := a["issues"].([]any); !ok || len(issues) == 0 || !strings.Contains(fmt.Sprint(issues[0]), "verdict") {
		t.Errorf("issues = %#v, want a structured issue naming the failing path", a["issues"])
	}

	r := resultFromSender(sender)
	if r.GetOutcome() != "approved" {
		t.Fatalf("outcome = %q, want approved after repair", r.GetOutcome())
	}
	if got := string(r.GetOutputsJson()); got != payload2 {
		t.Errorf("outputs_json = %s, want the repaired payload verbatim %s", got, payload2)
	}
	if r.GetComment() != "fixed now" {
		t.Errorf("comment = %q, want the repair-stage comment", r.GetComment())
	}
	if adapterEventPayload(sender, "turn.finalize_exhausted") != nil {
		t.Error("turn.finalize_exhausted must not fire on a repaired finalize")
	}
}

// comment_missing is its own in-turn class: a schema-valid payload with an
// empty comment (require_comment contract) is rejected for the comment, and
// the repair stage resubmits both.
func TestConformanceContractCommentMissingRejection(t *testing.T) {
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_SCENARIO": "contract-payload-repair",
		"FAKE_COPILOT_OUTCOME":  "approved",
		"FAKE_COPILOT_PAYLOAD":  `{"note":"first-stage","verdict":"approved"}`,
		"FAKE_COPILOT_PAYLOAD2": `{"note":"second-stage","verdict":"approved"}`,
		"FAKE_COPILOT_COMMENT2": "second-stage comment",
	})
	mustOpenConformanceSession(t, p, "kb47-comment-missing")
	sender := runConformanceExecuteWith(t, p, "kb47-comment-missing", conformanceExecuteOpts{
		contracts: []*v2.OutcomeContract{conformanceContract("approved", conformanceApprovedSchema, true, false)},
	})

	a := adapterEventPayload(sender, "outcome.payload_invalid")
	if a == nil || a["kind"] != "comment_missing" {
		t.Fatalf("payload_invalid = %v, want kind comment_missing", a)
	}
	r := resultFromSender(sender)
	if r.GetOutcome() != "approved" || r.GetComment() != "second-stage comment" {
		t.Errorf("result = %q/%q, want approved with the stage-2 comment", r.GetOutcome(), r.GetComment())
	}
	if got := string(r.GetOutputsJson()); got != `{"note":"second-stage","verdict":"approved"}` {
		t.Errorf("outputs_json = %s, want the stage-2 payload verbatim", got)
	}
}

// Acceptance "fallback fires": a finalize loop that never submits finalizes
// the fallback contract's outcome (no outputs, no comment) instead of the
// legacy failure, keeping turn.finalize_exhausted as operator evidence.
func TestConformanceContractFallbackOnMissingFinalize(t *testing.T) {
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_SCENARIO": "missing",
	})
	mustOpenConformanceSession(t, p, "kb47-fallback")
	sender := runConformanceExecuteWith(t, p, "kb47-fallback", conformanceExecuteOpts{
		allowed: []string{"approved", "failure"},
		contracts: []*v2.OutcomeContract{
			conformanceContract("approved", conformanceApprovedSchema, false, false),
			conformanceContract("failure", "", false, true),
		},
	})
	r := resultFromSender(sender)
	if r.GetOutcome() != "failure" {
		t.Fatalf("outcome = %q, want the fallback failure", r.GetOutcome())
	}
	if len(r.GetOutputsJson()) != 0 || r.GetComment() != "" {
		t.Errorf("fallback result must carry no outputs/comment, got %s/%q", r.GetOutputsJson(), r.GetComment())
	}
	if adapterEventPayload(sender, "turn.finalize_exhausted") == nil {
		t.Error("expected turn.finalize_exhausted operator evidence")
	}
	if adapterEventPayload(sender, "outcome.failure") != nil {
		t.Error("outcome.failure must be skipped when the fallback fires")
	}
}

// Acceptance "no-contract passthrough": without OutcomeContracts the legacy
// session-state outputs assembly is byte-for-byte unchanged.
func TestConformanceNoContractPassthrough(t *testing.T) {
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_OUTCOME": "approved",
	})
	mustOpenConformanceSession(t, p, "kb47-passthrough")
	sender := runConformanceExecuteWith(t, p, "kb47-passthrough", conformanceExecuteOpts{})
	r := resultFromSender(sender)
	if r.GetOutcome() != "approved" {
		t.Fatalf("outcome = %q, want approved", r.GetOutcome())
	}
	if got := string(r.GetOutputsJson()); got != `{"outcome":"approved","reason":"step completed"}` {
		t.Errorf("legacy outputs_json drift: %s", got)
	}
	if r.GetComment() != "" {
		t.Errorf("legacy result must not carry comment, got %q", r.GetComment())
	}
}

// Acceptance "comment redaction": held secrets are redacted from the forwarded
// payload AND the comment.
func TestConformanceContractCommentRedaction(t *testing.T) {
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_SCENARIO": "contract-payload-ok",
		"FAKE_COPILOT_OUTCOME":  "approved",
		"FAKE_COPILOT_PAYLOAD":  `{"note":"the token was super-secret-token-abc","verdict":"approved"}`,
		"FAKE_COPILOT_COMMENT":  "the token was super-secret-token-abc",
	})
	mustOpenConformanceSessionWith(t, p, "kb47-redaction", map[string]string{
		"COPILOT_GITHUB_TOKEN": "super-secret-token-abc",
	})
	sender := runConformanceExecuteWith(t, p, "kb47-redaction", conformanceExecuteOpts{
		contracts: []*v2.OutcomeContract{conformanceContract("approved", conformanceApprovedSchema, true, false)},
	})
	r := resultFromSender(sender)
	if r.GetOutcome() != "approved" {
		t.Fatalf("outcome = %q, want approved", r.GetOutcome())
	}
	if got := string(r.GetOutputsJson()); strings.Contains(got, "super-secret-token-abc") {
		t.Errorf("outputs_json leaks the held secret: %s", got)
	}
	if !strings.Contains(string(r.GetOutputsJson()), "[REDACTED]") {
		t.Errorf("outputs_json missing redacted payload note: %s", r.GetOutputsJson())
	}
	if r.GetComment() != "the token was [REDACTED]" {
		t.Errorf("comment = %q, want %q", r.GetComment(), "the token was [REDACTED]")
	}
}

// Acceptance "rejection-repair prompt path": the host rejects the finalize it
// received from Execute #1; the repair execute runs a minimal prompt into the
// same live session (the fixture only resubmits stage 2 on the
// "rejected by the workflow" marker), the resubmission lands, and
// outcome.recovered precedes the terminal result.
func TestConformanceRejectionRepairInLiveSession(t *testing.T) {
	p := newConformanceAdapter(t, map[string]string{
		"FAKE_COPILOT_SCENARIO": "contract-repair",
		"FAKE_COPILOT_OUTCOME":  "approved",
		"FAKE_COPILOT_PAYLOAD":  `{"note":"first","verdict":"approved"}`,
		"FAKE_COPILOT_PAYLOAD2": `{"note":"second","verdict":"approved"}`,
		"FAKE_COPILOT_COMMENT":  "first comment",
		"FAKE_COPILOT_COMMENT2": "second comment",
	})
	mustOpenConformanceSession(t, p, "kb47-host-repair")
	contracts := []*v2.OutcomeContract{conformanceContract("approved", conformanceApprovedSchema, false, false)}

	// Execute #1: accepted and forwarded.
	sender1 := runConformanceExecuteWith(t, p, "kb47-host-repair", conformanceExecuteOpts{contracts: contracts})
	r1 := resultFromSender(sender1)
	if r1.GetOutcome() != "approved" || string(r1.GetOutputsJson()) != `{"note":"first","verdict":"approved"}` {
		t.Fatalf("execute #1 = %q/%s, want approved with the first payload verbatim", r1.GetOutcome(), r1.GetOutputsJson())
	}
	if adapterEventPayload(sender1, "outcome.recovered") != nil {
		t.Error("execute #1 must not emit outcome.recovered")
	}

	// Execute #2: the same live session receives the minimal repair prompt and
	// resubmits with the stage-2 payload.
	sender2 := runConformanceExecuteWith(t, p, "kb47-host-repair", conformanceExecuteOpts{
		contracts: contracts,
		rejection: &v2.ExecutionRejection{
			Outcome: "approved",
			Issues:  "payload_schema: verdict must name the reviewed branch",
			Attempt: 2,
		},
	})
	a := adapterEventPayload(sender2, "outcome.recovered")
	if a == nil || a["outcome"] != "approved" || a["repair_attempt"] != float64(2) {
		t.Fatalf("outcome.recovered = %v, want outcome approved repair_attempt 2", a)
	}
	r2 := resultFromSender(sender2)
	if r2.GetOutcome() != "approved" {
		t.Fatalf("execute #2 outcome = %q, want approved", r2.GetOutcome())
	}
	if got := string(r2.GetOutputsJson()); got != `{"note":"second","verdict":"approved"}` {
		t.Errorf("execute #2 outputs_json = %s, want the resubmitted payload verbatim", got)
	}
	if r2.GetComment() != "second comment" {
		t.Errorf("execute #2 comment = %q, want %q", r2.GetComment(), "second comment")
	}
	if adapterEventPayload(sender2, "outcome.payload_invalid") != nil {
		t.Error("a HOST rejection is not an in-turn payload rejection; no payload_invalid event expected")
	}
}
