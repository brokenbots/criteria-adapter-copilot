// copilot_session_test.go — KB-71: the submit_outcome tool-presentation
// probe. The finalize loop can only converge when the session actually
// serves submit_outcome to the model; these tests pin the probe's verdicts
// (present → open, verified-missing → loud failure + teardown, inconclusive
// → warn and proceed) and its wiring into openSDKSession for both the resume
// and create paths.

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	copilot "github.com/github/copilot-sdk/go"
)

// servedTools builds the scripted metadata for a session that serves the
// standard adapter tool set.
func servedTools(names ...string) []string {
	return names
}

// bareSession strips the optional tool-metadata capability from a session,
// scripting an SDK session type that predates session.tools.getCurrentMetadata.
type bareSession struct {
	copilotSession
}

func TestVerifySubmitOutcomePresentedAcceptsPresentTool(t *testing.T) {
	fake := &fakeSession{sessionID: "sdk-probe-ok", toolNames: servedTools("bash", "submit_outcome", "adapter_tool")}
	if err := verifySubmitOutcomePresented(context.Background(), fake); err != nil {
		t.Fatalf("verifySubmitOutcomePresented err = %v, want nil when submit_outcome is served", err)
	}
	if fake.disconnectCount != 0 {
		t.Fatalf("a passing probe must not disconnect the session (disconnects = %d)", fake.disconnectCount)
	}
}

func TestVerifySubmitOutcomePresentedFailsLoudlyWhenMissing(t *testing.T) {
	fake := &fakeSession{sessionID: "sdk-probe-missing", toolNames: servedTools("bash", "adapter_tool")}
	err := verifySubmitOutcomePresented(context.Background(), fake)
	if err == nil {
		t.Fatal("verifySubmitOutcomePresented err = nil, want a loud failure when submit_outcome is absent from the served list")
	}
	if !strings.Contains(err.Error(), submitOutcomeToolName) {
		t.Errorf("err = %v, want it to name %q", err, submitOutcomeToolName)
	}
	// The served list must be in the error so the operator sees what the CLI
	// actually presented (KB-71 diagnostics posture).
	if !strings.Contains(err.Error(), "bash") || !strings.Contains(err.Error(), "adapter_tool") {
		t.Errorf("err = %v, want the served tool list included", err)
	}
}

func TestVerifySubmitOutcomePresentedInconclusiveWhenCapabilityMissing(t *testing.T) {
	fake := &fakeSession{sessionID: "sdk-probe-cap"}
	bare := bareSession{copilotSession: fake}
	if err := verifySubmitOutcomePresented(context.Background(), bare); err != nil {
		t.Fatalf("verifySubmitOutcomePresented err = %v, want nil (inconclusive, warn-and-proceed)", err)
	}
}

func TestVerifySubmitOutcomePresentedInconclusiveOnRPCError(t *testing.T) {
	fake := &fakeSession{sessionID: "sdk-probe-rpc", toolNamesErr: context.DeadlineExceeded}
	if err := verifySubmitOutcomePresented(context.Background(), fake); err != nil {
		t.Fatalf("verifySubmitOutcomePresented err = %v, want nil (RPC error is inconclusive, not proof of absence)", err)
	}
}

func TestVerifySubmitOutcomePresentedInconclusiveOnEmptyMetadata(t *testing.T) {
	// nil scripts the CLI's null metadata ("tools not initialized yet").
	fake := &fakeSession{sessionID: "sdk-probe-nil", toolNames: nil}
	if err := verifySubmitOutcomePresented(context.Background(), fake); err != nil {
		t.Fatalf("verifySubmitOutcomePresented err = %v, want nil (empty metadata is inconclusive)", err)
	}
}

func TestVerifySubmitOutcomePresentedProbeDeadlineHonored(t *testing.T) {
	orig := sessionProbeTimeout
	sessionProbeTimeout = 30 * time.Millisecond
	t.Cleanup(func() { sessionProbeTimeout = orig })

	release := make(chan struct{})
	fake := &fakeSession{sessionID: "sdk-probe-hang", toolNamesBlock: release, toolNames: servedTools("adapter_tool")}
	defer close(release)

	start := time.Now()
	err := verifySubmitOutcomePresented(context.Background(), fake)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("probe took %v, want bounded by the sessionProbeTimeout window", elapsed)
	}
	if err != nil {
		t.Fatalf("verifySubmitOutcomePresented err = %v, want nil (a hung probe is inconclusive, not proof of absence)", err)
	}
}

// TestOpenSDKSessionCreateFailsLoudlyWhenToolMissing: a fresh CLI session
// that serves everything EXCEPT submit_outcome must fail the open with the
// served tool list in the error, disconnect the unusable session, and not
// persist the SDK session id (so later respawns do not resume it).
func TestOpenSDKSessionCreateFailsLoudlyWhenToolMissing(t *testing.T) {
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	fcs := &fakeSession{sessionID: "sdk-missing-tool", toolNames: servedTools("bash", "adapter_tool")}
	fc.handOut = fcs
	sc := &copilot.SessionConfig{Model: "m"}
	sess, resumed, err := p.openSDKSession(context.Background(), fc, "adapter-missing-tool", sc, buildResumeConfig(sc))
	if err == nil {
		t.Fatal("openSDKSession err = nil, want a loud failure when the created session omits submit_outcome")
	}
	if sess != nil || resumed {
		t.Fatalf("openSDKSession = (%v, %v), want (nil, false) on probe failure", sess, resumed)
	}
	if !strings.Contains(err.Error(), submitOutcomeToolName) || !strings.Contains(err.Error(), "adapter_tool") {
		t.Errorf("err = %v, want it to name the missing tool and the served list", err)
	}
	if fcs.disconnectCount != 1 {
		t.Fatalf("disconnects = %d, want 1 (the unusable session is torn down)", fcs.disconnectCount)
	}
	if _, statErr := os.Stat(sdkSessionIDPath("adapter-missing-tool")); !os.IsNotExist(statErr) {
		t.Errorf("sdk session id persist stat = %v, want the file absent (a failed session is not resumable)", statErr)
	}
}

// TestOpenSDKSessionCreateAcceptsServedToolAndPersistsID: a healthy fresh
// session passes the probe, is returned, and its SDK id is persisted for
// respawn-resume.
func TestOpenSDKSessionCreateAcceptsServedToolAndPersistsID(t *testing.T) {
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)

	fcs := &fakeSession{sessionID: "sdk-with-tool", toolNames: servedTools("bash", "submit_outcome", "adapter_tool")}
	fc.handOut = fcs
	sc := &copilot.SessionConfig{Model: "m"}
	sess, resumed, err := p.openSDKSession(context.Background(), fc, "adapter-with-tool", sc, buildResumeConfig(sc))
	if err != nil {
		t.Fatalf("openSDKSession err = %v, want nil", err)
	}
	if sess != fcs || resumed {
		t.Fatalf("openSDKSession = (%v, %v), want the created session and resumed=false", sess, resumed)
	}
	if fcs.disconnectCount != 0 {
		t.Fatalf("disconnects = %d, want 0", fcs.disconnectCount)
	}
	data, statErr := os.ReadFile(sdkSessionIDPath("adapter-with-tool"))
	if statErr != nil {
		t.Fatalf("sdk session id not persisted: %v", statErr)
	}
	if strings.TrimSpace(string(data)) != "sdk-with-tool" {
		t.Errorf("persisted id = %q, want %q", string(data), "sdk-with-tool")
	}
}

// TestOpenSDKSessionResumeMissingToolFallsBackToCreate (KB-71/KB-64): a
// resumed session whose served tool set dropped submit_outcome must be torn
// down and replaced by a fresh create rather than entering the finalize loop
// against a session the model cannot finalize with.
func TestOpenSDKSessionResumeMissingToolFallsBackToCreate(t *testing.T) {
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)
	t.Setenv("CRITERIA_HOME", t.TempDir())

	fcs := &fakeSession{sessionID: "sdk-missing-tool", toolNames: servedTools("bash", "adapter_tool")}
	fc.handOut = fcs
	persistSDKSessionID("adapter-fallback", "sdk-persisted")

	sc := &copilot.SessionConfig{Model: "m"}
	sess, resumed, err := p.openSDKSession(context.Background(), fc, "adapter-fallback", sc, buildResumeConfig(sc))
	// fakeClient hands the same session back for both resume and create, so
	// the create also probes missing and fails loudly; what matters is that
	// BOTH probes ran: resume was rejected and a create was attempted.
	if err == nil {
		t.Fatal("openSDKSession err = nil, want the create's loud probe failure (scripted session still omits submit_outcome)")
	}
	if sess != nil || resumed {
		t.Fatalf("openSDKSession = (%v, %v), want (nil, false)", sess, resumed)
	}
	if fcs.disconnectCount != 2 {
		t.Fatalf("disconnects = %d, want 2 (the rejected resume AND the failed create are torn down)", fcs.disconnectCount)
	}
	if len(fc.resumeIDs) == 0 || fc.resumeIDs[0] != "sdk-persisted" {
		t.Fatalf("resume ids = %v, want the persisted sdk session resumed first", fc.resumeIDs)
	}
	if fc.createCount != 1 {
		t.Fatalf("create count = %d, want 1 (resume fell through to a fresh create)", fc.createCount)
	}
}

// TestOpenSDKSessionResumeServedToolWins: a resumed session that still serves
// submit_outcome is used as-is (resumed=true), no create is attempted.
func TestOpenSDKSessionResumeServedToolWins(t *testing.T) {
	fc := &fakeClient{pingErr: nil}
	p := withRecoverableClient(t, fc)
	t.Setenv("CRITERIA_HOME", t.TempDir())

	fcs := &fakeSession{sessionID: "sdk-persisted", toolNames: servedTools("bash", "submit_outcome", "adapter_tool")}
	fc.handOut = fcs
	persistSDKSessionID("adapter-resume-ok", "sdk-persisted")

	sc := &copilot.SessionConfig{Model: "m"}
	sess, resumed, err := p.openSDKSession(context.Background(), fc, "adapter-resume-ok", sc, buildResumeConfig(sc))
	if err != nil {
		t.Fatalf("openSDKSession err = %v, want nil", err)
	}
	if sess != fcs || !resumed {
		t.Fatalf("openSDKSession = (%v, %v), want the resumed session, resumed=true", sess, resumed)
	}
	if fc.createCount != 0 {
		t.Fatalf("create count = %d, want 0 (the healthy session was resumed, not recreated)", fc.createCount)
	}
}
