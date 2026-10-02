// copilot_state_test.go — tests for the declared state contract (CRI-206):
// the Info state descriptor, Snapshot ref-token capture, Restore
// exact-address reattach, and the loud terminal reattach-failure path.

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// TestInfo_StateDeclaration pins the CRI-206 declaration on the wire contract:
// mode ref (token to adapter-owned Copilot SDK conversation), the ref schema
// tag, per-turn granularity, and the host-default max_bytes (a ref token is a
// single short string).
func TestInfo_StateDeclaration(t *testing.T) {
	var a copilotAdapter
	info, err := a.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	d := info.GetState()
	if d == nil {
		t.Fatal("InfoResponse.State is nil: adapter declares no state contract")
	}
	if d.GetMode() != "ref" {
		t.Errorf("State.Mode = %q, want %q (host persists the token, never the payload)", d.GetMode(), "ref")
	}
	if d.GetSchema() != copilotStateSchema {
		t.Errorf("State.Schema = %q, want %q", d.GetSchema(), copilotStateSchema)
	}
	if d.GetGranularity() != "per-turn" {
		t.Errorf(
			"State.Granularity = %q, want %q (develop turns run tens of minutes; per-turn checkpoints make resume meaningful)",
			d.GetGranularity(), "per-turn")
	}
	if d.GetMaxBytes() != 0 {
		t.Errorf("State.MaxBytes = %d, want 0 (host default applies for a ref token)", d.GetMaxBytes())
	}
}

func newSnapshotTestSession(t *testing.T, id, sdkSessionID string, fc *fakeClient) *copilotAdapter {
	t.Helper()
	p := withRecoverableClient(t, fc)
	// newWatchdogSession registers the session on p.
	newWatchdogSession(t, p, id, &fakeSession{sessionID: sdkSessionID})
	return p
}

// TestSnapshot_ReturnsSessionRefToken: Snapshot hands the host the address of
// the live conversation with the declared schema version.
func TestSnapshot_ReturnsSessionRefToken(t *testing.T) {
	p := newSnapshotTestSession(t, "main", "sdk-abc-123", &fakeClient{pingErr: nil})

	resp, err := p.Snapshot(context.Background(), &v2.SnapshotRequest{SessionId: "main"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if string(resp.GetState()) != "sdk-abc-123" {
		t.Errorf("Snapshot state = %q, want the live SDK session id", resp.GetState())
	}
	if resp.GetSchemaVersion() != copilotStateSchemaVersion {
		t.Errorf("Snapshot schema_version = %d, want %d", resp.GetSchemaVersion(), copilotStateSchemaVersion)
	}
}

func requireStatus(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error code %v, got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("error code = %v, want %v: %v", got, want, err)
	}
}

// TestSnapshot_UnknownSessionIsLoud: a snapshot for a session the adapter
// never opened must fail, not synthesize state.
func TestSnapshot_UnknownSessionIsLoud(t *testing.T) {
	p := newSnapshotTestSession(t, "main", "sdk-x", &fakeClient{pingErr: nil})

	_, err := p.Snapshot(context.Background(), &v2.SnapshotRequest{SessionId: "ghost"})
	requireStatus(t, err, codes.NotFound)
}

// TestSnapshot_DeadSessionIsLoud: no live conversation → nothing to checkpoint
// (the engine treats this as a failure rather than storing an empty ref).
func TestSnapshot_DeadSessionIsLoud(t *testing.T) {
	p := withRecoverableClient(t, &fakeClient{pingErr: nil})
	// A state with no live conversation behind it (empty SDK session id).
	p.sessions["main"] = newSessionState("main", &fakeSession{sessionID: ""}, nil, nil, nil, p)

	_, err := p.Snapshot(context.Background(), &v2.SnapshotRequest{SessionId: "main"})
	requireStatus(t, err, codes.Internal)
}

// TestRestore_ReattachesToExactAddress: the state payload is used verbatim as
// the resume token — no ranked/fuzzy resolution — and the session is swapped
// onto the reattached conversation.
func TestRestore_ReattachesToExactAddress(t *testing.T) {
	fc := &fakeClient{pingErr: nil, handOut: &fakeSession{sessionID: "sdk-reattached"}}
	p := newSnapshotTestSession(t, "main", "sdk-old", fc)

	if _, err := p.Restore(context.Background(), &v2.RestoreRequest{
		SessionId:     "main",
		State:         []byte("sdk-checkpoint-9"),
		SchemaVersion: copilotStateSchemaVersion,
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	fc.mu.Lock()
	resumes := append([]string(nil), fc.resumeIDs...)
	fc.mu.Unlock()
	if len(resumes) != 1 || resumes[0] != "sdk-checkpoint-9" {
		t.Fatalf("ResumeSessionWithOptions calls = %v, want exactly [%q] (exact-address reattach)",
			resumes, "sdk-checkpoint-9")
	}

	s := p.getSession("main")
	if s == nil {
		t.Fatal("session vanished after restore")
	}
	if got := s.currentSession().SessionID(); got != "sdk-reattached" {
		t.Fatalf("attached session = %q, want the reattached conversation; conversation continuity broken", got)
	}

	// The ref token is adapter-owned state; the local persistence keeps the
	// adapter's own recovery (CRI-272) pointed at the same address.
	if got := loadPersistedSDKSessionID("main"); got != "sdk-checkpoint-9" {
		t.Fatalf("persisted sdk session id = %q, want %q", got, "sdk-checkpoint-9")
	}
}

// TestRestore_AlreadyAttachedSkipsSecondResume: the open path (CRI-272) may
// already have reattached to the checkpointed address; a byte-equal token is
// the same address and does not re-resume.
func TestRestore_AlreadyAttachedSkipsSecondResume(t *testing.T) {
	fc := &fakeClient{pingErr: nil}
	p := newSnapshotTestSession(t, "main", "sdk-live-7", fc)

	if _, err := p.Restore(context.Background(), &v2.RestoreRequest{
		SessionId:     "main",
		State:         []byte("sdk-live-7"),
		SchemaVersion: copilotStateSchemaVersion,
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	fc.mu.Lock()
	resumes := append([]string(nil), fc.resumeIDs...)
	fc.mu.Unlock()
	if len(resumes) != 0 {
		t.Fatalf("ResumeSessionWithOptions called %v for an already-attached exact address", resumes)
	}
}

// TestRestore_SchemaVersionMismatchIsLoudTypedError: a restore carrying a
// state shape this adapter cannot read is refused with the typed
// SnapshotVersionMismatch detail and no reattach attempt.
func TestRestore_SchemaVersionMismatchIsLoudTypedError(t *testing.T) {
	fc := &fakeClient{pingErr: nil}
	p := newSnapshotTestSession(t, "main", "sdk-live-7", fc)

	_, err := p.Restore(context.Background(), &v2.RestoreRequest{
		SessionId:     "main",
		State:         []byte("sdk-checkpoint-9"),
		SchemaVersion: copilotStateSchemaVersion + 1,
	})
	requireStatus(t, err, codes.FailedPrecondition)

	st, ok := status.FromError(err)
	if !ok || len(st.Details()) != 1 {
		t.Fatalf("error missing SnapshotVersionMismatch detail: %v", err)
	}
	mismatch, ok := st.Details()[0].(*v2.SnapshotVersionMismatch)
	if !ok {
		t.Fatalf("detail type = %T, want *v2.SnapshotVersionMismatch", st.Details()[0])
	}
	if mismatch.GetHave() != copilotStateSchemaVersion+1 || mismatch.GetWant() != copilotStateSchemaVersion {
		t.Errorf("mismatch = have %d want %d, wrong direction", mismatch.GetHave(), mismatch.GetWant())
	}

	fc.mu.Lock()
	resumes := append([]string(nil), fc.resumeIDs...)
	fc.mu.Unlock()
	if len(resumes) != 0 {
		t.Fatalf("ResumeSessionWithOptions called %v despite schema mismatch", resumes)
	}
}

// TestRestore_ReattachFailureIsLoudAndTerminal is the wrong-state failure
// class regression test: when the harness rejects the reattach, Restore fails
// loudly and does NOT silently fall back to a fresh session (which would
// restore into a plausible-looking but semantically foreign conversation).
func TestRestore_ReattachFailureIsLoudAndTerminal(t *testing.T) {
	wantErr := errors.New("session not found")
	fc := &fakeClient{pingErr: nil, resumeErr: wantErr}
	p := newSnapshotTestSession(t, "main", "sdk-old", fc)

	_, err := p.Restore(context.Background(), &v2.RestoreRequest{
		SessionId:     "main",
		State:         []byte("sdk-checkpoint-gone"),
		SchemaVersion: copilotStateSchemaVersion,
	})
	if err == nil {
		t.Fatal("Restore succeeded despite a failed reattach: silent wrong-state start")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Restore err = %v, want wrapped original error %v", err, wantErr)
	}

	// No fresh session may be created as a fallback.
	fc.mu.Lock()
	creates, resumes := fc.createCount, len(fc.resumeIDs)
	fc.mu.Unlock()
	if creates != 0 || resumes != 1 {
		t.Fatalf("after failed reattach: createCount=%d resumes=%d, want 0 creates and 1 attempt"+
			" (loud and terminal; no fresh-session fallback)", creates, resumes)
	}
	// The adapter session stays on its prior conversation, not a fresh one.
	if got := p.getSession("main").currentSession().SessionID(); got != "sdk-old" {
		t.Fatalf("attached session after failed restore = %q, want unchanged %q", got, "sdk-old")
	}
}

func TestRestore_UnknownSessionIsLoud(t *testing.T) {
	p := newSnapshotTestSession(t, "main", "sdk-x", &fakeClient{pingErr: nil})

	_, err := p.Restore(context.Background(), &v2.RestoreRequest{
		SessionId:     "ghost",
		State:         []byte("sdk-checkpoint-9"),
		SchemaVersion: copilotStateSchemaVersion,
	})
	requireStatus(t, err, codes.NotFound)
}

// TestRestore_EmptyStateRefusesToGuess: an empty token is not an address.
func TestRestore_EmptyStateRefusesToGuess(t *testing.T) {
	fc := &fakeClient{pingErr: nil}
	p := newSnapshotTestSession(t, "main", "sdk-live-7", fc)

	_, err := p.Restore(context.Background(), &v2.RestoreRequest{
		SessionId:     "main",
		State:         []byte("   "),
		SchemaVersion: copilotStateSchemaVersion,
	})
	requireStatus(t, err, codes.InvalidArgument)

	fc.mu.Lock()
	resumes := len(fc.resumeIDs)
	fc.mu.Unlock()
	if resumes != 0 {
		t.Fatalf("ResumeSessionWithOptions called %v times with an empty address", resumes)
	}
}

// TestRestore_RacingRecoverySweepKeepsRestoredAddress (KB-65): a recovery
// sweep (recoverTransport → reopenSession) running concurrently with Restore
// must not re-attach the adapter to the pre-Restore persisted id after
// Restore's swap. Restore used to run its check/swap/persist outside
// reopenMu, so a sweep that resumed the stale persisted id could land its
// swapSession afterwards — the adapter ends up attached to the superseded
// conversation while Restore reports success and disk reflects the
// checkpointed token (silent wrong-state restore). The choreography parks
// both resumes on fakeClient gates so the interleaving is scripted, not
// lucky: Restore's resume is released while the sweep is still parked (or
// still queued on reopenMu), then the sweep's own resume is released last.
func TestRestore_RacingRecoverySweepKeepsRestoredAddress(t *testing.T) {
	withFastBackoff(t)
	fc := &fakeClient{pingErr: errors.New("client not connected")}
	p := withRecoverableClient(t, fc)
	persistSDKSessionID("adapter-race", "A")

	// Stale runtime binding by construction (epoch 0), a session attached to
	// the stale persisted id, and the checkpointed token "T" to restore.
	s := newWatchdogSession(t, p, "adapter-race", &fakeSession{sessionID: "A"})

	// Park every ResumeSessionWithOptions until the choreography releases it.
	// Gates are closed at most once (the body and the cleanup race otherwise).
	gateT := make(chan struct{})
	gateA := make(chan struct{})
	closeOnceT, closeOnceA := &sync.Once{}, &sync.Once{}
	release := func(once *sync.Once, gate chan struct{}) {
		once.Do(func() { close(gate) })
	}
	fc.mu.Lock()
	fc.holdResume = map[string]chan struct{}{"T": gateT, "A": gateA}
	fc.mu.Unlock()
	t.Cleanup(func() {
		release(closeOnceT, gateT)
		release(closeOnceA, gateA)
	})

	restoreErr := make(chan error, 1)
	restoreFinished := make(chan struct{})
	go func() {
		_, err := p.Restore(context.Background(), &v2.RestoreRequest{
			SessionId:     "adapter-race",
			SchemaVersion: copilotStateSchemaVersion,
			State:         []byte("T"),
		})
		restoreErr <- err
		close(restoreFinished)
	}()
	sweepDone := make(chan struct{}, 1)
	go func() {
		// trigger=nil sweeps every registered session including ours.
		p.recoverTransport(context.Background(), nil, false)
		close(sweepDone)
	}()

	// Stage 1: let Restore reach (and finish) its reattach to "T" first. When
	// the sweep won the reopenMu ordering post-fix, Restore never reaches its
	// resume before the sweep's is parked — the timeout tolerates that order
	// and closing the gate early is a no-op (nothing is parked on it yet;
	// Restore's later resume then runs against an already-closed gate).
	if !waitResumeArrived(fc, "T", 2*time.Second) {
		slog.Info("restore resume of the checkpointed token not parked before timeout (sweep won the lock ordering)")
	}
	release(closeOnceT, gateT)
	waitDoneOrDeadline(t, restoreFinished, 2*time.Second, "Restore")

	// Stage 2: unstick the sweep's parked resume. Post-fix the sweep either
	// already saw the epoch Restore claimed (no-op) or completes its reopen
	// of the just-persisted address; pre-fix it lands swapSession("A") over
	// the restored conversation here.
	release(closeOnceA, gateA)
	joinWithDeadline(t, sweepDone, 5*time.Second, "recovery sweep")

	if err := <-restoreErr; err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if sess := s.currentSession(); sess == nil || sess.SessionID() != "T" {
		t.Fatalf("adapter attached to %v after concurrent restore+sweep, want the checkpointed address %q restored",
			sess, "T")
	}
	if got := loadPersistedSDKSessionID("adapter-race"); got != "T" {
		t.Fatalf("persisted sdk session id = %q, want %q", got, "T")
	}
	if s.boundClientEpoch != p.currentClientEpoch() {
		t.Fatalf("boundClientEpoch = %d after restore, want the current runtime epoch %d (a following sweep must be a no-op)",
			s.boundClientEpoch, p.currentClientEpoch())
	}

	// The restored session reclaims its runtime binding: a follow-up sweep
	// must not reopen it again (no new resumes, restarts or creations; ping
	// probes are the liveness check itself and are expected to tick).
	before, after := fc.activityCounts(func() {
		p.recoverTransport(context.Background(), nil, false)
	})
	if after != before {
		t.Fatalf("follow-up sweep churned the restored session: %v -> %v", before, after)
	}
}

// waitResumeArrived polls the fake's recorded resume entries until id shows
// up or the deadline passes. Returns false when the caller never parked.
func waitResumeArrived(fc *fakeClient, id string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		fc.mu.Lock()
		seen := slices.Contains(fc.resumeIDs, id)
		fc.mu.Unlock()
		if seen {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// waitDoneOrDeadline waits for a resumer to finish or returns when the
// deadline passes (the sweep-first lock ordering keeps Restore parked on
// reopenMu until sweepDone's own join, below, unblocks it). Waiting on a
// close-only channel keeps the restore error buffer intact for the final
// assertions.
func waitDoneOrDeadline(t *testing.T, done chan struct{}, timeout time.Duration, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		slog.Info("resumer not finished before deadline; releasing next stage", "resumer", what)
	}
}

// joinWithDeadline waits for the sweep goroutine to finish; a goroutine still
// parked past every gate release means the choreography wedged and the test
// must fail rather than leak the goroutine into later assertions.
func joinWithDeadline(t *testing.T, done chan struct{}, timeout time.Duration, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("%s goroutine did not finish after both resume gates were released", what)
	}
}

// activityCounts snapshots the fake's restart/resume activity around fn.
// Ping probes are omitted — they are the follow-up sweep's liveness check
// itself and are expected to tick; what must not change is any actual churn
// (resumes, creations or CLI restarts).
func (c *fakeClient) activityCounts(fn func()) (before, after string) {
	c.mu.Lock()
	before = fmt.Sprintf("resumes=%v creates=%d stops=%d starts=%d",
		c.resumeIDs, c.createCount, c.stopCount, c.startCount)
	c.mu.Unlock()
	fn()
	c.mu.Lock()
	after = fmt.Sprintf("resumes=%v creates=%d stops=%d starts=%d",
		c.resumeIDs, c.createCount, c.stopCount, c.startCount)
	c.mu.Unlock()
	return before, after
}
