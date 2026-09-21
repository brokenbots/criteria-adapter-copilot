// copilot_retry_test.go — CRI-272: retry helper, CLI-child restart and
// session-reopen coverage.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	copilot "github.com/github/copilot-sdk/go"
	"github.com/github/copilot-sdk/go/rpc"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// withRetryRecorder replaces retrySleep with a recorder so tests observe the
// backoff sequence without real sleeping; it returns a pointer to the recorded
// durations. Real timer/ctx behaviour is covered by the dedicated sleep tests.
func withRetryRecorder(t *testing.T) *[]time.Duration {
	t.Helper()
	orig := retrySleep
	sleeps := []time.Duration{}
	retrySleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() { retrySleep = orig })
	return &sleeps
}

// withFastBackoff shrinks the backoff base/cap so tests that actually sleep
// (through real retrySleep) finish in milliseconds while keeping the 4× shape.
func withFastBackoff(t *testing.T) {
	t.Helper()
	origBase, origMax := retryBaseDelay, retryMaxDelay
	retryBaseDelay = 1 * time.Millisecond
	retryMaxDelay = 4 * time.Millisecond
	t.Cleanup(func() {
		retryBaseDelay, retryMaxDelay = origBase, origMax
	})
}

// fakeClient is an in-memory copilotClient for restart/resume tests.
type fakeClient struct {
	mu         sync.Mutex
	startErrs  []error // nth Start fails with startErrs[n] (nil → success)
	startCount int
	stopCount  int
	pingErr    error

	createCount   int
	createErr     error
	resumeIDs     []string
	resumeConfigs []*copilot.ResumeSessionConfig
	resumeErr     error

	// handOut is the session the next Create/Resume hands back.
	handOut copilotSession
}

func (c *fakeClient) Start(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.startCount < len(c.startErrs) {
		err := c.startErrs[c.startCount]
		c.startCount++
		return err
	}
	c.startCount++
	// A successful start simulates a freshly spawned, healthy CLI child.
	c.pingErr = nil
	return nil
}

func (c *fakeClient) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopCount++
	return nil
}

func (c *fakeClient) Ping(_ context.Context, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pingErr
}

func (c *fakeClient) CreateSession(_ context.Context, config *copilot.SessionConfig) (copilotSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createCount++
	if c.createErr != nil {
		return nil, c.createErr
	}
	if c.handOut == nil {
		return &fakeSession{sessionID: fmt.Sprintf("sdk-new-%d", c.createCount)}, nil
	}
	return c.handOut, nil
}

func (c *fakeClient) ResumeSessionWithOptions(_ context.Context, sessionID string, config *copilot.ResumeSessionConfig) (copilotSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeIDs = append(c.resumeIDs, sessionID)
	c.resumeConfigs = append(c.resumeConfigs, config)
	if c.resumeErr != nil {
		return nil, c.resumeErr
	}
	if c.handOut == nil {
		return &fakeSession{sessionID: sessionID}, nil
	}
	return c.handOut, nil
}

// send502 is a realistic SDK-wrapped provider 502: Session.Send wraps all
// failures with "failed to send message" and the CLI surfaces the provider
// HTTP status inside the JSON-RPC error message.
var send502 = fmt.Errorf("failed to send message: JSON-RPC Error -32603: provider request failed: HTTP 502")

func TestSendWithRetryRetriesProvider5xxAnd429(t *testing.T) {
	sleeps := withRetryRecorder(t)
	send429 := fmt.Errorf("failed to send message: JSON-RPC Error -32603: provider request failed: status code 429")
	fake := &fakeSession{sendErrSequence: []error{send502, send429}}
	s := &sessionState{session: fake}

	msgID, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"})
	if err != nil {
		t.Fatalf("sendWithRetry returned error: %v", err)
	}
	if msgID != "msg-1" {
		t.Fatalf("msgID = %q, want msg-1", msgID)
	}
	if fake.sendAttempts != 3 {
		t.Fatalf("send attempts = %d, want 3 (two transient failures then success)", fake.sendAttempts)
	}
	if got := *sleeps; len(got) != 2 || got[0] != time.Second || got[1] != 4*time.Second {
		t.Fatalf("backoff sequence = %v, want [1s 4s]", got)
	}
}

func TestSendWithRetryDoesNotRetry4xx(t *testing.T) {
	sleeps := withRetryRecorder(t)
	send401 := errors.New("failed to send message: JSON-RPC Error -32603: model not found: HTTP 401")
	fake := &fakeSession{sendErr: send401}
	s := &sessionState{session: fake}

	_, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"})
	if !errors.Is(err, send401) {
		t.Fatalf("sendWithRetry err = %v, want the original 401 error unwrapped", err)
	}
	if fake.sendAttempts != 1 {
		t.Fatalf("send attempts = %d, want 1 (4xx is not retried)", fake.sendAttempts)
	}
	if len(*sleeps) != 0 {
		t.Fatalf("sleeps = %v, want none for a non-retryable error", *sleeps)
	}
}

func TestSendWithRetryExhaustionSurfacesLastError(t *testing.T) {
	sleeps := withRetryRecorder(t)
	lastErr := fmt.Errorf("failed to send message: JSON-RPC Error -32603: HTTP 503 service unavailable")
	fake := &fakeSession{sendErr: lastErr}
	s := &sessionState{session: fake}

	_, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"})
	if err == nil {
		t.Fatal("sendWithRetry must surface an error after exhausting retries")
	}
	if !errors.Is(err, lastErr) {
		t.Fatalf("exhaustion error must wrap the LAST send error; got %v", err)
	}
	if !strings.Contains(err.Error(), "retries exhausted after 4 attempts") {
		t.Fatalf("exhaustion error = %v, want the attempts summary", err)
	}
	if fake.sendAttempts != retryMaxAttempts {
		t.Fatalf("send attempts = %d, want %d", fake.sendAttempts, retryMaxAttempts)
	}
	if got := *sleeps; len(got) != 3 || got[0] != time.Second || got[1] != 4*time.Second || got[2] != 16*time.Second {
		t.Fatalf("backoff sequence = %v, want [1s 4s 16s]", got)
	}
}

func TestSendWithRetrySuccessOnSecondAttempt(t *testing.T) {
	sleeps := withRetryRecorder(t)
	fake := &fakeSession{sendErrSequence: []error{fmt.Errorf("failed to send message: CLI process exited: EOF")}}
	s := &sessionState{session: fake}

	if _, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"}); err != nil {
		t.Fatalf("sendWithRetry returned error: %v", err)
	}
	if fake.sendAttempts != 2 {
		t.Fatalf("send attempts = %d, want 2", fake.sendAttempts)
	}
	if got := *sleeps; len(got) != 1 || got[0] != time.Second {
		t.Fatalf("backoff sequence = %v, want [1s]", got)
	}
}

func TestIsRetryableSendError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"http 502", fmt.Errorf("failed to send message: JSON-RPC Error -32603: HTTP 502"), true},
		{"status code 502", errors.New("failed to send message: JSON-RPC Error -32603: status code 502"), true},
		{"http=503", errors.New("failed to send message: http=503"), true},
		{"bad gateway", errors.New("failed to send message: JSON-RPC Error -32603: 502 Bad Gateway"), true},
		{"service unavailable", errors.New("failed to send message: JSON-RPC Error -32603: Service Unavailable"), true},
		{"gateway timeout", errors.New("failed to send message: JSON-RPC Error -32603: Gateway Timeout"), true},
		{"internal server error", errors.New("failed to send message: JSON-RPC Error -32603: Internal Server Error"), true},
		{"http 429", errors.New("failed to send message: JSON-RPC Error -32603: HTTP 429 Too Many Requests"), true},
		{"rate limit phrase", errors.New("failed to send message: JSON-RPC Error -32603: provider rate limit exceeded"), true},
		{"eof transport", errors.New("failed to send message: CLI process exited: EOF"), true},
		{"bare eof errors.Is", fmt.Errorf("failed to send message: %w", io.EOF), true},
		{"connection reset", errors.New("failed to send message: read |0: connection reset by peer"), true},
		{"process exited", errors.New("failed to send message: CLI process exited: signal: killed"), true},
		{"client stopped", errors.New("client stopped"), true},
		{"not connected", errors.New("client not connected"), true},
		{"http 401", errors.New("failed to send message: JSON-RPC Error -32603: HTTP 401 unauthorized"), false},
		{"status 403", errors.New("failed to send message: JSON-RPC Error -32603: status code 403"), false},
		{"model not found", errors.New("failed to send message: JSON-RPC Error -32603: model not-found-x"), false},
		{"invalid request", errors.New("failed to send message: JSON-RPC Error -32602: invalid arguments"), false},
		{"context deadline", errors.New("failed to send message: context deadline exceeded"), false},
		{"port number 4290", errors.New("failed to send message: dial tcp 127.0.0.1:4290"), false},
	}
	for _, tc := range cases {
		if got := isRetryableSendError(tc.err); got != tc.want {
			t.Errorf("%s: isRetryableSendError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

func TestBackoffSequence(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 4 * time.Second},
		{2, 16 * time.Second},
		{3, 16 * time.Second},
		{4, 16 * time.Second},
	} {
		if got := backoffDelay(tc.attempt); got != tc.want {
			t.Errorf("backoffDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

// fakeDeadlineContext carries a deadline without ever expiring on its own:
// Err() stays nil, so the retry loop's ctx check never fires and the deadline
// path can only trigger through the clamp arithmetic — asserted against a
// fake clock, not wall-clock scheduling margins.
type fakeDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c fakeDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestSendWithRetryDeadlineCapsSleep(t *testing.T) {
	origBase, origMax := retryBaseDelay, retryMaxDelay
	retryBaseDelay = 40 * time.Millisecond
	retryMaxDelay = time.Second
	t.Cleanup(func() { retryBaseDelay, retryMaxDelay = origBase, origMax })

	// Deterministic clock: the injected retrySleep records the requested
	// sleep and advances the fake clock; retryNow reports it. No real timers
	// are involved, so the assertions below cannot flake on scheduling.
	base := time.Unix(1000, 0)
	now := base
	origNow := retryNow
	retryNow = func() time.Time { return now }
	t.Cleanup(func() { retryNow = origNow })
	origSleep := retrySleep
	sleeps := []time.Duration{}
	retrySleep = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		now = now.Add(d)
		return nil
	}
	t.Cleanup(func() { retrySleep = origSleep })

	lastErr := fmt.Errorf("failed to send message: JSON-RPC Error -32603: HTTP 502")
	fake := &fakeSession{sendErr: lastErr}
	s := &sessionState{session: fake}
	ctx := fakeDeadlineContext{Context: context.Background(), deadline: base.Add(90 * time.Millisecond)}

	_, err := s.sendWithRetry(ctx, &copilot.MessageOptions{Prompt: "hi"})
	if err == nil {
		t.Fatal("expected the last send error once the deadline is spent")
	}
	if !errors.Is(err, lastErr) {
		t.Fatalf("err = %v, want the raw last provider error (no exhaustion wrap)", err)
	}
	if strings.Contains(err.Error(), "retries exhausted") {
		t.Fatalf("deadline-reached send must surface the last error, not the exhaustion wrap: %v", err)
	}
	if fake.sendAttempts != 3 {
		t.Fatalf("send attempts = %d, want 3 (attempt 2 sees remaining 0 and stops)", fake.sendAttempts)
	}
	// attempt 0 → 40ms; attempt 1 → 4s clamped to the 50ms remaining;
	// attempt 2 → remaining 0 → return before sleeping.
	if len(sleeps) != 2 || sleeps[0] != 40*time.Millisecond || sleeps[1] != 50*time.Millisecond {
		t.Fatalf("recorded sleeps = %v, want [40ms 50ms]", sleeps)
	}
}

func TestRetrySleepRespectsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := retrySleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("retrySleep on cancelled ctx = %v, want context.Canceled", err)
	}
}

// TestExecuteSurvivesTransient502 covers the workstream's mid-turn case: a
// provider 502 on the initial prompt must not fail the turn — the retry
// absorbs it and the turn proceeds normally (submit_outcome → success).
func TestExecuteSurvivesTransient502(t *testing.T) {
	withRetryRecorder(t)
	fake := &fakeSession{
		sendErrSequence: []error{fmt.Errorf("failed to send message: JSON-RPC Error -32603: HTTP 502")},
		emitOnSend: []copilot.SessionEvent{
			{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "working..."}},
			{Data: &copilot.SessionIdleData{}},
		},
	}
	p := &copilotAdapter{}
	s := &sessionState{session: fake}
	p.sessions = map[string]*sessionState{"s1": s}
	sender := &recordingSender{}

	// Simulate submit_outcome before the session goes idle so the turn
	// resolves to a success outcome after the retried send.
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		s.mu.Lock()
		s.finalizedOutcome = "success"
		s.mu.Unlock()
	}

	err := p.Execute(context.Background(), &v2.ExecuteRequest{SessionId: "s1", Input: map[string]string{"prompt": "hi", "max_turns": "2"}}, sender)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	found := false
	for _, ev := range sender.snapshot() {
		if r := ev.GetResult(); r != nil && r.GetOutcome() == "success" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a success result after the retried send; events: %+v", sender.snapshot())
	}
	if fake.sendAttempts != 2 {
		t.Fatalf("send attempts = %d, want 2 (502 then success)", fake.sendAttempts)
	}
}

// CRI-272: the CLI child dying mid-turn (EOF on stdio) must be recovered by
// restarting the client and re-opening the SDK session (resume via the
// persisted ID), after which the send succeeds — the adapter process stays up.
func TestSendWithRetryRestartsClientAndResumesSession(t *testing.T) {
	withFastBackoff(t)
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-eof", "sdk-orig")

	p := newCopilotAdapter()
	fc := &fakeClient{pingErr: errors.New("client not connected")}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })

	permissionHandler := func(_ copilot.PermissionRequest, _ copilot.PermissionInvocation) (rpc.PermissionDecision, error) {
		return &rpc.PermissionDecisionApproveOnce{}, nil
	}
	sc := &copilot.SessionConfig{
		Model:               "m",
		Streaming:           copilot.Bool(true),
		OnPermissionRequest: permissionHandler,
	}
	dead := &fakeSession{sessionID: "sdk-orig", sendErrSequence: []error{fmt.Errorf("failed to send message: CLI process exited: EOF")}}
	resumed := &fakeSession{
		sessionID: "sdk-orig",
		emitOnSend: []copilot.SessionEvent{
			{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "resumed"}},
		},
	}
	fc.handOut = resumed
	s := newSessionState("adapter-eof", dead, sc, buildResumeConfig(sc), adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil), p)
	p.sessions["adapter-eof"] = s

	// A subscriber registered on the pre-restart session must keep receiving
	// events after the SDK session swap (in-flight turn continuity).
	events := make(chan copilot.SessionEvent, 4)
	unsub := s.subscribeEvents(func(ev copilot.SessionEvent) { events <- ev })
	defer unsub()

	_, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"})
	if err != nil {
		t.Fatalf("sendWithRetry after CLI restart = %v, want success", err)
	}

	if dead.sendAttempts != 1 || dead.sendCount != 0 {
		t.Fatalf("dead session attempts/successes = %d/%d, want 1/0", dead.sendAttempts, dead.sendCount)
	}
	if resumed.sendCount != 1 {
		t.Fatalf("resumed session sendCount = %d, want 1", resumed.sendCount)
	}
	if fc.stopCount != 1 || fc.startCount != 1 {
		t.Fatalf("stop/start counts = %d/%d, want 1/1 (one CLI restart)", fc.stopCount, fc.startCount)
	}
	if len(fc.resumeIDs) != 1 || fc.resumeIDs[0] != "sdk-orig" {
		t.Fatalf("resume calls = %v, want [sdk-orig]", fc.resumeIDs)
	}
	if fc.createCount != 0 {
		t.Fatalf("create calls = %d, want 0 (resume succeeded)", fc.createCount)
	}
	rc := fc.resumeConfigs[0]
	if rc.Model != "m" || rc.Streaming == nil || !*rc.Streaming || rc.OnPermissionRequest == nil {
		t.Fatalf("resume config lost session setup: %+v", rc)
	}
	// A successful resume keeps the persisted ID.
	if got := loadPersistedSDKSessionID("adapter-eof"); got != "sdk-orig" {
		t.Fatalf("persisted ID = %q, want sdk-orig", got)
	}

	select {
	case ev := <-events:
		if _, ok := ev.Data.(*copilot.AssistantMessageData); !ok {
			t.Fatalf("post-restart event type = %T, want AssistantMessageData", ev.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event fanout did not survive the session swap")
	}
}

// TestRecoverTransportReopensStaleSessionsIncludingActive: after a CLI-child
// restart, recovery re-opens EVERY session still bound to the superseded
// runtime — the triggering one last, in-flight ones included (their retry loop
// re-reads the session each attempt). A session already re-opened on the new
// runtime (simulated here by a fresh boundClientEpoch) must not churn, and a
// second sweep over the same sessions must be a complete no-op.
func TestRecoverTransportReopensStaleSessionsIncludingActive(t *testing.T) {
	withFastBackoff(t)
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-1", "sdk-1")
	persistSDKSessionID("adapter-2", "sdk-2")
	persistSDKSessionID("adapter-3", "sdk-3")

	p := newCopilotAdapter()
	fc := &fakeClient{pingErr: errors.New("client not connected")}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })

	sc := &copilot.SessionConfig{Model: "m"}
	secrets := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil)
	dead1 := &fakeSession{sessionID: "sdk-1"}
	idle2 := &fakeSession{sessionID: "sdk-2"}
	// s3 simulates a session a peer goroutine already re-opened on the new
	// runtime epoch (1): its binding is current, so the sweep must skip it.
	alive3 := &fakeSession{sessionID: "sdk-3"}
	s1 := newSessionState("adapter-1", dead1, sc, buildResumeConfig(sc), secrets, p)
	s2 := newSessionState("adapter-2", idle2, sc, buildResumeConfig(sc), secrets, p)
	s3 := newSessionState("adapter-3", alive3, sc, buildResumeConfig(sc), secrets, p)
	// s2 is the in-flight case the defect class requires: its Execute is in
	// flight (active), yet its binding is stale after the CLI restart, so the
	// sweep must re-open it — the pre-fix logic skipped active sessions.
	s2.mu.Lock()
	s2.active = true
	s2.mu.Unlock()
	s3.reopenMu.Lock()
	s3.boundClientEpoch = 1
	s3.reopenMu.Unlock()
	p.sessions["adapter-1"] = s1
	p.sessions["adapter-2"] = s2
	p.sessions["adapter-3"] = s3

	p.recoverTransport(context.Background(), s1, false)

	// Non-trigger sessions first, triggering session last.
	if len(fc.resumeIDs) != 2 || fc.resumeIDs[0] != "sdk-2" || fc.resumeIDs[1] != "sdk-1" {
		t.Fatalf("resume calls = %v, want [sdk-2 sdk-1] (non-trigger first, trigger last)", fc.resumeIDs)
	}
	if got := s1.currentSession(); got == dead1 {
		t.Fatal("triggering session was not reopened")
	}
	if got := s2.currentSession(); got == idle2 {
		t.Fatal("active non-trigger session was not reopened despite being stale")
	}
	if got := s3.currentSession(); got != alive3 {
		t.Fatal("session already re-opened on the new runtime epoch must not churn")
	}
	if s1.boundClientEpoch != 1 || s2.boundClientEpoch != 1 || s3.boundClientEpoch != 1 {
		t.Fatalf("bound epochs = %d/%d/%d, want 1/1/1", s1.boundClientEpoch, s2.boundClientEpoch, s3.boundClientEpoch)
	}
	if fc.createCount != 0 {
		t.Fatalf("create calls = %d, want 0 (all resumes succeed)", fc.createCount)
	}
	if fc.stopCount != 1 || fc.startCount != 1 {
		t.Fatalf("stop/start = %d/%d, want 1/1", fc.stopCount, fc.startCount)
	}

	// A second sweep over the same state must be a no-op: no restart (child
	// is alive), no resumes, no swaps.
	p.recoverTransport(context.Background(), nil, false)
	if len(fc.resumeIDs) != 2 || fc.createCount != 0 || fc.stopCount != 1 || fc.startCount != 1 {
		t.Fatalf("second sweep changed state: resumes=%v create=%d stop=%d start=%d, want unchanged",
			fc.resumeIDs, fc.createCount, fc.stopCount, fc.startCount)
	}
	if got := s1.currentSession(); got == dead1 {
		t.Fatal("second sweep swapped s1 back")
	}
}

// gatedSendSession delays its first Send until the gate closes, then delegates
// to the wrapped session. Used to script "session B's EOF failure happens only
// after a peer already restarted the CLI child".
type gatedSendSession struct {
	copilotSession
	gate chan struct{}
}

func (g *gatedSendSession) Send(ctx context.Context, opts *copilot.MessageOptions) (string, error) {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return g.copilotSession.Send(ctx, opts)
}

// TestSendWithRetryPeerRestartHealsAllSessions covers the multi-session
// recovery gap: session A fails EOF and restarts the CLI child; session B then
// fails EOF only after that restart is complete. B must still land on the new
// runtime — re-opened by A's sweep (exactly once each), not skipped — and
// B's retry must succeed on the re-opened session. A live CLI (502 during a
// healthy turn) causes no restart and no resume churn at all.
func TestSendWithRetryPeerRestartHealsAllSessions(t *testing.T) {
	withFastBackoff(t)
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-a", "sdk-a")
	persistSDKSessionID("adapter-b", "sdk-b")

	p := newCopilotAdapter()
	fc := &fakeClient{pingErr: errors.New("client not connected")}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })

	sc := &copilot.SessionConfig{Model: "m"}
	secrets := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil)
	eofErr := fmt.Errorf("failed to send message: CLI process exited: EOF")
	deadA := &fakeSession{sessionID: "sdk-a", sendErrSequence: []error{eofErr}}
	// B's first Send is gated: it fails with EOF only after A's retry loop has
	// fully completed (restart + sweep), i.e. the restart is already done.
	deadB := &fakeSession{sessionID: "sdk-b", sendErrSequence: []error{eofErr}}
	gatedB := &gatedSendSession{copilotSession: deadB, gate: make(chan struct{})}
	sA := newSessionState("adapter-a", deadA, sc, buildResumeConfig(sc), secrets, p)
	sB := newSessionState("adapter-b", gatedB, sc, buildResumeConfig(sc), secrets, p)
	// B is in flight (active) while its binding goes stale: the sweep must
	// heal it anyway; the pre-fix logic skipped active sessions entirely.
	sB.mu.Lock()
	sB.active = true
	sB.mu.Unlock()
	p.sessions["adapter-a"] = sA
	p.sessions["adapter-b"] = sB

	doneA := make(chan error, 1)
	go func() {
		_, err := sA.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "a"})
		doneA <- err
	}()
	doneB := make(chan error, 1)
	go func() {
		_, err := sB.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "b"})
		doneB <- err
	}()

	// The gate opens once A's recovery is complete, so B's EOF failure lands
	// exactly in the "restart already performed by a peer" window. The deferred
	// open is a safety net so a failure path cannot hang the test.
	openGate := sync.OnceFunc(func() { close(gatedB.gate) })
	defer openGate()
	if err := <-doneA; err != nil {
		t.Fatalf("session A send = %v, want success after restart+resume", err)
	}
	openGate()
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatalf("session B send = %v, want success (peer restart must heal B)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session B send did not complete")
	}

	// Both sessions ended on freshly re-opened SDK sessions; neither stale
	// session ever completed a send.
	for _, tc := range []struct {
		name  string
		dead  *fakeSession
		s     *sessionState
		sdkID string
	}{
		{"A", deadA, sA, "sdk-a"},
		{"B", deadB, sB, "sdk-b"},
	} {
		resumed, ok := tc.s.currentSession().(*fakeSession)
		if !ok {
			t.Fatalf("session %s did not land on a fakeSession: %T", tc.name, tc.s.currentSession())
		}
		if resumed == tc.dead {
			t.Fatalf("session %s is still bound to the dead session", tc.name)
		}
		if tc.dead.sendCount != 0 || resumed.sendCount < 1 {
			t.Fatalf("session %s sends: dead=%d resumed=%d, want dead=0, resumed≥1", tc.name, tc.dead.sendCount, resumed.sendCount)
		}
	}
	// Each session was re-opened exactly once, via resume (no create churn).
	if len(fc.resumeIDs) != 2 {
		t.Fatalf("resume calls = %v, want exactly one per session", fc.resumeIDs)
	}
	if fc.resumeIDs[0] != "sdk-b" || fc.resumeIDs[1] != "sdk-a" {
		t.Fatalf("resume calls = %v, want [sdk-b sdk-a] (non-trigger first, trigger last)", fc.resumeIDs)
	}
	if fc.createCount != 0 {
		t.Fatalf("create calls = %d, want 0", fc.createCount)
	}
	// Exactly one CLI restart despite two sessions driving recovery.
	if fc.stopCount != 1 || fc.startCount != 1 {
		t.Fatalf("stop/start = %d/%d, want 1/1", fc.stopCount, fc.startCount)
	}
	if p.clientEpoch != 1 {
		t.Fatalf("client epoch = %d, want 1 (one restart)", p.clientEpoch)
	}
}

// TestSendWithRetry502OnLiveCLIDoesNotRestartOrReopen: a provider 502 with a
// healthy CLI child must not restart the child nor re-open/churn the session —
// recovery sweeps over a live runtime are complete no-ops.
func TestSendWithRetry502OnLiveCLIDoesNotRestartOrReopen(t *testing.T) {
	withRetryRecorder(t)
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-live", "sdk-live")

	p := newCopilotAdapter()
	fc := &fakeClient{} // ping succeeds: live CLI
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })

	sc := &copilot.SessionConfig{Model: "m"}
	live := &fakeSession{sessionID: "sdk-live", sendErrSequence: []error{send502}}
	s := newSessionState("adapter-live", live, sc, buildResumeConfig(sc), adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil), p)
	p.sessions["adapter-live"] = s

	if _, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"}); err != nil {
		t.Fatalf("sendWithRetry = %v, want success after the 502 retry", err)
	}
	if live.sendAttempts != 2 || live.sendCount != 1 {
		t.Fatalf("live session attempts/sends = %d/%d, want 2/1 (502 then success)", live.sendAttempts, live.sendCount)
	}
	if got := s.currentSession(); got != live {
		t.Fatal("a 502 on a live CLI must not swap the session")
	}
	if s.boundClientEpoch != 0 || p.clientEpoch != 0 {
		t.Fatalf("epochs = session %d / adapter %d, want 0/0 (no restart)", s.boundClientEpoch, p.clientEpoch)
	}
	if fc.stopCount != 0 || fc.startCount != 0 || len(fc.resumeIDs) != 0 || fc.createCount != 0 {
		t.Fatalf("502 on live CLI caused churn: stop=%d start=%d resumes=%v creates=%d, want all zero",
			fc.stopCount, fc.startCount, fc.resumeIDs, fc.createCount)
	}
}

// TestReopenSessionConcurrentWithReadersAndClose exercises the guarded session
// accessor under concurrency: repeated reopenSession swaps against hammering
// currentSession/subscribeEvents readers, then a CloseSession racing one more
// reopen. Outcomes stay sane (no panic, no hang, closed session skipped by the
// membership re-check); under -race this is the detector for unsynchronized
// session field access.
func TestReopenSessionConcurrentWithReadersAndClose(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-c", "sdk-c")

	p := newCopilotAdapter()
	fc := &fakeClient{}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}

	sc := &copilot.SessionConfig{Model: "m"}
	live := &fakeSession{sessionID: "sdk-c"}
	s := newSessionState("adapter-c", live, sc, buildResumeConfig(sc), adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil), p)
	p.sessions["adapter-c"] = s

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { // reader: guarded accessor
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.currentSession()
			}
		}
	}()
	go func() { // reader: event subscription path
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				un := s.subscribeEvents(func(copilot.SessionEvent) {})
				un()
			}
		}
	}()
	go func() { // writer: repeated reopens (forced staleness), then release the readers
		defer wg.Done()
		for i := 0; i < 50; i++ {
			p.reopenSession(context.Background(), s)
			s.reopenMu.Lock()
			s.boundClientEpoch = -1 // force the next reopen to swap again
			s.reopenMu.Unlock()
		}
		close(stop)
	}()
	wg.Wait()

	// CloseSession racing one more reopen: the loser must not panic or hang.
	closeReq := &v2.CloseSessionRequest{SessionId: "adapter-c"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.reopenMu.Lock()
		s.boundClientEpoch = -1
		s.reopenMu.Unlock()
		p.reopenSession(context.Background(), s)
	}()
	if _, err := p.CloseSession(context.Background(), closeReq); err != nil {
		t.Fatalf("CloseSession = %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reopenSession raced past CloseSession removal and hung")
	}
	if got := s.currentSession(); got == nil {
		t.Fatal("session accessor returned nil after concurrent reopen/close")
	}
}

func TestStartClientWithRetryRetriesTransient(t *testing.T) {
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{startErrs: []error{
		errors.New("client stopped"),
		fmt.Errorf("failed to send request: EOF"),
	}}
	if err := startClientWithRetry(context.Background(), fc); err != nil {
		t.Fatalf("startClientWithRetry = %v, want success on attempt 3", err)
	}
	if fc.startCount != 3 {
		t.Fatalf("start count = %d, want 3", fc.startCount)
	}
	if got := *sleeps; len(got) != 2 || got[0] != time.Second || got[1] != 4*time.Second {
		t.Fatalf("backoff sequence = %v, want [1s 4s]", got)
	}
}

func TestStartClientWithRetrySurfacesNonRetryable(t *testing.T) {
	sleeps := withRetryRecorder(t)
	startErr := errors.New("exec: copilot binary not found")
	fc := &fakeClient{startErrs: []error{startErr}}
	if err := startClientWithRetry(context.Background(), fc); !errors.Is(err, startErr) {
		t.Fatalf("startClientWithRetry err = %v, want the original error", err)
	}
	if fc.startCount != 1 {
		t.Fatalf("start count = %d, want 1 (non-retryable errors surface immediately)", fc.startCount)
	}
	if len(*sleeps) != 0 {
		t.Fatalf("sleeps = %v, want none", *sleeps)
	}
}

// TestEnsureClientRestartsDeadChild covers ensureClient's dead-CLI detection:
// the child is stopped and restarted instead of returning a terminal error on
// first failure, and a live child is left untouched.
func TestEnsureClientRestartsDeadChild(t *testing.T) {
	p := newCopilotAdapter()
	fc := &fakeClient{pingErr: errors.New("client stopped")}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })
	secrets := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil)

	got, err := p.ensureClient(context.Background(), secrets)
	if err != nil {
		t.Fatalf("ensureClient = %v, want the restarted client", err)
	}
	if got != fc || p.client != fc {
		t.Fatal("ensureClient must return the restarted client and store it")
	}
	if fc.stopCount != 1 || fc.startCount != 1 {
		t.Fatalf("stop/start = %d/%d, want 1/1", fc.stopCount, fc.startCount)
	}

	// Second call: the restarted child is alive; no further restarts.
	if _, err := p.ensureClient(context.Background(), secrets); err != nil {
		t.Fatalf("second ensureClient = %v", err)
	}
	if fc.stopCount != 1 || fc.startCount != 1 {
		t.Fatalf("stop/start after live probe = %d/%d, want unchanged", fc.stopCount, fc.startCount)
	}
}

// Acceptance #2 (CRI-272): OpenSession on an adapter session opened before a
// process respawn resumes the persisted SDK conversation — and the resume
// carries the full session setup (provider/BYOK, tools, permission gating,
// streaming, system message), not just client/model/tools.
func TestOpenSessionResumesPersistedSDKSession(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-r", "sdk-77")

	p := newCopilotAdapter()
	fc := &fakeClient{}
	fc.handOut = &fakeSession{sessionID: "sdk-77"}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}

	resp, err := p.OpenSession(context.Background(), &v2.OpenSessionRequest{
		SessionId: "adapter-r",
		Config: map[string]string{
			"model":             "m1",
			"provider_base_url": "http://ollama.local/v1",
			"provider_type":     "openai",
			"system_prompt":     "be brief",
			"working_directory": "/w",
		},
	})
	if err != nil {
		t.Fatalf("OpenSession = %v", err)
	}
	if resp == nil {
		t.Fatal("OpenSession returned nil response")
	}
	if fc.createCount != 0 {
		t.Fatalf("create calls = %d, want 0 (resume must win)", fc.createCount)
	}
	if len(fc.resumeIDs) != 1 || fc.resumeIDs[0] != "sdk-77" {
		t.Fatalf("resume calls = %v, want [sdk-77]", fc.resumeIDs)
	}
	rc := fc.resumeConfigs[0]
	if rc.Model != "m1" {
		t.Fatalf("resume model = %q, want m1", rc.Model)
	}
	if rc.Provider == nil || rc.Provider.BaseURL != "http://ollama.local/v1" {
		t.Fatalf("resume must carry the BYOK provider; got %+v", rc.Provider)
	}
	if rc.SystemMessage == nil || rc.SystemMessage.Content != "be brief" {
		t.Fatalf("resume must carry the system message; got %+v", rc.SystemMessage)
	}
	if rc.Streaming == nil || !*rc.Streaming {
		t.Fatal("resume must carry streaming")
	}
	if len(rc.Tools) != 2 {
		t.Fatalf("resume tools = %d, want 2 (submit_outcome + adapter_tool)", len(rc.Tools))
	}
	if rc.OnPermissionRequest == nil {
		t.Fatal("resume must carry the permission-gating callback")
	}
	if rc.WorkingDirectory != "/w" {
		t.Fatalf("resume working directory = %q, want /w", rc.WorkingDirectory)
	}
	if got := loadPersistedSDKSessionID("adapter-r"); got != "sdk-77" {
		t.Fatalf("persisted ID = %q, want sdk-77", got)
	}
}

func TestOpenSessionResumeFailureFallsBackToCreate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-f", "sdk-stale")

	p := newCopilotAdapter()
	fc := &fakeClient{resumeErr: errors.New("failed to send message: JSON-RPC Error -32603: session not found")}
	fc.handOut = &fakeSession{sessionID: "sdk-fresh"}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}

	if _, err := p.OpenSession(context.Background(), &v2.OpenSessionRequest{SessionId: "adapter-f", Config: map[string]string{"model": "m2"}}); err != nil {
		t.Fatalf("OpenSession = %v", err)
	}
	if len(fc.resumeIDs) != 1 || fc.resumeIDs[0] != "sdk-stale" {
		t.Fatalf("resume attempts = %v, want [sdk-stale]", fc.resumeIDs)
	}
	if fc.createCount != 1 {
		t.Fatalf("create calls = %d, want 1 (fallback after failed resume)", fc.createCount)
	}
	if got := loadPersistedSDKSessionID("adapter-f"); got != "sdk-fresh" {
		t.Fatalf("persisted ID after fallback = %q, want sdk-fresh", got)
	}
}

func TestBuildResumeConfigCarriesSessionSetup(t *testing.T) {
	permissionHandler := func(_ copilot.PermissionRequest, _ copilot.PermissionInvocation) (rpc.PermissionDecision, error) {
		return &rpc.PermissionDecisionApproveOnce{}, nil
	}
	sc := &copilot.SessionConfig{
		Model:               "m",
		Streaming:           copilot.Bool(true),
		SystemMessage:       &copilot.SystemMessageConfig{Content: "sp"},
		Provider:            &copilot.ProviderConfig{Type: "openai", BaseURL: "http://ollama.local/v1"},
		OnPermissionRequest: permissionHandler,
		WorkingDirectory:    "/w",
	}
	rc := buildResumeConfig(sc)
	if rc.Model != "m" || rc.Streaming == nil || !*rc.Streaming {
		t.Fatalf("resume config lost model/streaming: %+v", rc)
	}
	if rc.SystemMessage == nil || rc.SystemMessage.Content != "sp" {
		t.Fatalf("resume config lost the system message: %+v", rc.SystemMessage)
	}
	if rc.Provider == nil || rc.Provider.BaseURL != "http://ollama.local/v1" {
		t.Fatalf("resume config lost the provider: %+v", rc.Provider)
	}
	if rc.OnPermissionRequest == nil || rc.WorkingDirectory != "/w" {
		t.Fatalf("resume config lost the permission callback/working directory: %+v", rc)
	}
	if buildResumeConfig(nil) != nil {
		t.Fatal("buildResumeConfig(nil) must return nil")
	}
}
