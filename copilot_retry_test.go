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

func TestSendWithRetryDeadlineCapsSleep(t *testing.T) {
	origBase, origMax := retryBaseDelay, retryMaxDelay
	retryBaseDelay = 40 * time.Millisecond
	retryMaxDelay = time.Second
	t.Cleanup(func() { retryBaseDelay, retryMaxDelay = origBase, origMax })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
	defer cancel()

	fake := &fakeSession{sendErr: fmt.Errorf("failed to send message: JSON-RPC Error -32603: HTTP 502")}
	s := &sessionState{session: fake}
	_, err := s.sendWithRetry(ctx, &copilot.MessageOptions{Prompt: "hi"})
	if err == nil {
		t.Fatal("expected the last send error after the deadline expired")
	}
	if strings.Contains(err.Error(), "retries exhausted") {
		t.Fatalf("deadline-capped send must surface the last error, not the exhaustion wrap: %v", err)
	}
	if !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("err = %v, want the provider 502 surfaced", err)
	}
	if fake.sendAttempts < 3 || fake.sendAttempts > retryMaxAttempts {
		t.Fatalf("send attempts = %d, want 3..%d (deadline must stop further attempts)", fake.sendAttempts, retryMaxAttempts)
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

// TestRecoverTransportSkipsActiveNonTriggerSessions: recovery reopens the
// triggering session and idle ones, but must not swap a session whose Execute
// is in flight (its own retry loop recovers it) — swapping would race.
func TestRecoverTransportSkipsActiveNonTriggerSessions(t *testing.T) {
	withFastBackoff(t)
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)
	persistSDKSessionID("adapter-1", "sdk-1")
	persistSDKSessionID("adapter-2", "sdk-2")

	p := newCopilotAdapter()
	fc := &fakeClient{pingErr: errors.New("client not connected")}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}
	origNew := newClientFn
	newClientFn = func(*copilot.ClientOptions) copilotClient { return fc }
	t.Cleanup(func() { newClientFn = origNew })

	sc := &copilot.SessionConfig{Model: "m"}
	dead1 := &fakeSession{sessionID: "sdk-1"}
	idle2 := &fakeSession{sessionID: "sdk-2"}
	s1 := newSessionState("adapter-1", dead1, sc, buildResumeConfig(sc), adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil), p)
	s2 := newSessionState("adapter-2", idle2, sc, buildResumeConfig(sc), adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil), p)
	s2.mu.Lock()
	s2.active = true
	s2.mu.Unlock()
	p.sessions["adapter-1"] = s1
	p.sessions["adapter-2"] = s2

	recovered := &fakeSession{sessionID: "sdk-1"}
	fc.handOut = recovered

	p.recoverTransport(context.Background(), s1)

	if s1.session != recovered {
		t.Fatalf("triggering session was not reopened: got %T", s1.session)
	}
	s2.mu.Lock()
	stillIdle := s2.session == idle2
	s2.mu.Unlock()
	if !stillIdle {
		t.Fatal("in-flight (active) non-trigger session must not be swapped concurrently")
	}
	if len(fc.resumeIDs) == 0 || fc.resumeIDs[0] != "sdk-1" {
		t.Fatalf("resume order = %v, want the triggering session resumed", fc.resumeIDs)
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
