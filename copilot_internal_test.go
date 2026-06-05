package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	copilot "github.com/github/copilot-sdk/go"
	"github.com/github/copilot-sdk/go/rpc"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

type recordingSender struct {
	mu     sync.Mutex
	events []*v2.ExecuteEvent
}

func (r *recordingSender) Send(event *v2.ExecuteEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}

func (r *recordingSender) snapshot() []*v2.ExecuteEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*v2.ExecuteEvent, len(r.events))
	copy(out, r.events)
	return out
}

type fakeSession struct {
	mu          sync.Mutex
	handlers    []copilot.SessionEventHandler
	emitOnSend  []copilot.SessionEvent
	disconnect  func() error
	destroyed   bool
	setModelErr error
	sendErr     error

	// setModelCalls records the (model, effort) pairs passed to SetModel in order.
	setModelCalls []setModelCall

	// W15: per-Send hooks and recording.
	//   sendCount    — incremented on each Send call (under mu).
	//   sentOpts     — all MessageOptions passed to Send, in order.
	//   onSend       — optional hook called (with current call index and opts)
	//                  BEFORE events fire; allows tests to mutate sessionState.
	//   sendSequence — if non-nil, each element is the event slice to emit on
	//                  the corresponding Send call (overrides emitOnSend).
	sendCount    int
	sentOpts     []copilot.MessageOptions
	onSend       func(callIndex int, opts copilot.MessageOptions)
	sendSequence [][]copilot.SessionEvent
}

type setModelCall struct {
	model  string
	effort string // empty string when opts == nil or opts.ReasoningEffort == nil
}

func (f *fakeSession) On(handler copilot.SessionEventHandler) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, handler)
	idx := len(f.handlers) - 1
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if idx >= 0 && idx < len(f.handlers) {
			f.handlers[idx] = nil
		}
	}
}

func (f *fakeSession) Send(_ context.Context, opts *copilot.MessageOptions) (string, error) {
	if f.sendErr != nil {
		return "", f.sendErr
	}
	f.mu.Lock()
	callIndex := f.sendCount
	f.sendCount++
	f.sentOpts = append(f.sentOpts, *opts)
	onSend := f.onSend
	var events []copilot.SessionEvent
	if f.sendSequence != nil && callIndex < len(f.sendSequence) {
		events = append([]copilot.SessionEvent(nil), f.sendSequence[callIndex]...)
	} else {
		events = append([]copilot.SessionEvent(nil), f.emitOnSend...)
	}
	handlers := append([]copilot.SessionEventHandler(nil), f.handlers...)
	f.mu.Unlock()

	if onSend != nil {
		onSend(callIndex, *opts)
	}
	for _, event := range events {
		for _, handler := range handlers {
			if handler != nil {
				handler(event)
			}
		}
	}
	return "msg-1", nil
}

func (f *fakeSession) getSentOpts() []copilot.MessageOptions {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]copilot.MessageOptions, len(f.sentOpts))
	copy(out, f.sentOpts)
	return out
}

func (f *fakeSession) SetModel(_ context.Context, model string, opts *copilot.SetModelOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := setModelCall{model: model}
	if opts != nil && opts.ReasoningEffort != nil {
		call.effort = *opts.ReasoningEffort
	}
	f.setModelCalls = append(f.setModelCalls, call)
	return f.setModelErr
}

func (f *fakeSession) getSetModelCalls() []setModelCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]setModelCall, len(f.setModelCalls))
	copy(out, f.setModelCalls)
	return out
}

func (f *fakeSession) Disconnect() error {
	if f.disconnect != nil {
		return f.disconnect()
	}
	return nil
}

func (f *fakeSession) Destroy() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroyed = true
	return nil
}

// TestParseOutcome was removed in W15: outcome parsing via RESULT: prefix no longer exists.
// Outcome is now determined by the submit_outcome tool call; see copilot_outcome_test.go.

func TestStringifyAny(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if got := stringifyAny(nil); got != "" {
			t.Fatalf("stringifyAny(nil) = %q, want empty string", got)
		}
	})

	t.Run("map", func(t *testing.T) {
		got := stringifyAny(map[string]any{"tool": "bash"})
		if got == "" || got == "<nil>" {
			t.Fatalf("stringifyAny(map) returned empty/invalid: %q", got)
		}
	})
}

func TestPermissionDetails(t *testing.T) {
	t.Setenv(includeSensitivePermissionDetailsEnv, "")

	toolCallID := "tc-1"
	intention := "write file"
	fullCommand := "echo hi > out.txt"
	warning := "danger"
	path := "out.txt"
	cmds := []copilot.PermissionRequestShellCommand{{Identifier: "echo", ReadOnly: false}}

	request := copilot.PermissionRequestShell{
		ToolCallID:      &toolCallID,
		Intention:       intention,
		FullCommandText: fullCommand,
		Warning:         &warning,
		PossiblePaths:   []string{path},
		Commands:        cmds,
	}

	details := permissionDetails(request)
	if details["kind"] == "" {
		t.Fatalf("expected kind in details")
	}
	if details["tool_call_id"] != toolCallID {
		t.Fatalf("tool_call_id = %q, want %q", details["tool_call_id"], toolCallID)
	}
	if details["commands"] != "echo" {
		t.Fatalf("commands = %q, want %q", details["commands"], "echo")
	}
	if _, ok := details["request_json"]; ok {
		t.Fatalf("request_json should be redacted by default")
	}
	if _, ok := details["full_command_text"]; ok {
		t.Fatalf("full_command_text should be redacted by default")
	}
	if _, ok := details["path"]; ok {
		t.Fatalf("path should be redacted by default")
	}
}

func TestPermissionDetailsSensitiveOptIn(t *testing.T) {
	t.Setenv(includeSensitivePermissionDetailsEnv, "1")

	toolCallID := "tc-2"
	fullCommand := "echo hello > secret.txt"
	path := "secret.txt"
	request := copilot.PermissionRequestShell{
		ToolCallID:      &toolCallID,
		FullCommandText: fullCommand,
		PossiblePaths:   []string{path},
	}

	details := permissionDetails(request)
	if details["request_json"] == "" {
		t.Fatalf("expected request_json when sensitive details are enabled")
	}
	if details["full_command_text"] != fullCommand {
		t.Fatalf("full_command_text = %q, want %q", details["full_command_text"], fullCommand)
	}
	if details["path"] != path {
		t.Fatalf("path = %q, want %q", details["path"], path)
	}
}

// TestResolveGitHubTokenPrecedence checks the GitHub token is resolved from the
// secret channel in the documented precedence order, never from process env.
func TestResolveGitHubTokenPrecedence(t *testing.T) {
	declared := declaredGitHubTokenSecrets()
	cases := []struct {
		name      string
		delivered map[string]string
		want      string
	}{
		{"copilot_token_precedence", map[string]string{"COPILOT_GITHUB_TOKEN": "copilot-token", "GH_TOKEN": "gh-token", "GITHUB_TOKEN": "github-token"}, "copilot-token"},
		{"gh_token_fallback", map[string]string{"GH_TOKEN": "gh-token", "GITHUB_TOKEN": "github-token"}, "gh-token"},
		{"github_token_fallback", map[string]string{"GITHUB_TOKEN": "github-token"}, "github-token"},
		{"empty_when_absent", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sec := adapterhost.NewSecrets(declared, tc.delivered)
			if got := resolveGitHubToken(sec); got != tc.want {
				t.Fatalf("resolveGitHubToken() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestResolveGitHubTokenIgnoresEnv proves the token comes from the secret
// channel, not the process environment (D69): an env var must not leak in even
// when the channel delivered nothing.
func TestResolveGitHubTokenIgnoresEnv(t *testing.T) {
	t.Setenv("COPILOT_GITHUB_TOKEN", "from-env")
	t.Setenv("GH_TOKEN", "from-env")
	t.Setenv("GITHUB_TOKEN", "from-env")
	sec := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil)
	if got := resolveGitHubToken(sec); got != "" {
		t.Fatalf("resolveGitHubToken must ignore process env; got %q", got)
	}
}

// TestEnsureClientFailsClosedWithoutSecret verifies ensureClient returns a
// clear missing-secret error (before any CLI is started) when no token was
// delivered over the secret channel.
func TestEnsureClientFailsClosedWithoutSecret(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "from-env") // present in env, absent from the channel
	p := &copilotAdapter{}
	sec := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil)
	if _, err := p.ensureClient(context.Background(), sec); err == nil {
		t.Fatal("ensureClient must fail closed when no GitHub token is delivered")
	} else if !strings.Contains(err.Error(), "secret channel") {
		t.Fatalf("error should mention the secret channel; got %v", err)
	}
}

// TestPermissionAutoApprove verifies that handlePermissionRequest returns
// Approved and emits a permission.request adapter event when the host approves.
func TestPermissionAutoApprove(t *testing.T) {
	sender := &recordingSender{}
	s := &sessionState{
		session:  &fakeSession{},
		active:   true,
		activeCh: make(chan struct{}),
		sink:     sender,
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}

	toolCallID := "tc-123"
	request := copilot.PermissionRequestShell{
		ToolCallID: &toolCallID,
	}

	// handlePermissionRequest blocks until the host resolves the pending perm.
	// Simulate the host approving once the pending perm is registered.
	go func() {
		deadline := time.After(2 * time.Second)
		for {
			for _, ev := range sender.snapshot() {
				if a := ev.GetAdapter(); a != nil && a.GetEventKind() == "permission.request" && a.GetPayload() != nil {
					if requestID, ok := a.GetPayload().AsMap()["request_id"].(string); ok && requestID != "" {
						if ch := p.resolvePendingPerm(requestID); ch != nil {
							ch <- "allow"
							return
						}
					}
				}
			}
			select {
			case <-deadline:
				t.Errorf("timeout waiting for permission.request event")
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()

	result, err := p.handlePermissionRequest("s1", request)
	if err != nil {
		t.Fatalf("handlePermissionRequest returned error: %v", err)
	}
	if result.Kind() != rpc.PermissionDecisionKindApproveOnce {
		t.Fatalf("permission result kind = %q, want approve-once", result.Kind())
	}

	// Verify the permission.request AdapterEvent was emitted.
	var found bool
	for _, ev := range sender.snapshot() {
		if a := ev.GetAdapter(); a != nil && a.GetEventKind() == "permission.request" {
			found = true
			if a.GetPayload() == nil {
				t.Error("permission.request event must carry a payload")
			}
		}
	}
	if !found {
		t.Fatal("expected permission.request adapter event on Execute sink")
	}
}

func TestExecuteMaxTurnsLimit(t *testing.T) {
	fake := &fakeSession{
		emitOnSend: []copilot.SessionEvent{
			{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "hello"}},
		},
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{
		"s1": {session: fake},
	}}
	sender := &recordingSender{}

	err := p.Execute(context.Background(), &v2.ExecuteRequest{SessionId: "s1", Input: map[string]string{"prompt": "hi", "max_turns": "1"}}, sender)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	hasLimitReached := false
	hasFailure := false
	for _, ev := range sender.snapshot() {
		if adapter := ev.GetAdapter(); adapter != nil && adapter.GetEventKind() == "limit.reached" {
			hasLimitReached = true
		}
		if result := ev.GetResult(); result != nil && result.GetOutcome() == "failure" {
			hasFailure = true
		}
	}
	if !hasLimitReached {
		t.Fatal("expected limit.reached adapter event")
	}
	if !hasFailure {
		t.Fatal("expected failure result event (no allowed_outcomes set means needs_review is not in allowed set)")
	}
}

func TestCloseSessionTimeoutEscalatesToDestroy(t *testing.T) {
	origGrace := closeSessionGrace
	closeSessionGrace = 20 * time.Millisecond
	defer func() { closeSessionGrace = origGrace }()

	release := make(chan struct{})
	fake := &fakeSession{
		disconnect: func() error {
			<-release
			return nil
		},
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{
		"s1": {session: fake},
	}}

	start := time.Now()
	_, err := p.CloseSession(context.Background(), &v2.CloseSessionRequest{SessionId: "s1"})
	if err != nil {
		t.Fatalf("CloseSession returned error: %v", err)
	}

	if time.Since(start) > 250*time.Millisecond {
		t.Fatalf("CloseSession exceeded expected timeout bound: %v", time.Since(start))
	}

	if !fake.destroyed {
		t.Fatal("expected Destroy to be called after disconnect timeout")
	}
	close(release)
}

// ── W09 tests ────────────────────────────────────────────────────────────────

// Test 6.1: OpenSession with reasoning_effort="high" and no model succeeds and
// calls SetModel with the expected effort. Calls the production helper
// applyOpenSessionModel so any regression in it fails this test.
func TestOpenSessionReasoningEffortWithoutModel(t *testing.T) {
	fake := &fakeSession{}
	p := &copilotAdapter{sessions: map[string]*sessionState{}}

	s := &sessionState{
		session: fake,
	}

	cfg := map[string]string{"reasoning_effort": "high"}
	if err := p.applyOpenSessionModel(context.Background(), s, cfg); err != nil {
		t.Fatalf("applyOpenSessionModel returned error: %v", err)
	}

	calls := fake.getSetModelCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 SetModel call, got %d", len(calls))
	}
	if calls[0].effort != "high" {
		t.Errorf("SetModel called with effort=%q, want %q", calls[0].effort, "high")
	}
	if calls[0].model != "" {
		t.Errorf("SetModel called with model=%q, want empty string", calls[0].model)
	}
	if s.defaultEffort != "high" {
		t.Errorf("defaultEffort = %q, want %q", s.defaultEffort, "high")
	}
	if s.defaultModel != "" {
		t.Errorf("defaultModel = %q, want empty string", s.defaultModel)
	}
}

// Test 6.2: OpenSession with both reasoning_effort and model set (regression guard).
// Calls the production helper applyOpenSessionModel so any regression in it fails this test.
func TestOpenSessionReasoningEffortWithModel(t *testing.T) {
	fake := &fakeSession{}
	p := &copilotAdapter{sessions: map[string]*sessionState{}}

	s := &sessionState{
		session: fake,
	}

	cfg := map[string]string{"model": "claude-sonnet-4.6", "reasoning_effort": "medium"}
	if err := p.applyOpenSessionModel(context.Background(), s, cfg); err != nil {
		t.Fatalf("applyOpenSessionModel returned error: %v", err)
	}

	calls := fake.getSetModelCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 SetModel call, got %d", len(calls))
	}
	if calls[0].model != "claude-sonnet-4.6" {
		t.Errorf("SetModel called with model=%q, want %q", calls[0].model, "claude-sonnet-4.6")
	}
	if calls[0].effort != "medium" {
		t.Errorf("SetModel called with effort=%q, want %q", calls[0].effort, "medium")
	}
	if s.defaultModel != "claude-sonnet-4.6" {
		t.Errorf("defaultModel = %q, want %q", s.defaultModel, "claude-sonnet-4.6")
	}
	if s.defaultEffort != "medium" {
		t.Errorf("defaultEffort = %q, want %q", s.defaultEffort, "medium")
	}
}

// Test 6.3: OpenSession with reasoning_effort="invalid" fails with a clear error.
func TestOpenSessionInvalidReasoningEffort(t *testing.T) {
	err := validateReasoningEffort("invalid")
	if err == nil {
		t.Fatal("expected error for invalid reasoning_effort, got nil")
	}
	if !strings.Contains(err.Error(), "low, medium, high, xhigh") {
		t.Errorf("error message should list valid values; got: %v", err)
	}
}

func TestValidateReasoningEffort(t *testing.T) {
	tests := []struct {
		name    string
		effort  string
		wantErr bool
	}{
		{name: "low", effort: "low"},
		{name: "xhigh", effort: "xhigh"},
		{name: "empty", effort: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReasoningEffort(tt.effort)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateReasoningEffort(%q) error = %v, wantErr %v", tt.effort, err, tt.wantErr)
			}
		})
	}
}

// Test 6.4: Execute with per-step reasoning_effort="high" applies the override and
// restores the agent default ("medium") after the step. Assert the SDK call sequence.
func TestExecutePerStepReasoningEffortRestoresDefault(t *testing.T) {
	fake := &fakeSession{
		emitOnSend: []copilot.SessionEvent{
			{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "working..."}},
			{Data: &copilot.SessionIdleData{}},
		},
	}

	// Session has agent-level default effort "medium".
	s := &sessionState{
		session:       fake,
		defaultEffort: "medium",
		defaultModel:  "",
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}
	sender := &recordingSender{}

	// W15: simulate submit_outcome tool by setting finalizedOutcome via onSend hook
	// before session.idle fires, so awaitOutcome sees the outcome on the first attempt.
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		s.mu.Lock()
		s.finalizedOutcome = "success"
		s.mu.Unlock()
	}

	err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "hi", "reasoning_effort": "high"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Expected call sequence:
	//   1. applyRequestEffort: SetModel("", effort="high")
	//   2. (applyRequestModel: no-op, no model in cfg)
	//   3. Send
	//   4. defer restoreEffort: SetModel("", effort="medium")
	calls := fake.getSetModelCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 SetModel calls (apply + restore), got %d: %+v", len(calls), calls)
	}
	if calls[0].effort != "high" {
		t.Errorf("call[0].effort = %q, want %q (apply override)", calls[0].effort, "high")
	}
	if calls[1].effort != "medium" {
		t.Errorf("call[1].effort = %q, want %q (restore default)", calls[1].effort, "medium")
	}

	// Confirm the step still produced a result event.
	hasSuccess := false
	for _, ev := range sender.snapshot() {
		if r := ev.GetResult(); r != nil && r.GetOutcome() == "success" {
			hasSuccess = true
		}
	}
	if !hasSuccess {
		t.Fatal("expected success result event")
	}
}

// TestExecutePerStepEffortRestoresWhenNoDefault verifies B2 fix: when an agent
// session was opened without a reasoning_effort default, a per-step override
// must still be reversed at the end of the step by calling SetModel with a
// nil opts (clearing the effort), not by being silently skipped.
func TestExecutePerStepEffortRestoresWhenNoDefault(t *testing.T) {
	fake := &fakeSession{
		emitOnSend: []copilot.SessionEvent{
			{Data: &copilot.AssistantMessageData{MessageID: "m1", Content: "working..."}},
			{Data: &copilot.SessionIdleData{}},
		},
	}

	// Session has NO agent-level default effort (opened without reasoning_effort in config).
	s := &sessionState{
		session:       fake,
		defaultEffort: "",
		defaultModel:  "",
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}
	sender := &recordingSender{}

	// W15: simulate submit_outcome tool via onSend hook.
	fake.onSend = func(_ int, _ copilot.MessageOptions) {
		s.mu.Lock()
		s.finalizedOutcome = "success"
		s.mu.Unlock()
	}

	if err := p.Execute(context.Background(), &v2.ExecuteRequest{
		SessionId:       "s1",
		Input:           map[string]string{"prompt": "hi", "reasoning_effort": "high"},
		AllowedOutcomes: []string{"failure", "success"},
	}, sender); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Expected SDK call sequence:
	//   1. applyRequestEffort:  SetModel("", effort="high")   — apply override
	//   2. defer restoreEffort: SetModel("", nil)             — restore: clear effort
	calls := fake.getSetModelCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 SetModel calls (apply + restore), got %d: %+v", len(calls), calls)
	}
	if calls[0].effort != "high" {
		t.Errorf("calls[0].effort = %q, want %q (apply override)", calls[0].effort, "high")
	}
	// Restore call must have empty effort (nil opts → "" effort in the fake).
	if calls[1].effort != "" {
		t.Errorf("calls[1].effort = %q, want empty string (restore: clear effort)", calls[1].effort)
	}
}

// TestBuildProviderConfig covers BYOK provider plumbing for vllm/ollama-style
// custom endpoints. Provider is nil unless provider_base_url is set; otherwise
// fields are passed through verbatim with Azure options materialized lazily.
func TestBuildProviderConfig(t *testing.T) {
	if got := buildProviderConfig(map[string]string{}); got != nil {
		t.Fatalf("empty config: got %+v, want nil", got)
	}
	if got := buildProviderConfig(map[string]string{"provider_type": "openai"}); got != nil {
		t.Fatalf("no base_url: got %+v, want nil (provider must be opt-in via base_url)", got)
	}

	// Ollama-style local endpoint, no API key required.
	ollama := buildProviderConfig(map[string]string{
		"provider_base_url": "http://localhost:11434/v1",
	})
	if ollama == nil {
		t.Fatal("ollama config: got nil, want provider")
	}
	if ollama.BaseURL != "http://localhost:11434/v1" {
		t.Errorf("BaseURL = %q, want ollama endpoint", ollama.BaseURL)
	}
	if ollama.Azure != nil {
		t.Errorf("Azure = %+v, want nil when provider_azure_api_version unset", ollama.Azure)
	}

	// Azure provider with API version surfaces Azure options.
	azure := buildProviderConfig(map[string]string{
		"provider_type":              "azure",
		"provider_base_url":          "https://example.openai.azure.com",
		"provider_api_key":           "secret",
		"provider_bearer_token":      "bearer",
		"provider_wire_api":          "responses",
		"provider_azure_api_version": "2024-10-21",
	})
	if azure == nil {
		t.Fatal("azure config: got nil, want provider")
	}
	if azure.Type != "azure" || azure.WireAPI != "responses" {
		t.Errorf("Type/WireAPI = %q/%q, want azure/responses", azure.Type, azure.WireAPI)
	}
	if azure.APIKey != "secret" || azure.BearerToken != "bearer" {
		t.Errorf("APIKey/BearerToken = %q/%q, want secret/bearer", azure.APIKey, azure.BearerToken)
	}
	if azure.Azure == nil || azure.Azure.APIVersion != "2024-10-21" {
		t.Errorf("Azure.APIVersion = %+v, want 2024-10-21", azure.Azure)
	}
}
