// copilot_session.go — Copilot SDK session lifecycle: open, model setup, and close.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	copilot "github.com/github/copilot-sdk/go"
	"github.com/github/copilot-sdk/go/rpc"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// copilotSession abstracts the Copilot SDK session for testing.
type copilotSession interface {
	On(handler copilot.SessionEventHandler) func()
	Send(ctx context.Context, options *copilot.MessageOptions) (string, error)
	SetModel(ctx context.Context, model string, opts *copilot.SetModelOptions) error
	Disconnect() error
	// Destroy is a force-close path used when Disconnect stalls; the real SDK
	// implementation delegates to Disconnect, so we follow suit below.
	Destroy() error
}

// sdkSession wraps a real Copilot SDK session to satisfy copilotSession.
type sdkSession struct {
	inner *copilot.Session
}

func (s *sdkSession) On(handler copilot.SessionEventHandler) func() {
	return s.inner.On(handler)
}

func (s *sdkSession) Send(ctx context.Context, options *copilot.MessageOptions) (string, error) {
	return s.inner.Send(ctx, *options)
}

func (s *sdkSession) SetModel(ctx context.Context, model string, opts *copilot.SetModelOptions) error {
	return s.inner.SetModel(ctx, model, opts)
}

func (s *sdkSession) Disconnect() error {
	return s.inner.Disconnect()
}

// Destroy calls Disconnect, matching the SDK's own deprecated Destroy behaviour
// without invoking the deprecated method.
func (s *sdkSession) Destroy() error {
	return s.inner.Disconnect()
}

// sessionState holds all per-session runtime state for the copilot adapter.
type sessionState struct {
	session copilotSession

	execMu sync.Mutex

	mu       sync.Mutex
	active   bool
	activeCh chan struct{}
	sink     adapterhost.ExecuteEventSender

	// defaultModel and defaultEffort record the agent-level model and
	// reasoning_effort values set at OpenSession time. applyRequestEffort uses
	// these to restore the session's effort after a per-step override.
	// These values are constant for the lifetime of the session; any future
	// feature that dynamically updates the agent default mid-run must update
	// these fields accordingly.
	defaultModel  string
	defaultEffort string

	// submit_outcome per-execute state (mu-guarded). Reset at every
	// beginExecution call. activeAllowedOutcomes is the set the host declared
	// via ExecuteRequest.AllowedOutcomes for the current step; finalizedOutcome
	// captures a successful tool call; finalizeAttempts counts invocations
	// (valid + invalid) for the 3-attempt cap; finalizeFailureKind records the
	// reason category for the most-recent failed invocation ("missing",
	// "invalid_outcome", "duplicate", or "no_outcomes") and is used by
	// failExhausted to emit a structured diagnostic event.
	activeAllowedOutcomes map[string]struct{}
	finalizedOutcome      string
	finalizedReason       string
	finalizeAttempts      int
	finalizeFailureKind   string

	// heldSecrets caches non-empty values the adapter received over the secret
	// channel during OpenSession. These are the only secrets the adapter
	// redacts from `reason` before emitting step outputs.
	heldSecrets []string
}

// sdkSessionIDPath returns the file that persists the Copilot SDK session ID
// for the given adapter session, under CRITERIA_HOME (a PVC-backed path in k8s
// per-scope topology). Persisting the ID lets a respawned adapter process
// resume the SDK conversation (full history) instead of starting cold
// (CRI-272: every session death was losing the whole conversation context).
func sdkSessionIDPath(adapterSessionID string) string {
	home := os.Getenv("CRITERIA_HOME")
	if strings.TrimSpace(home) == "" {
		home = os.Getenv("HOME")
	}
	if strings.TrimSpace(home) == "" {
		home = "/tmp"
	}
	return filepath.Join(home, ".copilot-adapter", "sdk-sessions", adapterSessionID+".id")
}

// loadPersistedSDKSessionID returns the previously persisted Copilot SDK
// session ID for the adapter session, or "" when none is recorded.
func loadPersistedSDKSessionID(adapterSessionID string) string {
	data, err := os.ReadFile(sdkSessionIDPath(adapterSessionID))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// persistSDKSessionID records the Copilot SDK session ID so a later adapter
// process (respawn after OOM/crash) can resume the same conversation.
func persistSDKSessionID(adapterSessionID, sdkSessionID string) {
	if adapterSessionID == "" || sdkSessionID == "" {
		return
	}
	path := sdkSessionIDPath(adapterSessionID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		slog.Warn("copilot: persist sdk session id: mkdir failed", "path", filepath.Dir(path), "err", err)
		return
	}
	if err := os.WriteFile(path, []byte(sdkSessionID), 0o600); err != nil {
		slog.Warn("copilot: persist sdk session id failed", "path", path, "err", err)
	}
}

func (p *copilotAdapter) OpenSession(ctx context.Context, req *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	// Resolved secrets for this session, constrained to the names the adapter
	// declared in Info().Secrets. The GitHub token is sourced from here, never
	// from the process environment (D69).
	secrets := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), req.GetSecrets())
	client, err := p.ensureClient(ctx, secrets)
	if err != nil {
		return nil, err
	}

	cfg := req.GetConfig()
	adapterSessionID := req.GetSessionId()
	sessionConfig := p.buildSessionConfig(cfg, adapterSessionID)

	// CRI-272: if this adapter session was opened before (adapter process
	// respawned after a crash/OOM), resume the persisted Copilot SDK session
	// instead of creating a fresh one — the SDK restores the full conversation
	// history, so the developer is NOT reset. A fresh CreateSession is the
	// fallback (first open) and when a resume fails.
	var session *copilot.Session
	if persistedID := loadPersistedSDKSessionID(adapterSessionID); persistedID != "" {
		slog.Info("copilot: resuming persisted sdk session",
			"adapterSession", adapterSessionID, "sdkSession", persistedID)
		session, err = client.ResumeSessionWithOptions(ctx, persistedID, &copilot.ResumeSessionConfig{
			ClientName: sessionConfig.ClientName,
			Model:      sessionConfig.Model,
			Tools:      sessionConfig.Tools,
		})
		if err != nil {
			slog.Warn("copilot: sdk session resume failed; creating a fresh session",
				"adapterSession", adapterSessionID, "sdkSession", persistedID, "err", err)
			session = nil
		}
	}
	if session == nil {
		session, err = client.CreateSession(ctx, sessionConfig)
		if err != nil {
			return nil, fmt.Errorf("copilot: create session: %w", err)
		}
		persistSDKSessionID(adapterSessionID, session.SessionID)
	}

	s := &sessionState{
		session:     &sdkSession{inner: session},
		heldSecrets: heldGitHubTokenSecrets(secrets),
	}

	p.mu.Lock()
	p.sessions[adapterSessionID] = s
	p.mu.Unlock()

	if err := p.applyOpenSessionModel(ctx, s, cfg); err != nil {
		return nil, err
	}

	return &v2.OpenSessionResponse{}, nil
}

// buildSessionConfig constructs the SDK SessionConfig from agent-level config fields.
func (p *copilotAdapter) buildSessionConfig(cfg map[string]string, adapterSessionID string) *copilot.SessionConfig {
	// Register submit_outcome once per session. Validation against the active
	// step's allowed set happens in handleSubmitOutcome at call time so that
	// per-step scoping works without recreating the session.
	submitTool := copilot.DefineTool(
		submitOutcomeToolName,
		submitOutcomeToolDescription,
		func(args SubmitOutcomeArgs, _ copilot.ToolInvocation) (copilot.ToolResult, error) {
			return p.handleSubmitOutcome(adapterSessionID, args)
		},
	)
	submitTool.SkipPermission = true

	// Register adapter_tool (CRI-178): the agent-invocable channel for calls
	// to other adapters' tools. SkipPermission stays false — the call the
	// handler issues is itself the gated event end to end (ADR-0004: a tool
	// call IS a permission request) — and the SDK's own tool-permission
	// request for this tool is answered locally in handlePermissionRequest so
	// it adds no second host gate on top of the wire call. submit_outcome is
	// the opposite case: SkipPermission=true because it is the outcome
	// channel, not a gated side effect.
	adapterTool := copilot.DefineTool(
		adapterToolToolName,
		adapterToolToolDescription,
		func(args AdapterToolArgs, invocation copilot.ToolInvocation) (copilot.ToolResult, error) {
			return p.handleAdapterToolCall(adapterSessionID, invocation, args)
		},
	)

	sc := &copilot.SessionConfig{
		Streaming: copilot.Bool(true),
		Model:     cfg["model"],
		OnPermissionRequest: func(r copilot.PermissionRequest, _ copilot.PermissionInvocation) (rpc.PermissionDecision, error) {
			return p.handlePermissionRequest(adapterSessionID, r)
		},
		Tools: []copilot.Tool{submitTool, adapterTool},
	}
	if wd := strings.TrimSpace(cfg["working_directory"]); wd != "" {
		sc.WorkingDirectory = wd
	}
	if sp := strings.TrimSpace(cfg["system_prompt"]); sp != "" {
		sc.SystemMessage = &copilot.SystemMessageConfig{Content: sp}
	}
	if pc := buildProviderConfig(cfg); pc != nil {
		sc.Provider = pc
	}
	return sc
}

// buildProviderConfig assembles a Copilot SDK ProviderConfig (BYOK) from the
// flat agent config map. Returns nil when provider_base_url is empty, so the
// session falls back to GitHub Copilot's default backend.
//
// Telemetry: the adapter intentionally never sets ClientOptions.Telemetry, so
// COPILOT_OTEL_ENABLED stays unset and the CLI does not export OTel traces.
func buildProviderConfig(cfg map[string]string) *copilot.ProviderConfig {
	baseURL := strings.TrimSpace(cfg["provider_base_url"])
	if baseURL == "" {
		return nil
	}
	pc := &copilot.ProviderConfig{
		Type:        strings.TrimSpace(cfg["provider_type"]),
		WireAPI:     strings.TrimSpace(cfg["provider_wire_api"]),
		BaseURL:     baseURL,
		APIKey:      cfg["provider_api_key"],
		BearerToken: cfg["provider_bearer_token"],
	}
	if v := strings.TrimSpace(cfg["provider_azure_api_version"]); v != "" {
		pc.Azure = &copilot.AzureProviderOptions{APIVersion: v}
	}
	return pc
}

// applyOpenSessionModel validates and applies model/reasoning_effort at session open,
// then captures the agent-level defaults into s for per-step restore.
func (p *copilotAdapter) applyOpenSessionModel(ctx context.Context, s *sessionState, cfg map[string]string) error {
	model := strings.TrimSpace(cfg["model"])
	effort := strings.TrimSpace(cfg["reasoning_effort"])

	if effort != "" {
		if err := validateReasoningEffort(effort); err != nil {
			return err
		}
	}

	if model != "" || effort != "" {
		var opts *copilot.SetModelOptions
		if effort != "" {
			opts = &copilot.SetModelOptions{ReasoningEffort: &effort}
		}
		if err := s.session.SetModel(ctx, model, opts); err != nil {
			return fmt.Errorf("copilot: set model at open: %w", err)
		}
	}

	// Capture agent-level defaults so per-step overrides can restore them.
	s.defaultModel = model
	s.defaultEffort = effort
	return nil
}

func (p *copilotAdapter) CloseSession(_ context.Context, req *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	p.mu.Lock()
	s, ok := p.sessions[req.GetSessionId()]
	if ok {
		delete(p.sessions, req.GetSessionId())
	}
	p.mu.Unlock()
	if !ok {
		return &v2.CloseSessionResponse{}, nil
	}

	disconnectDone := make(chan error, 1)
	go func() {
		disconnectDone <- s.session.Disconnect()
	}()

	select {
	case err := <-disconnectDone:
		if err != nil {
			_ = s.session.Destroy()
			return &v2.CloseSessionResponse{}, fmt.Errorf("copilot: disconnect session: %w", err)
		}
	case <-time.After(closeSessionGrace):
		_ = s.session.Destroy()
	}

	return &v2.CloseSessionResponse{}, nil
}
