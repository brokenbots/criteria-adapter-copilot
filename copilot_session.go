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
	// SessionID exposes the SDK session identifier (persisted so a respawned
	// adapter or a restarted CLI child can resume the conversation).
	SessionID() string
	Disconnect() error
	// Destroy is a force-close path used when Disconnect stalls; the real SDK
	// implementation delegates to Disconnect, so we follow suit below.
	Destroy() error
}

// copilotClient abstracts the Copilot SDK client lifecycle (start/stop the CLI
// child, create or resume SDK sessions, liveness probe) so the CLI-restart
// path is testable without spawning a real CLI (CRI-272).
type copilotClient interface {
	Start(ctx context.Context) error
	Stop() error
	// Ping probes the runtime; a non-nil error means the CLI child or its
	// stdio connection is gone.
	Ping(ctx context.Context, message string) error
	CreateSession(ctx context.Context, config *copilot.SessionConfig) (copilotSession, error)
	ResumeSessionWithOptions(ctx context.Context, sessionID string, config *copilot.ResumeSessionConfig) (copilotSession, error)
}

// sdkClient adapts the concrete *copilot.Client to copilotClient, wrapping
// returned sessions in sdkSession.
type sdkClient struct {
	inner *copilot.Client
}

func (c *sdkClient) Start(ctx context.Context) error { return c.inner.Start(ctx) }

func (c *sdkClient) Stop() error { return c.inner.Stop() }

func (c *sdkClient) Ping(ctx context.Context, message string) error {
	_, err := c.inner.Ping(ctx, message)
	return err
}

func (c *sdkClient) CreateSession(ctx context.Context, config *copilot.SessionConfig) (copilotSession, error) {
	session, err := c.inner.CreateSession(ctx, config)
	if err != nil {
		return nil, err
	}
	return &sdkSession{inner: session}, nil
}

func (c *sdkClient) ResumeSessionWithOptions(ctx context.Context, sessionID string, config *copilot.ResumeSessionConfig) (copilotSession, error) {
	session, err := c.inner.ResumeSessionWithOptions(ctx, sessionID, config)
	if err != nil {
		return nil, err
	}
	return &sdkSession{inner: session}, nil
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

func (s *sdkSession) SessionID() string { return s.inner.SessionID }

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

	// CRI-272 recovery state. owner is the adapter that opened this session
	// (nil for bare unit-test states): sendWithRetry consults it to restart a
	// dead CLI child, and it is the only path back to the adapter's mutexes.
	owner *copilotAdapter

	// adapterSessionID identifies this session on the adapter protocol; the
	// Copilot SDK session ID is persisted under it.
	adapterSessionID string

	// sessionConfig and resumeConfig are retained so a CLI-child restart can
	// re-open the SDK session with the same setup (model, provider/BYOK,
	// tools, permission gating, streaming, system message). secrets is the
	// resolved secret set for the same purpose. All three are written once at
	// open and read-only afterwards.
	sessionConfig *copilot.SessionConfig
	resumeConfig  *copilot.ResumeSessionConfig
	secrets       *adapterhost.Secrets

	// fanout routes SDK session events to every active subscriber
	// (Execute-level handlers registered via subscribeEvents). It is
	// re-registered on each underlying SDK session by swapSession so an
	// in-flight turn keeps receiving events across a CLI-child restart.
	// eventUnregister removes fanout.handle from the current SDK session.
	fanout          *eventFanout
	eventUnregister func()
	eventMu         sync.Mutex
}

// eventFanout fans SDK session events out to all subscribed handlers. It is
// re-registered on every underlying SDK session (see swapSession) so an active
// Execute keeps receiving events across a CLI-child restart (CRI-272).
type eventFanout struct {
	mu       sync.Mutex
	handlers map[int]copilot.SessionEventHandler
	nextID   int
}

func newEventFanout() *eventFanout {
	return &eventFanout{handlers: map[int]copilot.SessionEventHandler{}}
}

// handle delivers one SDK event to every current subscriber.
func (f *eventFanout) handle(event copilot.SessionEvent) {
	f.mu.Lock()
	handlers := make([]copilot.SessionEventHandler, 0, len(f.handlers))
	for _, h := range f.handlers {
		handlers = append(handlers, h)
	}
	f.mu.Unlock()
	for _, h := range handlers {
		if h != nil {
			h(event)
		}
	}
}

// subscribe registers a handler; the returned func unsubscribes it.
func (f *eventFanout) subscribe(h copilot.SessionEventHandler) func() {
	if h == nil {
		return func() {}
	}
	f.mu.Lock()
	id := f.nextID
	f.nextID++
	f.handlers[id] = h
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.handlers, id)
	}
}

// subscribeEvents routes a turn's event handler through the session's fanout
// when one is attached (sessions opened through OpenSession), keeping the
// subscription alive across CLI-child restarts; bare sessionStates (unit
// tests) fall back to a direct SDK registration.
func (s *sessionState) subscribeEvents(h copilot.SessionEventHandler) func() {
	s.eventMu.Lock()
	fanout := s.fanout
	s.eventMu.Unlock()
	if fanout == nil {
		return s.session.On(h)
	}
	return fanout.subscribe(h)
}

// swapSession atomically replaces the SDK session backing an adapter session
// after a CLI-child restart and re-attaches the event fanout to the new
// session, so an in-flight turn keeps receiving events. The replaced session
// is disconnected best-effort.
func (s *sessionState) swapSession(sess copilotSession) {
	s.eventMu.Lock()
	old := s.session
	if s.fanout != nil {
		if s.eventUnregister != nil {
			s.eventUnregister()
		}
		s.eventUnregister = sess.On(s.fanout.handle)
	}
	s.session = sess
	s.eventMu.Unlock()
	if old != nil {
		_ = old.Disconnect()
	}
}

// newSessionState builds the adapter's per-session state and attaches the
// event fanout to the SDK session so a later CLI-child restart can re-register
// it (CRI-272).
func newSessionState(
	adapterSessionID string,
	sess copilotSession,
	sessionConfig *copilot.SessionConfig,
	resumeConfig *copilot.ResumeSessionConfig,
	secrets *adapterhost.Secrets,
	owner *copilotAdapter,
) *sessionState {
	fanout := newEventFanout()
	s := &sessionState{
		session:          sess,
		owner:            owner,
		adapterSessionID: adapterSessionID,
		sessionConfig:    sessionConfig,
		resumeConfig:     resumeConfig,
		secrets:          secrets,
		fanout:           fanout,
	}
	s.eventUnregister = sess.On(fanout.handle)
	return s
}

// buildResumeConfig derives a ResumeSessionConfig from the session config so a
// resumed SDK session keeps the same provider (BYOK), model, tools,
// permission-gating callback, streaming and system-message setup as the
// original session. Returns nil when there is no session config to derive from.
func buildResumeConfig(sc *copilot.SessionConfig) *copilot.ResumeSessionConfig {
	if sc == nil {
		return nil
	}
	return &copilot.ResumeSessionConfig{
		ClientName:          sc.ClientName,
		Model:               sc.Model,
		Tools:               sc.Tools,
		SystemMessage:       sc.SystemMessage,
		Provider:            sc.Provider,
		Streaming:           sc.Streaming,
		OnPermissionRequest: sc.OnPermissionRequest,
		WorkingDirectory:    sc.WorkingDirectory,
	}
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
	resumeConfig := buildResumeConfig(sessionConfig)
	session, _, err := p.openSDKSession(ctx, client, adapterSessionID, sessionConfig, resumeConfig)
	if err != nil {
		return nil, err
	}

	s := newSessionState(adapterSessionID, session, sessionConfig, resumeConfig, secrets, p)
	s.heldSecrets = heldGitHubTokenSecrets(secrets)

	p.mu.Lock()
	p.sessions[adapterSessionID] = s
	p.mu.Unlock()

	if err := p.applyOpenSessionModel(ctx, s, cfg); err != nil {
		return nil, err
	}

	return &v2.OpenSessionResponse{}, nil
}

// openSDKSession resumes the persisted SDK session for adapterSessionID when
// one exists (CRI-272: a resumed session restores the full conversation
// history), falling back to CreateSession on first open, when no ID is
// persisted, or when resume fails. A newly created session's ID is persisted
// for later respawns. Returns the session and whether it was resumed.
func (p *copilotAdapter) openSDKSession(ctx context.Context, client copilotClient, adapterSessionID string, sessionConfig *copilot.SessionConfig, resumeConfig *copilot.ResumeSessionConfig) (copilotSession, bool, error) {
	if persistedID := loadPersistedSDKSessionID(adapterSessionID); persistedID != "" {
		slog.Info("copilot: resuming persisted sdk session",
			"adapterSession", adapterSessionID, "sdkSession", persistedID)
		sess, err := client.ResumeSessionWithOptions(ctx, persistedID, resumeConfig)
		if err == nil {
			return sess, true, nil
		}
		slog.Warn("copilot: sdk session resume failed; creating a fresh session",
			"adapterSession", adapterSessionID, "sdkSession", persistedID, "err", err)
	}
	sess, err := client.CreateSession(ctx, sessionConfig)
	if err != nil {
		return nil, false, fmt.Errorf("copilot: create session: %w", err)
	}
	persistSDKSessionID(adapterSessionID, sess.SessionID())
	return sess, false, nil
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
