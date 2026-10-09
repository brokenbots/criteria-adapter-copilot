// copilot_session.go — Copilot SDK session lifecycle: open, model setup, and close.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	// ForceStop kills the CLI child without waiting for graceful teardown.
	// SDK Stop disconnects every session before killing the child, and those
	// disconnect RPCs are unbounded — a child that stays alive but stops
	// servicing RPCs wedges Stop forever (CRI-274), so bounded recovery kills
	// the child past the grace to fail the pending RPCs.
	ForceStop()
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

func (c *sdkClient) ForceStop() { c.inner.ForceStop() }

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

// CurrentToolMetadata reports the names of the tools the session currently
// serves to the model, via the CLI's session.tools.getCurrentMetadata RPC.
// An empty result is inconclusive, not proof of absence: the CLI returns
// null (and here an empty slice) when the tool set has not been initialized
// yet, and RPC failures are reported as such.
func (s *sdkSession) CurrentToolMetadata(ctx context.Context) ([]string, error) {
	res, err := s.inner.RPC.Tools.GetCurrentMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.Tools) == 0 {
		return []string{}, nil
	}
	names := make([]string, 0, len(res.Tools))
	for _, t := range res.Tools {
		names = append(names, t.Name)
	}
	return names, nil
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
	// "invalid_outcome", "duplicate", "no_outcomes", "invalid_payload", or
	// "comment_missing") and is used by failExhausted to emit a structured
	// diagnostic event.
	activeAllowedOutcomes map[string]struct{}
	finalizedOutcome      string
	finalizedReason       string
	finalizeAttempts      int
	finalizeFailureKind   string

	// KB-47 contract mode. contractMode is true for a step whose
	// ExecuteRequest carried outcome contracts; activeContracts maps outcome
	// name to its contract (nil entry possible via map check), and
	// fallbackContractName holds the single fallback contract's outcome name
	// ("" when none). On finalize, checkContractFinalize validates the
	// submission against the contract in-turn; accepted submissions are
	// validated: finalizedPayload keeps the model-submitted payload verbatim
	// (proto's absent/empty/null = empty-object normalization applied) and
	// finalizedComment the trimmed comment; the turn loop forwards both (with
	// secret redaction) instead of the legacy session-state assembly.
	contractMode         bool
	activeContracts      map[string]*v2.OutcomeContract
	fallbackContractName string

	// finalizeFailureIssues carries the structured validator issue list for the
	// most recent contract rejection (invalid_payload / comment_missing). The
	// in-turn ToolResult surfaces it; handleIdleTurn appends it to the
	// corrective reprompt. Reset at beginExecution.
	finalizeFailureIssues []string

	// finalizedPayload/finalizedComment record the accepted finalize's payload
	// (verbatim, proto normalization applied) and trimmed comment in contract
	// mode; the turn loop forwards them via contractResultEvent.
	finalizedPayload json.RawMessage
	finalizedComment string

	// KB-47 repair state. inRepairMode marks a step being re-finalized after a
	// host ExecutionRejection; inRepairAttempt is the host-provided attempt
	// number echoed in repair events. createdEpoch counts adapter-side SDK
	// session creations (1 on fresh create, bumped by reopenSession when the
	// session could only be created fresh); finalizeSessionEpoch records
	// createdEpoch at the last accepted finalize. In-session repair (minimal
	// repair prompt into the live session) is used when the last finalize is
	// known to still be in the current conversation
	// (finalizeSessionEpoch >= createdEpoch); both zero — bare unit-test
	// states or a Restore reattach, which restores the exact conversation —
	// also count as in-session.
	inRepairMode         bool
	inRepairAttempt      uint32
	createdEpoch         uint32
	finalizeSessionEpoch uint32

	// heldSecrets caches non-empty values the adapter received over the secret
	// channel during OpenSession. These are the only secrets the adapter
	// redacts from `reason` before emitting step outputs.
	heldSecrets []string

	// toolCommands maps a native CLI tool call's ID to its raw command argument
	// ( KB-57/CRI-260 follow-up). The Copilot CLI's shell permission requests
	// carry only parsed command identifiers ("git', not "git status ..."),
	// so the host's allow_tools policy could never match subcommand patterns.
	// ToolExecutionStart events carry the full arguments; the adapter records
	// the command per call ID and permissionDetails attaches it as the
	// details["command"] fingerprint the engine's requestFingerprints matchers
	// consume. Guarded by mu; entries are removed on ToolExecutionComplete and
	// the map is capped to avoid unbounded growth on pathological turns.
	toolCommands map[string]string

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

	// CRI-272 recovery state. The SDK session field is guarded by eventMu and
	// must only be read via currentSession (swapSession, the sole writer, and
	// all readers share that lock — the pre-change write-once invariant ended
	// with mid-run session swaps). boundClientEpoch records the CLI-child
	// runtime epoch this session was opened on: reopenSession re-opens the
	// session only when that epoch is superseded. reopenMu serializes
	// reopenSession per session so concurrent recovery sweeps churn a session
	// at most once.
	boundClientEpoch int
	reopenMu         sync.Mutex

	// CRI-274 watchdog state. lastActivityNs records the unix-nano time of the
	// last observed provider-side activity (any SDK session event, a send
	// attempt, a gate transition) and is read by waitTurnSignal to compute the
	// inter-event silence window. gatedWaits counts adapter-side waits that
	// legitimately suspend provider activity (host permission decisions,
	// adapter tool calls, native tool executions): while positive, the silence
	// window is replaced by watchdogGateWindow. stallNotify (cap 1) wakes a
	// parked waitTurnSignal so it re-reads fresh state; it is nil on bare
	// unit-test states, where every access is nil-safe by construction.
	// toolGates holds one gate release func per open native-tool gate, keyed by
	// tool call ID, so an abandoned call's gates can be force-closed.
	lastActivityNs atomic.Int64
	gatedWaits     atomic.Int64
	stallNotify    chan struct{}
	toolGateMu     sync.Mutex
	toolGates      map[string]func()

	// CRI-288 liveness clock: unix-nano time of the last event the adapter
	// actually forwarded to the host through the Execute sink (see
	// forwardTrackingSink), stamped at beginExecution and re-baselined after
	// each successful liveness tick. The liveness ticker measures host-visible
	// silence from it; unlike lastActivityNs it is NOT provider-side activity
	// and never feeds the watchdog.
	lastForwardNs atomic.Int64

	// CRI-277 per-session watchdog windows parsed from the agent-level config
	// (watchdog_window / watchdog_gate_window). Written once at OpenSession
	// — already resolved, unset keys holding the shipped defaults — and
	// read-only afterwards. The watchdogWindow / watchdogGateWindow
	// accessors' zero-field fallback covers bare unit-test states.
	watchdog watchdogSettings
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
		return s.currentSession().On(h)
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

// currentSession returns the SDK session currently backing this adapter
// session. It shares swapSession's eventMu so mid-run swaps (CLI-child
// restart recovery) cannot race reads from the retry loop, Execute, or close
// paths; all other readers must use this accessor instead of the bare field.
func (s *sessionState) currentSession() copilotSession {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	return s.session
}

// newSessionState builds the adapter's per-session state and attaches the
// event fanout to the SDK session so a later CLI-child restart can re-register
// it (CRI-272).
// toolCommandsMax bounds the per-session command fingerprint map (KB-57). A
// turn normally holds a handful of native tool calls; the cap keeps a
// pathological model turn from growing the map without bound. When full, the
// oldest entry is evicted (map order is random in Go, so use insertion-order
// tracking via a slice queue).
const toolCommandsMax = 128

// recordToolCommand stores the raw command argument the CLI passed to a
// native tool (KB-57). The Copilot CLI's shell permission requests carry only
// parsed command identifiers, so the host policy could never match
// subcommand patterns; permissionDetails attaches this recorded command as
// the details["command"] fingerprint the engine consumes.
func (s *sessionState) recordToolCommand(toolCallID string, command string) {
	if toolCallID == "" || command == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.toolCommands == nil {
		s.toolCommands = make(map[string]string)
	}
	if len(s.toolCommands) >= toolCommandsMax {
		for k := range s.toolCommands {
			delete(s.toolCommands, k)
			break // single eviction is enough; bounded amortized
		}
	}
	s.toolCommands[toolCallID] = command
}

// toolCommandFor returns the recorded raw command for a tool call ID, or "".
func (s *sessionState) toolCommandFor(toolCallID string) string {
	if toolCallID == "" {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.toolCommands[toolCallID]
}

// forgetToolCommand drops the recording for a completed tool call.
func (s *sessionState) forgetToolCommand(toolCallID string) {
	if toolCallID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.toolCommands, toolCallID)
}

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
		stallNotify:      make(chan struct{}, 1),
		toolCommands:     make(map[string]string),
	}
	if owner != nil {
		// Record the CLI-child runtime epoch at open so reopenSession can
		// tell whether this session's binding is superseded after a restart.
		s.boundClientEpoch = owner.currentClientEpoch()
	}
	s.eventUnregister = sess.On(fanout.handle)
	return s
}

// buildResumeConfig derives a ResumeSessionConfig from the session config so a
// resumed SDK session keeps the same provider (BYOK), model, tools,
// permission-gating callback, streaming, config-discovery posture (KB-43) and
// system-message setup as the original session. Returns nil when there is no
// session config to derive from.
func buildResumeConfig(sc *copilot.SessionConfig) *copilot.ResumeSessionConfig {
	if sc == nil {
		return nil
	}
	return &copilot.ResumeSessionConfig{
		ClientName:            sc.ClientName,
		Model:                 sc.Model,
		Tools:                 sc.Tools,
		SystemMessage:         sc.SystemMessage,
		Provider:              sc.Provider,
		Streaming:             sc.Streaming,
		OnPermissionRequest:   sc.OnPermissionRequest,
		WorkingDirectory:      sc.WorkingDirectory,
		EnableConfigDiscovery: sc.EnableConfigDiscovery,
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
	// Parse and validate the CRI-277 watchdog config first: a misconfigured
	// window must fail the open before any side effect (no CLI child start,
	// no SDK session) so the error reads as session misconfiguration.
	cfg := req.GetConfig()
	watchdogCfg, err := parseWatchdogSettings(cfg)
	if err != nil {
		return nil, err
	}

	// Resolved secrets for this session, constrained to the names the adapter
	// declared in Info().Secrets. The GitHub token is sourced from here, never
	// from the process environment (D69).
	secrets := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), req.GetSecrets())
	client, err := p.ensureClient(ctx, secrets)
	if err != nil {
		return nil, err
	}

	adapterSessionID := req.GetSessionId()
	sessionConfig := p.buildSessionConfig(cfg, adapterSessionID)
	resumeConfig := buildResumeConfig(sessionConfig)
	session, _, err := p.openSDKSession(ctx, client, adapterSessionID, sessionConfig, resumeConfig)
	if err != nil {
		return nil, err
	}

	s := newSessionState(adapterSessionID, session, sessionConfig, resumeConfig, secrets, p)
	s.watchdog = watchdogCfg
	s.heldSecrets = heldGitHubTokenSecrets(secrets)
	// First SDK session for this adapter session (KB-47): epoch 1 whether the
	// open created it fresh or resumed a persisted one — both restore/continue
	// a conversation a later repair can address in-session.
	s.createdEpoch = 1

	if strings.TrimSpace(cfg[watchdogWindowCfgKey]) != "" || strings.TrimSpace(cfg[watchdogGateWindowCfgKey]) != "" {
		slog.Info("copilot: watchdog windows configured",
			"adapterSession", adapterSessionID,
			"window", watchdogCfg.window.String(),
			"gateWindow", watchdogCfg.gateWindow.String())
	}

	p.mu.Lock()
	p.sessions[adapterSessionID] = s
	p.mu.Unlock()

	if err := p.applyOpenSessionModel(ctx, s, cfg); err != nil {
		return nil, err
	}

	return &v2.OpenSessionResponse{}, nil
}

// sessionProbeTimeout bounds the submit_outcome tool-presentation probe at
// session open (KB-71): the RPC must never wedge the open when a CLI stops
// answering, so the probe runs under its own deadline.
var sessionProbeTimeout = 10 * time.Second

// toolMetadataSource is an optional session capability to report the tool
// names actually served to the model (KB-71). SDK sessions implement it via
// the CLI's session.tools.getCurrentMetadata RPC; test fakes may or may not.
type toolMetadataSource interface {
	CurrentToolMetadata(ctx context.Context) ([]string, error)
}

// verifySubmitOutcomePresented asserts the structural precondition of the
// finalize loop (KB-71): the session must actually PRESENT the
// submit_outcome tool to the model. The adapter always registers it in the
// CreateSession/ResumeSession config, but a CLI that drops custom tools on
// session.resume — the KB-64 session-reuse shape: the reviewer inherits a
// session whose served tool set differs from the review step's contract —
// leaves the reviewer unable to finalize, and the corrective reprompt then
// tells the model to call a tool it cannot see. A session verified missing
// the tool fails the open loudly, with the served tool list in the error.
//
// Inconclusive outcomes (session without the capability, RPC error, empty
// tool metadata for a not-yet-initialized tool set) only warn: they are CLI
// API gaps, not proof that the tool is absent, and failing open on them
// would break healthy opens across CLI versions.
func verifySubmitOutcomePresented(ctx context.Context, sess copilotSession) error {
	src, ok := sess.(toolMetadataSource)
	if !ok {
		slog.Warn("copilot: session cannot report served tools; submit_outcome presentation not verified",
			"session", sess.SessionID())
		return nil
	}

	probeCtx, cancel := context.WithTimeout(ctx, sessionProbeTimeout)
	defer cancel()
	names, err := src.CurrentToolMetadata(probeCtx)
	if err != nil {
		slog.Warn("copilot: served-tool metadata probe failed; submit_outcome presentation not verified",
			"session", sess.SessionID(), "err", err)
		return nil
	}
	if len(names) == 0 {
		slog.Warn("copilot: served-tool metadata empty (tools not initialized yet); submit_outcome presentation not verified",
			"session", sess.SessionID())
		return nil
	}
	for _, name := range names {
		if name == submitOutcomeToolName {
			return nil
		}
	}
	return fmt.Errorf("copilot: CLI session does not present the %q tool to the model (served tools: %s); finalize cannot converge, failing the session open",
		submitOutcomeToolName, strings.Join(names, ", "))
}

// openSDKSession resumes the persisted SDK session for adapterSessionID when
// one exists (CRI-272: a resumed session restores the full conversation
// history), falling back to CreateSession on first open, when no ID is
// persisted, or when resume fails. A newly created session's ID is persisted
// for later respawns. Returns the session and whether it was resumed.
// A resumed session whose served tool set is missing submit_outcome is not
// retried: the open falls through to a fresh create, which re-binds the tools
// (KB-64 session-reuse precedent). A fresh create that still fails the probe
// fails the open loudly.
func (p *copilotAdapter) openSDKSession(ctx context.Context, client copilotClient, adapterSessionID string, sessionConfig *copilot.SessionConfig, resumeConfig *copilot.ResumeSessionConfig) (copilotSession, bool, error) {
	if persistedID := loadPersistedSDKSessionID(adapterSessionID); persistedID != "" {
		slog.Info("copilot: resuming persisted sdk session",
			"adapterSession", adapterSessionID, "sdkSession", persistedID)
		sess, err := client.ResumeSessionWithOptions(ctx, persistedID, resumeConfig)
		if err == nil {
			if probeErr := verifySubmitOutcomePresented(ctx, sess); probeErr != nil {
				slog.Warn("copilot: resumed sdk session does not present submit_outcome; creating a fresh session",
					"adapterSession", adapterSessionID, "sdkSession", persistedID, "err", probeErr)
				_ = sess.Disconnect()
			} else {
				return sess, true, nil
			}
		} else {
			slog.Warn("copilot: sdk session resume failed; creating a fresh session",
				"adapterSession", adapterSessionID, "sdkSession", persistedID, "err", err)
		}
	}
	sess, err := client.CreateSession(ctx, sessionConfig)
	if err != nil {
		return nil, false, fmt.Errorf("copilot: create session: %w", err)
	}
	if probeErr := verifySubmitOutcomePresented(ctx, sess); probeErr != nil {
		// The session cannot converge a finalize loop; fail the open loudly
		// so the engine surfaces the misconfiguration instead of burning a
		// review leg on reprompts for a tool the model cannot see (KB-71).
		_ = sess.Disconnect()
		return nil, false, probeErr
	}
	persistSDKSessionID(adapterSessionID, sess.SessionID())
	return sess, false, nil
}

// buildSessionConfig constructs the SDK SessionConfig from agent-level config fields.
func (p *copilotAdapter) buildSessionConfig(cfg map[string]string, adapterSessionID string) *copilot.SessionConfig {
	// Register submit_outcome once per session as a hand-built Tool (KB-47):
	// a static structural parameter schema — outcome: string, reason: string
	// (both required), comment: string, payload: {type: object}, and
	// additionalProperties: false (KB-216) — with no per-step enum. Tools
	// bind only at
	// session create/resume (the SDK has no Session-level tool mutation), so
	// the enforceable per-step specifics (allowed outcome set, payload schema,
	// require_comment) stay prompt-conveyed and handler-validated. Validation
	// against the active step's allowed set and contracts happens in
	// handleSubmitOutcome at call time so per-step scoping works without
	// recreating the session. The handler decodes the already-decoded `any`
	// arguments envelope itself (decodeSubmitOutcomeArgs).
	submitTool := copilot.Tool{
		Name:           submitOutcomeToolName,
		Description:    submitOutcomeToolDescription,
		Parameters:     submitOutcomeToolParameters(),
		SkipPermission: true,
		Handler: func(invocation copilot.ToolInvocation) (copilot.ToolResult, error) {
			return p.handleSubmitOutcomeRaw(adapterSessionID, invocation)
		},
	}

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
	// KB-43: the CLI child must not inherit tool side-channels the adapter
	// never declared. Config discovery would load MCP servers (and skills)
	// from the working directory (e.g. .mcp.json, .vscode/mcp.json) and merge
	// them into the session — an undeclared side-channel that can carry
	// tracker-mutation tools together with their credentials. The adapter's
	// tool surface is exactly the two registered tools plus the CLI's built-in
	// tools; cross-adapter capabilities are reachable only through host-gated
	// adapter_tool calls. Custom instruction files (AGENTS.md,
	// .github/copilot-instructions.md) are always loaded regardless, so the
	// developer agent keeps its instructions.
	sc.EnableConfigDiscovery = copilot.Bool(false)
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
		if err := s.currentSession().SetModel(ctx, model, opts); err != nil {
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
	// Snapshot the SDK session once via the guarded accessor: a concurrent
	// reopenSession may swap it mid-close, but the CloseSession contract
	// closes the session as it was when the request arrived.
	sess := s.currentSession()

	disconnectDone := make(chan error, 1)
	go func() {
		disconnectDone <- sess.Disconnect()
	}()

	select {
	case err := <-disconnectDone:
		if err != nil {
			_ = sess.Destroy()
			return &v2.CloseSessionResponse{}, fmt.Errorf("copilot: disconnect session: %w", err)
		}
	case <-time.After(closeSessionGrace):
		_ = sess.Destroy()
	}

	return &v2.CloseSessionResponse{}, nil
}
