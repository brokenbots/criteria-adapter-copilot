// Package main implements the criteria-adapter-copilot out-of-process adapter.
//
// The adapter preserves the Criteria adapter boundary while using the Copilot SDK
// internally for a structured session protocol (instead of parsing free-form CLI
// stdout). The SDK manages CLI daemon startup/transport and exposes typed events.
//
// One SDK session is created per OpenSession and can be reused for multiple
// Execute calls (multi-turn). Permission requests are bridged to the host via
// adapter Permit RPC: Execute blocks until Permit resolves each request.
//
// max_turns semantics:
//   - max_turns is enforced adapter-side per Execute call by counting assistant
//     message events for that turn.
//   - if the cap is reached, the adapter emits Adapter("limit.reached", ...)
//     and returns outcome "failure" (or "needs_review" if that outcome is in
//     the step's allowed set).
//
// Outcome semantics:
//   - the adapter registers a `submit_outcome` tool at OpenSession.
//   - per Execute, the host's allowed outcomes are loaded onto sessionState
//     before the prompt is sent.
//   - the model MUST call submit_outcome exactly once with a valid outcome;
//     the adapter forwards that value via ExecuteResult.
//   - on missing / invalid finalize, the adapter reprompts up to 2 additional
//     times. After 3 failed attempts the adapter returns "failure" with a
//     structured diagnostic event.
//   - permission denial returns "failure".
//
// File layout:
//   - copilot.go         — constants, types (copilotAdapter), Info/ensureClient/getSession
//   - copilot_session.go — session lifecycle: copilotSession interface, sdkSession, sessionState, Open/CloseSession
//   - copilot_turn.go    — Execute, turnState, event handlers
//   - copilot_outcome.go — submit_outcome tool: SubmitOutcomeArgs, handleSubmitOutcome, helpers
//   - copilot_model.go   — model/effort helpers: applyRequestModel, applyRequestEffort, validateReasoningEffort
//   - copilot_permission.go — Permit, handlePermissionRequest, permissionDetails
//   - copilot_util.go    — resultEvent, logEvent, adapterEvent, stringifyAny
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	copilot "github.com/github/copilot-sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

const (
	adapterName    = "copilot"
	adapterVersion = "0.1.0"

	defaultBinEnv = "CRITERIA_COPILOT_BIN"
	defaultBin    = "copilot"

	includeSensitivePermissionDetailsEnv = "CRITERIA_COPILOT_INCLUDE_SENSITIVE_PERMISSION_DETAILS"

	submitOutcomeToolName = "submit_outcome"

	// submitOutcomeToolDescription is the description surfaced to the model for
	// the submit_outcome tool. It conveys the contract: call exactly once with
	// a valid outcome before ending the turn, or the step fails.
	submitOutcomeToolDescription = "Finalize the outcome for the current step. Call this exactly once with one of the allowed outcomes for the step. The list of allowed outcomes is provided in the user prompt. Failure to call this tool with a valid outcome will fail the step."
)

var errMaxTurnsReached = errors.New("copilot: max_turns reached")
var closeSessionGrace = 5 * time.Second

// validReasoningEfforts is the documented set of accepted reasoning effort values.
var validReasoningEfforts = map[string]bool{
	"low":    true,
	"medium": true,
	"high":   true,
	"xhigh":  true,
}

type copilotAdapter struct {
	adapterhost.UnimplementedPermissions
	mu       sync.Mutex
	sessions map[string]*sessionState

	clientMu sync.Mutex
	client   *copilot.Client

	// pendingPerms tracks in-flight permission requests from Copilot SDK
	// callbacks that are waiting for a host decision over the Permissions
	// bidi stream. Each entry is a buffered (size 1) channel; a decision
	// value of "allow" or "deny" is sent by the Permissions stream handler
	// when the host sends a PermissionEvent.request or PermissionEvent.cancel.
	pendingPermsMu sync.Mutex
	pendingPerms   map[string]chan<- string
}

func (p *copilotAdapter) Info(_ context.Context, _ *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{
		Name:      adapterName,
		Version:   adapterVersion,
		SourceUrl: "https://github.com/brokenbots/criteria-adapter-copilot",
		Platforms: []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64"},
		Capabilities: []string{
			"multi_turn",
			"structured_events",
			"permission_gating",
		},
		ConfigSchema: &v2.AdapterSchemaProto{Fields: map[string]*v2.ConfigFieldProto{
			"model":             {Type: "string", Description: "Copilot model to use for this session."},
			"reasoning_effort":  {Type: "string", Description: "Reasoning effort level for the model: low, medium, high, xhigh."},
			"working_directory": {Type: "string", Description: "Working directory for tool invocations."},
			"max_turns":         {Type: "number", Description: "Maximum assistant turns per Execute call (default: unlimited)."},
			"system_prompt":     {Type: "string", Description: "System prompt prepended at session open."},
			// Custom provider (BYOK) — point the session at an OpenAI-compatible
			// endpoint (Ollama, vLLM, Azure OpenAI, etc.). When provider_base_url
			// is set, the session uses this provider instead of GitHub Copilot's
			// default backend; in that case `model` is required.
			"provider_type":              {Type: "string", Description: "Custom provider type: openai, azure, or anthropic. Default: openai. Only used when provider_base_url is set."},
			"provider_base_url":          {Type: "string", Description: "Custom provider API endpoint URL. Setting this enables BYOK mode (e.g. http://localhost:11434/v1 for Ollama, vLLM endpoint). Requires `model` to be set."},
			"provider_api_key":           {Type: "string", Description: "Custom provider API key. Optional for local providers like Ollama. Prefer env() in HCL to keep secrets out of source."},
			"provider_bearer_token":      {Type: "string", Description: "Custom provider bearer token. Sets Authorization header directly; takes precedence over provider_api_key."},
			"provider_wire_api":          {Type: "string", Description: "Custom provider wire format (openai/azure only): completions or responses. Default: completions."},
			"provider_azure_api_version": {Type: "string", Description: "Azure API version, used when provider_type=azure. Default: 2024-10-21."},
		}},
		InputSchema: &v2.AdapterSchemaProto{Fields: map[string]*v2.ConfigFieldProto{
			"prompt":           {Required: true, Type: "string", Description: "User prompt to send to the assistant."},
			"max_turns":        {Type: "number", Description: "Per-step override for max assistant turns."},
			"reasoning_effort": {Type: "string", Description: "Per-step override for reasoning effort. Resets to the session default after this step. Valid: low, medium, high, xhigh."},
		}},
		// Declared so the host resolves these from the workflow's secret stack
		// and delivers them over the secret channel (D69). When supplied, an
		// adapter secret is authoritative; when none is delivered, ensureClient
		// falls back to Copilot's standard auth (env vars / credential caches).
		Secrets: declaredGitHubTokenSecrets(),
	}, nil
}

// registerPendingPerm registers a pending permission request. The caller must
// read from ch exactly once after calling handlePermissionRequest to get the
// host's decision ("allow" or "deny").
func (p *copilotAdapter) registerPendingPerm(id string, ch chan<- string) {
	p.pendingPermsMu.Lock()
	defer p.pendingPermsMu.Unlock()
	if p.pendingPerms == nil {
		p.pendingPerms = make(map[string]chan<- string)
	}
	p.pendingPerms[id] = ch
}

// resolvePendingPerm retrieves and removes the pending-perm channel for id.
// Returns nil if the id is not registered.
func (p *copilotAdapter) resolvePendingPerm(id string) chan<- string {
	p.pendingPermsMu.Lock()
	defer p.pendingPermsMu.Unlock()
	ch := p.pendingPerms[id]
	delete(p.pendingPerms, id)
	return ch
}

// drainPendingPerms signals all outstanding pending permission channels with
// "deny" and clears the map. Called when the Permissions stream ends.
func (p *copilotAdapter) drainPendingPerms() {
	p.pendingPermsMu.Lock()
	defer p.pendingPermsMu.Unlock()
	for id, ch := range p.pendingPerms {
		select {
		case ch <- "deny":
		default:
		}
		delete(p.pendingPerms, id)
	}
}

// Permissions implements the adapter-side Permissions bidi stream. The host
// sends PermissionEvent messages (request=allow, cancel=deny); the adapter
// resolves the corresponding pending channel so that handlePermissionRequest
// can unblock the Copilot SDK callback and return the correct result.
func (p *copilotAdapter) Permissions(ctx context.Context, stream adapterhost.PermissionsStream) error {
	defer p.drainPendingPerms()
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled || status.Code(err) == codes.OK {
				return nil
			}
			return err
		}
		p.dispatchPermEvent(ev, stream)
	}
}

// dispatchPermEvent routes a single PermissionEvent to the appropriate pending
// channel and, for allow events, acknowledges back to the host.
func (p *copilotAdapter) dispatchPermEvent(ev *v2.PermissionEvent, stream adapterhost.PermissionsStream) {
	if req := ev.GetRequest(); req != nil {
		id := req.GetRequestId()
		p.sendPermDecision(id, "allow")
		// Acknowledge the decision back to the host.
		_ = stream.Send(&v2.PermissionDecision{
			RequestId: id,
			Decision:  "allow",
		})
		return
	}
	if cancel := ev.GetCancel(); cancel != nil {
		p.sendPermDecision(cancel.GetRequestId(), "deny")
	}
}

// sendPermDecision delivers decision to the pending channel for id, if any.
func (p *copilotAdapter) sendPermDecision(id, decision string) {
	if ch := p.resolvePendingPerm(id); ch != nil {
		select {
		case ch <- decision:
		default:
		}
	}
}

func (p *copilotAdapter) ensureClient(ctx context.Context, secrets *adapterhost.Secrets) (*copilot.Client, error) {
	p.clientMu.Lock()
	defer p.clientMu.Unlock()
	if p.client != nil {
		return p.client, nil
	}

	cliPath := os.Getenv(defaultBinEnv)
	if strings.TrimSpace(cliPath) == "" {
		cliPath = defaultBin
	}

	options := &copilot.ClientOptions{
		Connection: copilot.StdioConnection{Path: cliPath},
		LogLevel:   "info",
	}
	applyAuthOptions(options, secrets)

	client := copilot.NewClient(options)
	if err := client.Start(ctx); err != nil {
		return nil, fmt.Errorf("copilot: start client: %w", err)
	}
	p.client = client
	return p.client, nil
}

// githubTokenSecretNames are the accepted secret names for the Copilot GitHub
// token, in precedence order. The adapter declares all three (see
// [declaredGitHubTokenSecrets]) so a workflow can supply whichever it uses;
// [resolveGitHubToken] returns the first one the host delivered.
var githubTokenSecretNames = []string{"COPILOT_GITHUB_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"}

// declaredGitHubTokenSecrets is the adapter's InfoResponse.Secrets declaration:
// the GitHub token names the host is allowed to resolve and deliver over the
// secret channel. Declaring all three preserves the historical env precedence
// while routing the values through the channel (D69) instead of process env.
func declaredGitHubTokenSecrets() map[string]string {
	return map[string]string{
		"COPILOT_GITHUB_TOKEN": "GitHub token for Copilot authentication (highest precedence).",
		"GH_TOKEN":             "GitHub token for Copilot authentication (used if COPILOT_GITHUB_TOKEN is unset).",
		"GITHUB_TOKEN":         "GitHub token for Copilot authentication (used if the others are unset).",
	}
}

// applyAuthOptions configures the client's authentication on opts according to
// the adapter's precedence:
//
//   - A token delivered over the secret channel (D69) is authoritative: it is
//     set explicitly and auto-login is disabled so only that token is honored.
//   - Otherwise the adapter falls back to Copilot's own standard auth
//     mechanisms — environment variables (GH_TOKEN / GITHUB_TOKEN /
//     COPILOT_SDK_AUTH_TOKEN) and local credential caches (gh CLI / stored OAuth
//     via the logged-in user). The process environment is passed through so the
//     runtime can read those vars; in a sandboxed adapter the env is scrubbed
//     (D29/D32), so nothing leaks and auto-login simply finds no credentials.
func applyAuthOptions(opts *copilot.ClientOptions, secrets *adapterhost.Secrets) {
	if token := resolveGitHubToken(secrets); token != "" {
		opts.GitHubToken = token
		opts.UseLoggedInUser = copilot.Bool(false)
		return
	}
	opts.UseLoggedInUser = copilot.Bool(true)
	opts.Env = os.Environ()
}

// resolveGitHubToken returns the GitHub token from the secret channel, trying
// the accepted names in precedence order. It never reads the process
// environment itself: an adapter-supplied secret is authoritative and takes
// precedence (D69). Returns "" if none were delivered, in which case
// [copilotAdapter.ensureClient] falls back to Copilot's own standard auth
// (environment variables and local credential caches).
func resolveGitHubToken(secrets *adapterhost.Secrets) string {
	for _, name := range githubTokenSecretNames {
		if token, ok := secrets.Get(name); ok {
			if t := strings.TrimSpace(token); t != "" {
				return t
			}
		}
	}
	return ""
}

// Log blocks until ctx is cancelled (when the host closes the Log stream after
// Execute returns). WS15 wires real log line forwarding; WS03 drops log lines.
func (p *copilotAdapter) Log(ctx context.Context, _ *v2.LogRequest, _ adapterhost.LogEventSender) error {
	<-ctx.Done()
	return nil
}

func (p *copilotAdapter) getSession(sessionID string) *sessionState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessions[sessionID]
}
