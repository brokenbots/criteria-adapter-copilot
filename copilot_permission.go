// copilot_permission.go — Copilot permission-request bridging: the Copilot SDK
// OnPermissionRequest callback blocks on a pending channel until the host sends
// a PermissionEvent.request (allow) or PermissionEvent.cancel (deny) via the
// Permissions bidi stream. The Permissions method on copilotAdapter drives the
// host-side decisions.

package main

import (
	"encoding/json"
	"os"
	"strings"

	copilot "github.com/github/copilot-sdk/go"
	"github.com/github/copilot-sdk/go/rpc"
	"github.com/google/uuid"
)

// handlePermissionRequest is the SDK OnPermissionRequest callback. It:
//  1. Assembles the permission event payload (including a request_id).
//  2. Registers a pending channel with copilotAdapter.pendingPerms.
//  3. Forwards the permission.request event upstream via the Execute stream sink.
//  4. Blocks until the host sends a decision via the Permissions bidi stream
//     or the active Execute call ends.
//  5. Returns Approved or Rejected to the Copilot SDK based on the host decision.
func (p *copilotAdapter) handlePermissionRequest(sessionID string, request copilot.PermissionRequest) (rpc.PermissionDecision, error) {
	s := p.getSession(sessionID)
	if s == nil {
		return &rpc.PermissionDecisionUserNotAvailable{}, nil
	}

	s.mu.Lock()
	sink := s.sink
	active := s.active
	activeCh := s.activeCh
	s.mu.Unlock()

	if !active || sink == nil {
		return &rpc.PermissionDecisionUserNotAvailable{}, nil
	}

	payload, requestID := buildPermEventPayload(request)

	decisionCh := make(chan string, 1)
	p.registerPendingPerm(requestID, decisionCh)

	if err := sink.Send(adapterEvent("permission.request", payload)); err != nil {
		p.resolvePendingPerm(requestID)
		return &rpc.PermissionDecisionUserNotAvailable{}, nil //nolint:nilerr // fail closed at the SDK boundary; the host-visible result carries the denial reason
	}

	select {
	case decision := <-decisionCh:
		if decision == "allow" {
			return &rpc.PermissionDecisionApproveOnce{}, nil
		}
		return &rpc.PermissionDecisionReject{}, nil
	case <-activeCh:
		p.resolvePendingPerm(requestID)
		return &rpc.PermissionDecisionReject{}, nil
	}
}

// buildPermEventPayload converts the Copilot SDK request into the detailsAny
// map sent as the permission.request event payload, and derives a stable
// requestID and canonical tool name for the pending-perm registry.
//
// requestID is always a fresh UUID, never the raw ToolCallID from the SDK.
// ToolCallID values come from the Copilot model and can be reused across
// concurrent sessions, causing one session's allow/deny decision to unblock
// another session's request. The UUID is embedded in the event payload so the
// host echoes it back on the Permissions stream and the correct pending channel
// is resolved regardless of how many sessions are in flight.
func buildPermEventPayload(request copilot.PermissionRequest) (detailsAny map[string]any, requestID string) {
	raw := permissionDetails(request)
	detailsAny = make(map[string]any, len(raw)+2)
	for k, v := range raw {
		detailsAny[k] = v
	}
	requestID = uuid.NewString()
	detailsAny["request_id"] = requestID
	detailsAny["tool"] = permissionTool(request)
	return
}

func permissionDetails(request copilot.PermissionRequest) map[string]string { //nolint:funlen,gocognit,gocyclo // collecting optional fields from SDK request variants; splitting further would obscure the boundary mapping
	includeSensitive := includeSensitivePermissionDetails()

	details := map[string]string{
		"kind": string(request.Kind()),
	}
	switch req := request.(type) {
	case copilot.PermissionRequestCustomTool:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.ToolName != "" {
			details["tool_name"] = req.ToolName
		}
		if includeSensitive && req.Args != nil {
			details["args"] = stringifyAny(req.Args)
		}
	case copilot.PermissionRequestExtensionManagement:
		setString(details, "tool_call_id", req.ToolCallID)
		setString(details, "extension_name", req.ExtensionName)
		if req.Operation != "" {
			details["operation"] = req.Operation
		}
	case copilot.PermissionRequestExtensionPermissionAccess:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.ExtensionName != "" {
			details["extension_name"] = req.ExtensionName
		}
		if len(req.Capabilities) > 0 {
			details["capabilities"] = strings.Join(req.Capabilities, ",")
		}
	case copilot.PermissionRequestHook:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.ToolName != "" {
			details["tool_name"] = req.ToolName
		}
		setString(details, "hook_message", req.HookMessage)
		if includeSensitive && req.ToolArgs != nil {
			details["tool_args"] = stringifyAny(req.ToolArgs)
		}
	case copilot.PermissionRequestMCP:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.ServerName != "" {
			details["server_name"] = req.ServerName
		}
		if req.ToolName != "" {
			details["tool_name"] = req.ToolName
		}
		if includeSensitive && req.Args != nil {
			details["args"] = stringifyAny(req.Args)
		}
	case copilot.PermissionRequestMemory:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.Fact != "" {
			details["fact"] = req.Fact
		}
		setString(details, "subject", req.Subject)
		setString(details, "citations", req.Citations)
		setString(details, "reason", req.Reason)
	case copilot.PermissionRequestRead:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.Intention != "" {
			details["intention"] = req.Intention
		}
		if includeSensitive && req.Path != "" {
			details["path"] = req.Path
		}
	case copilot.PermissionRequestShell:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.Intention != "" {
			details["intention"] = req.Intention
		}
		setString(details, "warning", req.Warning)
		if includeSensitive && req.FullCommandText != "" {
			details["full_command_text"] = req.FullCommandText
		}
		if includeSensitive && len(req.PossiblePaths) > 0 {
			if len(req.PossiblePaths) == 1 {
				details["path"] = req.PossiblePaths[0]
			}
			details["possible_paths"] = strings.Join(req.PossiblePaths, ",")
		}
		if len(req.Commands) > 0 {
			cmds := make([]string, 0, len(req.Commands))
			for _, cmd := range req.Commands {
				if strings.TrimSpace(cmd.Identifier) != "" {
					cmds = append(cmds, cmd.Identifier)
				}
			}
			if len(cmds) > 0 {
				details["commands"] = strings.Join(cmds, ",")
			}
		}
	case copilot.PermissionRequestURL:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.Intention != "" {
			details["intention"] = req.Intention
		}
		if includeSensitive && req.URL != "" {
			details["url"] = req.URL
		}
	case copilot.PermissionRequestWrite:
		setString(details, "tool_call_id", req.ToolCallID)
		if req.Intention != "" {
			details["intention"] = req.Intention
		}
		if includeSensitive && req.FileName != "" {
			details["path"] = req.FileName
		}
	}

	if includeSensitive {
		if b, err := json.Marshal(request); err == nil {
			details["request_json"] = string(b)
		}
	}
	return details
}

func setString(details map[string]string, key string, value *string) {
	if value != nil && *value != "" {
		details[key] = *value
	}
}

// permissionTool returns the host policy tool name for a permission request.
// Tool-specific names win when the SDK exposes them; otherwise we fall back to
// the canonical permission kind such as "read", "write", or "shell".
func permissionTool(request copilot.PermissionRequest) string {
	switch req := request.(type) {
	case copilot.PermissionRequestCustomTool:
		if req.ToolName != "" {
			return req.ToolName
		}
	case copilot.PermissionRequestHook:
		if req.ToolName != "" {
			return req.ToolName
		}
	case copilot.PermissionRequestMCP:
		if req.ToolName != "" {
			return req.ToolName
		}
	}
	return string(request.Kind())
}

// includeSensitivePermissionDetails controls whether rich permission payload
// fields (full command, paths/URLs, args, raw request JSON) are forwarded.
// Default is redacted to reduce sensitive data retention risk.
func includeSensitivePermissionDetails() bool {
	return os.Getenv(includeSensitivePermissionDetailsEnv) == "1"
}
