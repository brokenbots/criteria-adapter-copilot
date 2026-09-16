// copilot_toolcall.go — adapter_tool tool: the agent-invocable custom tool
// the adapter registers alongside submit_outcome (CRI-178). Calling it issues
// an adapter tool call over the adapter-tools wire (ADR-0004 / CRI-152) via
// the SDK's ToolCallBridge and maps the callee's typed result back into a
// ToolResult for the agent.
//
// A tool call that fails — denied, typed call_error, or callee failure — is
// data for the agent, not a run failure (ADR-0004 §5): it surfaces as a
// ToolResult with Error set and the workflow's own outcome routing is
// untouched. Only submit_outcome decides the step outcome.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	copilot "github.com/github/copilot-sdk/go"

	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// AdapterToolArgs is the typed parameter struct for the adapter_tool tool.
type AdapterToolArgs struct {
	// Target is the full `adapter.<type>.<name>.tools.<tool>` string naming
	// the callee tool (ADR-0004 §2). The bare surface form without a tool is
	// rejected client-side.
	Target string `json:"target"`
	// Args are the callee's input arguments.
	Args map[string]any `json:"args,omitempty"`
}

// handleAdapterToolCall is the tool handler for adapter_tool. It is
// goroutine-safe: the SDK dispatches tool handlers from its own goroutines.
func (p *copilotAdapter) handleAdapterToolCall(adapterSessionID string, invocation copilot.ToolInvocation, args AdapterToolArgs) (copilot.ToolResult, error) {
	tool, err := parseAdapterToolTarget(args.Target)
	if err != nil {
		return adapterToolError(err.Error()), nil
	}

	s := p.getSession(adapterSessionID)
	if s == nil {
		return adapterToolError("adapter_tool invoked on an unknown session"), nil
	}

	s.mu.Lock()
	sink := s.sink
	active := s.active
	s.mu.Unlock()
	if !active || sink == nil {
		return adapterToolError("adapter_tool called outside an active step; it can only be used mid-conversation"), nil
	}

	ctx := invocation.TraceContext
	if ctx == nil {
		ctx = context.Background()
	}

	// The bridge sends the permission.request payload (kind=adapter_tool) on
	// the Execute stream and blocks for the correlated PermissionEvent reply
	// on the Permissions stream. The call is itself the gated event end to
	// end (ADR-0004 §8): the host gates it per call — capability, target
	// shape, step-level tools grants, policy, graph — so no additional
	// adapter-side gate is applied here.
	outcome, outputs, err := p.toolBridge.CallAdapterTool(ctx, sink, adapterhost.AdapterToolCall{
		SessionID: adapterSessionID,
		Target:    args.Target,
		Tool:      tool,
		Args:      args.Args,
		Preview:   adapterToolArgsPreview(args.Args),
	})
	if err != nil {
		return adapterToolCallError(args.Target, err), nil
	}
	if outcome != "success" {
		// A non-success callee outcome is a failed call for the agent. The
		// callee's outcome is returned to the host on the adapter's own
		// ExecuteResult routing, never from here.
		return adapterToolError(fmt.Sprintf("adapter tool call to %s returned outcome %q", args.Target, outcome)), nil
	}
	return adapterToolSuccess(outputs), nil
}

// parseAdapterToolTarget validates the strict §2 target form
// `adapter.<type>.<name>.tools.<tool>` and returns the callee tool name,
// mirroring the host's parser. The bare surface form (no tool) is rejected:
// a call must name the callee tool.
func parseAdapterToolTarget(target string) (tool string, err error) {
	labels := strings.Split(strings.TrimSpace(target), ".")
	if len(labels) != 5 || labels[0] != "adapter" || labels[3] != "tools" {
		return "", fmt.Errorf("target %q must be the full `adapter.<type>.<name>.tools.<tool>` string naming the callee tool", target)
	}
	for _, label := range labels {
		if !isBarewordTargetLabel(label) {
			return "", fmt.Errorf("target %q is not a valid adapter tool target: labels must be bareword identifiers", target)
		}
	}
	return labels[4], nil
}

// isBarewordTargetLabel reports whether s is a bareword identifier as the
// host's target grammar defines it: a leading letter or underscore followed
// by letters, digits, underscores, or hyphens.
func isBarewordTargetLabel(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
			// always allowed
		case c >= '0' && c <= '9', c == '-':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// adapterToolArgsPreview renders the bounded printable preview carried as the
// payload's optional args_preview key, for human-readable audit trails.
func adapterToolArgsPreview(args map[string]any) string {
	encoded, err := json.Marshal(args)
	if err != nil {
		return ""
	}
	const maxPreview = 256
	if len(encoded) <= maxPreview {
		return string(encoded)
	}
	// Trim back to the last complete rune so the preview stays valid UTF-8.
	truncated := encoded[:maxPreview]
	for len(truncated) > 0 && !utf8.ValidString(string(truncated)) {
		truncated = truncated[:len(truncated)-1]
	}
	return string(truncated) + "…"
}

// adapterToolSuccess returns the ToolResult for a successful call: the
// callee's outputs as JSON for the agent.
func adapterToolSuccess(outputs map[string]any) copilot.ToolResult {
	if outputs == nil {
		outputs = map[string]any{}
	}
	encoded, err := json.Marshal(outputs)
	if err != nil {
		// Outputs decoded from JSON on the wire always re-encode; guard anyway.
		return adapterToolError("adapter tool call produced undecodable outputs")
	}
	return copilot.ToolResult{
		TextResultForLLM: string(encoded),
		ResultType:       "success",
	}
}

// adapterToolCallError maps a typed call failure (denied, call_error, or
// transport failure) to a ToolResult with Error set, so the agent can surface
// it instead of retrying blindly.
func adapterToolCallError(target string, err error) copilot.ToolResult {
	return adapterToolError(fmt.Sprintf("adapter tool call to %s failed: %v", target, err))
}

// adapterToolError returns the ToolResult for a failed adapter_tool call.
// Using a ToolResult (not a Go error) so the model keeps control of the turn
// and can surface the failure; returning a Go error ends the turn
// unrecoverably.
func adapterToolError(msg string) copilot.ToolResult {
	return copilot.ToolResult{
		TextResultForLLM: msg,
		ResultType:       "failure",
		Error:            msg,
	}
}
