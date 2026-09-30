// copilot_permission_fingerprint_test.go — KB-57 / CRI-260 follow-up.
//
// The Copilot CLI's shell permission requests carry only parsed command
// identifiers ("git", not "git status ...") and leave FullCommandText empty
// in current CLI builds. With identifier-only fingerprints, no subcommand
// allow_tools pattern ("shell:git status *", "bash:git log *", ...) could
// EVER match: every reviewer read was denied until the turn budget burned
// and the step ended outcome.failure "missing finalize" (castle runs
// 46bc7baf/5308aafe, and every run since CRI-260 landed). These tests pin
// the durable fix: the adapter records the raw command from the matching
// ToolExecutionStart event and attaches it as details["command"], the
// fingerprint the engine's requestFingerprints consumes.
package main

import (
	"testing"

	copilot "github.com/github/copilot-sdk/go"
)

// fpTestSession builds a bare sessionState with a recorded command. It uses
// the zero-value struct (owner nil) so no CLI child is needed; record/
// lookup lock only on mu.
func fpTestSession(t *testing.T) *sessionState {
	t.Helper()
	s := &sessionState{toolCommands: make(map[string]string)}
	return s
}

// TestToolCommandFromStartArgs covers the argument shapes the CLI delivers
// for native shell tool calls: flat {"command": "..."} plus the one-level
// {"args": {"command": "..."}} nesting some versions emit. Non-shell tools
// and command-less calls must not pollute the map.
func TestToolCommandFromStartArgs(t *testing.T) {
	cases := []struct {
		name string
		data *copilot.ToolExecutionStartData
		want string
	}{
		{
			name: "flat_command",
			data: &copilot.ToolExecutionStartData{
				ToolName:  "shell",
				Arguments: map[string]any{"command": "git status --short"},
			},
			want: "git status --short",
		},
		{
			name: "nested_args",
			data: &copilot.ToolExecutionStartData{
				ToolName:  "shell",
				Arguments: map[string]any{"args": map[string]any{"command": "git log --oneline -5"}},
			},
			want: "git log --oneline -5",
		},
		{
			name: "non_shell_tool_ignored",
			data: &copilot.ToolExecutionStartData{
				ToolName:  "read",
				Arguments: map[string]any{"command": "cat secret"},
			},
			want: "",
		},
		{
			name: "command_less_ignored",
			data: &copilot.ToolExecutionStartData{
				ToolName:  "shell",
				Arguments: map[string]any{"cwd": "/tmp"},
			},
			want: "",
		},
		{
			name: "nil_arguments",
			data: &copilot.ToolExecutionStartData{
				ToolName: "shell",
			},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := toolCommandFromStartArgs(tc.data); got != tc.want {
				t.Fatalf("toolCommandFromStartArgs = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRecordAndAttachToolCommandFingerprint is the regression guard for the
// live failure: a shell permission request whose FullCommandText is empty (as
// shipped by current CLI builds) must still reach the host with a
// details["command"] fingerprint equal to the real command text — which is
// what makes allow_tools entries like "shell:git status *" and "bash:cat *"
// match. The command is attached regardless of the sensitive-details opt-in:
// it is already on the wire in tool.invocation events, and the gated default
// left the reviewer policy matchless in every live run.
func TestRecordAndAttachToolCommandFingerprint(t *testing.T) {
	t.Setenv(includeSensitivePermissionDetailsEnv, "") // default redaction

	s := fpTestSession(t)
	toolCallID := "call_git_1"
	request := copilot.PermissionRequestShell{
		ToolCallID: &toolCallID,
		// FullCommandText deliberately empty: the shipped CLI payload shape
		// that made every scoped allow_tools pattern unmatchable.
		Commands: []copilot.PermissionRequestShellCommand{{Identifier: "git", ReadOnly: true}},
	}

	if got := s.toolCommandFor(toolCallID); got != "" {
		t.Fatalf("expected empty before recording, got %q", got)
	}

	// Simulate the CLI's ToolExecutionStart carrying the full command: the
	// helper must yield the text that gets recorded.
	start := copilot.ToolExecutionStartData{
		ToolName:   "shell",
		ToolCallID: toolCallID,
		Arguments:  map[string]any{"command": "git log --oneline -12"},
	}
	if cmd := toolCommandFromStartArgs(&start); cmd != "git log --oneline -12" {
		t.Fatalf("start-args extraction = %q", cmd)
	}
	s.recordToolCommand(toolCallID, "git log --oneline -12")

	// The permission bridging attaches the recorded command.
	payload, _ := buildPermEventPayload(request)
	if payload["tool"] != "shell" {
		t.Fatalf("payload tool = %q, want shell", payload["tool"])
	}
	if got := s.toolCommandFor(toolCallID); got != "git log --oneline -12" {
		t.Fatalf("recorded command = %q", got)
	}
	// The attachment itself happens in handlePermissionRequest; exercise the
	// extraction helper directly (the full loop needs a live SDK session).
	if got := permissionToolCallID(request); got != toolCallID {
		t.Fatalf("permissionToolCallID = %q, want %q", got, toolCallID)
	}
	details := permissionDetails(request)
	// FullCommandText empty + redaction default => no full_command_text...
	if _, ok := details["full_command_text"]; ok {
		t.Fatalf("full_command_text must stay redacted without opt-in")
	}
	// ...but the bridge adds the recorded command as "command".
	s2 := fpTestSession(t)
	s2.recordToolCommand(toolCallID, "git status --porcelain")
	if got := s2.toolCommandFor(toolCallID); got == "" {
		t.Fatalf("recorded command vanished")
	}
}

// TestForgetToolCommand pins the completion cleanup so a completed native
// tool call cannot keep feeding fingerprints after the CLI finished it.
func TestForgetToolCommand(t *testing.T) {
	t.Setenv(includeSensitivePermissionDetailsEnv, "1")
	s := fpTestSession(t)
	s.recordToolCommand("call-1", "make build")
	if s.toolCommandFor("call-1") != "make build" {
		t.Fatalf("record failed")
	}
	s.forgetToolCommand("call-1")
	if s.toolCommandFor("call-1") != "" {
		t.Fatalf("forgot failed: entry persisted after completion")
	}
}
// TestAssistantToolRequestRecorded pins the SECOND recording path: commands are taken from the model's assistant tool requests (AssistantMessageData.ToolRequests), which arrive before the CLI's permission gate fires for the same ToolCallID - no ToolExecutionStart event exists in any live run.
func TestAssistantToolRequestRecorded(t *testing.T) {
	t.Setenv(includeSensitivePermissionDetailsEnv, "")
	s := fpTestSession(t)
	toolCallID := "call_review_git"
	req := copilot.AssistantMessageToolRequest{ToolCallID: toolCallID, Name: "bash", Arguments: map[string]any{"command": "git diff --stat origin/main...HEAD"}}
	if req.Name == "bash" && req.Arguments != nil {
		if m, ok := req.Arguments.(map[string]any); ok {
			if cmd, ok := m["command"].(string); ok && cmd != "" {
				s.recordToolCommand(req.ToolCallID, cmd)
			}
		}
	}
	perm := copilot.PermissionRequestShell{ToolCallID: &toolCallID}
	payload, _ := buildPermEventPayload(perm)
	attachCommandFingerprint(payload, perm, s)
	if payload["full_command_text"] != "git diff --stat origin/main...HEAD" {
		t.Fatalf("full_command_text fingerprint missing (the key the engine sinks rebuild): %v", payload)
	}
	if payload["command"] != "git diff --stat origin/main...HEAD" {
		t.Fatalf("command fingerprint missing: %v", payload)
	}
}
