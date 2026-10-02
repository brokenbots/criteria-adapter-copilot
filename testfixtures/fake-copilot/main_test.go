package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestReadWriteFrameRoundTrip verifies that writeFrame produces a valid
// Content-Length-framed payload that readFrame can decode.
func TestReadWriteFrameRoundTrip(t *testing.T) {
	payloads := [][]byte{
		[]byte(`{}`),
		[]byte(`{"jsonrpc":"2.0","method":"ping","params":{}}`),
		make([]byte, 4096), // large payload
	}
	for i, payload := range payloads {
		t.Run(fmt.Sprintf("payload_%d", i), func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeFrame(&buf, payload); err != nil {
				t.Fatalf("writeFrame: %v", err)
			}
			got, err := readFrame(bufio.NewReader(&buf))
			if err != nil {
				t.Fatalf("readFrame: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("round-trip mismatch: got %d bytes, want %d bytes", len(got), len(payload))
			}
		})
	}
}

// TestReadFrameEOF verifies that readFrame returns io.EOF on empty input.
func TestReadFrameEOF(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader(nil))
	_, err := readFrame(r)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

// TestReadFrameMissingContentLength verifies that readFrame returns an error
// when the Content-Length header is absent.
func TestReadFrameMissingContentLength(t *testing.T) {
	input := "X-Unknown: foo\r\n\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	_, err := readFrame(r)
	if err == nil {
		t.Fatal("expected error for missing Content-Length, got nil")
	}
}

// TestIsPermissionPrompt verifies the dispatch heuristic that routes
// session.send to the permission flow versus the normal streaming flow.
func TestIsPermissionPrompt(t *testing.T) {
	cases := []struct {
		prompt   string
		wantPerm bool
	}{
		{"Reply with only: RESULT: success", false},
		{"Use the fetch tool to retrieve something", true},
		{"fetch data from the server", true},
		{"FETCH (uppercase — case-sensitive match only)", false},
		{"", false},
		{"no special keyword here", false},
	}
	for _, tc := range cases {
		if got := isPermissionPrompt(tc.prompt); got != tc.wantPerm {
			t.Errorf("isPermissionPrompt(%q) = %v, want %v", tc.prompt, got, tc.wantPerm)
		}
	}
}

func TestPingResponseUsesRFC3339Timestamp(t *testing.T) {
	var buf bytes.Buffer
	stdout = &buf
	t.Cleanup(func() { stdout = os.Stdout })

	handleRequest(&rpcMsg{
		ID:     json.RawMessage(`1`),
		Method: "ping",
	})

	frame, err := readFrame(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}

	var resp struct {
		Result struct {
			Timestamp string `json:"timestamp"`
		} `json:"result"`
	}
	if err := json.Unmarshal(frame, &resp); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if resp.Result.Timestamp == "" {
		t.Fatal("ping response timestamp is empty")
	}
	if _, err := time.Parse(time.RFC3339Nano, resp.Result.Timestamp); err != nil {
		t.Fatalf("ping response timestamp %q is not RFC3339Nano: %v", resp.Result.Timestamp, err)
	}
}

// ── KB-71: served tool set + metadata probe wire surface ─────────────────────

// resetServedToolSet restores the global served-tool state after a test.
func resetServedToolSet(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		servedToolSetMu.Lock()
		servedToolSet = nil
		servedToolSetMu.Unlock()
	})
}

// TestCurrentToolMetadataNeverCapturedIsNil verifies the "uninitialized"
// shape: with no session.create/session.resume seen, the metadata responder
// returns nil so the wire response is {"tools": null}.
func TestCurrentToolMetadataNeverCapturedIsNil(t *testing.T) {
	resetServedToolSet(t)
	t.Setenv("FAKE_COPILOT_OMIT_TOOLS", "")
	if got := currentToolMetadata(); got != nil {
		t.Fatalf("currentToolMetadata() = %#v, want nil before any tool-set capture", got)
	}
	// Wire-level: the responder's {"tools": null} JSON keeps "tools" present
	// with a null value — what the SDK's ToolsGetCurrentMetadataResult
	// decodes into a nil slice.
	var buf bytes.Buffer
	stdout = &buf
	t.Cleanup(func() { stdout = os.Stdout })
	handleRequest(&rpcMsg{
		ID:     json.RawMessage(`7`),
		Method: "session.tools.getCurrentMetadata",
	})
	frame, err := readFrame(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if !strings.Contains(string(frame), `"tools":null`) {
		t.Fatalf("metadata response = %s, want \"tools\": null", frame)
	}
}

func TestCurrentToolMetadataMergesBuiltins(t *testing.T) {
	resetServedToolSet(t)
	t.Setenv("FAKE_COPILOT_OMIT_TOOLS", "")
	recordServedTools([]fakeToolMeta{
		{Name: "submit_outcome", Description: "Submit the verdict"},
		{Name: "adapter_tool", Description: "Call another adapter"},
	})
	got := currentToolMetadata()
	names := map[string]bool{}
	for _, tm := range got {
		names[tm.Name] = true
	}
	for _, want := range []string{"submit_outcome", "adapter_tool", "bash", "edit", "read"} {
		if !names[want] {
			t.Errorf("served tools %v missing %q", got, want)
		}
	}
}

// TestCurrentToolMetadataOmitDropsServedOnly verifies FAKE_COPILOT_OMIT_TOOLS
// removes names from the SERVED list at response time while the captured set
// stays complete — so omitting submit_outcome leaves a NON-empty list (the
// verified-missing shape the adapter must fail loudly on).
func TestCurrentToolMetadataOmitDropsServedOnly(t *testing.T) {
	resetServedToolSet(t)
	t.Setenv("FAKE_COPILOT_OMIT_TOOLS", "submit_outcome")
	recordServedTools([]fakeToolMeta{
		{Name: "submit_outcome", Description: "Submit the verdict"},
		{Name: "adapter_tool", Description: "Call another adapter"},
	})
	got := currentToolMetadata()
	if len(got) == 0 {
		t.Fatal("served list is empty; the omit filter must drop only the named tool, leaving the rest + built-ins")
	}
	for _, tm := range got {
		if tm.Name == "submit_outcome" {
			t.Errorf("served list still contains omit-marked tool submit_outcome: %v", got)
		}
	}
	// All omitted: an explicitly emptied list stays [] (not null) — a verified
	// empty serve is distinct from "never captured".
	t.Setenv("FAKE_COPILOT_OMIT_TOOLS", "submit_outcome,adapter_tool,bash,edit,read")
	got = currentToolMetadata()
	if got == nil {
		t.Fatal("all-omitted served list must be [] (verified empty), not nil (never captured)")
	}
	if len(got) != 0 {
		t.Fatalf("all-omitted served list = %v, want empty", got)
	}
}

// TestSessionCreateCapturesServedToolSet verifies session.create records the
// tools[] param as the served set, and session.resume re-captures it.
func TestSessionCreateCapturesServedToolSet(t *testing.T) {
	resetServedToolSet(t)
	t.Setenv("FAKE_COPILOT_OMIT_TOOLS", "")
	var buf bytes.Buffer
	stdout = &buf
	t.Cleanup(func() { stdout = os.Stdout })
	handleRequest(&rpcMsg{
		ID:     json.RawMessage(`11`),
		Method: "session.create",
		Params: json.RawMessage(`{"sessionId":"sdk-s1","tools":[{"name":"submit_outcome","description":"Submit"},{"name":"adapter_tool","description":"Call"}]}`),
	})
	got := currentToolMetadata()
	names := []string{}
	for _, tm := range got {
		names = append(names, tm.Name)
	}
	// Merged view: 2 captured custom tools + 3 built-ins.
	if len(got) != 5 || !slices.Equal(names, []string{"submit_outcome", "adapter_tool", "bash", "edit", "read"}) {
		t.Fatalf("served set after session.create = %v, want the 2 captured customs merged with 3 built-ins", got)
	}
	handleRequest(&rpcMsg{
		ID:     json.RawMessage(`12`),
		Method: "session.resume",
		Params: json.RawMessage(`{"sessionId":"sdk-s1","tools":[{"name":"submit_outcome","description":"Submit"}]}`),
	})
	got = currentToolMetadata()
	if len(got) != 4 {
		t.Fatalf("after resume, served set = %v, want 1 custom entry (last capture wins) merged with 3 built-ins", got)
	}
}

// TestSubmitOutcomeOverride verifies FAKE_COPILOT_OUTCOME overrides the
// scenario's default submitted outcome, and the default wins when unset.
func TestSubmitOutcomeOverride(t *testing.T) {
	t.Setenv("FAKE_COPILOT_OUTCOME", "")
	if got := submitOutcomeOverride("approved"); got != "approved" {
		t.Fatalf("default = %q, want approved", got)
	}
	t.Setenv("FAKE_COPILOT_OUTCOME", "changes_requested")
	if got := submitOutcomeOverride("approved"); got != "changes_requested" {
		t.Fatalf("override = %q, want changes_requested", got)
	}
}

// TestNewPermIDUniqueness verifies that newPermID returns distinct values on
// successive calls, preventing the map-key collision that would cause a
// deadlock when multiple permission requests are in-flight.
func TestNewPermIDUniqueness(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := newPermID()
		if seen[id] {
			t.Fatalf("duplicate permID %q on iteration %d", id, i)
		}
		seen[id] = true
	}
}

// TestPermissionHandshakeSequencing verifies the core ordering invariant:
// the goroutine that emits session.idle MUST NOT proceed until
// handlePendingPermissionRequest closes the channel.
func TestPermissionHandshakeSequencing(t *testing.T) {
	const reqID = "test-perm-seq"
	ch := make(chan struct{})

	permsMu.Lock()
	pendingPerms[reqID] = ch
	permsMu.Unlock()

	t.Cleanup(func() {
		permsMu.Lock()
		delete(pendingPerms, reqID)
		permsMu.Unlock()
	})

	// Goroutine simulates the async work after session.send: blocks on ch,
	// then signals completion (representing session.idle emission).
	unblocked := make(chan struct{})
	go func() {
		<-ch
		close(unblocked)
	}()

	// Goroutine must NOT unblock before handlePendingPermissionRequest fires.
	select {
	case <-unblocked:
		t.Fatal("goroutine unblocked before handlePendingPermissionRequest was called")
	case <-time.After(20 * time.Millisecond):
		// correct: still blocked
	}

	// Simulate handlePendingPermissionRequest arriving and resolving the channel.
	permsMu.Lock()
	c := pendingPerms[reqID]
	delete(pendingPerms, reqID)
	permsMu.Unlock()
	close(c)

	select {
	case <-unblocked:
		// correct: unblocked after resolution
	case <-time.After(time.Second):
		t.Fatal("goroutine did not unblock after permission resolved")
	}
}
