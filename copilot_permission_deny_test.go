// copilot_permission_deny_test.go — denial-path tests for handlePermissionRequest.
// WS03: permissions are auto-approved; this file covers the paths that return
// UserNotAvailable (no session, inactive session, or failed observability send)
// and verifies that a working active session returns Approved.

package main

import (
	"errors"
	"testing"
	"time"

	copilot "github.com/github/copilot-sdk/go"
	"github.com/github/copilot-sdk/go/rpc"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// failSender is an ExecuteEventSender that always returns the configured error.
type failSender struct {
	err error
}

func (f *failSender) Send(_ *v2.ExecuteEvent) error {
	return f.err
}

// TestHandlePermissionRequestNoSession asserts that an unknown session ID
// returns UserNotAvailable with no error and sends no event.
func TestHandlePermissionRequestNoSession(t *testing.T) {
	p := &copilotAdapter{sessions: map[string]*sessionState{}}
	req := copilot.PermissionRequestShell{}

	result, err := p.handlePermissionRequest("nonexistent", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Kind() != rpc.PermissionDecisionKindUserNotAvailable {
		t.Fatalf("result.Kind = %q, want %q", result.Kind(), rpc.PermissionDecisionKindUserNotAvailable)
	}
}

// TestHandlePermissionRequestInactiveSession asserts that an inactive session
// (active=false) returns UserNotAvailable with no error and sends no events,
// even when a recording sink is wired up (distinguishing the active=false branch
// from the sink=nil branch).
func TestHandlePermissionRequestInactiveSession(t *testing.T) {
	sink := &recordingSender{}
	s := &sessionState{
		session: &fakeSession{},
		active:  false,
		sink:    sink,
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}
	req := copilot.PermissionRequestShell{}

	result, err := p.handlePermissionRequest("s1", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Kind() != rpc.PermissionDecisionKindUserNotAvailable {
		t.Fatalf("result.Kind = %q, want %q", result.Kind(), rpc.PermissionDecisionKindUserNotAvailable)
	}
	if got := sink.snapshot(); len(got) != 0 {
		t.Fatalf("expected no events sent on sink, got %d event(s)", len(got))
	}
}

// TestHandlePermissionRequestSendError verifies that a sink.Send failure causes
// the adapter to return UserNotAvailable instead of Approved — fail closed so
// that a tool action never proceeds when the only in-scope observability event
// cannot be recorded.
func TestHandlePermissionRequestSendError(t *testing.T) {
	sendErr := errors.New("connection closed")
	s := &sessionState{
		session: &fakeSession{},
		active:  true,
		sink:    &failSender{err: sendErr},
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}
	req := copilot.PermissionRequestShell{}

	result, err := p.handlePermissionRequest("s1", req)
	if err != nil {
		t.Fatalf("unexpected error (non-nil means SDK-level failure): %v", err)
	}
	if result.Kind() != rpc.PermissionDecisionKindUserNotAvailable {
		t.Fatalf("result.Kind = %q, want %q (fail-closed when observability send fails)", result.Kind(), rpc.PermissionDecisionKindUserNotAvailable)
	}
}

// TestHandlePermissionRequestAutoApproveActive verifies that an active session
// with a working sink returns Approved and emits a permission.request event
// when the host approves the request.
func TestHandlePermissionRequestAutoApproveActive(t *testing.T) {
	sender := &recordingSender{}
	s := &sessionState{
		session:  &fakeSession{},
		active:   true,
		activeCh: make(chan struct{}),
		sink:     sender,
	}
	p := &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}
	toolCallID := "tc-active"
	req := copilot.PermissionRequestShell{ToolCallID: &toolCallID}

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

	result, err := p.handlePermissionRequest("s1", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Kind() != rpc.PermissionDecisionKindApproveOnce {
		t.Fatalf("result.Kind = %q, want %q", result.Kind(), rpc.PermissionDecisionKindApproveOnce)
	}

	var found bool
	for _, ev := range sender.snapshot() {
		if a := ev.GetAdapter(); a != nil && a.GetEventKind() == "permission.request" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected permission.request adapter event on sink")
	}
}
