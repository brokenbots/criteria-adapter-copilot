package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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
