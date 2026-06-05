// fake-copilot is a deterministic stub that speaks the Copilot SDK's
// JSON-RPC 2.0 stdio protocol (Content-Length framing) without requiring the
// real copilot CLI or network access. It is used by the copilot adapter
// conformance test so those tests run in the default (no env-var) lane.
//
// Supported methods:
//   - connect                                             => {ok:true, version, protocolVersion:3}
//   - ping                                                => {timestamp, protocolVersion}
//   - status.get                                         => {version, protocolVersion}
//   - session.create                                     => {sessionId}
//   - session.send                                       => {messageId}; async events follow
//   - session.destroy                                    => {}
//   - session.permissions.handlePendingPermissionRequest => {}
//   - session.tools.handlePendingToolCall                => {}
//   - (all other methods)                                => {} (empty success)
//
// session.send behaviour depends on FAKE_COPILOT_SCENARIO:
//
// "" / "success" (default):
//
//	Emits submit_outcome("success") tool call + session.idle
//
// "success-after-reprompt-1":
//
//	Turn 1: session.idle with no tool call
//	Turn 2: submit_outcome("success") + session.idle
//
// "success-after-reprompt-2":
//
//	Turns 1-2: session.idle with no tool call
//	Turn 3: submit_outcome("success") + session.idle
//
// "invalid-outcome":
//
//	Turn 1: submit_outcome("not-a-real-outcome") + session.idle
//	Turn 2: submit_outcome("success") + session.idle
//
// "duplicate-call":
//
//	Turn 1: submit_outcome("success") then submit_outcome("failure") in same turn + session.idle
//
// "missing":
//
//	All turns: session.idle with no tool call (exhausts 3 attempts => failure)
//
// Permission prompt (contains "fetch"): emits permission.requested (auto-approved),
// waits for handlePendingPermissionRequest, then emits session.idle with no tool call.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// rpcMsg is the combined wire shape for both incoming requests and outgoing
// responses / notifications.
type rpcMsg struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"` // absent on notifications
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

var (
	wrMu    sync.Mutex // serialises all writes to os.Stdout
	permsMu sync.Mutex // protects pendingPerms
	toolsMu sync.Mutex // protects pendingToolCalls

	// pendingPerms maps a permRequestID to a channel that is closed
	// when session.permissions.handlePendingPermissionRequest arrives.
	pendingPerms = map[string]chan struct{}{}

	// pendingToolCalls maps a requestId to a channel that is closed
	// when session.tools.handlePendingToolCall arrives.
	pendingToolCalls = map[string]chan struct{}{}

	// toolCallSessions maps a requestId to its sessionId so that
	// handlePendingToolCall can emit external_tool.completed to the right session.
	toolCallSessions = map[string]string{}

	evtSeq  int64 // monotonic counter for synthetic event IDs
	permSeq int64 // monotonic counter for permission request IDs
	toolSeq int64 // monotonic counter for tool request IDs

	// sendCounts tracks the per-session turn counter for scenario dispatch
	// (sessionID => *int64).
	sendCounts sync.Map
	stdout     io.Writer = os.Stdout
)

func main() {
	r := bufio.NewReader(os.Stdin)
	for {
		data, err := readFrame(r)
		if err != nil {
			return
		}
		var msg rpcMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.Method == "" {
			continue // ignore responses
		}
		handleRequest(&msg)
	}
}

func handleRequest(msg *rpcMsg) {
	switch msg.Method {
	case "connect":
		respond(msg.ID, map[string]any{
			"ok":              true,
			"version":         "fake-copilot-0.0.1",
			"protocolVersion": 3,
		})

	case "ping":
		v := 3
		respond(msg.ID, map[string]any{
			"message":         "",
			"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
			"protocolVersion": v,
		})

	case "status.get":
		respond(msg.ID, map[string]any{
			"version":         "fake-copilot-0.0.1",
			"protocolVersion": 3,
		})

	case "session.create":
		var p struct {
			SessionID string `json:"sessionId"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		respond(msg.ID, map[string]any{"sessionId": p.SessionID})

	case "session.send":
		handleSessionSend(msg)

	case "session.destroy":
		respond(msg.ID, map[string]any{})

	case "session.permissions.handlePendingPermissionRequest":
		var p struct {
			RequestID string `json:"requestId"`
		}
		_ = json.Unmarshal(msg.Params, &p)

		permsMu.Lock()
		ch := pendingPerms[p.RequestID]
		delete(pendingPerms, p.RequestID)
		permsMu.Unlock()

		if ch != nil {
			close(ch)
		}
		respond(msg.ID, map[string]any{"success": true})

	case "session.tools.handlePendingToolCall":
		var p struct {
			RequestID string `json:"requestId"`
		}
		_ = json.Unmarshal(msg.Params, &p)

		toolsMu.Lock()
		ch := pendingToolCalls[p.RequestID]
		sessionID := toolCallSessions[p.RequestID]
		toolsMu.Unlock()

		// Emit completion and signal waiters BEFORE removing the map entries.
		// If we deleted first, waitForToolCall could observe a nil channel and
		// return immediately — before external_tool.completed is sent — letting
		// the scenario goroutine send session.idle too early and causing the
		// adapter to miss the tool.result event.
		if sessionID != "" {
			sendEvent(sessionID, "external_tool.completed", map[string]any{"requestId": p.RequestID})
		}
		if ch != nil {
			close(ch)
		}

		toolsMu.Lock()
		delete(pendingToolCalls, p.RequestID)
		delete(toolCallSessions, p.RequestID)
		toolsMu.Unlock()
		respond(msg.ID, map[string]any{})

	default:
		// Forward-compatible: unknown calls return an empty success.
		if len(msg.ID) > 0 {
			respond(msg.ID, map[string]any{})
		}
	}
}

// handleSessionSend responds immediately with a messageId and then sends
// async session events in a goroutine based on the active scenario and turn.
func handleSessionSend(msg *rpcMsg) {
	var p struct {
		SessionID string `json:"sessionId"`
		Prompt    string `json:"prompt"`
	}
	_ = json.Unmarshal(msg.Params, &p)

	const msgID = "fake-msg-1"
	respond(msg.ID, map[string]any{"messageId": msgID})

	if isPermissionPrompt(p.Prompt) {
		// Permission scenario: emit a permission request (auto-approved), then
		// end the turn without a submit_outcome call. The adapter's awaitOutcome
		// will reprompt up to maxFinalizeAttempts before returning failure.
		permReqID := newPermID()
		ch := make(chan struct{})
		permsMu.Lock()
		pendingPerms[permReqID] = ch
		permsMu.Unlock()

		go func() {
			sendEvent(p.SessionID, "permission.requested", map[string]any{
				"requestId": permReqID,
				"permissionRequest": map[string]any{
					"kind": "web",
				},
			})
			<-ch // blocks until handlePendingPermissionRequest arrives
			sendEvent(p.SessionID, "session.idle", map[string]any{})
		}()
		return
	}

	// Increment and read the per-session turn counter (1-based).
	counter, _ := sendCounts.LoadOrStore(p.SessionID, new(int64))
	turn := int(atomic.AddInt64(counter.(*int64), 1))

	scenario := strings.TrimSpace(os.Getenv("FAKE_COPILOT_SCENARIO"))

	go func() {
		switch scenario {
		case "", "success":
			sendToolCallAndIdle(p.SessionID, "success", "step completed")

		case "success-after-reprompt-1":
			if turn == 1 {
				sendEvent(p.SessionID, "session.idle", map[string]any{})
			} else {
				sendToolCallAndIdle(p.SessionID, "success", "step completed")
			}

		case "success-after-reprompt-2":
			if turn <= 2 {
				sendEvent(p.SessionID, "session.idle", map[string]any{})
			} else {
				sendToolCallAndIdle(p.SessionID, "success", "step completed")
			}

		case "invalid-outcome":
			if turn == 1 {
				sendToolCallAndIdle(p.SessionID, "not-a-real-outcome", "")
			} else {
				sendToolCallAndIdle(p.SessionID, "success", "step completed")
			}

		case "duplicate-call":
			// First call: wait for handler to complete before sending the
			// second call. This ensures finalizedOutcome is set to "success"
			// deterministically before the second handler runs, making the
			// first-call-wins semantics observable at the boundary.
			reqID1 := sendToolCall(p.SessionID, "success", "first call")
			waitForToolCall(reqID1)
			reqID2 := sendToolCall(p.SessionID, "failure", "second call")
			waitForToolCall(reqID2)
			sendEvent(p.SessionID, "session.idle", map[string]any{})

		case "missing":
			sendEvent(p.SessionID, "session.idle", map[string]any{})

		default:
			// Unknown scenario: treat as success.
			sendToolCallAndIdle(p.SessionID, "success", "step completed")
		}
	}()
}

// sendToolCallAndIdle emits a submit_outcome tool call event, waits for the
// SDK to confirm the handler ran (via handlePendingToolCall), then emits
// session.idle to end the turn.
func sendToolCallAndIdle(sessionID, outcome, reason string) {
	reqID := sendToolCall(sessionID, outcome, reason)
	waitForToolCall(reqID)
	sendEvent(sessionID, "session.idle", map[string]any{})
}

// waitForToolCall blocks until handlePendingToolCall has been received for
// reqID (i.e., the SDK handler has run and external_tool.completed has been
// emitted). If the call has already completed, it returns immediately.
func waitForToolCall(reqID string) {
	toolsMu.Lock()
	ch := pendingToolCalls[reqID]
	toolsMu.Unlock()
	if ch != nil {
		<-ch
	}
}

// sendToolCall emits a single external_tool.requested event for submit_outcome
// and registers a pending channel for handlePendingToolCall. Returns the requestId.
func sendToolCall(sessionID, outcome, reason string) string {
	seq := atomic.AddInt64(&toolSeq, 1)
	reqID := fmt.Sprintf("fake-tool-req-%d", seq)
	toolCallID := fmt.Sprintf("fake-tc-%d", seq)

	ch := make(chan struct{})
	toolsMu.Lock()
	pendingToolCalls[reqID] = ch
	toolCallSessions[reqID] = sessionID
	toolsMu.Unlock()

	args := map[string]any{"outcome": outcome}
	if reason != "" {
		args["reason"] = reason
	}

	sendEvent(sessionID, "external_tool.requested", map[string]any{
		"requestId":  reqID,
		"sessionId":  sessionID,
		"toolCallId": toolCallID,
		"toolName":   "submit_outcome",
		"arguments":  args,
	})
	return reqID
}

// isPermissionPrompt reports whether the prompt should trigger the permission
// request flow rather than the normal streaming response.
func isPermissionPrompt(prompt string) bool {
	return strings.Contains(prompt, "fetch")
}

// newPermID returns a unique permission request ID.
func newPermID() string {
	return fmt.Sprintf("fake-perm-%d", atomic.AddInt64(&permSeq, 1))
}

// sendEvent sends a session.event notification to the SDK.
func sendEvent(sessionID, eventType string, data any) {
	seq := atomic.AddInt64(&evtSeq, 1)
	notify("session.event", map[string]any{
		"sessionId": sessionID,
		"event": map[string]any{
			"id":        fmt.Sprintf("evt-%d", seq),
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"type":      eventType,
			"data":      data,
		},
	})
}

// notify sends a JSON-RPC 2.0 notification (no id field).
func notify(method string, params any) {
	writeJSON(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	})
}

// respond sends a JSON-RPC 2.0 response.
func respond(id json.RawMessage, result any) {
	writeJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

// writeJSON serialises v and writes it as a Content-Length-framed message.
func writeJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	wrMu.Lock()
	defer wrMu.Unlock()
	_ = writeFrame(stdout, data)
}

// writeFrame writes a Content-Length-framed payload.
func writeFrame(w io.Writer, payload []byte) error {
	if _, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n", len(payload)); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readFrame reads a Content-Length-framed payload from r.
func readFrame(r *bufio.Reader) ([]byte, error) {
	length := -1
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF && line == "" {
				return nil, io.EOF
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(parts[0]), "Content-Length") {
			n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil {
				return nil, err
			}
			length = n
		}
	}
	if length < 0 {
		return nil, fmt.Errorf("missing content-length header")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
