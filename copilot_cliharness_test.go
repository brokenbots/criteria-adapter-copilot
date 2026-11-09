package main

// Real-copilot-CLI session-table contract harness (KB-396).
//
// Why this exists: fake-copilot conformance cannot observe the CLI's session
// table. The fake sessions' Disconnect is a local detach, while the real SDK
// (copilot-sdk/go v1.0.0, session.go:1481) sends a session.destroy RPC for the
// session id. KB-391's per-step surface re-bind (v0.5.20) exploited exactly
// that blind spot: it resumed the persisted same-id session and then
// disconnected the superseded wrapper, destroying the conversation at the CLI
// and failing the next send with JSON-RPC -32603 "Session not found for
// sessionId" — which regressed the KB-277 contracts suite from 107 ok/0 FAIL
// to 37 ok/70 FAIL while fake-copilot conformance stayed green.
//
// The two phases here pin the CLI contract the KB-396 fix relies on, no
// GitHub credentials required (the session table is served unauthenticated;
// auth only affects the model call inside send):
//
//   - phase A (the post-fix adapter flow): create → send → resume same id →
//     send succeeds at the session layer; the conversation survives the
//     re-open. A "Session not found" error here is a regression.
//   - phase B (the contract that justifies the KB-396 guard): create →
//     resume same id → session.destroy → send fails with "Session not found
//     for sessionId". This documents the real CLI destroy-after-resume
//     semantics the fake cannot emulate.
//
// The harness runs by default whenever the copilot CLI binary is resolvable
// (CI pins @github/copilot 1.0.82 via CRITERIA_COPILOT_BIN / PATH, matching
// cliPath()); it is disabled with CRITERIA_COPILOT_CLI_CONTRACT=0 and skipped
// with a pointed message in environments without the binary.

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const realCLIContractDisableEnv = "CRITERIA_COPILOT_CLI_CONTRACT"

// rpcError is the JSON-RPC error object a copilot CLI response carries.
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcFrame struct {
	ID     *int            `json:"id"`
	Method string          `json:"method"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

// cliSessionTable drives the session-table subset of the copilot CLI's stdio
// JSON-RPC surface (LSP Content-Length framing), one request in flight at a
// time — enough to pin the session lifecycle contract the fix depends on.
type cliSessionTable struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	outFile    *os.File // StdoutPipe's underlying pipe; supports read deadlines
	out        *bufio.Reader
	stderrPath string
	writeMu    sync.Mutex
	readMu     sync.Mutex
	nextID     int
}

// readStderr opportunistically returns the CLI's stderr log; used in error
// messages to surface auth/session-table diagnostics without racing live reads.
func readStderr(path string) string {
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return ""
	}
	return string(data)
}

// newSessionID returns a random UUIDv4-shaped session id, the form the CLI's
// session table keys and validates on.
func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// startContractCLI spawns the real CLI child with a credential-free,
// sandboxed environment: HOME points at a temp dir (no ambient CLI state) and
// every variable that could carry an auth token is stripped.
func startContractCLI(t *testing.T) *cliSessionTable {
	t.Helper()
	bin := cliPath()
	resolved, lookErr := exec.LookPath(bin)
	if lookErr != nil {
		t.Skipf("real copilot CLI not resolvable via cliPath()=%q: %v; fake-copilot conformance cannot observe the CLI session table, so this contract only runs where the CLI is installed (see file header)", bin, lookErr)
	}

	sandbox := t.TempDir()
	cmd := exec.Command(resolved, "--headless", "--no-auto-update", "--stdio")
	// Minimal environment: no ambient auth tokens (the harness must not
	// consume or leak parent credentials), sandboxed HOME, and writable
	// config/cache dirs inside the sandbox.
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + sandbox,
		"TMPDIR=" + sandbox,
		"USER=" + os.Getenv("USER"),
		"XDG_CONFIG_HOME=" + sandbox + "/.config",
		"XDG_CACHE_HOME=" + sandbox + "/.cache",
		"XDG_DATA_HOME=" + sandbox + "/.local/share",
		"LANG=" + os.Getenv("LANG"),
		"TZ=UTC",
	}
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	outFile, ok := stdout.(*os.File)
	if !ok {
		t.Fatalf("stdout pipe is %T, want *os.File for read deadlines", stdout)
	}
	// stderr goes to a file, not a bytes.Buffer: the CLI leaves an inherited
	// stderr fd open in a detached helper, so a drain goroutine (what
	// cmd.Stderr = &bytes.Buffer{} requires) never sees EOF and cmd.Wait hangs
	// the whole test binary. The file is read opportunistically on failure.
	stderrPath := sandbox + "/harness-stderr.log"
	var stderrFile *os.File
	stderrFile, err = os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("stderr file: %v", err)
	}
	cmd.Stderr = stderrFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %q: %v", resolved, err)
	}
	c := &cliSessionTable{cmd: cmd, stdin: stdin, out: bufio.NewReader(stdout), outFile: outFile, stderrPath: stderrPath}
	t.Cleanup(func() {
		if c.cmd.ProcessState == nil {
			_ = c.cmd.Process.Kill()
			_ = c.cmd.Wait()
		}
		_ = stderrFile.Close()
		if t.Failed() {
			if data, readErr := os.ReadFile(stderrPath); readErr == nil && len(data) > 0 {
				t.Logf("CLI stderr (%s): %.2000s", stderrPath, data)
			}
		}
	})
	return c
}

func writeLSPFrame(w io.Writer, body []byte) error {
	_, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n%s", len(body), body)
	return err
}

// call issues one JSON-RPC request and returns the matching response frame,
// skipping event notifications interleaved on the wire.
func (c *cliSessionTable) call(ctx context.Context, method string, params any) (rpcFrame, error) {
	c.writeMu.Lock()
	c.nextID++
	id := c.nextID
	req, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		c.writeMu.Unlock()
		return rpcFrame{}, fmt.Errorf("marshal %s: %w", method, err)
	}
	if err := writeLSPFrame(c.stdin, req); err != nil {
		c.writeMu.Unlock()
		return rpcFrame{}, fmt.Errorf("write %s: %w", method, err)
	}
	c.writeMu.Unlock()

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(30 * time.Second)
	}
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return rpcFrame{}, fmt.Errorf("%s: no response before deadline (CLI stderr: %.2000s)", method, readStderr(c.stderrPath))
		}
		// Bound the blocking stdin/stdout reads by arming the pipe read
		// deadline; without it a CLI that answers nothing would wedge the
		// harness past the test timeout.
		_ = c.outFile.SetReadDeadline(deadline)
		c.readMu.Lock()
		frame, err := c.readFrame()
		c.readMu.Unlock()
		_ = c.outFile.SetReadDeadline(time.Time{})
		if err != nil {
			return rpcFrame{}, fmt.Errorf("%s: read frame: %w (CLI stderr: %.2000s)", method, err, readStderr(c.stderrPath))
		}
		if frame.Method != "" {
			continue // event notification, not an RPC response
		}
		if frame.ID == nil || *frame.ID != id {
			return rpcFrame{}, fmt.Errorf("%s: out-of-order response frame %+v", method, frame)
		}
		return frame, nil
	}
}

// readFrame reads exactly one Content-Length-framed message. Callers
// serialize with readMu; exact-length reads keep notifications from
// desynchronizing the stream.
func (c *cliSessionTable) readFrame() (rpcFrame, error) {
	var frame rpcFrame
	contentLength := 0
	for {
		line, err := c.out.ReadString('\n')
		if err != nil {
			return frame, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return frame, fmt.Errorf("malformed header %q", line)
		}
		if strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			contentLength, err = strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return frame, fmt.Errorf("malformed Content-Length %q: %w", value, err)
			}
		}
	}
	if contentLength <= 0 {
		return frame, errors.New("missing Content-Length header")
	}
	body := make([]byte, contentLength)
	if _, err := io.ReadFull(c.out, body); err != nil {
		return frame, err
	}
	if err := json.Unmarshal(body, &frame); err != nil {
		return frame, fmt.Errorf("decode frame %q: %w", body, err)
	}
	return frame, nil
}

func (c *cliSessionTable) connect(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := c.call(ctx, "connect", map[string]any{"protocolVersion": 3})
	if err != nil {
		t.Fatalf("connect handshake: %v", err)
	}
	if frame.Error != nil {
		t.Fatalf("connect handshake: JSON-RPC error %d %q", frame.Error.Code, frame.Error.Message)
	}
	var result struct {
		Ok              bool   `json:"ok"`
		ProtocolVersion int64  `json:"protocolVersion"`
		Version         string `json:"version"`
	}
	if err := json.Unmarshal(frame.Result, &result); err != nil {
		t.Fatalf("decode connect result %s: %v", frame.Result, err)
	}
	if !result.Ok {
		t.Fatalf("connect handshake: ok=false (%+v)", result)
	}
	t.Logf("real CLI connected: protocol %d, version %s", result.ProtocolVersion, result.Version)
}

// create opens a session with a minimal submit_outcome surface, mirroring how
// the adapter's static lane declares the tool at CreateSession.
func (c *cliSessionTable) create(ctx context.Context, sessionID string) (rpcFrame, error) {
	return c.call(ctx, "session.create", map[string]any{
		"sessionId": sessionID,
		"streaming": false,
		"tools": []map[string]any{{
			"name":        "submit_outcome",
			"description": "submit the step outcome",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"outcome": map[string]any{"type": "string"},
					"reason":  map[string]any{"type": "string"},
				},
				"required":             []string{"outcome", "reason"},
				"additionalProperties": false,
			},
		}},
		"availableTools": []any{},
	})
}

// TestRealCLISessionTableResumeContract pins the KB-396 session lifecycle
// against the real CLI.
func TestRealCLISessionTableResumeContract(t *testing.T) {
	if os.Getenv(realCLIContractDisableEnv) == "0" {
		t.Skipf("%s=0: real-CLI session-table contract disabled", realCLIContractDisableEnv)
	}
	c := startContractCLI(t)
	c.connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Run("PhaseA_same-id_resume_then_send_survives", func(t *testing.T) {
		id := newSessionID()
		frame, err := c.create(ctx, id)
		if err != nil || frame.Error != nil {
			t.Fatalf("session.create %q: %v / %+v", id, err, frame.Error)
		}
		var createResult struct {
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(frame.Result, &createResult) == nil && createResult.SessionID != "" && createResult.SessionID != id {
			// Non-fatal: the CLI echoing a different id would change
			// openSDKSession's resume keying, worth noticing immediately.
			t.Errorf("CLI assigned a different session id than requested: %q vs %q", createResult.SessionID, id)
		}
		first, err := c.call(ctx, "session.send", map[string]any{"sessionId": id, "text": "contract harness ack"})
		if err != nil {
			t.Fatalf("first session.send: %v", err)
		}
		if e := first.Error; e != nil && strings.Contains(e.Message, "Session not found") {
			t.Fatalf("first session.send failed with %q: session table broken before any re-open", e.Message)
		}
		resume, err := c.call(ctx, "session.resume", map[string]any{"sessionId": id})
		if err != nil || resume.Error != nil {
			t.Fatalf("session.resume: %v / %+v", err, resume.Error)
		}
		var resumeResult struct {
			SessionID string `json:"sessionId"`
		}
		_ = json.Unmarshal(resume.Result, &resumeResult)
		if resumeResult.SessionID != "" && resumeResult.SessionID != id {
			t.Errorf("resume returned id %q, want the requested id %q preserved (conversation continuity contract)", resumeResult.SessionID, id)
		}
		next, err := c.call(ctx, "session.send", map[string]any{"sessionId": id, "text": "post-resume ack"})
		if err != nil {
			t.Fatalf("post-resume session.send: %v", err)
		}
		if e := next.Error; e != nil {
			if strings.Contains(e.Message, "Session not found for sessionId") {
				t.Fatalf("KB-396 regression at the real CLI: send after a same-id re-open died with %d %q", e.Code, e.Message)
			}
			// A provider/auth error in the model call is environment-dependent
			// (the harness runs credential-free); the session table stays
			// intact, which is the contract under test.
			t.Logf("post-resume send surfaced non-session error %d %q: session table intact, contract holds", e.Code, e.Message)
		}
	})

	t.Run("PhaseB_destroy_after_resume_is_the_death_contract", func(t *testing.T) {
		id := newSessionID()
		frame, err := c.create(ctx, id)
		if err != nil || frame.Error != nil {
			t.Fatalf("session.create %q: %v / %+v", id, err, frame.Error)
		}
		if _, err := c.call(ctx, "session.resume", map[string]any{"sessionId": id}); err != nil {
			t.Fatalf("session.resume: %v", err)
		}
		destroy, err := c.call(ctx, "session.destroy", map[string]any{"sessionId": id})
		if err != nil || destroy.Error != nil {
			t.Fatalf("session.destroy: %v / %+v", err, destroy.Error)
		}
		after, err := c.call(ctx, "session.send", map[string]any{"sessionId": id, "text": "post-destroy ack"})
		if err != nil {
			t.Fatalf("post-destroy session.send: %v", err)
		}
		if e := after.Error; e == nil {
			t.Fatal("session.send after session.destroy unexpectedly succeeded: this CLI build no longer implements destroy-by-sessionId; re-evaluate the KB-396 same-id disconnect guard against the new behavior before loosening the fix")
		} else if !strings.Contains(e.Message, "Session not found for sessionId") {
			t.Fatalf("post-destroy send error %d %q, want the 'Session not found for sessionId' signature the KB-392 diagnosis recorded", e.Code, e.Message)
		}
	})
}
