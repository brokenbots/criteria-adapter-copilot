// copilot_toolcall_test.go — tests for the agent-invocable adapter_tool tool
// (CRI-178). A fake host, mirroring the SDK's toolcall_test.go
// toolCallTestHost pattern, drives the registered tool handler end to end:
// the handler issues the call through the SDK's ToolCallBridge, the fake host
// replies over the Permissions stream, and the adapter's own Permissions loop
// forwards every event to the bridge via the in-memory pipe. This exercises
// the real dispatchPermEvent routing — grant, cancel, and tool_call_result —
// not a bridge-only shortcut.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	copilot "github.com/github/copilot-sdk/go"
	"github.com/github/copilot-sdk/go/rpc"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

const testAdapterToolSession = "sess-adapter-tool"

// fakeToolCallHost is the in-memory host side of the adapter-tools wire. Its
// execute loop consumes the permission.request events the bridge sends and
// replies with the PermissionEvents the test scripts via onRequest; those
// events flow into the adapter's Permissions stream, whose loop forwards them
// to the bridge through the pipe.
type fakeToolCallHost struct {
	t *testing.T

	// onRequest is the test's scripted reply: given the request_id and payload
	// of a permission.request AdapterEvent, it returns the PermissionEvents
	// the host emits on the Permissions stream.
	onRequest func(requestID string, payload *structpb.Struct) []*v2.PermissionEvent

	executeC chan *v2.ExecuteEvent
	permC    chan *v2.PermissionEvent
	stopC    chan struct{}
	loopDone chan struct{}

	ctx       context.Context
	cancelCtx context.CancelFunc

	mu   sync.Mutex
	sent []*structpb.Struct
	acks []*v2.PermissionDecision
}

func newFakeToolCallHost(t *testing.T) *fakeToolCallHost {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &fakeToolCallHost{
		t:         t,
		executeC:  make(chan *v2.ExecuteEvent, 16),
		permC:     make(chan *v2.PermissionEvent, 32),
		stopC:     make(chan struct{}),
		loopDone:  make(chan struct{}),
		ctx:       ctx,
		cancelCtx: cancel,
	}
}

// start launches the fake host's execute loop (consuming permission.request
// events and replying via onRequest) and the adapter's real Permissions loop,
// which forwards events into the bridge.
func (h *fakeToolCallHost) start(p *copilotAdapter) {
	go func() {
		defer close(h.loopDone)
		for {
			select {
			case ev := <-h.executeC:
				payload := ev.GetAdapter().GetPayload()
				requestID := payload.GetFields()["request_id"].GetStringValue()
				h.mu.Lock()
				h.sent = append(h.sent, payload)
				h.mu.Unlock()
				if h.onRequest != nil {
					for _, pe := range h.onRequest(requestID, payload) {
						select {
						case h.permC <- pe:
						case <-h.stopC:
							return
						}
					}
				}
			case <-h.stopC:
				return
			}
		}
	}()
	go func() {
		_ = p.Permissions(h.ctx, fakePermissionsStream{h: h})
	}()
}

// stop shuts the fake host down; the adapter's Permissions loop exits with it
// (its Recv observes the canceled context).
func (h *fakeToolCallHost) stop() {
	close(h.stopC)
	h.cancelCtx()
	select {
	case <-h.loopDone:
	case <-time.After(5 * time.Second):
		h.t.Fatal("fake host execute loop did not stop")
	}
}

func (h *fakeToolCallHost) sink() adapterhost.ExecuteEventSender { return fakeExecuteSink{h: h} }

func (h *fakeToolCallHost) sentPayloads() []*structpb.Struct {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*structpb.Struct(nil), h.sent...)
}

func (h *fakeToolCallHost) acksSent() []*v2.PermissionDecision {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*v2.PermissionDecision(nil), h.acks...)
}

type fakeExecuteSink struct{ h *fakeToolCallHost }

func (s fakeExecuteSink) Send(ev *v2.ExecuteEvent) error {
	select {
	case s.h.executeC <- ev:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("test: fake host did not consume the execute event")
	}
}

type fakePermissionsStream struct{ h *fakeToolCallHost }

func (s fakePermissionsStream) Recv() (*v2.PermissionEvent, error) {
	select {
	case ev, ok := <-s.h.permC:
		if !ok {
			return nil, io.EOF
		}
		return ev, nil
	case <-s.h.ctx.Done():
		return nil, s.h.ctx.Err()
	}
}

func (s fakePermissionsStream) Send(d *v2.PermissionDecision) error {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()
	s.h.acks = append(s.h.acks, d)
	return nil
}

func (s fakePermissionsStream) Context() context.Context { return s.h.ctx }

func toolCallGrantEvent(requestID string) *v2.PermissionEvent {
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_Request{Request: &v2.PermissionRequest{RequestId: requestID}},
	}
}

func toolCallCancelEvent(requestID, reason string) *v2.PermissionEvent {
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_Cancel{Cancel: &v2.PermissionCancel{RequestId: requestID, Reason: reason}},
	}
}

func toolCallResultEvent(requestID string, res *v2.ToolCallResult) *v2.PermissionEvent {
	res.RequestId = requestID
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_ToolCallResult{ToolCallResult: res},
	}
}

// newAdapterWithSession wires an adapter whose only session has the fake
// host's sink and an active step, so the tool handler can issue calls.
func newAdapterWithSession(t *testing.T, host *fakeToolCallHost) *copilotAdapter {
	t.Helper()
	p := newCopilotAdapter()
	p.mu.Lock()
	p.sessions[testAdapterToolSession] = &sessionState{
		sink:     host.sink(),
		active:   true,
		activeCh: make(chan struct{}),
	}
	p.mu.Unlock()
	return p
}

// invokeAdapterTool drives the registered tool handler the way the Copilot
// SDK would: typed args plus a ToolInvocation carrying the trace context.
func invokeAdapterTool(t *testing.T, p *copilotAdapter, ctx context.Context, target string, args map[string]any) copilot.ToolResult {
	t.Helper()
	res, err := p.handleAdapterToolCall(testAdapterToolSession, copilot.ToolInvocation{TraceContext: ctx}, AdapterToolArgs{Target: target, Args: args})
	if err != nil {
		t.Fatalf("handleAdapterToolCall returned a Go error (the handler must keep control of the turn): %v", err)
	}
	return res
}

// TestAdapterToolCallSuccessEndToEnd covers the success path end to end:
// grant -> tool_call_result -> ToolResult success whose text is the callee
// outputs JSON, with the CRI-152 wire contract on the permission.request
// payload and exactly one bridge ACK for the grant.
func TestAdapterToolCallSuccessEndToEnd(t *testing.T) {
	host := newFakeToolCallHost(t)
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			toolCallGrantEvent(requestID),
			toolCallResultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "success",
				OutputsJson: []byte(`{"files":["main.go"],"count":2}`),
			}),
		}
	}
	p := newAdapterWithSession(t, host)
	host.start(p)
	defer host.stop()

	res := invokeAdapterTool(t, p, context.Background(), "adapter.shell.worker.tools.git_status", map[string]any{"path": "."})
	if res.ResultType != "success" {
		t.Fatalf("ResultType = %q, want success", res.ResultType)
	}
	if res.Error != "" {
		t.Fatalf("unexpected Error on success: %q", res.Error)
	}
	// The outputs round-trip JSON: compare decoded, not byte-exact.
	var outputs map[string]any
	if err := json.Unmarshal([]byte(res.TextResultForLLM), &outputs); err != nil {
		t.Fatalf("TextResultForLLM is not JSON: %v", err)
	}
	want := map[string]any{"files": []any{"main.go"}, "count": float64(2)}
	if !reflect.DeepEqual(outputs, want) {
		t.Fatalf("TextResultForLLM decoded = %#v, want %#v", outputs, want)
	}

	sent := host.sentPayloads()
	if len(sent) != 1 {
		t.Fatalf("sent %d permission.request payloads, want 1", len(sent))
	}
	fields := sent[0].GetFields()
	if got := fields["kind"].GetStringValue(); got != "adapter_tool" {
		t.Errorf("payload kind = %q, want adapter_tool", got)
	}
	if got := fields["request_id"].GetStringValue(); got == "" {
		t.Error("payload request_id is empty")
	}
	if got := fields["target"].GetStringValue(); got != "adapter.shell.worker.tools.git_status" {
		t.Errorf("payload target = %q", got)
	}
	if got := fields["tool"].GetStringValue(); got != "git_status" {
		t.Errorf("payload tool = %q, want git_status", got)
	}
	if got := fields["args"].GetStructValue().GetFields()["path"].GetStringValue(); got != "." {
		t.Errorf("payload args.path = %q, want .", got)
	}
	if got := fields["args_digest"].GetStringValue(); got == "" {
		t.Error("payload args_digest is empty")
	}
	preview := fields["args_preview"].GetStringValue()
	if preview == "" {
		t.Error("payload args_preview missing")
	}
	if !strings.Contains(preview, `"path":"."`) {
		t.Errorf("payload args_preview = %q, want a printable view of the args", preview)
	}

	acks := host.acksSent()
	if len(acks) != 1 {
		t.Fatalf("bridge ACKed %d times, want exactly 1 (no double-ACK)", len(acks))
	}
	if acks[0].GetRequestId() != sent[0].GetFields()["request_id"].GetStringValue() || acks[0].GetDecision() != "allow" {
		t.Errorf("ACK = %q/%q, want allow for the call's request_id", acks[0].GetDecision(), acks[0].GetRequestId())
	}
}

// TestAdapterToolCallDenied covers the cancel path: a host cancel (e.g. the
// step's tools list does not grant the call) surfaces as a ToolResult with
// Error set — data for the agent, not a turn-ending Go error.
func TestAdapterToolCallDenied(t *testing.T) {
	host := newFakeToolCallHost(t)
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			toolCallCancelEvent(requestID, "step tools list does not grant this call"),
		}
	}
	p := newAdapterWithSession(t, host)
	host.start(p)
	defer host.stop()

	res := invokeAdapterTool(t, p, context.Background(), "adapter.shell.worker.tools.git_status", nil)
	if res.ResultType != "failure" {
		t.Fatalf("ResultType = %q, want failure", res.ResultType)
	}
	for _, want := range []string{"tool call denied", "step tools list does not grant this call"} {
		if !strings.Contains(res.Error, want) {
			t.Errorf("Error %q does not mention %q", res.Error, want)
		}
	}
	if res.Error == "" || res.TextResultForLLM == "" {
		t.Error("denied call must carry both Error and an LLM-visible message")
	}
	if len(host.sentPayloads()) != 1 {
		t.Errorf("sent %d payloads, want 1", len(host.sentPayloads()))
	}
	if n := len(host.acksSent()); n != 0 {
		t.Errorf("bridge ACKed %d times on the cancel path, want 0", n)
	}
}

// TestAdapterToolCallCalleeFailureOutcome covers a callee that completes with
// a non-success outcome: the agent sees a failure ToolResult.
func TestAdapterToolCallCalleeFailureOutcome(t *testing.T) {
	host := newFakeToolCallHost(t)
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			toolCallGrantEvent(requestID),
			toolCallResultEvent(requestID, &v2.ToolCallResult{Outcome: "failure"}),
		}
	}
	p := newAdapterWithSession(t, host)
	host.start(p)
	defer host.stop()

	res := invokeAdapterTool(t, p, context.Background(), "adapter.shell.worker.tools.git_status", nil)
	if res.ResultType != "failure" {
		t.Fatalf("ResultType = %q, want failure", res.ResultType)
	}
	if !strings.Contains(res.Error, `outcome "failure"`) {
		t.Errorf("Error %q does not report the callee outcome", res.Error)
	}
}

// TestAdapterToolCallHostUnsupportedCached covers the old-host pattern: a bare
// allow-grant with no tool_call_result until the call deadline is
// host_unsupported, and the session is cached so the next call fails fast
// without another wire send.
func TestAdapterToolCallHostUnsupportedCached(t *testing.T) {
	host := newFakeToolCallHost(t)
	host.onRequest = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{toolCallGrantEvent(requestID)}
	}
	p := newAdapterWithSession(t, host)
	host.start(p)
	defer host.stop()

	// The first call runs under a short caller deadline; deterministically
	// wait for the grant to be processed (the bridge ACKs the allow-grant)
	// before the deadline can fire. Results are collected, not asserted, on
	// the worker goroutine so a regression fails the test instead of hanging
	// or panicking.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	type callResult struct {
		res copilot.ToolResult
		err error
	}
	first := make(chan callResult, 1)
	go func() {
		res, err := p.handleAdapterToolCall(testAdapterToolSession, copilot.ToolInvocation{TraceContext: ctx}, AdapterToolArgs{Target: "adapter.shell.worker.tools.git_status"})
		first <- callResult{res: res, err: err}
	}()
	waitForAcks(t, host, 1, 2*time.Second)
	got := <-first
	if got.err != nil {
		t.Fatalf("first call returned a Go error (the handler must keep control of the turn): %v", got.err)
	}
	if got.res.ResultType != "failure" || !strings.Contains(got.res.Error, "host_unsupported") {
		t.Fatalf("first call: got %q/%q, want failure ToolResult with the typed host_unsupported error", got.res.ResultType, got.res.Error)
	}

	// The cached session makes the next call fail fast without sending.
	second := invokeAdapterTool(t, p, context.Background(), "adapter.shell.worker.tools.git_status", nil)
	if second.ResultType != "failure" || !strings.Contains(second.Error, "host_unsupported") {
		t.Fatalf("second call: got %q/%q, want deterministic host_unsupported failure", second.ResultType, second.Error)
	}
	if second.Error != got.res.Error {
		t.Errorf("host_unsupported error not deterministic: %q vs %q", second.Error, got.res.Error)
	}
	if n := len(host.sentPayloads()); n != 1 {
		t.Errorf("sent %d payloads after two calls, want 1 (cached session must not send again)", n)
	}
}

func waitForAcks(t *testing.T, host *fakeToolCallHost, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if n := len(host.acksSent()); n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d ACKs (have %d)", want, len(host.acksSent()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAdapterToolCallTargetValidation checks the client-side target grammar:
// the full `adapter.<type>.<name>.tools.<tool>` form is required, mirroring
// the host's parser, and a malformed target never reaches the wire.
func TestAdapterToolCallTargetValidation(t *testing.T) {
	cases := []struct {
		name   string
		target string
		want   string // expected tool; empty means rejected
	}{
		{name: "named form", target: "adapter.shell.worker.tools.git_status", want: "git_status"},
		{name: "bareword label charset", target: "adapter.data-ingest.worker2.tools.run_query", want: "run_query"},
		{name: "surrounding spaces trimmed", target: " adapter.shell.worker.tools.git_status ", want: "git_status"},
		{name: "bare surface form", target: "adapter.shell.worker.tools"},
		{name: "missing tools label", target: "adapter.shell.worker.run_tool"},
		{name: "wrong root", target: "mcp.shell.worker.tools.run_query"},
		{name: "too many labels", target: "adapter.shell.worker.tools.git_status.extra"},
		{name: "empty label", target: "adapter..worker.tools.git_status"},
		{name: "leading digit label", target: "adapter.1shell.worker.tools.git_status"},
		{name: "empty", target: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tool, err := parseAdapterToolTarget(tc.target)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("parseAdapterToolTarget(%q) succeeded with tool %q, want error", tc.target, tool)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAdapterToolTarget(%q): %v", tc.target, err)
			}
			if tool != tc.want {
				t.Fatalf("tool = %q, want %q", tool, tc.want)
			}
		})
	}
}

// TestAdapterToolCallMalformedTargetStaysLocal drives the handler with a
// malformed target: a failure ToolResult is returned and nothing is sent.
func TestAdapterToolCallMalformedTargetStaysLocal(t *testing.T) {
	host := newFakeToolCallHost(t)
	p := newAdapterWithSession(t, host)
	host.start(p)
	defer host.stop()

	res := invokeAdapterTool(t, p, context.Background(), "adapter.shell.worker.tools", nil)
	if res.ResultType != "failure" || res.Error == "" {
		t.Fatalf("got %q/%q, want failure ToolResult with a typed error", res.ResultType, res.Error)
	}
	if !strings.Contains(res.Error, "adapter.<type>.<name>.tools.<tool>") {
		t.Errorf("Error %q does not guide the agent to the target form", res.Error)
	}
	if n := len(host.sentPayloads()); n != 0 {
		t.Errorf("sent %d payloads for a malformed target, want 0", n)
	}
}

// TestAdapterToolCallUnknownSession covers a handler invocation on an
// unregistered adapter session.
func TestAdapterToolCallUnknownSession(t *testing.T) {
	p := newCopilotAdapter()
	res, err := p.handleAdapterToolCall("no-such-session", copilot.ToolInvocation{}, AdapterToolArgs{
		Target: "adapter.shell.worker.tools.git_status",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.ResultType != "failure" || !strings.Contains(res.Error, "unknown session") {
		t.Fatalf("got %q/%q, want failure ToolResult about the unknown session", res.ResultType, res.Error)
	}
}

// TestAdapterToolCallInactiveSession covers a handler invocation when the
// session exists but no step is active.
func TestAdapterToolCallInactiveSession(t *testing.T) {
	p := newCopilotAdapter()
	p.mu.Lock()
	p.sessions[testAdapterToolSession] = &sessionState{activeCh: make(chan struct{})}
	p.mu.Unlock()

	res := invokeAdapterTool(t, p, context.Background(), "adapter.shell.worker.tools.git_status", nil)
	if res.ResultType != "failure" || !strings.Contains(res.Error, "outside an active step") {
		t.Fatalf("got %q/%q, want failure ToolResult about the inactive step", res.ResultType, res.Error)
	}
}

// TestDispatchPermEventRoutesToolCallResult drives dispatchPermEvent through
// the real Permissions loop: a tool_call_result correlated with an in-flight
// call resolves it (the success end-to-end test covers the full call), a
// tool_call_result for an unknown id is dropped without side effects, and the
// plain request/cancel paths still resolve pendingPerms with a single ACK —
// no regression, no double-ACK from the forwarding design.
func TestDispatchPermEventRoutesToolCallResult(t *testing.T) {
	host := newFakeToolCallHost(t)
	p := newCopilotAdapter()
	host.start(p)
	defer host.stop()

	// Plain request: pendingPerm resolved with "allow" and exactly one ACK on
	// the real stream.
	allowCh := make(chan string, 1)
	p.registerPendingPerm("req-a", allowCh)
	host.permC <- toolCallGrantEvent("req-a")
	select {
	case decision := <-allowCh:
		if decision != "allow" {
			t.Fatalf("plain request decision = %q, want allow", decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("plain request was not resolved")
	}
	waitForAcks(t, host, 1, 2*time.Second)
	if n := len(host.acksSent()); n != 1 {
		t.Fatalf("plain request produced %d ACKs, want exactly 1 (no double-ACK)", n)
	}

	// Plain cancel: pendingPerm resolved with "deny", no ACK.
	denyCh := make(chan string, 1)
	p.registerPendingPerm("req-b", denyCh)
	host.permC <- toolCallCancelEvent("req-b", "host policy changed")
	select {
	case decision := <-denyCh:
		if decision != "deny" {
			t.Fatalf("plain cancel decision = %q, want deny", decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("plain cancel was not resolved")
	}
	if n := len(host.acksSent()); n != 1 {
		t.Fatalf("cancel produced an ACK, want none (have %d)", n)
	}

	// tool_call_result for an id the bridge does not know: dropped without
	// side effects.
	host.permC <- toolCallResultEvent("req-unknown", &v2.ToolCallResult{Outcome: "success"})
	if n := len(host.acksSent()); n != 1 {
		t.Errorf("unknown tool_call_result changed the ACK count to %d, want 1", n)
	}
}

// TestSessionConfigRegistersAdapterTool asserts the tool registration
// contract: adapter_tool is registered alongside submit_outcome, describes
// itself for the agent, and is NOT skip-permission — the wire call is the
// gated event (ADR-0004) and the SDK's own gate is answered locally — while
// submit_outcome keeps SkipPermission=true as the outcome channel.
func TestSessionConfigRegistersAdapterTool(t *testing.T) {
	p := newCopilotAdapter()
	sc := p.buildSessionConfig(nil, testAdapterToolSession)

	var adapterTool, submitTool *copilot.Tool
	for i := range sc.Tools {
		switch sc.Tools[i].Name {
		case adapterToolToolName:
			adapterTool = &sc.Tools[i]
		case submitOutcomeToolName:
			submitTool = &sc.Tools[i]
		}
	}
	if adapterTool == nil {
		t.Fatal("adapter_tool is not registered")
	}
	if submitTool == nil {
		t.Fatal("submit_outcome is not registered")
	}
	if adapterTool.SkipPermission {
		t.Error("adapter_tool must not set SkipPermission: the call itself is the gated permission request")
	}
	if !submitTool.SkipPermission {
		t.Error("submit_outcome must keep SkipPermission=true: it is the outcome channel, not a gated side effect")
	}
	for _, want := range []string{"target", "args"} {
		if !strings.Contains(adapterTool.Description, want) {
			t.Errorf("adapter_tool description does not document %q", want)
		}
	}
	if !strings.Contains(adapterTool.Description, "NOT a step failure") {
		t.Error("adapter_tool description must state that a failed call is not a step failure")
	}
}

// TestAdapterToolArgsPreview covers the bounded preview: short args pass
// through, long args are truncated on a rune boundary and stay valid UTF-8.
func TestAdapterToolArgsPreview(t *testing.T) {
	if got := adapterToolArgsPreview(nil); got != "{}" {
		t.Fatalf("nil-args preview = %q, want {} (matches the wire args object)", got)
	}
	if got := adapterToolArgsPreview(map[string]any{"path": "."}); got != `{"path":"."}` {
		t.Fatalf("short preview = %q", got)
	}
	long := strings.Repeat("é", 300) // 600 bytes of two-byte runes
	got := adapterToolArgsPreview(map[string]any{"blob": long})
	if len(got) > 258 { // 256 bytes + truncation marker
		t.Fatalf("preview too long: %d bytes", len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatal("preview is not valid UTF-8 after truncation")
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("truncated preview %q missing ellipsis", got)
	}
}

// TestAdapterToolPermissionRequestApprovedLocally covers the permission
// short-circuit for the adapter_tool custom tool itself (CRI-178): the SDK's
// tool-permission request for adapter_tool is answered locally with
// ApproveOnce and no error, and NO permission.request event reaches the sink,
// on any session state — the wire call the handler issues is the gated event
// (ADR-0004 §8), so the SDK's tool-permission layer must not add a second
// gate. Enforcement lives in the tool handler (target shape, active session)
// and on the host's per-call gate, not in this SDK callback.
func TestAdapterToolPermissionRequestApprovedLocally(t *testing.T) {
	activeSink := &recordingSender{}
	cases := []struct {
		name    string
		p       *copilotAdapter
		session string
	}{
		{
			name: "active session",
			p: func() *copilotAdapter {
				return &copilotAdapter{sessions: map[string]*sessionState{
					"s1": {session: &fakeSession{}, active: true, activeCh: make(chan struct{}), sink: activeSink},
				}}
			}(),
			session: "s1",
		},
		{
			name:    "unknown session",
			p:       &copilotAdapter{sessions: map[string]*sessionState{}},
			session: "nonexistent",
		},
		{
			name: "inactive session",
			p: func() *copilotAdapter {
				return &copilotAdapter{sessions: map[string]*sessionState{
					"s1": {session: &fakeSession{}, active: false, sink: &recordingSender{}},
				}}
			}(),
			session: "s1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A watchdog keeps the failure loud if the short-circuit is ever
			// dropped: the active-session case would otherwise block forever
			// on the pending-perm channel instead of returning locally.
			type decision struct {
				kind rpc.PermissionDecisionKind
				err  error
			}
			done := make(chan decision, 1)
			go func() {
				result, err := tc.p.handlePermissionRequest(tc.session, copilot.PermissionRequestCustomTool{ToolName: adapterToolToolName})
				if result == nil {
					done <- decision{err: err}
					return
				}
				done <- decision{kind: result.Kind(), err: err}
			}()
			var got decision
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handlePermissionRequest did not return: adapter_tool was not answered locally")
			}
			if got.err != nil {
				t.Fatalf("unexpected error: %v", got.err)
			}
			if got.kind != rpc.PermissionDecisionKindApproveOnce {
				t.Fatalf("result.Kind = %q, want %q (answered locally, independent of session state)", got.kind, rpc.PermissionDecisionKindApproveOnce)
			}
		})
	}
	// No permission.request may be emitted to a wired sink by the local approve.
	if got := activeSink.snapshot(); len(got) != 0 {
		t.Fatalf("local approve emitted %d event(s) to the sink, want 0 (no second gate)", len(got))
	}
}

// TestAdapterToolPermissionRequestDoesNotCatchOtherTools is the
// regression-sensitive negative: only the adapter_tool custom tool is
// answered locally. Any other custom tool takes the normal path — forwarded
// to the host gate for an active session (a permission.request event is
// emitted), UserNotAvailable for an unknown session.
func TestAdapterToolPermissionRequestDoesNotCatchOtherTools(t *testing.T) {
	t.Run("unknown session takes the normal path", func(t *testing.T) {
		p := &copilotAdapter{sessions: map[string]*sessionState{}}
		result, err := p.handlePermissionRequest("nonexistent", copilot.PermissionRequestCustomTool{ToolName: "some_other_tool"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Kind() != rpc.PermissionDecisionKindUserNotAvailable {
			t.Fatalf("result.Kind = %q, want %q (a non-adapter_tool request must not be locally approved)", result.Kind(), rpc.PermissionDecisionKindUserNotAvailable)
		}
	})

	t.Run("active session forwards to the host gate", func(t *testing.T) {
		sender := &recordingSender{}
		s := &sessionState{
			session:  &fakeSession{},
			active:   true,
			activeCh: make(chan struct{}),
			sink:     sender,
		}
		p := &copilotAdapter{sessions: map[string]*sessionState{"s1": s}}

		// The normal path blocks until the host resolves the pending perm;
		// simulate the host approving once the event is emitted.
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
					return
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}()

		result, err := p.handlePermissionRequest("s1", copilot.PermissionRequestCustomTool{ToolName: "some_other_tool"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Kind() != rpc.PermissionDecisionKindApproveOnce {
			t.Fatalf("result.Kind = %q, want %q (host-approved on the normal path)", result.Kind(), rpc.PermissionDecisionKindApproveOnce)
		}
		if got := sender.snapshot(); len(got) != 1 {
			t.Fatalf("normal path emitted %d event(s), want exactly 1 permission.request", len(got))
		}
	})
}
