package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	hplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// This file is the CRI-206 serving bridge. The SDK's grpcAdapterServer only
// wires the six WS03 methods — its Service interface has no lifecycle surface,
// so Pause/Resume/Snapshot/Restore would fall back to Unimplemented stubs and
// the declared mode-ref contract could never be exercised (the engine
// validates the declared state by driving Snapshot/Restore). Until the SDK
// ships a release that bridges the lifecycle RPCs, this adapter registers its
// own full-contract bridge over the same go-plugin transport. The narrow
// methods mirror the SDK bridge (v0.5.4 adapterhost/serve.go) byte for byte;
// only the registration differs.

// adapterPluginName must match the host's dispenser key (engine
// internal/adapterhost.AdapterName); it is the SDK's exported constant.
const adapterPluginName = adapterhost.AdapterName

// servePlugin serves the full adapter contract over the local go-plugin
// transport the engine uses to launch adapter subprocesses.
//
// In --emit-manifest mode (the build pipeline / `criteria adapter publish`
// extraction invocation) it delegates to the SDK, which writes adapter.yaml to
// stdout and exits without starting a plugin server. Manifest extraction only
// needs Info, which the SDK serves by calling our implementation directly.
func servePlugin(impl *copilotAdapter) {
	for _, arg := range os.Args[1:] {
		if arg == "--emit-manifest" {
			adapterhost.Serve(impl)
			return
		}
	}

	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: adapterhost.HandshakeConfig,
		Plugins: map[string]hplugin.Plugin{
			adapterPluginName: &copilotServePlugin{impl: impl},
		},
		GRPCServer: hplugin.DefaultGRPCServer,
	})
}

// copilotServePlugin registers the full-contract bridge in the local go-plugin
// transport. NetRPCUnsupportedPlugin keeps the legacy net/rpc mode unusable —
// the engine's ProtocolVersion 2 handshake only ever speaks gRPC.
type copilotServePlugin struct {
	hplugin.NetRPCUnsupportedPlugin
	impl *copilotAdapter
}

func (p *copilotServePlugin) GRPCServer(_ *hplugin.GRPCBroker, s *grpc.Server) error {
	if p.impl == nil {
		return errors.New("adapter implementation is nil")
	}
	v2.RegisterAdapterServiceServer(s, &fullContractServer{impl: p.impl})
	return nil
}

// GRPCClient is not used in the adapter process; the host-side client lives in
// the engine. This stub satisfies the hplugin.GRPCPlugin interface.
func (p *copilotServePlugin) GRPCClient(_ context.Context, _ *hplugin.GRPCBroker, _ *grpc.ClientConn) (interface{}, error) {
	return nil, errors.New("GRPCClient is not implemented in the adapter process")
}

// fullContractServer bridges copilotAdapter onto the complete generated
// v2.AdapterServiceServer interface.
//
// Snapshot/Restore are the declared-state contract: the engine's
// validateStateHandshake calls them to reattach a restored session, and a
// missing or stubbed implementation is a loud handshake failure, exactly the
// failure class the StateDescriptor design comment prescribes. Pause/Resume
// are acked by the adapter's no-op implementation (copilot_state.go). Inspect
// remains Unimplemented: the engine's control-plane InspectSession only reads
// adapter detail opportunistically and treats Unimplemented as "no detail",
// matching current engine behavior.
type fullContractServer struct {
	v2.UnimplementedAdapterServiceServer
	impl *copilotAdapter
}

func (s *fullContractServer) Info(ctx context.Context, req *v2.InfoRequest) (*v2.InfoResponse, error) {
	return s.impl.Info(ctx, req)
}

func (s *fullContractServer) OpenSession(ctx context.Context, req *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	return s.impl.OpenSession(ctx, req)
}

func (s *fullContractServer) Execute(req *v2.ExecuteRequest, stream v2.AdapterService_ExecuteServer) error {
	return s.impl.Execute(stream.Context(), req, &copilotExecuteEventServer{stream: stream})
}

// Log keeps the log stream and its heartbeat alive for the entire stream
// context, even if the adapter's Log returns early — the engine's
// heartbeat-stall detector is fed solely by this stream. Mirrors the SDK
// bridge's Log (v0.5.4 adapterhost/serve.go).
func (s *fullContractServer) Log(req *v2.LogRequest, stream v2.AdapterService_LogServer) error {
	sender := &copilotLogEventServer{stream: stream}

	go func() {
		_ = v2.RunHeartbeat(stream.Context(), "log", func(hb *v2.Heartbeat) error {
			return sender.Send(&v2.LogEvent{Heartbeat: hb})
		})
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.impl.Log(stream.Context(), req, sender)
	}()

	if err := <-errCh; err != nil {
		return err
	}

	// The adapter has finished logging. Keep the stream (and therefore the
	// heartbeat) open until the host cancels the stream context.
	<-stream.Context().Done()
	return nil
}

func (s *fullContractServer) Permissions(stream v2.AdapterService_PermissionsServer) error {
	return s.impl.Permissions(stream.Context(), &copilotPermissionsServer{stream: stream})
}

func (s *fullContractServer) CloseSession(ctx context.Context, req *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	return s.impl.CloseSession(ctx, req)
}

// Lifecycle contract (CRI-206): Snapshot hands the host the adapter-owned
// token, Restore reattaches the live session (exact-address, loud on failure).
func (s *fullContractServer) Snapshot(ctx context.Context, req *v2.SnapshotRequest) (*v2.SnapshotResponse, error) {
	return s.impl.Snapshot(ctx, req)
}

func (s *fullContractServer) Restore(ctx context.Context, req *v2.RestoreRequest) (*v2.RestoreResponse, error) {
	return s.impl.Restore(ctx, req)
}

func (s *fullContractServer) Pause(ctx context.Context, req *v2.PauseRequest) (*v2.PauseResponse, error) {
	return s.impl.Pause(ctx, req)
}

func (s *fullContractServer) Resume(ctx context.Context, req *v2.ResumeRequest) (*v2.ResumeResponse, error) {
	return s.impl.Resume(ctx, req)
}

// copilotExecuteEventServer adapts the generated server-streaming interface to
// adapterhost.ExecuteEventSender. The mutex serialises all Send calls because
// grpc.ServerStream.Send is not goroutine-safe.
type copilotExecuteEventServer struct {
	mu     sync.Mutex
	stream v2.AdapterService_ExecuteServer
}

func (s *copilotExecuteEventServer) Send(evt *v2.ExecuteEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(evt)
}

// copilotLogEventServer adapts the generated log stream to
// adapterhost.LogEventSender, with the same mutex discipline as above.
type copilotLogEventServer struct {
	mu     sync.Mutex
	stream v2.AdapterService_LogServer
}

func (s *copilotLogEventServer) Send(evt *v2.LogEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(evt)
}

// copilotPermissionsServer adapts the generated bidi permission stream to
// adapterhost.PermissionsStream.
type copilotPermissionsServer struct {
	stream v2.AdapterService_PermissionsServer
}

func (s *copilotPermissionsServer) Recv() (*v2.PermissionEvent, error) {
	return s.stream.Recv()
}

func (s *copilotPermissionsServer) Send(dec *v2.PermissionDecision) error {
	return s.stream.Send(dec)
}

func (s *copilotPermissionsServer) Context() context.Context {
	return s.stream.Context()
}

// serveRemoteFull is the phone-home counterpart of servePlugin: it dials the
// criteria host, sends the v2 identity handshake, and serves the full contract
// on the held connection. When opts.Reconnect is true it redials with
// exponential backoff and blocks indefinitely — the connection is the
// workflow's lifeline. Mechanics mirror the SDK's ServeRemote
// (v0.5.4 adapterhost/serve_remote.go); only the registration differs
// (full contract instead of the SDK's six-method bridge).
func serveRemoteFull(impl *copilotAdapter, opts *adapterhost.ServeRemoteOptions) error {
	if opts == nil {
		return errors.New("serveRemoteFull: opts is required")
	}
	if opts.Host == "" {
		return errors.New("serveRemoteFull: Host is required")
	}

	if !opts.Reconnect {
		_, err := serveRemoteOnceFull(impl, opts)
		return err
	}

	initial := opts.InitialDelay
	if initial <= 0 {
		initial = time.Second
	}
	maxDelay := opts.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}

	delay := initial
	for {
		connected, _ := serveRemoteOnceFull(impl, opts)
		if connected {
			delay = initial // reset backoff once a connection was established
		}
		time.Sleep(delay)
		if delay = delay * 2; delay > maxDelay {
			delay = maxDelay
		}
	}
}

// serveRemoteOnceFull dials, handshakes, and serves a single connection. It
// reports whether a connection was successfully established (so the caller can
// reset backoff) along with any error.
func serveRemoteOnceFull(impl *copilotAdapter, opts *adapterhost.ServeRemoteOptions) (connected bool, err error) {
	conn, err := dialRemoteCopilot(opts.Host, opts.TLSConfig)
	if err != nil {
		return false, fmt.Errorf("serveRemoteFull: dial %s: %w", opts.Host, err)
	}

	if err := sendCopilotRemoteHandshake(conn, opts.Identity, opts.AcceptToken); err != nil {
		_ = conn.Close()
		return false, fmt.Errorf("serveRemoteFull: handshake: %w", err)
	}

	server := grpc.NewServer()
	v2.RegisterAdapterServiceServer(server, &fullContractServer{impl: impl})

	wrapped := &copilotCloseSignalConn{Conn: conn, doneCh: make(chan struct{})}
	lis := newCopilotSingleConnListener(wrapped)

	// When the underlying connection is closed (by the peer or by the gRPC
	// transport), close the listener so grpc.Server.Serve unblocks and returns.
	go func() {
		<-wrapped.doneCh
		_ = lis.Close()
	}()

	return true, server.Serve(lis)
}

// dialRemoteCopilot opens a TCP or Unix connection to the host. TLS is applied
// only for TCP addresses when a config is provided.
func dialRemoteCopilot(host string, tlsConfig *tls.Config) (net.Conn, error) {
	network := "tcp"
	if filepath.IsAbs(host) || (host != "" && host[0] == '/') {
		network = "unix"
	}

	if tlsConfig != nil && network == "tcp" {
		return tls.Dial("tcp", host, tlsConfig)
	}
	return net.Dial(network, host)
}

// copilotRemoteHandshake is the JSON line sent immediately after the transport
// connection is established. The host shim reads it before allowing gRPC
// frames to flow.
type copilotRemoteHandshake struct {
	Name               string `json:"name"`
	Version            string `json:"version"`
	Digest             string `json:"digest"`
	Token              string `json:"token,omitempty"`
	SDKProtocolVersion int    `json:"sdk_protocol_version"`
}

// sendCopilotRemoteHandshake writes the identity handshake line to conn.
func sendCopilotRemoteHandshake(conn net.Conn, identity adapterhost.RemoteIdentity, token string) error {
	h := copilotRemoteHandshake{
		Name:               identity.Name,
		Version:            identity.Version,
		Digest:             identity.Digest,
		Token:              token,
		SDKProtocolVersion: 2,
	}
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := conn.Write(data); err != nil {
		return err
	}
	return nil
}

// copilotCloseSignalConn wraps a net.Conn and signals via doneCh the first
// time Close() is called, so serveRemoteOnceFull can detect a peer-closed
// connection and unblock grpc.Server.Serve.
type copilotCloseSignalConn struct {
	net.Conn
	once   sync.Once
	doneCh chan struct{}
}

func (c *copilotCloseSignalConn) Close() error {
	c.once.Do(func() { close(c.doneCh) })
	return c.Conn.Close()
}

// copilotSingleConnListener is a net.Listener that returns a pre-opened
// connection on its first Accept() and then blocks until Close() is called.
// It lets a grpc.Server serve on a connection that was dialed outbound (the
// phone-home model).
type copilotSingleConnListener struct {
	conn   net.Conn
	mu     sync.Mutex
	used   bool
	closed chan struct{}
}

func newCopilotSingleConnListener(conn net.Conn) *copilotSingleConnListener {
	return &copilotSingleConnListener{
		conn:   conn,
		closed: make(chan struct{}),
	}
}

func (l *copilotSingleConnListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if !l.used {
		l.used = true
		l.mu.Unlock()
		return l.conn, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, errors.New("listener closed")
}

func (l *copilotSingleConnListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.closed:
		// already closed
	default:
		close(l.closed)
	}
	return nil
}

func (l *copilotSingleConnListener) Addr() net.Addr { return l.conn.LocalAddr() }
