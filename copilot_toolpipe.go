// copilot_toolpipe.go — in-memory PermissionsStream that feeds the SDK's
// ToolCallBridge dispatch loop from the adapter's own Permissions receive
// loop, and forwards the bridge's PermissionDecision ACKs back onto the real
// stream (CRI-178).
//
// The adapter keeps its own plain-permission receive loop (pendingPerms for
// Copilot SDK callbacks); the ToolCallBridge's dispatch loop consumes the
// forwarded events and owns the tool-call side (ACK + old-host grant mark,
// cancel routing, tool_call_result routing, and resolving pending calls when
// the stream ends). See copilot.go Permissions/dispatchPermEvent.

package main

import (
	"context"
	"io"
	"sync"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// permissionPipe is an adapterhost.PermissionsStream backed by a channel.
// Recv yields events forwarded by the adapter's Permissions loop; Send
// forwards the bridge's ACKs onto the real stream. One pipe serves one
// Permissions bidi stream.
type permissionPipe struct {
	feed chan *v2.PermissionEvent
	real adapterhost.PermissionsStream

	// done closes when the consumer (the bridge's dispatch goroutine) has
	// returned, so forwards never block on a dead consumer.
	done chan struct{}

	closeFeedOnce sync.Once
}

func newPermissionPipe(real adapterhost.PermissionsStream) *permissionPipe {
	return &permissionPipe{
		feed: make(chan *v2.PermissionEvent),
		real: real,
		done: make(chan struct{}),
	}
}

// Context implements adapterhost.PermissionsStream: the pipe reports the
// context its consumer was started with.
func (pp *permissionPipe) Context() context.Context {
	return pp.real.Context()
}

// run starts the bridge's dispatch loop on its own goroutine. It must be
// called exactly once, before any forward.
func (pp *permissionPipe) run(ctx context.Context, bridge *adapterhost.ToolCallBridge) {
	go func() {
		defer close(pp.done)
		_ = bridge.Permissions(ctx, pp)
	}()
}

// Recv implements adapterhost.PermissionsStream: the feed ends with io.EOF so
// the bridge's dispatch loop exits and resolves every pending call.
func (pp *permissionPipe) Recv() (*v2.PermissionEvent, error) {
	ev, ok := <-pp.feed
	if !ok {
		return nil, io.EOF
	}
	return ev, nil
}

// Send implements adapterhost.PermissionsStream: the bridge's ACKs go
// straight onto the real stream.
func (pp *permissionPipe) Send(decision *v2.PermissionDecision) error {
	return pp.real.Send(decision)
}

// close ends the feed. Idempotent; pending forwards drain through done.
func (pp *permissionPipe) close() {
	pp.closeFeedOnce.Do(func() {
		close(pp.feed)
	})
}

// forward hands one PermissionEvent to the bridge's dispatch loop. It drops
// the event if the consumer has exited (the stream is ending anyway).
func (pp *permissionPipe) forward(ev *v2.PermissionEvent) {
	select {
	case pp.feed <- ev:
	case <-pp.done:
	}
}
