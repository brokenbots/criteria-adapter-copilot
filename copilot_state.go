// copilot_state.go — declared state contract (CRI-206): the state descriptor
// served from Info(), the Snapshot/Restore lifecycle RPCs, and the
// loud reattach failure path.
//
// Declaration (mode ref): the adapter's checkpointable state is an opaque
// token to ADAPTER-OWNED state — the Copilot SDK session id. The host
// persists the token, never the payload (StateDescriptor mode "ref" in
// criteria-adapter-proto; the harness owns the transcript). Snapshot returns
// the live conversation's address; Restore reattaches to it exactly and is
// never downgraded to a fuzzy match or a silent fresh start.
//
// Reattach failures are LOUD and terminal: Restore returns an error and the
// host kills the adapter run. A silently-fresh-start after a failed reattach
// is the wrong-state failure class the StateDescriptor contract exists to
// prevent (see the proto StateDescriptor comment — "Unknown values are NOT
// silently downgraded"; the same rationale governs a failed reattach, which
// would otherwise restore into a plausible-looking but semantically foreign
// session).

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

const (
	// copilotStateMode is the declared checkpoint state mode: "ref" —
	// Snapshot/Restore carry a token to the adapter-owned Copilot SDK
	// conversation rather than a serialized payload.
	copilotStateMode = "ref"

	// copilotStateSchema is the version tag of the state shape: the ref
	// payload is the Copilot SDK session id as a UTF-8 string. It travels
	// with every saved state so a restore can reject a mismatched shape
	// instead of misinterpreting the bytes.
	copilotStateSchema = "copilot.session.ref.v1"

	// copilotStateGranularity declares turn-boundary checkpoints. Copilot
	// develop turns run tens of minutes, so per-turn checkpoints are what
	// make a post-stop resume meaningful (CRI-202 ships per-turn in v1).
	copilotStateGranularity = "per-turn"

	// copilotStateSchemaVersion is the numeric schema version carried in
	// Snapshot responses and validated on Restore: v1 is the session-id
	// token described by copilotStateSchema.
	copilotStateSchemaVersion uint32 = 1
)

// stateDeclaration is the StateDescriptor Info() serves (CRI-201/CRI-206).
// max_bytes stays 0 (host default) — a ref token is a single short string.
func stateDeclaration() *v2.StateDescriptor {
	return &v2.StateDescriptor{
		Mode:        copilotStateMode,
		Schema:      copilotStateSchema,
		Granularity: copilotStateGranularity,
	}
}

// Snapshot returns the adapter's checkpoint state: the current Copilot SDK
// session id (the address of the live conversation). The host stores the
// token, never the payload.
func (p *copilotAdapter) Snapshot(_ context.Context, req *v2.SnapshotRequest) (*v2.SnapshotResponse, error) {
	s := p.getSession(req.GetSessionId())
	if s == nil {
		return nil, status.Errorf(codes.NotFound,
			"copilot: snapshot requested for unknown session %q (open it first)",
			req.GetSessionId())
	}
	if sess := s.currentSession(); sess != nil {
		if token := strings.TrimSpace(sess.SessionID()); token != "" {
			return &v2.SnapshotResponse{
				State:         []byte(token),
				SchemaVersion: copilotStateSchemaVersion,
			}, nil
		}
	}
	return nil, status.Errorf(codes.Internal,
		"copilot: no live Copilot conversation attached to session %q; nothing to checkpoint",
		req.GetSessionId())
}

// Restore reattaches the Copilot session to the exact address the host
// checkpointed. The state payload is the session id the adapter itself
// returned from Snapshot — byte-exact lookup on it, never ranked/fuzzy.
//
// Failure is loud and terminal: the error propagates to the host, which
// fails the run; the adapter does NOT fall back to a fresh session. A
// silently-fresh-start after a failed reattach is the wrong-state failure
// class the StateDescriptor contract exists to prevent (see package
// doc). Fallback to a fresh session happens only via an explicit logged
// decision elsewhere (CRI-272's same-process reopen path), never here.
func (p *copilotAdapter) Restore(ctx context.Context, req *v2.RestoreRequest) (*v2.RestoreResponse, error) {
	sessionID := req.GetSessionId()
	s := p.getSession(sessionID)
	if s == nil {
		return nil, status.Errorf(codes.NotFound,
			"copilot: restore requested for unknown session %q (open it first)", sessionID)
	}

	if have := req.GetSchemaVersion(); have != copilotStateSchemaVersion {
		st := status.Newf(codes.FailedPrecondition,
			"copilot: snapshot taken at v%d, adapter speaks v%d only — refusing to restore",
			have, copilotStateSchemaVersion)
		detail := &v2.SnapshotVersionMismatch{
			Have: have,
			Want: copilotStateSchemaVersion,
		}
		if withDetails, derr := st.WithDetails(detail); derr == nil {
			return nil, withDetails.Err()
		}
		return nil, st.Err()
	}

	token := strings.TrimSpace(string(req.GetState()))
	if token == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"copilot: restore state for session %q is empty; refusing to guess at the reattach address",
			sessionID)
	}

	// Exact-address equivalence: the open path already reattached to this
	// address (CRI-272 persisted-ID resume). Byte-equal token, same address —
	// a second resume of the same conversation is skipped, not approximated.
	if sess := s.currentSession(); sess != nil && strings.TrimSpace(sess.SessionID()) == token {
		persistSDKSessionID(sessionID, token)
		slog.Info("copilot: restore reattach verified",
			"adapterSession", sessionID, "sdkSession", token)
		return &v2.RestoreResponse{}, nil
	}

	client, err := p.ensureClient(ctx, s.secrets)
	if err != nil {
		slog.Error("copilot: no usable copilot runtime for state reattach; failing the restore loudly",
			"adapterSession", sessionID, "sdkSession", token, "err", err)
		return nil, fmt.Errorf("copilot: reattach session %q to %q: %w", sessionID, token, err)
	}
	sess, err := client.ResumeSessionWithOptions(ctx, token, s.resumeConfig)
	if err != nil {
		slog.Error(
			"copilot: state reattach failed; failing the restore loudly — no fresh-session fallback "+
				"(a silently-fresh start restores into a plausible-looking but semantically foreign session)",
			"adapterSession", sessionID, "sdkSession", token, "err", err)
		return nil, fmt.Errorf("copilot: reattach session %q: %w", sessionID, err)
	}

	s.swapSession(sess)
	persistSDKSessionID(sessionID, token)
	slog.Info("copilot: state reattached to checkpointed conversation",
		"adapterSession", sessionID, "sdkSession", token)
	return &v2.RestoreResponse{}, nil
}

// Pause and Resume acknowledge the host's pause lifecycle. The copilot
// harness has no mid-turn quiesce primitive; the engine drains nested tool
// calls and gates permission work host-side, and per-turn checkpointing
// (CRI-202) captures the state ref while the session is active by design —
// the token read is atomic, so a torn read is impossible. Pausing an
// already-paused session is a host-side no-op, so an ACK without side
// effects keeps the pause-path checkpoint save (explicit-pause saves are an
// adapter's expected behavior) alive.
func (p *copilotAdapter) Pause(_ context.Context, _ *v2.PauseRequest) (*v2.PauseResponse, error) {
	return &v2.PauseResponse{}, nil
}

func (p *copilotAdapter) Resume(_ context.Context, _ *v2.ResumeRequest) (*v2.ResumeResponse, error) {
	return &v2.ResumeResponse{}, nil
}