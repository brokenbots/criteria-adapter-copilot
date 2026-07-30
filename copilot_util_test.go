// copilot_util_test.go — unit tests for event-construction helpers.

package main

import (
	"strings"
	"testing"
)

// TestAdapterEventEncodeErrorFallback verifies that adapterEvent does not
// silently drop an event when structpb.NewStruct fails.  Channels are not
// JSON-serialisable, so passing one triggers the encode-error path.  The
// returned event must still carry the correct kind and expose an
// _encode_error field so the caller can diagnose the problem.
func TestAdapterEventEncodeErrorFallback(t *testing.T) {
	data := map[string]any{"ch": make(chan int)}
	evt := adapterEvent("test.kind", data)

	adap := evt.GetAdapter()
	if adap == nil {
		t.Fatal("expected non-nil adapter event")
	}
	if adap.EventKind != "test.kind" {
		t.Fatalf("kind=%q want test.kind", adap.EventKind)
	}
	if adap.Payload == nil {
		t.Fatal("expected non-nil Payload on encode error")
	}
	fields := adap.Payload.GetFields()
	errField, ok := fields["_encode_error"]
	if !ok {
		t.Fatalf("expected _encode_error field in fallback struct, got fields: %v", fields)
	}
	if errField.GetStringValue() == "" {
		t.Fatal("_encode_error must contain a non-empty error description")
	}
}

// TestRedactSecrets_ReplacesHeldSecrets verifies that every non-empty secret is
// replaced with a visible placeholder, including multiple occurrences.
func TestRedactSecrets_ReplacesHeldSecrets(t *testing.T) {
	secret := "ghp_abc123_super_secret"
	got := redactSecrets("token: ghp_abc123_super_secret and again ghp_abc123_super_secret", []string{secret})
	want := "token: [REDACTED] and again [REDACTED]"
	if got != want {
		t.Errorf("redactSecrets = %q, want %q", got, want)
	}
}

// TestRedactSecrets_PassthroughWithoutSecrets verifies that ordinary prose is
// returned byte-identical when no held secret appears in it.
func TestRedactSecrets_PassthroughWithoutSecrets(t *testing.T) {
	reason := "I reviewed the code and all checks passed."
	got := redactSecrets(reason, []string{"ghp_unrelated"})
	if got != reason {
		t.Errorf("redactSecrets altered clean reason:\n  got:  %q\n  want: %q", got, reason)
	}
}

// TestRedactSecrets_SkipsEmptySecrets verifies that empty or whitespace-only
// secret values are ignored rather than corrupting output.
func TestRedactSecrets_SkipsEmptySecrets(t *testing.T) {
	reason := "nothing to hide"
	got := redactSecrets(reason, []string{"", "   "})
	if got != reason {
		t.Errorf("redactSecrets = %q, want %q", got, reason)
	}
}

// TestRedactSecrets_LongestFirst verifies that when one secret is a substring
// of another, the longer secret is replaced first so its placeholder is not
// corrupted by the shorter match.
func TestRedactSecrets_LongestFirst(t *testing.T) {
	short := "abc"
	long := "abcdef"
	got := redactSecrets("x abc def abcdef y", []string{short, long})
	want := "x [REDACTED] def [REDACTED] y"
	if got != want {
		t.Errorf("redactSecrets = %q, want %q", got, want)
	}
	if strings.Contains(got, long) {
		t.Errorf("redactSecrets left longer secret %q in output", long)
	}
}
