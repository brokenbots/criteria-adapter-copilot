// copilot_util.go — event-construction helpers shared across the copilot adapter.

package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

const redactedPlaceholder = "[REDACTED]"

// resultEvent constructs the terminal ExecuteEvent for a step. The Outputs map
// always carries `outcome` and `reason` keys so downstream workflow expressions
// can read `steps.<name>.outcome` and `steps.<name>.reason` consistently across
// success and failure paths. `reason` is empty when the model did not call
// `submit_outcome` with one (e.g. permission denial, reprompt exhaustion,
// max_turns).
//
// If any adapter-held secrets are present in `reason`, they are replaced with
// redactedPlaceholder before the output is emitted. This is best-effort
// hygiene for values the adapter received over the secret channel; it does
// not claim to remove all sensitive material.
func resultEvent(outcome, reason string, secrets ...string) *v2.ExecuteEvent {
	cleaned := redactSecrets(reason, secrets)
	// outcome/reason are plain strings, so JSON marshaling cannot fail.
	ev, _ := v2.NewExecuteResultEvent(outcome, map[string]any{
		"outcome": outcome,
		"reason":  cleaned,
	})
	return ev
}

// redactSecrets replaces every non-empty adapter-held secret value that appears
// verbatim in `reason` with a visible placeholder. Secrets are replaced in
// descending length order so a shorter token cannot corrupt a longer one.
//
// This is intentionally narrow: it only redacts values the adapter actually
// holds (delivered over the secret channel). It does not attempt general
// secret detection, PII scrubbing, or entropy heuristics, because those
// approaches mangle legitimate agent prose and create a false sense of safety.
func redactSecrets(reason string, secrets []string) string {
	if reason == "" || len(secrets) == 0 {
		return reason
	}

	// Work on a copy so the sort does not mutate the caller's slice.
	ordered := make([]string, 0, len(secrets))
	for _, s := range secrets {
		if t := strings.TrimSpace(s); t != "" {
			ordered = append(ordered, t)
		}
	}
	if len(ordered) == 0 {
		return reason
	}

	sort.Slice(ordered, func(i, j int) bool {
		return len(ordered[i]) > len(ordered[j])
	})

	out := reason
	for _, secret := range ordered {
		out = strings.ReplaceAll(out, secret, redactedPlaceholder)
	}
	return out
}

func adapterEvent(kind string, data map[string]any) *v2.ExecuteEvent {
	s, err := structpb.NewStruct(data)
	if err != nil {
		// Encoding failed; emit a minimal struct so the event kind is preserved
		// and the encode error is diagnosable rather than silently dropped.
		s, _ = structpb.NewStruct(map[string]any{"_encode_error": err.Error()})
	}
	return &v2.ExecuteEvent{
		Event: &v2.ExecuteEvent_Adapter{
			Adapter: &v2.AdapterEvent{
				EventKind: kind,
				Payload:   s,
			},
		},
	}
}

func stringifyAny(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
