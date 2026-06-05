// copilot_util.go — event-construction helpers shared across the copilot adapter.

package main

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// resultEvent constructs the terminal ExecuteEvent for a step. The Outputs map
// always carries `outcome` and `reason` keys so downstream workflow expressions
// can read `steps.<name>.outcome` and `steps.<name>.reason` consistently across
// success and failure paths. `reason` is empty when the model did not call
// `submit_outcome` with one (e.g. permission denial, reprompt exhaustion,
// max_turns).
func resultEvent(outcome, reason string) *v2.ExecuteEvent {
	// outcome/reason are plain strings, so JSON marshaling cannot fail.
	ev, _ := v2.NewExecuteResultEvent(outcome, map[string]any{
		"outcome": outcome,
		"reason":  reason,
	})
	return ev
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
