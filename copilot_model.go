// copilot_model.go — per-step model and reasoning_effort override helpers.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	copilot "github.com/github/copilot-sdk/go"
)

// applyRequestModel applies a per-request model override if cfg["model"] is set.
func applyRequestModel(ctx context.Context, session copilotSession, cfg map[string]string) error {
	model := strings.TrimSpace(cfg["model"])
	if model == "" {
		return nil
	}
	var opts *copilot.SetModelOptions
	if effort := strings.TrimSpace(cfg["reasoning_effort"]); effort != "" {
		opts = &copilot.SetModelOptions{ReasoningEffort: &effort}
	}
	if err := session.SetModel(ctx, model, opts); err != nil {
		return fmt.Errorf("copilot: set model %q: %w", model, err)
	}
	return nil
}

// applyRequestEffort applies a per-step reasoning_effort override, returning a
// restore function that resets the effort to the agent-level default when called.
// When cfg["model"] is also set, applyRequestModel covers the combined SetModel call;
// the restore is still needed so the default effort is reinstated after the step.
// SetModel with modelId="" applies only the effort change, keeping the current model.
func applyRequestEffort(ctx context.Context, s *sessionState, session copilotSession, cfg map[string]string) (func(), error) {
	effort := strings.TrimSpace(cfg["reasoning_effort"])
	if effort == "" {
		return func() {}, nil
	}

	if err := validateReasoningEffort(effort); err != nil {
		return nil, err
	}

	// Skip the forward apply when cfg also specifies a model; applyRequestModel
	// (called after this) will handle the combined SetModel call. The restore is
	// still registered so the default effort is reinstated after the step.
	if strings.TrimSpace(cfg["model"]) == "" {
		if err := session.SetModel(ctx, "", &copilot.SetModelOptions{ReasoningEffort: &effort}); err != nil {
			return nil, fmt.Errorf("copilot: set per-step reasoning_effort %q: %w", effort, err)
		}
	}

	restore := func() {
		defaultModel := s.defaultModel
		defaultEffort := s.defaultEffort
		var opts *copilot.SetModelOptions
		if defaultEffort != "" {
			opts = &copilot.SetModelOptions{ReasoningEffort: &defaultEffort}
		}
		// Restore default effort; SetModel errors are best-effort (step already completed).
		if err := session.SetModel(ctx, defaultModel, opts); err != nil {
			slog.Warn("copilot: restore per-step reasoning_effort failed", "error", err)
		}
	}
	return restore, nil
}

// validateReasoningEffort returns an error when effort is not in the documented set.
func validateReasoningEffort(effort string) error {
	if !validReasoningEfforts[effort] {
		return fmt.Errorf("copilot: reasoning_effort %q is not valid; valid values: low, medium, high, xhigh", effort)
	}
	return nil
}
