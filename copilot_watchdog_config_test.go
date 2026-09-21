// copilot_watchdog_config_test.go — CRI-277: the watchdog windows become
// per-session config (watchdog_window / watchdog_gate_window) while the
// shipped 90s/10m defaults and the CRI-277 healthy-gap guardrails hold.
//
// Data the guardrails encode (CRI-277, 15 runs / ~15K events): the worst
// inter-event gap on a healthy turn was 217s, the worst gate-held wait
// reached ~10m, and the smallest silent gap on a death run was 994s — so the
// base window must stay well under 994s and the gate ceiling at/above 10m or
// the watchdog either misses the documented death modes or false-stalls
// healthy build/test-heavy turns.
//
// The ticket's validation order (probe validated before wiring): the probe is
// sendRPCTimeboxed with an explicit window — TestSendRPCTimeboxedHonorsExplicitWindow
// proves it detects a live-but-silent session (and TestSendRPCTimeboxedPasses
// ThroughSuccessfulSend in copilot_watchdog_test.go proves a healthy one
// answers) before the per-session windows are threaded through the turn loop.

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	copilot "github.com/github/copilot-sdk/go"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// CRI-277 data bounds (see parseWatchdogSettings and the ticket analysis).
const (
	cri277MaxHealthyGap       = 217 * time.Second
	cri277MaxHealthyGatedWait = 10 * time.Minute
	cri277MinDeathGap         = 994 * time.Second
)

// TestWatchdogGuardrailsMatchHealthyGapData: the shipped windows and their
// config guardrails must keep the measured separation between healthy turns
// (max gap 217s, max gate-held wait ~10m) and death runs (min silence 994s).
// Acceptance #3: healthy gaps up to 217s + margin never trip the watchdog.
func TestWatchdogGuardrailsMatchHealthyGapData(t *testing.T) {
	// Shipped defaults are unchanged by the CRI-277 config surface.
	if watchdogWindow != 90*time.Second {
		t.Fatalf("shipped watchdogWindow = %s, want 90s", watchdogWindow)
	}
	if watchdogGateWindow != 10*time.Minute {
		t.Fatalf("shipped watchdogGateWindow = %v, want 10m", watchdogGateWindow)
	}

	// Gate ceiling covers the worst healthy gate-held wait, with the 2.7x
	// margin over the worst raw healthy gap.
	if watchdogGateWindow < cri277MaxHealthyGatedWait {
		t.Fatalf("gate window %v < worst healthy gate-held wait %v", watchdogGateWindow, cri277MaxHealthyGatedWait)
	}
	if margin := cri277MaxHealthyGap * 27 / 10; watchdogGateWindow < margin {
		t.Fatalf("gate window %v below the 2.7x margin over the worst healthy gap (%v)", watchdogGateWindow, margin)
	}

	// Base window stays far under the smallest death-run silence: it must
	// catch every documented death mode (well) before any run that died.
	if watchdogWindow >= cri277MinDeathGap/4 {
		t.Fatalf("window %v at/above a quarter of the smallest death-run silence (%v)", watchdogWindow, cri277MinDeathGap/4)
	}
	if watchdogWindow*4 >= cri277MinDeathGap {
		t.Fatalf("window %v (x4) reaches the smallest death-run silence %v", watchdogWindow, cri277MinDeathGap)
	}

	// The guardrail constants pin the measured data.
	if maxWatchdogWindow != cri277MinDeathGap {
		t.Fatalf("maxWatchdogWindow = %v, want %v", maxWatchdogWindow, cri277MinDeathGap)
	}
	if minWatchdogGateWindow != cri277MaxHealthyGatedWait {
		t.Fatalf("minWatchdogGateWindow = %v, want %v", minWatchdogGateWindow, cri277MaxHealthyGatedWait)
	}
	if watchdogWindow >= maxWatchdogWindow {
		t.Fatalf("shipped window %v violates its own ceiling %v", watchdogWindow, maxWatchdogWindow)
	}
	if watchdogGateWindow < minWatchdogGateWindow {
		t.Fatalf("shipped gate window %v violates its own floor %v", watchdogGateWindow, minWatchdogGateWindow)
	}

	// Zero settings resolve to the shipped defaults (the fallback path).
	if w := (watchdogSettings{}).resolved(); w.window != watchdogWindow || w.gateWindow != watchdogGateWindow {
		t.Fatalf("zero settings resolved to %+v, want the shipped defaults", w)
	}
}

// TestParseWatchdogSettings: the CRI-277 config keys parse as Go durations,
// unset/blank keys resolve to the shipped defaults, and every guardrail
// rejection names its key and the data-derived bound.
func TestParseWatchdogSettings(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]string
		want    watchdogSettings
		wantErr string // empty = must parse
	}{
		{"unset keys keep the shipped defaults", nil, watchdogSettings{window: watchdogWindow, gateWindow: watchdogGateWindow}, ""},
		{"blank keys keep the shipped defaults", map[string]string{watchdogWindowCfgKey: "   ", watchdogGateWindowCfgKey: ""}, watchdogSettings{window: watchdogWindow, gateWindow: watchdogGateWindow}, ""},
		{"shipped defaults accepted", map[string]string{watchdogWindowCfgKey: "90s", watchdogGateWindowCfgKey: "10m"}, watchdogSettings{window: 90 * time.Second, gateWindow: 10 * time.Minute}, ""},
		{"both keys parsed", map[string]string{watchdogWindowCfgKey: "2m", watchdogGateWindowCfgKey: "15m"}, watchdogSettings{window: 2 * time.Minute, gateWindow: 15 * time.Minute}, ""},
		{"seconds parsed", map[string]string{watchdogWindowCfgKey: "45s"}, watchdogSettings{window: 45 * time.Second, gateWindow: watchdogGateWindow}, ""},
		{"compound duration", map[string]string{watchdogWindowCfgKey: "2m30s", watchdogGateWindowCfgKey: "1h30m"}, watchdogSettings{window: 150 * time.Second, gateWindow: 90 * time.Minute}, ""},
		{"whitespace trimmed", map[string]string{watchdogWindowCfgKey: "  2m  "}, watchdogSettings{window: 2 * time.Minute, gateWindow: watchdogGateWindow}, ""},
		{"gate equal to window accepted", map[string]string{watchdogWindowCfgKey: "10m", watchdogGateWindowCfgKey: "10m"}, watchdogSettings{window: 10 * time.Minute, gateWindow: 10 * time.Minute}, ""},
		{"window just under ceiling accepted", map[string]string{watchdogWindowCfgKey: "993s", watchdogGateWindowCfgKey: "20m"}, watchdogSettings{window: 993 * time.Second, gateWindow: 20 * time.Minute}, ""},
		{"unparsable window", map[string]string{watchdogWindowCfgKey: "soon"}, watchdogSettings{}, "watchdog_window"},
		{"missing unit", map[string]string{watchdogWindowCfgKey: "90"}, watchdogSettings{}, "watchdog_window"},
		{"negative window", map[string]string{watchdogWindowCfgKey: "-1m"}, watchdogSettings{}, "must be positive"},
		{"zero window", map[string]string{watchdogWindowCfgKey: "0s"}, watchdogSettings{}, "must be positive"},
		{"unparsable gate", map[string]string{watchdogGateWindowCfgKey: "soon"}, watchdogSettings{}, "watchdog_gate_window"},
		{"negative gate", map[string]string{watchdogGateWindowCfgKey: "-5m"}, watchdogSettings{}, "must be positive"},
		{"zero gate", map[string]string{watchdogGateWindowCfgKey: "0s"}, watchdogSettings{}, "must be positive"},
		{"gate below floor", map[string]string{watchdogGateWindowCfgKey: "5m"}, watchdogSettings{}, "must stay at or above"},
		{"window at death-gap ceiling", map[string]string{watchdogWindowCfgKey: "16m34s"}, watchdogSettings{}, "must stay below"},
		{"window above death-gap ceiling", map[string]string{watchdogWindowCfgKey: "20m"}, watchdogSettings{}, "must stay below"},
		{"gate below window", map[string]string{watchdogWindowCfgKey: "15m", watchdogGateWindowCfgKey: "12m"}, watchdogSettings{}, "must not be below"},
		{"window error wins over gate error", map[string]string{watchdogWindowCfgKey: "soon", watchdogGateWindowCfgKey: "soon"}, watchdogSettings{}, "watchdog_window"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWatchdogSettings(tt.cfg)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseWatchdogSettings(%v) = %+v, want error containing %q", tt.cfg, got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseWatchdogSettings(%v) err = %v, want it to contain %q", tt.cfg, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseWatchdogSettings(%v) = %v, want %+v", tt.cfg, err, tt.want)
			}
			if got != tt.want {
				t.Fatalf("parseWatchdogSettings(%v) = %+v, want %+v", tt.cfg, got, tt.want)
			}
		})
	}
}

// TestInfoSchemaDeclaresWatchdogKeys: the CRI-277 keys are advertised as
// string (duration) config with a default-bearing description.
func TestInfoSchemaDeclaresWatchdogKeys(t *testing.T) {
	var a copilotAdapter
	info, err := a.Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	schema := info.GetConfigSchema().GetFields()
	if schema == nil {
		t.Fatal("Info config schema has no fields")
	}
	for _, key := range []string{watchdogWindowCfgKey, watchdogGateWindowCfgKey} {
		field, ok := schema[key]
		if !ok || field == nil {
			t.Fatalf("config schema missing %q", key)
		}
		if field.GetType() != "string" {
			t.Fatalf("config %s type = %q, want string (duration text)", key, field.GetType())
		}
		if field.GetDescription() == "" {
			t.Fatalf("config %s has no description", key)
		}
	}
	if want := "Default: 90s"; !strings.Contains(schema[watchdogWindowCfgKey].GetDescription(), want) {
		t.Fatalf("watchdog_window description %q missing %q", schema[watchdogWindowCfgKey].GetDescription(), want)
	}
	if want := "10m"; !strings.Contains(schema[watchdogGateWindowCfgKey].GetDescription(), want) {
		t.Fatalf("watchdog_gate_window description %q missing %q", schema[watchdogGateWindowCfgKey].GetDescription(), want)
	}
}

// TestOpenSessionAppliesWatchdogConfig: configured windows land on the
// sessionState and flow through the accessors; a session opened without the
// keys keeps the shipped defaults.
func TestOpenSessionAppliesWatchdogConfig(t *testing.T) {
	t.Setenv("CRITERIA_HOME", t.TempDir())
	p := newCopilotAdapter()
	fc := &fakeClient{}
	fc.handOut = &fakeSession{sessionID: "sdk-wd"}
	p.client = fc
	p.clientOptions = &copilot.ClientOptions{}

	if _, err := p.OpenSession(context.Background(), &v2.OpenSessionRequest{
		SessionId: "adapter-wd",
		Config: map[string]string{
			"model":                  "m",
			watchdogWindowCfgKey:     "2m",
			watchdogGateWindowCfgKey: "15m",
		},
	}); err != nil {
		t.Fatalf("OpenSession = %v", err)
	}
	s := p.sessions["adapter-wd"]
	if s == nil {
		t.Fatal("OpenSession did not register the session")
	}
	if s.watchdog.window != 2*time.Minute || s.watchdog.gateWindow != 15*time.Minute {
		t.Fatalf("session watchdog = %+v, want {2m 15m}", s.watchdog)
	}
	if got := s.watchdogWindow(); got != 2*time.Minute {
		t.Fatalf("watchdogWindow() = %v, want 2m", got)
	}
	if got := s.watchdogGateWindow(); got != 15*time.Minute {
		t.Fatalf("watchdogGateWindow() = %v, want 15m", got)
	}

	// A session opened without the keys resolves to the shipped defaults.
	fc.handOut = &fakeSession{sessionID: "sdk-wd2"}
	if _, err := p.OpenSession(context.Background(), &v2.OpenSessionRequest{
		SessionId: "adapter-wd2",
		Config:    map[string]string{"model": "m"},
	}); err != nil {
		t.Fatalf("second OpenSession = %v", err)
	}
	s2 := p.sessions["adapter-wd2"]
	if s2 == nil {
		t.Fatal("second OpenSession did not register")
	}
	if s2.watchdog.window != watchdogWindow || s2.watchdog.gateWindow != watchdogGateWindow {
		t.Fatalf("unconfigured session watchdog = %+v, want the shipped defaults %+v", s2.watchdog, watchdogSettings{window: watchdogWindow, gateWindow: watchdogGateWindow})
	}
	if got := s2.watchdogWindow(); got != watchdogWindow {
		t.Fatalf("unconfigured watchdogWindow() = %v, want the shipped %v", got, watchdogWindow)
	}
	if got := s2.watchdogGateWindow(); got != watchdogGateWindow {
		t.Fatalf("unconfigured watchdogGateWindow() = %v, want the shipped %v", got, watchdogGateWindow)
	}
}

// TestOpenSessionRejectsInvalidWatchdogConfig: a misconfigured window fails
// the open before any side effect — no CLI child start, no session registered.
func TestOpenSessionRejectsInvalidWatchdogConfig(t *testing.T) {
	t.Setenv("CRITERIA_HOME", t.TempDir())
	tests := []struct {
		name    string
		cfg     map[string]string
		wantErr string
	}{
		{"unparsable window", map[string]string{"model": "m", watchdogWindowCfgKey: "soon"}, "watchdog_window"},
		{"unparsable gate", map[string]string{"model": "m", watchdogGateWindowCfgKey: "soon"}, "watchdog_gate_window"},
		{"nonpositive gate", map[string]string{"model": "m", watchdogGateWindowCfgKey: "-5m"}, "must be positive"},
		{"window at death-gap ceiling", map[string]string{"model": "m", watchdogWindowCfgKey: "16m34s"}, "must stay below"},
		{"gate below floor", map[string]string{"model": "m", watchdogGateWindowCfgKey: "5m"}, "must stay at or above"},
		{"gate below window", map[string]string{"model": "m", watchdogWindowCfgKey: "15m", watchdogGateWindowCfgKey: "12m"}, "must not be below"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newCopilotAdapter()
			fc := &fakeClient{}
			fc.handOut = &fakeSession{sessionID: "sdk-bad"}
			p.client = fc
			p.clientOptions = &copilot.ClientOptions{}

			resp, err := p.OpenSession(context.Background(), &v2.OpenSessionRequest{
				SessionId: "adapter-bad",
				Config:    tt.cfg,
			})
			if err == nil {
				t.Fatalf("OpenSession(%v) = %v, want config rejection", tt.cfg, resp)
			}
			if resp != nil {
				t.Fatalf("OpenSession returned a response alongside the error: %v", resp)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
			if len(p.sessions) != 0 {
				t.Fatalf("sessions registered despite the invalid config: %d", len(p.sessions))
			}
			if fc.startCount != 0 {
				t.Fatalf("CLI child started %d time(s); the config must be validated before any side effect", fc.startCount)
			}
		})
	}
}

// TestWaitTurnSignalUsesConfiguredWindows: the inter-event window comes from
// the session config, not the package default. Package vars are parked at an
// hour; only the session-configured 20ms window makes the stall fire (and it
// fires with the stream-silent kind).
func TestWaitTurnSignalUsesConfiguredWindows(t *testing.T) {
	withFastWatchdog(t, time.Hour, time.Hour)
	s := &sessionState{session: &fakeSession{}, stallNotify: make(chan struct{}, 1)}
	s.watchdog = watchdogSettings{window: 20 * time.Millisecond}
	ts := newTurnState(0)
	s.markActivity()

	results := make(chan stallSignal, 1)
	go func() {
		signal, err := ts.waitTurnSignal(context.Background(), s)
		results <- stallSignal{signal, err}
	}()

	select {
	case sig := <-results:
		if sig.signal != turnSignalStall || !isProviderStallError(sig.err) {
			t.Fatalf("result = (%v, %v), want a provider stall", sig.signal, sig.err)
		}
		if !strings.Contains(sig.err.Error(), string(stallStream)) {
			t.Fatalf("stall = %v, want kind %q", sig.err, stallStream)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitTurnSignal ignored the session-configured window (still on the 1h package default)")
	}
}

// TestWaitTurnSignalGateUsesConfiguredCeiling: a gate-held wait survives the
// configured base window but is bound by the configured gate ceiling, and the
// fired stall is the gate-ceiling kind (proving the ceiling, not the base
// window, drove the branch).
func TestWaitTurnSignalGateUsesConfiguredCeiling(t *testing.T) {
	withFastWatchdog(t, time.Hour, time.Hour)
	s := &sessionState{session: &fakeSession{}, stallNotify: make(chan struct{}, 1)}
	s.watchdog = watchdogSettings{window: 20 * time.Millisecond, gateWindow: 200 * time.Millisecond}
	ts := newTurnState(0)
	s.markActivity()

	release := s.beginWatchdogGate()
	defer release()
	results := make(chan stallSignal, 1)
	go func() {
		signal, err := ts.waitTurnSignal(context.Background(), s)
		results <- stallSignal{signal, err}
	}()

	// 3× the base window with the gate held: the gate branch must still be
	// holding (and on the configured 200ms ceiling, not the 1h package var).
	time.Sleep(3 * 20 * time.Millisecond)
	select {
	case sig := <-results:
		t.Fatalf("waitTurnSignal returned while the gate was held: %v, %v", sig.signal, sig.err)
	default:
	}

	select {
	case sig := <-results:
		if sig.signal != turnSignalStall || !isProviderStallError(sig.err) {
			t.Fatalf("result = (%v, %v), want a provider stall", sig.signal, sig.err)
		}
		if !strings.Contains(sig.err.Error(), string(stallGateCeiling)) {
			t.Fatalf("stall = %v, want kind %q", sig.err, stallGateCeiling)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitTurnSignal ignored the session-configured gate ceiling")
	}
}

// TestSendRPCTimeboxedHonorsExplicitWindow: the probe is driven by its
// explicit window argument — a live-but-silent session (Send accepted, never
// answered) is detected within the probe window even when the package
// defaults are minutes large. This is the probe validation the ticket's
// validation order requires before the windows are wired into the turn loop.
func TestSendRPCTimeboxedHonorsExplicitWindow(t *testing.T) {
	withFastWatchdog(t, time.Minute, time.Minute)
	release := make(chan struct{})
	hung := &hungSendSession{fakeSession: &fakeSession{sessionID: "sdk-hung"}, release: release, hangFirst: 1 << 30}
	t.Cleanup(func() { close(release) })

	_, err := sendRPCTimeboxed(context.Background(), hung, &copilot.MessageOptions{Prompt: "hi"}, 20*time.Millisecond)
	if !isProviderStallError(err) {
		t.Fatalf("err = %v, want a provider stall", err)
	}
	if !strings.Contains(err.Error(), string(stallSendRPC)) {
		t.Fatalf("stall = %v, want kind %q", err, stallSendRPC)
	}
}

// TestSendWithRetryUsesConfiguredWindow: sendWithRetry — the composition of
// probe + CRI-272 restart/backoff — drives every attempt with the
// session-configured window. Package vars stay at a minute; only the
// configured 20ms window explains the exhausted attempt count.
func TestSendWithRetryUsesConfiguredWindow(t *testing.T) {
	withFastWatchdog(t, time.Minute, time.Minute)
	sleeps := withRetryRecorder(t)
	fc := &fakeClient{}
	p := withRecoverableClient(t, fc)

	release := make(chan struct{})
	hung := &hungSendSession{fakeSession: &fakeSession{sessionID: "sdk-hung"}, release: release, hangFirst: 1 << 30}
	t.Cleanup(func() { close(release) })
	fc.handOut = hung
	s := newWatchdogSession(t, p, "adapter-wd-cfg", hung)
	s.watchdog = watchdogSettings{window: 20 * time.Millisecond}

	_, err := s.sendWithRetry(context.Background(), &copilot.MessageOptions{Prompt: "hi"})
	if err == nil {
		t.Fatal("sendWithRetry must exhaust on persistent stalls")
	}
	if !isProviderStallError(err) {
		t.Fatalf("exhaustion error = %v, want it to stay stall-typed", err)
	}
	if got := hung.attempts(); got != retryMaxAttempts {
		t.Fatalf("send attempts = %d, want %d (the configured 20ms window must drive each attempt)", got, retryMaxAttempts)
	}
	if fc.stopCount != retryMaxAttempts-1 {
		t.Fatalf("stop count = %d, want %d (forced restart before each retry)", fc.stopCount, retryMaxAttempts-1)
	}
	if got := *sleeps; len(got) != retryMaxAttempts-1 || got[0] != time.Second || got[1] != 4*time.Second || got[2] != 16*time.Second {
		t.Fatalf("backoff sequence = %v, want [1s 4s 16s]", got)
	}
}
