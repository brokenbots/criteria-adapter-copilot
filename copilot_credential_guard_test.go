// copilot_credential_guard_test.go — KB-43: the developer LLM must never be
// able to mutate the tracker. The Copilot CLI child this adapter spawns must
// therefore have no tracker credentials in scope (enforcement, not prompting),
// and the session must not pick up undeclared tool side-channels (MCP servers
// discovered from the working directory) that could carry tracker-mutation
// tools. Covers the CRI-201 reproduction: a Linear API key inherited by the
// CLI child was used to move CRI-201 from New to In Review mid-turn.

package main

import (
	"context"
	"slices"
	"strings"
	"testing"

	copilot "github.com/github/copilot-sdk/go"
	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// TestScrubTrackerCredentialsRemovesTrackerCreds verifies the scrubber drops
// every tracker credential entry by name — exact-list names and the LINEAR_
// prefix rule — regardless of value (including empty values), while keeping
// the documented Copilot auth variables and ordinary environment intact.
func TestScrubTrackerCredentialsRemovesTrackerCreds(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"HOME=/home/agent",
		"GH_TOKEN=gh-token",
		"GITHUB_TOKEN=github-token",
		"COPILOT_GITHUB_TOKEN=copilot-token",
		"COPILOT_SDK_AUTH_TOKEN=sdk-token",
		"LINEAR_API_KEY=lin_key",
		"LINEAR_API_KEY=",
		"LINEAR_ACCESS_TOKEN=lin_oauth",
		"LINEAR_WEBHOOK_SECRET=whsec",
		"KANBOARD_API_TOKEN=kb_token",
		"JIRA_API_TOKEN=jira_token",
		"ASANA_ACCESS_TOKEN=asana_token",
		"AGENT_WORKSPACE=/work", // a non-credential name that merely shares a prefix must survive
	}

	got := scrubTrackerCredentials(env)

	for _, denied := range []string{"LINEAR_API_KEY", "LINEAR_ACCESS_TOKEN", "LINEAR_WEBHOOK_SECRET", "KANBOARD_API_TOKEN", "JIRA_API_TOKEN", "ASANA_ACCESS_TOKEN"} {
		if slices.ContainsFunc(got, func(s string) bool { return strings.HasPrefix(s, denied+"=") }) {
			t.Fatalf("scrubbed env must not contain %s; got %v", denied, got)
		}
	}
	for _, want := range env[:6] {
		if !slices.Contains(got, want) {
			t.Fatalf("scrubbed env must keep %q; got %v", want, got)
		}
	}
	if !slices.Contains(got, "AGENT_WORKSPACE=/work") {
		t.Fatalf("scrubbed env must keep AGENT_WORKSPACE (no prefix collision); got %v", got)
	}
}

// TestIsTrackerCredentialEnvName pins the matching contract: exact deny-list
// names plus the LINEAR_ prefix; unrelated names (including lowercase and
// similarly-prefixed names) are not tracker credentials.
func TestIsTrackerCredentialEnvName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"LINEAR_API_KEY", true},
		{"LINEAR_OAUTH_REFRESH_TOKEN", true},
		{"LINEAR_ANYTHING_AT_ALL", true},
		{"KANBOARD_API_TOKEN", true},
		{"JIRA_API_TOKEN", true},
		{"JIRA_PERSONAL_ACCESS_TOKEN", true},
		{"ASANA_ACCESS_TOKEN", true},
		{"ASANA_PERSONAL_ACCESS_TOKEN", true},
		{"PATH", false},
		{"GH_TOKEN", false},
		{"GITHUB_TOKEN", false},
		{"COPILOT_GITHUB_TOKEN", false},
		{"COPILOT_SDK_AUTH_TOKEN", false},
		{"linear_api_key", false}, // env names are case-sensitive; the child never sees lowercase variants
		{"SUPERLINEAR_API_KEY", false},
	}
	for _, tc := range cases {
		if got := isTrackerCredentialEnvName(tc.name); got != tc.want {
			t.Errorf("isTrackerCredentialEnvName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestApplyAuthOptionsChildEnvExcludesTrackerCredentials is the KB-43
// regression test for the credential channel: with Linear and Kanboard
// credentials in the adapter's process environment, the CLI child environment
// built by applyAuthOptions must carry none of them — in BOTH auth modes —
// while the Copilot auth surface (channel token, GH_TOKEN/GITHUB_TOKEN, PATH)
// keeps working.
func TestApplyAuthOptionsChildEnvExcludesTrackerCredentials(t *testing.T) {
	t.Setenv("LINEAR_API_KEY", "lin_key_inherited")
	t.Setenv("KANBOARD_API_TOKEN", "kb_token_inherited")
	t.Setenv("PATH", "/usr/bin")

	assertNoTrackerCreds := func(t *testing.T, env []string) {
		t.Helper()
		for _, denied := range []string{"LINEAR_API_KEY=", "KANBOARD_API_TOKEN="} {
			if slices.ContainsFunc(env, func(s string) bool { return strings.HasPrefix(s, denied) }) {
				t.Fatalf("CLI child env must not contain %q; got %v", denied, env)
			}
		}
	}

	t.Run("secret_channel_token_mode", func(t *testing.T) {
		sec := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), map[string]string{"GITHUB_TOKEN": "from-secret"})
		opts := &copilot.ClientOptions{}
		if err := applyAuthOptions(opts, sec); err != nil {
			t.Fatalf("applyAuthOptions() err = %v", err)
		}
		assertNoTrackerCreds(t, opts.Env)
		// Auth still works: the channel token is forwarded and PATH survives.
		if !slices.Contains(opts.Env, "GITHUB_TOKEN=from-secret") {
			t.Fatalf("Env must contain the delivered GITHUB_TOKEN; got %v", opts.Env)
		}
		if !slices.Contains(opts.Env, "PATH=/usr/bin") {
			t.Fatalf("Env must carry the process environment (PATH); got %v", opts.Env)
		}
	})

	t.Run("standard_auth_fallback_mode", func(t *testing.T) {
		t.Setenv("GITHUB_TOKEN", "from-env")
		sec := adapterhost.NewSecrets(declaredGitHubTokenSecrets(), nil)
		opts := &copilot.ClientOptions{}
		if err := applyAuthOptions(opts, sec); err != nil {
			t.Fatalf("applyAuthOptions() err = %v", err)
		}
		assertNoTrackerCreds(t, opts.Env)
		// The documented fallback auth env var survives the scrub.
		if !slices.Contains(opts.Env, "GITHUB_TOKEN=from-env") {
			t.Fatalf("Env must carry GH_TOKEN/GITHUB_TOKEN for the fallback; got %v", opts.Env)
		}
		if opts.UseLoggedInUser == nil || !*opts.UseLoggedInUser {
			t.Fatalf("UseLoggedInUser = %v, want explicit true on the fallback path", opts.UseLoggedInUser)
		}
	})
}

// TestSessionConfigHasNoTrackerToolSideChannels pins the tool-surface guard:
// the session declares exactly the two adapter tools, configures no MCP
// servers, and disables working-directory config discovery (KB-43) — so no
// undeclared tracker-mutation tool can reach the developer session.
func TestSessionConfigHasNoTrackerToolSideChannels(t *testing.T) {
	p := newCopilotAdapter()
	sc := p.buildSessionConfig(map[string]string{}, "kb43-session")

	if sc.EnableConfigDiscovery == nil || *sc.EnableConfigDiscovery {
		t.Fatalf("EnableConfigDiscovery = %v, want explicit false: discovered MCP servers are undeclared tool side-channels", sc.EnableConfigDiscovery)
	}
	if len(sc.MCPServers) != 0 {
		t.Fatalf("MCPServers = %v, want none: the adapter never declares MCP servers", sc.MCPServers)
	}
	toolNames := make([]string, 0, len(sc.Tools))
	for _, tool := range sc.Tools {
		toolNames = append(toolNames, tool.Name)
	}
	slices.Sort(toolNames)
	if want := []string{adapterToolToolName, submitOutcomeToolName}; !slices.Equal(toolNames, want) {
		t.Fatalf("declared tools = %v, want exactly %v", toolNames, want)
	}
}

// TestBuildResumeConfigPreservesConfigDiscovery verifies the resumed session
// keeps the KB-43 discovery posture across CLI-child restarts.
func TestBuildResumeConfigPreservesConfigDiscovery(t *testing.T) {
	p := newCopilotAdapter()
	sc := p.buildSessionConfig(map[string]string{}, "kb43-resume")
	resume := buildResumeConfig(sc)

	if resume.EnableConfigDiscovery == nil || *resume.EnableConfigDiscovery {
		t.Fatalf("resumed EnableConfigDiscovery = %v, want the session's explicit false", resume.EnableConfigDiscovery)
	}

	var nilConfig *copilot.SessionConfig
	if got := buildResumeConfig(nilConfig); got != nil {
		t.Fatalf("buildResumeConfig(nil) = %v, want nil", got)
	}
}

// TestInfoDeclaresExcludeTrackerScopes is the workflow-level declares guard:
// the adapter's InfoResponse.Secrets declaration is what allows the host to
// resolve and deliver credentials over the secret channel, so it must stay
// GitHub-token-only. A tracker-mutation scope (e.g. a Linear API key) declared
// here would make the developer session a tracker writer again.
func TestInfoDeclaresExcludeTrackerScopes(t *testing.T) {
	p := newCopilotAdapter()
	info, err := p.Info(t.Context(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info() err = %v", err)
	}

	for name := range info.GetSecrets() {
		if isTrackerCredentialEnvName(name) {
			t.Errorf("Info().Secrets declares tracker credential scope %q; the developer adapter must never declare tracker-mutation scopes", name)
		}
	}
	for _, name := range githubTokenSecretNames {
		if _, ok := info.GetSecrets()[name]; !ok {
			t.Errorf("Info().Secrets must keep the GitHub token scope %q (Copilot auth)", name)
		}
	}
	if len(info.GetSecrets()) != len(githubTokenSecretNames) {
		t.Fatalf("Info().Secrets = %v, want exactly the GitHub token scopes %v", info.GetSecrets(), githubTokenSecretNames)
	}
}

// TestOpenSessionChildEnvAndConfigAreTrackerFree is the end-to-end regression
// test at the OpenSession boundary: with tracker credentials present in the
// adapter's process environment AND delivered over the secret channel under an
// undeclared name, the spawned CLI child environment carries no tracker
// credentials and the session config keeps discovery disabled.
func TestOpenSessionChildEnvAndConfigAreTrackerFree(t *testing.T) {
	t.Setenv("LINEAR_API_KEY", "lin_key_inherited")
	t.Setenv("KANBOARD_API_TOKEN", "kb_token_inherited")

	var capturedOptions *copilot.ClientOptions
	origNew := newClientFn
	newClientFn = func(options *copilot.ClientOptions) copilotClient {
		capturedOptions = options
		return &fakeClient{}
	}
	t.Cleanup(func() { newClientFn = origNew })

	t.Setenv("CRITERIA_HOME", t.TempDir())
	p := newCopilotAdapter()
	// An undeclared tracker secret delivered over the secret channel: the
	// adapter never spawns secrets it did not declare, so this must not reach
	// the CLI child either.
	req := &v2.OpenSessionRequest{
		SessionId: "kb43-open",
		Secrets: map[string]string{
			"LINEAR_API_KEY": "lin_key_via_channel",
			"GITHUB_TOKEN":   "from-secret",
		},
	}
	if _, err := p.OpenSession(context.Background(), req); err != nil {
		t.Fatalf("OpenSession() err = %v", err)
	}

	if capturedOptions == nil {
		t.Fatal("newClientFn was not called; the CLI child options were not built")
	}
	for _, denied := range []string{"LINEAR_API_KEY=", "KANBOARD_API_TOKEN="} {
		if slices.ContainsFunc(capturedOptions.Env, func(s string) bool { return strings.HasPrefix(s, denied) }) {
			t.Fatalf("CLI child env must not contain %q; got %v", denied, capturedOptions.Env)
		}
	}
	if !slices.Contains(capturedOptions.Env, "GITHUB_TOKEN=from-secret") {
		t.Fatalf("CLI child env must keep the delivered GITHUB_TOKEN; got %v", capturedOptions.Env)
	}

	s := p.getSession("kb43-open")
	if s == nil {
		t.Fatal("OpenSession did not register the session")
	}
	if sc := s.sessionConfig; sc.EnableConfigDiscovery == nil || *sc.EnableConfigDiscovery {
		t.Fatalf("session config EnableConfigDiscovery = %v, want explicit false", sc.EnableConfigDiscovery)
	}
}