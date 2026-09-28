// copilot_credential_guard.go — tracker credential isolation for the CLI child
// (KB-43). The workflow talks to trackers; the developer LLM must not. A
// develop run moved Linear issue CRI-201 from New to In Review mid-turn
// because the spawned CLI child had tracker credentials in scope (via the
// inherited process environment) and the model used them to "track its work".
// The guard is enforcement, not prompting: known tracker credential handles
// are stripped from the environment the Copilot runtime is spawned with, so
// the CLI child receives no tracker write credential under a covered name.
// This is a curated deny-list, not a guarantee against arbitrary host-injected
// credential handles — the durable posture (adapter pods only receive the
// secrets the adapter declares) is tracked as an architecture review item.

package main

import "strings"

// trackerCredentialEnvNames is the deny-list of tracker credential environment
// variable names the CLI child must never receive. Matching is exact (env
// names are case-sensitive and conventionally uppercase); Linear and Kanboard
// credentials are additionally matched by the prefix rules below, which cover
// the names this deployment actually injects (KANBOARD_APP_TOKEN,
// KANBOARD_URL) and their variants. Extend the list when a new tracker
// credential name is introduced — never by broad pattern.
var trackerCredentialEnvNames = []string{
	// Linear — the tracker these develop runs drive (CRI-201 moved New → In
	// Review mid-turn on an inherited API key). Every LINEAR_* variable is a
	// tracker credential in this child and none is a legitimate dependency of
	// the Copilot CLI.
	"LINEAR_API_KEY",
	"LINEAR_API_KEY_FILE",
	"LINEAR_API_TOKEN",
	"LINEAR_ACCESS_TOKEN",
	"LINEAR_OAUTH_TOKEN",
	"LINEAR_OAUTH_ACCESS_TOKEN",
	"LINEAR_OAUTH_REFRESH_TOKEN",
	"LINEAR_REFRESH_TOKEN",
	// Kanboard — the injected handles for the Kanboard workflow family are
	// KANBOARD_APP_TOKEN (the X-API-Key credential) and KANBOARD_URL (the
	// tracker endpoint); the exact variants below are covered by the prefix
	// rule and listed for documentation.
	"KANBOARD_APP_TOKEN",
	"KANBOARD_URL",
	"KANBOARD_API_TOKEN",
	"KANBOARD_API_KEY",
	// Other trackers the workflow family touches, same posture.
	"JIRA_API_TOKEN",
	"JIRA_PERSONAL_ACCESS_TOKEN",
	"ASANA_ACCESS_TOKEN",
	"ASANA_PERSONAL_ACCESS_TOKEN",
}

// linearCredentialEnvPrefix and kanboardCredentialEnvPrefix are the
// tracker-specific prefix rules: any environment variable named LINEAR_* or
// KANBOARD_* is treated as a tracker credential. The Copilot CLI child has no
// legitimate Linear or Kanboard dependency, so prefix matching (unlike the
// exact list, which must be curated) cannot over-block a needed variable.
const (
	linearCredentialEnvPrefix   = "LINEAR_"
	kanboardCredentialEnvPrefix = "KANBOARD_"
)

// isTrackerCredentialEnvName reports whether name carries tracker credentials
// that must never reach the CLI child.
func isTrackerCredentialEnvName(name string) bool {
	if strings.HasPrefix(name, linearCredentialEnvPrefix) || strings.HasPrefix(name, kanboardCredentialEnvPrefix) {
		return true
	}
	for _, denied := range trackerCredentialEnvNames {
		if name == denied {
			return true
		}
	}
	return false
}

// scrubTrackerCredentials returns env with every tracker credential entry
// removed. It is applied to the CLI child environment in both auth modes so
// mutation-scope tracker credentials inherited from the adapter process
// environment are dropped, while the documented Copilot auth fallback
// (GH_TOKEN / GITHUB_TOKEN / COPILOT_SDK_AUTH_TOKEN) and the workflow's
// GitHub credential handles (WORKFLOW_GITHUB_TOKEN and friends, which the
// git credential helper reads) keep working.
func scrubTrackerCredentials(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if isTrackerCredentialEnvName(name) {
			continue
		}
		out = append(out, entry)
	}
	return out
}
