# criteria-adapter-copilot

A [Criteria](https://github.com/brokenbots/criteria) adapter that drives **GitHub
Copilot** (via the [`copilot-sdk/go`](https://github.com/github/copilot-sdk))
over the v2 adapter protocol. It is an out-of-process plugin binary built on the
[Go adapter SDK](https://github.com/brokenbots/criteria-go-adapter-sdk) and the
[wire contract](https://github.com/brokenbots/criteria-adapter-proto).

It supports multi-turn sessions, the bidi permission stream (blocking tool
gating), per-step reasoning-effort overrides, custom/BYOK providers, and a
`submit_outcome` tool for structured results.

## Install

Published as a signed, multi-platform OCI artifact
(`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`). Pin and lock it:

```bash
criteria adapter lock <workflow-dir>
```

The compiled binary requires the GitHub Copilot CLI at runtime (`copilot` on
`PATH`, or set `CRITERIA_COPILOT_BIN`).

## Authentication (secret channel)

The GitHub token is delivered over the Criteria **secret channel** (D69), never
read from the process environment. The adapter declares `COPILOT_GITHUB_TOKEN`,
`GH_TOKEN`, and `GITHUB_TOKEN` (precedence in that order) and **fails closed**
with a clear error if none is supplied.

```hcl
adapter "copilot" "default" {
  secrets {
    GITHUB_TOKEN = ...   # or COPILOT_GITHUB_TOKEN / GH_TOKEN
  }
}
```

## Setup (adapter configuration)

Set session-wide defaults in the adapter `config {}` block. All keys are
optional; for BYOK mode, `provider_base_url` requires `model`.

| Config key | Type | Description |
| --- | --- | --- |
| `model` | string | Copilot model for the session (required in BYOK mode). |
| `reasoning_effort` | string | Default effort: `low`, `medium`, `high`, `xhigh`. |
| `working_directory` | string | Working directory for tool invocations. |
| `max_turns` | number | Max assistant turns per Execute (default: unlimited). |
| `system_prompt` | string | System prompt prepended at session open. |
| `provider_type` | string | BYOK provider: `openai` (default), `azure`, `anthropic`. |
| `provider_base_url` | string | OpenAI-compatible endpoint; setting it enables BYOK (e.g. `http://localhost:11434/v1`). Requires `model`. |
| `provider_api_key` | string | BYOK API key (optional for local providers). Prefer `env()`. |
| `provider_bearer_token` | string | Sets `Authorization` directly; precedence over `provider_api_key`. |
| `provider_wire_api` | string | `completions` (default) or `responses` (openai/azure). |
| `provider_azure_api_version` | string | Azure API version (default `2024-10-21`). |

```hcl
adapter "copilot" "coordinator" {
  config {
    model             = "minimax-m2.7:cloud"
    system_prompt     = file("./agents/coordinator.md")
    provider_base_url = "http://localhost:11434/v1"
    provider_wire_api = "responses"
  }
  secrets { GITHUB_TOKEN = ... }
}
```

## Step inputs

| Input | Required | Description |
| --- | --- | --- |
| `prompt` | **yes** | User prompt to send to the assistant. |
| `max_turns` | no | Per-step override of the session `max_turns`. |
| `reasoning_effort` | no | Per-step override; resets to session default after the step. `low`/`medium`/`high`/`xhigh`. |

```hcl
step "plan" {
  adapter = adapter.copilot.coordinator
  input {
    prompt           = "Draft the migration plan."
    reasoning_effort = "high"
  }
}
```

## Config overrides

`max_turns` and `reasoning_effort` exist in **both** the adapter `config {}`
(session default) and step `input {}` (per-step override). A step input wins for
that step only; `reasoning_effort` then resets to the session default. All other
config keys are session-scoped and not overridable per step.

## Outputs

Results are emitted as **structured events** (`structured_events` capability) —
assistant messages, tool calls/results gated through the permission stream, and a
final outcome submitted via the `submit_outcome` tool (`success` / `failure`
with an optional message). There is no flat output-key schema.

## Build & test

```bash
make build
make test   # runs against the deterministic fake CLI in testfixtures/fake-copilot
```

The host-driven conformance suite lives on the
[`deferred/conformance`](../../tree/deferred/conformance) branch.

## Security & dependencies

See [SECURITY.md](SECURITY.md) and [docs/dependency-policy.md](docs/dependency-policy.md).
Reproduce the CI security checks locally:

```bash
make vuln-scan      # osv-scanner — known-vulnerability gate (WS49)
make deps-outdated  # go-mod-outdated — freshness report (WS50)
make deps-majors    # gomajor — available major (/vN) upgrades
```

## Publish

Tagging `vX.Y.Z` runs [`.github/workflows/publish.yml`](.github/workflows/publish.yml),
which cross-builds all four platforms and publishes them as a single
multi-platform, signed OCI artifact to
`ghcr.io/brokenbots/criteria-adapter-copilot:X.Y.Z` via the reusable
[`brokenbots/publish-adapter`](https://github.com/brokenbots/publish-adapter)
action.

## License

Apache-2.0. See [LICENSE](LICENSE).
