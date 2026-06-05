# criteria-adapter-copilot

A [Criteria](https://github.com/brokenbots/criteria) adapter that drives **GitHub
Copilot** (via the [`copilot-sdk/go`](https://github.com/github/copilot-sdk))
over the v2 adapter protocol. It is an out-of-process plugin binary built on the
[Go adapter SDK](https://github.com/brokenbots/criteria-go-adapter-sdk) and the
[wire contract](https://github.com/brokenbots/criteria-adapter-proto).

It supports multi-turn sessions, the bidi permission stream (blocking tool
gating), per-step reasoning-effort overrides, custom/BYOK providers, and a
`submit_outcome` tool for structured results.

## Authentication (secret channel)

The GitHub token is delivered over the Criteria **secret channel** (D69), never
read from the process environment. Declare it in your workflow:

```hcl
adapter "copilot" "default" {
  secrets {
    GITHUB_TOKEN = ...   # or COPILOT_GITHUB_TOKEN / GH_TOKEN
  }
}
```

The adapter declares `COPILOT_GITHUB_TOKEN`, `GH_TOKEN`, and `GITHUB_TOKEN` in
its manifest (precedence in that order) and **fails closed** with a clear error
if none is supplied.

## Build

```bash
go build -o bin/criteria-adapter-copilot .
```

The compiled binary requires the GitHub Copilot CLI at runtime (`copilot` on
`PATH`, or set `CRITERIA_COPILOT_BIN`).

## Test

```bash
go test ./...
```

Tests run against a deterministic fake Copilot CLI (`testfixtures/fake-copilot`),
so no real CLI or network access is required. The host-driven conformance suite
lives on the [`deferred/conformance`](../../tree/deferred/conformance) branch
(it depends on the Criteria host's internal test harness and cannot build
standalone yet).

## Publish

Tagging `vX.Y.Z` runs `.github/workflows/publish.yml`, which builds the binary
and publishes it as an OCI artifact to
`ghcr.io/brokenbots/criteria-adapter-copilot:X.Y.Z` via the reusable
[`brokenbots/publish-adapter`](https://github.com/brokenbots/publish-adapter)
action.

## License

Apache-2.0. See [LICENSE](LICENSE).
