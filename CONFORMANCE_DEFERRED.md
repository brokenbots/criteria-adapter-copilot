# Deferred: conformance_test.go

`conformance_test.go` runs the Criteria host's shared conformance suite against
the copilot binary. It imports the host's internal test harness
(`github.com/brokenbots/criteria/internal/adapter/conformance` and
`.../internal/adapterhost`), which is not importable from this standalone
module, so it cannot build on `main`.

It is preserved here until the conformance harness is published as a consumable
package (so adapter repos can self-test). Until then, the host runs this suite
against the published OCI artifact. Unit tests + the fake-copilot fixture on
`main` provide standalone coverage.

Provenance: monorepo `cmd/criteria-adapter-copilot/conformance_test.go`,
preserved 2026-06-05 during the WS36 extraction.
