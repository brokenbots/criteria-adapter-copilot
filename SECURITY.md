# Security

## Reporting a vulnerability

Please report security issues privately via GitHub's **"Report a vulnerability"**
flow (Security → Advisories) on this repository, or email security@brokenbots.net.
Do not open a public issue for an undisclosed vulnerability.

## Supply-chain controls

This adapter ships as a **signed, multi-platform OCI artifact**
(`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`), keyless-signed
via Sigstore/Fulcio with a Rekor transparency-log entry, published by
[`brokenbots/publish-adapter`](https://github.com/brokenbots/publish-adapter).
Consumers can pin the signer in `.criteria.lock.hcl` (`criteria adapter lock`)
so `apply`/`pull` enforce the signature.

Dependency hygiene is enforced in CI and documented in
[docs/dependency-policy.md](docs/dependency-policy.md):

- **`osv-scan`** — osv-scanner runs on every PR/push; no shipping known
  vulnerabilities. Exceptions are documented + dated in
  [`osv-scanner.toml`](osv-scanner.toml).
- **`deps-report`** — non-blocking freshness report (latest major.minor target).
- **Dependabot** — routine minor/patch updates with a 7-day supply-chain cooldown
  (security fixes exempt).

Reproduce the CI security checks locally with `make vuln-scan` and
`make deps-outdated`.
