# Dependency-freshness & supply-chain policy

This adapter follows the same two locked mandates as the
[Criteria monorepo](https://github.com/brokenbots/criteria/blob/main/docs/dependency-policy.md).
Each adapter repo owns its own copy of the policy; this file is the local
authority. It applies to every ecosystem we vendor: this Go module and the
GitHub Actions used in CI.

## 1. Stay current — latest major.minor

Be on the **latest major and minor** of every dependency. Patch versions roll up
freely *within* the cooldown rule below.

The only reason to pin **below** latest is a concrete one:

- a newer version has a **known security vulnerability** that affects us, or
- a newer version carries a **bug we are actually hit by**.

Any such pin is a documented, dated exception — see
[Holding a dependency below latest](#holding-a-dependency-below-latest).

## 2. Defend against supply-chain attacks — 7-day cooldown

Do **not** adopt any release **newer than 7 days** unless it fixes a known
security issue or a specific bug we're hit by. A freshly-published (and possibly
compromised) release gets a cooldown window before we ingest it.

**Security updates bypass the cooldown.** Availability of a fix outranks the
supply-chain wait, so security-update PRs (Dependabot's security lane) are not
delayed.

## How "latest" is determined — Go tooling, not Dependabot

Dependabot is **not** the source of truth for freshness. It is slow, and it
cannot drive Go **major** upgrades: in Go a major bump is a *module-path change*
(`.../foo` → `.../foo/v2`) plus call-site edits, which neither Dependabot nor a
plain `go get -u` performs. Dependabot is demoted to the routine minor/patch lane
(see below); the freshness picture and major upgrades are driven by Go tooling,
version-pinned in the [`Makefile`](../Makefile) (no floating `@latest`):

| Command | Tool | Answers |
| --- | --- | --- |
| `make deps-outdated` | [`go-mod-outdated`](https://github.com/psampaz/go-mod-outdated) | Which **direct** deps are behind their latest minor/patch. |
| `make deps-majors` | [`gomajor`](https://github.com/icholy/gomajor) | Which **major** (`/vN`) upgrades are available. |
| `make vuln-scan` | [`osv-scanner`](https://github.com/google/osv-scanner) | Which deps carry a known advisory (WS49). |

> The monorepo pins these tools in a dedicated `tools/go.mod`. A single-module
> adapter pins them by version in the `Makefile` instead — same guarantee (no
> `@latest`), less ceremony.

A non-blocking `deps-report` CI job runs `make deps-outdated` on every PR and
posts the result to the job summary, so drift is visible without flaking the
build. Enforcement of "latest" stays with review, not a hard gate — upstream
release cadence would make a hard gate flap.

Applying the upgrades:

- **Patch/minor:** `go get <module>@<version>` (honor the 7-day cooldown).
- **Major:** `gomajor get <module>@latest`, which rewrites the `/vN` module path
  and import sites; absorb any remaining breaking API changes in source.

## The update bot — Dependabot (routine minor/patch lane)

[`.github/dependabot.yml`](../.github/dependabot.yml) is configured to:

- cover this Go module plus the `github-actions` ecosystem;
- **not** ignore `semver-major` updates (majors it raises are *signals* — drive
  them with `gomajor`);
- apply a **7-day cooldown** (`cooldown: default-days: 7`); security updates are
  exempt by Dependabot's design;
- group minor + patch updates to keep PR volume sane.

## Holding a dependency below latest

To pin a dependency below its latest version, record it as a dated exception so
the decision is auditable and re-reviewed — mirroring the `osv-scanner.toml`
"documented + dated" convention. Add an entry to the table below **and** the
matching `ignore` constraint in `.github/dependabot.yml`, citing the advisory or
bug id and a review date.

| Dependency | Held at | Reason (advisory / bug) | Review by |
| --- | --- | --- | --- |
| _none_ | | | |

On the review date the exception must be cleared or re-justified.
