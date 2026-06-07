# Makefile — criteria-adapter-copilot
#
# Build/test plus the security & dependency-freshness tooling required by the
# Criteria supply-chain policy (mirrors the monorepo's WS49/WS50). See
# docs/dependency-policy.md for the rules these targets enforce.
#
# Tool versions are pinned here (no floating @latest) so CI and local runs
# resolve the SAME version — reproducibility and supply-chain safety. This
# single-module repo pins tools in the Makefile rather than a separate tools/
# go.mod (the monorepo's mechanism); bump these deliberately.

GO ?= go

OSV_SCANNER_VERSION     := v2.3.8
GO_MOD_OUTDATED_VERSION := v0.9.0
GOMAJOR_VERSION         := v0.15.0

.PHONY: help build test vet tidy lint vuln-scan deps-outdated deps-majors

help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

build: ## Build the adapter
	$(GO) build ./...

test: ## Run tests
	$(GO) test ./...

vet: ## go vet
	$(GO) vet ./...

tidy: ## go mod tidy
	$(GO) mod tidy

# --- Security gate (WS49) -----------------------------------------------------

vuln-scan: ## Scan for known vulnerabilities (osv-scanner; local parity with CI osv-scan)
	$(GO) run github.com/google/osv-scanner/v2/cmd/osv-scanner@$(OSV_SCANNER_VERSION) scan source -r .

# --- Dependency freshness (WS50) ---------------------------------------------
# The source of truth for "are we on latest major.minor", not Dependabot.

deps-outdated: ## Report direct deps behind their latest minor/patch (go-mod-outdated)
	$(GO) list -u -m -json all | $(GO) run github.com/psampaz/go-mod-outdated@$(GO_MOD_OUTDATED_VERSION) -update -direct

deps-majors: ## List available major-version (/vN) upgrades (gomajor); apply per dependency-policy
	$(GO) run github.com/icholy/gomajor@$(GOMAJOR_VERSION) list
