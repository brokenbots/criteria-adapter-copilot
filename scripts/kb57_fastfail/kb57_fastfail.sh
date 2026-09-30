#!/usr/bin/env bash
# kb57_fastfail.sh — KB-57 fast-fail e2e harness driver (runs on the HOST,
# ~15s). Lives in the adapter repo: scripts/kb57_fastfail/.
#
# What it asserts: with the CRI-260-shaped scoped read-only reviewer policy
# (allow_tools naming plain git reads under the bash kind the copilot adapter
# reports), a real engine->adapter->CLI turn can actually EXECUTE
# `git status --short` and `git log --oneline -3` — i.e. full-text command
# fingerprints reach the engine's policy matcher end to end.
#
# RED on a broken stack: permission.denied on every native shell request
# (identifier-only fingerprints), the turn budget burns, the step ends
# needs_review => run FAILED in seconds (exit 1). Never burn a 40m develop
# run on this failure class again — this loop goes red in seconds.
# GREEN on a fixed stack: permission.granted "shell:git status *" and
# "shell:git log *", step success, exit 0.
#
# Prerequisites:
#   - github.com/brokenbots/criteria cloned at ../../criteria (or
#     $CRITERIA_BIN set); its bin/criteria built (make build)
#   - Copilot CLI 1.0.82 reachable (PATH or $CRITERIA_COPILOT_BIN) — the
#     version the live adapter images bake; other versions shift the
#     permission payload shape
#   - A GitHub token for CLI auth: $GH_TOKEN env (or GitHub CLI auth state)
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CRITERIA_BIN="${CRITERIA_BIN:-$(cd "$REPO_ROOT/../criteria" 2>/dev/null && pwd)/bin/criteria}"
ADAPTER_BIN="${1:-$REPO_ROOT/criteria-adapter-copilot}"
if [ ! -x "$CRITERIA_BIN" ]; then
  echo "FATAL: criteria engine binary not found at $CRITERIA_BIN (build the sibling criteria repo or set CRITERIA_BIN)" >&2
  exit 2
fi
WORK="$(mktemp -d /tmp/kb57.XXXXXX)"
LOG="$SCRIPT_DIR/last_run.ndjson"
rm -f "$LOG" "$SCRIPT_DIR/last_stdout.json"
export CRITERIA_COPILOT_BIN="${CRITERIA_COPILOT_BIN:-$(command -v copilot)}"
export CRITERIA_COPILOT_INCLUDE_SENSITIVE_PERMISSION_DETAILS=1
if [ -z "${GH_TOKEN:-}" ] && [ -f "$SCRIPT_DIR/.gh_token" ]; then
  export GH_TOKEN="$(cat "$SCRIPT_DIR/.gh_token")"
fi
if [ -z "${GH_TOKEN:-}" ] && ! gh auth status >/dev/null 2>&1; then
  echo "FATAL: no GH_TOKEN and no gh auth state; the Copilot CLI cannot authenticate" >&2
  exit 2
fi

# Adapter resolution: by-name install path via $CRITERIA_ADAPTERS.
if [ ! -f "$ADAPTER_BIN" ]; then
  echo "FATAL: adapter binary not found: $ADAPTER_BIN (build it first: CGO_ENABLED=0 go build -o criteria-adapter-copilot .)" >&2
  exit 2
fi
mkdir -p "$SCRIPT_DIR/adapters"
cp -f "$ADAPTER_BIN" "$SCRIPT_DIR/adapters/criteria-adapter-copilot"
export CRITERIA_ADAPTERS="$SCRIPT_DIR/adapters"

mkdir -p "$WORK/tree"
cd "$WORK/tree"
if [ ! -d .git ]; then
  git init -q .
  git config user.email h@example.com; git config user.name h
  printf 'hello\n' > README.md
  git add README.md; git commit -qm "harness: seed commit"
fi

echo "== workdir: $WORK"
echo "== adapter: $ADAPTER_BIN"
timeout 280 "$CRITERIA_BIN" apply "$SCRIPT_DIR/harness_review.hcl" \
  --var "gh_token=env:GH_TOKEN" \
  --events-file "$LOG" \
  --output json > "$SCRIPT_DIR/last_stdout.json" 2>&1
RC=$?
echo "== apply rc=$RC (events: $LOG)"
DENIED=$(grep -c permission.denied "$LOG" 2>/dev/null); DENIED="${DENIED:-0}"
GRANTED=$(grep -c permission.granted "$LOG" 2>/dev/null); GRANTED="${GRANTED:-0}"
echo "denied: $DENIED  granted: $GRANTED"
if [ "$RC" -eq 0 ] && [ "$GRANTED" -ge 2 ] && [ "$DENIED" -eq 0 ]; then
  echo "PASS: scoped reviewer policy admitted the plain git reads"
  exit 0
fi
echo "FAIL: expected the scoped policy to admit plain git reads (see $LOG)"
grep -m1 -o '"reason":"[^"]*"' "$LOG" | head -1
exit 1