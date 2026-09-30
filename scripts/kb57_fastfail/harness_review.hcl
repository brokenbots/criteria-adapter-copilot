# kb57_fastfail_review.hcl — KB-57 fast-fail e2e harness
#
# Reproduces the CRI-260 scoped-reviewer defect in minutes instead of a 40m
# develop run: one review step with the CRI-260-shaped scoped read-only
# allow-list (plain git reads under the bash kind the copilot adapter reports),
# a prompt directing the agent to run exactly those reads, and the outcome
# sentinel.
#
# RED expected on the unfixed stack: the CLI's shell permission requests carry
# identifier-only fingerprints AND the engine's permission sink rebuilds
# request Details with only full_command_text, so the adapter's merged
# details["command"] fingerprint is dropped and no subcommand pattern ever
# matches -> permission.denied until the turn budget burns -> step failure.
# GREEN expected once fingerprints reach the policy matcher end to end.

variable "gh_token" {
  type    = string
  secret  = true
}

workflow {
  name = "kb57_fastfail_review"
  version       = "1"
  initial_state = "review"
  target_state  = "done"
  policy {
    max_total_steps = 5
  }
}

adapter "copilot" "reviewer" {
  config {
    max_turns = 4
  }
  secrets {
    GH_TOKEN = var.gh_token
  }
}

step "review" {
  target = adapter.copilot.reviewer
  allow_tools = [
    "bash:git",
    "bash:git status",
    "bash:git status *",
    "bash:git log",
    "bash:git log *",
    "bash:git diff",
    "bash:git diff *",
    "shell:git",
    "shell:git status",
    "shell:git status *",
    "shell:git log",
    "shell:git log *",
    "shell:git diff",
    "shell:git diff *",
  ]
  input {
    prompt = <<-EOT
      You are acting as a code reviewer. The current directory is a git
      worktree. Use the bash tool to run EXACTLY these two commands, one at a
      time, as plain single commands (no cd, no &&, no -C, no sh -c):

        git status --short
        git log --oneline -3

      Do not use any other tool. Then write one sentence summarizing the
      change state, and end your final line with exactly:
      RESULT: success
    EOT
  }

  outcome "success" { next = state.done }
  outcome "failure" { next = state.failed }
}

state "done" {
  terminal = true
  success  = true
}

state "failed" {
  terminal = true
}