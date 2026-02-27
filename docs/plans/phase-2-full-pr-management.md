# Phase 2: Full PR Management

## Prerequisites

Phase 1 complete (core orchestrator + worker loop working).

## Scope

Mature the PR lifecycle from "monitor and report" to fully autonomous management. After this phase, Orcest handles CI failures, review feedback, merge conflicts, and auto-merge without human intervention for the happy path.

## Key Features

### CI Triage Escalation Chain

Four-tier escalation for CI failures:

1. **Heuristic** -- pattern-match known transient failures (network timeouts, flaky test signatures, registry 503s). If matched, retry immediately without burning a Claude call.
2. **CLASSIFY_CI** -- lightweight Claude call with the failure log. Outputs one of: `transient`, `fixable`, `needs_human`. Cheap and fast; exists to keep FIX_CI costs down.
3. **FIX_CI** -- full Claude session. Gets the failure log, relevant source files, and a fix prompt. Pushes a commit to the PR branch.
4. **HUMAN_NEEDED** -- if FIX_CI fails or CLASSIFY_CI says `needs_human`, add `orcest:needs-human` label and post a comment summarizing the failure.

### Auto-Merge

Conditions for automatic merge:

- All CI checks green
- No unresolved review comments
- Branch rebased on latest `main` (no merge commits)
- No `orcest:blocked` or `orcest:needs-human` labels

Merge strategy: rebase-merge (configurable per repo).

### Review Feedback Parsing

- Poll for new review comments on managed PRs
- Detect actionable comments (requested changes, nit fixes, questions needing code changes)
- Create `FIX_PR` tasks in the work queue with the comment context
- Track which comments have been addressed to avoid duplicate work

### Auto-Retry Transient CI Failures

- On CI failure, check the heuristic tier first
- If transient, run `gh run rerun --failed` (rerun only the failed jobs)
- Cap retries at 2 per PR per workflow run to avoid infinite loops

### Merge Conflict Detection

- After any push to `main`, check all open managed PRs for conflicts
- If conflict detected: post a comment explaining the conflict, add `orcest:blocked` label
- Create a `REBASE_PR` task in the work queue

### PR Priority Ordering

- Default ordering: oldest PR first (FIFO)
- Priority boost for PRs that are one step from mergeable (e.g., CI passing but needs rebase)
- Deprioritize PRs with `orcest:blocked` label
- Configurable priority weights

## Notes

- The escalation chain is the most complex piece. Start with heuristic + HUMAN_NEEDED, then layer in CLASSIFY_CI and FIX_CI.
- Auto-merge needs a safety latch: a repo-level config flag to disable it, and a per-PR `orcest:no-auto-merge` label override.
- Review feedback parsing doesn't need to be perfect. False negatives (missed comments) are fine initially; false positives (unnecessary fix attempts) are more costly.
- `gh run rerun --failed` only works for GitHub Actions. If repos use other CI, the retry mechanism needs to be pluggable.
