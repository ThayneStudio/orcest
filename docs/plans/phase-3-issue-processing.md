# Phase 3: Issue Processing

## Prerequisites

Phase 2 complete (mature PR management for the PRs that issue work creates).

## Scope

Turn GitHub issues into working pull requests. Orcest picks up issues labeled `ai-ready`, resolves implementation order via dependency analysis, runs Claude to implement, self-reviews, and opens PRs. The Phase 2 PR management pipeline then takes over.

## Key Features

### Issue Discovery

- Poll for issues with the `ai-ready` label on configured repos
- Filter out issues already tracked in Redis (prevent duplicate work)
- Respect rate limits: configurable max concurrent implementations

### Dependency Resolution + Topological Sort

- Port the dependency resolution logic from the existing `ralph.sh` in bbr-platform
- Parse issue references (`depends on #123`, `blocked by #456`) to build a dependency graph
- Topological sort to determine implementation order
- Skip issues whose dependencies are still open

### Claude Implementation

- Create branch: `orcest/<issue-number>-<slug>` (slug derived from issue title, lowercased, truncated)
- Assemble a Claude prompt with: issue body, relevant file context, repo conventions
- Run Claude in a sandboxed worktree
- Push the resulting commits to the branch

### Self-Review Cycle

- After implementation, run a separate Claude review pass on the diff
- If the review finds issues, iterate: feed the review comments back to Claude for a fix pass
- Cap self-review iterations (default: 2) to avoid infinite loops
- Only open the PR once self-review passes or iteration cap is hit

### Branch Naming Conventions

- Format: `orcest/<issue-number>-<slug>`
- Slug: first 40 chars of the issue title, lowercased, non-alphanumeric replaced with hyphens, trailing hyphens stripped
- Example: issue #42 "Add rate limiting to API endpoints" becomes `orcest/42-add-rate-limiting-to-api-endpoints`

### Issue State Tracking in Redis

- Track per-issue state: `pending`, `implementing`, `in_review`, `pr_open`, `failed`
- Store implementation attempt count to detect repeated failures
- Store branch name, PR number (once created), and last error
- TTL on failed states so issues can be retried after cooldown

### PR Creation

- After self-review passes, open a PR via `gh pr create`
- PR body references the source issue (`Closes #N`)
- Add `orcest:managed` label so Phase 2 pipeline picks it up
- Link the PR back to the Redis issue state

## Notes

- The dependency resolution port from `ralph.sh` is the riskiest piece. That script has implicit assumptions about the bbr-platform repo structure. Need to generalize it.
- Self-review quality depends heavily on the review prompt. Start with a simple "does this implementation match the issue requirements" check and iterate.
- Branch naming collisions are possible if an issue is retried after failure. Handle by appending `-attempt-N` suffix on retries.
- The implementation Claude session needs repo context (file tree, conventions, test patterns). How much context to include is a tuning problem -- start broad, narrow based on token costs.
