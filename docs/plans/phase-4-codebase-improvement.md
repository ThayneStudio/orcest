# Phase 4: Codebase Improvement

## Prerequisites

Phase 3 complete (full task pipeline working).

## Scope

Use idle time to proactively analyze the codebase and surface improvement opportunities. Orcest runs parallel Claude subagent analyses across configurable categories, creates GitHub issues for findings, and rate-limits output to avoid overwhelming maintainers. Created issues intentionally do NOT carry the `ai-ready` label -- they require human triage before Orcest acts on them.

## Key Features

### Idle-Time Detection

- Monitor the task queue: if no tasks (PR management, issue implementation) for N minutes (configurable, default: 30), trigger improvement analysis
- Re-check the queue before starting each analysis category -- abort if real work arrives
- Track last analysis run time to enforce cooldown between runs (default: 24 hours)

### Parallel Claude Subagent Analysis

Run independent Claude sessions across categories:

- **Security** -- scan for hardcoded secrets, insecure defaults, missing input validation, dependency vulnerabilities
- **Tech debt** -- identify code duplication, overly complex functions, outdated patterns, TODO/FIXME/HACK comments worth addressing
- **Test coverage** -- find untested code paths, suggest missing edge case tests, identify tests that are too tightly coupled to implementation
- **Performance** -- flag N+1 queries, unnecessary re-renders, missing indexes, inefficient algorithms
- **Accessibility** -- check for missing ARIA labels, keyboard navigation gaps, color contrast issues (web projects only)

Each category runs as an independent subagent with a category-specific prompt and file selection strategy.

### GitHub Issue Creation

- Create one issue per finding, with a clear title and description
- Label with `orcest:improvement` and the category (e.g., `orcest:security`, `orcest:tech-debt`)
- Do NOT add the `ai-ready` label -- findings require human review before implementation
- Include relevant code snippets and file references in the issue body
- Add a severity estimate: `low`, `medium`, `high`

### Rate Limiting

- Max N issues created per day per repo (configurable, default: 5)
- Cooldown between analysis runs (configurable, default: 24 hours)
- Per-category caps to avoid flooding with one type of finding (default: 2 per category per run)
- If the daily cap is hit, queue remaining findings for the next day

### Deduplication

- Before creating an issue, search existing open issues for similar titles and content
- Use a similarity heuristic: title keyword overlap + file path overlap
- If a similar issue exists, skip creation and log the duplicate
- Also check recently closed issues to avoid re-raising fixed problems

### Configurable Analysis Categories and Prompts

- Repo-level config (`.orcest/improvements.yml` or similar) to:
  - Enable/disable specific categories
  - Override default prompts per category
  - Set file inclusion/exclusion patterns per category
  - Adjust severity thresholds for issue creation
- Sensible defaults if no config file exists

## Notes

- This is the lowest-priority phase and the most speculative. The value depends heavily on Claude's ability to surface non-obvious issues rather than noise.
- The deduplication heuristic does not need to be perfect. Occasional duplicates are acceptable; missing real findings is worse than creating a duplicate.
- Rate limiting is critical for adoption. If Orcest floods a repo with low-quality improvement issues, maintainers will turn it off entirely.
- The parallel subagent approach means each category can have a tailored file selection strategy (e.g., security scans auth modules first, performance scans hot paths). This is where most of the tuning effort will go.
- Consider adding a feedback loop: if a human closes an improvement issue as `wontfix`, learn to deprioritize similar findings in future runs.
