# Transient Failure Retry for Worker Timeouts and Merge Network Errors

## Problem

Six PRs in dewdropsllc/asemly were labeled `orcest:needs-human` for failures that don't actually require human intervention: worker timeouts (4 PRs), a TLS handshake error during `gh merge` (1 PR), and a git auth failure mid-task (1 PR). The system already has a transient failure mechanism (`[transient]` prefix in task result summaries), but these failure types aren't classified as transient.

## Changes

### 1. Mark all runner failures as transient (worker side)

**File:** `src/orcest/worker/loop.py` (lines 751-781)

When converting a `RunnerResult` to a `TaskResult`, if `status == ResultStatus.FAILED` (i.e., `success=False` and not `usage_exhausted`), prepend `TRANSIENT_SUMMARY_PREFIX` to the summary.

**Why this is safe:** Every `ClaudeResult(success=False)` path in `claude_runner.py` is an infrastructure failure — timeout, subprocess crash, D-state hang, lock loss, or process creation failure. When Claude runs to completion (even if it couldn't fix the code), the CLI exits 0, producing `success=True`. There is no path where `success=False` represents "Claude tried and couldn't do it."

**Existing safeguards:** The `total_attempts` circuit breaker (default 50, 30-day TTL) is incremented at enqueue time and NOT cleared by transient retries. This prevents infinite retry loops for persistent infrastructure problems. The per-SHA attempt counter IS cleared on transient failure, allowing re-enqueue on the next poll cycle.

**Covers:** Worker timeouts (#505, #536, #550, #599), runner crashes, mid-task git failures (#568 — Claude exits non-zero when git fetch fails, producing `success=False`).

### 2. Detect network errors in the merge handler (orchestrator side)

**File:** `src/orcest/orchestrator/loop.py` (lines 285-369)

When `gh.merge_pr()` fails and the error is NOT a merge conflict, check the error message against network error patterns. If it matches, skip the `needs-human` label and comment — let the PR be rediscovered on the next poll cycle.

**Network error patterns** (tailored to Go HTTP errors from the `gh` CLI):

```python
_MERGE_NETWORK_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in [
        r"timed?\s*out",
        r"ETIMEDOUT",
        r"connection reset",
        r"ECONNRESET",
        r"ECONNREFUSED",
        r"dial tcp",
        r"TLS handshake",
        r"socket hang up",
        r"no such host",
        r"i/o timeout",
        r"network is unreachable",
    ]
]
```

**Why the retry works without counter changes:** Merge attempts do NOT increment per-SHA or total attempt counters (those are only incremented in `_publish_and_notify` for task enqueue). The PR will naturally re-enter the `MERGE` path on the next poll cycle since it's still APPROVED + CI green.

**Merge retry counter:** Add a Redis key `pr:{repo}:{number}:merge_retries` (TTL 1 hour) incremented on each network-error merge failure. After 5 retries, fall through to the existing `needs-human` path. This prevents unbounded retries for persistent network issues.

**Exclude `GhRateLimitError`:** Check `isinstance(e, GhRateLimitError)` before pattern matching. `_run_gh()` already exhausts its own retry budget for rate limits (3 retries with 30/60/120s backoff). If it still raises, the problem is persistent.

**Logging:** Log network-error merge failures at WARNING level with a "will retry on next poll cycle" message, instead of ERROR.

**Covers:** TLS handshake timeout (#497), and any future network blips during merge.

### What we're NOT changing

**Exception handler default in `worker/loop.py` (line 802):** We considered flipping the default so all non-`WorkspaceError(transient=False)` exceptions are transient. Review found this has minimal benefit — the only non-`WorkspaceError` exceptions reaching this handler are genuine code bugs, not the infra failures we're targeting. The mid-task git auth case (#568) is already covered by change #1 (Claude exits non-zero → `success=False` → transient).

## Files to modify

| File | Change |
|------|--------|
| `src/orcest/worker/loop.py` | Prepend `TRANSIENT_SUMMARY_PREFIX` for all `ResultStatus.FAILED` from runner |
| `src/orcest/orchestrator/loop.py` | Add network pattern matching in merge handler, merge retry counter |
| `src/orcest/orchestrator/pr_ops.py` | Add `get_merge_retry_count` / `increment_merge_retries` helpers (or add to `loop.py` if small) |
| `tests/worker/test_loop.py` | Update/add tests for transient runner failure classification |
| `tests/orchestrator/test_loop.py` | Add tests for network-error merge retry and merge retry counter exhaustion |

## Verification

```bash
make test-unit
```

Test cases to add:
- Runner timeout produces transient task result
- Runner crash (all retries exhausted) produces transient task result
- Runner usage exhaustion is NOT marked transient (already separate status)
- Merge network error skips `needs-human` label
- Merge network error increments retry counter
- Merge retry counter exhaustion falls through to `needs-human`
- Merge conflict is NOT classified as network error
- `GhRateLimitError` is NOT classified as network error
- Non-network merge error still labels `needs-human`
