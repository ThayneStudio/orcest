# Issue Dependencies

Orcest automatically defers `orcest:ready` issues that have still-open
prerequisites, so that downstream issues don't get implemented before
their blockers land. Two dependency sources are checked:

1. **GitHub-native "blocked by" relationships** — set via the
   *Relationships* section in the GitHub issue sidebar (or the
   `addBlockedBy` API). Cross-repo blockers are supported.
2. **Body-declared references** — plain-text patterns like
   `blocked by #N` in the issue body (same-repo only).

You don't need to apply the `orcest:blocked` label by hand — declare
the dependency either way and orcest will pick it up. If *either*
source has an open blocker, the issue is deferred.

## GitHub-native dependencies

Native blocked-by relationships arrive inline with the issue listing
(the `blockedBy` GraphQL connection), so checking them costs no extra
API calls. GitHub caps native dependencies at 50 per issue, so the
single-page fetch is always complete.

A native blocker defers the dependent while it is open; closed native
blockers don't block. A blocker whose state can't be read fails safe
to blocking. Because native relationships carry the blocker's repo,
cross-repo dependencies work — they're logged as `owner/repo#N`.

One caveat: a native blocker in a repo the orchestrator's token
**cannot access** is silently omitted from the API response (GitHub
returns no signal it exists), so it cannot defer the dependent. Give
the token read access to blocker repos, or fall back to manual
`orcest:blocked`.

## Body-declared reference syntax

Body text is parsed case-insensitively. The following patterns are
treated as prerequisite references:

| Pattern                                     | Example                              |
| ------------------------------------------- | ------------------------------------ |
| `blocked by #N`                             | `Blocked by #42`                     |
| `depends on #N`                             | `Depends on #7 landing first.`       |
| `requires #N`                               | `Requires #99 to merge.`             |
| `prerequisite[s]` `:` or whitespace + `#N`  | `Prerequisites: #15, #16`            |
| `after #N <verb>`                           | `Pick this up after #5 merges.`      |
| Unchecked task-list item                    | `- [ ] #101`                         |

The `<verb>` suffix after `after #N` must be one of `merges`, `lands`,
`closes`, `ships`, or `is done`. This avoids matching phrases like
"after 5pm" or "see #5 after we discuss."

### What is NOT treated as a dependency

| Pattern                            | Reason                                     |
| ---------------------------------- | ------------------------------------------ |
| `Closes #N` / `Fixes #N` / `Resolves #N` | These are PR *outputs*, not prerequisites. |
| Bare `#N` mentions                 | Too noisy — must be prefixed by one of the patterns above. |
| References inside ` ``` ` fenced code blocks | Pasted logs / transcripts often mention unrelated issue numbers. |
| Checked task-list item `- [x] #N`  | The author has marked it done.             |
| Cross-repo `owner/repo#N`          | Not supported in body text — use a native blocked-by relationship instead. |

## How deferral works

Once per discovery cycle (per project), the orchestrator:

1. Lists open issues with the `orcest:ready` label.
2. For each candidate that survives the cheaper filters (terminal
   labels, Redis lock, attempt budget, queue presence), first checks
   the native `blockedBy` data fetched with the listing. Any open
   native blocker defers the issue immediately — no further lookups
   are spent on it.
3. Otherwise parses the body for blocker references and looks each one
   up via `gh issue view --json state`. A per-cycle cache means N
   dependent issues sharing one blocker cost one `gh` call, not N, and
   states already known from native data seed the cache for free.
4. Classifies each body-declared blocker:
   - **open** — the dependent is deferred (`SKIP_DEPENDENCY`).
   - **closed** — the blocker is satisfied; not blocking.
   - **missing** — the referenced issue doesn't exist (deleted /
     wrong number); not blocking.
   - **unknown** — the `gh` lookup failed transiently (rate-limit,
     network). The dependent is deferred — fail-safe: a spuriously
     deferred issue self-corrects next cycle, but a spuriously
     enqueued one can ship a broken PR.

Deferred issues stay labeled `orcest:ready`. Nothing on GitHub
indicates the deferral (no comment, no label change). The next
discovery tick (~30s) re-evaluates everything, so when the blocker
closes the dependent flows through automatically.

To trace a deferral, search the orchestrator container logs for
`deferred, waiting on open blocker`:

```bash
ssh root@<proxmox-host>
ssh orcest@<orchestrator-vm>
docker logs orcest-<project>-orchestrator-1 2>&1 | grep -E 'Issue #[0-9]+: deferred'
```

## Limits and safety guards

- **Per-issue ref cap**: at most 32 distinct refs are parsed from one
  body. Beyond that, additional patterns are ignored.
- **Digit-length cap**: numeric refs longer than 7 digits are
  dropped. Real GitHub issue numbers fit comfortably.
- **No recursion**: orcest only checks direct blockers, not blockers'
  blockers. If you need a chain (`A → B → C`), each link is evaluated
  independently and `C` will simply re-defer until both `A` and `B`
  are closed.
- **No cross-repo refs in body text** (native relationships handle
  cross-repo).
- **No comment scanning** — only native relationships and the issue
  body are read.

## When to fall back to manual `orcest:blocked`

Apply `orcest:blocked` yourself when:

- You want to block on something that isn't a GitHub issue at all
  (a release window, a stakeholder decision, an external migration).
- You want orcest to stop touching the issue regardless of body text.

`orcest:blocked` is treated as a terminal label and is not
auto-removed when blockers close — you have to take it off yourself.

## How to extend

Adding a new reference pattern is one line in
`_BLOCKER_PATTERNS` in `src/orcest/orchestrator/issue_deps.py`, plus a
test in `tests/orchestrator/test_issue_deps.py`. The cascade does not
need to change.

Adding a *new resolution state* (e.g. "draft" for issues that have
been opened but not yet labeled `orcest:ready`) requires:

1. A return path in `gh.get_issue_state` for the new state.
2. A decision in `open_blockers` about whether the state is blocking.
3. Tests covering the new branch.

## Implementation pointers

| Concern                      | File                                              |
| ---------------------------- | ------------------------------------------------- |
| Parser + resolver (body + native) | `src/orcest/orchestrator/issue_deps.py`      |
| `gh issue view` wrapper      | `src/orcest/orchestrator/gh.py` (`get_issue_state`) |
| Native `blockedBy` fetch     | `src/orcest/orchestrator/gh.py` (`list_labeled_issues`) |
| Cascade integration          | `src/orcest/orchestrator/issue_ops.py`            |
| Log line on deferral         | `src/orcest/orchestrator/loop.py` (search `SKIP_DEPENDENCY`) |
| Tests                        | `tests/orchestrator/test_issue_deps.py`, `tests/orchestrator/test_issue_ops.py`, `tests/orchestrator/test_gh.py` |
