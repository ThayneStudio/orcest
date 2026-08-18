"""Issue dependency parsing and resolution.

Two dependency sources defer an issue while its blockers remain open:

1. GitHub-native blocked-by relationships (`native_open_blockers`) --
   states arrive inline with the issue listing, cross-repo supported.
2. Body-text prerequisite references ("blocked by #N", "depends on #N",
   etc.), resolved via per-blocker `gh` lookups.

Scope:
- Body-text references are same-repo `#N` only; cross-repo dependencies
  need a native relationship. Comments are not scanned.
- A native blocker the orchestrator token cannot see is silently omitted
  by the API (no signal it exists), so it cannot defer the dependent.
- No topological ordering: workers run in parallel and the next discovery
  tick re-evaluates everything, so deferral alone is sufficient.

`Closes #N` / `Fixes #N` / `Resolves #N` are deliberately ignored — they
describe what the issue's PR will *do*, not what the issue depends on.

Resolution states are one of:
- `"open"`    — blocker is open; defer the dependent issue
- `"closed"`  — blocker is closed; not blocking
- `"missing"` — blocker doesn't exist (deleted / wrong number); not blocking
- `"unknown"` — transient lookup failure (rate-limit, network); fail-safe
                to *blocking* so we don't ship a PR that depends on
                unverified state. Next discovery cycle re-checks.
"""

import logging
import re

from orcest.orchestrator import gh

logger = logging.getLogger(__name__)

# Per-body caps to bound CPU + downstream gh calls against pathological
# (or hallucinated) issue bodies. GitHub issue numbers are realistically
# ≤ 7 digits; >32 distinct refs in a single dependency declaration is
# almost certainly a bug or attack.
_MAX_REFS_PER_BODY = 32
_MAX_REF_DIGITS = 7

# Strip fenced code blocks before parsing so pasted error logs, command
# transcripts, and quoted prior bodies don't produce false-positive
# blockers. Inline code spans and blockquotes are left alone — body
# authors who want a reference *not* to count can put it in a fenced
# block.
_FENCED_CODE_RE = re.compile(r"```.*?```", re.DOTALL)

# Phrases that introduce a prerequisite reference. Each pattern captures
# one `#N` and the surrounding text is matched case-insensitively. Order
# doesn't matter; we union the matches.
_BLOCKER_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bblocked\s+by\s+#(\d+)", re.IGNORECASE),
    re.compile(r"\bdepends\s+on\s+#(\d+)", re.IGNORECASE),
    re.compile(r"\bprerequisite[s]?[:\s]+#(\d+)", re.IGNORECASE),
    re.compile(r"\brequires\s+#(\d+)", re.IGNORECASE),
    # Task-list checkbox referencing an issue, e.g. `- [ ] #123` or
    # `* [ ] #123`. Closed boxes (`- [x] #123`) are not blockers.
    re.compile(r"(?m)^\s*[-*]\s+\[\s\]\s+#(\d+)"),
    # "after #N <action-verb>" — narrow to verbs that imply issue closure
    # to avoid swallowing date phrases like "after 5pm".
    re.compile(
        r"\bafter\s+#(\d+)(?=\s+(?:merges?|lands?|closes?|ships?|is\s+done))",
        re.IGNORECASE,
    ),
)


def parse_blocker_refs(body: str) -> set[int]:
    """Extract the set of issue numbers this body declares as prerequisites.

    `Closes #N` / `Fixes #N` / `Resolves #N` are not included — those name
    the issue's *output*, not its dependencies.

    Refs are capped at `_MAX_REFS_PER_BODY` and digit lengths above
    `_MAX_REF_DIGITS` are dropped, both as DoS guards against pathological
    bodies. Fenced code blocks are stripped before matching so pasted
    logs / transcripts don't produce false positives.
    """
    if not body:
        return set()
    stripped = _FENCED_CODE_RE.sub("", body)
    refs: set[int] = set()
    for pattern in _BLOCKER_PATTERNS:
        for match in pattern.finditer(stripped):
            digits = match.group(1)
            if len(digits) > _MAX_REF_DIGITS:
                continue
            refs.add(int(digits))
            if len(refs) >= _MAX_REFS_PER_BODY:
                return refs
    return refs


def native_open_blockers(issue_data: dict, repo: str) -> list[str]:
    """Display refs of the issue's still-blocking GitHub-native dependencies.

    Reads the `blocked_by` list that `gh.list_labeled_issues` normalizes from
    the GraphQL `blockedBy` connection ({number, state, repo} dicts). Because
    the blocker's state arrives inline, this costs no extra API calls, and
    cross-repo blockers are supported (rendered as `owner/repo#N`; same-repo
    ones as `#N`).

    A blocker with a missing/unrecognized state fails safe to blocking,
    mirroring the "unknown" semantics of body-declared dependencies. Only
    "CLOSED" clears a blocker (the blockedBy connection holds Issues only,
    whose GraphQL states are exactly OPEN/CLOSED).

    Sorted same-repo first, then by repo and number, so log lines read
    naturally.
    """
    blockers: list[tuple[int, str, int]] = []
    for blocker in issue_data.get("blocked_by") or []:
        state = blocker.get("state")
        if isinstance(state, str) and state.upper() == "CLOSED":
            continue
        number = blocker["number"]
        blocker_repo = blocker.get("repo") or repo
        if blocker_repo == repo:
            blockers.append((0, "", number))
        else:
            blockers.append((1, blocker_repo, number))
    return [
        f"#{number}" if not blocker_repo else f"{blocker_repo}#{number}"
        for _, blocker_repo, number in sorted(blockers)
    ]


def fetch_blocker_states(
    repo: str,
    numbers: set[int],
    token: str,
    cache: dict[int, str],
) -> dict[int, str]:
    """Resolve each referenced issue to "open" / "closed" / "missing" / "unknown".

    `cache` is mutated in-place so multiple dependents sharing a blocker
    only cost one `gh` call per discovery cycle.

    Transient lookup failures (rate-limit, network, auth, anything that
    isn't a definitive NotFound from `get_issue_state`) become `"unknown"`,
    which `open_blockers` treats as still-blocking. Fail-safe: a
    spuriously-deferred issue self-corrects next cycle; a spuriously-
    enqueued one can ship a broken PR.
    """
    for number in numbers:
        if number in cache:
            continue
        try:
            cache[number] = gh.get_issue_state(repo, number, token)
        except gh.GhCliError as exc:
            logger.warning(
                "Failed to resolve blocker state for %s#%d: %s; "
                "treating as unknown (still blocking)",
                repo,
                number,
                exc,
            )
            cache[number] = "unknown"
    return {n: cache[n] for n in numbers}


def open_blockers(blockers: set[int], states: dict[int, str]) -> list[int]:
    """Return the sorted list of blockers that should defer the dependent.

    `open` and `unknown` are both treated as blocking — `unknown` is the
    fail-safe response to transient `gh` failures. `closed` and `missing`
    do not block.
    """
    return sorted(n for n in blockers if states.get(n) in ("open", "unknown"))
