"""Tests for orcest.orchestrator.issue_deps."""

import pytest

from orcest.orchestrator import gh
from orcest.orchestrator.issue_deps import (
    fetch_blocker_states,
    native_open_blockers,
    open_blockers,
    parse_blocker_refs,
)

REPO = "test-org/test-repo"
TOKEN = "fake-token"


# ---------------------------------------------------------------------------
# parse_blocker_refs
# ---------------------------------------------------------------------------


def test_empty_body_returns_empty_set():
    assert parse_blocker_refs("") == set()
    assert parse_blocker_refs("   \n\n") == set()


def test_parses_blocked_by():
    assert parse_blocker_refs("This is blocked by #42 right now.") == {42}


def test_parses_depends_on():
    assert parse_blocker_refs("Depends on #7 landing first.") == {7}


def test_parses_requires():
    assert parse_blocker_refs("Requires #99 to merge.") == {99}


def test_parses_prerequisite():
    assert parse_blocker_refs("Prerequisite: #15") == {15}
    assert parse_blocker_refs("Prerequisites: #15") == {15}
    # Whitespace delimiter (no colon).
    assert parse_blocker_refs("Prerequisite #15") == {15}
    assert parse_blocker_refs("Prerequisites #16") == {16}


def test_parses_after_with_action_verb():
    assert parse_blocker_refs("Should be picked up after #5 merges.") == {5}
    assert parse_blocker_refs("Pick this up after #6 lands") == {6}
    assert parse_blocker_refs("after #7 closes") == {7}


def test_after_without_action_verb_is_not_a_blocker():
    # Avoid date-like / temporal false positives.
    assert parse_blocker_refs("Try this after 5pm") == set()
    assert parse_blocker_refs("after #5 we should reconsider") == set()


def test_parses_unchecked_task_list_item():
    body = "- [ ] #101\n- [x] #102\n* [ ] #103"
    assert parse_blocker_refs(body) == {101, 103}


def test_closes_fixes_resolves_are_not_blockers():
    body = "Closes #1\nFixes #2\nResolves #3\nResolved by #4"
    assert parse_blocker_refs(body) == set()


def test_bare_reference_is_not_a_blocker():
    # We don't treat naked `#N` mentions as dependencies — too noisy.
    assert parse_blocker_refs("See also #99 for context.") == set()


def test_multiple_blockers_collected():
    body = "Blocked by #1, depends on #2. Also requires #3."
    assert parse_blocker_refs(body) == {1, 2, 3}


def test_case_insensitive():
    assert parse_blocker_refs("BLOCKED BY #5") == {5}
    assert parse_blocker_refs("Depends On #6") == {6}


# ---------------------------------------------------------------------------
# fetch_blocker_states (with cache)
# ---------------------------------------------------------------------------


def test_fetch_blocker_states_populates_cache(mocker):
    mock_get_state = mocker.patch.object(gh, "get_issue_state", side_effect=["open", "closed"])
    cache: dict[int, str] = {}
    states = fetch_blocker_states(REPO, {10, 20}, TOKEN, cache)
    assert states == {10: mocker.ANY, 20: mocker.ANY}
    assert set(cache.keys()) == {10, 20}
    assert mock_get_state.call_count == 2


def test_fetch_blocker_states_uses_cache(mocker):
    mock_get_state = mocker.patch.object(gh, "get_issue_state", return_value="open")
    cache: dict[int, str] = {5: "closed"}
    states = fetch_blocker_states(REPO, {5}, TOKEN, cache)
    assert states == {5: "closed"}
    mock_get_state.assert_not_called()


def test_fetch_blocker_states_treats_transient_gh_failure_as_unknown(mocker):
    """Generic GhCliError (rate-limit, network, auth) → 'unknown', fail-safe to blocking."""
    mocker.patch.object(
        gh,
        "get_issue_state",
        side_effect=gh.GhCliError("boom"),
    )
    cache: dict[int, str] = {}
    states = fetch_blocker_states(REPO, {123}, TOKEN, cache)
    assert states == {123: "unknown"}
    assert cache[123] == "unknown"


def test_fetch_blocker_states_treats_rate_limit_as_unknown(mocker):
    mocker.patch.object(
        gh,
        "get_issue_state",
        side_effect=gh.GhRateLimitError("rate limited"),
    )
    cache: dict[int, str] = {}
    states = fetch_blocker_states(REPO, {7}, TOKEN, cache)
    assert states == {7: "unknown"}


def test_fetch_blocker_states_shared_cache_across_calls(mocker):
    """Multiple dependents sharing a blocker only cost one gh call."""
    mock_get_state = mocker.patch.object(gh, "get_issue_state", return_value="open")
    cache: dict[int, str] = {}
    fetch_blocker_states(REPO, {7}, TOKEN, cache)
    fetch_blocker_states(REPO, {7}, TOKEN, cache)
    fetch_blocker_states(REPO, {7, 8}, TOKEN, cache)
    assert mock_get_state.call_count == 2  # 7 once, 8 once


# ---------------------------------------------------------------------------
# open_blockers
# ---------------------------------------------------------------------------


def test_open_blockers_returns_sorted_open_only():
    states = {1: "open", 2: "closed", 3: "open", 4: "missing"}
    assert open_blockers({1, 2, 3, 4}, states) == [1, 3]


def test_open_blockers_empty_when_all_resolved():
    states = {1: "closed", 2: "missing"}
    assert open_blockers({1, 2}, states) == []


def test_open_blockers_handles_missing_state_entry():
    # Defensive: blocker referenced but never resolved.
    assert open_blockers({99}, {}) == []


def test_open_blockers_treats_unknown_as_blocking():
    """Fail-safe: transient lookup failure must not silently unblock."""
    states = {1: "unknown", 2: "closed"}
    assert open_blockers({1, 2}, states) == [1]


# ---------------------------------------------------------------------------
# Smoke: combined flow
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body,expected",
    [
        ("Blocked by #1 and depends on #2", {1, 2}),
        ("- [ ] #5\nCloses #6\n", {5}),
        ("Nothing to do here.", set()),
    ],
)
def test_parse_smoke(body, expected):
    assert parse_blocker_refs(body) == expected


# ---------------------------------------------------------------------------
# DoS guards
# ---------------------------------------------------------------------------


def test_fenced_code_block_is_stripped_before_parsing():
    """Refs pasted inside a fenced code block (logs, transcripts) are ignored."""
    body = "Real dep: depends on #42\n```\nLog: blocked by #999 happened\n```"
    assert parse_blocker_refs(body) == {42}


def test_refs_per_body_cap():
    """An issue body with thousands of refs is capped to a sane bound."""
    body = " ".join(f"depends on #{i}" for i in range(1, 200))
    refs = parse_blocker_refs(body)
    # Cap is _MAX_REFS_PER_BODY (32 at time of writing).
    assert 0 < len(refs) <= 32


def test_huge_digit_run_is_dropped():
    """Refs with absurd digit lengths don't reach `int()` or downstream gh."""
    body = "blocked by #" + ("9" * 50)
    assert parse_blocker_refs(body) == set()


def test_huge_and_normal_refs_mixed():
    body = "blocked by #" + ("9" * 50) + " and depends on #5"
    assert parse_blocker_refs(body) == {5}


# ---------------------------------------------------------------------------
# native_open_blockers (GitHub-native issue dependencies)
# ---------------------------------------------------------------------------


def _issue_with_blocked_by(blocked_by: list[dict] | None) -> dict:
    issue = {"number": 1, "title": "t", "body": "", "labels": []}
    if blocked_by is not None:
        issue["blocked_by"] = blocked_by
    return issue


def test_native_open_blockers_reports_open_same_repo():
    issue = _issue_with_blocked_by([{"number": 5, "state": "OPEN", "repo": REPO}])
    assert native_open_blockers(issue, REPO) == ["#5"]


def test_native_open_blockers_ignores_closed():
    issue = _issue_with_blocked_by([{"number": 5, "state": "CLOSED", "repo": REPO}])
    assert native_open_blockers(issue, REPO) == []


def test_native_open_blockers_cross_repo_uses_full_ref():
    issue = _issue_with_blocked_by([{"number": 7, "state": "OPEN", "repo": "other-org/other-repo"}])
    assert native_open_blockers(issue, REPO) == ["other-org/other-repo#7"]


def test_native_open_blockers_missing_key_is_empty():
    assert native_open_blockers(_issue_with_blocked_by(None), REPO) == []


def test_native_open_blockers_unknown_state_blocks():
    """A blocker whose state we can't interpret must fail-safe to blocking,
    mirroring the body-dep 'unknown' semantics."""
    issue = _issue_with_blocked_by([{"number": 5, "state": None, "repo": REPO}])
    assert native_open_blockers(issue, REPO) == ["#5"]


def test_native_open_blockers_sorted_same_repo_first_then_by_number():
    issue = _issue_with_blocked_by(
        [
            {"number": 9, "state": "OPEN", "repo": "other-org/z-repo"},
            {"number": 12, "state": "OPEN", "repo": REPO},
            {"number": 3, "state": "OPEN", "repo": REPO},
        ]
    )
    assert native_open_blockers(issue, REPO) == ["#3", "#12", "other-org/z-repo#9"]
