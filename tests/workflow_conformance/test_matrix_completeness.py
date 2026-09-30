"""Release gate: every documented conformance case is accounted for.

operations-and-rollout.md's acceptance bar for this issue is "every normative
failure scenario has one named automated expected outcome." This module
enforces that mechanically over the two registries in this package:

- every ``case_id`` in ``matrix.FAILURE_INJECTION_MATRIX`` and
  ``rollout_matrix.ROLLOUT_COMPATIBILITY_MATRIX`` has exactly one
  ``coverage.CaseStatus`` entry (no missing case, no orphaned entry for a case
  that no longer exists);
- every ``automated`` entry's ``refs`` point at a real ``def test_...`` that
  actually exists in the referenced file today (catches renames/typos/deleted
  tests silently turning a "covered" case into a lie); and
- the ``deferred`` set -- cases whose production mechanism does not exist yet,
  so no automated test can prove them -- is pinned exactly. A newly deferred
  case requires a deliberate edit here, not a silent drop in coverage.

This keeps the ledger honest without requiring every future contributor to
re-verify all ~158 cases by hand: CI fails loudly the moment a reference goes
stale or a case is added to one of the two matrices without a paired
``coverage.py`` entry.
"""

from __future__ import annotations

import ast
from functools import lru_cache
from pathlib import Path

import pytest

from tests.workflow_conformance.coverage import (
    FAILURE_INJECTION_COVERAGE,
    ROLLOUT_COVERAGE,
    CaseStatus,
)
from tests.workflow_conformance.matrix import FAILURE_INJECTION_MATRIX
from tests.workflow_conformance.rollout_matrix import ROLLOUT_COMPATIBILITY_MATRIX

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Pinned exactly: a case may only join this set via a deliberate edit to this
# test (and to its CaseStatus.note explaining the missing production
# mechanism), never by a coverage.py entry quietly changing status.
_EXPECTED_DEFERRED_FAILURE_INJECTION_CASES = frozenset(
    {
        "fim.old_new_workers_coexist",
        "fim.legacy_worker_pel_reaper_injected_against_v1",
        "fim.startup_foreign_key_check_finds_only_missing",
        "fim.backup_restore_runs_retained_applied_cas_lost",
        "fim.terminal_secret_operation_replay_metadata_cleanup_races",
    }
)

_EXPECTED_DEFERRED_ROLLOUT_CASES = frozenset(
    {
        "vsr.controller_publishes_compatibility_matrix_in_readiness",
        "vsr.new_major_protocol_uses_new_stream_names",
        "vsr.controller_dual_publish_only_with_single_winner_claim",
        "vsr.rolling_worker_upgrade_canary_procedure",
        "vsr.controller_api_accepts_preceding_compatible_worker_minor_version",
        "vsr.filesystem_rewrite_migration_runs_in_maintenance_with_journal",
    }
)


@lru_cache(maxsize=None)
def _top_level_test_function_names(module_relpath: str) -> frozenset[str]:
    path = _REPO_ROOT / module_relpath
    if not path.is_file():
        return frozenset()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return frozenset(
        node.name
        for node in ast.iter_child_nodes(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    )


def _assert_registry_matches_coverage(
    case_ids: frozenset[str], coverage: dict[str, CaseStatus], *, registry_name: str
) -> None:
    covered_ids = frozenset(coverage.keys())
    missing = case_ids - covered_ids
    assert not missing, f"{registry_name}: cases with no coverage.py entry: {sorted(missing)}"
    orphaned = covered_ids - case_ids
    assert not orphaned, (
        f"{registry_name}: coverage.py entries for nonexistent cases: {sorted(orphaned)}"
    )


def test_every_failure_injection_case_has_a_coverage_entry() -> None:
    case_ids = frozenset(c.case_id for c in FAILURE_INJECTION_MATRIX)
    _assert_registry_matches_coverage(
        case_ids, FAILURE_INJECTION_COVERAGE, registry_name="FAILURE_INJECTION_MATRIX"
    )


def test_every_rollout_case_has_a_coverage_entry() -> None:
    case_ids = frozenset(c.case_id for c in ROLLOUT_COMPATIBILITY_MATRIX)
    _assert_registry_matches_coverage(
        case_ids, ROLLOUT_COVERAGE, registry_name="ROLLOUT_COMPATIBILITY_MATRIX"
    )


@pytest.mark.parametrize("case_id", sorted(FAILURE_INJECTION_COVERAGE), ids=lambda c: c)
def test_failure_injection_coverage_entry_is_well_formed(case_id: str) -> None:
    _assert_entry_well_formed(FAILURE_INJECTION_COVERAGE[case_id])


@pytest.mark.parametrize("case_id", sorted(ROLLOUT_COVERAGE), ids=lambda c: c)
def test_rollout_coverage_entry_is_well_formed(case_id: str) -> None:
    _assert_entry_well_formed(ROLLOUT_COVERAGE[case_id])


def _assert_entry_well_formed(entry: CaseStatus) -> None:
    if entry.status == "automated":
        assert entry.refs, f"{entry.case_id}: automated case must cite at least one ref"
        assert entry.confidence in {"exact", "thematic"}
    else:
        assert entry.status == "deferred"
        assert entry.confidence is None
        assert len(entry.note) >= 20, (
            f"{entry.case_id}: deferred case needs a real explanation, not a placeholder"
        )
    for ref in entry.refs:
        names = _top_level_test_function_names(ref.module)
        assert names, (
            f"{entry.case_id}: ref module does not exist or has no top-level defs: {ref.module}"
        )
        assert ref.function in names, (
            f"{entry.case_id}: {ref.module} has no top-level def named {ref.function!r} "
            "(stale reference -- the test was renamed, moved, or deleted)"
        )


def test_deferred_failure_injection_set_is_pinned() -> None:
    actual = frozenset(
        cid for cid, entry in FAILURE_INJECTION_COVERAGE.items() if entry.status == "deferred"
    )
    assert actual == _EXPECTED_DEFERRED_FAILURE_INJECTION_CASES, (
        "the set of cases without a production mechanism to test changed. If a case newly "
        "became deferred, that's a regression worth flagging loudly. If a case is now "
        "implemented, move it to 'automated' with real refs and shrink this pinned set."
    )


def test_deferred_rollout_set_is_pinned() -> None:
    actual = frozenset(cid for cid, entry in ROLLOUT_COVERAGE.items() if entry.status == "deferred")
    assert actual == _EXPECTED_DEFERRED_ROLLOUT_CASES, (
        "the set of version-skew/rollback requirements without a production mechanism to "
        "test changed -- update this pinned set deliberately, in either direction."
    )


def test_no_duplicate_case_ids_across_registries() -> None:
    fim_ids = {c.case_id for c in FAILURE_INJECTION_MATRIX}
    rollout_ids = {c.case_id for c in ROLLOUT_COMPATIBILITY_MATRIX}
    assert not (fim_ids & rollout_ids)
