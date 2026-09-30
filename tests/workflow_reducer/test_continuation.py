from __future__ import annotations

import pytest

from orcest.workflow_reducer.continuation import ContinuationWinner, arbitrate_internal_continuation
from orcest.workflow_reducer.contract import default_view

pytestmark = pytest.mark.unit


def test_arbitration_precedence() -> None:
    base = default_view("PLANNING", "INTERNAL", pending_internal_sequence=4)
    cancelled = default_view(
        "PLANNING",
        "INTERNAL",
        pending_internal_sequence=4,
        cancellation_source_kind="MANAGEMENT_COMMAND",
        cancellation_source_id="c1",
        pending_snapshot_id="22222222-2222-2222-2222-222222222222",
        pending_dependency_observation_id="dep-1",
    )
    assert arbitrate_internal_continuation(cancelled, 4) is ContinuationWinner.CANCELLATION
    snapshot = default_view(
        "PLANNING",
        "INTERNAL",
        pending_internal_sequence=4,
        pending_snapshot_id="22222222-2222-2222-2222-222222222222",
        pending_dependency_observation_id="dep-1",
    )
    assert arbitrate_internal_continuation(snapshot, 4) is ContinuationWinner.SPEC_SUPERSEDE
    dependency = default_view(
        "PLANNING",
        "INTERNAL",
        pending_internal_sequence=4,
        pending_dependency_observation_id="dep-1",
        safe_boundary=True,
    )
    assert arbitrate_internal_continuation(dependency, 4) is ContinuationWinner.DEPENDENCY
    staffing = default_view(
        "REVIEWING",
        "INTERNAL",
        pending_internal_sequence=4,
        latest_staffing_recheck_transition_sequence=4,
        claimed_unfilled_peer=False,
    )
    assert arbitrate_internal_continuation(staffing, 4) is ContinuationWinner.STAFFING
    assert arbitrate_internal_continuation(base, 4) is ContinuationWinner.STATE_SPECIFIC
