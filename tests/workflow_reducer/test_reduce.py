"""Named reducer branches beyond the default contract fixtures."""

from __future__ import annotations

import pytest

from orcest.workflow_reducer.contract import default_view
from orcest.workflow_reducer.reduce import reduce
from orcest.workflow_reducer.types import ReductionKind, Trigger

pytestmark = pytest.mark.unit


def test_review_receipts_in_either_order_reach_aggregating() -> None:
    first = default_view(
        "REVIEWING",
        "ATTEMPT_RESULT",
        panel_complete=False,
        filling_review_slots=("slot-a",),
    )
    r1 = reduce(
        first,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-a",
            facts={"outcome": "SUCCEEDED", "activity_kind": "REVIEW", "fills_slot": True},
        ),
    )
    assert r1.next_state == "REVIEWING"
    second = default_view(
        "REVIEWING",
        "ATTEMPT_RESULT",
        panel_complete=True,
        filling_review_slots=("slot-a", "slot-b"),
        unfilled_review_slots=(),
    )
    r2 = reduce(
        second,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-b",
            facts={
                "outcome": "SUCCEEDED",
                "activity_kind": "REVIEW",
                "fills_slot": True,
                "panel_complete": True,
            },
        ),
    )
    assert r2.next_state == "AGGREGATING"
    swapped = reduce(
        second,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-a",
            facts={
                "outcome": "SUCCEEDED",
                "activity_kind": "REVIEW",
                "fills_slot": True,
                "panel_complete": True,
            },
        ),
    )
    assert swapped.next_state == r2.next_state
    assert swapped.reason_code == r2.reason_code


def test_verify_fail_below_threshold_remediates() -> None:
    view = default_view("VERIFYING", "ATTEMPT_RESULT")
    reduction = reduce(
        view,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-v",
            facts={
                "outcome": "SUCCEEDED",
                "activity_kind": "VERIFY",
                "verification_outcome": "FAIL",
            },
        ),
    )
    assert reduction.kind is ReductionKind.ADVANCE
    assert reduction.next_state == "REMEDIATING"
    assert any(activity.kind == "REMEDIATE" for activity in reduction.planned_activities)


def test_cancellation_suppresses_semantic_work() -> None:
    view = default_view(
        "PLANNING",
        "ATTEMPT_RESULT",
        cancellation_source_kind="MANAGEMENT_COMMAND",
        cancellation_source_id="cmd-1",
    )
    reduction = reduce(
        view,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-1",
            facts={"outcome": "SUCCEEDED", "activity_kind": "PLAN"},
        ),
    )
    assert reduction.kind is ReductionKind.SUPERSEDED
    assert reduction.next_state == "PLANNING"
    assert reduction.reason_code == "CANCELLATION_PRECEDENCE"
    assert reduction.planned_activities == ()


def test_pending_snapshot_wins_internal_arbitration() -> None:
    view = default_view(
        "PLANNING",
        "INTERNAL",
        pending_snapshot_id="22222222-2222-2222-2222-222222222222",
        pending_internal_sequence=1,
    )
    reduction = reduce(view, Trigger(kind="INTERNAL", trigger_id="1", facts={}))
    assert reduction.reason_code == "PENDING_SNAPSHOT_PRECEDENCE"
    assert reduction.pending_continuation is not None
    assert reduction.pending_continuation.kind == "SPEC_SUPERSEDE"


def test_generation_install_does_not_plan_work() -> None:
    view = default_view("ADMITTED", "SPEC_SUPERSEDE")
    reduction = reduce(
        view,
        Trigger(
            kind="SPEC_SUPERSEDE",
            trigger_id="22222222-2222-2222-2222-222222222222",
            facts={"install": True},
        ),
    )
    assert reduction.next_state == "ADMITTED"
    assert reduction.specification_generation == 1
    assert reduction.planned_activities == ()
    assert reduction.pending_continuation is not None
    assert reduction.pending_continuation.kind == "INTERNAL"


def test_stale_health_fanout_does_not_change_state() -> None:
    view = default_view("BUILDING", "HEALTH_OBSERVATION")
    reduction = reduce(
        view,
        Trigger(kind="HEALTH_OBSERVATION", trigger_id="health-1", facts={"kind": "AVAILABLE"}),
    )
    assert reduction.kind is ReductionKind.STALE
    assert reduction.next_state == "BUILDING"
