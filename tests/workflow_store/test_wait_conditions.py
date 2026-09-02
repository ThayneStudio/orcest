"""Durable Wait Condition persistence and wake processing (issue #687)."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_store import (
    ActivityReviewAssignmentInput,
    AttemptOfferInput,
    IdempotencyConflictError,
    RunStore,
    WaitConditionPanelSlotInput,
    activity_offer_protocol,
)

pytestmark = pytest.mark.unit

POLICY_HASH = "sha256:" + "0" * 64
SEMANTIC_DIGEST = "sha256:" + "1" * 64
AUTHZ_DIGEST = "sha256:" + "a" * 64


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _create_waiting_run(store: RunStore, run_id: str, *, project_id: str = "project-a") -> None:
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id=project_id,
            work_item_key=f"work-{run_id}",
            state="WAITING",
            specification_generation=1,
        )


def _create_recovering_run(
    store: RunStore,
    run_id: str,
    *,
    project_id: str = "project-a",
    recovery_origin_state: str = "BUILDING",
) -> None:
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id=project_id,
            work_item_key=f"work-{run_id}",
            state="RECOVERING",
            specification_generation=1,
        )
        payload = {"recovery_origin_state": recovery_origin_state}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )


def _base_wait_kwargs(run_id: str, **overrides: object) -> dict:
    kwargs = dict(
        wait_condition_id=_uid(),
        run_id=run_id,
        reason="BACKOFF",
        resume_state="BUILDING",
        specification_generation=1,
        policy_hash=POLICY_HASH,
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=_uid(),
        created_transition_sequence=1,
        not_before_ms=_now_ms() + 60_000,
    )
    kwargs.update(overrides)
    return kwargs


# -- create_wait_condition: reason/wake compatibility matrix ----------------


def test_create_wait_condition_rejects_incompatible_reason_and_wake_shape(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_waiting_run(store, run_id)
    with store.transaction():
        with pytest.raises(ValueError):
            store.create_wait_condition(
                **_base_wait_kwargs(run_id, reason="CAPACITY", not_before_ms=_now_ms())
            )


def test_create_wait_condition_rejects_missing_timer_and_wake(store: RunStore) -> None:
    run_id = _uid()
    _create_waiting_run(store, run_id)
    with store.transaction():
        with pytest.raises(ValueError):
            store.create_wait_condition(
                **_base_wait_kwargs(run_id, reason="BACKOFF", not_before_ms=None)
            )


def test_create_wait_condition_requires_matched_wake_kind_and_identity(store: RunStore) -> None:
    run_id = _uid()
    _create_waiting_run(store, run_id)
    with store.transaction():
        with pytest.raises(ValueError):
            store.create_wait_condition(
                **_base_wait_kwargs(
                    run_id,
                    reason="EXTERNAL_DEPENDENCY",
                    not_before_ms=None,
                    wake_kind=None,
                    wake_identity={"dependency_set_digest": "sha256:" + "2" * 64},
                )
            )


def test_create_wait_condition_accepts_every_closed_reason_shape(store: RunStore) -> None:
    now = _now_ms()
    cases = [
        dict(reason="CAPACITY", not_before_ms=None, wake_kind="CAPACITY", wake_identity={"a": 1}),
        dict(reason="RATE_LIMIT", not_before_ms=now + 1000, wake_kind=None, wake_identity=None),
        dict(
            reason="RATE_LIMIT",
            not_before_ms=now + 1000,
            wake_kind="RATE_LIMIT_RESET",
            wake_identity={"scope_kind": "PROVIDER_ACCOUNT", "scope_id": "acct-1"},
        ),
        dict(
            reason="BUDGET",
            not_before_ms=now + 1000,
            wake_kind="BUDGET_WINDOW",
            wake_identity={"project_id": "p"},
        ),
        dict(reason="BACKOFF", not_before_ms=now + 1000, wake_kind=None, wake_identity=None),
        dict(
            reason="EXTERNAL_DEPENDENCY",
            not_before_ms=None,
            wake_kind="DEPENDENCY",
            wake_identity={"dependency_set_digest": "sha256:" + "2" * 64},
        ),
        dict(
            reason="FORGE_UNAVAILABLE",
            not_before_ms=now + 1000,
            wake_kind="FORGE",
            wake_identity={"forge_instance_id": "f"},
        ),
        dict(
            reason="STORAGE_RECOVERY",
            not_before_ms=None,
            wake_kind="STORAGE",
            wake_identity={"object_kind": "WORKFLOW_BLOB", "object_id": "b"},
        ),
        dict(
            reason="SECRET_RECOVERY",
            not_before_ms=None,
            wake_kind="SECRET",
            wake_identity={"secret_id": "s", "minimum_version": 2},
        ),
        dict(
            reason="EVIDENCE",
            not_before_ms=now + 1000,
            wake_kind="EVIDENCE",
            wake_identity={"target_kind": "WORK_ITEM", "target_id": "t"},
        ),
    ]
    for case in cases:
        run_id = _uid()
        _create_waiting_run(store, run_id)
        with store.transaction():
            record = store.create_wait_condition(**_base_wait_kwargs(run_id, **case))
        assert record.reason == case["reason"]
        assert record.wake_identity == case["wake_identity"]


def test_create_wait_condition_is_idempotent_by_created_from_identity(store: RunStore) -> None:
    run_id = _uid()
    _create_waiting_run(store, run_id)
    kwargs = _base_wait_kwargs(run_id)
    with store.transaction():
        first = store.create_wait_condition(**kwargs)
        replay = store.create_wait_condition(**kwargs)
    assert replay == first

    conflicting = dict(kwargs)
    conflicting["not_before_ms"] = kwargs["not_before_ms"] + 1
    with store.transaction():
        with pytest.raises(IdempotencyConflictError):
            store.create_wait_condition(**conflicting)

    conflicting_sequence = dict(kwargs)
    conflicting_sequence["created_transition_sequence"] = kwargs["created_transition_sequence"] + 1
    with store.transaction():
        with pytest.raises(IdempotencyConflictError):
            store.create_wait_condition(**conflicting_sequence)


def test_create_wait_condition_freezes_health_membership_digest_regardless_of_input_order(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_waiting_run(store, run_id)
    with store.transaction():
        first = store._insert_health_observation(
            scope_kind="PROVIDER_ACCOUNT",
            scope_id="acct-a",
            kind="AVAILABLE",
            source_kind="CAPACITY_REPORT",
            source_id="report-1",
            subject_bindings={"scope": "acct-a"},
            observed_revision=1,
            effective_at_ms=1000,
            expires_at_ms=100_000,
        )
        second = store._insert_health_observation(
            scope_kind="PROVIDER_ACCOUNT",
            scope_id="acct-b",
            kind="AVAILABLE",
            source_kind="CAPACITY_REPORT",
            source_id="report-2",
            subject_bindings={"scope": "acct-b"},
            observed_revision=1,
            effective_at_ms=1000,
            expires_at_ms=100_000,
        )
        forward = store.create_wait_condition(
            **_base_wait_kwargs(
                run_id,
                created_from_id=_uid(),
                health_observations=(first, second),
            )
        )
    run_id_2 = _uid()
    _create_waiting_run(store, run_id_2)
    with store.transaction():
        backward = store.create_wait_condition(
            **_base_wait_kwargs(
                run_id_2,
                created_from_id=_uid(),
                health_observations=(second, first),
            )
        )
    assert forward.health_observation_ids_digest == backward.health_observation_ids_digest
    assert forward.health_observation_ids == backward.health_observation_ids


# -- create_wait_condition: panel-slot membership ----------------------------


def _plan_review_activity(
    store: RunStore,
    *,
    run_id: str,
    state: str,
    activity_ordinal: int = 1,
    reviewer_slot: str = "slot-a",
    worker_profile: str = "codex",
) -> str:
    activity_id = _uid()
    attempt_id = _uid()
    outbox_id = _uid()
    store.create_activity(
        activity_id=activity_id,
        run_id=run_id,
        activity_ordinal=activity_ordinal,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="REVIEW",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={"a": 1},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key="sha256:" + uuid.uuid4().hex.ljust(64, "0")[:64],
        role="review",
        slot=reviewer_slot,
        review_assignment=ActivityReviewAssignmentInput(
            assignment_kind="REVIEW",
            panel_round=1,
            role="review",
            context_digest="sha256:" + "3" * 64,
            subject_refs=("snapshot:overall",),
            reviewer_slot=reviewer_slot,
        ),
        attempt=AttemptOfferInput(
            attempt_id=attempt_id,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile=worker_profile,
            offered_at_ms=_now_ms(),
            claim_timeout_ms=300_000,
        ),
        outbox_id=outbox_id,
    )
    if state == "PLANNED":
        with store.transaction():
            store.conn.execute(
                "UPDATE attempts SET state = 'EXPIRED' WHERE attempt_id = ?", (attempt_id,)
            )
            store.conn.execute(
                "UPDATE activities SET state = 'PLANNED' WHERE activity_id = ?", (activity_id,)
            )
    return activity_id


def test_create_wait_condition_rejects_panel_slot_with_live_attempt(store: RunStore) -> None:
    run_id = _uid()
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key="work-1",
            state="WAITING",
            specification_generation=1,
        )
    activity_id = _plan_review_activity(store, run_id=run_id, state="READY")
    with store.transaction():
        with pytest.raises(ValueError):
            store.create_wait_condition(
                **_base_wait_kwargs(
                    run_id,
                    reason="CAPACITY",
                    not_before_ms=None,
                    wake_kind="CAPACITY",
                    wake_identity={"assignment_kind": "REVIEW", "panel_round": 1},
                    panel_slots=(
                        WaitConditionPanelSlotInput(
                            activity_id=activity_id,
                            assignment_kind="REVIEW",
                            panel_round=1,
                            slot_id="slot-a",
                        ),
                    ),
                )
            )


def test_create_wait_condition_freezes_planned_panel_slots(store: RunStore) -> None:
    run_id = _uid()
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key="work-1",
            state="WAITING",
            specification_generation=1,
        )
    activity_id = _plan_review_activity(store, run_id=run_id, state="PLANNED")
    with store.transaction():
        record = store.create_wait_condition(
            **_base_wait_kwargs(
                run_id,
                reason="CAPACITY",
                not_before_ms=None,
                wake_kind="CAPACITY",
                wake_identity={"assignment_kind": "REVIEW", "panel_round": 1},
                panel_slots=(
                    WaitConditionPanelSlotInput(
                        activity_id=activity_id,
                        assignment_kind="REVIEW",
                        panel_round=1,
                        slot_id="slot-a",
                    ),
                ),
            )
        )
    assert len(record.panel_slots) == 1
    assert record.panel_slots[0].activity_id == activity_id
    assert record.panel_slots_digest != "sha256:" + "0" * 64


def test_wake_capacity_waits_panel_is_all_or_none(store: RunStore) -> None:
    """A panel Wait with two unfilled slots must not wake while only one
    slot's profile is compatibly available (domain-model.md "Wait Condition
    Panel Slot": "Partial staffing cannot commit or be reconstructed")."""
    run_id = _uid()
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key="work-1",
            state="WAITING",
            specification_generation=1,
        )
    activity_a = _plan_review_activity(
        store,
        run_id=run_id,
        state="PLANNED",
        activity_ordinal=1,
        reviewer_slot="slot-a",
        worker_profile="codex",
    )
    activity_b = _plan_review_activity(
        store,
        run_id=run_id,
        state="PLANNED",
        activity_ordinal=2,
        reviewer_slot="slot-b",
        worker_profile="claude",
    )
    with store.transaction():
        wait = store.create_wait_condition(
            **_base_wait_kwargs(
                run_id,
                reason="CAPACITY",
                not_before_ms=None,
                wake_kind="CAPACITY",
                wake_identity={"assignment_kind": "REVIEW", "panel_round": 1},
                panel_slots=(
                    WaitConditionPanelSlotInput(
                        activity_id=activity_a,
                        assignment_kind="REVIEW",
                        panel_round=1,
                        slot_id="slot-a",
                    ),
                    WaitConditionPanelSlotInput(
                        activity_id=activity_b,
                        assignment_kind="REVIEW",
                        panel_round=1,
                        slot_id="slot-b",
                    ),
                ),
            )
        )
        payload = {"wait_condition_id": wait.wait_condition_id, "wait_reason": "CAPACITY"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )
        codex_health = store._insert_health_observation(
            scope_kind="WORKER_PROFILE",
            scope_id="codex",
            kind="AVAILABLE",
            source_kind="CAPACITY_REPORT",
            source_id="report-1",
            subject_bindings={"worker_profile": "codex"},
            observed_revision=1,
            effective_at_ms=_now_ms(),
            expires_at_ms=_now_ms() + 100_000,
        )

    # Only slot-a's profile ("codex") is available; slot-b's ("claude") has no
    # Health Observation at all yet -- the panel must stay WAITING.
    woken = store._wake_capacity_waits([codex_health])
    assert woken == []
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "WAITING"

    with store.transaction():
        claude_health = store._insert_health_observation(
            scope_kind="WORKER_PROFILE",
            scope_id="claude",
            kind="AVAILABLE",
            source_kind="CAPACITY_REPORT",
            source_id="report-2",
            subject_bindings={"worker_profile": "claude"},
            observed_revision=1,
            effective_at_ms=_now_ms(),
            expires_at_ms=_now_ms() + 100_000,
        )

    # Now every slot is compatibly available: the whole panel wakes at once.
    woken = store._wake_capacity_waits([claude_health])
    assert woken == [wait.wait_condition_id]
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "RECOVERING"


# -- submit_recovery_evidence: WAIT_* tactic creates a real Wait ------------


def test_submit_recovery_evidence_wait_backoff_creates_wait_and_enters_waiting(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")

    outcome = store.submit_recovery_evidence(
        recovery_evidence_id=_uid(),
        run_id=run_id,
        source_kind="FORGE_REQUEST_FAILURE",
        source_id=_uid(),
        facts={"failure_kind": "TIMEOUT"},
        accepted_at_ms=_now_ms(),
    )

    assert outcome.selected_tactic == "WAIT_BACKOFF"
    assert outcome.wait_condition is not None
    assert outcome.wait_condition.reason == "BACKOFF"
    assert outcome.wait_condition.resume_state == "BUILDING"
    run_row = store.conn.execute(
        "SELECT state, wait_condition_id FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert run_row["state"] == "WAITING"
    assert run_row["wait_condition_id"] == outcome.wait_condition.wait_condition_id


def test_submit_recovery_evidence_wait_budget_already_satisfied_skips_wait(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    project_id = "77777777-7777-4777-8777-777777777777"
    accounting_scope_id = "default"

    report = store.submit_budget_report(
        budget_report_id=_uid(),
        project_id=project_id,
        accounting_scope_id=accounting_scope_id,
        budget_policy_ref="default",
        budget_reset_window_ref="default",
        window_id="window-1",
        window_start_ms=_now_ms() - 1000,
        reset_at_ms=_now_ms() + 3_600_000,
        source_sequence=5,
        source_revision="rev-5",
        limit_microunits=1_000_000,
        consumed_microunits=0,
        authenticated_principal_id="budget-accounting-service",
        authorization_context_digest=AUTHZ_DIGEST,
        max_budget_report_age_ms=600_000,
    )
    assert report.availability == "AVAILABLE"

    outcome = store.submit_recovery_evidence(
        recovery_evidence_id=_uid(),
        run_id=run_id,
        source_kind="BUDGET_REPORT",
        source_id=_uid(),
        facts={"availability": "EXHAUSTED"},
        budget_wake_identity={
            "project_id": project_id,
            "accounting_scope_id": accounting_scope_id,
            "budget_policy_ref": "default",
            "budget_reset_window_ref": "default",
            "budget_report_id": _uid(),
            "window_id": "window-0",
            "reset_at_ms": _now_ms() + 1_800_000,
            "minimum_source_sequence": 5,
        },
        accepted_at_ms=_now_ms(),
    )

    assert outcome.selected_tactic == "WAIT_BUDGET"
    assert outcome.predicate_check is not None
    assert outcome.predicate_check.already_satisfied is True
    assert outcome.wait_condition is None
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "BUILDING"


def test_submit_recovery_evidence_wait_budget_not_satisfied_creates_wait(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    project_id = "77777777-7777-4777-8777-777777777777"
    accounting_scope_id = "default"

    outcome = store.submit_recovery_evidence(
        recovery_evidence_id=_uid(),
        run_id=run_id,
        source_kind="BUDGET_REPORT",
        source_id=_uid(),
        facts={"availability": "EXHAUSTED"},
        budget_wake_identity={
            "project_id": project_id,
            "accounting_scope_id": accounting_scope_id,
            "budget_policy_ref": "default",
            "budget_reset_window_ref": "default",
            "budget_report_id": _uid(),
            "window_id": "window-0",
            "reset_at_ms": _now_ms() + 1_800_000,
            "minimum_source_sequence": 1,
        },
        accepted_at_ms=_now_ms(),
    )

    assert outcome.selected_tactic == "WAIT_BUDGET"
    assert outcome.predicate_check is not None
    assert outcome.predicate_check.already_satisfied is False
    assert outcome.wait_condition is not None
    assert outcome.wait_condition.reason == "BUDGET"
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "WAITING"


def test_submit_recovery_evidence_drops_panel_slot_raced_to_live_attempt(
    store: RunStore,
) -> None:
    """A named panel slot that raced to a live OFFERED/CLAIMED Attempt (e.g.
    another writer staffed it) between the caller gathering
    ``panel_wait_slots`` and this writer-lock recheck must be dropped from
    the frozen Wait's membership rather than reaching
    ``create_wait_condition``, which rejects anything but a PLANNED activity
    with no live Attempt."""
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    blocked_activity = _plan_review_activity(
        store,
        run_id=run_id,
        state="PLANNED",
        activity_ordinal=1,
        reviewer_slot="slot-a",
        worker_profile="codex",
    )
    raced_activity = _plan_review_activity(
        store,
        run_id=run_id,
        state="READY",
        activity_ordinal=2,
        reviewer_slot="slot-b",
        worker_profile="claude",
    )

    outcome = store.submit_recovery_evidence(
        recovery_evidence_id=_uid(),
        run_id=run_id,
        source_kind="HEALTH_OBSERVATION",
        source_id=_uid(),
        facts={"kind": "UNAVAILABLE"},
        panel_wait_slots=(
            WaitConditionPanelSlotInput(
                activity_id=blocked_activity,
                assignment_kind="REVIEW",
                panel_round=1,
                slot_id="slot-a",
            ),
            WaitConditionPanelSlotInput(
                activity_id=raced_activity,
                assignment_kind="REVIEW",
                panel_round=1,
                slot_id="slot-b",
            ),
        ),
        accepted_at_ms=_now_ms(),
    )

    assert outcome.selected_tactic == "WAIT_CAPACITY"
    assert outcome.predicate_check is not None
    assert outcome.predicate_check.already_satisfied is False
    assert outcome.wait_condition is not None
    assert [slot.activity_id for slot in outcome.wait_condition.panel_slots] == [blocked_activity]


def test_submit_recovery_evidence_panel_already_satisfied_when_all_slots_race_away(
    store: RunStore,
) -> None:
    """If every named panel slot has since raced to a live Attempt, the
    original capacity failure is moot -- the Run returns straight to its
    recovery origin instead of freezing an empty panel Wait."""
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    raced_activity = _plan_review_activity(
        store,
        run_id=run_id,
        state="READY",
        activity_ordinal=1,
        reviewer_slot="slot-a",
        worker_profile="codex",
    )

    outcome = store.submit_recovery_evidence(
        recovery_evidence_id=_uid(),
        run_id=run_id,
        source_kind="HEALTH_OBSERVATION",
        source_id=_uid(),
        facts={"kind": "UNAVAILABLE"},
        panel_wait_slots=(
            WaitConditionPanelSlotInput(
                activity_id=raced_activity,
                assignment_kind="REVIEW",
                panel_round=1,
                slot_id="slot-a",
            ),
        ),
        accepted_at_ms=_now_ms(),
    )

    assert outcome.selected_tactic == "WAIT_CAPACITY"
    assert outcome.predicate_check is not None
    assert outcome.predicate_check.already_satisfied is True
    assert outcome.wait_condition is None
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "BUILDING"


# -- Timer-driven wake --------------------------------------------------


def test_wake_due_wait_timers_wakes_exactly_once(store: RunStore) -> None:
    run_id = _uid()
    _create_waiting_run(store, run_id)
    with store.transaction():
        payload = {"wait_condition_id": None, "wait_reason": "BACKOFF"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )
        wait = store.create_wait_condition(
            **_base_wait_kwargs(run_id, not_before_ms=_now_ms() - 1000)
        )
        payload = {"wait_condition_id": wait.wait_condition_id, "wait_reason": "BACKOFF"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=1,
            payload_digest=request_digest(payload),
            payload=payload,
        )

    woken = store.wake_due_wait_timers()
    assert woken == [wait.wait_condition_id]
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "RECOVERING"

    # Replaying the sweep must not fire a second wake for the same due timer.
    woken_again = store.wake_due_wait_timers()
    assert woken_again == []


# -- Secret/external-dependency wake -------------------------------------


def test_wake_secret_recovery_wait_requires_minimum_version(store: RunStore) -> None:
    run_id = _uid()
    _create_waiting_run(store, run_id)
    secret_id = "11111111-1111-4111-8111-111111111111"
    with store.transaction():
        payload = {"wait_condition_id": None, "wait_reason": "SECRET_RECOVERY"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )
        wait = store.create_wait_condition(
            **_base_wait_kwargs(
                run_id,
                reason="SECRET_RECOVERY",
                not_before_ms=None,
                wake_kind="SECRET",
                wake_identity={"secret_id": secret_id, "minimum_version": 2},
            )
        )
        payload = {"wait_condition_id": wait.wait_condition_id, "wait_reason": "SECRET_RECOVERY"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=1,
            payload_digest=request_digest(payload),
            payload=payload,
        )

    # No current verified version yet: must not wake.
    assert store.wake_secret_recovery_wait(run_id, secret_version_id=_uid()) is False
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "WAITING"
