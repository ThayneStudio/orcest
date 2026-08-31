"""Transition ledger: persist one source-unique reduction per causal input."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Mapping
from typing import Any

import orcest.workflow_contract.v1.protocol_registry  # noqa: F401
from orcest.workflow_contract.v1.digest import (
    activity_idempotency_digest,
    bare_canonical_digest,
    request_digest,
    transition_digest,
)
from orcest.workflow_contract.v1.protocol import known_protocol_literals
from orcest.workflow_reducer.reduce import reduce
from orcest.workflow_reducer.types import (
    PRIOR_STATE_NONE,
    ActivityView,
    AppliedReduction,
    PlannedActivity,
    Reduction,
    RunView,
    Trigger,
)
from orcest.workflow_store.store import (
    DEFAULT_REDUCER_VERSION,
    RunStore,
)

__all__ = [
    "FORGE_OBSERVATION_SOURCE_KIND",
    "RUN_POINTERS_KIND",
    "apply",
    "load_view",
]

RUN_POINTERS_KIND = "run_pointers"
RUN_ACTIVITIES_KIND = "run_activities"
FORGE_OBSERVATION_SOURCE_KIND = "run_forge_observation"
ACTIVITY_IDEMPOTENCY_SOURCE_KIND = "activity_idempotency"

_IdFactory = Callable[[], str]


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _activity_offer_protocol() -> str:
    matches = sorted(
        literal
        for literal in known_protocol_literals()
        if literal.startswith("orcest.activity-offer/")
    )
    if len(matches) != 1:
        raise RuntimeError(f"expected one activity-offer protocol, got {matches!r}")
    return matches[0]


def load_view(store: RunStore, run_id: str) -> RunView | None:
    """Reconstruct a reducer view from the run row and pointer projection."""
    record = store.get_run(run_id)
    if record is None:
        return None
    stored = store.get_revisioned_object(RUN_POINTERS_KIND, run_id)
    pointers: dict[str, Any] = {}
    revision = 0
    if stored is not None:
        revision, _, payload_json = stored
        loaded = json.loads(payload_json)
        if isinstance(loaded, dict):
            pointers = loaded
    activities = tuple(
        ActivityView(
            activity_id=str(item["activity_id"]),
            kind=str(item["kind"]),
            state=str(item["state"]),
            specification_generation=int(item.get("specification_generation", 1)),
            candidate_id=item.get("candidate_id"),
            current_attempt_id=item.get("current_attempt_id"),
            current_attempt_state=item.get("current_attempt_state"),
            slot=item.get("slot"),
        )
        for item in pointers.get("activities", [])
    )
    return RunView(
        run_id=record.run_id,
        project_id=record.project_id,
        work_item_key=record.work_item_key,
        state=record.state,
        specification_generation=record.specification_generation,
        reducer_version=record.reducer_version,
        current_snapshot_id=pointers.get("current_snapshot_id"),
        pending_snapshot_id=pointers.get("pending_snapshot_id"),
        supersede_requested=bool(pointers.get("supersede_requested", False)),
        current_candidate_id=pointers.get("current_candidate_id"),
        policy_replan_candidate_id=pointers.get("policy_replan_candidate_id"),
        publication_id=pointers.get("publication_id"),
        publication_state=pointers.get("publication_state"),
        change_request_external_id=pointers.get("change_request_external_id"),
        next_activity_ordinal=int(pointers.get("next_activity_ordinal", 1)),
        next_transition_sequence=int(
            pointers.get("next_transition_sequence", record.current_revision + 1 or 1)
        ),
        pending_internal_sequence=pointers.get("pending_internal_sequence"),
        cancellation_source_kind=pointers.get("cancellation_source_kind"),
        cancellation_source_id=pointers.get("cancellation_source_id"),
        pending_dependency_observation_id=pointers.get("pending_dependency_observation_id"),
        panel_staffing_kind=pointers.get("panel_staffing_kind"),
        latest_staffing_recheck_transition_sequence=pointers.get(
            "latest_staffing_recheck_transition_sequence"
        ),
        wait_condition_id=pointers.get("wait_condition_id"),
        wait_reason=pointers.get("wait_reason"),
        human_boundary_id=pointers.get("human_boundary_id"),
        human_boundary_reason=pointers.get("human_boundary_reason"),
        recovery_origin_state=pointers.get("recovery_origin_state"),
        recovery_activity_id=pointers.get("recovery_activity_id"),
        recovery_tactic=pointers.get("recovery_tactic"),
        current_recovery_evidence_id=pointers.get("current_recovery_evidence_id"),
        offer_permitted=bool(pointers.get("offer_permitted", True)),
        safe_boundary=bool(pointers.get("safe_boundary", True)),
        generation_installed=bool(
            pointers.get("generation_installed", record.specification_generation > 0)
        ),
        initial_plan_absent=bool(pointers.get("initial_plan_absent", record.state == "ADMITTED")),
        claimed_unfilled_peer=bool(pointers.get("claimed_unfilled_peer", False)),
        panel_complete=bool(pointers.get("panel_complete", False)),
        policy_hash=str(pointers.get("policy_hash", "sha256:" + ("0" * 64))),
        filling_review_slots=tuple(pointers.get("filling_review_slots", ())),
        unfilled_review_slots=tuple(pointers.get("unfilled_review_slots", ())),
        activities=activities,
        consumed_forge_observation_ids=tuple(pointers.get("consumed_forge_observation_ids", ())),
        terminal_duplicate_cleanup_active=bool(
            pointers.get("terminal_duplicate_cleanup_active", False)
        ),
        revision=revision,
    )


def _pointers_payload(
    view: RunView, reduction: Reduction, extra: Mapping[str, Any]
) -> dict[str, Any]:
    payload = {
        "current_snapshot_id": view.current_snapshot_id,
        "pending_snapshot_id": view.pending_snapshot_id,
        "supersede_requested": view.supersede_requested,
        "current_candidate_id": view.current_candidate_id,
        "policy_replan_candidate_id": view.policy_replan_candidate_id,
        "publication_id": view.publication_id,
        "publication_state": view.publication_state,
        "change_request_external_id": view.change_request_external_id,
        "next_activity_ordinal": view.next_activity_ordinal,
        "next_transition_sequence": view.next_transition_sequence,
        "pending_internal_sequence": view.pending_internal_sequence,
        "cancellation_source_kind": view.cancellation_source_kind,
        "cancellation_source_id": view.cancellation_source_id,
        "pending_dependency_observation_id": view.pending_dependency_observation_id,
        "panel_staffing_kind": view.panel_staffing_kind,
        "latest_staffing_recheck_transition_sequence": (
            view.latest_staffing_recheck_transition_sequence
        ),
        "wait_condition_id": view.wait_condition_id,
        "wait_reason": view.wait_reason,
        "human_boundary_id": view.human_boundary_id,
        "human_boundary_reason": view.human_boundary_reason,
        "recovery_origin_state": view.recovery_origin_state,
        "recovery_activity_id": view.recovery_activity_id,
        "recovery_tactic": view.recovery_tactic,
        "current_recovery_evidence_id": view.current_recovery_evidence_id,
        "offer_permitted": view.offer_permitted,
        "safe_boundary": view.safe_boundary,
        "generation_installed": view.generation_installed,
        "initial_plan_absent": view.initial_plan_absent,
        "claimed_unfilled_peer": view.claimed_unfilled_peer,
        "panel_complete": view.panel_complete,
        "policy_hash": view.policy_hash,
        "filling_review_slots": list(view.filling_review_slots),
        "unfilled_review_slots": list(view.unfilled_review_slots),
        "activities": [
            {
                "activity_id": activity.activity_id,
                "kind": activity.kind,
                "state": activity.state,
                "specification_generation": activity.specification_generation,
                "candidate_id": activity.candidate_id,
                "current_attempt_id": activity.current_attempt_id,
                "current_attempt_state": activity.current_attempt_state,
                "slot": activity.slot,
            }
            for activity in view.activities
        ],
        "consumed_forge_observation_ids": list(view.consumed_forge_observation_ids),
        "terminal_duplicate_cleanup_active": view.terminal_duplicate_cleanup_active,
    }
    payload.update(dict(reduction.pointer_updates))
    payload.update(dict(extra))
    return payload


def _activity_key(
    *,
    view: RunView,
    planned: PlannedActivity,
    created_transition_sequence: int,
) -> str:
    semantic = bare_canonical_digest(dict(planned.semantic_input))
    return activity_idempotency_digest(
        {
            "reducer_version": view.reducer_version,
            "run_id": view.run_id,
            "specification_generation": view.specification_generation,
            "policy_hash": view.policy_hash,
            "created_transition_sequence": created_transition_sequence,
            "kind": planned.kind,
            "execution_class": planned.execution_class,
            "semantic_input_digest": semantic,
            "candidate_id": planned.candidate_id,
            "forge_observation_id": planned.forge_observation_id,
            "change_request_head_observation_id": None,
            "observed_change_request_head": None,
            "role": planned.role,
            "repair_cycle": planned.repair_cycle,
            "recovery_cycle": planned.recovery_cycle,
            "strategy_index": planned.strategy_index,
            "recovery_tactic": planned.recovery_tactic,
            "recovery_evidence_id": planned.recovery_evidence_id,
            "rescue_epoch": planned.rescue_epoch,
        }
    )


def _replayed(
    store: RunStore,
    run_id: str,
    transition: Any,
) -> AppliedReduction:
    view = load_view(store, run_id)
    if view is None:
        raise RuntimeError(f"run {run_id} missing after transition replay")
    return AppliedReduction(
        transition=transition,
        replayed=True,
        reduction=None,
        view=view,
        planned_activity_ids=tuple(activity.activity_id for activity in view.activities),
    )


def apply(
    store: RunStore,
    view: RunView,
    trigger: Trigger,
    *,
    run_id: str,
    id_factory: _IdFactory | None = None,
) -> AppliedReduction:
    """Reduce ``trigger`` and persist the Transition plus planned work.

    Must run inside ``RunStore.transaction``. Duplicate trigger identity or the
    ADMIT/FORGE_OBSERVATION cross-kind ``(run_id, forge_observation_id)`` identity
    returns the committed Transition without a second semantic effect.
    """
    new_id = id_factory or _new_uuid
    existing = store.get_transition_by_trigger(run_id, trigger.kind, trigger.trigger_id)
    if existing is not None:
        return _replayed(store, run_id, existing)

    forge_ids: list[str] = []
    if trigger.kind == "ADMIT":
        forge_ids.append(trigger.trigger_id)
        base_id = trigger.fact("base_observation_id")
        if base_id:
            forge_ids.append(str(base_id))
    elif trigger.kind == "FORGE_OBSERVATION":
        forge_ids.append(trigger.trigger_id)
    for observation_id in forge_ids:
        prior = store.get_source_unique_record(
            FORGE_OBSERVATION_SOURCE_KIND, f"{run_id}/{observation_id}"
        )
        if prior is not None:
            prior_payload = json.loads(prior.payload_json)
            prior_transition_id = str(prior_payload["transition_id"])
            prior_transition = store.conn.execute(
                "SELECT * FROM transitions WHERE transition_id = ?",
                (prior_transition_id,),
            ).fetchone()
            if prior_transition is not None:
                from orcest.workflow_store.store import _row_to_transition

                return _replayed(store, run_id, _row_to_transition(prior_transition))

    reduction = reduce(view, trigger)
    working = view
    if working.run_id is None:
        store.create_run(
            run_id=run_id,
            project_id=str(reduction.pointer_updates.get("project_id", working.project_id)),
            work_item_key=str(
                reduction.pointer_updates.get("work_item_key", working.work_item_key)
            ),
            state=reduction.next_state,
            reducer_version=working.reducer_version or DEFAULT_REDUCER_VERSION,
            specification_generation=reduction.specification_generation,
        )
        working = RunView(
            run_id=run_id,
            project_id=str(reduction.pointer_updates.get("project_id", working.project_id)),
            work_item_key=str(
                reduction.pointer_updates.get("work_item_key", working.work_item_key)
            ),
            state=reduction.next_state,
            specification_generation=reduction.specification_generation,
            reducer_version=working.reducer_version,
            revision=0,
        )

    assert working.run_id is not None
    input_digest = request_digest(
        {
            "trigger_kind": trigger.kind,
            "trigger_id": trigger.trigger_id,
            "facts": dict(trigger.facts),
            "prior_state": (
                working.prior_state if working.prior_state != PRIOR_STATE_NONE else view.prior_state
            ),
            "reason_code": reduction.reason_code,
        }
    )
    _ = transition_digest(
        {
            "input_digest": input_digest,
            "next_state": reduction.next_state,
            "reason_code": reduction.reason_code,
            "planned_kinds": [item.kind for item in reduction.planned_activities],
        }
    )
    transition = store.append_transition(
        run_id=run_id,
        transition_id=new_id(),
        prior_state=reduction.prior_state,
        trigger_kind=trigger.kind,
        trigger_id=trigger.trigger_id,
        next_state=reduction.next_state,
        reducer_version=working.reducer_version,
        input_digest=input_digest,
        specification_generation=reduction.specification_generation,
        admit_base_observation_id=reduction.admit_base_observation_id,
    )
    if reduction.terminal_outcome is not None:
        store.set_terminal_outcome(run_id, reduction.terminal_outcome)

    planned_ids: list[str] = []
    outbox_ids: list[str] = []
    activities = [
        {
            "activity_id": activity.activity_id,
            "kind": activity.kind,
            "state": "SUPERSEDED"
            if activity.activity_id in reduction.supersede_activity_ids
            else activity.state,
            "specification_generation": activity.specification_generation,
            "candidate_id": activity.candidate_id,
            "current_attempt_id": activity.current_attempt_id,
            "current_attempt_state": activity.current_attempt_state,
            "slot": activity.slot,
        }
        for activity in working.activities
    ]
    for index, planned in enumerate(reduction.planned_activities):
        activity_id = new_id()
        planned_ids.append(activity_id)
        key = _activity_key(
            view=RunView(
                run_id=run_id,
                project_id=working.project_id,
                work_item_key=working.work_item_key,
                state=working.state,
                specification_generation=reduction.specification_generation,
                reducer_version=working.reducer_version,
                policy_hash=working.policy_hash,
            ),
            planned=planned,
            created_transition_sequence=transition.transition_sequence,
        )
        existing_activity = store.get_source_unique_record(
            ACTIVITY_IDEMPOTENCY_SOURCE_KIND, f"{run_id}/{key}"
        )
        if existing_activity is not None:
            activity_id = existing_activity.record_id
            planned_ids[-1] = activity_id
        else:
            store.insert_source_unique_record(
                source_kind=ACTIVITY_IDEMPOTENCY_SOURCE_KIND,
                source_id=f"{run_id}/{key}",
                record_kind="activity",
                record_id=activity_id,
                payload_digest=request_digest({"activity_id": activity_id, "key": key}),
                payload={
                    "activity_id": activity_id,
                    "kind": planned.kind,
                    "state": planned.state,
                    "ordinal": working.next_activity_ordinal + index,
                },
            )
        attempt_id = None
        attempt_generation = None
        if planned.attempt is not None:
            attempt_id = new_id()
            attempt_generation = planned.attempt.generation
            outbox_id = new_id()
            outbox_ids.append(outbox_id)
            payload = {
                "activity_id": activity_id,
                "attempt_id": attempt_id,
                "generation": attempt_generation,
                "kind": planned.kind,
            }
            store.insert_outbox(
                outbox_id=outbox_id,
                source_kind="ACTIVITY",
                source_id=activity_id,
                destination="worker",
                protocol_version=_activity_offer_protocol(),
                payload_digest=request_digest(payload),
                payload=payload,
                next_delivery_at_ms=0,
                attempt_id=attempt_id,
                attempt_generation=attempt_generation,
            )
        elif planned.execution_class == "CONTROLLER" and planned.state == "READY":
            outbox_id = new_id()
            outbox_ids.append(outbox_id)
            payload = {"activity_id": activity_id, "kind": planned.kind}
            store.insert_outbox(
                outbox_id=outbox_id,
                source_kind="ACTIVITY",
                source_id=activity_id,
                destination="controller",
                protocol_version=_activity_offer_protocol(),
                payload_digest=request_digest(payload),
                payload=payload,
                next_delivery_at_ms=0,
            )
        activities.append(
            {
                "activity_id": activity_id,
                "kind": planned.kind,
                "state": planned.state,
                "specification_generation": reduction.specification_generation,
                "candidate_id": planned.candidate_id,
                "current_attempt_id": attempt_id,
                "current_attempt_state": planned.attempt.state if planned.attempt else None,
                "slot": planned.slot,
            }
        )

    continuation_seq = None
    continuation_kind = None
    if reduction.pending_continuation is not None:
        if reduction.pending_continuation.kind == "INTERNAL":
            continuation_seq = transition.transition_sequence
            continuation_kind = "INTERNAL"
        else:
            continuation_kind = reduction.pending_continuation.kind

    extra = {
        "activities": activities,
        "next_activity_ordinal": working.next_activity_ordinal + len(reduction.planned_activities),
        "next_transition_sequence": transition.transition_sequence + 1,
        "pending_internal_sequence": continuation_seq,
        "consumed_forge_observation_ids": list(
            dict.fromkeys(
                [
                    *working.consumed_forge_observation_ids,
                    *reduction.consume_forge_observation_ids,
                    *forge_ids,
                ]
            )
        ),
    }
    if continuation_kind == "SPEC_SUPERSEDE" and reduction.pending_continuation is not None:
        extra["pending_snapshot_id"] = reduction.pending_continuation.trigger_id
    payload = _pointers_payload(working, reduction, extra)
    store.put_revisioned_object(
        object_kind=RUN_POINTERS_KIND,
        object_id=run_id,
        expected_revision=working.revision,
        payload_digest=request_digest(payload),
        payload=payload,
    )
    projection_payload = {
        "run_id": run_id,
        "state": reduction.next_state,
        "reason_code": reduction.reason_code,
    }
    store.insert_projection_outbox(
        projection_outbox_id=new_id(),
        run_id=run_id,
        transition_sequence=transition.transition_sequence,
        kind="RUN_STATUS",
        target_kind="WORK_ITEM",
        target_id=working.work_item_key,
        payload_digest=request_digest(projection_payload),
        payload=projection_payload,
        idempotency_key=f"run-status:{run_id}:{transition.transition_sequence}",
        next_delivery_at_ms=0,
    )
    consumed_ids = extra["consumed_forge_observation_ids"]
    assert isinstance(consumed_ids, list)
    for observation_id in consumed_ids:
        if observation_id in working.consumed_forge_observation_ids:
            continue
        store.insert_source_unique_record(
            source_kind=FORGE_OBSERVATION_SOURCE_KIND,
            source_id=f"{run_id}/{observation_id}",
            record_kind="run_forge_observation",
            record_id=f"{run_id}/{observation_id}",
            payload_digest=request_digest(
                {
                    "run_id": run_id,
                    "observation_id": observation_id,
                    "transition_id": transition.transition_id,
                }
            ),
            payload={
                "run_id": run_id,
                "observation_id": observation_id,
                "transition_id": transition.transition_id,
            },
        )
    loaded = load_view(store, run_id)
    assert loaded is not None
    return AppliedReduction(
        transition=transition,
        replayed=False,
        reduction=reduction,
        view=loaded,
        planned_activity_ids=tuple(planned_ids),
        outbox_ids=tuple(outbox_ids),
    )
