"""Pure Workflow-Control v1 reducer: durable state + one trigger -> Reduction."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orcest.workflow_contract.v1 import enums
from orcest.workflow_reducer.continuation import ContinuationWinner, arbitrate_internal_continuation
from orcest.workflow_reducer.contract import is_legal_pair
from orcest.workflow_reducer.graph import (
    WORKER_ACTIVITY_STATES,
    activity_execution_class,
    is_terminal_state,
)
from orcest.workflow_reducer.types import (
    IllegalTransitionError,
    PendingContinuation,
    PlannedActivity,
    PlannedAttempt,
    Reduction,
    ReductionKind,
    RunView,
    Trigger,
)

__all__ = ["reduce"]

_AUDIT_TRIGGERS = frozenset(
    {
        "ATTEMPT_TERMINAL",
        "BUDGET_REPORT",
        "FORGE_OBSERVATION",
        "FORGE_REQUEST_FAILURE",
        "HEALTH_OBSERVATION",
        "POLICY_UPDATE",
        "SECRET_VERSION",
        "STORAGE_RESTORATION",
        "TIMER_FACT",
    }
)


def reduce(view: RunView, trigger: Trigger) -> Reduction:
    """Apply one persisted trigger to ``view``. Performs no I/O."""
    try:
        enums.parse_enum("transition.trigger_kind", trigger.kind)
    except enums.UnknownEnumValueError as exc:
        raise IllegalTransitionError(f"unknown trigger kind {trigger.kind!r}") from exc
    from_state = view.state
    if not is_legal_pair(from_state, trigger.kind):
        raise IllegalTransitionError(
            f"unlisted pair from_state={from_state!r} trigger={trigger.kind!r} fails closed"
        )
    handler = _HANDLERS[trigger.kind]
    reduction = handler(view, trigger)
    if reduction is None:
        if trigger.kind in _AUDIT_TRIGGERS:
            return _audit(view, trigger, "NO_OP_AUDIT")
        raise IllegalTransitionError(
            f"no matching reduction for from_state={from_state!r} trigger={trigger.kind!r}"
        )
    return _guard_semantic_work(view, trigger, reduction)


def _plan(
    kind: str,
    view: RunView,
    *,
    offer: bool,
    candidate_id: str | None = None,
    forge_observation_id: str | None = None,
    recovery_tactic: str | None = None,
    recovery_evidence_id: str | None = None,
    semantic_input: Mapping[str, Any] | None = None,
    slot: str | None = None,
) -> PlannedActivity:
    execution_class = activity_execution_class(kind)
    attempt = None
    state = "PLANNED"
    if offer and view.offer_permitted and execution_class == "WORKER":
        state = "READY"
        attempt = PlannedAttempt(generation=1, state="OFFERED")
    elif offer and view.offer_permitted and execution_class == "CONTROLLER":
        state = "READY"
    return PlannedActivity(
        kind=kind,
        execution_class=execution_class,
        state=state,
        role=kind.lower(),
        candidate_id=candidate_id if candidate_id is not None else view.current_candidate_id,
        forge_observation_id=forge_observation_id,
        semantic_input=dict(semantic_input or {}),
        recovery_tactic=recovery_tactic,
        recovery_evidence_id=recovery_evidence_id,
        slot=slot,
        attempt=attempt,
    )


def _reduction(
    view: RunView,
    trigger: Trigger,
    *,
    kind: ReductionKind,
    next_state: str,
    reason_code: str,
    plan: str | None = None,
    pointer_updates: Mapping[str, Any] | None = None,
    continuation: PendingContinuation | None = None,
    forge_ids: tuple[str, ...] = (),
    admit_base: str | None = None,
    specification_generation: int | None = None,
    emits_semantic_work: bool | None = None,
    terminal_outcome: str | None = None,
    extra_plans: tuple[PlannedActivity, ...] = (),
    supersede_activity_ids: tuple[str, ...] = (),
) -> Reduction:
    planned: tuple[PlannedActivity, ...] = extra_plans
    if plan is not None:
        planned = (
            _plan(plan, view, offer=True),
            *extra_plans,
        )
    if emits_semantic_work is None:
        emits_semantic_work = bool(planned) or kind is ReductionKind.ADVANCE
    if terminal_outcome is None and is_terminal_state(next_state):
        terminal_outcome = next_state
    generation = (
        view.specification_generation
        if specification_generation is None
        else specification_generation
    )
    if continuation is None and planned and not view.offer_permitted:
        continuation = PendingContinuation(kind="INTERNAL")
        planned = tuple(
            PlannedActivity(
                kind=item.kind,
                execution_class=item.execution_class,
                state="PLANNED",
                role=item.role,
                candidate_id=item.candidate_id,
                forge_observation_id=item.forge_observation_id,
                semantic_input=item.semantic_input,
                repair_cycle=item.repair_cycle,
                recovery_cycle=item.recovery_cycle,
                strategy_index=item.strategy_index,
                recovery_tactic=item.recovery_tactic,
                recovery_evidence_id=item.recovery_evidence_id,
                rescue_epoch=item.rescue_epoch,
                slot=item.slot,
                attempt=None,
            )
            for item in planned
        )
        emits_semantic_work = True
    return Reduction(
        kind=kind,
        prior_state=view.prior_state,
        next_state=next_state,
        reason_code=reason_code,
        specification_generation=generation,
        admit_base_observation_id=admit_base,
        pointer_updates=dict(pointer_updates or {}),
        planned_activities=planned,
        supersede_activity_ids=supersede_activity_ids,
        pending_continuation=continuation,
        consume_forge_observation_ids=forge_ids,
        emits_semantic_work=emits_semantic_work,
        terminal_outcome=terminal_outcome,
        projection_state=next_state,
    )


def _audit(view: RunView, trigger: Trigger, reason_code: str, **kwargs: Any) -> Reduction:
    next_state = view.state if view.state is not None else "ADMITTED"
    forge_ids: tuple[str, ...] = ()
    if trigger.kind in {"ADMIT", "FORGE_OBSERVATION"}:
        forge_ids = (trigger.trigger_id,)
        extra = trigger.fact("base_observation_id")
        if extra:
            forge_ids = (trigger.trigger_id, str(extra))
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.SAME_STATE_AUDIT,
        next_state=next_state,
        reason_code=reason_code,
        forge_ids=kwargs.get("forge_ids", forge_ids),
        emits_semantic_work=False,
        continuation=kwargs.get("continuation"),
        pointer_updates=kwargs.get("pointer_updates"),
        specification_generation=kwargs.get("specification_generation"),
    )


def _stale(view: RunView, trigger: Trigger, reason_code: str = "STALE") -> Reduction:
    next_state = view.state if view.state is not None else "ADMITTED"
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.STALE,
        next_state=next_state,
        reason_code=reason_code,
        emits_semantic_work=False,
    )


def _guard_semantic_work(view: RunView, trigger: Trigger, reduction: Reduction) -> Reduction:
    if trigger.kind == "INTERNAL":
        return reduction
    if not reduction.emits_semantic_work:
        return reduction
    if reduction.terminal_outcome is not None:
        return reduction
    if view.cancellation_pending:
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.SUPERSEDED,
            next_state=view.prior_state,
            reason_code="CANCELLATION_PRECEDENCE",
            continuation=PendingContinuation(kind="INTERNAL"),
            emits_semantic_work=False,
            supersede_activity_ids=tuple(
                activity.activity_id
                for activity in view.activities
                if activity.state not in {"SUCCEEDED", "FAILED", "CANCELLED", "SUPERSEDED"}
            ),
        )
    if view.pending_snapshot_id is not None and view.supersede_requested:
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.SUPERSEDED,
            next_state=view.prior_state,
            reason_code="PENDING_SNAPSHOT_PRECEDENCE",
            continuation=PendingContinuation(
                kind="SPEC_SUPERSEDE", trigger_id=view.pending_snapshot_id
            ),
            emits_semantic_work=False,
        )
    if view.pending_dependency_observation_id is not None and view.safe_boundary:
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.SUPERSEDED,
            next_state=view.prior_state,
            reason_code="PENDING_DEPENDENCY_PRECEDENCE",
            continuation=PendingContinuation(kind="INTERNAL"),
            emits_semantic_work=False,
        )
    return reduction


def _handle_admit(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state is not None:
        return None
    snapshot_id = trigger.fact("snapshot_id")
    base_id = trigger.fact("base_observation_id")
    if not snapshot_id or not base_id:
        return None
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.ADVANCE,
        next_state="ADMITTED",
        reason_code="ADMIT",
        specification_generation=0,
        admit_base=str(base_id),
        pointer_updates={
            "pending_snapshot_id": str(snapshot_id),
            "generation_installed": False,
            "initial_plan_absent": True,
            "project_id": trigger.fact("project_id", view.project_id),
            "work_item_key": trigger.fact("work_item_key", view.work_item_key),
        },
        continuation=PendingContinuation(kind="SPEC_SUPERSEDE", trigger_id=str(snapshot_id)),
        forge_ids=(trigger.trigger_id, str(base_id)),
        emits_semantic_work=False,
    )


def _handle_spec_supersede(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.pending_snapshot_id is None or trigger.trigger_id != view.pending_snapshot_id:
        return None
    if view.state == "ADMITTED" and not view.generation_installed:
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="ADMITTED",
            reason_code="INSTALL_GENERATION",
            specification_generation=1,
            pointer_updates={
                "current_snapshot_id": view.pending_snapshot_id,
                "pending_snapshot_id": None,
                "generation_installed": True,
                "supersede_requested": False,
            },
            continuation=PendingContinuation(kind="INTERNAL"),
            emits_semantic_work=False,
        )
    policy_only = trigger.fact_true("policy_only")
    updates: dict[str, Any] = {
        "current_snapshot_id": view.pending_snapshot_id,
        "pending_snapshot_id": None,
        "supersede_requested": False,
        "generation_installed": True,
        "current_candidate_id": None,
        "wait_condition_id": None,
        "human_boundary_id": None,
    }
    if policy_only and view.current_candidate_id is not None:
        updates["policy_replan_candidate_id"] = view.current_candidate_id
    else:
        updates["policy_replan_candidate_id"] = None
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.ADVANCE,
        next_state="REPLANNING",
        reason_code="SPEC_SUPERSEDE",
        plan="REPLAN",
        specification_generation=view.specification_generation + 1,
        pointer_updates=updates,
        supersede_activity_ids=tuple(
            activity.activity_id
            for activity in view.activities
            if activity.state in {"PLANNED", "READY", "ACTIVE"}
        ),
    )


def _handle_internal(view: RunView, trigger: Trigger) -> Reduction | None:
    try:
        entering = int(trigger.trigger_id)
    except (TypeError, ValueError):
        return None
    if view.pending_internal_sequence is not None and entering != view.pending_internal_sequence:
        if view.latest_staffing_recheck_transition_sequence != entering:
            return None
    winner = arbitrate_internal_continuation(view, entering)
    if winner is ContinuationWinner.CANCELLATION:
        if view.publication_state in {"CHANGE_REQUEST_OBSERVED", "ACTIVE"}:
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="PR_MONITORING",
                reason_code="CANCEL_RECONCILE",
                plan="CLOSE_PUBLICATION",
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="CANCELLED",
            reason_code="CANCEL",
            emits_semantic_work=False,
        )
    if winner is ContinuationWinner.SPEC_SUPERSEDE:
        assert view.pending_snapshot_id is not None
        return _audit(
            view,
            trigger,
            "PENDING_SNAPSHOT_PRECEDENCE",
            continuation=PendingContinuation(
                kind="SPEC_SUPERSEDE", trigger_id=view.pending_snapshot_id
            ),
        )
    if winner is ContinuationWinner.DEPENDENCY:
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="PENDING_DEPENDENCY",
            pointer_updates={
                "recovery_origin_state": view.state,
                "pending_dependency_observation_id": None,
            },
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
            emits_semantic_work=False,
        )
    if winner is ContinuationWinner.STAFFING:
        if view.claimed_unfilled_peer:
            return _audit(view, trigger, "STAFFING_OR_STATE")
        if trigger.fact_true("staffable") or view.offer_permitted:
            if not trigger.fact("no_complete_staffing", False):
                plans = tuple(
                    _plan(
                        "REVIEW" if view.state == "REVIEWING" else "ADJUDICATE",
                        view,
                        offer=True,
                        slot=slot,
                    )
                    for slot in view.unfilled_review_slots or ("default",)
                )
                return _reduction(
                    view,
                    trigger,
                    kind=ReductionKind.ADVANCE,
                    next_state=view.prior_state,
                    reason_code="STAFFING_OR_STATE",
                    extra_plans=plans,
                    pointer_updates={
                        "latest_staffing_recheck_transition_sequence": None,
                        "panel_staffing_kind": None,
                    },
                )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="WAITING",
            reason_code="STAFFING_OR_STATE",
            pointer_updates={
                "wait_reason": "CAPACITY",
                "latest_staffing_recheck_transition_sequence": None,
            },
        )
    return _state_specific_internal(view, trigger)


def _state_specific_internal(view: RunView, trigger: Trigger) -> Reduction | None:
    state = view.state
    if state == "ADMITTED" and view.generation_installed and view.initial_plan_absent:
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="PLANNING",
            reason_code="INITIAL_PLAN",
            plan="PLAN",
            pointer_updates={"initial_plan_absent": False},
        )
    if state == "AGGREGATING":
        outcome = trigger.fact("consensus_outcome", "APPROVED")
        if outcome == "APPROVED":
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="APPROVED",
                reason_code="CONSENSUS_APPROVED",
                continuation=PendingContinuation(kind="INTERNAL"),
                emits_semantic_work=True,
            )
        if outcome == "REMEDIATE":
            if trigger.fact_true("diagnosis_threshold"):
                return _reduction(
                    view,
                    trigger,
                    kind=ReductionKind.ADVANCE,
                    next_state="RECOVERING",
                    reason_code="CONSENSUS_DIAGNOSE",
                    pointer_updates={"recovery_origin_state": "AGGREGATING"},
                    continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
                )
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="REMEDIATING",
                reason_code="CONSENSUS_REMEDIATE",
                plan="REMEDIATE",
            )
        if outcome == "ADJUDICATE":
            if trigger.fact("staffable", True) and view.offer_permitted:
                return _reduction(
                    view,
                    trigger,
                    kind=ReductionKind.ADVANCE,
                    next_state="ADJUDICATING",
                    reason_code="CONSENSUS_ADJUDICATE",
                    plan="ADJUDICATE",
                )
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="WAITING",
                reason_code="ADJUDICATE_CAPACITY",
                extra_plans=(_plan("ADJUDICATE", view, offer=False, slot="default"),),
                pointer_updates={"wait_reason": "CAPACITY"},
            )
        return None
    if state == "APPROVED":
        base_policy = trigger.fact("base_policy", "REBASE_BEFORE_PUBLICATION")
        if trigger.fact_true("base_differs") and base_policy == "REBASE_BEFORE_PUBLICATION":
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="REMEDIATING",
                reason_code="APPROVED_REBASE",
                plan="REBASE",
            )
        if trigger.fact_true("pending_base_supersede"):
            return _audit(
                view,
                trigger,
                "APPROVED_SUPERSEDE_BOUNDARY",
                continuation=PendingContinuation(
                    kind="SPEC_SUPERSEDE", trigger_id=view.pending_snapshot_id
                ),
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="PUBLISHING",
            reason_code="INITIAL_PUBLISH",
            plan="PUBLISH",
            pointer_updates={"publication_state": "PLANNED"},
        )
    if state == "MERGED":
        return _audit(view, trigger, "CLEANUP_CONTINUATION")
    if state in {"REVIEWING", "ADJUDICATING"}:
        return _audit(view, trigger, "STAFFING_OR_STATE")
    return _audit(view, trigger, "INTERNAL_CONTINUATION")


_ATTEMPT_SUCCESS: dict[tuple[str, str], tuple[str, str | None, str]] = {
    ("PLANNING", "PLAN"): ("BUILDING", "BUILD", "PLAN_ACCEPTED"),
    ("BUILDING", "BUILD"): ("VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
    ("REMEDIATING", "REMEDIATE"): ("VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
    ("REMEDIATING", "REBASE"): ("VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
    ("PR_REMEDIATING", "PR_REMEDIATE"): ("VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
    ("PR_REMEDIATING", "REBASE"): ("VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
}


def _handle_attempt_result(view: RunView, trigger: Trigger) -> Reduction | None:
    outcome = trigger.fact("outcome")
    activity_kind = trigger.fact("activity_kind")
    state = view.state
    if state is None or activity_kind is None or outcome is None:
        return None
    if trigger.fact_true("repeated_non_progress"):
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="REPEATED_NON_PROGRESS",
            pointer_updates={"recovery_origin_state": state},
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    if outcome == "SUCCEEDED" and state == "VERIFYING":
        if trigger.fact("verification_outcome") == "FAIL":
            if trigger.fact_true("diagnosis_threshold"):
                return _reduction(
                    view,
                    trigger,
                    kind=ReductionKind.ADVANCE,
                    next_state="RECOVERING",
                    reason_code="VERIFICATION_FAILURE_DIAGNOSE",
                    pointer_updates={"recovery_origin_state": "VERIFYING"},
                    continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
                )
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="REMEDIATING",
                reason_code="VERIFICATION_FAIL",
                plan="REMEDIATE",
            )
        if trigger.fact("verification_outcome", "PASS") == "PASS":
            if trigger.fact("staffable", True):
                return _reduction(
                    view,
                    trigger,
                    kind=ReductionKind.ADVANCE,
                    next_state="REVIEWING",
                    reason_code="VERIFY_PASS",
                    extra_plans=tuple(
                        _plan("REVIEW", view, offer=True, slot=slot)
                        for slot in (view.unfilled_review_slots or ("slot-a", "slot-b"))
                    ),
                )
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="WAITING",
                reason_code="REVIEW_CAPACITY",
                pointer_updates={"wait_reason": "CAPACITY"},
            )
        return None
    if outcome == "SUCCEEDED" and state == "REVIEWING":
        if trigger.fact_true("panel_complete") or view.panel_complete:
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="AGGREGATING",
                reason_code="PANEL_COMPLETE",
                continuation=PendingContinuation(kind="INTERNAL"),
                emits_semantic_work=True,
            )
        if trigger.fact("no_capacity") and not view.claimed_unfilled_peer:
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="WAITING",
                reason_code="REVIEW_CAPACITY",
                pointer_updates={"wait_reason": "CAPACITY"},
            )
        if view.claimed_unfilled_peer:
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.SAME_STATE_AUDIT,
                next_state="REVIEWING",
                reason_code="PANEL_RECEIPT",
                pointer_updates={
                    "latest_staffing_recheck_transition_sequence": view.next_transition_sequence
                },
                emits_semantic_work=False,
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.SAME_STATE_AUDIT,
            next_state="REVIEWING",
            reason_code="PANEL_RECEIPT",
            emits_semantic_work=False,
        )
    if outcome == "SUCCEEDED" and state == "ADJUDICATING":
        disposition = trigger.fact("disposition")
        if disposition == "SUSTAIN":
            if trigger.fact_true("diagnosis_threshold"):
                return _reduction(
                    view,
                    trigger,
                    kind=ReductionKind.ADVANCE,
                    next_state="RECOVERING",
                    reason_code="ADJUDICATION_DIAGNOSE",
                    continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
                )
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="REMEDIATING",
                reason_code="ADJUDICATION_SUSTAIN",
                plan="REMEDIATE",
            )
        if disposition == "OVERRULE":
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="REVIEWING",
                reason_code="ADJUDICATION_OVERRULE",
                extra_plans=tuple(
                    _plan("REVIEW", view, offer=True, slot=slot)
                    for slot in (view.unfilled_review_slots or ("slot-a", "slot-b"))
                ),
            )
        if disposition in {"INCONCLUSIVE", "ABSTAIN"}:
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="RECOVERING",
                reason_code="ADJUDICATION_INCONCLUSIVE",
                continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
            )
        return None
    if outcome == "SUCCEEDED" and state == "DIAGNOSING":
        if trigger.fact("plan_assessment") == "INVALID" or trigger.fact_true("replan_threshold"):
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="RECOVERING",
                reason_code="DIAGNOSIS_REPLAN",
                continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="DIAGNOSIS_ACCEPTED",
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    if outcome == "SUCCEEDED" and state == "REPLANNING":
        if view.policy_replan_candidate_id is not None and trigger.fact(
            "policy_identity_holds", True
        ):
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="VERIFYING",
                reason_code="POLICY_REPLAN_RESELECT",
                plan="VERIFY",
                pointer_updates={
                    "current_candidate_id": view.policy_replan_candidate_id,
                    "policy_replan_candidate_id": None,
                },
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="BUILDING",
            reason_code="REPLAN_ACCEPTED",
            plan="BUILD",
            pointer_updates={"policy_replan_candidate_id": None},
        )
    if outcome == "SUCCEEDED":
        mapped = _ATTEMPT_SUCCESS.get((state, str(activity_kind)))
        if mapped is None:
            return None
        next_state, plan, reason = mapped
        updates: dict[str, Any] = {}
        if plan == "VERIFY":
            updates["current_candidate_id"] = trigger.fact(
                "candidate_id", view.current_candidate_id or "candidate-1"
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state=next_state,
            reason_code=reason,
            plan=plan,
            pointer_updates=updates,
        )
    if outcome in {"FAILED_RETRYABLE", "FAILED_PERMANENT", "ABSTAINED"}:
        if state == "VERIFYING" and trigger.fact("failure_class") == "VERIFICATION_ERROR":
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="RECOVERING",
                reason_code="VERIFICATION_ERROR",
                pointer_updates={"recovery_origin_state": "VERIFYING"},
                continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="RECOVERABLE_FAILURE",
            pointer_updates={"recovery_origin_state": state},
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    return None


def _handle_attempt_terminal(view: RunView, trigger: Trigger) -> Reduction | None:
    if trigger.fact_true("already_terminal") or trigger.fact("kind") == "RESULT_AFTER_TERMINAL":
        return _audit(view, trigger, "ALREADY_TERMINAL_AUDIT")
    state = view.state
    if state not in WORKER_ACTIVITY_STATES:
        return _audit(view, trigger, "ALREADY_TERMINAL_AUDIT")
    if (
        state in {"REVIEWING", "ADJUDICATING"}
        and trigger.fact("kind") == "CLAIM_DEADLINE"
        and view.claimed_unfilled_peer
    ):
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.SAME_STATE_AUDIT,
            next_state=state,
            reason_code="PANEL_CLAIM_DEADLINE",
            pointer_updates={
                "latest_staffing_recheck_transition_sequence": view.next_transition_sequence
            },
            emits_semantic_work=False,
        )
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.ADVANCE,
        next_state="RECOVERING",
        reason_code="ATTEMPT_TERMINAL",
        pointer_updates={"recovery_origin_state": state},
        continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
    )


def _handle_controller_operation(view: RunView, trigger: Trigger) -> Reduction | None:
    outcome = trigger.fact("outcome")
    kind = trigger.fact("activity_kind")
    if (
        outcome == "SUCCEEDED"
        and kind == "IMPORT"
        and view.state in {"REMEDIATING", "PR_REMEDIATING"}
    ):
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="VERIFYING",
            reason_code="IMPORT_ACCEPTED",
            plan="VERIFY",
        )
    if outcome == "SUCCEEDED" and kind == "CLOSE_PUBLICATION":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="CANCELLED",
            reason_code="CLOSE_PUBLICATION",
            emits_semantic_work=False,
        )
    if outcome == "FAILED":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="CONTROLLER_OPERATION_FAILED",
            pointer_updates={"recovery_origin_state": view.state},
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    return None


def _handle_forge_request_failure(view: RunView, trigger: Trigger) -> Reduction | None:
    if not trigger.fact("request_pending", True):
        return _stale(view, trigger)
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.ADVANCE,
        next_state="RECOVERING",
        reason_code="FORGE_TRANSIENT",
        pointer_updates={"recovery_origin_state": view.state},
        continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
    )


def _handle_forge_observation(view: RunView, trigger: Trigger) -> Reduction | None:
    obs_kind = trigger.fact("kind")
    state = view.state
    if trigger.fact_true("merged") or obs_kind == "CHANGE_REQUEST_MERGED":
        if state is not None and not is_terminal_state(state):
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="MERGED",
                reason_code="MERGED",
                forge_ids=(trigger.trigger_id,),
                emits_semantic_work=False,
            )
    if trigger.fact_true("closed_unmerged") or (
        obs_kind == "CHANGE_REQUEST_CLOSED" and not trigger.fact_true("redundant_cleanup")
    ):
        if state == "MERGED":
            return _audit(view, trigger, "CLEANUP_OBSERVATION", forge_ids=(trigger.trigger_id,))
        if (
            state is not None
            and not is_terminal_state(state)
            and not view.cancellation_pending
            and not trigger.fact_true("cleanup_action")
        ):
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state="CLOSED",
                reason_code="CLOSED",
                forge_ids=(trigger.trigger_id,),
                emits_semantic_work=False,
            )
    if trigger.fact_true("head_advanced") and state is not None and not is_terminal_state(state):
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.SUPERSEDED,
            next_state="PR_REMEDIATING",
            reason_code="HEAD_ADVANCED",
            plan="IMPORT",
            forge_ids=(trigger.trigger_id,),
        )
    if obs_kind == "DEPENDENCY_STATE":
        if trigger.fact_true("satisfied"):
            return _audit(
                view,
                trigger,
                "DEPENDENCY_CLEARED",
                pointer_updates={"pending_dependency_observation_id": None},
                forge_ids=(trigger.trigger_id,),
            )
        return _audit(
            view,
            trigger,
            "DEPENDENCY_PENDING",
            pointer_updates={"pending_dependency_observation_id": trigger.trigger_id},
            continuation=PendingContinuation(kind="INTERNAL"),
            forge_ids=(trigger.trigger_id,),
        )
    if trigger.fact_true("supersession_key_changed"):
        snapshot_id = str(trigger.fact("snapshot_id", "pending-snapshot"))
        return _audit(
            view,
            trigger,
            "SNAPSHOT_CAPTURE",
            pointer_updates={
                "pending_snapshot_id": snapshot_id,
                "supersede_requested": not view.safe_boundary,
            },
            continuation=PendingContinuation(kind="SPEC_SUPERSEDE", trigger_id=snapshot_id)
            if view.safe_boundary
            else PendingContinuation(kind="INTERNAL"),
            forge_ids=(trigger.trigger_id,),
        )
    if state == "MERGED":
        return _audit(view, trigger, "CLEANUP_OBSERVATION", forge_ids=(trigger.trigger_id,))
    if state == "WAITING" and trigger.fact_true("wakes_wait"):
        return _wake(view, trigger)
    return _audit(view, trigger, "NO_OP_AUDIT", forge_ids=(trigger.trigger_id,))


def _wake(view: RunView, trigger: Trigger) -> Reduction:
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.ADVANCE,
        next_state="RECOVERING",
        reason_code="WAIT_WAKE",
        pointer_updates={
            "wait_condition_id": None,
            "recovery_origin_state": view.recovery_origin_state or view.state,
        },
        continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        forge_ids=(trigger.trigger_id,) if trigger.kind == "FORGE_OBSERVATION" else (),
    )


def _handle_health_observation(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state == "WAITING" and trigger.fact_true("wakes_wait"):
        return _wake(view, trigger)
    if (
        trigger.fact_true("integrity_unavailable")
        and view.state is not None
        and not is_terminal_state(view.state)
    ):
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="INTEGRITY_UNAVAILABLE",
            pointer_updates={"recovery_origin_state": view.state},
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    return _stale(view, trigger, "STALE_FANOUT")


def _handle_budget_report(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state == "WAITING" and trigger.fact_true("wakes_wait"):
        return _wake(view, trigger)
    return _stale(view, trigger, "STALE_FANOUT")


def _handle_management_command(view: RunView, trigger: Trigger) -> Reduction | None:
    kind = trigger.fact("kind", "CANCEL")
    if kind == "RESOLVE_HUMAN_BOUNDARY":
        if view.state != "NEEDS_HUMAN":
            return _stale(view, trigger)
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="HUMAN_RESOLUTION",
            pointer_updates={
                "human_boundary_id": None,
                "recovery_origin_state": view.recovery_origin_state,
            },
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    if kind != "CANCEL":
        return None
    if view.cancellation_pending:
        return _audit(view, trigger, "CANCEL_ALREADY_PENDING")
    if view.state in {"PR_MONITORING", "PR_REMEDIATING"} or view.publication_state in {
        "CHANGE_REQUEST_OBSERVED",
        "ACTIVE",
    }:
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="PR_MONITORING",
            reason_code="CANCEL_RECONCILE",
            plan="CLOSE_PUBLICATION",
            pointer_updates={
                "cancellation_source_kind": "MANAGEMENT_COMMAND",
                "cancellation_source_id": trigger.trigger_id,
            },
        )
    return _reduction(
        view,
        trigger,
        kind=ReductionKind.ADVANCE,
        next_state="CANCELLED",
        reason_code="CANCEL",
        pointer_updates={
            "cancellation_source_kind": "MANAGEMENT_COMMAND",
            "cancellation_source_id": trigger.trigger_id,
        },
        emits_semantic_work=False,
    )


def _handle_policy_update(view: RunView, trigger: Trigger) -> Reduction | None:
    snapshot_id = str(trigger.fact("snapshot_id", trigger.trigger_id))
    return _audit(
        view,
        trigger,
        "SNAPSHOT_CAPTURE",
        pointer_updates={
            "pending_snapshot_id": snapshot_id,
            "supersede_requested": not view.safe_boundary,
        },
        continuation=PendingContinuation(kind="SPEC_SUPERSEDE", trigger_id=snapshot_id)
        if view.safe_boundary
        else PendingContinuation(kind="INTERNAL"),
    )


def _handle_publication_checkpoint(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state != "PUBLISHING":
        return None
    return _audit(view, trigger, "AMBIGUOUS_CHECKPOINT")


def _handle_reconciliation_fact(view: RunView, trigger: Trigger) -> Reduction | None:
    fact_kind = trigger.fact("kind")
    if fact_kind == "EFFECT_PRESENT" and view.state == "RECOVERING":
        origin = view.recovery_origin_state or "PLANNING"
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state=origin,
            reason_code="EFFECT_PRESENT",
            pointer_updates={"recovery_origin_state": None},
        )
    if fact_kind == "OWNERSHIP_CONFLICT":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="NEEDS_HUMAN",
            reason_code="OWNERSHIP_CONFLICT",
            pointer_updates={"human_boundary_reason": "PUBLICATION_OWNERSHIP_CONFLICT"},
        )
    if fact_kind in {"REDUNDANT_PUBLICATIONS_PROVEN", "NO_ACTIONABLE_DUPLICATE"}:
        return _audit(view, trigger, "RECONCILE")
    if fact_kind == "EFFECT_ABSENT" and view.state == "RECOVERING":
        origin = view.recovery_origin_state or "PLANNING"
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state=origin,
            reason_code="EFFECT_ABSENT",
        )
    if fact_kind == "PRELINK_REF_IMPORTABLE":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="REMEDIATING",
            reason_code="PRELINK_IMPORT",
            plan="IMPORT",
        )
    return _audit(view, trigger, "RECONCILE") if fact_kind else None


def _handle_recovery_evidence(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state != "RECOVERING":
        return None
    tactic = trigger.fact("selected_tactic", view.recovery_tactic)
    origin = view.recovery_origin_state or "PLANNING"
    if tactic == "RETRY_EXECUTION":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state=origin,
            reason_code="RETRY_EXECUTION",
            pointer_updates={"recovery_origin_state": None, "recovery_tactic": None},
        )
    if tactic == "DIAGNOSE":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="DIAGNOSING",
            reason_code="DIAGNOSE",
            plan="DIAGNOSE",
        )
    if tactic == "REPLAN":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="REPLANNING",
            reason_code="REPLAN",
            plan="REPLAN",
        )
    if tactic in {
        "WAIT_CAPACITY",
        "WAIT_BACKOFF",
        "WAIT_RATE_LIMIT",
        "WAIT_BUDGET",
        "WAIT_EXTERNAL",
        "WAIT_EVIDENCE",
    }:
        if tactic == "WAIT_EVIDENCE" and trigger.fact_true("predicate_already_met"):
            return _audit(
                view,
                trigger,
                "WAIT_EVIDENCE_ALREADY_MET",
                continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
            )
        reason = {
            "WAIT_CAPACITY": "CAPACITY",
            "WAIT_BACKOFF": "BACKOFF",
            "WAIT_RATE_LIMIT": "RATE_LIMIT",
            "WAIT_BUDGET": "BUDGET",
            "WAIT_EXTERNAL": "FORGE_UNAVAILABLE",
            "WAIT_EVIDENCE": "EVIDENCE",
        }[str(tactic)]
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="WAITING",
            reason_code=str(tactic),
            pointer_updates={"wait_reason": reason},
        )
    if tactic == "ENTER_HUMAN_BOUNDARY":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="NEEDS_HUMAN",
            reason_code="ENTER_HUMAN_BOUNDARY",
        )
    if tactic == "STAFF_PANEL":
        origin_panel = origin if origin in {"REVIEWING", "ADJUDICATING"} else "REVIEWING"
        if trigger.fact_true("complete_staffing") or trigger.fact("complete_staffing", True):
            plan_kind = "REVIEW" if origin_panel == "REVIEWING" else "ADJUDICATE"
            return _reduction(
                view,
                trigger,
                kind=ReductionKind.ADVANCE,
                next_state=origin_panel,
                reason_code="STAFF_PANEL",
                extra_plans=(_plan(plan_kind, view, offer=True, slot="default"),),
            )
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="WAITING",
            reason_code="STAFF_PANEL",
            pointer_updates={"wait_reason": "CAPACITY"},
        )
    if tactic == "PROBE_INTEGRITY":
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.SAME_STATE_AUDIT,
            next_state="RECOVERING",
            reason_code="PROBE_INTEGRITY",
            emits_semantic_work=False,
        )
    return None


def _handle_secret_version(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state == "NEEDS_HUMAN" and trigger.fact_true("satisfies_boundary"):
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="HUMAN_RESOLUTION",
            pointer_updates={"human_boundary_id": None},
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    if view.state == "WAITING" and trigger.fact_true("wakes_wait"):
        return _wake(view, trigger)
    return _stale(view, trigger, "STALE_FANOUT")


def _handle_storage_restoration(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state == "NEEDS_HUMAN" and trigger.fact_true("matches_object"):
        return _reduction(
            view,
            trigger,
            kind=ReductionKind.ADVANCE,
            next_state="RECOVERING",
            reason_code="HUMAN_RESOLUTION",
            pointer_updates={"human_boundary_id": None},
            continuation=PendingContinuation(kind="RECOVERY_EVIDENCE"),
        )
    if view.state == "WAITING" and trigger.fact_true("wakes_wait"):
        return _wake(view, trigger)
    return _stale(view, trigger, "STALE_FANOUT")


def _handle_timer_fact(view: RunView, trigger: Trigger) -> Reduction | None:
    if view.state == "WAITING" and trigger.fact("wait_current", True):
        return _wake(view, trigger)
    return _stale(view, trigger, "STALE_FANOUT")


_HANDLERS = {
    "ADMIT": _handle_admit,
    "INTERNAL": _handle_internal,
    "ATTEMPT_RESULT": _handle_attempt_result,
    "ATTEMPT_TERMINAL": _handle_attempt_terminal,
    "CONTROLLER_OPERATION": _handle_controller_operation,
    "FORGE_REQUEST_FAILURE": _handle_forge_request_failure,
    "FORGE_OBSERVATION": _handle_forge_observation,
    "HEALTH_OBSERVATION": _handle_health_observation,
    "BUDGET_REPORT": _handle_budget_report,
    "MANAGEMENT_COMMAND": _handle_management_command,
    "POLICY_UPDATE": _handle_policy_update,
    "PUBLICATION_CHECKPOINT": _handle_publication_checkpoint,
    "RECONCILIATION_FACT": _handle_reconciliation_fact,
    "RECOVERY_EVIDENCE": _handle_recovery_evidence,
    "SECRET_VERSION": _handle_secret_version,
    "SPEC_SUPERSEDE": _handle_spec_supersede,
    "STORAGE_RESTORATION": _handle_storage_restoration,
    "TIMER_FACT": _handle_timer_fact,
}


def _assert_handlers_cover_registry() -> None:
    missing = set(enums.TransitionTrigger.__members__) - set(_HANDLERS)
    extra = set(_HANDLERS) - set(enums.TransitionTrigger.__members__)
    if missing or extra:
        raise RuntimeError(
            f"reducer handlers drifted from registry: missing={missing} extra={extra}"
        )


_assert_handlers_cover_registry()
