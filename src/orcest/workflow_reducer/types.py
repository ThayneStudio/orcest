"""Pure reducer types for Workflow-Control v1."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from orcest.workflow_store.store import DEFAULT_REDUCER_VERSION, PRIOR_STATE_NONE

__all__ = [
    "PRIOR_STATE_NONE",
    "ActivityView",
    "AppliedReduction",
    "IllegalTransitionError",
    "PendingContinuation",
    "PlannedActivity",
    "PlannedAttempt",
    "Reduction",
    "ReductionKind",
    "RunView",
    "Trigger",
]


class IllegalTransitionError(ValueError):
    """Raised when a (state, trigger) pair is unlisted or cannot fail closed."""


class ReductionKind(str, Enum):
    ADVANCE = "ADVANCE"
    SAME_STATE_AUDIT = "SAME_STATE_AUDIT"
    STALE = "STALE"
    SUPERSEDED = "SUPERSEDED"
    DUPLICATE = "DUPLICATE"


@dataclass(frozen=True, slots=True)
class ActivityView:
    activity_id: str
    kind: str
    state: str
    specification_generation: int = 1
    candidate_id: str | None = None
    current_attempt_id: str | None = None
    current_attempt_state: str | None = None
    slot: str | None = None


@dataclass(frozen=True, slots=True)
class RunView:
    """Durable Run projection plus reducer-visible work, with no I/O."""

    run_id: str | None
    project_id: str
    work_item_key: str
    state: str | None
    specification_generation: int = 0
    reducer_version: str = DEFAULT_REDUCER_VERSION
    current_snapshot_id: str | None = None
    pending_snapshot_id: str | None = None
    supersede_requested: bool = False
    current_candidate_id: str | None = None
    policy_replan_candidate_id: str | None = None
    publication_id: str | None = None
    publication_state: str | None = None
    change_request_external_id: str | None = None
    next_activity_ordinal: int = 1
    next_transition_sequence: int = 1
    pending_internal_sequence: int | None = None
    cancellation_source_kind: str | None = None
    cancellation_source_id: str | None = None
    pending_dependency_observation_id: str | None = None
    panel_staffing_kind: str | None = None
    latest_staffing_recheck_transition_sequence: int | None = None
    wait_condition_id: str | None = None
    wait_reason: str | None = None
    human_boundary_id: str | None = None
    human_boundary_reason: str | None = None
    recovery_origin_state: str | None = None
    recovery_activity_id: str | None = None
    recovery_tactic: str | None = None
    current_recovery_evidence_id: str | None = None
    offer_permitted: bool = True
    safe_boundary: bool = True
    generation_installed: bool = False
    initial_plan_absent: bool = True
    claimed_unfilled_peer: bool = False
    panel_complete: bool = False
    policy_hash: str = "sha256:" + ("0" * 64)
    filling_review_slots: tuple[str, ...] = ()
    unfilled_review_slots: tuple[str, ...] = ()
    activities: tuple[ActivityView, ...] = ()
    consumed_forge_observation_ids: tuple[str, ...] = ()
    terminal_duplicate_cleanup_active: bool = False
    revision: int = 0

    @property
    def cancellation_pending(self) -> bool:
        return self.cancellation_source_kind is not None

    @property
    def prior_state(self) -> str:
        return PRIOR_STATE_NONE if self.state is None else self.state


@dataclass(frozen=True, slots=True)
class Trigger:
    kind: str
    trigger_id: str
    facts: Mapping[str, Any] = field(default_factory=dict)

    def fact(self, name: str, default: Any = None) -> Any:
        return self.facts.get(name, default)

    def fact_true(self, name: str) -> bool:
        return bool(self.facts.get(name))


@dataclass(frozen=True, slots=True)
class PlannedAttempt:
    generation: int
    state: str = "OFFERED"


@dataclass(frozen=True, slots=True)
class PlannedActivity:
    kind: str
    execution_class: str
    state: str
    role: str | None = None
    candidate_id: str | None = None
    forge_observation_id: str | None = None
    semantic_input: Mapping[str, Any] = field(default_factory=dict)
    repair_cycle: int = 0
    recovery_cycle: int = 0
    strategy_index: int = 0
    recovery_tactic: str | None = None
    recovery_evidence_id: str | None = None
    rescue_epoch: int = 0
    slot: str | None = None
    attempt: PlannedAttempt | None = None


@dataclass(frozen=True, slots=True)
class PendingContinuation:
    kind: str
    trigger_id: str | None = None


@dataclass(frozen=True, slots=True)
class Reduction:
    kind: ReductionKind
    prior_state: str
    next_state: str
    reason_code: str
    specification_generation: int
    admit_base_observation_id: str | None = None
    pointer_updates: Mapping[str, Any] = field(default_factory=dict)
    planned_activities: tuple[PlannedActivity, ...] = ()
    supersede_activity_ids: tuple[str, ...] = ()
    pending_continuation: PendingContinuation | None = None
    consume_forge_observation_ids: tuple[str, ...] = ()
    emits_semantic_work: bool = False
    terminal_outcome: str | None = None
    projection_state: str | None = None

    @property
    def same_state(self) -> bool:
        return self.prior_state == self.next_state


@dataclass(frozen=True, slots=True)
class AppliedReduction:
    transition: Any
    replayed: bool
    reduction: Reduction | None
    view: RunView
    planned_activity_ids: tuple[str, ...] = ()
    outbox_ids: tuple[str, ...] = ()
