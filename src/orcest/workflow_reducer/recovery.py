"""Deterministic Workflow-Control v1 recovery evidence and tactic selection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.digest import bare_canonical_digest, request_digest
from orcest.workflow_reducer.human_boundary import CATEGORY_HUMAN_BOUNDARY_REASONS

__all__ = [
    "DEFAULT_RECOVERY_LIMITS",
    "HealthObservationRef",
    "RecoveryDecision",
    "RecoveryEvidenceInput",
    "RecoveryLimits",
    "classify_recovery_category",
    "failure_fingerprint",
    "select_recovery_decision",
]


@dataclass(frozen=True, slots=True)
class RecoveryLimits:
    """Pinned numeric recovery limits from effective policy."""

    max_attempts_per_activity_before_diagnosis: int = 3
    max_repair_cycles_before_diagnosis: int = 4
    max_diagnoses_before_replan: int = 2
    max_alternative_candidates_per_rescue_epoch: int = 2
    backoff_initial_ms: int = 1_800_000
    backoff_max_ms: int = 86_400_000
    max_provider_rate_limit_wait_ms: int = 86_400_000


DEFAULT_RECOVERY_LIMITS = RecoveryLimits()


@dataclass(frozen=True, slots=True)
class HealthObservationRef:
    health_observation_id: str
    scope_kind: str
    scope_id: str
    health_sequence: int

    @property
    def sort_key(self) -> tuple[str, str, int, str]:
        return (self.scope_kind, self.scope_id, self.health_sequence, self.health_observation_id)


@dataclass(frozen=True, slots=True)
class RecoveryEvidenceInput:
    source_kind: str
    source_id: str
    category: str
    activity_id: str | None = None
    attempt_id: str | None = None
    specification_generation: int = 0
    candidate_id: str | None = None
    forge_observation_id: str | None = None
    failure_scope: Mapping[str, Any] | None = None
    bounded_evidence: Mapping[str, Any] | None = None
    prior_attempt_count: int = 0
    prior_repair_cycle_count: int = 0
    prior_diagnosis_count: int = 0
    rescue_epoch: int = 0
    accepted_at_ms: int = 0
    provider_retry_after_ms: int | None = None
    fallback_order: tuple[str, ...] = ()
    exhausted_autonomous: bool = False
    resumed_wait_condition_id: str | None = None
    resumed_human_boundary_id: str | None = None
    human_resolution_id: str | None = None
    human_boundary_reason: str | None = None


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    category: str
    failure_fingerprint: str
    strategy_index: int
    selected_tactic: str
    attempt_count: int
    repair_cycle_count: int
    diagnosis_count: int
    rescue_epoch: int
    selected_fallback: str | None
    ordered_health_observation_ids: tuple[str, ...]
    health_observation_ids_digest: str
    next_eligible_at_ms: int | None
    human_boundary_reason: str | None = None


_SOURCE_CATEGORIES: Mapping[str, frozenset[str]] = {
    "ATTEMPT_RESULT": frozenset(
        {
            "WORKER_LOST",
            "PROVIDER_TRANSIENT",
            "PROVIDER_RATE_LIMIT",
            "INVALID_RESULT",
            "CREDENTIAL",
            "SOURCE_READ",
            "VERIFICATION_ERROR",
            "VERIFICATION_FAILURE",
            "REPEATED_NON_PROGRESS",
            "REVIEW_DISAGREEMENT",
            "BASE_CONFLICT",
            "POLICY",
        }
    ),
    "ATTEMPT_TERMINAL": frozenset({"WORKER_LOST", "TIMEOUT", "CAPACITY"}),
    "CONTROLLER_OPERATION": frozenset(
        {"SOURCE_READ", "BASE_CONFLICT", "CREDENTIAL", "STORAGE", "INTEGRITY_SUSPECTED", "POLICY"}
    ),
    "FORGE_REQUEST_FAILURE": frozenset({"FORGE_TRANSIENT", "PROVIDER_RATE_LIMIT"}),
    "HEALTH_OBSERVATION": frozenset(
        {
            "CAPACITY",
            "WORKER_LOST",
            "PROVIDER_RATE_LIMIT",
            "STORAGE",
            "CREDENTIAL",
            "INTEGRITY_SUSPECTED",
        }
    ),
    "FORGE_OBSERVATION": frozenset(
        {"EXTERNAL_DEPENDENCY", "BASE_CONFLICT", "REVIEW_DISAGREEMENT", "FORGE_TRANSIENT"}
    ),
    "BUDGET_REPORT": frozenset({"BUDGET"}),
    "RECONCILIATION_FACT": frozenset({"FORGE_TRANSIENT", "INTEGRITY_SUSPECTED", "POLICY"}),
    "RECOVERY_EVIDENCE": frozenset({"REPEATED_NON_PROGRESS"}),
    "POLICY_UPDATE": frozenset({"POLICY", "EXTERNAL_DEPENDENCY"}),
    "STORAGE_RESTORATION": frozenset({"STORAGE", "INTEGRITY_SUSPECTED"}),
    "SECRET_VERSION": frozenset({"CREDENTIAL"}),
    "MANAGEMENT_COMMAND": frozenset({"CREDENTIAL", "POLICY", "EXTERNAL_DEPENDENCY"}),
    "TIMER_FACT": frozenset(
        {"CAPACITY", "BUDGET", "PROVIDER_RATE_LIMIT", "FORGE_TRANSIENT", "REPEATED_NON_PROGRESS"}
    ),
    "INTERNAL": frozenset({"EXTERNAL_DEPENDENCY", "BUDGET", "CAPACITY", "REVIEW_DISAGREEMENT"}),
}


_FAILURE_CLASS_CATEGORY: Mapping[str, str] = {
    "INFRASTRUCTURE": "WORKER_LOST",
    "PROVIDER_UNAVAILABLE": "PROVIDER_TRANSIENT",
    "PROVIDER_RATE_LIMIT": "PROVIDER_RATE_LIMIT",
    "INCOMPATIBLE_WORKER": "CAPACITY",
    "INVALID_AGENT_OUTPUT": "INVALID_RESULT",
    "VALIDATION_FAILURE": "INVALID_RESULT",
    "CREDENTIAL_UNAVAILABLE": "CREDENTIAL",
    "SOURCE_READ_FAILED": "SOURCE_READ",
    "VERIFICATION_ERROR": "VERIFICATION_ERROR",
    "BASE_CONFLICT": "BASE_CONFLICT",
    "POLICY_DENIED": "POLICY",
    "SPECIFICATION_CONFLICT": "POLICY",
    "MISSING_AUTHORITY": "POLICY",
    "INTEGRITY_FAILURE": "INTEGRITY_SUSPECTED",
}


_CONTROLLER_FAILURE_CATEGORY: Mapping[str, str] = {
    "SOURCE_READ": "SOURCE_READ",
    "BASE_CONFLICT": "BASE_CONFLICT",
    "CREDENTIAL": "CREDENTIAL",
    "STORAGE": "STORAGE",
    "INTEGRITY_SUSPECTED": "INTEGRITY_SUSPECTED",
    "POLICY": "POLICY",
}


def classify_recovery_category(source_kind: str, facts: Mapping[str, Any]) -> str:
    """Map one accepted failure source to the closed recovery category set."""
    enums.parse_enum("recovery_evidence.source_kind", source_kind)
    if source_kind == "ATTEMPT_RESULT":
        if facts.get("repeated_non_progress"):
            return "REPEATED_NON_PROGRESS"
        if facts.get("verification_outcome") == "FAIL":
            return "VERIFICATION_FAILURE"
        failure_class = facts.get("failure_class")
        if isinstance(failure_class, str) and failure_class in _FAILURE_CLASS_CATEGORY:
            return _FAILURE_CLASS_CATEGORY[failure_class]
        if facts.get("outcome") == "ABSTAINED":
            return "REVIEW_DISAGREEMENT"
    elif source_kind == "ATTEMPT_TERMINAL":
        kind = facts.get("kind")
        if kind == "WORKER_LOST":
            return "WORKER_LOST"
        if kind in {"CLAIM_DEADLINE", "EXECUTION_DEADLINE"}:
            return "TIMEOUT"
    elif source_kind == "CONTROLLER_OPERATION":
        category = facts.get("failure_category")
        if isinstance(category, str) and category in _CONTROLLER_FAILURE_CATEGORY:
            return _CONTROLLER_FAILURE_CATEGORY[category]
    elif source_kind == "FORGE_REQUEST_FAILURE":
        kind = facts.get("failure_kind")
        if kind == "RATE_LIMIT":
            return "PROVIDER_RATE_LIMIT"
        if kind in {"TIMEOUT", "UNAVAILABLE"}:
            return "FORGE_TRANSIENT"
    elif source_kind == "HEALTH_OBSERVATION":
        if facts.get("integrity_unavailable"):
            return "INTEGRITY_SUSPECTED"
        kind = facts.get("kind")
        if kind == "LOST":
            return "WORKER_LOST"
        if kind == "RATE_LIMITED":
            return "PROVIDER_RATE_LIMIT"
        if kind in {"UNAVAILABLE", "EXHAUSTED"}:
            return "CAPACITY"
    elif source_kind == "FORGE_OBSERVATION":
        kind = facts.get("kind")
        if kind == "DEPENDENCY_STATE":
            return "EXTERNAL_DEPENDENCY"
        if facts.get("head_advanced") or facts.get("merge_conflict"):
            return "BASE_CONFLICT"
    elif source_kind == "BUDGET_REPORT":
        if facts.get("availability") == "EXHAUSTED":
            return "BUDGET"
    elif source_kind == "RECONCILIATION_FACT":
        if facts.get("kind") == "OWNERSHIP_CONFLICT":
            return "POLICY"
        return "FORGE_TRANSIENT"
    elif source_kind == "RECOVERY_EVIDENCE":
        return "REPEATED_NON_PROGRESS"
    elif source_kind == "POLICY_UPDATE":
        return "POLICY"
    elif source_kind == "STORAGE_RESTORATION":
        return "STORAGE" if facts.get("matches_object") else "INTEGRITY_SUSPECTED"
    elif source_kind == "SECRET_VERSION":
        return "CREDENTIAL"
    elif source_kind == "MANAGEMENT_COMMAND":
        return "POLICY"
    elif source_kind == "TIMER_FACT":
        scope = facts.get("scope_kind")
        if scope == "BUDGET_REPORT_EXPIRY":
            return "BUDGET"
        if scope == "RECOVERY_ELIGIBILITY":
            return "REPEATED_NON_PROGRESS"
        if scope in {
            "WAIT_CONDITION_NOT_BEFORE",
            "HEALTH_OBSERVATION_EXPIRY",
            "ATTEMPT_CLAIM_DEADLINE",
            "ATTEMPT_EXECUTION_DEADLINE",
        }:
            return "CAPACITY"
    elif source_kind == "INTERNAL":
        return "EXTERNAL_DEPENDENCY"
    raise ValueError(f"{source_kind} facts do not map to a closed recovery category")


def failure_fingerprint(evidence: RecoveryEvidenceInput) -> str:
    return request_digest(
        {
            "category": evidence.category,
            "scope": dict(evidence.failure_scope or {}),
            "bindings": {
                "activity_id": evidence.activity_id,
                "attempt_id": evidence.attempt_id,
                "specification_generation": evidence.specification_generation,
                "candidate_id": evidence.candidate_id,
                "forge_observation_id": evidence.forge_observation_id,
            },
            "evidence": dict(evidence.bounded_evidence or {}),
        }
    )


def select_recovery_decision(
    evidence: RecoveryEvidenceInput,
    *,
    health_observations: Sequence[HealthObservationRef] = (),
    limits: RecoveryLimits = DEFAULT_RECOVERY_LIMITS,
) -> RecoveryDecision:
    """Select the one v1 recovery tactic for a classified failure input."""
    enums.parse_enum("recovery_evidence.source_kind", evidence.source_kind)
    enums.parse_enum("recovery_evidence.category", evidence.category)
    allowed = _SOURCE_CATEGORIES[evidence.source_kind]
    if evidence.category not in allowed:
        raise ValueError(
            f"{evidence.source_kind} cannot source recovery category {evidence.category}"
        )
    ordered_health = tuple(sorted(health_observations, key=lambda item: item.sort_key))
    health_ids = tuple(item.health_observation_id for item in ordered_health)
    fingerprint = failure_fingerprint(evidence)
    attempt_count = evidence.prior_attempt_count
    repair_count = evidence.prior_repair_cycle_count
    diagnosis_count = evidence.prior_diagnosis_count
    rescue_epoch = evidence.rescue_epoch
    selected_fallback = evidence.fallback_order[0] if evidence.fallback_order else None
    next_eligible_at_ms: int | None = None

    category = evidence.category
    if evidence.resumed_wait_condition_id is not None:
        tactic = "WAIT_EVIDENCE"
        strategy_index = 10
    elif category == "BASE_CONFLICT" and _evidence_flag(
        evidence, "import_external_head", "external_head_importable", "prelink_ref_importable"
    ):
        tactic = "IMPORT_EXTERNAL_HEAD"
        strategy_index = 8
    elif category == "BASE_CONFLICT" and _evidence_flag(
        evidence, "reconstruct_foreign_head", "foreign_head_reconstructable"
    ):
        tactic = "RECONSTRUCT_FOREIGN_HEAD"
        strategy_index = 8
    elif category == "CAPACITY" and _evidence_flag(
        evidence, "staff_panel", "panel_staffing", "review_panel_capacity"
    ):
        tactic = "STAFF_PANEL"
        strategy_index = 10
    elif _evidence_flag(
        evidence, "reconcile", "reconcile_publication", "publication_reconciliation"
    ):
        tactic = "RECONCILE"
        strategy_index = 9
    elif category == "FORGE_TRANSIENT" and _evidence_flag(
        evidence, "redeliver", "effect_absent", "delivery_effect_absent"
    ):
        tactic = "REDELIVER"
        strategy_index = 3
    elif category in {
        "CAPACITY",
        "CREDENTIAL",
        "FORGE_TRANSIENT",
        "INTEGRITY_SUSPECTED",
        "PROVIDER_RATE_LIMIT",
        "STORAGE",
    } and _evidence_flag(evidence, "integrity_available", "probe_available"):
        tactic = "RETRY_EXECUTION"
        strategy_index = 3
    elif category in {"WORKER_LOST", "TIMEOUT", "PROVIDER_TRANSIENT", "SOURCE_READ"}:
        if selected_fallback is not None:
            tactic = "REPLACE_CAPACITY"
            strategy_index = 4
            attempt_count += 1
        elif attempt_count < limits.max_attempts_per_activity_before_diagnosis:
            tactic = "RETRY_EXECUTION"
            strategy_index = 3
            attempt_count += 1
        else:
            tactic = "DIAGNOSE"
            strategy_index = 6
    elif category == "PROVIDER_RATE_LIMIT":
        if selected_fallback is not None:
            tactic = "REPLACE_CAPACITY"
            strategy_index = 4
            attempt_count += 1
        else:
            tactic = "WAIT_RATE_LIMIT"
            strategy_index = 10
            next_eligible_at_ms = _clamped_rate_limit_wait(evidence, limits)
    elif category == "CAPACITY":
        tactic = "WAIT_CAPACITY"
        strategy_index = 10
    elif category == "BUDGET":
        tactic = "WAIT_BUDGET"
        strategy_index = 10
    elif category == "INVALID_RESULT":
        if attempt_count == 0:
            tactic = "REPAIR_SCHEMA"
            strategy_index = 5
            attempt_count = 1
        elif selected_fallback is not None:
            tactic = "REPLACE_CAPACITY"
            strategy_index = 4
            attempt_count += 1
        else:
            tactic = "DIAGNOSE"
            strategy_index = 6
    elif category == "CREDENTIAL":
        tactic = "WAIT_EXTERNAL"
        strategy_index = 10
    elif category == "VERIFICATION_ERROR":
        if attempt_count < limits.max_attempts_per_activity_before_diagnosis:
            tactic = "RETRY_EXECUTION"
            strategy_index = 3
            attempt_count += 1
        else:
            tactic = "DIAGNOSE"
            strategy_index = 6
    elif category in {"VERIFICATION_FAILURE", "REVIEW_DISAGREEMENT"}:
        if repair_count + 1 >= limits.max_repair_cycles_before_diagnosis:
            tactic = "DIAGNOSE"
            strategy_index = 6
            repair_count += 1
        else:
            tactic = "ALTERNATIVE_CANDIDATE" if repair_count else "ADJUDICATE"
            strategy_index = 8 if tactic == "ALTERNATIVE_CANDIDATE" else 9
            repair_count += 1
    elif category == "REPEATED_NON_PROGRESS":
        if diagnosis_count >= limits.max_diagnoses_before_replan:
            tactic = "REPLAN"
            strategy_index = 7
        else:
            tactic = "DIAGNOSE"
            strategy_index = 6
            diagnosis_count += 1
    elif category == "BASE_CONFLICT":
        tactic = "REBASE"
        strategy_index = 8
    elif category == "FORGE_TRANSIENT":
        tactic = "WAIT_BACKOFF"
        strategy_index = 10
    elif category == "EXTERNAL_DEPENDENCY":
        tactic = "WAIT_EXTERNAL"
        strategy_index = 10
    elif category == "STORAGE":
        tactic = "WAIT_EXTERNAL"
        strategy_index = 10
    elif category == "INTEGRITY_SUSPECTED":
        tactic = "PROBE_INTEGRITY" if not evidence.exhausted_autonomous else "ENTER_HUMAN_BOUNDARY"
        strategy_index = 5 if tactic == "PROBE_INTEGRITY" else 11
    elif category == "POLICY":
        tactic = "ENTER_HUMAN_BOUNDARY" if evidence.exhausted_autonomous else "DIAGNOSE"
        strategy_index = 11 if tactic == "ENTER_HUMAN_BOUNDARY" else 6
    else:
        raise ValueError(f"unhandled recovery category {category}")

    if tactic == "WAIT_BACKOFF":
        next_eligible_at_ms = _backoff_wait(evidence, limits)
        rescue_epoch += 1

    human_boundary_reason: str | None = None
    if tactic == "ENTER_HUMAN_BOUNDARY":
        human_boundary_reason = _resolve_human_boundary_reason(category, evidence)

    enums.parse_enum("recovery_evidence.selected_tactic", tactic)
    return RecoveryDecision(
        category=category,
        failure_fingerprint=fingerprint,
        strategy_index=strategy_index,
        selected_tactic=tactic,
        attempt_count=attempt_count,
        repair_cycle_count=repair_count,
        diagnosis_count=diagnosis_count,
        rescue_epoch=rescue_epoch,
        human_boundary_reason=human_boundary_reason,
        selected_fallback=selected_fallback,
        ordered_health_observation_ids=health_ids,
        health_observation_ids_digest=bare_canonical_digest(list(health_ids)),
        next_eligible_at_ms=next_eligible_at_ms,
    )


def _clamped_rate_limit_wait(evidence: RecoveryEvidenceInput, limits: RecoveryLimits) -> int:
    if evidence.accepted_at_ms <= 0:
        raise ValueError("accepted_at_ms must be present and positive for timed recovery waits")
    lower = evidence.accepted_at_ms
    upper = lower + limits.max_provider_rate_limit_wait_ms
    if evidence.provider_retry_after_ms is None:
        return upper
    if evidence.provider_retry_after_ms < 0:
        raise ValueError("provider_retry_after_ms must be nonnegative")
    requested_until = lower + evidence.provider_retry_after_ms
    return min(max(requested_until, lower), upper)


def _backoff_wait(evidence: RecoveryEvidenceInput, limits: RecoveryLimits) -> int:
    if evidence.accepted_at_ms <= 0:
        raise ValueError("accepted_at_ms must be present and positive for timed recovery waits")
    return evidence.accepted_at_ms + min(
        limits.backoff_initial_ms * (2**evidence.rescue_epoch), limits.backoff_max_ms
    )


def _resolve_human_boundary_reason(category: str, evidence: RecoveryEvidenceInput) -> str:
    """The exact allowlisted Human Boundary reason an ``ENTER_HUMAN_BOUNDARY``
    tactic must carry (domain-model.md "Human Boundary" ``reason``).

    ``PUBLICATION_OWNERSHIP_CONFLICT`` has the sole direct Reconciliation
    Fact path and can never be produced here. The caller-supplied
    ``human_boundary_reason`` is code -- never raw agent prose -- so this
    only asserts it is one of the reasons this category may legally carry;
    it never invents or defaults one, which would let a convenience
    escalation through.
    """
    allowed = CATEGORY_HUMAN_BOUNDARY_REASONS.get(category)
    if not allowed:
        raise ValueError(f"recovery category {category!r} cannot select ENTER_HUMAN_BOUNDARY")
    reason = evidence.human_boundary_reason
    if reason is None:
        raise ValueError(
            f"ENTER_HUMAN_BOUNDARY for category {category!r} requires an explicit "
            "human_boundary_reason"
        )
    enums.parse_enum("human_boundary.reason", reason)
    if reason not in allowed:
        raise ValueError(
            f"human_boundary_reason {reason!r} is not allowlisted for category {category!r} "
            f"(allowed: {sorted(allowed)})"
        )
    return reason


def _evidence_flag(evidence: RecoveryEvidenceInput, *names: str) -> bool:
    for facts in (evidence.bounded_evidence, evidence.failure_scope):
        if facts is None:
            continue
        for name in names:
            if bool(facts.get(name)):
                return True
    return False
