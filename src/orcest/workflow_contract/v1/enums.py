"""Closed v1 identity, state, reason, trigger, outcome, receipt, and operation enums.

Every enum here is a verbatim transcription of a closed uppercase value set
from ``docs/wiki/domain-model.md``, ``docs/wiki/workflow-lifecycle.md``, or
``docs/wiki/worker-protocol.md``. This module defines vocabulary only -- it
contains no lifecycle/transition logic (which legal state follows which is
owned by the workflow lifecycle implementation, a separate Workflow Control
v1 issue). Every enum is registered in one name -> class registry so that
any component can look up, parse, and fail closed on an unknown value by
name alone, per the "one registry" acceptance criterion.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import Any

__all__ = [
    "UnknownEnumNameError",
    "UnknownEnumValueError",
    "registered_enum_names",
    "get_enum",
    "parse_enum",
]


class UnknownEnumNameError(KeyError):
    """Raised when a registry lookup names an enum that does not exist in v1."""


class UnknownEnumValueError(ValueError):
    """Raised when a value does not belong to a v1 enum's closed value set."""


_REGISTRY: dict[str, type[Enum]] = {}


def _register_enum(registry_name: str, class_name: str, values: Sequence[str]) -> Any:
    """Build and register one closed str-mixin Enum class.

    Returns ``Any`` rather than ``type[Enum]``: this is a dynamic class
    factory, and callers assign the result to a module-level name (e.g.
    ``RunState = _register_enum(...)``) and immediately use it as a concrete
    Enum subclass with real members (``RunState.MERGED``). Typing the return
    as the generic ``type[Enum]`` would make every such member access a
    false-positive type error.
    """
    if registry_name in _REGISTRY:
        raise RuntimeError(f"duplicate v1 enum registration: {registry_name!r}")
    if len(set(values)) != len(values):
        raise RuntimeError(f"{registry_name}: duplicate value(s) in {values!r}")
    cls = Enum(class_name, {value: value for value in values}, type=str)  # type: ignore[misc]
    cls.__module__ = __name__
    _REGISTRY[registry_name] = cls
    return cls


def registered_enum_names() -> frozenset[str]:
    return frozenset(_REGISTRY)


def get_enum(registry_name: str) -> type[Enum]:
    try:
        return _REGISTRY[registry_name]
    except KeyError as exc:
        raise UnknownEnumNameError(f"unknown v1 enum {registry_name!r}") from exc


def parse_enum(registry_name: str, value: object) -> Enum:
    """Parse ``value`` against the named closed enum, failing closed."""
    cls = get_enum(registry_name)
    try:
        return cls(value)  # type: ignore[call-arg]
    except ValueError as exc:
        allowed = sorted(member.value for member in cls)
        raise UnknownEnumValueError(
            f"{registry_name}: unknown enum value {value!r}; allowed={allowed!r}"
        ) from exc


# ---------------------------------------------------------------------------
# Controller Mode and Capability Key Registry
# ---------------------------------------------------------------------------

ControllerMode = _register_enum(
    "controller_mode.mode",
    "ControllerMode",
    ["RUNNING", "INTAKE_PAUSED", "DISPATCH_PAUSED", "DRAINING", "MAINTENANCE"],
)
DispatchPausedIntakePolicy = _register_enum(
    "controller_mode.dispatch_paused_intake_policy",
    "DispatchPausedIntakePolicy",
    ["ALLOW_ADMISSION", "PAUSE_ADMISSION"],
)
ControllerModeOperationKind = _register_enum(
    "controller_mode_operation.operation_kind",
    "ControllerModeOperationKind",
    ["INITIALIZE", "SET_MODE", "RESTORE_BACKUP"],
)
ControllerModeOperationStatus = _register_enum(
    "controller_mode_operation.status", "ControllerModeOperationStatus", ["SUCCEEDED", "REJECTED"]
)
ControllerModeOperationRejectionCode = _register_enum(
    "controller_mode_operation.rejection_code",
    "ControllerModeOperationRejectionCode",
    [
        "CAS_LOST",
        "ALREADY_INITIALIZED",
        "NOT_INITIALIZED",
        "NO_CHANGE",
        "TRANSITION_NOT_ALLOWED",
        "AUTHORITY_REVOKED",
        "INTEGRITY_CONFLICT",
    ],
)
CapabilityKeyOperationKind = _register_enum(
    "capability_key_operation.kind",
    "CapabilityKeyOperationKind",
    ["REGISTER", "SELECT", "RETIRE", "REVOKE"],
)
CapabilityKeyOperationStatus = _register_enum(
    "capability_key_operation.status", "CapabilityKeyOperationStatus", ["SUCCEEDED", "REJECTED"]
)
CapabilityKeyOperationRejectionCode = _register_enum(
    "capability_key_operation.rejection_code",
    "CapabilityKeyOperationRejectionCode",
    [
        "CAS_LOST",
        "KEY_ALREADY_EXISTS",
        "KEY_NOT_ACTIVE",
        "CURRENT_KEY_REQUIRES_REPLACEMENT",
        "AUTHORITY_REVOKED",
        "INTEGRITY_CONFLICT",
    ],
)
CapabilitySigningKeyState = _register_enum(
    "capability_signing_key.state", "CapabilitySigningKeyState", ["ACTIVE", "RETIRED", "REVOKED"]
)
SignatureAlgorithm = _register_enum(
    "capability_signing_key.signature_algorithm", "SignatureAlgorithm", ["ED25519"]
)

# ---------------------------------------------------------------------------
# Forge Instance, Project, Project Registration
# ---------------------------------------------------------------------------

ForgeAdapterKind = _register_enum("forge_instance.adapter_kind", "ForgeAdapterKind", ["GITHUB"])
ProjectRegistrationState = _register_enum(
    "project.registration_state", "ProjectRegistrationState", ["ACTIVE", "SUSPENDED", "REMOVED"]
)
ProjectRegistrationOperationMode = _register_enum(
    "project_registration_operation.mode",
    "ProjectRegistrationOperationMode",
    ["REGISTER", "REVALIDATE"],
)
ProjectRegistrationOperationStatus = _register_enum(
    "project_registration_operation.status",
    "ProjectRegistrationOperationStatus",
    ["SUCCEEDED", "REJECTED"],
)
ProjectRegistrationOperationRejectionCode = _register_enum(
    "project_registration_operation.rejection_code",
    "ProjectRegistrationOperationRejectionCode",
    [
        "STABLE_REPOSITORY_OWNERSHIP_CONFLICT",
        "WORKFLOW_INVALID",
        "CAPABILITY_UNSUPPORTED",
        "POLICY_VALIDATION_FAILED",
    ],
)

# ---------------------------------------------------------------------------
# Workflow Blob / Policy / Snapshot
# ---------------------------------------------------------------------------

WorkflowBlobMediaKind = _register_enum(
    "workflow_blob.media_kind",
    "WorkflowBlobMediaKind",
    ["CONFIG_JSON", "PROMPT_UTF8", "POLICY_JSON", "SERVER_POLICY_JSON"],
)
PolicyUpdateSourceKind = _register_enum(
    "policy_update.source_kind", "PolicyUpdateSourceKind", ["SERVER_ROLLOUT"]
)
WorkItemSnapshotSourceKind = _register_enum(
    "work_item_snapshot.source_kind",
    "WorkItemSnapshotSourceKind",
    ["FORGE_OBSERVATION", "POLICY_UPDATE"],
)
BaseMovementPolicy = _register_enum(
    "snapshot.base_movement_policy",
    "BaseMovementPolicy",
    ["REBASE_BEFORE_PUBLICATION", "PIN", "SUPERSEDE_AT_BOUNDARY"],
)
BudgetReportAvailability = _register_enum(
    "budget_report.availability", "BudgetReportAvailability", ["AVAILABLE", "EXHAUSTED"]
)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

RunState = _register_enum(
    "run.state",
    "RunState",
    [
        "ADMITTED",
        "PLANNING",
        "BUILDING",
        "VERIFYING",
        "REVIEWING",
        "AGGREGATING",
        "REMEDIATING",
        "DIAGNOSING",
        "REPLANNING",
        "ADJUDICATING",
        "APPROVED",
        "PUBLISHING",
        "PR_MONITORING",
        "PR_REMEDIATING",
        "RECOVERING",
        "WAITING",
        "NEEDS_HUMAN",
        "MERGED",
        "CLOSED",
        "CANCELLED",
    ],
)
RUN_TERMINAL_STATES = frozenset({RunState.MERGED, RunState.CLOSED, RunState.CANCELLED})
RunTerminalOutcome = _register_enum(
    "run.terminal_outcome", "RunTerminalOutcome", ["MERGED", "CLOSED", "CANCELLED"]
)

# ---------------------------------------------------------------------------
# Activity
# ---------------------------------------------------------------------------

ActivityExecutionClass = _register_enum(
    "activity.execution_class", "ActivityExecutionClass", ["WORKER", "CONTROLLER"]
)
ActivityState = _register_enum(
    "activity.state",
    "ActivityState",
    ["PLANNED", "READY", "ACTIVE", "SUCCEEDED", "FAILED", "CANCELLED", "SUPERSEDED"],
)
ActivityKind = _register_enum(
    "activity.kind",
    "ActivityKind",
    [
        "PLAN",
        "BUILD",
        "VERIFY",
        "REVIEW",
        "REMEDIATE",
        "DIAGNOSE",
        "REPLAN",
        "ADJUDICATE",
        "REBASE",
        "PR_REMEDIATE",
        "IMPORT",
        "PUBLISH",
        "CLOSE_PUBLICATION",
        "CLOSE_REDUNDANT_PUBLICATION",
        "REPAIR_RUN_MARKER",
        "RECONCILE",
    ],
)
# execution_class per Activity.kind, per "v1 Activity kinds" (domain-model.md).
WORKER_ACTIVITY_KINDS = frozenset(
    {
        ActivityKind.PLAN,
        ActivityKind.BUILD,
        ActivityKind.VERIFY,
        ActivityKind.REVIEW,
        ActivityKind.REMEDIATE,
        ActivityKind.DIAGNOSE,
        ActivityKind.REPLAN,
        ActivityKind.ADJUDICATE,
        ActivityKind.REBASE,
        ActivityKind.PR_REMEDIATE,
    }
)
CONTROLLER_ACTIVITY_KINDS = frozenset(
    {
        ActivityKind.IMPORT,
        ActivityKind.PUBLISH,
        ActivityKind.CLOSE_PUBLICATION,
        ActivityKind.CLOSE_REDUNDANT_PUBLICATION,
        ActivityKind.REPAIR_RUN_MARKER,
        ActivityKind.RECONCILE,
    }
)
ActivityReviewAssignmentKind = _register_enum(
    "activity_review_assignment.assignment_kind",
    "ActivityReviewAssignmentKind",
    ["REVIEW", "ADJUDICATE"],
)

# ---------------------------------------------------------------------------
# Attempt / Claim / Launch / Result
# ---------------------------------------------------------------------------

AttemptState = _register_enum(
    "attempt.state",
    "AttemptState",
    ["OFFERED", "CLAIMED", "SUCCEEDED", "FAILED", "ABSTAINED", "EXPIRED", "SUPERSEDED"],
)
SourceAccessKind = _register_enum(
    "attempt_claim.source_access_kind",
    "SourceAccessKind",
    ["SCOPED_CREDENTIAL", "BROKERED_ARCHIVE"],
)
LaunchAcceptedStatus = _register_enum(
    "launch_accepted.status", "LaunchAcceptedStatus", ["AVAILABLE", "EXPIRED"]
)
AttemptTerminalFactKind = _register_enum(
    "attempt_terminal_fact.kind",
    "AttemptTerminalFactKind",
    ["CLAIM_DEADLINE", "EXECUTION_DEADLINE", "WORKER_LOST", "RESULT_AFTER_TERMINAL"],
)
AttemptTerminalFactSourceKind = _register_enum(
    "attempt_terminal_fact.source_kind",
    "AttemptTerminalFactSourceKind",
    ["TIMER_FACT", "RESULT_REQUEST", "HEALTH_OBSERVATION"],
)
AttemptTerminalFactCapacityDisposition = _register_enum(
    "attempt_terminal_fact.capacity_disposition",
    "AttemptTerminalFactCapacityDisposition",
    ["COMPATIBLE_AVAILABLE", "NO_COMPATIBLE_AVAILABLE"],
)
AttemptTerminalFactReplacementOfferDisposition = _register_enum(
    "attempt_terminal_fact.replacement_offer_disposition",
    "AttemptTerminalFactReplacementOfferDisposition",
    ["OFFER_ALLOWED", "MODE_BLOCKED", "ISSUANCE_KEY_UNAVAILABLE"],
)
AttemptResultOutcome = _register_enum(
    "attempt_result.outcome",
    "AttemptResultOutcome",
    ["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_PERMANENT", "ABSTAINED"],
)
FailureClass = _register_enum(
    "attempt_result.failure_class",
    "FailureClass",
    [
        "INFRASTRUCTURE",
        "PROVIDER_UNAVAILABLE",
        "PROVIDER_RATE_LIMIT",
        "INCOMPATIBLE_WORKER",
        "INVALID_AGENT_OUTPUT",
        "VALIDATION_FAILURE",
        "CREDENTIAL_UNAVAILABLE",
        "SOURCE_READ_FAILED",
        "VERIFICATION_ERROR",
        "BASE_CONFLICT",
        "POLICY_DENIED",
        "SPECIFICATION_CONFLICT",
        "MISSING_AUTHORITY",
        "INTEGRITY_FAILURE",
    ],
)
ResultRequestDisposition = _register_enum(
    "result_request.disposition",
    "ResultRequestDisposition",
    ["ACCEPTED", "UPLOAD_EXPIRED", "STALE_ATTEMPT", "EXPIRED_CURRENT", "ALREADY_TERMINAL"],
)
ResultRequestStaleReason = _register_enum(
    "result_request.stale_reason",
    "ResultRequestStaleReason",
    [
        "GENERATION_SUPERSEDED",
        "CLAIM_BINDING_CHANGED",
        "RUN_BINDING_CHANGED",
        "TERMINAL_BEFORE_DEADLINE",
    ],
)

# ---------------------------------------------------------------------------
# Candidate Upload / Candidate / Receipts / Consensus
# ---------------------------------------------------------------------------

CandidateUploadState = _register_enum(
    "candidate_upload.state",
    "CandidateUploadState",
    ["RECEIVING", "VALIDATED", "PROMOTED", "CONSUMED", "EXPIRED"],
)
CandidateProvenanceKind = _register_enum(
    "candidate.provenance_kind", "CandidateProvenanceKind", ["WORKER_ATTEMPT", "FORGE_IMPORT"]
)
VerificationReceiptOutcome = _register_enum(
    "verification_receipt.outcome", "VerificationReceiptOutcome", ["PASS", "FAIL", "ERROR"]
)
ReviewAssessmentOutcome = _register_enum(
    "review_receipt.assessment_outcome",
    "ReviewAssessmentOutcome",
    ["SATISFIED", "VIOLATED", "UNVERIFIABLE"],
)
ReviewVerdict = _register_enum(
    "review_receipt.verdict", "ReviewVerdict", ["APPROVE", "BLOCK", "ABSTAIN"]
)
AdjudicationDisposition = _register_enum(
    "adjudication_receipt.disposition",
    "AdjudicationDisposition",
    ["SUSTAIN", "OVERRULE", "INCONCLUSIVE"],
)
ConsensusDecisionOutcome = _register_enum(
    "consensus_decision.outcome",
    "ConsensusDecisionOutcome",
    ["APPROVED", "REMEDIATE", "ADJUDICATE"],
)

# ---------------------------------------------------------------------------
# Publication
# ---------------------------------------------------------------------------

PublicationState = _register_enum(
    "publication.state",
    "PublicationState",
    ["PLANNED", "BRANCH_OBSERVED", "CHANGE_REQUEST_OBSERVED", "ACTIVE", "CLOSED"],
)
PublicationLinkCardinality = _register_enum(
    "publication.link_cardinality", "PublicationLinkCardinality", ["ZERO", "ONE", "MULTIPLE"]
)
PublicationInitialLinkTerminalState = _register_enum(
    "publication.initial_link_terminal_state",
    "PublicationInitialLinkTerminalState",
    ["MERGED", "CLOSED"],
)
PublicationEffectMode = _register_enum(
    "publication_effect.mode", "PublicationEffectMode", ["INITIAL", "UPDATE"]
)
PublicationEffectCheckpointSuboperationKind = _register_enum(
    "publication_effect_checkpoint.suboperation_kind",
    "PublicationEffectCheckpointSuboperationKind",
    [
        "BASE_READ_PRE",
        "REF_READ",
        "REF_CREATE",
        "REF_UPDATE",
        "COMPLETE_MARKER_SEARCH",
        "CHANGE_REQUEST_SEARCH",
        "CHANGE_REQUEST_CREATE",
        "BASE_READ_POST",
        "COMPLETE",
    ],
)
PublicationEffectCheckpointStatus = _register_enum(
    "publication_effect_checkpoint.status",
    "PublicationEffectCheckpointStatus",
    [
        "REQUEST_READY",
        "OBSERVED_ABSENT",
        "OBSERVED_SATISFIED",
        "AMBIGUOUS",
        "BASE_MISMATCH",
        "CAS_MISMATCH",
        "COMPLETED",
    ],
)

# ---------------------------------------------------------------------------
# Reconciliation / Controller Operation Fact / Terminal Duplicate Cleanup
# ---------------------------------------------------------------------------

ReconciliationFactKind = _register_enum(
    "reconciliation_fact.kind",
    "ReconciliationFactKind",
    [
        "EFFECT_PRESENT",
        "EFFECT_ABSENT",
        "PRELINK_REF_IMPORTABLE",
        "PRELINK_REF_RECONSTRUCT_REQUIRED",
        "REDUNDANT_PUBLICATIONS_PROVEN",
        "NO_ACTIONABLE_DUPLICATE",
        "OWNERSHIP_CONFLICT",
    ],
)
ReconciliationPinnedBaseRelationship = _register_enum(
    "reconciliation_fact.pinned_base_relationship",
    "ReconciliationPinnedBaseRelationship",
    ["EXACT_PINNED_BASE", "DESCENDANT_OF_PINNED_BASE", "DIVERGED_FROM_PINNED_BASE", "UNPROVEN"],
)
ReconciliationDuplicateMemberDisposition = _register_enum(
    "reconciliation_duplicate_member.disposition",
    "ReconciliationDuplicateMemberDisposition",
    ["RETAIN", "CLOSE"],
)
ControllerOperationFactKind = _register_enum(
    "controller_operation_fact.kind",
    "ControllerOperationFactKind",
    [
        "IMPORT",
        "PUBLISH",
        "CLOSE_PUBLICATION",
        "CLOSE_REDUNDANT_PUBLICATION",
        "REPAIR_RUN_MARKER",
        "RECONCILE",
    ],
)
ControllerOperationFactOutcome = _register_enum(
    "controller_operation_fact.outcome", "ControllerOperationFactOutcome", ["SUCCEEDED", "FAILED"]
)
ControllerOperationFactFailureCategory = _register_enum(
    "controller_operation_fact.failure_category",
    "ControllerOperationFactFailureCategory",
    ["SOURCE_READ", "BASE_CONFLICT", "CREDENTIAL", "STORAGE", "INTEGRITY_SUSPECTED", "POLICY"],
)
TerminalDuplicateCleanupReservationState = _register_enum(
    "terminal_duplicate_cleanup_reservation.state",
    "TerminalDuplicateCleanupReservationState",
    ["ACTIVE", "COMPLETED"],
)
TerminalDuplicateCleanupMemberPlannedAction = _register_enum(
    "terminal_duplicate_cleanup_member.planned_action",
    "TerminalDuplicateCleanupMemberPlannedAction",
    ["CLOSE", "DETACH_MARKER", "RECORD_ONLY"],
)
TerminalDuplicateCleanupActionState = _register_enum(
    "terminal_duplicate_cleanup_action.state",
    "TerminalDuplicateCleanupActionState",
    ["PENDING", "ACTIVE", "COMPLETED", "SUPERSEDED"],
)
TerminalDuplicateCleanupActionOutcome = _register_enum(
    "terminal_duplicate_cleanup_action.outcome",
    "TerminalDuplicateCleanupActionOutcome",
    ["CLOSED", "MARKER_DETACHED", "RETAINED_AUDIT"],
)
TerminalDuplicateCleanupActionRecordReason = _register_enum(
    "terminal_duplicate_cleanup_action.record_reason",
    "TerminalDuplicateCleanupActionRecordReason",
    ["EXTERNAL_RELIANCE", "INCOMPLETE_PROOF", "INCOMPATIBLE_OWNER", "CAS_UNSAFE"],
)

# ---------------------------------------------------------------------------
# Forge Observation Schedule / Request / Observation
# ---------------------------------------------------------------------------

ForgeObservationScheduleKind = _register_enum(
    "forge_observation_schedule.schedule_kind",
    "ForgeObservationScheduleKind",
    [
        "WORK_ITEM_DISCOVERY",
        "WORK_ITEM_POLL",
        "BASE_HEAD_POLL",
        "REF_POLL",
        "CHANGE_REQUEST_SEARCH",
        "CHANGE_REQUEST_POLL",
        "CI_POLL",
        "COMPLETE_MARKER_SEARCH",
    ],
)
ForgeObservationTargetKind = _register_enum(
    "forge_observation.target_kind",
    "ForgeObservationTargetKind",
    ["PROJECT", "WORK_ITEM", "PUBLICATION"],
)
ForgeObservationScheduleState = _register_enum(
    "forge_observation_schedule.state",
    "ForgeObservationScheduleState",
    ["ACTIVE", "PAUSED", "CLOSED"],
)
ForgeObservationRequestState = _register_enum(
    "forge_observation_request.state",
    "ForgeObservationRequestState",
    ["PENDING", "COMPLETED", "SUPERSEDED"],
)
ForgeCredentialPurpose = _register_enum(
    "forge_observation_request.credential_purpose",
    "ForgeCredentialPurpose",
    ["PROJECT_SOURCE_READ", "PUBLICATION", "FORGE_CONNECTIVITY"],
)
ForgeRequestFailureKind = _register_enum(
    "forge_request_failure_fact.failure_kind",
    "ForgeRequestFailureKind",
    ["TIMEOUT", "RATE_LIMIT", "UNAVAILABLE"],
)
ForgeObservationKind = _register_enum(
    "forge_observation.kind",
    "ForgeObservationKind",
    [
        "WORK_ITEM_SNAPSHOT",
        "DEPENDENCY_STATE",
        "BASE_HEAD",
        "REF_ABSENT",
        "REF_HEAD",
        "CHANGE_REQUEST_ABSENT",
        "CHANGE_REQUEST_DISCOVERED",
        "CHANGE_REQUEST_HEAD",
        "CHANGE_REQUEST_FEEDBACK",
        "CHANGE_REQUEST_SEARCH_RESULT",
        "CHANGE_REQUEST_MARKER",
        "CHANGE_REQUEST_MERGED",
        "CHANGE_REQUEST_CLOSED",
    ],
)
ChangeRequestMergeability = _register_enum(
    "change_request_feedback.mergeability",
    "ChangeRequestMergeability",
    ["CLEAN", "CONFLICTING", "UNKNOWN"],
)
ChangeRequestCheckStatus = _register_enum(
    "change_request_feedback.check_status", "ChangeRequestCheckStatus", ["PENDING", "PASS", "FAIL"]
)
ChangeRequestSearchMemberClass = _register_enum(
    "change_request_search_member.member_class",
    "ChangeRequestSearchMemberClass",
    ["LIVE", "TERMINAL"],
)
ChangeRequestSearchMemberTerminalState = _register_enum(
    "change_request_search_member.terminal_state",
    "ChangeRequestSearchMemberTerminalState",
    ["CLOSED", "MERGED"],
)
ChangeRequestSearchMemberOwnershipStatus = _register_enum(
    "change_request_search_member.ownership_status",
    "ChangeRequestSearchMemberOwnershipStatus",
    ["POSITIVE", "INCOMPATIBLE", "INCOMPLETE"],
)
ChangeRequestSearchMemberProofKind = _register_enum(
    "change_request_search_member.proof_kind",
    "ChangeRequestSearchMemberProofKind",
    ["EXACT_CREATE_RESPONSE", "AMBIGUOUS_CREATE_RECONCILED", "LIVE_ASSOCIATION"],
)
ChangeRequestSearchMemberOwnershipDefectCode = _register_enum(
    "change_request_search_member.ownership_defect_code",
    "ChangeRequestSearchMemberOwnershipDefectCode",
    [
        "CREATE_PROVENANCE_MISSING",
        "CREATOR_AUTHORITY_MISMATCH",
        "EFFECT_GENERATION_MISMATCH",
        "REF_MISMATCH",
        "MARKER_MISMATCH",
        "DESIRED_COMMIT_MISMATCH",
        "HEAD_UNPROVEN",
        "DURABLE_ASSOCIATION_MISMATCH",
    ],
)

# ---------------------------------------------------------------------------
# Wait Condition / Recovery Evidence / Worker Loss
# ---------------------------------------------------------------------------

WaitConditionReason = _register_enum(
    "wait_condition.reason",
    "WaitConditionReason",
    [
        "CAPACITY",
        "RATE_LIMIT",
        "BUDGET",
        "BACKOFF",
        "EXTERNAL_DEPENDENCY",
        "FORGE_UNAVAILABLE",
        "STORAGE_RECOVERY",
        "SECRET_RECOVERY",
        "EVIDENCE",
    ],
)
WaitConditionWakeKind = _register_enum(
    "wait_condition.wake_kind",
    "WaitConditionWakeKind",
    [
        "CAPACITY",
        "RATE_LIMIT_RESET",
        "BUDGET_WINDOW",
        "DEPENDENCY",
        "FORGE",
        "STORAGE",
        "SECRET",
        "EVIDENCE",
    ],
)
CreatedFromKind = _register_enum(
    "wait_condition.created_from_kind",
    "CreatedFromKind",
    [
        "ATTEMPT_RESULT",
        "ATTEMPT_TERMINAL",
        "CONTROLLER_OPERATION",
        "RECOVERY_EVIDENCE",
        "HEALTH_OBSERVATION",
        "FORGE_OBSERVATION",
        "POLICY_UPDATE",
        "MANAGEMENT_COMMAND",
        "STORAGE_RESTORATION",
        "SECRET_VERSION",
        "TIMER_FACT",
        "INTERNAL",
    ],
)
RecoveryEvidenceSourceKind = _register_enum(
    "recovery_evidence.source_kind",
    "RecoveryEvidenceSourceKind",
    [
        "ATTEMPT_RESULT",
        "ATTEMPT_TERMINAL",
        "CONTROLLER_OPERATION",
        "FORGE_REQUEST_FAILURE",
        "HEALTH_OBSERVATION",
        "FORGE_OBSERVATION",
        "BUDGET_REPORT",
        "RECONCILIATION_FACT",
        "RECOVERY_EVIDENCE",
        "POLICY_UPDATE",
        "STORAGE_RESTORATION",
        "SECRET_VERSION",
        "MANAGEMENT_COMMAND",
        "TIMER_FACT",
        "INTERNAL",
    ],
)
RecoveryInputCategory = _register_enum(
    "recovery_evidence.category",
    "RecoveryInputCategory",
    [
        "WORKER_LOST",
        "TIMEOUT",
        "PROVIDER_TRANSIENT",
        "PROVIDER_RATE_LIMIT",
        "CAPACITY",
        "BUDGET",
        "INVALID_RESULT",
        "CREDENTIAL",
        "SOURCE_READ",
        "VERIFICATION_ERROR",
        "VERIFICATION_FAILURE",
        "REPEATED_NON_PROGRESS",
        "REVIEW_DISAGREEMENT",
        "BASE_CONFLICT",
        "FORGE_TRANSIENT",
        "EXTERNAL_DEPENDENCY",
        "STORAGE",
        "INTEGRITY_SUSPECTED",
        "POLICY",
    ],
)
RecoveryTactic = _register_enum(
    "recovery_evidence.selected_tactic",
    "RecoveryTactic",
    [
        "RECONCILE",
        "REDELIVER",
        "RETRY_EXECUTION",
        "REPLACE_CAPACITY",
        "STAFF_PANEL",
        "REPAIR_SCHEMA",
        "PROBE_INTEGRITY",
        "DIAGNOSE",
        "REPLAN",
        "ALTERNATIVE_CANDIDATE",
        "ADJUDICATE",
        "REBASE",
        "IMPORT_EXTERNAL_HEAD",
        "RECONSTRUCT_FOREIGN_HEAD",
        "ENTER_HUMAN_BOUNDARY",
        "WAIT_BACKOFF",
        "WAIT_CAPACITY",
        "WAIT_RATE_LIMIT",
        "WAIT_BUDGET",
        "WAIT_EXTERNAL",
        "WAIT_EVIDENCE",
    ],
)
WorkerLossReason = _register_enum(
    "worker_loss_report.reason",
    "WorkerLossReason",
    ["VM_DESTROYED", "VM_MISSING", "CEILING_TIMEOUT", "OPERATOR_DRAIN"],
)
WorkerLossOutcome = _register_enum(
    "worker_loss_report.outcome", "WorkerLossOutcome", ["ACCEPTED", "STALE"]
)

# ---------------------------------------------------------------------------
# Health Probe / Health Observation / Timer Fact
# ---------------------------------------------------------------------------

HealthProbeKind = _register_enum(
    "health_probe.probe_kind",
    "HealthProbeKind",
    [
        "FORGE_CONNECTIVITY",
        "PROVIDER_ACCOUNT_STATUS",
        "STORAGE_OBJECT_INTEGRITY",
        "SECRET_VERSION_INTEGRITY",
    ],
)
HealthProbeRequestState = _register_enum(
    "health_probe_request.state", "HealthProbeRequestState", ["PENDING", "COMPLETED", "SUPERSEDED"]
)
HealthProbeIntegrityFailureCode = _register_enum(
    "health_probe_fact.integrity_failure_code",
    "HealthProbeIntegrityFailureCode",
    ["UNAVAILABLE", "MISSING", "UNREADABLE", "DIGEST_MISMATCH", "KEYED_ATTESTATION_MISMATCH"],
)
HealthScopeKind = _register_enum(
    "health_probe.scope_kind",
    "HealthScopeKind",
    [
        "FORGE",
        "PROVIDER_ACCOUNT",
        "CAPACITY_POOL",
        "WORKER_PROFILE",
        "WORKER_SESSION",
        "STORAGE",
        "SECRET",
    ],
)
HealthObservationKind = _register_enum(
    "health_observation.kind",
    "HealthObservationKind",
    ["AVAILABLE", "UNAVAILABLE", "RATE_LIMITED", "EXHAUSTED", "LOST", "RECOVERED"],
)
HealthObservationSourceKind = _register_enum(
    "health_observation.source_kind",
    "HealthObservationSourceKind",
    ["CAPACITY_REPORT", "WORKER_LOSS_REPORT", "STORAGE_RESTORATION", "HEALTH_PROBE_FACT"],
)
TimerFactScopeKind = _register_enum(
    "timer_fact.scope_kind",
    "TimerFactScopeKind",
    [
        "WAIT_CONDITION_NOT_BEFORE",
        "HEALTH_OBSERVATION_EXPIRY",
        "BUDGET_REPORT_EXPIRY",
        "ATTEMPT_CLAIM_DEADLINE",
        "ATTEMPT_EXECUTION_DEADLINE",
        "RECOVERY_ELIGIBILITY",
    ],
)
TimerFactSourceKind = _register_enum(
    "timer_fact.source_kind", "TimerFactSourceKind", ["SCHEDULED_SWEEP", "STARTUP_RECONCILIATION"]
)

# ---------------------------------------------------------------------------
# Storage Restoration
# ---------------------------------------------------------------------------

StorageRestorationOperationState = _register_enum(
    "storage_restoration_operation.state",
    "StorageRestorationOperationState",
    ["PENDING", "RESTORED", "REJECTED"],
)
StorageRestorationOperationRejectionCode = _register_enum(
    "storage_restoration_operation.rejection_code",
    "StorageRestorationOperationRejectionCode",
    [
        "OBJECT_NO_LONGER_LIVE",
        "AUTHORIZATION_REVOKED",
        "STAGED_OBJECT_INVALID",
        "INTEGRITY_CONFLICT",
    ],
)
StorageRestorationFactObjectKind = _register_enum(
    "storage_restoration_fact.object_kind",
    "StorageRestorationFactObjectKind",
    ["CANDIDATE_ARTIFACT", "SECRET_VERSION", "WORKFLOW_BLOB"],
)
StorageRestorationFactSourceKind = _register_enum(
    "storage_restoration_fact.source_kind",
    "StorageRestorationFactSourceKind",
    ["BACKUP_RESTORE", "AUTHENTICATED_STORAGE_OPERATION"],
)

# ---------------------------------------------------------------------------
# Management Command / Human Boundary / Human Resolution
# ---------------------------------------------------------------------------

ManagementCommandKind = _register_enum(
    "management_command.kind", "ManagementCommandKind", ["CANCEL", "RESOLVE_HUMAN_BOUNDARY"]
)
HumanBoundaryReason = _register_enum(
    "human_boundary.reason",
    "HumanBoundaryReason",
    [
        "MISSING_AUTHORITY",
        "REQUIRED_SECRET_OR_PERMISSION",
        "IRREVERSIBLE_DECISION",
        "SPECIFICATION_CONFLICT",
        "SECURITY_POLICY_BOUNDARY",
        "INTEGRITY_FAILURE",
        "UNSATISFIABLE_REQUIREMENTS",
        "PUBLICATION_OWNERSHIP_CONFLICT",
    ],
)
HumanBoundaryCreatedFromKind = _register_enum(
    "human_boundary.created_from_kind",
    "HumanBoundaryCreatedFromKind",
    ["RECOVERY_EVIDENCE", "RECONCILIATION_FACT"],
)
HumanResolutionSourceKind = _register_enum(
    "human_resolution.source_kind",
    "HumanResolutionSourceKind",
    ["MANAGEMENT_COMMAND", "FORGE_OBSERVATION", "SECRET_VERSION", "STORAGE_RESTORATION"],
)
HumanResolutionKind = _register_enum(
    "human_resolution.resolution_kind",
    "HumanResolutionKind",
    [
        "AUTHORITY_GRANTED",
        "SECRET_OR_PERMISSION_PROVIDED",
        "IRREVERSIBLE_ACTION_AUTHORIZED",
        "SPECIFICATION_AMENDED",
        "SECURITY_ACTION_AUTHORIZED",
        "INTEGRITY_RESTORED",
        "ENVIRONMENT_CAPABILITY_PROVIDED",
        "PUBLICATION_OWNERSHIP_RESOLVED",
    ],
)

# ---------------------------------------------------------------------------
# Transition / Outbox / Projection Outbox
# ---------------------------------------------------------------------------

TransitionTrigger = _register_enum(
    "transition.trigger_kind",
    "TransitionTrigger",
    [
        "ADMIT",
        "INTERNAL",
        "ATTEMPT_RESULT",
        "ATTEMPT_TERMINAL",
        "CONTROLLER_OPERATION",
        "FORGE_REQUEST_FAILURE",
        "FORGE_OBSERVATION",
        "HEALTH_OBSERVATION",
        "BUDGET_REPORT",
        "MANAGEMENT_COMMAND",
        "POLICY_UPDATE",
        "PUBLICATION_CHECKPOINT",
        "RECONCILIATION_FACT",
        "RECOVERY_EVIDENCE",
        "SECRET_VERSION",
        "SPEC_SUPERSEDE",
        "STORAGE_RESTORATION",
        "TIMER_FACT",
    ],
)
OutboxRecordSourceKind = _register_enum(
    "outbox_record.source_kind",
    "OutboxRecordSourceKind",
    [
        "ACTIVITY",
        "HEALTH_PROBE_REQUEST",
        "FORGE_OBSERVATION_REQUEST",
        "SECRET_PROVISION_OPERATION",
        "TERMINAL_DUPLICATE_CLEANUP_ACTION",
    ],
)
OutboxRecordState = _register_enum(
    "outbox_record.state", "OutboxRecordState", ["PENDING", "DELIVERED", "SUPERSEDED"]
)
ProjectionOutboxRecordKind = _register_enum(
    "projection_outbox_record.kind", "ProjectionOutboxRecordKind", ["RUN_STATUS"]
)
ProjectionOutboxRecordTargetKind = _register_enum(
    "projection_outbox_record.target_kind",
    "ProjectionOutboxRecordTargetKind",
    ["WORK_ITEM", "CHANGE_REQUEST"],
)

# ---------------------------------------------------------------------------
# Secret Provision / Credential Rotation
# ---------------------------------------------------------------------------

SecretProvisionOperationMode = _register_enum(
    "secret_provision_operation.mode",
    "SecretProvisionOperationMode",
    ["PROVISION", "ADOPT_EXISTING"],
)
SecretOwnerScopeKind = _register_enum(
    "secret_provision_operation.owner_scope_kind",
    "SecretOwnerScopeKind",
    ["PROJECT", "FORGE_INSTALLATION", "CONTROLLER"],
)
SecretPurpose = _register_enum(
    "secret_provision_operation.purpose",
    "SecretPurpose",
    ["FORGE_API", "SOURCE_READ", "PUBLICATION", "CAPABILITY_SIGNING_PRIVATE_KEY"],
)
SecretProvisionOperationState = _register_enum(
    "secret_provision_operation.state",
    "SecretProvisionOperationState",
    ["PENDING", "COMPLETED", "REJECTED"],
)
SecretProvisionOperationRejectionCode = _register_enum(
    "secret_provision_operation.rejection_code",
    "SecretProvisionOperationRejectionCode",
    ["CAS_LOST", "INTEGRITY_CONFLICT", "AUTHORITY_REVOKED", "STAGED_OBJECT_INVALID"],
)
SecretProvisionCheckpointPhase = _register_enum(
    "secret_provision_checkpoint.phase",
    "SecretProvisionCheckpointPhase",
    ["VERIFY_STAGING", "INSTALL_VERSION"],
)
SecretProvisionCheckpointOutcome = _register_enum(
    "secret_provision_checkpoint.outcome",
    "SecretProvisionCheckpointOutcome",
    ["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_TERMINAL"],
)
SecretProvisionCheckpointFailureCode = _register_enum(
    "secret_provision_checkpoint.failure_code",
    "SecretProvisionCheckpointFailureCode",
    [
        "SECRET_STORE_UNAVAILABLE",
        "TRANSIENT_STORAGE_ERROR",
        "TRANSIENT_DATABASE_BUSY",
        "CAS_LOST",
        "AUTHORITY_REVOKED",
        "STAGED_OBJECT_INVALID",
        "INTEGRITY_CONFLICT",
    ],
)
CredentialRotationDisposition = _register_enum(
    "credential_rotation_request.disposition",
    "CredentialRotationDisposition",
    ["APPLIED", "CAS_LOST"],
)
CredentialRotationReceiptSourceKind = _register_enum(
    "credential_rotation_receipt.source_kind",
    "CredentialRotationReceiptSourceKind",
    ["ATTEMPT_ROTATION", "MANAGEMENT_PROVISION"],
)

# ---------------------------------------------------------------------------
# Worker-protocol-only vocabulary (docs/wiki/worker-protocol.md)
# ---------------------------------------------------------------------------

WorkerProtocolErrorCode = _register_enum(
    "worker_protocol.error_code",
    "WorkerProtocolErrorCode",
    [
        "MALFORMED",
        "SCHEMA_INVALID",
        "DIGEST_MISMATCH",
        "AUTH_INVALID",
        "CAPABILITY_DENIED",
        "ATTEMPT_UNKNOWN",
        "UPLOAD_UNKNOWN",
        "ATTEMPT_STALE",
        "ATTEMPT_ALREADY_CLAIMED",
        "IDEMPOTENCY_CONFLICT",
        "RESULT_ALREADY_ACCEPTED",
        "CLAIM_EXPIRED",
        "EXECUTION_DEADLINE_EXCEEDED",
        "UPLOAD_EXPIRED",
        "CONTROLLER_RATE_LIMIT",
        "CONTROLLER_MAINTENANCE",
        "CONTROLLER_UNAVAILABLE",
        "STORE_UNAVAILABLE",
    ],
)
ReviewSlotKind = _register_enum("review_slot.kind", "ReviewSlotKind", ["REVIEW", "ADJUDICATE"])
LivenessState = _register_enum(
    "attempt_liveness.state",
    "LivenessState",
    ["STARTING", "ACTIVE", "WAITING_PROVIDER", "VALIDATING", "SUBMITTING"],
)
LivenessControl = _register_enum(
    "attempt_liveness_result.control", "LivenessControl", ["CONTINUE", "CANCEL"]
)
CapacityScopeKind = _register_enum(
    "capacity_report.scope_kind",
    "CapacityScopeKind",
    ["WORKER_SESSION", "WORKER_PROFILE", "CAPACITY_POOL"],
)
CapacityAvailability = _register_enum(
    "capacity_report.availability", "CapacityAvailability", ["AVAILABLE", "UNAVAILABLE"]
)
CapacityPoolTemplateClass = _register_enum(
    "capacity_pool.template_class", "CapacityPoolTemplateClass", ["LEGACY", "V1_CLONE_FIXED"]
)

__all__ += [name for name in globals() if name[:1].isupper()]
