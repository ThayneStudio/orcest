"""SQLite single-writer substrate for Workflow-Control v1.

This module intentionally stops at the base storage layer. Later workflow
leaves add feature tables and reducers on top of these primitives.
"""

from __future__ import annotations

import dataclasses
import fcntl
import json
import os
import sqlite3
import subprocess
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Any, Self

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.digest import (
    affected_run_ids_digest,
    bare_canonical_digest,
    capability_public_key_digest,
    checkpoint_digest,
    config_bundle_hash,
    forge_observation_payload_digest,
    forge_observation_result_membership_digest,
    forge_observation_schedule_digest,
    forge_request_failure_fact_digest,
    is_valid_content_digest,
    launch_capability_claims_digest,
    policy_digest,
    receipt_digest,
    request_digest,
    resolution_digest,
    response_digest,
    review_assignment_digest,
    specification_digest,
    subject_refs_digest,
    work_item_discovery_set_digest,
    workflow_blob_digest,
)
from orcest.workflow_contract.v1.identity import is_lowercase_uuid, require_lowercase_uuid
from orcest.workflow_contract.v1.protocol_registry import (
    ATTEMPT_CLAIM_PROTOCOL,
    CANDIDATE_UPLOAD_EXPIRED_PROTOCOL,
    CAPABILITY_KEY_OPERATION_PROTOCOL,
    CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL,
    CONTROLLER_MODE_OPERATION_PROTOCOL,
    CONTROLLER_MODE_RESULT_PROTOCOL,
    FORGE_OBSERVATION_REQUEST_PROTOCOL,
    PROJECT_REGISTRATION_PROTOCOL,
    PROJECT_REGISTRATION_RESULT_PROTOCOL,
    SECRET_PROVISION_ACCEPTED_PROTOCOL,
    SECRET_PROVISION_REQUEST_PROTOCOL,
    SECRET_PROVISION_RESULT_PROTOCOL,
)

SCHEMA_VERSION = 10
DEFAULT_REDUCER_VERSION = "workflow-control-v1/reducer-0"
SUPPORTED_REDUCER_VERSIONS = frozenset({DEFAULT_REDUCER_VERSION})
CONTROLLER_ID = "ORCEST_V1"
PRIOR_STATE_NONE = "NONE"
_DEFAULT_DISCOVERY_INTERVAL_MS = 300_000
_NON_MAINTENANCE_CONTROLLER_MODES = frozenset(
    {"RUNNING", "INTAKE_PAUSED", "DISPATCH_PAUSED", "DRAINING"}
)

_FORBIDDEN_STATE_FS = {
    "9p",
    "afs",
    "autofs",
    "cifs",
    "fuse",
    "fuseblk",
    "nfs",
    "nfs4",
    "smb3",
    "smbfs",
}


class _CandidateUploadExpiredDuringPromotion(Exception):
    pass


class RunStoreError(RuntimeError):
    """Base class for run-store failures."""


class WriterLockError(RunStoreError):
    """Raised when another controller already owns the writer lock."""


class StartupIntegrityError(RunStoreError):
    """Raised when startup checks require fail-closed operation."""


class SchemaVersionError(StartupIntegrityError):
    """Raised for unsupported schema versions."""


class ReducerVersionError(StartupIntegrityError):
    """Raised for unsupported persisted reducer versions."""


class TransactionFault(RunStoreError):
    """Raised by test fault injection at a specific transaction boundary."""


class IdempotencyConflictError(RunStoreError):
    """Raised when a replay key is reused with different immutable content."""


class CasMismatchError(RunStoreError):
    """Raised when a monotonic compare-and-swap update loses its fence."""


class WorkflowGateClosedError(RunStoreError):
    """Raised when the durable controller mode or key registry forbids work."""


class FaultInjectionPoint(str, Enum):
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"
    BEFORE_RESPONSE_ACK = "before_response_ack"


@dataclass(frozen=True, slots=True)
class MaintenanceMode:
    """Fail-closed startup result for callers that choose not to raise."""

    reason: str
    dispatch_enabled: bool = False
    receipt_acceptance_enabled: bool = False
    publication_enabled: bool = False


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    project_id: str
    work_item_key: str
    specification_generation: int
    state: str
    terminal_outcome: str | None
    reducer_version: str
    current_revision: int
    created_at_ms: int
    updated_at_ms: int


@dataclass(frozen=True, slots=True)
class Transition:
    run_id: str
    transition_sequence: int
    transition_id: str
    prior_state: str
    trigger_kind: str
    trigger_id: str
    next_state: str
    reducer_version: str
    input_digest: str
    created_at_ms: int
    specification_generation: int
    admit_base_observation_id: str | None = None


@dataclass(frozen=True, slots=True)
class ImmutableFact:
    fact_kind: str
    fact_id: str
    payload_digest: str
    payload_json: str
    source_kind: str | None
    source_id: str | None
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class SourceUniqueRecord:
    source_kind: str
    source_id: str
    record_kind: str
    record_id: str
    payload_digest: str
    payload_json: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    outbox_id: str
    source_kind: str
    source_id: str
    destination: str
    protocol_version: str
    payload_digest: str
    payload_json: str
    next_delivery_at_ms: int
    state: str
    delivery_count: int
    created_at_ms: int
    attempt_id: str | None = None
    attempt_generation: int | None = None
    publication_id: str | None = None
    effect_generation: int | None = None


@dataclass(frozen=True, slots=True)
class ProjectionOutboxRecord:
    projection_outbox_id: str
    run_id: str
    transition_sequence: int
    kind: str
    target_kind: str
    target_id: str
    payload_digest: str
    payload_json: str
    idempotency_key: str
    state: str
    delivery_count: int
    next_delivery_at_ms: int
    created_at_ms: int
    publication_id: str | None = None
    effect_generation: int | None = None


@dataclass(frozen=True, slots=True)
class DurableOperation:
    operation_id: str
    operation_kind: str
    principal_id: str
    idempotency_key: str
    request_digest: str
    status: str
    response_json: str
    response_digest: str
    response_http_status: int
    committed_at_ms: int


@dataclass(frozen=True, slots=True)
class ControllerModeProjection:
    controller_id: str
    mode_revision: int
    mode: str | None
    dispatch_paused_intake_policy: str | None
    maintenance_prior_mode: str | None
    maintenance_prior_dispatch_paused_intake_policy: str | None
    last_operation_id: str | None


@dataclass(frozen=True, slots=True)
class ControllerModeOperationResult:
    controller_mode_operation_id: str
    operation_kind: str
    status: str
    response_http_status: int
    response_json: str
    response_digest: str
    completed_at_ms: int
    rejection_code: str | None = None
    mode_revision: int | None = None
    mode: str | None = None
    dispatch_paused_intake_policy: str | None = None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class CapabilityKeyRegistryProjection:
    registry_id: str
    registry_revision: int
    current_issuance_key_id: str | None
    last_operation_id: str | None


@dataclass(frozen=True, slots=True)
class CapabilitySigningKey:
    capability_signing_key_id: str
    registration_operation_id: str
    signature_algorithm: str
    public_verification_key: bytes
    public_key_digest: str
    private_signing_secret_ref: str
    registered_at_ms: int
    not_before_ms: int
    state: str
    retired_at_ms: int | None = None
    retirement_change_id: str | None = None
    retirement_principal_id: str | None = None
    retirement_authorization_digest: str | None = None
    revoked_at_ms: int | None = None
    revocation_change_id: str | None = None
    revocation_principal_id: str | None = None
    revocation_authorization_digest: str | None = None


@dataclass(frozen=True, slots=True)
class CapabilityKeyOperationResult:
    capability_key_operation_id: str
    kind: str
    status: str
    response_http_status: int
    response_json: str
    response_digest: str
    completed_at_ms: int
    rejection_code: str | None = None
    registry_revision: int | None = None
    current_issuance_key_id: str | None = None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ControllerGatePermissions:
    mode_revision: int
    mode: str | None
    registry_revision: int
    current_issuance_key_id: str | None
    new_admission: bool
    new_claims: bool
    first_result_mutation: bool
    existing_result_replay: bool
    forge_reconciliation: bool
    management_operations: bool


@dataclass(frozen=True, slots=True)
class _ControllerGateEvaluation:
    permissions: ControllerGatePermissions
    registry: CapabilityKeyRegistryProjection
    selected_key: CapabilitySigningKey | None


@dataclass(frozen=True, slots=True)
class IssuedCapabilityBinding:
    capability_jti: str
    capability_signing_key_id: str
    signature_algorithm: str
    claim_digest: str
    immutable_assignment_digest: str
    immutable_assignment_json: str
    capability_key_registry_revision: int
    issued_at_ms: int


@dataclass(frozen=True, slots=True)
class AttemptClaimRecord:
    attempt_claim_id: str
    protocol_version: str
    attempt_id: str
    activity_id: str
    attempt_generation: int
    offer_outbox_id: str
    worker_id: str
    worker_session_id: str
    worker_profile: str
    worker_build_revision: str
    request_digest: str
    claimed_at_ms: int
    execution_deadline_ms: int
    capability_auth_expires_at_ms: int
    attempt_capability_jti: str
    attempt_capability_digest: str
    attempt_capability_signing_key_id: str
    attempt_capability_signature_algorithm: str
    capability_key_registry_revision: int
    source_access_kind: str
    source_access_descriptor_json: str
    source_access_descriptor_digest: str
    response_contract_digest: str
    created_at_ms: int
    launch_nonce_id: str | None = None
    launch_capability_jti: str | None = None
    launch_capability_digest: str | None = None
    launch_capability_signing_key_id: str | None = None
    launch_capability_signature_algorithm: str | None = None
    source_read_secret_ref: str | None = None
    provider_secret_ref: str | None = None


@dataclass(frozen=True, slots=True)
class AttemptClaimReplayMaterialization:
    claim: AttemptClaimRecord
    attempt: AttemptRecord
    can_rematerialize_source: bool
    can_rematerialize_launch_capability: bool
    can_rematerialize_attempt_capability: bool


@dataclass(frozen=True, slots=True)
class LaunchAttestationRecord:
    launch_attestation_id: str
    attempt_id: str
    activity_id: str
    attempt_generation: int
    attempt_claim_id: str
    worker_id: str
    worker_session_id: str
    pool_manager_id: str
    runner_principal_id: str
    runner_image_digest: str
    runner_registration_revision: int
    launch_nonce_id: str
    launch_capability_digest: str
    launch_capability_signing_key_id: str
    launch_capability_signature_algorithm: str
    workspace_instance_id: str
    context_instance_id: str
    invocation_instance_id: str
    fresh_workspace: bool
    fresh_context: bool
    fresh_invocation: bool
    prepared_at_ms: int
    attested_at_ms: int
    runner_signing_key_id: str
    runner_signature_algorithm: str
    attestation_digest: str
    signature: str
    response_contract_digest: str
    accepted_at_ms: int
    workspace_parent_id: str | None = None
    context_parent_id: str | None = None
    invocation_parent_id: str | None = None
    provider_secret_ref: str | None = None
    provider_material_descriptor_json: str | None = None
    provider_material_descriptor_digest: str | None = None


@dataclass(frozen=True, slots=True)
class LaunchAcceptedReplay:
    attestation: LaunchAttestationRecord
    attempt: AttemptRecord
    status: str
    can_rematerialize_provider: bool


@dataclass(frozen=True, slots=True)
class SecretCurrentVersionProjection:
    """CAS-fenced current-version pointer and immutable owner/purpose binding."""

    secret_id: str
    purpose: str | None
    owner_scope_kind: str | None
    owner_scope_id: str | None
    provider_account_ref: str | None
    current_version: int
    last_operation_id: str | None


@dataclass(frozen=True, slots=True)
class SecretVersionRecord:
    secret_id: str
    version: int
    creation_receipt_id: str
    storage_path: str
    affected_run_ids_digest: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class CredentialRotationReceiptRecord:
    credential_rotation_receipt_id: str
    source_kind: str
    source_id: str
    secret_id: str
    expected_prior_version: int | None
    new_version: int
    purpose: str
    owner_scope_kind: str
    owner_scope_id: str
    secret_integrity_attestation_id: str
    receipt_digest: str
    created_at_ms: int
    provider_account_ref: str | None = None
    credential_rotation_request_id: str | None = None
    attempt_id: str | None = None
    activity_id: str | None = None
    attempt_generation: int | None = None
    worker_id: str | None = None
    worker_session_id: str | None = None
    attempt_capability_digest: str | None = None
    launch_attestation_id: str | None = None
    management_operation_id: str | None = None
    authenticated_principal_id: str | None = None
    authorization_context_digest: str | None = None


@dataclass(frozen=True, slots=True)
class SecretProvisionCheckpointRecord:
    secret_provision_checkpoint_id: str
    secret_provision_operation_id: str
    checkpoint_sequence: int
    phase: str
    outcome: str
    recorded_at_ms: int
    failure_code: str | None = None
    failure_evidence_digest: str | None = None
    next_retry_ms: int | None = None


@dataclass(frozen=True, slots=True)
class SecretProvisionOperationResult:
    secret_provision_operation_id: str
    mode: str
    secret_id: str
    target_version: int
    state: str
    response_http_status: int
    response_json: str
    response_digest: str
    created_at_ms: int
    expected_prior_version: int | None = None
    rejection_code: str | None = None
    new_version: int | None = None
    credential_rotation_receipt_id: str | None = None
    secret_store_staging_receipt_id: str | None = None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ForgeInstanceRecord:
    forge_instance_id: str
    adapter_kind: str
    canonical_origin: str
    credential_secret_id: str
    registration_provenance_version: int
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class ProjectRecord:
    project_id: str
    forge_instance_id: str
    installation_or_account_ref: str
    repository_external_id: str
    repository_locator: str
    default_ref: str
    trusted_base_policy_ref: str
    budget_policy_ref: str
    budget_reset_window_ref: str
    source_read_secret_id: str
    publication_secret_id: str
    registration_source_read_secret_version: int
    registration_publication_secret_version: int
    registration_revision: int
    registration_operation_id: str
    work_item_discovery_schedule_id: str
    registration_state: str


@dataclass(frozen=True, slots=True)
class ForgeObservationScheduleRecord:
    forge_observation_schedule_id: str
    schedule_kind: str
    project_id: str
    forge_instance_id: str
    schedule_revision: int
    state: str
    target_kind: str
    target_id: str
    minimum_interval_ms: int
    next_due_at_ms: int
    schedule_digest: str
    created_at_ms: int
    run_id: str | None = None
    publication_id: str | None = None
    terminal_duplicate_cleanup_reservation_id: str | None = None
    last_request_id: str | None = None
    last_discovery_search_revision: str | None = None
    last_discovery_set_digest: str | None = None


@dataclass(frozen=True, slots=True)
class ForgeObservationRequestRecord:
    forge_observation_request_id: str
    protocol_version: str
    forge_observation_schedule_id: str
    schedule_revision: int
    request_sequence: int
    request_kind: str
    project_id: str
    forge_instance_id: str
    target_kind: str
    target_id: str
    created_under_controller_mode_revision: int
    created_under_controller_mode: str
    credential_purpose: str
    credential_secret_id: str
    credential_secret_version: int
    request_idempotency_key: str
    request_digest: str
    state: str
    outbox_id: str
    next_attempt_ordinal: int
    created_at_ms: int
    run_id: str | None = None
    publication_id: str | None = None
    terminal_duplicate_cleanup_reservation_id: str | None = None
    controller_activity_id: str | None = None
    effect_generation: int | None = None
    controller_operation_digest: str | None = None
    terminal_duplicate_cleanup_action_id: str | None = None
    terminal_cleanup_operation_digest: str | None = None
    expected_prior_observation_sequence: int | None = None
    expected_external_revision: str | None = None
    expected_discovery_search_revision: str | None = None
    expected_discovery_set_digest: str | None = None
    last_failure_fact_id: str | None = None
    next_retry_ms: int | None = None
    result_observation_ids_digest: str | None = None
    result_discovery_search_revision: str | None = None
    result_discovery_set_digest: str | None = None
    completed_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class ForgeRequestFailureFactRecord:
    forge_request_failure_fact_id: str
    forge_observation_request_id: str
    request_attempt_ordinal: int
    project_id: str
    failure_kind: str
    failure_code: str
    failure_evidence_digest: str
    retry_not_before_ms: int
    request_digest: str
    fact_digest: str
    recorded_at_ms: int
    run_id: str | None = None
    publication_id: str | None = None
    terminal_duplicate_cleanup_reservation_id: str | None = None


@dataclass(frozen=True, slots=True)
class ForgeObservationRecord:
    forge_observation_id: str
    project_id: str
    target_kind: str
    target_id: str
    kind: str
    external_revision: str
    fact_json: str
    payload_digest: str
    observation_sequence: int
    observed_at_ms: int
    run_id: str | None = None
    publication_id: str | None = None
    created_by_forge_observation_request_id: str | None = None
    credential_purpose: str | None = None
    credential_secret_id: str | None = None
    credential_secret_version: int | None = None
    publication_effect_generation: int | None = None
    controller_activity_id: str | None = None
    controller_operation_digest: str | None = None
    terminal_duplicate_cleanup_reservation_id: str | None = None
    terminal_duplicate_cleanup_action_id: str | None = None
    terminal_cleanup_operation_digest: str | None = None
    adapter_event_id: str | None = None
    actor_principal_id: str | None = None
    actor_authorization_digest: str | None = None

    @property
    def fact(self) -> Any:
        return json.loads(self.fact_json)


@dataclass(frozen=True, slots=True)
class WorkflowBlobSqlRecord:
    blob_digest: str
    media_kind: str
    byte_length: int
    normalized_bytes: bytes
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class PolicyUpdateRecord:
    policy_update_id: str
    project_id: str
    policy_update_sequence: int
    server_policy_revision: str
    server_policy_blob_digest: str
    default_ref: str
    trusted_base_policy_ref: str
    budget_policy_ref: str
    budget_reset_window_ref: str
    source_kind: str
    source_id: str
    authenticated_principal_id: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class NormalizedPromptBlob:
    path: str
    git_blob: str
    blob_digest: str


@dataclass(frozen=True, slots=True)
class WorkItemSnapshotRecord:
    snapshot_id: str
    run_id: str
    snapshot_sequence: int
    source_kind: str
    source_id: str
    work_item_observation_id: str
    base_observation_id: str
    project_id: str
    work_item_external_id: str
    forge_revision: str
    title: str
    body: str
    specification_comments_json: str
    base_ref: str
    base_commit_json: str
    workflow_schema_version: str
    workflow_hash: str
    normalized_workflow_blob_digest: str
    normalized_prompt_blobs_json: str
    effective_policy_blob_digest: str
    server_policy_revision: str
    trusted_base_policy_ref: str
    budget_policy_ref: str
    budget_reset_window_ref: str
    policy_hash: str
    reducer_version: str
    specification_hash: str
    generation_input_hash: str
    base_movement_policy: str
    supersession_key: str
    snapshot_hash: str
    captured_at_ms: int

    @property
    def specification_comments(self) -> Any:
        return json.loads(self.specification_comments_json)

    @property
    def base_commit(self) -> Any:
        return json.loads(self.base_commit_json)

    @property
    def normalized_prompt_blobs(self) -> list[dict[str, str]]:
        loaded = json.loads(self.normalized_prompt_blobs_json)
        if not isinstance(loaded, list):
            raise RunStoreError("stored normalized_prompt_blobs is not a JSON array")
        return loaded


@dataclass(frozen=True, slots=True)
class SnapshotGenerationRecord:
    run_id: str
    specification_generation: int
    snapshot_id: str
    installed_transition_sequence: int
    installed_at_ms: int


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    run_id: str
    snapshot_id: str
    transition: Transition | None
    projection_outbox_id: str | None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ForgeObservationInput:
    """One normalized adapter result to commit as a Forge Observation.

    ``target_id`` is required only for a ``WORK_ITEM_DISCOVERY`` result (the
    discovered Work Item's stable external id); every other request kind
    targets the Request's own ``target_id`` and must leave it ``None``.
    """

    kind: str
    external_revision: str
    fact: Any
    target_id: str | None = None
    adapter_event_id: str | None = None
    actor_principal_id: str | None = None
    actor_authorization_digest: str | None = None
    observed_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class ForgeObservationRequestCompletion:
    request: "ForgeObservationRequestRecord"
    observation_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProjectRegistrationOperationResult:
    project_registration_operation_id: str
    authenticated_principal_id: str
    idempotency_key: str
    mode: str
    status: str
    response_http_status: int
    response_json: str
    response_digest: str
    resolution_digest: str
    completed_at_ms: int
    request_digest: str
    authorization_context_digest: str
    installation_or_account_ref: str
    requested_project_id: str | None = None
    expected_registration_revision: int | None = None
    rejection_code: str | None = None
    result_project_id: str | None = None
    result_registration_revision: int | None = None
    result_work_item_discovery_schedule_id: str | None = None
    resolved_forge_instance_id: str | None = None
    resolved_repository_external_id: str | None = None
    resolved_base_commit_json: str | None = None
    resolved_forge_api_secret_id: str | None = None
    resolved_forge_api_secret_version: int | None = None
    resolved_source_read_secret_id: str | None = None
    resolved_source_read_secret_version: int | None = None
    resolved_publication_secret_id: str | None = None
    resolved_publication_secret_version: int | None = None
    replayed: bool = False

    def public_response_json(self) -> str:
        """Canonical public body with the transport ``replayed`` projection applied."""
        body = json.loads(self.response_json)
        if not isinstance(body, dict):
            raise RunStoreError("stored registration response is not a JSON object")
        body["replayed"] = self.replayed
        return canonical_json_text(body)


@dataclass(frozen=True, slots=True)
class ActivityReviewAssignmentRecord:
    activity_id: str
    assignment_kind: str
    panel_round: int
    role: str
    subject_refs_digest: str
    context_digest: str
    assignment_digest: str
    created_at_ms: int
    reviewer_slot: str | None = None
    adjudication_round: int | None = None
    adjudicator_slot: str | None = None
    disputed_finding_ids_digest: str | None = None
    subject_refs: tuple[str, ...] = ()
    disputed_finding_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ActivityRecord:
    activity_id: str
    run_id: str
    activity_ordinal: int
    specification_generation: int
    policy_hash: str
    kind: str
    execution_class: str
    state: str
    repair_cycle: int
    recovery_cycle: int
    strategy_index: int
    rescue_epoch: int
    created_transition_sequence: int
    semantic_input_json: str
    semantic_input_digest: str
    idempotency_key: str
    created_at_ms: int
    updated_at_ms: int
    candidate_id: str | None = None
    forge_observation_id: str | None = None
    change_request_head_observation_id: str | None = None
    observed_change_request_head_json: str | None = None
    role: str | None = None
    recovery_tactic: str | None = None
    recovery_evidence_id: str | None = None
    slot: str | None = None
    input_ref_json: str | None = None
    review_assignment: ActivityReviewAssignmentRecord | None = None


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    attempt_id: str
    activity_id: str
    generation: int
    state: str
    protocol_version: str
    worker_profile: str
    offered_at_ms: int
    claim_timeout_ms: int
    claim_deadline_ms: int
    created_at_ms: int
    execution_profile_id: str | None = None
    provider: str | None = None
    model: str | None = None
    provider_account_ref: str | None = None
    provider_family: str | None = None
    model_family: str | None = None
    classification_revision: str | None = None
    provider_secret_ref: str | None = None
    claimed_worker_id: str | None = None
    claimed_worker_session_id: str | None = None
    claimed_at_ms: int | None = None
    execution_deadline_ms: int | None = None
    capability_auth_expires_at_ms: int | None = None
    last_liveness_observed_ms: int | None = None
    attempt_capability_jti: str | None = None
    attempt_capability_digest: str | None = None
    attempt_capability_signing_key_id: str | None = None
    attempt_capability_signature_algorithm: str | None = None
    attempt_claim_id: str | None = None
    launch_nonce_id: str | None = None
    launch_capability_digest: str | None = None
    launch_attestation_id: str | None = None
    launch_capability_consumed_at_ms: int | None = None
    terminal_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ActivityReviewAssignmentInput:
    """Typed review assignment input for a new ``REVIEW``/``ADJUDICATE`` Activity."""

    assignment_kind: str
    panel_round: int
    role: str
    context_digest: str
    subject_refs: tuple[str, ...]
    reviewer_slot: str | None = None
    adjudication_round: int | None = None
    adjudicator_slot: str | None = None
    disputed_finding_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AttemptOfferInput:
    """Immutable execution assignment for a new generation's ``OFFERED`` Attempt."""

    attempt_id: str
    generation: int
    protocol_version: str
    worker_profile: str
    offered_at_ms: int
    claim_timeout_ms: int
    execution_profile_id: str | None = None
    provider: str | None = None
    model: str | None = None
    provider_account_ref: str | None = None
    provider_family: str | None = None
    model_family: str | None = None
    classification_revision: str | None = None


@dataclass(frozen=True, slots=True)
class GitCommitRef:
    object_format: str
    oid: str

    def as_json(self) -> str:
        return canonical_json_text({"object_format": self.object_format, "oid": self.oid})


@dataclass(frozen=True, slots=True)
class CandidateUploadRecord:
    upload_id: str
    attempt_id: str
    activity_id: str
    attempt_generation: int
    idempotency_key: str
    request_digest: str
    media_type: str
    declared_bytes: int
    expected_bundle_digest: str
    expected_base_commit_json: str
    expected_repository_external_id: str
    expected_snapshot_id: str | None
    incoming_path: str | None
    computed_bundle_digest: str | None
    computed_bytes: int | None
    verified_tip_json: str | None
    artifact_bundle_digest: str | None
    artifact_storage_key: str | None
    promoted_at_ms: int | None
    consumed_candidate_id: str | None
    state: str
    expires_at_ms: int
    created_at_ms: int
    updated_at_ms: int

    @property
    def expected_base_commit(self) -> Any:
        return json.loads(self.expected_base_commit_json)

    @property
    def verified_tip(self) -> Any | None:
        return None if self.verified_tip_json is None else json.loads(self.verified_tip_json)


@dataclass(frozen=True, slots=True)
class ArtifactObjectRecord:
    bundle_digest: str
    storage_key: str
    byte_length: int
    installed_at_ms: int


@dataclass(frozen=True, slots=True)
class CandidateRecord:
    candidate_id: str
    run_id: str
    candidate_generation: int
    provenance_kind: str
    producing_activity_id: str
    worker_attempt_id: str | None
    worker_attempt_generation: int | None
    import_forge_observation_id: str | None
    object_format: str
    oid: str
    base_commit_json: str
    bundle_digest: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class CandidateDownloadRecord:
    candidate: CandidateRecord
    bundle: ArtifactObjectRecord


@dataclass(frozen=True, slots=True)
class ControllerOperationFactRecord:
    controller_operation_fact_id: str
    activity_id: str
    operation_kind: str
    outcome: str
    operation_digest: str
    fact_digest: str
    recorded_at_ms: int
    failure_category: str | None = None
    candidate_id: str | None = None
    forge_observation_id: str | None = None


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _enum_values(registry_name: str) -> tuple[str, ...]:
    return tuple(member.value for member in enums.get_enum(registry_name))


def _sql_in(values: Iterable[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _forge_observation_schedule_digest_fields(
    *,
    schedule_kind: str,
    project_id: str,
    forge_instance_id: str,
    target_kind: str,
    target_id: str,
    run_id: str | None,
    publication_id: str | None,
    terminal_duplicate_cleanup_reservation_id: str | None,
    minimum_interval_ms: int,
) -> dict[str, Any]:
    """Normalized authority/target/kind/cadence fields for ``schedule_digest``."""
    return {
        "schedule_kind": schedule_kind,
        "project_id": project_id,
        "forge_instance_id": forge_instance_id,
        "target_kind": target_kind,
        "target_id": target_id,
        "run_id": run_id,
        "publication_id": publication_id,
        "terminal_duplicate_cleanup_reservation_id": terminal_duplicate_cleanup_reservation_id,
        "minimum_interval_ms": minimum_interval_ms,
    }


def _forge_observation_schedules_ddl(table_name: str, *, if_not_exists: bool = False) -> str:
    """DDL for the Forge Observation Schedule table.

    Factored out (rather than inlined once in ``_SCHEMA``) so the version-5
    -> version-6 migration can rebuild a real pre-existing table into this
    exact final shape under a temporary name, the same rename-dance every
    earlier column-adding migration in this module uses.
    """
    schedule_kinds = _sql_in(_enum_values("forge_observation_schedule.schedule_kind"))
    target_kinds = _sql_in(_enum_values("forge_observation.target_kind"))
    states = _sql_in(_enum_values("forge_observation_schedule.state"))
    maybe_if_not_exists = "IF NOT EXISTS " if if_not_exists else ""
    return f"""
CREATE TABLE {maybe_if_not_exists}{table_name} (
  forge_observation_schedule_id TEXT PRIMARY KEY,
  schedule_kind TEXT NOT NULL CHECK (schedule_kind IN ({schedule_kinds})),
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE RESTRICT,
  forge_instance_id TEXT NOT NULL
    REFERENCES forge_instances(forge_instance_id) ON DELETE RESTRICT,
  target_kind TEXT NOT NULL CHECK (target_kind IN ({target_kinds})),
  target_id TEXT NOT NULL,
  run_id TEXT,
  publication_id TEXT,
  terminal_duplicate_cleanup_reservation_id TEXT,
  minimum_interval_ms INTEGER NOT NULL CHECK (minimum_interval_ms > 0),
  next_due_at_ms INTEGER NOT NULL CHECK (next_due_at_ms >= 0),
  schedule_revision INTEGER NOT NULL CHECK (schedule_revision >= 0),
  last_request_id TEXT,
  last_discovery_search_revision TEXT,
  last_discovery_set_digest TEXT,
  state TEXT NOT NULL CHECK (state IN ({states})),
  schedule_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  identity_key TEXT GENERATED ALWAYS AS (
    project_id || '|' || schedule_kind || '|' || target_kind || '|' || target_id || '|' ||
    COALESCE(run_id, '') || '|' || COALESCE(publication_id, '') || '|' ||
    COALESCE(terminal_duplicate_cleanup_reservation_id, '')
  ) STORED,
  CHECK (
    (schedule_kind = 'WORK_ITEM_DISCOVERY' AND target_kind = 'PROJECT')
    OR (schedule_kind = 'WORK_ITEM_POLL' AND target_kind = 'WORK_ITEM')
    OR (schedule_kind = 'BASE_HEAD_POLL' AND target_kind IN ('WORK_ITEM', 'PUBLICATION'))
    OR (
      schedule_kind IN (
        'REF_POLL', 'CHANGE_REQUEST_SEARCH', 'CHANGE_REQUEST_POLL', 'CI_POLL',
        'COMPLETE_MARKER_SEARCH'
      )
      AND target_kind = 'PUBLICATION'
    )
  ),
  CHECK (
    (target_kind = 'PROJECT' AND run_id IS NULL AND publication_id IS NULL)
    OR (target_kind = 'WORK_ITEM' AND publication_id IS NULL)
    OR (target_kind = 'PUBLICATION' AND run_id IS NOT NULL AND publication_id IS NOT NULL)
  ),
  CHECK (
    terminal_duplicate_cleanup_reservation_id IS NULL
    OR schedule_kind IN ('CHANGE_REQUEST_POLL', 'COMPLETE_MARKER_SEARCH')
  ),
  CHECK ((last_discovery_search_revision IS NULL) = (last_discovery_set_digest IS NULL)),
  CHECK (
    schedule_kind = 'WORK_ITEM_DISCOVERY'
    OR (last_discovery_search_revision IS NULL AND last_discovery_set_digest IS NULL)
  )
);
"""


def _require_digest(value: str, *, field: str) -> str:
    if not is_valid_content_digest(value):
        raise ValueError(f"{field} must be a v1 sha256 content digest")
    return value


def _require_git_commit_ref(value: Mapping[str, Any], *, field: str) -> GitCommitRef:
    object_format = value.get("object_format")
    oid = value.get("oid")
    if object_format not in {"sha1", "sha256"}:
        raise ValueError(f"{field}.object_format must be sha1 or sha256")
    if not isinstance(oid, str):
        raise ValueError(f"{field}.oid must be a string")
    expected_len = 40 if object_format == "sha1" else 64
    if len(oid) != expected_len or any(ch not in "0123456789abcdef" for ch in oid):
        raise ValueError(f"{field}.oid does not match object format")
    return GitCommitRef(object_format=object_format, oid=oid)


def _require_positive_int(value: int, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _require_nonempty_text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _require_json_text(value: Any) -> str:
    return value if isinstance(value, str) else canonical_json_text(value)


def _run_git(args: Sequence[str], *, cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise CasMismatchError(f"git Candidate validation failed: {detail}")
    return completed.stdout.strip()


def _validate_candidate_bundle(
    bundle_path: Path,
    *,
    expected_base_commit: GitCommitRef,
    expected_repository_external_id: str,
) -> GitCommitRef:
    with TemporaryDirectory(prefix="orcest-candidate-") as tmp:
        root = Path(tmp)
        repo = root / "repo.git"
        _run_git(
            ["init", "--bare", f"--object-format={expected_base_commit.object_format}", str(repo)],
            cwd=root,
        )
        lines = _run_git(["bundle", "list-heads", str(bundle_path)], cwd=repo).splitlines()
        heads = [line.split() for line in lines if line.strip()]
        matching = [
            parts for parts in heads if len(parts) == 2 and parts[1] == "refs/orcest/candidate"
        ]
        if len(heads) != 1 or len(matching) != 1:
            raise CasMismatchError("Candidate bundle must advertise exactly refs/orcest/candidate")
        tip_oid = matching[0][0]
        expected_len = 40 if expected_base_commit.object_format == "sha1" else 64
        if len(tip_oid) != expected_len or any(ch not in "0123456789abcdef" for ch in tip_oid):
            raise CasMismatchError("Candidate tip oid does not match object format")
        _run_git(
            ["fetch", "--no-tags", str(bundle_path), "refs/orcest/candidate:refs/orcest/candidate"],
            cwd=repo,
        )
        if _run_git(["cat-file", "-t", tip_oid], cwd=repo) != "commit":
            raise CasMismatchError("Candidate tip is not a commit")
        if _run_git(["cat-file", "-t", expected_base_commit.oid], cwd=repo) != "commit":
            raise CasMismatchError("Candidate base is not a commit")
        _run_git(["merge-base", "--is-ancestor", expected_base_commit.oid, tip_oid], cwd=repo)
        embedded_repo = _run_git(
            [
                "config",
                "--blob",
                f"{tip_oid}:.orcest/candidate-repository",
                "orcest.repositoryExternalId",
            ],
            cwd=repo,
        )
        if embedded_repo != expected_repository_external_id:
            raise CasMismatchError("Candidate bundle repository identity mismatch")
        return GitCommitRef(object_format=expected_base_commit.object_format, oid=tip_oid)


def _require_target_id(observation: "ForgeObservationInput", *, field: str) -> str:
    if not observation.target_id:
        raise ValueError(f"{field} is required for a WORK_ITEM_DISCOVERY result")
    return observation.target_id


def _response_digest_preimage(value: Any) -> Any:
    if not isinstance(value, dict) or "replayed" not in value:
        return value
    stripped = dict(value)
    stripped.pop("replayed")
    return stripped


def _row_to_run(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        run_id=row["run_id"],
        project_id=row["project_id"],
        work_item_key=row["work_item_key"],
        specification_generation=row["specification_generation"],
        state=row["state"],
        terminal_outcome=row["terminal_outcome"],
        reducer_version=row["reducer_version"],
        current_revision=row["current_revision"],
        created_at_ms=row["created_at_ms"],
        updated_at_ms=row["updated_at_ms"],
    )


def _row_to_transition(row: sqlite3.Row) -> Transition:
    return Transition(
        run_id=row["run_id"],
        transition_sequence=row["transition_sequence"],
        transition_id=row["transition_id"],
        prior_state=row["prior_state"],
        trigger_kind=row["trigger_kind"],
        trigger_id=row["trigger_id"],
        next_state=row["next_state"],
        reducer_version=row["reducer_version"],
        input_digest=row["input_digest"],
        created_at_ms=row["created_at_ms"],
        specification_generation=row["specification_generation"],
        admit_base_observation_id=row["admit_base_observation_id"],
    )


def _row_to_workflow_blob(row: sqlite3.Row) -> WorkflowBlobSqlRecord:
    return WorkflowBlobSqlRecord(
        blob_digest=row["blob_digest"],
        media_kind=row["media_kind"],
        byte_length=row["byte_length"],
        normalized_bytes=bytes(row["normalized_bytes"]),
        created_at_ms=row["created_at_ms"],
    )


def _row_to_policy_update(row: sqlite3.Row) -> PolicyUpdateRecord:
    return PolicyUpdateRecord(
        policy_update_id=row["policy_update_id"],
        project_id=row["project_id"],
        policy_update_sequence=row["policy_update_sequence"],
        server_policy_revision=row["server_policy_revision"],
        server_policy_blob_digest=row["server_policy_blob_digest"],
        default_ref=row["default_ref"],
        trusted_base_policy_ref=row["trusted_base_policy_ref"],
        budget_policy_ref=row["budget_policy_ref"],
        budget_reset_window_ref=row["budget_reset_window_ref"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        authenticated_principal_id=row["authenticated_principal_id"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_work_item_snapshot(row: sqlite3.Row) -> WorkItemSnapshotRecord:
    return WorkItemSnapshotRecord(
        snapshot_id=row["snapshot_id"],
        run_id=row["run_id"],
        snapshot_sequence=row["snapshot_sequence"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        work_item_observation_id=row["work_item_observation_id"],
        base_observation_id=row["base_observation_id"],
        project_id=row["project_id"],
        work_item_external_id=row["work_item_external_id"],
        forge_revision=row["forge_revision"],
        title=row["title"],
        body=row["body"],
        specification_comments_json=row["specification_comments_json"],
        base_ref=row["base_ref"],
        base_commit_json=row["base_commit_json"],
        workflow_schema_version=row["workflow_schema_version"],
        workflow_hash=row["workflow_hash"],
        normalized_workflow_blob_digest=row["normalized_workflow_blob_digest"],
        normalized_prompt_blobs_json=row["normalized_prompt_blobs_json"],
        effective_policy_blob_digest=row["effective_policy_blob_digest"],
        server_policy_revision=row["server_policy_revision"],
        trusted_base_policy_ref=row["trusted_base_policy_ref"],
        budget_policy_ref=row["budget_policy_ref"],
        budget_reset_window_ref=row["budget_reset_window_ref"],
        policy_hash=row["policy_hash"],
        reducer_version=row["reducer_version"],
        specification_hash=row["specification_hash"],
        generation_input_hash=row["generation_input_hash"],
        base_movement_policy=row["base_movement_policy"],
        supersession_key=row["supersession_key"],
        snapshot_hash=row["snapshot_hash"],
        captured_at_ms=row["captured_at_ms"],
    )


def _row_to_snapshot_generation(row: sqlite3.Row) -> SnapshotGenerationRecord:
    return SnapshotGenerationRecord(
        run_id=row["run_id"],
        specification_generation=row["specification_generation"],
        snapshot_id=row["snapshot_id"],
        installed_transition_sequence=row["installed_transition_sequence"],
        installed_at_ms=row["installed_at_ms"],
    )


def _row_to_fact(row: sqlite3.Row) -> ImmutableFact:
    return ImmutableFact(
        fact_kind=row["fact_kind"],
        fact_id=row["fact_id"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_source_record(row: sqlite3.Row) -> SourceUniqueRecord:
    return SourceUniqueRecord(
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        record_kind=row["record_kind"],
        record_id=row["record_id"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_outbox(row: sqlite3.Row) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=row["outbox_id"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        destination=row["destination"],
        attempt_id=row["attempt_id"],
        attempt_generation=row["attempt_generation"],
        publication_id=row["publication_id"],
        effect_generation=row["effect_generation"],
        protocol_version=row["protocol_version"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        next_delivery_at_ms=row["next_delivery_at_ms"],
        state=row["state"],
        delivery_count=row["delivery_count"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_projection(row: sqlite3.Row) -> ProjectionOutboxRecord:
    return ProjectionOutboxRecord(
        projection_outbox_id=row["projection_outbox_id"],
        run_id=row["run_id"],
        transition_sequence=row["transition_sequence"],
        kind=row["kind"],
        target_kind=row["target_kind"],
        target_id=row["target_id"],
        publication_id=row["publication_id"],
        effect_generation=row["effect_generation"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        idempotency_key=row["idempotency_key"],
        state=row["state"],
        delivery_count=row["delivery_count"],
        next_delivery_at_ms=row["next_delivery_at_ms"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_operation(row: sqlite3.Row) -> DurableOperation:
    return DurableOperation(
        operation_id=row["operation_id"],
        operation_kind=row["operation_kind"],
        principal_id=row["principal_id"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        status=row["status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        response_http_status=row["response_http_status"],
        committed_at_ms=row["committed_at_ms"],
    )


def _row_to_controller_mode(row: sqlite3.Row) -> ControllerModeProjection:
    return ControllerModeProjection(
        controller_id=row["controller_id"],
        mode_revision=row["mode_revision"],
        mode=row["mode"],
        dispatch_paused_intake_policy=row["dispatch_paused_intake_policy"],
        maintenance_prior_mode=row["maintenance_prior_mode"],
        maintenance_prior_dispatch_paused_intake_policy=row[
            "maintenance_prior_dispatch_paused_intake_policy"
        ],
        last_operation_id=row["last_operation_id"],
    )


def _row_to_controller_mode_operation(
    row: sqlite3.Row, *, replayed: bool
) -> ControllerModeOperationResult:
    return ControllerModeOperationResult(
        controller_mode_operation_id=row["controller_mode_operation_id"],
        operation_kind=row["operation_kind"],
        status=row["status"],
        rejection_code=row["rejection_code"],
        mode_revision=row["result_mode_revision"],
        mode=row["result_mode"],
        dispatch_paused_intake_policy=row["result_dispatch_paused_intake_policy"],
        response_http_status=row["response_http_status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        completed_at_ms=row["completed_at_ms"],
        replayed=replayed,
    )


def _row_to_capability_registry(row: sqlite3.Row) -> CapabilityKeyRegistryProjection:
    return CapabilityKeyRegistryProjection(
        registry_id=row["registry_id"],
        registry_revision=row["registry_revision"],
        current_issuance_key_id=row["current_issuance_key_id"],
        last_operation_id=row["last_operation_id"],
    )


def _row_to_capability_key(row: sqlite3.Row) -> CapabilitySigningKey:
    return CapabilitySigningKey(
        capability_signing_key_id=row["capability_signing_key_id"],
        registration_operation_id=row["registration_operation_id"],
        signature_algorithm=row["signature_algorithm"],
        public_verification_key=row["public_verification_key"],
        public_key_digest=row["public_key_digest"],
        private_signing_secret_ref=row["private_signing_secret_ref"],
        registered_at_ms=row["registered_at_ms"],
        not_before_ms=row["not_before_ms"],
        state=row["state"],
        retired_at_ms=row["retired_at_ms"],
        retirement_change_id=row["retirement_change_id"],
        retirement_principal_id=row["retirement_principal_id"],
        retirement_authorization_digest=row["retirement_authorization_digest"],
        revoked_at_ms=row["revoked_at_ms"],
        revocation_change_id=row["revocation_change_id"],
        revocation_principal_id=row["revocation_principal_id"],
        revocation_authorization_digest=row["revocation_authorization_digest"],
    )


def _row_to_capability_key_operation(
    row: sqlite3.Row, *, replayed: bool
) -> CapabilityKeyOperationResult:
    return CapabilityKeyOperationResult(
        capability_key_operation_id=row["capability_key_operation_id"],
        kind=row["kind"],
        status=row["status"],
        rejection_code=row["rejection_code"],
        registry_revision=row["result_registry_revision"],
        current_issuance_key_id=row["result_issuance_key_id"],
        response_http_status=row["response_http_status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        completed_at_ms=row["completed_at_ms"],
        replayed=replayed,
    )


def _row_to_issued_capability(row: sqlite3.Row) -> IssuedCapabilityBinding:
    return IssuedCapabilityBinding(
        capability_jti=row["capability_jti"],
        capability_signing_key_id=row["capability_signing_key_id"],
        signature_algorithm=row["signature_algorithm"],
        claim_digest=row["claim_digest"],
        immutable_assignment_digest=row["immutable_assignment_digest"],
        immutable_assignment_json=row["immutable_assignment_json"],
        capability_key_registry_revision=row["capability_key_registry_revision"],
        issued_at_ms=row["issued_at_ms"],
    )


def _row_to_attempt_claim(row: sqlite3.Row) -> AttemptClaimRecord:
    return AttemptClaimRecord(
        attempt_claim_id=row["attempt_claim_id"],
        protocol_version=row["protocol_version"],
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        attempt_generation=row["attempt_generation"],
        offer_outbox_id=row["offer_outbox_id"],
        worker_id=row["worker_id"],
        worker_session_id=row["worker_session_id"],
        worker_profile=row["worker_profile"],
        worker_build_revision=row["worker_build_revision"],
        request_digest=row["request_digest"],
        claimed_at_ms=row["claimed_at_ms"],
        execution_deadline_ms=row["execution_deadline_ms"],
        capability_auth_expires_at_ms=row["capability_auth_expires_at_ms"],
        attempt_capability_jti=row["attempt_capability_jti"],
        attempt_capability_digest=row["attempt_capability_digest"],
        attempt_capability_signing_key_id=row["attempt_capability_signing_key_id"],
        attempt_capability_signature_algorithm=row["attempt_capability_signature_algorithm"],
        capability_key_registry_revision=row["capability_key_registry_revision"],
        launch_nonce_id=row["launch_nonce_id"],
        launch_capability_jti=row["launch_capability_jti"],
        launch_capability_digest=row["launch_capability_digest"],
        launch_capability_signing_key_id=row["launch_capability_signing_key_id"],
        launch_capability_signature_algorithm=row["launch_capability_signature_algorithm"],
        source_access_kind=row["source_access_kind"],
        source_read_secret_ref=row["source_read_secret_ref"],
        provider_secret_ref=row["provider_secret_ref"],
        source_access_descriptor_json=row["source_access_descriptor_json"],
        source_access_descriptor_digest=row["source_access_descriptor_digest"],
        response_contract_digest=row["response_contract_digest"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_launch_attestation(row: sqlite3.Row) -> LaunchAttestationRecord:
    return LaunchAttestationRecord(
        launch_attestation_id=row["launch_attestation_id"],
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        attempt_generation=row["attempt_generation"],
        attempt_claim_id=row["attempt_claim_id"],
        worker_id=row["worker_id"],
        worker_session_id=row["worker_session_id"],
        pool_manager_id=row["pool_manager_id"],
        runner_principal_id=row["runner_principal_id"],
        runner_image_digest=row["runner_image_digest"],
        runner_registration_revision=row["runner_registration_revision"],
        launch_nonce_id=row["launch_nonce_id"],
        launch_capability_digest=row["launch_capability_digest"],
        launch_capability_signing_key_id=row["launch_capability_signing_key_id"],
        launch_capability_signature_algorithm=row["launch_capability_signature_algorithm"],
        workspace_instance_id=row["workspace_instance_id"],
        context_instance_id=row["context_instance_id"],
        invocation_instance_id=row["invocation_instance_id"],
        workspace_parent_id=row["workspace_parent_id"],
        context_parent_id=row["context_parent_id"],
        invocation_parent_id=row["invocation_parent_id"],
        fresh_workspace=bool(row["fresh_workspace"]),
        fresh_context=bool(row["fresh_context"]),
        fresh_invocation=bool(row["fresh_invocation"]),
        prepared_at_ms=row["prepared_at_ms"],
        attested_at_ms=row["attested_at_ms"],
        runner_signing_key_id=row["runner_signing_key_id"],
        runner_signature_algorithm=row["runner_signature_algorithm"],
        signature=row["signature"],
        attestation_digest=row["attestation_digest"],
        provider_secret_ref=row["provider_secret_ref"],
        provider_material_descriptor_json=row["provider_material_descriptor_json"],
        provider_material_descriptor_digest=row["provider_material_descriptor_digest"],
        response_contract_digest=row["response_contract_digest"],
        accepted_at_ms=row["accepted_at_ms"],
    )


def _row_to_forge_instance(row: sqlite3.Row) -> ForgeInstanceRecord:
    return ForgeInstanceRecord(
        forge_instance_id=row["forge_instance_id"],
        adapter_kind=row["adapter_kind"],
        canonical_origin=row["canonical_origin"],
        credential_secret_id=row["credential_secret_id"],
        registration_provenance_version=row["registration_provenance_version"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_project(row: sqlite3.Row) -> ProjectRecord:
    return ProjectRecord(
        project_id=row["project_id"],
        forge_instance_id=row["forge_instance_id"],
        installation_or_account_ref=row["installation_or_account_ref"],
        repository_external_id=row["repository_external_id"],
        repository_locator=row["repository_locator"],
        default_ref=row["default_ref"],
        trusted_base_policy_ref=row["trusted_base_policy_ref"],
        budget_policy_ref=row["budget_policy_ref"],
        budget_reset_window_ref=row["budget_reset_window_ref"],
        source_read_secret_id=row["source_read_secret_id"],
        publication_secret_id=row["publication_secret_id"],
        registration_source_read_secret_version=row["registration_source_read_secret_version"],
        registration_publication_secret_version=row["registration_publication_secret_version"],
        registration_revision=row["registration_revision"],
        registration_operation_id=row["registration_operation_id"],
        work_item_discovery_schedule_id=row["work_item_discovery_schedule_id"],
        registration_state=row["registration_state"],
    )


def _row_to_forge_observation_schedule(row: sqlite3.Row) -> ForgeObservationScheduleRecord:
    return ForgeObservationScheduleRecord(
        forge_observation_schedule_id=row["forge_observation_schedule_id"],
        schedule_kind=row["schedule_kind"],
        project_id=row["project_id"],
        forge_instance_id=row["forge_instance_id"],
        schedule_revision=row["schedule_revision"],
        state=row["state"],
        target_kind=row["target_kind"],
        target_id=row["target_id"],
        run_id=row["run_id"],
        publication_id=row["publication_id"],
        terminal_duplicate_cleanup_reservation_id=row["terminal_duplicate_cleanup_reservation_id"],
        minimum_interval_ms=row["minimum_interval_ms"],
        last_request_id=row["last_request_id"],
        last_discovery_search_revision=row["last_discovery_search_revision"],
        last_discovery_set_digest=row["last_discovery_set_digest"],
        next_due_at_ms=row["next_due_at_ms"],
        schedule_digest=row["schedule_digest"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_forge_observation_request(row: sqlite3.Row) -> ForgeObservationRequestRecord:
    return ForgeObservationRequestRecord(
        forge_observation_request_id=row["forge_observation_request_id"],
        protocol_version=row["protocol_version"],
        forge_observation_schedule_id=row["forge_observation_schedule_id"],
        schedule_revision=row["schedule_revision"],
        request_sequence=row["request_sequence"],
        request_kind=row["request_kind"],
        project_id=row["project_id"],
        forge_instance_id=row["forge_instance_id"],
        target_kind=row["target_kind"],
        target_id=row["target_id"],
        run_id=row["run_id"],
        publication_id=row["publication_id"],
        terminal_duplicate_cleanup_reservation_id=row["terminal_duplicate_cleanup_reservation_id"],
        created_under_controller_mode_revision=row["created_under_controller_mode_revision"],
        created_under_controller_mode=row["created_under_controller_mode"],
        credential_purpose=row["credential_purpose"],
        credential_secret_id=row["credential_secret_id"],
        credential_secret_version=row["credential_secret_version"],
        controller_activity_id=row["controller_activity_id"],
        effect_generation=row["effect_generation"],
        controller_operation_digest=row["controller_operation_digest"],
        terminal_duplicate_cleanup_action_id=row["terminal_duplicate_cleanup_action_id"],
        terminal_cleanup_operation_digest=row["terminal_cleanup_operation_digest"],
        expected_prior_observation_sequence=row["expected_prior_observation_sequence"],
        expected_external_revision=row["expected_external_revision"],
        expected_discovery_search_revision=row["expected_discovery_search_revision"],
        expected_discovery_set_digest=row["expected_discovery_set_digest"],
        request_idempotency_key=row["request_idempotency_key"],
        request_digest=row["request_digest"],
        state=row["state"],
        outbox_id=row["outbox_id"],
        next_attempt_ordinal=row["next_attempt_ordinal"],
        last_failure_fact_id=row["last_failure_fact_id"],
        next_retry_ms=row["next_retry_ms"],
        result_observation_ids_digest=row["result_observation_ids_digest"],
        result_discovery_search_revision=row["result_discovery_search_revision"],
        result_discovery_set_digest=row["result_discovery_set_digest"],
        created_at_ms=row["created_at_ms"],
        completed_at_ms=row["completed_at_ms"],
    )


def _row_to_forge_request_failure_fact(row: sqlite3.Row) -> ForgeRequestFailureFactRecord:
    return ForgeRequestFailureFactRecord(
        forge_request_failure_fact_id=row["forge_request_failure_fact_id"],
        forge_observation_request_id=row["forge_observation_request_id"],
        request_attempt_ordinal=row["request_attempt_ordinal"],
        project_id=row["project_id"],
        run_id=row["run_id"],
        publication_id=row["publication_id"],
        terminal_duplicate_cleanup_reservation_id=row["terminal_duplicate_cleanup_reservation_id"],
        failure_kind=row["failure_kind"],
        failure_code=row["failure_code"],
        failure_evidence_digest=row["failure_evidence_digest"],
        retry_not_before_ms=row["retry_not_before_ms"],
        request_digest=row["request_digest"],
        fact_digest=row["fact_digest"],
        recorded_at_ms=row["recorded_at_ms"],
    )


def _row_to_forge_observation(row: sqlite3.Row) -> ForgeObservationRecord:
    return ForgeObservationRecord(
        forge_observation_id=row["forge_observation_id"],
        project_id=row["project_id"],
        target_kind=row["target_kind"],
        target_id=row["target_id"],
        run_id=row["run_id"],
        publication_id=row["publication_id"],
        created_by_forge_observation_request_id=row["created_by_forge_observation_request_id"],
        credential_purpose=row["credential_purpose"],
        credential_secret_id=row["credential_secret_id"],
        credential_secret_version=row["credential_secret_version"],
        publication_effect_generation=row["publication_effect_generation"],
        controller_activity_id=row["controller_activity_id"],
        controller_operation_digest=row["controller_operation_digest"],
        terminal_duplicate_cleanup_reservation_id=row["terminal_duplicate_cleanup_reservation_id"],
        terminal_duplicate_cleanup_action_id=row["terminal_duplicate_cleanup_action_id"],
        terminal_cleanup_operation_digest=row["terminal_cleanup_operation_digest"],
        kind=row["kind"],
        external_revision=row["external_revision"],
        adapter_event_id=row["adapter_event_id"],
        actor_principal_id=row["actor_principal_id"],
        actor_authorization_digest=row["actor_authorization_digest"],
        fact_json=row["fact_json"],
        payload_digest=row["payload_digest"],
        observation_sequence=row["observation_sequence"],
        observed_at_ms=row["observed_at_ms"],
    )


def _row_to_activity(
    row: sqlite3.Row, *, review_assignment: ActivityReviewAssignmentRecord | None = None
) -> ActivityRecord:
    return ActivityRecord(
        activity_id=row["activity_id"],
        run_id=row["run_id"],
        activity_ordinal=row["activity_ordinal"],
        specification_generation=row["specification_generation"],
        policy_hash=row["policy_hash"],
        kind=row["kind"],
        execution_class=row["execution_class"],
        state=row["state"],
        candidate_id=row["candidate_id"],
        forge_observation_id=row["forge_observation_id"],
        change_request_head_observation_id=row["change_request_head_observation_id"],
        observed_change_request_head_json=row["observed_change_request_head_json"],
        role=row["role"],
        repair_cycle=row["repair_cycle"],
        recovery_cycle=row["recovery_cycle"],
        strategy_index=row["strategy_index"],
        recovery_tactic=row["recovery_tactic"],
        recovery_evidence_id=row["recovery_evidence_id"],
        rescue_epoch=row["rescue_epoch"],
        created_transition_sequence=row["created_transition_sequence"],
        semantic_input_json=row["semantic_input_json"],
        semantic_input_digest=row["semantic_input_digest"],
        idempotency_key=row["idempotency_key"],
        slot=row["slot"],
        input_ref_json=row["input_ref_json"],
        created_at_ms=row["created_at_ms"],
        updated_at_ms=row["updated_at_ms"],
        review_assignment=review_assignment,
    )


def _row_to_activity_review_assignment(
    row: sqlite3.Row,
    *,
    subject_refs: tuple[str, ...] = (),
    disputed_finding_ids: tuple[str, ...] = (),
) -> ActivityReviewAssignmentRecord:
    return ActivityReviewAssignmentRecord(
        activity_id=row["activity_id"],
        assignment_kind=row["assignment_kind"],
        panel_round=row["panel_round"],
        reviewer_slot=row["reviewer_slot"],
        adjudication_round=row["adjudication_round"],
        adjudicator_slot=row["adjudicator_slot"],
        role=row["role"],
        subject_refs_digest=row["subject_refs_digest"],
        context_digest=row["context_digest"],
        disputed_finding_ids_digest=row["disputed_finding_ids_digest"],
        assignment_digest=row["assignment_digest"],
        created_at_ms=row["created_at_ms"],
        subject_refs=subject_refs,
        disputed_finding_ids=disputed_finding_ids,
    )


def _review_assignment_digest_for_input(assignment: ActivityReviewAssignmentInput) -> str:
    subject_refs_digest_value = subject_refs_digest(assignment.subject_refs)
    disputed_digest_value = (
        bare_canonical_digest(list(assignment.disputed_finding_ids))
        if assignment.disputed_finding_ids
        else None
    )
    return review_assignment_digest(
        assignment_kind=assignment.assignment_kind,
        panel_round=assignment.panel_round,
        reviewer_slot=assignment.reviewer_slot,
        adjudication_round=assignment.adjudication_round,
        adjudicator_slot=assignment.adjudicator_slot,
        role=assignment.role,
        subject_refs_digest=subject_refs_digest_value,
        context_digest=assignment.context_digest,
        disputed_finding_ids_digest=disputed_digest_value,
    )


def _row_to_attempt(row: sqlite3.Row) -> AttemptRecord:
    return AttemptRecord(
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        generation=row["generation"],
        state=row["state"],
        protocol_version=row["protocol_version"],
        execution_profile_id=row["execution_profile_id"],
        worker_profile=row["worker_profile"],
        provider=row["provider"],
        model=row["model"],
        provider_account_ref=row["provider_account_ref"],
        provider_family=row["provider_family"],
        model_family=row["model_family"],
        classification_revision=row["classification_revision"],
        provider_secret_ref=row["provider_secret_ref"],
        offered_at_ms=row["offered_at_ms"],
        claim_timeout_ms=row["claim_timeout_ms"],
        claim_deadline_ms=row["claim_deadline_ms"],
        claimed_worker_id=row["claimed_worker_id"],
        claimed_worker_session_id=row["claimed_worker_session_id"],
        claimed_at_ms=row["claimed_at_ms"],
        execution_deadline_ms=row["execution_deadline_ms"],
        capability_auth_expires_at_ms=row["capability_auth_expires_at_ms"],
        last_liveness_observed_ms=row["last_liveness_observed_ms"],
        attempt_capability_jti=row["attempt_capability_jti"],
        attempt_capability_digest=row["attempt_capability_digest"],
        attempt_capability_signing_key_id=row["attempt_capability_signing_key_id"],
        attempt_capability_signature_algorithm=row["attempt_capability_signature_algorithm"],
        attempt_claim_id=row["attempt_claim_id"],
        launch_nonce_id=row["launch_nonce_id"],
        launch_capability_digest=row["launch_capability_digest"],
        launch_attestation_id=row["launch_attestation_id"],
        launch_capability_consumed_at_ms=row["launch_capability_consumed_at_ms"],
        terminal_reason=row["terminal_reason"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_candidate_upload(row: sqlite3.Row) -> CandidateUploadRecord:
    return CandidateUploadRecord(
        upload_id=row["upload_id"],
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        attempt_generation=row["attempt_generation"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        media_type=row["media_type"],
        declared_bytes=row["declared_bytes"],
        expected_bundle_digest=row["expected_bundle_digest"],
        expected_base_commit_json=row["expected_base_commit_json"],
        expected_repository_external_id=row["expected_repository_external_id"],
        expected_snapshot_id=row["expected_snapshot_id"],
        incoming_path=row["incoming_path"],
        computed_bundle_digest=row["computed_bundle_digest"],
        computed_bytes=row["computed_bytes"],
        verified_tip_json=row["verified_tip_json"],
        artifact_bundle_digest=row["artifact_bundle_digest"],
        artifact_storage_key=row["artifact_storage_key"],
        promoted_at_ms=row["promoted_at_ms"],
        consumed_candidate_id=row["consumed_candidate_id"],
        state=row["state"],
        expires_at_ms=row["expires_at_ms"],
        created_at_ms=row["created_at_ms"],
        updated_at_ms=row["updated_at_ms"],
    )


def _row_to_artifact_object(row: sqlite3.Row) -> ArtifactObjectRecord:
    return ArtifactObjectRecord(
        bundle_digest=row["bundle_digest"],
        storage_key=row["storage_key"],
        byte_length=row["byte_length"],
        installed_at_ms=row["installed_at_ms"],
    )


def _row_to_candidate(row: sqlite3.Row) -> CandidateRecord:
    return CandidateRecord(
        candidate_id=row["candidate_id"],
        run_id=row["run_id"],
        candidate_generation=row["candidate_generation"],
        provenance_kind=row["provenance_kind"],
        producing_activity_id=row["producing_activity_id"],
        worker_attempt_id=row["worker_attempt_id"],
        worker_attempt_generation=row["worker_attempt_generation"],
        import_forge_observation_id=row["import_forge_observation_id"],
        object_format=row["object_format"],
        oid=row["oid"],
        base_commit_json=row["base_commit_json"],
        bundle_digest=row["bundle_digest"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_controller_operation_fact(row: sqlite3.Row) -> ControllerOperationFactRecord:
    return ControllerOperationFactRecord(
        controller_operation_fact_id=row["controller_operation_fact_id"],
        activity_id=row["activity_id"],
        operation_kind=row["operation_kind"],
        outcome=row["outcome"],
        failure_category=row["failure_category"],
        candidate_id=row["candidate_id"],
        forge_observation_id=row["forge_observation_id"],
        operation_digest=row["operation_digest"],
        fact_digest=row["fact_digest"],
        recorded_at_ms=row["recorded_at_ms"],
    )


def _row_to_project_registration_operation(
    row: sqlite3.Row, *, replayed: bool
) -> ProjectRegistrationOperationResult:
    return ProjectRegistrationOperationResult(
        project_registration_operation_id=row["project_registration_operation_id"],
        authenticated_principal_id=row["authenticated_principal_id"],
        idempotency_key=row["idempotency_key"],
        mode=row["mode"],
        requested_project_id=row["requested_project_id"],
        expected_registration_revision=row["expected_registration_revision"],
        installation_or_account_ref=row["installation_or_account_ref"],
        request_digest=row["request_digest"],
        authorization_context_digest=row["authorization_context_digest"],
        resolved_forge_instance_id=row["resolved_forge_instance_id"],
        resolved_repository_external_id=row["resolved_repository_external_id"],
        resolved_base_commit_json=row["resolved_base_commit_json"],
        resolved_forge_api_secret_id=row["resolved_forge_api_secret_id"],
        resolved_forge_api_secret_version=row["resolved_forge_api_secret_version"],
        resolved_source_read_secret_id=row["resolved_source_read_secret_id"],
        resolved_source_read_secret_version=row["resolved_source_read_secret_version"],
        resolved_publication_secret_id=row["resolved_publication_secret_id"],
        resolved_publication_secret_version=row["resolved_publication_secret_version"],
        resolution_digest=row["resolution_digest"],
        status=row["status"],
        result_project_id=row["result_project_id"],
        result_registration_revision=row["result_registration_revision"],
        result_work_item_discovery_schedule_id=row["result_work_item_discovery_schedule_id"],
        rejection_code=row["rejection_code"],
        response_http_status=row["response_http_status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        completed_at_ms=row["completed_at_ms"],
        replayed=replayed,
    )


def _row_to_secret_current_version(row: sqlite3.Row) -> SecretCurrentVersionProjection:
    return SecretCurrentVersionProjection(
        secret_id=row["secret_id"],
        purpose=row["purpose"],
        owner_scope_kind=row["owner_scope_kind"],
        owner_scope_id=row["owner_scope_id"],
        provider_account_ref=row["provider_account_ref"],
        current_version=row["current_version"],
        last_operation_id=row["last_operation_id"],
    )


def _row_to_secret_version(row: sqlite3.Row) -> SecretVersionRecord:
    return SecretVersionRecord(
        secret_id=row["secret_id"],
        version=row["version"],
        creation_receipt_id=row["creation_receipt_id"],
        storage_path=row["storage_path"],
        affected_run_ids_digest=row["affected_run_ids_digest"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_credential_rotation_receipt(row: sqlite3.Row) -> CredentialRotationReceiptRecord:
    return CredentialRotationReceiptRecord(
        credential_rotation_receipt_id=row["credential_rotation_receipt_id"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        credential_rotation_request_id=row["credential_rotation_request_id"],
        secret_id=row["secret_id"],
        expected_prior_version=row["expected_prior_version"],
        new_version=row["new_version"],
        purpose=row["purpose"],
        owner_scope_kind=row["owner_scope_kind"],
        owner_scope_id=row["owner_scope_id"],
        provider_account_ref=row["provider_account_ref"],
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        attempt_generation=row["attempt_generation"],
        worker_id=row["worker_id"],
        worker_session_id=row["worker_session_id"],
        attempt_capability_digest=row["attempt_capability_digest"],
        launch_attestation_id=row["launch_attestation_id"],
        management_operation_id=row["management_operation_id"],
        authenticated_principal_id=row["authenticated_principal_id"],
        authorization_context_digest=row["authorization_context_digest"],
        secret_integrity_attestation_id=row["secret_integrity_attestation_id"],
        receipt_digest=row["receipt_digest"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_secret_provision_checkpoint(row: sqlite3.Row) -> SecretProvisionCheckpointRecord:
    return SecretProvisionCheckpointRecord(
        secret_provision_checkpoint_id=row["secret_provision_checkpoint_id"],
        secret_provision_operation_id=row["secret_provision_operation_id"],
        checkpoint_sequence=row["checkpoint_sequence"],
        phase=row["phase"],
        outcome=row["outcome"],
        failure_code=row["failure_code"],
        failure_evidence_digest=row["failure_evidence_digest"],
        next_retry_ms=row["next_retry_ms"],
        recorded_at_ms=row["recorded_at_ms"],
    )


def _mount_for(path: Path) -> tuple[Path, str]:
    path = path.resolve()
    best_mount = Path("/")
    best_type = ""
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return best_mount, best_type
    for line in lines:
        before, _, after = line.partition(" - ")
        if not after:
            continue
        fields = before.split()
        mount_point = Path(fields[4].replace("\\040", " "))
        fs_type = after.split()[0]
        try:
            path.relative_to(mount_point)
        except ValueError:
            continue
        if len(str(mount_point)) >= len(str(best_mount)):
            best_mount = mount_point
            best_type = fs_type
    return best_mount, best_type


def _verify_local_state_root(root: Path, *, min_free_bytes: int) -> None:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    if not root.is_dir():
        raise StartupIntegrityError(f"state root is not a directory: {root}")
    stat = root.stat()
    if stat.st_uid != os.getuid():
        raise StartupIntegrityError(f"state root {root} is not owned by uid {os.getuid()}")
    if stat.st_mode & 0o777 != 0o700:
        raise StartupIntegrityError(f"state root {root} must have mode 0700")
    _, fs_type = _mount_for(root)
    if fs_type in _FORBIDDEN_STATE_FS or fs_type.startswith("fuse."):
        raise StartupIntegrityError(f"state root {root} is on forbidden filesystem {fs_type}")
    free_bytes = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
    if free_bytes < min_free_bytes:
        raise StartupIntegrityError(
            f"state root {root} has {free_bytes} free bytes below safety floor {min_free_bytes}"
        )
    probe = root / f".fsync-probe.{uuid.uuid4().hex}"
    probe_fd = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(probe_fd, "wb") as file:
            file.write(b"orcest workflow store fsync probe\n")
            file.flush()
            os.fsync(file.fileno())
        dir_fd = os.open(root, os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        probe.unlink()


def _verify_lock_file(path: Path) -> None:
    if path.stat().st_mode & 0o777 != 0o600:
        raise StartupIntegrityError(f"{path.name} must have mode 0600")


def open_read_only(db_path: Path | str) -> sqlite3.Connection:
    """Open a workflow database for query-only reads."""

    conn = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  applied_at_ms INTEGER NOT NULL CHECK (applied_at_ms >= 0)
);

CREATE TABLE IF NOT EXISTS controller_mode (
  controller_id TEXT PRIMARY KEY CHECK (controller_id = '{CONTROLLER_ID}'),
  mode_revision INTEGER NOT NULL CHECK (mode_revision >= 0),
  mode TEXT CHECK (mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  dispatch_paused_intake_policy TEXT
    CHECK (dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  maintenance_prior_mode TEXT CHECK (
    maintenance_prior_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  maintenance_prior_dispatch_paused_intake_policy TEXT
    CHECK (maintenance_prior_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  last_operation_id TEXT,
  FOREIGN KEY (last_operation_id)
    REFERENCES controller_mode_operations(controller_mode_operation_id) ON DELETE RESTRICT,
  CHECK ((mode_revision = 0 AND mode IS NULL) OR (mode_revision > 0 AND mode IS NOT NULL)),
  CHECK (
    (mode = 'DISPATCH_PAUSED' AND dispatch_paused_intake_policy IS NOT NULL)
    OR (mode IS NULL AND dispatch_paused_intake_policy IS NULL)
    OR (mode != 'DISPATCH_PAUSED' AND dispatch_paused_intake_policy IS NULL)
  ),
  CHECK (
    (maintenance_prior_dispatch_paused_intake_policy IS NOT NULL)
    = (maintenance_prior_mode = 'DISPATCH_PAUSED')
  )
);

CREATE TABLE IF NOT EXISTS controller_mode_operations (
  controller_mode_operation_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  operation_kind TEXT NOT NULL CHECK (
    operation_kind IN ({_sql_in(_enum_values("controller_mode_operation.operation_kind"))})
  ),
  expected_mode_revision INTEGER NOT NULL CHECK (expected_mode_revision >= 0),
  expected_mode TEXT CHECK (expected_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  requested_mode TEXT CHECK (
    requested_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  requested_dispatch_paused_intake_policy TEXT
    CHECK (requested_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  backup_manifest_digest TEXT,
  backup_prior_mode TEXT CHECK (
    backup_prior_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  backup_prior_dispatch_paused_intake_policy TEXT
    CHECK (backup_prior_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ({_sql_in(_enum_values("controller_mode_operation.status"))})
  ),
  rejection_code TEXT CHECK (
    rejection_code IN ({_sql_in(_enum_values("controller_mode_operation.rejection_code"))})
  ),
  result_mode_revision INTEGER CHECK (result_mode_revision > 0),
  result_mode TEXT CHECK (result_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  result_dispatch_paused_intake_policy TEXT
    CHECK (result_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  completed_at_ms INTEGER NOT NULL CHECK (completed_at_ms >= 0),
  CHECK ((status = 'SUCCEEDED' AND rejection_code IS NULL)
    OR (status = 'REJECTED' AND rejection_code IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS capability_key_registry (
  registry_id TEXT PRIMARY KEY CHECK (registry_id = '{CONTROLLER_ID}'),
  registry_revision INTEGER NOT NULL CHECK (registry_revision >= 0),
  current_issuance_key_id TEXT,
  last_operation_id TEXT,
  FOREIGN KEY (current_issuance_key_id)
    REFERENCES capability_signing_keys(capability_signing_key_id) ON DELETE RESTRICT,
  FOREIGN KEY (last_operation_id)
    REFERENCES capability_key_operations(capability_key_operation_id) ON DELETE RESTRICT,
  CHECK (
    (registry_revision = 0 AND current_issuance_key_id IS NULL AND last_operation_id IS NULL)
    OR registry_revision > 0
  )
);

CREATE TABLE IF NOT EXISTS capability_key_operations (
  capability_key_operation_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (
    kind IN ({_sql_in(_enum_values("capability_key_operation.kind"))})
  ),
  expected_registry_revision INTEGER NOT NULL CHECK (expected_registry_revision >= 0),
  expected_issuance_key_id TEXT,
  target_capability_signing_key_id TEXT NOT NULL,
  replacement_issuance_key_id TEXT,
  register_public_verification_key BLOB,
  register_public_key_digest TEXT,
  register_private_signing_secret_ref TEXT,
  register_not_before_ms INTEGER CHECK (
    register_not_before_ms IS NULL OR register_not_before_ms >= 0
  ),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ({_sql_in(_enum_values("capability_key_operation.status"))})
  ),
  rejection_code TEXT CHECK (
    rejection_code IN ({_sql_in(_enum_values("capability_key_operation.rejection_code"))})
  ),
  result_registry_revision INTEGER CHECK (result_registry_revision > 0),
  result_issuance_key_id TEXT,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  completed_at_ms INTEGER NOT NULL CHECK (completed_at_ms >= 0),
  CHECK ((status = 'SUCCEEDED' AND rejection_code IS NULL)
    OR (status = 'REJECTED' AND rejection_code IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS capability_signing_keys (
  capability_signing_key_id TEXT PRIMARY KEY,
  registration_operation_id TEXT NOT NULL UNIQUE
    REFERENCES capability_key_operations(capability_key_operation_id) ON DELETE RESTRICT,
  signature_algorithm TEXT NOT NULL CHECK (
    signature_algorithm IN ({_sql_in(_enum_values("capability_signing_key.signature_algorithm"))})
  ),
  public_verification_key BLOB NOT NULL CHECK (length(public_verification_key) = 32),
  public_key_digest TEXT NOT NULL UNIQUE,
  private_signing_secret_ref TEXT NOT NULL,
  registered_at_ms INTEGER NOT NULL CHECK (registered_at_ms >= 0),
  not_before_ms INTEGER NOT NULL CHECK (not_before_ms >= 0),
  state TEXT NOT NULL CHECK (
    state IN ({_sql_in(_enum_values("capability_signing_key.state"))})
  ),
  retired_at_ms INTEGER CHECK (retired_at_ms IS NULL OR retired_at_ms >= registered_at_ms),
  retirement_change_id TEXT,
  retirement_principal_id TEXT,
  retirement_authorization_digest TEXT,
  revoked_at_ms INTEGER CHECK (revoked_at_ms IS NULL OR revoked_at_ms >= registered_at_ms),
  revocation_change_id TEXT,
  revocation_principal_id TEXT,
  revocation_authorization_digest TEXT,
  CHECK (
    (state = 'ACTIVE' AND retired_at_ms IS NULL AND retirement_change_id IS NULL
      AND retirement_principal_id IS NULL AND retirement_authorization_digest IS NULL
      AND revoked_at_ms IS NULL AND revocation_change_id IS NULL
      AND revocation_principal_id IS NULL AND revocation_authorization_digest IS NULL)
    OR (state = 'RETIRED' AND retired_at_ms IS NOT NULL AND retirement_change_id IS NOT NULL
      AND retirement_principal_id IS NOT NULL AND retirement_authorization_digest IS NOT NULL
      AND revoked_at_ms IS NULL AND revocation_change_id IS NULL
      AND revocation_principal_id IS NULL AND revocation_authorization_digest IS NULL)
    OR (state = 'REVOKED' AND revoked_at_ms IS NOT NULL AND revocation_change_id IS NOT NULL
      AND revocation_principal_id IS NOT NULL AND revocation_authorization_digest IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS capability_issuance_audit (
  capability_jti TEXT PRIMARY KEY,
  capability_signing_key_id TEXT NOT NULL
    REFERENCES capability_signing_keys(capability_signing_key_id) ON DELETE RESTRICT,
  signature_algorithm TEXT NOT NULL CHECK (
    signature_algorithm IN ({_sql_in(_enum_values("capability_signing_key.signature_algorithm"))})
  ),
  claim_digest TEXT NOT NULL,
  immutable_assignment_digest TEXT NOT NULL,
  immutable_assignment_json TEXT NOT NULL,
  capability_key_registry_revision INTEGER NOT NULL CHECK (
    capability_key_registry_revision > 0
  ),
  issued_at_ms INTEGER NOT NULL CHECK (issued_at_ms >= 0)
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  work_item_key TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("run.state"))})),
  current_snapshot_id TEXT,
  pending_snapshot_id TEXT,
  supersede_requested INTEGER NOT NULL DEFAULT 0 CHECK (supersede_requested IN (0, 1)),
  supersede_requested_transition_sequence INTEGER CHECK (
    supersede_requested_transition_sequence IS NULL
    OR supersede_requested_transition_sequence > 0
  ),
  terminal_outcome TEXT CHECK (
    terminal_outcome IN ({_sql_in(_enum_values("run.terminal_outcome"))})
  ),
  reducer_version TEXT NOT NULL,
  current_revision INTEGER NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active_work_item
ON runs(project_id, work_item_key) WHERE terminal_outcome IS NULL;

CREATE TABLE IF NOT EXISTS workflow_blobs (
  blob_digest TEXT PRIMARY KEY,
  media_kind TEXT NOT NULL CHECK (
    media_kind IN ({_sql_in(_enum_values("workflow_blob.media_kind"))})
  ),
  byte_length INTEGER NOT NULL CHECK (byte_length >= 0),
  normalized_bytes BLOB NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  CHECK (length(normalized_bytes) = byte_length)
);

CREATE TABLE IF NOT EXISTS policy_updates (
  policy_update_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE RESTRICT,
  policy_update_sequence INTEGER NOT NULL CHECK (policy_update_sequence > 0),
  server_policy_revision TEXT NOT NULL,
  server_policy_blob_digest TEXT NOT NULL
    REFERENCES workflow_blobs(blob_digest) ON DELETE RESTRICT,
  default_ref TEXT NOT NULL,
  trusted_base_policy_ref TEXT NOT NULL,
  budget_policy_ref TEXT NOT NULL,
  budget_reset_window_ref TEXT NOT NULL,
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("policy_update.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  authenticated_principal_id TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (project_id, policy_update_sequence),
  UNIQUE (source_kind, source_id)
);

CREATE TABLE IF NOT EXISTS work_item_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  snapshot_sequence INTEGER NOT NULL CHECK (snapshot_sequence > 0),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("work_item_snapshot.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  work_item_observation_id TEXT NOT NULL
    REFERENCES forge_observations(forge_observation_id) ON DELETE RESTRICT,
  base_observation_id TEXT NOT NULL
    REFERENCES forge_observations(forge_observation_id) ON DELETE RESTRICT,
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE RESTRICT,
  work_item_external_id TEXT NOT NULL,
  forge_revision TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  specification_comments_json TEXT NOT NULL,
  base_ref TEXT NOT NULL,
  base_commit_json TEXT NOT NULL,
  workflow_schema_version TEXT NOT NULL,
  workflow_hash TEXT NOT NULL,
  normalized_workflow_blob_digest TEXT NOT NULL
    REFERENCES workflow_blobs(blob_digest) ON DELETE RESTRICT,
  normalized_prompt_blobs_json TEXT NOT NULL,
  effective_policy_blob_digest TEXT NOT NULL
    REFERENCES workflow_blobs(blob_digest) ON DELETE RESTRICT,
  server_policy_revision TEXT NOT NULL,
  trusted_base_policy_ref TEXT NOT NULL,
  budget_policy_ref TEXT NOT NULL,
  budget_reset_window_ref TEXT NOT NULL,
  policy_hash TEXT NOT NULL,
  reducer_version TEXT NOT NULL,
  specification_hash TEXT NOT NULL,
  generation_input_hash TEXT NOT NULL,
  base_movement_policy TEXT NOT NULL CHECK (
    base_movement_policy IN ({_sql_in(_enum_values("snapshot.base_movement_policy"))})
  ),
  supersession_key TEXT NOT NULL,
  snapshot_hash TEXT NOT NULL,
  captured_at_ms INTEGER NOT NULL CHECK (captured_at_ms >= 0),
  UNIQUE (run_id, snapshot_sequence),
  UNIQUE (source_kind, source_id, run_id)
);

CREATE TABLE IF NOT EXISTS snapshot_generations (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  specification_generation INTEGER NOT NULL CHECK (specification_generation > 0),
  snapshot_id TEXT NOT NULL UNIQUE
    REFERENCES work_item_snapshots(snapshot_id) ON DELETE RESTRICT,
  installed_transition_sequence INTEGER NOT NULL CHECK (installed_transition_sequence > 0),
  installed_at_ms INTEGER NOT NULL CHECK (installed_at_ms >= 0),
  PRIMARY KEY (run_id, specification_generation),
  FOREIGN KEY (run_id, installed_transition_sequence)
    REFERENCES transitions(run_id, transition_sequence) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS transitions (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  transition_sequence INTEGER NOT NULL CHECK (transition_sequence > 0),
  transition_id TEXT NOT NULL UNIQUE,
  prior_state TEXT NOT NULL CHECK (
    prior_state = '{PRIOR_STATE_NONE}'
    OR prior_state IN ({_sql_in(_enum_values("run.state"))})
  ),
  trigger_kind TEXT NOT NULL CHECK (
    trigger_kind IN ({_sql_in(_enum_values("transition.trigger_kind"))})
  ),
  trigger_id TEXT NOT NULL,
  admit_base_observation_id TEXT,
  next_state TEXT NOT NULL CHECK (next_state IN ({_sql_in(_enum_values("run.state"))})),
  reducer_version TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (run_id, transition_sequence),
  UNIQUE (run_id, trigger_kind, trigger_id)
);

CREATE TABLE IF NOT EXISTS outbox (
  outbox_id TEXT PRIMARY KEY,
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("outbox_record.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  destination TEXT NOT NULL,
  attempt_id TEXT,
  attempt_generation INTEGER CHECK (attempt_generation IS NULL OR attempt_generation > 0),
  publication_id TEXT,
  effect_generation INTEGER CHECK (effect_generation IS NULL OR effect_generation > 0),
  protocol_version TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  next_delivery_at_ms INTEGER NOT NULL CHECK (next_delivery_at_ms >= 0),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("outbox_record.state"))})),
  delivery_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
  last_redis_epoch INTEGER CHECK (last_redis_epoch IS NULL OR last_redis_epoch >= 0),
  last_redis_entry TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (source_kind, source_id, destination, payload_digest)
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(state, next_delivery_at_ms);

CREATE TABLE IF NOT EXISTS projection_outbox (
  projection_outbox_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  transition_sequence INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (
    kind IN ({_sql_in(_enum_values("projection_outbox_record.kind"))})
  ),
  target_kind TEXT NOT NULL CHECK (
    target_kind IN ({_sql_in(_enum_values("projection_outbox_record.target_kind"))})
  ),
  target_id TEXT NOT NULL,
  publication_id TEXT,
  effect_generation INTEGER CHECK (effect_generation IS NULL OR effect_generation > 0),
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("outbox_record.state"))})),
  delivery_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
  next_delivery_at_ms INTEGER NOT NULL CHECK (next_delivery_at_ms >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  FOREIGN KEY (run_id, transition_sequence)
    REFERENCES transitions(run_id, transition_sequence) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_projection_outbox_pending
ON projection_outbox(state, next_delivery_at_ms);

CREATE TABLE IF NOT EXISTS immutable_facts (
  fact_kind TEXT NOT NULL,
  fact_id TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  source_kind TEXT,
  source_id TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (fact_kind, fact_id),
  UNIQUE (source_kind, source_id)
);

CREATE TABLE IF NOT EXISTS source_unique_records (
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  record_kind TEXT NOT NULL,
  record_id TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (source_kind, source_id),
  UNIQUE (record_kind, record_id)
);

CREATE TABLE IF NOT EXISTS revisioned_objects (
  object_kind TEXT NOT NULL,
  object_id TEXT NOT NULL,
  revision INTEGER NOT NULL CHECK (revision >= 0),
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0),
  PRIMARY KEY (object_kind, object_id)
);

CREATE TABLE IF NOT EXISTS durable_operations (
  operation_id TEXT PRIMARY KEY,
  operation_kind TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL,
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  committed_at_ms INTEGER NOT NULL CHECK (committed_at_ms >= 0),
  UNIQUE (principal_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS secret_current_versions (
  secret_id TEXT PRIMARY KEY,
  purpose TEXT NOT NULL CHECK (
    purpose IN ({_sql_in(_enum_values("secret_provision_operation.purpose"))})
  ),
  owner_scope_kind TEXT NOT NULL CHECK (
    owner_scope_kind IN ({_sql_in(_enum_values("secret_provision_operation.owner_scope_kind"))})
  ),
  owner_scope_id TEXT NOT NULL,
  provider_account_ref TEXT,
  current_version INTEGER NOT NULL CHECK (current_version >= 0),
  last_operation_id TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
  CHECK ((current_version = 0 AND last_operation_id IS NULL)
    OR (current_version > 0 AND last_operation_id IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS secret_versions (
  secret_id TEXT NOT NULL,
  version INTEGER NOT NULL CHECK (version > 0),
  creation_receipt_id TEXT NOT NULL,
  storage_path TEXT NOT NULL,
  affected_run_ids_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (secret_id, version)
);

CREATE TABLE IF NOT EXISTS credential_rotation_receipts (
  credential_rotation_receipt_id TEXT PRIMARY KEY,
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("credential_rotation_receipt.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  credential_rotation_request_id TEXT,
  secret_id TEXT NOT NULL,
  expected_prior_version INTEGER
    CHECK (expected_prior_version IS NULL OR expected_prior_version > 0),
  new_version INTEGER NOT NULL CHECK (new_version > 0),
  purpose TEXT NOT NULL CHECK (
    purpose IN ({_sql_in(_enum_values("secret_provision_operation.purpose"))})
  ),
  owner_scope_kind TEXT NOT NULL CHECK (
    owner_scope_kind IN ({_sql_in(_enum_values("secret_provision_operation.owner_scope_kind"))})
  ),
  owner_scope_id TEXT NOT NULL,
  provider_account_ref TEXT,
  attempt_id TEXT,
  activity_id TEXT,
  attempt_generation INTEGER CHECK (attempt_generation IS NULL OR attempt_generation > 0),
  worker_id TEXT,
  worker_session_id TEXT,
  attempt_capability_digest TEXT,
  launch_attestation_id TEXT,
  management_operation_id TEXT,
  authenticated_principal_id TEXT,
  authorization_context_digest TEXT,
  secret_integrity_attestation_id TEXT NOT NULL,
  receipt_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (secret_id, new_version),
  UNIQUE (source_kind, source_id),
  CHECK (
    (source_kind = 'MANAGEMENT_PROVISION'
      AND management_operation_id IS NOT NULL
      AND authenticated_principal_id IS NOT NULL
      AND authorization_context_digest IS NOT NULL
      AND credential_rotation_request_id IS NULL
      AND attempt_id IS NULL AND activity_id IS NULL AND attempt_generation IS NULL
      AND worker_id IS NULL AND worker_session_id IS NULL
      AND attempt_capability_digest IS NULL AND launch_attestation_id IS NULL)
    OR
    (source_kind = 'ATTEMPT_ROTATION'
      AND attempt_id IS NOT NULL AND activity_id IS NOT NULL AND attempt_generation IS NOT NULL
      AND worker_id IS NOT NULL AND worker_session_id IS NOT NULL
      AND attempt_capability_digest IS NOT NULL AND launch_attestation_id IS NOT NULL
      AND management_operation_id IS NULL
      AND authenticated_principal_id IS NULL AND authorization_context_digest IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS secret_provision_operations (
  secret_provision_operation_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (
    mode IN ({_sql_in(_enum_values("secret_provision_operation.mode"))})
  ),
  secret_id TEXT NOT NULL,
  expected_prior_version INTEGER
    CHECK (expected_prior_version IS NULL OR expected_prior_version > 0),
  target_version INTEGER NOT NULL CHECK (target_version > 0),
  purpose TEXT NOT NULL CHECK (
    purpose IN ({_sql_in(_enum_values("secret_provision_operation.purpose"))})
  ),
  owner_scope_kind TEXT NOT NULL CHECK (
    owner_scope_kind IN ({_sql_in(_enum_values("secret_provision_operation.owner_scope_kind"))})
  ),
  owner_scope_id TEXT NOT NULL,
  provider_account_ref TEXT,
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  secret_store_staging_receipt_id TEXT NOT NULL,
  secret_integrity_attestation_id TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK (
    state IN ({_sql_in(_enum_values("secret_provision_operation.state"))})
  ),
  rejection_code TEXT CHECK (
    rejection_code IN ({_sql_in(_enum_values("secret_provision_operation.rejection_code"))})
  ),
  new_version INTEGER CHECK (new_version IS NULL OR new_version > 0),
  credential_rotation_receipt_id TEXT,
  terminal_http_status INTEGER
    CHECK (terminal_http_status IS NULL OR terminal_http_status BETWEEN 100 AND 599),
  terminal_response_json TEXT,
  terminal_response_digest TEXT,
  last_checkpoint_id TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  CHECK (
    (state = 'PENDING'
      AND credential_rotation_receipt_id IS NULL AND new_version IS NULL
      AND rejection_code IS NULL AND terminal_http_status IS NULL
      AND terminal_response_json IS NULL AND terminal_response_digest IS NULL)
    OR (state = 'COMPLETED'
      AND credential_rotation_receipt_id IS NOT NULL AND new_version IS NOT NULL
      AND rejection_code IS NULL AND terminal_http_status IS NOT NULL
      AND terminal_response_json IS NOT NULL AND terminal_response_digest IS NOT NULL)
    OR (state = 'REJECTED'
      AND credential_rotation_receipt_id IS NULL AND new_version IS NULL
      AND rejection_code IS NOT NULL AND terminal_http_status IS NOT NULL
      AND terminal_response_json IS NOT NULL AND terminal_response_digest IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_secret_provision_target_reservation
ON secret_provision_operations(secret_id, target_version)
WHERE state IN ('PENDING', 'COMPLETED');

CREATE TABLE IF NOT EXISTS secret_provision_checkpoints (
  secret_provision_checkpoint_id TEXT PRIMARY KEY,
  secret_provision_operation_id TEXT NOT NULL
    REFERENCES secret_provision_operations(secret_provision_operation_id) ON DELETE RESTRICT,
  checkpoint_sequence INTEGER NOT NULL CHECK (checkpoint_sequence > 0),
  phase TEXT NOT NULL CHECK (
    phase IN ({_sql_in(_enum_values("secret_provision_checkpoint.phase"))})
  ),
  outcome TEXT NOT NULL CHECK (
    outcome IN ({_sql_in(_enum_values("secret_provision_checkpoint.outcome"))})
  ),
  failure_code TEXT CHECK (
    failure_code IN ({_sql_in(_enum_values("secret_provision_checkpoint.failure_code"))})
  ),
  failure_evidence_digest TEXT,
  next_retry_ms INTEGER CHECK (next_retry_ms IS NULL OR next_retry_ms >= 0),
  checkpoint_digest TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (secret_provision_operation_id, checkpoint_sequence),
  CHECK (outcome != 'SUCCEEDED' OR phase = 'INSTALL_VERSION'),
  CHECK (
    (outcome = 'SUCCEEDED'
      AND failure_code IS NULL AND failure_evidence_digest IS NULL AND next_retry_ms IS NULL)
    OR (outcome = 'FAILED_RETRYABLE'
      AND failure_code IN ('SECRET_STORE_UNAVAILABLE', 'TRANSIENT_STORAGE_ERROR',
        'TRANSIENT_DATABASE_BUSY')
      AND failure_evidence_digest IS NOT NULL AND next_retry_ms IS NOT NULL)
    OR (outcome = 'FAILED_TERMINAL'
      AND failure_code IN ('CAS_LOST', 'AUTHORITY_REVOKED', 'STAGED_OBJECT_INVALID',
        'INTEGRITY_CONFLICT')
      AND failure_evidence_digest IS NOT NULL AND next_retry_ms IS NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_secret_provision_checkpoint_terminal
ON secret_provision_checkpoints(secret_provision_operation_id)
WHERE outcome IN ('SUCCEEDED', 'FAILED_TERMINAL');

CREATE TABLE IF NOT EXISTS forge_instances (
  forge_instance_id TEXT PRIMARY KEY,
  adapter_kind TEXT NOT NULL CHECK (
    adapter_kind IN ({_sql_in(_enum_values("forge_instance.adapter_kind"))})
  ),
  canonical_origin TEXT NOT NULL UNIQUE,
  credential_secret_id TEXT NOT NULL,
  registration_provenance_version INTEGER NOT NULL CHECK (registration_provenance_version > 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  FOREIGN KEY (credential_secret_id)
    REFERENCES secret_current_versions(secret_id) ON DELETE RESTRICT
);

{_forge_observation_schedules_ddl("forge_observation_schedules", if_not_exists=True)}

CREATE TABLE IF NOT EXISTS forge_observation_requests (
  forge_observation_request_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  forge_observation_schedule_id TEXT NOT NULL
    REFERENCES forge_observation_schedules(forge_observation_schedule_id) ON DELETE RESTRICT,
  schedule_revision INTEGER NOT NULL CHECK (schedule_revision >= 0),
  request_sequence INTEGER NOT NULL CHECK (request_sequence > 0),
  request_kind TEXT NOT NULL CHECK (
    request_kind IN ({_sql_in(_enum_values("forge_observation_schedule.schedule_kind"))})
  ),
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE RESTRICT,
  forge_instance_id TEXT NOT NULL
    REFERENCES forge_instances(forge_instance_id) ON DELETE RESTRICT,
  target_kind TEXT NOT NULL CHECK (
    target_kind IN ({_sql_in(_enum_values("forge_observation.target_kind"))})
  ),
  target_id TEXT NOT NULL,
  run_id TEXT,
  publication_id TEXT,
  terminal_duplicate_cleanup_reservation_id TEXT,
  created_under_controller_mode_revision INTEGER NOT NULL
    CHECK (created_under_controller_mode_revision >= 0),
  created_under_controller_mode TEXT NOT NULL CHECK (
    created_under_controller_mode IN ('RUNNING', 'INTAKE_PAUSED', 'DISPATCH_PAUSED', 'DRAINING')
  ),
  credential_purpose TEXT NOT NULL CHECK (
    credential_purpose IN ('PROJECT_SOURCE_READ', 'PUBLICATION')
  ),
  credential_secret_id TEXT NOT NULL,
  credential_secret_version INTEGER NOT NULL CHECK (credential_secret_version > 0),
  controller_activity_id TEXT,
  effect_generation INTEGER CHECK (effect_generation IS NULL OR effect_generation > 0),
  controller_operation_digest TEXT,
  terminal_duplicate_cleanup_action_id TEXT,
  terminal_cleanup_operation_digest TEXT,
  expected_prior_observation_sequence INTEGER
    CHECK (expected_prior_observation_sequence IS NULL OR expected_prior_observation_sequence >= 0),
  expected_external_revision TEXT,
  expected_discovery_search_revision TEXT,
  expected_discovery_set_digest TEXT,
  request_idempotency_key TEXT NOT NULL UNIQUE,
  request_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('PENDING', 'COMPLETED', 'SUPERSEDED')),
  outbox_id TEXT NOT NULL REFERENCES outbox(outbox_id) ON DELETE RESTRICT,
  next_attempt_ordinal INTEGER NOT NULL CHECK (next_attempt_ordinal > 0),
  last_failure_fact_id TEXT,
  next_retry_ms INTEGER,
  result_observation_ids_digest TEXT,
  result_discovery_search_revision TEXT,
  result_discovery_set_digest TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  completed_at_ms INTEGER CHECK (completed_at_ms IS NULL OR completed_at_ms >= created_at_ms),
  UNIQUE (forge_observation_schedule_id, request_sequence),
  CHECK (
    (request_kind = 'WORK_ITEM_DISCOVERY' AND target_kind = 'PROJECT')
    OR (request_kind = 'WORK_ITEM_POLL' AND target_kind = 'WORK_ITEM')
    OR (request_kind = 'BASE_HEAD_POLL' AND target_kind IN ('WORK_ITEM', 'PUBLICATION'))
    OR (
      request_kind IN (
        'REF_POLL', 'CHANGE_REQUEST_SEARCH', 'CHANGE_REQUEST_POLL', 'CI_POLL',
        'COMPLETE_MARKER_SEARCH'
      )
      AND target_kind = 'PUBLICATION'
    )
  ),
  CHECK (
    (target_kind = 'PROJECT' AND run_id IS NULL AND publication_id IS NULL)
    OR (target_kind = 'WORK_ITEM' AND publication_id IS NULL)
    OR (target_kind = 'PUBLICATION' AND run_id IS NOT NULL AND publication_id IS NOT NULL)
  ),
  CHECK ((last_failure_fact_id IS NULL) = (next_retry_ms IS NULL)),
  CHECK ((controller_activity_id IS NULL) = (controller_operation_digest IS NULL)),
  CHECK (
    (terminal_duplicate_cleanup_action_id IS NULL) = (terminal_cleanup_operation_digest IS NULL)
  ),
  CHECK ((expected_discovery_search_revision IS NULL) = (expected_discovery_set_digest IS NULL)),
  CHECK (
    request_kind = 'WORK_ITEM_DISCOVERY'
    OR (expected_discovery_search_revision IS NULL AND expected_discovery_set_digest IS NULL)
  ),
  CHECK (
    request_kind != 'WORK_ITEM_DISCOVERY'
    OR (expected_prior_observation_sequence IS NULL AND expected_external_revision IS NULL)
  ),
  CHECK (
    request_kind = 'WORK_ITEM_DISCOVERY'
    OR (result_discovery_search_revision IS NULL AND result_discovery_set_digest IS NULL)
  ),
  CHECK (
    (state = 'PENDING'
      AND completed_at_ms IS NULL AND result_observation_ids_digest IS NULL
      AND result_discovery_search_revision IS NULL AND result_discovery_set_digest IS NULL)
    OR (state != 'PENDING'
      AND completed_at_ms IS NOT NULL AND result_observation_ids_digest IS NOT NULL)
  ),
  CHECK (
    state != 'COMPLETED' OR request_kind != 'WORK_ITEM_DISCOVERY'
    OR (result_discovery_search_revision IS NOT NULL AND result_discovery_set_digest IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_forge_observation_request_one_pending
ON forge_observation_requests(forge_observation_schedule_id) WHERE state = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_forge_observation_request_pending_retry
ON forge_observation_requests(state, next_retry_ms) WHERE state = 'PENDING';

CREATE TABLE IF NOT EXISTS forge_observations (
  forge_observation_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE RESTRICT,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('WORK_ITEM', 'PUBLICATION')),
  target_id TEXT NOT NULL,
  run_id TEXT,
  publication_id TEXT,
  created_by_forge_observation_request_id TEXT
    REFERENCES forge_observation_requests(forge_observation_request_id) ON DELETE RESTRICT,
  credential_purpose TEXT CHECK (
    credential_purpose IS NULL OR credential_purpose IN ('PROJECT_SOURCE_READ', 'PUBLICATION')
  ),
  credential_secret_id TEXT,
  credential_secret_version INTEGER CHECK (
    credential_secret_version IS NULL OR credential_secret_version > 0
  ),
  publication_effect_generation INTEGER CHECK (
    publication_effect_generation IS NULL OR publication_effect_generation > 0
  ),
  controller_activity_id TEXT,
  controller_operation_digest TEXT,
  terminal_duplicate_cleanup_reservation_id TEXT,
  terminal_duplicate_cleanup_action_id TEXT,
  terminal_cleanup_operation_digest TEXT,
  kind TEXT NOT NULL CHECK (kind IN ({_sql_in(_enum_values("forge_observation.kind"))})),
  external_revision TEXT NOT NULL,
  adapter_event_id TEXT,
  actor_principal_id TEXT,
  actor_authorization_digest TEXT,
  fact_json TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  observation_sequence INTEGER NOT NULL CHECK (observation_sequence > 0),
  observed_at_ms INTEGER NOT NULL CHECK (observed_at_ms >= 0),
  UNIQUE (project_id, target_kind, target_id, observation_sequence),
  CHECK ((credential_purpose IS NULL) = (credential_secret_id IS NULL)),
  CHECK ((credential_secret_id IS NULL) = (credential_secret_version IS NULL)),
  CHECK ((created_by_forge_observation_request_id IS NULL) = (credential_purpose IS NULL)),
  CHECK ((controller_activity_id IS NULL) = (controller_operation_digest IS NULL)),
  CHECK (
    (terminal_duplicate_cleanup_action_id IS NULL) = (terminal_cleanup_operation_digest IS NULL)
  ),
  CHECK (
    terminal_duplicate_cleanup_action_id IS NULL
    OR terminal_duplicate_cleanup_reservation_id IS NOT NULL
  ),
  CHECK (target_kind != 'WORK_ITEM' OR publication_id IS NULL),
  CHECK ((actor_principal_id IS NULL) = (actor_authorization_digest IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_forge_observation_adapter_event
ON forge_observations(project_id, target_kind, target_id, adapter_event_id)
WHERE adapter_event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_forge_observation_target_sequence
ON forge_observations(project_id, target_kind, target_id, observation_sequence DESC);

CREATE TABLE IF NOT EXISTS forge_observation_request_results (
  forge_observation_request_id TEXT NOT NULL
    REFERENCES forge_observation_requests(forge_observation_request_id) ON DELETE RESTRICT,
  observation_ordinal INTEGER NOT NULL CHECK (observation_ordinal >= 0),
  forge_observation_id TEXT NOT NULL
    REFERENCES forge_observations(forge_observation_id) ON DELETE RESTRICT,
  PRIMARY KEY (forge_observation_request_id, observation_ordinal),
  UNIQUE (forge_observation_request_id, forge_observation_id)
);

CREATE TABLE IF NOT EXISTS forge_request_failure_facts (
  forge_request_failure_fact_id TEXT PRIMARY KEY,
  forge_observation_request_id TEXT NOT NULL
    REFERENCES forge_observation_requests(forge_observation_request_id) ON DELETE RESTRICT,
  request_attempt_ordinal INTEGER NOT NULL CHECK (request_attempt_ordinal > 0),
  project_id TEXT NOT NULL,
  run_id TEXT,
  publication_id TEXT,
  terminal_duplicate_cleanup_reservation_id TEXT,
  failure_kind TEXT NOT NULL CHECK (failure_kind IN ('TIMEOUT', 'RATE_LIMIT', 'UNAVAILABLE')),
  failure_code TEXT NOT NULL,
  failure_evidence_digest TEXT NOT NULL,
  retry_not_before_ms INTEGER NOT NULL CHECK (retry_not_before_ms >= 0),
  request_digest TEXT NOT NULL,
  fact_digest TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (forge_observation_request_id, request_attempt_ordinal)
);

CREATE TABLE IF NOT EXISTS project_registration_operations (
  project_registration_operation_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  authenticated_principal_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (
    mode IN ({_sql_in(_enum_values("project_registration_operation.mode"))})
  ),
  requested_project_id TEXT,
  expected_registration_revision INTEGER
    CHECK (expected_registration_revision IS NULL OR expected_registration_revision > 0),
  installation_or_account_ref TEXT NOT NULL,
  request_json TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  resolved_forge_instance_id TEXT,
  resolved_repository_external_id TEXT,
  resolved_base_commit_json TEXT,
  resolved_forge_api_secret_id TEXT,
  resolved_forge_api_secret_version INTEGER
    CHECK (resolved_forge_api_secret_version IS NULL OR resolved_forge_api_secret_version > 0),
  resolved_source_read_secret_id TEXT,
  resolved_source_read_secret_version INTEGER
    CHECK (resolved_source_read_secret_version IS NULL OR resolved_source_read_secret_version > 0),
  resolved_publication_secret_id TEXT,
  resolved_publication_secret_version INTEGER
    CHECK (resolved_publication_secret_version IS NULL OR resolved_publication_secret_version > 0),
  resolution_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ({_sql_in(_enum_values("project_registration_operation.status"))})
  ),
  result_project_id TEXT,
  result_registration_revision INTEGER
    CHECK (result_registration_revision IS NULL OR result_registration_revision > 0),
  result_work_item_discovery_schedule_id TEXT,
  rejection_code TEXT CHECK (
    rejection_code IN ({_sql_in(_enum_values("project_registration_operation.rejection_code"))})
  ),
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  completed_at_ms INTEGER NOT NULL CHECK (completed_at_ms >= 0),
  UNIQUE (authenticated_principal_id, idempotency_key),
  CHECK (
    (mode = 'REGISTER' AND requested_project_id IS NULL
      AND expected_registration_revision IS NULL)
    OR (mode = 'REVALIDATE' AND requested_project_id IS NOT NULL
      AND expected_registration_revision IS NOT NULL)
  ),
  CHECK (
    (status = 'SUCCEEDED'
      AND rejection_code IS NULL
      AND result_project_id IS NOT NULL
      AND result_registration_revision IS NOT NULL
      AND result_work_item_discovery_schedule_id IS NOT NULL
      AND resolved_forge_instance_id IS NOT NULL
      AND resolved_repository_external_id IS NOT NULL
      AND resolved_base_commit_json IS NOT NULL
      AND resolved_forge_api_secret_id IS NOT NULL
      AND resolved_forge_api_secret_version IS NOT NULL
      AND resolved_source_read_secret_id IS NOT NULL
      AND resolved_source_read_secret_version IS NOT NULL
      AND resolved_publication_secret_id IS NOT NULL
      AND resolved_publication_secret_version IS NOT NULL)
    OR (status = 'REJECTED'
      AND rejection_code IS NOT NULL
      AND result_project_id IS NULL
      AND result_registration_revision IS NULL
      AND result_work_item_discovery_schedule_id IS NULL
      AND resolved_forge_api_secret_id IS NULL
      AND resolved_forge_api_secret_version IS NULL
      AND resolved_source_read_secret_id IS NULL
      AND resolved_source_read_secret_version IS NULL
      AND resolved_publication_secret_id IS NULL
      AND resolved_publication_secret_version IS NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_registration_success_revision
ON project_registration_operations(result_project_id, result_registration_revision)
WHERE status = 'SUCCEEDED';

CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  forge_instance_id TEXT NOT NULL
    REFERENCES forge_instances(forge_instance_id) ON DELETE RESTRICT,
  installation_or_account_ref TEXT NOT NULL,
  repository_external_id TEXT NOT NULL,
  repository_locator TEXT NOT NULL,
  default_ref TEXT NOT NULL,
  trusted_base_policy_ref TEXT NOT NULL,
  budget_policy_ref TEXT NOT NULL,
  budget_reset_window_ref TEXT NOT NULL,
  source_read_secret_id TEXT NOT NULL
    REFERENCES secret_current_versions(secret_id) ON DELETE RESTRICT,
  publication_secret_id TEXT NOT NULL
    REFERENCES secret_current_versions(secret_id) ON DELETE RESTRICT,
  registration_source_read_secret_version INTEGER NOT NULL
    CHECK (registration_source_read_secret_version > 0),
  registration_publication_secret_version INTEGER NOT NULL
    CHECK (registration_publication_secret_version > 0),
  registration_revision INTEGER NOT NULL CHECK (registration_revision > 0),
  registration_operation_id TEXT NOT NULL,
  work_item_discovery_schedule_id TEXT NOT NULL,
  registration_state TEXT NOT NULL CHECK (
    registration_state IN ({_sql_in(_enum_values("project.registration_state"))})
  ),
  UNIQUE (forge_instance_id, repository_external_id),
  CHECK (source_read_secret_id != publication_secret_id)
);

CREATE TABLE IF NOT EXISTS activities (
  activity_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  activity_ordinal INTEGER NOT NULL CHECK (activity_ordinal > 0),
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  policy_hash TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ({_sql_in(_enum_values("activity.kind"))})),
  execution_class TEXT NOT NULL CHECK (
    execution_class IN ({_sql_in(_enum_values("activity.execution_class"))})
  ),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("activity.state"))})),
  input_ref_json TEXT,
  candidate_id TEXT,
  forge_observation_id TEXT,
  change_request_head_observation_id TEXT,
  observed_change_request_head_json TEXT,
  role TEXT,
  repair_cycle INTEGER NOT NULL DEFAULT 0 CHECK (repair_cycle >= 0),
  recovery_cycle INTEGER NOT NULL DEFAULT 0 CHECK (recovery_cycle >= 0),
  strategy_index INTEGER NOT NULL DEFAULT 0 CHECK (strategy_index >= 0),
  recovery_tactic TEXT,
  recovery_evidence_id TEXT,
  rescue_epoch INTEGER NOT NULL DEFAULT 0 CHECK (rescue_epoch >= 0),
  created_transition_sequence INTEGER NOT NULL CHECK (created_transition_sequence > 0),
  semantic_input_json TEXT NOT NULL,
  semantic_input_digest TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  slot TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
  UNIQUE (run_id, activity_ordinal),
  UNIQUE (run_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS activity_review_assignments (
  activity_id TEXT PRIMARY KEY REFERENCES activities(activity_id) ON DELETE RESTRICT,
  assignment_kind TEXT NOT NULL CHECK (
    assignment_kind IN ({_sql_in(_enum_values("activity_review_assignment.assignment_kind"))})
  ),
  panel_round INTEGER NOT NULL CHECK (panel_round > 0),
  reviewer_slot TEXT,
  adjudication_round INTEGER CHECK (adjudication_round IS NULL OR adjudication_round = 1),
  adjudicator_slot TEXT,
  role TEXT NOT NULL,
  subject_refs_digest TEXT NOT NULL,
  context_digest TEXT NOT NULL,
  disputed_finding_ids_digest TEXT,
  assignment_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  CHECK (
    (assignment_kind = 'REVIEW'
      AND reviewer_slot IS NOT NULL
      AND adjudication_round IS NULL AND adjudicator_slot IS NULL
      AND disputed_finding_ids_digest IS NULL)
    OR
    (assignment_kind = 'ADJUDICATE'
      AND reviewer_slot IS NULL
      AND adjudication_round = 1 AND adjudicator_slot = 'default'
      AND role = 'adjudicator'
      AND disputed_finding_ids_digest IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS activity_review_subjects (
  activity_id TEXT NOT NULL REFERENCES activity_review_assignments(activity_id) ON DELETE RESTRICT,
  subject_ordinal INTEGER NOT NULL CHECK (subject_ordinal >= 0),
  subject_ref TEXT NOT NULL,
  PRIMARY KEY (activity_id, subject_ordinal),
  UNIQUE (activity_id, subject_ref)
);

CREATE TABLE IF NOT EXISTS activity_adjudication_findings (
  activity_id TEXT NOT NULL REFERENCES activity_review_assignments(activity_id) ON DELETE RESTRICT,
  finding_ordinal INTEGER NOT NULL CHECK (finding_ordinal >= 0),
  finding_id TEXT NOT NULL,
  PRIMARY KEY (activity_id, finding_ordinal),
  UNIQUE (activity_id, finding_id)
);

CREATE TABLE IF NOT EXISTS attempts (
  attempt_id TEXT PRIMARY KEY,
  activity_id TEXT NOT NULL REFERENCES activities(activity_id) ON DELETE RESTRICT,
  generation INTEGER NOT NULL CHECK (generation > 0),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("attempt.state"))})),
  protocol_version TEXT NOT NULL,
  execution_profile_id TEXT,
  worker_profile TEXT NOT NULL,
  provider TEXT,
  model TEXT,
  provider_account_ref TEXT,
  provider_family TEXT,
  model_family TEXT,
  classification_revision TEXT,
  provider_secret_ref TEXT,
  offered_at_ms INTEGER NOT NULL CHECK (offered_at_ms >= 0),
  claim_timeout_ms INTEGER NOT NULL CHECK (claim_timeout_ms > 0),
  claim_deadline_ms INTEGER NOT NULL CHECK (claim_deadline_ms > offered_at_ms),
  claimed_worker_id TEXT,
  claimed_worker_session_id TEXT,
  claimed_at_ms INTEGER,
  execution_deadline_ms INTEGER,
  capability_auth_expires_at_ms INTEGER,
  last_liveness_observed_ms INTEGER,
  attempt_capability_jti TEXT,
  attempt_capability_digest TEXT,
  attempt_capability_signing_key_id TEXT,
  attempt_capability_signature_algorithm TEXT,
  attempt_claim_id TEXT,
  launch_nonce_id TEXT,
  launch_capability_digest TEXT,
  launch_attestation_id TEXT,
  launch_capability_consumed_at_ms INTEGER,
  terminal_reason TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (activity_id, generation),
  UNIQUE (attempt_id, activity_id, generation),
  CHECK (
    state != 'OFFERED'
    OR (claimed_worker_id IS NULL AND claimed_worker_session_id IS NULL
      AND claimed_at_ms IS NULL AND execution_deadline_ms IS NULL
      AND capability_auth_expires_at_ms IS NULL
      AND attempt_capability_jti IS NULL AND attempt_capability_digest IS NULL
      AND attempt_capability_signing_key_id IS NULL
      AND attempt_capability_signature_algorithm IS NULL
      AND attempt_claim_id IS NULL AND provider_secret_ref IS NULL
      AND terminal_reason IS NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_attempts_nonterminal_activity
ON attempts(activity_id) WHERE state IN ('OFFERED', 'CLAIMED');

CREATE TABLE IF NOT EXISTS attempt_claims (
  attempt_claim_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(attempt_id) ON DELETE RESTRICT,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  offer_outbox_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  worker_session_id TEXT NOT NULL,
  worker_profile TEXT NOT NULL,
  worker_build_revision TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  claimed_at_ms INTEGER NOT NULL CHECK (claimed_at_ms >= 0),
  execution_deadline_ms INTEGER NOT NULL CHECK (execution_deadline_ms > claimed_at_ms),
  capability_auth_expires_at_ms INTEGER NOT NULL
    CHECK (capability_auth_expires_at_ms > execution_deadline_ms),
  attempt_capability_jti TEXT NOT NULL UNIQUE,
  attempt_capability_digest TEXT NOT NULL,
  attempt_capability_signing_key_id TEXT NOT NULL,
  attempt_capability_signature_algorithm TEXT NOT NULL,
  capability_key_registry_revision INTEGER NOT NULL CHECK (capability_key_registry_revision >= 0),
  launch_nonce_id TEXT,
  launch_capability_jti TEXT UNIQUE,
  launch_capability_digest TEXT,
  launch_capability_signing_key_id TEXT,
  launch_capability_signature_algorithm TEXT,
  source_access_kind TEXT NOT NULL CHECK (
    source_access_kind IN ({_sql_in(_enum_values("attempt_claim.source_access_kind"))})
  ),
  source_read_secret_ref TEXT,
  provider_secret_ref TEXT,
  source_access_descriptor_json TEXT NOT NULL,
  source_access_descriptor_digest TEXT NOT NULL,
  response_contract_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (worker_session_id, attempt_claim_id),
  FOREIGN KEY (attempt_id, activity_id, attempt_generation)
    REFERENCES attempts(attempt_id, activity_id, generation)
);

CREATE TABLE IF NOT EXISTS launch_attestations (
  launch_attestation_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(attempt_id) ON DELETE RESTRICT,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  attempt_claim_id TEXT NOT NULL REFERENCES attempt_claims(attempt_claim_id) ON DELETE RESTRICT,
  worker_id TEXT NOT NULL,
  worker_session_id TEXT NOT NULL,
  pool_manager_id TEXT NOT NULL,
  runner_principal_id TEXT NOT NULL,
  runner_image_digest TEXT NOT NULL,
  runner_registration_revision INTEGER NOT NULL CHECK (runner_registration_revision >= 0),
  launch_nonce_id TEXT NOT NULL UNIQUE,
  launch_capability_digest TEXT NOT NULL,
  launch_capability_signing_key_id TEXT NOT NULL,
  launch_capability_signature_algorithm TEXT NOT NULL,
  workspace_instance_id TEXT NOT NULL UNIQUE,
  context_instance_id TEXT NOT NULL UNIQUE,
  invocation_instance_id TEXT NOT NULL UNIQUE,
  workspace_parent_id TEXT,
  context_parent_id TEXT,
  invocation_parent_id TEXT,
  fresh_workspace INTEGER NOT NULL CHECK (fresh_workspace = 1),
  fresh_context INTEGER NOT NULL CHECK (fresh_context = 1),
  fresh_invocation INTEGER NOT NULL CHECK (fresh_invocation = 1),
  prepared_at_ms INTEGER NOT NULL CHECK (prepared_at_ms >= 0),
  attested_at_ms INTEGER NOT NULL CHECK (attested_at_ms >= prepared_at_ms),
  runner_signing_key_id TEXT NOT NULL,
  runner_signature_algorithm TEXT NOT NULL,
  signature TEXT NOT NULL,
  attestation_digest TEXT NOT NULL,
  provider_secret_ref TEXT,
  provider_material_descriptor_json TEXT,
  provider_material_descriptor_digest TEXT,
  response_contract_digest TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (launch_attestation_id, attestation_digest),
  UNIQUE (worker_session_id, launch_attestation_id),
  CHECK (
    workspace_parent_id IS NULL
    AND context_parent_id IS NULL
    AND invocation_parent_id IS NULL
  ),
  CHECK (
    (provider_material_descriptor_json IS NULL AND provider_material_descriptor_digest IS NULL)
    OR (
      provider_material_descriptor_json IS NOT NULL
      AND provider_material_descriptor_digest IS NOT NULL
    )
  ),
  FOREIGN KEY (attempt_id, activity_id, attempt_generation)
    REFERENCES attempts(attempt_id, activity_id, generation)
);

CREATE TABLE IF NOT EXISTS artifact_objects (
  bundle_digest TEXT PRIMARY KEY,
  storage_key TEXT NOT NULL UNIQUE,
  byte_length INTEGER NOT NULL CHECK (byte_length > 0),
  installed_at_ms INTEGER NOT NULL CHECK (installed_at_ms >= 0)
);

CREATE TABLE IF NOT EXISTS candidate_uploads (
  upload_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE RESTRICT,
  activity_id TEXT NOT NULL REFERENCES activities(activity_id) ON DELETE RESTRICT,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  idempotency_key TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  media_type TEXT NOT NULL CHECK (media_type = 'application/x-git-bundle'),
  declared_bytes INTEGER NOT NULL CHECK (declared_bytes > 0),
  expected_bundle_digest TEXT NOT NULL,
  expected_base_commit_json TEXT NOT NULL,
  expected_repository_external_id TEXT NOT NULL,
  expected_snapshot_id TEXT,
  incoming_path TEXT,
  computed_bundle_digest TEXT,
  computed_bytes INTEGER CHECK (computed_bytes IS NULL OR computed_bytes > 0),
  verified_tip_json TEXT,
  artifact_bundle_digest TEXT REFERENCES artifact_objects(bundle_digest) ON DELETE RESTRICT,
  artifact_storage_key TEXT,
  promoted_at_ms INTEGER CHECK (promoted_at_ms IS NULL OR promoted_at_ms >= 0),
  consumed_candidate_id TEXT,
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("candidate_upload.state"))})),
  expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
  UNIQUE (attempt_id, idempotency_key),
  FOREIGN KEY (attempt_id, activity_id, attempt_generation)
    REFERENCES attempts(attempt_id, activity_id, generation),
  CHECK (
    (state = 'RECEIVING'
      AND incoming_path IS NULL AND computed_bundle_digest IS NULL AND computed_bytes IS NULL
      AND verified_tip_json IS NULL AND artifact_bundle_digest IS NULL
      AND artifact_storage_key IS NULL AND promoted_at_ms IS NULL
      AND consumed_candidate_id IS NULL)
    OR (state = 'VALIDATED'
      AND incoming_path IS NOT NULL AND computed_bundle_digest IS NOT NULL
      AND computed_bytes IS NOT NULL AND verified_tip_json IS NOT NULL
      AND artifact_bundle_digest IS NULL AND artifact_storage_key IS NULL
      AND promoted_at_ms IS NULL AND consumed_candidate_id IS NULL)
    OR (state = 'PROMOTED'
      AND incoming_path IS NOT NULL AND computed_bundle_digest IS NOT NULL
      AND computed_bytes IS NOT NULL AND verified_tip_json IS NOT NULL
      AND artifact_bundle_digest IS NOT NULL AND artifact_storage_key IS NOT NULL
      AND promoted_at_ms IS NOT NULL AND consumed_candidate_id IS NULL)
    OR (state = 'CONSUMED'
      AND incoming_path IS NOT NULL AND computed_bundle_digest IS NOT NULL
      AND computed_bytes IS NOT NULL AND verified_tip_json IS NOT NULL
      AND artifact_bundle_digest IS NOT NULL AND artifact_storage_key IS NOT NULL
      AND promoted_at_ms IS NOT NULL AND consumed_candidate_id IS NOT NULL)
    OR state = 'EXPIRED'
  )
);

CREATE TABLE IF NOT EXISTS candidates (
  candidate_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  candidate_generation INTEGER NOT NULL CHECK (candidate_generation > 0),
  provenance_kind TEXT NOT NULL CHECK (
    provenance_kind IN ({_sql_in(_enum_values("candidate.provenance_kind"))})
  ),
  producing_activity_id TEXT NOT NULL UNIQUE REFERENCES activities(activity_id) ON DELETE RESTRICT,
  worker_attempt_id TEXT,
  worker_attempt_generation INTEGER CHECK (
    worker_attempt_generation IS NULL OR worker_attempt_generation > 0
  ),
  import_forge_observation_id TEXT,
  object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
  oid TEXT NOT NULL,
  base_commit_json TEXT NOT NULL,
  bundle_digest TEXT NOT NULL REFERENCES artifact_objects(bundle_digest) ON DELETE RESTRICT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (run_id, candidate_generation),
  UNIQUE (run_id, object_format, oid),
  CHECK (
    (provenance_kind = 'WORKER_ATTEMPT'
      AND worker_attempt_id IS NOT NULL AND worker_attempt_generation IS NOT NULL
      AND import_forge_observation_id IS NULL)
    OR
    (provenance_kind = 'FORGE_IMPORT'
      AND worker_attempt_id IS NULL AND worker_attempt_generation IS NULL
      AND import_forge_observation_id IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS controller_operation_facts (
  controller_operation_fact_id TEXT PRIMARY KEY,
  activity_id TEXT NOT NULL UNIQUE REFERENCES activities(activity_id) ON DELETE RESTRICT,
  operation_kind TEXT NOT NULL CHECK (
    operation_kind IN ({_sql_in(_enum_values("controller_operation_fact.kind"))})
  ),
  outcome TEXT NOT NULL CHECK (
    outcome IN ({_sql_in(_enum_values("controller_operation_fact.outcome"))})
  ),
  failure_category TEXT CHECK (
    failure_category IN ({_sql_in(_enum_values("controller_operation_fact.failure_category"))})
  ),
  candidate_id TEXT REFERENCES candidates(candidate_id) ON DELETE RESTRICT,
  forge_observation_id TEXT,
  operation_digest TEXT NOT NULL,
  fact_digest TEXT NOT NULL UNIQUE,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  CHECK (
    (outcome = 'SUCCEEDED' AND failure_category IS NULL)
    OR (outcome = 'FAILED' AND failure_category IS NOT NULL AND candidate_id IS NULL)
  )
);
"""

_V8_TO_V9 = """
CREATE TABLE IF NOT EXISTS launch_attestations (
  launch_attestation_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(attempt_id) ON DELETE RESTRICT,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  attempt_claim_id TEXT NOT NULL REFERENCES attempt_claims(attempt_claim_id) ON DELETE RESTRICT,
  worker_id TEXT NOT NULL,
  worker_session_id TEXT NOT NULL,
  pool_manager_id TEXT NOT NULL,
  runner_principal_id TEXT NOT NULL,
  runner_image_digest TEXT NOT NULL,
  runner_registration_revision INTEGER NOT NULL CHECK (runner_registration_revision >= 0),
  launch_nonce_id TEXT NOT NULL UNIQUE,
  launch_capability_digest TEXT NOT NULL,
  launch_capability_signing_key_id TEXT NOT NULL,
  launch_capability_signature_algorithm TEXT NOT NULL,
  workspace_instance_id TEXT NOT NULL UNIQUE,
  context_instance_id TEXT NOT NULL UNIQUE,
  invocation_instance_id TEXT NOT NULL UNIQUE,
  workspace_parent_id TEXT,
  context_parent_id TEXT,
  invocation_parent_id TEXT,
  fresh_workspace INTEGER NOT NULL CHECK (fresh_workspace = 1),
  fresh_context INTEGER NOT NULL CHECK (fresh_context = 1),
  fresh_invocation INTEGER NOT NULL CHECK (fresh_invocation = 1),
  prepared_at_ms INTEGER NOT NULL CHECK (prepared_at_ms >= 0),
  attested_at_ms INTEGER NOT NULL CHECK (attested_at_ms >= prepared_at_ms),
  runner_signing_key_id TEXT NOT NULL,
  runner_signature_algorithm TEXT NOT NULL,
  signature TEXT NOT NULL,
  attestation_digest TEXT NOT NULL,
  provider_secret_ref TEXT,
  provider_material_descriptor_json TEXT,
  provider_material_descriptor_digest TEXT,
  response_contract_digest TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (launch_attestation_id, attestation_digest),
  UNIQUE (worker_session_id, launch_attestation_id),
  CHECK (
    workspace_parent_id IS NULL
    AND context_parent_id IS NULL
    AND invocation_parent_id IS NULL
  ),
  CHECK (
    (provider_material_descriptor_json IS NULL AND provider_material_descriptor_digest IS NULL)
    OR (
      provider_material_descriptor_json IS NOT NULL
      AND provider_material_descriptor_digest IS NOT NULL
    )
  ),
  FOREIGN KEY (attempt_id, activity_id, attempt_generation)
    REFERENCES attempts(attempt_id, activity_id, generation)
);
"""

_V1_TO_V2 = f"""
PRAGMA foreign_keys=OFF;
CREATE TABLE runs_v2 (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  work_item_key TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("run.state"))})),
  current_snapshot_id TEXT,
  pending_snapshot_id TEXT,
  supersede_requested INTEGER NOT NULL DEFAULT 0 CHECK (supersede_requested IN (0, 1)),
  supersede_requested_transition_sequence INTEGER CHECK (
    supersede_requested_transition_sequence IS NULL
    OR supersede_requested_transition_sequence > 0
  ),
  terminal_outcome TEXT CHECK (
    terminal_outcome IN ({_sql_in(_enum_values("run.terminal_outcome"))})
  ),
  reducer_version TEXT NOT NULL,
  current_revision INTEGER NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms)
);
INSERT INTO runs_v2 (
  run_id, project_id, work_item_key, specification_generation, state, terminal_outcome,
  reducer_version, current_revision, created_at_ms, updated_at_ms
)
SELECT
  run_id, project_id, work_item_key, specification_generation, state, terminal_outcome,
  reducer_version, current_revision, created_at_ms, updated_at_ms
FROM runs;
DROP TABLE runs;
ALTER TABLE runs_v2 RENAME TO runs;
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active_work_item
ON runs(project_id, work_item_key) WHERE terminal_outcome IS NULL;
CREATE TABLE transitions_v2 (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  transition_sequence INTEGER NOT NULL CHECK (transition_sequence > 0),
  transition_id TEXT NOT NULL UNIQUE,
  prior_state TEXT NOT NULL CHECK (
    prior_state = '{PRIOR_STATE_NONE}'
    OR prior_state IN ({_sql_in(_enum_values("run.state"))})
  ),
  trigger_kind TEXT NOT NULL CHECK (
    trigger_kind IN ({_sql_in(_enum_values("transition.trigger_kind"))})
  ),
  trigger_id TEXT NOT NULL,
  admit_base_observation_id TEXT,
  next_state TEXT NOT NULL CHECK (next_state IN ({_sql_in(_enum_values("run.state"))})),
  reducer_version TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (run_id, transition_sequence),
  UNIQUE (run_id, trigger_kind, trigger_id)
);
INSERT INTO transitions_v2 SELECT * FROM transitions;
DROP TABLE transitions;
ALTER TABLE transitions_v2 RENAME TO transitions;
PRAGMA foreign_keys=ON;
"""

_V2_TO_V3 = f"""
PRAGMA foreign_keys=OFF;
CREATE TABLE controller_mode_operations_v3 (
  controller_mode_operation_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  operation_kind TEXT NOT NULL CHECK (
    operation_kind IN ({_sql_in(_enum_values("controller_mode_operation.operation_kind"))})
  ),
  expected_mode_revision INTEGER NOT NULL CHECK (expected_mode_revision >= 0),
  expected_mode TEXT CHECK (expected_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  requested_mode TEXT CHECK (
    requested_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  requested_dispatch_paused_intake_policy TEXT
    CHECK (requested_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  backup_manifest_digest TEXT,
  backup_prior_mode TEXT CHECK (
    backup_prior_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  backup_prior_dispatch_paused_intake_policy TEXT
    CHECK (backup_prior_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ({_sql_in(_enum_values("controller_mode_operation.status"))})
  ),
  rejection_code TEXT CHECK (
    rejection_code IN ({_sql_in(_enum_values("controller_mode_operation.rejection_code"))})
  ),
  result_mode_revision INTEGER CHECK (result_mode_revision > 0),
  result_mode TEXT CHECK (result_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  result_dispatch_paused_intake_policy TEXT
    CHECK (result_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  completed_at_ms INTEGER NOT NULL CHECK (completed_at_ms >= 0),
  CHECK ((status = 'SUCCEEDED' AND rejection_code IS NULL)
    OR (status = 'REJECTED' AND rejection_code IS NOT NULL))
);
INSERT INTO controller_mode_operations_v3 (
  controller_mode_operation_id, protocol_version, operation_kind,
  expected_mode_revision, expected_mode, requested_mode,
  requested_dispatch_paused_intake_policy, authenticated_principal_id,
  authorization_context_digest, request_digest, status, rejection_code,
  result_mode_revision, result_mode, result_dispatch_paused_intake_policy,
  response_http_status, response_json, response_digest, completed_at_ms
)
SELECT
  controller_mode_operation_id, protocol_version, operation_kind,
  expected_mode_revision, expected_mode, requested_mode,
  requested_dispatch_paused_intake_policy, authenticated_principal_id,
  authorization_context_digest, request_digest, status, rejection_code,
  result_mode_revision, result_mode, result_dispatch_paused_intake_policy,
  response_http_status, response_json, response_digest, completed_at_ms
FROM controller_mode_operations;
CREATE TABLE controller_mode_v3 (
  controller_id TEXT PRIMARY KEY CHECK (controller_id = '{CONTROLLER_ID}'),
  mode_revision INTEGER NOT NULL CHECK (mode_revision >= 0),
  mode TEXT CHECK (mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  dispatch_paused_intake_policy TEXT
    CHECK (dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  maintenance_prior_mode TEXT CHECK (
    maintenance_prior_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  maintenance_prior_dispatch_paused_intake_policy TEXT
    CHECK (maintenance_prior_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  last_operation_id TEXT,
  FOREIGN KEY (last_operation_id)
    REFERENCES controller_mode_operations(controller_mode_operation_id) ON DELETE RESTRICT,
  CHECK ((mode_revision = 0 AND mode IS NULL) OR (mode_revision > 0 AND mode IS NOT NULL)),
  CHECK (
    (mode = 'DISPATCH_PAUSED' AND dispatch_paused_intake_policy IS NOT NULL)
    OR (mode IS NULL AND dispatch_paused_intake_policy IS NULL)
    OR (mode != 'DISPATCH_PAUSED' AND dispatch_paused_intake_policy IS NULL)
  ),
  CHECK (
    (maintenance_prior_dispatch_paused_intake_policy IS NOT NULL)
    = (maintenance_prior_mode = 'DISPATCH_PAUSED')
  )
);
INSERT INTO controller_mode_v3 (
  controller_id, mode_revision, mode, dispatch_paused_intake_policy,
  maintenance_prior_mode, maintenance_prior_dispatch_paused_intake_policy,
  last_operation_id
)
SELECT
  controller_id, mode_revision, mode, dispatch_paused_intake_policy,
  maintenance_prior_mode, maintenance_prior_dispatch_paused_intake_policy,
  last_operation_id
FROM controller_mode;
DROP TABLE controller_mode;
DROP TABLE controller_mode_operations;
ALTER TABLE controller_mode_operations_v3 RENAME TO controller_mode_operations;
ALTER TABLE controller_mode_v3 RENAME TO controller_mode;
PRAGMA foreign_keys=ON;
"""

_V5_TO_V6 = f"""
PRAGMA foreign_keys=OFF;
{_forge_observation_schedules_ddl("forge_observation_schedules_v6")}
INSERT INTO forge_observation_schedules_v6 (
  forge_observation_schedule_id, schedule_kind, project_id, forge_instance_id, target_kind,
  target_id, run_id, publication_id, terminal_duplicate_cleanup_reservation_id,
  minimum_interval_ms, next_due_at_ms, schedule_revision, last_request_id,
  last_discovery_search_revision, last_discovery_set_digest, state, schedule_digest,
  created_at_ms
)
SELECT
  s.forge_observation_schedule_id, s.schedule_kind, s.target_id,
  (SELECT p.forge_instance_id FROM projects p WHERE p.project_id = s.target_id),
  s.target_kind, s.target_id, s.run_id, s.publication_id, NULL,
  {_DEFAULT_DISCOVERY_INTERVAL_MS}, s.next_due_at_ms, s.schedule_revision, s.last_request_id,
  NULL, NULL, s.state, '', s.created_at_ms
FROM forge_observation_schedules s;
DROP TABLE forge_observation_schedules;
ALTER TABLE forge_observation_schedules_v6 RENAME TO forge_observation_schedules;
PRAGMA foreign_keys=ON;
"""

_V6_TO_V7 = """
ALTER TABLE runs ADD COLUMN current_snapshot_id TEXT;
ALTER TABLE runs ADD COLUMN pending_snapshot_id TEXT;
ALTER TABLE runs ADD COLUMN supersede_requested INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN supersede_requested_transition_sequence INTEGER;
"""

# Appended after whichever script actually put forge_observation_schedules into
# its final shape (a plain CREATE TABLE for a fresh/pre-v5 database, or the
# _V5_TO_V6 rename-dance for a real v5 one) so these two CREATE INDEX
# statements only ever run once the table already has ``identity_key``.
_FORGE_OBSERVATION_SCHEDULE_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_forge_observation_schedules_identity
ON forge_observation_schedules(identity_key) WHERE state != 'CLOSED';
CREATE INDEX IF NOT EXISTS idx_forge_observation_schedules_due
ON forge_observation_schedules(state, next_due_at_ms) WHERE state = 'ACTIVE';
"""


class RunStore:
    """Controller-owned SQLite store with an exclusive process writer lock."""

    def __init__(
        self,
        state_root: Path | str,
        *,
        reducer_versions: Iterable[str] = SUPPORTED_REDUCER_VERSIONS,
        min_free_bytes: int = 1,
        verify_local_filesystem: bool = True,
        fail_closed: bool = True,
    ) -> None:
        self.state_root = Path(state_root)
        self.db_path = self.state_root / "workflow.db"
        self.controller_lock_path = self.state_root / "controller.lock"
        self.storage_lock_path = self.state_root / "storage.lock"
        self._supported_reducer_versions = frozenset(reducer_versions)
        self._fail_closed = fail_closed
        self.maintenance_mode: MaintenanceMode | None = None
        self._lock_fd: int | None = None
        self._conn: sqlite3.Connection | None = None

        if verify_local_filesystem:
            _verify_local_state_root(self.state_root, min_free_bytes=min_free_bytes)
        else:
            self.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)

        self._acquire_writer_lock()
        try:
            self._conn = self._open_connection()
            self._migrate()
            self._startup_checks()
        except Exception:
            self.close()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    @classmethod
    def open_maintenance(
        cls,
        state_root: Path | str,
        *,
        reducer_versions: Iterable[str] = SUPPORTED_REDUCER_VERSIONS,
        min_free_bytes: int = 1,
        verify_local_filesystem: bool = True,
    ) -> "RunStore":
        return cls(
            state_root,
            reducer_versions=reducer_versions,
            min_free_bytes=min_free_bytes,
            verify_local_filesystem=verify_local_filesystem,
            fail_closed=False,
        )

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RunStoreError("run store is closed")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    def _acquire_writer_lock(self) -> None:
        self.controller_lock_path.touch(mode=0o600, exist_ok=True)
        self.controller_lock_path.chmod(0o600)
        _verify_lock_file(self.controller_lock_path)
        self.storage_lock_path.touch(mode=0o600, exist_ok=True)
        self.storage_lock_path.chmod(0o600)
        _verify_lock_file(self.storage_lock_path)
        fd = os.open(self.controller_lock_path, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise WriterLockError("another workflow controller owns the writer lock") from exc
        self._lock_fd = fd

    def _open_connection(self) -> sqlite3.Connection:
        self.db_path.touch(mode=0o600, exist_ok=True)
        self.db_path.chmod(0o600)
        _verify_lock_file(self.db_path)
        conn = sqlite3.connect(self.db_path, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        pragmas = {
            "journal_mode": "WAL",
            "synchronous": "FULL",
            "foreign_keys": "ON",
            "busy_timeout": "5000",
            "trusted_schema": "OFF",
            "wal_autocheckpoint": "1000",
        }
        for name, value in pragmas.items():
            conn.execute(f"PRAGMA {name}={value}")
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            raise StartupIntegrityError("SQLite journal_mode=WAL could not be enabled")
        if int(synchronous) != 2:
            raise StartupIntegrityError("SQLite synchronous=FULL could not be enabled")
        if int(foreign_keys) != 1:
            raise StartupIntegrityError("SQLite foreign_keys=ON could not be enabled")
        return conn

    def _migrate(self) -> None:
        current = int(self.conn.execute("PRAGMA user_version").fetchone()[0])
        if current > SCHEMA_VERSION:
            if not self._fail_closed:
                return
            raise SchemaVersionError(
                f"workflow.db schema version {current} is newer than supported {SCHEMA_VERSION}"
            )
        if current == SCHEMA_VERSION:
            return
        if current not in {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}:
            raise SchemaVersionError(
                f"unsupported workflow.db schema version {current}; "
                f"supported version is {SCHEMA_VERSION}"
            )
        # Connection.executescript() issues COMMIT first whenever a transaction
        # is already open (sqlite3 documented behavior, independent of
        # isolation_level). BEGIN EXCLUSIVE therefore has to live inside the
        # script so DDL, seed rows, and the user_version bump share one txn.
        #
        # PRAGMA foreign_keys is a no-op once a transaction is open (SQLite
        # only honors it in autocommit mode), so the table-rebuild scripts'
        # own "PRAGMA foreign_keys=OFF;" lines can't actually suspend
        # enforcement for the BEGIN EXCLUSIVE they run inside. Toggle it here,
        # before that transaction starts, so DROP TABLE on a table still
        # carrying real rows referenced by another table's FK doesn't fail.
        self.conn.execute("PRAGMA foreign_keys=OFF")
        run_columns = {
            str(row["name"]) for row in self.conn.execute("PRAGMA table_info(runs)").fetchall()
        }
        v6_to_v7 = "" if "current_snapshot_id" in run_columns else _V6_TO_V7
        try:
            if current == 0:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n" + _SCHEMA + "\n" + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (SCHEMA_VERSION, "workflow-control-v1-base-store", _now_ms()),
                )
            elif current == 1:
                # _SCHEMA is idempotent (CREATE TABLE IF NOT EXISTS): a real
                # version-1 database already has controller_mode /
                # controller_mode_operations, so those two tables stay in their
                # version-1 shape here. Missing tables (capability-key) are
                # created; _V1_TO_V2 rebuilds runs/transitions; _V2_TO_V3 then
                # rebuilds controller_mode_operations (three new columns) and
                # controller_mode (bidirectional maintenance_prior_* CHECK) so
                # a v1-to-v3 upgrade lands in the same final shape as v2-to-v3.
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + _V1_TO_V2
                    + "\n"
                    + _V2_TO_V3
                    + "\n"
                    + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (SCHEMA_VERSION, "workflow-control-v1-reducer-ledger", _now_ms()),
                )
            elif current == 2:
                # _SCHEMA is idempotent (CREATE TABLE IF NOT EXISTS): a real version-2
                # database already has controller_mode/controller_mode_operations, so
                # only the capability-key tables get created here; _V2_TO_V3 then
                # rebuilds controller_mode_operations (three new columns) and
                # controller_mode (bidirectional maintenance_prior_* CHECK) in place.
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + _V2_TO_V3
                    + "\n"
                    + v6_to_v7
                    + "\n"
                    + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-controller-mode-and-key-gates",
                        _now_ms(),
                    ),
                )
            elif current == 3:
                # A real version-3 database already has every table in its
                # final v3 shape; _SCHEMA creates secret-provision tables and
                # the later project-registration tables in one step.
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v6_to_v7
                    + "\n"
                    + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-secret-provision-and-adoption",
                        _now_ms(),
                    ),
                )
            elif current == 4:
                # A real version-4 database already has secret-provision
                # tables; _SCHEMA only needs to create Project / Forge
                # Instance / registration-operation / discovery-schedule
                # tables (all CREATE TABLE IF NOT EXISTS), and it creates
                # forge_observation_schedules directly in its final v6 shape
                # since a v4 database never had that table at all.
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v6_to_v7
                    + "\n"
                    + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-project-registration",
                        _now_ms(),
                    ),
                )
            elif current == 5:
                # A real version-5 database already has forge_observation_schedules
                # in its pre-v6 shape (no project_id/forge_instance_id/cadence/
                # digest columns); _V5_TO_V6 rebuilds it into the final shape,
                # backfilling project_id/forge_instance_id from the existing
                # WORK_ITEM_DISCOVERY rows' Project-scoped target_id, and
                # _SCHEMA creates the new Forge Observation Request/Result/
                # Failure-Fact/Observation tables (all CREATE TABLE IF NOT
                # EXISTS). schedule_digest can't be computed inside raw SQL, so
                # every migrated row is stamped with the real digest below
                # before the transaction commits.
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + _V5_TO_V6
                    + "\n"
                    + v6_to_v7
                    + "\n"
                    + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                for row in self.conn.execute(
                    "SELECT * FROM forge_observation_schedules WHERE schedule_digest = ''"
                ).fetchall():
                    digest = forge_observation_schedule_digest(
                        _forge_observation_schedule_digest_fields(
                            schedule_kind=row["schedule_kind"],
                            project_id=row["project_id"],
                            forge_instance_id=row["forge_instance_id"],
                            target_kind=row["target_kind"],
                            target_id=row["target_id"],
                            run_id=row["run_id"],
                            publication_id=row["publication_id"],
                            terminal_duplicate_cleanup_reservation_id=row[
                                "terminal_duplicate_cleanup_reservation_id"
                            ],
                            minimum_interval_ms=row["minimum_interval_ms"],
                        )
                    )
                    self.conn.execute(
                        "UPDATE forge_observation_schedules SET schedule_digest = ? "
                        "WHERE forge_observation_schedule_id = ?",
                        (digest, row["forge_observation_schedule_id"]),
                    )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-forge-observations",
                        _now_ms(),
                    ),
                )
            elif current == 6:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v6_to_v7
                    + "\n"
                    + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-snapshot-admission",
                        _now_ms(),
                    ),
                )
            elif current == 7:
                # A real version-7 database already has every table in its
                # final v7 shape; _SCHEMA only needs to create the new v8
                # activities/attempts/attempt_claims tables and v9 launch
                # attestation table (all CREATE TABLE IF NOT EXISTS).
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n" + _SCHEMA + "\n" + _FORGE_OBSERVATION_SCHEDULE_INDEXES
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-durable-activity-attempts",
                        _now_ms(),
                    ),
                )
            elif current == 8:
                self.conn.executescript("BEGIN EXCLUSIVE;\n" + _V8_TO_V9 + "\n" + _SCHEMA)
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-launch-attestations",
                        _now_ms(),
                    ),
                )
            else:
                assert current == 9
                self.conn.executescript("BEGIN EXCLUSIVE;\n" + _SCHEMA)
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-candidate-transfer",
                        _now_ms(),
                    ),
                )
            self.conn.execute(
                "INSERT OR IGNORE INTO controller_mode"
                "(controller_id, mode_revision, mode, dispatch_paused_intake_policy, "
                "maintenance_prior_mode, maintenance_prior_dispatch_paused_intake_policy, "
                "last_operation_id) VALUES (?, 0, NULL, NULL, NULL, NULL, NULL)",
                (CONTROLLER_ID,),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO capability_key_registry"
                "(registry_id, registry_revision, current_issuance_key_id, last_operation_id) "
                "VALUES (?, 0, NULL, NULL)",
                (CONTROLLER_ID,),
            )
            self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.execute("PRAGMA foreign_keys=ON")

    def _startup_checks(self) -> None:
        try:
            self._raise_unhealthy_startup()
        except StartupIntegrityError as exc:
            if self._fail_closed:
                raise
            self.maintenance_mode = MaintenanceMode(reason=str(exc))
            try:
                self.conn.execute(
                    "UPDATE controller_mode SET mode = 'MAINTENANCE' "
                    "WHERE controller_id = ? AND mode_revision > 0",
                    (CONTROLLER_ID,),
                )
            except sqlite3.Error:
                pass

    def _raise_unhealthy_startup(self) -> None:
        user_version = int(self.conn.execute("PRAGMA user_version").fetchone()[0])
        if user_version != SCHEMA_VERSION:
            raise SchemaVersionError(
                f"unsupported workflow.db schema version {user_version}; "
                f"supported version is {SCHEMA_VERSION}"
            )
        quick_check = self.conn.execute("PRAGMA quick_check").fetchone()[0]
        if quick_check != "ok":
            raise StartupIntegrityError(f"SQLite quick_check failed: {quick_check}")
        fk_rows = self.conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_rows:
            raise StartupIntegrityError(f"SQLite foreign_key_check failed: {len(fk_rows)} row(s)")
        versions = {
            row[0]
            for row in self.conn.execute(
                "SELECT reducer_version FROM runs UNION SELECT reducer_version FROM transitions"
            )
        }
        unsupported = versions - self._supported_reducer_versions
        if unsupported:
            raise ReducerVersionError(
                "unsupported reducer version(s): " + ", ".join(sorted(unsupported))
            )

    @contextmanager
    def storage_mutation_lock(self) -> Iterator[None]:
        fd = os.open(self.storage_lock_path, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextmanager
    def transaction(
        self,
        *,
        fault: FaultInjectionPoint | None = None,
        before_response_ack: Callable[[], None] | None = None,
    ) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        committed = False
        try:
            yield self.conn
            if fault is FaultInjectionPoint.BEFORE_COMMIT:
                raise TransactionFault(FaultInjectionPoint.BEFORE_COMMIT.value)
            self.conn.commit()
            committed = True
            if fault is FaultInjectionPoint.AFTER_COMMIT:
                raise TransactionFault(FaultInjectionPoint.AFTER_COMMIT.value)
            if fault is FaultInjectionPoint.BEFORE_RESPONSE_ACK:
                raise TransactionFault(FaultInjectionPoint.BEFORE_RESPONSE_ACK.value)
            if before_response_ack is not None:
                before_response_ack()
        except Exception:
            if not committed:
                self.conn.rollback()
            raise

    def get_controller_mode(self) -> ControllerModeProjection:
        row = self.conn.execute(
            "SELECT * FROM controller_mode WHERE controller_id = ?", (CONTROLLER_ID,)
        ).fetchone()
        assert row is not None
        return _row_to_controller_mode(row)

    def get_capability_key_registry(self) -> CapabilityKeyRegistryProjection:
        row = self.conn.execute(
            "SELECT * FROM capability_key_registry WHERE registry_id = ?", (CONTROLLER_ID,)
        ).fetchone()
        assert row is not None
        return _row_to_capability_registry(row)

    def get_capability_signing_key(self, key_id: str) -> CapabilitySigningKey | None:
        require_lowercase_uuid(key_id, field="capability_signing_key_id")
        row = self.conn.execute(
            "SELECT * FROM capability_signing_keys WHERE capability_signing_key_id = ?",
            (key_id,),
        ).fetchone()
        return None if row is None else _row_to_capability_key(row)

    def _controller_mode_response(
        self,
        *,
        operation_id: str,
        operation_kind: str,
        status: str,
        rejection_code: str | None = None,
        mode_revision: int | None = None,
        mode: str | None = None,
        dispatch_paused_intake_policy: str | None = None,
    ) -> tuple[int, str, str]:
        body: dict[str, object] = {
            "protocol_version": CONTROLLER_MODE_RESULT_PROTOCOL,
            "controller_mode_operation_id": operation_id,
            "operation_kind": operation_kind,
            "status": status,
            "replayed": False,
        }
        if status == "SUCCEEDED":
            body.update(
                {
                    "mode_revision": mode_revision,
                    "mode": mode,
                    "dispatch_paused_intake_policy": dispatch_paused_intake_policy,
                }
            )
            http_status = 200
        else:
            body["rejection_code"] = rejection_code
            http_status = 403 if rejection_code == "AUTHORITY_REVOKED" else 409
        body_json = canonical_json_text(body)
        digest = response_digest(
            {"http_status": http_status, "body": _response_digest_preimage(body)}
        )
        return http_status, body_json, digest

    def _capability_key_response(
        self,
        *,
        operation_id: str,
        kind: str,
        status: str,
        rejection_code: str | None = None,
        registry_revision: int | None = None,
        current_issuance_key_id: str | None = None,
    ) -> tuple[int, str, str]:
        body: dict[str, object] = {
            "protocol_version": CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL,
            "capability_key_operation_id": operation_id,
            "kind": kind,
            "status": status,
            "replayed": False,
        }
        if status == "SUCCEEDED":
            body.update(
                {
                    "registry_revision": registry_revision,
                    "current_issuance_key_id": current_issuance_key_id,
                }
            )
            http_status = 200
        else:
            body["rejection_code"] = rejection_code
            http_status = 403 if rejection_code == "AUTHORITY_REVOKED" else 409
        body_json = canonical_json_text(body)
        digest = response_digest(
            {"http_status": http_status, "body": _response_digest_preimage(body)}
        )
        return http_status, body_json, digest

    def _controller_mode_request_digest(
        self,
        *,
        operation_kind: str,
        expected_mode_revision: int,
        expected_mode: str | None,
        requested_mode: str | None,
        requested_dispatch_paused_intake_policy: str | None,
        backup_manifest_digest: str | None,
        backup_prior_mode: str | None,
        backup_prior_dispatch_paused_intake_policy: str | None,
        authenticated_principal_id: str,
        authorization_context_digest: str,
    ) -> str:
        return request_digest(
            {
                "protocol_version": CONTROLLER_MODE_OPERATION_PROTOCOL,
                "operation_kind": operation_kind,
                "expected_mode_revision": expected_mode_revision,
                "expected_mode": expected_mode,
                "requested_mode": requested_mode,
                "requested_dispatch_paused_intake_policy": requested_dispatch_paused_intake_policy,
                "backup_manifest_digest": backup_manifest_digest,
                "backup_prior_mode": backup_prior_mode,
                "backup_prior_dispatch_paused_intake_policy": (
                    backup_prior_dispatch_paused_intake_policy
                ),
                "authenticated_principal_id": authenticated_principal_id,
                "authorization_context_digest": authorization_context_digest,
            }
        )

    def apply_controller_mode_operation(
        self,
        *,
        controller_mode_operation_id: str,
        operation_kind: str,
        expected_mode_revision: int,
        expected_mode: str | None,
        requested_mode: str | None,
        requested_dispatch_paused_intake_policy: str | None = None,
        authenticated_principal_id: str,
        authorization_context_digest: str,
        authority_revoked: bool = False,
        backup_manifest_digest: str | None = None,
        backup_prior_mode: str | None = None,
        backup_prior_dispatch_paused_intake_policy: str | None = None,
    ) -> ControllerModeOperationResult:
        require_lowercase_uuid(controller_mode_operation_id, field="controller_mode_operation_id")
        enums.parse_enum("controller_mode_operation.operation_kind", operation_kind)
        if expected_mode is not None:
            enums.parse_enum("controller_mode.mode", expected_mode)
        if requested_mode is not None:
            enums.parse_enum("controller_mode.mode", requested_mode)
        if requested_dispatch_paused_intake_policy is not None:
            enums.parse_enum(
                "controller_mode.dispatch_paused_intake_policy",
                requested_dispatch_paused_intake_policy,
            )
        if backup_prior_mode is not None:
            enums.parse_enum("controller_mode.mode", backup_prior_mode)
        if backup_prior_dispatch_paused_intake_policy is not None:
            enums.parse_enum(
                "controller_mode.dispatch_paused_intake_policy",
                backup_prior_dispatch_paused_intake_policy,
            )
        _require_digest(authorization_context_digest, field="authorization_context_digest")
        if backup_manifest_digest is not None:
            _require_digest(backup_manifest_digest, field="backup_manifest_digest")
        req_digest = self._controller_mode_request_digest(
            operation_kind=operation_kind,
            expected_mode_revision=expected_mode_revision,
            expected_mode=expected_mode,
            requested_mode=requested_mode,
            requested_dispatch_paused_intake_policy=requested_dispatch_paused_intake_policy,
            backup_manifest_digest=backup_manifest_digest,
            backup_prior_mode=backup_prior_mode,
            backup_prior_dispatch_paused_intake_policy=(backup_prior_dispatch_paused_intake_policy),
            authenticated_principal_id=authenticated_principal_id,
            authorization_context_digest=authorization_context_digest,
        )
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM controller_mode_operations WHERE controller_mode_operation_id = ?",
                (controller_mode_operation_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["request_digest"] == req_digest
                ):
                    return _row_to_controller_mode_operation(existing, replayed=True)
                return self._transient_controller_mode_conflict(
                    controller_mode_operation_id, operation_kind
                )
            projection = self.get_controller_mode()
            rejection = self._validate_controller_mode_operation(
                projection=projection,
                operation_kind=operation_kind,
                expected_mode_revision=expected_mode_revision,
                expected_mode=expected_mode,
                requested_mode=requested_mode,
                requested_dispatch_paused_intake_policy=requested_dispatch_paused_intake_policy,
                authority_revoked=authority_revoked,
                backup_manifest_digest=backup_manifest_digest,
                backup_prior_mode=backup_prior_mode,
                backup_prior_dispatch_paused_intake_policy=(
                    backup_prior_dispatch_paused_intake_policy
                ),
            )
            result_revision = None
            result_mode = None
            result_policy = None
            prior_mode = None
            prior_policy = None
            if rejection is None:
                result_revision = projection.mode_revision + 1
                result_mode = requested_mode
                result_policy = requested_dispatch_paused_intake_policy
                if result_mode == "MAINTENANCE":
                    prior_mode = projection.maintenance_prior_mode
                    prior_policy = projection.maintenance_prior_dispatch_paused_intake_policy
                    if operation_kind == "SET_MODE":
                        prior_mode = projection.mode
                        prior_policy = projection.dispatch_paused_intake_policy
                    elif operation_kind == "RESTORE_BACKUP":
                        prior_mode = backup_prior_mode
                        prior_policy = backup_prior_dispatch_paused_intake_policy
                http_status, body_json, resp_digest = self._controller_mode_response(
                    operation_id=controller_mode_operation_id,
                    operation_kind=operation_kind,
                    status="SUCCEEDED",
                    mode_revision=result_revision,
                    mode=result_mode,
                    dispatch_paused_intake_policy=result_policy,
                )
                status = "SUCCEEDED"
            else:
                http_status, body_json, resp_digest = self._controller_mode_response(
                    operation_id=controller_mode_operation_id,
                    operation_kind=operation_kind,
                    status="REJECTED",
                    rejection_code=rejection,
                )
                status = "REJECTED"
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO controller_mode_operations("
                "controller_mode_operation_id, protocol_version, operation_kind, "
                "expected_mode_revision, expected_mode, requested_mode, "
                "requested_dispatch_paused_intake_policy, backup_manifest_digest, "
                "backup_prior_mode, backup_prior_dispatch_paused_intake_policy, "
                "authenticated_principal_id, authorization_context_digest, request_digest, "
                "status, rejection_code, result_mode_revision, result_mode, "
                "result_dispatch_paused_intake_policy, response_http_status, response_json, "
                "response_digest, completed_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    controller_mode_operation_id,
                    CONTROLLER_MODE_OPERATION_PROTOCOL,
                    operation_kind,
                    expected_mode_revision,
                    expected_mode,
                    requested_mode,
                    requested_dispatch_paused_intake_policy,
                    backup_manifest_digest,
                    backup_prior_mode,
                    backup_prior_dispatch_paused_intake_policy,
                    authenticated_principal_id,
                    authorization_context_digest,
                    req_digest,
                    status,
                    rejection,
                    result_revision,
                    result_mode,
                    result_policy,
                    http_status,
                    body_json,
                    resp_digest,
                    now,
                ),
            )
            if status == "SUCCEEDED":
                self.conn.execute(
                    "UPDATE controller_mode SET mode_revision = ?, mode = ?, "
                    "dispatch_paused_intake_policy = ?, maintenance_prior_mode = ?, "
                    "maintenance_prior_dispatch_paused_intake_policy = ?, "
                    "last_operation_id = ? WHERE controller_id = ? AND mode_revision = ?",
                    (
                        result_revision,
                        result_mode,
                        result_policy,
                        prior_mode,
                        prior_policy,
                        controller_mode_operation_id,
                        CONTROLLER_ID,
                        expected_mode_revision,
                    ),
                )
            row = self.conn.execute(
                "SELECT * FROM controller_mode_operations WHERE controller_mode_operation_id = ?",
                (controller_mode_operation_id,),
            ).fetchone()
            assert row is not None
            return _row_to_controller_mode_operation(row, replayed=False)

    def _transient_controller_mode_conflict(
        self, operation_id: str, operation_kind: str
    ) -> ControllerModeOperationResult:
        http_status, body_json, resp_digest = self._controller_mode_response(
            operation_id=operation_id,
            operation_kind=operation_kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
        )
        return ControllerModeOperationResult(
            controller_mode_operation_id=operation_id,
            operation_kind=operation_kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
            response_http_status=http_status,
            response_json=body_json,
            response_digest=resp_digest,
            completed_at_ms=_now_ms(),
        )

    def _validate_controller_mode_operation(
        self,
        *,
        projection: ControllerModeProjection,
        operation_kind: str,
        expected_mode_revision: int,
        expected_mode: str | None,
        requested_mode: str | None,
        requested_dispatch_paused_intake_policy: str | None,
        authority_revoked: bool,
        backup_manifest_digest: str | None,
        backup_prior_mode: str | None,
        backup_prior_dispatch_paused_intake_policy: str | None,
    ) -> str | None:
        if authority_revoked:
            return "AUTHORITY_REVOKED"
        if operation_kind == "INITIALIZE" and projection.mode_revision > 0:
            return "ALREADY_INITIALIZED"
        if projection.mode_revision != expected_mode_revision or projection.mode != expected_mode:
            return "CAS_LOST"
        requested_policy_ok = (
            requested_mode == "DISPATCH_PAUSED"
            and requested_dispatch_paused_intake_policy is not None
        ) or (
            requested_mode != "DISPATCH_PAUSED" and requested_dispatch_paused_intake_policy is None
        )
        if requested_mode is None or not requested_policy_ok:
            return "TRANSITION_NOT_ALLOWED"
        if operation_kind == "INITIALIZE":
            if requested_mode != "MAINTENANCE":
                return "TRANSITION_NOT_ALLOWED"
            return None
        if projection.mode_revision == 0 or projection.mode is None:
            return "NOT_INITIALIZED"
        if operation_kind == "SET_MODE":
            if backup_manifest_digest is not None:
                return "TRANSITION_NOT_ALLOWED"
            if (
                projection.mode == requested_mode
                and projection.dispatch_paused_intake_policy
                == requested_dispatch_paused_intake_policy
            ):
                return "NO_CHANGE"
            return None
        if operation_kind == "RESTORE_BACKUP":
            if backup_manifest_digest is None:
                return "TRANSITION_NOT_ALLOWED"
            if expected_mode == "MAINTENANCE":
                if requested_mode != "MAINTENANCE" or requested_dispatch_paused_intake_policy:
                    return "TRANSITION_NOT_ALLOWED"
                if (backup_prior_dispatch_paused_intake_policy is not None) != (
                    backup_prior_mode == "DISPATCH_PAUSED"
                ):
                    return "TRANSITION_NOT_ALLOWED"
                return None
            if (
                requested_mode == "DISPATCH_PAUSED"
                and requested_dispatch_paused_intake_policy == "PAUSE_ADMISSION"
                and backup_prior_mode is None
                and backup_prior_dispatch_paused_intake_policy is None
            ):
                return None
            return "TRANSITION_NOT_ALLOWED"
        raise AssertionError("unreachable operation kind")

    def _capability_key_request_digest(
        self,
        *,
        kind: str,
        expected_registry_revision: int,
        expected_issuance_key_id: str | None,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
        register_public_verification_key: bytes | None,
        register_public_key_digest: str | None,
        register_private_signing_secret_ref: str | None,
        register_not_before_ms: int | None,
        authenticated_principal_id: str,
        authorization_context_digest: str,
    ) -> str:
        public_key_hex = (
            None
            if register_public_verification_key is None
            else register_public_verification_key.hex()
        )
        return request_digest(
            {
                "protocol_version": CAPABILITY_KEY_OPERATION_PROTOCOL,
                "kind": kind,
                "expected_registry_revision": expected_registry_revision,
                "expected_issuance_key_id": expected_issuance_key_id,
                "target_capability_signing_key_id": target_capability_signing_key_id,
                "replacement_issuance_key_id": replacement_issuance_key_id,
                "register_public_verification_key": public_key_hex,
                "register_public_key_digest": register_public_key_digest,
                "register_private_signing_secret_ref": register_private_signing_secret_ref,
                "register_not_before_ms": register_not_before_ms,
                "authenticated_principal_id": authenticated_principal_id,
                "authorization_context_digest": authorization_context_digest,
            }
        )

    def apply_capability_key_operation(
        self,
        *,
        capability_key_operation_id: str,
        kind: str,
        expected_registry_revision: int,
        expected_issuance_key_id: str | None,
        target_capability_signing_key_id: str,
        authenticated_principal_id: str,
        authorization_context_digest: str,
        replacement_issuance_key_id: str | None = None,
        register_public_verification_key: bytes | None = None,
        register_public_key_digest: str | None = None,
        register_private_signing_secret_ref: str | None = None,
        register_not_before_ms: int | None = None,
        authority_revoked: bool = False,
        private_key_proof_valid: bool = False,
    ) -> CapabilityKeyOperationResult:
        require_lowercase_uuid(capability_key_operation_id, field="capability_key_operation_id")
        require_lowercase_uuid(
            target_capability_signing_key_id, field="target_capability_signing_key_id"
        )
        if expected_issuance_key_id is not None:
            require_lowercase_uuid(expected_issuance_key_id, field="expected_issuance_key_id")
        if replacement_issuance_key_id is not None:
            require_lowercase_uuid(replacement_issuance_key_id, field="replacement_issuance_key_id")
        enums.parse_enum("capability_key_operation.kind", kind)
        _require_digest(authorization_context_digest, field="authorization_context_digest")
        if register_public_key_digest is not None:
            _require_digest(register_public_key_digest, field="register_public_key_digest")
        req_digest = self._capability_key_request_digest(
            kind=kind,
            expected_registry_revision=expected_registry_revision,
            expected_issuance_key_id=expected_issuance_key_id,
            target_capability_signing_key_id=target_capability_signing_key_id,
            replacement_issuance_key_id=replacement_issuance_key_id,
            register_public_verification_key=register_public_verification_key,
            register_public_key_digest=register_public_key_digest,
            register_private_signing_secret_ref=register_private_signing_secret_ref,
            register_not_before_ms=register_not_before_ms,
            authenticated_principal_id=authenticated_principal_id,
            authorization_context_digest=authorization_context_digest,
        )
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM capability_key_operations WHERE capability_key_operation_id = ?",
                (capability_key_operation_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["request_digest"] == req_digest
                ):
                    return _row_to_capability_key_operation(existing, replayed=True)
                return self._transient_capability_key_conflict(capability_key_operation_id, kind)
            registry = self.get_capability_key_registry()
            rejection = self._validate_capability_key_operation(
                registry=registry,
                kind=kind,
                expected_registry_revision=expected_registry_revision,
                expected_issuance_key_id=expected_issuance_key_id,
                target_capability_signing_key_id=target_capability_signing_key_id,
                replacement_issuance_key_id=replacement_issuance_key_id,
                register_public_verification_key=register_public_verification_key,
                register_public_key_digest=register_public_key_digest,
                register_private_signing_secret_ref=register_private_signing_secret_ref,
                register_not_before_ms=register_not_before_ms,
                authority_revoked=authority_revoked,
                private_key_proof_valid=private_key_proof_valid,
            )
            result_revision = None
            result_key = None
            status = "REJECTED"
            if rejection is None:
                status = "SUCCEEDED"
                result_revision = registry.registry_revision + 1
                result_key = self._result_issuance_key_id(
                    current_key_id=registry.current_issuance_key_id,
                    kind=kind,
                    target_capability_signing_key_id=target_capability_signing_key_id,
                    replacement_issuance_key_id=replacement_issuance_key_id,
                )
                http_status, body_json, resp_digest = self._capability_key_response(
                    operation_id=capability_key_operation_id,
                    kind=kind,
                    status=status,
                    registry_revision=result_revision,
                    current_issuance_key_id=result_key,
                )
            else:
                http_status, body_json, resp_digest = self._capability_key_response(
                    operation_id=capability_key_operation_id,
                    kind=kind,
                    status=status,
                    rejection_code=rejection,
                )
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO capability_key_operations("
                "capability_key_operation_id, protocol_version, kind, "
                "expected_registry_revision, expected_issuance_key_id, "
                "target_capability_signing_key_id, replacement_issuance_key_id, "
                "register_public_verification_key, register_public_key_digest, "
                "register_private_signing_secret_ref, register_not_before_ms, "
                "authenticated_principal_id, authorization_context_digest, request_digest, "
                "status, rejection_code, result_registry_revision, result_issuance_key_id, "
                "response_http_status, response_json, response_digest, completed_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    capability_key_operation_id,
                    CAPABILITY_KEY_OPERATION_PROTOCOL,
                    kind,
                    expected_registry_revision,
                    expected_issuance_key_id,
                    target_capability_signing_key_id,
                    replacement_issuance_key_id,
                    register_public_verification_key,
                    register_public_key_digest,
                    register_private_signing_secret_ref,
                    register_not_before_ms,
                    authenticated_principal_id,
                    authorization_context_digest,
                    req_digest,
                    status,
                    rejection,
                    result_revision,
                    result_key,
                    http_status,
                    body_json,
                    resp_digest,
                    now,
                ),
            )
            if status == "SUCCEEDED":
                self._apply_successful_capability_key_operation(
                    kind=kind,
                    capability_key_operation_id=capability_key_operation_id,
                    target_capability_signing_key_id=target_capability_signing_key_id,
                    replacement_issuance_key_id=replacement_issuance_key_id,
                    register_public_verification_key=register_public_verification_key,
                    register_public_key_digest=register_public_key_digest,
                    register_private_signing_secret_ref=register_private_signing_secret_ref,
                    register_not_before_ms=register_not_before_ms,
                    result_revision=result_revision,
                    result_key=result_key,
                    now=now,
                )
            row = self.conn.execute(
                "SELECT * FROM capability_key_operations WHERE capability_key_operation_id = ?",
                (capability_key_operation_id,),
            ).fetchone()
            assert row is not None
            return _row_to_capability_key_operation(row, replayed=False)

    def _transient_capability_key_conflict(
        self, operation_id: str, kind: str
    ) -> CapabilityKeyOperationResult:
        http_status, body_json, resp_digest = self._capability_key_response(
            operation_id=operation_id,
            kind=kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
        )
        return CapabilityKeyOperationResult(
            capability_key_operation_id=operation_id,
            kind=kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
            response_http_status=http_status,
            response_json=body_json,
            response_digest=resp_digest,
            completed_at_ms=_now_ms(),
        )

    def _validate_capability_key_operation(
        self,
        *,
        registry: CapabilityKeyRegistryProjection,
        kind: str,
        expected_registry_revision: int,
        expected_issuance_key_id: str | None,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
        register_public_verification_key: bytes | None,
        register_public_key_digest: str | None,
        register_private_signing_secret_ref: str | None,
        register_not_before_ms: int | None,
        authority_revoked: bool,
        private_key_proof_valid: bool,
    ) -> str | None:
        if authority_revoked:
            return "AUTHORITY_REVOKED"
        if (
            registry.registry_revision != expected_registry_revision
            or registry.current_issuance_key_id != expected_issuance_key_id
        ):
            return "CAS_LOST"
        target = self.get_capability_signing_key(target_capability_signing_key_id)
        if kind == "REGISTER":
            if target is not None:
                return "KEY_ALREADY_EXISTS"
            if (
                register_public_verification_key is None
                or register_public_key_digest is None
                or register_private_signing_secret_ref is None
                or register_not_before_ms is None
                or replacement_issuance_key_id is not None
                or len(register_public_verification_key) != 32
                or capability_public_key_digest(register_public_verification_key)
                != register_public_key_digest
                or not private_key_proof_valid
            ):
                return "INTEGRITY_CONFLICT"
            digest_collision = self.conn.execute(
                "SELECT 1 FROM capability_signing_keys WHERE public_key_digest = ?",
                (register_public_key_digest,),
            ).fetchone()
            if digest_collision is not None:
                return "INTEGRITY_CONFLICT"
            return None
        if any(
            value is not None
            for value in (
                register_public_verification_key,
                register_public_key_digest,
                register_private_signing_secret_ref,
                register_not_before_ms,
            )
        ):
            return "INTEGRITY_CONFLICT"
        if target is None or target.state != "ACTIVE":
            if not (kind == "REVOKE" and target is not None and target.state == "RETIRED"):
                return "KEY_NOT_ACTIVE"
        replacement = (
            None
            if replacement_issuance_key_id is None
            else self.get_capability_signing_key(replacement_issuance_key_id)
        )
        if replacement_issuance_key_id is not None and (
            replacement is None or replacement.state != "ACTIVE"
        ):
            return "KEY_NOT_ACTIVE"
        if kind == "SELECT":
            if replacement_issuance_key_id is not None:
                return "INTEGRITY_CONFLICT"
            return None
        if kind == "RETIRE":
            if target_capability_signing_key_id == registry.current_issuance_key_id:
                if replacement_issuance_key_id is None:
                    return "CURRENT_KEY_REQUIRES_REPLACEMENT"
                if replacement_issuance_key_id == target_capability_signing_key_id:
                    return "KEY_NOT_ACTIVE"
            elif replacement_issuance_key_id is not None:
                return "INTEGRITY_CONFLICT"
            return None
        if kind == "REVOKE":
            if (
                target_capability_signing_key_id != registry.current_issuance_key_id
                and replacement_issuance_key_id is not None
            ):
                return "INTEGRITY_CONFLICT"
            if replacement_issuance_key_id == target_capability_signing_key_id:
                return "KEY_NOT_ACTIVE"
            return None
        raise AssertionError("unreachable capability key operation kind")

    def _result_issuance_key_id(
        self,
        *,
        current_key_id: str | None,
        kind: str,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
    ) -> str | None:
        if kind == "REGISTER":
            return current_key_id
        if kind == "SELECT":
            return target_capability_signing_key_id
        if kind == "RETIRE":
            return (
                replacement_issuance_key_id
                if target_capability_signing_key_id == current_key_id
                else current_key_id
            )
        if kind == "REVOKE":
            return (
                replacement_issuance_key_id
                if target_capability_signing_key_id == current_key_id
                else current_key_id
            )
        raise AssertionError("unreachable capability key operation kind")

    def _apply_successful_capability_key_operation(
        self,
        *,
        kind: str,
        capability_key_operation_id: str,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
        register_public_verification_key: bytes | None,
        register_public_key_digest: str | None,
        register_private_signing_secret_ref: str | None,
        register_not_before_ms: int | None,
        result_revision: int | None,
        result_key: str | None,
        now: int,
    ) -> None:
        assert result_revision is not None
        if kind == "REGISTER":
            assert register_public_verification_key is not None
            assert register_public_key_digest is not None
            assert register_private_signing_secret_ref is not None
            assert register_not_before_ms is not None
            self.conn.execute(
                "INSERT INTO capability_signing_keys("
                "capability_signing_key_id, registration_operation_id, signature_algorithm, "
                "public_verification_key, public_key_digest, private_signing_secret_ref, "
                "registered_at_ms, not_before_ms, state) "
                "VALUES (?, ?, 'ED25519', ?, ?, ?, ?, ?, 'ACTIVE')",
                (
                    target_capability_signing_key_id,
                    capability_key_operation_id,
                    register_public_verification_key,
                    register_public_key_digest,
                    register_private_signing_secret_ref,
                    now,
                    register_not_before_ms,
                ),
            )
        elif kind == "RETIRE":
            self.conn.execute(
                "UPDATE capability_signing_keys SET state = 'RETIRED', retired_at_ms = ?, "
                "retirement_change_id = ?, retirement_principal_id = ("
                "SELECT authenticated_principal_id FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?), retirement_authorization_digest = ("
                "SELECT authorization_context_digest FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?) "
                "WHERE capability_signing_key_id = ? AND state = 'ACTIVE'",
                (
                    now,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    target_capability_signing_key_id,
                ),
            )
        elif kind == "REVOKE":
            self.conn.execute(
                "UPDATE capability_signing_keys SET state = 'REVOKED', revoked_at_ms = ?, "
                "revocation_change_id = ?, revocation_principal_id = ("
                "SELECT authenticated_principal_id FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?), revocation_authorization_digest = ("
                "SELECT authorization_context_digest FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?) "
                "WHERE capability_signing_key_id = ? AND state IN ('ACTIVE', 'RETIRED')",
                (
                    now,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    target_capability_signing_key_id,
                ),
            )
        self.conn.execute(
            "UPDATE capability_key_registry SET registry_revision = ?, "
            "current_issuance_key_id = ?, last_operation_id = ? WHERE registry_id = ?",
            (result_revision, result_key, capability_key_operation_id, CONTROLLER_ID),
        )

    def _selected_issuance_key_from_registry(
        self, registry: CapabilityKeyRegistryProjection, *, now_ms: int | None = None
    ) -> CapabilitySigningKey | None:
        if registry.current_issuance_key_id is None:
            return None
        key = self.get_capability_signing_key(registry.current_issuance_key_id)
        now = _now_ms() if now_ms is None else now_ms
        if key is None or key.state != "ACTIVE" or key.not_before_ms > now:
            return None
        if capability_public_key_digest(key.public_verification_key) != key.public_key_digest:
            return None
        return key

    def selected_issuance_key(self, *, now_ms: int | None = None) -> CapabilitySigningKey | None:
        registry = self.get_capability_key_registry()
        return self._selected_issuance_key_from_registry(registry, now_ms=now_ms)

    def _controller_gate_evaluation(self) -> _ControllerGateEvaluation:
        mode = self.get_controller_mode()
        registry = self.get_capability_key_registry()
        selected_key = self._selected_issuance_key_from_registry(registry)
        issuance_ready = selected_key is not None
        current_mode = mode.mode
        new_admission = current_mode == "RUNNING" or (
            current_mode == "DISPATCH_PAUSED"
            and mode.dispatch_paused_intake_policy == "ALLOW_ADMISSION"
        )
        new_claims = current_mode in {"RUNNING", "INTAKE_PAUSED"} and issuance_ready
        return _ControllerGateEvaluation(
            permissions=ControllerGatePermissions(
                mode_revision=mode.mode_revision,
                mode=current_mode,
                registry_revision=registry.registry_revision,
                current_issuance_key_id=registry.current_issuance_key_id,
                new_admission=new_admission,
                new_claims=new_claims,
                first_result_mutation=current_mode
                in {"RUNNING", "INTAKE_PAUSED", "DISPATCH_PAUSED", "DRAINING"},
                existing_result_replay=current_mode is not None,
                forge_reconciliation=current_mode
                in {"RUNNING", "INTAKE_PAUSED", "DISPATCH_PAUSED", "DRAINING"},
                management_operations=current_mode is not None,
            ),
            registry=registry,
            selected_key=selected_key,
        )

    def controller_gate_permissions(self) -> ControllerGatePermissions:
        return self._controller_gate_evaluation().permissions

    def _assert_offer_planning_permitted(
        self,
    ) -> tuple[CapabilityKeyRegistryProjection, CapabilitySigningKey]:
        evaluation = self._controller_gate_evaluation()
        if not evaluation.permissions.new_claims:
            raise WorkflowGateClosedError(
                "offer planning requires an active issuance key and a dispatch-permitting mode"
            )
        assert evaluation.selected_key is not None
        return evaluation.registry, evaluation.selected_key

    def assert_offer_planning_permitted(self) -> None:
        self._assert_offer_planning_permitted()

    def record_issued_capability_binding(
        self,
        *,
        capability_jti: str,
        claim_digest: str,
        immutable_assignment_digest: str,
        immutable_assignment: Any,
    ) -> IssuedCapabilityBinding:
        require_lowercase_uuid(capability_jti, field="capability_jti")
        _require_digest(claim_digest, field="claim_digest")
        _require_digest(immutable_assignment_digest, field="immutable_assignment_digest")
        assignment_json = _require_json_text(immutable_assignment)
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM capability_issuance_audit WHERE capability_jti = ?",
                (capability_jti,),
            ).fetchone()
            if existing is not None:
                row = _row_to_issued_capability(existing)
                if (
                    row.claim_digest == claim_digest
                    and row.immutable_assignment_digest == immutable_assignment_digest
                    and row.immutable_assignment_json == assignment_json
                ):
                    return row
                raise IdempotencyConflictError("capability JTI was reused")
            registry, key = self._assert_offer_planning_permitted()
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO capability_issuance_audit("
                "capability_jti, capability_signing_key_id, signature_algorithm, "
                "claim_digest, immutable_assignment_digest, immutable_assignment_json, "
                "capability_key_registry_revision, issued_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    capability_jti,
                    key.capability_signing_key_id,
                    key.signature_algorithm,
                    claim_digest,
                    immutable_assignment_digest,
                    assignment_json,
                    registry.registry_revision,
                    now,
                ),
            )
            row = self.conn.execute(
                "SELECT * FROM capability_issuance_audit WHERE capability_jti = ?",
                (capability_jti,),
            ).fetchone()
            assert row is not None
            return _row_to_issued_capability(row)

    def get_attempt_claim(self, attempt_claim_id: str) -> AttemptClaimRecord | None:
        require_lowercase_uuid(attempt_claim_id, field="attempt_claim_id")
        row = self.conn.execute(
            "SELECT * FROM attempt_claims WHERE attempt_claim_id = ?",
            (attempt_claim_id,),
        ).fetchone()
        return None if row is None else _row_to_attempt_claim(row)

    def get_attempt_claim_for_attempt(self, attempt_id: str) -> AttemptClaimRecord | None:
        require_lowercase_uuid(attempt_id, field="attempt_id")
        row = self.conn.execute(
            "SELECT * FROM attempt_claims WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return None if row is None else _row_to_attempt_claim(row)

    def claim_attempt(
        self,
        *,
        attempt_claim_id: str,
        attempt_id: str,
        activity_id: str,
        generation: int,
        offer_outbox_id: str,
        worker_id: str,
        worker_session_id: str,
        worker_profile: str,
        worker_build_revision: str,
        request_digest: str,
        execution_deadline_ms: int,
        attempt_capability_jti: str,
        attempt_capability_digest: str,
        source_access_kind: str,
        source_access_descriptor: Any,
        source_access_descriptor_digest: str,
        response_contract_digest: str,
        source_read_secret_ref: str | None = None,
        provider_secret_ref: str | None = None,
        launch_nonce_id: str | None = None,
        launch_capability_jti: str | None = None,
        launch_capability_claims: Mapping[str, Any] | None = None,
        launch_capability_digest: str | None = None,
    ) -> AttemptClaimReplayMaterialization:
        """Durably claim one offered Attempt generation.

        Raw bearers and secret bytes are intentionally outside this API; callers
        rematerialize them from the returned immutable identifiers only while
        the returned replay flags permit it.
        """
        require_lowercase_uuid(attempt_claim_id, field="attempt_claim_id")
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(activity_id, field="activity_id")
        require_lowercase_uuid(attempt_capability_jti, field="attempt_capability_jti")
        _require_positive_int(generation, field="generation")
        _require_nonempty_text(offer_outbox_id, field="offer_outbox_id")
        _require_nonempty_text(worker_id, field="worker_id")
        _require_nonempty_text(worker_session_id, field="worker_session_id")
        _require_nonempty_text(worker_profile, field="worker_profile")
        _require_nonempty_text(worker_build_revision, field="worker_build_revision")
        _require_digest(request_digest, field="request_digest")
        _require_positive_int(execution_deadline_ms, field="execution_deadline_ms")
        _require_digest(attempt_capability_digest, field="attempt_capability_digest")
        _require_digest(source_access_descriptor_digest, field="source_access_descriptor_digest")
        _require_digest(response_contract_digest, field="response_contract_digest")
        enums.parse_enum("attempt_claim.source_access_kind", source_access_kind)
        source_access_descriptor_json = _require_json_text(source_access_descriptor)
        if launch_capability_claims is not None:
            computed = launch_capability_claims_digest(launch_capability_claims)
            if launch_capability_digest is not None and launch_capability_digest != computed:
                raise IdempotencyConflictError("launch capability digest does not match claims")
            launch_capability_digest = computed
        if launch_nonce_id is not None:
            require_lowercase_uuid(launch_nonce_id, field="launch_nonce_id")
        if launch_capability_jti is not None:
            require_lowercase_uuid(launch_capability_jti, field="launch_capability_jti")
        if launch_capability_digest is not None:
            _require_digest(launch_capability_digest, field="launch_capability_digest")

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM attempt_claims WHERE attempt_claim_id = ?",
                (attempt_claim_id,),
            ).fetchone()
            if existing is not None:
                claim = _row_to_attempt_claim(existing)
                if (
                    claim.attempt_id != attempt_id
                    or claim.activity_id != activity_id
                    or claim.attempt_generation != generation
                    or claim.worker_id != worker_id
                    or claim.worker_session_id != worker_session_id
                    or claim.request_digest != request_digest
                ):
                    raise IdempotencyConflictError(
                        "attempt claim id was reused with different content"
                    )
                attempt = self.get_attempt(attempt_id)
                if attempt is None:
                    raise RunStoreError(f"attempt {attempt_id!r} was not found")
                return self._claim_replay_materialization(claim, attempt)

            row = self.conn.execute(
                "SELECT attempts.*, activities.state AS activity_state, "
                "activities.run_id AS run_id, "
                "activities.specification_generation AS activity_spec, "
                "runs.state AS run_state, runs.specification_generation AS run_spec, "
                "outbox.outbox_id AS outbox_row_id "
                "FROM attempts "
                "JOIN activities ON activities.activity_id = attempts.activity_id "
                "JOIN runs ON runs.run_id = activities.run_id "
                "JOIN outbox ON outbox.attempt_id = attempts.attempt_id "
                "AND outbox.attempt_generation = attempts.generation "
                "WHERE attempts.attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(f"attempt {attempt_id!r} was not found")
            if row["activity_id"] != activity_id or row["generation"] != generation:
                raise CasMismatchError("attempt identity does not match claim body")
            if row["outbox_row_id"] != offer_outbox_id:
                raise CasMismatchError("offer outbox does not match attempt")
            if row["state"] != "OFFERED" or row["activity_state"] != "READY":
                other = self.get_attempt_claim_for_attempt(attempt_id)
                if other is not None and other.worker_session_id != worker_session_id:
                    raise IdempotencyConflictError("attempt is already claimed")
                raise CasMismatchError("attempt is not claimable")
            now = _now_ms()
            if now >= row["claim_deadline_ms"]:
                raise CasMismatchError("attempt claim deadline has passed")
            if row["worker_profile"] != worker_profile:
                raise CasMismatchError("worker profile does not match attempt")
            if (
                row["run_state"] not in {"ADMITTED", "ACTIVE"}
                or row["run_spec"] != row["activity_spec"]
            ):
                raise CasMismatchError("run generation is stale")
            registry, key = self._assert_offer_planning_permitted()
            if execution_deadline_ms <= now:
                raise CasMismatchError("execution deadline is not in the future")
            auth_expires_at_ms = execution_deadline_ms + 86_400_000
            model_backed = row["provider"] is not None or row["model"] is not None
            has_launch = (
                launch_nonce_id is not None
                and launch_capability_jti is not None
                and launch_capability_digest is not None
            )
            if model_backed != has_launch:
                raise CasMismatchError("launch capability presence does not match attempt kind")
            if model_backed and provider_secret_ref is None:
                raise CasMismatchError("model-backed attempt requires provider secret binding")
            if not model_backed and provider_secret_ref is not None:
                raise CasMismatchError("deterministic attempt cannot bind a provider secret")

            self.conn.execute(
                "INSERT INTO attempt_claims("
                "attempt_claim_id, protocol_version, attempt_id, activity_id, "
                "attempt_generation, offer_outbox_id, worker_id, worker_session_id, "
                "worker_profile, worker_build_revision, request_digest, claimed_at_ms, "
                "execution_deadline_ms, capability_auth_expires_at_ms, "
                "attempt_capability_jti, attempt_capability_digest, "
                "attempt_capability_signing_key_id, attempt_capability_signature_algorithm, "
                "capability_key_registry_revision, launch_nonce_id, launch_capability_jti, "
                "launch_capability_digest, launch_capability_signing_key_id, "
                "launch_capability_signature_algorithm, source_access_kind, "
                "source_read_secret_ref, provider_secret_ref, source_access_descriptor_json, "
                "source_access_descriptor_digest, response_contract_digest, created_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    attempt_claim_id,
                    ATTEMPT_CLAIM_PROTOCOL,
                    attempt_id,
                    activity_id,
                    generation,
                    offer_outbox_id,
                    worker_id,
                    worker_session_id,
                    worker_profile,
                    worker_build_revision,
                    request_digest,
                    now,
                    execution_deadline_ms,
                    auth_expires_at_ms,
                    attempt_capability_jti,
                    attempt_capability_digest,
                    key.capability_signing_key_id,
                    key.signature_algorithm,
                    registry.registry_revision,
                    launch_nonce_id,
                    launch_capability_jti,
                    launch_capability_digest,
                    key.capability_signing_key_id if has_launch else None,
                    key.signature_algorithm if has_launch else None,
                    source_access_kind,
                    source_read_secret_ref,
                    provider_secret_ref,
                    source_access_descriptor_json,
                    source_access_descriptor_digest,
                    response_contract_digest,
                    now,
                ),
            )
            self.conn.execute(
                "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
                "claimed_worker_session_id = ?, claimed_at_ms = ?, execution_deadline_ms = ?, "
                "capability_auth_expires_at_ms = ?, attempt_capability_jti = ?, "
                "attempt_capability_digest = ?, attempt_capability_signing_key_id = ?, "
                "attempt_capability_signature_algorithm = ?, attempt_claim_id = ?, "
                "launch_nonce_id = ?, launch_capability_digest = ?, provider_secret_ref = ? "
                "WHERE attempt_id = ? AND state = 'OFFERED'",
                (
                    worker_id,
                    worker_session_id,
                    now,
                    execution_deadline_ms,
                    auth_expires_at_ms,
                    attempt_capability_jti,
                    attempt_capability_digest,
                    key.capability_signing_key_id,
                    key.signature_algorithm,
                    attempt_claim_id,
                    launch_nonce_id,
                    launch_capability_digest,
                    provider_secret_ref,
                    attempt_id,
                ),
            )
            self.conn.execute(
                "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ? AND state = 'READY'",
                (activity_id,),
            )
            self.conn.execute(
                "UPDATE outbox SET state = 'DELIVERED', delivery_count = delivery_count + 1 "
                "WHERE outbox_id = ? AND state != 'DELIVERED'",
                (offer_outbox_id,),
            )
            claim_row = self.conn.execute(
                "SELECT * FROM attempt_claims WHERE attempt_claim_id = ?",
                (attempt_claim_id,),
            ).fetchone()
            assert claim_row is not None
            attempt = self.get_attempt(attempt_id)
            assert attempt is not None
            return self._claim_replay_materialization(_row_to_attempt_claim(claim_row), attempt)

    def _claim_replay_materialization(
        self, claim: AttemptClaimRecord, attempt: AttemptRecord
    ) -> AttemptClaimReplayMaterialization:
        now = _now_ms()
        before_execution_deadline = now < claim.execution_deadline_ms
        signing_key = self.get_capability_signing_key(claim.attempt_capability_signing_key_id)
        capability_key_retained = signing_key is not None and signing_key.state != "REVOKED"
        return AttemptClaimReplayMaterialization(
            claim=claim,
            attempt=attempt,
            can_rematerialize_source=before_execution_deadline,
            can_rematerialize_launch_capability=(
                before_execution_deadline
                and capability_key_retained
                and claim.launch_capability_digest is not None
                and attempt.launch_capability_consumed_at_ms is None
            ),
            can_rematerialize_attempt_capability=(
                capability_key_retained and now < claim.capability_auth_expires_at_ms
            ),
        )

    def get_launch_attestation(self, launch_attestation_id: str) -> LaunchAttestationRecord | None:
        require_lowercase_uuid(launch_attestation_id, field="launch_attestation_id")
        row = self.conn.execute(
            "SELECT * FROM launch_attestations WHERE launch_attestation_id = ?",
            (launch_attestation_id,),
        ).fetchone()
        return None if row is None else _row_to_launch_attestation(row)

    def accept_launch_attestation(
        self,
        *,
        launch_attestation_id: str,
        attempt_id: str,
        activity_id: str,
        attempt_generation: int,
        worker_id: str,
        worker_session_id: str,
        pool_manager_id: str,
        runner_principal_id: str,
        runner_image_digest: str,
        runner_registration_revision: int,
        launch_nonce_id: str,
        launch_capability_digest: str,
        launch_capability_signing_key_id: str,
        launch_capability_signature_algorithm: str,
        workspace_instance_id: str,
        context_instance_id: str,
        invocation_instance_id: str,
        workspace_parent_id: str | None,
        context_parent_id: str | None,
        invocation_parent_id: str | None,
        fresh_workspace: bool,
        fresh_context: bool,
        fresh_invocation: bool,
        prepared_at_ms: int,
        attested_at_ms: int,
        runner_signing_key_id: str,
        runner_signature_algorithm: str,
        signature: str,
        attestation_digest: str,
        response_contract_digest: str,
        provider_material_descriptor: Any | None = None,
        provider_material_descriptor_digest: str | None = None,
    ) -> LaunchAcceptedReplay:
        require_lowercase_uuid(launch_attestation_id, field="launch_attestation_id")
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(activity_id, field="activity_id")
        require_lowercase_uuid(launch_nonce_id, field="launch_nonce_id")
        require_lowercase_uuid(workspace_instance_id, field="workspace_instance_id")
        require_lowercase_uuid(context_instance_id, field="context_instance_id")
        require_lowercase_uuid(invocation_instance_id, field="invocation_instance_id")
        _require_positive_int(attempt_generation, field="attempt_generation")
        _require_nonempty_text(worker_id, field="worker_id")
        _require_nonempty_text(worker_session_id, field="worker_session_id")
        _require_nonempty_text(pool_manager_id, field="pool_manager_id")
        _require_nonempty_text(runner_principal_id, field="runner_principal_id")
        _require_digest(runner_image_digest, field="runner_image_digest")
        if not isinstance(runner_registration_revision, int) or runner_registration_revision < 0:
            raise ValueError("runner_registration_revision must be a nonnegative integer")
        _require_digest(launch_capability_digest, field="launch_capability_digest")
        _require_nonempty_text(
            launch_capability_signing_key_id, field="launch_capability_signing_key_id"
        )
        enums.parse_enum(
            "capability_signing_key.signature_algorithm",
            launch_capability_signature_algorithm,
        )
        _require_nonempty_text(runner_signing_key_id, field="runner_signing_key_id")
        enums.parse_enum(
            "capability_signing_key.signature_algorithm",
            runner_signature_algorithm,
        )
        _require_nonempty_text(signature, field="signature")
        _require_digest(attestation_digest, field="attestation_digest")
        _require_digest(response_contract_digest, field="response_contract_digest")
        if not isinstance(prepared_at_ms, int) or prepared_at_ms < 0:
            raise ValueError("prepared_at_ms must be a nonnegative integer")
        if not isinstance(attested_at_ms, int) or attested_at_ms < prepared_at_ms:
            raise ValueError("attested_at_ms must be greater than or equal to prepared_at_ms")
        if (
            workspace_parent_id is not None
            or context_parent_id is not None
            or invocation_parent_id is not None
        ):
            raise CasMismatchError("launch attestation parent ids must be null")
        if not (fresh_workspace and fresh_context and fresh_invocation):
            raise CasMismatchError("launch attestation must prove fresh isolation")
        descriptor_json: str | None = None
        if provider_material_descriptor is not None:
            descriptor_json = _require_json_text(provider_material_descriptor)
            if provider_material_descriptor_digest is None:
                provider_material_descriptor_digest = request_digest(provider_material_descriptor)
            _require_digest(
                provider_material_descriptor_digest,
                field="provider_material_descriptor_digest",
            )
        elif provider_material_descriptor_digest is not None:
            raise ValueError("provider material descriptor digest requires a descriptor")

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM launch_attestations WHERE launch_attestation_id = ?",
                (launch_attestation_id,),
            ).fetchone()
            if existing is not None:
                attestation = _row_to_launch_attestation(existing)
                if (
                    attestation.attempt_id != attempt_id
                    or attestation.activity_id != activity_id
                    or attestation.attempt_generation != attempt_generation
                    or attestation.worker_id != worker_id
                    or attestation.worker_session_id != worker_session_id
                    or attestation.launch_capability_digest != launch_capability_digest
                    or attestation.attestation_digest != attestation_digest
                ):
                    raise IdempotencyConflictError(
                        "launch attestation id was reused with different content"
                    )
                self._assert_launch_capability_key_lookup_permitted(
                    attestation.launch_capability_signing_key_id
                )
                attempt = self.get_attempt(attempt_id)
                if attempt is None:
                    raise RunStoreError(f"attempt {attempt_id!r} was not found")
                return self._launch_replay(attestation, attempt)

            claim_row = self.conn.execute(
                "SELECT * FROM attempt_claims WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
            if claim_row is None:
                raise CasMismatchError("attempt has no durable claim")
            claim = _row_to_attempt_claim(claim_row)
            attempt = self.get_attempt(attempt_id)
            if attempt is None:
                raise RunStoreError(f"attempt {attempt_id!r} was not found")
            if attempt.state != "CLAIMED":
                raise CasMismatchError("attempt is not currently claimed")
            if _now_ms() >= claim.execution_deadline_ms:
                raise CasMismatchError("launch attestation arrived after execution deadline")
            if (
                claim.activity_id != activity_id
                or claim.attempt_generation != attempt_generation
                or claim.worker_id != worker_id
                or claim.worker_session_id != worker_session_id
                or claim.launch_nonce_id != launch_nonce_id
                or claim.launch_capability_digest != launch_capability_digest
                or claim.launch_capability_signing_key_id != launch_capability_signing_key_id
                or claim.launch_capability_signature_algorithm
                != launch_capability_signature_algorithm
            ):
                raise CasMismatchError("launch attestation does not match frozen claim")
            if (
                claim.launch_capability_digest is None
                or attempt.launch_capability_consumed_at_ms is not None
            ):
                raise CasMismatchError("launch capability is not available")
            self._assert_launch_capability_key_lookup_permitted(launch_capability_signing_key_id)
            if attempt.provider_secret_ref != claim.provider_secret_ref:
                raise CasMismatchError("provider secret binding drifted from claim")
            if provider_material_descriptor is None and claim.provider_secret_ref is not None:
                raise CasMismatchError("provider material descriptor is required")
            for column, value in (
                ("workspace_instance_id", workspace_instance_id),
                ("context_instance_id", context_instance_id),
                ("invocation_instance_id", invocation_instance_id),
            ):
                reused = self.conn.execute(
                    f"SELECT 1 FROM launch_attestations WHERE {column} = ?", (value,)
                ).fetchone()
                if reused is not None:
                    raise IdempotencyConflictError(f"{column} was already attested")

            now = _now_ms()
            self.conn.execute(
                "INSERT INTO launch_attestations("
                "launch_attestation_id, attempt_id, activity_id, attempt_generation, "
                "attempt_claim_id, worker_id, worker_session_id, pool_manager_id, "
                "runner_principal_id, runner_image_digest, runner_registration_revision, "
                "launch_nonce_id, launch_capability_digest, launch_capability_signing_key_id, "
                "launch_capability_signature_algorithm, workspace_instance_id, "
                "context_instance_id, invocation_instance_id, workspace_parent_id, "
                "context_parent_id, invocation_parent_id, fresh_workspace, fresh_context, "
                "fresh_invocation, prepared_at_ms, attested_at_ms, runner_signing_key_id, "
                "runner_signature_algorithm, signature, attestation_digest, provider_secret_ref, "
                "provider_material_descriptor_json, provider_material_descriptor_digest, "
                "response_contract_digest, accepted_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "1, 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    launch_attestation_id,
                    attempt_id,
                    activity_id,
                    attempt_generation,
                    claim.attempt_claim_id,
                    worker_id,
                    worker_session_id,
                    pool_manager_id,
                    runner_principal_id,
                    runner_image_digest,
                    runner_registration_revision,
                    launch_nonce_id,
                    launch_capability_digest,
                    launch_capability_signing_key_id,
                    launch_capability_signature_algorithm,
                    workspace_instance_id,
                    context_instance_id,
                    invocation_instance_id,
                    workspace_parent_id,
                    context_parent_id,
                    invocation_parent_id,
                    prepared_at_ms,
                    attested_at_ms,
                    runner_signing_key_id,
                    runner_signature_algorithm,
                    signature,
                    attestation_digest,
                    claim.provider_secret_ref,
                    descriptor_json,
                    provider_material_descriptor_digest,
                    response_contract_digest,
                    now,
                ),
            )
            self.conn.execute(
                "UPDATE attempts SET launch_attestation_id = ?, "
                "launch_capability_consumed_at_ms = ? "
                "WHERE attempt_id = ? AND launch_capability_consumed_at_ms IS NULL",
                (launch_attestation_id, now, attempt_id),
            )
            row = self.conn.execute(
                "SELECT * FROM launch_attestations WHERE launch_attestation_id = ?",
                (launch_attestation_id,),
            ).fetchone()
            assert row is not None
            updated_attempt = self.get_attempt(attempt_id)
            assert updated_attempt is not None
            return self._launch_replay(_row_to_launch_attestation(row), updated_attempt)

    def _assert_launch_capability_key_lookup_permitted(self, key_id: str) -> None:
        key = self.get_capability_signing_key(key_id)
        if key is None or key.state == "REVOKED":
            raise WorkflowGateClosedError("launch capability signing key is revoked or unknown")

    def _launch_replay(
        self, attestation: LaunchAttestationRecord, attempt: AttemptRecord
    ) -> LaunchAcceptedReplay:
        available = (
            attempt.state == "CLAIMED"
            and attempt.execution_deadline_ms is not None
            and _now_ms() < attempt.execution_deadline_ms
        )
        can_materialize = available and attestation.provider_material_descriptor_json is not None
        return LaunchAcceptedReplay(
            attestation=attestation,
            attempt=attempt,
            status="AVAILABLE" if can_materialize else "EXPIRED",
            can_rematerialize_provider=can_materialize,
        )

    # -- Secret Provision Operation (MANAGEMENT_PROVISION provision/adopt) --

    def get_secret_current_version(self, secret_id: str) -> SecretCurrentVersionProjection | None:
        require_lowercase_uuid(secret_id, field="secret_id")
        row = self.conn.execute(
            "SELECT * FROM secret_current_versions WHERE secret_id = ?", (secret_id,)
        ).fetchone()
        return None if row is None else _row_to_secret_current_version(row)

    def list_current_secrets_for_owner(
        self, *, owner_scope_kind: str, owner_scope_id: str
    ) -> list[SecretCurrentVersionProjection]:
        enums.parse_enum("secret_provision_operation.owner_scope_kind", owner_scope_kind)
        rows = self.conn.execute(
            "SELECT * FROM secret_current_versions WHERE owner_scope_kind = ? "
            "AND owner_scope_id = ? AND current_version > 0 ORDER BY purpose, secret_id",
            (owner_scope_kind, owner_scope_id),
        ).fetchall()
        return [_row_to_secret_current_version(row) for row in rows]

    def get_secret_version(self, secret_id: str, version: int) -> SecretVersionRecord | None:
        require_lowercase_uuid(secret_id, field="secret_id")
        row = self.conn.execute(
            "SELECT * FROM secret_versions WHERE secret_id = ? AND version = ?",
            (secret_id, version),
        ).fetchone()
        return None if row is None else _row_to_secret_version(row)

    def get_credential_rotation_receipt(
        self, credential_rotation_receipt_id: str
    ) -> CredentialRotationReceiptRecord | None:
        require_lowercase_uuid(
            credential_rotation_receipt_id, field="credential_rotation_receipt_id"
        )
        row = self.conn.execute(
            "SELECT * FROM credential_rotation_receipts WHERE credential_rotation_receipt_id = ?",
            (credential_rotation_receipt_id,),
        ).fetchone()
        return None if row is None else _row_to_credential_rotation_receipt(row)

    def list_secret_provision_checkpoints(
        self, secret_provision_operation_id: str
    ) -> list[SecretProvisionCheckpointRecord]:
        require_lowercase_uuid(secret_provision_operation_id, field="secret_provision_operation_id")
        rows = self.conn.execute(
            "SELECT * FROM secret_provision_checkpoints "
            "WHERE secret_provision_operation_id = ? ORDER BY checkpoint_sequence",
            (secret_provision_operation_id,),
        ).fetchall()
        return [_row_to_secret_provision_checkpoint(row) for row in rows]

    def get_secret_provision_operation(
        self, secret_provision_operation_id: str
    ) -> SecretProvisionOperationResult | None:
        require_lowercase_uuid(secret_provision_operation_id, field="secret_provision_operation_id")
        row = self.conn.execute(
            "SELECT * FROM secret_provision_operations WHERE secret_provision_operation_id = ?",
            (secret_provision_operation_id,),
        ).fetchone()
        if row is None:
            return None
        return self._secret_provision_operation_from_row(row, replayed=True)

    def _secret_provision_operation_from_row(
        self, row: sqlite3.Row, *, replayed: bool
    ) -> SecretProvisionOperationResult:
        if row["state"] == "PENDING":
            http_status, body_json, resp_digest = self._secret_provision_accepted_response(
                operation_id=row["secret_provision_operation_id"],
                secret_id=row["secret_id"],
                target_version=row["target_version"],
            )
        else:
            http_status = row["terminal_http_status"]
            body_json = row["terminal_response_json"]
            resp_digest = row["terminal_response_digest"]
        return SecretProvisionOperationResult(
            secret_provision_operation_id=row["secret_provision_operation_id"],
            mode=row["mode"],
            secret_id=row["secret_id"],
            expected_prior_version=row["expected_prior_version"],
            target_version=row["target_version"],
            state=row["state"],
            rejection_code=row["rejection_code"],
            new_version=row["new_version"],
            credential_rotation_receipt_id=row["credential_rotation_receipt_id"],
            secret_store_staging_receipt_id=row["secret_store_staging_receipt_id"],
            response_http_status=http_status,
            response_json=body_json,
            response_digest=resp_digest,
            created_at_ms=row["created_at_ms"],
            replayed=replayed,
        )

    def _secret_provision_request_digest(
        self,
        *,
        mode: str,
        secret_id: str,
        expected_prior_version: int | None,
        target_version: int,
        purpose: str,
        owner_scope_kind: str,
        owner_scope_id: str,
        provider_account_ref: str | None,
        authenticated_principal_id: str,
        authorization_context_digest: str,
        secret_store_staging_receipt_id: str,
        secret_integrity_attestation_id: str,
    ) -> str:
        return request_digest(
            {
                "protocol_version": SECRET_PROVISION_REQUEST_PROTOCOL,
                "mode": mode,
                "secret_id": secret_id,
                "expected_prior_version": expected_prior_version,
                "target_version": target_version,
                "purpose": purpose,
                "owner_scope_kind": owner_scope_kind,
                "owner_scope_id": owner_scope_id,
                "provider_account_ref": provider_account_ref,
                "authenticated_principal_id": authenticated_principal_id,
                "authorization_context_digest": authorization_context_digest,
                "secret_store_staging_receipt_id": secret_store_staging_receipt_id,
                "secret_integrity_attestation_id": secret_integrity_attestation_id,
            }
        )

    def _secret_provision_accepted_response(
        self, *, operation_id: str, secret_id: str, target_version: int
    ) -> tuple[int, str, str]:
        body: dict[str, object] = {
            "protocol": SECRET_PROVISION_ACCEPTED_PROTOCOL,
            "secret_provision_operation_id": operation_id,
            "state": "PENDING",
            "secret_id": secret_id,
            "target_version": target_version,
        }
        body_json = canonical_json_text(body)
        digest = response_digest({"http_status": 202, "body": body})
        return 202, body_json, digest

    def _secret_provision_result_response(
        self,
        *,
        operation_id: str,
        secret_id: str,
        target_version: int,
        state: str,
        new_version: int | None = None,
        secret_version_key: str | None = None,
        credential_rotation_receipt_id: str | None = None,
        rejection_code: str | None = None,
    ) -> tuple[int, str, str]:
        body: dict[str, object] = {
            "protocol": SECRET_PROVISION_RESULT_PROTOCOL,
            "secret_provision_operation_id": operation_id,
            "state": state,
            "secret_id": secret_id,
            "target_version": target_version,
        }
        if state == "COMPLETED":
            body["secret_version_key"] = secret_version_key
            body["new_version"] = new_version
            body["credential_rotation_receipt_id"] = credential_rotation_receipt_id
            http_status = 200
        else:
            body["rejection_code"] = rejection_code
            http_status = (
                403
                if rejection_code == "AUTHORITY_REVOKED"
                else 422
                if rejection_code == "STAGED_OBJECT_INVALID"
                else 409
            )
        body_json = canonical_json_text(body)
        digest = response_digest({"http_status": http_status, "body": body})
        return http_status, body_json, digest

    def _owner_purpose_matrix_valid(
        self,
        *,
        purpose: str,
        owner_scope_kind: str,
        owner_scope_id: str,
        provider_account_ref: str | None,
    ) -> bool:
        if owner_scope_kind == "FORGE_INSTALLATION":
            return (
                purpose in ("FORGE_API", "SOURCE_READ", "PUBLICATION")
                and provider_account_ref == owner_scope_id
            )
        if owner_scope_kind == "CONTROLLER":
            return (
                purpose == "CAPABILITY_SIGNING_PRIVATE_KEY"
                and owner_scope_id == CONTROLLER_ID
                and provider_account_ref is None
            )
        if owner_scope_kind == "PROJECT":
            return (
                purpose in ("FORGE_API", "SOURCE_READ", "PUBLICATION")
                and provider_account_ref is None
            )
        return False

    def _validate_secret_provision_operation(
        self,
        *,
        current: SecretCurrentVersionProjection | None,
        secret_id: str,
        target_version: int,
        mode: str,
        expected_prior_version: int | None,
        purpose: str,
        owner_scope_kind: str,
        owner_scope_id: str,
        provider_account_ref: str | None,
        authority_revoked: bool,
    ) -> str | None:
        if authority_revoked:
            return "AUTHORITY_REVOKED"
        current_version = 0 if current is None else current.current_version
        if expected_prior_version is None:
            if current_version != 0:
                return "CAS_LOST"
        elif current_version != expected_prior_version:
            return "CAS_LOST"
        # A concurrent operation may already have reserved this exact target
        # (single-writer serialization means it committed its PENDING/COMPLETED
        # row before this transaction's BEGIN IMMEDIATE was granted, but before
        # its own COMPLETED transition ever touches secret_current_versions).
        reserved = self.conn.execute(
            "SELECT 1 FROM secret_provision_operations WHERE secret_id = ? "
            "AND target_version = ? AND state IN ('PENDING', 'COMPLETED')",
            (secret_id, target_version),
        ).fetchone()
        if reserved is not None:
            return "CAS_LOST"
        if current is not None and current_version > 0:
            if (
                current.purpose != purpose
                or current.owner_scope_kind != owner_scope_kind
                or current.owner_scope_id != owner_scope_id
                or current.provider_account_ref != provider_account_ref
            ):
                return "INTEGRITY_CONFLICT"
        if not self._owner_purpose_matrix_valid(
            purpose=purpose,
            owner_scope_kind=owner_scope_kind,
            owner_scope_id=owner_scope_id,
            provider_account_ref=provider_account_ref,
        ):
            return "INTEGRITY_CONFLICT"
        if purpose != "CAPABILITY_SIGNING_PRIVATE_KEY":
            # Stage 0: no other Secret purpose may be provisioned or adopted
            # until the Capability Registry has a selected ACTIVE issuance
            # key -- there is no verifiable authority chain yet.
            registry = self.get_capability_key_registry()
            if registry.current_issuance_key_id is None:
                return "AUTHORITY_REVOKED"
        return None

    def _next_secret_provision_checkpoint_sequence(self, secret_provision_operation_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(checkpoint_sequence), 0) AS seq "
            "FROM secret_provision_checkpoints WHERE secret_provision_operation_id = ?",
            (secret_provision_operation_id,),
        ).fetchone()
        return int(row["seq"]) + 1

    def begin_secret_provision_operation(
        self,
        *,
        secret_provision_operation_id: str,
        mode: str,
        secret_id: str,
        expected_prior_version: int | None,
        purpose: str,
        owner_scope_kind: str,
        owner_scope_id: str,
        authenticated_principal_id: str,
        authorization_context_digest: str,
        secret_store_staging_receipt_id: str,
        secret_integrity_attestation_id: str,
        provider_account_ref: str | None = None,
        authority_revoked: bool = False,
    ) -> SecretProvisionOperationResult:
        """Accept (or replay) one MANAGEMENT_PROVISION request.

        Validates the closed owner/purpose matrix and the prior-version CAS,
        then freezes ``target_version`` and commits the ``PENDING`` Operation
        plus its retry Outbox record in one transaction. A CAS/authority
        rejection detected here has no install phase and is committed as an
        immediately terminal ``REJECTED`` row with no Checkpoint, matching
        "invalid syntax/initial authority is rejected before Operation
        acceptance and has no checkpoint".
        """
        require_lowercase_uuid(secret_provision_operation_id, field="secret_provision_operation_id")
        require_lowercase_uuid(secret_id, field="secret_id")
        require_lowercase_uuid(
            secret_store_staging_receipt_id, field="secret_store_staging_receipt_id"
        )
        require_lowercase_uuid(
            secret_integrity_attestation_id, field="secret_integrity_attestation_id"
        )
        enums.parse_enum("secret_provision_operation.mode", mode)
        enums.parse_enum("secret_provision_operation.purpose", purpose)
        enums.parse_enum("secret_provision_operation.owner_scope_kind", owner_scope_kind)
        _require_digest(authorization_context_digest, field="authorization_context_digest")
        if expected_prior_version is not None:
            _require_positive_int(expected_prior_version, field="expected_prior_version")
        target_version = 1 if expected_prior_version is None else expected_prior_version + 1

        req_digest = self._secret_provision_request_digest(
            mode=mode,
            secret_id=secret_id,
            expected_prior_version=expected_prior_version,
            target_version=target_version,
            purpose=purpose,
            owner_scope_kind=owner_scope_kind,
            owner_scope_id=owner_scope_id,
            provider_account_ref=provider_account_ref,
            authenticated_principal_id=authenticated_principal_id,
            authorization_context_digest=authorization_context_digest,
            secret_store_staging_receipt_id=secret_store_staging_receipt_id,
            secret_integrity_attestation_id=secret_integrity_attestation_id,
        )
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM secret_provision_operations WHERE secret_provision_operation_id = ?",
                (secret_provision_operation_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["request_digest"] == req_digest
                ):
                    return self._secret_provision_operation_from_row(existing, replayed=True)
                raise IdempotencyConflictError(
                    "secret provision operation id was reused with different content"
                )
            current = self.get_secret_current_version(secret_id)
            rejection = self._validate_secret_provision_operation(
                current=current,
                secret_id=secret_id,
                target_version=target_version,
                mode=mode,
                expected_prior_version=expected_prior_version,
                purpose=purpose,
                owner_scope_kind=owner_scope_kind,
                owner_scope_id=owner_scope_id,
                provider_account_ref=provider_account_ref,
                authority_revoked=authority_revoked,
            )
            now = _now_ms()
            if rejection is not None:
                http_status, body_json, resp_digest = self._secret_provision_result_response(
                    operation_id=secret_provision_operation_id,
                    secret_id=secret_id,
                    target_version=target_version,
                    state="REJECTED",
                    rejection_code=rejection,
                )
                self.conn.execute(
                    "INSERT INTO secret_provision_operations("
                    "secret_provision_operation_id, protocol_version, mode, secret_id, "
                    "expected_prior_version, target_version, purpose, owner_scope_kind, "
                    "owner_scope_id, provider_account_ref, authenticated_principal_id, "
                    "authorization_context_digest, secret_store_staging_receipt_id, "
                    "secret_integrity_attestation_id, request_digest, state, rejection_code, "
                    "new_version, credential_rotation_receipt_id, terminal_http_status, "
                    "terminal_response_json, terminal_response_digest, last_checkpoint_id, "
                    "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "'REJECTED', ?, NULL, NULL, ?, ?, ?, NULL, ?)",
                    (
                        secret_provision_operation_id,
                        SECRET_PROVISION_REQUEST_PROTOCOL,
                        mode,
                        secret_id,
                        expected_prior_version,
                        target_version,
                        purpose,
                        owner_scope_kind,
                        owner_scope_id,
                        provider_account_ref,
                        authenticated_principal_id,
                        authorization_context_digest,
                        secret_store_staging_receipt_id,
                        secret_integrity_attestation_id,
                        req_digest,
                        rejection,
                        http_status,
                        body_json,
                        resp_digest,
                        now,
                    ),
                )
            else:
                self.conn.execute(
                    "INSERT INTO secret_provision_operations("
                    "secret_provision_operation_id, protocol_version, mode, secret_id, "
                    "expected_prior_version, target_version, purpose, owner_scope_kind, "
                    "owner_scope_id, provider_account_ref, authenticated_principal_id, "
                    "authorization_context_digest, secret_store_staging_receipt_id, "
                    "secret_integrity_attestation_id, request_digest, state, rejection_code, "
                    "new_version, credential_rotation_receipt_id, terminal_http_status, "
                    "terminal_response_json, terminal_response_digest, last_checkpoint_id, "
                    "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "'PENDING', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?)",
                    (
                        secret_provision_operation_id,
                        SECRET_PROVISION_REQUEST_PROTOCOL,
                        mode,
                        secret_id,
                        expected_prior_version,
                        target_version,
                        purpose,
                        owner_scope_kind,
                        owner_scope_id,
                        provider_account_ref,
                        authenticated_principal_id,
                        authorization_context_digest,
                        secret_store_staging_receipt_id,
                        secret_integrity_attestation_id,
                        req_digest,
                        now,
                    ),
                )
                outbox_payload = {
                    "secret_provision_operation_id": secret_provision_operation_id,
                    "secret_id": secret_id,
                    "mode": mode,
                    "target_version": target_version,
                    "secret_store_staging_receipt_id": secret_store_staging_receipt_id,
                }
                self.insert_outbox(
                    outbox_id=secret_provision_operation_id,
                    source_kind="SECRET_PROVISION_OPERATION",
                    source_id=secret_provision_operation_id,
                    destination="secret-store:install",
                    protocol_version=SECRET_PROVISION_REQUEST_PROTOCOL,
                    payload_digest=request_digest(outbox_payload),
                    payload=outbox_payload,
                    next_delivery_at_ms=now,
                )
            row = self.conn.execute(
                "SELECT * FROM secret_provision_operations WHERE secret_provision_operation_id = ?",
                (secret_provision_operation_id,),
            ).fetchone()
            assert row is not None
            return self._secret_provision_operation_from_row(row, replayed=False)

    def complete_secret_provision_operation(
        self,
        *,
        secret_provision_operation_id: str,
        storage_path: str,
        secret_integrity_attestation_id: str,
    ) -> SecretProvisionOperationResult:
        """Install the frozen ``target_version`` and close the Operation.

        Must run only after the Secret Store has durably promoted the staged
        bytes into an immutable Secret Version (write-before-reference): this
        method creates the Credential Rotation Receipt and Secret Version
        rows, advances the current-version CAS pointer, appends the
        ``INSTALL_VERSION``/``SUCCEEDED`` Checkpoint, and marks the Operation
        ``COMPLETED`` -- all in one transaction. Replaying against an
        already-terminal Operation returns its stored projection unchanged.
        """
        require_lowercase_uuid(secret_provision_operation_id, field="secret_provision_operation_id")
        require_lowercase_uuid(
            secret_integrity_attestation_id, field="secret_integrity_attestation_id"
        )
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM secret_provision_operations WHERE secret_provision_operation_id = ?",
                (secret_provision_operation_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(
                    f"secret provision operation {secret_provision_operation_id!r} was not found"
                )
            if row["state"] != "PENDING":
                return self._secret_provision_operation_from_row(row, replayed=True)
            secret_id = row["secret_id"]
            target_version = row["target_version"]
            expected_prior_version = row["expected_prior_version"]
            purpose = row["purpose"]
            owner_scope_kind = row["owner_scope_kind"]
            owner_scope_id = row["owner_scope_id"]
            provider_account_ref = row["provider_account_ref"]
            now = _now_ms()

            receipt_id = str(uuid.uuid4())
            rc_digest = receipt_digest(
                {
                    "source_kind": "MANAGEMENT_PROVISION",
                    "source_id": secret_provision_operation_id,
                    "secret_id": secret_id,
                    "expected_prior_version": expected_prior_version,
                    "new_version": target_version,
                    "purpose": purpose,
                    "owner_scope_kind": owner_scope_kind,
                    "owner_scope_id": owner_scope_id,
                    "provider_account_ref": provider_account_ref,
                    "management_operation_id": secret_provision_operation_id,
                    "authenticated_principal_id": row["authenticated_principal_id"],
                    "authorization_context_digest": row["authorization_context_digest"],
                    "secret_integrity_attestation_id": secret_integrity_attestation_id,
                }
            )
            self.conn.execute(
                "INSERT INTO credential_rotation_receipts("
                "credential_rotation_receipt_id, source_kind, source_id, "
                "credential_rotation_request_id, secret_id, expected_prior_version, "
                "new_version, purpose, owner_scope_kind, owner_scope_id, "
                "provider_account_ref, attempt_id, activity_id, attempt_generation, "
                "worker_id, worker_session_id, attempt_capability_digest, "
                "launch_attestation_id, management_operation_id, "
                "authenticated_principal_id, authorization_context_digest, "
                "secret_integrity_attestation_id, receipt_digest, created_at_ms) "
                "VALUES (?, 'MANAGEMENT_PROVISION', ?, NULL, ?, ?, ?, ?, ?, ?, ?, "
                "NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?)",
                (
                    receipt_id,
                    secret_provision_operation_id,
                    secret_id,
                    expected_prior_version,
                    target_version,
                    purpose,
                    owner_scope_kind,
                    owner_scope_id,
                    provider_account_ref,
                    secret_provision_operation_id,
                    row["authenticated_principal_id"],
                    row["authorization_context_digest"],
                    secret_integrity_attestation_id,
                    rc_digest,
                    now,
                ),
            )
            self.conn.execute(
                "INSERT INTO secret_versions("
                "secret_id, version, creation_receipt_id, storage_path, "
                "affected_run_ids_digest, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    secret_id,
                    target_version,
                    receipt_id,
                    storage_path,
                    affected_run_ids_digest([]),
                    now,
                ),
            )
            if expected_prior_version is None:
                self.conn.execute(
                    "INSERT INTO secret_current_versions("
                    "secret_id, purpose, owner_scope_kind, owner_scope_id, "
                    "provider_account_ref, current_version, last_operation_id, "
                    "created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        secret_id,
                        purpose,
                        owner_scope_kind,
                        owner_scope_id,
                        provider_account_ref,
                        target_version,
                        secret_provision_operation_id,
                        now,
                        now,
                    ),
                )
            else:
                cur = self.conn.execute(
                    "UPDATE secret_current_versions SET current_version = ?, "
                    "last_operation_id = ?, updated_at_ms = ? "
                    "WHERE secret_id = ? AND current_version = ?",
                    (
                        target_version,
                        secret_provision_operation_id,
                        now,
                        secret_id,
                        expected_prior_version,
                    ),
                )
                if cur.rowcount != 1:
                    # The target_version reservation held since acceptance makes
                    # this unreachable in normal operation; treat it as a real
                    # storage inconsistency rather than a client-facing CAS_LOST.
                    raise CasMismatchError(
                        "secret current-version CAS was lost between acceptance and install"
                    )

            checkpoint_id = str(uuid.uuid4())
            checkpoint_seq = self._next_secret_provision_checkpoint_sequence(
                secret_provision_operation_id
            )
            cp_digest = checkpoint_digest(
                {
                    "secret_provision_operation_id": secret_provision_operation_id,
                    "checkpoint_sequence": checkpoint_seq,
                    "phase": "INSTALL_VERSION",
                    "outcome": "SUCCEEDED",
                }
            )
            self.conn.execute(
                "INSERT INTO secret_provision_checkpoints("
                "secret_provision_checkpoint_id, secret_provision_operation_id, "
                "checkpoint_sequence, phase, outcome, failure_code, "
                "failure_evidence_digest, next_retry_ms, checkpoint_digest, "
                "recorded_at_ms) VALUES (?, ?, ?, 'INSTALL_VERSION', 'SUCCEEDED', "
                "NULL, NULL, NULL, ?, ?)",
                (checkpoint_id, secret_provision_operation_id, checkpoint_seq, cp_digest, now),
            )

            http_status, body_json, resp_digest = self._secret_provision_result_response(
                operation_id=secret_provision_operation_id,
                secret_id=secret_id,
                target_version=target_version,
                state="COMPLETED",
                new_version=target_version,
                secret_version_key=f"{secret_id}:{target_version}",
                credential_rotation_receipt_id=receipt_id,
            )
            cur = self.conn.execute(
                "UPDATE secret_provision_operations SET state = 'COMPLETED', "
                "new_version = ?, credential_rotation_receipt_id = ?, "
                "terminal_http_status = ?, terminal_response_json = ?, "
                "terminal_response_digest = ?, last_checkpoint_id = ? "
                "WHERE secret_provision_operation_id = ? AND state = 'PENDING'",
                (
                    target_version,
                    receipt_id,
                    http_status,
                    body_json,
                    resp_digest,
                    checkpoint_id,
                    secret_provision_operation_id,
                ),
            )
            if cur.rowcount != 1:
                raise RunStoreError("secret provision operation was not PENDING at install commit")
            row = self.conn.execute(
                "SELECT * FROM secret_provision_operations WHERE secret_provision_operation_id = ?",
                (secret_provision_operation_id,),
            ).fetchone()
            assert row is not None
            return self._secret_provision_operation_from_row(row, replayed=False)

    def fail_secret_provision_operation(
        self,
        *,
        secret_provision_operation_id: str,
        rejection_code: str,
        failure_evidence_digest: str,
    ) -> SecretProvisionOperationResult:
        """Close a still-``PENDING`` Operation as ``REJECTED`` after an install failure.

        Only ``STAGED_OBJECT_INVALID`` and ``INTEGRITY_CONFLICT`` are decided
        here: ``CAS_LOST``/``AUTHORITY_REVOKED`` are decided at acceptance and
        never reach ``PENDING``. Releases the ``target_version`` reservation
        (the partial unique index only covers ``PENDING``/``COMPLETED``), so a
        corrected request may immediately reserve the same target again.
        """
        require_lowercase_uuid(secret_provision_operation_id, field="secret_provision_operation_id")
        enums.parse_enum("secret_provision_operation.rejection_code", rejection_code)
        if rejection_code not in ("STAGED_OBJECT_INVALID", "INTEGRITY_CONFLICT"):
            raise ValueError(
                "fail_secret_provision_operation only closes an install-phase "
                "rejection (STAGED_OBJECT_INVALID or INTEGRITY_CONFLICT)"
            )
        _require_digest(failure_evidence_digest, field="failure_evidence_digest")
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM secret_provision_operations WHERE secret_provision_operation_id = ?",
                (secret_provision_operation_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(
                    f"secret provision operation {secret_provision_operation_id!r} was not found"
                )
            if row["state"] != "PENDING":
                return self._secret_provision_operation_from_row(row, replayed=True)
            now = _now_ms()
            checkpoint_id = str(uuid.uuid4())
            checkpoint_seq = self._next_secret_provision_checkpoint_sequence(
                secret_provision_operation_id
            )
            cp_digest = checkpoint_digest(
                {
                    "secret_provision_operation_id": secret_provision_operation_id,
                    "checkpoint_sequence": checkpoint_seq,
                    "phase": "INSTALL_VERSION",
                    "outcome": "FAILED_TERMINAL",
                    "failure_code": rejection_code,
                    "failure_evidence_digest": failure_evidence_digest,
                }
            )
            self.conn.execute(
                "INSERT INTO secret_provision_checkpoints("
                "secret_provision_checkpoint_id, secret_provision_operation_id, "
                "checkpoint_sequence, phase, outcome, failure_code, "
                "failure_evidence_digest, next_retry_ms, checkpoint_digest, "
                "recorded_at_ms) VALUES (?, ?, ?, 'INSTALL_VERSION', 'FAILED_TERMINAL', "
                "?, ?, NULL, ?, ?)",
                (
                    checkpoint_id,
                    secret_provision_operation_id,
                    checkpoint_seq,
                    rejection_code,
                    failure_evidence_digest,
                    cp_digest,
                    now,
                ),
            )
            http_status, body_json, resp_digest = self._secret_provision_result_response(
                operation_id=secret_provision_operation_id,
                secret_id=row["secret_id"],
                target_version=row["target_version"],
                state="REJECTED",
                rejection_code=rejection_code,
            )
            cur = self.conn.execute(
                "UPDATE secret_provision_operations SET state = 'REJECTED', "
                "rejection_code = ?, terminal_http_status = ?, terminal_response_json = ?, "
                "terminal_response_digest = ?, last_checkpoint_id = ? "
                "WHERE secret_provision_operation_id = ? AND state = 'PENDING'",
                (
                    rejection_code,
                    http_status,
                    body_json,
                    resp_digest,
                    checkpoint_id,
                    secret_provision_operation_id,
                ),
            )
            if cur.rowcount != 1:
                raise RunStoreError(
                    "secret provision operation was not PENDING at rejection commit"
                )
            row = self.conn.execute(
                "SELECT * FROM secret_provision_operations WHERE secret_provision_operation_id = ?",
                (secret_provision_operation_id,),
            ).fetchone()
            assert row is not None
            return self._secret_provision_operation_from_row(row, replayed=False)

    def record_secret_provision_retry_checkpoint(
        self,
        *,
        secret_provision_operation_id: str,
        phase: str,
        failure_code: str,
        failure_evidence_digest: str,
        next_retry_ms: int,
    ) -> SecretProvisionCheckpointRecord:
        """Append a ``FAILED_RETRYABLE`` checkpoint without closing the Operation.

        Only ``SECRET_STORE_UNAVAILABLE``, ``TRANSIENT_STORAGE_ERROR``, and
        ``TRANSIENT_DATABASE_BUSY`` are valid here: terminal codes such as
        ``CAS_LOST`` are decided at acceptance or install rejection and never
        recorded as retryable.
        """
        require_lowercase_uuid(secret_provision_operation_id, field="secret_provision_operation_id")
        enums.parse_enum("secret_provision_checkpoint.phase", phase)
        enums.parse_enum("secret_provision_checkpoint.failure_code", failure_code)
        if failure_code not in (
            "SECRET_STORE_UNAVAILABLE",
            "TRANSIENT_STORAGE_ERROR",
            "TRANSIENT_DATABASE_BUSY",
        ):
            raise ValueError(
                "record_secret_provision_retry_checkpoint only records a "
                "retryable failure (SECRET_STORE_UNAVAILABLE, "
                "TRANSIENT_STORAGE_ERROR, or TRANSIENT_DATABASE_BUSY)"
            )
        _require_digest(failure_evidence_digest, field="failure_evidence_digest")
        with self.transaction():
            row = self.conn.execute(
                "SELECT state FROM secret_provision_operations "
                "WHERE secret_provision_operation_id = ?",
                (secret_provision_operation_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(
                    f"secret provision operation {secret_provision_operation_id!r} was not found"
                )
            if row["state"] != "PENDING":
                raise RunStoreError("a retry checkpoint requires a PENDING operation")
            now = _now_ms()
            checkpoint_id = str(uuid.uuid4())
            checkpoint_seq = self._next_secret_provision_checkpoint_sequence(
                secret_provision_operation_id
            )
            cp_digest = checkpoint_digest(
                {
                    "secret_provision_operation_id": secret_provision_operation_id,
                    "checkpoint_sequence": checkpoint_seq,
                    "phase": phase,
                    "outcome": "FAILED_RETRYABLE",
                    "failure_code": failure_code,
                    "failure_evidence_digest": failure_evidence_digest,
                    "next_retry_ms": next_retry_ms,
                }
            )
            self.conn.execute(
                "INSERT INTO secret_provision_checkpoints("
                "secret_provision_checkpoint_id, secret_provision_operation_id, "
                "checkpoint_sequence, phase, outcome, failure_code, "
                "failure_evidence_digest, next_retry_ms, checkpoint_digest, "
                "recorded_at_ms) VALUES (?, ?, ?, ?, 'FAILED_RETRYABLE', ?, ?, ?, ?, ?)",
                (
                    checkpoint_id,
                    secret_provision_operation_id,
                    checkpoint_seq,
                    phase,
                    failure_code,
                    failure_evidence_digest,
                    next_retry_ms,
                    cp_digest,
                    now,
                ),
            )
            self.conn.execute(
                "UPDATE secret_provision_operations SET last_checkpoint_id = ? "
                "WHERE secret_provision_operation_id = ?",
                (checkpoint_id, secret_provision_operation_id),
            )
            row = self.conn.execute(
                "SELECT * FROM secret_provision_checkpoints "
                "WHERE secret_provision_checkpoint_id = ?",
                (checkpoint_id,),
            ).fetchone()
            assert row is not None
            return _row_to_secret_provision_checkpoint(row)

    def get_forge_instance(self, forge_instance_id: str) -> ForgeInstanceRecord | None:
        require_lowercase_uuid(forge_instance_id, field="forge_instance_id")
        row = self.conn.execute(
            "SELECT * FROM forge_instances WHERE forge_instance_id = ?", (forge_instance_id,)
        ).fetchone()
        return None if row is None else _row_to_forge_instance(row)

    def get_forge_instance_by_origin(self, canonical_origin: str) -> ForgeInstanceRecord | None:
        row = self.conn.execute(
            "SELECT * FROM forge_instances WHERE canonical_origin = ?", (canonical_origin,)
        ).fetchone()
        return None if row is None else _row_to_forge_instance(row)

    def get_project(self, project_id: str) -> ProjectRecord | None:
        require_lowercase_uuid(project_id, field="project_id")
        row = self.conn.execute(
            "SELECT * FROM projects WHERE project_id = ?", (project_id,)
        ).fetchone()
        return None if row is None else _row_to_project(row)

    def get_project_by_repository(
        self, *, forge_instance_id: str, repository_external_id: str
    ) -> ProjectRecord | None:
        require_lowercase_uuid(forge_instance_id, field="forge_instance_id")
        row = self.conn.execute(
            "SELECT * FROM projects WHERE forge_instance_id = ? AND repository_external_id = ?",
            (forge_instance_id, repository_external_id),
        ).fetchone()
        return None if row is None else _row_to_project(row)

    def get_forge_observation_schedule(
        self, forge_observation_schedule_id: str
    ) -> ForgeObservationScheduleRecord | None:
        require_lowercase_uuid(forge_observation_schedule_id, field="forge_observation_schedule_id")
        row = self.conn.execute(
            "SELECT * FROM forge_observation_schedules WHERE forge_observation_schedule_id = ?",
            (forge_observation_schedule_id,),
        ).fetchone()
        return None if row is None else _row_to_forge_observation_schedule(row)

    def get_forge_observation_schedule_by_identity(
        self,
        *,
        project_id: str,
        schedule_kind: str,
        target_kind: str,
        target_id: str,
        run_id: str | None = None,
        publication_id: str | None = None,
        terminal_duplicate_cleanup_reservation_id: str | None = None,
    ) -> ForgeObservationScheduleRecord | None:
        """Return the one non-``CLOSED`` Schedule for this null-normalized identity, if any."""
        identity_key = "|".join(
            [
                project_id,
                schedule_kind,
                target_kind,
                target_id,
                run_id or "",
                publication_id or "",
                terminal_duplicate_cleanup_reservation_id or "",
            ]
        )
        row = self.conn.execute(
            "SELECT * FROM forge_observation_schedules "
            "WHERE identity_key = ? AND state != 'CLOSED'",
            (identity_key,),
        ).fetchone()
        return None if row is None else _row_to_forge_observation_schedule(row)

    def create_forge_observation_schedule(
        self,
        *,
        forge_observation_schedule_id: str,
        schedule_kind: str,
        project_id: str,
        forge_instance_id: str,
        target_kind: str,
        target_id: str,
        minimum_interval_ms: int,
        next_due_at_ms: int,
        run_id: str | None = None,
        publication_id: str | None = None,
        terminal_duplicate_cleanup_reservation_id: str | None = None,
        initial_state: str = "ACTIVE",
    ) -> ForgeObservationScheduleRecord:
        """Create the durable Schedule for this identity at revision 0.

        Idempotent: an existing non-``CLOSED`` Schedule for the same
        null-normalized identity is returned unchanged rather than recreated
        or reactivated (the CAS-reuse rule discovery completion and every
        other Schedule-creating Transition relies on).
        """
        with self.transaction():
            return self._create_or_reuse_forge_observation_schedule(
                forge_observation_schedule_id=forge_observation_schedule_id,
                schedule_kind=schedule_kind,
                project_id=project_id,
                forge_instance_id=forge_instance_id,
                target_kind=target_kind,
                target_id=target_id,
                minimum_interval_ms=minimum_interval_ms,
                next_due_at_ms=next_due_at_ms,
                run_id=run_id,
                publication_id=publication_id,
                terminal_duplicate_cleanup_reservation_id=terminal_duplicate_cleanup_reservation_id,
                initial_state=initial_state,
            )

    def _create_or_reuse_forge_observation_schedule(
        self,
        *,
        forge_observation_schedule_id: str,
        schedule_kind: str,
        project_id: str,
        forge_instance_id: str,
        target_kind: str,
        target_id: str,
        minimum_interval_ms: int,
        next_due_at_ms: int,
        run_id: str | None = None,
        publication_id: str | None = None,
        terminal_duplicate_cleanup_reservation_id: str | None = None,
        initial_state: str = "ACTIVE",
    ) -> ForgeObservationScheduleRecord:
        """Transaction-free body of :meth:`create_forge_observation_schedule`.

        Called both by that public entry point (which opens the transaction)
        and by :meth:`complete_work_item_discovery_request`, which is already
        inside its own writer transaction when it creates child Schedules.
        """
        require_lowercase_uuid(forge_observation_schedule_id, field="forge_observation_schedule_id")
        require_lowercase_uuid(project_id, field="project_id")
        require_lowercase_uuid(forge_instance_id, field="forge_instance_id")
        enums.parse_enum("forge_observation_schedule.schedule_kind", schedule_kind)
        enums.parse_enum("forge_observation.target_kind", target_kind)
        enums.parse_enum("forge_observation_schedule.state", initial_state)
        if initial_state == "CLOSED":
            raise ValueError("a Forge Observation Schedule cannot be created already CLOSED")
        if minimum_interval_ms <= 0:
            raise ValueError("minimum_interval_ms must be positive")
        existing = self.get_forge_observation_schedule_by_identity(
            project_id=project_id,
            schedule_kind=schedule_kind,
            target_kind=target_kind,
            target_id=target_id,
            run_id=run_id,
            publication_id=publication_id,
            terminal_duplicate_cleanup_reservation_id=terminal_duplicate_cleanup_reservation_id,
        )
        if existing is not None:
            return existing
        digest = forge_observation_schedule_digest(
            _forge_observation_schedule_digest_fields(
                schedule_kind=schedule_kind,
                project_id=project_id,
                forge_instance_id=forge_instance_id,
                target_kind=target_kind,
                target_id=target_id,
                run_id=run_id,
                publication_id=publication_id,
                terminal_duplicate_cleanup_reservation_id=terminal_duplicate_cleanup_reservation_id,
                minimum_interval_ms=minimum_interval_ms,
            )
        )
        now = _now_ms()
        try:
            self.conn.execute(
                "INSERT INTO forge_observation_schedules("
                "forge_observation_schedule_id, schedule_kind, project_id, "
                "forge_instance_id, target_kind, target_id, run_id, publication_id, "
                "terminal_duplicate_cleanup_reservation_id, minimum_interval_ms, "
                "next_due_at_ms, schedule_revision, last_request_id, "
                "last_discovery_search_revision, last_discovery_set_digest, state, "
                "schedule_digest, created_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, ?, ?, ?)",
                (
                    forge_observation_schedule_id,
                    schedule_kind,
                    project_id,
                    forge_instance_id,
                    target_kind,
                    target_id,
                    run_id,
                    publication_id,
                    terminal_duplicate_cleanup_reservation_id,
                    minimum_interval_ms,
                    next_due_at_ms,
                    initial_state,
                    digest,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise CasMismatchError(
                "a non-closed Forge Observation Schedule already exists for this identity"
            ) from exc
        row = self.conn.execute(
            "SELECT * FROM forge_observation_schedules WHERE forge_observation_schedule_id = ?",
            (forge_observation_schedule_id,),
        ).fetchone()
        assert row is not None
        return _row_to_forge_observation_schedule(row)

    def close_forge_observation_schedule(
        self, forge_observation_schedule_id: str, *, expected_revision: int
    ) -> ForgeObservationScheduleRecord:
        """CAS a Schedule to ``CLOSED``, superseding any still-``PENDING`` Request
        and its still-``PENDING`` reciprocal Outbox before any further I/O.

        Idempotent: an already-``CLOSED`` Schedule is returned unchanged.
        """
        require_lowercase_uuid(forge_observation_schedule_id, field="forge_observation_schedule_id")
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM forge_observation_schedules WHERE forge_observation_schedule_id = ?",
                (forge_observation_schedule_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(
                    f"forge observation schedule {forge_observation_schedule_id!r} was not found"
                )
            current = _row_to_forge_observation_schedule(row)
            if current.state == "CLOSED":
                return current
            if current.schedule_revision != expected_revision:
                raise CasMismatchError("forge_observation_schedule_id revision changed")
            updated = self.conn.execute(
                "UPDATE forge_observation_schedules SET state = 'CLOSED', "
                "schedule_revision = schedule_revision + 1 "
                "WHERE forge_observation_schedule_id = ? AND schedule_revision = ?",
                (forge_observation_schedule_id, expected_revision),
            )
            if updated.rowcount != 1:
                raise CasMismatchError("forge_observation_schedule_id revision changed")
            pending = self.conn.execute(
                "SELECT * FROM forge_observation_requests "
                "WHERE forge_observation_schedule_id = ? AND state = 'PENDING'",
                (forge_observation_schedule_id,),
            ).fetchone()
            if pending is not None:
                self._supersede_pending_request_before_io(pending)
        row = self.conn.execute(
            "SELECT * FROM forge_observation_schedules WHERE forge_observation_schedule_id = ?",
            (forge_observation_schedule_id,),
        ).fetchone()
        assert row is not None
        return _row_to_forge_observation_schedule(row)

    def _supersede_pending_request_before_io(self, request_row: sqlite3.Row) -> None:
        now = _now_ms()
        empty_digest = forge_observation_result_membership_digest([])
        self.conn.execute(
            "UPDATE forge_observation_requests SET state = 'SUPERSEDED', "
            "result_observation_ids_digest = ?, completed_at_ms = ? "
            "WHERE forge_observation_request_id = ? AND state = 'PENDING'",
            (empty_digest, now, request_row["forge_observation_request_id"]),
        )
        self._mark_outbox_superseded(request_row["outbox_id"])

    def _mark_outbox_delivered(self, outbox_id: str) -> None:
        self.conn.execute(
            "UPDATE outbox SET state = 'DELIVERED', delivery_count = delivery_count + 1 "
            "WHERE outbox_id = ? AND state != 'DELIVERED'",
            (outbox_id,),
        )

    def _mark_outbox_superseded(self, outbox_id: str) -> None:
        self.conn.execute(
            "UPDATE outbox SET state = 'SUPERSEDED' WHERE outbox_id = ? AND state = 'PENDING'",
            (outbox_id,),
        )

    def create_due_forge_observation_request(
        self,
        *,
        forge_observation_request_id: str,
        forge_observation_schedule_id: str,
        now_ms: int,
        controller_mode: str,
        controller_mode_revision: int,
        credential_purpose: str,
        credential_secret_id: str,
        credential_secret_version: int,
        outbox_id: str,
        outbox_destination: str = "forge-observation-dispatch/1",
        expected_prior_observation_sequence: int | None = None,
        expected_external_revision: str | None = None,
        controller_activity_id: str | None = None,
        effect_generation: int | None = None,
        controller_operation_digest: str | None = None,
        terminal_duplicate_cleanup_action_id: str | None = None,
        terminal_cleanup_operation_digest: str | None = None,
    ) -> ForgeObservationRequestRecord | None:
        """Create the next due Request for an ``ACTIVE`` due Schedule.

        Returns the existing ``PENDING`` Request unchanged when one is already
        outstanding (the "no existing PENDING Request" precondition makes this
        idempotent rather than an error), or ``None`` when the Schedule is not
        currently ``ACTIVE``-and-due or the controller is in ``MAINTENANCE``
        (which creates no ordinary Request). Commits the Request and its
        reciprocal Outbox in the same writer transaction as the Schedule CAS,
        before any forge I/O.
        """
        require_lowercase_uuid(forge_observation_request_id, field="forge_observation_request_id")
        require_lowercase_uuid(outbox_id, field="outbox_id")
        enums.parse_enum("controller_mode.mode", controller_mode)
        if controller_mode == "MAINTENANCE":
            return None
        enums.parse_enum("forge_observation_request.credential_purpose", credential_purpose)
        if credential_purpose == "FORGE_CONNECTIVITY":
            raise ValueError("FORGE_CONNECTIVITY credentials belong only to Health Probe Request")
        protocol_version = FORGE_OBSERVATION_REQUEST_PROTOCOL
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM forge_observation_schedules WHERE forge_observation_schedule_id = ?",
                (forge_observation_schedule_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(
                    f"forge observation schedule {forge_observation_schedule_id!r} was not found"
                )
            schedule = _row_to_forge_observation_schedule(row)
            if schedule.state != "ACTIVE":
                return None
            pending = self.conn.execute(
                "SELECT * FROM forge_observation_requests "
                "WHERE forge_observation_schedule_id = ? AND state = 'PENDING'",
                (forge_observation_schedule_id,),
            ).fetchone()
            if pending is not None:
                # A retried create call (e.g. after a crash before the caller
                # observed success) returns the still-outstanding Request
                # regardless of due time -- due time only gates *new* work.
                return _row_to_forge_observation_request(pending)
            if schedule.next_due_at_ms > now_ms:
                return None
            next_sequence = self.conn.execute(
                "SELECT COALESCE(MAX(request_sequence), 0) + 1 FROM forge_observation_requests "
                "WHERE forge_observation_schedule_id = ?",
                (forge_observation_schedule_id,),
            ).fetchone()[0]
            request_kind = schedule.schedule_kind
            expected_discovery_search_revision = None
            expected_discovery_set_digest = None
            if request_kind == "WORK_ITEM_DISCOVERY":
                expected_discovery_search_revision = schedule.last_discovery_search_revision
                expected_discovery_set_digest = schedule.last_discovery_set_digest
                expected_prior_observation_sequence = None
                expected_external_revision = None
            digest = request_digest(
                {
                    "protocol_version": protocol_version,
                    "forge_observation_schedule_id": forge_observation_schedule_id,
                    "schedule_revision": schedule.schedule_revision,
                    "request_sequence": next_sequence,
                    "request_kind": request_kind,
                    "project_id": schedule.project_id,
                    "forge_instance_id": schedule.forge_instance_id,
                    "target_kind": schedule.target_kind,
                    "target_id": schedule.target_id,
                    "run_id": schedule.run_id,
                    "publication_id": schedule.publication_id,
                    "terminal_duplicate_cleanup_reservation_id": (
                        schedule.terminal_duplicate_cleanup_reservation_id
                    ),
                    "created_under_controller_mode_revision": controller_mode_revision,
                    "created_under_controller_mode": controller_mode,
                    "credential_purpose": credential_purpose,
                    "credential_secret_id": credential_secret_id,
                    "credential_secret_version": credential_secret_version,
                    "controller_activity_id": controller_activity_id,
                    "effect_generation": effect_generation,
                    "controller_operation_digest": controller_operation_digest,
                    "terminal_duplicate_cleanup_action_id": terminal_duplicate_cleanup_action_id,
                    "terminal_cleanup_operation_digest": terminal_cleanup_operation_digest,
                    "expected_prior_observation_sequence": expected_prior_observation_sequence,
                    "expected_external_revision": expected_external_revision,
                    "expected_discovery_search_revision": expected_discovery_search_revision,
                    "expected_discovery_set_digest": expected_discovery_set_digest,
                }
            )
            updated = self.conn.execute(
                "UPDATE forge_observation_schedules SET last_request_id = ?, "
                "next_due_at_ms = ?, schedule_revision = schedule_revision + 1 "
                "WHERE forge_observation_schedule_id = ? AND schedule_revision = ? "
                "AND state = 'ACTIVE'",
                (
                    forge_observation_request_id,
                    now_ms + schedule.minimum_interval_ms,
                    forge_observation_schedule_id,
                    schedule.schedule_revision,
                ),
            )
            if updated.rowcount != 1:
                raise CasMismatchError("forge_observation_schedule_id revision changed")
            self.insert_outbox(
                outbox_id=outbox_id,
                source_kind="FORGE_OBSERVATION_REQUEST",
                source_id=forge_observation_request_id,
                destination=outbox_destination,
                protocol_version=protocol_version,
                payload_digest=digest,
                payload={
                    "forge_observation_request_id": forge_observation_request_id,
                    "request_kind": request_kind,
                    "project_id": schedule.project_id,
                    "forge_instance_id": schedule.forge_instance_id,
                    "target_kind": schedule.target_kind,
                    "target_id": schedule.target_id,
                },
                next_delivery_at_ms=now_ms,
                publication_id=schedule.publication_id,
                effect_generation=effect_generation,
            )
            self.conn.execute(
                "INSERT INTO forge_observation_requests("
                "forge_observation_request_id, protocol_version, "
                "forge_observation_schedule_id, schedule_revision, request_sequence, "
                "request_kind, project_id, forge_instance_id, target_kind, target_id, "
                "run_id, publication_id, terminal_duplicate_cleanup_reservation_id, "
                "created_under_controller_mode_revision, created_under_controller_mode, "
                "credential_purpose, credential_secret_id, credential_secret_version, "
                "controller_activity_id, effect_generation, controller_operation_digest, "
                "terminal_duplicate_cleanup_action_id, terminal_cleanup_operation_digest, "
                "expected_prior_observation_sequence, expected_external_revision, "
                "expected_discovery_search_revision, expected_discovery_set_digest, "
                "request_idempotency_key, request_digest, state, outbox_id, "
                "next_attempt_ordinal, last_failure_fact_id, next_retry_ms, "
                "result_observation_ids_digest, result_discovery_search_revision, "
                "result_discovery_set_digest, created_at_ms, completed_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, 'PENDING', ?, 1, NULL, NULL, NULL, NULL, NULL, ?, NULL)",
                (
                    forge_observation_request_id,
                    protocol_version,
                    forge_observation_schedule_id,
                    schedule.schedule_revision,
                    next_sequence,
                    request_kind,
                    schedule.project_id,
                    schedule.forge_instance_id,
                    schedule.target_kind,
                    schedule.target_id,
                    schedule.run_id,
                    schedule.publication_id,
                    schedule.terminal_duplicate_cleanup_reservation_id,
                    controller_mode_revision,
                    controller_mode,
                    credential_purpose,
                    credential_secret_id,
                    credential_secret_version,
                    controller_activity_id,
                    effect_generation,
                    controller_operation_digest,
                    terminal_duplicate_cleanup_action_id,
                    terminal_cleanup_operation_digest,
                    expected_prior_observation_sequence,
                    expected_external_revision,
                    expected_discovery_search_revision,
                    expected_discovery_set_digest,
                    forge_observation_request_id,
                    digest,
                    outbox_id,
                    now_ms,
                ),
            )
        row = self.conn.execute(
            "SELECT * FROM forge_observation_requests WHERE forge_observation_request_id = ?",
            (forge_observation_request_id,),
        ).fetchone()
        assert row is not None
        return _row_to_forge_observation_request(row)

    def record_forge_observation_request_attempt(self, forge_observation_request_id: str) -> int:
        """Commit and return the Request's next outbound transport-attempt ordinal.

        Must be called once, before each real outbound adapter attempt, per
        the closed error taxonomy's "the writer commits the Request's next
        attempt ordinal" pre-I/O rule.
        """
        require_lowercase_uuid(forge_observation_request_id, field="forge_observation_request_id")
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM forge_observation_requests WHERE forge_observation_request_id = ?",
                (forge_observation_request_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(
                    f"forge observation request {forge_observation_request_id!r} was not found"
                )
            if row["state"] != "PENDING":
                raise CasMismatchError("forge_observation_request_id is no longer PENDING")
            ordinal = row["next_attempt_ordinal"]
            updated = self.conn.execute(
                "UPDATE forge_observation_requests SET next_attempt_ordinal = "
                "next_attempt_ordinal + 1 "
                "WHERE forge_observation_request_id = ? AND state = 'PENDING' "
                "AND next_attempt_ordinal = ?",
                (forge_observation_request_id, ordinal),
            )
            if updated.rowcount != 1:
                raise CasMismatchError("forge_observation_request_id attempt ordinal changed")
        return ordinal

    def record_forge_request_failure_fact(
        self,
        *,
        forge_request_failure_fact_id: str,
        forge_observation_request_id: str,
        request_attempt_ordinal: int,
        failure_kind: str,
        failure_code: str,
        failure_evidence_digest: str,
        retry_not_before_ms: int,
    ) -> ForgeRequestFailureFactRecord:
        """Record one failed transport attempt for a still-``PENDING`` Request.

        Idempotent on ``(forge_observation_request_id, request_attempt_ordinal)``:
        an exact retry with identical content returns the existing Fact.
        Leaves the Request and its reciprocal Outbox ``PENDING``. A failure
        Fact for a Request that already won its terminal-state CAS
        (``COMPLETED``/``SUPERSEDED``) is rejected as late.
        """
        require_lowercase_uuid(forge_request_failure_fact_id, field="forge_request_failure_fact_id")
        enums.parse_enum("forge_request_failure_fact.failure_kind", failure_kind)
        _require_digest(failure_evidence_digest, field="failure_evidence_digest")
        with self.transaction():
            req_row = self.conn.execute(
                "SELECT * FROM forge_observation_requests WHERE forge_observation_request_id = ?",
                (forge_observation_request_id,),
            ).fetchone()
            if req_row is None:
                raise RunStoreError(
                    f"forge observation request {forge_observation_request_id!r} was not found"
                )
            existing = self.conn.execute(
                "SELECT * FROM forge_request_failure_facts "
                "WHERE forge_observation_request_id = ? AND request_attempt_ordinal = ?",
                (forge_observation_request_id, request_attempt_ordinal),
            ).fetchone()
            fact_fields = {
                "forge_observation_request_id": forge_observation_request_id,
                "request_attempt_ordinal": request_attempt_ordinal,
                "project_id": req_row["project_id"],
                "run_id": req_row["run_id"],
                "publication_id": req_row["publication_id"],
                "terminal_duplicate_cleanup_reservation_id": req_row[
                    "terminal_duplicate_cleanup_reservation_id"
                ],
                "failure_kind": failure_kind,
                "failure_code": failure_code,
                "failure_evidence_digest": failure_evidence_digest,
                "retry_not_before_ms": retry_not_before_ms,
                "request_digest": req_row["request_digest"],
            }
            fact_digest = forge_request_failure_fact_digest(fact_fields)
            if existing is not None:
                if existing["fact_digest"] != fact_digest:
                    raise IdempotencyConflictError(
                        "forge_request_failure_fact_id attempt was reused with different content"
                    )
                return _row_to_forge_request_failure_fact(existing)
            if req_row["state"] != "PENDING":
                raise CasMismatchError(
                    "forge_observation_request_id already reached a terminal state; "
                    "rejecting a late failure Fact"
                )
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO forge_request_failure_facts("
                "forge_request_failure_fact_id, forge_observation_request_id, "
                "request_attempt_ordinal, project_id, run_id, publication_id, "
                "terminal_duplicate_cleanup_reservation_id, failure_kind, failure_code, "
                "failure_evidence_digest, retry_not_before_ms, request_digest, fact_digest, "
                "recorded_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    forge_request_failure_fact_id,
                    forge_observation_request_id,
                    request_attempt_ordinal,
                    req_row["project_id"],
                    req_row["run_id"],
                    req_row["publication_id"],
                    req_row["terminal_duplicate_cleanup_reservation_id"],
                    failure_kind,
                    failure_code,
                    failure_evidence_digest,
                    retry_not_before_ms,
                    req_row["request_digest"],
                    fact_digest,
                    now,
                ),
            )
            self.conn.execute(
                "UPDATE forge_observation_requests SET last_failure_fact_id = ?, "
                "next_retry_ms = ? "
                "WHERE forge_observation_request_id = ? AND state = 'PENDING'",
                (forge_request_failure_fact_id, retry_not_before_ms, forge_observation_request_id),
            )
        row = self.conn.execute(
            "SELECT * FROM forge_request_failure_facts WHERE forge_request_failure_fact_id = ?",
            (forge_request_failure_fact_id,),
        ).fetchone()
        assert row is not None
        return _row_to_forge_request_failure_fact(row)

    def _insert_or_coalesce_observation(
        self,
        *,
        forge_observation_id_factory: Callable[[], str],
        project_id: str,
        target_kind: str,
        target_id: str,
        run_id: str | None,
        publication_id: str | None,
        created_by_forge_observation_request_id: str | None,
        credential_purpose: str | None,
        credential_secret_id: str | None,
        credential_secret_version: int | None,
        obs: ForgeObservationInput,
        now: int,
    ) -> tuple[str, str]:
        """Insert one new Forge Observation, or reuse an eligible coalesced row.

        Returns ``(forge_observation_id, payload_digest)``. Idempotent replay
        by ``adapter_event_id`` is checked first; absent that, a payload
        identical to the immediately preceding observation for this exact
        target is coalesced (never across an intervening different payload,
        so an ``A -> B -> A`` sequence still records all three).
        """
        enums.parse_enum("forge_observation.kind", obs.kind)
        fact_json = _require_json_text(obs.fact)
        payload_digest = forge_observation_payload_digest(
            {
                "kind": obs.kind,
                "target_kind": target_kind,
                "target_id": target_id,
                "external_revision": obs.external_revision,
                "fact": json.loads(fact_json),
            }
        )
        if obs.adapter_event_id is not None:
            existing = self.conn.execute(
                "SELECT * FROM forge_observations WHERE project_id = ? AND target_kind = ? "
                "AND target_id = ? AND adapter_event_id = ?",
                (project_id, target_kind, target_id, obs.adapter_event_id),
            ).fetchone()
            if existing is not None:
                if existing["payload_digest"] != payload_digest:
                    raise IdempotencyConflictError(
                        "adapter_event_id was replayed with a different observation payload"
                    )
                return existing["forge_observation_id"], existing["payload_digest"]
        else:
            latest = self.conn.execute(
                "SELECT * FROM forge_observations WHERE project_id = ? AND target_kind = ? "
                "AND target_id = ? ORDER BY observation_sequence DESC LIMIT 1",
                (project_id, target_kind, target_id),
            ).fetchone()
            if (
                latest is not None
                and latest["payload_digest"] == payload_digest
                and latest["run_id"] == run_id
                and latest["publication_id"] == publication_id
                and latest["credential_purpose"] == credential_purpose
                and latest["credential_secret_id"] == credential_secret_id
                and latest["credential_secret_version"] == credential_secret_version
            ):
                return latest["forge_observation_id"], latest["payload_digest"]
        next_sequence = self.conn.execute(
            "SELECT COALESCE(MAX(observation_sequence), 0) + 1 FROM forge_observations "
            "WHERE project_id = ? AND target_kind = ? AND target_id = ?",
            (project_id, target_kind, target_id),
        ).fetchone()[0]
        forge_observation_id = forge_observation_id_factory()
        require_lowercase_uuid(forge_observation_id, field="forge_observation_id")
        observed_at_ms = obs.observed_at_ms if obs.observed_at_ms is not None else now
        self.conn.execute(
            "INSERT INTO forge_observations("
            "forge_observation_id, project_id, target_kind, target_id, run_id, "
            "publication_id, created_by_forge_observation_request_id, credential_purpose, "
            "credential_secret_id, credential_secret_version, publication_effect_generation, "
            "controller_activity_id, controller_operation_digest, "
            "terminal_duplicate_cleanup_reservation_id, terminal_duplicate_cleanup_action_id, "
            "terminal_cleanup_operation_digest, kind, external_revision, adapter_event_id, "
            "actor_principal_id, actor_authorization_digest, fact_json, payload_digest, "
            "observation_sequence, observed_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?)",
            (
                forge_observation_id,
                project_id,
                target_kind,
                target_id,
                run_id,
                publication_id,
                created_by_forge_observation_request_id,
                credential_purpose,
                credential_secret_id,
                credential_secret_version,
                obs.kind,
                obs.external_revision,
                obs.adapter_event_id,
                obs.actor_principal_id,
                obs.actor_authorization_digest,
                fact_json,
                payload_digest,
                next_sequence,
                observed_at_ms,
            ),
        )
        return forge_observation_id, payload_digest

    def complete_forge_observation_request(
        self,
        *,
        forge_observation_request_id: str,
        observations: Sequence[ForgeObservationInput],
        forge_observation_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> ForgeObservationRequestCompletion:
        """Complete a non-``WORK_ITEM_DISCOVERY`` Request from a real adapter response.

        Commits the ordered Observation result (coalescing per
        ``_insert_or_coalesce_observation``), marks the Request ``COMPLETED``,
        and marks its reciprocal Outbox ``DELIVERED`` -- all in one writer
        transaction. A stale schedule-scope fence (the Schedule ``CLOSED`` or
        no longer naming this Request as current) instead records the Request
        ``SUPERSEDED`` with no Observations, but the Outbox is still marked
        ``DELIVERED`` because real adapter I/O produced this response. A
        response for an already-terminal Request (a duplicate delivery, or one
        that raced a schedule closure) redelivers the Outbox without
        regressing the Request's own terminal state.
        """
        require_lowercase_uuid(forge_observation_request_id, field="forge_observation_request_id")
        with self.transaction():
            req_row = self.conn.execute(
                "SELECT * FROM forge_observation_requests WHERE forge_observation_request_id = ?",
                (forge_observation_request_id,),
            ).fetchone()
            if req_row is None:
                raise RunStoreError(
                    f"forge observation request {forge_observation_request_id!r} was not found"
                )
            request = _row_to_forge_observation_request(req_row)
            if request.request_kind == "WORK_ITEM_DISCOVERY":
                raise ValueError("use complete_work_item_discovery_request for WORK_ITEM_DISCOVERY")
            if request.state != "PENDING":
                self._mark_outbox_delivered(request.outbox_id)
                observation_ids = tuple(
                    r["forge_observation_id"]
                    for r in self.conn.execute(
                        "SELECT forge_observation_id FROM forge_observation_request_results "
                        "WHERE forge_observation_request_id = ? ORDER BY observation_ordinal",
                        (forge_observation_request_id,),
                    )
                )
                return ForgeObservationRequestCompletion(
                    request=self._current_forge_observation_request(forge_observation_request_id),
                    observation_ids=observation_ids,
                )
            schedule_row = self.conn.execute(
                "SELECT * FROM forge_observation_schedules WHERE forge_observation_schedule_id = ?",
                (request.forge_observation_schedule_id,),
            ).fetchone()
            assert schedule_row is not None
            schedule = _row_to_forge_observation_schedule(schedule_row)
            stale = (
                schedule.state == "CLOSED"
                or schedule.last_request_id != forge_observation_request_id
            )
            now = _now_ms()
            if stale:
                empty_digest = forge_observation_result_membership_digest([])
                self.conn.execute(
                    "UPDATE forge_observation_requests SET state = 'SUPERSEDED', "
                    "result_observation_ids_digest = ?, completed_at_ms = ? "
                    "WHERE forge_observation_request_id = ? AND state = 'PENDING'",
                    (empty_digest, now, forge_observation_request_id),
                )
                self._mark_outbox_delivered(request.outbox_id)
                observation_ids = ()
            else:
                observation_ids = tuple(
                    self._insert_or_coalesce_observation(
                        forge_observation_id_factory=forge_observation_id_factory,
                        project_id=request.project_id,
                        target_kind=request.target_kind,
                        target_id=request.target_id,
                        run_id=request.run_id,
                        publication_id=request.publication_id,
                        created_by_forge_observation_request_id=forge_observation_request_id,
                        credential_purpose=request.credential_purpose,
                        credential_secret_id=request.credential_secret_id,
                        credential_secret_version=request.credential_secret_version,
                        obs=obs,
                        now=now,
                    )[0]
                    for obs in observations
                )
                for ordinal, forge_observation_id in enumerate(observation_ids):
                    self.conn.execute(
                        "INSERT OR IGNORE INTO forge_observation_request_results("
                        "forge_observation_request_id, observation_ordinal, "
                        "forge_observation_id) VALUES (?, ?, ?)",
                        (forge_observation_request_id, ordinal, forge_observation_id),
                    )
                result_digest = forge_observation_result_membership_digest(observation_ids)
                self.conn.execute(
                    "UPDATE forge_observation_requests SET state = 'COMPLETED', "
                    "result_observation_ids_digest = ?, completed_at_ms = ? "
                    "WHERE forge_observation_request_id = ? AND state = 'PENDING'",
                    (result_digest, now, forge_observation_request_id),
                )
                self._mark_outbox_delivered(request.outbox_id)
        return ForgeObservationRequestCompletion(
            request=self._current_forge_observation_request(forge_observation_request_id),
            observation_ids=observation_ids,
        )

    def _current_forge_observation_request(
        self, forge_observation_request_id: str
    ) -> ForgeObservationRequestRecord:
        row = self.conn.execute(
            "SELECT * FROM forge_observation_requests WHERE forge_observation_request_id = ?",
            (forge_observation_request_id,),
        ).fetchone()
        assert row is not None
        return _row_to_forge_observation_request(row)

    def complete_work_item_discovery_request(
        self,
        *,
        forge_observation_request_id: str,
        discovery_search_revision: str,
        work_items: Sequence[ForgeObservationInput],
        forge_observation_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
        child_schedule_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
        work_item_poll_minimum_interval_ms: int = _DEFAULT_DISCOVERY_INTERVAL_MS,
        base_head_poll_minimum_interval_ms: int = _DEFAULT_DISCOVERY_INTERVAL_MS,
    ) -> ForgeObservationRequestCompletion:
        """Complete a ``WORK_ITEM_DISCOVERY`` Request from a real adapter response.

        ``work_items`` is normalized to bytewise Work Item stable-ID order
        before anything commits, so discovery ordering and child Schedule
        creation are deterministic regardless of adapter response order. In
        the same writer transaction: commits each item's ``WORK_ITEM_SNAPSHOT``
        Observation (coalesced as usual), CASes the Schedule's discovery
        search-revision/set-digest pair, creates-or-CAS-reuses a Run-null
        ``WORK_ITEM_POLL`` and ``BASE_HEAD_POLL`` child Schedule per item
        (``PAUSED`` when the discovery Schedule itself is ``PAUSED``, and
        never reactivated by this completion), and closes any Run-null child
        Schedule for a Work Item no longer in the discovered set that has no
        active Run. A stale schedule-scope fence behaves like
        :meth:`complete_forge_observation_request`: ``SUPERSEDED`` with no
        Observations or children, Outbox still ``DELIVERED``.
        """
        require_lowercase_uuid(forge_observation_request_id, field="forge_observation_request_id")
        ordered_items = sorted(
            work_items, key=lambda item: _require_target_id(item, field="work_items[].target_id")
        )
        with self.transaction():
            req_row = self.conn.execute(
                "SELECT * FROM forge_observation_requests WHERE forge_observation_request_id = ?",
                (forge_observation_request_id,),
            ).fetchone()
            if req_row is None:
                raise RunStoreError(
                    f"forge observation request {forge_observation_request_id!r} was not found"
                )
            request = _row_to_forge_observation_request(req_row)
            if request.request_kind != "WORK_ITEM_DISCOVERY":
                raise ValueError(
                    "complete_work_item_discovery_request only completes WORK_ITEM_DISCOVERY"
                )
            if request.state != "PENDING":
                self._mark_outbox_delivered(request.outbox_id)
                observation_ids = tuple(
                    r["forge_observation_id"]
                    for r in self.conn.execute(
                        "SELECT forge_observation_id FROM forge_observation_request_results "
                        "WHERE forge_observation_request_id = ? ORDER BY observation_ordinal",
                        (forge_observation_request_id,),
                    )
                )
                return ForgeObservationRequestCompletion(
                    request=self._current_forge_observation_request(forge_observation_request_id),
                    observation_ids=observation_ids,
                )
            schedule_row = self.conn.execute(
                "SELECT * FROM forge_observation_schedules WHERE forge_observation_schedule_id = ?",
                (request.forge_observation_schedule_id,),
            ).fetchone()
            assert schedule_row is not None
            schedule = _row_to_forge_observation_schedule(schedule_row)
            stale = (
                schedule.state == "CLOSED"
                or schedule.last_request_id != forge_observation_request_id
            )
            now = _now_ms()
            if stale:
                empty_digest = forge_observation_result_membership_digest([])
                self.conn.execute(
                    "UPDATE forge_observation_requests SET state = 'SUPERSEDED', "
                    "result_observation_ids_digest = ?, completed_at_ms = ? "
                    "WHERE forge_observation_request_id = ? AND state = 'PENDING'",
                    (empty_digest, now, forge_observation_request_id),
                )
                self._mark_outbox_delivered(request.outbox_id)
                observation_ids = ()
            else:
                observation_ids, discovery_set_digest = self._commit_discovery_results(
                    forge_observation_request_id=forge_observation_request_id,
                    request=request,
                    ordered_items=ordered_items,
                    forge_observation_id_factory=forge_observation_id_factory,
                    now=now,
                )
                result_digest = forge_observation_result_membership_digest(observation_ids)
                self.conn.execute(
                    "UPDATE forge_observation_requests SET state = 'COMPLETED', "
                    "result_observation_ids_digest = ?, result_discovery_search_revision = ?, "
                    "result_discovery_set_digest = ?, completed_at_ms = ? "
                    "WHERE forge_observation_request_id = ? AND state = 'PENDING'",
                    (
                        result_digest,
                        discovery_search_revision,
                        discovery_set_digest,
                        now,
                        forge_observation_request_id,
                    ),
                )
                updated = self.conn.execute(
                    "UPDATE forge_observation_schedules SET "
                    "last_discovery_search_revision = ?, last_discovery_set_digest = ?, "
                    "schedule_revision = schedule_revision + 1 "
                    "WHERE forge_observation_schedule_id = ? AND schedule_revision = ? "
                    "AND state != 'CLOSED' AND last_request_id = ?",
                    (
                        discovery_search_revision,
                        discovery_set_digest,
                        request.forge_observation_schedule_id,
                        schedule.schedule_revision,
                        forge_observation_request_id,
                    ),
                )
                if updated.rowcount != 1:
                    raise CasMismatchError(
                        "forge_observation_schedule_id revision changed during discovery completion"
                    )
                children_initial_state = "PAUSED" if schedule.state == "PAUSED" else "ACTIVE"
                live_target_ids: list[str] = []
                for item in ordered_items:
                    assert item.target_id is not None
                    live_target_ids.append(item.target_id)
                    self._create_or_reuse_forge_observation_schedule(
                        forge_observation_schedule_id=child_schedule_id_factory(),
                        schedule_kind="WORK_ITEM_POLL",
                        project_id=request.project_id,
                        forge_instance_id=request.forge_instance_id,
                        target_kind="WORK_ITEM",
                        target_id=item.target_id,
                        minimum_interval_ms=work_item_poll_minimum_interval_ms,
                        next_due_at_ms=now,
                        initial_state=children_initial_state,
                    )
                    self._create_or_reuse_forge_observation_schedule(
                        forge_observation_schedule_id=child_schedule_id_factory(),
                        schedule_kind="BASE_HEAD_POLL",
                        project_id=request.project_id,
                        forge_instance_id=request.forge_instance_id,
                        target_kind="WORK_ITEM",
                        target_id=item.target_id,
                        minimum_interval_ms=base_head_poll_minimum_interval_ms,
                        next_due_at_ms=now,
                        initial_state=children_initial_state,
                    )
                self._close_stale_run_null_work_item_schedules(
                    project_id=request.project_id,
                    live_target_ids=live_target_ids,
                )
                self._mark_outbox_delivered(request.outbox_id)
        return ForgeObservationRequestCompletion(
            request=self._current_forge_observation_request(forge_observation_request_id),
            observation_ids=observation_ids,
        )

    def _commit_discovery_results(
        self,
        *,
        forge_observation_request_id: str,
        request: ForgeObservationRequestRecord,
        ordered_items: Sequence[ForgeObservationInput],
        forge_observation_id_factory: Callable[[], str],
        now: int,
    ) -> tuple[tuple[str, ...], str]:
        observation_ids: list[str] = []
        set_members: list[tuple[str, str, str]] = []
        for ordinal, item in enumerate(ordered_items):
            target_id = _require_target_id(item, field="work_items[].target_id")
            forge_observation_id, payload_digest = self._insert_or_coalesce_observation(
                forge_observation_id_factory=forge_observation_id_factory,
                project_id=request.project_id,
                target_kind="WORK_ITEM",
                target_id=target_id,
                run_id=None,
                publication_id=None,
                created_by_forge_observation_request_id=forge_observation_request_id,
                credential_purpose=request.credential_purpose,
                credential_secret_id=request.credential_secret_id,
                credential_secret_version=request.credential_secret_version,
                obs=item,
                now=now,
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO forge_observation_request_results("
                "forge_observation_request_id, observation_ordinal, forge_observation_id) "
                "VALUES (?, ?, ?)",
                (forge_observation_request_id, ordinal, forge_observation_id),
            )
            observation_ids.append(forge_observation_id)
            set_members.append((target_id, item.external_revision, payload_digest))
        discovery_set_digest = work_item_discovery_set_digest(set_members)
        return tuple(observation_ids), discovery_set_digest

    def _close_stale_run_null_work_item_schedules(
        self, *, project_id: str, live_target_ids: Sequence[str]
    ) -> None:
        """Close every Run-null Work-Item-targeted Schedule outside ``live_target_ids``
        that has no active Run, using the complete discovery set as authority."""
        # "NOT IN ()" with an empty list is not valid SQL, and "NOT IN (NULL)"
        # is SQL NULL (never true) rather than "matches everything" -- an
        # empty live_target_ids (discovery now returns zero Work Items) must
        # therefore drop the membership filter entirely rather than matching
        # nothing.
        if live_target_ids:
            placeholders = ",".join("?" * len(live_target_ids))
            membership_filter = f"AND target_id NOT IN ({placeholders})"
            params: tuple[Any, ...] = (project_id, *live_target_ids)
        else:
            membership_filter = ""
            params = (project_id,)
        stale = self.conn.execute(
            "SELECT * FROM forge_observation_schedules "
            "WHERE project_id = ? AND target_kind = 'WORK_ITEM' AND run_id IS NULL "
            f"AND state != 'CLOSED' {membership_filter}",
            params,
        ).fetchall()
        for row in stale:
            active_run = self.conn.execute(
                "SELECT 1 FROM runs WHERE project_id = ? AND work_item_key = ? "
                "AND terminal_outcome IS NULL LIMIT 1",
                (project_id, row["target_id"]),
            ).fetchone()
            if active_run is not None:
                continue
            self.conn.execute(
                "UPDATE forge_observation_schedules SET state = 'CLOSED', "
                "schedule_revision = schedule_revision + 1 "
                "WHERE forge_observation_schedule_id = ? AND schedule_revision = ?",
                (row["forge_observation_schedule_id"], row["schedule_revision"]),
            )
            pending = self.conn.execute(
                "SELECT * FROM forge_observation_requests "
                "WHERE forge_observation_schedule_id = ? AND state = 'PENDING'",
                (row["forge_observation_schedule_id"],),
            ).fetchone()
            if pending is not None:
                self._supersede_pending_request_before_io(pending)

    def list_active_due_forge_observation_schedules(
        self, *, now_ms: int, limit: int = 100
    ) -> list[ForgeObservationScheduleRecord]:
        """List ``ACTIVE`` Schedules currently due, oldest-due first.

        Restart/startup reconciliation scans exactly this: every ``ACTIVE``
        due Schedule, never a synthesized one.
        """
        rows = self.conn.execute(
            "SELECT * FROM forge_observation_schedules "
            "WHERE state = 'ACTIVE' AND next_due_at_ms <= ? "
            "ORDER BY next_due_at_ms ASC, forge_observation_schedule_id ASC LIMIT ?",
            (now_ms, limit),
        ).fetchall()
        return [_row_to_forge_observation_schedule(row) for row in rows]

    def list_pending_forge_observation_requests(
        self, *, limit: int = 100
    ) -> list[ForgeObservationRequestRecord]:
        """List still-``PENDING`` Requests for restart redelivery.

        Each is retried with its same ``request_idempotency_key``; none is
        recreated.
        """
        rows = self.conn.execute(
            "SELECT * FROM forge_observation_requests WHERE state = 'PENDING' "
            "ORDER BY created_at_ms ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_forge_observation_request(row) for row in rows]

    def get_forge_observation_request(
        self, forge_observation_request_id: str
    ) -> ForgeObservationRequestRecord | None:
        require_lowercase_uuid(forge_observation_request_id, field="forge_observation_request_id")
        row = self.conn.execute(
            "SELECT * FROM forge_observation_requests WHERE forge_observation_request_id = ?",
            (forge_observation_request_id,),
        ).fetchone()
        return None if row is None else _row_to_forge_observation_request(row)

    def get_forge_observation(self, forge_observation_id: str) -> ForgeObservationRecord | None:
        require_lowercase_uuid(forge_observation_id, field="forge_observation_id")
        row = self.conn.execute(
            "SELECT * FROM forge_observations WHERE forge_observation_id = ?",
            (forge_observation_id,),
        ).fetchone()
        return None if row is None else _row_to_forge_observation(row)

    def list_forge_observations_for_target(
        self, *, project_id: str, target_kind: str, target_id: str
    ) -> list[ForgeObservationRecord]:
        rows = self.conn.execute(
            "SELECT * FROM forge_observations WHERE project_id = ? AND target_kind = ? "
            "AND target_id = ? ORDER BY observation_sequence ASC",
            (project_id, target_kind, target_id),
        ).fetchall()
        return [_row_to_forge_observation(row) for row in rows]

    def get_project_registration_operation(
        self, *, authenticated_principal_id: str, idempotency_key: str
    ) -> ProjectRegistrationOperationResult | None:
        require_lowercase_uuid(idempotency_key, field="idempotency_key")
        row = self.conn.execute(
            "SELECT * FROM project_registration_operations "
            "WHERE authenticated_principal_id = ? AND idempotency_key = ?",
            (authenticated_principal_id, idempotency_key),
        ).fetchone()
        return None if row is None else _row_to_project_registration_operation(row, replayed=False)

    def _project_registration_http_status(self, *, status: str, rejection_code: str | None) -> int:
        if status == "SUCCEEDED":
            return 200
        if rejection_code == "STABLE_REPOSITORY_OWNERSHIP_CONFLICT":
            return 409
        return 422

    def _project_registration_public_body(
        self,
        *,
        idempotency_key: str,
        mode: str,
        status: str,
        rejection_code: str | None = None,
        diagnostics: list[dict[str, str]] | None = None,
        project_id: str | None = None,
        registration_revision: int | None = None,
        forge_instance_id: str | None = None,
        installation_or_account_ref: str | None = None,
        repository_external_id: str | None = None,
        repository_locator: str | None = None,
        default_ref: str | None = None,
        trusted_base_commit: dict[str, str] | None = None,
        workflow_hash: str | None = None,
        policy_hash: str | None = None,
        trusted_base_policy_ref: str | None = None,
        budget_policy_ref: str | None = None,
        budget_reset_window_ref: str | None = None,
        readiness: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "protocol": PROJECT_REGISTRATION_RESULT_PROTOCOL,
            "idempotency_key": idempotency_key,
            "mode": mode,
            "status": status,
            "replayed": False,
        }
        if status == "SUCCEEDED":
            body.update(
                {
                    "project_id": project_id,
                    "registration_revision": registration_revision,
                    "registration_state": "ACTIVE",
                    "forge_instance_id": forge_instance_id,
                    "installation_or_account_ref": installation_or_account_ref,
                    "repository_external_id": repository_external_id,
                    "repository_locator": repository_locator,
                    "default_ref": default_ref,
                    "trusted_base_commit": trusted_base_commit,
                    "workflow_hash": workflow_hash,
                    "policy_hash": policy_hash,
                    "trusted_base_policy_ref": trusted_base_policy_ref,
                    "budget_policy_ref": budget_policy_ref,
                    "budget_reset_window_ref": budget_reset_window_ref,
                    "readiness": readiness,
                }
            )
        else:
            body["rejection_code"] = rejection_code
            body["diagnostics"] = diagnostics or []
        return body

    def _insert_project_registration_operation(
        self,
        *,
        operation_id: str,
        authenticated_principal_id: str,
        idempotency_key: str,
        mode: str,
        requested_project_id: str | None,
        expected_registration_revision: int | None,
        installation_or_account_ref: str,
        request_json: str,
        request_digest: str,
        authorization_context_digest: str,
        resolved_forge_instance_id: str | None,
        resolved_repository_external_id: str | None,
        resolved_base_commit_json: str | None,
        forge_api_secret_id: str | None,
        forge_api_secret_version: int | None,
        source_read_secret_id: str | None,
        source_read_secret_version: int | None,
        publication_secret_id: str | None,
        publication_secret_version: int | None,
        resolution_digest: str,
        status: str,
        result_project_id: str | None,
        result_registration_revision: int | None,
        result_schedule_id: str | None,
        rejection_code: str | None,
        response_http_status: int,
        response_json: str,
        response_digest: str,
        completed_at_ms: int,
    ) -> None:
        self.conn.execute(
            "INSERT INTO project_registration_operations("
            "project_registration_operation_id, protocol_version, authenticated_principal_id, "
            "idempotency_key, mode, requested_project_id, expected_registration_revision, "
            "installation_or_account_ref, request_json, request_digest, "
            "authorization_context_digest, resolved_forge_instance_id, "
            "resolved_repository_external_id, resolved_base_commit_json, "
            "resolved_forge_api_secret_id, resolved_forge_api_secret_version, "
            "resolved_source_read_secret_id, resolved_source_read_secret_version, "
            "resolved_publication_secret_id, resolved_publication_secret_version, "
            "resolution_digest, status, result_project_id, result_registration_revision, "
            "result_work_item_discovery_schedule_id, rejection_code, response_http_status, "
            "response_json, response_digest, completed_at_ms) VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                operation_id,
                PROJECT_REGISTRATION_PROTOCOL,
                authenticated_principal_id,
                idempotency_key,
                mode,
                requested_project_id,
                expected_registration_revision,
                installation_or_account_ref,
                request_json,
                request_digest,
                authorization_context_digest,
                resolved_forge_instance_id,
                resolved_repository_external_id,
                resolved_base_commit_json,
                forge_api_secret_id,
                forge_api_secret_version,
                source_read_secret_id,
                source_read_secret_version,
                publication_secret_id,
                publication_secret_version,
                resolution_digest,
                status,
                result_project_id,
                result_registration_revision,
                result_schedule_id,
                rejection_code,
                response_http_status,
                response_json,
                response_digest,
                completed_at_ms,
            ),
        )

    def commit_project_registration(
        self,
        *,
        authenticated_principal_id: str,
        idempotency_key: str,
        request: Mapping[str, Any],
        authorization_context_digest: str,
        adapter_kind: str,
        canonical_origin: str,
        installation_or_account_ref: str,
        default_ref: str,
        trusted_base_policy_ref: str,
        budget_policy_ref: str,
        budget_reset_window_ref: str,
        resolved_repository_external_id: str | None,
        resolved_repository_locator: str | None,
        resolved_base_commit: Mapping[str, str] | None,
        resolved_forge_api_secret_id: str | None,
        resolved_forge_api_secret_version: int | None,
        resolved_source_read_secret_id: str | None,
        resolved_source_read_secret_version: int | None,
        resolved_publication_secret_id: str | None,
        resolved_publication_secret_version: int | None,
        workflow_hash: str | None = None,
        policy_hash: str | None = None,
        readiness: Mapping[str, Any] | None = None,
        business_rejection_code: str | None = None,
        diagnostics: list[dict[str, str]] | None = None,
        fault: FaultInjectionPoint | None = None,
    ) -> ProjectRegistrationOperationResult:
        """Atomically claim the replay key and insert Project + Schedule + Operation.

        Idempotency/body conflicts and REVALIDATE CAS/authority-reference
        mismatches raise without writing an Operation. Bounded business
        rejections persist only the immutable Operation. Success inserts the
        Project, reciprocal provenance, and revision-0 WORK_ITEM_DISCOVERY
        Schedule in the same writer transaction.
        """
        require_lowercase_uuid(idempotency_key, field="idempotency_key")
        _require_digest(authorization_context_digest, field="authorization_context_digest")
        enums.parse_enum("forge_instance.adapter_kind", adapter_kind)
        request_json = canonical_json_text(request)
        req_digest = request_digest(request)
        requested_project_id = request.get("project_id")
        expected_revision = request.get("expected_registration_revision")
        mode = "REGISTER" if requested_project_id is None else "REVALIDATE"
        if requested_project_id is not None:
            require_lowercase_uuid(requested_project_id, field="project_id")
        if expected_revision is not None:
            _require_positive_int(expected_revision, field="expected_registration_revision")

        with self.transaction(fault=fault):
            existing = self.conn.execute(
                "SELECT * FROM project_registration_operations "
                "WHERE authenticated_principal_id = ? AND idempotency_key = ?",
                (authenticated_principal_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if existing["request_digest"] == req_digest:
                    return _row_to_project_registration_operation(existing, replayed=True)
                raise IdempotencyConflictError(
                    "idempotency key was reused with a different registration body"
                )

            if mode == "REVALIDATE":
                assert requested_project_id is not None
                assert expected_revision is not None
                project = self.get_project(requested_project_id)
                if project is None or project.registration_revision != expected_revision:
                    raise CasMismatchError("registration revision CAS mismatch")
                if (
                    project.installation_or_account_ref != installation_or_account_ref
                    or project.default_ref != default_ref
                    or project.trusted_base_policy_ref != trusted_base_policy_ref
                    or project.budget_policy_ref != budget_policy_ref
                    or project.budget_reset_window_ref != budget_reset_window_ref
                ):
                    raise CasMismatchError(
                        "revalidation cannot change installation or authority-bearing policy refs"
                    )

            now = _now_ms()
            operation_id = str(uuid.uuid4())
            base_commit_json = (
                canonical_json_text(resolved_base_commit)
                if isinstance(resolved_base_commit, Mapping)
                else resolved_base_commit
            )

            if business_rejection_code is not None:
                enums.parse_enum(
                    "project_registration_operation.rejection_code", business_rejection_code
                )
                status = "REJECTED"
                sorted_diagnostics = sorted(
                    diagnostics or [], key=lambda item: (item.get("code", ""), item.get("path", ""))
                )
                body = self._project_registration_public_body(
                    idempotency_key=idempotency_key,
                    mode=mode,
                    status=status,
                    rejection_code=business_rejection_code,
                    diagnostics=sorted_diagnostics,
                )
                http_status = self._project_registration_http_status(
                    status=status, rejection_code=business_rejection_code
                )
                body_json = canonical_json_text(body)
                resp_digest = response_digest(
                    {"http_status": http_status, "body": _response_digest_preimage(body)}
                )
                res_digest = resolution_digest(
                    {
                        "request_digest": req_digest,
                        "authorization_context_digest": authorization_context_digest,
                        "installation_or_account_ref": installation_or_account_ref,
                        "resolved_forge_instance_id": None,
                        "resolved_repository_external_id": resolved_repository_external_id,
                        "resolved_base_commit": resolved_base_commit,
                        "resolved_forge_api_secret_ref": None,
                        "resolved_source_read_secret_ref": None,
                        "resolved_publication_secret_ref": None,
                        "result_work_item_discovery_schedule_id": None,
                    }
                )
                self._insert_project_registration_operation(
                    operation_id=operation_id,
                    authenticated_principal_id=authenticated_principal_id,
                    idempotency_key=idempotency_key,
                    mode=mode,
                    requested_project_id=requested_project_id,
                    expected_registration_revision=expected_revision,
                    installation_or_account_ref=installation_or_account_ref,
                    request_json=request_json,
                    request_digest=req_digest,
                    authorization_context_digest=authorization_context_digest,
                    resolved_forge_instance_id=None,
                    resolved_repository_external_id=resolved_repository_external_id,
                    resolved_base_commit_json=base_commit_json,
                    forge_api_secret_id=None,
                    forge_api_secret_version=None,
                    source_read_secret_id=None,
                    source_read_secret_version=None,
                    publication_secret_id=None,
                    publication_secret_version=None,
                    resolution_digest=res_digest,
                    status=status,
                    result_project_id=None,
                    result_registration_revision=None,
                    result_schedule_id=None,
                    rejection_code=business_rejection_code,
                    response_http_status=http_status,
                    response_json=body_json,
                    response_digest=resp_digest,
                    completed_at_ms=now,
                )
            else:
                if (
                    resolved_forge_api_secret_id is None
                    or resolved_forge_api_secret_version is None
                    or resolved_source_read_secret_id is None
                    or resolved_source_read_secret_version is None
                    or resolved_publication_secret_id is None
                    or resolved_publication_secret_version is None
                    or resolved_repository_external_id is None
                    or resolved_repository_locator is None
                    or resolved_base_commit is None
                    or workflow_hash is None
                    or policy_hash is None
                    or readiness is None
                ):
                    raise ValueError("successful registration requires complete secret resolution")
                require_lowercase_uuid(resolved_forge_api_secret_id, field="forge_api_secret_id")
                require_lowercase_uuid(
                    resolved_source_read_secret_id, field="source_read_secret_id"
                )
                require_lowercase_uuid(
                    resolved_publication_secret_id, field="publication_secret_id"
                )
                _require_digest(workflow_hash, field="workflow_hash")
                _require_digest(policy_hash, field="policy_hash")
                if resolved_source_read_secret_id == resolved_publication_secret_id:
                    raise ValueError("source-read and publication Secret IDs must differ")

                forge_row = self.conn.execute(
                    "SELECT * FROM forge_instances WHERE canonical_origin = ?",
                    (canonical_origin,),
                ).fetchone()
                if forge_row is None:
                    forge_instance_id = str(uuid.uuid4())
                    self.conn.execute(
                        "INSERT INTO forge_instances("
                        "forge_instance_id, adapter_kind, canonical_origin, "
                        "credential_secret_id, registration_provenance_version, created_at_ms) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            forge_instance_id,
                            adapter_kind,
                            canonical_origin,
                            resolved_forge_api_secret_id,
                            resolved_forge_api_secret_version,
                            now,
                        ),
                    )
                else:
                    forge_instance_id = forge_row["forge_instance_id"]
                    if forge_row["credential_secret_id"] != resolved_forge_api_secret_id:
                        raise CasMismatchError(
                            "registration cannot rewrite an existing Forge Instance credential"
                        )

                owner = self.conn.execute(
                    "SELECT * FROM projects WHERE forge_instance_id = ? "
                    "AND repository_external_id = ?",
                    (forge_instance_id, resolved_repository_external_id),
                ).fetchone()
                if owner is not None and (
                    mode == "REGISTER" or owner["project_id"] != requested_project_id
                ):
                    status = "REJECTED"
                    body = self._project_registration_public_body(
                        idempotency_key=idempotency_key,
                        mode=mode,
                        status=status,
                        rejection_code="STABLE_REPOSITORY_OWNERSHIP_CONFLICT",
                        diagnostics=[
                            {
                                "code": "STABLE_REPOSITORY_OWNERSHIP_CONFLICT",
                                "message": "repository is already registered to another Project",
                                "path": "forge.repository_locator",
                            }
                        ],
                    )
                    http_status = 409
                    body_json = canonical_json_text(body)
                    resp_digest = response_digest(
                        {"http_status": http_status, "body": _response_digest_preimage(body)}
                    )
                    res_digest = resolution_digest(
                        {
                            "request_digest": req_digest,
                            "authorization_context_digest": authorization_context_digest,
                            "installation_or_account_ref": installation_or_account_ref,
                            "resolved_forge_instance_id": forge_instance_id,
                            "resolved_repository_external_id": resolved_repository_external_id,
                            "resolved_base_commit": resolved_base_commit,
                            "resolved_forge_api_secret_ref": None,
                            "resolved_source_read_secret_ref": None,
                            "resolved_publication_secret_ref": None,
                            "result_work_item_discovery_schedule_id": None,
                        }
                    )
                    self._insert_project_registration_operation(
                        operation_id=operation_id,
                        authenticated_principal_id=authenticated_principal_id,
                        idempotency_key=idempotency_key,
                        mode=mode,
                        requested_project_id=requested_project_id,
                        expected_registration_revision=expected_revision,
                        installation_or_account_ref=installation_or_account_ref,
                        request_json=request_json,
                        request_digest=req_digest,
                        authorization_context_digest=authorization_context_digest,
                        resolved_forge_instance_id=forge_instance_id,
                        resolved_repository_external_id=resolved_repository_external_id,
                        resolved_base_commit_json=base_commit_json,
                        forge_api_secret_id=None,
                        forge_api_secret_version=None,
                        source_read_secret_id=None,
                        source_read_secret_version=None,
                        publication_secret_id=None,
                        publication_secret_version=None,
                        resolution_digest=res_digest,
                        status=status,
                        result_project_id=None,
                        result_registration_revision=None,
                        result_schedule_id=None,
                        rejection_code="STABLE_REPOSITORY_OWNERSHIP_CONFLICT",
                        response_http_status=http_status,
                        response_json=body_json,
                        response_digest=resp_digest,
                        completed_at_ms=now,
                    )
                elif mode == "REGISTER":
                    project_id = str(uuid.uuid4())
                    schedule_id = str(uuid.uuid4())
                    schedule_digest = forge_observation_schedule_digest(
                        _forge_observation_schedule_digest_fields(
                            schedule_kind="WORK_ITEM_DISCOVERY",
                            project_id=project_id,
                            forge_instance_id=forge_instance_id,
                            target_kind="PROJECT",
                            target_id=project_id,
                            run_id=None,
                            publication_id=None,
                            terminal_duplicate_cleanup_reservation_id=None,
                            minimum_interval_ms=_DEFAULT_DISCOVERY_INTERVAL_MS,
                        )
                    )
                    # projects is inserted before forge_observation_schedules: the
                    # Schedule's project_id is now a real foreign key, and SQLite
                    # checks FKs per-statement (not deferred), so the Project row
                    # must already exist when the Schedule row is inserted.
                    self.conn.execute(
                        "INSERT INTO projects("
                        "project_id, forge_instance_id, installation_or_account_ref, "
                        "repository_external_id, repository_locator, default_ref, "
                        "trusted_base_policy_ref, budget_policy_ref, budget_reset_window_ref, "
                        "source_read_secret_id, publication_secret_id, "
                        "registration_source_read_secret_version, "
                        "registration_publication_secret_version, registration_revision, "
                        "registration_operation_id, work_item_discovery_schedule_id, "
                        "registration_state) VALUES ("
                        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 'ACTIVE')",
                        (
                            project_id,
                            forge_instance_id,
                            installation_or_account_ref,
                            resolved_repository_external_id,
                            resolved_repository_locator,
                            default_ref,
                            trusted_base_policy_ref,
                            budget_policy_ref,
                            budget_reset_window_ref,
                            resolved_source_read_secret_id,
                            resolved_publication_secret_id,
                            resolved_source_read_secret_version,
                            resolved_publication_secret_version,
                            operation_id,
                            schedule_id,
                        ),
                    )
                    self.conn.execute(
                        "INSERT INTO forge_observation_schedules("
                        "forge_observation_schedule_id, schedule_kind, project_id, "
                        "forge_instance_id, schedule_revision, state, target_kind, target_id, "
                        "run_id, publication_id, terminal_duplicate_cleanup_reservation_id, "
                        "minimum_interval_ms, last_request_id, last_discovery_search_revision, "
                        "last_discovery_set_digest, schedule_digest, next_due_at_ms, "
                        "created_at_ms) "
                        "VALUES (?, 'WORK_ITEM_DISCOVERY', ?, ?, 0, 'ACTIVE', 'PROJECT', ?, "
                        "NULL, NULL, NULL, ?, NULL, NULL, NULL, ?, ?, ?)",
                        (
                            schedule_id,
                            project_id,
                            forge_instance_id,
                            project_id,
                            _DEFAULT_DISCOVERY_INTERVAL_MS,
                            schedule_digest,
                            now,
                            now,
                        ),
                    )
                    body = self._project_registration_public_body(
                        idempotency_key=idempotency_key,
                        mode=mode,
                        status="SUCCEEDED",
                        project_id=project_id,
                        registration_revision=1,
                        forge_instance_id=forge_instance_id,
                        installation_or_account_ref=installation_or_account_ref,
                        repository_external_id=resolved_repository_external_id,
                        repository_locator=resolved_repository_locator,
                        default_ref=default_ref,
                        trusted_base_commit=dict(resolved_base_commit),
                        workflow_hash=workflow_hash,
                        policy_hash=policy_hash,
                        trusted_base_policy_ref=trusted_base_policy_ref,
                        budget_policy_ref=budget_policy_ref,
                        budget_reset_window_ref=budget_reset_window_ref,
                        readiness=dict(readiness),
                    )
                    http_status = 200
                    body_json = canonical_json_text(body)
                    resp_digest = response_digest(
                        {"http_status": http_status, "body": _response_digest_preimage(body)}
                    )
                    res_digest = resolution_digest(
                        {
                            "request_digest": req_digest,
                            "authorization_context_digest": authorization_context_digest,
                            "installation_or_account_ref": installation_or_account_ref,
                            "resolved_forge_instance_id": forge_instance_id,
                            "resolved_repository_external_id": resolved_repository_external_id,
                            "resolved_base_commit": resolved_base_commit,
                            "resolved_forge_api_secret_ref": {
                                "secret_id": resolved_forge_api_secret_id,
                                "version": resolved_forge_api_secret_version,
                            },
                            "resolved_source_read_secret_ref": {
                                "secret_id": resolved_source_read_secret_id,
                                "version": resolved_source_read_secret_version,
                            },
                            "resolved_publication_secret_ref": {
                                "secret_id": resolved_publication_secret_id,
                                "version": resolved_publication_secret_version,
                            },
                            "result_work_item_discovery_schedule_id": schedule_id,
                        }
                    )
                    self._insert_project_registration_operation(
                        operation_id=operation_id,
                        authenticated_principal_id=authenticated_principal_id,
                        idempotency_key=idempotency_key,
                        mode=mode,
                        requested_project_id=requested_project_id,
                        expected_registration_revision=expected_revision,
                        installation_or_account_ref=installation_or_account_ref,
                        request_json=request_json,
                        request_digest=req_digest,
                        authorization_context_digest=authorization_context_digest,
                        resolved_forge_instance_id=forge_instance_id,
                        resolved_repository_external_id=resolved_repository_external_id,
                        resolved_base_commit_json=base_commit_json,
                        forge_api_secret_id=resolved_forge_api_secret_id,
                        forge_api_secret_version=resolved_forge_api_secret_version,
                        source_read_secret_id=resolved_source_read_secret_id,
                        source_read_secret_version=resolved_source_read_secret_version,
                        publication_secret_id=resolved_publication_secret_id,
                        publication_secret_version=resolved_publication_secret_version,
                        resolution_digest=res_digest,
                        status="SUCCEEDED",
                        result_project_id=project_id,
                        result_registration_revision=1,
                        result_schedule_id=schedule_id,
                        rejection_code=None,
                        response_http_status=http_status,
                        response_json=body_json,
                        response_digest=resp_digest,
                        completed_at_ms=now,
                    )
                else:
                    assert requested_project_id is not None
                    assert expected_revision is not None
                    project_id = requested_project_id
                    current = self.get_project(project_id)
                    assert current is not None
                    if current.forge_instance_id != forge_instance_id:
                        raise CasMismatchError(
                            "revalidation cannot rewrite Forge Instance identity"
                        )
                    if current.repository_external_id != resolved_repository_external_id:
                        raise CasMismatchError("revalidation cannot rewrite stable repository id")
                    if current.installation_or_account_ref != installation_or_account_ref:
                        raise CasMismatchError("revalidation cannot rewrite installation binding")
                    if (
                        current.source_read_secret_id != resolved_source_read_secret_id
                        or current.publication_secret_id != resolved_publication_secret_id
                    ):
                        raise CasMismatchError("revalidation cannot substitute Project secrets")
                    new_revision = expected_revision + 1
                    schedule_id = current.work_item_discovery_schedule_id
                    updated = self.conn.execute(
                        "UPDATE projects SET repository_locator = ?, "
                        "registration_source_read_secret_version = ?, "
                        "registration_publication_secret_version = ?, "
                        "registration_revision = ?, registration_operation_id = ? "
                        "WHERE project_id = ? AND registration_revision = ?",
                        (
                            resolved_repository_locator,
                            resolved_source_read_secret_version,
                            resolved_publication_secret_version,
                            new_revision,
                            operation_id,
                            project_id,
                            expected_revision,
                        ),
                    )
                    if updated.rowcount != 1:
                        raise CasMismatchError("registration revision CAS mismatch")
                    body = self._project_registration_public_body(
                        idempotency_key=idempotency_key,
                        mode=mode,
                        status="SUCCEEDED",
                        project_id=project_id,
                        registration_revision=new_revision,
                        forge_instance_id=forge_instance_id,
                        installation_or_account_ref=installation_or_account_ref,
                        repository_external_id=resolved_repository_external_id,
                        repository_locator=resolved_repository_locator,
                        default_ref=default_ref,
                        trusted_base_commit=dict(resolved_base_commit),
                        workflow_hash=workflow_hash,
                        policy_hash=policy_hash,
                        trusted_base_policy_ref=trusted_base_policy_ref,
                        budget_policy_ref=budget_policy_ref,
                        budget_reset_window_ref=budget_reset_window_ref,
                        readiness=dict(readiness),
                    )
                    http_status = 200
                    body_json = canonical_json_text(body)
                    resp_digest = response_digest(
                        {"http_status": http_status, "body": _response_digest_preimage(body)}
                    )
                    res_digest = resolution_digest(
                        {
                            "request_digest": req_digest,
                            "authorization_context_digest": authorization_context_digest,
                            "installation_or_account_ref": installation_or_account_ref,
                            "resolved_forge_instance_id": forge_instance_id,
                            "resolved_repository_external_id": resolved_repository_external_id,
                            "resolved_base_commit": resolved_base_commit,
                            "resolved_forge_api_secret_ref": {
                                "secret_id": resolved_forge_api_secret_id,
                                "version": resolved_forge_api_secret_version,
                            },
                            "resolved_source_read_secret_ref": {
                                "secret_id": resolved_source_read_secret_id,
                                "version": resolved_source_read_secret_version,
                            },
                            "resolved_publication_secret_ref": {
                                "secret_id": resolved_publication_secret_id,
                                "version": resolved_publication_secret_version,
                            },
                            "result_work_item_discovery_schedule_id": schedule_id,
                        }
                    )
                    self._insert_project_registration_operation(
                        operation_id=operation_id,
                        authenticated_principal_id=authenticated_principal_id,
                        idempotency_key=idempotency_key,
                        mode=mode,
                        requested_project_id=requested_project_id,
                        expected_registration_revision=expected_revision,
                        installation_or_account_ref=installation_or_account_ref,
                        request_json=request_json,
                        request_digest=req_digest,
                        authorization_context_digest=authorization_context_digest,
                        resolved_forge_instance_id=forge_instance_id,
                        resolved_repository_external_id=resolved_repository_external_id,
                        resolved_base_commit_json=base_commit_json,
                        forge_api_secret_id=resolved_forge_api_secret_id,
                        forge_api_secret_version=resolved_forge_api_secret_version,
                        source_read_secret_id=resolved_source_read_secret_id,
                        source_read_secret_version=resolved_source_read_secret_version,
                        publication_secret_id=resolved_publication_secret_id,
                        publication_secret_version=resolved_publication_secret_version,
                        resolution_digest=res_digest,
                        status="SUCCEEDED",
                        result_project_id=project_id,
                        result_registration_revision=new_revision,
                        result_schedule_id=schedule_id,
                        rejection_code=None,
                        response_http_status=http_status,
                        response_json=body_json,
                        response_digest=resp_digest,
                        completed_at_ms=now,
                    )

            row = self.conn.execute(
                "SELECT * FROM project_registration_operations "
                "WHERE project_registration_operation_id = ?",
                (operation_id,),
            ).fetchone()
            assert row is not None
            return _row_to_project_registration_operation(row, replayed=False)

    def create_run(
        self,
        *,
        run_id: str,
        project_id: str,
        work_item_key: str,
        state: str,
        reducer_version: str = DEFAULT_REDUCER_VERSION,
        specification_generation: int = 1,
    ) -> None:
        require_lowercase_uuid(run_id, field="run_id")
        if reducer_version not in self._supported_reducer_versions:
            raise ReducerVersionError(f"unsupported reducer version {reducer_version!r}")
        if specification_generation < 0:
            raise ValueError("specification_generation must be nonnegative")
        enums.parse_enum("run.state", state)
        existing = self.conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if existing is not None:
            if (
                existing["project_id"] == project_id
                and existing["work_item_key"] == work_item_key
                and existing["reducer_version"] == reducer_version
            ):
                return
            raise IdempotencyConflictError("run id was reused with different content")
        active_existing = self.conn.execute(
            "SELECT run_id FROM runs WHERE project_id = ? AND work_item_key = ? "
            "AND terminal_outcome IS NULL",
            (project_id, work_item_key),
        ).fetchone()
        if active_existing is not None:
            raise IdempotencyConflictError("work item already has an active run")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO runs(run_id, project_id, work_item_key, specification_generation, "
            "state, terminal_outcome, reducer_version, current_revision, created_at_ms, "
            "updated_at_ms) VALUES (?, ?, ?, ?, ?, NULL, ?, 0, ?, ?)",
            (
                run_id,
                project_id,
                work_item_key,
                specification_generation,
                state,
                reducer_version,
                now,
                now,
            ),
        )

    def append_transition(
        self,
        *,
        run_id: str,
        transition_id: str,
        prior_state: str,
        trigger_kind: str,
        trigger_id: str,
        next_state: str,
        reducer_version: str,
        input_digest: str,
        specification_generation: int,
        admit_base_observation_id: str | None = None,
    ) -> Transition:
        require_lowercase_uuid(run_id, field="run_id")
        require_lowercase_uuid(transition_id, field="transition_id")
        if prior_state != PRIOR_STATE_NONE:
            enums.parse_enum("run.state", prior_state)
        enums.parse_enum("transition.trigger_kind", trigger_kind)
        enums.parse_enum("run.state", next_state)
        _require_digest(input_digest, field="input_digest")
        if reducer_version not in self._supported_reducer_versions:
            raise ReducerVersionError(f"unsupported reducer version {reducer_version!r}")
        existing = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? AND trigger_kind = ? AND trigger_id = ?",
            (run_id, trigger_kind, trigger_id),
        ).fetchone()
        if existing is not None:
            row = _row_to_transition(existing)
            if (
                row.transition_id == transition_id
                and row.prior_state == prior_state
                and row.next_state == next_state
                and row.reducer_version == reducer_version
                and row.input_digest == input_digest
                and row.specification_generation == specification_generation
                and row.admit_base_observation_id == admit_base_observation_id
            ):
                return row
            raise IdempotencyConflictError("transition trigger was already consumed differently")
        row = self.conn.execute(
            "SELECT COALESCE(MAX(transition_sequence), 0) + 1 FROM transitions WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        sequence = int(row[0])
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO transitions(run_id, transition_sequence, transition_id, prior_state, "
            "trigger_kind, trigger_id, admit_base_observation_id, next_state, reducer_version, "
            "input_digest, specification_generation, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                sequence,
                transition_id,
                prior_state,
                trigger_kind,
                trigger_id,
                admit_base_observation_id,
                next_state,
                reducer_version,
                input_digest,
                specification_generation,
                now,
            ),
        )
        self.conn.execute(
            "UPDATE runs SET state = ?, specification_generation = ?, current_revision = ?, "
            "updated_at_ms = ? "
            "WHERE run_id = ?",
            (next_state, specification_generation, sequence, now, run_id),
        )
        inserted = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? AND transition_sequence = ?",
            (run_id, sequence),
        ).fetchone()
        assert inserted is not None
        return _row_to_transition(inserted)

    def put_workflow_blob(
        self,
        *,
        media_kind: str,
        normalized_bytes: bytes,
    ) -> WorkflowBlobSqlRecord:
        enums.parse_enum("workflow_blob.media_kind", media_kind)
        blob_digest = workflow_blob_digest(media_kind, normalized_bytes)
        existing = self.conn.execute(
            "SELECT * FROM workflow_blobs WHERE blob_digest = ?", (blob_digest,)
        ).fetchone()
        if existing is not None:
            row = _row_to_workflow_blob(existing)
            if row.media_kind == media_kind and row.normalized_bytes == normalized_bytes:
                return row
            raise IdempotencyConflictError("workflow blob digest was reused")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO workflow_blobs(blob_digest, media_kind, byte_length, "
            "normalized_bytes, created_at_ms) VALUES (?, ?, ?, ?, ?)",
            (blob_digest, media_kind, len(normalized_bytes), normalized_bytes, now),
        )
        row = self.conn.execute(
            "SELECT * FROM workflow_blobs WHERE blob_digest = ?", (blob_digest,)
        ).fetchone()
        assert row is not None
        return _row_to_workflow_blob(row)

    def record_policy_update(
        self,
        *,
        policy_update_id: str,
        project_id: str,
        server_policy_revision: str,
        server_policy: Any,
        default_ref: str,
        trusted_base_policy_ref: str,
        budget_policy_ref: str,
        budget_reset_window_ref: str,
        source_id: str,
        authenticated_principal_id: str,
    ) -> PolicyUpdateRecord:
        require_lowercase_uuid(policy_update_id, field="policy_update_id")
        server_policy_blob = self.put_workflow_blob(
            media_kind="SERVER_POLICY_JSON",
            normalized_bytes=canonical_json_text(server_policy).encode("utf-8"),
        )
        existing = self.conn.execute(
            "SELECT * FROM policy_updates WHERE source_kind = 'SERVER_ROLLOUT' AND source_id = ?",
            (source_id,),
        ).fetchone()
        if existing is not None:
            row = _row_to_policy_update(existing)
            if (
                row.policy_update_id == policy_update_id
                and row.project_id == project_id
                and row.server_policy_revision == server_policy_revision
                and row.server_policy_blob_digest == server_policy_blob.blob_digest
                and row.default_ref == default_ref
                and row.trusted_base_policy_ref == trusted_base_policy_ref
                and row.budget_policy_ref == budget_policy_ref
                and row.budget_reset_window_ref == budget_reset_window_ref
                and row.authenticated_principal_id == authenticated_principal_id
            ):
                return row
            raise IdempotencyConflictError("policy update source was reused")
        row = self.conn.execute(
            "SELECT COALESCE(MAX(policy_update_sequence), 0) + 1 "
            "FROM policy_updates WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        sequence = int(row[0])
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO policy_updates(policy_update_id, project_id, policy_update_sequence, "
            "server_policy_revision, server_policy_blob_digest, default_ref, "
            "trusted_base_policy_ref, budget_policy_ref, budget_reset_window_ref, "
            "source_kind, source_id, authenticated_principal_id, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'SERVER_ROLLOUT', ?, ?, ?)",
            (
                policy_update_id,
                project_id,
                sequence,
                server_policy_revision,
                server_policy_blob.blob_digest,
                default_ref,
                trusted_base_policy_ref,
                budget_policy_ref,
                budget_reset_window_ref,
                source_id,
                authenticated_principal_id,
                now,
            ),
        )
        inserted = self.conn.execute(
            "SELECT * FROM policy_updates WHERE policy_update_id = ?", (policy_update_id,)
        ).fetchone()
        assert inserted is not None
        return _row_to_policy_update(inserted)

    def capture_work_item_snapshot(
        self,
        *,
        snapshot_id: str,
        run_id: str,
        source_kind: str,
        source_id: str,
        work_item_observation_id: str,
        base_observation_id: str,
        project_id: str,
        work_item_external_id: str,
        forge_revision: str,
        title: str,
        body: str,
        specification_comments: Sequence[Any] = (),
        base_ref: str,
        base_commit: Any,
        workflow_schema_version: str,
        normalized_workflow: Any,
        normalized_prompt_blobs: Sequence[Mapping[str, Any]] = (),
        effective_policy: Any,
        server_policy_revision: str,
        trusted_base_policy_ref: str,
        budget_policy_ref: str,
        budget_reset_window_ref: str,
        base_movement_policy: str,
        reducer_version: str = DEFAULT_REDUCER_VERSION,
        captured_at_ms: int | None = None,
    ) -> WorkItemSnapshotRecord:
        require_lowercase_uuid(snapshot_id, field="snapshot_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("work_item_snapshot.source_kind", source_kind)
        enums.parse_enum("snapshot.base_movement_policy", base_movement_policy)
        if reducer_version not in self._supported_reducer_versions:
            raise ReducerVersionError(f"unsupported reducer version {reducer_version!r}")
        normalized_workflow_json = canonical_json_text(normalized_workflow)
        workflow_blob = self.put_workflow_blob(
            media_kind="CONFIG_JSON", normalized_bytes=normalized_workflow_json.encode("utf-8")
        )
        prompt_entries: list[dict[str, str]] = []
        for item in sorted(normalized_prompt_blobs, key=lambda entry: str(entry["path"])):
            if "normalized_bytes" in item:
                prompt_bytes = item["normalized_bytes"]
                if isinstance(prompt_bytes, str):
                    prompt_bytes = prompt_bytes.encode("utf-8")
                if not isinstance(prompt_bytes, bytes):
                    raise ValueError("prompt normalized_bytes must be bytes or str")
                prompt_blob = self.put_workflow_blob(
                    media_kind="PROMPT_UTF8", normalized_bytes=prompt_bytes
                )
                blob_digest = prompt_blob.blob_digest
            else:
                blob_digest = str(item["blob_digest"])
                _require_digest(blob_digest, field="normalized_prompt_blobs[].blob_digest")
            prompt_entries.append(
                {
                    "path": str(item["path"]),
                    "git_blob": str(item["git_blob"]),
                    "blob_digest": blob_digest,
                }
            )
        policy_json = canonical_json_text(effective_policy)
        policy_blob = self.put_workflow_blob(
            media_kind="POLICY_JSON", normalized_bytes=policy_json.encode("utf-8")
        )
        policy_hash = policy_digest(effective_policy)
        specification_hash = specification_digest(
            {
                "title": title,
                "body": body,
                "comments": list(specification_comments),
            }
        )
        workflow_hash = config_bundle_hash(normalized_workflow)
        generation_input_hash = bare_canonical_digest(
            {
                "specification_hash": specification_hash,
                "workflow_schema_version": workflow_schema_version,
                "workflow_hash": workflow_hash,
                "policy_hash": policy_hash,
            }
        )
        if base_movement_policy == "SUPERSEDE_AT_BOUNDARY":
            supersession_key = bare_canonical_digest(
                {
                    "generation_input_hash": generation_input_hash,
                    "base_commit": base_commit,
                }
            )
        else:
            supersession_key = generation_input_hash
        comments_json = canonical_json_text(list(specification_comments))
        base_commit_json = canonical_json_text(base_commit)
        prompt_json = canonical_json_text(prompt_entries)
        snapshot_preimage = {
            "run_id": run_id,
            "source_kind": source_kind,
            "source_id": source_id,
            "work_item_observation_id": work_item_observation_id,
            "base_observation_id": base_observation_id,
            "project_id": project_id,
            "work_item_external_id": work_item_external_id,
            "forge_revision": forge_revision,
            "title": title,
            "body": body,
            "specification_comments": list(specification_comments),
            "base_ref": base_ref,
            "base_commit": base_commit,
            "workflow_schema_version": workflow_schema_version,
            "workflow_hash": workflow_hash,
            "normalized_workflow_blob_digest": workflow_blob.blob_digest,
            "normalized_prompt_blobs": prompt_entries,
            "effective_policy_blob_digest": policy_blob.blob_digest,
            "server_policy_revision": server_policy_revision,
            "trusted_base_policy_ref": trusted_base_policy_ref,
            "budget_policy_ref": budget_policy_ref,
            "budget_reset_window_ref": budget_reset_window_ref,
            "policy_hash": policy_hash,
            "reducer_version": reducer_version,
            "specification_hash": specification_hash,
            "generation_input_hash": generation_input_hash,
            "base_movement_policy": base_movement_policy,
            "supersession_key": supersession_key,
        }
        snapshot_hash = request_digest(snapshot_preimage)
        existing = self.conn.execute(
            "SELECT * FROM work_item_snapshots WHERE run_id = ? AND source_kind = ? "
            "AND source_id = ?",
            (run_id, source_kind, source_id),
        ).fetchone()
        if existing is not None:
            row = _row_to_work_item_snapshot(existing)
            if row.snapshot_id == snapshot_id and row.snapshot_hash == snapshot_hash:
                return row
            raise IdempotencyConflictError("snapshot source was reused")
        row = self.conn.execute(
            "SELECT COALESCE(MAX(snapshot_sequence), 0) + 1 FROM work_item_snapshots "
            "WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        sequence = int(row[0])
        now = _now_ms() if captured_at_ms is None else captured_at_ms
        self.conn.execute(
            "INSERT INTO work_item_snapshots(snapshot_id, run_id, snapshot_sequence, "
            "source_kind, source_id, work_item_observation_id, base_observation_id, "
            "project_id, work_item_external_id, forge_revision, title, body, "
            "specification_comments_json, base_ref, base_commit_json, "
            "workflow_schema_version, workflow_hash, normalized_workflow_blob_digest, "
            "normalized_prompt_blobs_json, effective_policy_blob_digest, "
            "server_policy_revision, trusted_base_policy_ref, budget_policy_ref, "
            "budget_reset_window_ref, policy_hash, reducer_version, specification_hash, "
            "generation_input_hash, base_movement_policy, supersession_key, snapshot_hash, "
            "captured_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                snapshot_id,
                run_id,
                sequence,
                source_kind,
                source_id,
                work_item_observation_id,
                base_observation_id,
                project_id,
                work_item_external_id,
                forge_revision,
                title,
                body,
                comments_json,
                base_ref,
                base_commit_json,
                workflow_schema_version,
                workflow_hash,
                workflow_blob.blob_digest,
                prompt_json,
                policy_blob.blob_digest,
                server_policy_revision,
                trusted_base_policy_ref,
                budget_policy_ref,
                budget_reset_window_ref,
                policy_hash,
                reducer_version,
                specification_hash,
                generation_input_hash,
                base_movement_policy,
                supersession_key,
                snapshot_hash,
                now,
            ),
        )
        inserted = self.conn.execute(
            "SELECT * FROM work_item_snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        assert inserted is not None
        return _row_to_work_item_snapshot(inserted)

    def admit_work_item_from_observations(
        self,
        *,
        run_id: str,
        snapshot_id: str,
        work_item_observation_id: str,
        transition_id: str,
        projection_outbox_id: str,
        projection_idempotency_key: str,
        normalized_workflow: Any,
        effective_policy: Any,
        server_policy_revision: str,
        trusted_base_policy_ref: str,
        budget_policy_ref: str,
        budget_reset_window_ref: str,
        base_movement_policy: str,
        workflow_schema_version: str = "1",
        normalized_prompt_blobs: Sequence[Mapping[str, Any]] = (),
        reducer_version: str = DEFAULT_REDUCER_VERSION,
        fault: FaultInjectionPoint | None = None,
    ) -> AdmissionResult:
        with self.transaction(fault=fault):
            work = self.get_forge_observation(work_item_observation_id)
            if work is None or work.kind != "WORK_ITEM_SNAPSHOT":
                raise RunStoreError("admission requires an accepted WORK_ITEM_SNAPSHOT observation")
            active = self.conn.execute(
                "SELECT * FROM runs WHERE project_id = ? AND work_item_key = ? "
                "AND terminal_outcome IS NULL",
                (work.project_id, work.target_id),
            ).fetchone()
            if active is not None:
                existing_snapshot = self.conn.execute(
                    "SELECT * FROM work_item_snapshots WHERE run_id = ? "
                    "AND source_kind = 'FORGE_OBSERVATION' AND source_id = ?",
                    (active["run_id"], work.forge_observation_id),
                ).fetchone()
                if existing_snapshot is None:
                    raise IdempotencyConflictError(
                        "work item already has an active run admitted by a different observation"
                    )
                if (
                    active["run_id"] != run_id
                    or existing_snapshot["snapshot_id"] != snapshot_id
                    or existing_snapshot["work_item_observation_id"] != work.forge_observation_id
                ):
                    raise IdempotencyConflictError(
                        "active run admission was replayed with different content"
                    )
                return AdmissionResult(
                    run_id=active["run_id"],
                    snapshot_id=existing_snapshot["snapshot_id"],
                    transition=None,
                    projection_outbox_id=None,
                    replayed=True,
                )
            base = self._select_admission_base_observation(work)
            fact = work.fact
            base_fact = base.fact
            self.create_run(
                run_id=run_id,
                project_id=work.project_id,
                work_item_key=work.target_id,
                state="ADMITTED",
                reducer_version=reducer_version,
                specification_generation=0,
            )
            snapshot = self.capture_work_item_snapshot(
                snapshot_id=snapshot_id,
                run_id=run_id,
                source_kind="FORGE_OBSERVATION",
                source_id=work.forge_observation_id,
                work_item_observation_id=work.forge_observation_id,
                base_observation_id=base.forge_observation_id,
                project_id=work.project_id,
                work_item_external_id=work.target_id,
                forge_revision=work.external_revision,
                title=str(fact.get("title", "")) if isinstance(fact, dict) else "",
                body=str(fact.get("body", "")) if isinstance(fact, dict) else "",
                specification_comments=(
                    fact.get("specification_comments", []) if isinstance(fact, dict) else []
                ),
                base_ref=str(base_fact.get("base_ref", "")) if isinstance(base_fact, dict) else "",
                base_commit=base_fact.get("base_commit") if isinstance(base_fact, dict) else {},
                workflow_schema_version=workflow_schema_version,
                normalized_workflow=normalized_workflow,
                normalized_prompt_blobs=normalized_prompt_blobs,
                effective_policy=effective_policy,
                server_policy_revision=server_policy_revision,
                trusted_base_policy_ref=trusted_base_policy_ref,
                budget_policy_ref=budget_policy_ref,
                budget_reset_window_ref=budget_reset_window_ref,
                base_movement_policy=base_movement_policy,
                reducer_version=reducer_version,
                captured_at_ms=work.observed_at_ms,
            )
            input_payload = {
                "trigger_kind": "ADMIT",
                "trigger_id": work.forge_observation_id,
                "facts": {
                    "snapshot_id": snapshot.snapshot_id,
                    "base_observation_id": base.forge_observation_id,
                    "project_id": work.project_id,
                    "work_item_key": work.target_id,
                },
                "prior_state": PRIOR_STATE_NONE,
                "reason_code": "ADMIT",
            }
            transition = self.append_transition(
                run_id=run_id,
                transition_id=transition_id,
                prior_state=PRIOR_STATE_NONE,
                trigger_kind="ADMIT",
                trigger_id=work.forge_observation_id,
                next_state="ADMITTED",
                reducer_version=reducer_version,
                input_digest=request_digest(input_payload),
                specification_generation=0,
                admit_base_observation_id=base.forge_observation_id,
            )
            self._set_run_snapshot_pointers(
                run_id=run_id,
                current_snapshot_id=None,
                pending_snapshot_id=snapshot.snapshot_id,
                supersede_requested=False,
                supersede_requested_transition_sequence=None,
            )
            self._put_admission_pointer_projection(
                run_id=run_id,
                work_item_key=work.target_id,
                snapshot=snapshot,
                transition=transition,
                reducer_version=reducer_version,
            )
            projection_payload = {
                "run_id": run_id,
                "state": "ADMITTED",
                "reason_code": "ADMIT",
                "ready_label": "remove",
                "working_label": "add",
            }
            projection = self.insert_projection_outbox(
                projection_outbox_id=projection_outbox_id,
                run_id=run_id,
                transition_sequence=transition.transition_sequence,
                kind="RUN_STATUS",
                target_kind="WORK_ITEM",
                target_id=work.target_id,
                payload_digest=request_digest(projection_payload),
                payload=projection_payload,
                idempotency_key=projection_idempotency_key,
                next_delivery_at_ms=0,
            )
            self._record_run_observation_consumption(
                run_id=run_id,
                observation_id=work.forge_observation_id,
                transition_id=transition.transition_id,
            )
            self._record_run_observation_consumption(
                run_id=run_id,
                observation_id=base.forge_observation_id,
                transition_id=transition.transition_id,
            )
            return AdmissionResult(
                run_id=run_id,
                snapshot_id=snapshot.snapshot_id,
                transition=transition,
                projection_outbox_id=projection.projection_outbox_id,
            )

    def install_pending_snapshot_generation(
        self,
        *,
        run_id: str,
        transition_id: str,
        fault: FaultInjectionPoint | None = None,
    ) -> SnapshotGenerationRecord:
        from orcest.workflow_reducer.ledger import load_view

        with self.transaction(fault=fault):
            view = load_view(self, run_id)
            if view is None or view.pending_snapshot_id is None:
                raise RunStoreError("run has no pending snapshot to install")
            existing = self.get_transition_by_trigger(
                run_id, "SPEC_SUPERSEDE", view.pending_snapshot_id
            )
            if existing is not None:
                generation = self.conn.execute(
                    "SELECT * FROM snapshot_generations WHERE run_id = ? "
                    "AND installed_transition_sequence = ?",
                    (run_id, existing.transition_sequence),
                ).fetchone()
                if generation is None:
                    raise RunStoreError("snapshot generation missing for replayed transition")
                return _row_to_snapshot_generation(generation)
            snapshot = self.get_work_item_snapshot(view.pending_snapshot_id)
            if snapshot is None:
                raise RunStoreError("pending snapshot row is missing")
            generation = (
                1 if view.specification_generation == 0 else view.specification_generation + 1
            )
            input_payload = {
                "trigger_kind": "SPEC_SUPERSEDE",
                "trigger_id": snapshot.snapshot_id,
                "facts": {},
                "prior_state": view.prior_state,
                "reason_code": "INSTALL_GENERATION"
                if view.state == "ADMITTED" and not view.generation_installed
                else "SPEC_SUPERSEDE",
            }
            transition = self.append_transition(
                run_id=run_id,
                transition_id=transition_id,
                prior_state=view.prior_state,
                trigger_kind="SPEC_SUPERSEDE",
                trigger_id=snapshot.snapshot_id,
                next_state="ADMITTED" if generation == 1 else "REPLANNING",
                reducer_version=view.reducer_version,
                input_digest=request_digest(input_payload),
                specification_generation=generation,
            )
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO snapshot_generations(run_id, specification_generation, "
                "snapshot_id, installed_transition_sequence, installed_at_ms) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, generation, snapshot.snapshot_id, transition.transition_sequence, now),
            )
            self._set_run_snapshot_pointers(
                run_id=run_id,
                current_snapshot_id=snapshot.snapshot_id,
                pending_snapshot_id=None,
                supersede_requested=False,
                supersede_requested_transition_sequence=None,
            )
            stored = self.get_revisioned_object("run_pointers", run_id)
            expected_revision = 0
            pointers: dict[str, Any] = {}
            if stored is not None:
                expected_revision, _, payload_json = stored
                loaded = json.loads(payload_json)
                if isinstance(loaded, dict):
                    pointers = loaded
            pointers.update(
                {
                    "current_snapshot_id": snapshot.snapshot_id,
                    "pending_snapshot_id": None,
                    "supersede_requested": False,
                    "generation_installed": True,
                    "initial_plan_absent": generation == 1,
                    "policy_hash": snapshot.policy_hash,
                    "pending_internal_sequence": transition.transition_sequence,
                    "next_transition_sequence": transition.transition_sequence + 1,
                }
            )
            if generation > 1:
                activity_id = str(uuid.uuid4())
                attempt_id = str(uuid.uuid4())
                outbox_id = str(uuid.uuid4())
                payload = {
                    "activity_id": activity_id,
                    "attempt_id": attempt_id,
                    "generation": 1,
                    "kind": "REPLAN",
                    "snapshot_id": snapshot.snapshot_id,
                    "policy_hash": snapshot.policy_hash,
                }
                self.insert_source_unique_record(
                    source_kind="activity_idempotency",
                    source_id=f"{run_id}/spec-supersede:{snapshot.snapshot_id}:replan",
                    record_kind="activity",
                    record_id=activity_id,
                    payload_digest=request_digest(payload),
                    payload=payload,
                )
                self.insert_outbox(
                    outbox_id=outbox_id,
                    source_kind="ACTIVITY",
                    source_id=activity_id,
                    destination="worker",
                    protocol_version=self._activity_offer_protocol(),
                    payload_digest=request_digest(payload),
                    payload=payload,
                    next_delivery_at_ms=0,
                    attempt_id=attempt_id,
                    attempt_generation=1,
                )
                activities = list(pointers.get("activities", []))
                activities.append(
                    {
                        "activity_id": activity_id,
                        "kind": "REPLAN",
                        "state": "READY",
                        "specification_generation": generation,
                        "candidate_id": None,
                        "current_attempt_id": attempt_id,
                        "current_attempt_state": "OFFERED",
                        "slot": None,
                    }
                )
                pointers["activities"] = activities
                pointers["initial_plan_absent"] = False
                pointers["next_activity_ordinal"] = (
                    int(pointers.get("next_activity_ordinal", 1)) + 1
                )
                pointers["pending_internal_sequence"] = None
            self.put_revisioned_object(
                object_kind="run_pointers",
                object_id=run_id,
                expected_revision=expected_revision,
                payload_digest=request_digest(pointers),
                payload=pointers,
            )
            inserted = self.conn.execute(
                "SELECT * FROM snapshot_generations WHERE run_id = ? "
                "AND specification_generation = ?",
                (run_id, generation),
            ).fetchone()
            assert inserted is not None
            return _row_to_snapshot_generation(inserted)

    def plan_initial_activity(
        self,
        *,
        run_id: str,
        id_factory: Callable[[], str] | None = None,
        fault: FaultInjectionPoint | None = None,
    ) -> Any:
        from orcest.workflow_reducer.ledger import apply, load_view
        from orcest.workflow_reducer.types import Trigger

        with self.transaction(fault=fault):
            view = load_view(self, run_id)
            if view is None or view.pending_internal_sequence is None:
                raise RunStoreError("run has no pending internal planning continuation")
            return apply(
                self,
                view,
                Trigger(kind="INTERNAL", trigger_id=str(view.pending_internal_sequence), facts={}),
                run_id=run_id,
                id_factory=id_factory,
            )

    def _activity_offer_protocol(self) -> str:
        from orcest.workflow_contract.v1.protocol import known_protocol_literals

        matches = sorted(
            literal
            for literal in known_protocol_literals()
            if literal.startswith("orcest.activity-offer/")
        )
        if len(matches) != 1:
            raise RuntimeError(f"expected one activity-offer protocol, got {matches!r}")
        return matches[0]

    def get_workflow_blob(self, blob_digest: str) -> WorkflowBlobSqlRecord | None:
        row = self.conn.execute(
            "SELECT * FROM workflow_blobs WHERE blob_digest = ?", (blob_digest,)
        ).fetchone()
        return None if row is None else _row_to_workflow_blob(row)

    def get_policy_update(self, policy_update_id: str) -> PolicyUpdateRecord | None:
        row = self.conn.execute(
            "SELECT * FROM policy_updates WHERE policy_update_id = ?", (policy_update_id,)
        ).fetchone()
        return None if row is None else _row_to_policy_update(row)

    def get_work_item_snapshot(self, snapshot_id: str) -> WorkItemSnapshotRecord | None:
        row = self.conn.execute(
            "SELECT * FROM work_item_snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        return None if row is None else _row_to_work_item_snapshot(row)

    def _select_admission_base_observation(
        self, work_observation: ForgeObservationRecord
    ) -> ForgeObservationRecord:
        row = self.conn.execute(
            "SELECT * FROM forge_observations WHERE project_id = ? AND target_kind = 'WORK_ITEM' "
            "AND target_id = ? AND kind = 'BASE_HEAD' AND observation_sequence < ? "
            "ORDER BY observation_sequence DESC LIMIT 1",
            (
                work_observation.project_id,
                work_observation.target_id,
                work_observation.observation_sequence,
            ),
        ).fetchone()
        if row is None:
            raise RunStoreError("admission requires a prior accepted BASE_HEAD observation")
        return _row_to_forge_observation(row)

    def _set_run_snapshot_pointers(
        self,
        *,
        run_id: str,
        current_snapshot_id: str | None,
        pending_snapshot_id: str | None,
        supersede_requested: bool,
        supersede_requested_transition_sequence: int | None,
    ) -> None:
        now = _now_ms()
        self.conn.execute(
            "UPDATE runs SET current_snapshot_id = ?, pending_snapshot_id = ?, "
            "supersede_requested = ?, supersede_requested_transition_sequence = ?, "
            "updated_at_ms = ? WHERE run_id = ?",
            (
                current_snapshot_id,
                pending_snapshot_id,
                1 if supersede_requested else 0,
                supersede_requested_transition_sequence,
                now,
                run_id,
            ),
        )

    def _put_admission_pointer_projection(
        self,
        *,
        run_id: str,
        work_item_key: str,
        snapshot: WorkItemSnapshotRecord,
        transition: Transition,
        reducer_version: str,
    ) -> None:
        payload = {
            "current_snapshot_id": None,
            "pending_snapshot_id": snapshot.snapshot_id,
            "supersede_requested": False,
            "current_candidate_id": None,
            "policy_replan_candidate_id": None,
            "publication_id": None,
            "publication_state": None,
            "change_request_external_id": None,
            "next_activity_ordinal": 1,
            "next_transition_sequence": transition.transition_sequence + 1,
            "pending_internal_sequence": None,
            "cancellation_source_kind": None,
            "cancellation_source_id": None,
            "pending_dependency_observation_id": None,
            "panel_staffing_kind": None,
            "latest_staffing_recheck_transition_sequence": None,
            "wait_condition_id": None,
            "wait_reason": None,
            "human_boundary_id": None,
            "human_boundary_reason": None,
            "recovery_origin_state": None,
            "recovery_activity_id": None,
            "recovery_tactic": None,
            "current_recovery_evidence_id": None,
            "offer_permitted": True,
            "safe_boundary": True,
            "generation_installed": False,
            "initial_plan_absent": True,
            "claimed_unfilled_peer": False,
            "panel_complete": False,
            "policy_hash": snapshot.policy_hash,
            "filling_review_slots": [],
            "unfilled_review_slots": [],
            "activities": [],
            "consumed_forge_observation_ids": [
                snapshot.work_item_observation_id,
                snapshot.base_observation_id,
            ],
            "terminal_duplicate_cleanup_active": False,
            "project_id": snapshot.project_id,
            "work_item_key": work_item_key,
            "reducer_version": reducer_version,
        }
        self.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )

    def _record_run_observation_consumption(
        self,
        *,
        run_id: str,
        observation_id: str,
        transition_id: str,
    ) -> None:
        self.insert_source_unique_record(
            source_kind="run_forge_observation",
            source_id=f"{run_id}/{observation_id}",
            record_kind="run_forge_observation",
            record_id=f"{run_id}/{observation_id}",
            payload_digest=request_digest(
                {
                    "run_id": run_id,
                    "observation_id": observation_id,
                    "transition_id": transition_id,
                }
            ),
            payload={
                "run_id": run_id,
                "observation_id": observation_id,
                "transition_id": transition_id,
            },
        )

    def insert_immutable_fact(
        self,
        *,
        fact_kind: str,
        fact_id: str,
        payload_digest: str,
        payload: Any,
        source_kind: str | None = None,
        source_id: str | None = None,
    ) -> ImmutableFact:
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM immutable_facts WHERE fact_kind = ? AND fact_id = ?",
            (fact_kind, fact_id),
        ).fetchone()
        if existing is not None:
            row = _row_to_fact(existing)
            if (
                row.payload_digest == payload_digest
                and row.payload_json == payload_json
                and row.source_kind == source_kind
                and row.source_id == source_id
            ):
                return row
            raise IdempotencyConflictError("immutable fact id was reused with different content")
        source_existing = None
        if source_kind is not None and source_id is not None:
            source_existing = self.conn.execute(
                "SELECT * FROM immutable_facts WHERE source_kind = ? AND source_id = ?",
                (source_kind, source_id),
            ).fetchone()
        if source_existing is not None:
            row = _row_to_fact(source_existing)
            if (
                row.payload_digest == payload_digest
                and row.payload_json == payload_json
                and row.fact_kind == fact_kind
                and row.fact_id == fact_id
            ):
                return row
            raise IdempotencyConflictError("source identity already produced a different fact")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO immutable_facts(fact_kind, fact_id, payload_digest, payload_json, "
            "source_kind, source_id, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (fact_kind, fact_id, payload_digest, payload_json, source_kind, source_id, now),
        )
        row = self.conn.execute(
            "SELECT * FROM immutable_facts WHERE fact_kind = ? AND fact_id = ?",
            (fact_kind, fact_id),
        ).fetchone()
        assert row is not None
        return _row_to_fact(row)

    def insert_source_unique_record(
        self,
        *,
        source_kind: str,
        source_id: str,
        record_kind: str,
        record_id: str,
        payload_digest: str,
        payload: Any,
    ) -> SourceUniqueRecord:
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM source_unique_records WHERE source_kind = ? AND source_id = ?",
            (source_kind, source_id),
        ).fetchone()
        if existing is not None:
            row = _row_to_source_record(existing)
            if (
                row.record_kind == record_kind
                and row.record_id == record_id
                and row.payload_digest == payload_digest
                and row.payload_json == payload_json
            ):
                return row
            raise IdempotencyConflictError("source identity was reused with different content")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO source_unique_records(source_kind, source_id, record_kind, record_id, "
            "payload_digest, payload_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (source_kind, source_id, record_kind, record_id, payload_digest, payload_json, now),
        )
        row = self.conn.execute(
            "SELECT * FROM source_unique_records WHERE source_kind = ? AND source_id = ?",
            (source_kind, source_id),
        ).fetchone()
        assert row is not None
        return _row_to_source_record(row)

    def create_activity(
        self,
        *,
        activity_id: str,
        run_id: str,
        activity_ordinal: int,
        specification_generation: int,
        policy_hash: str,
        kind: str,
        execution_class: str,
        state: str,
        created_transition_sequence: int,
        semantic_input: Any,
        semantic_input_digest: str,
        idempotency_key: str,
        candidate_id: str | None = None,
        forge_observation_id: str | None = None,
        role: str | None = None,
        repair_cycle: int = 0,
        recovery_cycle: int = 0,
        strategy_index: int = 0,
        recovery_tactic: str | None = None,
        recovery_evidence_id: str | None = None,
        rescue_epoch: int = 0,
        slot: str | None = None,
        review_assignment: ActivityReviewAssignmentInput | None = None,
        attempt: AttemptOfferInput | None = None,
        outbox_id: str | None = None,
        outbox_destination: str | None = None,
        outbox_protocol_version: str | None = None,
    ) -> tuple[ActivityRecord, AttemptRecord | None, OutboxRecord | None]:
        """Durably create one Activity, its optional Review Assignment, and optional offer.

        When ``attempt`` is given, the ``OFFERED`` Attempt and its dispatch
        Outbox row commit in the same transaction as the Activity (and its
        Review Assignment/subject/finding memberships, when given) -- the
        "plan before dispatch" invariant. Replaying with the same
        ``(run_id, idempotency_key)`` and identical content returns the
        already-committed Activity unchanged; different content is a
        conflict.
        """
        require_lowercase_uuid(activity_id, field="activity_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("activity.kind", kind)
        enums.parse_enum("activity.execution_class", execution_class)
        enums.parse_enum("activity.state", state)
        semantic_input_json = _require_json_text(semantic_input)
        if review_assignment is not None:
            enums.parse_enum(
                "activity_review_assignment.assignment_kind", review_assignment.assignment_kind
            )
            if review_assignment.assignment_kind != kind:
                raise ValueError("review_assignment.assignment_kind must equal Activity.kind")
            if review_assignment.role != role:
                raise ValueError("review_assignment.role must equal Activity.role")
        if attempt is not None and attempt.generation != 1:
            raise ValueError("create_activity only offers the first Attempt generation")
        if attempt is not None and outbox_id is None:
            raise ValueError("outbox_id is required whenever attempt is given")
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM activities WHERE run_id = ? AND idempotency_key = ?",
                (run_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                record = _row_to_activity(existing)
                if (
                    record.activity_id != activity_id
                    or record.activity_ordinal != activity_ordinal
                    or record.specification_generation != specification_generation
                    or record.policy_hash != policy_hash
                    or record.kind != kind
                    or record.execution_class != execution_class
                    or record.state != state
                    or record.candidate_id != candidate_id
                    or record.forge_observation_id != forge_observation_id
                    or record.role != role
                    or record.repair_cycle != repair_cycle
                    or record.recovery_cycle != recovery_cycle
                    or record.strategy_index != strategy_index
                    or record.recovery_tactic != recovery_tactic
                    or record.recovery_evidence_id != recovery_evidence_id
                    or record.rescue_epoch != rescue_epoch
                    or record.slot != slot
                    or record.semantic_input_json != semantic_input_json
                    or record.semantic_input_digest != semantic_input_digest
                    or record.created_transition_sequence != created_transition_sequence
                ):
                    raise IdempotencyConflictError(
                        "activity idempotency_key was reused with different content"
                    )
                existing_assignment = self._get_activity_review_assignment(record.activity_id)
                if (existing_assignment is None) != (review_assignment is None):
                    raise IdempotencyConflictError(
                        "activity idempotency_key was reused with different content"
                    )
                if (
                    existing_assignment is not None
                    and review_assignment is not None
                    and existing_assignment.assignment_digest
                    != _review_assignment_digest_for_input(review_assignment)
                ):
                    raise IdempotencyConflictError(
                        "activity idempotency_key was reused with different content"
                    )
                existing_attempt_row = self.conn.execute(
                    "SELECT * FROM attempts WHERE activity_id = ? ORDER BY generation DESC LIMIT 1",
                    (record.activity_id,),
                ).fetchone()
                existing_attempt = (
                    _row_to_attempt(existing_attempt_row)
                    if existing_attempt_row is not None
                    else None
                )
                if (existing_attempt is None) != (attempt is None):
                    raise IdempotencyConflictError(
                        "activity idempotency_key was reused with different content"
                    )
                if (
                    existing_attempt is not None
                    and attempt is not None
                    and (
                        existing_attempt.attempt_id,
                        existing_attempt.generation,
                        existing_attempt.protocol_version,
                        existing_attempt.execution_profile_id,
                        existing_attempt.worker_profile,
                        existing_attempt.provider,
                        existing_attempt.model,
                        existing_attempt.provider_account_ref,
                        existing_attempt.provider_family,
                        existing_attempt.model_family,
                        existing_attempt.classification_revision,
                        existing_attempt.offered_at_ms,
                        existing_attempt.claim_timeout_ms,
                    )
                    != (
                        attempt.attempt_id,
                        attempt.generation,
                        attempt.protocol_version,
                        attempt.execution_profile_id,
                        attempt.worker_profile,
                        attempt.provider,
                        attempt.model,
                        attempt.provider_account_ref,
                        attempt.provider_family,
                        attempt.model_family,
                        attempt.classification_revision,
                        attempt.offered_at_ms,
                        attempt.claim_timeout_ms,
                    )
                ):
                    raise IdempotencyConflictError(
                        "activity idempotency_key was reused with different content"
                    )
                return (
                    dataclasses.replace(record, review_assignment=existing_assignment),
                    existing_attempt,
                    None,
                )
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO activities(activity_id, run_id, activity_ordinal, "
                "specification_generation, policy_hash, kind, execution_class, state, "
                "input_ref_json, candidate_id, forge_observation_id, "
                "change_request_head_observation_id, observed_change_request_head_json, role, "
                "repair_cycle, recovery_cycle, strategy_index, recovery_tactic, "
                "recovery_evidence_id, rescue_epoch, created_transition_sequence, "
                "semantic_input_json, semantic_input_digest, idempotency_key, slot, "
                "created_at_ms, updated_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?)",
                (
                    activity_id,
                    run_id,
                    activity_ordinal,
                    specification_generation,
                    policy_hash,
                    kind,
                    execution_class,
                    state,
                    candidate_id,
                    forge_observation_id,
                    role,
                    repair_cycle,
                    recovery_cycle,
                    strategy_index,
                    recovery_tactic,
                    recovery_evidence_id,
                    rescue_epoch,
                    created_transition_sequence,
                    semantic_input_json,
                    semantic_input_digest,
                    idempotency_key,
                    slot,
                    now,
                    now,
                ),
            )
            assignment_record = None
            if review_assignment is not None:
                assignment_record = self._insert_activity_review_assignment(
                    activity_id=activity_id, assignment=review_assignment, now_ms=now
                )
            attempt_record = None
            outbox_record = None
            if attempt is not None:
                assert outbox_id is not None
                attempt_record = self._insert_attempt_row(activity_id=activity_id, offer=attempt)
                outbox_record = self._insert_activity_offer_outbox(
                    outbox_id=outbox_id,
                    activity_id=activity_id,
                    attempt=attempt_record,
                    destination=outbox_destination or f"activity-offer/1/{attempt.worker_profile}",
                    protocol_version=outbox_protocol_version or attempt.protocol_version,
                )
            row = self.conn.execute(
                "SELECT * FROM activities WHERE activity_id = ?", (activity_id,)
            ).fetchone()
            assert row is not None
            return (
                _row_to_activity(row, review_assignment=assignment_record),
                (attempt_record),
                outbox_record,
            )

    def create_next_attempt(
        self,
        *,
        activity_id: str,
        prior_attempt_terminal_state: str,
        offer: AttemptOfferInput,
        outbox_id: str,
        outbox_destination: str | None = None,
        outbox_protocol_version: str | None = None,
    ) -> tuple[AttemptRecord, OutboxRecord]:
        """Terminalize the current generation and atomically offer ``g + 1``.

        Only one nonterminal Attempt may exist per Activity; the caller
        supplies the exact terminal state (``FAILED``, ``ABSTAINED``,
        ``EXPIRED``, or ``SUPERSEDED``) the prior generation already reduced
        to before this call, and this method fences the write on that prior
        generation having actually reduced to that exact state in storage.
        """
        require_lowercase_uuid(activity_id, field="activity_id")
        if prior_attempt_terminal_state not in {"FAILED", "ABSTAINED", "EXPIRED", "SUPERSEDED"}:
            raise ValueError("prior_attempt_terminal_state must be a closed terminal Attempt state")
        with self.transaction():
            prior = self.conn.execute(
                "SELECT * FROM attempts WHERE activity_id = ? ORDER BY generation DESC LIMIT 1",
                (activity_id,),
            ).fetchone()
            if prior is None:
                raise RunStoreError(f"activity {activity_id!r} has no prior Attempt")
            if int(prior["generation"]) != offer.generation - 1:
                raise CasMismatchError("create_next_attempt generation is not the successor")
            if prior["state"] in {"OFFERED", "CLAIMED"}:
                raise CasMismatchError(
                    "prior Attempt generation is still nonterminal; terminalize it first"
                )
            if prior["state"] != prior_attempt_terminal_state:
                raise CasMismatchError(
                    "prior_attempt_terminal_state does not match the prior Attempt's actual "
                    f"terminal state ({prior['state']!r})"
                )
            attempt_record = self._insert_attempt_row(activity_id=activity_id, offer=offer)
            outbox_record = self._insert_activity_offer_outbox(
                outbox_id=outbox_id,
                activity_id=activity_id,
                attempt=attempt_record,
                destination=outbox_destination or f"activity-offer/1/{offer.worker_profile}",
                protocol_version=outbox_protocol_version or offer.protocol_version,
            )
            return attempt_record, outbox_record

    def _insert_attempt_row(self, *, activity_id: str, offer: AttemptOfferInput) -> AttemptRecord:
        require_lowercase_uuid(offer.attempt_id, field="attempt_id")
        claim_deadline_ms = offer.offered_at_ms + offer.claim_timeout_ms
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO attempts(attempt_id, activity_id, generation, state, "
            "protocol_version, execution_profile_id, worker_profile, provider, model, "
            "provider_account_ref, provider_family, model_family, classification_revision, "
            "offered_at_ms, claim_timeout_ms, claim_deadline_ms, created_at_ms) "
            "VALUES (?, ?, ?, 'OFFERED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                offer.attempt_id,
                activity_id,
                offer.generation,
                offer.protocol_version,
                offer.execution_profile_id,
                offer.worker_profile,
                offer.provider,
                offer.model,
                offer.provider_account_ref,
                offer.provider_family,
                offer.model_family,
                offer.classification_revision,
                offer.offered_at_ms,
                offer.claim_timeout_ms,
                claim_deadline_ms,
                now,
            ),
        )
        row = self.conn.execute(
            "SELECT * FROM attempts WHERE attempt_id = ?", (offer.attempt_id,)
        ).fetchone()
        assert row is not None
        return _row_to_attempt(row)

    def _insert_activity_offer_outbox(
        self,
        *,
        outbox_id: str,
        activity_id: str,
        attempt: AttemptRecord,
        destination: str,
        protocol_version: str,
    ) -> OutboxRecord:
        payload = {
            "outbox_id": outbox_id,
            "attempt_id": attempt.attempt_id,
            "activity_id": activity_id,
            "generation": attempt.generation,
            "worker_profile": attempt.worker_profile,
            "claim_deadline_ms": attempt.claim_deadline_ms,
        }
        return self.insert_outbox(
            outbox_id=outbox_id,
            source_kind="ACTIVITY",
            source_id=activity_id,
            destination=destination,
            protocol_version=protocol_version,
            payload_digest=request_digest(payload),
            payload=payload,
            next_delivery_at_ms=attempt.offered_at_ms,
            attempt_id=attempt.attempt_id,
            attempt_generation=attempt.generation,
        )

    def _insert_activity_review_assignment(
        self,
        *,
        activity_id: str,
        assignment: ActivityReviewAssignmentInput,
        now_ms: int,
    ) -> ActivityReviewAssignmentRecord:
        subject_refs_digest_value = subject_refs_digest(assignment.subject_refs)
        disputed_digest_value = (
            bare_canonical_digest(list(assignment.disputed_finding_ids))
            if assignment.disputed_finding_ids
            else None
        )
        assignment_digest_value = _review_assignment_digest_for_input(assignment)
        self.conn.execute(
            "INSERT INTO activity_review_assignments(activity_id, assignment_kind, panel_round, "
            "reviewer_slot, adjudication_round, adjudicator_slot, role, subject_refs_digest, "
            "context_digest, disputed_finding_ids_digest, assignment_digest, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                activity_id,
                assignment.assignment_kind,
                assignment.panel_round,
                assignment.reviewer_slot,
                assignment.adjudication_round,
                assignment.adjudicator_slot,
                assignment.role,
                subject_refs_digest_value,
                assignment.context_digest,
                disputed_digest_value,
                assignment_digest_value,
                now_ms,
            ),
        )
        for ordinal, subject_ref in enumerate(assignment.subject_refs):
            self.conn.execute(
                "INSERT INTO activity_review_subjects(activity_id, subject_ordinal, subject_ref) "
                "VALUES (?, ?, ?)",
                (activity_id, ordinal, subject_ref),
            )
        for ordinal, finding_id in enumerate(assignment.disputed_finding_ids):
            self.conn.execute(
                "INSERT INTO activity_adjudication_findings"
                "(activity_id, finding_ordinal, finding_id) VALUES (?, ?, ?)",
                (activity_id, ordinal, finding_id),
            )
        row = self.conn.execute(
            "SELECT * FROM activity_review_assignments WHERE activity_id = ?", (activity_id,)
        ).fetchone()
        assert row is not None
        return _row_to_activity_review_assignment(
            row,
            subject_refs=tuple(assignment.subject_refs),
            disputed_finding_ids=tuple(assignment.disputed_finding_ids),
        )

    def _get_activity_review_assignment(
        self, activity_id: str
    ) -> ActivityReviewAssignmentRecord | None:
        row = self.conn.execute(
            "SELECT * FROM activity_review_assignments WHERE activity_id = ?", (activity_id,)
        ).fetchone()
        if row is None:
            return None
        subject_rows = self.conn.execute(
            "SELECT subject_ref FROM activity_review_subjects "
            "WHERE activity_id = ? ORDER BY subject_ordinal",
            (activity_id,),
        ).fetchall()
        finding_rows = self.conn.execute(
            "SELECT finding_id FROM activity_adjudication_findings "
            "WHERE activity_id = ? ORDER BY finding_ordinal",
            (activity_id,),
        ).fetchall()
        return _row_to_activity_review_assignment(
            row,
            subject_refs=tuple(r["subject_ref"] for r in subject_rows),
            disputed_finding_ids=tuple(r["finding_id"] for r in finding_rows),
        )

    def get_activity(self, activity_id: str) -> ActivityRecord | None:
        row = self.conn.execute(
            "SELECT * FROM activities WHERE activity_id = ?", (activity_id,)
        ).fetchone()
        if row is None:
            return None
        return _row_to_activity(
            row, review_assignment=self._get_activity_review_assignment(activity_id)
        )

    def get_attempt(self, attempt_id: str) -> AttemptRecord | None:
        row = self.conn.execute(
            "SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return None if row is None else _row_to_attempt(row)

    def get_candidate_upload(self, upload_id: str) -> CandidateUploadRecord | None:
        require_lowercase_uuid(upload_id, field="upload_id")
        row = self.conn.execute(
            "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
        ).fetchone()
        return None if row is None else _row_to_candidate_upload(row)

    def create_candidate_upload(
        self,
        *,
        upload_id: str,
        attempt_id: str,
        activity_id: str,
        generation: int,
        idempotency_key: str,
        request_digest: str,
        media_type: str,
        declared_bytes: int,
        expected_bundle_digest: str,
        expected_base_commit: Mapping[str, Any],
        expected_repository_external_id: str,
        expires_at_ms: int,
        expected_snapshot_id: str | None = None,
    ) -> CandidateUploadRecord:
        require_lowercase_uuid(upload_id, field="upload_id")
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(activity_id, field="activity_id")
        require_lowercase_uuid(idempotency_key, field="idempotency_key")
        if expected_snapshot_id is not None:
            require_lowercase_uuid(expected_snapshot_id, field="expected_snapshot_id")
        _require_positive_int(generation, field="generation")
        _require_positive_int(declared_bytes, field="declared_bytes")
        _require_digest(request_digest, field="request_digest")
        _require_digest(expected_bundle_digest, field="expected_bundle_digest")
        base = _require_git_commit_ref(expected_base_commit, field="expected_base_commit")
        _require_nonempty_text(
            expected_repository_external_id, field="expected_repository_external_id"
        )
        if media_type != "application/x-git-bundle":
            raise ValueError("Candidate uploads require application/x-git-bundle")
        if not isinstance(expires_at_ms, int) or expires_at_ms < 0:
            raise ValueError("expires_at_ms must be a nonnegative integer")
        now = _now_ms()
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM candidate_uploads WHERE attempt_id = ? AND idempotency_key = ?",
                (attempt_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                record = _row_to_candidate_upload(existing)
                if (
                    record.upload_id != upload_id
                    or record.activity_id != activity_id
                    or record.attempt_generation != generation
                    or record.request_digest != request_digest
                    or record.declared_bytes != declared_bytes
                    or record.expected_bundle_digest != expected_bundle_digest
                    or record.expected_base_commit_json != base.as_json()
                    or record.expected_repository_external_id != expected_repository_external_id
                    or record.expected_snapshot_id != expected_snapshot_id
                    or record.expires_at_ms != expires_at_ms
                ):
                    raise IdempotencyConflictError("candidate upload create key was reused")
                return record
            row = self.conn.execute(
                "SELECT attempts.*, activities.kind AS activity_kind, "
                "activities.execution_class AS execution_class, "
                "runs.current_snapshot_id AS current_snapshot_id "
                "FROM attempts JOIN activities ON activities.activity_id = attempts.activity_id "
                "JOIN runs ON runs.run_id = activities.run_id "
                "WHERE attempts.attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if row is None:
                raise RunStoreError(f"attempt {attempt_id!r} was not found")
            if row["activity_id"] != activity_id or row["generation"] != generation:
                raise CasMismatchError("candidate upload does not match attempt")
            if row["state"] != "CLAIMED":
                raise CasMismatchError("candidate upload requires a claimed attempt")
            if row["activity_kind"] not in {"BUILD", "REMEDIATE", "REBASE", "PR_REMEDIATE"}:
                raise CasMismatchError("activity kind cannot produce a Candidate")
            if expected_snapshot_id is not None:
                if row["current_snapshot_id"] != expected_snapshot_id:
                    raise CasMismatchError("candidate upload snapshot binding is not current")
                snapshot = self.conn.execute(
                    "SELECT project_id, base_commit_json FROM work_item_snapshots "
                    "WHERE snapshot_id = ?",
                    (expected_snapshot_id,),
                ).fetchone()
                if snapshot is None:
                    raise CasMismatchError("candidate upload snapshot binding is missing")
                project = self.conn.execute(
                    "SELECT repository_external_id FROM projects WHERE project_id = ?",
                    (snapshot["project_id"],),
                ).fetchone()
                if project is None:
                    raise CasMismatchError("candidate upload snapshot project is missing")
                if (
                    snapshot["base_commit_json"] != base.as_json()
                    or project["repository_external_id"] != expected_repository_external_id
                ):
                    raise CasMismatchError("candidate upload does not match snapshot binding")
            if row["execution_deadline_ms"] is None or expires_at_ms > row["execution_deadline_ms"]:
                raise CasMismatchError("candidate upload expiry exceeds execution deadline")
            if now >= expires_at_ms:
                raise CasMismatchError("candidate upload is already expired")
            self.conn.execute(
                "INSERT INTO candidate_uploads(upload_id, attempt_id, activity_id, "
                "attempt_generation, idempotency_key, request_digest, media_type, "
                "declared_bytes, expected_bundle_digest, expected_base_commit_json, "
                "expected_repository_external_id, expected_snapshot_id, state, expires_at_ms, "
                "created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "'RECEIVING', ?, ?, ?)",
                (
                    upload_id,
                    attempt_id,
                    activity_id,
                    generation,
                    idempotency_key,
                    request_digest,
                    media_type,
                    declared_bytes,
                    expected_bundle_digest,
                    base.as_json(),
                    expected_repository_external_id,
                    expected_snapshot_id,
                    expires_at_ms,
                    now,
                    now,
                ),
            )
            inserted = self.conn.execute(
                "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            assert inserted is not None
            return _row_to_candidate_upload(inserted)

    def candidate_upload_expired_body(self, upload: CandidateUploadRecord) -> dict[str, Any]:
        return {
            "protocol": CANDIDATE_UPLOAD_EXPIRED_PROTOCOL,
            "upload_id": upload.upload_id,
            "state": "EXPIRED",
            "code": "UPLOAD_EXPIRED",
            "expires_at_ms": upload.expires_at_ms,
        }

    def put_candidate_upload_content(
        self,
        *,
        candidate_store: Any,
        upload_id: str,
        bundle_bytes: bytes,
        now_ms: int | None = None,
    ) -> CandidateUploadRecord:
        require_lowercase_uuid(upload_id, field="upload_id")
        now = _now_ms() if now_ms is None else now_ms
        existing = self.get_candidate_upload(upload_id)
        if existing is None:
            raise RunStoreError(f"candidate upload {upload_id!r} was not found")
        if now >= existing.expires_at_ms:
            return self.expire_candidate_upload(
                upload_id, candidate_store=candidate_store, now_ms=now
            )
        if existing.state in {"VALIDATED", "PROMOTED", "CONSUMED"}:
            digest = candidate_store.identity(bundle_bytes).bundle_digest
            if digest != existing.computed_bundle_digest:
                raise IdempotencyConflictError("candidate upload content changed")
            return existing
        if existing.state == "EXPIRED":
            return existing
        if existing.state != "RECEIVING":
            raise CasMismatchError("candidate upload is not receiving content")
        if len(bundle_bytes) != existing.declared_bytes:
            raise CasMismatchError("candidate upload byte length mismatch")
        staged_path, staged = candidate_store.stage_upload_bytes(bundle_bytes)
        if staged.bundle_digest != existing.expected_bundle_digest:
            candidate_store.discard_staged(staged_path)
            raise CasMismatchError("candidate upload digest mismatch")
        try:
            tip = _validate_candidate_bundle(
                candidate_store._incoming_path(staged_path),
                expected_base_commit=_require_git_commit_ref(
                    existing.expected_base_commit, field="expected_base_commit"
                ),
                expected_repository_external_id=existing.expected_repository_external_id,
            )
            with self.transaction():
                row = self.conn.execute(
                    "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
                ).fetchone()
                if row is None:
                    raise RunStoreError(f"candidate upload {upload_id!r} was not found")
                current = _row_to_candidate_upload(row)
                if now >= current.expires_at_ms:
                    self._expire_candidate_upload_row(
                        current, candidate_store=candidate_store, now_ms=now
                    )
                    candidate_store.discard_staged(staged_path)
                    refreshed = self.conn.execute(
                        "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
                    ).fetchone()
                    assert refreshed is not None
                    return _row_to_candidate_upload(refreshed)
                if current.state != "RECEIVING":
                    raise IdempotencyConflictError("candidate upload content race lost")
                self.conn.execute(
                    "UPDATE candidate_uploads SET state = 'VALIDATED', incoming_path = ?, "
                    "computed_bundle_digest = ?, computed_bytes = ?, verified_tip_json = ?, "
                    "updated_at_ms = ? WHERE upload_id = ? AND state = 'RECEIVING'",
                    (
                        staged_path,
                        staged.bundle_digest,
                        staged.byte_length,
                        tip.as_json(),
                        now,
                        upload_id,
                    ),
                )
                updated = self.conn.execute(
                    "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
                ).fetchone()
                assert updated is not None
                return _row_to_candidate_upload(updated)
        except Exception:
            candidate_store.discard_staged(staged_path)
            raise

    def expire_candidate_upload(
        self, upload_id: str, *, candidate_store: Any, now_ms: int | None = None
    ) -> CandidateUploadRecord:
        require_lowercase_uuid(upload_id, field="upload_id")
        now = _now_ms() if now_ms is None else now_ms
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            if row is None:
                raise RunStoreError(f"candidate upload {upload_id!r} was not found")
            upload = _row_to_candidate_upload(row)
            if upload.state in {"CONSUMED", "EXPIRED"}:
                return upload
            if now < upload.expires_at_ms:
                raise CasMismatchError("candidate upload has not expired")
            self._expire_candidate_upload_row(upload, candidate_store=candidate_store, now_ms=now)
            updated = self.conn.execute(
                "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            assert updated is not None
            return _row_to_candidate_upload(updated)

    def _expire_candidate_upload_row(
        self,
        upload: CandidateUploadRecord,
        *,
        candidate_store: Any,
        now_ms: int,
    ) -> None:
        if upload.incoming_path is not None:
            candidate_store.discard_staged(upload.incoming_path)
        self.conn.execute(
            "UPDATE candidate_uploads SET state = 'EXPIRED', incoming_path = NULL, "
            "computed_bundle_digest = NULL, computed_bytes = NULL, verified_tip_json = NULL, "
            "artifact_bundle_digest = NULL, artifact_storage_key = NULL, promoted_at_ms = NULL, "
            "consumed_candidate_id = NULL, updated_at_ms = ? "
            "WHERE upload_id = ? AND state != 'CONSUMED'",
            (now_ms, upload.upload_id),
        )

    def promote_candidate_upload(
        self,
        *,
        candidate_store: Any,
        upload_id: str,
        now_ms: int | None = None,
    ) -> CandidateUploadRecord:
        upload = self.get_candidate_upload(upload_id)
        if upload is None:
            raise RunStoreError(f"candidate upload {upload_id!r} was not found")
        now = _now_ms() if now_ms is None else now_ms
        if upload.state == "PROMOTED":
            return upload
        if now >= upload.expires_at_ms:
            return self.expire_candidate_upload(
                upload_id, candidate_store=candidate_store, now_ms=now
            )
        if upload.state != "VALIDATED" or upload.incoming_path is None:
            raise CasMismatchError("candidate upload is not validated")
        if upload.computed_bundle_digest is None or upload.computed_bytes is None:
            raise CasMismatchError("validated candidate upload is missing computed identity")
        expected = candidate_store.identity(candidate_store.read_staged(upload.incoming_path))
        if (
            expected.bundle_digest != upload.computed_bundle_digest
            or expected.byte_length != upload.computed_bytes
        ):
            raise CasMismatchError("validated candidate upload staged bytes changed")

        promoted: CandidateUploadRecord | None = None

        def reference(record: Any) -> None:
            nonlocal promoted
            with self.transaction():
                row = self.conn.execute(
                    "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
                ).fetchone()
                if row is None:
                    raise RunStoreError(f"candidate upload {upload_id!r} was not found")
                current = _row_to_candidate_upload(row)
                if now >= current.expires_at_ms:
                    self._expire_candidate_upload_row(
                        current, candidate_store=candidate_store, now_ms=now
                    )
                    expired_row = self.conn.execute(
                        "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
                    ).fetchone()
                    assert expired_row is not None
                    promoted = _row_to_candidate_upload(expired_row)
                    raise _CandidateUploadExpiredDuringPromotion
                self.conn.execute(
                    "INSERT OR IGNORE INTO artifact_objects(bundle_digest, storage_key, "
                    "byte_length, installed_at_ms) VALUES (?, ?, ?, ?)",
                    (record.bundle_digest, record.storage_key, record.byte_length, now),
                )
                self.conn.execute(
                    "UPDATE candidate_uploads SET state = 'PROMOTED', "
                    "artifact_bundle_digest = ?, artifact_storage_key = ?, promoted_at_ms = ?, "
                    "updated_at_ms = ? WHERE upload_id = ? AND state = 'VALIDATED'",
                    (record.bundle_digest, record.storage_key, now, now, upload_id),
                )
                updated = self.conn.execute(
                    "SELECT * FROM candidate_uploads WHERE upload_id = ?", (upload_id,)
                ).fetchone()
                assert updated is not None
                promoted = _row_to_candidate_upload(updated)

        try:
            candidate_store.promote_staged_with_reference(
                upload.incoming_path, expected, reference=reference
            )
        except _CandidateUploadExpiredDuringPromotion:
            assert promoted is not None
            return promoted
        assert promoted is not None
        return promoted

    def admit_controller_import_candidate(
        self,
        *,
        candidate_store: Any,
        candidate_id: str,
        controller_operation_fact_id: str,
        activity_id: str,
        forge_observation_id: str,
        operation_digest: str,
        fact_digest: str,
        bundle_bytes: bytes,
        expected_base_commit: Mapping[str, Any],
        expected_repository_external_id: str,
        now_ms: int | None = None,
    ) -> tuple[CandidateRecord, ControllerOperationFactRecord]:
        require_lowercase_uuid(candidate_id, field="candidate_id")
        require_lowercase_uuid(controller_operation_fact_id, field="controller_operation_fact_id")
        require_lowercase_uuid(activity_id, field="activity_id")
        _require_nonempty_text(forge_observation_id, field="forge_observation_id")
        _require_digest(operation_digest, field="operation_digest")
        _require_digest(fact_digest, field="fact_digest")
        base = _require_git_commit_ref(expected_base_commit, field="expected_base_commit")
        _require_nonempty_text(
            expected_repository_external_id, field="expected_repository_external_id"
        )
        staged_path, staged = candidate_store.stage_upload_bytes(bundle_bytes)
        try:
            tip = _validate_candidate_bundle(
                candidate_store._incoming_path(staged_path),
                expected_base_commit=base,
                expected_repository_external_id=expected_repository_external_id,
            )
            now = _now_ms() if now_ms is None else now_ms
            candidate: CandidateRecord | None = None
            fact: ControllerOperationFactRecord | None = None

            def reference(record: Any) -> None:
                nonlocal candidate, fact
                with self.transaction():
                    existing_fact = self.conn.execute(
                        "SELECT * FROM controller_operation_facts WHERE "
                        "controller_operation_fact_id = ?",
                        (controller_operation_fact_id,),
                    ).fetchone()
                    if existing_fact is not None:
                        fact = _row_to_controller_operation_fact(existing_fact)
                        existing_candidate = self.get_candidate(candidate_id)
                        if existing_candidate is None:
                            raise RunStoreError("controller import fact has no Candidate")
                        candidate = existing_candidate
                        return
                    activity = self.get_activity(activity_id)
                    if activity is None:
                        raise RunStoreError(f"activity {activity_id!r} was not found")
                    if activity.execution_class != "CONTROLLER" or activity.kind != "IMPORT":
                        raise CasMismatchError("controller import requires an IMPORT Activity")
                    if activity.state not in {"READY", "ACTIVE"}:
                        raise CasMismatchError("controller import Activity is not current")
                    run = self.get_run(activity.run_id)
                    if run is None:
                        raise RunStoreError(f"run {activity.run_id!r} was not found")
                    generation = self.conn.execute(
                        "SELECT COALESCE(MAX(candidate_generation), 0) + 1 "
                        "FROM candidates WHERE run_id = ?",
                        (activity.run_id,),
                    ).fetchone()[0]
                    self.conn.execute(
                        "INSERT OR IGNORE INTO artifact_objects(bundle_digest, storage_key, "
                        "byte_length, installed_at_ms) VALUES (?, ?, ?, ?)",
                        (record.bundle_digest, record.storage_key, record.byte_length, now),
                    )
                    self.conn.execute(
                        "INSERT INTO candidates(candidate_id, run_id, candidate_generation, "
                        "provenance_kind, producing_activity_id, import_forge_observation_id, "
                        "object_format, oid, base_commit_json, bundle_digest, created_at_ms) "
                        "VALUES (?, ?, ?, 'FORGE_IMPORT', ?, ?, ?, ?, ?, ?, ?)",
                        (
                            candidate_id,
                            activity.run_id,
                            generation,
                            activity_id,
                            forge_observation_id,
                            tip.object_format,
                            tip.oid,
                            base.as_json(),
                            record.bundle_digest,
                            now,
                        ),
                    )
                    self.conn.execute(
                        "INSERT INTO controller_operation_facts("
                        "controller_operation_fact_id, activity_id, operation_kind, outcome, "
                        "candidate_id, forge_observation_id, operation_digest, fact_digest, "
                        "recorded_at_ms) VALUES (?, ?, 'IMPORT', 'SUCCEEDED', ?, ?, ?, ?, ?)",
                        (
                            controller_operation_fact_id,
                            activity_id,
                            candidate_id,
                            forge_observation_id,
                            operation_digest,
                            fact_digest,
                            now,
                        ),
                    )
                    self.conn.execute(
                        "UPDATE activities SET state = 'SUCCEEDED', updated_at_ms = ? "
                        "WHERE activity_id = ?",
                        (now, activity_id),
                    )
                    candidate_row = self.conn.execute(
                        "SELECT * FROM candidates WHERE candidate_id = ?", (candidate_id,)
                    ).fetchone()
                    fact_row = self.conn.execute(
                        "SELECT * FROM controller_operation_facts "
                        "WHERE controller_operation_fact_id = ?",
                        (controller_operation_fact_id,),
                    ).fetchone()
                    assert candidate_row is not None
                    assert fact_row is not None
                    candidate = _row_to_candidate(candidate_row)
                    fact = _row_to_controller_operation_fact(fact_row)

            candidate_store.promote_staged_with_reference(staged_path, staged, reference=reference)
            assert candidate is not None
            assert fact is not None
            return candidate, fact
        except Exception:
            candidate_store.discard_staged(staged_path)
            raise

    def get_candidate(self, candidate_id: str) -> CandidateRecord | None:
        require_lowercase_uuid(candidate_id, field="candidate_id")
        row = self.conn.execute(
            "SELECT * FROM candidates WHERE candidate_id = ?", (candidate_id,)
        ).fetchone()
        return None if row is None else _row_to_candidate(row)

    def get_candidate_download_for_attempt(
        self, *, attempt_id: str, candidate_id: str
    ) -> CandidateDownloadRecord:
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(candidate_id, field="candidate_id")
        row = self.conn.execute(
            "SELECT candidates.*, artifact_objects.storage_key AS artifact_storage_key, "
            "artifact_objects.byte_length AS artifact_byte_length, "
            "artifact_objects.installed_at_ms AS artifact_installed_at_ms "
            "FROM attempts "
            "JOIN activities ON activities.activity_id = attempts.activity_id "
            "JOIN candidates ON candidates.candidate_id = activities.candidate_id "
            "JOIN artifact_objects ON artifact_objects.bundle_digest = candidates.bundle_digest "
            "WHERE attempts.attempt_id = ? AND candidates.candidate_id = ? "
            "AND attempts.state = 'CLAIMED'",
            (attempt_id, candidate_id),
        ).fetchone()
        if row is None:
            raise CasMismatchError("Candidate download is not authorized for this Attempt")
        candidate = _row_to_candidate(row)
        bundle = ArtifactObjectRecord(
            bundle_digest=row["bundle_digest"],
            storage_key=row["artifact_storage_key"],
            byte_length=row["artifact_byte_length"],
            installed_at_ms=row["artifact_installed_at_ms"],
        )
        return CandidateDownloadRecord(candidate=candidate, bundle=bundle)

    def list_open_activity_offers(self) -> list[tuple[AttemptRecord, OutboxRecord]]:
        """Every current, unexpired ``OFFERED`` Attempt with its dispatch Outbox row.

        This is exactly the durable set Redis reconstruction must republish
        (never a ``CLAIMED`` Attempt, which is not schedulable work).
        """
        now = _now_ms()
        rows = self.conn.execute(
            "SELECT attempts.*, outbox.outbox_id AS outbox_row_id FROM attempts "
            "JOIN outbox ON outbox.attempt_id = attempts.attempt_id "
            "AND outbox.attempt_generation = attempts.generation "
            "WHERE attempts.state = 'OFFERED' AND attempts.claim_deadline_ms > ? "
            "AND outbox.source_kind = 'ACTIVITY'",
            (now,),
        ).fetchall()
        results: list[tuple[AttemptRecord, OutboxRecord]] = []
        for row in rows:
            attempt_record = _row_to_attempt(row)
            outbox_row = self.conn.execute(
                "SELECT * FROM outbox WHERE outbox_id = ?", (row["outbox_row_id"],)
            ).fetchone()
            assert outbox_row is not None
            results.append((attempt_record, _row_to_outbox(outbox_row)))
        return results

    def get_outbox(self, outbox_id: str) -> OutboxRecord | None:
        row = self.conn.execute("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,)).fetchone()
        return None if row is None else _row_to_outbox(row)

    def list_pending_activity_offers(self, *, limit: int = 100) -> list[OutboxRecord]:
        """Due ``PENDING`` ``ACTIVITY``-sourced Outbox rows, oldest first."""
        rows = self.conn.execute(
            "SELECT * FROM outbox WHERE source_kind = 'ACTIVITY' AND state = 'PENDING' "
            "ORDER BY next_delivery_at_ms LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_outbox(row) for row in rows]

    def mark_outbox_redis_delivered(
        self, outbox_id: str, *, redis_epoch: int, redis_entry_id: str
    ) -> None:
        """Record a successful Redis append. Safe to call again on republish/reconstruction."""
        cur = self.conn.execute(
            "UPDATE outbox SET state = 'DELIVERED', delivery_count = delivery_count + 1, "
            "last_redis_epoch = ?, last_redis_entry = ? WHERE outbox_id = ?",
            (redis_epoch, redis_entry_id, outbox_id),
        )
        if cur.rowcount != 1:
            raise RunStoreError(f"outbox {outbox_id!r} was not found")

    def insert_outbox(
        self,
        *,
        outbox_id: str,
        source_kind: str,
        source_id: str,
        destination: str,
        protocol_version: str,
        payload_digest: str,
        payload: Any,
        next_delivery_at_ms: int,
        attempt_id: str | None = None,
        attempt_generation: int | None = None,
        publication_id: str | None = None,
        effect_generation: int | None = None,
    ) -> OutboxRecord:
        require_lowercase_uuid(outbox_id, field="outbox_id")
        enums.parse_enum("outbox_record.source_kind", source_kind)
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM outbox WHERE source_kind = ? AND source_id = ? "
            "AND destination = ? AND payload_digest = ?",
            (source_kind, source_id, destination, payload_digest),
        ).fetchone()
        if existing is not None:
            row = _row_to_outbox(existing)
            if (
                row.payload_json == payload_json
                and row.protocol_version == protocol_version
                and row.attempt_id == attempt_id
                and row.attempt_generation == attempt_generation
                and row.publication_id == publication_id
                and row.effect_generation == effect_generation
            ):
                return row
            raise IdempotencyConflictError("outbox source was reused with different content")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO outbox(outbox_id, source_kind, source_id, destination, attempt_id, "
            "attempt_generation, publication_id, effect_generation, protocol_version, "
            "payload_digest, payload_json, next_delivery_at_ms, state, delivery_count, "
            "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?)",
            (
                outbox_id,
                source_kind,
                source_id,
                destination,
                attempt_id,
                attempt_generation,
                publication_id,
                effect_generation,
                protocol_version,
                payload_digest,
                payload_json,
                next_delivery_at_ms,
                now,
            ),
        )
        row = self.conn.execute("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,)).fetchone()
        assert row is not None
        return _row_to_outbox(row)

    def insert_projection_outbox(
        self,
        *,
        projection_outbox_id: str,
        run_id: str,
        transition_sequence: int,
        kind: str,
        target_kind: str,
        target_id: str,
        payload_digest: str,
        payload: Any,
        idempotency_key: str,
        next_delivery_at_ms: int,
        publication_id: str | None = None,
        effect_generation: int | None = None,
    ) -> ProjectionOutboxRecord:
        require_lowercase_uuid(projection_outbox_id, field="projection_outbox_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("projection_outbox_record.kind", kind)
        enums.parse_enum("projection_outbox_record.target_kind", target_kind)
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM projection_outbox WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if existing is not None:
            row = _row_to_projection(existing)
            if (
                row.run_id == run_id
                and row.transition_sequence == transition_sequence
                and row.kind == kind
                and row.target_kind == target_kind
                and row.target_id == target_id
                and row.publication_id == publication_id
                and row.effect_generation == effect_generation
                and row.payload_digest == payload_digest
                and row.payload_json == payload_json
            ):
                return row
            raise IdempotencyConflictError("projection idempotency key was reused")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO projection_outbox(projection_outbox_id, run_id, transition_sequence, "
            "kind, target_kind, target_id, publication_id, effect_generation, payload_digest, "
            "payload_json, idempotency_key, state, delivery_count, next_delivery_at_ms, "
            "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)",
            (
                projection_outbox_id,
                run_id,
                transition_sequence,
                kind,
                target_kind,
                target_id,
                publication_id,
                effect_generation,
                payload_digest,
                payload_json,
                idempotency_key,
                next_delivery_at_ms,
                now,
            ),
        )
        row = self.conn.execute(
            "SELECT * FROM projection_outbox WHERE projection_outbox_id = ?",
            (projection_outbox_id,),
        ).fetchone()
        assert row is not None
        return _row_to_projection(row)

    def put_revisioned_object(
        self,
        *,
        object_kind: str,
        object_id: str,
        expected_revision: int,
        payload_digest: str,
        payload: Any,
    ) -> int:
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        now = _now_ms()
        existing = self.conn.execute(
            "SELECT revision, payload_digest, payload_json FROM revisioned_objects "
            "WHERE object_kind = ? AND object_id = ?",
            (object_kind, object_id),
        ).fetchone()
        if existing is None:
            if expected_revision != 0:
                raise CasMismatchError("missing revisioned object did not match expected revision")
            self.conn.execute(
                "INSERT INTO revisioned_objects(object_kind, object_id, revision, payload_digest, "
                "payload_json, updated_at_ms) VALUES (?, ?, 1, ?, ?, ?)",
                (object_kind, object_id, payload_digest, payload_json, now),
            )
            return 1
        if int(existing["revision"]) != expected_revision:
            raise CasMismatchError("revisioned object CAS lost")
        if (
            existing["payload_digest"] == payload_digest
            and existing["payload_json"] == payload_json
        ):
            return expected_revision
        new_revision = expected_revision + 1
        cur = self.conn.execute(
            "UPDATE revisioned_objects SET revision = ?, payload_digest = ?, payload_json = ?, "
            "updated_at_ms = ? WHERE object_kind = ? AND object_id = ? AND revision = ?",
            (
                new_revision,
                payload_digest,
                payload_json,
                now,
                object_kind,
                object_id,
                expected_revision,
            ),
        )
        if cur.rowcount != 1:
            raise CasMismatchError("revisioned object CAS lost")
        return new_revision

    def record_durable_operation(
        self,
        *,
        operation_id: str,
        operation_kind: str,
        principal_id: str,
        idempotency_key: str,
        request_payload: Any,
        status: str,
        response_payload: Any,
        response_http_status: int,
    ) -> DurableOperation:
        require_lowercase_uuid(operation_id, field="operation_id")
        if not is_lowercase_uuid(idempotency_key):
            raise ValueError("idempotency_key must be a lowercase canonical UUID string")
        req_digest = request_digest(request_payload)
        response_json = _require_json_text(response_payload)
        resp_digest = response_digest(
            {
                "http_status": response_http_status,
                "body": _response_digest_preimage(response_payload),
            }
        )
        existing = self.conn.execute(
            "SELECT * FROM durable_operations WHERE principal_id = ? AND idempotency_key = ?",
            (principal_id, idempotency_key),
        ).fetchone()
        if existing is not None:
            row = _row_to_operation(existing)
            if row.request_digest == req_digest:
                return row
            raise IdempotencyConflictError("operation idempotency key was reused")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO durable_operations(operation_id, operation_kind, principal_id, "
            "idempotency_key, request_digest, status, response_json, response_digest, "
            "response_http_status, committed_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                operation_id,
                operation_kind,
                principal_id,
                idempotency_key,
                req_digest,
                status,
                response_json,
                resp_digest,
                response_http_status,
                now,
            ),
        )
        row = self.conn.execute(
            "SELECT * FROM durable_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        assert row is not None
        return _row_to_operation(row)

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        return None if row is None else _row_to_run(row)

    def get_transition_by_trigger(
        self, run_id: str, trigger_kind: str, trigger_id: str
    ) -> Transition | None:
        row = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? AND trigger_kind = ? AND trigger_id = ?",
            (run_id, trigger_kind, trigger_id),
        ).fetchone()
        return None if row is None else _row_to_transition(row)

    def list_transitions(self, run_id: str) -> list[Transition]:
        rows = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? ORDER BY transition_sequence",
            (run_id,),
        ).fetchall()
        return [_row_to_transition(row) for row in rows]

    def set_terminal_outcome(self, run_id: str, terminal_outcome: str) -> None:
        enums.parse_enum("run.terminal_outcome", terminal_outcome)
        now = _now_ms()
        cur = self.conn.execute(
            "UPDATE runs SET terminal_outcome = ?, updated_at_ms = ? WHERE run_id = ?",
            (terminal_outcome, now, run_id),
        )
        if cur.rowcount != 1:
            raise RunStoreError(f"run {run_id!r} was not updated")

    def get_revisioned_object(
        self, object_kind: str, object_id: str
    ) -> tuple[int, str, str] | None:
        row = self.conn.execute(
            "SELECT revision, payload_digest, payload_json FROM revisioned_objects "
            "WHERE object_kind = ? AND object_id = ?",
            (object_kind, object_id),
        ).fetchone()
        if row is None:
            return None
        return int(row["revision"]), str(row["payload_digest"]), str(row["payload_json"])

    def get_source_unique_record(
        self, source_kind: str, source_id: str
    ) -> SourceUniqueRecord | None:
        row = self.conn.execute(
            "SELECT * FROM source_unique_records WHERE source_kind = ? AND source_id = ?",
            (source_kind, source_id),
        ).fetchone()
        return None if row is None else _row_to_source_record(row)
