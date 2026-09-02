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
    attempt_result_receipt_digest,
    attempt_terminal_fact_digest,
    attempt_terminal_fact_health_membership_digest,
    bare_canonical_digest,
    budget_report_digest,
    capability_public_key_digest,
    capacity_report_digest,
    checkpoint_digest,
    config_bundle_hash,
    forge_observation_payload_digest,
    forge_observation_result_membership_digest,
    forge_observation_schedule_digest,
    forge_request_failure_fact_digest,
    health_observation_payload_digest,
    health_probe_fact_digest,
    health_probe_request_digest,
    health_probe_run_membership_digest,
    human_boundary_digest,
    human_resolution_digest,
    is_valid_content_digest,
    launch_capability_claims_digest,
    policy_digest,
    receipt_digest,
    recovery_evidence_digest,
    request_digest,
    resolution_digest,
    response_digest,
    result_digest,
    review_assignment_digest,
    specification_digest,
    subject_refs_digest,
    timer_fact_digest,
    wait_condition_digest,
    work_item_discovery_set_digest,
    worker_loss_report_digest,
    workflow_blob_digest,
)
from orcest.workflow_contract.v1.identity import is_lowercase_uuid, require_lowercase_uuid
from orcest.workflow_contract.v1.protocol import ProtocolValidationError, validate_envelope
from orcest.workflow_contract.v1.protocol_registry import (
    ADJUDICATION_RECEIPT_PROTOCOL,
    ATTEMPT_CLAIM_PROTOCOL,
    ATTEMPT_LIVENESS_RESULT_PROTOCOL,
    ATTEMPT_RESULT_ACCEPTED_PROTOCOL,
    ATTEMPT_RESULT_PROTOCOL,
    BUDGET_REPORT_RESULT_PROTOCOL,
    CANDIDATE_UPLOAD_EXPIRED_PROTOCOL,
    CAPABILITY_KEY_OPERATION_PROTOCOL,
    CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL,
    CAPACITY_REPORT_PROTOCOL,
    CAPACITY_REPORT_RESULT_PROTOCOL,
    CONTROLLER_MODE_OPERATION_PROTOCOL,
    CONTROLLER_MODE_RESULT_PROTOCOL,
    CREDENTIAL_ROTATION_RESULT_PROTOCOL,
    ERROR_PROTOCOL,
    FORGE_OBSERVATION_REQUEST_PROTOCOL,
    HEALTH_PROBE_REQUEST_PROTOCOL,
    PROJECT_REGISTRATION_PROTOCOL,
    PROJECT_REGISTRATION_RESULT_PROTOCOL,
    REVIEW_RECEIPT_PROTOCOL,
    SECRET_PROVISION_ACCEPTED_PROTOCOL,
    SECRET_PROVISION_REQUEST_PROTOCOL,
    SECRET_PROVISION_RESULT_PROTOCOL,
    WORKER_LOSS_PROTOCOL,
    WORKER_LOSS_RESULT_PROTOCOL,
)
from orcest.workflow_contract.v1.structured_outputs import validate_attempt_structured_output
from orcest.workflow_contract.v1.verification import (
    VerificationReceiptRejectedError,
    validate_verification_receipt,
    verification_profile_from_effective_policy,
)

SCHEMA_VERSION = 19
_NEW_ATTEMPT_TERMINAL_FACT_COLUMNS = {
    "expected_deadline_ms": "INTEGER",
    "controller_now_ms": "INTEGER",
    "capacity_disposition": "TEXT",
    "health_observation_ids_digest": "TEXT",
    "resolved_provider_secret_ref": "TEXT",
    "controller_mode_revision": "INTEGER",
    "controller_mode": "TEXT",
    "capability_registry_revision": "INTEGER",
    "selected_issuance_key_id": "TEXT",
    "replacement_offer_disposition": "TEXT",
}
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


class AttemptUnknownError(RunStoreError):
    """Raised when a report's exact (attempt_id, activity_id, generation) triple
    has no durable Attempt at all (HTTP 404 ATTEMPT_UNKNOWN); no ledger row is
    created for this case."""


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
class CredentialRotationRequestResult:
    credential_rotation_request_id: str
    attempt_id: str
    activity_id: str
    attempt_generation: int
    worker_id: str
    worker_session_id: str
    attempt_capability_digest: str
    launch_attestation_id: str
    secret_id: str
    expected_prior_version: int
    secret_request_attestation_id: str
    request_digest: str
    disposition: str
    current_version: int
    response_http_status: int
    response_json: str
    response_digest: str
    accepted_at_ms: int
    provider_account_ref: str | None = None
    credential_rotation_receipt_id: str | None = None
    accepted_version: int | None = None
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
    last_liveness_sequence: int | None = None
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


@dataclass(frozen=True, slots=True)
class CapacityReportEntryInput:
    """One caller-submitted scope observation within a Capacity Report."""

    scope_kind: str
    scope_id: str
    capacity_pool_id: str
    available_slots: int
    worker_profile: str | None = None
    session_evidence: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class HealthObservationRecord:
    health_observation_id: str
    scope_kind: str
    scope_id: str
    health_sequence: int
    kind: str
    source_kind: str
    source_id: str
    subject_bindings_json: str
    effective_at_ms: int
    expires_at_ms: int | None
    payload_digest: str
    created_at_ms: int
    observed_revision: int | None = None


@dataclass(frozen=True, slots=True)
class HealthProbeRequestRecord:
    health_probe_request_id: str
    protocol_version: str
    probe_kind: str
    scope_kind: str
    scope_id: str
    request_identity: str
    subject_bindings_json: str
    expected_revision: int | None
    implementation_digest: str
    input_digest: str
    evidence_digest: str
    request_digest: str
    state: str
    outbox_id: str
    created_at_ms: int
    not_after_ms: int | None = None
    completed_at_ms: int | None = None
    health_probe_fact_id: str | None = None


@dataclass(frozen=True, slots=True)
class HealthProbeFactRecord:
    health_probe_fact_id: str
    health_probe_request_id: str
    probe_kind: str
    scope_kind: str
    scope_id: str
    request_identity: str
    outcome: str
    observed_revision: int | None
    implementation_digest: str
    input_digest: str
    evidence_digest: str
    integrity_failure_code: str | None
    subject_bindings_json: str
    affected_run_ids_digest: str
    health_observation_id: str
    fact_digest: str
    recorded_at_ms: int
    affected_run_ids: tuple[str, ...] = ()
    fanout_cursor_ordinal: int = 0
    fanout_completed_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class HealthProbeCompletion:
    request: HealthProbeRequestRecord
    fact: HealthProbeFactRecord
    observation: HealthObservationRecord
    applied_run_ids: tuple[str, ...]
    recovery_evidence_ids: tuple[str, ...]
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class RecoveryEvidenceRecord:
    recovery_evidence_id: str
    run_id: str
    recovery_sequence: int
    source_kind: str
    source_id: str
    category: str
    failure_fingerprint: str
    strategy_index: int
    selected_tactic: str
    attempt_count: int
    repair_cycle_count: int
    diagnosis_count: int
    rescue_epoch: int
    health_observation_ids_digest: str
    evidence_digest: str
    recorded_at_ms: int
    resumed_wait_condition_id: str | None = None
    resumed_human_boundary_id: str | None = None
    human_resolution_id: str | None = None
    activity_id: str | None = None
    attempt_id: str | None = None
    candidate_id: str | None = None
    forge_observation_id: str | None = None
    selected_fallback: str | None = None
    next_eligible_at_ms: int | None = None
    health_observation_ids: tuple[str, ...] = ()
    specification_generation: int = 0


@dataclass(frozen=True, slots=True)
class WaitConditionPanelSlotInput:
    """One still-unfilled panel slot to freeze into a panel-scoped ``CAPACITY``
    Wait (domain-model.md "Wait Condition Panel Slot")."""

    activity_id: str
    assignment_kind: str
    panel_round: int
    slot_id: str


@dataclass(frozen=True, slots=True)
class WaitConditionPanelSlotRecord:
    slot_ordinal: int
    activity_id: str
    assignment_kind: str
    panel_round: int
    slot_id: str


@dataclass(frozen=True, slots=True)
class WaitConditionRecord:
    wait_condition_id: str
    run_id: str
    reason: str
    resume_state: str
    specification_generation: int
    policy_hash: str
    health_observation_ids_digest: str
    panel_slots_digest: str
    created_from_kind: str
    created_from_id: str
    condition_digest: str
    created_transition_sequence: int
    created_at_ms: int
    candidate_id: str | None = None
    forge_observation_id: str | None = None
    not_before_ms: int | None = None
    wake_kind: str | None = None
    wake_identity: Mapping[str, Any] | None = None
    health_observation_ids: tuple[str, ...] = ()
    panel_slots: tuple[WaitConditionPanelSlotRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class WaitPredicateCheck:
    """Result of rechecking a Wait's wake predicate under the writer lock
    before inserting it (domain-model.md "Wait Condition": Budget/Forge/
    Evidence/Secret "GUARDED ... rechecks ..." paragraphs).

    ``already_satisfied`` is ``True`` exactly when live durable state already
    meets the predicate the caller was about to freeze into a Wait; the
    caller MUST NOT call :meth:`RunStore.create_wait_condition` in that case
    and instead appends a successor Recovery Evidence naming
    ``satisfying_source_kind``/``satisfying_source_id``.
    """

    already_satisfied: bool
    satisfying_source_kind: str | None = None
    satisfying_source_id: str | None = None


@dataclass(frozen=True, slots=True)
class RecoveryEvidenceOutcome:
    """Result of :meth:`RunStore.submit_recovery_evidence`.

    ``wait_condition`` is non-``None`` exactly when the applied Reduction
    entered ``WAITING`` and a fresh Wait Condition was frozen for it.
    ``predicate_check`` is always present when the deterministically selected
    tactic was a ``WAIT_*`` tactic (``None`` for every other tactic) and
    records whether the writer-lock recheck found the predicate already met,
    in which case no Wait was created and the Run instead returned directly
    to its recovery-origin state.
    """

    recovery_evidence: RecoveryEvidenceRecord
    applied: Any
    selected_tactic: str
    wait_condition: WaitConditionRecord | None = None
    predicate_check: WaitPredicateCheck | None = None
    human_boundary: "HumanBoundaryRecord | None" = None


@dataclass(frozen=True, slots=True)
class HumanBoundaryChoice:
    """One ordered, bounded permitted choice inside a Human Boundary packet
    (domain-model.md "Human Boundary" ``choices``)."""

    choice_id: str
    resolution_kind: str
    consequence: str


@dataclass(frozen=True, slots=True)
class HumanBoundaryRecord:
    """The controller-issued immutable exceptional decision packet for one
    exact Run state (domain-model.md "Human Boundary")."""

    human_boundary_id: str
    run_id: str
    reason: str
    resume_state: str
    minimum_request: str
    required_resolution_kinds: tuple[str, ...]
    created_from_kind: str
    created_from_id: str
    packet_digest: str
    created_transition_sequence: int
    created_at_ms: int
    specification_generation: int | None = None
    candidate_id: str | None = None
    policy_hash: str | None = None
    forge_observation_id: str | None = None
    publication_id: str | None = None
    publication_effect_generation: int | None = None
    ownership_project_id: str | None = None
    ownership_deterministic_ref: str | None = None
    ownership_change_request_external_id: str | None = None
    ownership_run_marker: str | None = None
    evidence_refs: tuple[str, ...] = ()
    attempted_strategy_digests: tuple[str, ...] = ()
    choices: tuple[HumanBoundaryChoice, ...] = ()


@dataclass(frozen=True, slots=True)
class HumanResolutionRecord:
    """One immutable, authenticated resolution accepted for an exact Human
    Boundary (domain-model.md "Human Resolution")."""

    human_resolution_id: str
    human_boundary_id: str
    run_id: str
    idempotency_key: str
    source_kind: str
    source_id: str
    authenticated_principal_id: str
    resolution_kind: str
    resolution: Mapping[str, Any]
    resolution_digest: str
    accepted_at_ms: int
    specification_generation: int | None = None
    candidate_id: str | None = None
    policy_hash: str | None = None
    forge_observation_id: str | None = None
    publication_id: str | None = None
    publication_effect_generation: int | None = None
    ownership_project_id: str | None = None
    ownership_deterministic_ref: str | None = None
    ownership_change_request_external_id: str | None = None
    ownership_run_marker: str | None = None


@dataclass(frozen=True, slots=True)
class HumanResolutionOutcome:
    """Result of :meth:`RunStore.submit_human_resolution`."""

    human_resolution: HumanResolutionRecord
    applied: Any


@dataclass(frozen=True, slots=True)
class CapacityReportResult:
    capacity_report_id: str
    pool_manager_id: str
    report_id: str
    idempotency_key: str
    report_sequence: int
    health_observations: tuple[HealthObservationRecord, ...]
    woken_wait_condition_ids: tuple[str, ...]
    response_http_status: int
    response_json: str
    response_digest: str
    accepted_at_ms: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class WorkerLossReportResult:
    worker_loss_report_id: str
    pool_manager_id: str
    idempotency_key: str
    worker_id: str
    worker_session_id: str
    attempt_id: str
    activity_id: str
    attempt_generation: int
    reason: str
    outcome: str
    health_observation_id: str | None
    attempt_terminal_fact_id: str | None
    response_http_status: int
    response_json: str
    response_digest: str
    accepted_at_ms: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class AttemptLivenessResult:
    """Response to one ``PUT /api/v1/attempts/{attempt_id}/liveness`` call.

    Liveness has no idempotency key or durable request/response ledger
    (worker-protocol.md "Liveness, control, and deadlines"): every call is
    freshly derived from current durable state and this result is never
    stored. ``liveness_recorded`` reflects only the disposable Redis-backed
    lease this store has no client for, so it is always ``False`` here; the
    durable ``last_liveness_observed_ms``/``last_liveness_sequence`` columns
    are an informational courtesy checkpoint, not correctness authority.
    """

    attempt_id: str
    activity_id: str
    generation: int
    control: str
    execution_deadline_ms: int
    liveness_recorded: bool
    sequence_advanced: bool
    response_http_status: int
    response_json: str
    response_digest: str


@dataclass(frozen=True, slots=True)
class TimerFactRecord:
    timer_fact_id: str
    run_id: str | None
    scope_kind: str
    scope_id: str
    fired_for_ms: int
    controller_now_ms: int
    source_kind: str
    source_id: str
    fact_digest: str
    recorded_at_ms: int


@dataclass(frozen=True, slots=True)
class AttemptDeadlineExpiryResult:
    """Outcome of one scheduled-sweep/reconciliation Attempt-deadline pass.

    ``outcome`` is ``EXPIRED`` when this call won the terminal fence and
    created the Attempt Terminal Fact, or ``STALE`` when the referenced
    Attempt had already moved past the scope this Timer Fact was about (a
    prior claim, an already-terminal Attempt, or a deadline that no longer
    matches the durable object) -- the Timer Fact is still durably recorded
    as evidence either way, per "Timer Facts are evidence, not a second
    terminal trigger".
    """

    timer_fact_id: str
    outcome: str
    attempt_terminal_fact_id: str | None = None
    capacity_disposition: str | None = None
    replacement_offer_disposition: str | None = None


@dataclass(frozen=True, slots=True)
class AttemptResultRecord:
    attempt_result_id: str
    result_request_id: str
    attempt_id: str
    activity_id: str
    attempt_generation: int
    outcome: str
    result_digest: str
    body_json: str
    failure_class: str | None
    failure_json: str | None
    evidence_refs_json: str | None
    retry_delay_ms: int | None
    receipt_id: str | None
    receipt_json: str | None
    receipt_digest: str | None
    candidate_id: str | None
    accepted_at_ms: int


@dataclass(frozen=True, slots=True)
class ResultRequestRecord:
    result_request_id: str
    attempt_result_id: str | None
    attempt_id: str
    activity_id: str
    attempt_generation: int
    worker_id: str
    worker_session_id: str
    attempt_capability_digest: str
    request_digest: str
    result_digest: str
    disposition: str
    stale_reason: str | None
    accepted_result_created: bool
    candidate_upload_id: str | None
    attempt_terminal_fact_id: str | None
    response_http_status: int
    response_json: str
    response_digest: str
    created_at_ms: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class AttemptResultSubmissionResult:
    request: ResultRequestRecord
    attempt_result: AttemptResultRecord | None
    candidate: CandidateRecord | None


@dataclass(frozen=True, slots=True)
class BudgetReportResult:
    budget_report_id: str
    project_id: str
    accounting_scope_id: str
    source_sequence: int
    availability: str
    affected_run_ids_digest: str
    response_http_status: int
    response_json: str
    response_digest: str
    accepted_at_ms: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class OfferGateEvaluation:
    """Combined dispatch gate for a proposed new Attempt offer.

    A purely read-only snapshot over already-durable inputs -- Controller
    Mode, the Capability Registry's selected issuance key, and the latest
    applicable unexpired capacity Health Observations plus Budget Report for
    the caller's given scopes. It never mutates state and never invents
    evidence when a required input is simply absent: an absent Health
    Observation or Budget Report leaves the corresponding ``*_health``/
    ``budget_report`` field ``None`` and the corresponding ``*_available``
    flag ``False``, exactly as domain-model.md requires ("absence ... leaves
    work durably PLANNED ... and creates no invented Evidence").

    ``disposition`` mirrors ``AttemptTerminalFactReplacementOfferDisposition``
    and reflects only the controller-mode/issuance-key precedence
    (``MODE_BLOCKED`` first, then ``ISSUANCE_KEY_UNAVAILABLE``, else
    ``OFFER_ALLOWED``); it does not fold in capacity/budget availability
    because "current capacity" and "budget" are scope-specific and the caller
    is better placed to combine them with its own compatibility rules.
    """

    disposition: str
    controller_mode: str | None
    controller_mode_revision: int
    capability_registry_revision: int
    selected_issuance_key_id: str | None
    worker_profile_health: HealthObservationRecord | None
    capacity_pool_health: HealthObservationRecord | None
    provider_account_health: HealthObservationRecord | None
    budget_report: BudgetReportResult | None
    capacity_available: bool
    budget_available: bool


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
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
    except subprocess.TimeoutExpired as exc:
        raise CasMismatchError("git Candidate validation timed out") from exc
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


def _require_controller_import_replay_match(
    fact: ControllerOperationFactRecord,
    *,
    candidate_id: str,
    activity_id: str,
    forge_observation_id: str,
    operation_digest: str,
    fact_digest: str,
) -> None:
    if (
        fact.operation_kind != "IMPORT"
        or fact.outcome != "SUCCEEDED"
        or fact.candidate_id != candidate_id
        or fact.activity_id != activity_id
        or fact.forge_observation_id != forge_observation_id
        or fact.operation_digest != operation_digest
        or fact.fact_digest != fact_digest
    ):
        raise IdempotencyConflictError(
            "controller operation fact id was reused with different content"
        )


_CAPACITY_SCOPE_ORDER = {"WORKER_SESSION": 0, "WORKER_PROFILE": 1, "CAPACITY_POOL": 2}
_CAPACITY_SESSION_UNAVAILABLE_STATES = frozenset(
    {"SESSION_STOPPED", "VM_MISSING", "DRAIN_COMPLETE", "SESSION_UNREACHABLE"}
)


def _validate_capacity_report_entries(entries: Sequence["CapacityReportEntryInput"]) -> None:
    """Validate one Capacity Report's ``observations`` per worker-protocol.md
    "Pool-manager capacity report": non-empty, no duplicate scope, canonically
    ordered (``WORKER_SESSION``, ``WORKER_PROFILE``, ``CAPACITY_POOL``, then by
    ``scope_id``), and the closed per-scope-kind field matrix."""
    if not entries:
        raise ValueError("a Capacity Report requires at least one entry")
    seen: set[tuple[str, str]] = set()
    prior_order_key: tuple[int, str] | None = None
    for entry in entries:
        enums.parse_enum("capacity_report.scope_kind", entry.scope_kind)
        scope_key = (entry.scope_kind, entry.scope_id)
        if scope_key in seen:
            raise ValueError(f"duplicate capacity report scope {scope_key!r}")
        seen.add(scope_key)
        order_key = (_CAPACITY_SCOPE_ORDER[entry.scope_kind], entry.scope_id)
        if prior_order_key is not None and order_key <= prior_order_key:
            raise ValueError("capacity report entries must be in canonical scope order")
        prior_order_key = order_key
        if entry.available_slots < 0:
            raise ValueError("available_slots must be nonnegative")
        if entry.scope_kind == "WORKER_SESSION":
            if entry.available_slots not in (0, 1):
                raise ValueError("WORKER_SESSION available_slots must be 0 or 1")
            if not entry.session_evidence:
                raise ValueError("WORKER_SESSION requires session_evidence")
            if entry.session_evidence.get("worker_session_id") != entry.scope_id:
                raise ValueError(
                    "WORKER_SESSION scope_id must equal session_evidence.worker_session_id"
                )
            state = entry.session_evidence.get("state")
            if entry.available_slots > 0:
                if state != "SESSION_READY":
                    raise ValueError("AVAILABLE WORKER_SESSION requires state SESSION_READY")
            elif state not in _CAPACITY_SESSION_UNAVAILABLE_STATES:
                raise ValueError(
                    "UNAVAILABLE WORKER_SESSION requires a recognized stopped/unreachable state"
                )
        elif entry.scope_kind == "WORKER_PROFILE":
            if entry.session_evidence is not None:
                raise ValueError("WORKER_PROFILE must not carry session_evidence")
            if entry.worker_profile != entry.scope_id:
                raise ValueError("WORKER_PROFILE scope_id must equal worker_profile")

        else:
            if entry.session_evidence is not None or entry.worker_profile is not None:
                raise ValueError("CAPACITY_POOL must not carry worker_profile/session_evidence")
            if entry.capacity_pool_id != entry.scope_id:
                raise ValueError("CAPACITY_POOL scope_id must equal capacity_pool_id")


_HEALTH_PROBE_SCOPE_MATRIX: Mapping[str, frozenset[str]] = {
    "FORGE_CONNECTIVITY": frozenset({"FORGE"}),
    "PROVIDER_ACCOUNT_STATUS": frozenset({"PROVIDER_ACCOUNT"}),
    "STORAGE_OBJECT_INTEGRITY": frozenset({"STORAGE"}),
    "SECRET_VERSION_INTEGRITY": frozenset({"SECRET"}),
}

_HEALTH_PROBE_OUTCOME_MATRIX: Mapping[str, frozenset[str]] = {
    "FORGE_CONNECTIVITY": frozenset({"AVAILABLE", "UNAVAILABLE"}),
    "PROVIDER_ACCOUNT_STATUS": frozenset({"AVAILABLE", "UNAVAILABLE", "RATE_LIMITED", "EXHAUSTED"}),
    "STORAGE_OBJECT_INTEGRITY": frozenset({"AVAILABLE", "UNAVAILABLE"}),
    "SECRET_VERSION_INTEGRITY": frozenset({"AVAILABLE", "UNAVAILABLE"}),
}

_HEALTH_PROBE_INTEGRITY_FAILURE_MATRIX: Mapping[str, frozenset[str]] = {
    "STORAGE_OBJECT_INTEGRITY": frozenset({"MISSING", "UNREADABLE", "DIGEST_MISMATCH"}),
    "SECRET_VERSION_INTEGRITY": frozenset({"MISSING", "UNREADABLE", "KEYED_ATTESTATION_MISMATCH"}),
}


def _validate_health_probe_matrix(*, probe_kind: str, scope_kind: str) -> None:
    allowed_scopes = _HEALTH_PROBE_SCOPE_MATRIX.get(probe_kind)
    if allowed_scopes is None:
        enums.parse_enum("health_probe.probe_kind", probe_kind)
        raise ValueError(f"unsupported health probe kind {probe_kind!r}")
    if scope_kind not in allowed_scopes:
        raise ValueError(f"{probe_kind} cannot probe health scope {scope_kind}")


def _validate_health_probe_outcome(*, probe_kind: str, scope_kind: str, outcome: str) -> None:
    _validate_health_probe_matrix(probe_kind=probe_kind, scope_kind=scope_kind)
    allowed = _HEALTH_PROBE_OUTCOME_MATRIX[probe_kind]
    if outcome not in allowed:
        raise ValueError(f"{probe_kind} cannot produce outcome {outcome}")


def _validate_health_probe_integrity_failure(
    *, probe_kind: str, outcome: str, integrity_failure_code: str | None
) -> None:
    allowed = _HEALTH_PROBE_INTEGRITY_FAILURE_MATRIX.get(probe_kind)
    if outcome == "UNAVAILABLE" and allowed is not None:
        if integrity_failure_code not in allowed:
            raise ValueError(
                f"{probe_kind} UNAVAILABLE requires integrity_failure_code in {sorted(allowed)!r}"
            )
        return
    if integrity_failure_code is not None:
        raise ValueError(f"{probe_kind} outcome {outcome} must not carry integrity_failure_code")


def _health_probe_subject(fact: "HealthProbeFactRecord") -> dict[str, Any]:
    subject = json.loads(fact.subject_bindings_json)
    if not isinstance(subject, dict):
        raise RunStoreError("health probe fact subject bindings are corrupt")
    return {key: value for key, value in subject.items() if key != "probe_evidence"}


def _parse_secret_version_scope(
    scope_id: str, subject_bindings: Mapping[str, Any]
) -> tuple[str, int]:
    secret_id = subject_bindings.get("secret_id")
    version = subject_bindings.get("version")
    if secret_id is None or version is None:
        left, sep, right = scope_id.partition("/")
        if sep:
            secret_id = left
            version = right
    if secret_id is None or version is None:
        raise ValueError("SECRET_VERSION_INTEGRITY requires secret_id/version bindings")
    require_lowercase_uuid(str(secret_id), field="secret_id")
    parsed_version = int(version)
    if parsed_version < 1:
        raise ValueError("secret version must be positive")
    return str(secret_id), parsed_version


def _health_probe_recovery_category(fact: "HealthProbeFactRecord") -> str:
    if fact.outcome == "AVAILABLE":
        if fact.probe_kind in {"STORAGE_OBJECT_INTEGRITY", "SECRET_VERSION_INTEGRITY"}:
            return "INTEGRITY_SUSPECTED"
        if fact.scope_kind == "PROVIDER_ACCOUNT":
            return "CAPACITY"
        if fact.scope_kind == "FORGE":
            return "FORGE_TRANSIENT"
    if fact.scope_kind == "SECRET":
        return "CREDENTIAL"
    if fact.scope_kind == "STORAGE":
        return "STORAGE"
    if fact.scope_kind == "PROVIDER_ACCOUNT":
        return "PROVIDER_RATE_LIMIT" if fact.outcome == "RATE_LIMITED" else "CAPACITY"
    if fact.scope_kind == "FORGE":
        return "PROVIDER_RATE_LIMIT" if fact.outcome == "RATE_LIMITED" else "FORGE_TRANSIENT"
    raise ValueError(f"cannot map health probe fact {fact.health_probe_fact_id!r} to recovery")


def _health_probe_wait_kind(fact: "HealthProbeFactRecord", selected_tactic: str) -> str | None:
    if selected_tactic in {"WAIT_BACKOFF", "RETRY_EXECUTION"}:
        return None
    if selected_tactic == "WAIT_RATE_LIMIT":
        return "RATE_LIMIT_RESET"
    if selected_tactic == "WAIT_CAPACITY":
        return "CAPACITY"
    if fact.scope_kind == "SECRET":
        return "SECRET"
    if fact.scope_kind == "STORAGE":
        return "STORAGE"
    if fact.scope_kind == "FORGE":
        return "FORGE"
    return None


def _health_probe_wait_identity(
    fact: "HealthProbeFactRecord", selected_tactic: str
) -> dict[str, Any] | None:
    if selected_tactic in {"WAIT_BACKOFF", "RETRY_EXECUTION"}:
        return None
    if selected_tactic in {"WAIT_CAPACITY", "WAIT_RATE_LIMIT"}:
        return {"scope_kind": fact.scope_kind, "scope_id": fact.scope_id}
    subject = _health_probe_subject(fact)
    if fact.scope_kind == "SECRET":
        secret_id, version = _parse_secret_version_scope(fact.scope_id, subject)
        return {"secret_id": secret_id, "minimum_version": version}
    return {
        "scope_kind": fact.scope_kind,
        "scope_id": fact.scope_id,
        "object_kind": subject.get("object_kind"),
        "object_id": subject.get("object_id", fact.scope_id),
    }


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


def _response_json_with_replayed(response_json: str, *, replayed: bool) -> str:
    body = json.loads(response_json)
    if not isinstance(body, dict):
        raise RunStoreError("stored response is not a JSON object")
    body["replayed"] = replayed
    return canonical_json_text(body)


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


def _row_to_recovery_evidence(
    row: sqlite3.Row, *, health_observation_ids: Sequence[str] = ()
) -> RecoveryEvidenceRecord:
    return RecoveryEvidenceRecord(
        recovery_evidence_id=row["recovery_evidence_id"],
        run_id=row["run_id"],
        recovery_sequence=row["recovery_sequence"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        resumed_wait_condition_id=row["resumed_wait_condition_id"],
        resumed_human_boundary_id=row["resumed_human_boundary_id"],
        human_resolution_id=row["human_resolution_id"],
        activity_id=row["activity_id"],
        attempt_id=row["attempt_id"],
        specification_generation=row["specification_generation"],
        candidate_id=row["candidate_id"],
        forge_observation_id=row["forge_observation_id"],
        category=row["category"],
        failure_fingerprint=row["failure_fingerprint"],
        strategy_index=row["strategy_index"],
        selected_tactic=row["selected_tactic"],
        attempt_count=row["attempt_count"],
        repair_cycle_count=row["repair_cycle_count"],
        diagnosis_count=row["diagnosis_count"],
        rescue_epoch=row["rescue_epoch"],
        selected_fallback=row["selected_fallback"],
        health_observation_ids_digest=row["health_observation_ids_digest"],
        next_eligible_at_ms=row["next_eligible_at_ms"],
        evidence_digest=row["evidence_digest"],
        recorded_at_ms=row["recorded_at_ms"],
        health_observation_ids=tuple(health_observation_ids),
    )


def _row_to_health_probe_request(row: sqlite3.Row) -> HealthProbeRequestRecord:
    return HealthProbeRequestRecord(
        health_probe_request_id=row["health_probe_request_id"],
        protocol_version=row["protocol_version"],
        probe_kind=row["probe_kind"],
        scope_kind=row["scope_kind"],
        scope_id=row["scope_id"],
        request_identity=row["request_identity"],
        subject_bindings_json=row["subject_bindings_json"],
        expected_revision=row["expected_revision"],
        implementation_digest=row["implementation_digest"],
        input_digest=row["input_digest"],
        evidence_digest=row["evidence_digest"],
        request_digest=row["request_digest"],
        state=row["state"],
        outbox_id=row["outbox_id"],
        not_after_ms=row["not_after_ms"],
        completed_at_ms=row["completed_at_ms"],
        health_probe_fact_id=row["health_probe_fact_id"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_health_probe_fact(
    row: sqlite3.Row, *, affected_run_ids: Sequence[str] = ()
) -> HealthProbeFactRecord:
    return HealthProbeFactRecord(
        health_probe_fact_id=row["health_probe_fact_id"],
        health_probe_request_id=row["health_probe_request_id"],
        probe_kind=row["probe_kind"],
        scope_kind=row["scope_kind"],
        scope_id=row["scope_id"],
        request_identity=row["request_identity"],
        outcome=row["outcome"],
        observed_revision=row["observed_revision"],
        implementation_digest=row["implementation_digest"],
        input_digest=row["input_digest"],
        evidence_digest=row["evidence_digest"],
        integrity_failure_code=row["integrity_failure_code"],
        subject_bindings_json=row["subject_bindings_json"],
        affected_run_ids_digest=row["affected_run_ids_digest"],
        health_observation_id=row["health_observation_id"],
        fact_digest=row["fact_digest"],
        fanout_cursor_ordinal=row["fanout_cursor_ordinal"],
        fanout_completed_at_ms=row["fanout_completed_at_ms"],
        recorded_at_ms=row["recorded_at_ms"],
        affected_run_ids=tuple(affected_run_ids),
    )


def _row_to_wait_condition(
    row: sqlite3.Row,
    *,
    health_observation_ids: Sequence[str] = (),
    panel_slots: Sequence[WaitConditionPanelSlotRecord] = (),
) -> WaitConditionRecord:
    wake_identity_json = row["wake_identity_json"]
    return WaitConditionRecord(
        wait_condition_id=row["wait_condition_id"],
        run_id=row["run_id"],
        reason=row["reason"],
        resume_state=row["resume_state"],
        specification_generation=row["specification_generation"],
        candidate_id=row["candidate_id"],
        policy_hash=row["policy_hash"],
        forge_observation_id=row["forge_observation_id"],
        not_before_ms=row["not_before_ms"],
        wake_kind=row["wake_kind"],
        wake_identity=json.loads(wake_identity_json) if wake_identity_json is not None else None,
        health_observation_ids_digest=row["health_observation_ids_digest"],
        panel_slots_digest=row["panel_slots_digest"],
        created_from_kind=row["created_from_kind"],
        created_from_id=row["created_from_id"],
        condition_digest=row["condition_digest"],
        created_transition_sequence=row["created_transition_sequence"],
        created_at_ms=row["created_at_ms"],
        health_observation_ids=tuple(health_observation_ids),
        panel_slots=tuple(panel_slots),
    )


def _row_to_human_boundary(
    row: sqlite3.Row,
    *,
    evidence_refs: Sequence[str] = (),
    attempted_strategy_digests: Sequence[str] = (),
    required_resolution_kinds: Sequence[str] = (),
    choices: Sequence["HumanBoundaryChoice"] = (),
) -> "HumanBoundaryRecord":
    return HumanBoundaryRecord(
        human_boundary_id=row["human_boundary_id"],
        run_id=row["run_id"],
        reason=row["reason"],
        resume_state=row["resume_state"],
        minimum_request=row["minimum_request"],
        required_resolution_kinds=tuple(required_resolution_kinds),
        created_from_kind=row["created_from_kind"],
        created_from_id=row["created_from_id"],
        packet_digest=row["packet_digest"],
        created_transition_sequence=row["created_transition_sequence"],
        created_at_ms=row["created_at_ms"],
        specification_generation=row["specification_generation"],
        candidate_id=row["candidate_id"],
        policy_hash=row["policy_hash"],
        forge_observation_id=row["forge_observation_id"],
        publication_id=row["publication_id"],
        publication_effect_generation=row["publication_effect_generation"],
        ownership_project_id=row["ownership_project_id"],
        ownership_deterministic_ref=row["ownership_deterministic_ref"],
        ownership_change_request_external_id=row["ownership_change_request_external_id"],
        ownership_run_marker=row["ownership_run_marker"],
        evidence_refs=tuple(evidence_refs),
        attempted_strategy_digests=tuple(attempted_strategy_digests),
        choices=tuple(choices),
    )


def _row_to_human_resolution(row: sqlite3.Row) -> "HumanResolutionRecord":
    return HumanResolutionRecord(
        human_resolution_id=row["human_resolution_id"],
        human_boundary_id=row["human_boundary_id"],
        run_id=row["run_id"],
        idempotency_key=row["idempotency_key"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        authenticated_principal_id=row["authenticated_principal_id"],
        resolution_kind=row["resolution_kind"],
        resolution=json.loads(row["resolution_json"]),
        resolution_digest=row["resolution_digest"],
        accepted_at_ms=row["accepted_at_ms"],
        specification_generation=row["specification_generation"],
        candidate_id=row["candidate_id"],
        policy_hash=row["policy_hash"],
        forge_observation_id=row["forge_observation_id"],
        publication_id=row["publication_id"],
        publication_effect_generation=row["publication_effect_generation"],
        ownership_project_id=row["ownership_project_id"],
        ownership_deterministic_ref=row["ownership_deterministic_ref"],
        ownership_change_request_external_id=row["ownership_change_request_external_id"],
        ownership_run_marker=row["ownership_run_marker"],
    )


_WAIT_REASON_WAKE_RULES: Mapping[
    str, Callable[[int | None, str | None, Mapping[str, Any] | None], bool]
] = {
    "CAPACITY": lambda not_before_ms, wake_kind, wake_identity: (
        wake_kind == "CAPACITY" and not_before_ms is None
    ),
    "RATE_LIMIT": lambda not_before_ms, wake_kind, wake_identity: (
        not_before_ms is not None and wake_kind in (None, "RATE_LIMIT_RESET")
    ),
    "BUDGET": lambda not_before_ms, wake_kind, wake_identity: (
        not_before_ms is not None and wake_kind == "BUDGET_WINDOW"
    ),
    "BACKOFF": lambda not_before_ms, wake_kind, wake_identity: (
        not_before_ms is not None and wake_kind is None and wake_identity is None
    ),
    "EXTERNAL_DEPENDENCY": lambda not_before_ms, wake_kind, wake_identity: (
        wake_kind == "DEPENDENCY"
    ),
    "FORGE_UNAVAILABLE": lambda not_before_ms, wake_kind, wake_identity: (
        not_before_ms is not None and wake_kind == "FORGE"
    ),
    "STORAGE_RECOVERY": lambda not_before_ms, wake_kind, wake_identity: wake_kind == "STORAGE",
    "SECRET_RECOVERY": lambda not_before_ms, wake_kind, wake_identity: wake_kind == "SECRET",
    "EVIDENCE": lambda not_before_ms, wake_kind, wake_identity: (
        not_before_ms is not None and wake_kind == "EVIDENCE"
    ),
}


def _validate_wait_condition_shape(
    *,
    reason: str,
    not_before_ms: int | None,
    wake_kind: str | None,
    wake_identity: Mapping[str, Any] | None,
) -> None:
    """The exhaustive Wait reason/wake compatibility matrix (domain-model.md
    "Wait Condition" CHECK clause), enforced in Python ahead of the mirrored
    SQL CHECK so a violation raises a legible error before it ever reaches
    SQLite."""
    enums.parse_enum("wait_condition.reason", reason)
    if wake_kind is not None:
        enums.parse_enum("wait_condition.wake_kind", wake_kind)
    if (wake_kind is None) != (wake_identity is None):
        raise ValueError(
            "wait condition wake_kind and wake_identity must both be set or both be null"
        )
    if not_before_ms is None and wake_kind is None:
        raise ValueError("wait condition requires not_before_ms and/or a wake predicate")
    if not_before_ms is not None and not_before_ms < 0:
        raise ValueError("wait condition not_before_ms must be nonnegative")
    rule = _WAIT_REASON_WAKE_RULES[reason]
    if not rule(not_before_ms, wake_kind, wake_identity):
        raise ValueError(
            f"wait condition reason={reason!r} is incompatible with "
            f"not_before_ms={not_before_ms!r} wake_kind={wake_kind!r}"
        )


def _external_wait_reason(category: str) -> str:
    """Mirrors ``orcest.workflow_reducer.reduce._external_wait_reason`` --
    the closed category -> ``WAIT_EXTERNAL`` reason mapping is defined once
    there (a pure reducer decision) and must stay identical here so the
    store recognizes the exact same reason the Transition it is about to
    freeze a Wait for actually selected."""
    return {
        "CREDENTIAL": "SECRET_RECOVERY",
        "EXTERNAL_DEPENDENCY": "EXTERNAL_DEPENDENCY",
        "FORGE_TRANSIENT": "FORGE_UNAVAILABLE",
        "STORAGE": "STORAGE_RECOVERY",
    }.get(category, "FORGE_UNAVAILABLE")


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
        last_liveness_sequence=row["last_liveness_sequence"],
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


def _row_to_timer_fact(row: sqlite3.Row) -> TimerFactRecord:
    return TimerFactRecord(
        timer_fact_id=row["timer_fact_id"],
        run_id=row["run_id"],
        scope_kind=row["scope_kind"],
        scope_id=row["scope_id"],
        fired_for_ms=row["fired_for_ms"],
        controller_now_ms=row["controller_now_ms"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        fact_digest=row["fact_digest"],
        recorded_at_ms=row["recorded_at_ms"],
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


def _row_to_attempt_result(row: sqlite3.Row) -> AttemptResultRecord:
    return AttemptResultRecord(
        attempt_result_id=row["attempt_result_id"],
        result_request_id=row["result_request_id"],
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        attempt_generation=row["attempt_generation"],
        outcome=row["outcome"],
        result_digest=row["result_digest"],
        body_json=row["body_json"],
        failure_class=row["failure_class"],
        failure_json=row["failure_json"],
        evidence_refs_json=row["evidence_refs_json"],
        retry_delay_ms=row["retry_delay_ms"],
        receipt_id=row["receipt_id"],
        receipt_json=row["receipt_json"],
        receipt_digest=row["receipt_digest"],
        candidate_id=row["candidate_id"],
        accepted_at_ms=row["accepted_at_ms"],
    )


def _row_to_result_request(row: sqlite3.Row, *, replayed: bool) -> ResultRequestRecord:
    return ResultRequestRecord(
        result_request_id=row["result_request_id"],
        attempt_result_id=row["attempt_result_id"],
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        attempt_generation=row["attempt_generation"],
        worker_id=row["worker_id"],
        worker_session_id=row["worker_session_id"],
        attempt_capability_digest=row["attempt_capability_digest"],
        request_digest=row["request_digest"],
        result_digest=row["result_digest"],
        disposition=row["disposition"],
        stale_reason=row["stale_reason"],
        accepted_result_created=bool(row["accepted_result_created"]),
        candidate_upload_id=row["candidate_upload_id"],
        attempt_terminal_fact_id=row["attempt_terminal_fact_id"],
        response_http_status=row["response_http_status"],
        response_json=_response_json_with_replayed(row["response_json"], replayed=replayed),
        response_digest=row["response_digest"],
        created_at_ms=row["created_at_ms"],
        replayed=replayed,
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


def _row_to_credential_rotation_request(
    row: sqlite3.Row, *, replayed: bool
) -> CredentialRotationRequestResult:
    return CredentialRotationRequestResult(
        credential_rotation_request_id=row["credential_rotation_request_id"],
        attempt_id=row["attempt_id"],
        activity_id=row["activity_id"],
        attempt_generation=row["attempt_generation"],
        worker_id=row["worker_id"],
        worker_session_id=row["worker_session_id"],
        attempt_capability_digest=row["attempt_capability_digest"],
        launch_attestation_id=row["launch_attestation_id"],
        provider_account_ref=row["provider_account_ref"],
        secret_id=row["secret_id"],
        expected_prior_version=row["expected_prior_version"],
        secret_request_attestation_id=row["secret_request_attestation_id"],
        request_digest=row["request_digest"],
        disposition=row["disposition"],
        credential_rotation_receipt_id=row["credential_rotation_receipt_id"],
        accepted_version=row["accepted_version"],
        current_version=row["current_version"],
        response_http_status=row["response_http_status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        accepted_at_ms=row["accepted_at_ms"],
        replayed=replayed,
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
  current_recovery_evidence_id TEXT,
  wait_condition_id TEXT,
  human_boundary_id TEXT,
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
  last_liveness_sequence INTEGER
    CHECK (last_liveness_sequence IS NULL OR last_liveness_sequence > 0),
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

CREATE TABLE IF NOT EXISTS attempt_results (
  attempt_result_id TEXT PRIMARY KEY,
  result_request_id TEXT NOT NULL UNIQUE,
  attempt_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  outcome TEXT NOT NULL CHECK (outcome IN ({_sql_in(_enum_values("attempt_result.outcome"))})),
  result_digest TEXT NOT NULL UNIQUE,
  body_json TEXT NOT NULL,
  failure_class TEXT CHECK (
    failure_class IN ({_sql_in(_enum_values("attempt_result.failure_class"))})
  ),
  failure_json TEXT,
  evidence_refs_json TEXT,
  retry_delay_ms INTEGER CHECK (retry_delay_ms IS NULL OR retry_delay_ms >= 0),
  receipt_id TEXT UNIQUE,
  receipt_json TEXT,
  receipt_digest TEXT UNIQUE,
  candidate_id TEXT UNIQUE REFERENCES candidates(candidate_id) ON DELETE RESTRICT,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (attempt_id, activity_id, attempt_generation),
  FOREIGN KEY (attempt_id, activity_id, attempt_generation)
    REFERENCES attempts(attempt_id, activity_id, generation),
  CHECK (
    (outcome = 'SUCCEEDED' AND failure_class IS NULL AND failure_json IS NULL)
    OR (outcome != 'SUCCEEDED' AND failure_class IS NOT NULL AND failure_json IS NOT NULL)
  ),
  CHECK ((receipt_id IS NULL) = (receipt_json IS NULL)),
  CHECK ((receipt_id IS NULL) = (receipt_digest IS NULL))
);

CREATE TABLE IF NOT EXISTS review_receipts (
  receipt_id TEXT PRIMARY KEY REFERENCES attempt_results(receipt_id) ON DELETE RESTRICT,
  attempt_result_id TEXT NOT NULL UNIQUE REFERENCES attempt_results(attempt_result_id)
    ON DELETE RESTRICT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id) ON DELETE RESTRICT,
  object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
  oid TEXT NOT NULL,
  activity_id TEXT NOT NULL REFERENCES activities(activity_id) ON DELETE RESTRICT,
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE RESTRICT,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  specification_generation INTEGER NOT NULL CHECK (specification_generation > 0),
  policy_hash TEXT NOT NULL,
  panel_round INTEGER NOT NULL CHECK (panel_round > 0),
  reviewer_slot TEXT NOT NULL,
  role TEXT NOT NULL,
  subject_refs_digest TEXT NOT NULL,
  context_digest TEXT NOT NULL,
  execution_profile_id TEXT,
  worker_profile TEXT NOT NULL,
  provider TEXT,
  model TEXT,
  provider_account_ref TEXT,
  provider_family TEXT,
  model_family TEXT,
  classification_revision TEXT,
  launch_attestation_id TEXT NOT NULL REFERENCES launch_attestations(launch_attestation_id)
    ON DELETE RESTRICT,
  verdict TEXT NOT NULL CHECK (verdict IN ({_sql_in(_enum_values("review_receipt.verdict"))})),
  fills_slot INTEGER NOT NULL CHECK (fills_slot IN (0, 1)),
  finding_ids_digest TEXT NOT NULL,
  receipt_digest TEXT NOT NULL UNIQUE,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (attempt_id, activity_id, attempt_generation)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_review_receipts_one_filling_slot
ON review_receipts(candidate_id, panel_round, reviewer_slot) WHERE fills_slot = 1;

CREATE TABLE IF NOT EXISTS adjudication_receipts (
  receipt_id TEXT PRIMARY KEY REFERENCES attempt_results(receipt_id) ON DELETE RESTRICT,
  attempt_result_id TEXT NOT NULL UNIQUE REFERENCES attempt_results(attempt_result_id)
    ON DELETE RESTRICT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id) ON DELETE RESTRICT,
  object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
  oid TEXT NOT NULL,
  activity_id TEXT NOT NULL REFERENCES activities(activity_id) ON DELETE RESTRICT,
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE RESTRICT,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  specification_generation INTEGER NOT NULL CHECK (specification_generation > 0),
  policy_hash TEXT NOT NULL,
  panel_round INTEGER NOT NULL CHECK (panel_round > 0),
  adjudication_round INTEGER NOT NULL CHECK (adjudication_round = 1),
  adjudicator_slot TEXT NOT NULL CHECK (adjudicator_slot = 'default'),
  subject_refs_digest TEXT NOT NULL,
  context_digest TEXT NOT NULL,
  execution_profile_id TEXT,
  worker_profile TEXT NOT NULL,
  provider TEXT,
  model TEXT,
  provider_account_ref TEXT,
  provider_family TEXT,
  model_family TEXT,
  classification_revision TEXT,
  launch_attestation_id TEXT NOT NULL REFERENCES launch_attestations(launch_attestation_id)
    ON DELETE RESTRICT,
  disposition_summary TEXT NOT NULL,
  fills_slot INTEGER NOT NULL CHECK (fills_slot IN (0, 1)),
  receipt_digest TEXT NOT NULL UNIQUE,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (attempt_id, activity_id, attempt_generation)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_adjudication_receipts_one_filling_slot
ON adjudication_receipts(candidate_id, panel_round, adjudication_round, adjudicator_slot)
WHERE fills_slot = 1;

CREATE TABLE IF NOT EXISTS consensus_decisions (
  consensus_decision_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id) ON DELETE RESTRICT,
  object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
  oid TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation > 0),
  policy_hash TEXT NOT NULL,
  panel_round INTEGER NOT NULL CHECK (panel_round > 0),
  verification_receipt_id TEXT NOT NULL,
  review_receipt_ids_json TEXT NOT NULL,
  unresolved_finding_ids_json TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ({_sql_in(_enum_values("consensus_decision.outcome"))})),
  decision_digest TEXT NOT NULL UNIQUE,
  created_transition_sequence INTEGER NOT NULL CHECK (created_transition_sequence > 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (candidate_id, panel_round)
);

CREATE TABLE IF NOT EXISTS result_requests (
  result_request_id TEXT PRIMARY KEY,
  attempt_result_id TEXT REFERENCES attempt_results(attempt_result_id) ON DELETE RESTRICT,
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE RESTRICT,
  activity_id TEXT NOT NULL REFERENCES activities(activity_id) ON DELETE RESTRICT,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  worker_id TEXT NOT NULL,
  worker_session_id TEXT NOT NULL,
  attempt_capability_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  result_digest TEXT NOT NULL,
  disposition TEXT NOT NULL CHECK (
    disposition IN ({_sql_in(_enum_values("result_request.disposition"))})
  ),
  stale_reason TEXT CHECK (
    stale_reason IN ({_sql_in(_enum_values("result_request.stale_reason"))})
  ),
  accepted_result_created INTEGER NOT NULL CHECK (accepted_result_created IN (0, 1)),
  candidate_upload_id TEXT REFERENCES candidate_uploads(upload_id) ON DELETE RESTRICT,
  attempt_terminal_fact_id TEXT
    REFERENCES attempt_terminal_facts(attempt_terminal_fact_id) ON DELETE RESTRICT,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (attempt_id, request_digest),
  CHECK (
    (disposition = 'ACCEPTED' AND attempt_result_id IS NOT NULL AND stale_reason IS NULL)
    OR (disposition = 'STALE_ATTEMPT' AND attempt_result_id IS NULL
      AND stale_reason IS NOT NULL AND attempt_terminal_fact_id IS NULL)
    OR (disposition = 'UPLOAD_EXPIRED' AND attempt_result_id IS NULL
      AND stale_reason IS NULL AND candidate_upload_id IS NOT NULL
      AND attempt_terminal_fact_id IS NULL)
    OR (disposition IN ('EXPIRED_CURRENT', 'ALREADY_TERMINAL')
      AND attempt_result_id IS NULL AND stale_reason IS NULL
      AND attempt_terminal_fact_id IS NOT NULL)
    OR (disposition = 'RESULT_ALREADY_ACCEPTED' AND attempt_result_id IS NULL
      AND stale_reason IS NULL AND attempt_terminal_fact_id IS NULL)
  ),
  CHECK (disposition = 'ACCEPTED' OR accepted_result_created = 0)
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

CREATE TABLE IF NOT EXISTS health_probe_requests (
  health_probe_request_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  probe_kind TEXT NOT NULL CHECK (
    probe_kind IN ({_sql_in(_enum_values("health_probe.probe_kind"))})
  ),
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("health_probe.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  request_identity TEXT NOT NULL,
  subject_bindings_json TEXT NOT NULL,
  expected_revision INTEGER,
  implementation_digest TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  evidence_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK (
    state IN ({_sql_in(_enum_values("health_probe_request.state"))})
  ),
  outbox_id TEXT NOT NULL UNIQUE REFERENCES outbox(outbox_id) ON DELETE RESTRICT,
  not_after_ms INTEGER CHECK (not_after_ms IS NULL OR not_after_ms > created_at_ms),
  completed_at_ms INTEGER CHECK (completed_at_ms IS NULL OR completed_at_ms >= created_at_ms),
  health_probe_fact_id TEXT UNIQUE,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (probe_kind, scope_kind, scope_id, request_identity),
  CHECK (
    (state = 'PENDING' AND completed_at_ms IS NULL AND health_probe_fact_id IS NULL)
    OR (state = 'COMPLETED' AND completed_at_ms IS NOT NULL AND health_probe_fact_id IS NOT NULL)
    OR (state = 'SUPERSEDED' AND completed_at_ms IS NOT NULL AND health_probe_fact_id IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_health_probe_requests_pending
ON health_probe_requests(state, not_after_ms) WHERE state = 'PENDING';

CREATE TABLE IF NOT EXISTS health_probe_facts (
  health_probe_fact_id TEXT PRIMARY KEY,
  health_probe_request_id TEXT NOT NULL UNIQUE
    REFERENCES health_probe_requests(health_probe_request_id) ON DELETE RESTRICT,
  probe_kind TEXT NOT NULL CHECK (
    probe_kind IN ({_sql_in(_enum_values("health_probe.probe_kind"))})
  ),
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("health_probe.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  request_identity TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (
    outcome IN ({_sql_in(_enum_values("health_probe_fact.outcome"))})
  ),
  observed_revision INTEGER,
  implementation_digest TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  evidence_digest TEXT NOT NULL,
  integrity_failure_code TEXT CHECK (
    integrity_failure_code IS NULL
    OR integrity_failure_code IN ({
    _sql_in(_enum_values("health_probe_fact.integrity_failure_code"))
})
  ),
  subject_bindings_json TEXT NOT NULL,
  affected_run_ids_digest TEXT NOT NULL,
  health_observation_id TEXT NOT NULL UNIQUE,
  fact_digest TEXT NOT NULL UNIQUE,
  fanout_cursor_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (fanout_cursor_ordinal >= 0),
  fanout_completed_at_ms INTEGER CHECK (
    fanout_completed_at_ms IS NULL OR fanout_completed_at_ms >= recorded_at_ms
  ),
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (probe_kind, scope_kind, scope_id, request_identity),
  CHECK (
    (
      probe_kind = 'STORAGE_OBJECT_INTEGRITY'
      AND (
        (outcome = 'AVAILABLE' AND integrity_failure_code IS NULL)
        OR (
          outcome = 'UNAVAILABLE'
          AND integrity_failure_code IN ('MISSING', 'UNREADABLE', 'DIGEST_MISMATCH')
        )
      )
    )
    OR (
      probe_kind = 'SECRET_VERSION_INTEGRITY'
      AND (
        (outcome = 'AVAILABLE' AND integrity_failure_code IS NULL)
        OR (
          outcome = 'UNAVAILABLE'
          AND integrity_failure_code IN ('MISSING', 'UNREADABLE', 'KEYED_ATTESTATION_MISMATCH')
        )
      )
    )
    OR (
      probe_kind = 'FORGE_CONNECTIVITY'
      AND outcome IN ('AVAILABLE', 'UNAVAILABLE')
      AND integrity_failure_code IS NULL
    )
    OR (
      probe_kind = 'PROVIDER_ACCOUNT_STATUS'
      AND outcome IN ('AVAILABLE', 'UNAVAILABLE', 'RATE_LIMITED', 'EXHAUSTED')
      AND integrity_failure_code IS NULL
    )
  )
);

CREATE TABLE IF NOT EXISTS health_probe_fact_runs (
  health_probe_fact_id TEXT NOT NULL
    REFERENCES health_probe_facts(health_probe_fact_id) ON DELETE RESTRICT,
  member_ordinal INTEGER NOT NULL CHECK (member_ordinal >= 0),
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  transition_sequence INTEGER,
  recovery_evidence_id TEXT,
  PRIMARY KEY (health_probe_fact_id, member_ordinal),
  UNIQUE (health_probe_fact_id, run_id),
  CHECK (
    (transition_sequence IS NULL AND recovery_evidence_id IS NULL)
    OR transition_sequence IS NOT NULL
  )
);

CREATE TABLE IF NOT EXISTS health_observations (
  health_observation_id TEXT PRIMARY KEY,
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("health_probe.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  health_sequence INTEGER NOT NULL CHECK (health_sequence > 0),
  kind TEXT NOT NULL CHECK (kind IN ({_sql_in(_enum_values("health_observation.kind"))})),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("health_observation.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  subject_bindings_json TEXT NOT NULL,
  observed_revision INTEGER,
  effective_at_ms INTEGER NOT NULL CHECK (effective_at_ms >= 0),
  expires_at_ms INTEGER CHECK (expires_at_ms IS NULL OR expires_at_ms > effective_at_ms),
  payload_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (scope_kind, scope_id, health_sequence),
  UNIQUE (scope_kind, scope_id, source_kind, source_id)
);

CREATE INDEX IF NOT EXISTS idx_health_observations_scope_sequence
ON health_observations(scope_kind, scope_id, health_sequence);

CREATE TABLE IF NOT EXISTS recovery_evidence (
  recovery_evidence_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  recovery_sequence INTEGER NOT NULL CHECK (recovery_sequence > 0),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("recovery_evidence.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  resumed_wait_condition_id TEXT,
  resumed_human_boundary_id TEXT,
  human_resolution_id TEXT,
  activity_id TEXT,
  attempt_id TEXT,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  candidate_id TEXT,
  forge_observation_id TEXT,
  category TEXT NOT NULL CHECK (
    category IN ({_sql_in(_enum_values("recovery_evidence.category"))})
  ),
  failure_fingerprint TEXT NOT NULL,
  strategy_index INTEGER NOT NULL CHECK (strategy_index >= 0),
  selected_tactic TEXT NOT NULL CHECK (
    selected_tactic IN ({_sql_in(_enum_values("recovery_evidence.selected_tactic"))})
  ),
  attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
  repair_cycle_count INTEGER NOT NULL CHECK (repair_cycle_count >= 0),
  diagnosis_count INTEGER NOT NULL CHECK (diagnosis_count >= 0),
  rescue_epoch INTEGER NOT NULL CHECK (rescue_epoch >= 0),
  selected_fallback TEXT,
  health_observation_ids_digest TEXT NOT NULL,
  next_eligible_at_ms INTEGER CHECK (next_eligible_at_ms IS NULL OR next_eligible_at_ms >= 0),
  evidence_digest TEXT NOT NULL UNIQUE,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (run_id, recovery_sequence),
  UNIQUE (run_id, source_kind, source_id),
  CHECK ((resumed_human_boundary_id IS NULL) = (human_resolution_id IS NULL))
);

CREATE TABLE IF NOT EXISTS recovery_evidence_health_observations (
  recovery_evidence_id TEXT NOT NULL
    REFERENCES recovery_evidence(recovery_evidence_id) ON DELETE RESTRICT,
  observation_ordinal INTEGER NOT NULL CHECK (observation_ordinal >= 0),
  health_observation_id TEXT NOT NULL
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  PRIMARY KEY (recovery_evidence_id, observation_ordinal),
  UNIQUE (recovery_evidence_id, health_observation_id)
);

CREATE TABLE IF NOT EXISTS wait_conditions (
  wait_condition_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  reason TEXT NOT NULL CHECK (reason IN ({_sql_in(_enum_values("wait_condition.reason"))})),
  resume_state TEXT NOT NULL CHECK (resume_state IN ({_sql_in(_enum_values("run.state"))})),
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  candidate_id TEXT,
  policy_hash TEXT NOT NULL,
  forge_observation_id TEXT,
  not_before_ms INTEGER CHECK (not_before_ms IS NULL OR not_before_ms >= 0),
  wake_kind TEXT CHECK (
    wake_kind IS NULL OR wake_kind IN ({_sql_in(_enum_values("wait_condition.wake_kind"))})
  ),
  wake_identity_json TEXT,
  health_observation_ids_digest TEXT NOT NULL,
  panel_slots_digest TEXT NOT NULL,
  created_from_kind TEXT NOT NULL CHECK (
    created_from_kind IN ({_sql_in(_enum_values("wait_condition.created_from_kind"))})
  ),
  created_from_id TEXT NOT NULL,
  condition_digest TEXT NOT NULL UNIQUE,
  created_transition_sequence INTEGER NOT NULL CHECK (created_transition_sequence > 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (run_id, wait_condition_id),
  UNIQUE (created_from_kind, created_from_id),
  CHECK (not_before_ms IS NOT NULL OR wake_kind IS NOT NULL),
  CHECK ((wake_kind IS NULL) = (wake_identity_json IS NULL)),
  CHECK (
    (reason = 'CAPACITY' AND wake_kind = 'CAPACITY' AND not_before_ms IS NULL)
    OR (reason = 'RATE_LIMIT' AND not_before_ms IS NOT NULL
        AND (wake_kind IS NULL OR wake_kind = 'RATE_LIMIT_RESET'))
    OR (reason = 'BUDGET' AND not_before_ms IS NOT NULL AND wake_kind = 'BUDGET_WINDOW')
    OR (reason = 'BACKOFF' AND not_before_ms IS NOT NULL
        AND wake_kind IS NULL AND wake_identity_json IS NULL)
    OR (reason = 'EXTERNAL_DEPENDENCY' AND wake_kind = 'DEPENDENCY')
    OR (reason = 'FORGE_UNAVAILABLE' AND not_before_ms IS NOT NULL AND wake_kind = 'FORGE')
    OR (reason = 'STORAGE_RECOVERY' AND wake_kind = 'STORAGE')
    OR (reason = 'SECRET_RECOVERY' AND wake_kind = 'SECRET')
    OR (reason = 'EVIDENCE' AND not_before_ms IS NOT NULL AND wake_kind = 'EVIDENCE')
  )
);

CREATE INDEX IF NOT EXISTS idx_wait_conditions_reason ON wait_conditions(reason, run_id);

CREATE TABLE IF NOT EXISTS wait_condition_health_observations (
  wait_condition_id TEXT NOT NULL
    REFERENCES wait_conditions(wait_condition_id) ON DELETE RESTRICT,
  observation_ordinal INTEGER NOT NULL CHECK (observation_ordinal >= 0),
  health_observation_id TEXT NOT NULL
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  PRIMARY KEY (wait_condition_id, observation_ordinal),
  UNIQUE (wait_condition_id, health_observation_id)
);

CREATE TABLE IF NOT EXISTS wait_condition_panel_slots (
  wait_condition_id TEXT NOT NULL
    REFERENCES wait_conditions(wait_condition_id) ON DELETE RESTRICT,
  slot_ordinal INTEGER NOT NULL CHECK (slot_ordinal >= 0),
  activity_id TEXT NOT NULL REFERENCES activities(activity_id) ON DELETE RESTRICT,
  assignment_kind TEXT NOT NULL CHECK (assignment_kind IN ('REVIEW', 'ADJUDICATE')),
  panel_round INTEGER NOT NULL CHECK (panel_round > 0),
  slot_id TEXT NOT NULL,
  PRIMARY KEY (wait_condition_id, slot_ordinal),
  UNIQUE (wait_condition_id, activity_id),
  UNIQUE (wait_condition_id, assignment_kind, panel_round, slot_id)
);

CREATE TABLE IF NOT EXISTS human_boundaries (
  human_boundary_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  reason TEXT NOT NULL CHECK (
    reason IN ({_sql_in(_enum_values("human_boundary.reason"))})
  ),
  resume_state TEXT NOT NULL CHECK (resume_state IN ({_sql_in(_enum_values("run.state"))})),
  specification_generation INTEGER CHECK (
    specification_generation IS NULL OR specification_generation >= 0
  ),
  candidate_id TEXT,
  policy_hash TEXT,
  forge_observation_id TEXT,
  publication_id TEXT,
  publication_effect_generation INTEGER CHECK (
    publication_effect_generation IS NULL OR publication_effect_generation > 0
  ),
  ownership_project_id TEXT,
  ownership_deterministic_ref TEXT,
  ownership_change_request_external_id TEXT,
  ownership_run_marker TEXT,
  minimum_request TEXT NOT NULL CHECK (length(minimum_request) BETWEEN 1 AND 2048),
  evidence_refs_json TEXT NOT NULL,
  attempted_strategy_digests_json TEXT NOT NULL,
  required_resolution_kinds_json TEXT NOT NULL,
  created_from_kind TEXT NOT NULL CHECK (
    created_from_kind IN ({_sql_in(_enum_values("human_boundary.created_from_kind"))})
  ),
  created_from_id TEXT NOT NULL,
  packet_digest TEXT NOT NULL UNIQUE,
  created_transition_sequence INTEGER NOT NULL CHECK (created_transition_sequence > 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (created_from_kind, created_from_id),
  CHECK (
    (ownership_project_id IS NULL) = (ownership_deterministic_ref IS NULL)
    AND (ownership_project_id IS NULL) = (ownership_change_request_external_id IS NULL)
    AND (ownership_project_id IS NULL) = (ownership_run_marker IS NULL)
  ),
  CHECK (
    (
      reason = 'PUBLICATION_OWNERSHIP_CONFLICT'
      AND ownership_project_id IS NOT NULL
      AND created_from_kind = 'RECONCILIATION_FACT'
    )
    OR (
      reason != 'PUBLICATION_OWNERSHIP_CONFLICT'
      AND ownership_project_id IS NULL
      AND created_from_kind = 'RECOVERY_EVIDENCE'
    )
  )
);

CREATE INDEX IF NOT EXISTS idx_human_boundaries_run ON human_boundaries(run_id);

CREATE TABLE IF NOT EXISTS human_boundary_choices (
  human_boundary_id TEXT NOT NULL
    REFERENCES human_boundaries(human_boundary_id) ON DELETE RESTRICT,
  choice_ordinal INTEGER NOT NULL CHECK (choice_ordinal >= 0),
  choice_id TEXT NOT NULL,
  resolution_kind TEXT NOT NULL CHECK (
    resolution_kind IN ({_sql_in(_enum_values("human_resolution.resolution_kind"))})
  ),
  consequence TEXT NOT NULL CHECK (length(consequence) BETWEEN 1 AND 2048),
  PRIMARY KEY (human_boundary_id, choice_ordinal),
  UNIQUE (human_boundary_id, choice_id),
  UNIQUE (human_boundary_id, resolution_kind)
);

CREATE TABLE IF NOT EXISTS human_resolutions (
  human_resolution_id TEXT PRIMARY KEY,
  human_boundary_id TEXT NOT NULL UNIQUE
    REFERENCES human_boundaries(human_boundary_id) ON DELETE RESTRICT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  idempotency_key TEXT NOT NULL,
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("human_resolution.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  authenticated_principal_id TEXT NOT NULL,
  resolution_kind TEXT NOT NULL CHECK (
    resolution_kind IN ({_sql_in(_enum_values("human_resolution.resolution_kind"))})
  ),
  resolution_json TEXT NOT NULL,
  specification_generation INTEGER,
  candidate_id TEXT,
  policy_hash TEXT,
  forge_observation_id TEXT,
  publication_id TEXT,
  publication_effect_generation INTEGER,
  ownership_project_id TEXT,
  ownership_deterministic_ref TEXT,
  ownership_change_request_external_id TEXT,
  ownership_run_marker TEXT,
  resolution_digest TEXT NOT NULL UNIQUE,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (source_kind, idempotency_key)
);

CREATE TABLE IF NOT EXISTS capacity_reports (
  capacity_report_id TEXT PRIMARY KEY,
  pool_manager_id TEXT NOT NULL,
  report_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  report_sequence INTEGER NOT NULL CHECK (report_sequence > 0),
  observed_at_ms INTEGER NOT NULL CHECK (observed_at_ms >= 0),
  expires_at_ms INTEGER NOT NULL,
  configured_max_ttl_ms INTEGER NOT NULL CHECK (configured_max_ttl_ms > 0),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (pool_manager_id, report_id),
  UNIQUE (pool_manager_id, idempotency_key),
  UNIQUE (pool_manager_id, report_sequence),
  CHECK (
    expires_at_ms > accepted_at_ms
    AND expires_at_ms <= accepted_at_ms + configured_max_ttl_ms
  )
);

CREATE TABLE IF NOT EXISTS capacity_report_entries (
  capacity_report_id TEXT NOT NULL
    REFERENCES capacity_reports(capacity_report_id) ON DELETE RESTRICT,
  entry_ordinal INTEGER NOT NULL CHECK (entry_ordinal >= 0),
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("capacity_report.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  availability TEXT NOT NULL CHECK (
    availability IN ({_sql_in(_enum_values("capacity_report.availability"))})
  ),
  capacity_pool_id TEXT NOT NULL,
  worker_profile TEXT,
  available_slots INTEGER NOT NULL CHECK (available_slots >= 0),
  session_evidence_json TEXT,
  health_observation_id TEXT NOT NULL UNIQUE
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  PRIMARY KEY (capacity_report_id, entry_ordinal),
  UNIQUE (capacity_report_id, scope_kind, scope_id)
);

-- domain-model.md "Attempt Terminal Fact" closed matrix. This leaf (#683)
-- widens the table #680 originally narrowed to the pool-loss
-- WORKER_LOST/HEALTH_OBSERVATION member: the additional columns are the
-- frozen claim-deadline capacity classifier evidence (mode/issuance-key gate
-- plus health membership), populated only for `kind = 'CLAIM_DEADLINE'` and
-- otherwise left NULL for every other Fact kind.
CREATE TABLE IF NOT EXISTS attempt_terminal_facts (
  attempt_terminal_fact_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  kind TEXT NOT NULL CHECK (kind IN ({_sql_in(_enum_values("attempt_terminal_fact.kind"))})),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("attempt_terminal_fact.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  health_observation_id TEXT
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  expected_deadline_ms INTEGER,
  controller_now_ms INTEGER CHECK (
    controller_now_ms IS NULL OR expected_deadline_ms IS NULL
    OR controller_now_ms >= expected_deadline_ms
  ),
  capacity_disposition TEXT CHECK (
    capacity_disposition IS NULL
    OR capacity_disposition IN (
      {_sql_in(_enum_values("attempt_terminal_fact.capacity_disposition"))}
    )
  ),
  health_observation_ids_digest TEXT,
  resolved_provider_secret_ref TEXT,
  controller_mode_revision INTEGER,
  controller_mode TEXT,
  capability_registry_revision INTEGER,
  selected_issuance_key_id TEXT,
  replacement_offer_disposition TEXT CHECK (
    replacement_offer_disposition IS NULL
    OR replacement_offer_disposition IN (
      {_sql_in(_enum_values("attempt_terminal_fact.replacement_offer_disposition"))}
    )
  ),
  fact_digest TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (attempt_id, kind, source_kind, source_id),
  CHECK (
    kind != 'WORKER_LOST'
    OR (source_kind = 'HEALTH_OBSERVATION' AND health_observation_id IS NOT NULL)
  ),
  CHECK (kind = 'CLAIM_DEADLINE' OR capacity_disposition IS NULL),
  CHECK (kind = 'CLAIM_DEADLINE' OR replacement_offer_disposition IS NULL)
);

CREATE TABLE IF NOT EXISTS attempt_terminal_fact_health_observations (
  attempt_terminal_fact_id TEXT NOT NULL
    REFERENCES attempt_terminal_facts(attempt_terminal_fact_id) ON DELETE RESTRICT,
  observation_ordinal INTEGER NOT NULL CHECK (observation_ordinal >= 0),
  health_observation_id TEXT NOT NULL UNIQUE
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  PRIMARY KEY (attempt_terminal_fact_id, observation_ordinal)
);

CREATE TABLE IF NOT EXISTS timer_facts (
  timer_fact_id TEXT PRIMARY KEY,
  run_id TEXT,
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("timer_fact.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  fired_for_ms INTEGER NOT NULL CHECK (fired_for_ms >= 0),
  controller_now_ms INTEGER NOT NULL CHECK (controller_now_ms >= fired_for_ms),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("timer_fact.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  fact_digest TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (scope_kind, scope_id, fired_for_ms)
);

CREATE TABLE IF NOT EXISTS worker_loss_reports (
  worker_loss_report_id TEXT PRIMARY KEY,
  pool_manager_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  worker_session_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  reason TEXT NOT NULL CHECK (reason IN ({_sql_in(_enum_values("worker_loss_report.reason"))})),
  observed_at_ms INTEGER NOT NULL CHECK (observed_at_ms >= 0),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (
    outcome IN ({_sql_in(_enum_values("worker_loss_report.outcome"))})
  ),
  health_observation_id TEXT
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  attempt_terminal_fact_id TEXT
    REFERENCES attempt_terminal_facts(attempt_terminal_fact_id) ON DELETE RESTRICT,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (pool_manager_id, idempotency_key),
  CHECK (
    (outcome = 'ACCEPTED' AND health_observation_id IS NOT NULL
      AND attempt_terminal_fact_id IS NOT NULL)
    OR (outcome = 'STALE' AND health_observation_id IS NULL
      AND attempt_terminal_fact_id IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS budget_reports (
  budget_report_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  accounting_scope_id TEXT NOT NULL,
  budget_policy_ref TEXT NOT NULL,
  budget_reset_window_ref TEXT NOT NULL,
  window_id TEXT NOT NULL,
  window_start_ms INTEGER NOT NULL CHECK (window_start_ms >= 0),
  reset_at_ms INTEGER NOT NULL CHECK (reset_at_ms > window_start_ms),
  source_sequence INTEGER NOT NULL CHECK (source_sequence > 0),
  source_revision TEXT NOT NULL,
  limit_microunits INTEGER NOT NULL CHECK (limit_microunits > 0),
  consumed_microunits INTEGER NOT NULL CHECK (consumed_microunits >= 0),
  availability TEXT NOT NULL CHECK (
    availability IN ({_sql_in(_enum_values("budget_report.availability"))})
  ),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  report_digest TEXT NOT NULL,
  affected_run_ids_digest TEXT NOT NULL,
  next_member_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (next_member_ordinal >= 0),
  fanout_completed_at_ms INTEGER,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms > accepted_at_ms),
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  UNIQUE (project_id, accounting_scope_id, source_sequence),
  UNIQUE (project_id, accounting_scope_id, source_revision)
);

CREATE TABLE IF NOT EXISTS budget_report_runs (
  budget_report_id TEXT NOT NULL
    REFERENCES budget_reports(budget_report_id) ON DELETE RESTRICT,
  member_ordinal INTEGER NOT NULL CHECK (member_ordinal >= 0),
  run_id TEXT NOT NULL,
  PRIMARY KEY (budget_report_id, member_ordinal),
  UNIQUE (budget_report_id, run_id)
);

CREATE TABLE IF NOT EXISTS credential_rotation_requests (
  credential_rotation_request_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  worker_id TEXT NOT NULL,
  worker_session_id TEXT NOT NULL,
  attempt_capability_digest TEXT NOT NULL,
  launch_attestation_id TEXT NOT NULL,
  provider_account_ref TEXT,
  secret_id TEXT NOT NULL,
  expected_prior_version INTEGER NOT NULL CHECK (expected_prior_version > 0),
  secret_request_attestation_id TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  disposition TEXT NOT NULL CHECK (
    disposition IN ({_sql_in(_enum_values("credential_rotation_request.disposition"))})
  ),
  credential_rotation_receipt_id TEXT,
  accepted_version INTEGER CHECK (accepted_version IS NULL OR accepted_version > 0),
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (credential_rotation_receipt_id),
  CHECK (
    (disposition = 'APPLIED'
      AND credential_rotation_receipt_id IS NOT NULL
      AND accepted_version IS NOT NULL
      AND accepted_version = current_version)
    OR
    (disposition = 'CAS_LOST'
      AND credential_rotation_receipt_id IS NULL
      AND accepted_version IS NULL)
  ),
  FOREIGN KEY (attempt_id, activity_id, attempt_generation)
    REFERENCES attempts(attempt_id, activity_id, generation)
);

CREATE TABLE IF NOT EXISTS secret_version_runs (
  secret_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  run_ordinal INTEGER NOT NULL CHECK (run_ordinal >= 0),
  run_id TEXT NOT NULL,
  PRIMARY KEY (secret_id, version, run_ordinal),
  UNIQUE (secret_id, version, run_id)
);

CREATE TABLE IF NOT EXISTS secret_version_fanouts (
  secret_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  member_count INTEGER NOT NULL CHECK (member_count >= 0),
  next_member_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (next_member_ordinal >= 0),
  fanout_completed_at_ms INTEGER,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (secret_id, version)
);
"""

_V8_TO_V9 = f"""
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

CREATE TABLE IF NOT EXISTS health_observations (
  health_observation_id TEXT PRIMARY KEY,
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("health_probe.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  health_sequence INTEGER NOT NULL CHECK (health_sequence > 0),
  kind TEXT NOT NULL CHECK (kind IN ({_sql_in(_enum_values("health_observation.kind"))})),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("health_observation.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  subject_bindings_json TEXT NOT NULL,
  observed_revision INTEGER,
  effective_at_ms INTEGER NOT NULL CHECK (effective_at_ms >= 0),
  expires_at_ms INTEGER CHECK (expires_at_ms IS NULL OR expires_at_ms > effective_at_ms),
  payload_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (scope_kind, scope_id, health_sequence),
  UNIQUE (scope_kind, scope_id, source_kind, source_id)
);

CREATE INDEX IF NOT EXISTS idx_health_observations_scope_sequence
ON health_observations(scope_kind, scope_id, health_sequence);

CREATE TABLE IF NOT EXISTS capacity_reports (
  capacity_report_id TEXT PRIMARY KEY,
  pool_manager_id TEXT NOT NULL,
  report_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  report_sequence INTEGER NOT NULL CHECK (report_sequence > 0),
  observed_at_ms INTEGER NOT NULL CHECK (observed_at_ms >= 0),
  expires_at_ms INTEGER NOT NULL,
  configured_max_ttl_ms INTEGER NOT NULL CHECK (configured_max_ttl_ms > 0),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (pool_manager_id, report_id),
  UNIQUE (pool_manager_id, idempotency_key),
  UNIQUE (pool_manager_id, report_sequence),
  CHECK (
    expires_at_ms > accepted_at_ms
    AND expires_at_ms <= accepted_at_ms + configured_max_ttl_ms
  )
);

CREATE TABLE IF NOT EXISTS capacity_report_entries (
  capacity_report_id TEXT NOT NULL
    REFERENCES capacity_reports(capacity_report_id) ON DELETE RESTRICT,
  entry_ordinal INTEGER NOT NULL CHECK (entry_ordinal >= 0),
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("capacity_report.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  availability TEXT NOT NULL CHECK (
    availability IN ({_sql_in(_enum_values("capacity_report.availability"))})
  ),
  capacity_pool_id TEXT NOT NULL,
  worker_profile TEXT,
  available_slots INTEGER NOT NULL CHECK (available_slots >= 0),
  session_evidence_json TEXT,
  health_observation_id TEXT NOT NULL UNIQUE
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  PRIMARY KEY (capacity_report_id, entry_ordinal),
  UNIQUE (capacity_report_id, scope_kind, scope_id)
);

-- domain-model.md "Attempt Terminal Fact" closed matrix. This leaf (#683)
-- widens the table #680 originally narrowed to the pool-loss
-- WORKER_LOST/HEALTH_OBSERVATION member: the additional columns are the
-- frozen claim-deadline capacity classifier evidence (mode/issuance-key gate
-- plus health membership), populated only for `kind = 'CLAIM_DEADLINE'` and
-- otherwise left NULL for every other Fact kind.
CREATE TABLE IF NOT EXISTS attempt_terminal_facts (
  attempt_terminal_fact_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  kind TEXT NOT NULL CHECK (kind IN ({_sql_in(_enum_values("attempt_terminal_fact.kind"))})),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("attempt_terminal_fact.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  health_observation_id TEXT
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  expected_deadline_ms INTEGER,
  controller_now_ms INTEGER CHECK (
    controller_now_ms IS NULL OR expected_deadline_ms IS NULL
    OR controller_now_ms >= expected_deadline_ms
  ),
  capacity_disposition TEXT CHECK (
    capacity_disposition IS NULL
    OR capacity_disposition IN (
      {_sql_in(_enum_values("attempt_terminal_fact.capacity_disposition"))}
    )
  ),
  health_observation_ids_digest TEXT,
  resolved_provider_secret_ref TEXT,
  controller_mode_revision INTEGER,
  controller_mode TEXT,
  capability_registry_revision INTEGER,
  selected_issuance_key_id TEXT,
  replacement_offer_disposition TEXT CHECK (
    replacement_offer_disposition IS NULL
    OR replacement_offer_disposition IN (
      {_sql_in(_enum_values("attempt_terminal_fact.replacement_offer_disposition"))}
    )
  ),
  fact_digest TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (attempt_id, kind, source_kind, source_id),
  CHECK (
    kind != 'WORKER_LOST'
    OR (source_kind = 'HEALTH_OBSERVATION' AND health_observation_id IS NOT NULL)
  ),
  CHECK (kind = 'CLAIM_DEADLINE' OR capacity_disposition IS NULL),
  CHECK (kind = 'CLAIM_DEADLINE' OR replacement_offer_disposition IS NULL)
);

CREATE TABLE IF NOT EXISTS attempt_terminal_fact_health_observations (
  attempt_terminal_fact_id TEXT NOT NULL
    REFERENCES attempt_terminal_facts(attempt_terminal_fact_id) ON DELETE RESTRICT,
  observation_ordinal INTEGER NOT NULL CHECK (observation_ordinal >= 0),
  health_observation_id TEXT NOT NULL UNIQUE
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  PRIMARY KEY (attempt_terminal_fact_id, observation_ordinal)
);

CREATE TABLE IF NOT EXISTS timer_facts (
  timer_fact_id TEXT PRIMARY KEY,
  run_id TEXT,
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("timer_fact.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  fired_for_ms INTEGER NOT NULL CHECK (fired_for_ms >= 0),
  controller_now_ms INTEGER NOT NULL CHECK (controller_now_ms >= fired_for_ms),
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("timer_fact.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  fact_digest TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (scope_kind, scope_id, fired_for_ms)
);

CREATE TABLE IF NOT EXISTS worker_loss_reports (
  worker_loss_report_id TEXT PRIMARY KEY,
  pool_manager_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  worker_session_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
  reason TEXT NOT NULL CHECK (reason IN ({_sql_in(_enum_values("worker_loss_report.reason"))})),
  observed_at_ms INTEGER NOT NULL CHECK (observed_at_ms >= 0),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (
    outcome IN ({_sql_in(_enum_values("worker_loss_report.outcome"))})
  ),
  health_observation_id TEXT
    REFERENCES health_observations(health_observation_id) ON DELETE RESTRICT,
  attempt_terminal_fact_id TEXT
    REFERENCES attempt_terminal_facts(attempt_terminal_fact_id) ON DELETE RESTRICT,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (pool_manager_id, idempotency_key),
  CHECK (
    (outcome = 'ACCEPTED' AND health_observation_id IS NOT NULL
      AND attempt_terminal_fact_id IS NOT NULL)
    OR (outcome = 'STALE' AND health_observation_id IS NULL
      AND attempt_terminal_fact_id IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS budget_reports (
  budget_report_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  accounting_scope_id TEXT NOT NULL,
  budget_policy_ref TEXT NOT NULL,
  budget_reset_window_ref TEXT NOT NULL,
  window_id TEXT NOT NULL,
  window_start_ms INTEGER NOT NULL CHECK (window_start_ms >= 0),
  reset_at_ms INTEGER NOT NULL CHECK (reset_at_ms > window_start_ms),
  source_sequence INTEGER NOT NULL CHECK (source_sequence > 0),
  source_revision TEXT NOT NULL,
  limit_microunits INTEGER NOT NULL CHECK (limit_microunits > 0),
  consumed_microunits INTEGER NOT NULL CHECK (consumed_microunits >= 0),
  availability TEXT NOT NULL CHECK (
    availability IN ({_sql_in(_enum_values("budget_report.availability"))})
  ),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  report_digest TEXT NOT NULL,
  affected_run_ids_digest TEXT NOT NULL,
  next_member_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (next_member_ordinal >= 0),
  fanout_completed_at_ms INTEGER,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms > accepted_at_ms),
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  UNIQUE (project_id, accounting_scope_id, source_sequence),
  UNIQUE (project_id, accounting_scope_id, source_revision)
);

CREATE TABLE IF NOT EXISTS budget_report_runs (
  budget_report_id TEXT NOT NULL
    REFERENCES budget_reports(budget_report_id) ON DELETE RESTRICT,
  member_ordinal INTEGER NOT NULL CHECK (member_ordinal >= 0),
  run_id TEXT NOT NULL,
  PRIMARY KEY (budget_report_id, member_ordinal),
  UNIQUE (budget_report_id, run_id)
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
  current_recovery_evidence_id TEXT,
  wait_condition_id TEXT,
  human_boundary_id TEXT,
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

_ADD_CURRENT_RECOVERY_EVIDENCE_ID = """
ALTER TABLE runs ADD COLUMN current_recovery_evidence_id TEXT;
"""

_ADD_WAIT_CONDITION_ID = """
ALTER TABLE runs ADD COLUMN wait_condition_id TEXT;
"""

_V16_TO_V17 = f"""
CREATE TABLE IF NOT EXISTS health_probe_requests (
  health_probe_request_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  probe_kind TEXT NOT NULL CHECK (
    probe_kind IN ({_sql_in(_enum_values("health_probe.probe_kind"))})
  ),
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("health_probe.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  request_identity TEXT NOT NULL,
  subject_bindings_json TEXT NOT NULL,
  expected_revision INTEGER,
  implementation_digest TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  evidence_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK (
    state IN ({_sql_in(_enum_values("health_probe_request.state"))})
  ),
  outbox_id TEXT NOT NULL UNIQUE REFERENCES outbox(outbox_id) ON DELETE RESTRICT,
  not_after_ms INTEGER CHECK (not_after_ms IS NULL OR not_after_ms > created_at_ms),
  completed_at_ms INTEGER CHECK (completed_at_ms IS NULL OR completed_at_ms >= created_at_ms),
  health_probe_fact_id TEXT UNIQUE,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (probe_kind, scope_kind, scope_id, request_identity),
  CHECK (
    (state = 'PENDING' AND completed_at_ms IS NULL AND health_probe_fact_id IS NULL)
    OR (state = 'COMPLETED' AND completed_at_ms IS NOT NULL AND health_probe_fact_id IS NOT NULL)
    OR (state = 'SUPERSEDED' AND completed_at_ms IS NOT NULL AND health_probe_fact_id IS NULL)
  )
);
CREATE INDEX IF NOT EXISTS idx_health_probe_requests_pending
ON health_probe_requests(state, not_after_ms) WHERE state = 'PENDING';
CREATE TABLE IF NOT EXISTS health_probe_facts (
  health_probe_fact_id TEXT PRIMARY KEY,
  health_probe_request_id TEXT NOT NULL UNIQUE
    REFERENCES health_probe_requests(health_probe_request_id) ON DELETE RESTRICT,
  probe_kind TEXT NOT NULL CHECK (
    probe_kind IN ({_sql_in(_enum_values("health_probe.probe_kind"))})
  ),
  scope_kind TEXT NOT NULL CHECK (
    scope_kind IN ({_sql_in(_enum_values("health_probe.scope_kind"))})
  ),
  scope_id TEXT NOT NULL,
  request_identity TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (
    outcome IN ({_sql_in(_enum_values("health_probe_fact.outcome"))})
  ),
  observed_revision INTEGER,
  implementation_digest TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  evidence_digest TEXT NOT NULL,
  integrity_failure_code TEXT CHECK (
    integrity_failure_code IS NULL
    OR integrity_failure_code IN ({
    _sql_in(_enum_values("health_probe_fact.integrity_failure_code"))
})
  ),
  subject_bindings_json TEXT NOT NULL,
  affected_run_ids_digest TEXT NOT NULL,
  health_observation_id TEXT NOT NULL UNIQUE,
  fact_digest TEXT NOT NULL UNIQUE,
  fanout_cursor_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (fanout_cursor_ordinal >= 0),
  fanout_completed_at_ms INTEGER CHECK (
    fanout_completed_at_ms IS NULL OR fanout_completed_at_ms >= recorded_at_ms
  ),
  recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
  UNIQUE (probe_kind, scope_kind, scope_id, request_identity),
  CHECK (
    (
      probe_kind = 'STORAGE_OBJECT_INTEGRITY'
      AND (
        (outcome = 'AVAILABLE' AND integrity_failure_code IS NULL)
        OR (
          outcome = 'UNAVAILABLE'
          AND integrity_failure_code IN ('MISSING', 'UNREADABLE', 'DIGEST_MISMATCH')
        )
      )
    )
    OR (
      probe_kind = 'SECRET_VERSION_INTEGRITY'
      AND (
        (outcome = 'AVAILABLE' AND integrity_failure_code IS NULL)
        OR (
          outcome = 'UNAVAILABLE'
          AND integrity_failure_code IN ('MISSING', 'UNREADABLE', 'KEYED_ATTESTATION_MISMATCH')
        )
      )
    )
    OR (
      probe_kind = 'FORGE_CONNECTIVITY'
      AND outcome IN ('AVAILABLE', 'UNAVAILABLE')
      AND integrity_failure_code IS NULL
    )
    OR (
      probe_kind = 'PROVIDER_ACCOUNT_STATUS'
      AND outcome IN ('AVAILABLE', 'UNAVAILABLE', 'RATE_LIMITED', 'EXHAUSTED')
      AND integrity_failure_code IS NULL
    )
  )
);
CREATE TABLE IF NOT EXISTS health_probe_fact_runs (
  health_probe_fact_id TEXT NOT NULL
    REFERENCES health_probe_facts(health_probe_fact_id) ON DELETE RESTRICT,
  member_ordinal INTEGER NOT NULL CHECK (member_ordinal >= 0),
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  transition_sequence INTEGER,
  recovery_evidence_id TEXT,
  PRIMARY KEY (health_probe_fact_id, member_ordinal),
  UNIQUE (health_probe_fact_id, run_id),
  CHECK (
    (transition_sequence IS NULL AND recovery_evidence_id IS NULL)
    OR transition_sequence IS NOT NULL
  )
);
"""

_ADD_HUMAN_BOUNDARY_ID = """
ALTER TABLE runs ADD COLUMN human_boundary_id TEXT;
"""

_V17_TO_V18 = f"""
CREATE TABLE IF NOT EXISTS human_boundaries (
  human_boundary_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  reason TEXT NOT NULL CHECK (
    reason IN ({_sql_in(_enum_values("human_boundary.reason"))})
  ),
  resume_state TEXT NOT NULL CHECK (resume_state IN ({_sql_in(_enum_values("run.state"))})),
  specification_generation INTEGER CHECK (
    specification_generation IS NULL OR specification_generation >= 0
  ),
  candidate_id TEXT,
  policy_hash TEXT,
  forge_observation_id TEXT,
  publication_id TEXT,
  publication_effect_generation INTEGER CHECK (
    publication_effect_generation IS NULL OR publication_effect_generation > 0
  ),
  ownership_project_id TEXT,
  ownership_deterministic_ref TEXT,
  ownership_change_request_external_id TEXT,
  ownership_run_marker TEXT,
  minimum_request TEXT NOT NULL CHECK (length(minimum_request) BETWEEN 1 AND 2048),
  evidence_refs_json TEXT NOT NULL,
  attempted_strategy_digests_json TEXT NOT NULL,
  required_resolution_kinds_json TEXT NOT NULL,
  created_from_kind TEXT NOT NULL CHECK (
    created_from_kind IN ({_sql_in(_enum_values("human_boundary.created_from_kind"))})
  ),
  created_from_id TEXT NOT NULL,
  packet_digest TEXT NOT NULL UNIQUE,
  created_transition_sequence INTEGER NOT NULL CHECK (created_transition_sequence > 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (created_from_kind, created_from_id),
  CHECK (
    (ownership_project_id IS NULL) = (ownership_deterministic_ref IS NULL)
    AND (ownership_project_id IS NULL) = (ownership_change_request_external_id IS NULL)
    AND (ownership_project_id IS NULL) = (ownership_run_marker IS NULL)
  ),
  CHECK (
    (
      reason = 'PUBLICATION_OWNERSHIP_CONFLICT'
      AND ownership_project_id IS NOT NULL
      AND created_from_kind = 'RECONCILIATION_FACT'
    )
    OR (
      reason != 'PUBLICATION_OWNERSHIP_CONFLICT'
      AND ownership_project_id IS NULL
      AND created_from_kind = 'RECOVERY_EVIDENCE'
    )
  )
);
CREATE INDEX IF NOT EXISTS idx_human_boundaries_run ON human_boundaries(run_id);
CREATE TABLE IF NOT EXISTS human_boundary_choices (
  human_boundary_id TEXT NOT NULL
    REFERENCES human_boundaries(human_boundary_id) ON DELETE RESTRICT,
  choice_ordinal INTEGER NOT NULL CHECK (choice_ordinal >= 0),
  choice_id TEXT NOT NULL,
  resolution_kind TEXT NOT NULL CHECK (
    resolution_kind IN ({_sql_in(_enum_values("human_resolution.resolution_kind"))})
  ),
  consequence TEXT NOT NULL CHECK (length(consequence) BETWEEN 1 AND 2048),
  PRIMARY KEY (human_boundary_id, choice_ordinal),
  UNIQUE (human_boundary_id, choice_id),
  UNIQUE (human_boundary_id, resolution_kind)
);
CREATE TABLE IF NOT EXISTS human_resolutions (
  human_resolution_id TEXT PRIMARY KEY,
  human_boundary_id TEXT NOT NULL UNIQUE
    REFERENCES human_boundaries(human_boundary_id) ON DELETE RESTRICT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  idempotency_key TEXT NOT NULL,
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("human_resolution.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  authenticated_principal_id TEXT NOT NULL,
  resolution_kind TEXT NOT NULL CHECK (
    resolution_kind IN ({_sql_in(_enum_values("human_resolution.resolution_kind"))})
  ),
  resolution_json TEXT NOT NULL,
  specification_generation INTEGER,
  candidate_id TEXT,
  policy_hash TEXT,
  forge_observation_id TEXT,
  publication_id TEXT,
  publication_effect_generation INTEGER,
  ownership_project_id TEXT,
  ownership_deterministic_ref TEXT,
  ownership_change_request_external_id TEXT,
  ownership_run_marker TEXT,
  resolution_digest TEXT NOT NULL UNIQUE,
  accepted_at_ms INTEGER NOT NULL CHECK (accepted_at_ms >= 0),
  UNIQUE (source_kind, idempotency_key)
);
"""

_V18_TO_V19 = _SCHEMA[_SCHEMA.index("CREATE TABLE IF NOT EXISTS review_receipts") :]

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

    def _existing_columns(self, table: str) -> set[str] | None:
        """``None`` if ``table`` does not exist yet, else its current column names."""
        exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()
        if exists is None:
            return None
        return {str(row["name"]) for row in self.conn.execute(f"PRAGMA table_info({table})")}

    def _v13_to_v14_script(self) -> str:
        """``ALTER TABLE ADD COLUMN`` fragments for #683's new liveness/deadline columns.

        A pre-v14 database only lacks these columns on tables that already
        existed (``attempts`` since v8, ``attempt_terminal_facts`` since
        v10); ``_SCHEMA``'s own ``CREATE TABLE IF NOT EXISTS`` already
        creates both in their final v14 shape for any database fresh enough
        to not have them yet, so this fragment is empty in that case.
        """
        statements: list[str] = []
        attempt_columns = self._existing_columns("attempts")
        if attempt_columns is not None and "last_liveness_sequence" not in attempt_columns:
            statements.append("ALTER TABLE attempts ADD COLUMN last_liveness_sequence INTEGER;")
        terminal_fact_columns = self._existing_columns("attempt_terminal_facts")
        if terminal_fact_columns is not None:
            for name, decl in _NEW_ATTEMPT_TERMINAL_FACT_COLUMNS.items():
                if name not in terminal_fact_columns:
                    statements.append(
                        f"ALTER TABLE attempt_terminal_facts ADD COLUMN {name} {decl};"
                    )
        return "\n".join(statements)

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
        if current not in {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}:
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
        v13_to_v14 = self._v13_to_v14_script()
        add_recovery_pointer = (
            _ADD_CURRENT_RECOVERY_EVIDENCE_ID
            if current >= 2 and "current_recovery_evidence_id" not in run_columns
            else ""
        )
        add_wait_condition_pointer = (
            _ADD_WAIT_CONDITION_ID
            if current >= 2 and "wait_condition_id" not in run_columns
            else ""
        )
        add_human_boundary_pointer = (
            _ADD_HUMAN_BOUNDARY_ID
            if current >= 2 and "human_boundary_id" not in run_columns
            else ""
        )
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
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
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
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
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
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
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
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
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
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
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
                # activities/attempts/attempt_claims tables and later additive
                # tables (all CREATE TABLE IF NOT EXISTS).
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                    + "\n"
                    + _FORGE_OBSERVATION_SCHEDULE_INDEXES
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
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _V8_TO_V9
                    + "\n"
                    + _SCHEMA
                    + "\n"
                    + v13_to_v14
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-launch-attestations-and-reports",
                        _now_ms(),
                    ),
                )
            elif current == 9:
                assert current == 9
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v13_to_v14
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-candidate-transfer",
                        _now_ms(),
                    ),
                )
            elif current == 10:
                assert current == 10
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v13_to_v14
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-capacity-budget-worker-loss-reports",
                        _now_ms(),
                    ),
                )
            elif current == 11:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v13_to_v14
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-idempotent-results-terminal-facts",
                        _now_ms(),
                    ),
                )
            elif current == 12:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v13_to_v14
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-credential-rotation",
                        _now_ms(),
                    ),
                )
            elif current == 13:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + v13_to_v14
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-liveness-and-deadline-processing",
                        _now_ms(),
                    ),
                )
            elif current == 14:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + add_recovery_pointer
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-recovery-evidence",
                        _now_ms(),
                    ),
                )
            elif current == 15:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _SCHEMA
                    + "\n"
                    + add_wait_condition_pointer
                    + add_human_boundary_pointer
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-wait-and-wake-processing",
                        _now_ms(),
                    ),
                )
            elif current == 16:
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _V16_TO_V17
                    + "\n"
                    + _V17_TO_V18
                    + "\n"
                    + add_human_boundary_pointer
                    + "\n"
                    + _V18_TO_V19
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-health-probes",
                        _now_ms(),
                    ),
                )
            elif current == 17:
                assert current == 17
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n"
                    + _V17_TO_V18
                    + "\n"
                    + add_human_boundary_pointer
                    + "\n"
                    + _V18_TO_V19
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-review-consensus-panels",
                        _now_ms(),
                    ),
                )
            else:
                assert current == 18
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n" + add_human_boundary_pointer + "\n" + _V18_TO_V19
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-review-consensus-panels",
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
            affected_digest = self._freeze_secret_version_membership(secret_id, target_version)
            self.conn.execute(
                "INSERT INTO secret_versions("
                "secret_id, version, creation_receipt_id, storage_path, "
                "affected_run_ids_digest, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    secret_id,
                    target_version,
                    receipt_id,
                    storage_path,
                    affected_digest,
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
            self._run_secret_version_fanout(secret_id, target_version)
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

    def _freeze_secret_version_membership(self, secret_id: str, version: int) -> str:
        """Freeze the affected active-Run membership for one new Secret Version.

        Must run inside the same writer transaction that installs the Secret
        Version row, before that transaction's ``INSERT INTO secret_versions``
        (which stores the returned digest). No Wait Condition/Human Boundary
        leaf has landed yet (see ``RunStore._waiting_run_ids``), so there is
        currently no queryable Run scoped to an exact Secret ID/minimum
        version and the frozen membership is always empty today. The fanout
        intent is still durably recorded so the restartable reconciler and
        its cursor are exercised end to end, and so a later leaf that adds
        real Secret-scoped Wait/Boundary membership only has to change what
        is frozen here, not the commit/fanout protocol itself.
        """
        members: list[str] = []
        for ordinal, run_id in enumerate(members):
            self.conn.execute(
                "INSERT INTO secret_version_runs(secret_id, version, run_ordinal, run_id) "
                "VALUES (?, ?, ?, ?)",
                (secret_id, version, ordinal, run_id),
            )
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO secret_version_fanouts("
            "secret_id, version, member_count, next_member_ordinal, "
            "fanout_completed_at_ms, created_at_ms) VALUES (?, ?, ?, 0, ?, ?)",
            (secret_id, version, len(members), now if not members else None, now),
        )
        return affected_run_ids_digest([{"run_id": run_id} for run_id in members])

    def run_secret_version_fanout(self, secret_id: str, version: int) -> None:
        """Advance one Secret Version's restartable per-Run wake fanout.

        Safe to call repeatedly -- once right after install, and again from a
        startup or crash-reconciliation sweep: each member Transition is
        idempotent by trigger identity and the durable cursor only advances
        transactionally after that Transition commits.
        """
        require_lowercase_uuid(secret_id, field="secret_id")
        with self.transaction():
            self._run_secret_version_fanout(secret_id, version)

    def _run_secret_version_fanout(self, secret_id: str, version: int) -> None:
        """Must run inside ``self.transaction()``."""
        from orcest.workflow_reducer.ledger import apply, load_view
        from orcest.workflow_reducer.types import Trigger

        row = self.conn.execute(
            "SELECT * FROM secret_version_fanouts WHERE secret_id = ? AND version = ?",
            (secret_id, version),
        ).fetchone()
        if row is None:
            raise RunStoreError(f"secret version fanout {secret_id!r}:{version} was not found")
        if row["fanout_completed_at_ms"] is not None:
            return
        members = self.conn.execute(
            "SELECT run_ordinal, run_id FROM secret_version_runs WHERE secret_id = ? "
            "AND version = ? AND run_ordinal >= ? ORDER BY run_ordinal",
            (secret_id, version, row["next_member_ordinal"]),
        ).fetchall()
        secret_version_key = f"{secret_id}:{version}"
        for member in members:
            view = load_view(self, member["run_id"])
            if view is not None:
                apply(
                    self,
                    view,
                    Trigger(
                        kind="SECRET_VERSION",
                        trigger_id=secret_version_key,
                        facts={"wakes_wait": True},
                    ),
                    run_id=member["run_id"],
                )
            self.conn.execute(
                "UPDATE secret_version_fanouts SET next_member_ordinal = ? "
                "WHERE secret_id = ? AND version = ?",
                (member["run_ordinal"] + 1, secret_id, version),
            )
        cursor = self.conn.execute(
            "SELECT next_member_ordinal AS n FROM secret_version_fanouts "
            "WHERE secret_id = ? AND version = ?",
            (secret_id, version),
        ).fetchone()["n"]
        if cursor >= row["member_count"]:
            self.conn.execute(
                "UPDATE secret_version_fanouts SET fanout_completed_at_ms = ? "
                "WHERE secret_id = ? AND version = ?",
                (_now_ms(), secret_id, version),
            )

    def get_credential_rotation_request(
        self, credential_rotation_request_id: str
    ) -> CredentialRotationRequestResult | None:
        require_lowercase_uuid(
            credential_rotation_request_id, field="credential_rotation_request_id"
        )
        row = self.conn.execute(
            "SELECT * FROM credential_rotation_requests WHERE credential_rotation_request_id = ?",
            (credential_rotation_request_id,),
        ).fetchone()
        return None if row is None else _row_to_credential_rotation_request(row, replayed=True)

    def _credential_rotation_result_response(
        self,
        *,
        credential_rotation_request_id: str,
        disposition: str,
        secret_id: str,
        expected_prior_version: int,
        current_version: int,
        accepted_version: int | None = None,
        credential_rotation_receipt_id: str | None = None,
    ) -> tuple[int, str, str]:
        body: dict[str, object] = {
            "protocol": CREDENTIAL_ROTATION_RESULT_PROTOCOL,
            "credential_rotation_request_id": credential_rotation_request_id,
            "disposition": disposition,
            "secret_id": secret_id,
            "expected_prior_version": expected_prior_version,
            "current_version": current_version,
        }
        if disposition == "APPLIED":
            body["accepted_version"] = accepted_version
            body["credential_rotation_receipt_id"] = credential_rotation_receipt_id
            http_status = 200
        else:
            http_status = 409
        body_json = canonical_json_text(body)
        digest = response_digest({"http_status": http_status, "body": body})
        return http_status, body_json, digest

    def require_current_rotation_authority(
        self,
        *,
        attempt_id: str,
        activity_id: str,
        attempt_generation: int,
        worker_id: str,
        worker_session_id: str,
        attempt_capability_digest: str,
        launch_attestation_id: str,
        provider_account_ref: str | None,
        secret_id: str,
    ) -> None:
        """Fail closed unless this is the exact current claimed model-backed Attempt fence.

        Required, strictly before ``execution_deadline_ms``, for first
        acceptance of a Credential Rotation Request -- with its accepted
        Launch Attestation and matching provider account (worker-protocol.md,
        "Credential rotation handoff"). Deterministic ``VERIFY`` Attempts
        (``provider_secret_ref is None``) can never rotate a credential, and
        an Attempt can only rotate the exact secret bound to it -- never an
        arbitrary ``secret_id`` -- mirroring the ``provider_secret_ref``
        drift check :meth:`accept_launch_attestation` performs against the
        frozen claim.
        """
        self._require_current_rotation_authority(
            attempt_id=attempt_id,
            activity_id=activity_id,
            attempt_generation=attempt_generation,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
            attempt_capability_digest=attempt_capability_digest,
            launch_attestation_id=launch_attestation_id,
            provider_account_ref=provider_account_ref,
            secret_id=secret_id,
        )

    def _require_current_rotation_authority(
        self,
        *,
        attempt_id: str,
        activity_id: str,
        attempt_generation: int,
        worker_id: str,
        worker_session_id: str,
        attempt_capability_digest: str,
        launch_attestation_id: str,
        provider_account_ref: str | None,
        secret_id: str,
    ) -> None:
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(secret_id, field="secret_id")
        attempt = self.get_attempt(attempt_id)
        if attempt is None:
            raise RunStoreError(f"attempt {attempt_id!r} was not found")
        if (
            attempt.activity_id != activity_id
            or attempt.generation != attempt_generation
            or attempt.state != "CLAIMED"
            or attempt.claimed_worker_id != worker_id
            or attempt.claimed_worker_session_id != worker_session_id
            or attempt.attempt_capability_digest != attempt_capability_digest
            or attempt.launch_attestation_id != launch_attestation_id
            or attempt.provider_account_ref != provider_account_ref
        ):
            raise CasMismatchError("credential rotation does not match the current claimed attempt")
        if attempt.provider_secret_ref is None:
            raise CasMismatchError("deterministic attempt cannot rotate a provider credential")
        if attempt.provider_secret_ref != secret_id:
            raise CasMismatchError(
                "credential rotation secret_id does not match the attempt's bound secret"
            )
        if attempt.execution_deadline_ms is None or _now_ms() >= attempt.execution_deadline_ms:
            raise CasMismatchError("credential rotation arrived at or after the execution deadline")

    def require_rotation_replay_before_deadline(self, attempt_id: str) -> None:
        """Fail closed unless this Attempt's execution deadline has not yet passed.

        An exact already-ledgered Credential Rotation Request may replay
        after Attempt terminalization, but only while
        ``controller_now_ms < execution_deadline_ms``; at or after that
        deadline the rotation endpoint denies both first acceptance and
        replay (domain-model.md I3, worker-protocol.md "Credential rotation
        handoff").
        """
        require_lowercase_uuid(attempt_id, field="attempt_id")
        attempt = self.get_attempt(attempt_id)
        if (
            attempt is None
            or attempt.execution_deadline_ms is None
            or _now_ms() >= attempt.execution_deadline_ms
        ):
            raise CasMismatchError(
                "credential rotation replay arrived at or after the execution deadline"
            )

    def install_applied_credential_rotation(
        self,
        *,
        credential_rotation_request_id: str,
        protocol_version: str,
        attempt_id: str,
        activity_id: str,
        attempt_generation: int,
        worker_id: str,
        worker_session_id: str,
        attempt_capability_digest: str,
        launch_attestation_id: str,
        provider_account_ref: str | None,
        secret_id: str,
        expected_prior_version: int,
        secret_request_attestation_id: str,
        request_digest_value: str,
        storage_path: str,
        secret_integrity_attestation_id: str,
    ) -> CredentialRotationRequestResult:
        """Atomically commit one ``APPLIED`` Credential Rotation Request.

        Must run only after the Secret Store has durably promoted the staged
        bytes into the immutable target version (write-before-reference),
        while its storage mutation lock is still held: inserts the Request,
        its reciprocal ``ATTEMPT_ROTATION`` Credential Rotation Receipt and
        Secret Version, compare-and-swaps the current reference, freezes
        affected-Run membership, and creates the durable fanout intent -- all
        in one transaction (persistence-and-recovery.md, "Secret Store and
        rotation").
        """
        require_lowercase_uuid(
            credential_rotation_request_id, field="credential_rotation_request_id"
        )
        require_lowercase_uuid(secret_id, field="secret_id")
        require_lowercase_uuid(secret_request_attestation_id, field="secret_request_attestation_id")
        require_lowercase_uuid(
            secret_integrity_attestation_id, field="secret_integrity_attestation_id"
        )
        _require_positive_int(expected_prior_version, field="expected_prior_version")
        target_version = expected_prior_version + 1
        with self.transaction():
            current = self.get_secret_current_version(secret_id)
            if current is None or current.current_version == 0:
                raise RunStoreError(f"secret {secret_id!r} has no current version to rotate")
            purpose = current.purpose
            owner_scope_kind = current.owner_scope_kind
            owner_scope_id = current.owner_scope_id
            now = _now_ms()
            receipt_id = str(uuid.uuid4())
            rc_digest = receipt_digest(
                {
                    "source_kind": "ATTEMPT_ROTATION",
                    "source_id": credential_rotation_request_id,
                    "secret_id": secret_id,
                    "expected_prior_version": expected_prior_version,
                    "new_version": target_version,
                    "purpose": purpose,
                    "owner_scope_kind": owner_scope_kind,
                    "owner_scope_id": owner_scope_id,
                    "provider_account_ref": provider_account_ref,
                    "attempt_id": attempt_id,
                    "activity_id": activity_id,
                    "attempt_generation": attempt_generation,
                    "worker_id": worker_id,
                    "worker_session_id": worker_session_id,
                    "attempt_capability_digest": attempt_capability_digest,
                    "launch_attestation_id": launch_attestation_id,
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
                "VALUES (?, 'ATTEMPT_ROTATION', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "NULL, NULL, NULL, ?, ?, ?)",
                (
                    receipt_id,
                    credential_rotation_request_id,
                    credential_rotation_request_id,
                    secret_id,
                    expected_prior_version,
                    target_version,
                    purpose,
                    owner_scope_kind,
                    owner_scope_id,
                    provider_account_ref,
                    attempt_id,
                    activity_id,
                    attempt_generation,
                    worker_id,
                    worker_session_id,
                    attempt_capability_digest,
                    launch_attestation_id,
                    secret_integrity_attestation_id,
                    rc_digest,
                    now,
                ),
            )
            affected_digest = self._freeze_secret_version_membership(secret_id, target_version)
            self.conn.execute(
                "INSERT INTO secret_versions("
                "secret_id, version, creation_receipt_id, storage_path, "
                "affected_run_ids_digest, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
                (secret_id, target_version, receipt_id, storage_path, affected_digest, now),
            )
            self._require_current_rotation_authority(
                attempt_id=attempt_id,
                activity_id=activity_id,
                attempt_generation=attempt_generation,
                worker_id=worker_id,
                worker_session_id=worker_session_id,
                attempt_capability_digest=attempt_capability_digest,
                launch_attestation_id=launch_attestation_id,
                provider_account_ref=provider_account_ref,
                secret_id=secret_id,
            )
            cur = self.conn.execute(
                "UPDATE secret_current_versions SET current_version = ?, "
                "last_operation_id = ?, updated_at_ms = ? "
                "WHERE secret_id = ? AND current_version = ?",
                (
                    target_version,
                    credential_rotation_request_id,
                    now,
                    secret_id,
                    expected_prior_version,
                ),
            )
            if cur.rowcount != 1:
                # The Secret Store storage-mutation lock, held continuously
                # from the caller's precheck through this install, makes this
                # unreachable in normal operation; treat it as a real storage
                # inconsistency rather than a client-facing CAS_LOST.
                raise CasMismatchError(
                    "secret current-version CAS was lost between precheck and install"
                )
            http_status, body_json, resp_digest = self._credential_rotation_result_response(
                credential_rotation_request_id=credential_rotation_request_id,
                disposition="APPLIED",
                secret_id=secret_id,
                expected_prior_version=expected_prior_version,
                current_version=target_version,
                accepted_version=target_version,
                credential_rotation_receipt_id=receipt_id,
            )
            self.conn.execute(
                "INSERT INTO credential_rotation_requests("
                "credential_rotation_request_id, protocol_version, attempt_id, activity_id, "
                "attempt_generation, worker_id, worker_session_id, attempt_capability_digest, "
                "launch_attestation_id, provider_account_ref, secret_id, "
                "expected_prior_version, secret_request_attestation_id, request_digest, "
                "disposition, credential_rotation_receipt_id, accepted_version, "
                "current_version, response_http_status, response_json, response_digest, "
                "accepted_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'APPLIED', "
                "?, ?, ?, ?, ?, ?, ?)",
                (
                    credential_rotation_request_id,
                    protocol_version,
                    attempt_id,
                    activity_id,
                    attempt_generation,
                    worker_id,
                    worker_session_id,
                    attempt_capability_digest,
                    launch_attestation_id,
                    provider_account_ref,
                    secret_id,
                    expected_prior_version,
                    secret_request_attestation_id,
                    request_digest_value,
                    receipt_id,
                    target_version,
                    target_version,
                    http_status,
                    body_json,
                    resp_digest,
                    now,
                ),
            )
            self._run_secret_version_fanout(secret_id, target_version)
            row = self.conn.execute(
                "SELECT * FROM credential_rotation_requests "
                "WHERE credential_rotation_request_id = ?",
                (credential_rotation_request_id,),
            ).fetchone()
            assert row is not None
            return _row_to_credential_rotation_request(row, replayed=False)

    def record_cas_lost_credential_rotation(
        self,
        *,
        credential_rotation_request_id: str,
        protocol_version: str,
        attempt_id: str,
        activity_id: str,
        attempt_generation: int,
        worker_id: str,
        worker_session_id: str,
        attempt_capability_digest: str,
        launch_attestation_id: str,
        provider_account_ref: str | None,
        secret_id: str,
        expected_prior_version: int,
        secret_request_attestation_id: str,
        request_digest_value: str,
    ) -> CredentialRotationRequestResult:
        """Atomically ledger one ``CAS_LOST`` Credential Rotation Request.

        Creates no Receipt, Version, reference mutation, fanout, Result, or
        Transition; stores only the request/response ledger and the current
        observed non-secret version.
        """
        require_lowercase_uuid(
            credential_rotation_request_id, field="credential_rotation_request_id"
        )
        require_lowercase_uuid(secret_id, field="secret_id")
        require_lowercase_uuid(secret_request_attestation_id, field="secret_request_attestation_id")
        _require_positive_int(expected_prior_version, field="expected_prior_version")
        with self.transaction():
            self._require_current_rotation_authority(
                attempt_id=attempt_id,
                activity_id=activity_id,
                attempt_generation=attempt_generation,
                worker_id=worker_id,
                worker_session_id=worker_session_id,
                attempt_capability_digest=attempt_capability_digest,
                launch_attestation_id=launch_attestation_id,
                provider_account_ref=provider_account_ref,
                secret_id=secret_id,
            )
            current = self.get_secret_current_version(secret_id)
            current_version = 0 if current is None else current.current_version
            now = _now_ms()
            http_status, body_json, resp_digest = self._credential_rotation_result_response(
                credential_rotation_request_id=credential_rotation_request_id,
                disposition="CAS_LOST",
                secret_id=secret_id,
                expected_prior_version=expected_prior_version,
                current_version=current_version,
            )
            self.conn.execute(
                "INSERT INTO credential_rotation_requests("
                "credential_rotation_request_id, protocol_version, attempt_id, activity_id, "
                "attempt_generation, worker_id, worker_session_id, attempt_capability_digest, "
                "launch_attestation_id, provider_account_ref, secret_id, "
                "expected_prior_version, secret_request_attestation_id, request_digest, "
                "disposition, credential_rotation_receipt_id, accepted_version, "
                "current_version, response_http_status, response_json, response_digest, "
                "accepted_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'CAS_LOST', "
                "NULL, NULL, ?, ?, ?, ?, ?)",
                (
                    credential_rotation_request_id,
                    protocol_version,
                    attempt_id,
                    activity_id,
                    attempt_generation,
                    worker_id,
                    worker_session_id,
                    attempt_capability_digest,
                    launch_attestation_id,
                    provider_account_ref,
                    secret_id,
                    expected_prior_version,
                    secret_request_attestation_id,
                    request_digest_value,
                    current_version,
                    http_status,
                    body_json,
                    resp_digest,
                    now,
                ),
            )
            row = self.conn.execute(
                "SELECT * FROM credential_rotation_requests "
                "WHERE credential_rotation_request_id = ?",
                (credential_rotation_request_id,),
            ).fetchone()
            assert row is not None
            return _row_to_credential_rotation_request(row, replayed=False)

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
        expired_during_promotion = False

        def reference(record: Any) -> None:
            nonlocal promoted, expired_during_promotion
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
                    expired_during_promotion = True
                else:
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
            if expired_during_promotion:
                raise _CandidateUploadExpiredDuringPromotion

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
                        _require_controller_import_replay_match(
                            fact,
                            candidate_id=candidate_id,
                            activity_id=activity_id,
                            forge_observation_id=forge_observation_id,
                            operation_digest=operation_digest,
                            fact_digest=fact_digest,
                        )
                        fact_candidate_id = fact.candidate_id
                        if fact_candidate_id is None:
                            raise IdempotencyConflictError(
                                "controller operation fact id was reused with different content"
                            )
                        existing_candidate = self.get_candidate(fact_candidate_id)
                        if existing_candidate is None:
                            raise RunStoreError("controller import fact has no Candidate")
                        if (
                            existing_candidate.producing_activity_id != activity_id
                            or existing_candidate.import_forge_observation_id
                            != forge_observation_id
                            or existing_candidate.object_format != tip.object_format
                            or existing_candidate.oid != tip.oid
                            or existing_candidate.base_commit_json != base.as_json()
                            or existing_candidate.bundle_digest != staged.bundle_digest
                        ):
                            raise IdempotencyConflictError(
                                "controller import Candidate replay content changed"
                            )
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

    def get_result_request(self, result_request_id: str) -> ResultRequestRecord | None:
        require_lowercase_uuid(result_request_id, field="result_request_id")
        row = self.conn.execute(
            "SELECT * FROM result_requests WHERE result_request_id = ?", (result_request_id,)
        ).fetchone()
        return None if row is None else _row_to_result_request(row, replayed=True)

    def get_attempt_result(self, attempt_result_id: str) -> AttemptResultRecord | None:
        require_lowercase_uuid(attempt_result_id, field="attempt_result_id")
        row = self.conn.execute(
            "SELECT * FROM attempt_results WHERE attempt_result_id = ?", (attempt_result_id,)
        ).fetchone()
        return None if row is None else _row_to_attempt_result(row)

    def _attempt_result_body(
        self,
        *,
        idempotency_key: str,
        attempt_id: str,
        activity_id: str,
        generation: int,
        launch_attestation_id: str | None,
        outcome: str,
        candidate_upload_id: str | None,
        receipt: Any | None,
        structured_output: Any | None,
        failure: Any | None,
        summary: str | None,
    ) -> dict[str, Any]:
        return {
            "protocol": ATTEMPT_RESULT_PROTOCOL,
            "idempotency_key": idempotency_key,
            "attempt_id": attempt_id,
            "activity_id": activity_id,
            "generation": generation,
            "launch_attestation_id": launch_attestation_id,
            "outcome": outcome,
            "candidate_upload_id": candidate_upload_id,
            "receipt": receipt,
            "structured_output": structured_output,
            "failure": failure,
            "summary": summary,
        }

    def _attempt_result_semantic_body(self, body: Mapping[str, Any]) -> dict[str, Any]:
        semantic = dict(body)
        semantic.pop("idempotency_key", None)
        return semantic

    def _attempt_result_accepted_response(
        self,
        *,
        attempt_id: str,
        activity_id: str,
        generation: int,
        outcome: str,
        candidate_id: str | None,
        receipt_id: str | None,
    ) -> tuple[int, str, str]:
        body = {
            "protocol": ATTEMPT_RESULT_ACCEPTED_PROTOCOL,
            "attempt_id": attempt_id,
            "activity_id": activity_id,
            "generation": generation,
            "outcome": outcome,
            "candidate_id": candidate_id,
            "receipt_id": receipt_id,
            "replayed": False,
        }
        return (
            200,
            canonical_json_text(body),
            response_digest({"http_status": 200, "body": _response_digest_preimage(body)}),
        )

    def _attempt_result_error_response(
        self,
        *,
        http_status: int,
        code: str,
        attempt_id: str,
        current_attempt_generation: int | None,
    ) -> tuple[int, str, str]:
        body: dict[str, Any] = {
            "protocol": ERROR_PROTOCOL,
            "code": code,
            "attempt_id": attempt_id,
            "current_attempt_generation": current_attempt_generation,
            "retryable": False,
            "replayed": False,
        }
        return (
            http_status,
            canonical_json_text(body),
            response_digest({"http_status": http_status, "body": _response_digest_preimage(body)}),
        )

    def _result_request_from_row(
        self, row: sqlite3.Row, *, replayed: bool
    ) -> AttemptResultSubmissionResult:
        request = _row_to_result_request(row, replayed=replayed)
        result = None
        candidate = None
        if request.attempt_result_id is not None:
            result_row = self.conn.execute(
                "SELECT * FROM attempt_results WHERE attempt_result_id = ?",
                (request.attempt_result_id,),
            ).fetchone()
            assert result_row is not None
            result = _row_to_attempt_result(result_row)
            if result.candidate_id is not None:
                candidate = self.get_candidate(result.candidate_id)
                assert candidate is not None
        return AttemptResultSubmissionResult(
            request=request, attempt_result=result, candidate=candidate
        )

    def _require_valid_verification_receipt(
        self,
        *,
        row: sqlite3.Row,
        outcome: str,
        receipt: Any | None,
        failure_class: str | None,
        launch_attestation_id: str | None,
    ) -> None:
        """Admit a ``VERIFY`` Attempt Result's receipt only after independently
        recomputing its outcome against the controller's own trusted Candidate
        and pinned Verification Profile bindings (review-and-consensus.md's
        "Verification Receipt" section). Never trusts the worker's claims for
        the Candidate identity, profile, or outcome.
        """
        if launch_attestation_id is not None:
            raise VerificationReceiptRejectedError(
                "VERIFY Attempt Results require a null launch_attestation_id"
            )
        if receipt is None:
            raise VerificationReceiptRejectedError(
                "VERIFY Attempt Results require a verification receipt"
            )
        candidate_id = row["activity_candidate_id"]
        if candidate_id is None:
            raise RunStoreError("VERIFY Activity is missing its bound Candidate")
        candidate = self.get_candidate(candidate_id)
        if candidate is None:
            raise RunStoreError("VERIFY Activity's bound Candidate is missing")
        snapshot_row = self.conn.execute(
            "SELECT * FROM work_item_snapshots WHERE run_id = ? AND policy_hash = ? LIMIT 1",
            (row["run_id"], row["activity_policy_hash"]),
        ).fetchone()
        if snapshot_row is None:
            raise RunStoreError("no Snapshot matches the VERIFY Activity's pinned policy_hash")
        snapshot = _row_to_work_item_snapshot(snapshot_row)
        policy_blob = self.get_workflow_blob(snapshot.effective_policy_blob_digest)
        if policy_blob is None:
            raise RunStoreError("the Snapshot's effective policy blob is missing")
        effective_policy = json.loads(policy_blob.normalized_bytes.decode("utf-8"))
        profile_id, commands, profile_hash = verification_profile_from_effective_policy(
            effective_policy
        )
        recomputed_outcome = validate_verification_receipt(
            receipt,
            expected_candidate_id=candidate.candidate_id,
            expected_commit={"object_format": candidate.object_format, "oid": candidate.oid},
            expected_profile_id=profile_id,
            expected_profile_hash=profile_hash,
            expected_commands=commands,
        )
        expected_result_outcome = (
            "SUCCEEDED" if recomputed_outcome in ("PASS", "FAIL") else "FAILED_RETRYABLE"
        )
        if outcome != expected_result_outcome:
            raise VerificationReceiptRejectedError(
                f"a {recomputed_outcome} verification receipt requires Attempt Result outcome "
                f"{expected_result_outcome}, got {outcome!r}"
            )
        if recomputed_outcome == "ERROR" and failure_class != "VERIFICATION_ERROR":
            raise VerificationReceiptRejectedError(
                "an ERROR verification receipt requires failure_class VERIFICATION_ERROR"
            )

    def _receipt_candidate_and_assignment(
        self, *, row: sqlite3.Row, receipt: Mapping[str, Any], expected_kind: str
    ) -> tuple[CandidateRecord, ActivityReviewAssignmentRecord, dict[str, Any]]:
        assignment = self._get_activity_review_assignment(str(row["activity_id"]))
        if assignment is None or assignment.assignment_kind != expected_kind:
            raise ProtocolValidationError(f"{expected_kind} receipt has no matching assignment")
        candidate_id = row["activity_candidate_id"]
        if candidate_id is None:
            raise ProtocolValidationError(f"{expected_kind} Activity is missing its Candidate")
        candidate = self.get_candidate(str(candidate_id))
        if candidate is None:
            raise ProtocolValidationError(f"{expected_kind} Activity's Candidate is missing")
        candidate_obj = receipt["candidate"]
        commit_obj = candidate_obj["commit"]
        if candidate_obj["candidate_id"] != candidate.candidate_id:
            raise ProtocolValidationError("receipt candidate_id does not match assignment")
        if (
            commit_obj["object_format"] != candidate.object_format
            or commit_obj["oid"] != candidate.oid
        ):
            raise ProtocolValidationError("receipt commit does not match assigned Candidate")
        pointers = self.get_revisioned_object("run_pointers", str(row["run_id"]))
        if pointers is not None:
            current = json.loads(pointers[2])
            if isinstance(current, Mapping):
                if current.get("current_candidate_id") != candidate.candidate_id:
                    raise CasMismatchError("receipt Candidate is no longer current")
                if current.get("policy_hash") not in {None, row["activity_policy_hash"]}:
                    raise CasMismatchError("receipt policy binding is no longer current")
        return candidate, assignment, current if pointers is not None else {}

    def _require_matching_launch_attestation(
        self,
        *,
        row: sqlite3.Row,
        launch_attestation_id: str | None,
    ) -> str:
        if launch_attestation_id is None:
            raise ProtocolValidationError("model-backed receipt requires launch_attestation_id")
        if row["launch_attestation_id"] != launch_attestation_id:
            raise CasMismatchError("launch attestation does not match Attempt")
        attestation = self.get_launch_attestation(launch_attestation_id)
        if attestation is None:
            raise CasMismatchError("launch attestation was not accepted")
        return launch_attestation_id

    def _derive_review_receipt(
        self,
        *,
        row: sqlite3.Row,
        receipt: Any | None,
        outcome: str,
        launch_attestation_id: str | None,
    ) -> tuple[dict[str, Any], bool, str]:
        if receipt is None:
            raise ProtocolValidationError("REVIEW Attempt Result requires a review receipt")
        validated = validate_envelope(receipt)
        if validated["protocol"] != REVIEW_RECEIPT_PROTOCOL:
            raise ProtocolValidationError("REVIEW Attempt Result requires a review receipt")
        candidate, assignment, _current = self._receipt_candidate_and_assignment(
            row=row, receipt=validated, expected_kind="REVIEW"
        )
        if validated["panel_round"] != assignment.panel_round:
            raise ProtocolValidationError("review panel_round does not match assignment")
        if validated["reviewer_slot"] != assignment.reviewer_slot:
            raise ProtocolValidationError("reviewer_slot does not match assignment")
        if validated["role"] != assignment.role:
            raise ProtocolValidationError("review role does not match assignment")
        if validated["subject_refs_digest"] != assignment.subject_refs_digest:
            raise ProtocolValidationError("subject_refs_digest does not match assignment")
        if validated["context_digest"] != assignment.context_digest:
            raise ProtocolValidationError("context_digest does not match assignment")
        assessments = validated["assessments"]
        if tuple(item["subject_ref"] for item in assessments) != assignment.subject_refs:
            raise ProtocolValidationError("assessments do not match the assigned subjects")
        verdict = str(validated["verdict"])
        fills_slot = verdict in {"APPROVE", "BLOCK"}
        if verdict == "APPROVE":
            if validated["findings"]:
                raise ProtocolValidationError("APPROVE must not carry findings")
            if any(item["outcome"] != "SATISFIED" for item in assessments):
                raise ProtocolValidationError("APPROVE requires every subject to be SATISFIED")
        expected_outcome = "SUCCEEDED" if fills_slot else "ABSTAINED"
        if outcome != expected_outcome:
            raise ProtocolValidationError(
                f"{verdict} review receipt requires Attempt Result outcome {expected_outcome}"
            )
        launch_id = self._require_matching_launch_attestation(
            row=row, launch_attestation_id=launch_attestation_id
        )
        finding_keys = tuple(str(item["finding_key"]) for item in validated["findings"])
        normalized = {
            "candidate_id": candidate.candidate_id,
            "object_format": candidate.object_format,
            "oid": candidate.oid,
            "assignment_digest": assignment.assignment_digest,
            "attempt_id": row["attempt_id"],
            "activity_id": row["activity_id"],
            "attempt_generation": row["generation"],
            "launch_attestation_id": launch_id,
            "receipt": validated,
        }
        return (
            dict(validated),
            fills_slot,
            bare_canonical_digest(list(finding_keys) + [receipt_digest(normalized)]),
        )

    def _derive_adjudication_receipt(
        self,
        *,
        row: sqlite3.Row,
        receipt: Any | None,
        outcome: str,
        launch_attestation_id: str | None,
    ) -> tuple[dict[str, Any], bool, str]:
        if receipt is None:
            raise ProtocolValidationError(
                "ADJUDICATE Attempt Result requires an adjudication receipt"
            )
        validated = validate_envelope(receipt)
        if validated["protocol"] != ADJUDICATION_RECEIPT_PROTOCOL:
            raise ProtocolValidationError(
                "ADJUDICATE Attempt Result requires an adjudication receipt"
            )
        _candidate, assignment, _current = self._receipt_candidate_and_assignment(
            row=row, receipt=validated, expected_kind="ADJUDICATE"
        )
        if validated["panel_round"] != assignment.panel_round:
            raise ProtocolValidationError("adjudication panel_round does not match assignment")
        if validated["adjudication_round"] != 1 or assignment.adjudication_round != 1:
            raise ProtocolValidationError("adjudication_round must be 1")
        if validated["adjudicator_slot"] != assignment.adjudicator_slot:
            raise ProtocolValidationError("adjudicator_slot does not match assignment")
        if validated["subject_refs_digest"] != assignment.subject_refs_digest:
            raise ProtocolValidationError("subject_refs_digest does not match assignment")
        if validated["context_digest"] != assignment.context_digest:
            raise ProtocolValidationError("context_digest does not match assignment")
        dispositions = validated["dispositions"]
        disposition_ids = tuple(str(item["finding_id"]) for item in dispositions)
        if (
            validated["abstention_code"] is None
            and disposition_ids != assignment.disputed_finding_ids
        ):
            raise ProtocolValidationError("dispositions do not match assigned disputed findings")
        fills_slot = (
            validated["abstention_code"] is None
            and not validated["new_findings"]
            and bool(dispositions)
            and all(item["disposition"] != "INCONCLUSIVE" for item in dispositions)
        )
        expected_outcome = "SUCCEEDED" if fills_slot else "ABSTAINED"
        if outcome != expected_outcome:
            raise ProtocolValidationError(
                "adjudication receipt requires Attempt Result outcome " + expected_outcome
            )
        self._require_matching_launch_attestation(
            row=row, launch_attestation_id=launch_attestation_id
        )
        if not fills_slot:
            summary = "INCONCLUSIVE"
        elif (
            any(item["disposition"] == "SUSTAIN" for item in dispositions)
            or validated["new_findings"]
        ):
            summary = "SUSTAIN"
        else:
            summary = "OVERRULE"
        return dict(validated), fills_slot, summary

    def _review_panel_complete(self, *, run_id: str, candidate_id: str, panel_round: int) -> bool:
        required = self.conn.execute(
            "SELECT COUNT(*) FROM activities a "
            "JOIN activity_review_assignments r ON r.activity_id = a.activity_id "
            "WHERE a.run_id = ? AND a.candidate_id = ? "
            "AND r.assignment_kind = 'REVIEW' AND r.panel_round = ?",
            (run_id, candidate_id, panel_round),
        ).fetchone()
        filled = self.conn.execute(
            "SELECT COUNT(*) FROM review_receipts "
            "WHERE run_id = ? AND candidate_id = ? AND panel_round = ? AND fills_slot = 1",
            (run_id, candidate_id, panel_round),
        ).fetchone()
        return int(required[0]) > 0 and int(required[0]) == int(filled[0])

    def submit_attempt_result(
        self,
        *,
        candidate_store: Any,
        result_request_id: str,
        attempt_id: str,
        activity_id: str,
        generation: int,
        worker_id: str,
        worker_session_id: str,
        attempt_capability_digest: str,
        outcome: str,
        launch_attestation_id: str | None = None,
        candidate_upload_id: str | None = None,
        receipt: Any | None = None,
        structured_output: Any | None = None,
        failure: Any | None = None,
        summary: str | None = None,
        now_ms: int | None = None,
    ) -> AttemptResultSubmissionResult:
        require_lowercase_uuid(result_request_id, field="result_request_id")
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(activity_id, field="activity_id")
        if launch_attestation_id is not None:
            require_lowercase_uuid(launch_attestation_id, field="launch_attestation_id")
        if candidate_upload_id is not None:
            require_lowercase_uuid(candidate_upload_id, field="candidate_upload_id")
        _require_positive_int(generation, field="generation")
        _require_nonempty_text(worker_id, field="worker_id")
        _require_nonempty_text(worker_session_id, field="worker_session_id")
        _require_digest(attempt_capability_digest, field="attempt_capability_digest")
        enums.parse_enum("attempt_result.outcome", outcome)
        if (outcome == "SUCCEEDED") == (failure is not None):
            raise ValueError("failure is required only for non-SUCCEEDED Attempt Results")
        failure_class = None
        evidence_refs_json = None
        retry_delay_ms = None
        failure_json = None
        if failure is not None:
            if not isinstance(failure, Mapping):
                raise ValueError("failure must be a JSON object")
            failure_class = str(failure.get("failure_class", ""))
            enums.parse_enum("attempt_result.failure_class", failure_class)
            evidence_refs = failure.get("evidence_refs")
            if evidence_refs is not None:
                evidence_refs_json = canonical_json_text(evidence_refs)
            retry_delay = failure.get("retry_delay_ms")
            if retry_delay is not None:
                if not isinstance(retry_delay, int) or retry_delay < 0:
                    raise ValueError("failure.retry_delay_ms must be a nonnegative integer")
                retry_delay_ms = retry_delay
            failure_json = canonical_json_text(failure)

        body = self._attempt_result_body(
            idempotency_key=result_request_id,
            attempt_id=attempt_id,
            activity_id=activity_id,
            generation=generation,
            launch_attestation_id=launch_attestation_id,
            outcome=outcome,
            candidate_upload_id=candidate_upload_id,
            receipt=receipt,
            structured_output=structured_output,
            failure=failure,
            summary=summary,
        )
        req_digest = request_digest(body)
        res_digest = result_digest(self._attempt_result_semantic_body(body))
        receipt_json = None if receipt is None else canonical_json_text(receipt)
        receipt_digest_value = (
            None
            if receipt is None
            else attempt_result_receipt_digest(
                {
                    "result_request_id": result_request_id,
                    "attempt_id": attempt_id,
                    "activity_id": activity_id,
                    "generation": generation,
                    "receipt": receipt,
                }
            )
        )
        now = _now_ms() if now_ms is None else now_ms

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM result_requests WHERE result_request_id = ?",
                (result_request_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_digest"] == req_digest
                    and existing["result_digest"] == res_digest
                    and existing["attempt_id"] == attempt_id
                    and existing["activity_id"] == activity_id
                    and existing["attempt_generation"] == generation
                    and existing["worker_id"] == worker_id
                    and existing["worker_session_id"] == worker_session_id
                    and existing["attempt_capability_digest"] == attempt_capability_digest
                ):
                    attempt = self.get_attempt(attempt_id)
                    if (
                        attempt is not None
                        and attempt.capability_auth_expires_at_ms is not None
                        and now >= attempt.capability_auth_expires_at_ms
                    ):
                        raise CasMismatchError("attempt capability authentication expired")
                    return self._result_request_from_row(existing, replayed=True)
                raise IdempotencyConflictError("result request key was reused")

            evaluation = self._controller_gate_evaluation()
            if not evaluation.permissions.first_result_mutation:
                raise WorkflowGateClosedError(
                    "controller mode does not allow first Result mutation"
                )

            row = self.conn.execute(
                "SELECT attempts.*, activities.run_id AS run_id, activities.kind AS activity_kind, "
                "activities.state AS activity_state, "
                "activities.specification_generation AS specification_generation, "
                "activities.execution_class AS execution_class, "
                "activities.candidate_id AS activity_candidate_id, "
                "activities.policy_hash AS activity_policy_hash "
                "FROM attempts JOIN activities ON activities.activity_id = attempts.activity_id "
                "WHERE attempts.attempt_id = ? AND attempts.activity_id = ? "
                "ORDER BY attempts.generation DESC LIMIT 1",
                (attempt_id, activity_id),
            ).fetchone()
            if row is None:
                raise AttemptUnknownError(
                    f"no Attempt with attempt_id={attempt_id!r} activity_id={activity_id!r} "
                    f"generation={generation!r}"
                )
            if row["execution_deadline_ms"] is None:
                raise CasMismatchError("attempt has no execution deadline")
            if (
                row["capability_auth_expires_at_ms"] is not None
                and now >= row["capability_auth_expires_at_ms"]
            ):
                raise CasMismatchError("attempt capability authentication expired")
            binding_ok = (
                row["claimed_worker_id"] == worker_id
                and row["claimed_worker_session_id"] == worker_session_id
                and row["attempt_capability_digest"] == attempt_capability_digest
            )

            accepted = self.conn.execute(
                "SELECT result_requests.* FROM result_requests "
                "JOIN attempt_results ON attempt_results.attempt_result_id = "
                "result_requests.attempt_result_id "
                "WHERE result_requests.attempt_id = ? "
                "AND result_requests.activity_id = ? "
                "AND result_requests.attempt_generation = ? "
                "AND result_requests.result_digest = ? "
                "AND result_requests.disposition = 'ACCEPTED' "
                "ORDER BY result_requests.created_at_ms LIMIT 1",
                (attempt_id, activity_id, generation, res_digest),
            ).fetchone()
            if accepted is not None and binding_ok:
                result_row = self.conn.execute(
                    "SELECT * FROM attempt_results WHERE attempt_result_id = ?",
                    (accepted["attempt_result_id"],),
                ).fetchone()
                assert result_row is not None
                result = _row_to_attempt_result(result_row)
                http_status, body_json, resp_digest = self._attempt_result_accepted_response(
                    attempt_id=attempt_id,
                    activity_id=activity_id,
                    generation=generation,
                    outcome=outcome,
                    candidate_id=result.candidate_id,
                    receipt_id=result.receipt_id,
                )
                self.conn.execute(
                    "INSERT INTO result_requests(result_request_id, attempt_result_id, "
                    "attempt_id, activity_id, attempt_generation, worker_id, worker_session_id, "
                    "attempt_capability_digest, request_digest, result_digest, disposition, "
                    "stale_reason, accepted_result_created, candidate_upload_id, "
                    "attempt_terminal_fact_id, response_http_status, response_json, "
                    "response_digest, created_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACCEPTED', NULL, 0, ?, NULL, "
                    "?, ?, ?, ?)",
                    (
                        result_request_id,
                        result.attempt_result_id,
                        attempt_id,
                        activity_id,
                        generation,
                        worker_id,
                        worker_session_id,
                        attempt_capability_digest,
                        req_digest,
                        res_digest,
                        candidate_upload_id,
                        http_status,
                        body_json,
                        resp_digest,
                        now,
                    ),
                )
                inserted = self.conn.execute(
                    "SELECT * FROM result_requests WHERE result_request_id = ?",
                    (result_request_id,),
                ).fetchone()
                assert inserted is not None
                return self._result_request_from_row(inserted, replayed=False)

            prior_any = self.conn.execute(
                "SELECT * FROM attempt_results WHERE attempt_id = ? AND activity_id = ? "
                "AND attempt_generation = ?",
                (attempt_id, activity_id, generation),
            ).fetchone()
            if prior_any is not None:
                if not binding_ok:
                    raise CasMismatchError("attempt is stale")
                http_status, body_json, resp_digest = self._attempt_result_error_response(
                    http_status=409,
                    code="RESULT_ALREADY_ACCEPTED",
                    attempt_id=attempt_id,
                    current_attempt_generation=generation,
                )
                self.conn.execute(
                    "INSERT INTO result_requests(result_request_id, attempt_result_id, "
                    "attempt_id, activity_id, attempt_generation, worker_id, worker_session_id, "
                    "attempt_capability_digest, request_digest, result_digest, disposition, "
                    "stale_reason, accepted_result_created, candidate_upload_id, "
                    "attempt_terminal_fact_id, response_http_status, response_json, "
                    "response_digest, created_at_ms) "
                    "VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'RESULT_ALREADY_ACCEPTED', "
                    "NULL, 0, ?, NULL, ?, ?, ?, ?)",
                    (
                        result_request_id,
                        attempt_id,
                        activity_id,
                        generation,
                        worker_id,
                        worker_session_id,
                        attempt_capability_digest,
                        req_digest,
                        res_digest,
                        candidate_upload_id,
                        http_status,
                        body_json,
                        resp_digest,
                        now,
                    ),
                )
                inserted = self.conn.execute(
                    "SELECT * FROM result_requests WHERE result_request_id = ?",
                    (result_request_id,),
                ).fetchone()
                assert inserted is not None
                return self._result_request_from_row(inserted, replayed=False)

            before_execution_deadline = now < row["execution_deadline_ms"]
            if row["generation"] != generation:
                http_status, body_json, resp_digest = self._attempt_result_error_response(
                    http_status=409,
                    code="ATTEMPT_STALE",
                    attempt_id=attempt_id,
                    current_attempt_generation=row["generation"],
                )
                self.conn.execute(
                    "INSERT INTO result_requests(result_request_id, attempt_result_id, "
                    "attempt_id, activity_id, attempt_generation, worker_id, worker_session_id, "
                    "attempt_capability_digest, request_digest, result_digest, disposition, "
                    "stale_reason, accepted_result_created, candidate_upload_id, "
                    "attempt_terminal_fact_id, response_http_status, response_json, "
                    "response_digest, created_at_ms) "
                    "VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'STALE_ATTEMPT', "
                    "'GENERATION_SUPERSEDED', 0, ?, NULL, ?, ?, ?, ?)",
                    (
                        result_request_id,
                        attempt_id,
                        activity_id,
                        generation,
                        worker_id,
                        worker_session_id,
                        attempt_capability_digest,
                        req_digest,
                        res_digest,
                        candidate_upload_id,
                        http_status,
                        body_json,
                        resp_digest,
                        now,
                    ),
                )
                inserted = self.conn.execute(
                    "SELECT * FROM result_requests WHERE result_request_id = ?",
                    (result_request_id,),
                ).fetchone()
                assert inserted is not None
                return self._result_request_from_row(inserted, replayed=False)

            if before_execution_deadline and (row["state"] != "CLAIMED" or not binding_ok):
                stale_reason = "TERMINAL_BEFORE_DEADLINE"
                if not binding_ok:
                    stale_reason = "CLAIM_BINDING_CHANGED"
                http_status, body_json, resp_digest = self._attempt_result_error_response(
                    http_status=409,
                    code="ATTEMPT_STALE",
                    attempt_id=attempt_id,
                    current_attempt_generation=row["generation"],
                )
                self.conn.execute(
                    "INSERT INTO result_requests(result_request_id, attempt_result_id, "
                    "attempt_id, activity_id, attempt_generation, worker_id, worker_session_id, "
                    "attempt_capability_digest, request_digest, result_digest, disposition, "
                    "stale_reason, accepted_result_created, candidate_upload_id, "
                    "attempt_terminal_fact_id, response_http_status, response_json, "
                    "response_digest, created_at_ms) "
                    "VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'STALE_ATTEMPT', ?, 0, ?, "
                    "NULL, ?, ?, ?, ?)",
                    (
                        result_request_id,
                        attempt_id,
                        activity_id,
                        generation,
                        worker_id,
                        worker_session_id,
                        attempt_capability_digest,
                        req_digest,
                        res_digest,
                        stale_reason,
                        candidate_upload_id,
                        http_status,
                        body_json,
                        resp_digest,
                        now,
                    ),
                )
                inserted = self.conn.execute(
                    "SELECT * FROM result_requests WHERE result_request_id = ?",
                    (result_request_id,),
                ).fetchone()
                assert inserted is not None
                return self._result_request_from_row(inserted, replayed=False)

            if before_execution_deadline:
                candidate_id = None
                candidate = None
                if candidate_upload_id is not None:
                    upload = self.get_candidate_upload(candidate_upload_id)
                    if upload is None:
                        raise RunStoreError(
                            f"candidate upload {candidate_upload_id!r} was not found"
                        )
                    if (
                        upload.attempt_id != attempt_id
                        or upload.activity_id != activity_id
                        or upload.attempt_generation != generation
                    ):
                        raise CasMismatchError("candidate upload does not match Attempt Result")
                    if now >= upload.expires_at_ms:
                        self._expire_candidate_upload_row(
                            upload, candidate_store=candidate_store, now_ms=now
                        )
                        expired_row = self.conn.execute(
                            "SELECT * FROM candidate_uploads WHERE upload_id = ?",
                            (candidate_upload_id,),
                        ).fetchone()
                        assert expired_row is not None
                        expired = _row_to_candidate_upload(expired_row)
                        http_status = 410
                        body_obj = self.candidate_upload_expired_body(expired)
                        body_obj["replayed"] = False
                        body_json = canonical_json_text(body_obj)
                        resp_digest = response_digest(
                            {
                                "http_status": http_status,
                                "body": _response_digest_preimage(body_obj),
                            }
                        )
                        self.conn.execute(
                            "INSERT INTO result_requests(result_request_id, attempt_result_id, "
                            "attempt_id, activity_id, attempt_generation, worker_id, "
                            "worker_session_id, attempt_capability_digest, request_digest, "
                            "result_digest, disposition, stale_reason, accepted_result_created, "
                            "candidate_upload_id, attempt_terminal_fact_id, response_http_status, "
                            "response_json, response_digest, created_at_ms) "
                            "VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'UPLOAD_EXPIRED', NULL, "
                            "0, ?, NULL, ?, ?, ?, ?)",
                            (
                                result_request_id,
                                attempt_id,
                                activity_id,
                                generation,
                                worker_id,
                                worker_session_id,
                                attempt_capability_digest,
                                req_digest,
                                res_digest,
                                candidate_upload_id,
                                http_status,
                                body_json,
                                resp_digest,
                                now,
                            ),
                        )
                        inserted = self.conn.execute(
                            "SELECT * FROM result_requests WHERE result_request_id = ?",
                            (result_request_id,),
                        ).fetchone()
                        assert inserted is not None
                        return self._result_request_from_row(inserted, replayed=False)
                    if upload.state != "PROMOTED":
                        raise CasMismatchError("Attempt Result requires a PROMOTED upload")
                    if upload.artifact_bundle_digest is None or upload.verified_tip_json is None:
                        raise CasMismatchError("PROMOTED upload is missing artifact identity")
                    tip = json.loads(upload.verified_tip_json)
                    candidate_id = str(uuid.uuid4())
                    generation_row = self.conn.execute(
                        "SELECT COALESCE(MAX(candidate_generation), 0) + 1 "
                        "FROM candidates WHERE run_id = ?",
                        (row["run_id"],),
                    ).fetchone()
                    candidate_generation = int(generation_row[0])
                    self.conn.execute(
                        "INSERT INTO candidates(candidate_id, run_id, candidate_generation, "
                        "provenance_kind, producing_activity_id, worker_attempt_id, "
                        "worker_attempt_generation, object_format, oid, base_commit_json, "
                        "bundle_digest, created_at_ms) "
                        "VALUES (?, ?, ?, 'WORKER_ATTEMPT', ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            candidate_id,
                            row["run_id"],
                            candidate_generation,
                            activity_id,
                            attempt_id,
                            generation,
                            tip["object_format"],
                            tip["oid"],
                            upload.expected_base_commit_json,
                            upload.artifact_bundle_digest,
                            now,
                        ),
                    )
                    self.conn.execute(
                        "UPDATE candidate_uploads SET state = 'CONSUMED', "
                        "consumed_candidate_id = ?, updated_at_ms = ? "
                        "WHERE upload_id = ? AND state = 'PROMOTED'",
                        (candidate_id, now, candidate_upload_id),
                    )
                    candidate = self.get_candidate(candidate_id)
                    assert candidate is not None

                validate_attempt_structured_output(
                    activity_kind=str(row["activity_kind"]),
                    outcome=outcome,
                    structured_output=structured_output,
                    summary=summary,
                )
                if row["activity_kind"] == "VERIFY":
                    self._require_valid_verification_receipt(
                        row=row,
                        outcome=outcome,
                        receipt=receipt,
                        launch_attestation_id=launch_attestation_id,
                        failure_class=failure_class,
                    )
                review_receipt = None
                review_fills_slot = False
                review_finding_ids_digest = None
                adjudication_receipt = None
                adjudication_fills_slot = False
                adjudication_summary = None
                if row["activity_kind"] == "REVIEW":
                    review_receipt, review_fills_slot, review_finding_ids_digest = (
                        self._derive_review_receipt(
                            row=row,
                            receipt=receipt,
                            outcome=outcome,
                            launch_attestation_id=launch_attestation_id,
                        )
                    )
                if row["activity_kind"] == "ADJUDICATE":
                    adjudication_receipt, adjudication_fills_slot, adjudication_summary = (
                        self._derive_adjudication_receipt(
                            row=row,
                            receipt=receipt,
                            outcome=outcome,
                            launch_attestation_id=launch_attestation_id,
                        )
                    )

                attempt_result_id = str(uuid.uuid4())
                receipt_id = str(uuid.uuid4()) if receipt_json is not None else None
                http_status, body_json, resp_digest = self._attempt_result_accepted_response(
                    attempt_id=attempt_id,
                    activity_id=activity_id,
                    generation=generation,
                    outcome=outcome,
                    candidate_id=candidate_id,
                    receipt_id=receipt_id,
                )
                self.conn.execute(
                    "INSERT INTO attempt_results(attempt_result_id, result_request_id, "
                    "attempt_id, activity_id, attempt_generation, outcome, result_digest, "
                    "body_json, failure_class, failure_json, evidence_refs_json, retry_delay_ms, "
                    "receipt_id, receipt_json, receipt_digest, candidate_id, accepted_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        attempt_result_id,
                        result_request_id,
                        attempt_id,
                        activity_id,
                        generation,
                        outcome,
                        res_digest,
                        canonical_json_text(self._attempt_result_semantic_body(body)),
                        failure_class,
                        failure_json,
                        evidence_refs_json,
                        retry_delay_ms,
                        receipt_id,
                        receipt_json,
                        receipt_digest_value,
                        candidate_id,
                        now,
                    ),
                )
                if review_receipt is not None:
                    assert receipt_id is not None
                    assert review_finding_ids_digest is not None
                    candidate_record, assignment, _current = self._receipt_candidate_and_assignment(
                        row=row, receipt=review_receipt, expected_kind="REVIEW"
                    )
                    trusted_digest = receipt_digest(
                        {
                            "protocol": REVIEW_RECEIPT_PROTOCOL,
                            "receipt": review_receipt,
                            "activity_id": activity_id,
                            "attempt_id": attempt_id,
                            "attempt_generation": generation,
                            "assignment_digest": assignment.assignment_digest,
                            "execution_profile_id": row["execution_profile_id"],
                            "worker_profile": row["worker_profile"],
                            "provider": row["provider"],
                            "model": row["model"],
                            "provider_account_ref": row["provider_account_ref"],
                            "provider_family": row["provider_family"],
                            "model_family": row["model_family"],
                            "classification_revision": row["classification_revision"],
                            "launch_attestation_id": launch_attestation_id,
                        }
                    )
                    self.conn.execute(
                        "INSERT INTO review_receipts(receipt_id, attempt_result_id, run_id, "
                        "candidate_id, object_format, oid, activity_id, attempt_id, "
                        "attempt_generation, specification_generation, policy_hash, "
                        "panel_round, reviewer_slot, role, subject_refs_digest, "
                        "context_digest, execution_profile_id, worker_profile, provider, "
                        "model, provider_account_ref, provider_family, model_family, "
                        "classification_revision, launch_attestation_id, verdict, "
                        "fills_slot, finding_ids_digest, receipt_digest, accepted_at_ms) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            receipt_id,
                            attempt_result_id,
                            row["run_id"],
                            candidate_record.candidate_id,
                            candidate_record.object_format,
                            candidate_record.oid,
                            activity_id,
                            attempt_id,
                            generation,
                            row["specification_generation"],
                            row["activity_policy_hash"],
                            assignment.panel_round,
                            assignment.reviewer_slot,
                            assignment.role,
                            assignment.subject_refs_digest,
                            assignment.context_digest,
                            row["execution_profile_id"],
                            row["worker_profile"],
                            row["provider"],
                            row["model"],
                            row["provider_account_ref"],
                            row["provider_family"],
                            row["model_family"],
                            row["classification_revision"],
                            launch_attestation_id,
                            review_receipt["verdict"],
                            1 if review_fills_slot else 0,
                            review_finding_ids_digest,
                            trusted_digest,
                            now,
                        ),
                    )
                if adjudication_receipt is not None:
                    assert receipt_id is not None
                    assert adjudication_summary is not None
                    candidate_record, assignment, _current = self._receipt_candidate_and_assignment(
                        row=row, receipt=adjudication_receipt, expected_kind="ADJUDICATE"
                    )
                    trusted_digest = receipt_digest(
                        {
                            "protocol": ADJUDICATION_RECEIPT_PROTOCOL,
                            "receipt": adjudication_receipt,
                            "activity_id": activity_id,
                            "attempt_id": attempt_id,
                            "attempt_generation": generation,
                            "assignment_digest": assignment.assignment_digest,
                            "execution_profile_id": row["execution_profile_id"],
                            "worker_profile": row["worker_profile"],
                            "provider": row["provider"],
                            "model": row["model"],
                            "provider_account_ref": row["provider_account_ref"],
                            "provider_family": row["provider_family"],
                            "model_family": row["model_family"],
                            "classification_revision": row["classification_revision"],
                            "launch_attestation_id": launch_attestation_id,
                        }
                    )
                    self.conn.execute(
                        "INSERT INTO adjudication_receipts(receipt_id, attempt_result_id, "
                        "run_id, candidate_id, object_format, oid, activity_id, attempt_id, "
                        "attempt_generation, specification_generation, policy_hash, "
                        "panel_round, adjudication_round, adjudicator_slot, "
                        "subject_refs_digest, context_digest, execution_profile_id, "
                        "worker_profile, provider, model, provider_account_ref, "
                        "provider_family, model_family, classification_revision, "
                        "launch_attestation_id, disposition_summary, fills_slot, "
                        "receipt_digest, accepted_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
                        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            receipt_id,
                            attempt_result_id,
                            row["run_id"],
                            candidate_record.candidate_id,
                            candidate_record.object_format,
                            candidate_record.oid,
                            activity_id,
                            attempt_id,
                            generation,
                            row["specification_generation"],
                            row["activity_policy_hash"],
                            assignment.panel_round,
                            assignment.adjudication_round,
                            assignment.adjudicator_slot,
                            assignment.subject_refs_digest,
                            assignment.context_digest,
                            row["execution_profile_id"],
                            row["worker_profile"],
                            row["provider"],
                            row["model"],
                            row["provider_account_ref"],
                            row["provider_family"],
                            row["model_family"],
                            row["classification_revision"],
                            launch_attestation_id,
                            adjudication_summary,
                            1 if adjudication_fills_slot else 0,
                            trusted_digest,
                            now,
                        ),
                    )
                self.conn.execute(
                    "UPDATE attempts SET state = ?, terminal_reason = ? "
                    "WHERE attempt_id = ? AND state = 'CLAIMED'",
                    (
                        "SUCCEEDED"
                        if outcome == "SUCCEEDED"
                        else "ABSTAINED"
                        if outcome == "ABSTAINED"
                        else "FAILED",
                        outcome,
                        attempt_id,
                    ),
                )
                self.conn.execute(
                    "UPDATE activities SET state = ?, candidate_id = COALESCE(?, candidate_id), "
                    "updated_at_ms = ? WHERE activity_id = ?",
                    (
                        "SUCCEEDED" if outcome == "SUCCEEDED" else "FAILED",
                        candidate_id,
                        now,
                        activity_id,
                    ),
                )
                from orcest.workflow_reducer.ledger import apply, load_view
                from orcest.workflow_reducer.types import Trigger

                view = load_view(self, row["run_id"])
                if view is not None:
                    apply(
                        self,
                        view,
                        Trigger(
                            kind="ATTEMPT_RESULT",
                            trigger_id=attempt_result_id,
                            facts={
                                "outcome": outcome,
                                "activity_kind": row["activity_kind"],
                                "candidate_id": candidate_id,
                                "failure_class": failure_class,
                                "verification_outcome": receipt.get("outcome")
                                if row["activity_kind"] == "VERIFY" and isinstance(receipt, Mapping)
                                else None,
                                "fills_slot": review_fills_slot or adjudication_fills_slot,
                                "panel_complete": self._review_panel_complete(
                                    run_id=str(row["run_id"]),
                                    candidate_id=str(row["activity_candidate_id"]),
                                    panel_round=int(review_receipt["panel_round"]),
                                )
                                if review_receipt is not None
                                else None,
                                "disposition": adjudication_summary,
                                "structured_output_protocol": (
                                    structured_output.get("protocol_version")
                                    if isinstance(structured_output, Mapping)
                                    else None
                                ),
                            },
                        ),
                        run_id=row["run_id"],
                    )
                self.conn.execute(
                    "INSERT INTO result_requests(result_request_id, attempt_result_id, "
                    "attempt_id, activity_id, attempt_generation, worker_id, worker_session_id, "
                    "attempt_capability_digest, request_digest, result_digest, disposition, "
                    "stale_reason, accepted_result_created, candidate_upload_id, "
                    "attempt_terminal_fact_id, response_http_status, response_json, "
                    "response_digest, created_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACCEPTED', NULL, 1, ?, NULL, "
                    "?, ?, ?, ?)",
                    (
                        result_request_id,
                        attempt_result_id,
                        attempt_id,
                        activity_id,
                        generation,
                        worker_id,
                        worker_session_id,
                        attempt_capability_digest,
                        req_digest,
                        res_digest,
                        candidate_upload_id,
                        http_status,
                        body_json,
                        resp_digest,
                        now,
                    ),
                )
                inserted = self.conn.execute(
                    "SELECT * FROM result_requests WHERE result_request_id = ?",
                    (result_request_id,),
                ).fetchone()
                assert inserted is not None
                return self._result_request_from_row(inserted, replayed=False)

            terminal_kind = (
                "EXECUTION_DEADLINE"
                if row["state"] == "CLAIMED" and binding_ok
                else "RESULT_AFTER_TERMINAL"
            )
            disposition = (
                "EXPIRED_CURRENT" if terminal_kind == "EXECUTION_DEADLINE" else "ALREADY_TERMINAL"
            )
            fact_id = str(uuid.uuid4())
            fact_digest = attempt_terminal_fact_digest(
                {
                    "attempt_id": attempt_id,
                    "activity_id": activity_id,
                    "attempt_generation": generation,
                    "kind": terminal_kind,
                    "source_kind": "RESULT_REQUEST",
                    "source_id": result_request_id,
                }
            )
            self.conn.execute(
                "INSERT INTO attempt_terminal_facts(attempt_terminal_fact_id, attempt_id, "
                "activity_id, attempt_generation, kind, source_kind, source_id, "
                "health_observation_id, fact_digest, recorded_at_ms) "
                "VALUES (?, ?, ?, ?, ?, 'RESULT_REQUEST', ?, NULL, ?, ?)",
                (
                    fact_id,
                    attempt_id,
                    activity_id,
                    generation,
                    terminal_kind,
                    result_request_id,
                    fact_digest,
                    now,
                ),
            )
            if disposition == "EXPIRED_CURRENT":
                self.conn.execute(
                    "UPDATE attempts SET state = 'EXPIRED', terminal_reason = 'EXECUTION_DEADLINE' "
                    "WHERE attempt_id = ? AND state = 'CLAIMED'",
                    (attempt_id,),
                )
                self.conn.execute(
                    "UPDATE activities SET state = 'PLANNED', updated_at_ms = ? "
                    "WHERE activity_id = ? AND state = 'ACTIVE'",
                    (now, activity_id),
                )
            from orcest.workflow_reducer.ledger import apply, load_view
            from orcest.workflow_reducer.types import Trigger

            view = load_view(self, row["run_id"])
            if view is not None:
                apply(
                    self,
                    view,
                    Trigger(
                        kind="ATTEMPT_TERMINAL",
                        trigger_id=fact_id,
                        facts={
                            "kind": terminal_kind,
                            "already_terminal": disposition == "ALREADY_TERMINAL",
                        },
                    ),
                    run_id=row["run_id"],
                )
            http_status, body_json, resp_digest = self._attempt_result_error_response(
                http_status=410 if disposition == "EXPIRED_CURRENT" else 409,
                code="EXECUTION_DEADLINE_EXCEEDED"
                if disposition == "EXPIRED_CURRENT"
                else "ATTEMPT_STALE",
                attempt_id=attempt_id,
                current_attempt_generation=row["generation"],
            )
            self.conn.execute(
                "INSERT INTO result_requests(result_request_id, attempt_result_id, "
                "attempt_id, activity_id, attempt_generation, worker_id, worker_session_id, "
                "attempt_capability_digest, request_digest, result_digest, disposition, "
                "stale_reason, accepted_result_created, candidate_upload_id, "
                "attempt_terminal_fact_id, response_http_status, response_json, "
                "response_digest, created_at_ms) "
                "VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?, ?, ?, ?, ?)",
                (
                    result_request_id,
                    attempt_id,
                    activity_id,
                    generation,
                    worker_id,
                    worker_session_id,
                    attempt_capability_digest,
                    req_digest,
                    res_digest,
                    disposition,
                    candidate_upload_id,
                    fact_id,
                    http_status,
                    body_json,
                    resp_digest,
                    now,
                ),
            )
            inserted = self.conn.execute(
                "SELECT * FROM result_requests WHERE result_request_id = ?",
                (result_request_id,),
            ).fetchone()
            assert inserted is not None
            return self._result_request_from_row(inserted, replayed=False)

    # -- Health Observation ------------------------------------------------

    def _next_health_sequence(self, scope_kind: str, scope_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(health_sequence), 0) AS seq FROM health_observations "
            "WHERE scope_kind = ? AND scope_id = ?",
            (scope_kind, scope_id),
        ).fetchone()
        return int(row["seq"]) + 1

    def _row_to_health_observation(self, row: sqlite3.Row) -> HealthObservationRecord:
        return HealthObservationRecord(
            health_observation_id=row["health_observation_id"],
            scope_kind=row["scope_kind"],
            scope_id=row["scope_id"],
            health_sequence=row["health_sequence"],
            kind=row["kind"],
            source_kind=row["source_kind"],
            source_id=row["source_id"],
            subject_bindings_json=row["subject_bindings_json"],
            observed_revision=row["observed_revision"],
            effective_at_ms=row["effective_at_ms"],
            expires_at_ms=row["expires_at_ms"],
            payload_digest=row["payload_digest"],
            created_at_ms=row["created_at_ms"],
        )

    def _insert_health_observation(
        self,
        *,
        scope_kind: str,
        scope_id: str,
        kind: str,
        source_kind: str,
        source_id: str,
        subject_bindings: Mapping[str, Any],
        observed_revision: int | None,
        effective_at_ms: int,
        expires_at_ms: int | None,
    ) -> HealthObservationRecord:
        """Insert one immutable, ordered Health Observation.

        Must run inside ``self.transaction()``, alongside the report/fact that
        sources it, per domain-model.md "Health Observation":
        ``health_sequence`` is strictly increasing within
        ``(scope_kind, scope_id)`` and a fresh authenticated observation
        always gets a new ID/sequence, even when its payload repeats an
        earlier one.
        """
        health_observation_id = str(uuid.uuid4())
        health_sequence = self._next_health_sequence(scope_kind, scope_id)
        payload_digest = health_observation_payload_digest(
            {
                "scope_kind": scope_kind,
                "scope_id": scope_id,
                "kind": kind,
                "source_kind": source_kind,
                "source_id": source_id,
                "subject_bindings": dict(subject_bindings),
                "observed_revision": observed_revision,
                "effective_at_ms": effective_at_ms,
                "expires_at_ms": expires_at_ms,
            }
        )
        self.conn.execute(
            "INSERT INTO health_observations(health_observation_id, scope_kind, scope_id, "
            "health_sequence, kind, source_kind, source_id, subject_bindings_json, "
            "observed_revision, effective_at_ms, expires_at_ms, payload_digest, "
            "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                health_observation_id,
                scope_kind,
                scope_id,
                health_sequence,
                kind,
                source_kind,
                source_id,
                canonical_json_text(subject_bindings),
                observed_revision,
                effective_at_ms,
                expires_at_ms,
                payload_digest,
                effective_at_ms,
            ),
        )
        row = self.conn.execute(
            "SELECT * FROM health_observations WHERE health_observation_id = ?",
            (health_observation_id,),
        ).fetchone()
        assert row is not None
        return self._row_to_health_observation(row)

    def get_latest_health_observation(
        self, scope_kind: str, scope_id: str, *, now_ms: int | None = None
    ) -> HealthObservationRecord | None:
        """The highest applicable unexpired Health Observation for one scope.

        Never a fallback: an expired or absent Observation returns ``None``
        rather than synthesizing availability (domain-model.md "Health
        Observation": "the reducer uses the highest applicable unexpired
        sequence ... Health arrival order, wall-clock timestamps, or Redis
        lease presence cannot choose a fallback").
        """
        now = _now_ms() if now_ms is None else now_ms
        row = self.conn.execute(
            "SELECT * FROM health_observations WHERE scope_kind = ? AND scope_id = ? "
            "AND (expires_at_ms IS NULL OR expires_at_ms > ?) "
            "ORDER BY health_sequence DESC LIMIT 1",
            (scope_kind, scope_id, now),
        ).fetchone()
        return None if row is None else self._row_to_health_observation(row)

    def get_health_probe_request(
        self, health_probe_request_id: str
    ) -> HealthProbeRequestRecord | None:
        require_lowercase_uuid(health_probe_request_id, field="health_probe_request_id")
        row = self.conn.execute(
            "SELECT * FROM health_probe_requests WHERE health_probe_request_id = ?",
            (health_probe_request_id,),
        ).fetchone()
        return None if row is None else _row_to_health_probe_request(row)

    def get_health_probe_fact(self, health_probe_fact_id: str) -> HealthProbeFactRecord | None:
        require_lowercase_uuid(health_probe_fact_id, field="health_probe_fact_id")
        row = self.conn.execute(
            "SELECT * FROM health_probe_facts WHERE health_probe_fact_id = ?",
            (health_probe_fact_id,),
        ).fetchone()
        if row is None:
            return None
        members = self.conn.execute(
            "SELECT run_id FROM health_probe_fact_runs WHERE health_probe_fact_id = ? "
            "ORDER BY member_ordinal",
            (health_probe_fact_id,),
        ).fetchall()
        return _row_to_health_probe_fact(
            row, affected_run_ids=tuple(member["run_id"] for member in members)
        )

    def create_health_probe_request(
        self,
        *,
        health_probe_request_id: str,
        probe_kind: str,
        scope_kind: str,
        scope_id: str,
        request_identity: str,
        subject_bindings: Mapping[str, Any],
        implementation_digest: str,
        input_digest: str,
        evidence_digest: str,
        expected_revision: int | None = None,
        outbox_id: str | None = None,
        destination: str = "controller",
        not_after_ms: int | None = None,
        now_ms: int | None = None,
    ) -> HealthProbeRequestRecord:
        """Persist a Health Probe Request and outbox before probe I/O.

        ``request_identity`` is the deterministic replay key for the exact
        probe target and implementation/input/evidence digests. Replaying it
        returns the original request/outbox rather than creating a second
        authority source.
        """
        require_lowercase_uuid(health_probe_request_id, field="health_probe_request_id")
        enums.parse_enum("health_probe.probe_kind", probe_kind)
        enums.parse_enum("health_probe.scope_kind", scope_kind)
        _validate_health_probe_matrix(probe_kind=probe_kind, scope_kind=scope_kind)
        _require_digest(implementation_digest, field="implementation_digest")
        _require_digest(input_digest, field="input_digest")
        _require_digest(evidence_digest, field="evidence_digest")
        if expected_revision is not None and expected_revision < 0:
            raise ValueError("expected_revision must be nonnegative")
        now = _now_ms() if now_ms is None else now_ms
        if not_after_ms is not None and not_after_ms <= now:
            raise ValueError("not_after_ms must be after creation time")
        payload = {
            "protocol": HEALTH_PROBE_REQUEST_PROTOCOL,
            "probe_kind": probe_kind,
            "scope_kind": scope_kind,
            "scope_id": scope_id,
            "request_identity": request_identity,
            "subject_bindings": dict(subject_bindings),
            "expected_revision": expected_revision,
            "implementation_digest": implementation_digest,
            "input_digest": input_digest,
            "evidence_digest": evidence_digest,
            "not_after_ms": not_after_ms,
        }
        req_digest = health_probe_request_digest(payload)
        outbox_payload_digest = request_digest(payload)
        resolved_outbox_id = outbox_id or str(uuid.uuid4())
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM health_probe_requests WHERE probe_kind = ? AND scope_kind = ? "
                "AND scope_id = ? AND request_identity = ?",
                (probe_kind, scope_kind, scope_id, request_identity),
            ).fetchone()
            if existing is not None:
                record = _row_to_health_probe_request(existing)
                if (
                    record.health_probe_request_id == health_probe_request_id
                    and record.subject_bindings_json == canonical_json_text(subject_bindings)
                    and record.expected_revision == expected_revision
                    and record.implementation_digest == implementation_digest
                    and record.input_digest == input_digest
                    and record.evidence_digest == evidence_digest
                    and record.request_digest == req_digest
                    and record.not_after_ms == not_after_ms
                ):
                    return record
                raise IdempotencyConflictError(
                    "health probe request identity was reused with different content"
                )
            self.insert_outbox(
                outbox_id=resolved_outbox_id,
                source_kind="HEALTH_PROBE_REQUEST",
                source_id=health_probe_request_id,
                destination=destination,
                protocol_version=HEALTH_PROBE_REQUEST_PROTOCOL,
                payload_digest=outbox_payload_digest,
                payload=payload,
                next_delivery_at_ms=now,
            )
            self.conn.execute(
                "INSERT INTO health_probe_requests(health_probe_request_id, protocol_version, "
                "probe_kind, scope_kind, scope_id, request_identity, subject_bindings_json, "
                "expected_revision, implementation_digest, input_digest, evidence_digest, "
                "request_digest, state, outbox_id, not_after_ms, created_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)",
                (
                    health_probe_request_id,
                    HEALTH_PROBE_REQUEST_PROTOCOL,
                    probe_kind,
                    scope_kind,
                    scope_id,
                    request_identity,
                    canonical_json_text(subject_bindings),
                    expected_revision,
                    implementation_digest,
                    input_digest,
                    evidence_digest,
                    req_digest,
                    resolved_outbox_id,
                    not_after_ms,
                    now,
                ),
            )
            row = self.conn.execute(
                "SELECT * FROM health_probe_requests WHERE health_probe_request_id = ?",
                (health_probe_request_id,),
            ).fetchone()
            assert row is not None
            return _row_to_health_probe_request(row)

    def complete_health_probe_request(
        self,
        *,
        health_probe_request_id: str,
        health_probe_fact_id: str,
        outcome: str,
        evidence: Mapping[str, Any],
        integrity_failure_code: str | None = None,
        observed_revision: int | None = None,
        expires_at_ms: int | None = None,
        affected_run_ids: Sequence[str] | None = None,
        now_ms: int | None = None,
    ) -> HealthProbeCompletion:
        """Complete one persisted Health Probe Request and resume fanout.

        Completion is idempotent by the deterministic request identity: after
        response loss, callers replay the same request and get the same Fact,
        Observation, and cursor-driven reductions.
        """
        require_lowercase_uuid(health_probe_request_id, field="health_probe_request_id")
        require_lowercase_uuid(health_probe_fact_id, field="health_probe_fact_id")
        enums.parse_enum("health_probe_fact.outcome", outcome)
        if integrity_failure_code is not None:
            enums.parse_enum("health_probe_fact.integrity_failure_code", integrity_failure_code)
        now = _now_ms() if now_ms is None else now_ms
        existing_request = self.get_health_probe_request(health_probe_request_id)
        if existing_request is not None and existing_request.state == "COMPLETED":
            if existing_request.health_probe_fact_id != health_probe_fact_id:
                raise IdempotencyConflictError("health probe completion fact id changed")
            self.run_health_probe_fact_fanout(health_probe_fact_id)
            return self._health_probe_completion_from_fact_id(health_probe_fact_id, replayed=True)
        with self.transaction():
            request_row = self.conn.execute(
                "SELECT * FROM health_probe_requests WHERE health_probe_request_id = ?",
                (health_probe_request_id,),
            ).fetchone()
            if request_row is None:
                raise RunStoreError(f"health probe request {health_probe_request_id!r} not found")
            request_record = _row_to_health_probe_request(request_row)
            if request_record.state == "COMPLETED":
                if request_record.health_probe_fact_id != health_probe_fact_id:
                    raise IdempotencyConflictError("health probe completion fact id changed")
                raise CasMismatchError("health probe request completed concurrently")
            if request_record.state != "PENDING":
                raise CasMismatchError("health probe request is not pending")
            _validate_health_probe_outcome(
                probe_kind=request_record.probe_kind,
                scope_kind=request_record.scope_kind,
                outcome=outcome,
            )
            _validate_health_probe_integrity_failure(
                probe_kind=request_record.probe_kind,
                outcome=outcome,
                integrity_failure_code=integrity_failure_code,
            )
            subject_bindings = json.loads(request_record.subject_bindings_json)
            if not isinstance(subject_bindings, dict):
                raise RunStoreError("health probe request bindings are corrupt")
            members = tuple(
                sorted(
                    dict.fromkeys(
                        affected_run_ids
                        if affected_run_ids is not None
                        else self._health_probe_affected_run_ids(
                            probe_kind=request_record.probe_kind,
                            scope_kind=request_record.scope_kind,
                            scope_id=request_record.scope_id,
                            subject_bindings=subject_bindings,
                        )
                    )
                )
            )
            for run_id in members:
                require_lowercase_uuid(run_id, field="affected_run_ids[]")
            membership_digest = health_probe_run_membership_digest(members)
            fact_preimage = {
                "health_probe_request_id": health_probe_request_id,
                "probe_kind": request_record.probe_kind,
                "scope_kind": request_record.scope_kind,
                "scope_id": request_record.scope_id,
                "request_identity": request_record.request_identity,
                "outcome": outcome,
                "observed_revision": observed_revision,
                "implementation_digest": request_record.implementation_digest,
                "input_digest": request_record.input_digest,
                "evidence_digest": request_record.evidence_digest,
                "integrity_failure_code": integrity_failure_code,
                "subject_bindings": subject_bindings,
                "probe_evidence": dict(evidence),
                "affected_run_ids_digest": membership_digest,
            }
            fact_digest = health_probe_fact_digest(fact_preimage)
            observation = self._insert_health_observation(
                scope_kind=request_record.scope_kind,
                scope_id=request_record.scope_id,
                kind=outcome,
                source_kind="HEALTH_PROBE_FACT",
                source_id=health_probe_fact_id,
                subject_bindings={
                    **subject_bindings,
                    "health_probe_request_id": health_probe_request_id,
                    "request_identity": request_record.request_identity,
                    "probe_kind": request_record.probe_kind,
                    "integrity_failure_code": integrity_failure_code,
                },
                observed_revision=observed_revision,
                effective_at_ms=now,
                expires_at_ms=expires_at_ms,
            )
            self.conn.execute(
                "INSERT INTO health_probe_facts(health_probe_fact_id, health_probe_request_id, "
                "probe_kind, scope_kind, scope_id, request_identity, outcome, observed_revision, "
                "implementation_digest, input_digest, evidence_digest, integrity_failure_code, "
                "subject_bindings_json, affected_run_ids_digest, health_observation_id, "
                "fact_digest, recorded_at_ms) VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    health_probe_fact_id,
                    health_probe_request_id,
                    request_record.probe_kind,
                    request_record.scope_kind,
                    request_record.scope_id,
                    request_record.request_identity,
                    outcome,
                    observed_revision,
                    request_record.implementation_digest,
                    request_record.input_digest,
                    request_record.evidence_digest,
                    integrity_failure_code,
                    canonical_json_text({**subject_bindings, "probe_evidence": dict(evidence)}),
                    membership_digest,
                    observation.health_observation_id,
                    fact_digest,
                    now,
                ),
            )
            for ordinal, run_id in enumerate(members):
                self.conn.execute(
                    "INSERT INTO health_probe_fact_runs(health_probe_fact_id, member_ordinal, "
                    "run_id) VALUES (?, ?, ?)",
                    (health_probe_fact_id, ordinal, run_id),
                )
            self.conn.execute(
                "UPDATE health_probe_requests SET state = 'COMPLETED', completed_at_ms = ?, "
                "health_probe_fact_id = ? WHERE health_probe_request_id = ?",
                (now, health_probe_fact_id, health_probe_request_id),
            )

        self.run_health_probe_fact_fanout(health_probe_fact_id)
        return self._health_probe_completion_from_fact_id(health_probe_fact_id, replayed=False)

    def _health_probe_completion_from_fact_id(
        self, health_probe_fact_id: str, *, replayed: bool
    ) -> HealthProbeCompletion:
        fact = self.get_health_probe_fact(health_probe_fact_id)
        if fact is None:
            raise RunStoreError(f"health probe fact {health_probe_fact_id!r} not found")
        request = self.get_health_probe_request(fact.health_probe_request_id)
        if request is None:
            raise RunStoreError("health probe fact is missing its reciprocal request")
        observation = self.conn.execute(
            "SELECT * FROM health_observations WHERE health_observation_id = ?",
            (fact.health_observation_id,),
        ).fetchone()
        if observation is None:
            raise RunStoreError("health probe fact is missing its reciprocal observation")
        applied_rows = self.conn.execute(
            "SELECT run_id, recovery_evidence_id FROM health_probe_fact_runs "
            "WHERE health_probe_fact_id = ? AND transition_sequence IS NOT NULL "
            "ORDER BY member_ordinal",
            (health_probe_fact_id,),
        ).fetchall()
        return HealthProbeCompletion(
            request=request,
            fact=fact,
            observation=self._row_to_health_observation(observation),
            applied_run_ids=tuple(row["run_id"] for row in applied_rows),
            recovery_evidence_ids=tuple(
                row["recovery_evidence_id"]
                for row in applied_rows
                if row["recovery_evidence_id"] is not None
            ),
            replayed=replayed,
        )

    def run_health_probe_fact_fanout(
        self, health_probe_fact_id: str
    ) -> tuple[list[str], list[str]]:
        """Resume cursor-driven per-Run Health Probe Fact fanout."""
        require_lowercase_uuid(health_probe_fact_id, field="health_probe_fact_id")
        applied: list[str] = []
        evidences: list[str] = []
        while True:
            with self.transaction():
                row = self.conn.execute(
                    "SELECT * FROM health_probe_facts WHERE health_probe_fact_id = ?",
                    (health_probe_fact_id,),
                ).fetchone()
                if row is None:
                    raise RunStoreError(f"health probe fact {health_probe_fact_id!r} not found")
                fact = _row_to_health_probe_fact(row)
                member = self.conn.execute(
                    "SELECT * FROM health_probe_fact_runs WHERE health_probe_fact_id = ? "
                    "AND member_ordinal = ?",
                    (health_probe_fact_id, fact.fanout_cursor_ordinal),
                ).fetchone()
                if member is None:
                    if fact.fanout_completed_at_ms is None:
                        self.conn.execute(
                            "UPDATE health_probe_facts SET fanout_completed_at_ms = ? "
                            "WHERE health_probe_fact_id = ?",
                            (_now_ms(), health_probe_fact_id),
                        )
                    return applied, evidences
                if member["transition_sequence"] is not None:
                    self.conn.execute(
                        "UPDATE health_probe_facts SET fanout_cursor_ordinal = ? "
                        "WHERE health_probe_fact_id = ?",
                        (fact.fanout_cursor_ordinal + 1, health_probe_fact_id),
                    )
                    continue
                run_id = str(member["run_id"])
                result = self._apply_health_probe_fact_member(fact, run_id)
                self.conn.execute(
                    "UPDATE health_probe_fact_runs SET transition_sequence = ?, "
                    "recovery_evidence_id = ? WHERE health_probe_fact_id = ? "
                    "AND member_ordinal = ?",
                    (
                        result[0],
                        result[1],
                        health_probe_fact_id,
                        fact.fanout_cursor_ordinal,
                    ),
                )
                self.conn.execute(
                    "UPDATE health_probe_facts SET fanout_cursor_ordinal = ? "
                    "WHERE health_probe_fact_id = ?",
                    (fact.fanout_cursor_ordinal + 1, health_probe_fact_id),
                )
                applied.append(run_id)
                if result[1] is not None:
                    evidences.append(result[1])

    def _apply_health_probe_fact_member(
        self, fact: HealthProbeFactRecord, run_id: str
    ) -> tuple[int, str | None]:
        from orcest.workflow_reducer.ledger import apply, load_view
        from orcest.workflow_reducer.recovery import (
            HealthObservationRef,
            RecoveryEvidenceInput,
            select_recovery_decision,
        )
        from orcest.workflow_reducer.types import Trigger

        view = load_view(self, run_id)
        if view is None:
            raise RunStoreError(f"health probe fanout run {run_id!r} disappeared")
        subject = _health_probe_subject(fact)
        facts = {
            "integrity_unavailable": fact.outcome == "UNAVAILABLE",
            "integrity_available": fact.outcome == "AVAILABLE"
            and fact.probe_kind in {"STORAGE_OBJECT_INTEGRITY", "SECRET_VERSION_INTEGRITY"},
            "health_unavailable": fact.outcome in {"UNAVAILABLE", "RATE_LIMITED", "EXHAUSTED"},
            "health_available": fact.outcome == "AVAILABLE",
            "wakes_wait": fact.outcome == "AVAILABLE"
            and self._health_probe_matches_current_wait(fact, view),
            "probe_kind": fact.probe_kind,
            "scope_kind": fact.scope_kind,
            "scope_id": fact.scope_id,
            "request_identity": fact.request_identity,
            "object_kind": subject.get("object_kind"),
            "object_id": subject.get("object_id", fact.scope_id),
        }
        applied = apply(
            self,
            view,
            Trigger(kind="HEALTH_OBSERVATION", trigger_id=fact.health_observation_id, facts=facts),
            run_id=run_id,
        )
        recovery_evidence_id: str | None = None
        if (
            fact.outcome in {"AVAILABLE", "UNAVAILABLE", "RATE_LIMITED", "EXHAUSTED"}
            and not applied.replayed
        ):
            refreshed = load_view(self, run_id)
            if refreshed is not None and refreshed.state == "RECOVERING":
                category = _health_probe_recovery_category(fact)
                bounded_evidence: dict[str, Any] = {
                    "health_probe_fact_id": fact.health_probe_fact_id,
                    "integrity_failure_code": fact.integrity_failure_code,
                }
                if fact.outcome == "AVAILABLE":
                    bounded_evidence["probe_available"] = True
                evidence_input = RecoveryEvidenceInput(
                    source_kind="HEALTH_OBSERVATION",
                    source_id=fact.health_observation_id,
                    category=category,
                    specification_generation=refreshed.specification_generation,
                    candidate_id=refreshed.current_candidate_id,
                    failure_scope={
                        "scope_kind": fact.scope_kind,
                        "scope_id": fact.scope_id,
                        "object_kind": subject.get("object_kind"),
                        "object_id": subject.get("object_id", fact.scope_id),
                    },
                    bounded_evidence={
                        **bounded_evidence,
                    },
                    accepted_at_ms=fact.recorded_at_ms,
                )
                decision = select_recovery_decision(
                    evidence_input,
                    health_observations=(
                        HealthObservationRef(
                            health_observation_id=fact.health_observation_id,
                            scope_kind=fact.scope_kind,
                            scope_id=fact.scope_id,
                            health_sequence=self._health_observation_sequence(
                                fact.health_observation_id
                            ),
                        ),
                    ),
                )
                recovery_evidence_id = str(uuid.uuid4())
                evidence = self._create_recovery_evidence(
                    recovery_evidence_id=recovery_evidence_id,
                    run_id=run_id,
                    source_kind="HEALTH_OBSERVATION",
                    source_id=fact.health_observation_id,
                    category=decision.category,
                    failure_fingerprint=decision.failure_fingerprint,
                    strategy_index=decision.strategy_index,
                    selected_tactic=decision.selected_tactic,
                    attempt_count=decision.attempt_count,
                    repair_cycle_count=decision.repair_cycle_count,
                    diagnosis_count=decision.diagnosis_count,
                    rescue_epoch=decision.rescue_epoch,
                    health_observations=(
                        self._health_observation_record(fact.health_observation_id),
                    ),
                    specification_generation=refreshed.specification_generation,
                    candidate_id=refreshed.current_candidate_id,
                    next_eligible_at_ms=decision.next_eligible_at_ms,
                )
                recovery_applied = apply(
                    self,
                    refreshed,
                    Trigger(
                        kind="RECOVERY_EVIDENCE",
                        trigger_id=evidence.recovery_evidence_id,
                        facts={
                            "source_kind": "HEALTH_OBSERVATION",
                            "source_id": fact.health_observation_id,
                            "category": decision.category,
                            "selected_tactic": decision.selected_tactic,
                            "candidate_id": refreshed.current_candidate_id,
                            "accepted_at_ms": fact.recorded_at_ms,
                            "failure_scope": evidence_input.failure_scope,
                            "bounded_evidence": bounded_evidence,
                            "health_observations": [
                                {
                                    "health_observation_id": fact.health_observation_id,
                                    "scope_kind": fact.scope_kind,
                                    "scope_id": fact.scope_id,
                                    "health_sequence": self._health_observation_sequence(
                                        fact.health_observation_id
                                    ),
                                }
                            ],
                            "pending_wait_condition_id": str(uuid.uuid4()),
                        },
                    ),
                    run_id=run_id,
                )
                if (
                    recovery_applied.reduction is not None
                    and recovery_applied.reduction.next_state == "WAITING"
                ):
                    self.create_wait_condition(
                        wait_condition_id=str(
                            recovery_applied.reduction.pointer_updates["wait_condition_id"]
                            if "wait_condition_id" in recovery_applied.reduction.pointer_updates
                            else recovery_applied.transition.transition_id
                        ),
                        run_id=run_id,
                        reason=str(recovery_applied.reduction.pointer_updates["wait_reason"]),
                        resume_state=(
                            refreshed.recovery_origin_state or refreshed.state or "PLANNING"
                        ),
                        specification_generation=recovery_applied.reduction.specification_generation,
                        policy_hash=refreshed.policy_hash,
                        created_from_kind="RECOVERY_EVIDENCE",
                        created_from_id=evidence.recovery_evidence_id,
                        created_transition_sequence=recovery_applied.transition.transition_sequence,
                        not_before_ms=decision.next_eligible_at_ms,
                        wake_kind=_health_probe_wait_kind(fact, decision.selected_tactic),
                        wake_identity=_health_probe_wait_identity(fact, decision.selected_tactic),
                        health_observations=(
                            self._health_observation_record(fact.health_observation_id),
                        ),
                    )
        return applied.transition.transition_sequence, recovery_evidence_id

    def _health_probe_matches_current_wait(self, fact: HealthProbeFactRecord, view: Any) -> bool:
        if view.state != "WAITING" or view.wait_condition_id is None:
            return False
        wait = self.get_wait_condition(view.wait_condition_id)
        if wait is None:
            return False
        observation = self._health_observation_record(fact.health_observation_id)
        if wait.health_observation_ids:
            max_bound_sequence = max(
                self._health_observation_record(observation_id).health_sequence
                for observation_id in wait.health_observation_ids
            )
            if observation.health_sequence <= max_bound_sequence:
                return False
        expected_kind = _health_probe_wait_kind(fact, "WAIT_EXTERNAL")
        if wait.wake_kind == expected_kind and wait.wake_identity == _health_probe_wait_identity(
            fact, "WAIT_EXTERNAL"
        ):
            return True
        if wait.wake_kind == "CAPACITY" and wait.wake_identity == {
            "scope_kind": fact.scope_kind,
            "scope_id": fact.scope_id,
        }:
            return True
        if wait.wake_kind == "RATE_LIMIT_RESET" and wait.wake_identity == {
            "scope_kind": fact.scope_kind,
            "scope_id": fact.scope_id,
        }:
            return True
        return False

    def _health_observation_record(self, health_observation_id: str) -> HealthObservationRecord:
        row = self.conn.execute(
            "SELECT * FROM health_observations WHERE health_observation_id = ?",
            (health_observation_id,),
        ).fetchone()
        if row is None:
            raise RunStoreError(f"health observation {health_observation_id!r} not found")
        return self._row_to_health_observation(row)

    def _health_observation_sequence(self, health_observation_id: str) -> int:
        return self._health_observation_record(health_observation_id).health_sequence

    def _health_probe_affected_run_ids(
        self,
        *,
        probe_kind: str,
        scope_kind: str,
        scope_id: str,
        subject_bindings: Mapping[str, Any],
    ) -> tuple[str, ...]:
        if probe_kind == "STORAGE_OBJECT_INTEGRITY":
            object_kind = str(subject_bindings.get("object_kind", ""))
            object_id = str(subject_bindings.get("object_id", scope_id))
            if object_kind == "CANDIDATE_ARTIFACT":
                rows = self.conn.execute(
                    "SELECT DISTINCT runs.run_id FROM runs "
                    "JOIN candidates ON candidates.run_id = runs.run_id "
                    "WHERE runs.terminal_outcome IS NULL AND candidates.bundle_digest = ?",
                    (object_id,),
                ).fetchall()
            elif object_kind == "WORKFLOW_BLOB":
                rows = self.conn.execute(
                    "SELECT DISTINCT runs.run_id FROM runs "
                    "JOIN work_item_snapshots s ON s.run_id = runs.run_id "
                    "WHERE runs.terminal_outcome IS NULL AND "
                    "(s.normalized_workflow_blob_digest = ? OR s.effective_policy_blob_digest = ? "
                    "OR s.normalized_prompt_blobs_json LIKE ?)",
                    (object_id, object_id, f"%{object_id}%"),
                ).fetchall()
            else:
                raise ValueError("STORAGE_OBJECT_INTEGRITY requires object_kind/object_id bindings")
            return tuple(sorted(row["run_id"] for row in rows))
        if probe_kind == "SECRET_VERSION_INTEGRITY":
            secret_id, version = _parse_secret_version_scope(scope_id, subject_bindings)
            rows = self.conn.execute(
                "SELECT DISTINCT runs.run_id FROM runs "
                "JOIN projects ON projects.project_id = runs.project_id "
                "WHERE runs.terminal_outcome IS NULL AND ("
                "(projects.source_read_secret_id = ? "
                "AND projects.registration_source_read_secret_version = ?) "
                "OR (projects.publication_secret_id = ? "
                "AND projects.registration_publication_secret_version = ?))",
                (secret_id, version, secret_id, version),
            ).fetchall()
            return tuple(sorted(row["run_id"] for row in rows))
        if scope_kind == "PROVIDER_ACCOUNT":
            rows = self.conn.execute(
                "SELECT DISTINCT runs.run_id FROM runs "
                "JOIN activities ON activities.run_id = runs.run_id "
                "JOIN attempts ON attempts.activity_id = activities.activity_id "
                "WHERE runs.terminal_outcome IS NULL AND attempts.provider_account_ref = ?",
                (scope_id,),
            ).fetchall()
            return tuple(sorted(row["run_id"] for row in rows))
        if scope_kind == "FORGE":
            rows = self.conn.execute(
                "SELECT DISTINCT runs.run_id FROM runs "
                "JOIN projects ON projects.project_id = runs.project_id "
                "WHERE runs.terminal_outcome IS NULL AND projects.forge_instance_id = ?",
                (scope_id,),
            ).fetchall()
            return tuple(sorted(row["run_id"] for row in rows))
        return ()

    def _next_recovery_sequence(self, run_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(recovery_sequence), 0) AS seq FROM recovery_evidence "
            "WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return int(row["seq"]) + 1

    def get_recovery_evidence(self, recovery_evidence_id: str) -> RecoveryEvidenceRecord | None:
        row = self.conn.execute(
            "SELECT * FROM recovery_evidence WHERE recovery_evidence_id = ?",
            (recovery_evidence_id,),
        ).fetchone()
        if row is None:
            return None
        health_rows = self.conn.execute(
            "SELECT health_observation_id FROM recovery_evidence_health_observations "
            "WHERE recovery_evidence_id = ? ORDER BY observation_ordinal",
            (recovery_evidence_id,),
        ).fetchall()
        return _row_to_recovery_evidence(
            row,
            health_observation_ids=tuple(
                str(item["health_observation_id"]) for item in health_rows
            ),
        )

    def create_recovery_evidence(
        self,
        *,
        recovery_evidence_id: str,
        run_id: str,
        source_kind: str,
        source_id: str,
        category: str,
        failure_fingerprint: str,
        strategy_index: int,
        selected_tactic: str,
        attempt_count: int,
        repair_cycle_count: int,
        diagnosis_count: int,
        rescue_epoch: int,
        health_observations: Sequence[HealthObservationRecord] = (),
        specification_generation: int = 0,
        resumed_wait_condition_id: str | None = None,
        resumed_human_boundary_id: str | None = None,
        human_resolution_id: str | None = None,
        activity_id: str | None = None,
        attempt_id: str | None = None,
        candidate_id: str | None = None,
        forge_observation_id: str | None = None,
        selected_fallback: str | None = None,
        next_eligible_at_ms: int | None = None,
    ) -> RecoveryEvidenceRecord:
        """Insert one immutable source-unique Recovery Evidence record.

        Health observations are frozen in spec order regardless of caller order:
        ``(scope_kind, scope_id, health_sequence, health_observation_id)``.
        """
        with self.transaction():
            return self._create_recovery_evidence(
                recovery_evidence_id=recovery_evidence_id,
                run_id=run_id,
                source_kind=source_kind,
                source_id=source_id,
                category=category,
                failure_fingerprint=failure_fingerprint,
                strategy_index=strategy_index,
                selected_tactic=selected_tactic,
                attempt_count=attempt_count,
                repair_cycle_count=repair_cycle_count,
                diagnosis_count=diagnosis_count,
                rescue_epoch=rescue_epoch,
                health_observations=health_observations,
                specification_generation=specification_generation,
                resumed_wait_condition_id=resumed_wait_condition_id,
                resumed_human_boundary_id=resumed_human_boundary_id,
                human_resolution_id=human_resolution_id,
                activity_id=activity_id,
                attempt_id=attempt_id,
                candidate_id=candidate_id,
                forge_observation_id=forge_observation_id,
                selected_fallback=selected_fallback,
                next_eligible_at_ms=next_eligible_at_ms,
            )

    def _create_recovery_evidence(
        self,
        *,
        recovery_evidence_id: str,
        run_id: str,
        source_kind: str,
        source_id: str,
        category: str,
        failure_fingerprint: str,
        strategy_index: int,
        selected_tactic: str,
        attempt_count: int,
        repair_cycle_count: int,
        diagnosis_count: int,
        rescue_epoch: int,
        health_observations: Sequence[HealthObservationRecord] = (),
        specification_generation: int = 0,
        resumed_wait_condition_id: str | None = None,
        resumed_human_boundary_id: str | None = None,
        human_resolution_id: str | None = None,
        activity_id: str | None = None,
        attempt_id: str | None = None,
        candidate_id: str | None = None,
        forge_observation_id: str | None = None,
        selected_fallback: str | None = None,
        next_eligible_at_ms: int | None = None,
    ) -> RecoveryEvidenceRecord:
        """Must run inside a caller-held ``self.transaction()``."""
        require_lowercase_uuid(recovery_evidence_id, field="recovery_evidence_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("recovery_evidence.source_kind", source_kind)
        enums.parse_enum("recovery_evidence.category", category)
        enums.parse_enum("recovery_evidence.selected_tactic", selected_tactic)
        _require_digest(failure_fingerprint, field="failure_fingerprint")
        if (
            min(
                strategy_index,
                attempt_count,
                repair_cycle_count,
                diagnosis_count,
                rescue_epoch,
                specification_generation,
            )
            < 0
        ):
            raise ValueError("Recovery Evidence counters and generations must be nonnegative")
        if (resumed_human_boundary_id is None) != (human_resolution_id is None):
            raise ValueError("human boundary recovery requires both boundary and resolution ids")
        ordered_health = tuple(
            sorted(
                health_observations,
                key=lambda item: (
                    item.scope_kind,
                    item.scope_id,
                    item.health_sequence,
                    item.health_observation_id,
                ),
            )
        )
        health_ids = tuple(item.health_observation_id for item in ordered_health)
        if len(set(health_ids)) != len(health_ids):
            raise ValueError("health observation membership contains duplicate ids")
        health_digest = bare_canonical_digest(list(health_ids))

        existing = self.conn.execute(
            "SELECT * FROM recovery_evidence WHERE run_id = ? "
            "AND source_kind = ? AND source_id = ?",
            (run_id, source_kind, source_id),
        ).fetchone()
        if existing is not None:
            record = self.get_recovery_evidence(str(existing["recovery_evidence_id"]))
            assert record is not None
            if (
                record.recovery_evidence_id == recovery_evidence_id
                and record.category == category
                and record.failure_fingerprint == failure_fingerprint
                and record.strategy_index == strategy_index
                and record.selected_tactic == selected_tactic
                and record.attempt_count == attempt_count
                and record.repair_cycle_count == repair_cycle_count
                and record.diagnosis_count == diagnosis_count
                and record.rescue_epoch == rescue_epoch
                and record.resumed_wait_condition_id == resumed_wait_condition_id
                and record.resumed_human_boundary_id == resumed_human_boundary_id
                and record.human_resolution_id == human_resolution_id
                and record.activity_id == activity_id
                and record.attempt_id == attempt_id
                and record.candidate_id == candidate_id
                and record.forge_observation_id == forge_observation_id
                and record.selected_fallback == selected_fallback
                and record.health_observation_ids == health_ids
                and record.next_eligible_at_ms == next_eligible_at_ms
                and record.specification_generation == specification_generation
            ):
                return record
            raise IdempotencyConflictError(
                "recovery evidence source identity was reused with different content"
            )

        recovery_sequence = self._next_recovery_sequence(run_id)
        now = _now_ms()
        evidence_preimage = {
            "recovery_evidence_id": recovery_evidence_id,
            "run_id": run_id,
            "recovery_sequence": recovery_sequence,
            "source_kind": source_kind,
            "source_id": source_id,
            "resumed_wait_condition_id": resumed_wait_condition_id,
            "resumed_human_boundary_id": resumed_human_boundary_id,
            "human_resolution_id": human_resolution_id,
            "activity_id": activity_id,
            "attempt_id": attempt_id,
            "specification_generation": specification_generation,
            "candidate_id": candidate_id,
            "forge_observation_id": forge_observation_id,
            "category": category,
            "failure_fingerprint": failure_fingerprint,
            "strategy_index": strategy_index,
            "selected_tactic": selected_tactic,
            "attempt_count": attempt_count,
            "repair_cycle_count": repair_cycle_count,
            "diagnosis_count": diagnosis_count,
            "rescue_epoch": rescue_epoch,
            "selected_fallback": selected_fallback,
            "health_observation_ids_digest": health_digest,
            "next_eligible_at_ms": next_eligible_at_ms,
        }
        evidence_digest = recovery_evidence_digest(evidence_preimage)
        self.conn.execute(
            "INSERT INTO recovery_evidence(recovery_evidence_id, run_id, "
            "recovery_sequence, source_kind, source_id, resumed_wait_condition_id, "
            "resumed_human_boundary_id, human_resolution_id, activity_id, attempt_id, "
            "specification_generation, candidate_id, forge_observation_id, category, "
            "failure_fingerprint, strategy_index, selected_tactic, attempt_count, "
            "repair_cycle_count, diagnosis_count, rescue_epoch, selected_fallback, "
            "health_observation_ids_digest, next_eligible_at_ms, evidence_digest, "
            "recorded_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                recovery_evidence_id,
                run_id,
                recovery_sequence,
                source_kind,
                source_id,
                resumed_wait_condition_id,
                resumed_human_boundary_id,
                human_resolution_id,
                activity_id,
                attempt_id,
                specification_generation,
                candidate_id,
                forge_observation_id,
                category,
                failure_fingerprint,
                strategy_index,
                selected_tactic,
                attempt_count,
                repair_cycle_count,
                diagnosis_count,
                rescue_epoch,
                selected_fallback,
                health_digest,
                next_eligible_at_ms,
                evidence_digest,
                now,
            ),
        )
        for ordinal, observation in enumerate(ordered_health):
            self.conn.execute(
                "INSERT INTO recovery_evidence_health_observations("
                "recovery_evidence_id, observation_ordinal, health_observation_id) "
                "VALUES (?, ?, ?)",
                (recovery_evidence_id, ordinal, observation.health_observation_id),
            )
        self.conn.execute(
            "UPDATE runs SET current_recovery_evidence_id = ?, updated_at_ms = ? WHERE run_id = ?",
            (recovery_evidence_id, now, run_id),
        )
        row = self.conn.execute(
            "SELECT * FROM recovery_evidence WHERE recovery_evidence_id = ?",
            (recovery_evidence_id,),
        ).fetchone()
        assert row is not None
        return _row_to_recovery_evidence(row, health_observation_ids=health_ids)

    # -- Wait Condition ----------------------------------------------------

    def get_wait_condition(self, wait_condition_id: str) -> WaitConditionRecord | None:
        row = self.conn.execute(
            "SELECT * FROM wait_conditions WHERE wait_condition_id = ?", (wait_condition_id,)
        ).fetchone()
        if row is None:
            return None
        health_rows = self.conn.execute(
            "SELECT health_observation_id FROM wait_condition_health_observations "
            "WHERE wait_condition_id = ? ORDER BY observation_ordinal",
            (wait_condition_id,),
        ).fetchall()
        panel_rows = self.conn.execute(
            "SELECT * FROM wait_condition_panel_slots WHERE wait_condition_id = ? "
            "ORDER BY slot_ordinal",
            (wait_condition_id,),
        ).fetchall()
        return _row_to_wait_condition(
            row,
            health_observation_ids=tuple(
                str(item["health_observation_id"]) for item in health_rows
            ),
            panel_slots=tuple(
                WaitConditionPanelSlotRecord(
                    slot_ordinal=item["slot_ordinal"],
                    activity_id=item["activity_id"],
                    assignment_kind=item["assignment_kind"],
                    panel_round=item["panel_round"],
                    slot_id=item["slot_id"],
                )
                for item in panel_rows
            ),
        )

    def get_current_wait_condition(self, run_id: str) -> WaitConditionRecord | None:
        """The Run's live Wait, resolved through its durable pointer.

        A Wait Condition row is never mutated or deleted once created
        (domain-model.md "Wait Condition": "immutable history, current only
        through Run.wait_condition_id"); this is the only way to tell a
        still-open Wait from one a later wake or replan already closed.
        """
        from orcest.workflow_reducer.ledger import load_view

        view = load_view(self, run_id)
        if view is None or view.wait_condition_id is None:
            return None
        return self.get_wait_condition(view.wait_condition_id)

    def create_wait_condition(
        self,
        *,
        wait_condition_id: str,
        run_id: str,
        reason: str,
        resume_state: str,
        specification_generation: int,
        policy_hash: str,
        created_from_kind: str,
        created_from_id: str,
        created_transition_sequence: int,
        candidate_id: str | None = None,
        forge_observation_id: str | None = None,
        not_before_ms: int | None = None,
        wake_kind: str | None = None,
        wake_identity: Mapping[str, Any] | None = None,
        health_observations: Sequence[HealthObservationRecord] = (),
        panel_slots: Sequence[WaitConditionPanelSlotInput] = (),
    ) -> WaitConditionRecord:
        """Insert one immutable Wait Condition (or return the identical replay).

        Callers MUST hold the same writer transaction that already rechecked
        the wake predicate is not currently satisfied
        (persistence-and-recovery.md "WaitCondition" GUARDED paragraphs for
        Budget/Forge/Evidence/Secret/Capacity) -- this method freezes exactly
        what it is given and never itself recomputes or reverifies a
        predicate, so calling it unconditionally would risk creating a Wait
        that a concurrent wake already raced past.

        ``(created_from_kind, created_from_id)`` is the idempotent creation
        identity: replaying the same creating Transition/trigger returns the
        already-recorded Wait instead of a second row.
        """
        require_lowercase_uuid(wait_condition_id, field="wait_condition_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("run.state", resume_state)
        enums.parse_enum("wait_condition.created_from_kind", created_from_kind)
        _validate_wait_condition_shape(
            reason=reason,
            not_before_ms=not_before_ms,
            wake_kind=wake_kind,
            wake_identity=wake_identity,
        )
        if specification_generation < 0:
            raise ValueError("wait condition specification_generation must be nonnegative")

        ordered_health = tuple(
            sorted(
                health_observations,
                key=lambda item: (
                    item.scope_kind,
                    item.scope_id,
                    item.health_sequence,
                    item.health_observation_id,
                ),
            )
        )
        health_ids = tuple(item.health_observation_id for item in ordered_health)
        if len(set(health_ids)) != len(health_ids):
            raise ValueError("wait condition health observation membership contains duplicate ids")
        health_digest = bare_canonical_digest(list(health_ids))

        if panel_slots and reason != "CAPACITY":
            raise ValueError("only a CAPACITY wait condition may freeze panel-slot membership")
        ordered_panel = tuple(
            sorted(
                panel_slots, key=lambda item: (item.assignment_kind, item.panel_round, item.slot_id)
            )
        )
        panel_activity_ids = tuple(item.activity_id for item in ordered_panel)
        if len(set(panel_activity_ids)) != len(panel_activity_ids):
            raise ValueError("wait condition panel slot membership contains duplicate activities")
        if len(
            {(item.assignment_kind, item.panel_round, item.slot_id) for item in ordered_panel}
        ) != len(ordered_panel):
            raise ValueError(
                "wait condition panel slot membership contains a duplicate "
                "(assignment_kind, panel_round, slot_id)"
            )
        for slot in ordered_panel:
            activity = self.get_activity(slot.activity_id)
            if activity is None or activity.run_id != run_id:
                raise ValueError(
                    f"panel slot activity {slot.activity_id!r} is not owned by this run"
                )
            if activity.state != "PLANNED":
                raise ValueError(
                    f"panel slot activity {slot.activity_id!r} is not PLANNED "
                    f"(state={activity.state!r})"
                )
            live = self.conn.execute(
                "SELECT 1 FROM attempts WHERE activity_id = ? AND state IN ('OFFERED', 'CLAIMED')",
                (slot.activity_id,),
            ).fetchone()
            if live is not None:
                raise ValueError(
                    f"panel slot activity {slot.activity_id!r} still has a live "
                    "OFFERED/CLAIMED Attempt"
                )
        panel_preimage = [
            {
                "activity_id": item.activity_id,
                "assignment_kind": item.assignment_kind,
                "panel_round": item.panel_round,
                "slot_id": item.slot_id,
            }
            for item in ordered_panel
        ]
        panel_digest = bare_canonical_digest(panel_preimage)

        wake_identity_payload = dict(wake_identity) if wake_identity is not None else None
        wake_identity_json = (
            canonical_json_text(wake_identity_payload)
            if wake_identity_payload is not None
            else None
        )

        existing = self.conn.execute(
            "SELECT * FROM wait_conditions WHERE created_from_kind = ? AND created_from_id = ?",
            (created_from_kind, created_from_id),
        ).fetchone()
        if existing is not None:
            record = self.get_wait_condition(str(existing["wait_condition_id"]))
            assert record is not None
            same_panel = tuple(
                (item.activity_id, item.assignment_kind, item.panel_round, item.slot_id)
                for item in record.panel_slots
            ) == tuple(
                (item.activity_id, item.assignment_kind, item.panel_round, item.slot_id)
                for item in ordered_panel
            )
            if (
                record.wait_condition_id == wait_condition_id
                and record.run_id == run_id
                and record.reason == reason
                and record.resume_state == resume_state
                and record.specification_generation == specification_generation
                and record.policy_hash == policy_hash
                and record.candidate_id == candidate_id
                and record.forge_observation_id == forge_observation_id
                and record.not_before_ms == not_before_ms
                and record.wake_kind == wake_kind
                and record.wake_identity == wake_identity_payload
                and record.health_observation_ids == health_ids
                and same_panel
                and record.created_transition_sequence == created_transition_sequence
            ):
                return record
            raise IdempotencyConflictError(
                "wait condition creating Transition was reused with different content"
            )

        run_row = self.conn.execute(
            "SELECT wait_condition_id FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if run_row is None:
            raise RunStoreError(f"run {run_id!r} was not found")
        if run_row["wait_condition_id"] is not None:
            raise ValueError(
                f"run {run_id!r} already has a current wait condition "
                f"({run_row['wait_condition_id']!r}); at most one can be current"
            )

        condition_preimage = {
            "wait_condition_id": wait_condition_id,
            "run_id": run_id,
            "reason": reason,
            "resume_state": resume_state,
            "specification_generation": specification_generation,
            "policy_hash": policy_hash,
            "candidate_id": candidate_id,
            "forge_observation_id": forge_observation_id,
            "not_before_ms": not_before_ms,
            "wake_kind": wake_kind,
            "wake_identity": wake_identity_payload,
            "health_observation_ids_digest": health_digest,
            "panel_slots_digest": panel_digest,
            "created_from_kind": created_from_kind,
            "created_from_id": created_from_id,
            "created_transition_sequence": created_transition_sequence,
        }
        digest = wait_condition_digest(condition_preimage)
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO wait_conditions(wait_condition_id, run_id, reason, resume_state, "
            "specification_generation, candidate_id, policy_hash, forge_observation_id, "
            "not_before_ms, wake_kind, wake_identity_json, health_observation_ids_digest, "
            "panel_slots_digest, created_from_kind, created_from_id, condition_digest, "
            "created_transition_sequence, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                wait_condition_id,
                run_id,
                reason,
                resume_state,
                specification_generation,
                candidate_id,
                policy_hash,
                forge_observation_id,
                not_before_ms,
                wake_kind,
                wake_identity_json,
                health_digest,
                panel_digest,
                created_from_kind,
                created_from_id,
                digest,
                created_transition_sequence,
                now,
            ),
        )
        for ordinal, observation in enumerate(ordered_health):
            self.conn.execute(
                "INSERT INTO wait_condition_health_observations("
                "wait_condition_id, observation_ordinal, health_observation_id) "
                "VALUES (?, ?, ?)",
                (wait_condition_id, ordinal, observation.health_observation_id),
            )
        for ordinal, slot in enumerate(ordered_panel):
            self.conn.execute(
                "INSERT INTO wait_condition_panel_slots(wait_condition_id, slot_ordinal, "
                "activity_id, assignment_kind, panel_round, slot_id) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    wait_condition_id,
                    ordinal,
                    slot.activity_id,
                    slot.assignment_kind,
                    slot.panel_round,
                    slot.slot_id,
                ),
            )
        self.conn.execute(
            "UPDATE runs SET wait_condition_id = ?, updated_at_ms = ? WHERE run_id = ?",
            (wait_condition_id, now, run_id),
        )
        record = self.get_wait_condition(wait_condition_id)
        assert record is not None
        return record

    def _capacity_wait_satisfied(
        self, *, scope_kind: str, scope_id: str, now_ms: int
    ) -> WaitPredicateCheck:
        observation = self.get_latest_health_observation(scope_kind, scope_id, now_ms=now_ms)
        if observation is not None and observation.kind == "AVAILABLE":
            return WaitPredicateCheck(
                already_satisfied=True,
                satisfying_source_kind="HEALTH_OBSERVATION",
                satisfying_source_id=observation.health_observation_id,
            )
        return WaitPredicateCheck(already_satisfied=False)

    def _panel_wait_satisfied(
        self, slots: Sequence[WaitConditionPanelSlotInput], *, now_ms: int
    ) -> WaitPredicateCheck:
        """All-or-none: every named slot's Activity must itself still be
        unfilled and compatibly available, or the panel Wait stays open
        (domain-model.md "Wait Condition Panel Slot": "Partial staffing
        cannot commit or be reconstructed")."""
        if not slots:
            return WaitPredicateCheck(already_satisfied=True)
        for slot in slots:
            activity = self.get_activity(slot.activity_id)
            if activity is None or activity.state != "PLANNED":
                return WaitPredicateCheck(already_satisfied=False)
            if not self._activity_has_compatible_capacity(activity, now_ms=now_ms):
                return WaitPredicateCheck(already_satisfied=False)
        return WaitPredicateCheck(
            already_satisfied=True, satisfying_source_kind="HEALTH_OBSERVATION"
        )

    def _budget_wait_satisfied(
        self, wake_identity: Mapping[str, Any], *, now_ms: int
    ) -> WaitPredicateCheck:
        project_id = str(wake_identity["project_id"])
        accounting_scope_id = str(wake_identity["accounting_scope_id"])
        minimum_source_sequence = int(wake_identity["minimum_source_sequence"])
        row = self.conn.execute(
            "SELECT * FROM budget_reports WHERE project_id = ? AND accounting_scope_id = ? "
            "ORDER BY source_sequence DESC LIMIT 1",
            (project_id, accounting_scope_id),
        ).fetchone()
        if (
            row is not None
            and row["availability"] == "AVAILABLE"
            and int(row["source_sequence"]) >= minimum_source_sequence
        ):
            return WaitPredicateCheck(
                already_satisfied=True,
                satisfying_source_kind="BUDGET_REPORT",
                satisfying_source_id=row["budget_report_id"],
            )
        return WaitPredicateCheck(already_satisfied=False)

    def _secret_recovery_wait_satisfied(
        self, wake_identity: Mapping[str, Any]
    ) -> WaitPredicateCheck:
        secret_id = str(wake_identity["secret_id"])
        minimum_version = int(wake_identity["minimum_version"])
        projection = self.get_secret_current_version(secret_id)
        if projection is not None and projection.current_version >= minimum_version:
            return WaitPredicateCheck(
                already_satisfied=True,
                satisfying_source_kind="SECRET_VERSION",
                satisfying_source_id=f"{secret_id}/{projection.current_version}",
            )
        return WaitPredicateCheck(already_satisfied=False)

    def _external_dependency_wait_satisfied(
        self, wake_identity: Mapping[str, Any]
    ) -> WaitPredicateCheck:
        project_id = str(wake_identity["project_id"])
        target_kind = str(wake_identity["target_kind"])
        target_id = str(wake_identity["target_id"])
        minimum_observation_sequence = int(wake_identity["minimum_observation_sequence"])
        row = self.conn.execute(
            "SELECT * FROM forge_observations WHERE project_id = ? AND target_kind = ? "
            "AND target_id = ? AND kind = 'DEPENDENCY_STATE' AND observation_sequence >= ? "
            "ORDER BY observation_sequence DESC LIMIT 1",
            (project_id, target_kind, target_id, minimum_observation_sequence),
        ).fetchone()
        if row is not None:
            fact = json.loads(row["fact_json"])
            if fact.get("satisfied"):
                return WaitPredicateCheck(
                    already_satisfied=True,
                    satisfying_source_kind="FORGE_OBSERVATION",
                    satisfying_source_id=row["forge_observation_id"],
                )
        return WaitPredicateCheck(already_satisfied=False)

    def _timer_wait_satisfied(
        self, not_before_ms: int | None, *, now_ms: int
    ) -> WaitPredicateCheck:
        if not_before_ms is not None and now_ms >= not_before_ms:
            return WaitPredicateCheck(already_satisfied=True, satisfying_source_kind="TIMER_FACT")
        return WaitPredicateCheck(already_satisfied=False)

    def submit_recovery_evidence(
        self,
        *,
        recovery_evidence_id: str,
        run_id: str,
        source_kind: str,
        source_id: str,
        category: str | None = None,
        facts: Mapping[str, Any] | None = None,
        activity_id: str | None = None,
        attempt_id: str | None = None,
        candidate_id: str | None = None,
        forge_observation_id: str | None = None,
        health_observations: Sequence[HealthObservationRecord] = (),
        fallback_order: Sequence[str] = (),
        exhausted_autonomous: bool = False,
        resumed_wait_condition_id: str | None = None,
        resumed_human_boundary_id: str | None = None,
        human_resolution_id: str | None = None,
        provider_retry_after_ms: int | None = None,
        bounded_evidence: Mapping[str, Any] | None = None,
        capacity_wake_identity: Mapping[str, str] | None = None,
        panel_wait_slots: Sequence[WaitConditionPanelSlotInput] = (),
        budget_wake_identity: Mapping[str, Any] | None = None,
        external_wake_identity: Mapping[str, Any] | None = None,
        accepted_at_ms: int | None = None,
        human_boundary_reason: str | None = None,
        human_boundary_minimum_request: str | None = None,
        human_boundary_choice_consequences: Mapping[str, str] | None = None,
        human_boundary_evidence_refs: Sequence[str] = (),
        human_boundary_attempted_strategy_digests: Sequence[str] = (),
    ) -> RecoveryEvidenceOutcome:
        """Classify one accepted failure, persist its Recovery Evidence,
        apply the deterministic Transition it selects, and -- exactly when
        that selects a ``WAIT_*`` tactic -- either durably freeze the Wait it
        names or, when the writer-lock recheck finds the predicate already
        met, skip the Wait entirely and return straight to the recovery
        origin state (workflow-lifecycle.md/persistence-and-recovery.md
        "Wait Condition"). The whole classify-recheck-insert sequence runs in
        one writer transaction so a wake racing the insertion cannot be lost.
        """
        from orcest.workflow_reducer.ledger import apply, load_view
        from orcest.workflow_reducer.recovery import (
            DEFAULT_RECOVERY_LIMITS,
            HealthObservationRef,
            RecoveryEvidenceInput,
            classify_recovery_category,
            select_recovery_decision,
        )
        from orcest.workflow_reducer.types import Trigger

        require_lowercase_uuid(recovery_evidence_id, field="recovery_evidence_id")
        require_lowercase_uuid(run_id, field="run_id")
        resolved_facts = dict(facts or {})
        now = _now_ms() if accepted_at_ms is None else accepted_at_ms

        with self.transaction():
            view = load_view(self, run_id)
            if view is None or view.state != "RECOVERING":
                raise RunStoreError(
                    f"run {run_id!r} is not RECOVERING; cannot submit recovery evidence"
                )

            prior = (
                self.get_recovery_evidence(view.current_recovery_evidence_id)
                if view.current_recovery_evidence_id is not None
                else None
            )
            resolved_category = (
                category
                if category is not None
                else classify_recovery_category(source_kind, resolved_facts)
            )
            resolved_activity_id = (
                activity_id if activity_id is not None else view.recovery_activity_id
            )
            resolved_candidate_id = (
                candidate_id if candidate_id is not None else view.current_candidate_id
            )
            evidence_input = RecoveryEvidenceInput(
                source_kind=source_kind,
                source_id=source_id,
                category=resolved_category,
                activity_id=resolved_activity_id,
                attempt_id=attempt_id,
                specification_generation=view.specification_generation,
                candidate_id=resolved_candidate_id,
                forge_observation_id=forge_observation_id,
                failure_scope=resolved_facts.get("failure_scope", {}),
                bounded_evidence=bounded_evidence or {},
                prior_attempt_count=prior.attempt_count if prior is not None else 0,
                prior_repair_cycle_count=prior.repair_cycle_count if prior is not None else 0,
                prior_diagnosis_count=prior.diagnosis_count if prior is not None else 0,
                rescue_epoch=prior.rescue_epoch if prior is not None else 0,
                accepted_at_ms=now,
                provider_retry_after_ms=provider_retry_after_ms,
                fallback_order=tuple(fallback_order),
                exhausted_autonomous=exhausted_autonomous,
                resumed_wait_condition_id=resumed_wait_condition_id,
                resumed_human_boundary_id=resumed_human_boundary_id,
                human_resolution_id=human_resolution_id,
                human_boundary_reason=human_boundary_reason,
            )
            health_refs = tuple(
                HealthObservationRef(
                    health_observation_id=item.health_observation_id,
                    scope_kind=item.scope_kind,
                    scope_id=item.scope_id,
                    health_sequence=item.health_sequence,
                )
                for item in health_observations
            )
            decision = select_recovery_decision(
                evidence_input, health_observations=health_refs, limits=DEFAULT_RECOVERY_LIMITS
            )

            predicate_check: WaitPredicateCheck | None = None
            wait_wake_kind: str | None = None
            wait_wake_identity: Mapping[str, Any] | None = None
            wait_not_before_ms: int | None = None
            resolved_panel_slots: tuple[WaitConditionPanelSlotInput, ...] = ()

            if decision.selected_tactic == "WAIT_CAPACITY":
                if panel_wait_slots:
                    # Re-derive against current durable state under the writer
                    # lock: a named slot may have raced to a live
                    # OFFERED/CLAIMED Attempt (activity no longer PLANNED)
                    # since the caller gathered panel_wait_slots. Such a slot
                    # is no longer "still unfilled" and must be dropped here
                    # rather than freed into create_wait_condition's
                    # panel-slot membership, which rejects anything but a
                    # PLANNED activity with no live OFFERED/CLAIMED Attempt.
                    resolved_panel_slots = tuple(
                        slot
                        for slot in panel_wait_slots
                        if (activity := self.get_activity(slot.activity_id)) is not None
                        and activity.state == "PLANNED"
                    )
                    predicate_check = self._panel_wait_satisfied(resolved_panel_slots, now_ms=now)
                    wait_wake_kind = "CAPACITY"
                    wait_wake_identity = {
                        "assignment_kind": panel_wait_slots[0].assignment_kind,
                        "panel_round": panel_wait_slots[0].panel_round,
                    }
                else:
                    if capacity_wake_identity is None:
                        raise ValueError("WAIT_CAPACITY requires capacity_wake_identity")
                    predicate_check = self._capacity_wait_satisfied(
                        scope_kind=str(capacity_wake_identity["scope_kind"]),
                        scope_id=str(capacity_wake_identity["scope_id"]),
                        now_ms=now,
                    )
                    wait_wake_kind = "CAPACITY"
                    wait_wake_identity = dict(capacity_wake_identity)
            elif decision.selected_tactic == "WAIT_BUDGET":
                if budget_wake_identity is None:
                    raise ValueError("WAIT_BUDGET requires budget_wake_identity")
                predicate_check = self._budget_wait_satisfied(budget_wake_identity, now_ms=now)
                wait_wake_kind = "BUDGET_WINDOW"
                wait_wake_identity = dict(budget_wake_identity)
                wait_not_before_ms = int(budget_wake_identity["reset_at_ms"])
            elif decision.selected_tactic in {"WAIT_BACKOFF", "WAIT_RATE_LIMIT"}:
                wait_not_before_ms = decision.next_eligible_at_ms
                predicate_check = self._timer_wait_satisfied(wait_not_before_ms, now_ms=now)
            elif decision.selected_tactic == "WAIT_EXTERNAL":
                reason = _external_wait_reason(decision.category)
                if external_wake_identity is None:
                    raise ValueError(f"WAIT_EXTERNAL/{reason} requires external_wake_identity")
                if reason == "SECRET_RECOVERY":
                    predicate_check = self._secret_recovery_wait_satisfied(external_wake_identity)
                    wait_wake_kind = "SECRET"
                    wait_wake_identity = dict(external_wake_identity)
                elif reason == "EXTERNAL_DEPENDENCY":
                    predicate_check = self._external_dependency_wait_satisfied(
                        external_wake_identity
                    )
                    wait_wake_kind = "DEPENDENCY"
                    wait_wake_identity = dict(external_wake_identity)
                else:
                    # FORGE_UNAVAILABLE / STORAGE_RECOVERY: no automatic
                    # recheck source is wired yet (the health-probe leaf
                    # this reuses has not landed); the Wait is always
                    # created and a future health-probe leaf wakes it.
                    predicate_check = WaitPredicateCheck(already_satisfied=False)
                    wait_wake_kind = "FORGE" if reason == "FORGE_UNAVAILABLE" else "STORAGE"
                    wait_wake_identity = dict(external_wake_identity)
                    if reason == "FORGE_UNAVAILABLE":
                        wait_not_before_ms = int(external_wake_identity["not_before_ms"])
            elif decision.selected_tactic == "WAIT_EVIDENCE":
                if external_wake_identity is None:
                    raise ValueError("WAIT_EVIDENCE requires external_wake_identity")
                wait_wake_kind = "EVIDENCE"
                wait_wake_identity = dict(external_wake_identity)
                wait_not_before_ms = int(external_wake_identity["not_before_ms"])
                # Evidence membership/predicate revalidation is owned by the
                # caller (it alone knows the applicable Candidate/panel/
                # dispute/specification/policy/Change-Request bindings this
                # predicate_digest covers); this leaf only freezes the Wait.
                predicate_check = WaitPredicateCheck(already_satisfied=False)

            predicate_already_met = bool(
                predicate_check is not None and predicate_check.already_satisfied
            )
            pending_wait_condition_id = (
                str(uuid.uuid4())
                if predicate_check is not None and not predicate_already_met
                else None
            )
            pending_human_boundary_id = (
                str(uuid.uuid4()) if decision.selected_tactic == "ENTER_HUMAN_BOUNDARY" else None
            )

            evidence = self._create_recovery_evidence(
                recovery_evidence_id=recovery_evidence_id,
                run_id=run_id,
                source_kind=source_kind,
                source_id=source_id,
                category=decision.category,
                failure_fingerprint=decision.failure_fingerprint,
                strategy_index=decision.strategy_index,
                selected_tactic=decision.selected_tactic,
                attempt_count=decision.attempt_count,
                repair_cycle_count=decision.repair_cycle_count,
                diagnosis_count=decision.diagnosis_count,
                rescue_epoch=decision.rescue_epoch,
                health_observations=health_observations,
                specification_generation=view.specification_generation,
                resumed_wait_condition_id=resumed_wait_condition_id,
                resumed_human_boundary_id=resumed_human_boundary_id,
                human_resolution_id=human_resolution_id,
                activity_id=resolved_activity_id,
                attempt_id=attempt_id,
                candidate_id=resolved_candidate_id,
                forge_observation_id=forge_observation_id,
                selected_fallback=decision.selected_fallback,
                next_eligible_at_ms=decision.next_eligible_at_ms,
            )

            trigger_facts: dict[str, Any] = {
                **resolved_facts,
                "source_kind": source_kind,
                "source_id": source_id,
                "category": decision.category,
                "activity_id": resolved_activity_id,
                "attempt_id": attempt_id,
                "specification_generation": view.specification_generation,
                "candidate_id": resolved_candidate_id,
                "forge_observation_id": forge_observation_id,
                "prior_attempt_count": evidence_input.prior_attempt_count,
                "prior_repair_cycle_count": evidence_input.prior_repair_cycle_count,
                "prior_diagnosis_count": evidence_input.prior_diagnosis_count,
                "rescue_epoch": evidence_input.rescue_epoch,
                "accepted_at_ms": now,
                "provider_retry_after_ms": provider_retry_after_ms,
                "fallback_order": tuple(fallback_order),
                "exhausted_autonomous": exhausted_autonomous,
                "resumed_wait_condition_id": resumed_wait_condition_id,
                "resumed_human_boundary_id": resumed_human_boundary_id,
                "human_resolution_id": human_resolution_id,
                "selected_tactic": decision.selected_tactic,
                "health_observations": [
                    {
                        "health_observation_id": item.health_observation_id,
                        "scope_kind": item.scope_kind,
                        "scope_id": item.scope_id,
                        "health_sequence": item.health_sequence,
                    }
                    for item in health_observations
                ],
                "pending_wait_condition_id": pending_wait_condition_id,
                "human_boundary_reason": human_boundary_reason,
                "pending_human_boundary_id": pending_human_boundary_id,
            }
            if predicate_check is not None:
                trigger_facts["predicate_already_met"] = predicate_already_met

            applied = apply(
                self,
                view,
                Trigger(
                    kind="RECOVERY_EVIDENCE", trigger_id=recovery_evidence_id, facts=trigger_facts
                ),
                run_id=run_id,
            )

            wait_condition: WaitConditionRecord | None = None
            if (
                not applied.replayed
                and applied.reduction is not None
                and applied.reduction.next_state == "WAITING"
                and pending_wait_condition_id is not None
            ):
                resume_state = view.recovery_origin_state or "PLANNING"
                wait_condition = self.create_wait_condition(
                    wait_condition_id=pending_wait_condition_id,
                    run_id=run_id,
                    reason=str(applied.reduction.pointer_updates.get("wait_reason")),
                    resume_state=resume_state,
                    specification_generation=applied.reduction.specification_generation,
                    policy_hash=view.policy_hash,
                    created_from_kind="RECOVERY_EVIDENCE",
                    created_from_id=recovery_evidence_id,
                    created_transition_sequence=applied.transition.transition_sequence,
                    candidate_id=resolved_candidate_id,
                    forge_observation_id=forge_observation_id,
                    not_before_ms=wait_not_before_ms,
                    wake_kind=wait_wake_kind,
                    wake_identity=wait_wake_identity,
                    health_observations=health_observations,
                    panel_slots=resolved_panel_slots,
                )

            human_boundary: HumanBoundaryRecord | None = None
            if (
                not applied.replayed
                and applied.reduction is not None
                and applied.reduction.next_state == "NEEDS_HUMAN"
                and decision.selected_tactic == "ENTER_HUMAN_BOUNDARY"
            ):
                if human_boundary_minimum_request is None:
                    raise ValueError("ENTER_HUMAN_BOUNDARY requires human_boundary_minimum_request")
                assert decision.human_boundary_reason is not None
                assert pending_human_boundary_id is not None
                resume_state = view.recovery_origin_state or "PLANNING"
                human_boundary = self.create_human_boundary(
                    human_boundary_id=pending_human_boundary_id,
                    run_id=run_id,
                    reason=decision.human_boundary_reason,
                    resume_state=resume_state,
                    minimum_request=human_boundary_minimum_request,
                    created_from_kind="RECOVERY_EVIDENCE",
                    created_from_id=recovery_evidence_id,
                    created_transition_sequence=applied.transition.transition_sequence,
                    evidence_refs=human_boundary_evidence_refs,
                    attempted_strategy_digests=human_boundary_attempted_strategy_digests,
                    choice_consequences=human_boundary_choice_consequences,
                    specification_generation=view.specification_generation,
                    candidate_id=resolved_candidate_id,
                    policy_hash=view.policy_hash,
                    forge_observation_id=forge_observation_id,
                )

            return RecoveryEvidenceOutcome(
                recovery_evidence=evidence,
                applied=applied,
                selected_tactic=decision.selected_tactic,
                wait_condition=wait_condition,
                predicate_check=predicate_check,
                human_boundary=human_boundary,
            )

    # -- Human Boundary / Human Resolution ----------------------------------

    def get_human_boundary(self, human_boundary_id: str) -> HumanBoundaryRecord | None:
        row = self.conn.execute(
            "SELECT * FROM human_boundaries WHERE human_boundary_id = ?", (human_boundary_id,)
        ).fetchone()
        return None if row is None else self._load_human_boundary(row)

    def get_current_human_boundary(self, run_id: str) -> HumanBoundaryRecord | None:
        row = self.conn.execute(
            "SELECT human_boundary_id FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None or row["human_boundary_id"] is None:
            return None
        return self.get_human_boundary(str(row["human_boundary_id"]))

    def _load_human_boundary(self, row: sqlite3.Row) -> HumanBoundaryRecord:
        choice_rows = self.conn.execute(
            "SELECT * FROM human_boundary_choices WHERE human_boundary_id = ? "
            "ORDER BY choice_ordinal",
            (row["human_boundary_id"],),
        ).fetchall()
        return _row_to_human_boundary(
            row,
            evidence_refs=json.loads(row["evidence_refs_json"]),
            attempted_strategy_digests=json.loads(row["attempted_strategy_digests_json"]),
            required_resolution_kinds=json.loads(row["required_resolution_kinds_json"]),
            choices=[
                HumanBoundaryChoice(
                    choice_id=item["choice_id"],
                    resolution_kind=item["resolution_kind"],
                    consequence=item["consequence"],
                )
                for item in choice_rows
            ],
        )

    def create_human_boundary(
        self,
        *,
        human_boundary_id: str,
        run_id: str,
        reason: str,
        resume_state: str,
        minimum_request: str,
        created_from_kind: str,
        created_from_id: str,
        created_transition_sequence: int,
        evidence_refs: Sequence[str] = (),
        attempted_strategy_digests: Sequence[str] = (),
        choice_consequences: Mapping[str, str] | None = None,
        specification_generation: int | None = None,
        candidate_id: str | None = None,
        policy_hash: str | None = None,
        forge_observation_id: str | None = None,
        publication_id: str | None = None,
        publication_effect_generation: int | None = None,
        ownership_project_id: str | None = None,
        ownership_deterministic_ref: str | None = None,
        ownership_change_request_external_id: str | None = None,
        ownership_run_marker: str | None = None,
    ) -> HumanBoundaryRecord:
        """Insert one immutable Human Boundary decision packet (domain-model.md
        "Human Boundary"), or return the identical replay keyed by its
        creating ``(created_from_kind, created_from_id)``.

        Every worker/deadline/health/forge/policy/storage/secret problem must
        first traverse autonomous recovery and can create a boundary only
        through its Recovery Evidence; the sole direct exception is the
        positive ownership-conflict Reconciliation Fact. ``reason``,
        ``required_resolution_kinds``, and (for the ownership reason) the
        single fixed choice are code-owned here, never caller-widened.
        """
        from orcest.workflow_reducer.human_boundary import (
            MAX_BOUNDED_ENTRIES,
            MAX_PROSE_LENGTH,
            OWNERSHIP_CHOICE_ID,
            required_resolution_kinds as _required_resolution_kinds,
        )

        require_lowercase_uuid(human_boundary_id, field="human_boundary_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("human_boundary.reason", reason)
        enums.parse_enum("run.state", resume_state)
        enums.parse_enum("human_boundary.created_from_kind", created_from_kind)
        if created_transition_sequence <= 0:
            raise ValueError("created_transition_sequence must be positive")

        is_ownership = reason == "PUBLICATION_OWNERSHIP_CONFLICT"
        if is_ownership != (created_from_kind == "RECONCILIATION_FACT"):
            raise ValueError(
                "PUBLICATION_OWNERSHIP_CONFLICT is the sole direct Reconciliation Fact "
                "path; every other reason must be sourced from Recovery Evidence"
            )
        ownership_fields = (
            ownership_project_id,
            ownership_deterministic_ref,
            ownership_change_request_external_id,
            ownership_run_marker,
        )
        if is_ownership:
            if any(field is None for field in ownership_fields):
                raise ValueError(
                    "PUBLICATION_OWNERSHIP_CONFLICT requires all four ownership bindings"
                )
        elif any(field is not None for field in ownership_fields):
            raise ValueError("ownership bindings are only valid for PUBLICATION_OWNERSHIP_CONFLICT")

        if not minimum_request.strip():
            raise ValueError("minimum_request must not be blank")
        if len(minimum_request) > MAX_PROSE_LENGTH:
            raise ValueError(f"minimum_request exceeds {MAX_PROSE_LENGTH} scalars")

        ordered_evidence_refs = tuple(dict.fromkeys(evidence_refs))
        if len(ordered_evidence_refs) > MAX_BOUNDED_ENTRIES:
            raise ValueError(f"evidence_refs exceeds {MAX_BOUNDED_ENTRIES} entries")
        ordered_strategy_digests = tuple(dict.fromkeys(attempted_strategy_digests))
        if len(ordered_strategy_digests) > MAX_BOUNDED_ENTRIES:
            raise ValueError(f"attempted_strategy_digests exceeds {MAX_BOUNDED_ENTRIES} entries")

        kinds = _required_resolution_kinds(reason)

        choices: tuple[HumanBoundaryChoice, ...]
        if is_ownership:
            if choice_consequences:
                raise ValueError(
                    "PUBLICATION_OWNERSHIP_CONFLICT choices are fixed and cannot be overridden"
                )
            choices = (
                HumanBoundaryChoice(
                    choice_id=OWNERSHIP_CHOICE_ID,
                    resolution_kind="PUBLICATION_OWNERSHIP_RESOLVED",
                    consequence="Continue Orcest v1 ownership of this Change Request.",
                ),
            )
        else:
            consequences = dict(choice_consequences or {})
            if set(consequences) != set(kinds):
                raise ValueError(
                    f"choice_consequences must supply exactly {list(kinds)}, "
                    f"got {sorted(consequences)}"
                )
            for kind, consequence in consequences.items():
                if not consequence.strip():
                    raise ValueError(f"choice consequence for {kind} must not be blank")
                if len(consequence) > MAX_PROSE_LENGTH:
                    raise ValueError(
                        f"choice consequence for {kind} exceeds {MAX_PROSE_LENGTH} scalars"
                    )
            choices = tuple(
                HumanBoundaryChoice(
                    choice_id=f"resolve-{kind.lower().replace('_', '-')}",
                    resolution_kind=kind,
                    consequence=consequences[kind],
                )
                for kind in kinds
            )

        existing = self.conn.execute(
            "SELECT * FROM human_boundaries WHERE created_from_kind = ? AND created_from_id = ?",
            (created_from_kind, created_from_id),
        ).fetchone()
        if existing is not None:
            record = self._load_human_boundary(existing)
            if (
                record.human_boundary_id == human_boundary_id
                and record.run_id == run_id
                and record.reason == reason
                and record.resume_state == resume_state
                and record.minimum_request == minimum_request
                and record.evidence_refs == ordered_evidence_refs
                and record.attempted_strategy_digests == ordered_strategy_digests
                and record.required_resolution_kinds == kinds
                and record.choices == choices
                and record.specification_generation == specification_generation
                and record.candidate_id == candidate_id
                and record.policy_hash == policy_hash
                and record.forge_observation_id == forge_observation_id
                and record.publication_id == publication_id
                and record.publication_effect_generation == publication_effect_generation
                and record.ownership_project_id == ownership_project_id
                and record.ownership_deterministic_ref == ownership_deterministic_ref
                and record.ownership_change_request_external_id
                == ownership_change_request_external_id
                and record.ownership_run_marker == ownership_run_marker
                and record.created_transition_sequence == created_transition_sequence
            ):
                return record
            raise IdempotencyConflictError(
                "human boundary creating Transition was reused with different content"
            )

        run_row = self.conn.execute(
            "SELECT human_boundary_id FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if run_row is None:
            raise RunStoreError(f"run {run_id!r} was not found")
        if run_row["human_boundary_id"] is not None:
            raise ValueError(
                f"run {run_id!r} already has a current human boundary "
                f"({run_row['human_boundary_id']!r}); at most one can be current"
            )

        packet_preimage = {
            "human_boundary_id": human_boundary_id,
            "run_id": run_id,
            "reason": reason,
            "resume_state": resume_state,
            "specification_generation": specification_generation,
            "candidate_id": candidate_id,
            "policy_hash": policy_hash,
            "forge_observation_id": forge_observation_id,
            "publication_id": publication_id,
            "publication_effect_generation": publication_effect_generation,
            "ownership_project_id": ownership_project_id,
            "ownership_deterministic_ref": ownership_deterministic_ref,
            "ownership_change_request_external_id": ownership_change_request_external_id,
            "ownership_run_marker": ownership_run_marker,
            "minimum_request": minimum_request,
            "evidence_refs": list(ordered_evidence_refs),
            "attempted_strategy_digests": list(ordered_strategy_digests),
            "required_resolution_kinds": list(kinds),
            "choices": [
                {
                    "choice_id": choice.choice_id,
                    "resolution_kind": choice.resolution_kind,
                    "consequence": choice.consequence,
                }
                for choice in choices
            ],
            "created_from_kind": created_from_kind,
            "created_from_id": created_from_id,
            "created_transition_sequence": created_transition_sequence,
        }
        digest = human_boundary_digest(packet_preimage)
        now = _now_ms()
        columns = [
            "human_boundary_id",
            "run_id",
            "reason",
            "resume_state",
            "specification_generation",
            "candidate_id",
            "policy_hash",
            "forge_observation_id",
            "publication_id",
            "publication_effect_generation",
            "ownership_project_id",
            "ownership_deterministic_ref",
            "ownership_change_request_external_id",
            "ownership_run_marker",
            "minimum_request",
            "evidence_refs_json",
            "attempted_strategy_digests_json",
            "required_resolution_kinds_json",
            "created_from_kind",
            "created_from_id",
            "packet_digest",
            "created_transition_sequence",
            "created_at_ms",
        ]
        values = (
            human_boundary_id,
            run_id,
            reason,
            resume_state,
            specification_generation,
            candidate_id,
            policy_hash,
            forge_observation_id,
            publication_id,
            publication_effect_generation,
            ownership_project_id,
            ownership_deterministic_ref,
            ownership_change_request_external_id,
            ownership_run_marker,
            minimum_request,
            canonical_json_text(list(ordered_evidence_refs)),
            canonical_json_text(list(ordered_strategy_digests)),
            canonical_json_text(list(kinds)),
            created_from_kind,
            created_from_id,
            digest,
            created_transition_sequence,
            now,
        )
        self.conn.execute(
            f"INSERT INTO human_boundaries({', '.join(columns)}) "
            f"VALUES ({', '.join(['?'] * len(columns))})",
            values,
        )
        for ordinal, choice in enumerate(choices):
            self.conn.execute(
                "INSERT INTO human_boundary_choices(human_boundary_id, choice_ordinal, "
                "choice_id, resolution_kind, consequence) VALUES (?, ?, ?, ?, ?)",
                (
                    human_boundary_id,
                    ordinal,
                    choice.choice_id,
                    choice.resolution_kind,
                    choice.consequence,
                ),
            )
        self.conn.execute(
            "UPDATE runs SET human_boundary_id = ?, updated_at_ms = ? WHERE run_id = ?",
            (human_boundary_id, now, run_id),
        )
        created = self.get_human_boundary(human_boundary_id)
        assert created is not None
        return created

    def get_human_resolution(self, human_resolution_id: str) -> HumanResolutionRecord | None:
        row = self.conn.execute(
            "SELECT * FROM human_resolutions WHERE human_resolution_id = ?",
            (human_resolution_id,),
        ).fetchone()
        return None if row is None else _row_to_human_resolution(row)

    def get_human_resolution_for_boundary(
        self, human_boundary_id: str
    ) -> HumanResolutionRecord | None:
        row = self.conn.execute(
            "SELECT * FROM human_resolutions WHERE human_boundary_id = ?", (human_boundary_id,)
        ).fetchone()
        return None if row is None else _row_to_human_resolution(row)

    def submit_human_resolution(
        self,
        *,
        human_resolution_id: str,
        human_boundary_id: str,
        run_id: str,
        source_kind: str,
        source_id: str,
        authenticated_principal_id: str,
        resolution_kind: str,
        resolution: Mapping[str, Any],
        accepted_at_ms: int | None = None,
    ) -> HumanResolutionOutcome:
        """Validate and persist one closed Human Resolution for the exact
        current Human Boundary, then apply the Transition it resumes
        (workflow-lifecycle.md "Resumption").

        Only ``MANAGEMENT_COMMAND``/``SECRET_VERSION``/``STORAGE_RESTORATION``
        sources are driven through the ledger here. ``SPECIFICATION_AMENDED``'s
        ``FORGE_OBSERVATION``-sourced Snapshot capture and its separate
        ``SPEC_SUPERSEDE`` continuation belong to the replanning leaf: "A
        Management Command cannot synthesize SPECIFICATION_AMENDED" is
        enforced below, and callers must drive that resolution kind through
        that leaf's own transaction instead of this method.

        Idempotent by ``(source_kind, source_id)`` -- an identical replay
        (same boundary, resolution kind, payload, principal) returns the
        original record and re-applies the already-committed Transition via
        the ledger's own trigger-identity replay; a reused source identity
        with different content is an integrity conflict; a fresh acceptance
        requires ``human_boundary_id`` still be this Run's current boundary.
        """
        from orcest.workflow_reducer.human_boundary import (
            SECRET_STORE_VERIFIER_PRINCIPAL_ID,
            resolution_source_kinds,
            validate_resolution_payload,
        )
        from orcest.workflow_reducer.ledger import apply, load_view
        from orcest.workflow_reducer.types import Trigger

        require_lowercase_uuid(human_resolution_id, field="human_resolution_id")
        require_lowercase_uuid(human_boundary_id, field="human_boundary_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("human_resolution.source_kind", source_kind)
        enums.parse_enum("human_resolution.resolution_kind", resolution_kind)
        if not authenticated_principal_id.strip():
            raise ValueError("authenticated_principal_id is required")
        if source_kind not in resolution_source_kinds(resolution_kind):
            raise ValueError(
                f"{resolution_kind} cannot be sourced from {source_kind}; a generic "
                "source may never synthesize a resolution kind reserved for another"
            )
        if source_kind not in {"MANAGEMENT_COMMAND", "SECRET_VERSION", "STORAGE_RESTORATION"}:
            raise ValueError(
                f"{source_kind} resolutions are applied by their own leaf's Transition "
                "(e.g. FORGE_OBSERVATION/SPEC_SUPERSEDE for SPECIFICATION_AMENDED), "
                "not submit_human_resolution"
            )
        validate_resolution_payload(resolution_kind, resolution)
        if source_kind == "SECRET_VERSION":
            parts = source_id.split(":")
            if (
                len(parts) != 2
                or not is_lowercase_uuid(parts[0])
                or not parts[1].isdigit()
                or parts[1] != str(int(parts[1]))
            ):
                raise ValueError(
                    "SECRET_VERSION source_id must be the canonical "
                    "'<lowercase-uuid>:<base-10-version>' composite key"
                )
            if authenticated_principal_id != SECRET_STORE_VERIFIER_PRINCIPAL_ID:
                raise ValueError(
                    "an automatic SECRET_OR_PERMISSION_PROVIDED resolution from a Secret "
                    "Version must be authenticated as the registered Secret-Store "
                    "verifier/reconciler service principal"
                )
        else:
            require_lowercase_uuid(source_id, field="source_id")

        now = _now_ms() if accepted_at_ms is None else accepted_at_ms
        resolution_payload = dict(resolution)

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM human_resolutions WHERE source_kind = ? AND idempotency_key = ?",
                (source_kind, source_id),
            ).fetchone()

            boundary = self.get_human_boundary(human_boundary_id)
            if boundary is None:
                raise RunStoreError(f"human boundary {human_boundary_id!r} was not found")
            if boundary.run_id != run_id:
                raise ValueError("human boundary does not belong to this run")

            if existing is not None:
                record = _row_to_human_resolution(existing)
                if not (
                    record.human_resolution_id == human_resolution_id
                    and record.human_boundary_id == human_boundary_id
                    and record.run_id == run_id
                    and record.authenticated_principal_id == authenticated_principal_id
                    and record.resolution_kind == resolution_kind
                    and record.resolution == resolution_payload
                ):
                    raise IdempotencyConflictError(
                        "human resolution source identity was reused with different content"
                    )
            else:
                run_row = self.conn.execute(
                    "SELECT human_boundary_id FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if run_row is None:
                    raise RunStoreError(f"run {run_id!r} was not found")
                if run_row["human_boundary_id"] != human_boundary_id:
                    raise RunStoreError(
                        f"human boundary {human_boundary_id!r} is not current for run "
                        f"{run_id!r}; the boundary was already resolved or superseded"
                    )
                if resolution_kind not in boundary.required_resolution_kinds:
                    raise ValueError(
                        f"{resolution_kind} is not permitted by boundary reason "
                        f"{boundary.reason!r} (allowed: {list(boundary.required_resolution_kinds)})"
                    )
                if resolution_kind == "PUBLICATION_OWNERSHIP_RESOLVED":
                    expected = {
                        "project_id": boundary.ownership_project_id,
                        "deterministic_ref": boundary.ownership_deterministic_ref,
                        "change_request_external_id": (
                            boundary.ownership_change_request_external_id
                        ),
                        "run_marker": boundary.ownership_run_marker,
                    }
                    for key, expected_value in expected.items():
                        if resolution_payload[key] != expected_value:
                            raise ValueError(
                                f"PUBLICATION_OWNERSHIP_RESOLVED.{key} must equal the "
                                "current boundary's copied ownership binding"
                            )
                if resolution_kind == "SECRET_OR_PERMISSION_PROVIDED" and source_kind == (
                    "SECRET_VERSION"
                ):
                    secret_id, version_str = source_id.split(":")
                    secret_version = self.get_secret_version(secret_id, int(version_str))
                    if secret_version is None:
                        raise ValueError(f"secret version {source_id!r} was not found")
                    current = self.get_secret_current_version(secret_id)
                    if current is None or current.current_version != int(version_str):
                        raise ValueError(f"secret version {source_id!r} is not the current version")
                    if resolution_payload["secret_version_key"] != source_id:
                        raise ValueError("resolution.secret_version_key must equal source_id")
                    if (
                        resolution_payload["creation_receipt_id"]
                        != secret_version.creation_receipt_id
                    ):
                        raise ValueError(
                            "resolution.creation_receipt_id must match the Secret "
                            "Version's creation Receipt"
                        )

                resolution_preimage = {
                    "human_boundary_id": human_boundary_id,
                    "run_id": run_id,
                    "source_kind": source_kind,
                    "source_id": source_id,
                    "authenticated_principal_id": authenticated_principal_id,
                    "resolution_kind": resolution_kind,
                    "resolution": resolution_payload,
                    "specification_generation": boundary.specification_generation,
                    "candidate_id": boundary.candidate_id,
                    "policy_hash": boundary.policy_hash,
                    "forge_observation_id": boundary.forge_observation_id,
                    "publication_id": boundary.publication_id,
                    "publication_effect_generation": boundary.publication_effect_generation,
                    "ownership_project_id": boundary.ownership_project_id,
                    "ownership_deterministic_ref": boundary.ownership_deterministic_ref,
                    "ownership_change_request_external_id": (
                        boundary.ownership_change_request_external_id
                    ),
                    "ownership_run_marker": boundary.ownership_run_marker,
                }
                digest = human_resolution_digest(resolution_preimage)
                columns = [
                    "human_resolution_id",
                    "human_boundary_id",
                    "run_id",
                    "idempotency_key",
                    "source_kind",
                    "source_id",
                    "authenticated_principal_id",
                    "resolution_kind",
                    "resolution_json",
                    "specification_generation",
                    "candidate_id",
                    "policy_hash",
                    "forge_observation_id",
                    "publication_id",
                    "publication_effect_generation",
                    "ownership_project_id",
                    "ownership_deterministic_ref",
                    "ownership_change_request_external_id",
                    "ownership_run_marker",
                    "resolution_digest",
                    "accepted_at_ms",
                ]
                values = (
                    human_resolution_id,
                    human_boundary_id,
                    run_id,
                    source_id,
                    source_kind,
                    source_id,
                    authenticated_principal_id,
                    resolution_kind,
                    canonical_json_text(resolution_payload),
                    boundary.specification_generation,
                    boundary.candidate_id,
                    boundary.policy_hash,
                    boundary.forge_observation_id,
                    boundary.publication_id,
                    boundary.publication_effect_generation,
                    boundary.ownership_project_id,
                    boundary.ownership_deterministic_ref,
                    boundary.ownership_change_request_external_id,
                    boundary.ownership_run_marker,
                    digest,
                    now,
                )
                self.conn.execute(
                    f"INSERT INTO human_resolutions({', '.join(columns)}) "
                    f"VALUES ({', '.join(['?'] * len(columns))})",
                    values,
                )

            created = self.get_human_resolution(human_resolution_id)
            assert created is not None

            view = load_view(self, run_id)
            if view is None:
                raise RunStoreError(f"run {run_id!r} was not found")
            trigger_facts: dict[str, Any] = {"human_boundary_id": human_boundary_id}
            if source_kind == "MANAGEMENT_COMMAND":
                trigger_kind = "MANAGEMENT_COMMAND"
                trigger_facts["kind"] = "RESOLVE_HUMAN_BOUNDARY"
            elif source_kind == "SECRET_VERSION":
                trigger_kind = "SECRET_VERSION"
                trigger_facts["satisfies_boundary"] = True
            else:
                trigger_kind = "STORAGE_RESTORATION"
                trigger_facts["matches_object"] = True

            applied = apply(
                self,
                view,
                Trigger(kind=trigger_kind, trigger_id=source_id, facts=trigger_facts),
                run_id=run_id,
            )
            if (
                not applied.replayed
                and applied.reduction is not None
                and applied.reduction.reason_code == "HUMAN_RESOLUTION"
            ):
                self._clear_run_human_boundary_pointer(run_id)
            return HumanResolutionOutcome(human_resolution=created, applied=applied)

    def _clear_run_human_boundary_pointer(self, run_id: str) -> None:
        self.conn.execute(
            "UPDATE runs SET human_boundary_id = NULL, updated_at_ms = ? WHERE run_id = ?",
            (_now_ms(), run_id),
        )

    def _waiting_run_ids(self, *, wait_reason: str, project_id: str | None = None) -> list[str]:
        """Bytewise-sorted ``run_id``s currently ``WAITING`` for ``wait_reason``.

        A cheap candidate scan over ``state = 'WAITING'`` Runs, reconstructed
        from durable pointers -- the exact information the pure reducer
        already exposes via ``RunView.wait_reason``. Wake callers MUST still
        resolve each candidate's actual current Wait Condition (via
        :meth:`get_current_wait_condition`) and match its typed
        ``wake_kind``/``wake_identity`` before proposing a wake: this scan
        alone cannot distinguish two Waits sharing one reason but different
        wake bindings.
        """
        from orcest.workflow_reducer.ledger import load_view

        query = "SELECT run_id FROM runs WHERE state = 'WAITING'"
        params: list[Any] = []
        if project_id is not None:
            query += " AND project_id = ?"
            params.append(project_id)
        run_ids = [row["run_id"] for row in self.conn.execute(query, params).fetchall()]
        matching = [
            run_id
            for run_id in run_ids
            if (view := load_view(self, run_id)) is not None and view.wait_reason == wait_reason
        ]
        return sorted(matching)

    def _clear_run_wait_pointer(self, run_id: str) -> None:
        self.conn.execute(
            "UPDATE runs SET wait_condition_id = NULL, updated_at_ms = ? WHERE run_id = ?",
            (_now_ms(), run_id),
        )

    def _wake_wait_condition(
        self,
        *,
        run_id: str,
        view: Any,
        trigger_kind: str,
        trigger_id: str,
    ) -> bool:
        """Apply one wake Trigger to a Run already confirmed to be
        ``WAITING`` on a Wait Condition whose typed wake predicate this
        specific input satisfies. Clears the SQL ``runs.wait_condition_id``
        pointer (the JSON pointer projection is cleared by the reducer's own
        ``_wake`` reduction inside ``apply``) and returns whether this call
        actually closed the Wait."""
        from orcest.workflow_reducer.ledger import apply
        from orcest.workflow_reducer.types import Trigger

        applied = apply(
            self,
            view,
            Trigger(kind=trigger_kind, trigger_id=trigger_id, facts={"wakes_wait": True}),
            run_id=run_id,
        )
        if (
            not applied.replayed
            and applied.reduction is not None
            and applied.reduction.reason_code == "WAIT_WAKE"
        ):
            self._clear_run_wait_pointer(run_id)
            return True
        return False

    def _wake_capacity_waits(self, observations: Sequence[HealthObservationRecord]) -> list[str]:
        """Apply each ``AVAILABLE`` capacity Health Observation to every
        current ``WAITING``/``CAPACITY`` Run whose actual Wait Condition
        names a matching scope (or, for a panel-scoped Wait, whose every
        named slot is now compatibly available -- all or none), in bytewise
        Run-ID order, and return the distinct ``wait_condition_id``s
        actually cleared.

        Must run inside the same writer transaction as the Capacity Report and
        its Health Observations (domain-model.md "Capacity Report": "The
        Capacity Report, all Health Observations, per-Run wake
        Transitions/outboxes, and response commit in one writer
        transaction"). ``UNAVAILABLE`` never proposes a wake here. A Run
        whose Wait names a scope this Report never touched, or a panel Wait
        with any slot still incompatible, is left untouched -- an unrelated
        or partial capacity signal cannot close it.
        """
        available = [obs for obs in observations if obs.kind == "AVAILABLE"]
        if not available:
            return []
        available_by_scope = {(obs.scope_kind, obs.scope_id): obs for obs in available}
        from orcest.workflow_reducer.ledger import load_view

        now = _now_ms()
        woken: list[str] = []
        for run_id in self._waiting_run_ids(wait_reason="CAPACITY"):
            view = load_view(self, run_id)
            if view is None or view.state != "WAITING" or view.wait_reason != "CAPACITY":
                continue
            wait_condition_id = view.wait_condition_id
            if wait_condition_id is None:
                continue
            wait = self.get_wait_condition(wait_condition_id)
            if wait is None:
                continue
            if wait.panel_slots:
                slots = tuple(
                    WaitConditionPanelSlotInput(
                        activity_id=slot.activity_id,
                        assignment_kind=slot.assignment_kind,
                        panel_round=slot.panel_round,
                        slot_id=slot.slot_id,
                    )
                    for slot in wait.panel_slots
                )
                if not self._panel_wait_satisfied(slots, now_ms=now).already_satisfied:
                    continue
                trigger_id = available[0].health_observation_id
            else:
                identity = wait.wake_identity or {}
                scope = (identity.get("scope_kind"), identity.get("scope_id"))
                observation = available_by_scope.get(scope)  # type: ignore[arg-type]
                if observation is None:
                    continue
                trigger_id = observation.health_observation_id
            if self._wake_wait_condition(
                run_id=run_id, view=view, trigger_kind="HEALTH_OBSERVATION", trigger_id=trigger_id
            ):
                woken.append(wait_condition_id)
        return woken

    def _budget_wait_matching_run_ids(
        self, *, project_id: str, accounting_scope_id: str, source_sequence: int
    ) -> list[str]:
        """Bytewise-sorted ``run_id``s currently ``WAITING``/``BUDGET`` in
        this Project whose actual Wait Condition names this exact
        accounting scope and whose minimum source sequence
        ``source_sequence`` meets -- never every ``BUDGET``-reason Run in
        the Project regardless of which exhausted scope/sequence it is
        actually bound to (persistence-and-recovery.md "WAIT_BUDGET"
        GUARDED paragraph)."""
        from orcest.workflow_reducer.ledger import load_view

        matching: list[str] = []
        for run_id in self._waiting_run_ids(wait_reason="BUDGET", project_id=project_id):
            view = load_view(self, run_id)
            if view is None or view.state != "WAITING" or view.wait_reason != "BUDGET":
                continue
            if view.wait_condition_id is None:
                continue
            wait = self.get_wait_condition(view.wait_condition_id)
            if wait is None or wait.wake_identity is None:
                continue
            identity = wait.wake_identity
            if (
                identity.get("project_id") != project_id
                or identity.get("accounting_scope_id") != accounting_scope_id
                or source_sequence < int(identity.get("minimum_source_sequence", 0))
            ):
                continue
            matching.append(run_id)
        return matching

    def wake_due_wait_timers(
        self,
        *,
        source_kind: str = "SCHEDULED_SWEEP",
        source_id: str | None = None,
        now_ms: int | None = None,
    ) -> list[str]:
        """Scheduled-sweep/startup-reconciliation pass over every currently
        ``WAITING`` Run whose Wait's ``not_before_ms`` is now due.

        Records one scope/deadline-unique ``WAIT_CONDITION_NOT_BEFORE`` Timer
        Fact per due Wait (idempotent via :meth:`_record_timer_fact`'s
        ``UNIQUE (scope_kind, scope_id, fired_for_ms)``: a repeated sweep
        over a still-due Wait, or the exact same due deadline reappearing
        after a Run's generation changes, records no second Fact and applies
        no second wake) and, only on the call that actually records the
        Fact, wakes the Run. Returns the distinct ``wait_condition_id``s
        actually cleared.
        """
        enums.parse_enum("timer_fact.source_kind", source_kind)
        now = _now_ms() if now_ms is None else now_ms
        resolved_source_id = source_id if source_id is not None else str(uuid.uuid4())
        from orcest.workflow_reducer.ledger import load_view

        with self.transaction():
            woken: list[str] = []
            run_ids = sorted(
                row["run_id"]
                for row in self.conn.execute(
                    "SELECT run_id FROM runs WHERE state = 'WAITING'"
                ).fetchall()
            )
            for run_id in run_ids:
                view = load_view(self, run_id)
                if view is None or view.state != "WAITING" or view.wait_condition_id is None:
                    continue
                wait = self.get_wait_condition(view.wait_condition_id)
                if wait is None or wait.not_before_ms is None or now < wait.not_before_ms:
                    continue
                timer_fact = self._record_timer_fact(
                    scope_kind="WAIT_CONDITION_NOT_BEFORE",
                    scope_id=wait.wait_condition_id,
                    fired_for_ms=wait.not_before_ms,
                    source_kind=source_kind,
                    source_id=resolved_source_id,
                    run_id=run_id,
                    now_ms=now,
                )
                already_fired = self.conn.execute(
                    "SELECT 1 FROM transitions WHERE run_id = ? AND trigger_kind = 'TIMER_FACT' "
                    "AND trigger_id = ?",
                    (run_id, timer_fact.timer_fact_id),
                ).fetchone()
                if already_fired is not None:
                    continue
                if self._wake_wait_condition(
                    run_id=run_id,
                    view=view,
                    trigger_kind="TIMER_FACT",
                    trigger_id=timer_fact.timer_fact_id,
                ):
                    woken.append(wait.wait_condition_id)
            return woken

    def wake_secret_recovery_wait(self, run_id: str, *, secret_version_id: str) -> bool:
        """Wake one ``WAITING``/``SECRET_RECOVERY`` Run whose Wait names the
        exact logical Secret ID and whose now-current verified version meets
        its ``minimum_version`` fence -- never a mutable-current tag or a
        stale frozen SecretRef (persistence-and-recovery.md "SECRET_RECOVERY
        Waits")."""
        from orcest.workflow_reducer.ledger import load_view

        with self.transaction():
            view = load_view(self, run_id)
            if view is None or view.state != "WAITING" or view.wait_reason != "SECRET_RECOVERY":
                return False
            if view.wait_condition_id is None:
                return False
            wait = self.get_wait_condition(view.wait_condition_id)
            if wait is None or wait.wake_identity is None:
                return False
            check = self._secret_recovery_wait_satisfied(wait.wake_identity)
            if not check.already_satisfied:
                return False
            return self._wake_wait_condition(
                run_id=run_id,
                view=view,
                trigger_kind="SECRET_VERSION",
                trigger_id=secret_version_id,
            )

    def wake_external_dependency_wait(self, run_id: str, *, forge_observation_id: str) -> bool:
        """Wake one ``WAITING``/``EXTERNAL_DEPENDENCY`` Run whose Wait names
        the exact Project/target and whose observation sequence/satisfaction
        the accepted Forge Observation meets."""
        from orcest.workflow_reducer.ledger import load_view

        with self.transaction():
            view = load_view(self, run_id)
            if view is None or view.state != "WAITING" or view.wait_reason != "EXTERNAL_DEPENDENCY":
                return False
            if view.wait_condition_id is None:
                return False
            wait = self.get_wait_condition(view.wait_condition_id)
            if wait is None or wait.wake_identity is None:
                return False
            check = self._external_dependency_wait_satisfied(wait.wake_identity)
            if not check.already_satisfied:
                return False
            return self._wake_wait_condition(
                run_id=run_id,
                view=view,
                trigger_kind="FORGE_OBSERVATION",
                trigger_id=forge_observation_id,
            )

    # -- Capacity Report -----------------------------------------------

    def _capacity_report_result_from_row(
        self, row: sqlite3.Row, *, replayed: bool
    ) -> CapacityReportResult:
        entry_rows = self.conn.execute(
            "SELECT ho.* FROM capacity_report_entries cre "
            "JOIN health_observations ho ON ho.health_observation_id = cre.health_observation_id "
            "WHERE cre.capacity_report_id = ? ORDER BY cre.entry_ordinal",
            (row["capacity_report_id"],),
        ).fetchall()
        observations = tuple(self._row_to_health_observation(r) for r in entry_rows)
        woken = tuple(json.loads(row["response_json"]).get("woken_wait_condition_ids", ()))
        return CapacityReportResult(
            capacity_report_id=row["capacity_report_id"],
            pool_manager_id=row["pool_manager_id"],
            report_id=row["report_id"],
            idempotency_key=row["idempotency_key"],
            report_sequence=row["report_sequence"],
            health_observations=observations,
            woken_wait_condition_ids=woken,
            response_http_status=row["response_http_status"],
            response_json=_response_json_with_replayed(row["response_json"], replayed=replayed),
            response_digest=row["response_digest"],
            accepted_at_ms=row["accepted_at_ms"],
            replayed=replayed,
        )

    def submit_capacity_report(
        self,
        *,
        capacity_report_id: str,
        pool_manager_id: str,
        report_id: str,
        idempotency_key: str,
        report_sequence: int,
        observed_at_ms: int,
        expires_at_ms: int,
        configured_max_ttl_ms: int,
        entries: Sequence[CapacityReportEntryInput],
        authenticated_principal_id: str,
        authorization_context_digest: str,
    ) -> CapacityReportResult:
        """Accept (or replay) one pool-manager Capacity Report.

        Commits the Capacity Report, one Health Observation per entry, and any
        resulting capacity-wait wake Transitions in a single writer
        transaction (domain-model.md "Capacity Report"). A previously unseen
        report whose sequence is not greater than the pool manager's last
        accepted sequence is rejected without a ledger row or Health
        Observation.
        """
        require_lowercase_uuid(capacity_report_id, field="capacity_report_id")
        require_lowercase_uuid(report_id, field="report_id")
        require_lowercase_uuid(idempotency_key, field="idempotency_key")
        _require_positive_int(report_sequence, field="report_sequence")
        _require_positive_int(configured_max_ttl_ms, field="configured_max_ttl_ms")
        _require_digest(authorization_context_digest, field="authorization_context_digest")
        _validate_capacity_report_entries(entries)

        entry_payload = [
            {
                "scope_kind": entry.scope_kind,
                "scope_id": entry.scope_id,
                "capacity_pool_id": entry.capacity_pool_id,
                "worker_profile": entry.worker_profile,
                "available_slots": entry.available_slots,
                "session_evidence": dict(entry.session_evidence)
                if entry.session_evidence
                else None,
            }
            for entry in entries
        ]
        req_digest = capacity_report_digest(
            {
                "protocol": CAPACITY_REPORT_PROTOCOL,
                "pool_manager_id": pool_manager_id,
                "report_id": report_id,
                "idempotency_key": idempotency_key,
                "report_sequence": report_sequence,
                "observed_at_ms": observed_at_ms,
                "expires_at_ms": expires_at_ms,
                "entries": entry_payload,
            }
        )

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM capacity_reports WHERE pool_manager_id = ? "
                "AND (report_id = ? OR idempotency_key = ?)",
                (pool_manager_id, report_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if (
                    existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["payload_digest"] == req_digest
                ):
                    return self._capacity_report_result_from_row(existing, replayed=True)
                raise IdempotencyConflictError(
                    "capacity report id/idempotency key was reused with different content"
                )

            last_sequence = self.conn.execute(
                "SELECT COALESCE(MAX(report_sequence), 0) AS seq FROM capacity_reports "
                "WHERE pool_manager_id = ?",
                (pool_manager_id,),
            ).fetchone()["seq"]
            if report_sequence <= int(last_sequence):
                raise CasMismatchError(
                    "capacity report sequence is not greater than the last accepted sequence"
                )

            accepted_at_ms = _now_ms()
            if not (
                expires_at_ms > accepted_at_ms
                and expires_at_ms <= accepted_at_ms + configured_max_ttl_ms
            ):
                raise ValueError(
                    "capacity report expires_at_ms must satisfy accepted_at_ms < expires_at_ms "
                    "<= accepted_at_ms + configured_max_ttl_ms"
                )

            self.conn.execute(
                "INSERT INTO capacity_reports(capacity_report_id, pool_manager_id, report_id, "
                "idempotency_key, report_sequence, observed_at_ms, expires_at_ms, "
                "configured_max_ttl_ms, authenticated_principal_id, "
                "authorization_context_digest, payload_digest, response_http_status, "
                "response_json, response_digest, accepted_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    capacity_report_id,
                    pool_manager_id,
                    report_id,
                    idempotency_key,
                    report_sequence,
                    observed_at_ms,
                    expires_at_ms,
                    configured_max_ttl_ms,
                    authenticated_principal_id,
                    authorization_context_digest,
                    req_digest,
                    200,
                    "",
                    "",
                    accepted_at_ms,
                ),
            )

            observations: list[HealthObservationRecord] = []
            for ordinal, entry in enumerate(entries):
                availability = "AVAILABLE" if entry.available_slots > 0 else "UNAVAILABLE"
                subject_bindings: dict[str, Any] = {
                    "capacity_pool_id": entry.capacity_pool_id,
                    "worker_profile": entry.worker_profile,
                    "available_slots": entry.available_slots,
                }
                if entry.session_evidence is not None:
                    subject_bindings["session_evidence"] = dict(entry.session_evidence)
                observation = self._insert_health_observation(
                    scope_kind=entry.scope_kind,
                    scope_id=entry.scope_id,
                    kind=availability,
                    source_kind="CAPACITY_REPORT",
                    source_id=capacity_report_id,
                    subject_bindings=subject_bindings,
                    observed_revision=report_sequence,
                    effective_at_ms=accepted_at_ms,
                    expires_at_ms=expires_at_ms,
                )
                observations.append(observation)
                self.conn.execute(
                    "INSERT INTO capacity_report_entries(capacity_report_id, entry_ordinal, "
                    "scope_kind, scope_id, availability, capacity_pool_id, worker_profile, "
                    "available_slots, session_evidence_json, health_observation_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        capacity_report_id,
                        ordinal,
                        entry.scope_kind,
                        entry.scope_id,
                        availability,
                        entry.capacity_pool_id,
                        entry.worker_profile,
                        entry.available_slots,
                        canonical_json_text(entry.session_evidence)
                        if entry.session_evidence
                        else None,
                        observation.health_observation_id,
                    ),
                )

            woken = self._wake_capacity_waits(observations)

            body = {
                "protocol": CAPACITY_REPORT_RESULT_PROTOCOL,
                "capacity_report_id": capacity_report_id,
                "report_id": report_id,
                "report_sequence": report_sequence,
                "replayed": False,
                "health_observations": [
                    {
                        "health_observation_id": obs.health_observation_id,
                        "scope_kind": obs.scope_kind,
                        "scope_id": obs.scope_id,
                        "health_sequence": obs.health_sequence,
                        "kind": obs.kind,
                        "effective_at_ms": obs.effective_at_ms,
                        "expires_at_ms": obs.expires_at_ms,
                    }
                    for obs in observations
                ],
                "woken_wait_condition_ids": list(woken),
            }
            resp_digest = response_digest(
                {"http_status": 200, "body": _response_digest_preimage(body)}
            )
            body_json = canonical_json_text(body)
            self.conn.execute(
                "UPDATE capacity_reports SET response_json = ?, response_digest = ? "
                "WHERE capacity_report_id = ?",
                (body_json, resp_digest, capacity_report_id),
            )
            return CapacityReportResult(
                capacity_report_id=capacity_report_id,
                pool_manager_id=pool_manager_id,
                report_id=report_id,
                idempotency_key=idempotency_key,
                report_sequence=report_sequence,
                health_observations=tuple(observations),
                woken_wait_condition_ids=tuple(woken),
                response_http_status=200,
                response_json=body_json,
                response_digest=resp_digest,
                accepted_at_ms=accepted_at_ms,
                replayed=False,
            )

    # -- Worker Loss Report --------------------------------------------

    def _worker_loss_report_result_response(
        self,
        *,
        worker_loss_report_id: str,
        attempt_id: str,
        activity_id: str,
        attempt_generation: int,
        accepted: bool,
        stale: bool,
        health_observation_id: str | None = None,
        attempt_terminal_fact_id: str | None = None,
    ) -> tuple[int, str, str]:
        body: dict[str, Any] = {
            "protocol": WORKER_LOSS_RESULT_PROTOCOL,
            "worker_loss_report_id": worker_loss_report_id,
            "attempt_id": attempt_id,
            "activity_id": activity_id,
            "generation": attempt_generation,
            "accepted": accepted,
            "stale": stale,
            "replayed": False,
            "health_observation_id": health_observation_id,
            "attempt_terminal_fact_id": attempt_terminal_fact_id,
        }
        resp_digest = response_digest({"http_status": 200, "body": _response_digest_preimage(body)})
        return 200, canonical_json_text(body), resp_digest

    def _worker_loss_report_result_from_row(
        self, row: sqlite3.Row, *, replayed: bool
    ) -> WorkerLossReportResult:
        return WorkerLossReportResult(
            worker_loss_report_id=row["worker_loss_report_id"],
            pool_manager_id=row["pool_manager_id"],
            idempotency_key=row["idempotency_key"],
            worker_id=row["worker_id"],
            worker_session_id=row["worker_session_id"],
            attempt_id=row["attempt_id"],
            activity_id=row["activity_id"],
            attempt_generation=row["attempt_generation"],
            reason=row["reason"],
            outcome=row["outcome"],
            health_observation_id=row["health_observation_id"],
            attempt_terminal_fact_id=row["attempt_terminal_fact_id"],
            response_http_status=row["response_http_status"],
            response_json=_response_json_with_replayed(row["response_json"], replayed=replayed),
            response_digest=row["response_digest"],
            accepted_at_ms=row["accepted_at_ms"],
            replayed=replayed,
        )

    def submit_worker_loss_report(
        self,
        *,
        worker_loss_report_id: str,
        pool_manager_id: str,
        idempotency_key: str,
        worker_id: str,
        worker_session_id: str,
        attempt_id: str,
        activity_id: str,
        attempt_generation: int,
        reason: str,
        observed_at_ms: int,
        authenticated_principal_id: str,
        authorization_context_digest: str,
    ) -> WorkerLossReportResult:
        """Accept (or replay) one pool-manager Worker Loss Report.

        ``ACCEPTED`` requires the exact current claimed Attempt/session: in
        that one writer transaction this terminalizes the Attempt
        (``FAILED``/``WORKER_LOST``), returns its Activity to ``PLANNED``, and
        reduces the resulting Attempt Terminal Fact. An already-terminal or
        mismatched Attempt yields the durable ``STALE`` path instead and
        cannot rewrite state or reason (domain-model.md "Worker Loss Report").
        An unknown Attempt triple raises :class:`AttemptUnknownError` and
        creates no ledger row at all.
        """
        require_lowercase_uuid(worker_loss_report_id, field="worker_loss_report_id")
        require_lowercase_uuid(idempotency_key, field="idempotency_key")
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(activity_id, field="activity_id")
        _require_positive_int(attempt_generation, field="attempt_generation")
        enums.parse_enum("worker_loss_report.reason", reason)
        _require_digest(authorization_context_digest, field="authorization_context_digest")

        req_digest = worker_loss_report_digest(
            {
                "protocol": WORKER_LOSS_PROTOCOL,
                "pool_manager_id": pool_manager_id,
                "idempotency_key": idempotency_key,
                "worker_id": worker_id,
                "worker_session_id": worker_session_id,
                "attempt_id": attempt_id,
                "activity_id": activity_id,
                "attempt_generation": attempt_generation,
                "reason": reason,
                "observed_at_ms": observed_at_ms,
            }
        )

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM worker_loss_reports "
                "WHERE pool_manager_id = ? AND idempotency_key = ?",
                (pool_manager_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if (
                    existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["payload_digest"] == req_digest
                ):
                    return self._worker_loss_report_result_from_row(existing, replayed=True)
                raise IdempotencyConflictError(
                    "worker loss report idempotency key was reused with different content"
                )

            attempt_row = self.conn.execute(
                "SELECT * FROM attempts WHERE attempt_id = ? AND activity_id = ? "
                "AND generation = ?",
                (attempt_id, activity_id, attempt_generation),
            ).fetchone()
            if attempt_row is None:
                raise AttemptUnknownError(
                    f"no Attempt with attempt_id={attempt_id!r} activity_id={activity_id!r} "
                    f"generation={attempt_generation!r}"
                )

            accepted_at_ms = _now_ms()
            matches_current_claim = (
                attempt_row["state"] == "CLAIMED"
                and attempt_row["claimed_worker_id"] == worker_id
                and attempt_row["claimed_worker_session_id"] == worker_session_id
            )

            if not matches_current_claim:
                http_status, body_json, resp_digest = self._worker_loss_report_result_response(
                    worker_loss_report_id=worker_loss_report_id,
                    attempt_id=attempt_id,
                    activity_id=activity_id,
                    attempt_generation=attempt_generation,
                    accepted=False,
                    stale=True,
                )
                self.conn.execute(
                    "INSERT INTO worker_loss_reports(worker_loss_report_id, pool_manager_id, "
                    "idempotency_key, worker_id, worker_session_id, attempt_id, activity_id, "
                    "attempt_generation, reason, observed_at_ms, authenticated_principal_id, "
                    "authorization_context_digest, payload_digest, outcome, "
                    "health_observation_id, attempt_terminal_fact_id, response_http_status, "
                    "response_json, response_digest, accepted_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'STALE', NULL, NULL, "
                    "?, ?, ?, ?)",
                    (
                        worker_loss_report_id,
                        pool_manager_id,
                        idempotency_key,
                        worker_id,
                        worker_session_id,
                        attempt_id,
                        activity_id,
                        attempt_generation,
                        reason,
                        observed_at_ms,
                        authenticated_principal_id,
                        authorization_context_digest,
                        req_digest,
                        http_status,
                        body_json,
                        resp_digest,
                        accepted_at_ms,
                    ),
                )
                return WorkerLossReportResult(
                    worker_loss_report_id=worker_loss_report_id,
                    pool_manager_id=pool_manager_id,
                    idempotency_key=idempotency_key,
                    worker_id=worker_id,
                    worker_session_id=worker_session_id,
                    attempt_id=attempt_id,
                    activity_id=activity_id,
                    attempt_generation=attempt_generation,
                    reason=reason,
                    outcome="STALE",
                    health_observation_id=None,
                    attempt_terminal_fact_id=None,
                    response_http_status=http_status,
                    response_json=body_json,
                    response_digest=resp_digest,
                    accepted_at_ms=accepted_at_ms,
                    replayed=False,
                )

            activity_row = self.conn.execute(
                "SELECT * FROM activities WHERE activity_id = ?", (activity_id,)
            ).fetchone()
            assert activity_row is not None
            run_id = activity_row["run_id"]

            subject_bindings = {
                "worker_id": worker_id,
                "worker_session_id": worker_session_id,
                "attempt_id": attempt_id,
                "activity_id": activity_id,
                "attempt_generation": attempt_generation,
            }
            observation = self._insert_health_observation(
                scope_kind="WORKER_SESSION",
                scope_id=worker_session_id,
                kind="LOST",
                source_kind="WORKER_LOSS_REPORT",
                source_id=worker_loss_report_id,
                subject_bindings=subject_bindings,
                observed_revision=None,
                effective_at_ms=accepted_at_ms,
                expires_at_ms=None,
            )

            attempt_terminal_fact_id = str(uuid.uuid4())
            fact_digest = attempt_terminal_fact_digest(
                {
                    "attempt_id": attempt_id,
                    "activity_id": activity_id,
                    "attempt_generation": attempt_generation,
                    "kind": "WORKER_LOST",
                    "source_kind": "HEALTH_OBSERVATION",
                    "source_id": observation.health_observation_id,
                }
            )
            self.conn.execute(
                "INSERT INTO attempt_terminal_facts(attempt_terminal_fact_id, attempt_id, "
                "activity_id, attempt_generation, kind, source_kind, source_id, "
                "health_observation_id, fact_digest, recorded_at_ms) "
                "VALUES (?, ?, ?, ?, 'WORKER_LOST', 'HEALTH_OBSERVATION', ?, ?, ?, ?)",
                (
                    attempt_terminal_fact_id,
                    attempt_id,
                    activity_id,
                    attempt_generation,
                    observation.health_observation_id,
                    observation.health_observation_id,
                    fact_digest,
                    accepted_at_ms,
                ),
            )

            cur = self.conn.execute(
                "UPDATE attempts SET state = 'FAILED', terminal_reason = 'WORKER_LOST' "
                "WHERE attempt_id = ? AND activity_id = ? AND generation = ? "
                "AND state = 'CLAIMED'",
                (attempt_id, activity_id, attempt_generation),
            )
            if cur.rowcount != 1:
                raise CasMismatchError(
                    "attempt claim was lost between the match check and terminalization"
                )
            cur = self.conn.execute(
                "UPDATE activities SET state = 'PLANNED', updated_at_ms = ? "
                "WHERE activity_id = ? AND state = 'ACTIVE'",
                (accepted_at_ms, activity_id),
            )
            if cur.rowcount != 1:
                raise RunStoreError(
                    f"activity {activity_id!r} was not ACTIVE for its exact claimed Attempt"
                )

            from orcest.workflow_reducer.ledger import apply, load_view
            from orcest.workflow_reducer.types import Trigger

            view = load_view(self, run_id)
            if view is None:
                raise RunStoreError(f"run {run_id!r} for activity {activity_id!r} was not found")
            apply(
                self,
                view,
                Trigger(
                    kind="ATTEMPT_TERMINAL",
                    trigger_id=attempt_terminal_fact_id,
                    facts={"kind": "WORKER_LOST", "already_terminal": False},
                ),
                run_id=run_id,
            )

            http_status, body_json, resp_digest = self._worker_loss_report_result_response(
                worker_loss_report_id=worker_loss_report_id,
                attempt_id=attempt_id,
                activity_id=activity_id,
                attempt_generation=attempt_generation,
                accepted=True,
                stale=False,
                health_observation_id=observation.health_observation_id,
                attempt_terminal_fact_id=attempt_terminal_fact_id,
            )
            self.conn.execute(
                "INSERT INTO worker_loss_reports(worker_loss_report_id, pool_manager_id, "
                "idempotency_key, worker_id, worker_session_id, attempt_id, activity_id, "
                "attempt_generation, reason, observed_at_ms, authenticated_principal_id, "
                "authorization_context_digest, payload_digest, outcome, "
                "health_observation_id, attempt_terminal_fact_id, response_http_status, "
                "response_json, response_digest, accepted_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACCEPTED', ?, ?, ?, ?, ?, ?)",
                (
                    worker_loss_report_id,
                    pool_manager_id,
                    idempotency_key,
                    worker_id,
                    worker_session_id,
                    attempt_id,
                    activity_id,
                    attempt_generation,
                    reason,
                    observed_at_ms,
                    authenticated_principal_id,
                    authorization_context_digest,
                    req_digest,
                    observation.health_observation_id,
                    attempt_terminal_fact_id,
                    http_status,
                    body_json,
                    resp_digest,
                    accepted_at_ms,
                ),
            )
            return WorkerLossReportResult(
                worker_loss_report_id=worker_loss_report_id,
                pool_manager_id=pool_manager_id,
                idempotency_key=idempotency_key,
                worker_id=worker_id,
                worker_session_id=worker_session_id,
                attempt_id=attempt_id,
                activity_id=activity_id,
                attempt_generation=attempt_generation,
                reason=reason,
                outcome="ACCEPTED",
                health_observation_id=observation.health_observation_id,
                attempt_terminal_fact_id=attempt_terminal_fact_id,
                response_http_status=http_status,
                response_json=body_json,
                response_digest=resp_digest,
                accepted_at_ms=accepted_at_ms,
                replayed=False,
            )

    # -- Attempt Liveness -------------------------------------------------

    def submit_attempt_liveness(
        self,
        *,
        attempt_id: str,
        activity_id: str,
        generation: int,
        worker_id: str,
        worker_session_id: str,
        attempt_capability_digest: str,
        sequence: int,
        observed_at_ms: int,
        state: str,
        now_ms: int | None = None,
    ) -> AttemptLivenessResult:
        """Accept one ``PUT /api/v1/attempts/{attempt_id}/liveness`` update.

        worker-protocol.md "Liveness, control, and deadlines": liveness has
        no idempotency key, durable request row, or original-response
        replay, so this never durably rejects a replayed, skipped, or
        out-of-order ``sequence`` -- it is disposable current-control
        evidence, not a second terminal trigger. A lower-or-equal sequence
        "never rewinds state": it still gets a freshly derived response, it
        just does not advance the informational
        ``last_liveness_observed_ms``/``last_liveness_sequence`` high-water
        mark. The response ``control`` is always freshly derived from the
        current durable Attempt/Run state, so replay, a skipped sequence, an
        ambiguous (timed-out) prior call, and post-restart reconstruction
        all resolve to the same one ordered conversation instead of a
        second/competing one.

        Source access, upload, credential rotation, liveness, and launch all
        end at ``execution_deadline_ms`` (domain-model.md "Attempt"): a
        liveness call at or after that fixed deadline is not merely told to
        ``CANCEL``, it is refused outright, mirroring
        ``require_current_rotation_authority``'s identical cutoff for
        credential rotation. Equality belongs to the deadline, never to this
        call arriving "first".
        """
        require_lowercase_uuid(attempt_id, field="attempt_id")
        require_lowercase_uuid(activity_id, field="activity_id")
        _require_positive_int(generation, field="generation")
        _require_positive_int(sequence, field="sequence")
        enums.parse_enum("attempt_liveness.state", state)
        now = _now_ms() if now_ms is None else now_ms
        with self.transaction():
            attempt = self.get_attempt(attempt_id)
            if attempt is None or attempt.activity_id != activity_id:
                raise AttemptUnknownError(
                    f"no Attempt with attempt_id={attempt_id!r} activity_id={activity_id!r}"
                )
            if (
                attempt.generation != generation
                or attempt.state != "CLAIMED"
                or attempt.claimed_worker_id != worker_id
                or attempt.claimed_worker_session_id != worker_session_id
                or attempt.attempt_capability_digest != attempt_capability_digest
            ):
                raise CasMismatchError("liveness update does not match the current claimed attempt")
            assert attempt.execution_deadline_ms is not None
            if now >= attempt.execution_deadline_ms:
                raise CasMismatchError("liveness update arrived at or after the execution deadline")

            activity = self.get_activity(activity_id)
            assert activity is not None
            from orcest.workflow_reducer.ledger import load_view

            view = load_view(self, activity.run_id)
            control = "CANCEL" if view is not None and view.cancellation_pending else "CONTINUE"

            sequence_advanced = (
                attempt.last_liveness_sequence is None or sequence > attempt.last_liveness_sequence
            )
            if sequence_advanced:
                self.conn.execute(
                    "UPDATE attempts SET last_liveness_observed_ms = ?, "
                    "last_liveness_sequence = ? WHERE attempt_id = ?",
                    (observed_at_ms, sequence, attempt_id),
                )

            body = {
                "protocol": ATTEMPT_LIVENESS_RESULT_PROTOCOL,
                "attempt_id": attempt_id,
                "activity_id": activity_id,
                "generation": generation,
                "control": control,
                "execution_deadline_ms": attempt.execution_deadline_ms,
                "liveness_recorded": False,
            }
            resp_digest = response_digest({"http_status": 202, "body": body})
            return AttemptLivenessResult(
                attempt_id=attempt_id,
                activity_id=activity_id,
                generation=generation,
                control=control,
                execution_deadline_ms=attempt.execution_deadline_ms,
                liveness_recorded=False,
                sequence_advanced=sequence_advanced,
                response_http_status=202,
                response_json=canonical_json_text(body),
                response_digest=resp_digest,
            )

    # -- Timer Fact ---------------------------------------------------------

    def _record_timer_fact(
        self,
        *,
        scope_kind: str,
        scope_id: str,
        fired_for_ms: int,
        source_kind: str,
        source_id: str,
        now_ms: int,
        run_id: str | None = None,
    ) -> TimerFactRecord:
        """Insert-or-reuse; MUST run inside a caller-held ``self.transaction()``."""
        enums.parse_enum("timer_fact.scope_kind", scope_kind)
        enums.parse_enum("timer_fact.source_kind", source_kind)
        if now_ms < fired_for_ms:
            raise ValueError("timer fact fired_for_ms has not yet occurred")
        existing = self.conn.execute(
            "SELECT * FROM timer_facts WHERE scope_kind = ? AND scope_id = ? AND fired_for_ms = ?",
            (scope_kind, scope_id, fired_for_ms),
        ).fetchone()
        if existing is not None:
            return _row_to_timer_fact(existing)
        timer_fact_id = str(uuid.uuid4())
        fact_digest = timer_fact_digest(
            {
                "scope_kind": scope_kind,
                "scope_id": scope_id,
                "fired_for_ms": fired_for_ms,
                "source_kind": source_kind,
                "source_id": source_id,
                "run_id": run_id,
            }
        )
        self.conn.execute(
            "INSERT INTO timer_facts(timer_fact_id, run_id, scope_kind, scope_id, "
            "fired_for_ms, controller_now_ms, source_kind, source_id, fact_digest, "
            "recorded_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                timer_fact_id,
                run_id,
                scope_kind,
                scope_id,
                fired_for_ms,
                now_ms,
                source_kind,
                source_id,
                fact_digest,
                now_ms,
            ),
        )
        row = self.conn.execute(
            "SELECT * FROM timer_facts WHERE timer_fact_id = ?", (timer_fact_id,)
        ).fetchone()
        assert row is not None
        return _row_to_timer_fact(row)

    def record_timer_fact(
        self,
        *,
        scope_kind: str,
        scope_id: str,
        fired_for_ms: int,
        source_kind: str,
        source_id: str,
        run_id: str | None = None,
        now_ms: int | None = None,
    ) -> TimerFactRecord:
        """Durably prove the controller evaluated one due deadline.

        ``(scope_kind, scope_id, fired_for_ms)`` is the durable identity
        (domain-model.md "Timer Fact"): replaying the exact same due
        deadline for the same scope returns the already-recorded Fact
        instead of a second row, so repeated scheduled-sweep passes over a
        still-due deadline never fabricate distinct evidence. Insertion
        alone carries no consequence -- a Timer Fact is evidence, never a
        second terminal trigger for ``ATTEMPT_CLAIM_DEADLINE``/
        ``ATTEMPT_EXECUTION_DEADLINE`` scopes.
        """
        now = _now_ms() if now_ms is None else now_ms
        with self.transaction():
            return self._record_timer_fact(
                scope_kind=scope_kind,
                scope_id=scope_id,
                fired_for_ms=fired_for_ms,
                source_kind=source_kind,
                source_id=source_id,
                run_id=run_id,
                now_ms=now,
            )

    # -- Attempt-deadline capacity classifier + Terminal Fact -------------

    def _claim_deadline_offer_gate(self) -> tuple[str, _ControllerGateEvaluation]:
        gate = self._controller_gate_evaluation()
        if gate.permissions.mode not in {"RUNNING", "INTAKE_PAUSED"}:
            return "MODE_BLOCKED", gate
        if gate.selected_key is None:
            return "ISSUANCE_KEY_UNAVAILABLE", gate
        return "OFFER_ALLOWED", gate

    def _panel_has_claimed_peer(self, activity: ActivityRecord) -> bool:
        assignment = self._get_activity_review_assignment(activity.activity_id)
        if assignment is None:
            return False
        siblings = self.conn.execute(
            "SELECT a.activity_id FROM activities a "
            "JOIN activity_review_assignments r ON r.activity_id = a.activity_id "
            "WHERE a.run_id = ? AND r.assignment_kind = ? AND r.panel_round = ? "
            "AND a.activity_id != ?",
            (
                activity.run_id,
                assignment.assignment_kind,
                assignment.panel_round,
                activity.activity_id,
            ),
        ).fetchall()
        for sibling in siblings:
            claimed = self.conn.execute(
                "SELECT 1 FROM attempts WHERE activity_id = ? AND state = 'CLAIMED'",
                (sibling["activity_id"],),
            ).fetchone()
            if claimed is not None:
                return True
        return False

    def expire_attempt_claim_deadline(
        self,
        *,
        attempt_id: str,
        source_kind: str = "SCHEDULED_SWEEP",
        source_id: str | None = None,
        now_ms: int | None = None,
    ) -> AttemptDeadlineExpiryResult:
        """Evaluate one due ``ATTEMPT_CLAIM_DEADLINE`` for a still-``OFFERED`` Attempt.

        domain-model.md "Timer Fact" / "Attempt Terminal Fact": re-reads the
        still-current Attempt and its exact ``claim_deadline_ms``, records
        the scope/deadline-unique Timer Fact, then -- only if the Attempt is
        still exactly the ``OFFERED`` generation this deadline names --
        freezes the Controller Mode/Capability Registry offer gate and the
        highest applicable unexpired Worker Profile Health Observation into
        one complete Attempt Terminal Fact, expires the Attempt, returns its
        Activity to ``PLANNED``, and applies the Fact's sole
        ``T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`` Transition. A
        replayed or already-superseded call still durably records the Timer
        Fact as evidence but creates no second Terminal Fact and mutates
        nothing else (``outcome="STALE"``).

        For a ``REVIEW``/``ADJUDICATE`` panel slot with a still-``CLAIMED``
        peer in the same panel round, the reducer's own
        ``PANEL_CLAIM_DEADLINE`` handling (reduce.py) coalesces this into the
        Run's existing single pending staffing recheck rather than starting
        independent recovery -- this method supplies that real
        ``claimed_unfilled_peer`` fact instead of leaving it at its always-
        ``False`` default so that coalescing actually engages.

        Scoping note: capacity evidence here is frozen at Worker-Profile
        granularity only (no Capacity-Pool linkage is tracked on an Attempt
        today), and ``resolved_provider_secret_ref`` is the Attempt's own
        bound secret id without re-verifying its current version under the
        Secret Store lock. Both are narrower than the full wiki algorithm
        and are intentional simplifications for this leaf.
        """
        require_lowercase_uuid(attempt_id, field="attempt_id")
        enums.parse_enum("timer_fact.source_kind", source_kind)
        now = _now_ms() if now_ms is None else now_ms
        resolved_source_id = source_id if source_id is not None else str(uuid.uuid4())
        with self.transaction():
            attempt = self.get_attempt(attempt_id)
            if attempt is None:
                raise AttemptUnknownError(f"no Attempt with attempt_id={attempt_id!r}")
            activity = self.get_activity(attempt.activity_id)
            assert activity is not None
            fired_for_ms = attempt.claim_deadline_ms
            if now < fired_for_ms:
                raise ValueError("claim deadline has not yet occurred")

            timer_fact = self._record_timer_fact(
                scope_kind="ATTEMPT_CLAIM_DEADLINE",
                scope_id=attempt_id,
                fired_for_ms=fired_for_ms,
                source_kind=source_kind,
                source_id=resolved_source_id,
                run_id=activity.run_id,
                now_ms=now,
            )

            existing_fact = self.conn.execute(
                "SELECT * FROM attempt_terminal_facts WHERE attempt_id = ? "
                "AND kind = 'CLAIM_DEADLINE' AND source_kind = 'TIMER_FACT' AND source_id = ?",
                (attempt_id, timer_fact.timer_fact_id),
            ).fetchone()
            if existing_fact is not None:
                return AttemptDeadlineExpiryResult(
                    timer_fact_id=timer_fact.timer_fact_id,
                    outcome="EXPIRED",
                    attempt_terminal_fact_id=existing_fact["attempt_terminal_fact_id"],
                    capacity_disposition=existing_fact["capacity_disposition"],
                    replacement_offer_disposition=existing_fact["replacement_offer_disposition"],
                )

            current = self.get_attempt(attempt_id)
            assert current is not None
            if current.state != "OFFERED" or current.claim_deadline_ms != fired_for_ms:
                return AttemptDeadlineExpiryResult(
                    timer_fact_id=timer_fact.timer_fact_id, outcome="STALE"
                )

            replacement_offer_disposition, gate = self._claim_deadline_offer_gate()
            profile_health = self.get_latest_health_observation(
                "WORKER_PROFILE", current.worker_profile, now_ms=now
            )
            membership_ids = (
                [profile_health.health_observation_id] if profile_health is not None else []
            )
            capacity_disposition = (
                "COMPATIBLE_AVAILABLE"
                if profile_health is not None and profile_health.kind == "AVAILABLE"
                else "NO_COMPATIBLE_AVAILABLE"
            )
            membership_digest = attempt_terminal_fact_health_membership_digest(membership_ids)

            resolved_provider_secret_ref = None
            if current.provider_secret_ref is not None:
                secret_version = self.get_secret_current_version(current.provider_secret_ref)
                if secret_version is not None:
                    resolved_provider_secret_ref = current.provider_secret_ref

            selected_issuance_key_id = (
                gate.selected_key.capability_signing_key_id
                if gate.selected_key is not None
                else None
            )
            attempt_terminal_fact_id = str(uuid.uuid4())
            fact_digest = attempt_terminal_fact_digest(
                {
                    "attempt_id": attempt_id,
                    "activity_id": activity.activity_id,
                    "attempt_generation": current.generation,
                    "kind": "CLAIM_DEADLINE",
                    "source_kind": "TIMER_FACT",
                    "source_id": timer_fact.timer_fact_id,
                    "expected_deadline_ms": fired_for_ms,
                    "controller_now_ms": now,
                    "capacity_disposition": capacity_disposition,
                    "health_observation_ids_digest": membership_digest,
                    "resolved_provider_secret_ref": resolved_provider_secret_ref,
                    "controller_mode_revision": gate.permissions.mode_revision,
                    "controller_mode": gate.permissions.mode,
                    "capability_registry_revision": gate.permissions.registry_revision,
                    "selected_issuance_key_id": selected_issuance_key_id,
                    "replacement_offer_disposition": replacement_offer_disposition,
                }
            )
            self.conn.execute(
                "INSERT INTO attempt_terminal_facts(attempt_terminal_fact_id, attempt_id, "
                "activity_id, attempt_generation, kind, source_kind, source_id, "
                "health_observation_id, expected_deadline_ms, controller_now_ms, "
                "capacity_disposition, health_observation_ids_digest, "
                "resolved_provider_secret_ref, controller_mode_revision, controller_mode, "
                "capability_registry_revision, selected_issuance_key_id, "
                "replacement_offer_disposition, fact_digest, recorded_at_ms) "
                "VALUES (?, ?, ?, ?, 'CLAIM_DEADLINE', 'TIMER_FACT', ?, NULL, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?)",
                (
                    attempt_terminal_fact_id,
                    attempt_id,
                    activity.activity_id,
                    current.generation,
                    timer_fact.timer_fact_id,
                    fired_for_ms,
                    now,
                    capacity_disposition,
                    membership_digest,
                    resolved_provider_secret_ref,
                    gate.permissions.mode_revision,
                    gate.permissions.mode,
                    gate.permissions.registry_revision,
                    selected_issuance_key_id,
                    replacement_offer_disposition,
                    fact_digest,
                    now,
                ),
            )
            for ordinal, health_observation_id in enumerate(membership_ids):
                self.conn.execute(
                    "INSERT INTO attempt_terminal_fact_health_observations("
                    "attempt_terminal_fact_id, observation_ordinal, health_observation_id) "
                    "VALUES (?, ?, ?)",
                    (attempt_terminal_fact_id, ordinal, health_observation_id),
                )

            cur = self.conn.execute(
                "UPDATE attempts SET state = 'EXPIRED', terminal_reason = 'CLAIM_DEADLINE' "
                "WHERE attempt_id = ? AND state = 'OFFERED'",
                (attempt_id,),
            )
            if cur.rowcount != 1:
                raise CasMismatchError("attempt claim was won between the match check and expiry")
            self.conn.execute(
                "UPDATE activities SET state = 'PLANNED', updated_at_ms = ? "
                "WHERE activity_id = ? AND state = 'READY'",
                (now, activity.activity_id),
            )

            panel_peer_claimed = (
                self._panel_has_claimed_peer(activity)
                if activity.kind in {"REVIEW", "ADJUDICATE"}
                else False
            )

            from orcest.workflow_reducer.ledger import apply, load_view
            from orcest.workflow_reducer.types import Trigger

            view = load_view(self, activity.run_id)
            if view is not None:
                if activity.kind in {"REVIEW", "ADJUDICATE"}:
                    view = dataclasses.replace(view, claimed_unfilled_peer=panel_peer_claimed)
                apply(
                    self,
                    view,
                    Trigger(
                        kind="ATTEMPT_TERMINAL",
                        trigger_id=attempt_terminal_fact_id,
                        facts={"kind": "CLAIM_DEADLINE", "already_terminal": False},
                    ),
                    run_id=activity.run_id,
                )

            return AttemptDeadlineExpiryResult(
                timer_fact_id=timer_fact.timer_fact_id,
                outcome="EXPIRED",
                attempt_terminal_fact_id=attempt_terminal_fact_id,
                capacity_disposition=capacity_disposition,
                replacement_offer_disposition=replacement_offer_disposition,
            )

    def expire_attempt_execution_deadline(
        self,
        *,
        attempt_id: str,
        source_kind: str = "SCHEDULED_SWEEP",
        source_id: str | None = None,
        now_ms: int | None = None,
    ) -> AttemptDeadlineExpiryResult:
        """Proactively evaluate one due ``ATTEMPT_EXECUTION_DEADLINE`` Timer Fact.

        The reactive counterpart -- a late Result Request arriving after
        ``execution_deadline_ms`` -- is ``submit_attempt_result``'s existing
        ``EXECUTION_DEADLINE`` path. This is the scheduled-sweep/startup-
        reconciliation counterpart for a Claimed Attempt whose worker never
        submits a Result or liveness update at all. ``capacity_disposition``
        and ``replacement_offer_disposition`` are ``NULL`` for this kind
        (domain-model.md "Attempt Terminal Fact": only ``CLAIM_DEADLINE``
        carries a capacity classification).
        """
        require_lowercase_uuid(attempt_id, field="attempt_id")
        enums.parse_enum("timer_fact.source_kind", source_kind)
        now = _now_ms() if now_ms is None else now_ms
        resolved_source_id = source_id if source_id is not None else str(uuid.uuid4())
        with self.transaction():
            attempt = self.get_attempt(attempt_id)
            if attempt is None:
                raise AttemptUnknownError(f"no Attempt with attempt_id={attempt_id!r}")
            activity = self.get_activity(attempt.activity_id)
            assert activity is not None
            if attempt.execution_deadline_ms is None:
                raise ValueError("attempt has no execution deadline (never claimed)")
            fired_for_ms = attempt.execution_deadline_ms
            if now < fired_for_ms:
                raise ValueError("execution deadline has not yet occurred")

            timer_fact = self._record_timer_fact(
                scope_kind="ATTEMPT_EXECUTION_DEADLINE",
                scope_id=attempt_id,
                fired_for_ms=fired_for_ms,
                source_kind=source_kind,
                source_id=resolved_source_id,
                run_id=activity.run_id,
                now_ms=now,
            )

            existing_fact = self.conn.execute(
                "SELECT * FROM attempt_terminal_facts WHERE attempt_id = ? "
                "AND kind = 'EXECUTION_DEADLINE' AND source_kind = 'TIMER_FACT' "
                "AND source_id = ?",
                (attempt_id, timer_fact.timer_fact_id),
            ).fetchone()
            if existing_fact is not None:
                return AttemptDeadlineExpiryResult(
                    timer_fact_id=timer_fact.timer_fact_id,
                    outcome="EXPIRED",
                    attempt_terminal_fact_id=existing_fact["attempt_terminal_fact_id"],
                )

            current = self.get_attempt(attempt_id)
            assert current is not None
            if current.state != "CLAIMED" or current.execution_deadline_ms != fired_for_ms:
                return AttemptDeadlineExpiryResult(
                    timer_fact_id=timer_fact.timer_fact_id, outcome="STALE"
                )

            attempt_terminal_fact_id = str(uuid.uuid4())
            fact_digest = attempt_terminal_fact_digest(
                {
                    "attempt_id": attempt_id,
                    "activity_id": activity.activity_id,
                    "attempt_generation": current.generation,
                    "kind": "EXECUTION_DEADLINE",
                    "source_kind": "TIMER_FACT",
                    "source_id": timer_fact.timer_fact_id,
                    "expected_deadline_ms": fired_for_ms,
                    "controller_now_ms": now,
                }
            )
            self.conn.execute(
                "INSERT INTO attempt_terminal_facts(attempt_terminal_fact_id, attempt_id, "
                "activity_id, attempt_generation, kind, source_kind, source_id, "
                "health_observation_id, expected_deadline_ms, controller_now_ms, fact_digest, "
                "recorded_at_ms) "
                "VALUES (?, ?, ?, ?, 'EXECUTION_DEADLINE', 'TIMER_FACT', ?, NULL, ?, ?, ?, ?)",
                (
                    attempt_terminal_fact_id,
                    attempt_id,
                    activity.activity_id,
                    current.generation,
                    timer_fact.timer_fact_id,
                    fired_for_ms,
                    now,
                    fact_digest,
                    now,
                ),
            )
            cur = self.conn.execute(
                "UPDATE attempts SET state = 'EXPIRED', terminal_reason = 'EXECUTION_DEADLINE' "
                "WHERE attempt_id = ? AND state = 'CLAIMED'",
                (attempt_id,),
            )
            if cur.rowcount != 1:
                raise CasMismatchError(
                    "attempt result was accepted between the match check and expiry"
                )
            self.conn.execute(
                "UPDATE activities SET state = 'PLANNED', updated_at_ms = ? "
                "WHERE activity_id = ? AND state = 'ACTIVE'",
                (now, activity.activity_id),
            )

            from orcest.workflow_reducer.ledger import apply, load_view
            from orcest.workflow_reducer.types import Trigger

            view = load_view(self, activity.run_id)
            if view is not None:
                apply(
                    self,
                    view,
                    Trigger(
                        kind="ATTEMPT_TERMINAL",
                        trigger_id=attempt_terminal_fact_id,
                        facts={"kind": "EXECUTION_DEADLINE", "already_terminal": False},
                    ),
                    run_id=activity.run_id,
                )

            return AttemptDeadlineExpiryResult(
                timer_fact_id=timer_fact.timer_fact_id,
                outcome="EXPIRED",
                attempt_terminal_fact_id=attempt_terminal_fact_id,
            )

    def _unfilled_panel_activities(
        self, *, run_id: str, assignment_kind: str, panel_round: int
    ) -> list[ActivityRecord]:
        rows = self.conn.execute(
            "SELECT a.* FROM activities a "
            "JOIN activity_review_assignments r ON r.activity_id = a.activity_id "
            "WHERE a.run_id = ? AND r.assignment_kind = ? AND r.panel_round = ? "
            "AND a.state = 'PLANNED'",
            (run_id, assignment_kind, panel_round),
        ).fetchall()
        return [
            _row_to_activity(
                row, review_assignment=self._get_activity_review_assignment(row["activity_id"])
            )
            for row in rows
        ]

    def resolve_panel_staffing_recheck(
        self,
        *,
        run_id: str,
        assignment_kind: str,
        panel_round: int,
        now_ms: int | None = None,
    ) -> Any | None:
        """Resolve one Run's coalesced panel-staffing recheck, if one is pending.

        Returns the reducer's ``AppliedReduction`` (see
        ``orcest.workflow_reducer.types``) or ``None`` when no recheck is
        pending.

        Applies the reducer's single coalesced ``INTERNAL`` continuation
        (``latest_staffing_recheck_transition_sequence``) with ``staffable``
        computed from the same mode/issuance-key/capacity gates
        :meth:`expire_attempt_claim_deadline` freezes, evaluated against
        *every* still-unfilled Activity in ``(assignment_kind, panel_round)``
        -- never a subset -- so the panel is staffed completely or not at
        all (never a partial restaffing) and a stale/duplicate recheck can
        never re-fire once the reducer's own sequence match has moved on.
        Returns ``None`` when no recheck is pending for this Run.
        """
        enums.parse_enum("activity_review_assignment.assignment_kind", assignment_kind)
        from orcest.workflow_reducer.ledger import apply, load_view
        from orcest.workflow_reducer.types import Trigger

        now = _now_ms() if now_ms is None else now_ms
        with self.transaction():
            view = load_view(self, run_id)
            if view is None or view.latest_staffing_recheck_transition_sequence is None:
                return None
            unfilled = self._unfilled_panel_activities(
                run_id=run_id, assignment_kind=assignment_kind, panel_round=panel_round
            )
            if not unfilled:
                staffable = True
            else:
                replacement_offer_disposition, _gate = self._claim_deadline_offer_gate()
                staffable = replacement_offer_disposition == "OFFER_ALLOWED" and all(
                    self._activity_has_compatible_capacity(activity, now_ms=now)
                    for activity in unfilled
                )
            applied = apply(
                self,
                view,
                Trigger(
                    kind="INTERNAL",
                    trigger_id=str(view.latest_staffing_recheck_transition_sequence),
                    facts={"staffable": staffable, "no_complete_staffing": not staffable},
                ),
                run_id=run_id,
            )
            return applied

    def _activity_has_compatible_capacity(self, activity: ActivityRecord, *, now_ms: int) -> bool:
        """Best-effort capacity check for a not-currently-offered panel slot.

        Reuses the most recent (highest-generation) Attempt this Activity
        ever had for its ``worker_profile`` target -- there is no other
        durable source of an unoffered slot's intended execution profile.
        An Activity that never had any Attempt yet is treated as
        incompatible (fail closed) rather than assumed available.
        """
        row = self.conn.execute(
            "SELECT worker_profile FROM attempts WHERE activity_id = ? "
            "ORDER BY generation DESC LIMIT 1",
            (activity.activity_id,),
        ).fetchone()
        if row is None:
            return False
        observation = self.get_latest_health_observation(
            "WORKER_PROFILE", row["worker_profile"], now_ms=now_ms
        )
        return observation is not None and observation.kind == "AVAILABLE"

    # -- Budget Report ---------------------------------------------------

    def _budget_report_result_from_row(
        self, row: sqlite3.Row, *, replayed: bool
    ) -> BudgetReportResult:
        return BudgetReportResult(
            budget_report_id=row["budget_report_id"],
            project_id=row["project_id"],
            accounting_scope_id=row["accounting_scope_id"],
            source_sequence=row["source_sequence"],
            availability=row["availability"],
            affected_run_ids_digest=row["affected_run_ids_digest"],
            response_http_status=row["response_http_status"],
            response_json=_response_json_with_replayed(row["response_json"], replayed=replayed),
            response_digest=row["response_digest"],
            accepted_at_ms=row["accepted_at_ms"],
            replayed=replayed,
        )

    def submit_budget_report(
        self,
        *,
        budget_report_id: str,
        project_id: str,
        accounting_scope_id: str,
        budget_policy_ref: str,
        budget_reset_window_ref: str,
        window_id: str,
        window_start_ms: int,
        reset_at_ms: int,
        source_sequence: int,
        source_revision: str,
        limit_microunits: int,
        consumed_microunits: int,
        authenticated_principal_id: str,
        authorization_context_digest: str,
        max_budget_report_age_ms: int,
    ) -> BudgetReportResult:
        """Accept (or replay) one cumulative Budget Report.

        ``availability`` is always controller-derived from the normalized
        integers, never trusted from the caller. Freezes, in bytewise Run-ID
        order, every current same-Project ``WAITING``/``BUDGET`` Run and fans
        its wake Transition out immediately (restartable via
        :meth:`run_budget_report_fanout` on crash/Redis-loss reconciliation)
        (domain-model.md "Budget Report").
        """
        require_lowercase_uuid(budget_report_id, field="budget_report_id")
        require_lowercase_uuid(project_id, field="project_id")
        _require_positive_int(source_sequence, field="source_sequence")
        _require_positive_int(limit_microunits, field="limit_microunits")
        _require_positive_int(max_budget_report_age_ms, field="max_budget_report_age_ms")
        if consumed_microunits < 0:
            raise ValueError("consumed_microunits must be nonnegative")
        if window_start_ms >= reset_at_ms:
            raise ValueError("window_start_ms must precede reset_at_ms")
        _require_digest(authorization_context_digest, field="authorization_context_digest")

        req_digest = budget_report_digest(
            {
                "budget_report_id": budget_report_id,
                "project_id": project_id,
                "accounting_scope_id": accounting_scope_id,
                "budget_policy_ref": budget_policy_ref,
                "budget_reset_window_ref": budget_reset_window_ref,
                "window_id": window_id,
                "window_start_ms": window_start_ms,
                "reset_at_ms": reset_at_ms,
                "source_sequence": source_sequence,
                "source_revision": source_revision,
                "limit_microunits": limit_microunits,
                "consumed_microunits": consumed_microunits,
            }
        )

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM budget_reports WHERE budget_report_id = ?",
                (budget_report_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["project_id"] == project_id
                    and existing["accounting_scope_id"] == accounting_scope_id
                    and existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["report_digest"] == req_digest
                ):
                    return self._budget_report_result_from_row(existing, replayed=True)
                raise IdempotencyConflictError("budget report id was reused with different content")

            revision_conflict = self.conn.execute(
                "SELECT 1 FROM budget_reports WHERE project_id = ? AND accounting_scope_id = ? "
                "AND source_revision = ? AND budget_report_id != ?",
                (project_id, accounting_scope_id, source_revision, budget_report_id),
            ).fetchone()
            if revision_conflict is not None:
                raise IdempotencyConflictError(
                    "budget report source_revision was reused under a different budget_report_id"
                )

            last_sequence = self.conn.execute(
                "SELECT COALESCE(MAX(source_sequence), 0) AS seq FROM budget_reports "
                "WHERE project_id = ? AND accounting_scope_id = ?",
                (project_id, accounting_scope_id),
            ).fetchone()["seq"]
            if source_sequence <= int(last_sequence):
                raise CasMismatchError(
                    "budget report source_sequence is not greater than the last accepted sequence"
                )

            accepted_at_ms = _now_ms()
            expires_at_ms = min(reset_at_ms, accepted_at_ms + max_budget_report_age_ms)
            if accepted_at_ms >= expires_at_ms:
                raise ValueError("budget report expires_at_ms must be after accepted_at_ms")
            availability = "AVAILABLE" if consumed_microunits < limit_microunits else "EXHAUSTED"

            member_run_ids = (
                self._budget_wait_matching_run_ids(
                    project_id=project_id,
                    accounting_scope_id=accounting_scope_id,
                    source_sequence=source_sequence,
                )
                if availability == "AVAILABLE"
                else []
            )
            members_digest = affected_run_ids_digest(
                [{"run_id": run_id} for run_id in member_run_ids]
            )

            body = {
                "protocol": BUDGET_REPORT_RESULT_PROTOCOL,
                "budget_report_id": budget_report_id,
                "project_id": project_id,
                "accounting_scope_id": accounting_scope_id,
                "source_sequence": source_sequence,
                "replayed": False,
                "availability": availability,
                "affected_run_ids_digest": members_digest,
            }
            resp_digest = response_digest(
                {"http_status": 200, "body": _response_digest_preimage(body)}
            )
            body_json = canonical_json_text(body)

            self.conn.execute(
                "INSERT INTO budget_reports(budget_report_id, project_id, accounting_scope_id, "
                "budget_policy_ref, budget_reset_window_ref, window_id, window_start_ms, "
                "reset_at_ms, source_sequence, source_revision, limit_microunits, "
                "consumed_microunits, availability, authenticated_principal_id, "
                "authorization_context_digest, report_digest, affected_run_ids_digest, "
                "next_member_ordinal, fanout_completed_at_ms, accepted_at_ms, expires_at_ms, "
                "response_http_status, response_json, response_digest) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, "
                "?, ?)",
                (
                    budget_report_id,
                    project_id,
                    accounting_scope_id,
                    budget_policy_ref,
                    budget_reset_window_ref,
                    window_id,
                    window_start_ms,
                    reset_at_ms,
                    source_sequence,
                    source_revision,
                    limit_microunits,
                    consumed_microunits,
                    availability,
                    authenticated_principal_id,
                    authorization_context_digest,
                    req_digest,
                    members_digest,
                    accepted_at_ms,
                    expires_at_ms,
                    200,
                    body_json,
                    resp_digest,
                ),
            )
            for ordinal, run_id in enumerate(member_run_ids):
                self.conn.execute(
                    "INSERT INTO budget_report_runs(budget_report_id, member_ordinal, run_id) "
                    "VALUES (?, ?, ?)",
                    (budget_report_id, ordinal, run_id),
                )

            self._run_budget_report_fanout(budget_report_id)

            row = self.conn.execute(
                "SELECT * FROM budget_reports WHERE budget_report_id = ?", (budget_report_id,)
            ).fetchone()
            assert row is not None
            return self._budget_report_result_from_row(row, replayed=False)

    def run_budget_report_fanout(self, budget_report_id: str) -> None:
        """Advance one Budget Report's restartable per-Run wake fanout.

        Safe to call repeatedly -- on acceptance, and again from a startup or
        Redis-loss reconciliation sweep after a crash mid-fanout: each member
        Transition is idempotent by trigger identity and the cursor only
        advances transactionally after that Transition commits.
        """
        with self.transaction():
            self._run_budget_report_fanout(budget_report_id)

    def _run_budget_report_fanout(self, budget_report_id: str) -> None:
        """Must run inside ``self.transaction()``."""
        from orcest.workflow_reducer.ledger import load_view

        row = self.conn.execute(
            "SELECT * FROM budget_reports WHERE budget_report_id = ?", (budget_report_id,)
        ).fetchone()
        if row is None:
            raise RunStoreError(f"budget report {budget_report_id!r} was not found")
        if row["fanout_completed_at_ms"] is not None:
            return
        members = self.conn.execute(
            "SELECT member_ordinal, run_id FROM budget_report_runs WHERE budget_report_id = ? "
            "AND member_ordinal >= ? ORDER BY member_ordinal",
            (budget_report_id, row["next_member_ordinal"]),
        ).fetchall()
        for member in members:
            view = load_view(self, member["run_id"])
            if view is not None and view.state == "WAITING":
                self._wake_wait_condition(
                    run_id=member["run_id"],
                    view=view,
                    trigger_kind="BUDGET_REPORT",
                    trigger_id=budget_report_id,
                )
            self.conn.execute(
                "UPDATE budget_reports SET next_member_ordinal = ? WHERE budget_report_id = ?",
                (member["member_ordinal"] + 1, budget_report_id),
            )
        total = self.conn.execute(
            "SELECT COUNT(*) AS n FROM budget_report_runs WHERE budget_report_id = ?",
            (budget_report_id,),
        ).fetchone()["n"]
        cursor = self.conn.execute(
            "SELECT next_member_ordinal AS n FROM budget_reports WHERE budget_report_id = ?",
            (budget_report_id,),
        ).fetchone()["n"]
        if cursor >= total:
            self.conn.execute(
                "UPDATE budget_reports SET fanout_completed_at_ms = ? WHERE budget_report_id = ?",
                (_now_ms(), budget_report_id),
            )

    def get_latest_budget_report(
        self,
        project_id: str,
        accounting_scope_id: str,
        *,
        budget_policy_ref: str | None = None,
        budget_reset_window_ref: str | None = None,
        window_id: str | None = None,
        now_ms: int | None = None,
    ) -> BudgetReportResult | None:
        """The latest applicable Budget Report: greatest accepted sequence for
        the exact current scope/policy/window whose freshness deadline has
        not been reached. An old-window, expired, or policy-mismatched Report
        cannot authorize an offer (domain-model.md "Budget Report")."""
        now = _now_ms() if now_ms is None else now_ms
        query = (
            "SELECT * FROM budget_reports WHERE project_id = ? AND accounting_scope_id = ? "
            "AND expires_at_ms > ?"
        )
        params: list[Any] = [project_id, accounting_scope_id, now]
        if budget_policy_ref is not None:
            query += " AND budget_policy_ref = ?"
            params.append(budget_policy_ref)
        if budget_reset_window_ref is not None:
            query += " AND budget_reset_window_ref = ?"
            params.append(budget_reset_window_ref)
        if window_id is not None:
            query += " AND window_id = ?"
            params.append(window_id)
        query += " ORDER BY source_sequence DESC LIMIT 1"
        row = self.conn.execute(query, params).fetchone()
        return None if row is None else self._budget_report_result_from_row(row, replayed=False)

    # -- Offer gate --------------------------------------------------------

    def evaluate_offer_gate(
        self,
        *,
        worker_profile_scope_id: str | None = None,
        capacity_pool_scope_id: str | None = None,
        provider_account_scope_id: str | None = None,
        project_id: str | None = None,
        accounting_scope_id: str | None = None,
        now_ms: int | None = None,
    ) -> OfferGateEvaluation:
        """Read-only dispatch-gate snapshot for a proposed new Attempt offer.

        See :class:`OfferGateEvaluation`. Omit a scope to skip evaluating that
        dimension; its ``*_health``/``budget_report`` field is then ``None``
        and its ``*_available`` flag is ``False``.
        """
        now = _now_ms() if now_ms is None else now_ms
        evaluation = self._controller_gate_evaluation()
        mode = evaluation.permissions.mode
        if mode not in {"RUNNING", "INTAKE_PAUSED"}:
            disposition = "MODE_BLOCKED"
        elif evaluation.selected_key is None:
            disposition = "ISSUANCE_KEY_UNAVAILABLE"
        else:
            disposition = "OFFER_ALLOWED"

        worker_profile_health = (
            self.get_latest_health_observation(
                "WORKER_PROFILE", worker_profile_scope_id, now_ms=now
            )
            if worker_profile_scope_id is not None
            else None
        )
        capacity_pool_health = (
            self.get_latest_health_observation("CAPACITY_POOL", capacity_pool_scope_id, now_ms=now)
            if capacity_pool_scope_id is not None
            else None
        )
        provider_account_health = (
            self.get_latest_health_observation(
                "PROVIDER_ACCOUNT", provider_account_scope_id, now_ms=now
            )
            if provider_account_scope_id is not None
            else None
        )
        budget_report = (
            self.get_latest_budget_report(project_id, accounting_scope_id, now_ms=now)
            if project_id is not None and accounting_scope_id is not None
            else None
        )

        capacity_available = (
            worker_profile_health is not None
            and worker_profile_health.kind == "AVAILABLE"
            and capacity_pool_health is not None
            and capacity_pool_health.kind == "AVAILABLE"
        )
        budget_available = budget_report is not None and budget_report.availability == "AVAILABLE"

        return OfferGateEvaluation(
            disposition=disposition,
            controller_mode=mode,
            controller_mode_revision=evaluation.permissions.mode_revision,
            capability_registry_revision=evaluation.permissions.registry_revision,
            selected_issuance_key_id=(
                evaluation.selected_key.capability_signing_key_id
                if evaluation.selected_key is not None
                else None
            ),
            worker_profile_health=worker_profile_health,
            capacity_pool_health=capacity_pool_health,
            provider_account_health=provider_account_health,
            budget_report=budget_report,
            capacity_available=capacity_available,
            budget_available=budget_available,
        )

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
