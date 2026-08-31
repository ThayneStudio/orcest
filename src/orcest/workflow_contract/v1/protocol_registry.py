"""Concrete Workflow-Control v1 protocol envelope registrations.

This is the ONLY file permitted to hard-code an ``orcest.<name>/<n>``
protocol-version literal (enforced by
``tests/workflow_contract/test_no_shadow_contracts.py``). Every other
component imports the literal and its schema from here via
:mod:`orcest.workflow_contract.v1.protocol`.

Coverage: every protocol literal found in the normative wiki is registered
so that :func:`orcest.workflow_contract.v1.protocol.known_protocol_literals`
is the complete closed v1 set and an unknown literal always fails closed.
Literals whose full wire schema is spelled out in the wiki (worker-protocol
request/response tables, or an explicit domain-model.md terminal-body
description/JSON example) are given a complete field schema. A handful of
literals are owned by another document not in scope for this issue (the Plan
and Diagnosis Result schemas in the planning contract, the Project
Registration success body in the repository-configuration contract) or are
embedded object shapes rather than standalone envelopes (the launch
capability claims); those are registered with their discriminant field only,
as an explicit extension point for the leaf issue that implements that
endpoint -- extending them here (never by hard-coding the literal elsewhere)
is exactly what keeps this the one registry.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.digest import is_valid_content_digest
from orcest.workflow_contract.v1.identity import is_lowercase_uuid
from orcest.workflow_contract.v1.protocol import Field, ProtocolValidationError, register_envelope

CAPABILITY_KEY_OPERATION_PROTOCOL = "orcest.capability-key-operation/1"
CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL = "orcest.capability-key-operation-result/1"
CONTROLLER_MODE_OPERATION_PROTOCOL = "orcest.controller-mode-operation/1"
CONTROLLER_MODE_RESULT_PROTOCOL = "orcest.controller-mode-result/1"

__all__ = [
    "CAPABILITY_KEY_OPERATION_PROTOCOL",
    "CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL",
    "CONTROLLER_MODE_OPERATION_PROTOCOL",
    "CONTROLLER_MODE_RESULT_PROTOCOL",
]


def _is_bool(value: Any) -> None:
    if not isinstance(value, bool):
        raise ValueError("must be a boolean")


def _is_int(value: Any) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("must be an integer")


def _is_nonneg_int(value: Any) -> None:
    _is_int(value)
    if value < 0:
        raise ValueError("must be a nonnegative integer")


def _is_str(value: Any) -> None:
    if not isinstance(value, str):
        raise ValueError("must be a string")


def _is_nonempty_str(value: Any) -> None:
    _is_str(value)
    if not value:
        raise ValueError("must be a non-empty string")


def _is_uuid(value: Any) -> None:
    if not is_lowercase_uuid(value):
        raise ValueError("must be a lowercase canonical UUID")


def _is_digest(value: Any) -> None:
    if not is_valid_content_digest(value):
        raise ValueError("must be a sha256:<64 hex> content digest")


def _enum_values(enum_cls: Any) -> frozenset[str]:
    return frozenset(member.value for member in enum_cls)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ProtocolValidationError(message)


# ---------------------------------------------------------------------------
# Generic error envelope
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.error/1",
    {
        "code": Field(enum=_enum_values(enums.WorkerProtocolErrorCode) | {"ATTEMPT_STALE"}),
        "retryable": Field(validator=_is_bool),
        "message": Field(required=False, validator=_is_str),
        "retry_after_seconds": Field(required=False, nullable=True, validator=_is_nonneg_int),
        "attempt_id": Field(required=False, validator=_is_uuid),
        "current_attempt_generation": Field(
            required=False, nullable=True, validator=_is_nonneg_int
        ),
    },
    protocol_field="protocol",
)


# ---------------------------------------------------------------------------
# Controller Mode Operation
# ---------------------------------------------------------------------------


def _controller_mode_result_invariant(value: Mapping[str, Any]) -> None:
    status = value.get("status")
    if status == "SUCCEEDED":
        _require(
            value.get("mode_revision") is not None, "SUCCEEDED requires non-null mode_revision"
        )
        _require(value.get("mode") is not None, "SUCCEEDED requires non-null mode")
        _require(value.get("rejection_code") is None, "SUCCEEDED must not carry rejection_code")
    elif status == "REJECTED":
        _require(value.get("rejection_code") is not None, "REJECTED requires a rejection_code")
        for field_name in ("mode_revision", "mode", "dispatch_paused_intake_policy"):
            _require(value.get(field_name) is None, f"REJECTED must not carry {field_name}")


register_envelope(
    CONTROLLER_MODE_RESULT_PROTOCOL,
    {
        "controller_mode_operation_id": Field(validator=_is_uuid),
        "operation_kind": Field(enum=_enum_values(enums.ControllerModeOperationKind)),
        "status": Field(enum=_enum_values(enums.ControllerModeOperationStatus)),
        "replayed": Field(validator=_is_bool),
        "mode_revision": Field(required=False, nullable=True, validator=_is_nonneg_int),
        "mode": Field(required=False, nullable=True, enum=_enum_values(enums.ControllerMode)),
        "dispatch_paused_intake_policy": Field(
            required=False, nullable=True, enum=_enum_values(enums.DispatchPausedIntakePolicy)
        ),
        "rejection_code": Field(
            required=False,
            nullable=True,
            enum=_enum_values(enums.ControllerModeOperationRejectionCode),
        ),
    },
    object_validator=_controller_mode_result_invariant,
)
register_envelope(CONTROLLER_MODE_OPERATION_PROTOCOL, {})


# ---------------------------------------------------------------------------
# Capability Key Operation
# ---------------------------------------------------------------------------


def _capability_key_result_invariant(value: Mapping[str, Any]) -> None:
    status = value.get("status")
    if status == "SUCCEEDED":
        _require(
            value.get("registry_revision") is not None,
            "SUCCEEDED requires non-null registry_revision",
        )
        _require(value.get("rejection_code") is None, "SUCCEEDED must not carry rejection_code")
    elif status == "REJECTED":
        _require(value.get("rejection_code") is not None, "REJECTED requires a rejection_code")
        _require(
            value.get("registry_revision") is None, "REJECTED must not carry registry_revision"
        )
        _require(
            value.get("current_issuance_key_id") is None,
            "REJECTED must not carry current_issuance_key_id",
        )


register_envelope(
    CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL,
    {
        "capability_key_operation_id": Field(validator=_is_uuid),
        "kind": Field(enum=_enum_values(enums.CapabilityKeyOperationKind)),
        "status": Field(enum=_enum_values(enums.CapabilityKeyOperationStatus)),
        "replayed": Field(validator=_is_bool),
        "registry_revision": Field(required=False, nullable=True, validator=_is_nonneg_int),
        "current_issuance_key_id": Field(required=False, nullable=True, validator=_is_uuid),
        "rejection_code": Field(
            required=False,
            nullable=True,
            enum=_enum_values(enums.CapabilityKeyOperationRejectionCode),
        ),
    },
    object_validator=_capability_key_result_invariant,
)
register_envelope(CAPABILITY_KEY_OPERATION_PROTOCOL, {})


# ---------------------------------------------------------------------------
# Project Registration -- REJECTED body is exact; SUCCEEDED body is owned by
# the repository-configuration contract (not fetched by this issue).
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.project-registration-result/1",
    {
        "idempotency_key": Field(validator=_is_uuid),
        "mode": Field(enum=_enum_values(enums.ProjectRegistrationOperationMode)),
        "status": Field(enum=_enum_values(enums.ProjectRegistrationOperationStatus)),
        "replayed": Field(validator=_is_bool),
        "rejection_code": Field(
            required=False,
            nullable=True,
            enum=_enum_values(enums.ProjectRegistrationOperationRejectionCode),
        ),
        "diagnostics": Field(required=False, nullable=True),
    },
)
register_envelope("orcest.project-registration/1", {})


# ---------------------------------------------------------------------------
# Attempt Claim (worker-protocol.md request/response)
# ---------------------------------------------------------------------------

_WORKER_IDENTITY_SCHEMA_FIELDS = {
    "worker_id": Field(validator=_is_nonempty_str),
    "worker_session_id": Field(validator=_is_nonempty_str),
    "worker_profile": Field(validator=_is_nonempty_str),
    "build_revision": Field(validator=_is_nonempty_str),
}

register_envelope(
    "orcest.attempt-claim/1",
    {
        "attempt_claim_id": Field(validator=_is_uuid),
        "redis_epoch": Field(validator=_is_nonneg_int),
        "outbox_id": Field(validator=_is_nonempty_str),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "worker": Field(),
    },
    protocol_field="protocol",
)
register_envelope("orcest.attempt-claim-result/1", {})
register_envelope("orcest.launch-capability/1", {})


# ---------------------------------------------------------------------------
# Launch Attestation / Launch Accepted
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.launch-attestation/1",
    {
        "launch_attestation_id": Field(validator=_is_uuid),
        "attempt_id": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "attempt_generation": Field(validator=_is_nonneg_int),
        "worker_id": Field(validator=_is_nonempty_str),
        "worker_session_id": Field(validator=_is_nonempty_str),
        "pool_manager_id": Field(validator=_is_nonempty_str),
        "runner_principal_id": Field(validator=_is_nonempty_str),
        "runner_image_digest": Field(validator=_is_digest),
        "runner_registration_revision": Field(validator=_is_nonneg_int),
        "launch_nonce_id": Field(required=False, nullable=True, validator=_is_uuid),
        "launch_capability_digest": Field(required=False, nullable=True, validator=_is_digest),
        "launch_capability_signing_key_id": Field(
            required=False, nullable=True, validator=_is_uuid
        ),
        "launch_capability_signature_algorithm": Field(
            required=False, nullable=True, enum=_enum_values(enums.SignatureAlgorithm)
        ),
        "workspace_instance_id": Field(validator=_is_uuid),
        "context_instance_id": Field(validator=_is_uuid),
        "invocation_instance_id": Field(validator=_is_uuid),
        "workspace_parent_id": Field(nullable=True),
        "context_parent_id": Field(nullable=True),
        "invocation_parent_id": Field(nullable=True),
        "fresh_workspace": Field(validator=_is_bool),
        "fresh_context": Field(validator=_is_bool),
        "fresh_invocation": Field(validator=_is_bool),
        "prepared_at_ms": Field(validator=_is_nonneg_int),
        "attested_at_ms": Field(validator=_is_nonneg_int),
        "runner_signing_key_id": Field(validator=_is_nonempty_str),
        "runner_signature_algorithm": Field(enum=_enum_values(enums.SignatureAlgorithm)),
        "attestation_digest": Field(validator=_is_digest),
        "signature": Field(validator=_is_nonempty_str),
    },
)


def _launch_accepted_invariant(value: Mapping[str, Any]) -> None:
    status = value.get("status")
    provider = value.get("provider")
    if status == "AVAILABLE":
        _require(provider is not None, "AVAILABLE requires a non-null provider")
    elif status == "EXPIRED":
        _require(provider is None, "EXPIRED must carry a null provider")


register_envelope(
    "orcest.launch-accepted/1",
    {
        "launch_attestation_id": Field(validator=_is_uuid),
        "attempt_id": Field(validator=_is_uuid),
        "status": Field(enum=_enum_values(enums.LaunchAcceptedStatus)),
        "provider": Field(nullable=True),
    },
    protocol_field="protocol",
    object_validator=_launch_accepted_invariant,
)


# ---------------------------------------------------------------------------
# Capacity Report
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.capacity-report/1",
    {
        "idempotency_key": Field(validator=_is_uuid),
        "report_id": Field(validator=_is_uuid),
        "report_sequence": Field(validator=_is_nonneg_int),
        "observed_at_ms": Field(validator=_is_nonneg_int),
        "expires_at_ms": Field(validator=_is_nonneg_int),
        "observations": Field(),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.capacity-report-result/1",
    {
        "capacity_report_id": Field(validator=_is_uuid),
        "report_id": Field(validator=_is_uuid),
        "report_sequence": Field(validator=_is_nonneg_int),
        "replayed": Field(validator=_is_bool),
        "health_observations": Field(),
        "woken_wait_condition_ids": Field(),
    },
    protocol_field="protocol",
)


# ---------------------------------------------------------------------------
# Worker Loss Report
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.worker-loss/1",
    {
        "idempotency_key": Field(validator=_is_uuid),
        "worker_session_id": Field(validator=_is_nonempty_str),
        "attempt_id": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "observed_at_ms": Field(validator=_is_nonneg_int),
        "reason": Field(enum=_enum_values(enums.WorkerLossReason)),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.worker-loss-result/1",
    {
        "worker_loss_report_id": Field(validator=_is_uuid),
        "attempt_id": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "accepted": Field(validator=_is_bool),
        "stale": Field(validator=_is_bool),
        "replayed": Field(validator=_is_bool),
        "health_observation_id": Field(required=False, nullable=True, validator=_is_uuid),
        "attempt_terminal_fact_id": Field(required=False, nullable=True, validator=_is_uuid),
    },
    protocol_field="protocol",
)


# ---------------------------------------------------------------------------
# Candidate Upload
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.candidate-upload-create/1",
    {
        "idempotency_key": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "media_type": Field(validator=_is_nonempty_str),
        "declared_bytes": Field(validator=_is_nonneg_int),
        "declared_digest": Field(validator=_is_digest),
        "proposed_tip": Field(nullable=True),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.candidate-upload-create-result/1",
    {
        "upload_id": Field(validator=_is_uuid),
        "state": Field(enum=frozenset({"RECEIVING"})),
        "upload_url": Field(validator=_is_nonempty_str),
        "expires_at_ms": Field(validator=_is_nonneg_int),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.candidate-upload-result/1",
    {
        "upload_id": Field(validator=_is_uuid),
        "state": Field(enum=_enum_values(enums.CandidateUploadState)),
        "computed_digest": Field(validator=_is_digest),
        "computed_bytes": Field(validator=_is_nonneg_int),
        "verified_tip": Field(required=False, nullable=True),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.candidate-upload-expired/1",
    {
        "upload_id": Field(validator=_is_uuid),
        "state": Field(enum=frozenset({"EXPIRED"})),
        "code": Field(enum=frozenset({"UPLOAD_EXPIRED"})),
        "expires_at_ms": Field(validator=_is_nonneg_int),
    },
    protocol_field="protocol",
)


# ---------------------------------------------------------------------------
# Attempt Result
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.attempt-result/1",
    {
        "idempotency_key": Field(validator=_is_uuid),
        "attempt_id": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "launch_attestation_id": Field(required=False, nullable=True, validator=_is_uuid),
        "outcome": Field(enum=_enum_values(enums.AttemptResultOutcome)),
        "candidate_upload_id": Field(required=False, nullable=True, validator=_is_uuid),
        "receipt": Field(required=False, nullable=True),
        "structured_output": Field(required=False, nullable=True),
        "failure": Field(required=False, nullable=True),
        "summary": Field(required=False, nullable=True, validator=_is_str),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.attempt-result-accepted/1",
    {
        "attempt_id": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "outcome": Field(enum=_enum_values(enums.AttemptResultOutcome)),
        "candidate_id": Field(required=False, nullable=True, validator=_is_uuid),
        "receipt_id": Field(required=False, nullable=True, validator=_is_uuid),
        "replayed": Field(validator=_is_bool),
    },
    protocol_field="protocol",
)


# ---------------------------------------------------------------------------
# Credential Rotation
# ---------------------------------------------------------------------------


def _credential_rotation_result_invariant(value: Mapping[str, Any]) -> None:
    disposition = value.get("disposition")
    if disposition == "APPLIED":
        _require(value.get("accepted_version") is not None, "APPLIED requires accepted_version")
        _require(
            value.get("credential_rotation_receipt_id") is not None,
            "APPLIED requires credential_rotation_receipt_id",
        )
    elif disposition == "CAS_LOST":
        _require(value.get("accepted_version") is None, "CAS_LOST must not carry accepted_version")
        _require(
            value.get("credential_rotation_receipt_id") is None,
            "CAS_LOST must not carry credential_rotation_receipt_id",
        )


register_envelope(
    "orcest.credential-rotation-result/1",
    {
        "credential_rotation_request_id": Field(validator=_is_uuid),
        "disposition": Field(enum=_enum_values(enums.CredentialRotationDisposition)),
        "secret_id": Field(validator=_is_uuid),
        "expected_prior_version": Field(validator=_is_nonneg_int),
        "current_version": Field(validator=_is_nonneg_int),
        "accepted_version": Field(required=False, nullable=True, validator=_is_nonneg_int),
        "credential_rotation_receipt_id": Field(required=False, nullable=True, validator=_is_uuid),
    },
    protocol_field="protocol",
    object_validator=_credential_rotation_result_invariant,
)
register_envelope("orcest.credential-rotation/1", {})


# ---------------------------------------------------------------------------
# Attempt Liveness
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.attempt-liveness/1",
    {
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "sequence": Field(validator=_is_nonneg_int),
        "observed_at_ms": Field(validator=_is_nonneg_int),
        "state": Field(enum=_enum_values(enums.LivenessState)),
        "progress": Field(required=False, nullable=True),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.attempt-liveness-result/1",
    {
        "attempt_id": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_nonneg_int),
        "control": Field(enum=_enum_values(enums.LivenessControl)),
        "execution_deadline_ms": Field(validator=_is_nonneg_int),
        "liveness_recorded": Field(validator=_is_bool),
    },
    protocol_field="protocol",
)


# ---------------------------------------------------------------------------
# Golden-example-only envelopes (domain-model.md worked JSON bodies)
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.evidence-wake/1",
    {
        "project_id": Field(validator=_is_uuid),
        "target_kind": Field(enum=frozenset({"WORK_ITEM", "PUBLICATION"})),
        "target_id": Field(validator=_is_nonempty_str),
        "minimum_observation_sequence": Field(validator=_is_nonneg_int),
        "allowed_observation_kinds": Field(),
        "predicate_digest": Field(validator=_is_digest),
    },
    protocol_field="protocol",
)
register_envelope(
    "orcest.storage-restoration-accepted/1",
    {
        "operation_id": Field(validator=_is_uuid),
        "state": Field(enum=frozenset({"PENDING"})),
        "object_kind": Field(enum=_enum_values(enums.StorageRestorationFactObjectKind)),
        "object_id": Field(validator=_is_nonempty_str),
    },
    protocol_field="protocol",
)
register_envelope("orcest.storage-restoration-result/1", {})
register_envelope("orcest.storage-management/1", {})
register_envelope(
    "orcest.management-result/1",
    {
        "command_id": Field(validator=_is_uuid),
        "run_id": Field(validator=_is_uuid),
        "kind": Field(enum=_enum_values(enums.ManagementCommandKind)),
        "outcome": Field(validator=_is_nonempty_str),
        "result_transition_sequence": Field(
            required=False, nullable=True, validator=_is_nonneg_int
        ),
        "human_resolution_id": Field(required=False, nullable=True, validator=_is_uuid),
        "replayed": Field(validator=_is_bool),
    },
    protocol_field="protocol",
)
register_envelope("orcest.management/1", {})
register_envelope(
    "orcest.secret-provision-accepted/1",
    {
        "secret_provision_operation_id": Field(validator=_is_uuid),
        "state": Field(enum=frozenset({"PENDING"})),
        "secret_id": Field(validator=_is_uuid),
        "target_version": Field(validator=_is_nonneg_int),
    },
    protocol_field="protocol",
)


def _secret_provision_result_invariant(value: Mapping[str, Any]) -> None:
    state = value.get("state")
    if state == "COMPLETED":
        for field_name in ("secret_version_key", "new_version", "credential_rotation_receipt_id"):
            _require(value.get(field_name) is not None, f"COMPLETED requires {field_name}")
        _require(value.get("rejection_code") is None, "COMPLETED must not carry rejection_code")
    elif state == "REJECTED":
        _require(value.get("rejection_code") is not None, "REJECTED requires rejection_code")
        for field_name in ("secret_version_key", "new_version", "credential_rotation_receipt_id"):
            _require(value.get(field_name) is None, f"REJECTED must not carry {field_name}")


register_envelope(
    "orcest.secret-provision-result/1",
    {
        "secret_provision_operation_id": Field(validator=_is_uuid),
        "state": Field(enum=frozenset({"COMPLETED", "REJECTED"})),
        "secret_id": Field(validator=_is_uuid),
        "target_version": Field(validator=_is_nonneg_int),
        "secret_version_key": Field(required=False, nullable=True, validator=_is_nonempty_str),
        "new_version": Field(required=False, nullable=True, validator=_is_nonneg_int),
        "credential_rotation_receipt_id": Field(required=False, nullable=True, validator=_is_uuid),
        "rejection_code": Field(
            required=False,
            nullable=True,
            enum=_enum_values(enums.SecretProvisionOperationRejectionCode),
        ),
    },
    protocol_field="protocol",
    object_validator=_secret_provision_result_invariant,
)
register_envelope("orcest.secret-provision/1", {})


# ---------------------------------------------------------------------------
# Structured-output payload protocols and controller-operation input refs --
# full schemas are owned by the planning contract / other leaf issues.
# ---------------------------------------------------------------------------

register_envelope("orcest.plan/1", {})
register_envelope("orcest.diagnosis/1", {})
register_envelope("orcest.redundant-publication-cleanup/1", {})
register_envelope("orcest.run-marker-repair/1", {})
register_envelope("orcest.forge-observation-request/1", {})
register_envelope("orcest.activity-offer/1", {}, protocol_field="protocol")
