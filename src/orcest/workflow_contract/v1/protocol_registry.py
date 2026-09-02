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
and Diagnosis Result schemas in the planning contract) or are embedded object
shapes rather than standalone envelopes (the launch capability claims); those
are registered with their discriminant field only, as an explicit extension
point for the leaf issue that implements that endpoint -- extending them here
(never by hard-coding the literal elsewhere) is exactly what keeps this the
one registry.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.digest import is_valid_content_digest
from orcest.workflow_contract.v1.identity import is_lowercase_uuid
from orcest.workflow_contract.v1.protocol import (
    Field,
    ProtocolValidationError,
    Schema,
    register_envelope,
)

ATTEMPT_CLAIM_PROTOCOL = "orcest.attempt-claim/1"
ATTEMPT_RESULT_PROTOCOL = "orcest.attempt-result/1"
ATTEMPT_RESULT_ACCEPTED_PROTOCOL = "orcest.attempt-result-accepted/1"
BUDGET_REPORT_RESULT_PROTOCOL = "orcest.budget-report-result/1"
CAPABILITY_KEY_OPERATION_PROTOCOL = "orcest.capability-key-operation/1"
CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL = "orcest.capability-key-operation-result/1"
CANDIDATE_UPLOAD_EXPIRED_PROTOCOL = "orcest.candidate-upload-expired/1"
CAPACITY_REPORT_PROTOCOL = "orcest.capacity-report/1"
CAPACITY_REPORT_RESULT_PROTOCOL = "orcest.capacity-report-result/1"
CONTROLLER_MODE_OPERATION_PROTOCOL = "orcest.controller-mode-operation/1"
CONTROLLER_MODE_RESULT_PROTOCOL = "orcest.controller-mode-result/1"
ERROR_PROTOCOL = "orcest.error/1"
FORGE_OBSERVATION_REQUEST_PROTOCOL = "orcest.forge-observation-request/1"
PLAN_PROTOCOL = "orcest.plan/1"
DIAGNOSIS_PROTOCOL = "orcest.diagnosis/1"
PROJECT_REGISTRATION_PROTOCOL = "orcest.project-registration/1"
PROJECT_REGISTRATION_RESULT_PROTOCOL = "orcest.project-registration-result/1"
SECRET_PROVISION_REQUEST_PROTOCOL = "orcest.secret-provision/1"
SECRET_PROVISION_ACCEPTED_PROTOCOL = "orcest.secret-provision-accepted/1"
SECRET_PROVISION_RESULT_PROTOCOL = "orcest.secret-provision-result/1"
CREDENTIAL_ROTATION_REQUEST_PROTOCOL = "orcest.credential-rotation/1"
CREDENTIAL_ROTATION_RESULT_PROTOCOL = "orcest.credential-rotation-result/1"
WORKER_LOSS_PROTOCOL = "orcest.worker-loss/1"
WORKER_LOSS_RESULT_PROTOCOL = "orcest.worker-loss-result/1"

__all__ = [
    "ATTEMPT_CLAIM_PROTOCOL",
    "ATTEMPT_RESULT_PROTOCOL",
    "ATTEMPT_RESULT_ACCEPTED_PROTOCOL",
    "BUDGET_REPORT_RESULT_PROTOCOL",
    "CAPABILITY_KEY_OPERATION_PROTOCOL",
    "CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL",
    "CANDIDATE_UPLOAD_EXPIRED_PROTOCOL",
    "CAPACITY_REPORT_PROTOCOL",
    "CAPACITY_REPORT_RESULT_PROTOCOL",
    "CONTROLLER_MODE_OPERATION_PROTOCOL",
    "CONTROLLER_MODE_RESULT_PROTOCOL",
    "ERROR_PROTOCOL",
    "FORGE_OBSERVATION_REQUEST_PROTOCOL",
    "PLAN_PROTOCOL",
    "DIAGNOSIS_PROTOCOL",
    "PROJECT_REGISTRATION_PROTOCOL",
    "PROJECT_REGISTRATION_RESULT_PROTOCOL",
    "SECRET_PROVISION_REQUEST_PROTOCOL",
    "SECRET_PROVISION_ACCEPTED_PROTOCOL",
    "SECRET_PROVISION_RESULT_PROTOCOL",
    "CREDENTIAL_ROTATION_REQUEST_PROTOCOL",
    "CREDENTIAL_ROTATION_RESULT_PROTOCOL",
    "WORKER_LOSS_PROTOCOL",
    "WORKER_LOSS_RESULT_PROTOCOL",
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


def _is_positive_int(value: Any) -> None:
    _is_int(value)
    if value < 1:
        raise ValueError("must be a positive integer")


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
    ERROR_PROTOCOL,
    {
        "code": Field(enum=_enum_values(enums.WorkerProtocolErrorCode) | {"CAS_LOST"}),
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
# Project Registration -- request and public result bodies from
# repository-configuration.md / domain-model.md.
# ---------------------------------------------------------------------------

_COMMIT_ID_SCHEMA = Schema(
    fields={
        "object_format": Field(validator=_is_nonempty_str),
        "oid": Field(validator=_is_nonempty_str),
    }
)

_REGISTRATION_FORGE_SCHEMA = Schema(
    fields={
        "adapter_kind": Field(enum=_enum_values(enums.ForgeAdapterKind)),
        "canonical_origin": Field(validator=_is_nonempty_str),
        "installation_or_account_ref": Field(validator=_is_nonempty_str),
        "repository_locator": Field(validator=_is_nonempty_str),
    }
)

_DIAGNOSTIC_SCHEMA = Schema(
    fields={
        "code": Field(validator=_is_nonempty_str),
        "message": Field(validator=_is_str),
        "path": Field(validator=_is_str),
    }
)

_READINESS_SCHEMA = Schema(
    fields={
        "ready": Field(validator=_is_bool),
        "diagnostics": Field(item_schema=_DIAGNOSTIC_SCHEMA),
    }
)


def _project_registration_request_invariant(value: Mapping[str, Any]) -> None:
    project_id = value.get("project_id")
    expected = value.get("expected_registration_revision")
    if project_id is None:
        _require(expected is None, "REGISTER requires expected_registration_revision = null")
    else:
        _require(
            isinstance(expected, int) and not isinstance(expected, bool) and expected >= 1,
            "REVALIDATE requires a positive expected_registration_revision",
        )


def _project_registration_result_invariant(value: Mapping[str, Any]) -> None:
    status = value.get("status")
    success_fields = (
        "project_id",
        "registration_revision",
        "registration_state",
        "forge_instance_id",
        "installation_or_account_ref",
        "repository_external_id",
        "repository_locator",
        "default_ref",
        "trusted_base_commit",
        "workflow_hash",
        "policy_hash",
        "trusted_base_policy_ref",
        "budget_policy_ref",
        "budget_reset_window_ref",
        "readiness",
    )
    if status == "SUCCEEDED":
        _require(value.get("rejection_code") is None, "SUCCEEDED must not carry rejection_code")
        _require(value.get("diagnostics") is None, "SUCCEEDED must not carry diagnostics")
        for field_name in success_fields:
            _require(value.get(field_name) is not None, f"SUCCEEDED requires {field_name}")
    elif status == "REJECTED":
        _require(value.get("rejection_code") is not None, "REJECTED requires a rejection_code")
        _require(value.get("diagnostics") is not None, "REJECTED requires diagnostics")
        for field_name in success_fields:
            _require(value.get(field_name) is None, f"REJECTED must not carry {field_name}")


register_envelope(
    PROJECT_REGISTRATION_PROTOCOL,
    {
        "idempotency_key": Field(validator=_is_uuid),
        "project_id": Field(required=True, nullable=True, validator=_is_uuid),
        "expected_registration_revision": Field(
            required=True, nullable=True, validator=_is_positive_int
        ),
        "forge": Field(schema=_REGISTRATION_FORGE_SCHEMA),
        "requested_default_ref": Field(validator=_is_nonempty_str),
        "trusted_base_policy_ref": Field(validator=_is_nonempty_str),
        "budget_policy_ref": Field(validator=_is_nonempty_str),
        "budget_reset_window_ref": Field(validator=_is_nonempty_str),
    },
    protocol_field="protocol",
    object_validator=_project_registration_request_invariant,
)
register_envelope(
    PROJECT_REGISTRATION_RESULT_PROTOCOL,
    {
        "idempotency_key": Field(validator=_is_uuid),
        "replayed": Field(validator=_is_bool),
        "mode": Field(enum=_enum_values(enums.ProjectRegistrationOperationMode)),
        "status": Field(enum=_enum_values(enums.ProjectRegistrationOperationStatus)),
        "project_id": Field(required=False, nullable=True, validator=_is_uuid),
        "registration_revision": Field(required=False, nullable=True, validator=_is_positive_int),
        "registration_state": Field(
            required=False,
            nullable=True,
            enum=_enum_values(enums.ProjectRegistrationState),
        ),
        "forge_instance_id": Field(required=False, nullable=True, validator=_is_uuid),
        "installation_or_account_ref": Field(
            required=False, nullable=True, validator=_is_nonempty_str
        ),
        "repository_external_id": Field(required=False, nullable=True, validator=_is_nonempty_str),
        "repository_locator": Field(required=False, nullable=True, validator=_is_nonempty_str),
        "default_ref": Field(required=False, nullable=True, validator=_is_nonempty_str),
        "trusted_base_commit": Field(required=False, nullable=True, schema=_COMMIT_ID_SCHEMA),
        "workflow_hash": Field(required=False, nullable=True, validator=_is_digest),
        "policy_hash": Field(required=False, nullable=True, validator=_is_digest),
        "trusted_base_policy_ref": Field(required=False, nullable=True, validator=_is_nonempty_str),
        "budget_policy_ref": Field(required=False, nullable=True, validator=_is_nonempty_str),
        "budget_reset_window_ref": Field(required=False, nullable=True, validator=_is_nonempty_str),
        "readiness": Field(required=False, nullable=True, schema=_READINESS_SCHEMA),
        "rejection_code": Field(
            required=False,
            nullable=True,
            enum=_enum_values(enums.ProjectRegistrationOperationRejectionCode),
        ),
        "diagnostics": Field(required=False, nullable=True, item_schema=_DIAGNOSTIC_SCHEMA),
    },
    protocol_field="protocol",
    object_validator=_project_registration_result_invariant,
)


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
# Budget Report -- the budget-accounting service's own submission wire shape
# is owned by a document not in scope for this issue; the field set below is
# exactly the caller-submitted subset of domain-model.md's "Budget Report"
# ledger (excluding controller-derived ``availability``/times and transport
# authentication, which never travel in the body).
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.budget-report/1",
    {
        "budget_report_id": Field(validator=_is_uuid),
        "project_id": Field(validator=_is_uuid),
        "accounting_scope_id": Field(validator=_is_nonempty_str),
        "budget_policy_ref": Field(validator=_is_nonempty_str),
        "budget_reset_window_ref": Field(validator=_is_nonempty_str),
        "window_id": Field(validator=_is_nonempty_str),
        "window_start_ms": Field(validator=_is_nonneg_int),
        "reset_at_ms": Field(validator=_is_nonneg_int),
        "source_sequence": Field(validator=_is_positive_int),
        "source_revision": Field(validator=_is_nonempty_str),
        "limit_microunits": Field(validator=_is_positive_int),
        "consumed_microunits": Field(validator=_is_nonneg_int),
    },
    protocol_field="protocol",
)
register_envelope(
    BUDGET_REPORT_RESULT_PROTOCOL,
    {
        "budget_report_id": Field(validator=_is_uuid),
        "project_id": Field(validator=_is_uuid),
        "accounting_scope_id": Field(validator=_is_nonempty_str),
        "source_sequence": Field(validator=_is_positive_int),
        "replayed": Field(validator=_is_bool),
        "availability": Field(enum=_enum_values(enums.BudgetReportAvailability)),
        "affected_run_ids_digest": Field(validator=_is_digest),
    },
    protocol_field="protocol",
)


# ---------------------------------------------------------------------------
# Capacity Report
# ---------------------------------------------------------------------------

register_envelope(
    CAPACITY_REPORT_PROTOCOL,
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
    CAPACITY_REPORT_RESULT_PROTOCOL,
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
    WORKER_LOSS_PROTOCOL,
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
    WORKER_LOSS_RESULT_PROTOCOL,
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
    CANDIDATE_UPLOAD_EXPIRED_PROTOCOL,
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
    CREDENTIAL_ROTATION_RESULT_PROTOCOL,
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
register_envelope(CREDENTIAL_ROTATION_REQUEST_PROTOCOL, {})


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
# Structured-output payload protocols and controller-operation input refs.
# ---------------------------------------------------------------------------

_PLAN_ITEM_SCHEMA = Schema(
    {
        "id": Field(validator=_is_nonempty_str),
        "summary": Field(validator=_is_nonempty_str),
        "depends_on": Field(item_schema=Schema({"id": Field(validator=_is_nonempty_str)})),
    }
)
_PLAN_STEP_SCHEMA = Schema(
    {
        "id": Field(validator=_is_nonempty_str),
        "summary": Field(validator=_is_nonempty_str),
        "requirement_ids": Field(item_schema=Schema({"id": Field(validator=_is_nonempty_str)})),
        "depends_on": Field(item_schema=Schema({"id": Field(validator=_is_nonempty_str)})),
        "verification_ids": Field(item_schema=Schema({"id": Field(validator=_is_nonempty_str)})),
    }
)
_PLAN_VERIFICATION_SCHEMA = Schema(
    {
        "id": Field(validator=_is_nonempty_str),
        "summary": Field(validator=_is_nonempty_str),
        "command_ids": Field(item_schema=Schema({"id": Field(validator=_is_nonempty_str)})),
    }
)


def _field_id_list(value: Mapping[str, Any], field_name: str) -> list[str]:
    items = value.get(field_name)
    if not isinstance(items, list):
        raise ProtocolValidationError(f"{field_name} must be an array")
    ids: list[str] = []
    for item in items:
        if not isinstance(item, Mapping) or not isinstance(item.get("id"), str):
            raise ProtocolValidationError(f"{field_name} members must be id objects")
        ids.append(str(item["id"]))
    return ids


def _assert_unique(ids: list[str], field_name: str) -> None:
    if len(ids) != len(set(ids)):
        raise ProtocolValidationError(f"{field_name} contains duplicate ids")


def _assert_refs_known(refs: list[str], known: set[str], field_name: str) -> None:
    missing = sorted(set(refs) - known)
    if missing:
        raise ProtocolValidationError(f"{field_name} references unknown ids {missing!r}")


def _assert_acyclic(edges: Mapping[str, list[str]], field_name: str) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise ProtocolValidationError(f"{field_name} contains a cycle")
        visiting.add(node)
        for dependency in edges[node]:
            visit(dependency)
        visiting.remove(node)
        visited.add(node)

    for node in sorted(edges):
        visit(node)


def _plan_invariant(value: Mapping[str, Any]) -> None:
    requirements = value.get("requirements")
    steps = value.get("steps")
    verifications = value.get("verification_mapping")
    if not isinstance(requirements, list) or not requirements:
        raise ProtocolValidationError("requirements must be a non-empty array")
    if not isinstance(steps, list) or not steps:
        raise ProtocolValidationError("steps must be a non-empty array")
    if not isinstance(verifications, list) or not verifications:
        raise ProtocolValidationError("verification_mapping must be a non-empty array")
    requirement_ids = _field_id_list(value, "requirements")
    step_ids = _field_id_list(value, "steps")
    verification_ids = _field_id_list(value, "verification_mapping")
    _assert_unique(requirement_ids, "requirements")
    _assert_unique(step_ids, "steps")
    _assert_unique(verification_ids, "verification_mapping")
    requirement_set = set(requirement_ids)
    step_set = set(step_ids)
    verification_set = set(verification_ids)
    requirement_edges: dict[str, list[str]] = {}
    for requirement in requirements:
        assert isinstance(requirement, Mapping)
        deps = [str(item["id"]) for item in requirement["depends_on"]]
        _assert_refs_known(deps, requirement_set, "requirements.depends_on")
        requirement_edges[str(requirement["id"])] = deps
    step_edges: dict[str, list[str]] = {}
    covered_requirements: set[str] = set()
    covered_verifications: set[str] = set()
    for step in steps:
        assert isinstance(step, Mapping)
        deps = [str(item["id"]) for item in step["depends_on"]]
        reqs = [str(item["id"]) for item in step["requirement_ids"]]
        checks = [str(item["id"]) for item in step["verification_ids"]]
        _assert_refs_known(deps, step_set, "steps.depends_on")
        _assert_refs_known(reqs, requirement_set, "steps.requirement_ids")
        _assert_refs_known(checks, verification_set, "steps.verification_ids")
        step_edges[str(step["id"])] = deps
        covered_requirements.update(reqs)
        covered_verifications.update(checks)
    if covered_requirements != requirement_set:
        missing = sorted(requirement_set - covered_requirements)
        raise ProtocolValidationError(f"requirements not mapped to steps {missing!r}")
    if covered_verifications != verification_set:
        missing = sorted(verification_set - covered_verifications)
        raise ProtocolValidationError(f"verification_mapping not mapped to steps {missing!r}")
    _assert_acyclic(requirement_edges, "requirements.depends_on")
    _assert_acyclic(step_edges, "steps.depends_on")


register_envelope(
    PLAN_PROTOCOL,
    {
        "plan_id": Field(validator=_is_uuid),
        "snapshot_id": Field(validator=_is_uuid),
        "policy_hash": Field(validator=_is_digest),
        "requirements": Field(item_schema=_PLAN_ITEM_SCHEMA),
        "steps": Field(item_schema=_PLAN_STEP_SCHEMA),
        "verification_mapping": Field(item_schema=_PLAN_VERIFICATION_SCHEMA),
        "notes": Field(required=False, validator=_is_str),
    },
    object_validator=_plan_invariant,
)

_DIAGNOSIS_FINDING_SCHEMA = Schema(
    {
        "id": Field(validator=_is_nonempty_str),
        "category": Field(
            enum=frozenset(
                {
                    "NO_PROGRESS",
                    "REPEATED_FAILURE",
                    "CONFLICTING_EVIDENCE",
                    "INVALID_PLAN",
                    "TRANSIENT_INFRASTRUCTURE",
                }
            )
        ),
        "summary": Field(validator=_is_nonempty_str),
        "evidence_refs": Field(item_schema=Schema({"id": Field(validator=_is_nonempty_str)})),
    }
)


def _diagnosis_invariant(value: Mapping[str, Any]) -> None:
    findings = value.get("findings")
    if not isinstance(findings, list) or not findings:
        raise ProtocolValidationError("findings must be a non-empty array")
    finding_ids = _field_id_list(value, "findings")
    _assert_unique(finding_ids, "findings")


register_envelope(
    DIAGNOSIS_PROTOCOL,
    {
        "diagnosis_id": Field(validator=_is_uuid),
        "snapshot_id": Field(validator=_is_uuid),
        "candidate_id": Field(required=False, nullable=True, validator=_is_uuid),
        "policy_hash": Field(validator=_is_digest),
        "findings": Field(item_schema=_DIAGNOSIS_FINDING_SCHEMA),
        "recommended_tactic": Field(
            enum=frozenset({"RETRY_EXECUTION", "REPAIR", "REBASE", "REPLAN", "WAIT_EVIDENCE"})
        ),
        "summary": Field(validator=_is_nonempty_str),
    },
    object_validator=_diagnosis_invariant,
)
register_envelope("orcest.redundant-publication-cleanup/1", {})
register_envelope("orcest.run-marker-repair/1", {})
register_envelope(FORGE_OBSERVATION_REQUEST_PROTOCOL, {})


# ---------------------------------------------------------------------------
# Redis activity offer (worker-protocol.md "Redis activity offer")
# ---------------------------------------------------------------------------

register_envelope(
    "orcest.activity-offer/1",
    {
        "protocol_version": Field(enum=frozenset({"1"})),
        "redis_epoch": Field(validator=_is_positive_int),
        "outbox_id": Field(validator=_is_uuid),
        "attempt_id": Field(validator=_is_uuid),
        "activity_id": Field(validator=_is_uuid),
        "generation": Field(validator=_is_positive_int),
        "worker_profile": Field(validator=_is_nonempty_str),
        "claim_deadline_ms": Field(validator=_is_nonneg_int),
    },
    protocol_field="protocol",
)
