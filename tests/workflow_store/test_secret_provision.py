"""Idempotent MANAGEMENT_PROVISION secret provision/adoption operations."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest
from orcest.workflow_store import (
    IdempotencyConflictError,
    RunStore,
    RunStoreError,
)
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.secret_provision import (
    SecretProvisionReplayConflictError,
    provision_or_adopt_secret,
    reconcile_pending_secret_provision_operation,
)
from orcest.workflow_store.v1.secrets import SecretStore

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64
PRINCIPAL = "operator-1"
INSTALLATION = "installation-1"


@pytest.fixture
def run_store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


@pytest.fixture
def secret_store(tmp_path: Path) -> SecretStore:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    quota = QuotaConfig(
        min_free_bytes=0,
        max_object_bytes=1024 * 1024,
        max_store_bytes=8 * 1024 * 1024,
        max_objects=1024,
    )
    lock = StorageLock(layout.storage_lock_path)
    return SecretStore(layout, quota=quota, lock=lock)


def _select_capability_key(store: RunStore) -> None:
    key_id = str(uuid.uuid4())
    public_key = bytes([9]) * 32
    result = store.apply_capability_key_operation(
        capability_key_operation_id=str(uuid.uuid4()),
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        register_public_verification_key=public_key,
        register_public_key_digest=capability_public_key_digest(public_key),
        register_private_signing_secret_ref="bootstrap:0",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"
    result = store.apply_capability_key_operation(
        capability_key_operation_id=str(uuid.uuid4()),
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _provision(
    run_store: RunStore,
    secret_store: SecretStore,
    *,
    operation_id: str,
    secret_id: str,
    secret_bytes: bytes,
    expected_prior_version: int | None = None,
    purpose: str = "FORGE_API",
    owner_scope_kind: str = "FORGE_INSTALLATION",
    owner_scope_id: str = INSTALLATION,
    provider_account_ref: str | None = INSTALLATION,
    mode: str = "PROVISION",
    authority_revoked: bool = False,
):
    return provision_or_adopt_secret(
        run_store,
        secret_store,
        secret_provision_operation_id=operation_id,
        mode=mode,
        secret_id=secret_id,
        expected_prior_version=expected_prior_version,
        purpose=purpose,
        owner_scope_kind=owner_scope_kind,
        owner_scope_id=owner_scope_id,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_bytes=secret_bytes,
        provider_account_ref=provider_account_ref,
        authority_revoked=authority_revoked,
    )


def test_provision_creates_version_receipt_and_current_pointer(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())

    result = _provision(
        run_store, secret_store, operation_id=op_id, secret_id=secret_id, secret_bytes=b"v1-bytes"
    )

    assert result.state == "COMPLETED"
    assert result.new_version == 1
    assert result.response_http_status == 200
    assert b"v1-bytes" not in result.response_json.encode()
    assert result.credential_rotation_receipt_id is not None

    version = run_store.get_secret_version(secret_id, 1)
    assert version is not None
    assert version.creation_receipt_id == result.credential_rotation_receipt_id

    receipt = run_store.get_credential_rotation_receipt(result.credential_rotation_receipt_id)
    assert receipt is not None
    assert receipt.source_kind == "MANAGEMENT_PROVISION"
    assert receipt.management_operation_id == op_id
    assert receipt.new_version == 1

    current = run_store.get_secret_current_version(secret_id)
    assert current is not None
    assert current.current_version == 1
    assert current.last_operation_id == op_id

    assert secret_store.read_value(secret_id, 1) == b"v1-bytes"


def test_identical_replay_returns_same_response(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())

    first = _provision(
        run_store, secret_store, operation_id=op_id, secret_id=secret_id, secret_bytes=b"same"
    )
    second = _provision(
        run_store, secret_store, operation_id=op_id, secret_id=secret_id, secret_bytes=b"same"
    )

    assert second.replayed is True
    assert second.response_json == first.response_json
    assert second.response_digest == first.response_digest
    assert second.new_version == first.new_version


def test_replay_with_different_bytes_conflicts_without_leaking_either_value(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    _provision(
        run_store, secret_store, operation_id=op_id, secret_id=secret_id, secret_bytes=b"correct"
    )

    with pytest.raises(SecretProvisionReplayConflictError) as excinfo:
        _provision(
            run_store,
            secret_store,
            operation_id=op_id,
            secret_id=secret_id,
            secret_bytes=b"wrong-bytes",
        )
    assert b"correct" not in str(excinfo.value).encode()
    assert b"wrong-bytes" not in str(excinfo.value).encode()


def test_begin_rejects_reused_operation_id_with_different_non_secret_fields(
    run_store: RunStore,
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    run_store.begin_secret_provision_operation(
        secret_provision_operation_id=op_id,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=str(uuid.uuid4()),
        secret_integrity_attestation_id=str(uuid.uuid4()),
    )
    with pytest.raises(IdempotencyConflictError):
        run_store.begin_secret_provision_operation(
            secret_provision_operation_id=op_id,
            mode="PROVISION",
            secret_id=secret_id,
            expected_prior_version=None,
            purpose="SOURCE_READ",  # different non-secret field
            owner_scope_kind="FORGE_INSTALLATION",
            owner_scope_id=INSTALLATION,
            provider_account_ref=INSTALLATION,
            authenticated_principal_id=PRINCIPAL,
            authorization_context_digest=AUTHZ_DIGEST,
            secret_store_staging_receipt_id=str(uuid.uuid4()),
            secret_integrity_attestation_id=str(uuid.uuid4()),
        )


def test_stale_prior_version_is_rejected_cas_lost(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"stale-cas",
        expected_prior_version=7,
    )
    assert result.state == "REJECTED"
    assert result.rejection_code == "CAS_LOST"
    assert result.response_http_status == 409
    assert '"rejection_code":"CAS_LOST"' in result.response_json


def test_revoked_authority_is_rejected_with_403(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"revoked",
        authority_revoked=True,
    )
    assert result.state == "REJECTED"
    assert result.rejection_code == "AUTHORITY_REVOKED"
    assert result.response_http_status == 403


def test_owner_purpose_matrix_violation_is_integrity_conflict(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"mismatched",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref="a-different-installation",
    )
    assert result.state == "REJECTED"
    assert result.rejection_code == "INTEGRITY_CONFLICT"
    assert result.response_http_status == 409


def test_project_scope_requires_no_provider_account_ref(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())

    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"project-scope",
        owner_scope_kind="PROJECT",
        owner_scope_id="project-1",
        provider_account_ref="installation-1",
    )
    assert result.state == "REJECTED"
    assert result.rejection_code == "INTEGRITY_CONFLICT"

    accepted = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"project-scope",
        owner_scope_kind="PROJECT",
        owner_scope_id="project-1",
        provider_account_ref=None,
    )
    assert accepted.state == "COMPLETED"
    current = run_store.get_secret_current_version(secret_id)
    assert current is not None
    assert current.owner_scope_kind == "PROJECT"
    assert current.provider_account_ref is None


def test_rotation_cannot_relabel_purpose_or_owner(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"v1",
    )
    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"v2",
        expected_prior_version=1,
        purpose="SOURCE_READ",
    )
    assert result.state == "REJECTED"
    assert result.rejection_code == "INTEGRITY_CONFLICT"


def test_rotation_advances_current_version_and_receipt_chain(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    first = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"v1",
    )
    second = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"v2",
        expected_prior_version=1,
    )
    assert second.state == "COMPLETED"
    assert second.new_version == 2
    assert second.credential_rotation_receipt_id != first.credential_rotation_receipt_id
    assert secret_store.read_value(secret_id, 2) == b"v2"
    assert secret_store.read_value(secret_id, 1) == b"v1"
    current = run_store.get_secret_current_version(secret_id)
    assert current is not None
    assert current.current_version == 2


def test_rejection_releases_target_version_for_a_corrected_request(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    bad_op = str(uuid.uuid4())
    accepted = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=bad_op,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=str(uuid.uuid4()),  # never actually staged
        secret_integrity_attestation_id=str(uuid.uuid4()),
    )
    assert accepted.state == "PENDING"
    assert accepted.target_version == 1

    rejected = reconcile_pending_secret_provision_operation(run_store, secret_store, bad_op)
    assert rejected is not None
    assert rejected.state == "REJECTED"
    assert rejected.rejection_code == "STAGED_OBJECT_INVALID"

    corrected = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"corrected",
    )
    assert corrected.state == "COMPLETED"
    assert corrected.target_version == 1
    assert corrected.new_version == 1


def test_concurrent_reservation_of_the_same_target_is_cas_lost(
    run_store: RunStore,
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    first = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=str(uuid.uuid4()),
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=str(uuid.uuid4()),
        secret_integrity_attestation_id=str(uuid.uuid4()),
    )
    assert first.state == "PENDING"
    second = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=str(uuid.uuid4()),
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=str(uuid.uuid4()),
        secret_integrity_attestation_id=str(uuid.uuid4()),
    )
    assert second.state == "REJECTED"
    assert second.rejection_code == "CAS_LOST"


def test_crash_between_accept_and_install_resumes_the_same_target_version(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    staging = secret_store.stage(b"resume-me")
    accepted = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=op_id,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=staging.staging_id,
        secret_integrity_attestation_id=staging.attestation_id,
    )
    assert accepted.state == "PENDING"
    # Simulate process restart: nothing else touches the operation, then a
    # fresh reconciliation pass resumes it using only durable identities.
    resumed = reconcile_pending_secret_provision_operation(run_store, secret_store, op_id)
    assert resumed is not None
    assert resumed.state == "COMPLETED"
    assert resumed.new_version == 1
    assert resumed.target_version == accepted.target_version
    # Idempotent: reconciling an already-COMPLETED operation is a no-op replay.
    again = reconcile_pending_secret_provision_operation(run_store, secret_store, op_id)
    assert again is not None
    assert again.state == "COMPLETED"
    assert again.response_json == resumed.response_json


def test_integrity_conflict_when_installed_bytes_differ_at_same_target(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    # A version already lives at target 1 through a channel outside this
    # Operation's own staging (e.g. a prior repair): the real install must
    # fail closed rather than silently overwrite it.
    secret_store.put_version(secret_id, 1, b"already-here")

    op_id = str(uuid.uuid4())
    staging = secret_store.stage(b"conflicting-bytes")
    accepted = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=op_id,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=staging.staging_id,
        secret_integrity_attestation_id=staging.attestation_id,
    )
    assert accepted.state == "PENDING"

    rejected = reconcile_pending_secret_provision_operation(run_store, secret_store, op_id)
    assert rejected is not None
    assert rejected.state == "REJECTED"
    assert rejected.rejection_code == "INTEGRITY_CONFLICT"
    assert rejected.response_http_status == 409
    assert secret_store.read_value(secret_id, 1) == b"already-here"
    assert not (secret_store._incoming / staging.staging_id).exists()
    assert not (secret_store._incoming / f"{staging.staging_id}.integrity").exists()


def test_stage0_capability_signing_key_is_provisionable_before_any_active_key(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=str(uuid.uuid4()),
        secret_bytes=b"private-signing-key-bytes",
        purpose="CAPABILITY_SIGNING_PRIVATE_KEY",
        owner_scope_kind="CONTROLLER",
        owner_scope_id="ORCEST_V1",
        provider_account_ref=None,
    )
    assert result.state == "COMPLETED"


def test_other_purposes_are_rejected_before_an_active_key_is_selected(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=str(uuid.uuid4()),
        secret_bytes=b"too-early",
    )
    assert result.state == "REJECTED"
    assert result.rejection_code == "AUTHORITY_REVOKED"


def test_adopt_existing_mode_produces_a_real_operation_and_receipt(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    result = _provision(
        run_store,
        secret_store,
        operation_id=str(uuid.uuid4()),
        secret_id=secret_id,
        secret_bytes=b"adopted-legacy-bytes",
        mode="ADOPT_EXISTING",
    )
    assert result.state == "COMPLETED"
    receipt = run_store.get_credential_rotation_receipt(result.credential_rotation_receipt_id)
    assert receipt is not None
    assert receipt.source_kind == "MANAGEMENT_PROVISION"


def test_record_retry_checkpoint_keeps_operation_pending(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    staging = secret_store.stage(b"still-pending")
    accepted = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=op_id,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=staging.staging_id,
        secret_integrity_attestation_id=staging.attestation_id,
    )
    assert accepted.state == "PENDING"

    checkpoint = run_store.record_secret_provision_retry_checkpoint(
        secret_provision_operation_id=op_id,
        phase="VERIFY_STAGING",
        failure_code="SECRET_STORE_UNAVAILABLE",
        failure_evidence_digest="sha256:" + "b" * 64,
        next_retry_ms=1_000,
    )
    assert checkpoint.outcome == "FAILED_RETRYABLE"
    assert checkpoint.checkpoint_sequence == 1

    still_pending = run_store.get_secret_provision_operation(op_id)
    assert still_pending is not None
    assert still_pending.state == "PENDING"

    checkpoints = run_store.list_secret_provision_checkpoints(op_id)
    assert [c.outcome for c in checkpoints] == ["FAILED_RETRYABLE"]

    resumed = reconcile_pending_secret_provision_operation(run_store, secret_store, op_id)
    assert resumed is not None
    assert resumed.state == "COMPLETED"
    checkpoints = run_store.list_secret_provision_checkpoints(op_id)
    assert [c.outcome for c in checkpoints] == ["FAILED_RETRYABLE", "SUCCEEDED"]


def test_complete_and_fail_reject_operations_not_found(run_store: RunStore) -> None:
    with pytest.raises(RunStoreError):
        run_store.complete_secret_provision_operation(
            secret_provision_operation_id=str(uuid.uuid4()),
            storage_path="nowhere",
            secret_integrity_attestation_id=str(uuid.uuid4()),
        )
    with pytest.raises(RunStoreError):
        run_store.fail_secret_provision_operation(
            secret_provision_operation_id=str(uuid.uuid4()),
            rejection_code="INTEGRITY_CONFLICT",
            failure_evidence_digest="sha256:" + "c" * 64,
        )


def test_fail_secret_provision_operation_rejects_accept_time_codes(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    staging = secret_store.stage(b"whatever")
    run_store.begin_secret_provision_operation(
        secret_provision_operation_id=op_id,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=staging.staging_id,
        secret_integrity_attestation_id=staging.attestation_id,
    )
    with pytest.raises(ValueError):
        run_store.fail_secret_provision_operation(
            secret_provision_operation_id=op_id,
            rejection_code="CAS_LOST",
            failure_evidence_digest="sha256:" + "d" * 64,
        )


def test_record_retry_checkpoint_rejects_non_retryable_failure_codes(
    run_store: RunStore, secret_store: SecretStore
) -> None:
    _select_capability_key(run_store)
    secret_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    staging = secret_store.stage(b"whatever")
    run_store.begin_secret_provision_operation(
        secret_provision_operation_id=op_id,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        provider_account_ref=INSTALLATION,
        authenticated_principal_id=PRINCIPAL,
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=staging.staging_id,
        secret_integrity_attestation_id=staging.attestation_id,
    )
    with pytest.raises(ValueError, match="retryable failure"):
        run_store.record_secret_provision_retry_checkpoint(
            secret_provision_operation_id=op_id,
            phase="VERIFY_STAGING",
            failure_code="CAS_LOST",
            failure_evidence_digest="sha256:" + "e" * 64,
            next_retry_ms=1_000,
        )
    still_pending = run_store.get_secret_provision_operation(op_id)
    assert still_pending is not None
    assert still_pending.state == "PENDING"
    assert run_store.list_secret_provision_checkpoints(op_id) == []
