"""Composed MANAGEMENT_PROVISION provision/adopt operations.

Ties the controller-only :class:`~orcest.workflow_store.v1.secrets.SecretStore`
(durable staging and write-before-reference promotion of exact bytes) to the
:class:`~orcest.workflow_store.store.RunStore` Secret Provision Operation
ledger (request digest, target-version CAS, Checkpoint, Credential Rotation
Receipt) so a caller gets one idempotent, replay-safe, crash-resumable
operation. Raw secret bytes never reach SQLite, logs, or an ordinary
response; only opaque Secret Store identities and non-secret metadata cross
that boundary.
"""

from __future__ import annotations

from orcest.workflow_contract.v1.digest import failure_evidence_digest
from orcest.workflow_store.store import (
    RunStore,
    RunStoreError,
    SecretProvisionOperationResult,
)
from orcest.workflow_store.v1.errors import IntegrityConflictError, ObjectNotFoundError
from orcest.workflow_store.v1.secrets import SecretStore, SecretVersionHandle

__all__ = [
    "SecretProvisionReplayConflictError",
    "provision_or_adopt_secret",
    "reconcile_pending_secret_provision_operation",
]


class SecretProvisionReplayConflictError(RunStoreError):
    """Raised when an operation id is replayed with different secret bytes.

    The comparison happens entirely in-process against the already-installed
    Secret Version bytes; neither value is logged, persisted, or returned.
    """


def provision_or_adopt_secret(
    run_store: RunStore,
    secret_store: SecretStore,
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
    secret_bytes: bytes,
    provider_account_ref: str | None = None,
    authority_revoked: bool = False,
) -> SecretProvisionOperationResult:
    """Run one end-to-end ``PROVISION`` or ``ADOPT_EXISTING`` operation.

    ``mode`` only changes provenance/authorization framing (both closed modes
    still stage the exact bytes through the Secret Store and produce the same
    real Operation, Checkpoint, Receipt, Version, and audit retention). An
    identical replay (same operation id, same non-secret fields, same bytes)
    never re-stages and returns the stored projection; the same id replayed
    with different bytes raises :class:`SecretProvisionReplayConflictError`
    without ever comparing or exposing either value outside this call.
    """
    existing = run_store.get_secret_provision_operation(secret_provision_operation_id)
    if existing is not None:
        _verify_replay_bytes(secret_store, existing, secret_bytes)
        return existing

    staging = secret_store.stage(secret_bytes)
    accepted = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=secret_provision_operation_id,
        mode=mode,
        secret_id=secret_id,
        expected_prior_version=expected_prior_version,
        purpose=purpose,
        owner_scope_kind=owner_scope_kind,
        owner_scope_id=owner_scope_id,
        provider_account_ref=provider_account_ref,
        authenticated_principal_id=authenticated_principal_id,
        authorization_context_digest=authorization_context_digest,
        secret_store_staging_receipt_id=staging.staging_id,
        secret_integrity_attestation_id=staging.attestation_id,
        authority_revoked=authority_revoked,
    )
    if accepted.state != "PENDING":
        # Rejected before acceptance (CAS_LOST/AUTHORITY_REVOKED/owner-purpose
        # identity conflict): the staged bytes never became a live reference.
        secret_store.quarantine_staging(staging.staging_id)
        return accepted
    return _install(run_store, secret_store, accepted, staging_id=staging.staging_id)


def reconcile_pending_secret_provision_operation(
    run_store: RunStore, secret_store: SecretStore, secret_provision_operation_id: str
) -> SecretProvisionOperationResult | None:
    """Resume a ``PENDING`` Operation after a crash between accept and install.

    Reuses the durable ``secret_store_staging_receipt_id`` already recorded on
    the accepted row, so the caller never needs to resend secret bytes. A
    terminal or unknown operation id is returned unchanged (or ``None``).
    """
    op = run_store.get_secret_provision_operation(secret_provision_operation_id)
    if op is None or op.state != "PENDING":
        return op
    assert op.secret_store_staging_receipt_id is not None
    return _install(run_store, secret_store, op, staging_id=op.secret_store_staging_receipt_id)


def _verify_replay_bytes(
    secret_store: SecretStore, existing: SecretProvisionOperationResult, secret_bytes: bytes
) -> None:
    if existing.state != "COMPLETED":
        # PENDING never asks the client to resend bytes after acceptance;
        # REJECTED keeps no durable bytes to compare a resend against.
        return
    assert existing.new_version is not None
    installed = secret_store.read_value(existing.secret_id, existing.new_version)
    if installed != secret_bytes:
        raise SecretProvisionReplayConflictError(
            "secret provision operation id was replayed with different secret bytes"
        )


def _install(
    run_store: RunStore,
    secret_store: SecretStore,
    accepted: SecretProvisionOperationResult,
    *,
    staging_id: str,
) -> SecretProvisionOperationResult:
    operation_id = accepted.secret_provision_operation_id
    secret_id = accepted.secret_id
    target_version = accepted.target_version
    outcome: dict[str, SecretProvisionOperationResult] = {}

    def _reference(handle: SecretVersionHandle) -> None:
        # Runs only after the Secret Store has durably promoted the bytes:
        # the write-before-reference ordering the wiki requires.
        outcome["result"] = run_store.complete_secret_provision_operation(
            secret_provision_operation_id=operation_id,
            storage_path=handle.storage_key,
            secret_integrity_attestation_id=handle.attestation_id,
        )

    try:
        secret_store.promote_version(
            staging_id=staging_id,
            secret_id=secret_id,
            version=target_version,
            reference=_reference,
        )
    except ObjectNotFoundError as exc:
        return run_store.fail_secret_provision_operation(
            secret_provision_operation_id=operation_id,
            rejection_code="STAGED_OBJECT_INVALID",
            failure_evidence_digest=_evidence_digest(operation_id, "STAGED_OBJECT_INVALID", exc),
        )
    except IntegrityConflictError as exc:
        return run_store.fail_secret_provision_operation(
            secret_provision_operation_id=operation_id,
            rejection_code="INTEGRITY_CONFLICT",
            failure_evidence_digest=_evidence_digest(operation_id, "INTEGRITY_CONFLICT", exc),
        )
    assert "result" in outcome
    return outcome["result"]


def _evidence_digest(operation_id: str, code: str, exc: Exception) -> str:
    return failure_evidence_digest(
        {
            "secret_provision_operation_id": operation_id,
            "failure_code": code,
            "reason": type(exc).__name__,
        }
    )
