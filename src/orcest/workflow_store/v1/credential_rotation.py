"""Composed ``ATTEMPT_ROTATION`` Credential Rotation Request/Receipt operation.

Ties the controller-only :class:`~orcest.workflow_store.v1.secrets.SecretStore`
(durable staging and write-before-reference promotion of exact bytes, keyed to
the caller's own request identity so a retry reproduces the identical
request-attestation) to the
:class:`~orcest.workflow_store.store.RunStore` Credential Rotation Request/
Receipt ledger, so a caller gets one idempotent, replay-safe,
crash-resumable, closed ``APPLIED``/``CAS_LOST`` operation. Raw secret bytes
never reach SQLite, logs, or an ordinary response; only opaque Secret Store
identities and non-secret metadata cross that boundary.
"""

from __future__ import annotations

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_contract.v1.protocol_registry import CREDENTIAL_ROTATION_REQUEST_PROTOCOL
from orcest.workflow_store.store import CredentialRotationRequestResult, RunStore, RunStoreError
from orcest.workflow_store.v1.errors import IntegrityConflictError
from orcest.workflow_store.v1.secrets import SecretStore, SecretVersionHandle

__all__ = [
    "CredentialRotationReplayConflictError",
    "apply_credential_rotation",
]


class CredentialRotationReplayConflictError(RunStoreError):
    """Raised when a request id is replayed with different secret bytes.

    The comparison happens entirely in-process against the already-installed
    (``APPLIED``) or already-quarantined (``CAS_LOST``) Secret Store bytes;
    neither value is logged, persisted, or returned.
    """


class _CasLost(Exception):
    """Internal signal: the submitted prior version is no longer current."""


def apply_credential_rotation(
    run_store: RunStore,
    secret_store: SecretStore,
    *,
    credential_rotation_request_id: str,
    attempt_id: str,
    activity_id: str,
    attempt_generation: int,
    worker_id: str,
    worker_session_id: str,
    attempt_capability_digest: str,
    launch_attestation_id: str,
    secret_id: str,
    expected_prior_version: int,
    secret_bytes: bytes,
    provider_account_ref: str | None = None,
) -> CredentialRotationRequestResult:
    """Run one end-to-end Attempt-scoped credential rotation request.

    An identical replay (same request id, same non-secret fields, same
    bytes) never re-stages or re-installs and returns the stored projection;
    the same id replayed with different bytes raises
    :class:`CredentialRotationReplayConflictError` without ever comparing or
    exposing either value outside this call. Both first acceptance and
    replay require ``controller_now_ms`` to still be strictly before the
    Attempt's ``execution_deadline_ms``.
    """
    existing = run_store.get_credential_rotation_request(credential_rotation_request_id)
    if existing is not None:
        run_store.require_rotation_replay_before_deadline(existing.attempt_id)
        _verify_rotation_replay(secret_store, existing, secret_bytes)
        return existing

    run_store.require_current_rotation_authority(
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

    staging = secret_store.stage_for_request(credential_rotation_request_id, secret_bytes)
    req_digest = request_digest(
        {
            "protocol_version": CREDENTIAL_ROTATION_REQUEST_PROTOCOL,
            "attempt_id": attempt_id,
            "activity_id": activity_id,
            "attempt_generation": attempt_generation,
            "worker_id": worker_id,
            "worker_session_id": worker_session_id,
            "attempt_capability_digest": attempt_capability_digest,
            "launch_attestation_id": launch_attestation_id,
            "provider_account_ref": provider_account_ref,
            "secret_id": secret_id,
            "expected_prior_version": expected_prior_version,
            "secret_request_attestation_id": staging.attestation_id,
        }
    )

    outcome: dict[str, CredentialRotationRequestResult] = {}

    def _precheck() -> None:
        current = run_store.get_secret_current_version(secret_id)
        current_version = 0 if current is None else current.current_version
        if current_version != expected_prior_version:
            raise _CasLost()

    def _reference(handle: SecretVersionHandle) -> None:
        # Runs only after the Secret Store has durably promoted the bytes,
        # while its storage-mutation lock is still held: the
        # write-before-reference ordering the wiki requires.
        outcome["result"] = run_store.install_applied_credential_rotation(
            credential_rotation_request_id=credential_rotation_request_id,
            protocol_version=CREDENTIAL_ROTATION_REQUEST_PROTOCOL,
            attempt_id=attempt_id,
            activity_id=activity_id,
            attempt_generation=attempt_generation,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
            attempt_capability_digest=attempt_capability_digest,
            launch_attestation_id=launch_attestation_id,
            provider_account_ref=provider_account_ref,
            secret_id=secret_id,
            expected_prior_version=expected_prior_version,
            secret_request_attestation_id=staging.attestation_id,
            request_digest_value=req_digest,
            storage_path=handle.storage_key,
            secret_integrity_attestation_id=handle.attestation_id,
        )

    try:
        secret_store.promote_version(
            staging_id=staging.staging_id,
            secret_id=secret_id,
            version=expected_prior_version + 1,
            precheck=_precheck,
            reference=_reference,
        )
    except _CasLost:
        secret_store.quarantine_request_value(credential_rotation_request_id)
        return run_store.record_cas_lost_credential_rotation(
            credential_rotation_request_id=credential_rotation_request_id,
            protocol_version=CREDENTIAL_ROTATION_REQUEST_PROTOCOL,
            attempt_id=attempt_id,
            activity_id=activity_id,
            attempt_generation=attempt_generation,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
            attempt_capability_digest=attempt_capability_digest,
            launch_attestation_id=launch_attestation_id,
            provider_account_ref=provider_account_ref,
            secret_id=secret_id,
            expected_prior_version=expected_prior_version,
            secret_request_attestation_id=staging.attestation_id,
            request_digest_value=req_digest,
        )
    assert "result" in outcome
    return outcome["result"]


def _verify_rotation_replay(
    secret_store: SecretStore,
    existing: CredentialRotationRequestResult,
    secret_bytes: bytes,
) -> None:
    if existing.disposition == "APPLIED":
        assert existing.accepted_version is not None
        installed = secret_store.read_value(existing.secret_id, existing.accepted_version)
        if installed != secret_bytes:
            raise CredentialRotationReplayConflictError(
                "credential rotation request id was replayed with different bytes"
            )
        return
    try:
        secret_store.stage_for_request(existing.credential_rotation_request_id, secret_bytes)
    except IntegrityConflictError as exc:
        raise CredentialRotationReplayConflictError(
            "credential rotation request id was replayed with different bytes"
        ) from exc
    finally:
        secret_store.quarantine_request_value(existing.credential_rotation_request_id)
