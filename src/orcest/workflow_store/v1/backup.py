"""Exclusive controller backup barrier and complete-backup-unit creation.

persistence-and-recovery.md "Backup and restore": a complete backup unit is a
standalone SQLite snapshot plus every live Candidate object, Workflow Blob
object, and Secret version referenced by it, captured only after the
three-branch zero-``CLAIMED``-Attempt barrier. Redis, WAL/SHM files, caches,
and active worker workspaces are never backup inputs.

This module currently backs up every locally installed Candidate/Workflow
Blob object (a safe superset of "referenced") rather than computing exact
SQL-reachability from ``candidates``/``artifact_objects``/snapshot rows; v1
retains all such state anyway (persistence-and-recovery.md "Retention and
garbage collection"), so the superset never omits a live object.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.digest import content_digest, sha256_file_hex, sha256_hex
from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.candidates import CandidateObjectStore
from orcest.workflow_store.v1.fs import DIR_MODE, FILE_MODE, fsync_dir, fsync_file
from orcest.workflow_store.v1.secrets import SecretStore

BACKUP_SERVICE_PRINCIPAL_ID = "controller-backup-service"
MANIFEST_NAME = "manifest.json"
COMPLETE_MARKER_NAME = "COMPLETE"
_SECRET_NONCE_BYTES = 12


class BackupBarrierTimeoutError(RuntimeError):
    """Raised when the zero-``CLAIMED``-Attempt barrier is not reached in time."""


class BackupModeTransitionError(RuntimeError):
    """Raised when the temporary-pause branch's ``SET_MODE`` is rejected."""


@dataclass(frozen=True, slots=True)
class BackupManifestEntry:
    relative_path: str
    kind: str
    size: int
    mode: int
    sha256: str


@dataclass(frozen=True, slots=True)
class BackupResult:
    backup_id: str
    destination: Path
    manifest: tuple[BackupManifestEntry, ...]
    manifest_digest: str
    branch: str
    created_at_ms: int


def _claimed_attempt_count(run_store: RunStore) -> int:
    row = run_store.conn.execute(
        "SELECT COUNT(*) AS n FROM attempts WHERE state = 'CLAIMED'"
    ).fetchone()
    return int(row["n"])


def _wait_for_zero_claimed(
    run_store: RunStore, *, barrier_max_ms: int, poll_interval_s: float = 0.05
) -> None:
    deadline = time.monotonic() + (barrier_max_ms / 1000.0)
    while _claimed_attempt_count(run_store) > 0:
        if time.monotonic() >= deadline:
            raise BackupBarrierTimeoutError(
                "zero-CLAIMED-Attempt barrier was not reached before backup_barrier_max_ms"
            )
        time.sleep(poll_interval_s)


def _select_barrier_branch(run_store: RunStore) -> str:
    mode = run_store.get_controller_mode()
    if mode.mode == "MAINTENANCE":
        return "MAINTENANCE_IN_PLACE"
    if mode.mode == "DISPATCH_PAUSED" and mode.dispatch_paused_intake_policy == "PAUSE_ADMISSION":
        return "ALREADY_PAUSED_IN_PLACE"
    return "TEMPORARY_PAUSE"


def _pause_for_backup(run_store: RunStore, *, authorization_context_digest: str) -> str:
    """Commit the third-branch ``SET_MODE`` pause and return its Operation id
    so the caller can restore the exact prior mode after the backup releases."""
    mode = run_store.get_controller_mode()
    operation_id = str(uuid.uuid4())
    result = run_store.apply_controller_mode_operation(
        controller_mode_operation_id=operation_id,
        operation_kind="SET_MODE",
        expected_mode_revision=mode.mode_revision,
        expected_mode=mode.mode,
        requested_mode="DISPATCH_PAUSED",
        requested_dispatch_paused_intake_policy="PAUSE_ADMISSION",
        authenticated_principal_id=BACKUP_SERVICE_PRINCIPAL_ID,
        authorization_context_digest=authorization_context_digest,
    )
    if result.status != "SUCCEEDED":
        raise BackupModeTransitionError(
            f"backup pause SET_MODE was rejected: {result.rejection_code}"
        )
    return operation_id


def _restore_prior_mode(run_store: RunStore, *, pause_operation_id: str) -> None:
    """Restore the exact prior mode/intake-policy from the pause Operation,
    only if the mode revision still equals that pause's result (an
    intervening operator change is never overwritten)."""
    pause_row = run_store.conn.execute(
        "SELECT * FROM controller_mode_operations WHERE controller_mode_operation_id = ?",
        (pause_operation_id,),
    ).fetchone()
    if pause_row is None or pause_row["status"] != "SUCCEEDED":
        return
    mode = run_store.get_controller_mode()
    if mode.mode_revision != pause_row["result_mode_revision"]:
        return
    prior_mode = pause_row["expected_mode"]
    prior_policy = None
    if prior_mode == "DISPATCH_PAUSED":
        # _select_barrier_branch() only reaches the temporary-pause branch
        # for DISPATCH_PAUSED when its policy is ALLOW_ADMISSION (the
        # PAUSE_ADMISSION case is its own in-place branch), so this is the
        # only prior policy a temporary pause's expected_mode can name.
        prior_policy = "ALLOW_ADMISSION"
    run_store.apply_controller_mode_operation(
        controller_mode_operation_id=str(uuid.uuid4()),
        operation_kind="SET_MODE",
        expected_mode_revision=mode.mode_revision,
        expected_mode=mode.mode,
        requested_mode=prior_mode,
        requested_dispatch_paused_intake_policy=prior_policy,
        authenticated_principal_id=BACKUP_SERVICE_PRINCIPAL_ID,
        authorization_context_digest=content_digest(pause_operation_id.encode()),
    )


def _write_backup_file(dest: Path, data: bytes) -> None:
    dest.parent.mkdir(mode=DIR_MODE, parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + f".tmp-{uuid.uuid4().hex}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, FILE_MODE)
    try:
        os.write(fd, data)
        os.fchmod(fd, FILE_MODE)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, dest)
    fsync_file(dest)
    fsync_dir(dest.parent)


def create_backup(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
    *,
    destination_root: Path,
    encryption_key: bytes,
    backup_barrier_max_ms: int = 30_000,
) -> BackupResult:
    """Create one complete backup unit under the exclusive backup barrier.

    ``destination_root`` MUST be outside the live state root, on a separately
    configured backup target (persistence-and-recovery.md "Backup and
    restore"). ``encryption_key`` is a 32-byte AES-256-GCM key used only to
    seal Secret version bytes into authenticated encrypted envelopes; neither
    plaintext secret bytes nor an unkeyed plaintext digest ever enters the
    manifest.
    """
    if len(encryption_key) != 32:
        raise ValueError("encryption_key must be 32 bytes (AES-256-GCM)")
    backup_id = str(uuid.uuid4())
    staging = destination_root / f"{backup_id}.staging"
    staging.mkdir(mode=DIR_MODE, parents=True)

    branch = _select_barrier_branch(run_store)
    pause_operation_id: str | None = None
    pause_result_mode_revision: int | None = None
    if branch == "TEMPORARY_PAUSE":
        pause_operation_id = _pause_for_backup(
            run_store,
            authorization_context_digest=content_digest(backup_id.encode()),
        )
        pause_row = run_store.conn.execute(
            "SELECT result_mode_revision FROM controller_mode_operations "
            "WHERE controller_mode_operation_id = ?",
            (pause_operation_id,),
        ).fetchone()
        pause_result_mode_revision = int(pause_row["result_mode_revision"])
    try:
        _wait_for_zero_claimed(run_store, barrier_max_ms=backup_barrier_max_ms)
        with run_store.storage_mutation_lock():
            if _claimed_attempt_count(run_store) != 0:
                raise BackupBarrierTimeoutError(
                    "a CLAIMED Attempt raced the storage-lock-held barrier recheck"
                )
            mode = run_store.get_controller_mode()
            if branch == "TEMPORARY_PAUSE":
                if mode.mode_revision != pause_result_mode_revision:
                    raise BackupModeTransitionError(
                        "controller mode changed after the committed pause revision"
                    )
            elif _select_barrier_branch(run_store) != branch:
                raise BackupModeTransitionError(
                    "controller mode branch changed under the storage-lock barrier"
                )
            entries, created_at_ms = _capture_backup_unit(
                run_store,
                candidate_store,
                blob_store,
                secret_store,
                staging=staging,
                encryption_key=encryption_key,
            )
    finally:
        if pause_operation_id is not None:
            _restore_prior_mode(run_store, pause_operation_id=pause_operation_id)

    manifest_body = {
        "backup_id": backup_id,
        "schema_version": run_store.get_controller_state().schema_version,
        "reducer_version": run_store.get_controller_state().reducer_version,
        "created_at_ms": created_at_ms,
        "entries": [
            {
                "relative_path": entry.relative_path,
                "kind": entry.kind,
                "size": entry.size,
                "mode": entry.mode,
                "sha256": entry.sha256,
            }
            for entry in entries
        ],
    }
    manifest_json = canonical_json_text(manifest_body)
    _write_backup_file(staging / MANIFEST_NAME, manifest_json.encode("utf-8"))
    manifest_digest = sha256_hex(manifest_json.encode("utf-8"))

    completed = destination_root / backup_id
    os.rename(staging, completed)
    fsync_dir(destination_root)
    _write_backup_file(completed / COMPLETE_MARKER_NAME, manifest_digest.encode("utf-8"))
    fsync_dir(completed)

    return BackupResult(
        backup_id=backup_id,
        destination=completed,
        manifest=tuple(entries),
        manifest_digest=manifest_digest,
        branch=branch,
        created_at_ms=created_at_ms,
    )


def _capture_backup_unit(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
    *,
    staging: Path,
    encryption_key: bytes,
) -> tuple[list[BackupManifestEntry], int]:
    entries: list[BackupManifestEntry] = []
    now_ms = int(time.time() * 1000)

    db_dest = staging / "workflow.db"
    with sqlite3.connect(db_dest) as dest_conn:
        run_store.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        run_store.conn.backup(dest_conn)
    os.chmod(db_dest, FILE_MODE)
    fsync_file(db_dest)
    entries.append(
        BackupManifestEntry(
            relative_path="workflow.db",
            kind="SQLITE_SNAPSHOT",
            size=db_dest.stat().st_size,
            mode=FILE_MODE,
            sha256=sha256_file_hex(db_dest),
        )
    )

    for record in candidate_store.iter_objects():
        data = candidate_store.read(record.bundle_digest)
        dest = staging / "candidates" / record.storage_key
        _write_backup_file(dest, data)
        entries.append(
            BackupManifestEntry(
                relative_path=str(dest.relative_to(staging)),
                kind="CANDIDATE_ARTIFACT",
                size=len(data),
                mode=FILE_MODE,
                sha256=sha256_hex(data),
            )
        )

    for blob_record in blob_store.iter_objects():
        data = blob_store.read(blob_record.blob_digest)
        dest = staging / "blobs" / blob_record.storage_key
        _write_backup_file(dest, data)
        entries.append(
            BackupManifestEntry(
                relative_path=str(dest.relative_to(staging)),
                kind="WORKFLOW_BLOB",
                size=len(data),
                mode=FILE_MODE,
                sha256=sha256_hex(data),
            )
        )

    aesgcm = AESGCM(encryption_key)
    for handle in secret_store.iter_versions():
        value = secret_store.read_value(handle.reference.secret_id, handle.reference.version)
        nonce = os.urandom(_SECRET_NONCE_BYTES)
        ciphertext = aesgcm.encrypt(
            nonce,
            value,
            json.dumps(
                {
                    "secret_id": handle.reference.secret_id,
                    "version": handle.reference.version,
                },
                sort_keys=True,
            ).encode("utf-8"),
        )
        envelope = nonce + ciphertext
        dest = (
            staging
            / "secrets"
            / handle.reference.secret_id
            / "versions"
            / f"{handle.reference.version}.enc"
        )
        _write_backup_file(dest, envelope)
        entries.append(
            BackupManifestEntry(
                relative_path=str(dest.relative_to(staging)),
                kind="SECRET_VERSION_ENVELOPE",
                size=len(envelope),
                mode=FILE_MODE,
                sha256=sha256_hex(envelope),
            )
        )

    return entries, now_ms
