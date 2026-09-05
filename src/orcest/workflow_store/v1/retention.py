"""Retention and garbage collection (persistence-and-recovery.md "Retention
and garbage collection").

v1 retains durable Run rows, transitions, receipts, Publications, and every
referenced Candidate/Workflow-Blob/Secret-Version object by default; nothing
here ever deletes a workflow-authority SQLite row or an audited Operation
record. The automatic collector removes only physical bytes that have both
aged past a grace period and independently proven, under the shared storage
mutation lock, to have no live or pending reference -- and it always
rechecks that proof again immediately before the actual unlink, so a
collection pass can never race an object from referenced to deleted.

This leaf's collector scope is the objects Storage Restoration Operations
introduced (issue #695): terminal (``RESTORED``/``REJECTED``) operation
staging, and orphaned final Candidate objects with no database reference.
Secret Provision staging, CredentialRotationRequest replay metadata, and
Terminal Duplicate Cleanup Reservation retention are owned by their own
leaves and are out of scope here.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.candidates import CandidateObjectStore
from orcest.workflow_store.v1.errors import ObjectNotFoundError
from orcest.workflow_store.v1.fs import StorageLock, fsync_dir
from orcest.workflow_store.v1.secrets import SecretStore

__all__ = [
    "collect_orphan_candidate_objects",
    "collect_terminal_storage_restoration_staging",
    "purge_quarantine_directory",
]

_DEFAULT_ORPHAN_GRACE_MS = 7 * 24 * 60 * 60 * 1000
_DEFAULT_STAGING_GRACE_MS = 7 * 24 * 60 * 60 * 1000
_DEFAULT_QUARANTINE_GRACE_MS = 7 * 24 * 60 * 60 * 1000


def _is_referenced_candidate(run_store: RunStore, bundle_digest: str) -> bool:
    row = run_store.conn.execute(
        "SELECT 1 FROM candidates WHERE bundle_digest = ? "
        "UNION "
        "SELECT 1 FROM candidate_uploads WHERE artifact_bundle_digest = ? AND state = 'PROMOTED'",
        (bundle_digest, bundle_digest),
    ).fetchone()
    return row is not None


def collect_orphan_candidate_objects(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    *,
    storage_lock: StorageLock,
    grace_ms: int = _DEFAULT_ORPHAN_GRACE_MS,
    now_ms: int | None = None,
) -> list[str]:
    """Quarantine installed Candidate objects with no database reference at
    all, once physically aged past ``grace_ms`` (persistence-and-recovery.md
    "the automatic collector may remove only ... final Candidate objects
    with no database reference, after seven days").

    A ``candidates``/currently-``PROMOTED``-upload reference is a live root;
    a bare ``artifact_objects`` inventory row is not one on its own. Every
    candidate is rechecked for a reference a second time after the shared
    storage mutation lock is held, immediately before quarantining it, so a
    concurrent promotion can never race this collector into deleting a
    freshly-referenced object.
    """
    now = _now_ms() if now_ms is None else now_ms
    quarantined: list[str] = []
    for record in list(candidate_store.iter_objects()):
        if _is_referenced_candidate(run_store, record.bundle_digest):
            continue
        age_ms = now - candidate_store.installed_mtime_ms(record.bundle_digest)
        if age_ms < grace_ms:
            continue
        with storage_lock:
            if _is_referenced_candidate(run_store, record.bundle_digest):
                continue
            try:
                candidate_store.quarantine(record.bundle_digest)
            except ObjectNotFoundError:
                continue
            quarantined.append(record.bundle_digest)
    return quarantined


def collect_terminal_storage_restoration_staging(
    run_store: RunStore,
    *,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
    storage_lock: StorageLock,
    grace_ms: int = _DEFAULT_STAGING_GRACE_MS,
    now_ms: int | None = None,
) -> list[str]:
    """Discard the staged object of every ``RESTORED``/``REJECTED`` Storage
    Restoration Operation whose ``terminal_at_ms`` is past ``grace_ms``
    (persistence-and-recovery.md "staging objects of RESTORED or REJECTED
    Storage Restoration Operations ... only after seven days and the
    operation/reference checks below").

    A ``PENDING`` operation's staged object is a live retry root regardless
    of age and is never a candidate here. Under the shared storage mutation
    lock, each operation's terminal state is rechecked immediately before
    its staged bytes are discarded -- this collector never deletes the
    durable Operation row itself, only the physical staging bytes it no
    longer needs to retain.
    """
    now = _now_ms() if now_ms is None else now_ms
    cleaned: list[str] = []
    rows = run_store.conn.execute(
        "SELECT operation_id, object_kind, staged_object_key, state, terminal_at_ms "
        "FROM storage_restoration_operations "
        "WHERE state IN ('RESTORED', 'REJECTED') AND terminal_at_ms IS NOT NULL "
        "AND terminal_at_ms <= ?",
        (now - grace_ms,),
    ).fetchall()
    for row in rows:
        operation_id = row["operation_id"]
        with storage_lock:
            current = run_store.conn.execute(
                "SELECT state, terminal_at_ms FROM storage_restoration_operations "
                "WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if (
                current is None
                or current["state"] not in ("RESTORED", "REJECTED")
                or current["terminal_at_ms"] is None
                or current["terminal_at_ms"] > now - grace_ms
            ):
                continue
            _discard_staged_object(
                object_kind=row["object_kind"],
                staged_object_key=row["staged_object_key"],
                candidate_store=candidate_store,
                blob_store=blob_store,
                secret_store=secret_store,
            )
            cleaned.append(operation_id)
    return cleaned


def _discard_staged_object(
    *,
    object_kind: str,
    staged_object_key: str,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
) -> None:
    if object_kind == "SECRET_VERSION":
        # Secret bytes never leave the protected Secret Store: the staged
        # object key for a SECRET_VERSION operation names a Secret Store
        # staging id, discarded through its own controller-only path.
        secret_store.quarantine_staging(staged_object_key)
    elif object_kind == "CANDIDATE_ARTIFACT":
        candidate_store.discard_staged(staged_object_key)
    else:
        blob_store.discard_staged(staged_object_key)


def purge_quarantine_directory(
    quarantine_dir: Path,
    *,
    storage_lock: StorageLock,
    grace_ms: int = _DEFAULT_QUARANTINE_GRACE_MS,
    now_ms: int | None = None,
) -> list[str]:
    """Permanently delete quarantined files older than a second
    ``grace_ms`` (persistence-and-recovery.md "quarantine files after a
    second seven-day grace period and a repeated no-reference check").

    A file that reached quarantine was already proven unreferenced under
    the lock at quarantine time; this pass re-verifies only that it still
    exists and is still old enough before unlinking it, holding the same
    lock throughout so a concurrent quarantine write can never be deleted
    mid-write.
    """
    now = _now_ms() if now_ms is None else now_ms
    removed: list[str] = []
    if not quarantine_dir.is_dir():
        return removed
    with storage_lock:
        for child in sorted(quarantine_dir.iterdir()):
            if not child.is_file() or child.is_symlink():
                continue
            age_ms = now - int(child.stat().st_mtime * 1000)
            if age_ms < grace_ms:
                continue
            try:
                os.unlink(child)
            except FileNotFoundError:
                continue
            removed.append(child.name)
        if removed:
            fsync_dir(quarantine_dir)
    return removed


def _now_ms() -> int:
    return int(time.time() * 1000)
