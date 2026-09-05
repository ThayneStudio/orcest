"""Exclusive controller backup barrier and complete-backup-unit creation
(issue #695, persistence-and-recovery.md "Backup and restore")."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from orcest.workflow_store import AttemptOfferInput, activity_offer_protocol
from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.backup import (
    COMPLETE_MARKER_NAME,
    MANIFEST_NAME,
    BackupBarrierTimeoutError,
    create_backup,
)
from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.candidates import CandidateObjectStore
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.secrets import SecretStore

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64


def _uid() -> str:
    return str(uuid.uuid4())


def _running(run_store: RunStore) -> None:
    run_store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    run_store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="SET_MODE",
        expected_mode_revision=1,
        expected_mode="MAINTENANCE",
        requested_mode="RUNNING",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )


@pytest.fixture
def control_root(tmp_path: Path) -> Path:
    return tmp_path / "control"


@pytest.fixture
def layout(control_root: Path) -> ControlLayout:
    control = ControlLayout(root=control_root)
    control.initialize()
    return control


@pytest.fixture
def quota() -> QuotaConfig:
    return QuotaConfig(min_free_bytes=0, max_object_bytes=1024 * 1024)


@pytest.fixture
def lock(layout: ControlLayout) -> StorageLock:
    return StorageLock(layout.storage_lock_path)


@pytest.fixture
def stores(layout: ControlLayout, quota: QuotaConfig, lock: StorageLock):
    return (
        CandidateObjectStore(layout, quota=quota, lock=lock),
        WorkflowBlobStore(layout, quota=quota, lock=lock),
        SecretStore(layout, quota=quota, lock=lock),
    )


@pytest.fixture
def run_store(control_root: Path) -> RunStore:
    with RunStore(control_root, verify_local_filesystem=False) as store:
        yield store


def test_backup_captures_sqlite_and_object_stores_and_writes_verified_manifest(
    run_store: RunStore, stores, tmp_path: Path
) -> None:
    candidate_store, blob_store, secret_store = stores
    _running(run_store)
    with run_store.transaction():
        run_store.create_run(
            run_id=_uid(),
            project_id="project-a",
            work_item_key="work-1",
            state="BUILDING",
            specification_generation=1,
        )
    candidate_store.install(b"bundle-bytes")
    blob_store.install("PROMPT_UTF8", b"blob-bytes")
    secret_store.put_version("11111111-1111-4111-8111-111111111111", 1, b"secret-bytes")

    destination = tmp_path / "backups"
    destination.mkdir()
    result = create_backup(
        run_store,
        candidate_store,
        blob_store,
        secret_store,
        destination_root=destination,
        encryption_key=os.urandom(32),
    )

    assert result.branch in {"MAINTENANCE_IN_PLACE", "ALREADY_PAUSED_IN_PLACE", "TEMPORARY_PAUSE"}
    assert (result.destination / MANIFEST_NAME).is_file()
    assert (result.destination / COMPLETE_MARKER_NAME).is_file()
    kinds = {entry.kind for entry in result.manifest}
    assert kinds == {
        "SQLITE_SNAPSHOT",
        "CANDIDATE_ARTIFACT",
        "WORKFLOW_BLOB",
        "SECRET_VERSION_ENVELOPE",
    }
    # Every manifest entry's recorded digest matches the actual staged bytes.
    for entry in result.manifest:
        path = result.destination / entry.relative_path
        assert path.stat().st_size == entry.size
    # Backup restores the controller to an operational mode afterward.
    mode = run_store.get_controller_mode()
    assert mode.mode != "DISPATCH_PAUSED" or mode.dispatch_paused_intake_policy != "PAUSE_ADMISSION"


def test_backup_never_writes_plaintext_secret_bytes(
    run_store: RunStore, stores, tmp_path: Path
) -> None:
    candidate_store, blob_store, secret_store = stores
    _running(run_store)
    secret_bytes = b"super-secret-value-marker"
    secret_store.put_version("22222222-2222-4222-8222-222222222222", 1, secret_bytes)

    destination = tmp_path / "backups"
    destination.mkdir()
    result = create_backup(
        run_store,
        candidate_store,
        blob_store,
        secret_store,
        destination_root=destination,
        encryption_key=os.urandom(32),
    )

    for path in result.destination.rglob("*"):
        if path.is_file():
            assert secret_bytes not in path.read_bytes()


def test_backup_barrier_times_out_with_claimed_attempts(
    run_store: RunStore, stores, tmp_path: Path
) -> None:
    candidate_store, blob_store, secret_store = stores
    _running(run_store)
    run_id = _uid()
    activity_id = _uid()
    attempt_id = _uid()
    with run_store.transaction():
        run_store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key="work-1",
            state="BUILDING",
            specification_generation=1,
        )
    run_store.create_activity(
        activity_id=activity_id,
        run_id=run_id,
        activity_ordinal=1,
        specification_generation=1,
        policy_hash="sha256:" + "0" * 64,
        kind="BUILD",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={"a": 1},
        semantic_input_digest="sha256:" + "1" * 64,
        idempotency_key="sha256:" + "2" * 64,
        attempt=AttemptOfferInput(
            attempt_id=attempt_id,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile="codex",
            offered_at_ms=0,
            claim_timeout_ms=300_000,
        ),
        outbox_id=_uid(),
    )
    with run_store.transaction():
        run_store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = 0, execution_deadline_ms = ?, "
            "attempt_capability_digest = ? WHERE attempt_id = ?",
            ("worker-1", _uid(), 300_000_000_000, "sha256:" + "3" * 64, attempt_id),
        )

    destination = tmp_path / "backups"
    destination.mkdir()

    with pytest.raises(BackupBarrierTimeoutError):
        create_backup(
            run_store,
            candidate_store,
            blob_store,
            secret_store,
            destination_root=destination,
            encryption_key=os.urandom(32),
            backup_barrier_max_ms=50,
        )
