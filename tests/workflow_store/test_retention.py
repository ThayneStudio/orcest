"""Retention and garbage collection (issue #695, persistence-and-recovery.md
"Retention and garbage collection")."""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.candidates import CandidateObjectStore
from orcest.workflow_store.v1.errors import ObjectNotFoundError
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.retention import (
    collect_orphan_candidate_objects,
    collect_terminal_storage_restoration_staging,
    purge_quarantine_directory,
)
from orcest.workflow_store.v1.secrets import SecretStore

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64
DAY_MS = 24 * 60 * 60 * 1000


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


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
def candidate_store(
    layout: ControlLayout, quota: QuotaConfig, lock: StorageLock
) -> CandidateObjectStore:
    return CandidateObjectStore(layout, quota=quota, lock=lock)


@pytest.fixture
def blob_store(layout: ControlLayout, quota: QuotaConfig, lock: StorageLock) -> WorkflowBlobStore:
    return WorkflowBlobStore(layout, quota=quota, lock=lock)


@pytest.fixture
def secret_store(layout: ControlLayout, quota: QuotaConfig, lock: StorageLock) -> SecretStore:
    return SecretStore(layout, quota=quota, lock=lock)


@pytest.fixture
def run_store(control_root: Path) -> RunStore:
    with RunStore(control_root, verify_local_filesystem=False) as store:
        yield store


def _age_installed_object(path: Path, *, age_ms: int) -> None:
    stamp = time.time() - (age_ms / 1000.0)
    os.utime(path, (stamp, stamp))


# -- collect_orphan_candidate_objects -----------------------------------------


def test_collect_orphan_candidate_objects_skips_a_referenced_object(
    run_store: RunStore, candidate_store: CandidateObjectStore, lock: StorageLock
) -> None:
    run_id = _uid()
    activity_id = _uid()
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
        state="SUCCEEDED",
        created_transition_sequence=1,
        semantic_input={},
        semantic_input_digest="sha256:" + "1" * 64,
        idempotency_key="sha256:" + "2" * 64,
    )
    record = candidate_store.install(b"bundle-bytes")
    with run_store.transaction():
        run_store.conn.execute(
            "INSERT INTO artifact_objects(bundle_digest, storage_key, byte_length, "
            "installed_at_ms) VALUES (?, ?, ?, ?)",
            (record.bundle_digest, record.storage_key, record.byte_length, 0),
        )
        run_store.conn.execute(
            "INSERT INTO candidates(candidate_id, run_id, candidate_generation, "
            "provenance_kind, producing_activity_id, worker_attempt_id, "
            "worker_attempt_generation, object_format, oid, "
            "base_commit_json, bundle_digest, created_at_ms) "
            "VALUES (?, ?, 1, 'WORKER_ATTEMPT', ?, ?, 1, 'sha256', ?, '{}', ?, 0)",
            (_uid(), run_id, activity_id, _uid(), "a" * 64, record.bundle_digest),
        )
    dest = candidate_store._dest(record)
    _age_installed_object(dest, age_ms=30 * DAY_MS)

    quarantined = collect_orphan_candidate_objects(
        run_store, candidate_store, storage_lock=lock, grace_ms=7 * DAY_MS
    )

    assert quarantined == []
    assert dest.is_file()


def test_collect_orphan_candidate_objects_quarantines_an_aged_unreferenced_object(
    run_store: RunStore, candidate_store: CandidateObjectStore, lock: StorageLock
) -> None:
    record = candidate_store.install(b"orphan-bytes")
    dest = candidate_store._dest(record)
    _age_installed_object(dest, age_ms=30 * DAY_MS)

    quarantined = collect_orphan_candidate_objects(
        run_store, candidate_store, storage_lock=lock, grace_ms=7 * DAY_MS
    )

    assert quarantined == [record.bundle_digest]
    assert not dest.is_file()
    quarantine_dir = candidate_store._root / "quarantine"
    assert len(list(quarantine_dir.iterdir())) == 1


def test_collect_orphan_candidate_objects_skips_an_object_still_within_grace(
    run_store: RunStore, candidate_store: CandidateObjectStore, lock: StorageLock
) -> None:
    record = candidate_store.install(b"fresh-orphan-bytes")

    quarantined = collect_orphan_candidate_objects(
        run_store, candidate_store, storage_lock=lock, grace_ms=7 * DAY_MS
    )

    assert quarantined == []
    assert candidate_store._dest(record).is_file()


def test_collect_orphan_candidate_objects_continues_when_mtime_lookup_races(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    lock: StorageLock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vanished = candidate_store.install(b"vanishes-during-mtime")
    surviving = candidate_store.install(b"survives-mtime-race")
    _age_installed_object(candidate_store._dest(vanished), age_ms=30 * DAY_MS)
    _age_installed_object(candidate_store._dest(surviving), age_ms=30 * DAY_MS)

    original = CandidateObjectStore.installed_mtime_ms

    def flaky_mtime(self: CandidateObjectStore, bundle_digest: str) -> int:
        if bundle_digest == vanished.bundle_digest:
            raise ObjectNotFoundError("Candidate object is not installed")
        return original(self, bundle_digest)

    monkeypatch.setattr(CandidateObjectStore, "installed_mtime_ms", flaky_mtime)

    quarantined = collect_orphan_candidate_objects(
        run_store, candidate_store, storage_lock=lock, grace_ms=7 * DAY_MS
    )

    assert vanished.bundle_digest not in quarantined
    assert quarantined == [surviving.bundle_digest]
    assert candidate_store._dest(vanished).is_file()
    assert not candidate_store._dest(surviving).is_file()


# -- collect_terminal_storage_restoration_staging -----------------------------


def _begin_restoration(run_store: RunStore, *, staged_object_key: str, now_ms: int = 0) -> str:
    op_id = _uid()
    run_store.begin_storage_restoration_operation(
        operation_id=op_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id="sha256:" + "c" * 64,
        expected_byte_length=10,
        media_kind=None,
        authenticated_principal_id="controller-storage-reconciler",
        authorization_context_digest=AUTHZ_DIGEST,
        staged_object_key=staged_object_key,
        now_ms=now_ms,
    )
    return op_id


def test_collect_terminal_staging_never_touches_a_pending_operation(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
    lock: StorageLock,
) -> None:
    incoming_path, _record = candidate_store.stage_upload_bytes(b"still-pending")
    _begin_restoration(run_store, staged_object_key=incoming_path)

    cleaned = collect_terminal_storage_restoration_staging(
        run_store,
        candidate_store=candidate_store,
        blob_store=blob_store,
        secret_store=secret_store,
        storage_lock=lock,
        grace_ms=0,
    )

    assert cleaned == []
    assert candidate_store._incoming_path(incoming_path).is_file()


def test_collect_terminal_staging_discards_a_rejected_operation_past_grace(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
    lock: StorageLock,
) -> None:
    incoming_path, _record = candidate_store.stage_upload_bytes(b"now-rejected")
    op_id = _begin_restoration(run_store, staged_object_key=incoming_path)
    run_store.fail_storage_restoration_operation(
        operation_id=op_id, rejection_code="INTEGRITY_CONFLICT", now_ms=0
    )

    cleaned = collect_terminal_storage_restoration_staging(
        run_store,
        candidate_store=candidate_store,
        blob_store=blob_store,
        secret_store=secret_store,
        storage_lock=lock,
        grace_ms=7 * DAY_MS,
        now_ms=8 * DAY_MS,
    )

    assert cleaned == [op_id]
    assert not candidate_store._incoming_path(incoming_path).is_file()


def test_collect_terminal_staging_respects_grace_period(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
    lock: StorageLock,
) -> None:
    incoming_path, _record = candidate_store.stage_upload_bytes(b"too-recent")
    op_id = _begin_restoration(run_store, staged_object_key=incoming_path)
    run_store.fail_storage_restoration_operation(
        operation_id=op_id, rejection_code="INTEGRITY_CONFLICT", now_ms=8 * DAY_MS
    )

    cleaned = collect_terminal_storage_restoration_staging(
        run_store,
        candidate_store=candidate_store,
        blob_store=blob_store,
        secret_store=secret_store,
        storage_lock=lock,
        grace_ms=7 * DAY_MS,
        now_ms=9 * DAY_MS,
    )

    assert cleaned == []
    assert candidate_store._incoming_path(incoming_path).is_file()


def test_collect_terminal_staging_continues_when_staged_object_vanishes(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    blob_store: WorkflowBlobStore,
    secret_store: SecretStore,
    lock: StorageLock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming_a, _record_a = candidate_store.stage_upload_bytes(b"gone-staging")
    incoming_b, _record_b = candidate_store.stage_upload_bytes(b"kept-staging")
    op_a = _begin_restoration(run_store, staged_object_key=incoming_a)
    op_b = _begin_restoration(run_store, staged_object_key=incoming_b)
    run_store.fail_storage_restoration_operation(
        operation_id=op_a, rejection_code="INTEGRITY_CONFLICT", now_ms=0
    )
    run_store.fail_storage_restoration_operation(
        operation_id=op_b, rejection_code="INTEGRITY_CONFLICT", now_ms=0
    )

    original = CandidateObjectStore.discard_staged

    def flaky_discard(self: CandidateObjectStore, incoming_path: str) -> None:
        if incoming_path == incoming_a:
            raise ObjectNotFoundError("staged object is not installed")
        original(self, incoming_path)

    monkeypatch.setattr(CandidateObjectStore, "discard_staged", flaky_discard)

    cleaned = collect_terminal_storage_restoration_staging(
        run_store,
        candidate_store=candidate_store,
        blob_store=blob_store,
        secret_store=secret_store,
        storage_lock=lock,
        grace_ms=7 * DAY_MS,
        now_ms=8 * DAY_MS,
    )

    assert op_a in cleaned
    assert op_b in cleaned
    assert not candidate_store._incoming_path(incoming_b).is_file()


# -- purge_quarantine_directory -----------------------------------------------


def test_purge_quarantine_directory_deletes_only_aged_files(
    candidate_store: CandidateObjectStore, layout: ControlLayout, lock: StorageLock
) -> None:
    record = candidate_store.install(b"quarantine-me")
    candidate_store.quarantine(record.bundle_digest)
    quarantine_dir = layout.candidates_root / "quarantine"
    quarantined_files = list(quarantine_dir.iterdir())
    assert len(quarantined_files) == 1
    _age_installed_object(quarantined_files[0], age_ms=30 * DAY_MS)

    removed_too_early = purge_quarantine_directory(
        quarantine_dir, storage_lock=lock, grace_ms=90 * DAY_MS
    )
    assert removed_too_early == []

    removed = purge_quarantine_directory(quarantine_dir, storage_lock=lock, grace_ms=7 * DAY_MS)
    assert len(removed) == 1
    assert list(quarantine_dir.iterdir()) == []


def test_purge_quarantine_directory_starts_grace_from_quarantine_time(
    run_store: RunStore,
    candidate_store: CandidateObjectStore,
    layout: ControlLayout,
    lock: StorageLock,
) -> None:
    record = candidate_store.install(b"already-aged-orphan")
    dest = candidate_store._dest(record)
    _age_installed_object(dest, age_ms=30 * DAY_MS)
    original_mtime = dest.stat().st_mtime

    quarantined = collect_orphan_candidate_objects(
        run_store, candidate_store, storage_lock=lock, grace_ms=7 * DAY_MS
    )
    assert quarantined == [record.bundle_digest]

    quarantine_dir = layout.candidates_root / "quarantine"
    quarantined_files = list(quarantine_dir.iterdir())
    assert len(quarantined_files) == 1
    assert quarantined_files[0].stat().st_mtime > original_mtime

    removed = purge_quarantine_directory(quarantine_dir, storage_lock=lock, grace_ms=7 * DAY_MS)
    assert removed == []
    assert len(list(quarantine_dir.iterdir())) == 1


def test_purge_quarantine_directory_continues_when_a_file_vanishes(
    candidate_store: CandidateObjectStore,
    layout: ControlLayout,
    lock: StorageLock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = candidate_store.install(b"purge-first")
    second = candidate_store.install(b"purge-second")
    candidate_store.quarantine(first.bundle_digest)
    candidate_store.quarantine(second.bundle_digest)
    quarantine_dir = layout.candidates_root / "quarantine"
    quarantined_files = sorted(quarantine_dir.iterdir())
    assert len(quarantined_files) == 2
    for path in quarantined_files:
        _age_installed_object(path, age_ms=30 * DAY_MS)

    vanished = quarantined_files[0]
    surviving = quarantined_files[1]
    original_stat = Path.stat
    seen = {"count": 0}

    def flaky_stat(self: Path, *args: object, **kwargs: object) -> os.stat_result:
        follow_symlinks = kwargs.get("follow_symlinks", True)
        # is_file() and the age check both follow symlinks; is_symlink() does not.
        # Raise only on the age-check stat so the listing still sees the file.
        if self == vanished and follow_symlinks:
            seen["count"] += 1
            if seen["count"] >= 2:
                raise FileNotFoundError
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_stat)

    removed = purge_quarantine_directory(quarantine_dir, storage_lock=lock, grace_ms=7 * DAY_MS)

    assert removed == [surviving.name]
    assert not surviving.exists()
