"""Crash boundaries, quota/free-space rejection, and concurrent no-clobber."""

from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path

import pytest

from orcest.workflow_store.v1 import fs as fs_mod
from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.candidates import CandidateObjectStore
from orcest.workflow_store.v1.errors import IntegrityConflictError, QuotaExceededError
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.secrets import SecretStore

PAYLOAD = b'{"k":1}'


class Crash(RuntimeError):
    """Simulated process kill at a write/promotion boundary."""


def _blob_store(
    tmp_path: Path,
    quota: QuotaConfig | None = None,
    *,
    free_space: object = None,
) -> WorkflowBlobStore:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    lock = StorageLock(layout.storage_lock_path)
    q = quota or QuotaConfig(min_free_bytes=0, max_object_bytes=1024, max_store_bytes=4096)
    if free_space is None:
        return WorkflowBlobStore(layout, quota=q, lock=lock)
    return WorkflowBlobStore(
        layout,
        quota=q,
        lock=lock,
        free_space=free_space,  # type: ignore[arg-type]
    )


def test_crash_before_file_fsync_leaves_no_object_and_no_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _blob_store(tmp_path)
    live: dict[str, str] = {}

    def boom(_fd: int) -> None:
        raise Crash("crash before fsync-file")

    monkeypatch.setattr(os, "fsync", boom)
    with pytest.raises(Crash):
        store.install(
            "CONFIG_JSON",
            PAYLOAD,
            reference=lambda r: live.__setitem__(r.blob_digest, "x"),
        )
    assert live == {}
    assert list(store.iter_objects()) == []
    incoming = list((store._incoming).glob("*"))
    assert incoming == []


def test_crash_before_promote_leaves_no_live_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _blob_store(tmp_path)
    live: list[str] = []

    def boom(src: str | os.PathLike[str], dst: str | os.PathLike[str], **_kwargs: object) -> None:
        raise Crash("crash before no-clobber promote")

    monkeypatch.setattr(os, "link", boom)
    with pytest.raises(Crash):
        store.install("CONFIG_JSON", PAYLOAD, reference=lambda r: live.append(r.blob_digest))
    assert live == []
    assert list(store.iter_objects()) == []


def test_crash_after_promote_before_reference_leaves_exact_object(
    tmp_path: Path,
) -> None:
    store = _blob_store(tmp_path)
    live: list[str] = []

    def reference(record: object) -> None:
        raise Crash("crash after promotion before reference")

    with pytest.raises(Crash):
        store.install("CONFIG_JSON", PAYLOAD, reference=reference)
    assert live == []
    records = list(store.iter_objects())
    assert len(records) == 1
    assert store.read(records[0].blob_digest) == PAYLOAD


def test_quota_rejects_oversize_object_before_accepting_bytes(tmp_path: Path) -> None:
    store = _blob_store(tmp_path, QuotaConfig(min_free_bytes=0, max_object_bytes=4))
    with pytest.raises(QuotaExceededError, match="max_object_bytes"):
        store.install("CONFIG_JSON", PAYLOAD)
    assert list(store.iter_objects()) == []
    assert list(store._incoming.glob("*")) == []


def test_quota_rejects_store_capacity(tmp_path: Path) -> None:
    store = _blob_store(
        tmp_path, QuotaConfig(min_free_bytes=0, max_object_bytes=100, max_store_bytes=8)
    )
    store.install("CONFIG_JSON", b"abcd")
    with pytest.raises(QuotaExceededError, match="store quota"):
        store.install("POLICY_JSON", b"abcdefgh")
    assert sum(1 for _ in store.iter_objects()) == 1


def test_free_space_safety_floor_rejects_before_write(tmp_path: Path) -> None:
    def free_space(_path: Path) -> int:
        return 50

    store = _blob_store(
        tmp_path,
        QuotaConfig(min_free_bytes=48, max_object_bytes=100),
        free_space=free_space,
    )
    with pytest.raises(QuotaExceededError, match="safety floor"):
        store.install("CONFIG_JSON", PAYLOAD)
    assert list(store.iter_objects()) == []


def test_insufficient_free_space_rejects(tmp_path: Path) -> None:
    store = _blob_store(
        tmp_path,
        QuotaConfig(min_free_bytes=0, max_object_bytes=100),
        free_space=lambda _p: 3,
    )
    with pytest.raises(QuotaExceededError, match="insufficient free space"):
        store.install("CONFIG_JSON", PAYLOAD)


def test_empty_object_rejected(tmp_path: Path) -> None:
    store = _blob_store(tmp_path)
    with pytest.raises(QuotaExceededError, match="positive"):
        store.install("CONFIG_JSON", b"")


def test_candidate_quota_and_crash_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    lock = StorageLock(layout.storage_lock_path)
    store = CandidateObjectStore(
        layout,
        quota=QuotaConfig(min_free_bytes=0, max_object_bytes=16),
        lock=lock,
    )
    with pytest.raises(QuotaExceededError):
        store.install(b"0123456789abcdef-too-long")
    live: list[str] = []
    with pytest.raises(Crash):
        store.install(b"bundle-bytes", reference=lambda r: (_ for _ in ()).throw(Crash("ref")))
    assert live == []
    assert list(store.iter_objects())[0].byte_length == len(b"bundle-bytes")


def test_concurrent_secret_writers_fail_closed(tmp_path: Path) -> None:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    lock = StorageLock(layout.storage_lock_path)
    quota = QuotaConfig(min_free_bytes=0, max_object_bytes=1024, max_store_bytes=64 * 1024)
    store = SecretStore(layout, quota=quota, lock=lock)
    secret_id = str(uuid.uuid4())
    errors: list[BaseException] = []
    wins: list[bytes] = []

    def writer(value: bytes) -> None:
        try:
            store.put_version(secret_id, 1, value)
            wins.append(value)
        except IntegrityConflictError as exc:
            errors.append(exc)
        except Exception as exc:  # pragma: no cover - unexpected
            errors.append(exc)

    threads = [
        threading.Thread(target=writer, args=(b"first-secret-value-xxxx",)),
        threading.Thread(target=writer, args=(b"second-secret-value-yyyy",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stored = store.read_value(secret_id, 1)
    assert stored in {b"first-secret-value-xxxx", b"second-secret-value-yyyy"}
    assert len(wins) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], IntegrityConflictError)
    assert b"first-secret-value-xxxx" not in str(errors[0]).encode()
    assert b"second-secret-value-yyyy" not in str(errors[0]).encode()


def test_concurrent_identical_blob_writers_are_idempotent(tmp_path: Path) -> None:
    store = _blob_store(tmp_path)
    results: list[str] = []
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            record = store.install("CONFIG_JSON", PAYLOAD)
            results.append(record.blob_digest)
        except Exception as exc:  # pragma: no cover - unexpected
            errors.append(exc)

    threads = [threading.Thread(target=writer) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(set(results)) == 1
    assert sum(1 for _ in store.iter_objects()) == 1


def test_write_exclusive_file_fsyncs_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = tmp_path / "exclusive"
    parent.mkdir()
    synced: list[Path] = []
    real = fs_mod.fsync_dir

    def spy(path: Path) -> None:
        synced.append(Path(path).resolve())
        real(path)

    monkeypatch.setattr(fs_mod, "fsync_dir", spy)
    target = parent / "created.bin"
    fs_mod.write_exclusive_file(target, b"exclusive-bytes")
    assert parent.resolve() in synced
    assert target.read_bytes() == b"exclusive-bytes"


def test_integrity_key_create_fsyncs_secrets_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    synced: list[Path] = []
    real = fs_mod.fsync_dir

    def spy(path: Path) -> None:
        synced.append(Path(path).resolve())
        real(path)

    monkeypatch.setattr(fs_mod, "fsync_dir", spy)
    monkeypatch.setattr("orcest.workflow_store.v1.secrets.fsync_dir", spy)
    SecretStore(
        layout,
        quota=QuotaConfig(min_free_bytes=0, max_object_bytes=1024),
        lock=StorageLock(layout.storage_lock_path),
    )
    assert layout.secrets_root.resolve() in synced
    assert (layout.secrets_root / "integrity.key").is_file()


def test_read_exact_file_missing_is_integrity_conflict(tmp_path: Path) -> None:
    with pytest.raises(IntegrityConflictError, match="object file is missing"):
        fs_mod.read_exact_file(tmp_path / "absent.bin", max_bytes=32)


def test_storage_lock_excludes_other_holder(tmp_path: Path) -> None:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    a = StorageLock(layout.storage_lock_path)
    b = StorageLock(layout.storage_lock_path)
    assert a.acquire(blocking=True) is True
    try:
        assert b.acquire(blocking=False) is False
    finally:
        a.release()
    assert b.acquire(blocking=False) is True
    b.release()


def test_storage_lock_reentrant_shortcut_is_thread_owned(tmp_path: Path) -> None:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    lock = StorageLock(layout.storage_lock_path)

    assert lock.acquire(blocking=True) is True
    results: list[bool] = []
    thread = threading.Thread(target=lambda: results.append(lock.acquire(blocking=False)))
    thread.start()
    thread.join(timeout=5)
    try:
        assert results == [False]
    finally:
        lock.release()

    assert lock.acquire(blocking=True) is True
    assert lock.acquire(blocking=False) is True
    lock.release()
    lock.release()
