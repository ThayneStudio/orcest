"""Workflow Blob identity, no-clobber, and exact-object APIs."""

from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import workflow_blob_digest
from orcest.workflow_contract.v1.enums import UnknownEnumValueError
from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.errors import IntegrityConflictError, ObjectNotFoundError
from orcest.workflow_store.v1.fs import FILE_MODE, ControlLayout, QuotaConfig, StorageLock

CONFIG_BYTES = b'{"reducer_version":1,"profiles":[]}'


def test_media_kind_changes_identity_and_does_not_alias(blob_store: WorkflowBlobStore) -> None:
    config = blob_store.install("CONFIG_JSON", CONFIG_BYTES)
    policy = blob_store.install("POLICY_JSON", CONFIG_BYTES)
    assert config.blob_digest != policy.blob_digest
    assert config.byte_length == policy.byte_length == len(CONFIG_BYTES)
    assert config.media_kind == "CONFIG_JSON"
    assert policy.media_kind == "POLICY_JSON"
    assert config.storage_key != policy.storage_key
    assert blob_store.read(config.blob_digest) == CONFIG_BYTES
    assert blob_store.read(policy.blob_digest) == CONFIG_BYTES
    records = {record.blob_digest: record for record in blob_store.iter_objects()}
    assert set(records) == {config.blob_digest, policy.blob_digest}


def test_install_is_idempotent_for_exact_same_blob(blob_store: WorkflowBlobStore) -> None:
    first = blob_store.install("PROMPT_UTF8", b"implement the change")
    second = blob_store.install("PROMPT_UTF8", b"implement the change")
    assert first == second
    assert sum(1 for _ in blob_store.iter_objects()) == 1


def test_unknown_media_kind_fails_closed(blob_store: WorkflowBlobStore) -> None:
    with pytest.raises(UnknownEnumValueError):
        blob_store.install("IMAGE_PNG", CONFIG_BYTES)


def test_verify_recomputes_domain_separated_digest(
    blob_store: WorkflowBlobStore, layout: ControlLayout
) -> None:
    record = blob_store.install("SERVER_POLICY_JSON", CONFIG_BYTES)
    dest = layout.blobs_root / record.storage_key
    assert dest.is_file()
    assert dest.stat().st_mode & 0o777 == FILE_MODE
    verified = blob_store.verify(record.blob_digest)
    assert verified.blob_digest == workflow_blob_digest("SERVER_POLICY_JSON", CONFIG_BYTES)
    missing = workflow_blob_digest("CONFIG_JSON", b'{"other":true}')
    with pytest.raises(ObjectNotFoundError):
        blob_store.verify(missing)


def test_no_clobber_mismatch_leaves_existing_bytes(
    blob_store: WorkflowBlobStore, layout: ControlLayout
) -> None:
    record = blob_store.identity("CONFIG_JSON", CONFIG_BYTES)
    dest = layout.blobs_root / record.storage_key
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b'{"tampered":true}')
    dest.chmod(FILE_MODE)
    with pytest.raises(IntegrityConflictError, match="no-clobber"):
        blob_store.install("CONFIG_JSON", CONFIG_BYTES)
    assert dest.read_bytes() == b'{"tampered":true}'


def test_write_before_reference_skips_callback_until_durable(
    blob_store: WorkflowBlobStore,
) -> None:
    live: dict[str, str] = {}

    def reference(record: object) -> None:
        from orcest.workflow_store.v1.blobs import WorkflowBlobRecord

        assert isinstance(record, WorkflowBlobRecord)
        blob_store.verify(record.blob_digest)
        live[record.blob_digest] = record.storage_key

    installed = blob_store.install("CONFIG_JSON", CONFIG_BYTES, reference=reference)
    assert installed.blob_digest in live
    assert live[installed.blob_digest] == installed.storage_key


def test_reference_failure_leaves_object_without_live_ref(
    blob_store: WorkflowBlobStore,
) -> None:
    live: list[str] = []

    def reference(record: object) -> None:
        raise RuntimeError("crash after promotion before reference")

    with pytest.raises(RuntimeError, match="crash after promotion"):
        blob_store.install("CONFIG_JSON", CONFIG_BYTES, reference=reference)
    assert live == []
    records = list(blob_store.iter_objects())
    assert len(records) == 1
    assert blob_store.read(records[0].blob_digest) == CONFIG_BYTES


def test_corrupt_installed_blob_fails_verify(
    blob_store: WorkflowBlobStore, layout: ControlLayout
) -> None:
    record = blob_store.install("CONFIG_JSON", CONFIG_BYTES)
    dest = layout.blobs_root / record.storage_key
    dest.write_bytes(CONFIG_BYTES + b"\n")
    dest.chmod(FILE_MODE)
    with pytest.raises(IntegrityConflictError):
        blob_store.verify(record.blob_digest)


def test_blob_store_uses_shared_storage_lock(tmp_path: Path, quota: QuotaConfig) -> None:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    lock = StorageLock(layout.storage_lock_path)
    store = WorkflowBlobStore(layout, quota=quota, lock=lock)
    assert lock.held() is False
    store.install("CONFIG_JSON", CONFIG_BYTES)
    assert lock.held() is False
