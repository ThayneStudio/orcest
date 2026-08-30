"""Fixtures for durable object-store tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.candidates import CandidateObjectStore
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.secrets import SecretStore


@pytest.fixture
def layout(tmp_path: Path) -> ControlLayout:
    control = ControlLayout(root=tmp_path / "control")
    control.initialize()
    return control


@pytest.fixture
def quota() -> QuotaConfig:
    return QuotaConfig(
        min_free_bytes=0,
        max_object_bytes=1024 * 1024,
        max_store_bytes=8 * 1024 * 1024,
        max_objects=1024,
    )


@pytest.fixture
def storage_lock(layout: ControlLayout) -> StorageLock:
    return StorageLock(layout.storage_lock_path)


@pytest.fixture
def blob_store(
    layout: ControlLayout, quota: QuotaConfig, storage_lock: StorageLock
) -> WorkflowBlobStore:
    return WorkflowBlobStore(layout, quota=quota, lock=storage_lock)


@pytest.fixture
def candidate_store(
    layout: ControlLayout, quota: QuotaConfig, storage_lock: StorageLock
) -> CandidateObjectStore:
    return CandidateObjectStore(layout, quota=quota, lock=storage_lock)


@pytest.fixture
def secret_store(
    layout: ControlLayout, quota: QuotaConfig, storage_lock: StorageLock
) -> SecretStore:
    return SecretStore(layout, quota=quota, lock=storage_lock)
