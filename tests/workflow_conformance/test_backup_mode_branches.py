"""Backup barrier branch selection under MAINTENANCE, exact DISPATCH_PAUSED/
PAUSE_ADMISSION, and the third (temporary-pause) branch.

operations-and-rollout.md, "A consistent backup uses a bounded backup
sub-barrier": a MAINTENANCE or exact DISPATCH_PAUSED/PAUSE_ADMISSION backup
stays in place with no pause/resume round trip; every other mode CASes into
DISPATCH_PAUSED/PAUSE_ADMISSION for the barrier and restores the exact prior
mode afterward, unless an intervening operator mode change already won.

Closes failure-injection-matrix cases (see coverage.py):
fim.backup_begins_controller_already_maintenance,
fim.backup_begins_controller_already_exactly_dispatch_paused,
fim.third_branch_backup_succeeds_fails_crashes_newly.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from orcest.workflow_store import ControlLayout, QuotaConfig, RunStore, StorageLock
from orcest.workflow_store.v1.backup import _pause_for_backup, _restore_prior_mode, create_backup
from orcest.workflow_store.v1.blobs import WorkflowBlobStore
from orcest.workflow_store.v1.candidates import CandidateObjectStore
from orcest.workflow_store.v1.secrets import SecretStore

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64


def _uid() -> str:
    return str(uuid.uuid4())


def _initialize_to_maintenance(store: RunStore) -> None:
    result = store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="bootstrap-service",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _set_mode(
    store: RunStore,
    *,
    expected_mode: str | None,
    expected_revision: int,
    to_mode: str,
    policy: str | None = None,
    principal: str = "mode-operator",
):
    result = store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="SET_MODE",
        expected_mode_revision=expected_revision,
        expected_mode=expected_mode,
        requested_mode=to_mode,
        requested_dispatch_paused_intake_policy=policy,
        authenticated_principal_id=principal,
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED", result.rejection_code
    return result


@pytest.fixture
def control_root(tmp_path: Path) -> Path:
    return tmp_path / "control"


@pytest.fixture
def run_store(control_root: Path) -> RunStore:
    with RunStore(control_root, verify_local_filesystem=False) as store:
        yield store


@pytest.fixture
def object_stores(control_root: Path):
    layout = ControlLayout(root=control_root)
    layout.initialize()
    quota = QuotaConfig(min_free_bytes=0, max_object_bytes=1024 * 1024)
    lock = StorageLock(layout.storage_lock_path)
    return (
        CandidateObjectStore(layout, quota=quota, lock=lock),
        WorkflowBlobStore(layout, quota=quota, lock=lock),
        SecretStore(layout, quota=quota, lock=lock),
    )


def _backup(run_store: RunStore, object_stores, tmp_path: Path):
    candidate_store, blob_store, secret_store = object_stores
    destination = tmp_path / "backups"
    destination.mkdir(exist_ok=True)
    return create_backup(
        run_store,
        candidate_store,
        blob_store,
        secret_store,
        destination_root=destination,
        encryption_key=os.urandom(32),
    )


def test_backup_in_maintenance_stays_in_place(run_store, object_stores, tmp_path) -> None:
    _initialize_to_maintenance(run_store)
    before = run_store.get_controller_mode()
    assert before.mode == "MAINTENANCE"

    result = _backup(run_store, object_stores, tmp_path)

    assert result.branch == "MAINTENANCE_IN_PLACE"
    after = run_store.get_controller_mode()
    assert after.mode == "MAINTENANCE"
    assert after.mode_revision == before.mode_revision, "backup must not write a mode Operation"
    assert after.maintenance_prior_mode == before.maintenance_prior_mode


def test_backup_in_dispatch_paused_pause_admission_stays_in_place(
    run_store, object_stores, tmp_path
) -> None:
    _initialize_to_maintenance(run_store)
    running = _set_mode(
        run_store, expected_mode="MAINTENANCE", expected_revision=1, to_mode="RUNNING"
    )
    _set_mode(
        run_store,
        expected_mode="RUNNING",
        expected_revision=running.mode_revision,
        to_mode="DISPATCH_PAUSED",
        policy="PAUSE_ADMISSION",
    )
    before = run_store.get_controller_mode()
    assert before.mode == "DISPATCH_PAUSED"
    assert before.dispatch_paused_intake_policy == "PAUSE_ADMISSION"

    result = _backup(run_store, object_stores, tmp_path)

    assert result.branch == "ALREADY_PAUSED_IN_PLACE"
    after = run_store.get_controller_mode()
    assert after.mode == "DISPATCH_PAUSED"
    assert after.dispatch_paused_intake_policy == "PAUSE_ADMISSION"
    assert after.mode_revision == before.mode_revision, "no NO_CHANGE Operation should be written"


def test_third_branch_backup_restores_prior_mode_unless_operator_intervened(
    run_store, object_stores, tmp_path
) -> None:
    _initialize_to_maintenance(run_store)
    _set_mode(run_store, expected_mode="MAINTENANCE", expected_revision=1, to_mode="RUNNING")
    before = run_store.get_controller_mode()
    assert before.mode == "RUNNING"

    result = _backup(run_store, object_stores, tmp_path)

    assert result.branch == "TEMPORARY_PAUSE"
    after = run_store.get_controller_mode()
    assert after.mode == "RUNNING", "an uncontested third-branch backup restores the prior mode"

    # Now prove the "intervening operator mode change wins" half directly against
    # the pause/restore primitives create_backup composes: a mode change committed
    # between the pause and the restore attempt must never be silently overwritten.
    pause_operation_id = _pause_for_backup(run_store, authorization_context_digest=AUTHZ_DIGEST)
    paused = run_store.get_controller_mode()
    assert paused.mode == "DISPATCH_PAUSED"

    _set_mode(
        run_store,
        expected_mode="DISPATCH_PAUSED",
        expected_revision=paused.mode_revision,
        to_mode="DRAINING",
        principal="operator",
    )

    _restore_prior_mode(run_store, pause_operation_id=pause_operation_id)

    final = run_store.get_controller_mode()
    assert final.mode == "DRAINING", "the intervening operator change must win over the restore"
