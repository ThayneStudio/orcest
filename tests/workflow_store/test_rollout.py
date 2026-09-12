"""Staged dark launch, pilot publication, and legacy retirement (issue #698)."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest
from orcest.workflow_store import (
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
    allow_rollout_project,
    apply_rollout_operation,
    archive_legacy_work,
    engine_write_scope,
    evaluate_rollout_checklist,
    get_rollout_projection,
    load_legacy_rollout_controls,
    record_rollout_evidence,
    register_rollout_capacity_pool,
)

pytestmark = pytest.mark.unit

AUTHZ = "sha256:" + "a" * 64
MODE_OP = "12345678-1234-4234-9234-123456789abc"
KEY_ID = "77777777-7777-4777-8777-777777777777"
KEY_OP = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
KEY_OP_2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
PROJECT_ID = "12345678-1234-4234-8234-123456789011"
PROJECT_ID_2 = "12345678-1234-4234-8234-123456789099"
POOL_LEGACY = "11111111-1111-4111-8111-111111111111"
POOL_V1 = "22222222-2222-4222-8222-222222222222"
REPO = "test-org/test-repo"


def _uid() -> str:
    return str(uuid.uuid4())


def _initialize_mode(store: RunStore) -> None:
    result = store.apply_controller_mode_operation(
        controller_mode_operation_id=MODE_OP,
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="bootstrap-service",
        authorization_context_digest=AUTHZ,
    )
    assert result.status == "SUCCEEDED"


def _select_issuance_key(store: RunStore) -> None:
    public_key = b"\x01" * 32
    registered = store.apply_capability_key_operation(
        capability_key_operation_id=KEY_OP,
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=KEY_ID,
        register_public_verification_key=public_key,
        register_public_key_digest=capability_public_key_digest(public_key),
        register_private_signing_secret_ref=f"secret:{KEY_ID}:1",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ,
    )
    assert registered.status == "SUCCEEDED"
    selected = store.apply_capability_key_operation(
        capability_key_operation_id=KEY_OP_2,
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=KEY_ID,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ,
    )
    assert selected.status == "SUCCEEDED"


def _insert_project(store: RunStore, project_id: str = PROJECT_ID, locator: str = REPO) -> None:
    now = 1
    forge_secret = _uid()
    source_secret = _uid()
    publication_secret = _uid()
    forge_id = _uid()
    for secret_id, purpose in (
        (forge_secret, "FORGE_API"),
        (source_secret, "SOURCE_READ"),
        (publication_secret, "PUBLICATION"),
    ):
        store.conn.execute(
            "INSERT INTO secret_current_versions(secret_id, purpose, owner_scope_kind, "
            "owner_scope_id, current_version, last_operation_id, created_at_ms, updated_at_ms) "
            "VALUES (?, ?, 'PROJECT', ?, 1, ?, ?, ?)",
            (secret_id, purpose, project_id, _uid(), now, now),
        )
    store.conn.execute(
        "INSERT INTO forge_instances(forge_instance_id, adapter_kind, canonical_origin, "
        "credential_secret_id, registration_provenance_version, created_at_ms) "
        "VALUES (?, 'GITHUB', ?, ?, 1, ?)",
        (forge_id, f"github.com/{locator}", forge_secret, now),
    )
    store.conn.execute(
        "INSERT INTO projects(project_id, forge_instance_id, installation_or_account_ref, "
        "repository_external_id, repository_locator, default_ref, trusted_base_policy_ref, "
        "budget_policy_ref, budget_reset_window_ref, source_read_secret_id, "
        "publication_secret_id, registration_source_read_secret_version, "
        "registration_publication_secret_version, registration_revision, "
        "registration_operation_id, work_item_discovery_schedule_id, registration_state) "
        "VALUES (?, ?, 'installation-a', ?, ?, 'main', "
        "'base-v1', 'budget-v1', 'window-v1', ?, ?, 1, 1, 1, ?, ?, 'ACTIVE')",
        (project_id, forge_id, locator, locator, source_secret, publication_secret, _uid(), _uid()),
    )
    store.conn.commit()


def _op(
    store: RunStore,
    *,
    kind: str,
    expected_stage: int,
    expected_revision: int,
    requested_stage: int | None = None,
    now_ms: int = 1_000,
    observation_period_ms: int = 1,
) -> object:
    return apply_rollout_operation(
        store,
        rollout_operation_id=_uid(),
        operation_kind=kind,
        expected_stage=expected_stage,
        expected_stage_revision=expected_revision,
        requested_stage=requested_stage,
        authenticated_principal_id="rollout-operator",
        authorization_context_digest=AUTHZ,
        now_ms=now_ms,
        observation_period_ms=observation_period_ms,
    )


def _register_pools(store: RunStore) -> None:
    register_rollout_capacity_pool(
        store,
        capacity_pool_id=POOL_LEGACY,
        template_class="LEGACY",
        template_id="tpl-legacy",
        pool_manager_principal_id="pm-legacy",
        redis_acl_identity="acl-legacy",
        redis_prefix="legacy:",
        consumer_group="workers",
        stream_namespace="tasks:claude",
        reaper_authority="LEGACY_PEL_ALLOWLIST",
        clone_credential_removal_attested=False,
    )
    register_rollout_capacity_pool(
        store,
        capacity_pool_id=POOL_V1,
        template_class="V1_CLONE_FIXED",
        template_id="tpl-v1",
        pool_manager_principal_id="pm-v1",
        redis_acl_identity="acl-v1",
        redis_prefix="v1:",
        consumer_group="workers-v1",
        stream_namespace="tasks:activity:v1:default",
        reaper_authority="V1_AUTHENTICATED_LOSS",
        clone_credential_removal_attested=True,
    )


def _complete_stage0(store: RunStore) -> None:
    _initialize_mode(store)
    _select_issuance_key(store)
    _insert_project(store)
    record_rollout_evidence(
        store, evidence_id=_uid(), stage=0, item_id="backup_restore_drill", now_ms=1
    )
    result = _op(store, kind="INITIALIZE", expected_stage=0, expected_revision=0)
    assert result.status == "SUCCEEDED"


def test_stage0_initialize_and_no_forge_io(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _complete_stage0(store)
        projection = get_rollout_projection(store)
        assert projection.stage == 0
        assert projection.status == "ACTIVE"
        checklist = evaluate_rollout_checklist(store, stage=0, gate="EXIT")
        assert checklist.passed
        created = store.create_due_forge_observation_request(
            forge_observation_request_id=_uid(),
            forge_observation_schedule_id=_uid(),
            now_ms=1,
            controller_mode="MAINTENANCE",
            controller_mode_revision=1,
            credential_purpose="FORGE_API",
            credential_secret_id=_uid(),
            credential_secret_version=1,
            outbox_id=_uid(),
        )
        assert created is None


def test_initialize_replays_and_rejects_conflict(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        op_id = _uid()
        first = apply_rollout_operation(
            store,
            rollout_operation_id=op_id,
            operation_kind="INITIALIZE",
            expected_stage=0,
            expected_stage_revision=0,
            authenticated_principal_id="rollout-operator",
            authorization_context_digest=AUTHZ,
            now_ms=10,
        )
        assert first.status == "SUCCEEDED"
        replay = apply_rollout_operation(
            store,
            rollout_operation_id=op_id,
            operation_kind="INITIALIZE",
            expected_stage=0,
            expected_stage_revision=0,
            authenticated_principal_id="rollout-operator",
            authorization_context_digest=AUTHZ,
            now_ms=10,
        )
        assert replay.replayed is True
        with pytest.raises(IdempotencyConflictError):
            apply_rollout_operation(
                store,
                rollout_operation_id=op_id,
                operation_kind="ADVANCE",
                expected_stage=0,
                expected_stage_revision=0,
                requested_stage=1,
                authenticated_principal_id="rollout-operator",
                authorization_context_digest=AUTHZ,
                now_ms=10,
            )


def test_dual_class_pool_identity_is_rejected(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _register_pools(store)
        with pytest.raises(CasMismatchError, match="DUAL_CLASS_IDENTITY"):
            register_rollout_capacity_pool(
                store,
                capacity_pool_id=_uid(),
                template_class="V1_CLONE_FIXED",
                template_id="tpl-other",
                pool_manager_principal_id="pm-other",
                redis_acl_identity="acl-other",
                redis_prefix="legacy:",
                consumer_group="workers-other",
                stream_namespace="tasks:activity:v1:other",
                reaper_authority="V1_AUTHENTICATED_LOSS",
                clone_credential_removal_attested=True,
            )


def test_project_ref_pr_never_dual_writable(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _insert_project(store)
        allow_rollout_project(
            store,
            project_id=PROJECT_ID,
            repository_locator=REPO,
            engine="V1",
            publication_enabled=False,
            intake_enabled=True,
        )
        assert engine_write_scope(store, repository_locator=REPO) == frozenset({"V1"})
        with pytest.raises(CasMismatchError, match="DUAL_ENGINE_OWNERSHIP"):
            allow_rollout_project(
                store,
                project_id=PROJECT_ID_2,
                repository_locator=REPO,
                engine="LEGACY",
                publication_enabled=False,
                intake_enabled=True,
            )
        assert engine_write_scope(
            store, repository_locator=REPO, change_request_external_id="12"
        ) == frozenset({"LEGACY"})


def test_stage3_fails_on_duplicate_publication(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _complete_stage0(store)
        _register_pools(store)
        record_rollout_evidence(
            store, evidence_id=_uid(), stage=1, item_id="synthetic_canary", now_ms=2
        )
        allow_rollout_project(
            store,
            project_id=PROJECT_ID,
            repository_locator=REPO,
            engine="V1",
            publication_enabled=False,
            intake_enabled=True,
        )
        for requested, expected, revision in ((1, 0, 1), (2, 1, 2), (3, 2, 3)):
            result = _op(
                store,
                kind="ADVANCE",
                expected_stage=expected,
                expected_revision=revision,
                requested_stage=requested,
                now_ms=requested * 10,
            )
            assert result.status == "SUCCEEDED", result.rejection_code
        store.conn.execute("PRAGMA foreign_keys=OFF")
        store.conn.execute(
            "INSERT INTO terminal_duplicate_cleanup_reservations("
            "terminal_duplicate_cleanup_reservation_id, publication_id, effect_generation, "
            "selected_terminal_external_id, selecting_search_observation_id, "
            "selecting_member_ordinal, state, reservation_digest, "
            "created_transition_sequence, created_at_ms) "
            "VALUES (?, ?, 1, '1', ?, 0, 'ACTIVE', ?, 1, 1)",
            (_uid(), _uid(), _uid(), AUTHZ),
        )
        store.conn.commit()
        result = _op(
            store,
            kind="ADVANCE",
            expected_stage=3,
            expected_revision=get_rollout_projection(store).stage_revision,
            requested_stage=4,
            now_ms=40,
        )
        assert result.status == "REJECTED"
        assert result.rejection_code == "DUPLICATE_PUBLICATION"


def test_stage3_fails_on_stale_overwrite(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _complete_stage0(store)
        store.conn.execute("PRAGMA foreign_keys=OFF")
        store.conn.execute(
            "INSERT INTO publication_effect_checkpoints("
            "publication_effect_checkpoint_id, publication_id, effect_generation, "
            "checkpoint_sequence, suboperation_kind, status, checkpoint_digest, "
            "recorded_at_ms) VALUES (?, ?, 1, 1, 'REF_UPDATE', 'CAS_MISMATCH', ?, 1)",
            (_uid(), _uid(), AUTHZ),
        )
        store.conn.commit()
        checklist = evaluate_rollout_checklist(store, stage=3, gate="EXIT")
        failed = {item.item_id for item in checklist.items if not item.passed}
        assert "no_stale_overwrite" in failed


def test_stage5_drain_archive_credentials_and_observation(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _complete_stage0(store)
        _register_pools(store)
        record_rollout_evidence(
            store, evidence_id=_uid(), stage=1, item_id="synthetic_canary", now_ms=2
        )
        allow_rollout_project(
            store,
            project_id=PROJECT_ID,
            repository_locator=REPO,
            engine="V1",
            publication_enabled=False,
            intake_enabled=True,
        )
        revision = 1
        stage = 0
        now = 100
        for target in (1, 2, 3, 4, 5):
            result = _op(
                store,
                kind="ADVANCE",
                expected_stage=stage,
                expected_revision=revision,
                requested_stage=target,
                now_ms=now,
                observation_period_ms=50,
            )
            assert result.status == "SUCCEEDED", (
                result.rejection_code,
                None if result.checklist is None else result.checklist.items,
            )
            stage = target
            revision = get_rollout_projection(store).stage_revision
            now += 10
        freeze = _op(
            store,
            kind="FREEZE_LEGACY",
            expected_stage=5,
            expected_revision=revision,
            now_ms=now,
        )
        assert freeze.status == "SUCCEEDED"
        revision = get_rollout_projection(store).stage_revision
        removed = _op(
            store,
            kind="REMOVE_CREDENTIALS",
            expected_stage=5,
            expected_revision=revision,
            now_ms=now,
        )
        assert removed.status == "SUCCEEDED"
        revision = get_rollout_projection(store).stage_revision
        archived = _op(
            store,
            kind="ARCHIVE_LEGACY",
            expected_stage=5,
            expected_revision=revision,
            now_ms=now,
        )
        assert archived.status == "SUCCEEDED"
        for kind in ("TASK", "PR", "ISSUE", "EVIDENCE"):
            archive_legacy_work(
                store,
                archive_id=_uid(),
                kind=kind,
                identity=f"{kind}-1",
                project_id=PROJECT_ID,
                now_ms=now,
            )
        record_rollout_evidence(
            store, evidence_id=_uid(), stage=5, item_id="representative_runs", now_ms=now
        )
        record_rollout_evidence(
            store, evidence_id=_uid(), stage=5, item_id="legacy_drained", now_ms=now
        )
        revision = get_rollout_projection(store).stage_revision
        too_soon = _op(
            store,
            kind="ADVANCE",
            expected_stage=5,
            expected_revision=revision,
            requested_stage=5,
            now_ms=now + 1,
            observation_period_ms=10_000,
        )
        assert too_soon.status == "REJECTED"
        assert too_soon.rejection_code == "OBSERVATION_PERIOD_OPEN"
        complete = _op(
            store,
            kind="ADVANCE",
            expected_stage=5,
            expected_revision=revision,
            requested_stage=5,
            now_ms=now + 20_000,
            observation_period_ms=50,
        )
        assert complete.status == "SUCCEEDED", complete.rejection_code
        assert get_rollout_projection(store).status == "RETIRED"
        controls = load_legacy_rollout_controls(tmp_path, repository_locator=REPO)
        assert controls.excludes_issue_intake()
        assert controls.omit_raw_task_credentials
        assert engine_write_scope(store, repository_locator=REPO) == frozenset({"V1"})


def test_rollback_blocked_after_publication(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _complete_stage0(store)
        _register_pools(store)
        record_rollout_evidence(
            store, evidence_id=_uid(), stage=1, item_id="synthetic_canary", now_ms=2
        )
        allow_rollout_project(
            store,
            project_id=PROJECT_ID,
            repository_locator=REPO,
            engine="V1",
            publication_enabled=False,
            intake_enabled=True,
        )
        revision = 1
        stage = 0
        for target in (1, 2, 3):
            result = _op(
                store,
                kind="ADVANCE",
                expected_stage=stage,
                expected_revision=revision,
                requested_stage=target,
                now_ms=target * 10,
            )
            assert result.status == "SUCCEEDED"
            stage = target
            revision = get_rollout_projection(store).stage_revision
        store.conn.execute("PRAGMA foreign_keys=OFF")
        store.conn.execute(
            "INSERT INTO publications(publication_id, run_id, candidate_id, "
            "approved_commit_json, effect_generation, deterministic_branch, run_marker, "
            "state, created_transition_sequence, created_at_ms, updated_at_ms) "
            "VALUES (?, ?, ?, '{}', 1, 'refs/heads/orcest/run/x', 'marker', "
            "'ACTIVE', 1, 1, 1)",
            (_uid(), _uid(), _uid()),
        )
        store.conn.commit()
        rolled = _op(
            store,
            kind="ROLLBACK",
            expected_stage=3,
            expected_revision=get_rollout_projection(store).stage_revision,
            requested_stage=1,
            now_ms=90,
        )
        assert rolled.status == "REJECTED"
        assert rolled.rejection_code == "PUBLICATION_EXISTS"
