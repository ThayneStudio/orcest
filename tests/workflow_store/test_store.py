from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest, request_digest
from orcest.workflow_store import (
    DEFAULT_REDUCER_VERSION,
    PRIOR_STATE_NONE,
    SCHEMA_VERSION,
    SUPPORTED_REDUCER_VERSIONS,
    CasMismatchError,
    FaultInjectionPoint,
    IdempotencyConflictError,
    ReducerVersionError,
    RunStore,
    SchemaVersionError,
    TransactionFault,
    WorkflowGateClosedError,
    WriterLockError,
    open_read_only,
)
from orcest.workflow_store.store import _verify_local_state_root

RUN_ID = "11111111-1111-1111-1111-111111111111"
TRANSITION_ID = "22222222-2222-2222-2222-222222222222"
OUTBOX_ID = "33333333-3333-3333-3333-333333333333"
PROJECTION_ID = "44444444-4444-4444-4444-444444444444"
OPERATION_ID = "55555555-5555-5555-5555-555555555555"
IDEMPOTENCY_KEY = "66666666-6666-6666-6666-666666666666"
REDUCER_VERSION = DEFAULT_REDUCER_VERSION
AUTHZ_DIGEST = "sha256:" + "a" * 64
KEY_ID = "77777777-7777-7777-7777-777777777777"
KEY_ID_2 = "88888888-8888-8888-8888-888888888888"
KEY_ID_3 = "99999999-9999-9999-9999-999999999999"
KEY_OP_ID = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa"
KEY_OP_ID_2 = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
KEY_OP_ID_3 = "cccccccc-cccc-4ccc-cccc-cccccccccccc"
KEY_OP_ID_4 = "dddddddd-dddd-4ddd-dddd-dddddddddddd"
CAPABILITY_JTI = "eeeeeeee-eeee-4eee-eeee-eeeeeeeeeeee"

pytestmark = pytest.mark.unit


def _digest(value: object) -> str:
    return request_digest(value)


def _create_run(store: RunStore, run_id: str = RUN_ID) -> None:
    store.create_run(
        run_id=run_id,
        project_id="project-a",
        work_item_key=f"work-{run_id}",
        state="ADMITTED",
        reducer_version=REDUCER_VERSION,
    )


def _public_key(seed: int) -> bytes:
    return bytes([seed]) * 32


def _register_key(
    store: RunStore,
    *,
    key_id: str = KEY_ID,
    operation_id: str = KEY_OP_ID,
    expected_revision: int = 0,
    expected_key: str | None = None,
) -> None:
    public_key = _public_key(1 if key_id == KEY_ID else 2)
    result = store.apply_capability_key_operation(
        capability_key_operation_id=operation_id,
        kind="REGISTER",
        expected_registry_revision=expected_revision,
        expected_issuance_key_id=expected_key,
        target_capability_signing_key_id=key_id,
        register_public_verification_key=public_key,
        register_public_key_digest=capability_public_key_digest(public_key),
        register_private_signing_secret_ref=f"secret:{key_id}:1",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _select_key(
    store: RunStore,
    *,
    key_id: str = KEY_ID,
    operation_id: str = KEY_OP_ID_2,
    expected_revision: int = 1,
    expected_key: str | None = None,
) -> None:
    result = store.apply_capability_key_operation(
        capability_key_operation_id=operation_id,
        kind="SELECT",
        expected_registry_revision=expected_revision,
        expected_issuance_key_id=expected_key,
        target_capability_signing_key_id=key_id,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _initialize_mode(store: RunStore) -> None:
    result = store.apply_controller_mode_operation(
        controller_mode_operation_id="12345678-1234-4234-9234-123456789abc",
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="bootstrap-service",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _select_count(statements: list[str], table: str) -> int:
    return sum(
        1
        for statement in statements
        if statement.strip().lower().startswith("select ") and f" from {table}" in statement.lower()
    )


def test_capability_key_registration_requires_private_key_proof(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        public_key = _public_key(1)
        result = store.apply_capability_key_operation(
            capability_key_operation_id=KEY_OP_ID,
            kind="REGISTER",
            expected_registry_revision=0,
            expected_issuance_key_id=None,
            target_capability_signing_key_id=KEY_ID,
            register_public_verification_key=public_key,
            register_public_key_digest=capability_public_key_digest(public_key),
            register_private_signing_secret_ref=f"secret:{KEY_ID}:1",
            register_not_before_ms=0,
            authenticated_principal_id="key-operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )

        assert result.status == "REJECTED"
        assert result.rejection_code == "INTEGRITY_CONFLICT"
        assert store.get_capability_signing_key(KEY_ID) is None


def test_default_reducer_version_is_an_explicit_supported_constant() -> None:
    assert DEFAULT_REDUCER_VERSION == "workflow-control-v1/reducer-0"
    assert DEFAULT_REDUCER_VERSION in SUPPORTED_REDUCER_VERSIONS


def test_create_run_defaults_to_explicit_reducer_version(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.create_run(
                run_id=RUN_ID,
                project_id="project-a",
                work_item_key="work-1",
                state="ADMITTED",
            )
        row = store.conn.execute(
            "SELECT reducer_version FROM runs WHERE run_id = ?",
            (RUN_ID,),
        ).fetchone()
        assert row[0] == DEFAULT_REDUCER_VERSION


def test_create_run_replay_is_idempotent(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
        with store.transaction():
            _create_run(store)
        assert store.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1


def test_create_run_replay_after_transition_is_idempotent(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
            transition = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="ADMIT",
                trigger_id="admit-source-1",
                next_state="PLANNING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"source": 1}),
                specification_generation=1,
            )
        with store.transaction():
            _create_run(store)
            replayed = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="ADMIT",
                trigger_id="admit-source-1",
                next_state="PLANNING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"source": 1}),
                specification_generation=1,
            )
        assert replayed == transition
        assert (
            store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (RUN_ID,)).fetchone()[0]
            == "PLANNING"
        )
        assert store.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1


def test_create_run_replay_conflicts_on_run_identity_mismatch(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
        with pytest.raises(IdempotencyConflictError):
            with store.transaction():
                store.create_run(
                    run_id=RUN_ID,
                    project_id="project-b",
                    work_item_key="work-1",
                    state="ADMITTED",
                    reducer_version=REDUCER_VERSION,
                )


def test_create_run_conflicts_on_active_work_item_mismatch(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.create_run(
                run_id=RUN_ID,
                project_id="project-a",
                work_item_key="work-1",
                state="ADMITTED",
                reducer_version=REDUCER_VERSION,
            )
        with pytest.raises(IdempotencyConflictError):
            with store.transaction():
                store.create_run(
                    run_id="77777777-7777-7777-7777-777777777777",
                    project_id="project-a",
                    work_item_key="work-1",
                    state="ADMITTED",
                    reducer_version=REDUCER_VERSION,
                )


def test_schema_migration_rolls_back_as_one_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_open = RunStore._open_connection

    class _FailOnUserVersion:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def execute(self, sql: str, *args: object, **kwargs: object) -> sqlite3.Cursor:
            if isinstance(sql, str) and sql.startswith("PRAGMA user_version="):
                raise sqlite3.OperationalError("injected migration failure")
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(self._conn, name)

    def open_and_wrap(self: RunStore) -> _FailOnUserVersion:
        return _FailOnUserVersion(real_open(self))

    monkeypatch.setattr(RunStore, "_open_connection", open_and_wrap)
    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        RunStore(tmp_path, verify_local_filesystem=False)

    conn = sqlite3.connect(tmp_path / "workflow.db")
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "runs" not in tables
        assert "schema_migrations" not in tables
        assert "controller_mode" not in tables
    finally:
        conn.close()


def test_startup_sets_sqlite_profile_and_bootstrap_mode(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        assert store.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert store.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert store.conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert store.conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert store.conn.execute("PRAGMA trusted_schema").fetchone()[0] == 0
        mode = store.conn.execute("SELECT * FROM controller_mode").fetchone()
        assert mode["controller_id"] == "ORCEST_V1"
        assert mode["mode_revision"] == 0
        assert mode["mode"] is None
        registry = store.conn.execute("SELECT * FROM capability_key_registry").fetchone()
        assert registry["registry_id"] == "ORCEST_V1"
        assert registry["registry_revision"] == 0
        assert registry["current_issuance_key_id"] is None


def test_controller_mode_initializes_to_maintenance_without_history(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        result = store.apply_controller_mode_operation(
            controller_mode_operation_id=OPERATION_ID,
            operation_kind="INITIALIZE",
            expected_mode_revision=0,
            expected_mode=None,
            requested_mode="MAINTENANCE",
            authenticated_principal_id="bootstrap-service",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert result.status == "SUCCEEDED"
        assert result.mode_revision == 1
        assert result.mode == "MAINTENANCE"
        assert result.dispatch_paused_intake_policy is None
        mode = store.get_controller_mode()
        assert mode.mode_revision == 1
        assert mode.maintenance_prior_mode is None
        assert mode.last_operation_id == OPERATION_ID

        replay = store.apply_controller_mode_operation(
            controller_mode_operation_id=OPERATION_ID,
            operation_kind="INITIALIZE",
            expected_mode_revision=0,
            expected_mode=None,
            requested_mode="MAINTENANCE",
            authenticated_principal_id="bootstrap-service",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert replay.replayed is True
        assert replay.response_json == result.response_json
        operation_count = store.conn.execute(
            "SELECT COUNT(*) FROM controller_mode_operations"
        ).fetchone()[0]
        assert operation_count == 1


@pytest.mark.parametrize(
    ("from_mode", "from_policy", "to_mode", "to_policy"),
    [
        (from_mode, from_policy, to_mode, to_policy)
        for from_mode, from_policy in [
            ("RUNNING", None),
            ("INTAKE_PAUSED", None),
            ("DISPATCH_PAUSED", "ALLOW_ADMISSION"),
            ("DISPATCH_PAUSED", "PAUSE_ADMISSION"),
            ("DRAINING", None),
            ("MAINTENANCE", None),
        ]
        for to_mode, to_policy in [
            ("RUNNING", None),
            ("INTAKE_PAUSED", None),
            ("DISPATCH_PAUSED", "ALLOW_ADMISSION"),
            ("DISPATCH_PAUSED", "PAUSE_ADMISSION"),
            ("DRAINING", None),
            ("MAINTENANCE", None),
        ]
        if (from_mode, from_policy) != (to_mode, to_policy)
    ],
)
def test_controller_mode_set_mode_closed_matrix_all_distinct_pairs(
    tmp_path: Path,
    from_mode: str,
    from_policy: str | None,
    to_mode: str,
    to_policy: str | None,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        revision = 1
        if (from_mode, from_policy) != ("MAINTENANCE", None):
            store.apply_controller_mode_operation(
                controller_mode_operation_id="aaaaaaaa-1111-4111-9111-aaaaaaaaaaaa",
                operation_kind="SET_MODE",
                expected_mode_revision=revision,
                expected_mode="MAINTENANCE",
                requested_mode=from_mode,
                requested_dispatch_paused_intake_policy=from_policy,
                authenticated_principal_id="operator",
                authorization_context_digest=AUTHZ_DIGEST,
            )
            revision += 1

        result = store.apply_controller_mode_operation(
            controller_mode_operation_id="bbbbbbbb-2222-4222-9222-bbbbbbbbbbbb",
            operation_kind="SET_MODE",
            expected_mode_revision=revision,
            expected_mode=from_mode,
            requested_mode=to_mode,
            requested_dispatch_paused_intake_policy=to_policy,
            authenticated_principal_id="operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert result.status == "SUCCEEDED"
        assert result.mode_revision == revision + 1
        assert result.mode == to_mode
        mode = store.get_controller_mode()
        assert mode.mode == to_mode
        if to_mode == "MAINTENANCE":
            assert mode.maintenance_prior_mode == from_mode
            assert mode.maintenance_prior_dispatch_paused_intake_policy == from_policy
        else:
            assert mode.maintenance_prior_mode is None


@pytest.mark.parametrize(
    ("mode", "policy"),
    [
        ("RUNNING", None),
        ("INTAKE_PAUSED", None),
        ("DISPATCH_PAUSED", "ALLOW_ADMISSION"),
        ("DISPATCH_PAUSED", "PAUSE_ADMISSION"),
        ("DRAINING", None),
        ("MAINTENANCE", None),
    ],
)
def test_controller_mode_rejects_no_change_except_dispatch_policy_change(
    tmp_path: Path, mode: str, policy: str | None
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        revision = 1
        if (mode, policy) != ("MAINTENANCE", None):
            store.apply_controller_mode_operation(
                controller_mode_operation_id="aaaaaaaa-1111-4111-9111-aaaaaaaaaaaa",
                operation_kind="SET_MODE",
                expected_mode_revision=revision,
                expected_mode="MAINTENANCE",
                requested_mode=mode,
                requested_dispatch_paused_intake_policy=policy,
                authenticated_principal_id="operator",
                authorization_context_digest=AUTHZ_DIGEST,
            )
            revision += 1
        no_change = store.apply_controller_mode_operation(
            controller_mode_operation_id="bbbbbbbb-2222-4222-9222-bbbbbbbbbbbb",
            operation_kind="SET_MODE",
            expected_mode_revision=revision,
            expected_mode=mode,
            requested_mode=mode,
            requested_dispatch_paused_intake_policy=policy,
            authenticated_principal_id="operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert no_change.status == "REJECTED"
        assert no_change.rejection_code == "NO_CHANGE"

        if mode == "DISPATCH_PAUSED":
            changed = store.apply_controller_mode_operation(
                controller_mode_operation_id="cccccccc-3333-4333-9333-cccccccccccc",
                operation_kind="SET_MODE",
                expected_mode_revision=revision,
                expected_mode=mode,
                requested_mode=mode,
                requested_dispatch_paused_intake_policy=(
                    "PAUSE_ADMISSION" if policy == "ALLOW_ADMISSION" else "ALLOW_ADMISSION"
                ),
                authenticated_principal_id="operator",
                authorization_context_digest=AUTHZ_DIGEST,
            )
            assert changed.status == "SUCCEEDED"


@pytest.mark.parametrize(
    (
        "mode",
        "policy",
        "new_admission",
        "new_claims",
        "first_result_mutation",
        "forge_reconciliation",
    ),
    [
        ("RUNNING", None, True, True, True, True),
        ("INTAKE_PAUSED", None, False, True, True, True),
        ("DISPATCH_PAUSED", "ALLOW_ADMISSION", True, False, True, True),
        ("DISPATCH_PAUSED", "PAUSE_ADMISSION", False, False, True, True),
        ("DRAINING", None, False, False, True, True),
        ("MAINTENANCE", None, False, False, False, False),
    ],
)
def test_controller_mode_behavior_gates_are_closed(
    tmp_path: Path,
    mode: str,
    policy: str | None,
    new_admission: bool,
    new_claims: bool,
    first_result_mutation: bool,
    forge_reconciliation: bool,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _register_key(store)
        _select_key(store)
        revision = 1
        if (mode, policy) != ("MAINTENANCE", None):
            result = store.apply_controller_mode_operation(
                controller_mode_operation_id="aaaaaaaa-1111-4111-9111-aaaaaaaaaaaa",
                operation_kind="SET_MODE",
                expected_mode_revision=revision,
                expected_mode="MAINTENANCE",
                requested_mode=mode,
                requested_dispatch_paused_intake_policy=policy,
                authenticated_principal_id="operator",
                authorization_context_digest=AUTHZ_DIGEST,
            )
            assert result.status == "SUCCEEDED"
        gates = store.controller_gate_permissions()
        assert gates.new_admission is new_admission
        assert gates.new_claims is new_claims
        assert gates.first_result_mutation is first_result_mutation
        assert gates.existing_result_replay is True
        assert gates.forge_reconciliation is forge_reconciliation
        assert gates.management_operations is True


def test_controller_mode_restore_branches_are_fail_closed(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        restore = store.apply_controller_mode_operation(
            controller_mode_operation_id="aaaaaaaa-1111-4111-9111-aaaaaaaaaaaa",
            operation_kind="RESTORE_BACKUP",
            expected_mode_revision=1,
            expected_mode="MAINTENANCE",
            requested_mode="MAINTENANCE",
            authenticated_principal_id="storage-reconciler",
            authorization_context_digest=AUTHZ_DIGEST,
            backup_manifest_digest=_digest({"backup": 1}),
            backup_prior_mode=None,
            backup_prior_dispatch_paused_intake_policy=None,
        )
        assert restore.status == "SUCCEEDED"
        assert restore.mode == "MAINTENANCE"

    with RunStore(tmp_path / "second", verify_local_filesystem=False) as store:
        _initialize_mode(store)
        store.apply_controller_mode_operation(
            controller_mode_operation_id="aaaaaaaa-1111-4111-9111-aaaaaaaaaaaa",
            operation_kind="SET_MODE",
            expected_mode_revision=1,
            expected_mode="MAINTENANCE",
            requested_mode="RUNNING",
            authenticated_principal_id="operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        restore = store.apply_controller_mode_operation(
            controller_mode_operation_id="bbbbbbbb-2222-4222-9222-bbbbbbbbbbbb",
            operation_kind="RESTORE_BACKUP",
            expected_mode_revision=2,
            expected_mode="RUNNING",
            requested_mode="DISPATCH_PAUSED",
            requested_dispatch_paused_intake_policy="PAUSE_ADMISSION",
            authenticated_principal_id="storage-reconciler",
            authorization_context_digest=AUTHZ_DIGEST,
            backup_manifest_digest=_digest({"backup": 2}),
        )
        assert restore.status == "SUCCEEDED"
        assert restore.mode == "DISPATCH_PAUSED"
        assert restore.dispatch_paused_intake_policy == "PAUSE_ADMISSION"

        illegal = store.apply_controller_mode_operation(
            controller_mode_operation_id="cccccccc-3333-4333-9333-cccccccccccc",
            operation_kind="RESTORE_BACKUP",
            expected_mode_revision=3,
            expected_mode="DISPATCH_PAUSED",
            requested_mode="RUNNING",
            authenticated_principal_id="storage-reconciler",
            authorization_context_digest=AUTHZ_DIGEST,
            backup_manifest_digest=_digest({"backup": 3}),
        )
        assert illegal.status == "REJECTED"
        assert illegal.rejection_code == "TRANSITION_NOT_ALLOWED"


def test_capability_key_bootstrap_register_then_select_and_issue_binding(
    tmp_path: Path,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _register_key(store)
        registry = store.get_capability_key_registry()
        assert registry.registry_revision == 1
        assert registry.current_issuance_key_id is None
        assert store.selected_issuance_key() is None
        with pytest.raises(WorkflowGateClosedError):
            store.record_issued_capability_binding(
                capability_jti=CAPABILITY_JTI,
                claim_digest=_digest({"claim": 1}),
                immutable_assignment_digest=_digest({"attempt": 1}),
                immutable_assignment={"attempt": 1},
            )

        _select_key(store)
        store.apply_controller_mode_operation(
            controller_mode_operation_id="ffffffff-ffff-4fff-ffff-ffffffffffff",
            operation_kind="SET_MODE",
            expected_mode_revision=1,
            expected_mode="MAINTENANCE",
            requested_mode="RUNNING",
            authenticated_principal_id="operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        binding = store.record_issued_capability_binding(
            capability_jti=CAPABILITY_JTI,
            claim_digest=_digest({"claim": 1}),
            immutable_assignment_digest=_digest({"attempt": 1}),
            immutable_assignment={"attempt": 1},
        )
        assert binding.capability_signing_key_id == KEY_ID
        assert binding.signature_algorithm == "ED25519"
        assert binding.capability_key_registry_revision == 2


def test_issued_capability_binding_reuses_controller_gate_reads(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _register_key(store)
        _select_key(store)
        store.apply_controller_mode_operation(
            controller_mode_operation_id="ffffffff-ffff-4fff-ffff-ffffffffffff",
            operation_kind="SET_MODE",
            expected_mode_revision=1,
            expected_mode="MAINTENANCE",
            requested_mode="RUNNING",
            authenticated_principal_id="operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )

        statements: list[str] = []
        store.conn.set_trace_callback(statements.append)
        binding = store.record_issued_capability_binding(
            capability_jti=CAPABILITY_JTI,
            claim_digest=_digest({"claim": 1}),
            immutable_assignment_digest=_digest({"attempt": 1}),
            immutable_assignment={"attempt": 1},
        )
        store.conn.set_trace_callback(None)

        assert binding.capability_signing_key_id == KEY_ID
        assert binding.capability_key_registry_revision == 2
        assert _select_count(statements, "controller_mode") == 1
        assert _select_count(statements, "capability_key_registry") == 1
        assert _select_count(statements, "capability_signing_keys") == 1


def test_issued_capability_binding_exact_replay_skips_gate_reads(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _register_key(store)
        _select_key(store)
        store.apply_controller_mode_operation(
            controller_mode_operation_id="ffffffff-ffff-4fff-ffff-ffffffffffff",
            operation_kind="SET_MODE",
            expected_mode_revision=1,
            expected_mode="MAINTENANCE",
            requested_mode="RUNNING",
            authenticated_principal_id="operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        binding = store.record_issued_capability_binding(
            capability_jti=CAPABILITY_JTI,
            claim_digest=_digest({"claim": 1}),
            immutable_assignment_digest=_digest({"attempt": 1}),
            immutable_assignment={"attempt": 1},
        )

        statements: list[str] = []
        store.conn.set_trace_callback(statements.append)
        replay = store.record_issued_capability_binding(
            capability_jti=CAPABILITY_JTI,
            claim_digest=_digest({"claim": 1}),
            immutable_assignment_digest=_digest({"attempt": 1}),
            immutable_assignment={"attempt": 1},
        )
        store.conn.set_trace_callback(None)

        assert replay == binding
        assert _select_count(statements, "capability_issuance_audit") == 1
        assert _select_count(statements, "controller_mode") == 0
        assert _select_count(statements, "capability_key_registry") == 0
        assert _select_count(statements, "capability_signing_keys") == 0


def test_capability_rotation_preserves_retired_key_and_revocation_disables_issuance(
    tmp_path: Path,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _register_key(store)
        _select_key(store)
        _register_key(
            store,
            key_id=KEY_ID_2,
            operation_id=KEY_OP_ID_3,
            expected_revision=2,
            expected_key=KEY_ID,
        )
        retire = store.apply_capability_key_operation(
            capability_key_operation_id=KEY_OP_ID_4,
            kind="RETIRE",
            expected_registry_revision=3,
            expected_issuance_key_id=KEY_ID,
            target_capability_signing_key_id=KEY_ID,
            replacement_issuance_key_id=KEY_ID_2,
            authenticated_principal_id="key-operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert retire.status == "SUCCEEDED"
        assert retire.current_issuance_key_id == KEY_ID_2
        assert store.get_capability_signing_key(KEY_ID).state == "RETIRED"

        revoke = store.apply_capability_key_operation(
            capability_key_operation_id="abcdefab-1111-4111-9111-abcdefabcdef",
            kind="REVOKE",
            expected_registry_revision=4,
            expected_issuance_key_id=KEY_ID_2,
            target_capability_signing_key_id=KEY_ID_2,
            replacement_issuance_key_id=None,
            authenticated_principal_id="key-operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert revoke.status == "SUCCEEDED"
        assert revoke.current_issuance_key_id is None
        assert store.selected_issuance_key() is None
        with pytest.raises(WorkflowGateClosedError):
            store.assert_offer_planning_permitted()


@pytest.mark.parametrize(
    ("kind", "target_key", "replacement_key", "rejection_code"),
    [
        ("SELECT", KEY_ID_2, None, "KEY_NOT_ACTIVE"),
        ("RETIRE", KEY_ID, None, "CURRENT_KEY_REQUIRES_REPLACEMENT"),
        ("RETIRE", KEY_ID, KEY_ID, "KEY_NOT_ACTIVE"),
        ("REVOKE", KEY_ID_2, KEY_ID, "KEY_NOT_ACTIVE"),
    ],
)
def test_capability_key_illegal_transitions_are_rejected(
    tmp_path: Path,
    kind: str,
    target_key: str,
    replacement_key: str | None,
    rejection_code: str,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _register_key(store)
        _select_key(store)
        result = store.apply_capability_key_operation(
            capability_key_operation_id=KEY_OP_ID_3,
            kind=kind,
            expected_registry_revision=2,
            expected_issuance_key_id=KEY_ID,
            target_capability_signing_key_id=target_key,
            replacement_issuance_key_id=replacement_key,
            authenticated_principal_id="key-operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert result.status == "REJECTED"
        assert result.rejection_code == rejection_code
        registry = store.get_capability_key_registry()
        assert registry.registry_revision == 2
        assert registry.current_issuance_key_id == KEY_ID


def test_read_only_connection_rejects_writes(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False):
        pass
    conn = open_read_only(tmp_path / "workflow.db")
    try:
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO runs(run_id) VALUES ('x')")
    finally:
        conn.close()


def test_two_writers_cannot_acquire_authority(tmp_path: Path) -> None:
    store = RunStore(tmp_path, verify_local_filesystem=False)
    code = textwrap.dedent(
        f"""
        import sys
        from pathlib import Path
        sys.path.insert(0, {str(Path.cwd() / "src")!r})
        from orcest.workflow_store import RunStore, WriterLockError
        try:
            RunStore(Path({str(tmp_path)!r}), verify_local_filesystem=False)
        except WriterLockError:
            sys.exit(0)
        sys.exit(1)
        """
    )
    try:
        result = subprocess.run([sys.executable, "-c", code], check=False)
        assert result.returncode == 0
    finally:
        store.close()


@pytest.mark.timeout(5)
def test_concurrent_fsync_probes_do_not_collide(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "state"
    barrier = threading.Barrier(2, timeout=5)
    real_fsync = os.fsync

    def synced_fsync(fd: int) -> None:
        # Force both invocations to have a probe file on disk at the same
        # time, on both sides of the directory fsync, instead of relying on
        # thread-scheduling luck to interleave them.
        real_fsync(fd)
        barrier.wait()

    monkeypatch.setattr(os, "fsync", synced_fsync)

    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def run() -> None:
        try:
            _verify_local_state_root(root, min_free_bytes=0)
        except BaseException as exc:  # noqa: BLE001 - captured for assertion below
            with errors_lock:
                errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert list(root.glob(".fsync-probe*")) == []


@pytest.mark.timeout(5)
def test_concurrent_run_store_startup_reaches_writer_lock_arbitration(tmp_path: Path) -> None:
    results: list[RunStore | WriterLockError] = []
    results_lock = threading.Lock()

    def start() -> None:
        try:
            store = RunStore(tmp_path)
        except WriterLockError as exc:
            with results_lock:
                results.append(exc)
        else:
            with results_lock:
                results.append(store)

    threads = [threading.Thread(target=start) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    stores = [r for r in results if isinstance(r, RunStore)]
    errors = [r for r in results if isinstance(r, WriterLockError)]
    try:
        assert len(stores) == 1
        assert len(errors) == 1
    finally:
        for store in stores:
            store.close()


def test_replay_returns_committed_transition_and_outbox_without_duplicates(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
            transition = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="ADMIT",
                trigger_id="admit-source-1",
                next_state="PLANNING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"source": 1}),
                specification_generation=1,
            )
            outbox = store.insert_outbox(
                outbox_id=OUTBOX_ID,
                source_kind="ACTIVITY",
                source_id="activity-1",
                destination="redis:worker",
                protocol_version="worker-envelope-v1",
                payload_digest=_digest({"activity": 1}),
                payload={"activity": 1},
                next_delivery_at_ms=0,
            )
            projection = store.insert_projection_outbox(
                projection_outbox_id=PROJECTION_ID,
                run_id=RUN_ID,
                transition_sequence=transition.transition_sequence,
                kind="RUN_STATUS",
                target_kind="WORK_ITEM",
                target_id="work-1",
                payload_digest=_digest({"state": "PLANNING"}),
                payload={"state": "PLANNING"},
                idempotency_key="run-status:work-1:1",
                next_delivery_at_ms=0,
            )

        with store.transaction():
            replayed_transition = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="ADMIT",
                trigger_id="admit-source-1",
                next_state="PLANNING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"source": 1}),
                specification_generation=1,
            )
            replayed_outbox = store.insert_outbox(
                outbox_id=OUTBOX_ID,
                source_kind="ACTIVITY",
                source_id="activity-1",
                destination="redis:worker",
                protocol_version="worker-envelope-v1",
                payload_digest=_digest({"activity": 1}),
                payload={"activity": 1},
                next_delivery_at_ms=0,
            )
            replayed_projection = store.insert_projection_outbox(
                projection_outbox_id=PROJECTION_ID,
                run_id=RUN_ID,
                transition_sequence=transition.transition_sequence,
                kind="RUN_STATUS",
                target_kind="WORK_ITEM",
                target_id="work-1",
                payload_digest=_digest({"state": "PLANNING"}),
                payload={"state": "PLANNING"},
                idempotency_key="run-status:work-1:1",
                next_delivery_at_ms=0,
            )

        assert replayed_transition == transition
        assert replayed_outbox == outbox
        assert replayed_projection == projection
        assert store.conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 1
        assert store.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
        assert store.conn.execute("SELECT COUNT(*) FROM projection_outbox").fetchone()[0] == 1


def test_append_transition_replay_conflicts_on_admit_base_observation_id(
    tmp_path: Path,
) -> None:
    observation_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    observation_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    payload = {"source": 1}
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
            original = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="ADMIT",
                trigger_id="admit-source-1",
                next_state="PLANNING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest(payload),
                specification_generation=1,
                admit_base_observation_id=observation_a,
            )
        with pytest.raises(IdempotencyConflictError):
            with store.transaction():
                store.append_transition(
                    run_id=RUN_ID,
                    transition_id=TRANSITION_ID,
                    prior_state="ADMITTED",
                    trigger_kind="ADMIT",
                    trigger_id="admit-source-1",
                    next_state="PLANNING",
                    reducer_version=REDUCER_VERSION,
                    input_digest=_digest(payload),
                    specification_generation=1,
                    admit_base_observation_id=observation_b,
                )
        with store.transaction():
            replayed = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="ADMIT",
                trigger_id="admit-source-1",
                next_state="PLANNING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest(payload),
                specification_generation=1,
                admit_base_observation_id=observation_a,
            )
        assert replayed == original
        assert replayed.admit_base_observation_id == observation_a


def test_transition_consumption_is_generation_independent(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
            store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="BUDGET_REPORT",
                trigger_id="report-1",
                next_state="WAITING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"report": 1}),
                specification_generation=1,
            )
        with pytest.raises(IdempotencyConflictError):
            with store.transaction():
                store.append_transition(
                    run_id=RUN_ID,
                    transition_id="77777777-7777-7777-7777-777777777777",
                    prior_state="WAITING",
                    trigger_kind="BUDGET_REPORT",
                    trigger_id="report-1",
                    next_state="RECOVERING",
                    reducer_version=REDUCER_VERSION,
                    input_digest=_digest({"report": 1, "generation": 2}),
                    specification_generation=2,
                )


def test_transaction_fault_injection_boundaries_and_wal_recovery(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with pytest.raises(TransactionFault):
            with store.transaction(fault=FaultInjectionPoint.BEFORE_COMMIT):
                _create_run(store)
        assert store.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0

        with pytest.raises(TransactionFault):
            with store.transaction(fault=FaultInjectionPoint.AFTER_COMMIT):
                _create_run(store)
        assert store.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1

    with RunStore(tmp_path, verify_local_filesystem=False) as recovered:
        assert recovered.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1
        assert recovered.conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"

        ack_delivered = False

        def mark_ack() -> None:
            nonlocal ack_delivered
            ack_delivered = True

        with pytest.raises(TransactionFault):
            with recovered.transaction(
                fault=FaultInjectionPoint.BEFORE_RESPONSE_ACK,
                before_response_ack=mark_ack,
            ):
                recovered.record_durable_operation(
                    operation_id=OPERATION_ID,
                    operation_kind="test-operation",
                    principal_id="principal",
                    idempotency_key=IDEMPOTENCY_KEY,
                    request_payload={"x": 1},
                    status="SUCCEEDED",
                    response_payload={"ok": True, "replayed": False},
                    response_http_status=200,
                )
        assert ack_delivered is False
        replayed = recovered.record_durable_operation(
            operation_id=OPERATION_ID,
            operation_kind="test-operation",
            principal_id="principal",
            idempotency_key=IDEMPOTENCY_KEY,
            request_payload={"x": 1},
            status="SUCCEEDED",
            response_payload={"ok": True, "replayed": False},
            response_http_status=200,
        )
        assert replayed.operation_id == OPERATION_ID


def test_fk_unique_sequence_and_cas_violations_are_atomic(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with pytest.raises(sqlite3.IntegrityError):
            with store.transaction():
                store.conn.execute(
                    "INSERT INTO projection_outbox(projection_outbox_id, run_id, "
                    "transition_sequence, kind, target_kind, target_id, payload_digest, "
                    "payload_json, idempotency_key, state, next_delivery_at_ms, created_at_ms) "
                    "VALUES (?, ?, 99, 'RUN_STATUS', 'WORK_ITEM', 'w', ?, '{}', 'k', "
                    "'PENDING', 0, 0)",
                    (PROJECTION_ID, RUN_ID, _digest({"x": 1})),
                )
                _create_run(store)
        assert store.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0

        with store.transaction():
            _create_run(store)
            next_rev = store.put_revisioned_object(
                object_kind="run",
                object_id=RUN_ID,
                expected_revision=0,
                payload_digest=_digest({"rev": 1}),
                payload={"rev": 1},
            )
        assert next_rev == 1

        with pytest.raises(CasMismatchError):
            with store.transaction():
                store.put_revisioned_object(
                    object_kind="run",
                    object_id=RUN_ID,
                    expected_revision=0,
                    payload_digest=_digest({"rev": 2}),
                    payload={"rev": 2},
                )
                store.conn.execute("UPDATE runs SET state = 'PLANNING' WHERE run_id = ?", (RUN_ID,))
        state = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (RUN_ID,)).fetchone()[
            0
        ]
        assert state == "ADMITTED"


def test_source_unique_insert_and_immutable_fact_replay(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            fact = store.insert_immutable_fact(
                fact_kind="timer",
                fact_id="timer-1",
                source_kind="TIMER_FACT",
                source_id="timer:run:1",
                payload_digest=_digest({"timer": 1}),
                payload={"timer": 1},
            )
            record = store.insert_source_unique_record(
                source_kind="INTERNAL",
                source_id="cursor:1",
                record_kind="continuation",
                record_id="cont-1",
                payload_digest=_digest({"cursor": 1}),
                payload={"cursor": 1},
            )
        with store.transaction():
            assert (
                store.insert_immutable_fact(
                    fact_kind="timer",
                    fact_id="timer-1",
                    source_kind="TIMER_FACT",
                    source_id="timer:run:1",
                    payload_digest=_digest({"timer": 1}),
                    payload={"timer": 1},
                )
                == fact
            )
            assert (
                store.insert_source_unique_record(
                    source_kind="INTERNAL",
                    source_id="cursor:1",
                    record_kind="continuation",
                    record_id="cont-1",
                    payload_digest=_digest({"cursor": 1}),
                    payload={"cursor": 1},
                )
                == record
            )
        with pytest.raises(IdempotencyConflictError):
            with store.transaction():
                store.insert_immutable_fact(
                    fact_kind="timer",
                    fact_id="timer-1",
                    source_kind="TIMER_FACT",
                    source_id="timer:run:other",
                    payload_digest=_digest({"timer": 1}),
                    payload={"timer": 1},
                )


def test_projection_outbox_replay_conflicts_on_immutable_identity(tmp_path: Path) -> None:
    payload = {"state": "PLANNING"}
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
            transition = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state="ADMITTED",
                trigger_kind="ADMIT",
                trigger_id="admit-source-1",
                next_state="PLANNING",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"source": 1}),
                specification_generation=1,
            )
            store.insert_projection_outbox(
                projection_outbox_id=PROJECTION_ID,
                run_id=RUN_ID,
                transition_sequence=transition.transition_sequence,
                kind="RUN_STATUS",
                target_kind="WORK_ITEM",
                target_id="work-1",
                payload_digest=_digest(payload),
                payload=payload,
                idempotency_key="run-status:work-1:1",
                next_delivery_at_ms=0,
            )
        with pytest.raises(IdempotencyConflictError):
            with store.transaction():
                store.insert_projection_outbox(
                    projection_outbox_id=PROJECTION_ID,
                    run_id=RUN_ID,
                    transition_sequence=transition.transition_sequence,
                    kind="RUN_STATUS",
                    target_kind="CHANGE_REQUEST",
                    target_id="work-1",
                    payload_digest=_digest(payload),
                    payload=payload,
                    idempotency_key="run-status:work-1:1",
                    next_delivery_at_ms=0,
                )


def test_outbox_replay_conflicts_on_effect_binding(tmp_path: Path) -> None:
    payload = {"activity": 1}
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.insert_outbox(
                outbox_id=OUTBOX_ID,
                source_kind="ACTIVITY",
                source_id="activity-1",
                destination="redis:worker",
                protocol_version="worker-envelope-v1",
                payload_digest=_digest(payload),
                payload=payload,
                next_delivery_at_ms=0,
                publication_id="pub-1",
                effect_generation=1,
            )
        with pytest.raises(IdempotencyConflictError):
            with store.transaction():
                store.insert_outbox(
                    outbox_id=OUTBOX_ID,
                    source_kind="ACTIVITY",
                    source_id="activity-1",
                    destination="redis:worker",
                    protocol_version="worker-envelope-v1",
                    payload_digest=_digest(payload),
                    payload=payload,
                    next_delivery_at_ms=0,
                    publication_id="pub-2",
                    effect_generation=1,
                )


def test_unsupported_schema_can_fail_closed_as_maintenance(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False):
        pass
    conn = sqlite3.connect(tmp_path / "workflow.db")
    conn.execute("PRAGMA user_version=999")
    conn.close()

    with pytest.raises(SchemaVersionError):
        RunStore(tmp_path, verify_local_filesystem=False)

    store = RunStore.open_maintenance(tmp_path, verify_local_filesystem=False)
    try:
        assert store.maintenance_mode is not None
        assert not store.maintenance_mode.dispatch_enabled
        assert not store.maintenance_mode.receipt_acceptance_enabled
        assert not store.maintenance_mode.publication_enabled
    finally:
        store.close()


def test_create_run_rejects_unsupported_reducer_version(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with pytest.raises(ReducerVersionError, match="unsupported reducer version"):
            with store.transaction():
                store.create_run(
                    run_id=RUN_ID,
                    project_id="project-a",
                    work_item_key="work-1",
                    state="ADMITTED",
                    reducer_version="workflow-control-v1/reducer-999",
                )


def test_unsupported_reducer_version_can_fail_closed_as_maintenance(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            _create_run(store)
        store.conn.execute(
            "UPDATE runs SET reducer_version = 'workflow-control-v1/reducer-999' WHERE run_id = ?",
            (RUN_ID,),
        )

    with pytest.raises(ReducerVersionError):
        RunStore(tmp_path, verify_local_filesystem=False)

    store = RunStore.open_maintenance(tmp_path, verify_local_filesystem=False)
    try:
        assert store.maintenance_mode is not None
        assert "workflow-control-v1/reducer-999" in store.maintenance_mode.reason
        assert not store.maintenance_mode.dispatch_enabled
    finally:
        store.close()


def test_schema_v2_allows_generation_zero_and_none_prior_state(tmp_path: Path) -> None:
    assert SCHEMA_VERSION == 7
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.create_run(
                run_id=RUN_ID,
                project_id="project-a",
                work_item_key="work-1",
                state="ADMITTED",
                specification_generation=0,
            )
            transition = store.append_transition(
                run_id=RUN_ID,
                transition_id=TRANSITION_ID,
                prior_state=PRIOR_STATE_NONE,
                trigger_kind="ADMIT",
                trigger_id="obs-work-1",
                next_state="ADMITTED",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"admit": 1}),
                specification_generation=0,
                admit_base_observation_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            )
        assert transition.prior_state == PRIOR_STATE_NONE
        assert transition.specification_generation == 0
        row = store.get_run(RUN_ID)
        assert row is not None
        assert row.specification_generation == 0


_PRE_V3_CONTROLLER_AND_LEDGER_DDL = """
            CREATE TABLE schema_migrations (
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL UNIQUE,
              applied_at_ms INTEGER NOT NULL
            );
            CREATE TABLE controller_mode_operations (
              controller_mode_operation_id TEXT PRIMARY KEY,
              protocol_version TEXT NOT NULL,
              operation_kind TEXT NOT NULL,
              expected_mode_revision INTEGER NOT NULL,
              expected_mode TEXT,
              requested_mode TEXT,
              requested_dispatch_paused_intake_policy TEXT,
              authenticated_principal_id TEXT NOT NULL,
              authorization_context_digest TEXT NOT NULL,
              request_digest TEXT NOT NULL,
              status TEXT NOT NULL,
              rejection_code TEXT,
              result_mode_revision INTEGER,
              result_mode TEXT,
              result_dispatch_paused_intake_policy TEXT,
              response_http_status INTEGER NOT NULL,
              response_json TEXT NOT NULL,
              response_digest TEXT NOT NULL,
              completed_at_ms INTEGER NOT NULL
            );
            CREATE TABLE controller_mode (
              controller_id TEXT PRIMARY KEY,
              mode_revision INTEGER NOT NULL,
              mode TEXT,
              dispatch_paused_intake_policy TEXT,
              maintenance_prior_mode TEXT,
              maintenance_prior_dispatch_paused_intake_policy TEXT,
              last_operation_id TEXT,
              CHECK (
                maintenance_prior_dispatch_paused_intake_policy IS NULL
                OR maintenance_prior_mode = 'DISPATCH_PAUSED'
              )
            );
            CREATE TABLE runs (
              run_id TEXT PRIMARY KEY,
              project_id TEXT NOT NULL,
              work_item_key TEXT NOT NULL,
              specification_generation INTEGER NOT NULL,
              state TEXT NOT NULL,
              terminal_outcome TEXT,
              reducer_version TEXT NOT NULL,
              current_revision INTEGER NOT NULL DEFAULT 0,
              created_at_ms INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE transitions (
              run_id TEXT NOT NULL,
              transition_sequence INTEGER NOT NULL,
              transition_id TEXT NOT NULL UNIQUE,
              prior_state TEXT NOT NULL,
              trigger_kind TEXT NOT NULL,
              trigger_id TEXT NOT NULL,
              admit_base_observation_id TEXT,
              next_state TEXT NOT NULL,
              reducer_version TEXT NOT NULL,
              input_digest TEXT NOT NULL,
              specification_generation INTEGER NOT NULL,
              created_at_ms INTEGER NOT NULL,
              PRIMARY KEY (run_id, transition_sequence)
            );
            """

_SEEDED_CONTROLLER_MODE_OPERATION_ID = "12345678-1234-4234-9234-123456789abc"


def _seed_pre_v3_controller_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO controller_mode_operations VALUES ("
        f"'{_SEEDED_CONTROLLER_MODE_OPERATION_ID}', 'orcest.controller-mode-operation/1', "
        "'INITIALIZE', 0, NULL, 'MAINTENANCE', NULL, 'bootstrap-service', "
        "'sha256:" + "a" * 64 + "', 'sha256:" + "b" * 64 + "', 'SUCCEEDED', NULL, "
        "1, 'MAINTENANCE', NULL, 200, '{}', 'sha256:" + "c" * 64 + "', 0)"
    )
    conn.execute(
        "INSERT INTO controller_mode VALUES ("
        f"'ORCEST_V1', 1, 'MAINTENANCE', NULL, NULL, NULL, "
        f"'{_SEEDED_CONTROLLER_MODE_OPERATION_ID}')"
    )


def _write_v1_shaped_database(db_path: Path) -> None:
    """Recreate a schema-version-1 workflow.db: controller_mode tables exist
    (that's why the pre-#717 current==1 branch never ran _SCHEMA), but they
    lack the v3 backup_* columns and bidirectional CHECK, and there are no
    capability-key tables."""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_PRE_V3_CONTROLLER_AND_LEDGER_DDL)
        _seed_pre_v3_controller_rows(conn)
        conn.execute(
            "INSERT INTO runs VALUES ("
            f"'{RUN_ID}', 'project-a', 'work-1', 1, 'ADMITTED', NULL, "
            f"'{REDUCER_VERSION}', 1, 0, 0)"
        )
        conn.execute(
            "INSERT INTO transitions VALUES ("
            f"'{RUN_ID}', 1, '{TRANSITION_ID}', 'ADMITTED', 'ADMIT', 'obs-work-1', "
            f"NULL, 'ADMITTED', '{REDUCER_VERSION}', 'sha256:" + "d" * 64 + "', 1, 0)"
        )
        conn.execute("PRAGMA user_version=1")
        conn.commit()
    finally:
        conn.close()


def _write_v2_shaped_database(db_path: Path) -> None:
    """Recreate a pre-#717 schema-version-2 workflow.db (no capability-key
    tables, controller_mode_operations lacking the backup_* columns)."""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_PRE_V3_CONTROLLER_AND_LEDGER_DDL)
        _seed_pre_v3_controller_rows(conn)
        conn.execute("PRAGMA user_version=2")
        conn.commit()
    finally:
        conn.close()


def _assert_migrated_to_v3_controller_and_capability_shape(store: RunStore) -> None:
    assert store.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    tables = {
        row[0] for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    for expected_table in (
        "capability_key_registry",
        "capability_key_operations",
        "capability_signing_keys",
        "capability_issuance_audit",
    ):
        assert expected_table in tables

    columns = {
        row[1] for row in store.conn.execute("PRAGMA table_info(controller_mode_operations)")
    }
    assert {
        "backup_manifest_digest",
        "backup_prior_mode",
        "backup_prior_dispatch_paused_intake_policy",
    } <= columns

    mode_sql = store.conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='controller_mode'"
    ).fetchone()[0]
    assert "(maintenance_prior_dispatch_paused_intake_policy IS NOT NULL)" in mode_sql
    assert "= (maintenance_prior_mode = 'DISPATCH_PAUSED')" in mode_sql

    preserved = store.conn.execute(
        "SELECT operation_kind, status FROM controller_mode_operations "
        "WHERE controller_mode_operation_id = ?",
        (_SEEDED_CONTROLLER_MODE_OPERATION_ID,),
    ).fetchone()
    assert tuple(preserved) == ("INITIALIZE", "SUCCEEDED")

    mode = store.conn.execute("SELECT mode, mode_revision FROM controller_mode").fetchone()
    assert tuple(mode) == ("MAINTENANCE", 1)

    registry = store.conn.execute(
        "SELECT registry_revision FROM capability_key_registry"
    ).fetchone()
    assert tuple(registry) == (0,)

    # apply_controller_mode_operation always INSERTs the backup_* columns;
    # this is the failure the 1→3 skip-version path used to hit.
    result = store.apply_controller_mode_operation(
        controller_mode_operation_id=OPERATION_ID,
        operation_kind="SET_MODE",
        expected_mode_revision=1,
        expected_mode="MAINTENANCE",
        requested_mode="RUNNING",
        authenticated_principal_id="operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"
    assert result.mode == "RUNNING"


def test_v1_database_migrates_capability_tables_and_backup_columns(tmp_path: Path) -> None:
    _write_v1_shaped_database(tmp_path / "workflow.db")

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _assert_migrated_to_v3_controller_and_capability_shape(store)
        preserved_run = store.conn.execute(
            "SELECT project_id, specification_generation, state FROM runs WHERE run_id = ?",
            (RUN_ID,),
        ).fetchone()
        assert tuple(preserved_run) == ("project-a", 1, "ADMITTED")
        with store.transaction():
            store.create_run(
                run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                project_id="project-b",
                work_item_key="work-gen-0",
                state="ADMITTED",
                specification_generation=0,
            )
            store.append_transition(
                run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                transition_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                prior_state=PRIOR_STATE_NONE,
                trigger_kind="ADMIT",
                trigger_id="obs-work-gen-0",
                next_state="ADMITTED",
                reducer_version=REDUCER_VERSION,
                input_digest=_digest({"admit": 1}),
                specification_generation=0,
                admit_base_observation_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            )


def test_v2_database_migrates_capability_tables_and_backup_columns(tmp_path: Path) -> None:
    _write_v2_shaped_database(tmp_path / "workflow.db")

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _assert_migrated_to_v3_controller_and_capability_shape(store)


_SECRET_PROVISION_TABLES = (
    "secret_current_versions",
    "secret_versions",
    "credential_rotation_receipts",
    "secret_provision_operations",
    "secret_provision_checkpoints",
)

_PROJECT_REGISTRATION_TABLES = (
    "forge_instances",
    "forge_observation_schedules",
    "project_registration_operations",
    "projects",
)


def _write_v3_shaped_database(db_path: Path) -> None:
    """Build a real v4 database, then strip it back to the v3 shape: every
    secret-provision table dropped and ``user_version`` rolled back to 3."""
    with RunStore(db_path.parent, verify_local_filesystem=False):
        pass
    conn = sqlite3.connect(db_path)
    try:
        for table in _SECRET_PROVISION_TABLES + _PROJECT_REGISTRATION_TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute("DELETE FROM schema_migrations WHERE version >= 4")
        conn.execute("PRAGMA user_version=3")
        conn.commit()
    finally:
        conn.close()


def test_v3_database_migrates_secret_provision_tables(tmp_path: Path) -> None:
    _write_v3_shaped_database(tmp_path / "workflow.db")

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        assert store.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        tables = {
            row[0]
            for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for expected_table in _SECRET_PROVISION_TABLES:
            assert expected_table in tables
        # A prior-version secret provision now round-trips end to end on the
        # freshly migrated database.
        result = store.apply_capability_key_operation(
            capability_key_operation_id=KEY_OP_ID,
            kind="REGISTER",
            expected_registry_revision=0,
            expected_issuance_key_id=None,
            target_capability_signing_key_id=KEY_ID,
            register_public_verification_key=_public_key(1),
            register_public_key_digest=capability_public_key_digest(_public_key(1)),
            register_private_signing_secret_ref="bootstrap:0",
            register_not_before_ms=0,
            private_key_proof_valid=True,
            authenticated_principal_id="key-operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert result.status == "SUCCEEDED"
        store.apply_capability_key_operation(
            capability_key_operation_id=KEY_OP_ID_2,
            kind="SELECT",
            expected_registry_revision=1,
            expected_issuance_key_id=None,
            target_capability_signing_key_id=KEY_ID,
            authenticated_principal_id="key-operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        secret_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        accepted = store.begin_secret_provision_operation(
            secret_provision_operation_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            mode="PROVISION",
            secret_id=secret_id,
            expected_prior_version=None,
            purpose="FORGE_API",
            owner_scope_kind="FORGE_INSTALLATION",
            owner_scope_id="installation-1",
            provider_account_ref="installation-1",
            authenticated_principal_id="operator-1",
            authorization_context_digest=AUTHZ_DIGEST,
            secret_store_staging_receipt_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
            secret_integrity_attestation_id="11111111-2222-4333-8444-555555555555",
        )
        assert accepted.state == "PENDING"
        assert accepted.target_version == 1


def test_capability_key_register_rejects_reused_digest_under_new_key_id(
    tmp_path: Path,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _register_key(store)
        public_key = _public_key(1)
        result = store.apply_capability_key_operation(
            capability_key_operation_id=KEY_OP_ID_2,
            kind="REGISTER",
            expected_registry_revision=1,
            expected_issuance_key_id=None,
            target_capability_signing_key_id=KEY_ID_2,
            register_public_verification_key=public_key,
            register_public_key_digest=capability_public_key_digest(public_key),
            register_private_signing_secret_ref=f"secret:{KEY_ID_2}:1",
            register_not_before_ms=0,
            private_key_proof_valid=True,
            authenticated_principal_id="key-operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert result.status == "REJECTED"
        assert result.rejection_code == "INTEGRITY_CONFLICT"
        assert store.get_capability_signing_key(KEY_ID_2) is None


def test_controller_mode_restore_backup_rejects_asymmetric_prior_policy(
    tmp_path: Path,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        illegal = store.apply_controller_mode_operation(
            controller_mode_operation_id="aaaaaaaa-1111-4111-9111-aaaaaaaaaaaa",
            operation_kind="RESTORE_BACKUP",
            expected_mode_revision=1,
            expected_mode="MAINTENANCE",
            requested_mode="MAINTENANCE",
            authenticated_principal_id="storage-reconciler",
            authorization_context_digest=AUTHZ_DIGEST,
            backup_manifest_digest=_digest({"backup": 1}),
            backup_prior_mode="DISPATCH_PAUSED",
            backup_prior_dispatch_paused_intake_policy=None,
        )
        assert illegal.status == "REJECTED"
        assert illegal.rejection_code == "TRANSITION_NOT_ALLOWED"


def _write_v4_shaped_database(db_path: Path) -> None:
    """Build a real current database, then strip project-registration tables
    and roll ``user_version`` back to 4."""
    with RunStore(db_path.parent, verify_local_filesystem=False):
        pass
    conn = sqlite3.connect(db_path)
    try:
        for table in _PROJECT_REGISTRATION_TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute("DELETE FROM schema_migrations WHERE version = 5")
        conn.execute("PRAGMA user_version=4")
        conn.commit()
    finally:
        conn.close()


def test_v4_database_migrates_project_registration_tables(tmp_path: Path) -> None:
    _write_v4_shaped_database(tmp_path / "workflow.db")

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        assert store.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        tables = {
            row[0]
            for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for expected_table in _PROJECT_REGISTRATION_TABLES:
            assert expected_table in tables


_FORGE_OBSERVATION_TABLES = (
    "forge_observation_requests",
    "forge_observation_request_results",
    "forge_request_failure_facts",
    "forge_observations",
)

_V5_FORGE_OBSERVATION_SCHEDULES_DDL = """
CREATE TABLE forge_observation_schedules (
  forge_observation_schedule_id TEXT PRIMARY KEY,
  schedule_kind TEXT NOT NULL,
  schedule_revision INTEGER NOT NULL,
  state TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL,
  run_id TEXT,
  publication_id TEXT,
  last_request_id TEXT,
  last_observation_id TEXT,
  next_due_at_ms INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
)
"""


def _write_v5_shaped_database(db_path: Path) -> None:
    """Build a real v6 database, seed one real pre-v6-shape WORK_ITEM_DISCOVERY
    Schedule row (the only kind the pre-#676 code ever wrote), drop the new
    Forge Observation Request/Result/Failure-Fact/Observation tables, and roll
    ``user_version`` back to 5."""
    with RunStore(db_path.parent, verify_local_filesystem=False) as store:
        now = 1_700_000_000_000
        secret_id = "aaaaaaaa-0000-4000-8000-000000000001"
        publication_secret_id = "aaaaaaaa-0000-4000-8000-000000000009"
        for sid in (secret_id, publication_secret_id):
            store.conn.execute(
                "INSERT INTO secret_current_versions(secret_id, purpose, owner_scope_kind, "
                "owner_scope_id, provider_account_ref, current_version, last_operation_id, "
                "created_at_ms, updated_at_ms) VALUES (?, 'FORGE_API', 'PROJECT', 'scope', NULL, "
                "1, ?, ?, ?)",
                (sid, "aaaaaaaa-0000-4000-8000-000000000002", now, now),
            )
        forge_instance_id = "aaaaaaaa-0000-4000-8000-000000000003"
        store.conn.execute(
            "INSERT INTO forge_instances(forge_instance_id, adapter_kind, canonical_origin, "
            "credential_secret_id, registration_provenance_version, created_at_ms) "
            "VALUES (?, 'GITHUB', 'github.com/legacy', ?, 1, ?)",
            (forge_instance_id, secret_id, now),
        )
        project_id = "aaaaaaaa-0000-4000-8000-000000000004"
        schedule_id = "aaaaaaaa-0000-4000-8000-000000000005"
        store.conn.execute(
            "INSERT INTO projects(project_id, forge_instance_id, installation_or_account_ref, "
            "repository_external_id, repository_locator, default_ref, trusted_base_policy_ref, "
            "budget_policy_ref, budget_reset_window_ref, source_read_secret_id, "
            "publication_secret_id, registration_source_read_secret_version, "
            "registration_publication_secret_version, registration_revision, "
            "registration_operation_id, work_item_discovery_schedule_id, registration_state) "
            "VALUES (?, ?, 'inst', 'legacy/repo', 'legacy/repo', 'main', 'default', 'default', "
            "'default', ?, ?, 1, 1, 1, 'aaaaaaaa-0000-4000-8000-000000000006', ?, 'ACTIVE')",
            (project_id, forge_instance_id, secret_id, publication_secret_id, schedule_id),
        )
        store.conn.commit()
        for table in _FORGE_OBSERVATION_TABLES + ("forge_observation_schedules",):
            store.conn.execute(f"DROP TABLE IF EXISTS {table}")
        store.conn.execute(_V5_FORGE_OBSERVATION_SCHEDULES_DDL)
        store.conn.execute(
            "INSERT INTO forge_observation_schedules(forge_observation_schedule_id, "
            "schedule_kind, schedule_revision, state, target_kind, target_id, run_id, "
            "publication_id, last_request_id, last_observation_id, next_due_at_ms, "
            "created_at_ms) VALUES (?, 'WORK_ITEM_DISCOVERY', 0, 'ACTIVE', 'PROJECT', ?, "
            "NULL, NULL, NULL, NULL, ?, ?)",
            (schedule_id, project_id, now, now),
        )
        store.conn.execute("DELETE FROM schema_migrations WHERE version = 6")
        store.conn.execute("PRAGMA user_version=5")
        store.conn.commit()


def test_v5_database_migrates_forge_observation_tables(tmp_path: Path) -> None:
    _write_v5_shaped_database(tmp_path / "workflow.db")

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        assert store.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        tables = {
            row[0]
            for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for expected_table in _FORGE_OBSERVATION_TABLES:
            assert expected_table in tables

        # The real legacy WORK_ITEM_DISCOVERY row was rebuilt into the final
        # shape with project_id/forge_instance_id backfilled from Project and
        # a real, non-empty schedule_digest computed (not left as the '' DDL
        # placeholder).
        row = store.conn.execute(
            "SELECT * FROM forge_observation_schedules WHERE schedule_kind = 'WORK_ITEM_DISCOVERY'"
        ).fetchone()
        assert row is not None
        assert row["project_id"] == row["target_id"]
        assert row["forge_instance_id"] == "aaaaaaaa-0000-4000-8000-000000000003"
        assert row["schedule_digest"] != ""
        assert row["minimum_interval_ms"] > 0

        # A due Request can now be created against the migrated Schedule.
        request = store.create_due_forge_observation_request(
            forge_observation_request_id="aaaaaaaa-0000-4000-8000-000000000007",
            forge_observation_schedule_id=row["forge_observation_schedule_id"],
            now_ms=row["next_due_at_ms"],
            controller_mode="RUNNING",
            controller_mode_revision=1,
            credential_purpose="PROJECT_SOURCE_READ",
            credential_secret_id="aaaaaaaa-0000-4000-8000-000000000001",
            credential_secret_version=1,
            outbox_id="aaaaaaaa-0000-4000-8000-000000000008",
        )
        assert request is not None
        assert request.state == "PENDING"
