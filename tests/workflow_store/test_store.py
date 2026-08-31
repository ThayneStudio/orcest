from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap
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
    open_read_only,
)

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
    assert SCHEMA_VERSION == 2
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
