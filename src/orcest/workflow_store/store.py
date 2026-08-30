"""SQLite single-writer substrate for Workflow-Control v1.

This module intentionally stops at the base storage layer. Later workflow
leaves add feature tables and reducers on top of these primitives.
"""

from __future__ import annotations

import fcntl
import os
import sqlite3
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.digest import (
    is_valid_content_digest,
    request_digest,
    response_digest,
)
from orcest.workflow_contract.v1.identity import is_lowercase_uuid, require_lowercase_uuid

SCHEMA_VERSION = 1
SUPPORTED_REDUCER_VERSIONS = frozenset({"workflow-control-v1/reducer-0"})
CONTROLLER_ID = "ORCEST_V1"

_FORBIDDEN_STATE_FS = {
    "9p",
    "afs",
    "autofs",
    "cifs",
    "fuse",
    "fuseblk",
    "nfs",
    "nfs4",
    "smb3",
    "smbfs",
}


class RunStoreError(RuntimeError):
    """Base class for run-store failures."""


class WriterLockError(RunStoreError):
    """Raised when another controller already owns the writer lock."""


class StartupIntegrityError(RunStoreError):
    """Raised when startup checks require fail-closed operation."""


class SchemaVersionError(StartupIntegrityError):
    """Raised for unsupported schema versions."""


class ReducerVersionError(StartupIntegrityError):
    """Raised for unsupported persisted reducer versions."""


class TransactionFault(RunStoreError):
    """Raised by test fault injection at a specific transaction boundary."""


class IdempotencyConflictError(RunStoreError):
    """Raised when a replay key is reused with different immutable content."""


class CasMismatchError(RunStoreError):
    """Raised when a monotonic compare-and-swap update loses its fence."""


class FaultInjectionPoint(str, Enum):
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"
    BEFORE_RESPONSE_ACK = "before_response_ack"


@dataclass(frozen=True, slots=True)
class MaintenanceMode:
    """Fail-closed startup result for callers that choose not to raise."""

    reason: str
    dispatch_enabled: bool = False
    receipt_acceptance_enabled: bool = False
    publication_enabled: bool = False


@dataclass(frozen=True, slots=True)
class Transition:
    run_id: str
    transition_sequence: int
    transition_id: str
    prior_state: str
    trigger_kind: str
    trigger_id: str
    next_state: str
    reducer_version: str
    input_digest: str
    created_at_ms: int
    specification_generation: int
    admit_base_observation_id: str | None = None


@dataclass(frozen=True, slots=True)
class ImmutableFact:
    fact_kind: str
    fact_id: str
    payload_digest: str
    payload_json: str
    source_kind: str | None
    source_id: str | None
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class SourceUniqueRecord:
    source_kind: str
    source_id: str
    record_kind: str
    record_id: str
    payload_digest: str
    payload_json: str
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    outbox_id: str
    source_kind: str
    source_id: str
    destination: str
    protocol_version: str
    payload_digest: str
    payload_json: str
    next_delivery_at_ms: int
    state: str
    delivery_count: int
    created_at_ms: int
    attempt_id: str | None = None
    attempt_generation: int | None = None
    publication_id: str | None = None
    effect_generation: int | None = None


@dataclass(frozen=True, slots=True)
class ProjectionOutboxRecord:
    projection_outbox_id: str
    run_id: str
    transition_sequence: int
    kind: str
    target_kind: str
    target_id: str
    payload_digest: str
    payload_json: str
    idempotency_key: str
    state: str
    delivery_count: int
    next_delivery_at_ms: int
    created_at_ms: int
    publication_id: str | None = None
    effect_generation: int | None = None


@dataclass(frozen=True, slots=True)
class DurableOperation:
    operation_id: str
    operation_kind: str
    principal_id: str
    idempotency_key: str
    request_digest: str
    status: str
    response_json: str
    response_digest: str
    response_http_status: int
    committed_at_ms: int


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _enum_values(registry_name: str) -> tuple[str, ...]:
    return tuple(member.value for member in enums.get_enum(registry_name))


def _sql_in(values: Iterable[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _require_digest(value: str, *, field: str) -> str:
    if not is_valid_content_digest(value):
        raise ValueError(f"{field} must be a v1 sha256 content digest")
    return value


def _require_json_text(value: Any) -> str:
    return value if isinstance(value, str) else canonical_json_text(value)


def _response_digest_preimage(value: Any) -> Any:
    if not isinstance(value, dict) or "replayed" not in value:
        return value
    stripped = dict(value)
    stripped.pop("replayed")
    return stripped


def _row_to_transition(row: sqlite3.Row) -> Transition:
    return Transition(
        run_id=row["run_id"],
        transition_sequence=row["transition_sequence"],
        transition_id=row["transition_id"],
        prior_state=row["prior_state"],
        trigger_kind=row["trigger_kind"],
        trigger_id=row["trigger_id"],
        next_state=row["next_state"],
        reducer_version=row["reducer_version"],
        input_digest=row["input_digest"],
        created_at_ms=row["created_at_ms"],
        specification_generation=row["specification_generation"],
        admit_base_observation_id=row["admit_base_observation_id"],
    )


def _row_to_fact(row: sqlite3.Row) -> ImmutableFact:
    return ImmutableFact(
        fact_kind=row["fact_kind"],
        fact_id=row["fact_id"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_source_record(row: sqlite3.Row) -> SourceUniqueRecord:
    return SourceUniqueRecord(
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        record_kind=row["record_kind"],
        record_id=row["record_id"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_outbox(row: sqlite3.Row) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=row["outbox_id"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        destination=row["destination"],
        attempt_id=row["attempt_id"],
        attempt_generation=row["attempt_generation"],
        publication_id=row["publication_id"],
        effect_generation=row["effect_generation"],
        protocol_version=row["protocol_version"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        next_delivery_at_ms=row["next_delivery_at_ms"],
        state=row["state"],
        delivery_count=row["delivery_count"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_projection(row: sqlite3.Row) -> ProjectionOutboxRecord:
    return ProjectionOutboxRecord(
        projection_outbox_id=row["projection_outbox_id"],
        run_id=row["run_id"],
        transition_sequence=row["transition_sequence"],
        kind=row["kind"],
        target_kind=row["target_kind"],
        target_id=row["target_id"],
        publication_id=row["publication_id"],
        effect_generation=row["effect_generation"],
        payload_digest=row["payload_digest"],
        payload_json=row["payload_json"],
        idempotency_key=row["idempotency_key"],
        state=row["state"],
        delivery_count=row["delivery_count"],
        next_delivery_at_ms=row["next_delivery_at_ms"],
        created_at_ms=row["created_at_ms"],
    )


def _row_to_operation(row: sqlite3.Row) -> DurableOperation:
    return DurableOperation(
        operation_id=row["operation_id"],
        operation_kind=row["operation_kind"],
        principal_id=row["principal_id"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        status=row["status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        response_http_status=row["response_http_status"],
        committed_at_ms=row["committed_at_ms"],
    )


def _mount_for(path: Path) -> tuple[Path, str]:
    path = path.resolve()
    best_mount = Path("/")
    best_type = ""
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return best_mount, best_type
    for line in lines:
        before, _, after = line.partition(" - ")
        if not after:
            continue
        fields = before.split()
        mount_point = Path(fields[4].replace("\\040", " "))
        fs_type = after.split()[0]
        try:
            path.relative_to(mount_point)
        except ValueError:
            continue
        if len(str(mount_point)) >= len(str(best_mount)):
            best_mount = mount_point
            best_type = fs_type
    return best_mount, best_type


def _verify_local_state_root(root: Path, *, min_free_bytes: int) -> None:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    if not root.is_dir():
        raise StartupIntegrityError(f"state root is not a directory: {root}")
    stat = root.stat()
    if stat.st_uid != os.getuid():
        raise StartupIntegrityError(f"state root {root} is not owned by uid {os.getuid()}")
    if stat.st_mode & 0o777 != 0o700:
        raise StartupIntegrityError(f"state root {root} must have mode 0700")
    _, fs_type = _mount_for(root)
    if fs_type in _FORBIDDEN_STATE_FS or fs_type.startswith("fuse."):
        raise StartupIntegrityError(f"state root {root} is on forbidden filesystem {fs_type}")
    free_bytes = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
    if free_bytes < min_free_bytes:
        raise StartupIntegrityError(
            f"state root {root} has {free_bytes} free bytes below safety floor {min_free_bytes}"
        )
    probe = root / ".fsync-probe"
    with probe.open("wb") as file:
        file.write(b"orcest workflow store fsync probe\n")
        file.flush()
        os.fsync(file.fileno())
    dir_fd = os.open(root, os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    probe.unlink()


def _verify_lock_file(path: Path) -> None:
    if path.stat().st_mode & 0o777 != 0o600:
        raise StartupIntegrityError(f"{path.name} must have mode 0600")


def open_read_only(db_path: Path | str) -> sqlite3.Connection:
    """Open a workflow database for query-only reads."""

    conn = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  applied_at_ms INTEGER NOT NULL CHECK (applied_at_ms >= 0)
);

CREATE TABLE IF NOT EXISTS controller_mode (
  controller_id TEXT PRIMARY KEY CHECK (controller_id = '{CONTROLLER_ID}'),
  mode_revision INTEGER NOT NULL CHECK (mode_revision >= 0),
  mode TEXT CHECK (mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  dispatch_paused_intake_policy TEXT
    CHECK (dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  maintenance_prior_mode TEXT CHECK (
    maintenance_prior_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  maintenance_prior_dispatch_paused_intake_policy TEXT
    CHECK (maintenance_prior_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  last_operation_id TEXT,
  FOREIGN KEY (last_operation_id)
    REFERENCES controller_mode_operations(controller_mode_operation_id) ON DELETE RESTRICT,
  CHECK ((mode_revision = 0 AND mode IS NULL) OR (mode_revision > 0 AND mode IS NOT NULL)),
  CHECK (
    (mode = 'DISPATCH_PAUSED' AND dispatch_paused_intake_policy IS NOT NULL)
    OR (mode IS NULL AND dispatch_paused_intake_policy IS NULL)
    OR (mode != 'DISPATCH_PAUSED' AND dispatch_paused_intake_policy IS NULL)
  ),
  CHECK (
    maintenance_prior_dispatch_paused_intake_policy IS NULL
    OR maintenance_prior_mode = 'DISPATCH_PAUSED'
  )
);

CREATE TABLE IF NOT EXISTS controller_mode_operations (
  controller_mode_operation_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  operation_kind TEXT NOT NULL CHECK (
    operation_kind IN ({_sql_in(_enum_values("controller_mode_operation.operation_kind"))})
  ),
  expected_mode_revision INTEGER NOT NULL CHECK (expected_mode_revision >= 0),
  expected_mode TEXT CHECK (expected_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  requested_mode TEXT CHECK (
    requested_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  requested_dispatch_paused_intake_policy TEXT
    CHECK (requested_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ({_sql_in(_enum_values("controller_mode_operation.status"))})
  ),
  rejection_code TEXT CHECK (
    rejection_code IN ({_sql_in(_enum_values("controller_mode_operation.rejection_code"))})
  ),
  result_mode_revision INTEGER CHECK (result_mode_revision > 0),
  result_mode TEXT CHECK (result_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})),
  result_dispatch_paused_intake_policy TEXT
    CHECK (result_dispatch_paused_intake_policy IN (
      {_sql_in(_enum_values("controller_mode.dispatch_paused_intake_policy"))}
    )),
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  completed_at_ms INTEGER NOT NULL CHECK (completed_at_ms >= 0),
  CHECK ((status = 'SUCCEEDED' AND rejection_code IS NULL)
    OR (status = 'REJECTED' AND rejection_code IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  work_item_key TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation > 0),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("run.state"))})),
  terminal_outcome TEXT CHECK (
    terminal_outcome IN ({_sql_in(_enum_values("run.terminal_outcome"))})
  ),
  reducer_version TEXT NOT NULL,
  current_revision INTEGER NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active_work_item
ON runs(project_id, work_item_key) WHERE terminal_outcome IS NULL;

CREATE TABLE IF NOT EXISTS transitions (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  transition_sequence INTEGER NOT NULL CHECK (transition_sequence > 0),
  transition_id TEXT NOT NULL UNIQUE,
  prior_state TEXT NOT NULL CHECK (prior_state IN ({_sql_in(_enum_values("run.state"))})),
  trigger_kind TEXT NOT NULL CHECK (
    trigger_kind IN ({_sql_in(_enum_values("transition.trigger_kind"))})
  ),
  trigger_id TEXT NOT NULL,
  admit_base_observation_id TEXT,
  next_state TEXT NOT NULL CHECK (next_state IN ({_sql_in(_enum_values("run.state"))})),
  reducer_version TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation > 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (run_id, transition_sequence),
  UNIQUE (run_id, trigger_kind, trigger_id)
);

CREATE TABLE IF NOT EXISTS outbox (
  outbox_id TEXT PRIMARY KEY,
  source_kind TEXT NOT NULL CHECK (
    source_kind IN ({_sql_in(_enum_values("outbox_record.source_kind"))})
  ),
  source_id TEXT NOT NULL,
  destination TEXT NOT NULL,
  attempt_id TEXT,
  attempt_generation INTEGER CHECK (attempt_generation IS NULL OR attempt_generation > 0),
  publication_id TEXT,
  effect_generation INTEGER CHECK (effect_generation IS NULL OR effect_generation > 0),
  protocol_version TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  next_delivery_at_ms INTEGER NOT NULL CHECK (next_delivery_at_ms >= 0),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("outbox_record.state"))})),
  delivery_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
  last_redis_epoch INTEGER CHECK (last_redis_epoch IS NULL OR last_redis_epoch >= 0),
  last_redis_entry TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  UNIQUE (source_kind, source_id, destination, payload_digest)
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(state, next_delivery_at_ms);

CREATE TABLE IF NOT EXISTS projection_outbox (
  projection_outbox_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  transition_sequence INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (
    kind IN ({_sql_in(_enum_values("projection_outbox_record.kind"))})
  ),
  target_kind TEXT NOT NULL CHECK (
    target_kind IN ({_sql_in(_enum_values("projection_outbox_record.target_kind"))})
  ),
  target_id TEXT NOT NULL,
  publication_id TEXT,
  effect_generation INTEGER CHECK (effect_generation IS NULL OR effect_generation > 0),
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("outbox_record.state"))})),
  delivery_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
  next_delivery_at_ms INTEGER NOT NULL CHECK (next_delivery_at_ms >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  FOREIGN KEY (run_id, transition_sequence)
    REFERENCES transitions(run_id, transition_sequence) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_projection_outbox_pending
ON projection_outbox(state, next_delivery_at_ms);

CREATE TABLE IF NOT EXISTS immutable_facts (
  fact_kind TEXT NOT NULL,
  fact_id TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  source_kind TEXT,
  source_id TEXT,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (fact_kind, fact_id),
  UNIQUE (source_kind, source_id)
);

CREATE TABLE IF NOT EXISTS source_unique_records (
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  record_kind TEXT NOT NULL,
  record_id TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (source_kind, source_id),
  UNIQUE (record_kind, record_id)
);

CREATE TABLE IF NOT EXISTS revisioned_objects (
  object_kind TEXT NOT NULL,
  object_id TEXT NOT NULL,
  revision INTEGER NOT NULL CHECK (revision >= 0),
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0),
  PRIMARY KEY (object_kind, object_id)
);

CREATE TABLE IF NOT EXISTS durable_operations (
  operation_id TEXT PRIMARY KEY,
  operation_kind TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL,
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  committed_at_ms INTEGER NOT NULL CHECK (committed_at_ms >= 0),
  UNIQUE (principal_id, idempotency_key)
);
"""


class RunStore:
    """Controller-owned SQLite store with an exclusive process writer lock."""

    def __init__(
        self,
        state_root: Path | str,
        *,
        reducer_versions: Iterable[str] = SUPPORTED_REDUCER_VERSIONS,
        min_free_bytes: int = 1,
        verify_local_filesystem: bool = True,
        fail_closed: bool = True,
    ) -> None:
        self.state_root = Path(state_root)
        self.db_path = self.state_root / "workflow.db"
        self.controller_lock_path = self.state_root / "controller.lock"
        self.storage_lock_path = self.state_root / "storage.lock"
        self._supported_reducer_versions = frozenset(reducer_versions)
        self._fail_closed = fail_closed
        self.maintenance_mode: MaintenanceMode | None = None
        self._lock_fd: int | None = None
        self._conn: sqlite3.Connection | None = None

        if verify_local_filesystem:
            _verify_local_state_root(self.state_root, min_free_bytes=min_free_bytes)
        else:
            self.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)

        self._acquire_writer_lock()
        try:
            self._conn = self._open_connection()
            self._migrate()
            self._startup_checks()
        except Exception:
            self.close()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    @classmethod
    def open_maintenance(
        cls,
        state_root: Path | str,
        *,
        reducer_versions: Iterable[str] = SUPPORTED_REDUCER_VERSIONS,
        min_free_bytes: int = 1,
        verify_local_filesystem: bool = True,
    ) -> "RunStore":
        return cls(
            state_root,
            reducer_versions=reducer_versions,
            min_free_bytes=min_free_bytes,
            verify_local_filesystem=verify_local_filesystem,
            fail_closed=False,
        )

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RunStoreError("run store is closed")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    def _acquire_writer_lock(self) -> None:
        self.controller_lock_path.touch(mode=0o600, exist_ok=True)
        self.controller_lock_path.chmod(0o600)
        _verify_lock_file(self.controller_lock_path)
        self.storage_lock_path.touch(mode=0o600, exist_ok=True)
        self.storage_lock_path.chmod(0o600)
        _verify_lock_file(self.storage_lock_path)
        fd = os.open(self.controller_lock_path, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise WriterLockError("another workflow controller owns the writer lock") from exc
        self._lock_fd = fd

    def _open_connection(self) -> sqlite3.Connection:
        self.db_path.touch(mode=0o600, exist_ok=True)
        self.db_path.chmod(0o600)
        _verify_lock_file(self.db_path)
        conn = sqlite3.connect(self.db_path, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        pragmas = {
            "journal_mode": "WAL",
            "synchronous": "FULL",
            "foreign_keys": "ON",
            "busy_timeout": "5000",
            "trusted_schema": "OFF",
            "wal_autocheckpoint": "1000",
        }
        for name, value in pragmas.items():
            conn.execute(f"PRAGMA {name}={value}")
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            raise StartupIntegrityError("SQLite journal_mode=WAL could not be enabled")
        if int(synchronous) != 2:
            raise StartupIntegrityError("SQLite synchronous=FULL could not be enabled")
        if int(foreign_keys) != 1:
            raise StartupIntegrityError("SQLite foreign_keys=ON could not be enabled")
        return conn

    def _migrate(self) -> None:
        current = int(self.conn.execute("PRAGMA user_version").fetchone()[0])
        if current > SCHEMA_VERSION:
            if not self._fail_closed:
                return
            raise SchemaVersionError(
                f"workflow.db schema version {current} is newer than supported {SCHEMA_VERSION}"
            )
        if current == SCHEMA_VERSION:
            return
        self.conn.execute("BEGIN EXCLUSIVE")
        try:
            self.conn.executescript(_SCHEMA)
            self.conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                "VALUES (?, ?, ?)",
                (SCHEMA_VERSION, "workflow-control-v1-base-store", _now_ms()),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO controller_mode"
                "(controller_id, mode_revision, mode, dispatch_paused_intake_policy, "
                "maintenance_prior_mode, maintenance_prior_dispatch_paused_intake_policy, "
                "last_operation_id) VALUES (?, 0, NULL, NULL, NULL, NULL, NULL)",
                (CONTROLLER_ID,),
            )
            self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _startup_checks(self) -> None:
        try:
            self._raise_unhealthy_startup()
        except StartupIntegrityError as exc:
            if self._fail_closed:
                raise
            self.maintenance_mode = MaintenanceMode(reason=str(exc))
            try:
                self.conn.execute(
                    "UPDATE controller_mode SET mode = 'MAINTENANCE' "
                    "WHERE controller_id = ? AND mode_revision > 0",
                    (CONTROLLER_ID,),
                )
            except sqlite3.Error:
                pass

    def _raise_unhealthy_startup(self) -> None:
        user_version = int(self.conn.execute("PRAGMA user_version").fetchone()[0])
        if user_version != SCHEMA_VERSION:
            raise SchemaVersionError(
                f"unsupported workflow.db schema version {user_version}; "
                f"supported version is {SCHEMA_VERSION}"
            )
        quick_check = self.conn.execute("PRAGMA quick_check").fetchone()[0]
        if quick_check != "ok":
            raise StartupIntegrityError(f"SQLite quick_check failed: {quick_check}")
        fk_rows = self.conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_rows:
            raise StartupIntegrityError(f"SQLite foreign_key_check failed: {len(fk_rows)} row(s)")
        versions = {
            row[0]
            for row in self.conn.execute(
                "SELECT reducer_version FROM runs UNION SELECT reducer_version FROM transitions"
            )
        }
        unsupported = versions - self._supported_reducer_versions
        if unsupported:
            raise ReducerVersionError(
                "unsupported reducer version(s): " + ", ".join(sorted(unsupported))
            )

    @contextmanager
    def storage_mutation_lock(self) -> Iterator[None]:
        fd = os.open(self.storage_lock_path, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextmanager
    def transaction(
        self,
        *,
        fault: FaultInjectionPoint | None = None,
        before_response_ack: Callable[[], None] | None = None,
    ) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        committed = False
        try:
            yield self.conn
            if fault is FaultInjectionPoint.BEFORE_COMMIT:
                raise TransactionFault(FaultInjectionPoint.BEFORE_COMMIT.value)
            self.conn.commit()
            committed = True
            if fault is FaultInjectionPoint.AFTER_COMMIT:
                raise TransactionFault(FaultInjectionPoint.AFTER_COMMIT.value)
            if before_response_ack is not None:
                before_response_ack()
            if fault is FaultInjectionPoint.BEFORE_RESPONSE_ACK:
                raise TransactionFault(FaultInjectionPoint.BEFORE_RESPONSE_ACK.value)
        except Exception:
            if not committed:
                self.conn.rollback()
            raise

    def create_run(
        self,
        *,
        run_id: str,
        project_id: str,
        work_item_key: str,
        state: str,
        reducer_version: str = next(iter(SUPPORTED_REDUCER_VERSIONS)),
        specification_generation: int = 1,
    ) -> None:
        require_lowercase_uuid(run_id, field="run_id")
        if reducer_version not in self._supported_reducer_versions:
            raise ReducerVersionError(f"unsupported reducer version {reducer_version!r}")
        enums.parse_enum("run.state", state)
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO runs(run_id, project_id, work_item_key, specification_generation, "
            "state, terminal_outcome, reducer_version, current_revision, created_at_ms, "
            "updated_at_ms) VALUES (?, ?, ?, ?, ?, NULL, ?, 0, ?, ?)",
            (
                run_id,
                project_id,
                work_item_key,
                specification_generation,
                state,
                reducer_version,
                now,
                now,
            ),
        )

    def append_transition(
        self,
        *,
        run_id: str,
        transition_id: str,
        prior_state: str,
        trigger_kind: str,
        trigger_id: str,
        next_state: str,
        reducer_version: str,
        input_digest: str,
        specification_generation: int,
        admit_base_observation_id: str | None = None,
    ) -> Transition:
        require_lowercase_uuid(run_id, field="run_id")
        require_lowercase_uuid(transition_id, field="transition_id")
        enums.parse_enum("run.state", prior_state)
        enums.parse_enum("transition.trigger_kind", trigger_kind)
        enums.parse_enum("run.state", next_state)
        _require_digest(input_digest, field="input_digest")
        if reducer_version not in self._supported_reducer_versions:
            raise ReducerVersionError(f"unsupported reducer version {reducer_version!r}")
        existing = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? AND trigger_kind = ? AND trigger_id = ?",
            (run_id, trigger_kind, trigger_id),
        ).fetchone()
        if existing is not None:
            row = _row_to_transition(existing)
            if (
                row.transition_id == transition_id
                and row.prior_state == prior_state
                and row.next_state == next_state
                and row.reducer_version == reducer_version
                and row.input_digest == input_digest
                and row.specification_generation == specification_generation
            ):
                return row
            raise IdempotencyConflictError("transition trigger was already consumed differently")
        row = self.conn.execute(
            "SELECT COALESCE(MAX(transition_sequence), 0) + 1 FROM transitions WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        sequence = int(row[0])
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO transitions(run_id, transition_sequence, transition_id, prior_state, "
            "trigger_kind, trigger_id, admit_base_observation_id, next_state, reducer_version, "
            "input_digest, specification_generation, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                sequence,
                transition_id,
                prior_state,
                trigger_kind,
                trigger_id,
                admit_base_observation_id,
                next_state,
                reducer_version,
                input_digest,
                specification_generation,
                now,
            ),
        )
        self.conn.execute(
            "UPDATE runs SET state = ?, specification_generation = ?, updated_at_ms = ? "
            "WHERE run_id = ?",
            (next_state, specification_generation, now, run_id),
        )
        inserted = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? AND transition_sequence = ?",
            (run_id, sequence),
        ).fetchone()
        assert inserted is not None
        return _row_to_transition(inserted)

    def insert_immutable_fact(
        self,
        *,
        fact_kind: str,
        fact_id: str,
        payload_digest: str,
        payload: Any,
        source_kind: str | None = None,
        source_id: str | None = None,
    ) -> ImmutableFact:
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM immutable_facts WHERE fact_kind = ? AND fact_id = ?",
            (fact_kind, fact_id),
        ).fetchone()
        if existing is not None:
            row = _row_to_fact(existing)
            if row.payload_digest == payload_digest and row.payload_json == payload_json:
                return row
            raise IdempotencyConflictError("immutable fact id was reused with different content")
        source_existing = None
        if source_kind is not None and source_id is not None:
            source_existing = self.conn.execute(
                "SELECT * FROM immutable_facts WHERE source_kind = ? AND source_id = ?",
                (source_kind, source_id),
            ).fetchone()
        if source_existing is not None:
            row = _row_to_fact(source_existing)
            if row.payload_digest == payload_digest and row.payload_json == payload_json:
                return row
            raise IdempotencyConflictError("source identity already produced a different fact")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO immutable_facts(fact_kind, fact_id, payload_digest, payload_json, "
            "source_kind, source_id, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (fact_kind, fact_id, payload_digest, payload_json, source_kind, source_id, now),
        )
        row = self.conn.execute(
            "SELECT * FROM immutable_facts WHERE fact_kind = ? AND fact_id = ?",
            (fact_kind, fact_id),
        ).fetchone()
        assert row is not None
        return _row_to_fact(row)

    def insert_source_unique_record(
        self,
        *,
        source_kind: str,
        source_id: str,
        record_kind: str,
        record_id: str,
        payload_digest: str,
        payload: Any,
    ) -> SourceUniqueRecord:
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM source_unique_records WHERE source_kind = ? AND source_id = ?",
            (source_kind, source_id),
        ).fetchone()
        if existing is not None:
            row = _row_to_source_record(existing)
            if (
                row.record_kind == record_kind
                and row.record_id == record_id
                and row.payload_digest == payload_digest
                and row.payload_json == payload_json
            ):
                return row
            raise IdempotencyConflictError("source identity was reused with different content")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO source_unique_records(source_kind, source_id, record_kind, record_id, "
            "payload_digest, payload_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (source_kind, source_id, record_kind, record_id, payload_digest, payload_json, now),
        )
        row = self.conn.execute(
            "SELECT * FROM source_unique_records WHERE source_kind = ? AND source_id = ?",
            (source_kind, source_id),
        ).fetchone()
        assert row is not None
        return _row_to_source_record(row)

    def insert_outbox(
        self,
        *,
        outbox_id: str,
        source_kind: str,
        source_id: str,
        destination: str,
        protocol_version: str,
        payload_digest: str,
        payload: Any,
        next_delivery_at_ms: int,
        attempt_id: str | None = None,
        attempt_generation: int | None = None,
        publication_id: str | None = None,
        effect_generation: int | None = None,
    ) -> OutboxRecord:
        require_lowercase_uuid(outbox_id, field="outbox_id")
        enums.parse_enum("outbox_record.source_kind", source_kind)
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM outbox WHERE source_kind = ? AND source_id = ? "
            "AND destination = ? AND payload_digest = ?",
            (source_kind, source_id, destination, payload_digest),
        ).fetchone()
        if existing is not None:
            row = _row_to_outbox(existing)
            if row.payload_json == payload_json and row.protocol_version == protocol_version:
                return row
            raise IdempotencyConflictError("outbox source was reused with different content")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO outbox(outbox_id, source_kind, source_id, destination, attempt_id, "
            "attempt_generation, publication_id, effect_generation, protocol_version, "
            "payload_digest, payload_json, next_delivery_at_ms, state, delivery_count, "
            "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?)",
            (
                outbox_id,
                source_kind,
                source_id,
                destination,
                attempt_id,
                attempt_generation,
                publication_id,
                effect_generation,
                protocol_version,
                payload_digest,
                payload_json,
                next_delivery_at_ms,
                now,
            ),
        )
        row = self.conn.execute("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,)).fetchone()
        assert row is not None
        return _row_to_outbox(row)

    def insert_projection_outbox(
        self,
        *,
        projection_outbox_id: str,
        run_id: str,
        transition_sequence: int,
        kind: str,
        target_kind: str,
        target_id: str,
        payload_digest: str,
        payload: Any,
        idempotency_key: str,
        next_delivery_at_ms: int,
        publication_id: str | None = None,
        effect_generation: int | None = None,
    ) -> ProjectionOutboxRecord:
        require_lowercase_uuid(projection_outbox_id, field="projection_outbox_id")
        require_lowercase_uuid(run_id, field="run_id")
        enums.parse_enum("projection_outbox_record.kind", kind)
        enums.parse_enum("projection_outbox_record.target_kind", target_kind)
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        existing = self.conn.execute(
            "SELECT * FROM projection_outbox WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if existing is not None:
            row = _row_to_projection(existing)
            if (
                row.run_id == run_id
                and row.transition_sequence == transition_sequence
                and row.payload_digest == payload_digest
                and row.payload_json == payload_json
            ):
                return row
            raise IdempotencyConflictError("projection idempotency key was reused")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO projection_outbox(projection_outbox_id, run_id, transition_sequence, "
            "kind, target_kind, target_id, publication_id, effect_generation, payload_digest, "
            "payload_json, idempotency_key, state, delivery_count, next_delivery_at_ms, "
            "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)",
            (
                projection_outbox_id,
                run_id,
                transition_sequence,
                kind,
                target_kind,
                target_id,
                publication_id,
                effect_generation,
                payload_digest,
                payload_json,
                idempotency_key,
                next_delivery_at_ms,
                now,
            ),
        )
        row = self.conn.execute(
            "SELECT * FROM projection_outbox WHERE projection_outbox_id = ?",
            (projection_outbox_id,),
        ).fetchone()
        assert row is not None
        return _row_to_projection(row)

    def put_revisioned_object(
        self,
        *,
        object_kind: str,
        object_id: str,
        expected_revision: int,
        payload_digest: str,
        payload: Any,
    ) -> int:
        _require_digest(payload_digest, field="payload_digest")
        payload_json = _require_json_text(payload)
        now = _now_ms()
        existing = self.conn.execute(
            "SELECT revision, payload_digest, payload_json FROM revisioned_objects "
            "WHERE object_kind = ? AND object_id = ?",
            (object_kind, object_id),
        ).fetchone()
        if existing is None:
            if expected_revision != 0:
                raise CasMismatchError("missing revisioned object did not match expected revision")
            self.conn.execute(
                "INSERT INTO revisioned_objects(object_kind, object_id, revision, payload_digest, "
                "payload_json, updated_at_ms) VALUES (?, ?, 1, ?, ?, ?)",
                (object_kind, object_id, payload_digest, payload_json, now),
            )
            return 1
        if int(existing["revision"]) != expected_revision:
            raise CasMismatchError("revisioned object CAS lost")
        if (
            existing["payload_digest"] == payload_digest
            and existing["payload_json"] == payload_json
        ):
            return expected_revision
        new_revision = expected_revision + 1
        cur = self.conn.execute(
            "UPDATE revisioned_objects SET revision = ?, payload_digest = ?, payload_json = ?, "
            "updated_at_ms = ? WHERE object_kind = ? AND object_id = ? AND revision = ?",
            (
                new_revision,
                payload_digest,
                payload_json,
                now,
                object_kind,
                object_id,
                expected_revision,
            ),
        )
        if cur.rowcount != 1:
            raise CasMismatchError("revisioned object CAS lost")
        return new_revision

    def record_durable_operation(
        self,
        *,
        operation_id: str,
        operation_kind: str,
        principal_id: str,
        idempotency_key: str,
        request_payload: Any,
        status: str,
        response_payload: Any,
        response_http_status: int,
    ) -> DurableOperation:
        require_lowercase_uuid(operation_id, field="operation_id")
        if not is_lowercase_uuid(idempotency_key):
            raise ValueError("idempotency_key must be a lowercase canonical UUID string")
        req_digest = request_digest(request_payload)
        response_json = _require_json_text(response_payload)
        resp_digest = response_digest(
            {
                "http_status": response_http_status,
                "body": _response_digest_preimage(response_payload),
            }
        )
        existing = self.conn.execute(
            "SELECT * FROM durable_operations WHERE principal_id = ? AND idempotency_key = ?",
            (principal_id, idempotency_key),
        ).fetchone()
        if existing is not None:
            row = _row_to_operation(existing)
            if row.request_digest == req_digest:
                return row
            raise IdempotencyConflictError("operation idempotency key was reused")
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO durable_operations(operation_id, operation_kind, principal_id, "
            "idempotency_key, request_digest, status, response_json, response_digest, "
            "response_http_status, committed_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                operation_id,
                operation_kind,
                principal_id,
                idempotency_key,
                req_digest,
                status,
                response_json,
                resp_digest,
                response_http_status,
                now,
            ),
        )
        row = self.conn.execute(
            "SELECT * FROM durable_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        assert row is not None
        return _row_to_operation(row)
