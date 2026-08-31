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
    capability_public_key_digest,
    is_valid_content_digest,
    request_digest,
    response_digest,
)
from orcest.workflow_contract.v1.identity import is_lowercase_uuid, require_lowercase_uuid
from orcest.workflow_contract.v1.protocol_registry import (
    CAPABILITY_KEY_OPERATION_PROTOCOL,
    CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL,
    CONTROLLER_MODE_OPERATION_PROTOCOL,
    CONTROLLER_MODE_RESULT_PROTOCOL,
)

SCHEMA_VERSION = 3
DEFAULT_REDUCER_VERSION = "workflow-control-v1/reducer-0"
SUPPORTED_REDUCER_VERSIONS = frozenset({DEFAULT_REDUCER_VERSION})
CONTROLLER_ID = "ORCEST_V1"
PRIOR_STATE_NONE = "NONE"

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


class WorkflowGateClosedError(RunStoreError):
    """Raised when the durable controller mode or key registry forbids work."""


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
class RunRecord:
    run_id: str
    project_id: str
    work_item_key: str
    specification_generation: int
    state: str
    terminal_outcome: str | None
    reducer_version: str
    current_revision: int
    created_at_ms: int
    updated_at_ms: int


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


@dataclass(frozen=True, slots=True)
class ControllerModeProjection:
    controller_id: str
    mode_revision: int
    mode: str | None
    dispatch_paused_intake_policy: str | None
    maintenance_prior_mode: str | None
    maintenance_prior_dispatch_paused_intake_policy: str | None
    last_operation_id: str | None


@dataclass(frozen=True, slots=True)
class ControllerModeOperationResult:
    controller_mode_operation_id: str
    operation_kind: str
    status: str
    response_http_status: int
    response_json: str
    response_digest: str
    completed_at_ms: int
    rejection_code: str | None = None
    mode_revision: int | None = None
    mode: str | None = None
    dispatch_paused_intake_policy: str | None = None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class CapabilityKeyRegistryProjection:
    registry_id: str
    registry_revision: int
    current_issuance_key_id: str | None
    last_operation_id: str | None


@dataclass(frozen=True, slots=True)
class CapabilitySigningKey:
    capability_signing_key_id: str
    registration_operation_id: str
    signature_algorithm: str
    public_verification_key: bytes
    public_key_digest: str
    private_signing_secret_ref: str
    registered_at_ms: int
    not_before_ms: int
    state: str
    retired_at_ms: int | None = None
    retirement_change_id: str | None = None
    retirement_principal_id: str | None = None
    retirement_authorization_digest: str | None = None
    revoked_at_ms: int | None = None
    revocation_change_id: str | None = None
    revocation_principal_id: str | None = None
    revocation_authorization_digest: str | None = None


@dataclass(frozen=True, slots=True)
class CapabilityKeyOperationResult:
    capability_key_operation_id: str
    kind: str
    status: str
    response_http_status: int
    response_json: str
    response_digest: str
    completed_at_ms: int
    rejection_code: str | None = None
    registry_revision: int | None = None
    current_issuance_key_id: str | None = None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ControllerGatePermissions:
    mode_revision: int
    mode: str | None
    registry_revision: int
    current_issuance_key_id: str | None
    new_admission: bool
    new_claims: bool
    first_result_mutation: bool
    existing_result_replay: bool
    forge_reconciliation: bool
    management_operations: bool


@dataclass(frozen=True, slots=True)
class IssuedCapabilityBinding:
    capability_jti: str
    capability_signing_key_id: str
    signature_algorithm: str
    claim_digest: str
    immutable_assignment_digest: str
    immutable_assignment_json: str
    capability_key_registry_revision: int
    issued_at_ms: int


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


def _row_to_run(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        run_id=row["run_id"],
        project_id=row["project_id"],
        work_item_key=row["work_item_key"],
        specification_generation=row["specification_generation"],
        state=row["state"],
        terminal_outcome=row["terminal_outcome"],
        reducer_version=row["reducer_version"],
        current_revision=row["current_revision"],
        created_at_ms=row["created_at_ms"],
        updated_at_ms=row["updated_at_ms"],
    )


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


def _row_to_controller_mode(row: sqlite3.Row) -> ControllerModeProjection:
    return ControllerModeProjection(
        controller_id=row["controller_id"],
        mode_revision=row["mode_revision"],
        mode=row["mode"],
        dispatch_paused_intake_policy=row["dispatch_paused_intake_policy"],
        maintenance_prior_mode=row["maintenance_prior_mode"],
        maintenance_prior_dispatch_paused_intake_policy=row[
            "maintenance_prior_dispatch_paused_intake_policy"
        ],
        last_operation_id=row["last_operation_id"],
    )


def _row_to_controller_mode_operation(
    row: sqlite3.Row, *, replayed: bool
) -> ControllerModeOperationResult:
    return ControllerModeOperationResult(
        controller_mode_operation_id=row["controller_mode_operation_id"],
        operation_kind=row["operation_kind"],
        status=row["status"],
        rejection_code=row["rejection_code"],
        mode_revision=row["result_mode_revision"],
        mode=row["result_mode"],
        dispatch_paused_intake_policy=row["result_dispatch_paused_intake_policy"],
        response_http_status=row["response_http_status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        completed_at_ms=row["completed_at_ms"],
        replayed=replayed,
    )


def _row_to_capability_registry(row: sqlite3.Row) -> CapabilityKeyRegistryProjection:
    return CapabilityKeyRegistryProjection(
        registry_id=row["registry_id"],
        registry_revision=row["registry_revision"],
        current_issuance_key_id=row["current_issuance_key_id"],
        last_operation_id=row["last_operation_id"],
    )


def _row_to_capability_key(row: sqlite3.Row) -> CapabilitySigningKey:
    return CapabilitySigningKey(
        capability_signing_key_id=row["capability_signing_key_id"],
        registration_operation_id=row["registration_operation_id"],
        signature_algorithm=row["signature_algorithm"],
        public_verification_key=row["public_verification_key"],
        public_key_digest=row["public_key_digest"],
        private_signing_secret_ref=row["private_signing_secret_ref"],
        registered_at_ms=row["registered_at_ms"],
        not_before_ms=row["not_before_ms"],
        state=row["state"],
        retired_at_ms=row["retired_at_ms"],
        retirement_change_id=row["retirement_change_id"],
        retirement_principal_id=row["retirement_principal_id"],
        retirement_authorization_digest=row["retirement_authorization_digest"],
        revoked_at_ms=row["revoked_at_ms"],
        revocation_change_id=row["revocation_change_id"],
        revocation_principal_id=row["revocation_principal_id"],
        revocation_authorization_digest=row["revocation_authorization_digest"],
    )


def _row_to_capability_key_operation(
    row: sqlite3.Row, *, replayed: bool
) -> CapabilityKeyOperationResult:
    return CapabilityKeyOperationResult(
        capability_key_operation_id=row["capability_key_operation_id"],
        kind=row["kind"],
        status=row["status"],
        rejection_code=row["rejection_code"],
        registry_revision=row["result_registry_revision"],
        current_issuance_key_id=row["result_issuance_key_id"],
        response_http_status=row["response_http_status"],
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        completed_at_ms=row["completed_at_ms"],
        replayed=replayed,
    )


def _row_to_issued_capability(row: sqlite3.Row) -> IssuedCapabilityBinding:
    return IssuedCapabilityBinding(
        capability_jti=row["capability_jti"],
        capability_signing_key_id=row["capability_signing_key_id"],
        signature_algorithm=row["signature_algorithm"],
        claim_digest=row["claim_digest"],
        immutable_assignment_digest=row["immutable_assignment_digest"],
        immutable_assignment_json=row["immutable_assignment_json"],
        capability_key_registry_revision=row["capability_key_registry_revision"],
        issued_at_ms=row["issued_at_ms"],
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
    (maintenance_prior_dispatch_paused_intake_policy IS NOT NULL)
    = (maintenance_prior_mode = 'DISPATCH_PAUSED')
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
  backup_manifest_digest TEXT,
  backup_prior_mode TEXT CHECK (
    backup_prior_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  backup_prior_dispatch_paused_intake_policy TEXT
    CHECK (backup_prior_dispatch_paused_intake_policy IN (
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

CREATE TABLE IF NOT EXISTS capability_key_registry (
  registry_id TEXT PRIMARY KEY CHECK (registry_id = '{CONTROLLER_ID}'),
  registry_revision INTEGER NOT NULL CHECK (registry_revision >= 0),
  current_issuance_key_id TEXT,
  last_operation_id TEXT,
  FOREIGN KEY (current_issuance_key_id)
    REFERENCES capability_signing_keys(capability_signing_key_id) ON DELETE RESTRICT,
  FOREIGN KEY (last_operation_id)
    REFERENCES capability_key_operations(capability_key_operation_id) ON DELETE RESTRICT,
  CHECK (
    (registry_revision = 0 AND current_issuance_key_id IS NULL AND last_operation_id IS NULL)
    OR registry_revision > 0
  )
);

CREATE TABLE IF NOT EXISTS capability_key_operations (
  capability_key_operation_id TEXT PRIMARY KEY,
  protocol_version TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (
    kind IN ({_sql_in(_enum_values("capability_key_operation.kind"))})
  ),
  expected_registry_revision INTEGER NOT NULL CHECK (expected_registry_revision >= 0),
  expected_issuance_key_id TEXT,
  target_capability_signing_key_id TEXT NOT NULL,
  replacement_issuance_key_id TEXT,
  register_public_verification_key BLOB,
  register_public_key_digest TEXT,
  register_private_signing_secret_ref TEXT,
  register_not_before_ms INTEGER CHECK (
    register_not_before_ms IS NULL OR register_not_before_ms >= 0
  ),
  authenticated_principal_id TEXT NOT NULL,
  authorization_context_digest TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ({_sql_in(_enum_values("capability_key_operation.status"))})
  ),
  rejection_code TEXT CHECK (
    rejection_code IN ({_sql_in(_enum_values("capability_key_operation.rejection_code"))})
  ),
  result_registry_revision INTEGER CHECK (result_registry_revision > 0),
  result_issuance_key_id TEXT,
  response_http_status INTEGER NOT NULL CHECK (response_http_status BETWEEN 100 AND 599),
  response_json TEXT NOT NULL,
  response_digest TEXT NOT NULL,
  completed_at_ms INTEGER NOT NULL CHECK (completed_at_ms >= 0),
  CHECK ((status = 'SUCCEEDED' AND rejection_code IS NULL)
    OR (status = 'REJECTED' AND rejection_code IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS capability_signing_keys (
  capability_signing_key_id TEXT PRIMARY KEY,
  registration_operation_id TEXT NOT NULL UNIQUE
    REFERENCES capability_key_operations(capability_key_operation_id) ON DELETE RESTRICT,
  signature_algorithm TEXT NOT NULL CHECK (
    signature_algorithm IN ({_sql_in(_enum_values("capability_signing_key.signature_algorithm"))})
  ),
  public_verification_key BLOB NOT NULL CHECK (length(public_verification_key) = 32),
  public_key_digest TEXT NOT NULL UNIQUE,
  private_signing_secret_ref TEXT NOT NULL,
  registered_at_ms INTEGER NOT NULL CHECK (registered_at_ms >= 0),
  not_before_ms INTEGER NOT NULL CHECK (not_before_ms >= 0),
  state TEXT NOT NULL CHECK (
    state IN ({_sql_in(_enum_values("capability_signing_key.state"))})
  ),
  retired_at_ms INTEGER CHECK (retired_at_ms IS NULL OR retired_at_ms >= registered_at_ms),
  retirement_change_id TEXT,
  retirement_principal_id TEXT,
  retirement_authorization_digest TEXT,
  revoked_at_ms INTEGER CHECK (revoked_at_ms IS NULL OR revoked_at_ms >= registered_at_ms),
  revocation_change_id TEXT,
  revocation_principal_id TEXT,
  revocation_authorization_digest TEXT,
  CHECK (
    (state = 'ACTIVE' AND retired_at_ms IS NULL AND retirement_change_id IS NULL
      AND retirement_principal_id IS NULL AND retirement_authorization_digest IS NULL
      AND revoked_at_ms IS NULL AND revocation_change_id IS NULL
      AND revocation_principal_id IS NULL AND revocation_authorization_digest IS NULL)
    OR (state = 'RETIRED' AND retired_at_ms IS NOT NULL AND retirement_change_id IS NOT NULL
      AND retirement_principal_id IS NOT NULL AND retirement_authorization_digest IS NOT NULL
      AND revoked_at_ms IS NULL AND revocation_change_id IS NULL
      AND revocation_principal_id IS NULL AND revocation_authorization_digest IS NULL)
    OR (state = 'REVOKED' AND revoked_at_ms IS NOT NULL AND revocation_change_id IS NOT NULL
      AND revocation_principal_id IS NOT NULL AND revocation_authorization_digest IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS capability_issuance_audit (
  capability_jti TEXT PRIMARY KEY,
  capability_signing_key_id TEXT NOT NULL
    REFERENCES capability_signing_keys(capability_signing_key_id) ON DELETE RESTRICT,
  signature_algorithm TEXT NOT NULL CHECK (
    signature_algorithm IN ({_sql_in(_enum_values("capability_signing_key.signature_algorithm"))})
  ),
  claim_digest TEXT NOT NULL,
  immutable_assignment_digest TEXT NOT NULL,
  immutable_assignment_json TEXT NOT NULL,
  capability_key_registry_revision INTEGER NOT NULL CHECK (
    capability_key_registry_revision > 0
  ),
  issued_at_ms INTEGER NOT NULL CHECK (issued_at_ms >= 0)
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  work_item_key TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
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
  prior_state TEXT NOT NULL CHECK (
    prior_state = '{PRIOR_STATE_NONE}'
    OR prior_state IN ({_sql_in(_enum_values("run.state"))})
  ),
  trigger_kind TEXT NOT NULL CHECK (
    trigger_kind IN ({_sql_in(_enum_values("transition.trigger_kind"))})
  ),
  trigger_id TEXT NOT NULL,
  admit_base_observation_id TEXT,
  next_state TEXT NOT NULL CHECK (next_state IN ({_sql_in(_enum_values("run.state"))})),
  reducer_version TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
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

_V1_TO_V2 = f"""
PRAGMA foreign_keys=OFF;
CREATE TABLE runs_v2 (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  work_item_key TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  state TEXT NOT NULL CHECK (state IN ({_sql_in(_enum_values("run.state"))})),
  terminal_outcome TEXT CHECK (
    terminal_outcome IN ({_sql_in(_enum_values("run.terminal_outcome"))})
  ),
  reducer_version TEXT NOT NULL,
  current_revision INTEGER NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms)
);
INSERT INTO runs_v2 SELECT * FROM runs;
DROP TABLE runs;
ALTER TABLE runs_v2 RENAME TO runs;
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active_work_item
ON runs(project_id, work_item_key) WHERE terminal_outcome IS NULL;
CREATE TABLE transitions_v2 (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  transition_sequence INTEGER NOT NULL CHECK (transition_sequence > 0),
  transition_id TEXT NOT NULL UNIQUE,
  prior_state TEXT NOT NULL CHECK (
    prior_state = '{PRIOR_STATE_NONE}'
    OR prior_state IN ({_sql_in(_enum_values("run.state"))})
  ),
  trigger_kind TEXT NOT NULL CHECK (
    trigger_kind IN ({_sql_in(_enum_values("transition.trigger_kind"))})
  ),
  trigger_id TEXT NOT NULL,
  admit_base_observation_id TEXT,
  next_state TEXT NOT NULL CHECK (next_state IN ({_sql_in(_enum_values("run.state"))})),
  reducer_version TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  specification_generation INTEGER NOT NULL CHECK (specification_generation >= 0),
  created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
  PRIMARY KEY (run_id, transition_sequence),
  UNIQUE (run_id, trigger_kind, trigger_id)
);
INSERT INTO transitions_v2 SELECT * FROM transitions;
DROP TABLE transitions;
ALTER TABLE transitions_v2 RENAME TO transitions;
PRAGMA foreign_keys=ON;
"""

_V2_TO_V3 = f"""
PRAGMA foreign_keys=OFF;
CREATE TABLE controller_mode_operations_v3 (
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
  backup_manifest_digest TEXT,
  backup_prior_mode TEXT CHECK (
    backup_prior_mode IN ({_sql_in(_enum_values("controller_mode.mode"))})
  ),
  backup_prior_dispatch_paused_intake_policy TEXT
    CHECK (backup_prior_dispatch_paused_intake_policy IN (
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
INSERT INTO controller_mode_operations_v3 (
  controller_mode_operation_id, protocol_version, operation_kind,
  expected_mode_revision, expected_mode, requested_mode,
  requested_dispatch_paused_intake_policy, authenticated_principal_id,
  authorization_context_digest, request_digest, status, rejection_code,
  result_mode_revision, result_mode, result_dispatch_paused_intake_policy,
  response_http_status, response_json, response_digest, completed_at_ms
)
SELECT
  controller_mode_operation_id, protocol_version, operation_kind,
  expected_mode_revision, expected_mode, requested_mode,
  requested_dispatch_paused_intake_policy, authenticated_principal_id,
  authorization_context_digest, request_digest, status, rejection_code,
  result_mode_revision, result_mode, result_dispatch_paused_intake_policy,
  response_http_status, response_json, response_digest, completed_at_ms
FROM controller_mode_operations;
CREATE TABLE controller_mode_v3 (
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
    (maintenance_prior_dispatch_paused_intake_policy IS NOT NULL)
    = (maintenance_prior_mode = 'DISPATCH_PAUSED')
  )
);
INSERT INTO controller_mode_v3 (
  controller_id, mode_revision, mode, dispatch_paused_intake_policy,
  maintenance_prior_mode, maintenance_prior_dispatch_paused_intake_policy,
  last_operation_id
)
SELECT
  controller_id, mode_revision, mode, dispatch_paused_intake_policy,
  maintenance_prior_mode, maintenance_prior_dispatch_paused_intake_policy,
  last_operation_id
FROM controller_mode;
DROP TABLE controller_mode;
DROP TABLE controller_mode_operations;
ALTER TABLE controller_mode_operations_v3 RENAME TO controller_mode_operations;
ALTER TABLE controller_mode_v3 RENAME TO controller_mode;
PRAGMA foreign_keys=ON;
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
        if current not in {0, 1, 2}:
            raise SchemaVersionError(
                f"unsupported workflow.db schema version {current}; "
                f"supported version is {SCHEMA_VERSION}"
            )
        # Connection.executescript() issues COMMIT first whenever a transaction
        # is already open (sqlite3 documented behavior, independent of
        # isolation_level). BEGIN EXCLUSIVE therefore has to live inside the
        # script so DDL, seed rows, and the user_version bump share one txn.
        #
        # PRAGMA foreign_keys is a no-op once a transaction is open (SQLite
        # only honors it in autocommit mode), so the table-rebuild scripts'
        # own "PRAGMA foreign_keys=OFF;" lines can't actually suspend
        # enforcement for the BEGIN EXCLUSIVE they run inside. Toggle it here,
        # before that transaction starts, so DROP TABLE on a table still
        # carrying real rows referenced by another table's FK doesn't fail.
        self.conn.execute("PRAGMA foreign_keys=OFF")
        try:
            if current == 0:
                self.conn.executescript("BEGIN EXCLUSIVE;\n" + _SCHEMA)
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (SCHEMA_VERSION, "workflow-control-v1-base-store", _now_ms()),
                )
            elif current == 1:
                # _SCHEMA is idempotent (CREATE TABLE IF NOT EXISTS): a real
                # version-1 database already has controller_mode /
                # controller_mode_operations, so those two tables stay in their
                # version-1 shape here. Missing tables (capability-key) are
                # created; _V1_TO_V2 rebuilds runs/transitions; _V2_TO_V3 then
                # rebuilds controller_mode_operations (three new columns) and
                # controller_mode (bidirectional maintenance_prior_* CHECK) so
                # a v1-to-v3 upgrade lands in the same final shape as v2-to-v3.
                self.conn.executescript(
                    "BEGIN EXCLUSIVE;\n" + _SCHEMA + "\n" + _V1_TO_V2 + "\n" + _V2_TO_V3
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (SCHEMA_VERSION, "workflow-control-v1-reducer-ledger", _now_ms()),
                )
            else:
                # _SCHEMA is idempotent (CREATE TABLE IF NOT EXISTS): a real version-2
                # database already has controller_mode/controller_mode_operations, so
                # only the capability-key tables get created here; _V2_TO_V3 then
                # rebuilds controller_mode_operations (three new columns) and
                # controller_mode (bidirectional maintenance_prior_* CHECK) in place.
                self.conn.executescript("BEGIN EXCLUSIVE;\n" + _SCHEMA + "\n" + _V2_TO_V3)
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_ms) "
                    "VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        "workflow-control-v1-controller-mode-and-key-gates",
                        _now_ms(),
                    ),
                )
            self.conn.execute(
                "INSERT OR IGNORE INTO controller_mode"
                "(controller_id, mode_revision, mode, dispatch_paused_intake_policy, "
                "maintenance_prior_mode, maintenance_prior_dispatch_paused_intake_policy, "
                "last_operation_id) VALUES (?, 0, NULL, NULL, NULL, NULL, NULL)",
                (CONTROLLER_ID,),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO capability_key_registry"
                "(registry_id, registry_revision, current_issuance_key_id, last_operation_id) "
                "VALUES (?, 0, NULL, NULL)",
                (CONTROLLER_ID,),
            )
            self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.execute("PRAGMA foreign_keys=ON")

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
            if fault is FaultInjectionPoint.BEFORE_RESPONSE_ACK:
                raise TransactionFault(FaultInjectionPoint.BEFORE_RESPONSE_ACK.value)
            if before_response_ack is not None:
                before_response_ack()
        except Exception:
            if not committed:
                self.conn.rollback()
            raise

    def get_controller_mode(self) -> ControllerModeProjection:
        row = self.conn.execute(
            "SELECT * FROM controller_mode WHERE controller_id = ?", (CONTROLLER_ID,)
        ).fetchone()
        assert row is not None
        return _row_to_controller_mode(row)

    def get_capability_key_registry(self) -> CapabilityKeyRegistryProjection:
        row = self.conn.execute(
            "SELECT * FROM capability_key_registry WHERE registry_id = ?", (CONTROLLER_ID,)
        ).fetchone()
        assert row is not None
        return _row_to_capability_registry(row)

    def get_capability_signing_key(self, key_id: str) -> CapabilitySigningKey | None:
        require_lowercase_uuid(key_id, field="capability_signing_key_id")
        row = self.conn.execute(
            "SELECT * FROM capability_signing_keys WHERE capability_signing_key_id = ?",
            (key_id,),
        ).fetchone()
        return None if row is None else _row_to_capability_key(row)

    def _controller_mode_response(
        self,
        *,
        operation_id: str,
        operation_kind: str,
        status: str,
        rejection_code: str | None = None,
        mode_revision: int | None = None,
        mode: str | None = None,
        dispatch_paused_intake_policy: str | None = None,
    ) -> tuple[int, str, str]:
        body: dict[str, object] = {
            "protocol_version": CONTROLLER_MODE_RESULT_PROTOCOL,
            "controller_mode_operation_id": operation_id,
            "operation_kind": operation_kind,
            "status": status,
            "replayed": False,
        }
        if status == "SUCCEEDED":
            body.update(
                {
                    "mode_revision": mode_revision,
                    "mode": mode,
                    "dispatch_paused_intake_policy": dispatch_paused_intake_policy,
                }
            )
            http_status = 200
        else:
            body["rejection_code"] = rejection_code
            http_status = 403 if rejection_code == "AUTHORITY_REVOKED" else 409
        body_json = canonical_json_text(body)
        digest = response_digest(
            {"http_status": http_status, "body": _response_digest_preimage(body)}
        )
        return http_status, body_json, digest

    def _capability_key_response(
        self,
        *,
        operation_id: str,
        kind: str,
        status: str,
        rejection_code: str | None = None,
        registry_revision: int | None = None,
        current_issuance_key_id: str | None = None,
    ) -> tuple[int, str, str]:
        body: dict[str, object] = {
            "protocol_version": CAPABILITY_KEY_OPERATION_RESULT_PROTOCOL,
            "capability_key_operation_id": operation_id,
            "kind": kind,
            "status": status,
            "replayed": False,
        }
        if status == "SUCCEEDED":
            body.update(
                {
                    "registry_revision": registry_revision,
                    "current_issuance_key_id": current_issuance_key_id,
                }
            )
            http_status = 200
        else:
            body["rejection_code"] = rejection_code
            http_status = 403 if rejection_code == "AUTHORITY_REVOKED" else 409
        body_json = canonical_json_text(body)
        digest = response_digest(
            {"http_status": http_status, "body": _response_digest_preimage(body)}
        )
        return http_status, body_json, digest

    def _controller_mode_request_digest(
        self,
        *,
        operation_kind: str,
        expected_mode_revision: int,
        expected_mode: str | None,
        requested_mode: str | None,
        requested_dispatch_paused_intake_policy: str | None,
        backup_manifest_digest: str | None,
        backup_prior_mode: str | None,
        backup_prior_dispatch_paused_intake_policy: str | None,
        authenticated_principal_id: str,
        authorization_context_digest: str,
    ) -> str:
        return request_digest(
            {
                "protocol_version": CONTROLLER_MODE_OPERATION_PROTOCOL,
                "operation_kind": operation_kind,
                "expected_mode_revision": expected_mode_revision,
                "expected_mode": expected_mode,
                "requested_mode": requested_mode,
                "requested_dispatch_paused_intake_policy": requested_dispatch_paused_intake_policy,
                "backup_manifest_digest": backup_manifest_digest,
                "backup_prior_mode": backup_prior_mode,
                "backup_prior_dispatch_paused_intake_policy": (
                    backup_prior_dispatch_paused_intake_policy
                ),
                "authenticated_principal_id": authenticated_principal_id,
                "authorization_context_digest": authorization_context_digest,
            }
        )

    def apply_controller_mode_operation(
        self,
        *,
        controller_mode_operation_id: str,
        operation_kind: str,
        expected_mode_revision: int,
        expected_mode: str | None,
        requested_mode: str | None,
        requested_dispatch_paused_intake_policy: str | None = None,
        authenticated_principal_id: str,
        authorization_context_digest: str,
        authority_revoked: bool = False,
        backup_manifest_digest: str | None = None,
        backup_prior_mode: str | None = None,
        backup_prior_dispatch_paused_intake_policy: str | None = None,
    ) -> ControllerModeOperationResult:
        require_lowercase_uuid(controller_mode_operation_id, field="controller_mode_operation_id")
        enums.parse_enum("controller_mode_operation.operation_kind", operation_kind)
        if expected_mode is not None:
            enums.parse_enum("controller_mode.mode", expected_mode)
        if requested_mode is not None:
            enums.parse_enum("controller_mode.mode", requested_mode)
        if requested_dispatch_paused_intake_policy is not None:
            enums.parse_enum(
                "controller_mode.dispatch_paused_intake_policy",
                requested_dispatch_paused_intake_policy,
            )
        if backup_prior_mode is not None:
            enums.parse_enum("controller_mode.mode", backup_prior_mode)
        if backup_prior_dispatch_paused_intake_policy is not None:
            enums.parse_enum(
                "controller_mode.dispatch_paused_intake_policy",
                backup_prior_dispatch_paused_intake_policy,
            )
        _require_digest(authorization_context_digest, field="authorization_context_digest")
        if backup_manifest_digest is not None:
            _require_digest(backup_manifest_digest, field="backup_manifest_digest")
        req_digest = self._controller_mode_request_digest(
            operation_kind=operation_kind,
            expected_mode_revision=expected_mode_revision,
            expected_mode=expected_mode,
            requested_mode=requested_mode,
            requested_dispatch_paused_intake_policy=requested_dispatch_paused_intake_policy,
            backup_manifest_digest=backup_manifest_digest,
            backup_prior_mode=backup_prior_mode,
            backup_prior_dispatch_paused_intake_policy=(backup_prior_dispatch_paused_intake_policy),
            authenticated_principal_id=authenticated_principal_id,
            authorization_context_digest=authorization_context_digest,
        )
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM controller_mode_operations WHERE controller_mode_operation_id = ?",
                (controller_mode_operation_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["request_digest"] == req_digest
                ):
                    return _row_to_controller_mode_operation(existing, replayed=True)
                return self._transient_controller_mode_conflict(
                    controller_mode_operation_id, operation_kind
                )
            projection = self.get_controller_mode()
            rejection = self._validate_controller_mode_operation(
                projection=projection,
                operation_kind=operation_kind,
                expected_mode_revision=expected_mode_revision,
                expected_mode=expected_mode,
                requested_mode=requested_mode,
                requested_dispatch_paused_intake_policy=requested_dispatch_paused_intake_policy,
                authority_revoked=authority_revoked,
                backup_manifest_digest=backup_manifest_digest,
                backup_prior_mode=backup_prior_mode,
                backup_prior_dispatch_paused_intake_policy=(
                    backup_prior_dispatch_paused_intake_policy
                ),
            )
            result_revision = None
            result_mode = None
            result_policy = None
            prior_mode = None
            prior_policy = None
            if rejection is None:
                result_revision = projection.mode_revision + 1
                result_mode = requested_mode
                result_policy = requested_dispatch_paused_intake_policy
                if result_mode == "MAINTENANCE":
                    prior_mode = projection.maintenance_prior_mode
                    prior_policy = projection.maintenance_prior_dispatch_paused_intake_policy
                    if operation_kind == "SET_MODE":
                        prior_mode = projection.mode
                        prior_policy = projection.dispatch_paused_intake_policy
                    elif operation_kind == "RESTORE_BACKUP":
                        prior_mode = backup_prior_mode
                        prior_policy = backup_prior_dispatch_paused_intake_policy
                http_status, body_json, resp_digest = self._controller_mode_response(
                    operation_id=controller_mode_operation_id,
                    operation_kind=operation_kind,
                    status="SUCCEEDED",
                    mode_revision=result_revision,
                    mode=result_mode,
                    dispatch_paused_intake_policy=result_policy,
                )
                status = "SUCCEEDED"
            else:
                http_status, body_json, resp_digest = self._controller_mode_response(
                    operation_id=controller_mode_operation_id,
                    operation_kind=operation_kind,
                    status="REJECTED",
                    rejection_code=rejection,
                )
                status = "REJECTED"
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO controller_mode_operations("
                "controller_mode_operation_id, protocol_version, operation_kind, "
                "expected_mode_revision, expected_mode, requested_mode, "
                "requested_dispatch_paused_intake_policy, backup_manifest_digest, "
                "backup_prior_mode, backup_prior_dispatch_paused_intake_policy, "
                "authenticated_principal_id, authorization_context_digest, request_digest, "
                "status, rejection_code, result_mode_revision, result_mode, "
                "result_dispatch_paused_intake_policy, response_http_status, response_json, "
                "response_digest, completed_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    controller_mode_operation_id,
                    CONTROLLER_MODE_OPERATION_PROTOCOL,
                    operation_kind,
                    expected_mode_revision,
                    expected_mode,
                    requested_mode,
                    requested_dispatch_paused_intake_policy,
                    backup_manifest_digest,
                    backup_prior_mode,
                    backup_prior_dispatch_paused_intake_policy,
                    authenticated_principal_id,
                    authorization_context_digest,
                    req_digest,
                    status,
                    rejection,
                    result_revision,
                    result_mode,
                    result_policy,
                    http_status,
                    body_json,
                    resp_digest,
                    now,
                ),
            )
            if status == "SUCCEEDED":
                self.conn.execute(
                    "UPDATE controller_mode SET mode_revision = ?, mode = ?, "
                    "dispatch_paused_intake_policy = ?, maintenance_prior_mode = ?, "
                    "maintenance_prior_dispatch_paused_intake_policy = ?, "
                    "last_operation_id = ? WHERE controller_id = ? AND mode_revision = ?",
                    (
                        result_revision,
                        result_mode,
                        result_policy,
                        prior_mode,
                        prior_policy,
                        controller_mode_operation_id,
                        CONTROLLER_ID,
                        expected_mode_revision,
                    ),
                )
            row = self.conn.execute(
                "SELECT * FROM controller_mode_operations WHERE controller_mode_operation_id = ?",
                (controller_mode_operation_id,),
            ).fetchone()
            assert row is not None
            return _row_to_controller_mode_operation(row, replayed=False)

    def _transient_controller_mode_conflict(
        self, operation_id: str, operation_kind: str
    ) -> ControllerModeOperationResult:
        http_status, body_json, resp_digest = self._controller_mode_response(
            operation_id=operation_id,
            operation_kind=operation_kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
        )
        return ControllerModeOperationResult(
            controller_mode_operation_id=operation_id,
            operation_kind=operation_kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
            response_http_status=http_status,
            response_json=body_json,
            response_digest=resp_digest,
            completed_at_ms=_now_ms(),
        )

    def _validate_controller_mode_operation(
        self,
        *,
        projection: ControllerModeProjection,
        operation_kind: str,
        expected_mode_revision: int,
        expected_mode: str | None,
        requested_mode: str | None,
        requested_dispatch_paused_intake_policy: str | None,
        authority_revoked: bool,
        backup_manifest_digest: str | None,
        backup_prior_mode: str | None,
        backup_prior_dispatch_paused_intake_policy: str | None,
    ) -> str | None:
        if authority_revoked:
            return "AUTHORITY_REVOKED"
        if operation_kind == "INITIALIZE" and projection.mode_revision > 0:
            return "ALREADY_INITIALIZED"
        if projection.mode_revision != expected_mode_revision or projection.mode != expected_mode:
            return "CAS_LOST"
        requested_policy_ok = (
            requested_mode == "DISPATCH_PAUSED"
            and requested_dispatch_paused_intake_policy is not None
        ) or (
            requested_mode != "DISPATCH_PAUSED" and requested_dispatch_paused_intake_policy is None
        )
        if requested_mode is None or not requested_policy_ok:
            return "TRANSITION_NOT_ALLOWED"
        if operation_kind == "INITIALIZE":
            if requested_mode != "MAINTENANCE":
                return "TRANSITION_NOT_ALLOWED"
            return None
        if projection.mode_revision == 0 or projection.mode is None:
            return "NOT_INITIALIZED"
        if operation_kind == "SET_MODE":
            if backup_manifest_digest is not None:
                return "TRANSITION_NOT_ALLOWED"
            if (
                projection.mode == requested_mode
                and projection.dispatch_paused_intake_policy
                == requested_dispatch_paused_intake_policy
            ):
                return "NO_CHANGE"
            return None
        if operation_kind == "RESTORE_BACKUP":
            if backup_manifest_digest is None:
                return "TRANSITION_NOT_ALLOWED"
            if expected_mode == "MAINTENANCE":
                if requested_mode != "MAINTENANCE" or requested_dispatch_paused_intake_policy:
                    return "TRANSITION_NOT_ALLOWED"
                if (backup_prior_dispatch_paused_intake_policy is not None) != (
                    backup_prior_mode == "DISPATCH_PAUSED"
                ):
                    return "TRANSITION_NOT_ALLOWED"
                return None
            if (
                requested_mode == "DISPATCH_PAUSED"
                and requested_dispatch_paused_intake_policy == "PAUSE_ADMISSION"
                and backup_prior_mode is None
                and backup_prior_dispatch_paused_intake_policy is None
            ):
                return None
            return "TRANSITION_NOT_ALLOWED"
        raise AssertionError("unreachable operation kind")

    def _capability_key_request_digest(
        self,
        *,
        kind: str,
        expected_registry_revision: int,
        expected_issuance_key_id: str | None,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
        register_public_verification_key: bytes | None,
        register_public_key_digest: str | None,
        register_private_signing_secret_ref: str | None,
        register_not_before_ms: int | None,
        authenticated_principal_id: str,
        authorization_context_digest: str,
    ) -> str:
        public_key_hex = (
            None
            if register_public_verification_key is None
            else register_public_verification_key.hex()
        )
        return request_digest(
            {
                "protocol_version": CAPABILITY_KEY_OPERATION_PROTOCOL,
                "kind": kind,
                "expected_registry_revision": expected_registry_revision,
                "expected_issuance_key_id": expected_issuance_key_id,
                "target_capability_signing_key_id": target_capability_signing_key_id,
                "replacement_issuance_key_id": replacement_issuance_key_id,
                "register_public_verification_key": public_key_hex,
                "register_public_key_digest": register_public_key_digest,
                "register_private_signing_secret_ref": register_private_signing_secret_ref,
                "register_not_before_ms": register_not_before_ms,
                "authenticated_principal_id": authenticated_principal_id,
                "authorization_context_digest": authorization_context_digest,
            }
        )

    def apply_capability_key_operation(
        self,
        *,
        capability_key_operation_id: str,
        kind: str,
        expected_registry_revision: int,
        expected_issuance_key_id: str | None,
        target_capability_signing_key_id: str,
        authenticated_principal_id: str,
        authorization_context_digest: str,
        replacement_issuance_key_id: str | None = None,
        register_public_verification_key: bytes | None = None,
        register_public_key_digest: str | None = None,
        register_private_signing_secret_ref: str | None = None,
        register_not_before_ms: int | None = None,
        authority_revoked: bool = False,
        private_key_proof_valid: bool = False,
    ) -> CapabilityKeyOperationResult:
        require_lowercase_uuid(capability_key_operation_id, field="capability_key_operation_id")
        require_lowercase_uuid(
            target_capability_signing_key_id, field="target_capability_signing_key_id"
        )
        if expected_issuance_key_id is not None:
            require_lowercase_uuid(expected_issuance_key_id, field="expected_issuance_key_id")
        if replacement_issuance_key_id is not None:
            require_lowercase_uuid(replacement_issuance_key_id, field="replacement_issuance_key_id")
        enums.parse_enum("capability_key_operation.kind", kind)
        _require_digest(authorization_context_digest, field="authorization_context_digest")
        if register_public_key_digest is not None:
            _require_digest(register_public_key_digest, field="register_public_key_digest")
        req_digest = self._capability_key_request_digest(
            kind=kind,
            expected_registry_revision=expected_registry_revision,
            expected_issuance_key_id=expected_issuance_key_id,
            target_capability_signing_key_id=target_capability_signing_key_id,
            replacement_issuance_key_id=replacement_issuance_key_id,
            register_public_verification_key=register_public_verification_key,
            register_public_key_digest=register_public_key_digest,
            register_private_signing_secret_ref=register_private_signing_secret_ref,
            register_not_before_ms=register_not_before_ms,
            authenticated_principal_id=authenticated_principal_id,
            authorization_context_digest=authorization_context_digest,
        )
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM capability_key_operations WHERE capability_key_operation_id = ?",
                (capability_key_operation_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["authenticated_principal_id"] == authenticated_principal_id
                    and existing["request_digest"] == req_digest
                ):
                    return _row_to_capability_key_operation(existing, replayed=True)
                return self._transient_capability_key_conflict(capability_key_operation_id, kind)
            registry = self.get_capability_key_registry()
            rejection = self._validate_capability_key_operation(
                registry=registry,
                kind=kind,
                expected_registry_revision=expected_registry_revision,
                expected_issuance_key_id=expected_issuance_key_id,
                target_capability_signing_key_id=target_capability_signing_key_id,
                replacement_issuance_key_id=replacement_issuance_key_id,
                register_public_verification_key=register_public_verification_key,
                register_public_key_digest=register_public_key_digest,
                register_private_signing_secret_ref=register_private_signing_secret_ref,
                register_not_before_ms=register_not_before_ms,
                authority_revoked=authority_revoked,
                private_key_proof_valid=private_key_proof_valid,
            )
            result_revision = None
            result_key = None
            status = "REJECTED"
            if rejection is None:
                status = "SUCCEEDED"
                result_revision = registry.registry_revision + 1
                result_key = self._result_issuance_key_id(
                    current_key_id=registry.current_issuance_key_id,
                    kind=kind,
                    target_capability_signing_key_id=target_capability_signing_key_id,
                    replacement_issuance_key_id=replacement_issuance_key_id,
                )
                http_status, body_json, resp_digest = self._capability_key_response(
                    operation_id=capability_key_operation_id,
                    kind=kind,
                    status=status,
                    registry_revision=result_revision,
                    current_issuance_key_id=result_key,
                )
            else:
                http_status, body_json, resp_digest = self._capability_key_response(
                    operation_id=capability_key_operation_id,
                    kind=kind,
                    status=status,
                    rejection_code=rejection,
                )
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO capability_key_operations("
                "capability_key_operation_id, protocol_version, kind, "
                "expected_registry_revision, expected_issuance_key_id, "
                "target_capability_signing_key_id, replacement_issuance_key_id, "
                "register_public_verification_key, register_public_key_digest, "
                "register_private_signing_secret_ref, register_not_before_ms, "
                "authenticated_principal_id, authorization_context_digest, request_digest, "
                "status, rejection_code, result_registry_revision, result_issuance_key_id, "
                "response_http_status, response_json, response_digest, completed_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    capability_key_operation_id,
                    CAPABILITY_KEY_OPERATION_PROTOCOL,
                    kind,
                    expected_registry_revision,
                    expected_issuance_key_id,
                    target_capability_signing_key_id,
                    replacement_issuance_key_id,
                    register_public_verification_key,
                    register_public_key_digest,
                    register_private_signing_secret_ref,
                    register_not_before_ms,
                    authenticated_principal_id,
                    authorization_context_digest,
                    req_digest,
                    status,
                    rejection,
                    result_revision,
                    result_key,
                    http_status,
                    body_json,
                    resp_digest,
                    now,
                ),
            )
            if status == "SUCCEEDED":
                self._apply_successful_capability_key_operation(
                    kind=kind,
                    capability_key_operation_id=capability_key_operation_id,
                    target_capability_signing_key_id=target_capability_signing_key_id,
                    replacement_issuance_key_id=replacement_issuance_key_id,
                    register_public_verification_key=register_public_verification_key,
                    register_public_key_digest=register_public_key_digest,
                    register_private_signing_secret_ref=register_private_signing_secret_ref,
                    register_not_before_ms=register_not_before_ms,
                    result_revision=result_revision,
                    result_key=result_key,
                    now=now,
                )
            row = self.conn.execute(
                "SELECT * FROM capability_key_operations WHERE capability_key_operation_id = ?",
                (capability_key_operation_id,),
            ).fetchone()
            assert row is not None
            return _row_to_capability_key_operation(row, replayed=False)

    def _transient_capability_key_conflict(
        self, operation_id: str, kind: str
    ) -> CapabilityKeyOperationResult:
        http_status, body_json, resp_digest = self._capability_key_response(
            operation_id=operation_id,
            kind=kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
        )
        return CapabilityKeyOperationResult(
            capability_key_operation_id=operation_id,
            kind=kind,
            status="REJECTED",
            rejection_code="INTEGRITY_CONFLICT",
            response_http_status=http_status,
            response_json=body_json,
            response_digest=resp_digest,
            completed_at_ms=_now_ms(),
        )

    def _validate_capability_key_operation(
        self,
        *,
        registry: CapabilityKeyRegistryProjection,
        kind: str,
        expected_registry_revision: int,
        expected_issuance_key_id: str | None,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
        register_public_verification_key: bytes | None,
        register_public_key_digest: str | None,
        register_private_signing_secret_ref: str | None,
        register_not_before_ms: int | None,
        authority_revoked: bool,
        private_key_proof_valid: bool,
    ) -> str | None:
        if authority_revoked:
            return "AUTHORITY_REVOKED"
        if (
            registry.registry_revision != expected_registry_revision
            or registry.current_issuance_key_id != expected_issuance_key_id
        ):
            return "CAS_LOST"
        target = self.get_capability_signing_key(target_capability_signing_key_id)
        if kind == "REGISTER":
            if target is not None:
                return "KEY_ALREADY_EXISTS"
            if (
                register_public_verification_key is None
                or register_public_key_digest is None
                or register_private_signing_secret_ref is None
                or register_not_before_ms is None
                or replacement_issuance_key_id is not None
                or len(register_public_verification_key) != 32
                or capability_public_key_digest(register_public_verification_key)
                != register_public_key_digest
                or not private_key_proof_valid
            ):
                return "INTEGRITY_CONFLICT"
            digest_collision = self.conn.execute(
                "SELECT 1 FROM capability_signing_keys WHERE public_key_digest = ?",
                (register_public_key_digest,),
            ).fetchone()
            if digest_collision is not None:
                return "INTEGRITY_CONFLICT"
            return None
        if any(
            value is not None
            for value in (
                register_public_verification_key,
                register_public_key_digest,
                register_private_signing_secret_ref,
                register_not_before_ms,
            )
        ):
            return "INTEGRITY_CONFLICT"
        if target is None or target.state != "ACTIVE":
            if not (kind == "REVOKE" and target is not None and target.state == "RETIRED"):
                return "KEY_NOT_ACTIVE"
        replacement = (
            None
            if replacement_issuance_key_id is None
            else self.get_capability_signing_key(replacement_issuance_key_id)
        )
        if replacement_issuance_key_id is not None and (
            replacement is None or replacement.state != "ACTIVE"
        ):
            return "KEY_NOT_ACTIVE"
        if kind == "SELECT":
            if replacement_issuance_key_id is not None:
                return "INTEGRITY_CONFLICT"
            return None
        if kind == "RETIRE":
            if target_capability_signing_key_id == registry.current_issuance_key_id:
                if replacement_issuance_key_id is None:
                    return "CURRENT_KEY_REQUIRES_REPLACEMENT"
                if replacement_issuance_key_id == target_capability_signing_key_id:
                    return "KEY_NOT_ACTIVE"
            elif replacement_issuance_key_id is not None:
                return "INTEGRITY_CONFLICT"
            return None
        if kind == "REVOKE":
            if (
                target_capability_signing_key_id != registry.current_issuance_key_id
                and replacement_issuance_key_id is not None
            ):
                return "INTEGRITY_CONFLICT"
            if replacement_issuance_key_id == target_capability_signing_key_id:
                return "KEY_NOT_ACTIVE"
            return None
        raise AssertionError("unreachable capability key operation kind")

    def _result_issuance_key_id(
        self,
        *,
        current_key_id: str | None,
        kind: str,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
    ) -> str | None:
        if kind == "REGISTER":
            return current_key_id
        if kind == "SELECT":
            return target_capability_signing_key_id
        if kind == "RETIRE":
            return (
                replacement_issuance_key_id
                if target_capability_signing_key_id == current_key_id
                else current_key_id
            )
        if kind == "REVOKE":
            return (
                replacement_issuance_key_id
                if target_capability_signing_key_id == current_key_id
                else current_key_id
            )
        raise AssertionError("unreachable capability key operation kind")

    def _apply_successful_capability_key_operation(
        self,
        *,
        kind: str,
        capability_key_operation_id: str,
        target_capability_signing_key_id: str,
        replacement_issuance_key_id: str | None,
        register_public_verification_key: bytes | None,
        register_public_key_digest: str | None,
        register_private_signing_secret_ref: str | None,
        register_not_before_ms: int | None,
        result_revision: int | None,
        result_key: str | None,
        now: int,
    ) -> None:
        assert result_revision is not None
        if kind == "REGISTER":
            assert register_public_verification_key is not None
            assert register_public_key_digest is not None
            assert register_private_signing_secret_ref is not None
            assert register_not_before_ms is not None
            self.conn.execute(
                "INSERT INTO capability_signing_keys("
                "capability_signing_key_id, registration_operation_id, signature_algorithm, "
                "public_verification_key, public_key_digest, private_signing_secret_ref, "
                "registered_at_ms, not_before_ms, state) "
                "VALUES (?, ?, 'ED25519', ?, ?, ?, ?, ?, 'ACTIVE')",
                (
                    target_capability_signing_key_id,
                    capability_key_operation_id,
                    register_public_verification_key,
                    register_public_key_digest,
                    register_private_signing_secret_ref,
                    now,
                    register_not_before_ms,
                ),
            )
        elif kind == "RETIRE":
            self.conn.execute(
                "UPDATE capability_signing_keys SET state = 'RETIRED', retired_at_ms = ?, "
                "retirement_change_id = ?, retirement_principal_id = ("
                "SELECT authenticated_principal_id FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?), retirement_authorization_digest = ("
                "SELECT authorization_context_digest FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?) "
                "WHERE capability_signing_key_id = ? AND state = 'ACTIVE'",
                (
                    now,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    target_capability_signing_key_id,
                ),
            )
        elif kind == "REVOKE":
            self.conn.execute(
                "UPDATE capability_signing_keys SET state = 'REVOKED', revoked_at_ms = ?, "
                "revocation_change_id = ?, revocation_principal_id = ("
                "SELECT authenticated_principal_id FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?), revocation_authorization_digest = ("
                "SELECT authorization_context_digest FROM capability_key_operations "
                "WHERE capability_key_operation_id = ?) "
                "WHERE capability_signing_key_id = ? AND state IN ('ACTIVE', 'RETIRED')",
                (
                    now,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    capability_key_operation_id,
                    target_capability_signing_key_id,
                ),
            )
        self.conn.execute(
            "UPDATE capability_key_registry SET registry_revision = ?, "
            "current_issuance_key_id = ?, last_operation_id = ? WHERE registry_id = ?",
            (result_revision, result_key, capability_key_operation_id, CONTROLLER_ID),
        )

    def selected_issuance_key(self, *, now_ms: int | None = None) -> CapabilitySigningKey | None:
        registry = self.get_capability_key_registry()
        if registry.current_issuance_key_id is None:
            return None
        key = self.get_capability_signing_key(registry.current_issuance_key_id)
        now = _now_ms() if now_ms is None else now_ms
        if key is None or key.state != "ACTIVE" or key.not_before_ms > now:
            return None
        if capability_public_key_digest(key.public_verification_key) != key.public_key_digest:
            return None
        return key

    def controller_gate_permissions(self) -> ControllerGatePermissions:
        mode = self.get_controller_mode()
        registry = self.get_capability_key_registry()
        issuance_ready = self.selected_issuance_key() is not None
        current_mode = mode.mode
        new_admission = current_mode == "RUNNING" or (
            current_mode == "DISPATCH_PAUSED"
            and mode.dispatch_paused_intake_policy == "ALLOW_ADMISSION"
        )
        new_claims = current_mode in {"RUNNING", "INTAKE_PAUSED"} and issuance_ready
        return ControllerGatePermissions(
            mode_revision=mode.mode_revision,
            mode=current_mode,
            registry_revision=registry.registry_revision,
            current_issuance_key_id=registry.current_issuance_key_id,
            new_admission=new_admission,
            new_claims=new_claims,
            first_result_mutation=current_mode
            in {"RUNNING", "INTAKE_PAUSED", "DISPATCH_PAUSED", "DRAINING"},
            existing_result_replay=current_mode is not None,
            forge_reconciliation=current_mode
            in {"RUNNING", "INTAKE_PAUSED", "DISPATCH_PAUSED", "DRAINING"},
            management_operations=current_mode is not None,
        )

    def assert_offer_planning_permitted(self) -> None:
        gates = self.controller_gate_permissions()
        if not gates.new_claims:
            raise WorkflowGateClosedError(
                "offer planning requires an active issuance key and a dispatch-permitting mode"
            )

    def record_issued_capability_binding(
        self,
        *,
        capability_jti: str,
        claim_digest: str,
        immutable_assignment_digest: str,
        immutable_assignment: Any,
    ) -> IssuedCapabilityBinding:
        require_lowercase_uuid(capability_jti, field="capability_jti")
        _require_digest(claim_digest, field="claim_digest")
        _require_digest(immutable_assignment_digest, field="immutable_assignment_digest")
        assignment_json = _require_json_text(immutable_assignment)
        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM capability_issuance_audit WHERE capability_jti = ?",
                (capability_jti,),
            ).fetchone()
            if existing is not None:
                row = _row_to_issued_capability(existing)
                if (
                    row.claim_digest == claim_digest
                    and row.immutable_assignment_digest == immutable_assignment_digest
                    and row.immutable_assignment_json == assignment_json
                ):
                    return row
                raise IdempotencyConflictError("capability JTI was reused")
            self.assert_offer_planning_permitted()
            registry = self.get_capability_key_registry()
            key = self.selected_issuance_key()
            if key is None:
                raise WorkflowGateClosedError("selected issuance key is absent or invalid")
            now = _now_ms()
            self.conn.execute(
                "INSERT INTO capability_issuance_audit("
                "capability_jti, capability_signing_key_id, signature_algorithm, "
                "claim_digest, immutable_assignment_digest, immutable_assignment_json, "
                "capability_key_registry_revision, issued_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    capability_jti,
                    key.capability_signing_key_id,
                    key.signature_algorithm,
                    claim_digest,
                    immutable_assignment_digest,
                    assignment_json,
                    registry.registry_revision,
                    now,
                ),
            )
            row = self.conn.execute(
                "SELECT * FROM capability_issuance_audit WHERE capability_jti = ?",
                (capability_jti,),
            ).fetchone()
            assert row is not None
            return _row_to_issued_capability(row)

    def create_run(
        self,
        *,
        run_id: str,
        project_id: str,
        work_item_key: str,
        state: str,
        reducer_version: str = DEFAULT_REDUCER_VERSION,
        specification_generation: int = 1,
    ) -> None:
        require_lowercase_uuid(run_id, field="run_id")
        if reducer_version not in self._supported_reducer_versions:
            raise ReducerVersionError(f"unsupported reducer version {reducer_version!r}")
        if specification_generation < 0:
            raise ValueError("specification_generation must be nonnegative")
        enums.parse_enum("run.state", state)
        existing = self.conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if existing is not None:
            if (
                existing["project_id"] == project_id
                and existing["work_item_key"] == work_item_key
                and existing["reducer_version"] == reducer_version
            ):
                return
            raise IdempotencyConflictError("run id was reused with different content")
        active_existing = self.conn.execute(
            "SELECT run_id FROM runs WHERE project_id = ? AND work_item_key = ? "
            "AND terminal_outcome IS NULL",
            (project_id, work_item_key),
        ).fetchone()
        if active_existing is not None:
            raise IdempotencyConflictError("work item already has an active run")
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
        if prior_state != PRIOR_STATE_NONE:
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
                and row.admit_base_observation_id == admit_base_observation_id
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
            if (
                row.payload_digest == payload_digest
                and row.payload_json == payload_json
                and row.source_kind == source_kind
                and row.source_id == source_id
            ):
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
            if (
                row.payload_digest == payload_digest
                and row.payload_json == payload_json
                and row.fact_kind == fact_kind
                and row.fact_id == fact_id
            ):
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
            if (
                row.payload_json == payload_json
                and row.protocol_version == protocol_version
                and row.attempt_id == attempt_id
                and row.attempt_generation == attempt_generation
                and row.publication_id == publication_id
                and row.effect_generation == effect_generation
            ):
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
                and row.kind == kind
                and row.target_kind == target_kind
                and row.target_id == target_id
                and row.publication_id == publication_id
                and row.effect_generation == effect_generation
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

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        return None if row is None else _row_to_run(row)

    def get_transition_by_trigger(
        self, run_id: str, trigger_kind: str, trigger_id: str
    ) -> Transition | None:
        row = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? AND trigger_kind = ? AND trigger_id = ?",
            (run_id, trigger_kind, trigger_id),
        ).fetchone()
        return None if row is None else _row_to_transition(row)

    def list_transitions(self, run_id: str) -> list[Transition]:
        rows = self.conn.execute(
            "SELECT * FROM transitions WHERE run_id = ? ORDER BY transition_sequence",
            (run_id,),
        ).fetchall()
        return [_row_to_transition(row) for row in rows]

    def set_terminal_outcome(self, run_id: str, terminal_outcome: str) -> None:
        enums.parse_enum("run.terminal_outcome", terminal_outcome)
        now = _now_ms()
        cur = self.conn.execute(
            "UPDATE runs SET terminal_outcome = ?, updated_at_ms = ? WHERE run_id = ?",
            (terminal_outcome, now, run_id),
        )
        if cur.rowcount != 1:
            raise RunStoreError(f"run {run_id!r} was not updated")

    def get_revisioned_object(
        self, object_kind: str, object_id: str
    ) -> tuple[int, str, str] | None:
        row = self.conn.execute(
            "SELECT revision, payload_digest, payload_json FROM revisioned_objects "
            "WHERE object_kind = ? AND object_id = ?",
            (object_kind, object_id),
        ).fetchone()
        if row is None:
            return None
        return int(row["revision"]), str(row["payload_digest"]), str(row["payload_json"])

    def get_source_unique_record(
        self, source_kind: str, source_id: str
    ) -> SourceUniqueRecord | None:
        row = self.conn.execute(
            "SELECT * FROM source_unique_records WHERE source_kind = ? AND source_id = ?",
            (source_kind, source_id),
        ).fetchone()
        return None if row is None else _row_to_source_record(row)
