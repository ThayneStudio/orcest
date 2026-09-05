"""Authenticated ``POST /api/v1/runs/{run_id}/commands`` composition
(workflow-lifecycle.md "Authenticated Run commands").

Validates the closed management-command request envelope, applies
server-owned RBAC for the exact Project/Run/command/resolution-kind before
ever touching
:class:`~orcest.workflow_store.store.RunStore`, and maps every failure to the
closed HTTP surface: ``401``/``403`` for authentication/authorization,
``404`` for an unknown Run, ``409`` for a stale fence or a reused
``command_id`` with different content, and ``422`` for a malformed or
unsupported body. Every rejection is durably audited via
``RunStore.record_management_command_denial`` -- unauthorized and unknown
command kinds fail closed and leave no Transition or Human Resolution behind.

Only the closed ``CANCEL`` and ``RESOLVE_HUMAN_BOUNDARY`` command kinds exist
in v1. Retry/resume are reducer outcomes, policy amendment is the Forge
Observation path, and ownership/policy decisions are reason-bound Human
Resolution effects -- none of those are generic commands this endpoint can
synthesize.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.identity import is_lowercase_uuid
from orcest.workflow_contract.v1.protocol_registry import (
    ERROR_PROTOCOL,
    MANAGEMENT_COMMAND_PROTOCOL,
)
from orcest.workflow_store.store import (
    CasMismatchError,
    IdempotencyConflictError,
    ManagementCommandResult,
    RunStore,
    RunStoreError,
)

__all__ = [
    "MAX_PAYLOAD_ENTRIES",
    "MAX_REQUEST_BYTES",
    "MAX_STRING_BYTES",
    "RunCommandHttpResult",
    "RunCommandPrincipalRecord",
    "ServerRunCommandCatalog",
    "TransportError",
    "handle_run_command",
    "parse_run_command_request",
    "submit_run_command",
]

MAX_REQUEST_BYTES = 16 * 1024
MAX_STRING_BYTES = 256
MAX_PAYLOAD_ENTRIES = 16

_WILDCARD_PROJECT = "*"


class TransportError(Exception):
    """Auth/schema/fence failure that must not create an accepted Management
    Command. Every raise site here is also durably audited by the caller
    (see :func:`handle_run_command`) so unauthorized and unknown-kind
    attempts fail closed without leaving the acceptance path silent.
    """

    def __init__(self, http_status: int, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.retryable = retryable

    def body(self) -> dict[str, Any]:
        return {
            "protocol": ERROR_PROTOCOL,
            "code": self.code,
            "retryable": self.retryable,
            "message": self.message,
        }

    def body_json(self) -> str:
        return canonical_json_text(self.body())


@dataclass(frozen=True, slots=True)
class RunCommandPrincipalRecord:
    """One server-issued management principal's exact command authority.

    Companion management-plane principal issuance/role administration is out
    of scope for this leaf (workflow-lifecycle.md "Authenticated Run
    commands"); this catalog is the narrow, server-owned RBAC surface it
    must define before any request reaches :class:`RunStore`.
    """

    principal_id: str
    authorized_command_kinds: frozenset[str]
    authorized_project_ids: frozenset[str] = frozenset({_WILDCARD_PROJECT})
    authorized_resolution_kinds: frozenset[str] = frozenset()

    def authorizes_project(self, project_id: str) -> bool:
        return (
            _WILDCARD_PROJECT in self.authorized_project_ids
            or project_id in self.authorized_project_ids
        )


@dataclass(frozen=True, slots=True)
class ServerRunCommandCatalog:
    """Server-owned management principals. Never sourced from ``.orcest``, a
    worker, or a forge comment."""

    principals: Mapping[str, RunCommandPrincipalRecord]


@dataclass(frozen=True, slots=True)
class RunCommandHttpResult:
    http_status: int
    body_json: str
    result: ManagementCommandResult | None = None

    @property
    def replayed(self) -> bool:
        return self.result.replayed if self.result is not None else False


def _bounded_str(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise TransportError(422, "SCHEMA_INVALID", f"{field} must be a non-empty string")
    if len(value.encode("utf-8")) > MAX_STRING_BYTES:
        raise TransportError(422, "SCHEMA_INVALID", f"{field} exceeds the v1 size bound")
    return value


def parse_run_command_request(raw: bytes, *, path_run_id: str) -> dict[str, Any]:
    """Validate the closed envelope shape; never touches authorization or
    the store. Raises :class:`TransportError` on any malformed input."""
    if not is_lowercase_uuid(path_run_id):
        raise TransportError(422, "SCHEMA_INVALID", "path run_id must be a lowercase UUID")
    if len(raw) > MAX_REQUEST_BYTES:
        raise TransportError(422, "SCHEMA_INVALID", "request exceeds the v1 size bound")
    if not raw:
        raise TransportError(400, "MALFORMED", "request body is required")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TransportError(400, "MALFORMED", "request body must be UTF-8 JSON") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise TransportError(400, "MALFORMED", "request body must be UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise TransportError(400, "MALFORMED", "request body must be a JSON object")
    if payload.get("protocol") != MANAGEMENT_COMMAND_PROTOCOL:
        raise TransportError(422, "SCHEMA_INVALID", "unsupported command protocol")

    command_id = payload.get("command_id")
    if not isinstance(command_id, str) or not is_lowercase_uuid(command_id):
        raise TransportError(422, "SCHEMA_INVALID", "command_id must be a lowercase UUID")

    body_run_id = payload.get("run_id")
    if not isinstance(body_run_id, str) or not is_lowercase_uuid(body_run_id):
        raise TransportError(422, "SCHEMA_INVALID", "run_id must be a lowercase UUID")
    if body_run_id != path_run_id:
        raise TransportError(422, "SCHEMA_INVALID", "path and body run_id must match")

    kind = payload.get("kind")
    try:
        enums.parse_enum("management_command.kind", kind)
    except ValueError as exc:
        raise TransportError(422, "SCHEMA_INVALID", f"unsupported command kind {kind!r}") from exc

    expected = payload.get("expected_last_transition_sequence")
    if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
        raise TransportError(
            422,
            "SCHEMA_INVALID",
            "expected_last_transition_sequence must be a non-negative integer",
        )

    command_payload = payload.get("payload")
    if not isinstance(command_payload, dict):
        raise TransportError(422, "SCHEMA_INVALID", "payload must be a JSON object")
    if len(command_payload) > MAX_PAYLOAD_ENTRIES:
        raise TransportError(422, "SCHEMA_INVALID", "payload exceeds the v1 size bound")

    resolution_kind: str | None = None
    if kind == "RESOLVE_HUMAN_BOUNDARY":
        human_boundary_id = command_payload.get("human_boundary_id")
        if not isinstance(human_boundary_id, str) or not is_lowercase_uuid(human_boundary_id):
            raise TransportError(
                422, "SCHEMA_INVALID", "payload.human_boundary_id must be a lowercase UUID"
            )
        resolution_kind_value = command_payload.get("resolution_kind")
        resolution_kind = _bounded_str(resolution_kind_value, field="payload.resolution_kind")
        resolution = command_payload.get("resolution")
        if not isinstance(resolution, dict):
            raise TransportError(422, "SCHEMA_INVALID", "payload.resolution must be a JSON object")
        if set(command_payload) != {"human_boundary_id", "resolution_kind", "resolution"}:
            raise TransportError(
                422,
                "SCHEMA_INVALID",
                "RESOLVE_HUMAN_BOUNDARY payload accepts only human_boundary_id, "
                "resolution_kind, and resolution",
            )
    elif command_payload:
        raise TransportError(422, "SCHEMA_INVALID", "CANCEL does not accept a payload")

    return {
        "command_id": command_id,
        "run_id": body_run_id,
        "kind": kind,
        "expected_last_transition_sequence": expected,
        "payload": command_payload,
        "resolution_kind": resolution_kind,
    }


def _require_principal(
    catalog: ServerRunCommandCatalog, principal_id: str | None
) -> RunCommandPrincipalRecord:
    if not principal_id:
        raise TransportError(401, "AUTH_INVALID", "transport authentication is required")
    principal = catalog.principals.get(principal_id)
    if principal is None:
        raise TransportError(401, "AUTH_INVALID", "unknown transport principal")
    return principal


def _authorize(
    principal: RunCommandPrincipalRecord,
    *,
    project_id: str,
    kind: str,
    resolution_kind: str | None,
) -> None:
    _authorize_command_shape(principal, kind=kind, resolution_kind=resolution_kind)
    if not principal.authorizes_project(project_id):
        raise TransportError(404, "RUN_NOT_FOUND", "run was not found")


def _authorize_command_shape(
    principal: RunCommandPrincipalRecord,
    *,
    kind: str,
    resolution_kind: str | None,
) -> None:
    if kind not in principal.authorized_command_kinds:
        raise TransportError(403, "CAPABILITY_DENIED", f"principal lacks {kind} authority")
    if kind == "RESOLVE_HUMAN_BOUNDARY":
        assert resolution_kind is not None
        if resolution_kind not in principal.authorized_resolution_kinds:
            raise TransportError(
                403,
                "CAPABILITY_DENIED",
                f"principal cannot accept {resolution_kind} resolutions",
            )


def _authorization_context_digest(
    *, principal_id: str, authorized_command_kinds: Any, authorized_project_ids: Any
) -> str:
    from orcest.workflow_contract.v1.digest import request_digest

    return request_digest(
        {
            "principal_id": principal_id,
            "authorized_command_kinds": sorted(authorized_command_kinds),
            "authorized_project_ids": sorted(authorized_project_ids),
        }
    )


def submit_run_command(
    run_store: RunStore,
    *,
    catalog: ServerRunCommandCatalog,
    raw_body: bytes,
    path_run_id: str,
    authenticated_principal_id: str | None,
) -> RunCommandHttpResult:
    """Run one authenticated command request to a terminal public response.

    Every :class:`TransportError` raised here is deliberately unaudited by
    this function -- :func:`handle_run_command` is the single place that
    turns a raised ``TransportError`` into a durable denial row, so callers
    that only need the accept path (e.g. tests) are not forced to also stand
    up denial storage.
    """
    request = parse_run_command_request(raw_body, path_run_id=path_run_id)
    principal = _require_principal(catalog, authenticated_principal_id)
    _authorize_command_shape(
        principal,
        kind=request["kind"],
        resolution_kind=request["resolution_kind"],
    )

    run = run_store.get_run(request["run_id"])
    if run is None:
        raise TransportError(404, "RUN_NOT_FOUND", "run was not found")

    _authorize(
        principal,
        project_id=run.project_id,
        kind=request["kind"],
        resolution_kind=request["resolution_kind"],
    )
    authz_digest = _authorization_context_digest(
        principal_id=principal.principal_id,
        authorized_command_kinds=principal.authorized_command_kinds,
        authorized_project_ids=principal.authorized_project_ids,
    )

    try:
        result = run_store.submit_management_command(
            command_id=request["command_id"],
            run_id=request["run_id"],
            kind=request["kind"],
            expected_last_transition_sequence=request["expected_last_transition_sequence"],
            payload=request["payload"],
            authenticated_principal_id=principal.principal_id,
            authorization_context_digest=authz_digest,
        )
    except IdempotencyConflictError as exc:
        raise TransportError(409, "IDEMPOTENCY_CONFLICT", str(exc)) from exc
    except CasMismatchError as exc:
        raise TransportError(409, "STALE_RUN", str(exc)) from exc
    except RunStoreError as exc:
        raise TransportError(409, "STALE_RUN", str(exc)) from exc
    except ValueError as exc:
        raise TransportError(422, "SCHEMA_INVALID", str(exc)) from exc

    return RunCommandHttpResult(
        http_status=result.response_http_status,
        body_json=result.public_response_json(),
        result=result,
    )


def handle_run_command(
    run_store: RunStore,
    *,
    catalog: ServerRunCommandCatalog,
    raw_body: bytes,
    path_run_id: str,
    authenticated_principal_id: str | None,
) -> RunCommandHttpResult:
    """:func:`submit_run_command`, durably auditing every rejection so
    unauthorized operations and unknown command kinds fail closed and leave
    an audit trail (see ``management_command_denials``).
    """
    try:
        return submit_run_command(
            run_store,
            catalog=catalog,
            raw_body=raw_body,
            path_run_id=path_run_id,
            authenticated_principal_id=authenticated_principal_id,
        )
    except TransportError as exc:
        parsed: dict[str, Any] | None
        try:
            parsed = parse_run_command_request(raw_body, path_run_id=path_run_id)
        except TransportError:
            parsed = None
        run_store.record_management_command_denial(
            code=exc.code,
            message=exc.message,
            http_status=exc.http_status,
            run_id=path_run_id if is_lowercase_uuid(path_run_id) else None,
            command_id=parsed["command_id"] if parsed is not None else None,
            kind=parsed["kind"] if parsed is not None else None,
            authenticated_principal_id=authenticated_principal_id,
        )
        raise
