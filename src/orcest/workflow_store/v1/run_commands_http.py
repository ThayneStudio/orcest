"""HTTPS transport for ``POST /api/v1/runs/{run_id}/commands``.

Mirrors :mod:`orcest.workflow_store.v1.registration_http`: TLS with full
client-certificate validation is required, and the transport identity comes
only from the verified certificate subject, never from the request body.
"""

from __future__ import annotations

import re
import ssl
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.protocol_registry import ERROR_PROTOCOL
from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.registration_http import principal_from_client_cert
from orcest.workflow_store.v1.run_commands import (
    MAX_REQUEST_BYTES,
    ServerRunCommandCatalog,
    TransportError,
    handle_run_command,
)

__all__ = [
    "RunCommandHttpsServer",
    "RunCommandTlsConfig",
    "handle_run_command_http",
    "match_run_command_path",
    "serve_run_command_https",
]

_PATH_RE = re.compile(r"^/api/v1/runs/(?P<run_id>[^/]+)/commands$")
_RUN_STORE_LOCK = threading.Lock()


def match_run_command_path(path: str) -> str | None:
    """The path-segment ``run_id`` if ``path`` is the commands route, else
    ``None``. Schema validation of the segment (lowercase UUID) happens
    downstream in :func:`orcest.workflow_store.v1.run_commands.parse_run_command_request`
    so a malformed id still produces a typed ``422``, not a bare ``404``.
    """
    match = _PATH_RE.match(path)
    return None if match is None else match.group("run_id")


def _error_body(code: str, message: str, *, retryable: bool = False) -> bytes:
    return canonical_json_text(
        {
            "protocol": ERROR_PROTOCOL,
            "code": code,
            "retryable": retryable,
            "message": message,
        }
    ).encode("utf-8")


def handle_run_command_http(
    *,
    method: str,
    path: str,
    headers: Mapping[str, str],
    body: bytes,
    principal_id: str | None,
    run_store: RunStore,
    catalog: ServerRunCommandCatalog,
) -> tuple[int, dict[str, str], bytes]:
    """Dispatch one already-TLS-authenticated HTTPS request."""
    del headers
    run_id = match_run_command_path(path)
    if run_id is None:
        return (
            404,
            {"Content-Type": "application/json"},
            _error_body("MALFORMED", "unknown path"),
        )
    if method != "POST":
        return (
            405,
            {"Content-Type": "application/json", "Allow": "POST"},
            _error_body("MALFORMED", "method not allowed"),
        )
    try:
        with _RUN_STORE_LOCK:
            result = handle_run_command(
                run_store,
                catalog=catalog,
                raw_body=body,
                path_run_id=run_id,
                authenticated_principal_id=principal_id,
            )
    except TransportError as exc:
        return (
            exc.http_status,
            {"Content-Type": "application/json"},
            exc.body_json().encode("utf-8"),
        )
    return (
        result.http_status,
        {"Content-Type": "application/json"},
        result.body_json.encode("utf-8"),
    )


@dataclass(frozen=True, slots=True)
class RunCommandTlsConfig:
    certfile: str
    keyfile: str
    cafile: str
    require_client_cert: bool = True


class RunCommandRequestHandler(BaseHTTPRequestHandler):
    server_version = "orcest-run-commands/1"

    def do_POST(self) -> None:  # noqa: N802
        header_value = self.headers.get("Content-Length")
        if header_value is None:
            self._reject(400, "MALFORMED", "Content-Length header is required")
            return
        try:
            length = int(header_value)
        except ValueError:
            length = -1
        if length < 0:
            self._reject(400, "MALFORMED", "Content-Length must be a non-negative integer")
            return
        if length > MAX_REQUEST_BYTES:
            self._reject(422, "SCHEMA_INVALID", "request exceeds the v1 size bound")
            return
        body = self.rfile.read(length) if length > 0 else b""
        cert = self.connection.getpeercert() if hasattr(self.connection, "getpeercert") else None
        principal_id = principal_from_client_cert(cert)
        status, headers, payload = handle_run_command_http(
            method="POST",
            path=self.path.split("?", 1)[0],
            headers=dict(self.headers.items()),
            body=body,
            principal_id=principal_id,
            run_store=self.server.run_store,  # type: ignore[attr-defined]
            catalog=self.server.catalog,  # type: ignore[attr-defined]
        )
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _reject(self, status: int, code: str, message: str) -> None:
        payload = _error_body(code, message)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


class RunCommandHttpsServer(ThreadingHTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        *,
        run_store: RunStore,
        catalog: ServerRunCommandCatalog,
        tls: RunCommandTlsConfig,
    ) -> None:
        super().__init__(address, RunCommandRequestHandler)
        self.run_store = run_store
        self.catalog = catalog
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED if tls.require_client_cert else ssl.CERT_OPTIONAL
        context.load_cert_chain(tls.certfile, tls.keyfile)
        context.load_verify_locations(tls.cafile)
        self.socket = context.wrap_socket(self.socket, server_side=True)


def serve_run_command_https(
    address: tuple[str, int],
    *,
    run_store: RunStore,
    catalog: ServerRunCommandCatalog,
    tls: RunCommandTlsConfig,
    ready: Callable[[str, int], None] | None = None,
) -> RunCommandHttpsServer:
    server = RunCommandHttpsServer(address, run_store=run_store, catalog=catalog, tls=tls)
    host, port = server.server_address[:2]
    if ready is not None:
        ready(str(host), int(port))
    return server
