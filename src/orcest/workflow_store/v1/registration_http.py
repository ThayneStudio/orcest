"""HTTPS transport for ``POST /api/v1/projects/registrations``.

The listener requires TLS with full certificate validation. Plaintext HTTP
and disabling certificate checks are rejected even on loopback.
"""

from __future__ import annotations

import ssl
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.protocol_registry import ERROR_PROTOCOL
from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.project_registration import (
    MAX_REQUEST_BYTES,
    ForgeResolver,
    ServerRegistrationCatalog,
    TransportError,
    register_or_revalidate_project,
)

REGISTRATIONS_PATH = "/api/v1/projects/registrations"
_RUN_STORE_LOCK = threading.Lock()


def _error_body(http_status: int, code: str, message: str, *, retryable: bool = False) -> bytes:
    del http_status
    return canonical_json_text(
        {
            "protocol": ERROR_PROTOCOL,
            "code": code,
            "retryable": retryable,
            "message": message,
        }
    ).encode("utf-8")


def handle_registration_http(
    *,
    method: str,
    path: str,
    headers: Mapping[str, str],
    body: bytes,
    principal_id: str | None,
    run_store: RunStore,
    catalog: ServerRegistrationCatalog,
    resolver: ForgeResolver,
) -> tuple[int, dict[str, str], bytes]:
    """Dispatch one already-authenticated HTTPS request.

    ``principal_id`` is the transport identity derived from the validated
    client certificate; this function never accepts a principal from the body.
    """
    if path != REGISTRATIONS_PATH:
        return (
            404,
            {"Content-Type": "application/json"},
            _error_body(404, "MALFORMED", "unknown path"),
        )
    if method != "POST":
        return (
            405,
            {"Content-Type": "application/json", "Allow": "POST"},
            _error_body(405, "MALFORMED", "method not allowed"),
        )
    header_key = None
    for name, value in headers.items():
        if name.lower() == "idempotency-key":
            header_key = value
            break
    try:
        with _RUN_STORE_LOCK:
            result = register_or_revalidate_project(
                run_store,
                catalog=catalog,
                resolver=resolver,
                raw_body=body,
                idempotency_key_header=header_key,
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


def principal_from_client_cert(cert: dict[str, Any] | None) -> str | None:
    """Map a verified client certificate subject CN to a principal id."""
    if not cert:
        return None
    subject = cert.get("subject")
    if not isinstance(subject, tuple):
        return None
    for relative in subject:
        for key, value in relative:
            if key == "commonName" and isinstance(value, str) and value:
                return value
    return None


@dataclass(frozen=True, slots=True)
class RegistrationTlsConfig:
    certfile: str
    keyfile: str
    cafile: str
    require_client_cert: bool = True


class RegistrationRequestHandler(BaseHTTPRequestHandler):
    server_version = "orcest-project-registration/1"

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
        status, headers, payload = handle_registration_http(
            method="POST",
            path=self.path.split("?", 1)[0],
            headers=dict(self.headers.items()),
            body=body,
            principal_id=principal_id,
            run_store=self.server.run_store,  # type: ignore[attr-defined]
            catalog=self.server.catalog,  # type: ignore[attr-defined]
            resolver=self.server.resolver,  # type: ignore[attr-defined]
        )
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _reject(self, status: int, code: str, message: str) -> None:
        payload = _error_body(status, code, message)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


class RegistrationHttpsServer(ThreadingHTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        *,
        run_store: RunStore,
        catalog: ServerRegistrationCatalog,
        resolver: ForgeResolver,
        tls: RegistrationTlsConfig,
    ) -> None:
        super().__init__(address, RegistrationRequestHandler)
        self.run_store = run_store
        self.catalog = catalog
        self.resolver = resolver
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED if tls.require_client_cert else ssl.CERT_OPTIONAL
        context.load_cert_chain(tls.certfile, tls.keyfile)
        context.load_verify_locations(tls.cafile)
        self.socket = context.wrap_socket(self.socket, server_side=True)


def serve_registration_https(
    address: tuple[str, int],
    *,
    run_store: RunStore,
    catalog: ServerRegistrationCatalog,
    resolver: ForgeResolver,
    tls: RegistrationTlsConfig,
    ready: Callable[[str, int], None] | None = None,
) -> RegistrationHttpsServer:
    server = RegistrationHttpsServer(
        address, run_store=run_store, catalog=catalog, resolver=resolver, tls=tls
    )
    host, port = server.server_address[:2]
    if ready is not None:
        ready(str(host), int(port))
    return server
