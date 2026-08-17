"""Scoped bearer-token auth for the monitor query listener.

Readers are configured in :class:`~orcest.monitor.config.MonitorConfig` with
a token and a set of scopes. Token comparison is timing-safe (sha256 digest
compared with ``hmac.compare_digest``), matching the pattern used by
``dashboard/server/auth.ts`` and ``orcest.monitor.ingest_app``.
"""

from __future__ import annotations

import hashlib
import hmac

from fastapi import HTTPException, Request

from orcest.monitor.config import MonitorConfig, Reader


def _token_matches(candidate: str, expected: str) -> bool:
    return hmac.compare_digest(
        hashlib.sha256(candidate.encode()).digest(),
        hashlib.sha256(expected.encode()).digest(),
    )


def _extract_bearer(authorization_header: str | None) -> str | None:
    if authorization_header is None or not authorization_header.startswith("Bearer "):
        return None
    return authorization_header[len("Bearer ") :]


def resolve_reader(cfg: MonitorConfig, authorization_header: str | None) -> Reader | None:
    """Resolve the :class:`Reader` matching ``authorization_header``.

    Returns ``None`` if the header is missing, malformed, or does not match
    any configured reader's token (timing-safe comparison against every
    configured reader, not a short-circuiting dict lookup).
    """
    token = _extract_bearer(authorization_header)
    if token is None:
        return None
    for reader in cfg.readers:
        if _token_matches(token, reader.token):
            return reader
    return None


def require_scope(scope: str):
    """Build a FastAPI dependency requiring ``scope`` on the resolved reader.

    Raises 401 if no/unknown token, 403 if the token's reader lacks ``scope``.
    """

    def _dependency(request: Request) -> Reader:
        cfg: MonitorConfig = request.app.state.cfg
        reader = resolve_reader(cfg, request.headers.get("Authorization"))
        if reader is None:
            raise HTTPException(status_code=401, detail="unauthorized")
        if scope not in reader.scopes:
            raise HTTPException(status_code=403, detail="forbidden")
        return reader

    return _dependency
