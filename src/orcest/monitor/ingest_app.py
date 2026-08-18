"""Private ingest listener for the monitor service.

Workers/orchestrators (via the relay) POST batches of event envelopes here.
The listener is not exposed publicly -- it trusts only holders of the single
shared ``write_token`` (compared with a timing-safe hash comparison, matching
the pattern used by ``dashboard/server/auth.ts``).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from orcest.monitor import db
from orcest.monitor.config import MonitorConfig


def _token_matches(candidate: str, expected: str) -> bool:
    return hmac.compare_digest(
        hashlib.sha256(candidate.encode()).digest(),
        hashlib.sha256(expected.encode()).digest(),
    )


def _extract_bearer(request: Request) -> str | None:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    return header[len("Bearer ") :]


def create_ingest_app(cfg: MonitorConfig) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        conn: sqlite3.Connection = app.state.conn
        try:
            yield
        finally:
            conn.close()

    app = FastAPI(lifespan=lifespan)
    app.state.cfg = cfg
    app.state.conn = db.open_rw(cfg.db_path)

    @app.post("/ingest/v1/events")
    async def ingest_events(request: Request) -> JSONResponse:
        token = _extract_bearer(request)
        if token is None or not _token_matches(token, cfg.write_token):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)

        try:
            body = await request.json()
        except json.JSONDecodeError:
            return JSONResponse({"detail": "malformed JSON body"}, status_code=400)

        if not isinstance(body, dict):
            return JSONResponse({"detail": "request body must be a JSON object"}, status_code=400)

        events = body.get("events", [])
        if not isinstance(events, list):
            return JSONResponse({"detail": "'events' must be a list"}, status_code=400)

        accepted = db.insert_events(app.state.conn, events)
        skipped = len(events) - accepted
        return JSONResponse({"accepted": accepted, "skipped": skipped})

    return app
