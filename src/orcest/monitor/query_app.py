"""Public read-only query API for the monitor service.

Every route except ``/api/v1/health`` and ``/api/v1/openapi.json`` requires a
bearer token configured in ``MonitorConfig.readers`` (see ``auth.py``): the
events/timeline/work/fleet routes require the ``events:read`` scope, and the
trace route requires the separate ``traces:read`` scope. The OpenAPI document
is the consumer contract, so it stays reachable without auth alongside the
health check.
The method gate (only GET/HEAD allowed) runs as pure ASGI middleware ahead
of FastAPI's routing/auth, so a non-GET/HEAD request is rejected with 405
before either the router or the auth dependency ever runs.

Each request opens (and closes) its own read-only SQLite connection --
sharing a single ``sqlite3.Connection`` across FastAPI's sync threadpool is
not thread-safe, even though SQLite itself enforces read-only via
``mode=ro`` + ``PRAGMA query_only=1`` (see ``db.open_ro``).
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import deque
from contextlib import closing
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from orcest.monitor import db
from orcest.monitor.auth import require_scope
from orcest.monitor.config import MonitorConfig
from orcest.shared.events import EVENT_TYPES

_TERMINAL_TYPES = (
    "net.orcest.task.completed",
    "net.orcest.task.failed",
    "net.orcest.task.killed",
    "net.orcest.task.reaped",
)

# Same allowlist as orcest.orchestrator.trace_archiver._TASK_ID_RE: task_id is
# used to build filesystem paths (index pointer lookup + trace file read), so
# anything outside this pattern is rejected before it ever touches the
# filesystem.
_TASK_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")


class _MethodGateMiddleware:
    """Pure ASGI middleware: reject non-GET/HEAD before routing/auth run."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["method"] not in ("GET", "HEAD"):
            response = JSONResponse({"detail": "method not allowed"}, status_code=405)
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)


def _envelope(row: sqlite3.Row) -> dict[str, Any]:
    """Parse the full CloudEvents envelope stored verbatim in the data column."""
    return json.loads(row["data"])


def create_query_app(cfg: MonitorConfig) -> FastAPI:
    app = FastAPI(openapi_url="/api/v1/openapi.json")
    app.state.cfg = cfg
    app.add_middleware(_MethodGateMiddleware)

    @app.api_route("/api/v1/health", methods=["GET", "HEAD"])
    def health() -> dict:
        return {"ok": True}

    router = APIRouter(dependencies=[Depends(require_scope("events:read"))])

    @router.api_route("/api/v1/events", methods=["GET", "HEAD"])
    def list_events(
        type: str | None = Query(default=None, json_schema_extra={"enum": sorted(EVENT_TYPES)}),
        repo: str | None = None,
        resource_id: int | None = None,
        since: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        clauses = []
        params: list[Any] = []
        if type is not None:
            clauses.append("type = ?")
            params.append(type)
        if repo is not None:
            clauses.append("repo = ?")
            params.append(repo)
        if resource_id is not None:
            clauses.append("resource_id = ?")
            params.append(resource_id)
        if since is not None:
            clauses.append("time >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT data FROM events {where} ORDER BY time DESC, rowid DESC LIMIT ?"
        params.append(limit)
        with closing(db.open_ro(cfg.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
        return {"events": [_envelope(r) for r in rows]}

    @router.api_route("/api/v1/tasks/{task_id}/timeline", methods=["GET", "HEAD"])
    def task_timeline(task_id: str) -> dict:
        with closing(db.open_ro(cfg.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT data FROM events WHERE subject = ? ORDER BY time ASC, rowid ASC",
                (task_id,),
            ).fetchall()
        return {"task_id": task_id, "events": [_envelope(r) for r in rows]}

    @router.api_route(
        "/api/v1/work/{owner}/{name}/{resource_type}/{resource_id}", methods=["GET", "HEAD"]
    )
    def work_detail(owner: str, name: str, resource_type: str, resource_id: int) -> dict:
        repo = f"{owner}/{name}"
        with closing(db.open_ro(cfg.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT data, attempt FROM events"
                " WHERE repo = ? AND resource_type = ? AND resource_id = ?"
                " ORDER BY time ASC, rowid ASC",
                (repo, resource_type, resource_id),
            ).fetchall()

        attempts: dict[int, dict[str, Any]] = {}
        for row in rows:
            env = _envelope(row)
            attempt = row["attempt"]
            bucket = attempts.setdefault(
                attempt,
                {
                    "attempt": attempt,
                    "task_ids": [],
                    "head_shas": [],
                    "first_time": env["time"],
                    "last_time": env["time"],
                    "last_type": env["type"],
                },
            )
            if env["subject"] not in bucket["task_ids"]:
                bucket["task_ids"].append(env["subject"])
            head_sha = env.get("data", {}).get("head_sha", "")
            if head_sha and head_sha not in bucket["head_shas"]:
                bucket["head_shas"].append(head_sha)
            bucket["last_time"] = env["time"]
            bucket["last_type"] = env["type"]

        ordered_attempts = [attempts[k] for k in sorted(attempts)]
        return {
            "work": {"repo": repo, "resource_type": resource_type, "resource_id": resource_id},
            "attempts": ordered_attempts,
        }

    @router.api_route("/api/v1/fleet", methods=["GET", "HEAD"])
    def fleet_status() -> dict:
        with closing(db.open_ro(cfg.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            pressure_row = conn.execute(
                "SELECT data FROM events WHERE type = 'net.orcest.fleet.pressure'"
                " ORDER BY time DESC, rowid DESC LIMIT 1"
            ).fetchone()
            kill_limit_row = conn.execute(
                "SELECT data FROM events WHERE type = 'net.orcest.fleet.kill_limit'"
                " ORDER BY time DESC, rowid DESC LIMIT 1"
            ).fetchone()
            placeholders = ",".join("?" * len(_TERMINAL_TYPES))
            # Terminal states are absorbing: a task_id never becomes active
            # again once a completed/failed/killed/reaped event exists for
            # it, regardless of arrival order (multiple producers -- worker
            # terminal events, the reaper, periodic task.activity -- can
            # emit for the same subject, and `time` is only second-resolution
            # RFC3339 so a late-arriving non-terminal event can share a
            # timestamp with an earlier terminal one). So classification is
            # "has >=1 task.% event AND zero terminal events" via NOT EXISTS,
            # which is immune to insertion order. Among the (necessarily
            # non-terminal, since the subject has no terminal event at all)
            # rows for a qualifying subject, rowid/time only pick which one
            # to show as last_type/time -- they never affect classification.
            active_rows = conn.execute(
                "SELECT subject, type, time FROM events e"
                " WHERE type LIKE 'net.orcest.task.%'"
                "   AND NOT EXISTS ("
                "     SELECT 1 FROM events t"
                f"     WHERE t.subject = e.subject AND t.type IN ({placeholders})"
                "   )"
                "   AND e.rowid = ("
                "     SELECT rowid FROM events"
                "     WHERE subject = e.subject AND type LIKE 'net.orcest.task.%'"
                "     ORDER BY time DESC, rowid DESC LIMIT 1"
                "   )"
                " ORDER BY time DESC, rowid DESC",
                _TERMINAL_TYPES,
            ).fetchall()

        return {
            "pressure": _envelope(pressure_row) if pressure_row else None,
            "kill_limit": _envelope(kill_limit_row) if kill_limit_row else None,
            "active_tasks": [
                {"task_id": r["subject"], "last_type": r["type"], "time": r["time"]}
                for r in active_rows
            ],
        }

    app.include_router(router)

    trace_router = APIRouter(dependencies=[Depends(require_scope("traces:read"))])

    @trace_router.api_route("/api/v1/tasks/{task_id:path}/trace", methods=["GET", "HEAD"])
    def task_trace(task_id: str, tail: int = Query(default=200, ge=1, le=5000)) -> dict:
        if not _TASK_ID_RE.match(task_id):
            raise HTTPException(status_code=400, detail="invalid task_id")
        archive_path = cfg.trace_archive_path
        if archive_path is None:
            raise HTTPException(status_code=404, detail="trace archive disabled")
        root = Path(archive_path)
        pointer_path = root / "index" / "by-task-id" / task_id[:2] / task_id
        try:
            pointer_content = pointer_path.read_text(encoding="utf-8").strip()
        except OSError:
            raise HTTPException(status_code=404, detail="unknown task") from None
        # Contract: trace_archiver._write_index_pointer (src/orcest/orchestrator/
        # trace_archiver.py:302-310) writes the trace file's *parent directory*
        # relative to the archive root, plus a trailing newline (already handled
        # by .strip() above) -- not the file path itself. Tolerate a full
        # "<dir>/<task_id>.jsonl" pointer too, in case a future archiver format
        # (or a hand-written test fixture) writes the file path directly.
        if pointer_content.endswith(".jsonl"):
            trace_file = Path(pointer_content)
        else:
            trace_file = Path(pointer_content) / f"{task_id}.jsonl"
        resolved = (root / trace_file).resolve()
        if not resolved.is_relative_to(root.resolve()):
            raise HTTPException(status_code=404, detail="unknown task")
        if not resolved.is_file():
            raise HTTPException(status_code=404, detail="unknown task")
        with open(resolved, encoding="utf-8") as f:
            lines = deque(f, maxlen=tail)
        return {"task_id": task_id, "lines": [line.rstrip("\n") for line in lines]}

    app.include_router(trace_router)
    return app
