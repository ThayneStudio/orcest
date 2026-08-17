"""SQLite event store for the monitor service.

The ingest listener writes through :func:`open_rw`; the query listener reads
through :func:`open_ro` (``mode=ro`` URI + ``PRAGMA query_only=1``, enforced
by SQLite itself so a coding mistake in the query app cannot mutate state).

Idempotency key is ``(source, id)`` per the CloudEvents envelope contract in
``orcest.shared.events`` -- ``insert_events`` uses ``INSERT OR IGNORE`` so
re-delivered events (at-least-once spool consumption) are silently deduped.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from orcest.shared.events import EVENT_TYPES

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  source TEXT NOT NULL,
  id TEXT NOT NULL,
  type TEXT NOT NULL,
  subject TEXT NOT NULL,           -- task_id
  time TEXT NOT NULL,              -- RFC3339
  repo TEXT NOT NULL DEFAULT '',
  resource_type TEXT NOT NULL DEFAULT '',
  resource_id INTEGER NOT NULL DEFAULT 0,
  attempt INTEGER NOT NULL DEFAULT 0,
  data TEXT NOT NULL,              -- full envelope JSON, verbatim
  ingested_at TEXT NOT NULL,
  PRIMARY KEY (source, id)
);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(type, time);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(subject, time);
CREATE INDEX IF NOT EXISTS idx_events_work ON events(repo, resource_type, resource_id, time);
"""

_REQUIRED = ("id", "source", "type", "subject", "time", "data")


def open_rw(db_path: str) -> sqlite3.Connection:
    """Open (creating if needed) the event store for read/write access.

    Enables WAL journaling and ensures the schema exists.
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def open_ro(db_path: str) -> sqlite3.Connection:
    """Open the event store read-only. Writes raise sqlite3.OperationalError."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
    conn.execute("PRAGMA query_only=1")
    return conn


def insert_events(conn: sqlite3.Connection, envelopes: list[dict]) -> int:
    """Insert well-formed envelopes, deduping on (source, id).

    Malformed envelopes (missing a required CloudEvents attribute, or a
    ``type`` outside the locked v1 taxonomy) are silently skipped. Returns
    the number of rows actually inserted (excludes both malformed envelopes
    and duplicates ignored by the unique constraint).
    """
    accepted = 0
    for env in envelopes:
        if not all(k in env for k in _REQUIRED) or env["type"] not in EVENT_TYPES:
            continue
        work = env["data"].get("work", {}) if isinstance(env["data"], dict) else {}
        cur = conn.execute(
            "INSERT OR IGNORE INTO events"
            " (source,id,type,subject,time,repo,resource_type,resource_id,attempt,data,ingested_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                env["source"], env["id"], env["type"], env["subject"], env["time"],
                work.get("repo", ""), work.get("resource_type", ""),
                int(work.get("resource_id", 0) or 0),
                int(env["data"].get("attempt", 0) or 0) if isinstance(env["data"], dict) else 0,
                json.dumps(env), datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        )
        accepted += cur.rowcount
    conn.commit()
    return accepted
