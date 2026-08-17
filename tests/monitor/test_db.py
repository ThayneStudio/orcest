import sqlite3

import pytest

from orcest.monitor import db
from orcest.shared.events import make_event


def _env(task_id="t1", type_="net.orcest.task.started"):
    return make_event(
        type_, source_project="p", task_id=task_id, repo="o/r",
        resource_type="pr", resource_id=5, attempt=1,
    )


def test_insert_is_idempotent(tmp_path):
    conn = db.open_rw(str(tmp_path / "m.db"))
    e = _env()
    assert db.insert_events(conn, [e]) == 1
    assert db.insert_events(conn, [e]) == 0  # duplicate (source,id) ignored
    rows = conn.execute("SELECT subject, repo, resource_id FROM events").fetchall()
    assert rows == [("t1", "o/r", 5)]


def test_malformed_envelopes_skipped(tmp_path):
    conn = db.open_rw(str(tmp_path / "m.db"))
    bad_type = _env()
    bad_type["type"] = "net.orcest.task.exploded"
    missing = {"id": "x", "source": "urn:orcest:p"}
    assert db.insert_events(conn, [bad_type, missing, _env("t2")]) == 1


def test_ro_connection_rejects_writes(tmp_path):
    path = str(tmp_path / "m.db")
    db.open_rw(path).close()
    ro = db.open_ro(path)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute(
            "INSERT INTO events(source,id,type,subject,time,data,ingested_at)"
            " VALUES('a','b','c','d','e','f','g')"
        )
