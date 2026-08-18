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


def test_poison_envelopes_skipped_not_raised(tmp_path):
    """A batch containing envelopes that pass the shape check but fail at
    insert time (NULL in a NOT NULL column, non-numeric resource_id) must
    not raise -- the whole batch would otherwise 500 the ingest endpoint and
    the relay would retry the same poison batch forever. Only the valid
    envelope is accepted; poison envelopes are skipped and the rest of the
    batch (including envelopes ordered after the poison ones) is retained.
    """
    conn = db.open_rw(str(tmp_path / "m.db"))

    poison_null_subject = _env("t-null-subject")
    poison_null_subject["subject"] = None  # NOT NULL column -> IntegrityError

    poison_bad_resource_id = _env("t-bad-resource-id")
    poison_bad_resource_id["data"]["work"]["resource_id"] = "abc"  # int() -> ValueError

    valid = _env("t-valid")

    accepted = db.insert_events(
        conn, [poison_null_subject, poison_bad_resource_id, valid]
    )

    assert accepted == 1
    rows = conn.execute("SELECT subject FROM events").fetchall()
    assert rows == [("t-valid",)]


def test_non_dict_envelopes_skipped_not_raised(tmp_path):
    """A batch containing valid-JSON-but-non-dict envelopes (``null``,
    ``42``, ``true``) must not raise -- membership checks like ``"id" in
    env`` blow up with TypeError on a non-dict, which would otherwise
    propagate past insert_events, 500 the ingest endpoint, and wedge the
    relay's retry loop on the same batch forever.
    """
    conn = db.open_rw(str(tmp_path / "m.db"))
    accepted = db.insert_events(conn, [None, 42, True, _env("t-valid")])
    assert accepted == 1
    rows = conn.execute("SELECT subject FROM events").fetchall()
    assert rows == [("t-valid",)]


def test_ro_connection_rejects_writes(tmp_path):
    path = str(tmp_path / "m.db")
    db.open_rw(path).close()
    ro = db.open_ro(path)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute(
            "INSERT INTO events(source,id,type,subject,time,data,ingested_at)"
            " VALUES('a','b','c','d','e','f','g')"
        )
