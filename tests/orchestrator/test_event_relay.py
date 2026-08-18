import json
import logging

from orcest.orchestrator.event_relay import EventRelay
from orcest.shared.events import EVENTS_STREAM, make_event


class _FakeRedis:
    def __init__(self, key_prefix="orcest"):
        self.entries = []  # list of (id, fields)
        self.kv = {}
        self.key_prefix = key_prefix

    def xadd_capped(self, stream, fields, maxlen):
        eid = f"{len(self.entries)+1}-0"
        self.entries.append((eid, fields))
        return eid

    def xread_after(self, stream, last_id, count):
        return [(i, f) for i, f in self.entries if _id_gt(i, last_id)][:count]

    def get(self, key):
        return self.kv.get(key)

    def set_value(self, key, value):
        self.kv[key] = value


def _id_gt(a, b):
    pa = tuple(int(x) for x in a.split("-"))
    pb = tuple(int(x) for x in b.split("-"))
    return pa > pb


def _spool(r, n):
    for i in range(n):
        env = make_event(
            "net.orcest.task.started", source_project="p", task_id=f"t{i}",
            repo="o/r", resource_type="pr", resource_id=i, attempt=0,
        )
        r.xadd_capped(EVENTS_STREAM, {"envelope": json.dumps(env)}, 100)


def test_pass_posts_batch_and_advances_cursor(monkeypatch):
    r = _FakeRedis()
    _spool(r, 3)
    posted = []

    def fake_post(url, json=None, headers=None, timeout=None):
        posted.append(json)

        class R:
            status_code = 200

        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fake_post)
    relay._pass_once()
    assert len(posted[0]["events"]) == 3
    assert r.kv["event_relay:cursor"] == "3-0"
    relay._pass_once()
    assert len(posted) == 1  # nothing new -> no POST


def test_cursor_holds_on_http_failure(monkeypatch):
    r = _FakeRedis()
    _spool(r, 2)

    def fail_post(url, json=None, headers=None, timeout=None):
        class R:
            status_code = 503

        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fail_post)
    relay._pass_once()
    assert r.kv.get("event_relay:cursor") is None  # not advanced


def test_malformed_entry_skipped(monkeypatch):
    r = _FakeRedis()
    r.xadd_capped(EVENTS_STREAM, {"envelope": "not json"}, 100)
    _spool(r, 1)
    posted = []

    def fake_post(url, json=None, headers=None, timeout=None):
        posted.append(json)

        class R:
            status_code = 200

        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fake_post)
    relay._pass_once()
    assert len(posted[0]["events"]) == 1
    assert r.kv["event_relay:cursor"] == "2-0"


def test_malformed_entry_warning_not_repeated_across_retry_passes(monkeypatch, caplog):
    """A malformed entry stuck behind a failing POST is warned about once, not per pass."""
    r = _FakeRedis()
    r.xadd_capped(EVENTS_STREAM, {"envelope": "not json"}, 100)
    _spool(r, 1)  # one good entry alongside the malformed one

    def fail_post(url, json=None, headers=None, timeout=None):
        class R:
            status_code = 503

        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fail_post)

    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            relay._pass_once()  # POST keeps failing -> cursor never advances

    malformed_warnings = [
        rec for rec in caplog.records if "Skipping malformed spool entry" in rec.message
    ]
    assert len(malformed_warnings) == 1
    assert r.kv.get("event_relay:cursor") is None  # cursor genuinely never advanced


def test_malformed_entry_seen_set_prunes_once_cursor_advances(monkeypatch):
    """Once the cursor advances past a malformed entry, it can be warned again later."""
    r = _FakeRedis()
    r.xadd_capped(EVENTS_STREAM, {"envelope": "not json"}, 100)  # id "1-0", malformed only

    def fake_post(url, json=None, headers=None, timeout=None):
        class R:
            status_code = 200

        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fake_post)

    relay._pass_once()  # all-malformed batch: cursor advances past id "1-0"
    assert r.kv["event_relay:cursor"] == "1-0"
    assert relay._warned_malformed_ids == set()


def test_start_warns_on_project_prefix_mismatch(monkeypatch, caplog):
    r = _FakeRedis(key_prefix="orcest")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", lambda *a, **k: None)
    relay = EventRelay(
        r,
        "http://monitor:9091/ingest/v1/events",
        "tok",
        project_prefixes=["orcest", "other-project"],
    )
    with caplog.at_level(logging.WARNING):
        relay.start()
    relay.stop(timeout=1)
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("other-project" in m for m in warnings)
    # The matching prefix should not be flagged.
    assert not any("'orcest'" in m and "other-project" not in m for m in warnings)


def test_start_does_not_warn_when_project_prefixes_match(monkeypatch, caplog):
    r = _FakeRedis(key_prefix="orcest")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", lambda *a, **k: None)
    relay = EventRelay(
        r,
        "http://monitor:9091/ingest/v1/events",
        "tok",
        project_prefixes=["orcest"],
    )
    with caplog.at_level(logging.WARNING):
        relay.start()
    relay.stop(timeout=1)
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert not any("NEVER be relayed" in m for m in warnings)


def test_start_warns_on_empty_write_token(monkeypatch, caplog):
    r = _FakeRedis(key_prefix="orcest")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", lambda *a, **k: None)
    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "")
    with caplog.at_level(logging.WARNING):
        relay.start()
    relay.stop(timeout=1)
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("write token is empty" in m for m in warnings)


def test_start_does_not_warn_when_write_token_set(monkeypatch, caplog):
    r = _FakeRedis(key_prefix="orcest")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", lambda *a, **k: None)
    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    with caplog.at_level(logging.WARNING):
        relay.start()
    relay.stop(timeout=1)
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert not any("write token is empty" in m for m in warnings)
