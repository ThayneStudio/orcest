# tests/shared/test_events.py
import json
from datetime import datetime

import pytest

from orcest.shared.events import (
    DEFAULT_EVENTS_MAXLEN,
    EVENT_TYPES,
    EVENTS_STREAM,
    EventPublisher,
    make_event,
)


def test_taxonomy_is_locked_v1_set():
    suffixes = {
        "task.enqueued", "task.started", "task.bootstrap", "task.active",
        "task.waiting", "task.suspect", "task.stuck", "task.looping",
        "task.killed", "task.completed", "task.failed", "task.reaped",
        "task.activity", "fleet.pressure", "fleet.kill_limit",
    }
    assert EVENT_TYPES == frozenset("net.orcest." + s for s in suffixes)


def test_make_event_envelope_shape():
    env = make_event(
        "net.orcest.task.started",
        source_project="myproj",
        task_id="abc123",
        repo="owner/repo",
        resource_type="pr",
        resource_id=42,
        attempt=2,
        head_sha="deadbeef",
        worker_id="w1",
        provider="claude",
        data={"extra": 1},
    )
    assert set(env) == {"id", "source", "type", "subject", "time", "data"}
    assert env["source"] == "urn:orcest:myproj"
    assert env["type"] == "net.orcest.task.started"
    assert env["subject"] == "abc123"
    # RFC3339 UTC with trailing Z
    datetime.strptime(env["time"], "%Y-%m-%dT%H:%M:%SZ")
    assert env["data"]["work"] == {
        "repo": "owner/repo", "resource_type": "pr", "resource_id": 42,
    }
    assert env["data"]["attempt"] == 2
    assert env["data"]["head_sha"] == "deadbeef"
    assert env["data"]["worker_id"] == "w1"
    assert env["data"]["provider"] == "claude"
    assert env["data"]["extra"] == 1
    # unique ids
    env2 = make_event(
        "net.orcest.task.started", source_project="myproj", task_id="abc123",
        repo="owner/repo", resource_type="pr", resource_id=42, attempt=2,
    )
    assert env2["id"] != env["id"]


@pytest.mark.parametrize(
    "key", ["work", "attempt", "head_sha", "worker_id", "provider"]
)
def test_make_event_rejects_reserved_data_key_collision(key):
    with pytest.raises(ValueError, match=key):
        make_event(
            "net.orcest.task.started",
            source_project="p",
            task_id="t",
            repo="o/r",
            resource_type="pr",
            resource_id=1,
            attempt=0,
            data={key: "clobbered"},
        )


def test_make_event_allows_non_colliding_data_keys():
    env = make_event(
        "net.orcest.task.started",
        source_project="p",
        task_id="t",
        repo="o/r",
        resource_type="pr",
        resource_id=1,
        attempt=3,
        data={"reason": "timeout", "elapsed_seconds": 12.5},
    )
    assert env["data"]["attempt"] == 3
    assert env["data"]["reason"] == "timeout"
    assert env["data"]["elapsed_seconds"] == 12.5


def test_make_event_rejects_unknown_type():
    with pytest.raises(ValueError):
        make_event(
            "net.orcest.task.exploded", source_project="p", task_id="t",
            repo="o/r", resource_type="pr", resource_id=1, attempt=0,
        )


class _FakeRedis:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    def xadd_capped(self, stream, fields, maxlen):
        if self.fail:
            raise ConnectionError("redis down")
        self.calls.append((stream, fields, maxlen))
        return "1-1"


def test_publisher_xadds_json_envelope():
    r = _FakeRedis()
    pub = EventPublisher(r)  # type: ignore[arg-type]
    env = make_event(
        "net.orcest.task.completed", source_project="p", task_id="t",
        repo="o/r", resource_type="pr", resource_id=1, attempt=0,
    )
    pub.publish(env)
    stream, fields, maxlen = r.calls[0]
    assert stream == EVENTS_STREAM
    assert maxlen == DEFAULT_EVENTS_MAXLEN
    assert json.loads(fields["envelope"])["type"] == "net.orcest.task.completed"


def test_publisher_swallows_redis_errors():
    pub = EventPublisher(_FakeRedis(fail=True))  # type: ignore[arg-type]
    env = make_event(
        "net.orcest.task.completed", source_project="p", task_id="t",
        repo="o/r", resource_type="pr", resource_id=1, attempt=0,
    )
    pub.publish(env)  # must not raise
