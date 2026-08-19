"""Tests for FleetHealthMonitor (task B10): fleet-wide pressure detector +
kill-budget limit mirror.

See ``.superpowers/sdd/2026-08-17-activity-watchdog/task-B10-brief.md`` for
the five-test contract. Uses the shared ``fake_redis_client`` fixture
(fakeredis-backed ``RedisClient``) and drives ``_pass_once`` directly with an
injected clock -- no background thread involved except in the one test that
exercises ``start()``/``stop()`` explicitly.
"""

from __future__ import annotations

import json

from orcest.orchestrator.fleet_health import FleetHealthMonitor
from orcest.shared.events import EVENTS_STREAM, make_event

_PRESSURE_KEY = "orcest:fleet:pressure"
_BUDGET_LIMIT_KEY = "orcest:fleet:kill_budget:limit"


class _FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _make_monitor(redis, **overrides):
    defaults: dict[str, object] = dict(
        pressure_min_tasks=3,
        pressure_window=600,
        pressure_hold=900,
        max_kills_per_hour=6,
    )
    defaults.update(overrides)
    clock = defaults.pop("clock", _FakeClock())
    return FleetHealthMonitor(redis, clock=clock, **defaults), clock


def _spool_suspect(redis, task_id: str, iso_time: str) -> None:
    """Push a task.suspect envelope with an explicit ``time`` field.

    Builds via make_event (for a realistic envelope shape/type gate) then
    overwrites ``time`` so tests can control aging independent of wall clock.
    """
    envelope = make_event(
        "net.orcest.task.suspect",
        source_project="p",
        task_id=task_id,
        repo="o/r",
        resource_type="pr",
        resource_id=1,
        attempt=0,
    )
    envelope["time"] = iso_time
    redis.xadd_capped(EVENTS_STREAM, {"envelope": json.dumps(envelope)}, 50000)


def _iso(epoch: float) -> str:
    import datetime

    return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def test_three_distinct_suspects_in_window_sets_pressure_and_emits_once(fake_redis_client):
    monitor, clock = _make_monitor(fake_redis_client)
    now = clock.t
    _spool_suspect(fake_redis_client, "t1", _iso(now))
    _spool_suspect(fake_redis_client, "t2", _iso(now))
    _spool_suspect(fake_redis_client, "t3", _iso(now))

    monitor._pass_once()

    assert fake_redis_client.get_raw(_PRESSURE_KEY) == "1"
    ttl = fake_redis_client.client.ttl(_PRESSURE_KEY)
    assert 0 < ttl <= 900

    events = fake_redis_client.xrange("events")
    pressure_events = [
        json.loads(f["envelope"])
        for _, f in events
        if json.loads(f["envelope"])["type"] == "net.orcest.fleet.pressure"
    ]
    assert len(pressure_events) == 1
    data = pressure_events[0]["data"]
    assert sorted(data["suspect_tasks"]) == ["t1", "t2", "t3"]
    assert data["window_seconds"] == 600
    assert pressure_events[0]["subject"] == "fleet"
    assert pressure_events[0]["data"]["work"]["repo"] == ""
    assert pressure_events[0]["data"]["work"]["resource_id"] == 0


def test_flapping_same_task_does_not_trip_pressure(fake_redis_client):
    """B10 (final review, minor deferred): pressure is evaluated on the
    DISTINCT count of task_ids in the window -- three task.suspect events
    for the SAME task_id (a single flapping task oscillating in/out of
    SUSPECT) must not trip pressure, even though three raw envelopes were
    spooled."""
    monitor, clock = _make_monitor(fake_redis_client)
    now = clock.t
    _spool_suspect(fake_redis_client, "flapping-task", _iso(now))
    _spool_suspect(fake_redis_client, "flapping-task", _iso(now))
    _spool_suspect(fake_redis_client, "flapping-task", _iso(now))

    monitor._pass_once()

    assert fake_redis_client.get_raw(_PRESSURE_KEY) is None
    events = fake_redis_client.xrange("events")
    pressure_events = [
        f for _, f in events if json.loads(f["envelope"])["type"] == "net.orcest.fleet.pressure"
    ]
    assert pressure_events == []


def test_two_suspects_do_not_trip(fake_redis_client):
    monitor, clock = _make_monitor(fake_redis_client)
    now = clock.t
    _spool_suspect(fake_redis_client, "t1", _iso(now))
    _spool_suspect(fake_redis_client, "t2", _iso(now))

    monitor._pass_once()

    assert fake_redis_client.get_raw(_PRESSURE_KEY) is None
    events = fake_redis_client.xrange("events")
    pressure_events = [
        f for _, f in events if json.loads(f["envelope"])["type"] == "net.orcest.fleet.pressure"
    ]
    assert pressure_events == []


def test_old_suspects_age_out_of_window(fake_redis_client):
    monitor, clock = _make_monitor(fake_redis_client, pressure_window=600)
    now = clock.t
    # Two suspects far in the past (outside the 600s window once the clock
    # advances), one recent -- only one distinct task remains in-window.
    _spool_suspect(fake_redis_client, "old1", _iso(now))
    _spool_suspect(fake_redis_client, "old2", _iso(now))
    clock.advance(700)
    _spool_suspect(fake_redis_client, "new1", _iso(clock.t))

    monitor._pass_once()

    assert fake_redis_client.get_raw(_PRESSURE_KEY) is None


def test_key_refreshed_but_event_not_duplicated_while_held(fake_redis_client):
    monitor, clock = _make_monitor(fake_redis_client, pressure_hold=900)
    now = clock.t
    _spool_suspect(fake_redis_client, "t1", _iso(now))
    _spool_suspect(fake_redis_client, "t2", _iso(now))
    _spool_suspect(fake_redis_client, "t3", _iso(now))

    monitor._pass_once()
    ttl_after_first = fake_redis_client.client.ttl(_PRESSURE_KEY)

    # Simulate TTL decay, then run another pass while the condition still
    # holds (same suspects still in-window) -- TTL should refresh back up,
    # but no second pressure event should be emitted.
    fake_redis_client.client.expire(_PRESSURE_KEY, 10)
    monitor._pass_once()
    ttl_after_second = fake_redis_client.client.ttl(_PRESSURE_KEY)

    assert ttl_after_second > 10
    assert fake_redis_client.get_raw(_PRESSURE_KEY) == "1"

    events = fake_redis_client.xrange("events")
    pressure_events = [
        f for _, f in events if json.loads(f["envelope"])["type"] == "net.orcest.fleet.pressure"
    ]
    assert len(pressure_events) == 1
    assert ttl_after_first > 0


def test_reemit_allowed_after_key_expires_and_condition_retrips(fake_redis_client):
    monitor, clock = _make_monitor(fake_redis_client)
    now = clock.t
    _spool_suspect(fake_redis_client, "t1", _iso(now))
    _spool_suspect(fake_redis_client, "t2", _iso(now))
    _spool_suspect(fake_redis_client, "t3", _iso(now))

    monitor._pass_once()

    # Simulate the hold TTL actually expiring (key gone).
    fake_redis_client.delete_raw(_PRESSURE_KEY)

    # A fresh batch of suspects re-trips the condition.
    clock.advance(10)
    _spool_suspect(fake_redis_client, "t4", _iso(clock.t))
    _spool_suspect(fake_redis_client, "t5", _iso(clock.t))
    _spool_suspect(fake_redis_client, "t6", _iso(clock.t))

    monitor._pass_once()

    assert fake_redis_client.get_raw(_PRESSURE_KEY) == "1"
    events = fake_redis_client.xrange("events")
    pressure_events = [
        f for _, f in events if json.loads(f["envelope"])["type"] == "net.orcest.fleet.pressure"
    ]
    assert len(pressure_events) == 2


def test_limit_mirrored_at_startup(fake_redis_client):
    monitor, _clock = _make_monitor(fake_redis_client, max_kills_per_hour=6)

    assert fake_redis_client.get_raw(_BUDGET_LIMIT_KEY) is None
    monitor.start()
    try:
        assert fake_redis_client.get_raw(_BUDGET_LIMIT_KEY) == "6"
        ttl = fake_redis_client.client.ttl(_BUDGET_LIMIT_KEY)
        assert 0 < ttl <= 7 * 24 * 3600
    finally:
        monitor.stop(timeout=2)


def test_limit_mirrored_and_refreshed_each_pass(fake_redis_client):
    monitor, _clock = _make_monitor(fake_redis_client, max_kills_per_hour=9)

    monitor._pass_once()
    assert fake_redis_client.get_raw(_BUDGET_LIMIT_KEY) == "9"


def test_malformed_envelope_skipped_and_cursor_advances(fake_redis_client):
    monitor, clock = _make_monitor(fake_redis_client)
    fake_redis_client.xadd_capped(EVENTS_STREAM, {"envelope": "not json"}, 50000)
    _spool_suspect(fake_redis_client, "t1", _iso(clock.t))

    monitor._pass_once()

    assert fake_redis_client.get("fleet_health:cursor") is not None
    # Only one valid suspect was recorded -- not enough to trip pressure.
    assert fake_redis_client.get_raw(_PRESSURE_KEY) is None


def test_non_dict_envelope_skipped_and_cursor_advances(fake_redis_client):
    """A spool entry whose ``envelope`` field is valid JSON but not a JSON
    object (e.g. ``"[]"`` or ``"42"``) must not raise -- ``envelope.get(...)``
    on a list/int raises AttributeError, which would otherwise propagate out
    of _pass_once before the cursor is advanced and wedge pressure detection
    on that entry forever. A following valid suspect must still be
    processed in the same pass.
    """
    monitor, clock = _make_monitor(fake_redis_client)
    fake_redis_client.xadd_capped(EVENTS_STREAM, {"envelope": "[]"}, 50000)
    fake_redis_client.xadd_capped(EVENTS_STREAM, {"envelope": "42"}, 50000)
    _spool_suspect(fake_redis_client, "t1", _iso(clock.t))

    monitor._pass_once()

    cursor = fake_redis_client.get("fleet_health:cursor")
    assert cursor is not None
    # Cursor advanced past all three entries (not stuck on the poison ones).
    assert cursor.split("-")[0] != "0"
    # Only one valid suspect recorded -- not enough to trip pressure, but it
    # was processed rather than skipped as a side effect of a wedge.
    assert fake_redis_client.get_raw(_PRESSURE_KEY) is None
    assert list(monitor._suspects)[-1][1] == "t1"


def test_numeric_time_envelope_skipped_and_cursor_advances(fake_redis_client):
    """A task.suspect envelope whose ``time`` field is a JSON number (truthy,
    so it passes the ``not task_id or not time_str`` check) must not raise --
    ``_parse_rfc3339`` calling ``.endswith`` on an int raises AttributeError,
    which only ValueError/TypeError were caught for. A following valid
    suspect must still be processed in the same pass.
    """
    monitor, clock = _make_monitor(fake_redis_client)
    envelope = make_event(
        "net.orcest.task.suspect",
        source_project="p",
        task_id="bad-time-task",
        repo="o/r",
        resource_type="pr",
        resource_id=1,
        attempt=0,
    )
    envelope["time"] = 12345
    fake_redis_client.xadd_capped(EVENTS_STREAM, {"envelope": json.dumps(envelope)}, 50000)
    _spool_suspect(fake_redis_client, "t1", _iso(clock.t))

    monitor._pass_once()

    cursor = fake_redis_client.get("fleet_health:cursor")
    assert cursor is not None
    assert cursor.split("-")[0] != "0"
    assert fake_redis_client.get_raw(_PRESSURE_KEY) is None
    task_ids = [task_id for _, task_id in monitor._suspects]
    assert task_ids == ["t1"]
