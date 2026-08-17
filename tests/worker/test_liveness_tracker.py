"""Tests for LivenessTracker (task B7): signals -> ladder -> events, the
Redis activity record, and fleet gates (pressure + kill budget).

See ``.superpowers/sdd/2026-08-17-activity-watchdog/task-B7-brief.md`` for
the seven-test contract. Samplers are monkeypatched on the tracker's own
``ProcessTreeSampler``/``WorkspaceSampler`` instances (never touching real
``/proc`` or the filesystem); the emit callback records every call; both the
ladder clock and the wall clock are fake and independently advanceable.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from orcest.shared.config import WatchdogConfig
from orcest.worker.liveness_tracker import LivenessTracker

_PRESSURE_KEY = "orcest:fleet:pressure"
_BUDGET_LIMIT_KEY = "orcest:fleet:kill_budget:limit"


class _FakeClock:
    """A manually-advanceable clock usable as a ``Callable[[], float]``."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _cfg(**overrides: object) -> WatchdogConfig:
    defaults: dict[str, object] = dict(
        sample_interval=30,
        startup_grace=100,
        idle_window=100,
        waiting_grace=200,
        loop_exact_threshold=4,
        loop_error_threshold=3,
        loop_pingpong_threshold=6,
    )
    defaults.update(overrides)
    return WatchdogConfig(**defaults)  # type: ignore[arg-type]


def _progress_line() -> str:
    return json.dumps({"type": "assistant", "message": {"content": []}})


def _make_tracker(
    redis,
    *,
    cfg: WatchdogConfig | None = None,
    ceiling: float = 1_000_000.0,
    worker_id: str = "worker-1",
    task_id: str = "task-1",
    cpu_value: float | None = 5.0,
    workspace_changed_value: bool = False,
):
    events: list[tuple[str, dict]] = []

    def emit(event_type: str, data: dict) -> None:
        events.append((event_type, data))

    clock = _FakeClock(0.0)
    wall_clock = _FakeClock(1_700_000_000.0)

    tracker = LivenessTracker(
        cfg or _cfg(),
        ceiling,
        redis=redis,
        emit=emit,
        worker_id=worker_id,
        task_id=task_id,
        root_pid=999_999,  # never a real pid; sampler is monkeypatched below
        workspace=Path("/nonexistent-liveness-tracker-test-workspace"),
        clock=clock,
        wall_clock=wall_clock,
    )
    # Deterministic samplers, per the brief: patch the instances directly.
    tracker._proc_sampler.sample = lambda: cpu_value  # type: ignore[method-assign]
    tracker._workspace_sampler.changed_since = (  # type: ignore[method-assign]
        lambda ts: workspace_changed_value
    )
    return tracker, events, clock, wall_clock


def _drive_to_suspect_then_hold(tracker, events, clock) -> str | None:
    """Establish ACTIVE at t=0, then let all signals go stale until the
    ladder reports SUSPECT (t=120) and then the second idle_window elapses
    (t=220), the point at which an unblocked ladder would kill STUCK.

    Mirrors ``test_stuck_requires_second_stale_window_then_kills`` in
    ``test_liveness_ladder.py`` (idle_window=100), driven through the
    tracker instead of the ladder directly. Returns the final (t=220)
    tick's result.
    """
    tracker.observe_line(_progress_line())
    tracker.tick()  # t=0: BOOTSTRAP -> ACTIVE

    for _ in range(4):  # t=30, 60, 90, 120
        clock.advance(30)
        tracker.tick()

    clock.advance(100)  # t=220: second idle_window elapses
    return tracker.tick()


def test_transitions_emit_events_with_snapshot(fake_redis_client):
    tracker, events, _clock, _wall = _make_tracker(fake_redis_client)

    tracker.observe_line(_progress_line())
    result = tracker.tick()

    assert result is None
    assert len(events) == 1
    event_type, data = events[0]
    assert event_type == "net.orcest.task.active"
    assert "snapshot" in data
    assert isinstance(data["snapshot"], dict)
    assert "s1_last_fresh_ts" in data["snapshot"]
    assert "reason" not in data


def test_activity_record_written_with_ttl_and_state(fake_redis_client):
    tracker, _events, _clock, wall_clock = _make_tracker(fake_redis_client)

    tracker.observe_line(_progress_line())
    tracker.tick()

    key = "workers:activity:worker-1"
    record = fake_redis_client.hgetall_raw(key)
    assert record["task_id"] == "task-1"
    assert record["state"] == "active"
    assert record["last_liveness_ts"] == str(wall_clock.t)
    assert record["needs_reap"] == "0"
    assert "ladder_since" in record
    snapshot = json.loads(record["snapshot"])
    assert "s1_last_fresh_ts" in snapshot

    ttl = fake_redis_client.client.ttl(key)
    assert ttl == int(4 * 30)


def test_pressure_flag_suppresses_stuck_kill_but_suspect_event_still_emitted(
    fake_redis_client,
):
    fake_redis_client.set_ex_raw(_PRESSURE_KEY, "1", 900)
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    result = _drive_to_suspect_then_hold(tracker, events, clock)

    # No kill ever returned: the held-STUCK tick stays deferred as SUSPECT.
    assert result is None

    suspect_events = [e for e in events if e[0] == "net.orcest.task.suspect"]
    assert len(suspect_events) >= 1

    stuck_events = [e for e in events if e[0] == "net.orcest.task.stuck"]
    assert stuck_events == []


def test_kill_budget_zero_defers_kill_and_emits_kill_limit_once(fake_redis_client):
    fake_redis_client.set_ex_raw(_BUDGET_LIMIT_KEY, "0", 3600)
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    result = _drive_to_suspect_then_hold(tracker, events, clock)
    clock.advance(30)
    result2 = tracker.tick()

    assert result is None
    assert result2 is None

    kill_limit_events = [e for e in events if e[0] == "net.orcest.fleet.kill_limit"]
    assert len(kill_limit_events) == 1
    assert kill_limit_events[0][1] == {"limit": 0}

    stuck_events = [e for e in events if e[0] == "net.orcest.task.stuck"]
    assert stuck_events == []


def test_budget_increments_and_allows_within_limit(fake_redis_client):
    fake_redis_client.set_ex_raw(_BUDGET_LIMIT_KEY, "1", 3600)
    tracker, events, clock, wall_clock = _make_tracker(fake_redis_client)

    result = _drive_to_suspect_then_hold(tracker, events, clock)

    assert result == "stuck"

    stuck_events = [e for e in events if e[0] == "net.orcest.task.stuck"]
    assert len(stuck_events) == 1

    kill_limit_events = [e for e in events if e[0] == "net.orcest.fleet.kill_limit"]
    assert kill_limit_events == []

    hour = time.strftime("%Y%m%d%H", time.gmtime(wall_clock.t))
    bucket_key = f"orcest:fleet:kill_budget:{hour}"
    assert fake_redis_client.get_raw(bucket_key) == "1"


def test_ceiling_kill_bypasses_gates(fake_redis_client):
    fake_redis_client.set_ex_raw(_PRESSURE_KEY, "1", 900)
    fake_redis_client.set_ex_raw(_BUDGET_LIMIT_KEY, "0", 3600)

    tracker, _events, clock, wall_clock = _make_tracker(fake_redis_client, ceiling=50.0)
    # Never send a progress line: stays in BOOTSTRAP until the ceiling hits.
    clock.advance(50)
    result = tracker.tick()

    assert result == "ceiling"

    # Ceiling bypasses the budget gate specifically: no kill is consumed
    # from it even though the pressure flag and a zero kill-budget limit
    # both leave escalation_blocked=True for this evaluate() call. (The
    # budget probe itself still fires its own once-per-task
    # fleet.kill_limit notification independent of any particular kill --
    # that's exercised by test_kill_budget_zero_defers_kill_and_emits_kill_limit_once,
    # not this test.)
    hour = time.strftime("%Y%m%d%H", time.gmtime(wall_clock.t))
    bucket_key = f"orcest:fleet:kill_budget:{hour}"
    assert fake_redis_client.get_raw(bucket_key) is None


def test_close_deletes_activity_record(fake_redis_client):
    tracker, _events, _clock, _wall = _make_tracker(fake_redis_client)

    tracker.observe_line(_progress_line())
    tracker.tick()

    key = "workers:activity:worker-1"
    assert fake_redis_client.hgetall_raw(key) != {}

    tracker.close()

    assert fake_redis_client.hgetall_raw(key) == {}
