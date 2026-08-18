"""Tests for LivenessTracker (task B7): signals -> ladder -> events, the
Redis activity record, and fleet gates (pressure + kill budget).

See ``.superpowers/sdd/2026-08-17-activity-watchdog/task-B7-brief.md`` for
the original seven-test contract, extended per the review-round-1 rulings
(see ``task-B7-report.md``) with: a real-clock/real-``WorkspaceSampler``
integration test for the wall-clock-vs-monotonic S3 bug; gate-read-failure
fail-safe tests; a no-I/O-under-the-lock regression test; an end-to-end
LOOPING kill driven purely through ``observe_line``; periodic activity-event
coverage; ``mark_needs_reap`` persistence; a deferred-kill-fires-after-
pressure-clears test; positive-limit budget exhaustion; the WAITING
transition's ``reason`` field; and malformed-budget-limit / tool-name-blind
error-streak pinning tests.

Samplers are monkeypatched on the tracker's own ``ProcessTreeSampler``/
``WorkspaceSampler`` instances for the synthetic (fake-clock) tests -- never
touching real ``/proc`` or the filesystem; the emit callback records every
call; both the ladder clock and the wall clock are fake and independently
advanceable there. The one real-clock integration test is called out
explicitly and uses neither monkeypatched samplers nor fake clocks.
"""

from __future__ import annotations

import json
import subprocess
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


def _tool_line(name: str = "Bash", args: dict | None = None) -> str:
    return json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [{"type": "tool_use", "name": name, "input": args or {"cmd": "ls"}}]
            },
        }
    )


def _error_line(error_text: str) -> str:
    return json.dumps(
        {
            "type": "user",
            "message": {
                "content": [{"type": "tool_result", "is_error": True, "content": error_text}]
            },
        }
    )


def _waiting_line() -> str:
    return json.dumps({"type": "system", "subtype": "api_retry"})


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


def test_kill_budget_zero_defers_kill_without_kill_limit_event(fake_redis_client):
    # Ruling (review round 1, finding 5): limit<=0 is "kills disabled by
    # config" (observation mode), not a budget breach -- it must gate
    # escalation without ever emitting fleet.kill_limit, which means
    # "the budget was actually exhausted."
    fake_redis_client.set_ex_raw(_BUDGET_LIMIT_KEY, "0", 3600)
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    result = _drive_to_suspect_then_hold(tracker, events, clock)
    clock.advance(30)
    result2 = tracker.tick()

    assert result is None
    assert result2 is None

    kill_limit_events = [e for e in events if e[0] == "net.orcest.fleet.kill_limit"]
    assert kill_limit_events == []

    stuck_events = [e for e in events if e[0] == "net.orcest.task.stuck"]
    assert stuck_events == []


def test_kill_budget_exhausted_with_positive_limit_emits_kill_limit_once(fake_redis_client):
    fake_redis_client.set_ex_raw(_BUDGET_LIMIT_KEY, "2", 3600)
    tracker, events, clock, wall_clock = _make_tracker(fake_redis_client)

    hour = time.strftime("%Y%m%d%H", time.gmtime(wall_clock.t))
    bucket_key = f"orcest:fleet:kill_budget:{hour}"
    fake_redis_client.set_ex_raw(bucket_key, "2", 3600)  # already at the limit

    result = _drive_to_suspect_then_hold(tracker, events, clock)
    clock.advance(30)
    result2 = tracker.tick()

    assert result is None
    assert result2 is None

    kill_limit_events = [e for e in events if e[0] == "net.orcest.fleet.kill_limit"]
    assert len(kill_limit_events) == 1
    assert kill_limit_events[0][1] == {"limit": 2}

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
    # both leave escalation_blocked=True for this evaluate() call.
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


def test_close_swallows_redis_failure(fake_redis_client):
    # Review round 1 (B8 fix): close() runs in the runner's `finally` after
    # every attempt -- a raised Redis error here must never propagate and
    # replace an already-decided RunnerResult.
    tracker, _events, _clock, _wall = _make_tracker(fake_redis_client)

    def raising_delete_raw(key: str) -> int:
        raise RuntimeError("redis is down")

    tracker._redis.delete_raw = raising_delete_raw  # type: ignore[method-assign]

    tracker.close()  # must not raise


# ---------------------------------------------------------------------
# Review round 1 findings: additional coverage
# ---------------------------------------------------------------------


def test_workspace_sampler_real_clock_domain_reaches_suspect(tmp_path, fake_redis_client):
    """Regression test for finding 1: WorkspaceSampler.changed_since compares
    against real (wall-clock/epoch) file mtimes. If the tracker ever fed it
    the ladder's monotonic clock instead, every real mtime would read as
    "newer" than that tiny monotonic timestamp forever, S3 would never go
    stale, and SUSPECT could never fire. This test uses the REAL
    WorkspaceSampler against a real tmp_path (with a real file already in
    it, so changed_since does real work) and REAL clocks (clock/wall_clock
    are left at their time.monotonic/time.time defaults) -- no monkeypatched
    samplers, no fake clocks.
    """
    # Simulate a freshly-checked-out workspace: a real file with a real
    # (epoch) mtime, present before the tracker is even constructed.
    (tmp_path / "checked_out_file.txt").write_text("hello")

    cfg = _cfg(startup_grace=0.05, idle_window=0.15, waiting_grace=1.0)

    events: list[tuple[str, dict]] = []

    def emit(event_type: str, data: dict) -> None:
        events.append((event_type, data))

    # A real, idle process (asleep, burns ~no CPU) so S2 samples are
    # genuinely zero-delta without needing to fake /proc.
    proc = subprocess.Popen(["sleep", "5"])
    try:
        tracker = LivenessTracker(
            cfg,
            ceiling=1_000_000.0,
            redis=fake_redis_client,
            emit=emit,
            worker_id="itest-worker",
            task_id="itest-task",
            root_pid=proc.pid,
            workspace=tmp_path,
            # clock/wall_clock intentionally NOT overridden: real clocks.
        )

        tracker.observe_line(_progress_line())
        tracker.tick()  # BOOTSTRAP -> ACTIVE; establishes S2/S3 baselines

        result = None
        for _ in range(3):
            time.sleep(0.05)
            result = tracker.tick()
        # Push well past idle_window with margin for scheduler jitter.
        time.sleep(0.3)
        result = tracker.tick()

        suspect_events = [e for e in events if e[0] == "net.orcest.task.suspect"]
        assert len(suspect_events) >= 1, (
            f"expected ladder to reach SUSPECT via real wall-clock-domain "
            f"S3 staleness; events so far: {[e[0] for e in events]}"
        )
        # Scheduler jitter means the held-STUCK window may or may not have
        # elapsed too by the last tick -- either is fine evidence the real
        # wall-clock-domain staleness math works end to end; only a crash
        # or "never left BOOTSTRAP/ACTIVE" would indicate the clock-domain
        # bug is back.
        assert result in (None, "stuck")
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_gate_read_failure_fails_safe_ceiling_still_fires(fake_redis_client):
    tracker, _events, clock, _wall = _make_tracker(fake_redis_client, ceiling=50.0)

    def raising_get_raw(key: str) -> str | None:
        raise RuntimeError("redis is down")

    tracker._redis.get_raw = raising_get_raw  # type: ignore[method-assign]

    clock.advance(50)
    result = tracker.tick()

    assert result == "ceiling"


def test_gate_read_failure_fails_safe_defers_stuck_without_crashing(fake_redis_client):
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    def raising_get_raw(key: str) -> str | None:
        raise RuntimeError("redis is down")

    tracker._redis.get_raw = raising_get_raw  # type: ignore[method-assign]

    # Redis is down for the whole sequence: gate reads fail safe to
    # blocked=True, so the would-be STUCK kill stays deferred -- but the
    # ladder keeps evaluating normally (no crash/propagated exception) and
    # SUSPECT is still reported.
    result = _drive_to_suspect_then_hold(tracker, events, clock)

    assert result is None
    suspect_events = [e for e in events if e[0] == "net.orcest.task.suspect"]
    assert len(suspect_events) >= 1


def test_kill_returned_even_if_side_effects_raise(fake_redis_client):
    fake_redis_client.set_ex_raw(_BUDGET_LIMIT_KEY, "1", 3600)
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    def raising_emit(event_type: str, data: dict) -> None:
        raise RuntimeError("emit callback exploded")

    def raising_hset_raw(*args: object, **kwargs: object) -> int:
        raise RuntimeError("redis hset exploded")

    tracker._emit_fn = raising_emit  # type: ignore[assignment]
    tracker._redis.hset_raw = raising_hset_raw  # type: ignore[method-assign]

    result = _drive_to_suspect_then_hold(tracker, events, clock)

    # Every side effect (event emits, activity-record write) is broken,
    # but the kill the ladder actually latched must still come back.
    assert result == "stuck"


def test_no_io_call_happens_while_lock_held(fake_redis_client):
    """Regression test for finding 3: sampling and gate reads must never
    run while ``self._lock`` is held, or a slow Redis/NFS call could block
    ``observe_line`` on the stdout-reader thread -- the watchdog inducing
    the very stall it's supposed to detect."""
    tracker, _events, _clock, _wall = _make_tracker(fake_redis_client)

    def checked_sample() -> float:
        assert tracker._lock.acquire(blocking=False), "sample() called while lock held"
        tracker._lock.release()
        return 5.0

    def checked_changed_since(ts: float) -> bool:
        assert tracker._lock.acquire(blocking=False), "changed_since() called while lock held"
        tracker._lock.release()
        return False

    real_get_raw = fake_redis_client.get_raw

    def checked_get_raw(key: str) -> str | None:
        assert tracker._lock.acquire(blocking=False), "get_raw() called while lock held"
        tracker._lock.release()
        return real_get_raw(key)

    tracker._proc_sampler.sample = checked_sample  # type: ignore[method-assign]
    tracker._workspace_sampler.changed_since = checked_changed_since  # type: ignore[method-assign]
    tracker._redis.get_raw = checked_get_raw  # type: ignore[method-assign]

    tracker.observe_line(_progress_line())
    tracker.tick()


def test_looping_kill_end_to_end_via_observe_line(fake_redis_client):
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    tracker.observe_line(_progress_line())
    tracker.tick()  # BOOTSTRAP -> ACTIVE

    for _ in range(4):  # loop_exact_threshold default is 4
        tracker.observe_line(_tool_line("Bash", {"cmd": "ls -la /tmp"}))

    clock.advance(30)
    result = tracker.tick()  # first non-None verdict: looping_streak=1
    assert result is None

    clock.advance(30)
    result = tracker.tick()  # second consecutive non-None verdict: fires
    assert result == "looping"

    looping_events = [e for e in events if e[0] == "net.orcest.task.looping"]
    assert len(looping_events) == 1
    assert looping_events[0][1]["snapshot"]["looping_verdict"]["stream"] == "exact"


def test_periodic_activity_event_every_tenth_tick_carries_recent_hashes(fake_redis_client):
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    tracker.observe_line(_progress_line())
    tracker.observe_line(_tool_line("Read", {"path": "/a"}))

    for _ in range(10):
        clock.advance(30)
        tracker.tick()

    activity_events = [e for e in events if e[0] == "net.orcest.task.activity"]
    assert len(activity_events) == 1
    _event_type, data = activity_events[0]
    assert data["recent_tool_hashes"]
    assert data["recent_tool_hashes"][0]["tool"] == "Read"
    assert "cpu_seconds" in data
    assert "snapshot" in data


def test_mark_needs_reap_persists_without_waiting_for_tick(fake_redis_client):
    tracker, _events, _clock, _wall = _make_tracker(fake_redis_client)

    tracker.observe_line(_progress_line())
    tracker.tick()

    key = "workers:activity:worker-1"
    assert fake_redis_client.hgetall_raw(key)["needs_reap"] == "0"

    tracker.mark_needs_reap()

    assert fake_redis_client.hgetall_raw(key)["needs_reap"] == "1"


def test_deferred_kill_fires_after_pressure_clears(fake_redis_client):
    fake_redis_client.set_ex_raw(_PRESSURE_KEY, "1", 900)
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    result = _drive_to_suspect_then_hold(tracker, events, clock)
    assert result is None  # deferred by pressure

    fake_redis_client.delete_raw(_PRESSURE_KEY)
    clock.advance(30)
    result = tracker.tick()

    assert result == "stuck"


def test_waiting_transition_event_carries_reason(fake_redis_client):
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)

    tracker.observe_line(_progress_line())
    tracker.tick()  # ACTIVE

    tracker.observe_line(_waiting_line())
    clock.advance(1)
    result = tracker.tick()

    assert result is None
    waiting_events = [e for e in events if e[0] == "net.orcest.task.waiting"]
    assert len(waiting_events) == 1
    _event_type, data = waiting_events[0]
    assert data["reason"] == "api_retry"


def test_budget_limit_malformed_value_falls_back_to_default(fake_redis_client):
    fake_redis_client.set_ex_raw(_BUDGET_LIMIT_KEY, "not-a-number", 3600)
    tracker, _events, _clock, _wall = _make_tracker(fake_redis_client)

    assert tracker._budget_limit() == 6
    # Calling again must stay tolerant (no raise, no re-log crash).
    assert tracker._budget_limit() == 6


def test_error_streaks_are_tool_name_blind(fake_redis_client):
    """classify_line never populates tool_name on a tool_result error
    signal (only tool_error_class), so the repetition detector's error
    streak is fed an empty name every time -- pinning that upstream
    behavior here rather than implying (as the old comment did) that
    tool_name might ever be meaningfully set for an error line."""
    tracker, events, clock, _wall = _make_tracker(fake_redis_client)
    tracker.observe_line(_progress_line())
    tracker.tick()

    for _ in range(3):  # loop_error_threshold default is 3
        tracker.observe_line(_error_line("boom: connection refused"))

    clock.advance(30)
    result = tracker.tick()
    assert result is None  # streak=1, not yet ready

    clock.advance(30)
    result = tracker.tick()
    assert result == "looping"

    looping_events = [e for e in events if e[0] == "net.orcest.task.looping"]
    assert looping_events[0][1]["snapshot"]["looping_verdict"]["stream"] == "error_class"
