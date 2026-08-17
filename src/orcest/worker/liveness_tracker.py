"""LivenessTracker: glues the activity-watchdog pieces together for one task.

Owns everything needed to turn a stream of stdout lines and periodic samples
into liveness-ladder decisions (spec §5), Redis-visible activity state, and
CloudEvents-shaped events (spec §8-§9):

- ``ProcessTreeSampler``/``WorkspaceSampler`` (B3, ``liveness_signals.py``)
  for S2/S3.
- ``classify_line`` (B4, ``stream_liveness.py``) for S1.
- ``RepetitionDetector`` (B5, ``repetition.py``) for the LOOPING trigger.
- ``LivenessLadder`` (B6, ``liveness_ladder.py``) -- the pure state machine.

and layers on:

- The ``workers:activity:{worker_id}`` Redis hash (for the reaper/dashboard).
- ``net.orcest.task.<state>`` transition events + a periodic
  ``net.orcest.task.activity`` snapshot event.
- Fleet gates (kill budget + pressure) applied *through* the ladder's
  ``escalation_blocked`` hook, never by intercepting a returned kill.

Interface note: the task-B7 brief's ``__init__`` stub omits ``task_id`` and
a wall-clock hook. Both are required in practice -- the activity record's
``task_id`` field has nowhere else to come from, and the record's
``last_liveness_ts``/``ladder_since`` fields need real wall-clock time for
the reaper's staleness math even though the ladder itself runs on the
injectable *monotonic* ``clock`` (spec timers are duration-based, so a
monotonic clock is correct for them; wall time is only needed for the
Redis-visible timestamps and the kill-budget hour bucket). Both are added
as keyword arguments with sensible defaults so existing call sites that
only pass the brief's original arguments still work modulo ``task_id``,
which callers must supply.

Thread-safety: ``observe_line`` runs on the runner's stdout-reader thread;
``tick`` runs on the watchdog thread. All ladder/detector/sampler-baseline
state is only ever touched while holding ``self._lock``; Redis I/O and
``emit`` calls happen *outside* the lock so a slow Redis call or emit
callback can never block the stdout thread.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from orcest.shared.config import WatchdogConfig
from orcest.shared.redis_client import RedisClient
from orcest.worker.liveness_ladder import Decision, LadderState, LivenessLadder
from orcest.worker.liveness_signals import ProcessTreeSampler, WorkspaceSampler
from orcest.worker.repetition import RepetitionDetector
from orcest.worker.stream_liveness import classify_line

_PRESSURE_KEY = "orcest:fleet:pressure"
_KILL_BUDGET_LIMIT_KEY = "orcest:fleet:kill_budget:limit"
_KILL_BUDGET_DEFAULT_LIMIT = "6"
_KILL_BUDGET_BUCKET_TTL = 7200

_ACTIVITY_KEY_PREFIX = "workers:activity:"

# Every 10th tick (~300s at the default 30s sample_interval) emits a
# task.activity snapshot event, independent of transitions.
_ACTIVITY_EVENT_TICK_STRIDE = 10

# Ladder state -> event type. Built explicitly (not string-concatenated)
# and checked against the locked taxonomy so a future ladder state can't
# silently produce an unregistered event type.
_STATE_EVENT_TYPES: dict[LadderState, str] = {
    LadderState.BOOTSTRAP: "net.orcest.task.bootstrap",
    LadderState.ACTIVE: "net.orcest.task.active",
    LadderState.WAITING: "net.orcest.task.waiting",
    LadderState.SUSPECT: "net.orcest.task.suspect",
    LadderState.STUCK: "net.orcest.task.stuck",
    LadderState.LOOPING: "net.orcest.task.looping",
}


class LivenessTracker:
    """Owns one task's liveness: feed lines in, get kill decisions out.

    Thread-safety: ``observe_line`` is called from the runner's stdout loop;
    ``tick()`` from the watchdog thread. Internal lock around ladder access.
    """

    def __init__(
        self,
        cfg: WatchdogConfig,
        ceiling: float,
        *,
        redis: RedisClient,
        emit: Callable[[str, dict], None],
        worker_id: str,
        task_id: str,
        root_pid: int,
        workspace: Path,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._cfg = cfg
        self._redis = redis
        self._emit_fn = emit
        self._worker_id = worker_id
        self._task_id = task_id
        self._clock = clock
        self._wall_clock = wall_clock

        self._lock = threading.Lock()

        started_at = clock()
        self._ladder = LivenessLadder(cfg, ceiling, started_at)
        self._proc_sampler = ProcessTreeSampler(root_pid)
        self._workspace_sampler = WorkspaceSampler(workspace)
        self._detector = RepetitionDetector(
            exact_threshold=cfg.loop_exact_threshold,
            error_threshold=cfg.loop_error_threshold,
            pingpong_threshold=cfg.loop_pingpong_threshold,
        )

        self._last_ws_sample_ts = started_at
        self._tick_count = 0
        self._needs_reap = False
        self._kill_limit_emitted = False
        self._state_since_wall = wall_clock()
        self._last_snapshot: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def observe_line(self, line: str) -> None:
        """Classify one stdout line and feed S1 + the repetition detector."""
        sig = classify_line(line)
        with self._lock:
            now = self._clock()
            self._ladder.note_stream(now, sig)
            # classify_line sets exactly one of these per line (never both):
            # tool_error_class for a failed tool_result, tool_name for a
            # tool_use block. Anything else (plain progress/output/waiting)
            # is not repetition-detector input.
            if sig.tool_error_class:
                self._detector.observe_tool_error(sig.tool_name, sig.tool_error_class)
            elif sig.tool_name:
                self._detector.observe_tool_call(sig.tool_name, sig.tool_args)

    # ------------------------------------------------------------------
    # Periodic tick
    # ------------------------------------------------------------------

    def tick(self) -> str | None:
        """Sample, evaluate the ladder (through fleet gates), and report.

        Returns the kill trigger ("stuck" | "looping" | "ceiling") the
        ladder decided on this tick, or None. The activity record is
        written and any transition/periodic events are emitted every call,
        regardless of the return value.
        """
        with self._lock:
            now = self._clock()
            cpu = self._proc_sampler.sample()
            changed = self._workspace_sampler.changed_since(self._last_ws_sample_ts)
            self._last_ws_sample_ts = now

            rep_verdict = self._detector.verdict()

            pressure_blocked = self._redis.get_raw(_PRESSURE_KEY) is not None
            limit = self._budget_limit()
            budget_blocked = self._budget_probe_blocked(limit)
            blocked = pressure_blocked or budget_blocked

            decision = self._ladder.evaluate(
                now, cpu, changed, rep_verdict, escalation_blocked=blocked
            )
            self._tick_count += 1
            if decision.transitioned:
                self._state_since_wall = self._wall_clock()
            self._last_snapshot = decision.snapshot
            tick_count = self._tick_count

            # Emit the kill-limit event once per task, the first time the
            # budget probe itself blocks (not on every subsequent tick).
            emit_kill_limit = budget_blocked and not self._kill_limit_emitted
            if emit_kill_limit:
                self._kill_limit_emitted = True

        # --- everything below runs without the lock held ---

        if decision.transitioned:
            self._emit_transition(decision)

        if tick_count % _ACTIVITY_EVENT_TICK_STRIDE == 0:
            self._emit_fn(
                "net.orcest.task.activity",
                {
                    "snapshot": decision.snapshot,
                    "recent_tool_hashes": self._detector.recent_hashes(20),
                    "cpu_seconds": cpu,
                },
            )

        self._write_activity_record()

        if emit_kill_limit:
            self._emit_fn("net.orcest.fleet.kill_limit", {"limit": limit})

        if decision.kill == "ceiling":
            # Ceiling kills bypass all fleet gates (the ladder already
            # exempts them from escalation_blocked); no budget consumed.
            return "ceiling"
        if decision.kill in ("stuck", "looping"):
            # The budget is only ever consumed when a kill actually fires.
            # We already probed (without incrementing) before evaluate();
            # a kill only reaches here when escalation_blocked was False on
            # this call, i.e. the probe said the budget wasn't exhausted.
            # Consuming now (rather than re-checking) accepts a small,
            # documented race: worst case one extra kill per hour
            # fleet-wide if another task's tick consumed the last budget
            # slot between our probe and this increment.
            self._consume_budget()
            return decision.kill
        return None

    def _emit_transition(self, decision: Decision) -> None:
        event_type = _STATE_EVENT_TYPES[decision.state]
        data: dict[str, Any] = {"snapshot": decision.snapshot}
        if decision.state == LadderState.WAITING:
            data["reason"] = decision.snapshot.get("waiting_reason", "")
        self._emit_fn(event_type, data)

    # ------------------------------------------------------------------
    # Fleet gates (budget)
    # ------------------------------------------------------------------

    def _budget_limit(self) -> int:
        raw = self._redis.get_raw(_KILL_BUDGET_LIMIT_KEY)
        return int(raw or _KILL_BUDGET_DEFAULT_LIMIT)

    def _budget_probe_blocked(self, limit: int) -> bool:
        # limit <= 0 means kills are disabled entirely (observation mode).
        if limit <= 0:
            return True
        raw = self._redis.get_raw(self._budget_hour_key())
        n = int(raw) if raw is not None else 0
        return n >= limit

    def _budget_hour_key(self) -> str:
        hour = time.strftime("%Y%m%d%H", time.gmtime(self._wall_clock()))
        return f"orcest:fleet:kill_budget:{hour}"

    def _consume_budget(self) -> None:
        key = self._budget_hour_key()
        self._redis.incr_raw(key)
        self._redis.expire_raw(key, _KILL_BUDGET_BUCKET_TTL)

    # ------------------------------------------------------------------
    # Activity record
    # ------------------------------------------------------------------

    def _activity_key(self) -> str:
        return f"{_ACTIVITY_KEY_PREFIX}{self._worker_id}"

    def _write_activity_record(self) -> None:
        with self._lock:
            state = self._ladder.state.value
            needs_reap = self._needs_reap
            state_since = self._state_since_wall
            snapshot = self._last_snapshot

        key = self._activity_key()
        wall_now = self._wall_clock()
        self._redis.hset_raw(key, "task_id", self._task_id)
        self._redis.hset_raw(key, "state", state)
        self._redis.hset_raw(key, "last_liveness_ts", str(wall_now))
        self._redis.hset_raw(key, "ladder_since", str(state_since))
        self._redis.hset_raw(key, "needs_reap", "1" if needs_reap else "0")
        self._redis.hset_raw(key, "snapshot", json.dumps(snapshot, default=str))
        self._redis.expire_raw(key, int(4 * self._cfg.sample_interval))

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def tree_states(self) -> list[str]:
        """Passthrough to ``ProcessTreeSampler.state_of_tree`` (B8 verify-death)."""
        return self._proc_sampler.state_of_tree()

    def mark_needs_reap(self) -> None:
        with self._lock:
            self._needs_reap = True
        # Flush immediately: after being marked, the task may be torn down
        # before the next tick() call, so the reaper flag must be visible
        # in Redis right away rather than waiting for the next sample.
        self._write_activity_record()

    def close(self) -> None:
        """Delete the activity record."""
        self._redis.delete_raw(self._activity_key())
