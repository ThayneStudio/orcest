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
``last_liveness_ts``/``ladder_since`` fields (and the workspace S3 baseline,
and the kill-budget hour bucket) need real wall-clock time even though the
ladder itself runs on the injectable *monotonic* ``clock`` (spec timers are
duration-based, so a monotonic clock is correct for them). Both are added
as keyword arguments with sensible defaults so existing call sites that
only pass the brief's original arguments still work modulo ``task_id``,
which callers must supply.

Clock-domain note (review round 1, finding 1): ``WorkspaceSampler.changed_since``
compares against file ``mtime``s, which are wall-clock (epoch) timestamps --
never the ladder's monotonic clock. Feeding it a monotonic ``now`` means
every real mtime is numerically greater than the (much smaller, process-
uptime-relative) monotonic timestamp, so S3 reads "changed" forever and
SUSPECT/STUCK can never fire. The workspace baseline is tracked and passed
in the wall-clock domain (``_last_ws_sample_wall_ts``, refreshed via
``wall_clock()``); only the ladder's own ``now`` stays monotonic.

Robustness note (review round 1, finding 2): once ``evaluate()`` latches a
kill, that decision must never be lost. ``decision.kill`` is captured into a
local immediately, and every post-evaluate side effect (event emits, the
activity-record write, budget consumption) is wrapped so an exception in
any one of them is logged and swallowed rather than propagating -- ``tick()``
always returns the kill trigger the ladder decided on, independent of
whether Redis or the emit callback are healthy. Symmetrically, the fleet-gate
*reads* that happen before ``evaluate()`` fail safe: a Redis error while
probing pressure/budget is treated as ``escalation_blocked=True`` (never
"more likely to kill") and the ladder is still evaluated normally --
CEILING, which bypasses gates entirely, must survive a Redis outage.

Thread-safety note (review round 1, finding 3): sampling (``/proc``,
workspace walk) and fleet-gate Redis reads are I/O and must never happen
while holding ``self._lock`` -- ``observe_line`` runs on the runner's
stdout-reader thread, and blocking it stalls the child's stdout pipe, which
is exactly the kind of stall this module is supposed to detect, not cause.
The lock is only ever held around the small, pure ladder/detector/shared-
field access in ``tick()`` and ``observe_line()``; a ``recent_hashes()``
snapshot for the periodic activity event is taken *inside* that locked
section too (finding 4: ``RepetitionDetector``'s internal deque is mutated
by ``observe_line`` from another thread, so iterating it outside the lock
can raise ``RuntimeError`` intermittently).
"""

from __future__ import annotations

import json
import logging
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

logger = logging.getLogger(__name__)

_PRESSURE_KEY = "orcest:fleet:pressure"
_KILL_BUDGET_LIMIT_KEY = "orcest:fleet:kill_budget:limit"
# Fail-closed default (final review, C1b): an absent or unreadable mirror
# key means kills are DISABLED, not "use the historical default of 6".
# ``FleetHealthMonitor`` refreshes this mirror every pass (see
# orchestrator/fleet_health.py's _mirror_kill_budget_limit), so an absent
# key means "no orchestrator has written a fresh limit recently" -- e.g. a
# worker template rolled out ahead of the orchestrator, a project whose
# orchestrator container isn't up yet, or Redis data loss. In every one of
# those cases the safe default is to observe (SUSPECT/STUCK/LOOPING still
# evaluate and emit events) rather than kill, exactly like an explicit
# ``max_kills_per_hour: 0``. Set a real limit is the mirror's job, not this
# constant's.
_KILL_BUDGET_DEFAULT_LIMIT = 0
_KILL_BUDGET_BUCKET_TTL = 7200

# I1: when a kill fires but post-kill D-state verification fails (the
# process tree didn't actually die), close() re-flushes the activity record
# instead of deleting it, with this longer TTL, so the pool reaper's fast
# (10s) loop reliably observes needs_reap=="1" before the record would
# otherwise expire.
_NEEDS_REAP_CLOSE_TTL = 600

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

        # Wall-clock baseline for S3 (see module docstring's clock-domain
        # note) -- deliberately NOT `started_at` (monotonic).
        self._last_ws_sample_wall_ts = wall_clock()
        self._tick_count = 0
        self._needs_reap = False
        self._kill_limit_emitted = False
        self._limit_malformed_logged = False
        self._state_since_wall = wall_clock()
        self._last_snapshot: dict[str, Any] = {}
        self._last_deferred_kill = False
        # M1: net.orcest.task.bootstrap fires exactly once, on this
        # tracker's first tick() call (not here in __init__, so the
        # constructor stays side-effect-free -- see tick()'s docstring
        # note).
        self._bootstrap_emitted = False

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def observe_line(self, line: str) -> None:
        """Classify one stdout line and feed S1 + the repetition detector."""
        sig = classify_line(line)  # pure, no I/O -- fine to do before the lock
        with self._lock:
            now = self._clock()
            self._ladder.note_stream(now, sig)
            # classify_line sets exactly one of these per line (never both):
            # tool_error_class for a failed tool_result, tool_name for a
            # tool_use block. Note that classify_line NEVER populates
            # tool_name on an error signal (the "user"/tool_result branch
            # only ever sets tool_error_class) -- error streaks are
            # therefore tool-name-blind by construction: two different
            # tools failing with the same error class hash identically.
            # That is upstream (stream_liveness) behavior, not something
            # this glue layer changes; see
            # test_error_streaks_are_tool_name_blind.
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
        written and any transition/periodic events are emitted every call
        on a best-effort basis: none of those side effects can suppress a
        kill the ladder already latched (see module docstring).
        """
        # --- I/O: sampling + fleet-gate reads. Never under the lock. ---
        cpu = self._proc_sampler.sample()

        ws_wall_now = self._wall_clock()
        with self._lock:
            last_ws_ts = self._last_ws_sample_wall_ts
        changed = self._workspace_sampler.changed_since(last_ws_ts)

        pressure_blocked = self._probe_pressure()
        limit = self._budget_limit()
        budget_blocked, budget_breached = self._probe_budget(limit)
        blocked = pressure_blocked or budget_blocked

        # --- Pure ladder/detector/shared-field access: under the lock. ---
        with self._lock:
            now = self._clock()
            self._last_ws_sample_wall_ts = ws_wall_now

            rep_verdict = self._detector.verdict()

            decision = self._ladder.evaluate(
                now, cpu, changed, rep_verdict, escalation_blocked=blocked
            )
            kill = decision.kill
            self._tick_count += 1
            tick_count = self._tick_count
            bootstrap_event = not self._bootstrap_emitted
            self._bootstrap_emitted = True
            if decision.transitioned:
                self._state_since_wall = self._wall_clock()
            self._last_snapshot = decision.snapshot
            # I3: the honest "would have killed here" signal for
            # observation-mode visibility -- surfaced on both the activity
            # record and the periodic task.activity event below.
            self._last_deferred_kill = decision.deferred

            emit_activity_event = tick_count % _ACTIVITY_EVENT_TICK_STRIDE == 0
            # Snapshot the recent-hashes view here, still under the lock --
            # RepetitionDetector's deque is mutated by observe_line from
            # another thread (finding 4).
            recent_hashes = self._detector.recent_hashes(20) if emit_activity_event else None

            # Emit the kill-limit event once per task, only for a genuine
            # budget breach (limit>0 and the bucket count met/exceeded it)
            # -- never for observation-mode (limit<=0) or a gate-read error.
            emit_kill_limit = budget_breached and not self._kill_limit_emitted
            if emit_kill_limit:
                self._kill_limit_emitted = True

        # --- Side effects: outside the lock, all best-effort. ---
        if bootstrap_event:
            # M1: emitted once, on the first tick(), regardless of which
            # state this evaluation landed on -- it marks "the tracker
            # started observing this task", not a ladder transition.
            self._safe(
                lambda: self._emit_fn("net.orcest.task.bootstrap", {"snapshot": decision.snapshot}),
                "bootstrap emit",
            )

        if decision.transitioned:
            self._safe(lambda: self._emit_transition(decision), "transition emit")

        if emit_activity_event:
            self._safe(
                lambda: self._emit_fn(
                    "net.orcest.task.activity",
                    {
                        "snapshot": decision.snapshot,
                        "recent_tool_hashes": recent_hashes,
                        "cpu_seconds": cpu,
                        "deferred_kill": decision.deferred,
                    },
                ),
                "activity event emit",
            )

        self._safe(self._write_activity_record, "activity record write")

        if emit_kill_limit:
            self._safe(
                lambda: self._emit_fn("net.orcest.fleet.kill_limit", {"limit": limit}),
                "kill_limit emit",
            )

        if kill == "ceiling":
            # Ceiling kills bypass all fleet gates (the ladder already
            # exempts them from escalation_blocked); no budget consumed.
            return "ceiling"
        if kill in ("stuck", "looping"):
            # The budget is only ever consumed when a kill actually fires.
            # We already probed (without incrementing) before evaluate();
            # a kill only reaches here when escalation_blocked was False on
            # this call, i.e. the probe said the budget wasn't exhausted.
            # Consuming now (rather than re-checking) accepts a small,
            # documented race: worst case one extra kill per hour
            # fleet-wide if another task's tick consumed the last budget
            # slot between our probe and this increment.
            self._safe(self._consume_budget, "budget consume")
            return kill
        return None

    def _safe(self, fn: Callable[[], None], what: str) -> None:
        """Run a post-evaluate side effect, logging and swallowing any
        exception so it can never cause a latched kill to be lost."""
        try:
            fn()
        except Exception:
            logger.warning("liveness tracker: %s failed", what, exc_info=True)

    def _emit_transition(self, decision: Decision) -> None:
        event_type = _STATE_EVENT_TYPES[decision.state]
        data: dict[str, Any] = {"snapshot": decision.snapshot}
        if decision.state == LadderState.WAITING:
            data["reason"] = decision.snapshot.get("waiting_reason", "")
        self._emit_fn(event_type, data)

    # ------------------------------------------------------------------
    # Fleet gates (pressure + budget)
    # ------------------------------------------------------------------

    def _probe_pressure(self) -> bool:
        try:
            return self._redis.get_raw(_PRESSURE_KEY) is not None
        except Exception:
            logger.warning("pressure gate read failed; failing safe to blocked", exc_info=True)
            return True

    def _budget_limit(self) -> int:
        try:
            raw = self._redis.get_raw(_KILL_BUDGET_LIMIT_KEY)
        except Exception:
            logger.warning(
                "kill-budget limit read failed; using default %d",
                _KILL_BUDGET_DEFAULT_LIMIT,
                exc_info=True,
            )
            return _KILL_BUDGET_DEFAULT_LIMIT
        if raw is None:
            return _KILL_BUDGET_DEFAULT_LIMIT
        try:
            return int(raw)
        except (TypeError, ValueError):
            if not self._limit_malformed_logged:
                logger.warning(
                    "malformed kill-budget limit %r; using default %d",
                    raw,
                    _KILL_BUDGET_DEFAULT_LIMIT,
                )
                self._limit_malformed_logged = True
            return _KILL_BUDGET_DEFAULT_LIMIT

    def _probe_budget(self, limit: int) -> tuple[bool, bool]:
        """Non-incrementing budget probe.

        Returns ``(blocked, breached)``. ``breached`` is True only when the
        hourly bucket was actually read and its count met/exceeded a
        positive ``limit`` -- a genuine budget breach, as opposed to
        "kills disabled by config" (``limit <= 0``) or "couldn't tell
        because Redis errored" (fails safe to blocked, but is not a
        breach and must not trigger the once-per-task kill_limit event).
        """
        if limit <= 0:
            return True, False
        try:
            raw = self._redis.get_raw(self._budget_hour_key())
        except Exception:
            logger.warning("kill-budget bucket read failed; failing safe to blocked", exc_info=True)
            return True, False
        try:
            n = int(raw) if raw is not None else 0
        except (TypeError, ValueError):
            # I2: a malformed bucket value must not raise out of the watchdog
            # thread -- treat it the same as an unreadable bucket (fail safe
            # to blocked, not a genuine breach).
            logger.warning("malformed kill-budget bucket value %r; failing safe to blocked", raw)
            return True, False
        breached = n >= limit
        return breached, breached

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

    def _write_activity_record(self, *, ttl_override: int | None = None) -> None:
        with self._lock:
            state = self._ladder.state.value
            needs_reap = self._needs_reap
            state_since = self._state_since_wall
            snapshot = self._last_snapshot
            deferred_kill = self._last_deferred_kill

        key = self._activity_key()
        wall_now = self._wall_clock()
        ttl = ttl_override if ttl_override is not None else int(4 * self._cfg.sample_interval)
        # RedisClient has no raw-key pipeline/mapping-hset helper, so use the
        # underlying client directly (same fully-qualified-key discipline as
        # hset_raw/expire_raw): one atomic MULTI/EXEC round trip for the whole
        # record + TTL instead of ~7 chatty sequential calls, so a reader can
        # never observe a half-written record.
        pipe = self._redis.client.pipeline(transaction=True)
        pipe.hset(
            key,
            mapping={
                "task_id": self._task_id,
                "state": state,
                "last_liveness_ts": str(wall_now),
                "ladder_since": str(state_since),
                "needs_reap": "1" if needs_reap else "0",
                "deferred_kill": "1" if deferred_kill else "0",
                "snapshot": json.dumps(snapshot, default=str),
            },
        )
        pipe.expire(key, ttl)
        pipe.execute()

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def tree_states(self) -> list[str]:
        """Passthrough to ``ProcessTreeSampler.state_of_tree`` (B8 verify-death)."""
        return self._proc_sampler.state_of_tree()

    @property
    def sample_interval(self) -> float:
        """The watchdog thread's poll cadence (B8: drives its ``cancelled.wait``)."""
        return self._cfg.sample_interval

    def last_snapshot(self) -> dict[str, Any]:
        """Most recent ladder decision snapshot (B8: STALLED result summary)."""
        with self._lock:
            return dict(self._last_snapshot)

    def emit_killed(
        self,
        trigger: str,
        verified: bool | None = None,
        *,
        superseded_by: str | None = None,
    ) -> None:
        """Emit ``net.orcest.task.killed`` (B8: after post-kill D-state
        verification). A small dedicated method rather than having the
        runner reach into ``_emit_fn`` directly, so the event shape and the
        best-effort-swallow semantics stay colocated with the tracker's
        other emits.

        ``verified`` is the post-kill D-state verification outcome; pass
        ``None`` (and the field is omitted) on paths where no verification
        ran -- e.g. a latched ladder kill whose attempt result was superseded
        by the abort/lock-lost path, marked via ``superseded_by="abort"`` so
        the consumed kill budget is still accounted for in the event stream.
        """
        data: dict[str, Any] = {"trigger": trigger}
        if verified is not None:
            data["verified"] = verified
        if superseded_by is not None:
            data["superseded_by"] = superseded_by
        self._safe(
            lambda: self._emit_fn("net.orcest.task.killed", data),
            "killed emit",
        )

    def mark_needs_reap(self) -> None:
        with self._lock:
            self._needs_reap = True
        # Flush immediately: after being marked, the task may be torn down
        # before the next tick() call, so the reaper flag must be visible
        # in Redis right away rather than waiting for the next sample.
        self._safe(self._write_activity_record, "activity record write (mark_needs_reap)")

    def close(self) -> None:
        """Delete the activity record -- unless ``needs_reap`` is set (I1),
        in which case the record is deliberately left behind: it is
        re-flushed with a longer TTL (``_NEEDS_REAP_CLOSE_TTL``) instead of
        deleted, so the pool reaper's fast (10s) polling loop reliably sees
        ``needs_reap=="1"`` even if this attempt's process is torn down
        (and this tracker discarded) before the reaper's next pass. Deleting
        it here -- the pre-I1 behavior -- could race the reaper into never
        observing the flag at all.

        Best-effort (review round 1, B8 fix): the runner calls this from a
        ``finally`` block after every attempt, so a raised Redis error here
        must never replace an already-decided ``RunnerResult`` (e.g. a
        completed, pushed task turning into a non-transient "Worker
        exception" FAILED). Swallowed and logged via the same ``_safe``
        pattern as every other tracker side effect."""
        with self._lock:
            needs_reap = self._needs_reap
        if needs_reap:
            self._safe(
                lambda: self._write_activity_record(ttl_override=_NEEDS_REAP_CLOSE_TTL),
                "close (needs_reap re-flush)",
            )
            return
        self._safe(lambda: self._redis.delete_raw(self._activity_key()), "close")
