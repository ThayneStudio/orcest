"""The liveness ladder: a pure state machine for the activity watchdog.

This is the heart of the design (spec §5). Given a stream of stream-json
classifications (``StreamSignal``, see ``stream_liveness.py``), periodic
process-tree/workspace samples, and repetition verdicts (``RepetitionVerdict``,
see ``repetition.py``), it decides whether a task looks alive, is merely
waiting on the provider, has gone quiet (SUSPECT), is definitively stuck
(STUCK), is livelocked (LOOPING), or has blown through its hard wall-clock
backstop (CEILING).

Pure: no I/O, no clock reads. The caller injects ``now`` on every call and
supplies CPU/workspace samples and repetition verdicts obtained elsewhere.

Signal freshness, precisely (spec §5, brief task-B6):

- **S1 (stream liveness)**: fresh when the last stream signal of kind
  ``progress`` or ``output`` was observed within ``idle_window``. Only a
  ``progress`` signal exits BOOTSTRAP; ``output`` freshens S1 but is a weak
  signal for that purpose.
- **S2 (CPU)**: idle after 3 consecutive ``evaluate()`` calls report zero
  CPU delta versus the previous call. A ``None`` sample (process tree gone)
  counts as idle immediately, with no baseline required.
- **S3 (workspace)**: fresh when ``workspace_changed`` was observed within
  ``idle_window`` of the last such observation (task start counts as an
  initial change -- the checkout populated the workspace).

WAITING gates only the SUSPECT -> STUCK edge (escalation), for
``waiting_grace`` seconds after the most recent waiting signal; SUSPECT
itself is always reachable and reported. LOOPING is evaluated independently
of S1-S3 and fires on the second consecutive non-None repetition verdict.
CEILING overrides everything and can fire from any state, including
BOOTSTRAP.

Once a kill fires (STUCK, LOOPING, or CEILING) the ladder is done: further
``evaluate()`` calls return the same terminal state with ``kill=None`` and
``transitioned=False`` without recomputing anything.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from orcest.shared.config import WatchdogConfig
from orcest.worker.repetition import RepetitionVerdict
from orcest.worker.stream_liveness import StreamSignal

# Consecutive zero-CPU-delta evaluate() calls required before S2 is idle
# (spec §4 S2 hysteresis).
_CPU_IDLE_STREAK_THRESHOLD = 3

# Consecutive non-None repetition verdicts required before LOOPING fires
# (spec §5 persistence: never kill on a single stale/tripped evaluation).
_LOOPING_STREAK_THRESHOLD = 2


class LadderState(str, Enum):
    BOOTSTRAP = "bootstrap"
    ACTIVE = "active"
    WAITING = "waiting"
    SUSPECT = "suspect"
    STUCK = "stuck"
    LOOPING = "looping"


@dataclass(frozen=True)
class Decision:
    """One ``evaluate()`` outcome. ``snapshot`` is event-safe: per-signal
    last-fresh timestamps, ages, and the waiting reason only -- never raw
    tool args (spec §8 redaction rule)."""

    state: LadderState
    transitioned: bool  # state changed this evaluation
    kill: str | None  # None | "stuck" | "looping" | "ceiling"
    snapshot: dict


class LivenessLadder:
    """Pure liveness-ladder state machine (spec §5).

    ``ceiling`` is the hard wall-clock backstop (seconds since
    ``started_at``); it is the caller's ``RunnerConfig.timeout``, not part
    of ``WatchdogConfig``, since it applies even when the ladder itself is
    disabled during BOOTSTRAP.
    """

    def __init__(self, cfg: WatchdogConfig, ceiling: float, started_at: float) -> None:
        self.cfg = cfg
        self.ceiling = ceiling
        self.started_at = started_at

        self.state = LadderState.BOOTSTRAP

        # S1: last stream signal of kind progress/output.
        self._last_s1_ts: float | None = None
        self._bootstrap_seen_progress = False

        # S2: CPU-delta hysteresis.
        self._prev_cpu_seconds: float | None = None
        self._has_cpu_baseline = False
        self._cpu_idle_streak = 0

        # S3: workspace change recency. Task start counts as an initial
        # change -- the checkout populated the workspace.
        self._last_workspace_change_ts: float = started_at

        # WAITING.
        self._waiting_active = False
        self._last_waiting_ts: float | None = None
        self._waiting_reason = ""

        # SUSPECT/STUCK persistence: timestamp SUSPECT was first entered,
        # continuously (reset to None the instant any signal is fresh).
        self._suspect_since: float | None = None

        # LOOPING persistence.
        self._looping_streak = 0

        # Terminal: a kill has fired. No further recomputation.
        self._done = False

    def note_stream(self, now: float, sig: StreamSignal) -> None:
        """Feed a classified stream-json line. Pure bookkeeping only --
        never gates on BOOTSTRAP/done here; ``evaluate()`` does the gating."""
        if sig.kind == "progress":
            self._last_s1_ts = now
            self._bootstrap_seen_progress = True
            self._waiting_active = False
        elif sig.kind == "output":
            self._last_s1_ts = now
            self._waiting_active = False
        elif sig.kind == "waiting":
            self._waiting_active = True
            self._last_waiting_ts = now
            self._waiting_reason = sig.reason

    def evaluate(
        self,
        now: float,
        cpu_seconds: float | None,
        workspace_changed: bool,
        rep_verdict: RepetitionVerdict | None,
    ) -> Decision:
        if self._done:
            return Decision(self.state, False, None, self._snapshot(now))

        prev_state = self.state
        ceiling_hit = (now - self.started_at) >= self.ceiling

        if prev_state == LadderState.BOOTSTRAP:
            exiting = self._bootstrap_seen_progress or (
                now - self.started_at
            ) >= self.cfg.startup_grace
            if not exiting:
                if ceiling_hit:
                    self._done = True
                    return Decision(LadderState.BOOTSTRAP, False, "ceiling", self._snapshot(now))
                return Decision(LadderState.BOOTSTRAP, False, None, self._snapshot(now))
            # Falls through: bootstrap exits this same evaluation, and the
            # resulting state is computed normally below using this `now`.

        s2_idle = self._update_cpu(cpu_seconds)

        if workspace_changed:
            self._last_workspace_change_ts = now
        s3_fresh = (now - self._last_workspace_change_ts) < self.cfg.idle_window

        s1_fresh = self._last_s1_ts is not None and (
            now - self._last_s1_ts
        ) < self.cfg.idle_window

        # LOOPING: independent of S1-S3 by design (spec §5) -- a retry loop
        # reads ACTIVE on every other signal, that's the whole point of S4.
        if rep_verdict is not None:
            self._looping_streak += 1
        else:
            self._looping_streak = 0

        if self._looping_streak >= _LOOPING_STREAK_THRESHOLD:
            self.state = LadderState.LOOPING
            self._done = True
            kill = "ceiling" if ceiling_hit else "looping"
            return Decision(
                LadderState.LOOPING, prev_state != LadderState.LOOPING, kill, self._snapshot(now)
            )

        all_stale = (not s1_fresh) and s2_idle and (not s3_fresh)

        if all_stale:
            if self._suspect_since is None:
                self._suspect_since = now
            held = (now - self._suspect_since) >= self.cfg.idle_window
            if held:
                blocked = self._waiting_active and (
                    now - self._last_waiting_ts
                ) < self.cfg.waiting_grace
                new_state = LadderState.SUSPECT if blocked else LadderState.STUCK
            else:
                new_state = LadderState.SUSPECT
        else:
            self._suspect_since = None
            new_state = LadderState.WAITING if self._waiting_active else LadderState.ACTIVE

        transitioned = new_state != prev_state
        kill: str | None = None
        if new_state == LadderState.STUCK:
            kill = "stuck"
            self._done = True
        if ceiling_hit:
            kill = "ceiling"
            self._done = True

        self.state = new_state
        return Decision(new_state, transitioned, kill, self._snapshot(now))

    def _update_cpu(self, cpu_seconds: float | None) -> bool:
        """Advance the S2 CPU-idle hysteresis by one sample. Returns whether
        S2 currently reads idle (streak >= threshold)."""
        if cpu_seconds is None:
            # Process tree gone: treat as idle unconditionally, no baseline
            # required (spec: "None = process gone, treat as idle").
            zero_delta = True
        elif self._has_cpu_baseline:
            zero_delta = cpu_seconds == self._prev_cpu_seconds
        else:
            # First real sample: no delta to compare against yet.
            zero_delta = False

        self._prev_cpu_seconds = cpu_seconds
        self._has_cpu_baseline = True

        self._cpu_idle_streak = self._cpu_idle_streak + 1 if zero_delta else 0
        return self._cpu_idle_streak >= _CPU_IDLE_STREAK_THRESHOLD

    def _snapshot(self, now: float) -> dict:
        return {
            "s1_last_fresh_ts": self._last_s1_ts,
            "s1_age": None if self._last_s1_ts is None else now - self._last_s1_ts,
            "s2_idle_streak": self._cpu_idle_streak,
            "s3_last_changed_ts": self._last_workspace_change_ts,
            "s3_age": now - self._last_workspace_change_ts,
            "suspect_since": self._suspect_since,
            "waiting_active": self._waiting_active,
            "waiting_reason": self._waiting_reason,
            "waiting_since": self._last_waiting_ts,
            "looping_streak": self._looping_streak,
        }
