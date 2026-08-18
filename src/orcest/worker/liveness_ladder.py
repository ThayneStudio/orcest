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
CEILING is the hard backstop and can fire from any state, including
BOOTSTRAP -- but it only wins when no corroborated STUCK/LOOPING trigger
also fires on the same evaluation: a corroborated stall/loop gets the
permanent (non-transient) classification, not a blind timing-coincidence
retry via the transient CEILING path (spec §5 kill semantics, review
ruling).

Continuous provider backoff is intentionally NOT bounded by the ladder
itself: as long as waiting signals keep arriving, STUCK stays deferred
indefinitely (grace is measured from the *most recent* waiting signal).
"Waiting on provider" is not stall; CEILING backstops truly-perpetual
waiting, and rate-limit/usage-exhausted handling is a separate, existing
mechanism. See ``test_perpetual_waiting_defers_to_ceiling``.

``evaluate()`` also accepts ``escalation_blocked`` (default ``False``), a
fleet-level gate hook (kill budget / pressure, spec §5 "Fleet gates" --
wired up by the caller, e.g. task B7). When ``True``, a would-be
SUSPECT -> STUCK or LOOPING kill transition is *deferred*: the reported
state stays at its pre-kill value (SUSPECT, or whatever S1-S3 compute for
a deferred LOOPING) and nothing latches the terminal ``_done`` flag, so a
later unblocked ``evaluate()`` can still kill if the underlying condition
still holds. CEILING is exempt from this gate -- it fires regardless.

When ``escalation_blocked`` is the reason a would-be STUCK/LOOPING kill was
suppressed (as opposed to WAITING's own ``waiting_grace`` mechanism, which
can independently hold a task at SUSPECT with the gate never in the
picture), ``Decision.deferred`` is ``True`` -- the honest "the ladder would
have killed here" signal an observation-mode operator needs, since a
gate-blocked run never emits a STUCK/LOOPING transition event at all.

Once a kill actually fires (STUCK, LOOPING, or CEILING -- not merely
deferred) the ladder is done: further ``evaluate()`` calls return the same
terminal state with ``kill=None`` and ``transitioned=False`` without
recomputing anything.
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
    last-fresh timestamps, ages, the waiting reason, and (stream/count/
    hashes only) the current repetition verdict -- never raw tool args
    (spec §8 redaction rule). This is the kill-time post-mortem evidence."""

    state: LadderState
    transitioned: bool  # state changed this evaluation
    kill: str | None  # None | "stuck" | "looping" | "ceiling"
    snapshot: dict
    # True when `escalation_blocked` alone suppressed a STUCK/LOOPING kill
    # that would otherwise have fired this evaluation (final-review I3): the
    # honest "would have killed" signal for observation-mode visibility.
    # False for every other outcome, including a waiting-grace-only defer
    # (that's WAITING's own mechanism, not the fleet gate) and an actual
    # kill. See ``evaluate()``'s two escalation_blocked checkpoints.
    deferred: bool = False


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

        # S2 last-fresh timestamp: wall time of the last *nonzero* CPU
        # delta (evidence the process tree was genuinely doing something),
        # for post-mortem snapshots. started_at before any sample.
        self._s2_last_fresh_ts: float = started_at

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
        escalation_blocked: bool = False,
    ) -> Decision:
        if self._done:
            return Decision(self.state, False, None, self._snapshot(now, rep_verdict))

        prev_state = self.state
        ceiling_hit = (now - self.started_at) >= self.ceiling

        if prev_state == LadderState.BOOTSTRAP:
            exiting = self._bootstrap_seen_progress or (
                now - self.started_at
            ) >= self.cfg.startup_grace
            if not exiting:
                # CEILING is the one thing that still applies during
                # BOOTSTRAP; there is no corroborated STUCK/LOOPING trigger
                # possible here (the ladder is disabled), so it always wins.
                if ceiling_hit:
                    self._done = True
                    return Decision(
                        LadderState.BOOTSTRAP, False, "ceiling", self._snapshot(now, rep_verdict)
                    )
                return Decision(
                    LadderState.BOOTSTRAP, False, None, self._snapshot(now, rep_verdict)
                )
            # Falls through: bootstrap exits this same evaluation, and the
            # resulting state is computed normally below using this `now`.

        s2_idle = self._update_cpu(now, cpu_seconds)

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

        looping_ready = self._looping_streak >= _LOOPING_STREAK_THRESHOLD
        if looping_ready and not escalation_blocked:
            self.state = LadderState.LOOPING
            self._done = True
            # Corroborated trigger wins over CEILING on the same evaluation
            # (review ruling: permanent classification, not a blind retry
            # via timing coincidence) -- looping always reports "looping"
            # here, never "ceiling".
            return Decision(
                LadderState.LOOPING,
                prev_state != LadderState.LOOPING,
                "looping",
                self._snapshot(now, rep_verdict),
            )
        # If looping_ready but escalation_blocked: deferred. Fall through to
        # the normal S1-S3 computation below for the *reported* state (the
        # streak is preserved above, so a later unblocked evaluate() with a
        # still-non-None verdict can still kill).
        deferred = looping_ready and escalation_blocked

        all_stale = (not s1_fresh) and s2_idle and (not s3_fresh)

        if all_stale:
            if self._suspect_since is None:
                self._suspect_since = now
            held = (now - self._suspect_since) >= self.cfg.idle_window
            if held:
                if self._waiting_active:
                    assert self._last_waiting_ts is not None, (
                        "waiting_active implies last_waiting_ts is set"
                    )
                    waiting_blocked = (now - self._last_waiting_ts) < self.cfg.waiting_grace
                else:
                    waiting_blocked = False
                # Deferred by either gate: report SUSPECT, not STUCK. Only
                # count it as a *gate*-deferred kill (I3) when the fleet gate
                # is what's holding it -- if waiting_blocked alone already
                # explains the SUSPECT (grace hasn't expired), that's
                # WAITING's own mechanism, not the fleet gate, even if
                # escalation_blocked also happens to be True this tick.
                if escalation_blocked and not waiting_blocked:
                    deferred = True
                # Nothing here latches _done -- a later evaluate() with
                # neither gate active can still escalate if all_stale holds.
                new_state = (
                    LadderState.SUSPECT
                    if (waiting_blocked or escalation_blocked)
                    else LadderState.STUCK
                )
            else:
                new_state = LadderState.SUSPECT
        else:
            self._suspect_since = None
            new_state = LadderState.WAITING if self._waiting_active else LadderState.ACTIVE

        transitioned = new_state != prev_state
        kill: str | None = None
        if new_state == LadderState.STUCK:
            # Corroborated trigger wins over CEILING on the same evaluation
            # (review ruling); only fall back to "ceiling" when STUCK did
            # not fire this call (not reached, not held, or deferred).
            kill = "stuck"
            self._done = True
        elif ceiling_hit:
            kill = "ceiling"
            self._done = True

        self.state = new_state
        return Decision(
            new_state, transitioned, kill, self._snapshot(now, rep_verdict), deferred=deferred
        )

    def _update_cpu(self, now: float, cpu_seconds: float | None) -> bool:
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

        if not zero_delta:
            self._s2_last_fresh_ts = now

        self._prev_cpu_seconds = cpu_seconds
        self._has_cpu_baseline = True

        self._cpu_idle_streak = self._cpu_idle_streak + 1 if zero_delta else 0
        return self._cpu_idle_streak >= _CPU_IDLE_STREAK_THRESHOLD

    def _snapshot(self, now: float, rep_verdict: RepetitionVerdict | None = None) -> dict:
        return {
            "s1_last_fresh_ts": self._last_s1_ts,
            "s1_age": None if self._last_s1_ts is None else now - self._last_s1_ts,
            "s2_last_fresh_ts": self._s2_last_fresh_ts,
            "s2_age": now - self._s2_last_fresh_ts,
            "s2_idle_streak": self._cpu_idle_streak,
            "s3_last_changed_ts": self._last_workspace_change_ts,
            "s3_age": now - self._last_workspace_change_ts,
            "suspect_since": self._suspect_since,
            "waiting_active": self._waiting_active,
            "waiting_reason": self._waiting_reason,
            "waiting_since": self._last_waiting_ts,
            "looping_streak": self._looping_streak,
            # Event-safe: only stream/count/hashes ever leave RepetitionVerdict
            # (spec §8 redaction rule) -- merged here so a LOOPING kill's
            # snapshot carries the tripped verdict without the caller having
            # to separately correlate it.
            "looping_verdict": None
            if rep_verdict is None
            else {
                "stream": rep_verdict.stream,
                "count": rep_verdict.count,
                "hashes": rep_verdict.hashes,
            },
        }
