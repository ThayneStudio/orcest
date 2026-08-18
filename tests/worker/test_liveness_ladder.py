"""Tests for the liveness ladder state machine.

See ``.superpowers/sdd/2026-08-17-activity-watchdog/task-B6-brief.md`` for
the binding eleven-test contract. These tests are the executable form of
the spec's §5 -- each one drives ``note_stream``/``evaluate`` with explicit
``now`` values and pins down exact ladder semantics.

Shared config for every test (per the brief):
``WatchdogConfig(sample_interval=30, startup_grace=100, idle_window=100,
waiting_grace=200)``.
"""

from __future__ import annotations

from orcest.shared.config import WatchdogConfig
from orcest.worker.liveness_ladder import LadderState, LivenessLadder
from orcest.worker.repetition import RepetitionVerdict
from orcest.worker.stream_liveness import StreamSignal


def _cfg() -> WatchdogConfig:
    return WatchdogConfig(sample_interval=30, startup_grace=100, idle_window=100, waiting_grace=200)


def _active_baseline(ladder: LivenessLadder) -> None:
    """Exit BOOTSTRAP and establish fresh S1/S2/S3 baselines at now=0."""
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))
    ladder.evaluate(now=0, cpu_seconds=5.0, workspace_changed=True, rep_verdict=None)


def test_bootstrap_exempt_until_first_progress_or_grace():
    cfg = _cfg()

    # A progress signal exits bootstrap immediately, well before the grace.
    ladder = LivenessLadder(cfg, ceiling=100_000, started_at=0)
    d = ladder.evaluate(now=50, cpu_seconds=0.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.BOOTSTRAP
    assert d.kill is None

    ladder.note_stream(now=60, sig=StreamSignal(kind="progress"))
    d = ladder.evaluate(now=60, cpu_seconds=0.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.ACTIVE
    assert d.transitioned is True

    # A weak "output" signal does NOT exit bootstrap.
    ladder2 = LivenessLadder(cfg, ceiling=100_000, started_at=0)
    ladder2.note_stream(now=10, sig=StreamSignal(kind="output"))
    d = ladder2.evaluate(now=99, cpu_seconds=0.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.BOOTSTRAP

    # With no progress signal at all, startup_grace elapsing exits bootstrap
    # -- resulting state computed normally (the earlier output signal is
    # still fresh at now=100, so it lands on ACTIVE, not SUSPECT).
    d = ladder2.evaluate(now=100, cpu_seconds=0.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.ACTIVE
    assert d.transitioned is True


def test_ceiling_during_bootstrap_kills_without_exiting_bootstrap():
    # startup_grace=100, ceiling well inside it: never gets a progress
    # signal or reaches the grace, but CEILING still applies during
    # BOOTSTRAP (spec §5: "ladder disabled; ceiling still applies").
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=50, started_at=0)
    d = ladder.evaluate(now=50, cpu_seconds=None, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.BOOTSTRAP
    assert d.kill == "ceiling"

    d2 = ladder.evaluate(now=80, cpu_seconds=None, workspace_changed=False, rep_verdict=None)
    assert d2.kill is None
    assert d2.transitioned is False


def test_active_survives_past_old_timeout_with_output():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))

    now = 0
    while now <= 3000:
        ladder.note_stream(now=now, sig=StreamSignal(kind="output"))
        d = ladder.evaluate(now=now, cpu_seconds=None, workspace_changed=False, rep_verdict=None)
        assert d.state not in (LadderState.SUSPECT, LadderState.STUCK)
        assert d.kill is None
        now += 30


def test_all_stale_goes_suspect_not_kill():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    for now in (30, 60, 90):
        d = ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
        assert d.state != LadderState.SUSPECT

    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None
    assert d.transitioned is True


def test_stuck_requires_second_stale_window_then_kills():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None

    # Still within the second stale window: not yet STUCK.
    d = ladder.evaluate(now=180, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None

    # A further full idle_window of continuous SUSPECT -> STUCK, kill fires
    # exactly on this transition.
    d = ladder.evaluate(now=240, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"
    assert d.transitioned is True

    # The ladder is done: later evaluates never re-fire the kill.
    d2 = ladder.evaluate(now=300, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d2.state == LadderState.STUCK
    assert d2.kill is None
    assert d2.transitioned is False


def test_single_fresh_signal_resets_suspect_to_active():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT

    # No stream signal, no workspace change -- a CPU delta alone rescues.
    d = ladder.evaluate(now=150, cpu_seconds=6.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.ACTIVE
    assert d.transitioned is True
    assert d.kill is None


def test_waiting_blocks_escalation_within_grace():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    ladder.note_stream(now=100, sig=StreamSignal(kind="waiting", reason="rate_limit"))

    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT  # SUSPECT can still be entered and reported

    # now=220: second stale idle_window has elapsed (STUCK-eligible), but
    # waiting_grace (200s from the waiting signal at 100 -> expires at 300)
    # is still active, so escalation beyond SUSPECT is blocked.
    d = ladder.evaluate(now=220, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None
    assert d.snapshot["waiting_reason"] == "rate_limit"


def test_waiting_escalates_after_grace_expires():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    ladder.note_stream(now=100, sig=StreamSignal(kind="waiting", reason="rate_limit"))

    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT

    d = ladder.evaluate(now=220, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT  # still blocked, grace not expired yet

    # waiting_grace expires at 100 + 200 = 300; with no fresh progress since,
    # escalation resumes and STUCK fires (SUSPECT has held continuously
    # since 120, well past the required second idle_window).
    d = ladder.evaluate(now=300, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"


def test_looping_requires_two_consecutive_verdicts():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))

    verdict = RepetitionVerdict(stream="exact", count=4, hashes=("abc123",))

    d = ladder.evaluate(now=0, cpu_seconds=1.0, workspace_changed=True, rep_verdict=verdict)
    assert d.state != LadderState.LOOPING
    assert d.kill is None

    d = ladder.evaluate(now=30, cpu_seconds=2.0, workspace_changed=True, rep_verdict=verdict)
    assert d.state == LadderState.LOOPING
    assert d.kill == "looping"
    assert d.transitioned is True

    # A None verdict, if it had arrived first, would have reset the streak
    # -- verified separately below.
    ladder2 = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    ladder2.note_stream(now=0, sig=StreamSignal(kind="progress"))
    ladder2.evaluate(now=0, cpu_seconds=1.0, workspace_changed=True, rep_verdict=verdict)
    ladder2.evaluate(now=30, cpu_seconds=1.0, workspace_changed=True, rep_verdict=None)
    d = ladder2.evaluate(now=60, cpu_seconds=1.0, workspace_changed=True, rep_verdict=verdict)
    assert d.state != LadderState.LOOPING
    assert d.kill is None


def test_looping_fires_even_while_cpu_and_output_active():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))

    verdict = RepetitionVerdict(stream="exact", count=4, hashes=("deadbeef",))

    now = 0
    d = None
    for cpu in (1.0, 2.0):
        ladder.note_stream(now=now, sig=StreamSignal(kind="output"))
        d = ladder.evaluate(now=now, cpu_seconds=cpu, workspace_changed=True, rep_verdict=verdict)
        now += 30

    # Every S1/S2/S3 signal reads fresh/active on this final call; LOOPING
    # still fires because S4 is evaluated independently.
    assert d.state == LadderState.LOOPING
    assert d.kill == "looping"


def test_ceiling_kills_regardless_of_activity():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=500, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))
    ladder.note_stream(now=500, sig=StreamSignal(kind="output"))

    d = ladder.evaluate(now=500, cpu_seconds=1.0, workspace_changed=True, rep_verdict=None)
    assert d.kill == "ceiling"

    # Done: a later evaluate never re-fires it.
    d2 = ladder.evaluate(now=530, cpu_seconds=1.0, workspace_changed=True, rep_verdict=None)
    assert d2.kill is None


def test_snapshot_records_signal_ages_and_reason():
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    ladder.note_stream(now=40, sig=StreamSignal(kind="waiting", reason="api_retry"))
    d = ladder.evaluate(now=70, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    assert d.state == LadderState.WAITING
    snap = d.snapshot
    assert snap["s1_last_fresh_ts"] == 0
    assert snap["s1_age"] == 70
    assert snap["s3_last_changed_ts"] == 0
    assert snap["s3_age"] == 70
    assert snap["waiting_reason"] == "api_retry"
    assert snap["waiting_since"] == 40

    # Event-safe: no raw tool args ever surface in the snapshot.
    assert "tool_args" not in snap
    assert snap["looping_verdict"] is None  # no dict values when no verdict is in play


# --- Round-1 review findings -------------------------------------------


def test_stuck_kill_snapshot_has_per_signal_last_fresh_timestamps():
    """Finding 1a: the kill-time snapshot must carry the S2 last-fresh
    timestamp (not just the idle streak count), alongside S1/S3, so a STUCK
    kill's post-mortem evidence is complete."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)  # progress + cpu=5.0 + workspace_changed=True at now=0

    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    ladder.evaluate(now=180, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    d = ladder.evaluate(now=240, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"
    snap = d.snapshot
    # S1: last progress signal at t=0, never refreshed.
    assert snap["s1_last_fresh_ts"] == 0
    assert snap["s1_age"] == 240
    # S2: cpu never actually changed after the baseline sample at t=0, so
    # the last *nonzero-delta* sample is that baseline call, not "now".
    assert snap["s2_last_fresh_ts"] == 0
    assert snap["s2_age"] == 240
    # S3: workspace only changed once, at t=0 (the checkout).
    assert snap["s3_last_changed_ts"] == 0
    assert snap["s3_age"] == 240
    assert snap["suspect_since"] == 120
    assert snap["looping_verdict"] is None


def test_looping_kill_snapshot_has_verdict_details():
    """Finding 1b: a LOOPING kill's snapshot must merge the tripped
    RepetitionVerdict's stream/count/hashes -- the Decision carries it, the
    caller shouldn't have to separately correlate it."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))

    verdict = RepetitionVerdict(stream="exact", count=4, hashes=("deadbeef01",))
    ladder.evaluate(now=0, cpu_seconds=1.0, workspace_changed=True, rep_verdict=verdict)
    d = ladder.evaluate(now=30, cpu_seconds=2.0, workspace_changed=True, rep_verdict=verdict)

    assert d.state == LadderState.LOOPING
    assert d.kill == "looping"
    snap = d.snapshot
    assert snap["looping_verdict"] == {
        "stream": "exact",
        "count": 4,
        "hashes": ("deadbeef01",),
    }
    # Per-signal ages are still present too (this is the kill post-mortem).
    assert snap["s1_last_fresh_ts"] == 0
    assert snap["s3_last_changed_ts"] == 30  # workspace_changed=True on this final call too


def test_perpetual_waiting_defers_to_ceiling():
    """Finding 2 (ruled): continuous api_retry/waiting signals defer STUCK
    indefinitely -- "waiting on provider" is not stall. Only CEILING can
    end a task that is perpetually (re-)entering WAITING grace."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1000, started_at=0)
    _active_baseline(ladder)
    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    now = 120
    while now < 1000:
        # Each new waiting signal arrives well inside waiting_grace (200s)
        # of the previous one, so the grace window never actually expires.
        ladder.note_stream(now=now, sig=StreamSignal(kind="waiting", reason="rate_limit"))
        d = ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
        assert d.kill is None
        assert d.state == LadderState.SUSPECT
        now += 150

    # CEILING backstops it regardless -- transient classification, unlike a
    # corroborated STUCK/LOOPING kill.
    d = ladder.evaluate(now=1000, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.kill == "ceiling"


def test_stuck_precedes_ceiling_on_collision():
    """Finding 3 (ruled): when STUCK and CEILING would both fire on the same
    evaluate(), report "stuck" (permanent), not "ceiling" (transient)."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=240, started_at=0)
    _active_baseline(ladder)
    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    ladder.evaluate(now=180, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    # now - started_at == 240 == ceiling, exactly when STUCK also fires.
    d = ladder.evaluate(now=240, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"


def test_looping_precedes_ceiling_on_collision():
    """Finding 3 (ruled): same precedence for LOOPING vs CEILING."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=30, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))
    verdict = RepetitionVerdict(stream="exact", count=4, hashes=("collide0",))
    ladder.evaluate(now=0, cpu_seconds=1.0, workspace_changed=True, rep_verdict=verdict)

    # now - started_at == 30 == ceiling, exactly when LOOPING also fires.
    d = ladder.evaluate(now=30, cpu_seconds=2.0, workspace_changed=True, rep_verdict=verdict)
    assert d.state == LadderState.LOOPING
    assert d.kill == "looping"


def test_escalation_blocked_defers_stuck_kill():
    """Finding 4 (ruled, load-bearing for B7): escalation_blocked=True
    defers a would-be STUCK kill -- state stays SUSPECT, nothing latches
    _done, and a later unblocked evaluate() can still kill if conditions
    persist."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)
    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    ladder.evaluate(now=180, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    d = ladder.evaluate(
        now=240,
        cpu_seconds=5.0,
        workspace_changed=False,
        rep_verdict=None,
        escalation_blocked=True,
    )
    assert d.state == LadderState.SUSPECT
    assert d.kill is None
    assert d.transitioned is False
    assert d.deferred is True  # I3: gate suppressed a would-be STUCK kill

    d = ladder.evaluate(now=270, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"
    assert d.deferred is False  # an actual kill is never itself "deferred"


def test_escalation_blocked_defers_looping_kill():
    """Finding 4 (ruled): same deferral behavior for LOOPING."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))
    verdict = RepetitionVerdict(stream="exact", count=4, hashes=("gated000",))

    ladder.evaluate(now=0, cpu_seconds=1.0, workspace_changed=True, rep_verdict=verdict)
    d = ladder.evaluate(
        now=30,
        cpu_seconds=2.0,
        workspace_changed=True,
        rep_verdict=verdict,
        escalation_blocked=True,
    )
    assert d.state != LadderState.LOOPING
    assert d.kill is None
    assert d.deferred is True  # I3: gate suppressed a would-be LOOPING kill

    d = ladder.evaluate(now=60, cpu_seconds=3.0, workspace_changed=True, rep_verdict=verdict)
    assert d.state == LadderState.LOOPING
    assert d.kill == "looping"
    assert d.deferred is False


def test_waiting_grace_defer_is_not_reported_as_gate_deferred():
    """I3 precision: waiting_grace holding a task at SUSPECT (no fleet gate
    involved -- escalation_blocked defaults to False) must NOT set
    ``deferred``. That flag means specifically "the fleet gate suppressed a
    kill", not "any mechanism kept this at SUSPECT"."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)
    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    ladder.note_stream(now=100, sig=StreamSignal(kind="waiting", reason="rate_limit"))

    d = ladder.evaluate(now=220, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None
    assert d.deferred is False


def test_normal_stuck_wait_before_hold_is_not_deferred():
    """A SUSPECT reported before the second idle_window has even elapsed
    (the "not held" branch) is not a gate defer either -- ``escalation_blocked``
    was never consulted."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)
    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)

    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.deferred is False


def test_suspect_persistence_resets_after_rescue():
    """Finding 5 coverage gap: after a rescue back to ACTIVE, re-entering
    SUSPECT requires a fresh full idle_window -- the old suspect_since must
    not carry over and let STUCK fire early."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)
    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT  # suspect_since = 120

    d = ladder.evaluate(now=150, cpu_seconds=6.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.ACTIVE  # rescued

    for now in (180, 210):
        d = ladder.evaluate(now=now, cpu_seconds=6.0, workspace_changed=False, rep_verdict=None)
        assert d.state != LadderState.SUSPECT

    d = ladder.evaluate(now=240, cpu_seconds=6.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT  # re-entered with a *new* suspect_since = 240

    # If suspect_since had incorrectly carried over from the first episode
    # (120), STUCK would already be reachable here (240 - 120 = 120 >=
    # idle_window). It must not fire -- a fresh full window is required.
    d = ladder.evaluate(now=250, cpu_seconds=6.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None

    d = ladder.evaluate(now=340, cpu_seconds=6.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"


def test_cpu_none_process_gone_drives_to_stuck():
    """Finding 5 coverage gap: cpu_seconds=None (process tree gone) must
    drive S2 idle and, combined with stale S1/S3, all the way to STUCK."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    ladder.note_stream(now=0, sig=StreamSignal(kind="progress"))
    d = ladder.evaluate(now=0, cpu_seconds=None, workspace_changed=True, rep_verdict=None)
    assert d.state == LadderState.ACTIVE

    for now in (30, 60, 90):
        d = ladder.evaluate(now=now, cpu_seconds=None, workspace_changed=False, rep_verdict=None)
        assert d.state != LadderState.SUSPECT

    d = ladder.evaluate(now=120, cpu_seconds=None, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT

    d = ladder.evaluate(now=180, cpu_seconds=None, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT

    d = ladder.evaluate(now=240, cpu_seconds=None, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"


def test_waiting_signal_during_suspect_still_blocks_stuck():
    """Finding 5 coverage gap: a waiting signal that arrives *after* SUSPECT
    was already entered still gates the SUSPECT -> STUCK edge from that
    point on."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)
    for now in (30, 60, 90):
        ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    d = ladder.evaluate(now=120, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT

    ladder.note_stream(now=150, sig=StreamSignal(kind="waiting", reason="rate_limit"))

    # Would-be STUCK moment (120 + idle_window), but within waiting_grace
    # (150 + 200 = 350) of the late-arriving signal.
    d = ladder.evaluate(now=240, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None

    d = ladder.evaluate(now=360, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"


def test_exact_staleness_and_stuck_hold_boundaries():
    """Finding 5 coverage gap: freshness is strict "<" (age == idle_window
    already counts as stale), while the STUCK hold check is ">=" (held ==
    idle_window already fires) -- pin both boundaries exactly."""
    cfg = _cfg()
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    for now in (25, 50, 75):
        d = ladder.evaluate(now=now, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
        assert d.state != LadderState.SUSPECT

    # age == idle_window (100) exactly -> already stale.
    d = ladder.evaluate(now=100, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None

    d = ladder.evaluate(now=199, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.SUSPECT
    assert d.kill is None

    # held == idle_window (100) exactly -> STUCK fires right on the
    # boundary, not one tick past it.
    d = ladder.evaluate(now=200, cpu_seconds=5.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.STUCK
    assert d.kill == "stuck"


def test_waiting_label_expires_after_grace_without_new_waiting_signal():
    """Label-only fix: a task kept alive purely by CPU/workspace signals
    (which never clear ``_waiting_active`` -- only stream progress/output
    lines do) must stop *reporting* WAITING once ``waiting_grace`` has
    elapsed since the most recent waiting signal. The grace's effect on the
    SUSPECT -> STUCK escalation gate is unchanged (covered by the existing
    waiting-grace tests above)."""
    cfg = _cfg()  # waiting_grace=200
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)
    _active_baseline(ladder)

    ladder.note_stream(now=10, sig=StreamSignal(kind="waiting", reason="api_retry"))

    # Alive purely via CPU/workspace from here on: no stream lines at all.
    d = ladder.evaluate(now=20, cpu_seconds=6.0, workspace_changed=True, rep_verdict=None)
    assert d.state == LadderState.WAITING

    # Still within waiting_grace of the t=10 signal.
    d = ladder.evaluate(now=150, cpu_seconds=7.0, workspace_changed=True, rep_verdict=None)
    assert d.state == LadderState.WAITING

    # Grace elapsed with no new waiting signal: label reverts to ACTIVE.
    d = ladder.evaluate(now=250, cpu_seconds=8.0, workspace_changed=True, rep_verdict=None)
    assert d.state == LadderState.ACTIVE
    assert d.kill is None


def test_bootstrap_workspace_change_freshens_s3_baseline():
    """A workspace change observed during a BOOTSTRAP evaluation must be
    recorded (S3 last-fresh timestamp), so post-bootstrap S3 staleness is
    measured from that change rather than from started_at. Strictly a
    false-SUSPECT reduction: freshening S3 can only delay all-stale."""
    cfg = _cfg()  # startup_grace=100, idle_window=100
    ladder = LivenessLadder(cfg, ceiling=1_000_000, started_at=0)

    # No progress yet and grace not elapsed: still BOOTSTRAP, but the
    # workspace change is recorded, not dropped.
    d = ladder.evaluate(now=50, cpu_seconds=0.0, workspace_changed=True, rep_verdict=None)
    assert d.state == LadderState.BOOTSTRAP
    assert d.kill is None
    assert d.snapshot["s3_last_changed_ts"] == 50

    # Exit bootstrap via startup_grace: S3 age is measured from the
    # bootstrap-era change (t=50), not started_at (t=0).
    d = ladder.evaluate(now=100, cpu_seconds=0.0, workspace_changed=False, rep_verdict=None)
    assert d.snapshot["s3_last_changed_ts"] == 50
    assert d.snapshot["s3_age"] == 50
