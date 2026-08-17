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
    assert d.state != LadderState.BOOTSTRAP
    assert d.transitioned is True

    # A weak "output" signal does NOT exit bootstrap.
    ladder2 = LivenessLadder(cfg, ceiling=100_000, started_at=0)
    ladder2.note_stream(now=10, sig=StreamSignal(kind="output"))
    d = ladder2.evaluate(now=99, cpu_seconds=0.0, workspace_changed=False, rep_verdict=None)
    assert d.state == LadderState.BOOTSTRAP

    # With no progress signal at all, startup_grace elapsing exits bootstrap.
    d = ladder2.evaluate(now=100, cpu_seconds=0.0, workspace_changed=False, rep_verdict=None)
    assert d.state != LadderState.BOOTSTRAP
    assert d.transitioned is True


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
    assert all(not isinstance(v, dict) for v in snap.values())
