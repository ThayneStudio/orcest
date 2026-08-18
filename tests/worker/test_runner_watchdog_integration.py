"""Integration tests for task B8: the activity watchdog wired into the
generic ``_run_cli_agent`` driver.

Unlike the rest of ``tests/worker/``, these spawn *real* subprocesses (fake
provider shell scripts) rather than mocking ``subprocess.Popen`` -- the
watchdog thread's ladder-driven kill decisions and the 2s post-kill
verify-death wait are genuinely timing-dependent, so a mock can't exercise
them. Marked ``integration`` per conftest's inline-marker allowance (a
real-subprocess test living alongside unit tests in tests/worker/ keeps its
explicit marker rather than being swept into ``unit``).

The tiny ``WatchdogConfig`` (sample_interval=0.2, startup_grace=0.5,
idle_window=1.0, waiting_grace=2.0) and ceiling=60 come straight from the
task-B8 brief's five-scenario contract; the tests exercise ``_run_cli_agent``
directly (not through ``Runner.run()``) since that's the function under
test and the thing the brief's Interfaces section specifies.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from orcest.shared.config import WatchdogConfig
from orcest.worker._runner_base import _BaseCliRunner, _run_cli_agent
from orcest.worker.liveness_tracker import LivenessTracker


class _FakeCliRunner(_BaseCliRunner):
    """Minimal concrete _BaseCliRunner: argv is just the fake script path,
    the prompt is ignored (the scripts don't read stdin or argv)."""

    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        return [binary]

    def extract_summary(self, stdout: str) -> str:
        return "ok"

    def extract_agent_text(self, stdout: str) -> str:
        return stdout

    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        return False, 0

    def detect_overload(self, stdout: str, stderr: str) -> bool:
        return False


def _write_script(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\n{body}\n")
    path.chmod(0o755)
    return path


def _watchdog_cfg(**overrides: object) -> WatchdogConfig:
    defaults: dict[str, object] = dict(
        sample_interval=0.2,
        startup_grace=0.5,
        idle_window=1.0,
        waiting_grace=2.0,
        loop_exact_threshold=4,
        loop_error_threshold=3,
        loop_pingpong_threshold=6,
    )
    defaults.update(overrides)
    return WatchdogConfig(**defaults)  # type: ignore[arg-type]


def _run(work_dir: Path, home_dir: Path, script: Path, *, timeout: int = 60, tracker_factory=None):
    return _run_cli_agent(
        _FakeCliRunner(max_retries=1, retry_backoff=0),
        "do work",
        work_dir,
        "tok",
        timeout,
        binary=str(script),
        env_var_name="",
        credential="",
        model="",
        home_dir=home_dir,
        logger=None,
        on_output=None,
        on_stderr=None,
        abort_event=threading.Event(),
        tracker_factory=tracker_factory,
    )


def _make_tracker_factory(
    fake_redis_client,
    workspace: Path,
    events: list,
    ceiling: float = 60.0,
    cfg: WatchdogConfig | None = None,
    kill_budget_limit: int | None = 100,
):
    # C1b: LivenessTracker's kill-budget default is now fail-closed (an
    # absent/unreadable orcest:fleet:kill_budget:limit mirror means kills
    # disabled). In production FleetHealthMonitor keeps this mirror fresh;
    # here we mirror that by default so these STUCK/LOOPING-kill tests keep
    # exercising the ladder's actual kill path rather than getting gated by
    # the safe-by-default fallback. Pass kill_budget_limit=None to opt out
    # (e.g. to exercise observation-mode-like behavior).
    if kill_budget_limit is not None:
        fake_redis_client.set_ex_raw(
            "orcest:fleet:kill_budget:limit", str(kill_budget_limit), 3600
        )

    def _emit(event_type: str, data: dict) -> None:
        events.append((event_type, data))

    def _factory(root_pid: int) -> LivenessTracker:
        return LivenessTracker(
            cfg or _watchdog_cfg(),
            ceiling,
            redis=fake_redis_client,
            emit=_emit,
            worker_id="test-worker",
            task_id="test-task",
            root_pid=root_pid,
            workspace=workspace,
        )

    return _factory


@pytest.mark.integration
def test_productive_slow_task_survives_past_idle_window(tmp_path, fake_redis_client):
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    script = _write_script(
        tmp_path / "agent.sh",
        """
i=0
while [ $i -lt 10 ]; do
  echo '{"type":"assistant","message":{"content":[]}}'
  sleep 0.5
  i=$((i+1))
done
exit 0
""",
    )
    events: list = []
    tracker_factory = _make_tracker_factory(fake_redis_client, work_dir, events)

    result = _run(work_dir, tmp_path, script, tracker_factory=tracker_factory)

    assert result.success is True
    assert not any(event_type == "net.orcest.task.killed" for event_type, _ in events)


@pytest.mark.integration
def test_silent_hang_killed_as_stuck_with_snapshot(tmp_path, fake_redis_client):
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    script = _write_script(
        tmp_path / "agent.sh",
        """
echo '{"type":"assistant","message":{"content":[]}}'
sleep 600
""",
    )
    events: list = []
    tracker_factory = _make_tracker_factory(fake_redis_client, work_dir, events)

    result = _run(work_dir, tmp_path, script, tracker_factory=tracker_factory)

    assert result.success is False
    assert result.transient is False
    assert result.summary.startswith("STALLED(stuck)")

    event_types = [event_type for event_type, _ in events]
    assert "net.orcest.task.suspect" in event_types
    assert "net.orcest.task.stuck" in event_types
    assert "net.orcest.task.killed" in event_types
    assert (
        event_types.index("net.orcest.task.suspect")
        < event_types.index("net.orcest.task.stuck")
        < event_types.index("net.orcest.task.killed")
    )
    killed_data = next(
        data for event_type, data in events if event_type == "net.orcest.task.killed"
    )
    assert killed_data == {"trigger": "stuck", "verified": True}


@pytest.mark.integration
def test_loop_killed_as_looping(tmp_path, fake_redis_client):
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    script = _write_script(
        tmp_path / "agent.sh",
        """
while true; do
  echo '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}}'
  sleep 0.3
done
""",
    )
    events: list = []
    tracker_factory = _make_tracker_factory(fake_redis_client, work_dir, events)

    result = _run(work_dir, tmp_path, script, tracker_factory=tracker_factory)

    assert result.success is False
    assert result.transient is False
    assert result.summary.startswith("STALLED(looping)")
    killed_data = next(
        (data for event_type, data in events if event_type == "net.orcest.task.killed"), None
    )
    assert killed_data == {"trigger": "looping", "verified": True}


@pytest.mark.integration
def test_waiting_script_not_killed_within_grace(tmp_path, fake_redis_client):
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    script = _write_script(
        tmp_path / "agent.sh",
        """
echo '{"type":"system","subtype":"api_retry"}'
sleep 2.0
echo '{"type":"assistant","message":{"content":[]}}'
exit 0
""",
    )
    events: list = []
    # waiting_grace widened to 4.0 (vs the shared 2.0 default) so the
    # sleep-2.0s script has a comfortable scheduling margin under load
    # instead of the original ~0.5s (review round 1, test robustness fix).
    tracker_factory = _make_tracker_factory(
        fake_redis_client, work_dir, events, cfg=_watchdog_cfg(waiting_grace=4.0)
    )

    result = _run(work_dir, tmp_path, script, tracker_factory=tracker_factory)

    assert result.success is True
    assert not any(event_type == "net.orcest.task.killed" for event_type, _ in events)


class _ExhaustionNoisyRunner(_FakeCliRunner):
    """detect_exhaustion always fires — simulates rate-limit noise present in
    the partial output of a run the activity ladder killed."""

    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        return True, 1778302800


@pytest.mark.integration
def test_stalled_kill_never_reclassified_by_exhaustion_noise(tmp_path, fake_redis_client):
    """Task B9 controller ruling: a corroborated stall (stuck/looping ladder
    kill) is returned as STALLED even when the exhaustion scan would fire on
    the partial output — it must NOT be converted to usage_exhausted (or a
    transient timeout). Guards the STALLED-before-exhaustion ordering in
    ``_run_cli_agent``; restoring the pre-B9 ordering would fail this test
    while passing every other one."""
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    script = _write_script(
        tmp_path / "agent.sh",
        """
echo '{"type":"assistant","message":{"content":[]}}'
sleep 600
""",
    )
    events: list = []
    tracker_factory = _make_tracker_factory(fake_redis_client, work_dir, events)

    result = _run_cli_agent(
        _ExhaustionNoisyRunner(max_retries=1, retry_backoff=0),
        "do work",
        work_dir,
        "tok",
        60,
        binary=str(script),
        env_var_name="",
        credential="",
        model="",
        home_dir=tmp_path,
        logger=None,
        on_output=None,
        on_stderr=None,
        abort_event=threading.Event(),
        tracker_factory=tracker_factory,
    )

    assert result.success is False
    assert result.summary.startswith("STALLED(")
    assert result.usage_exhausted is False
    assert result.transient is False
    killed_data = next(
        (data for event_type, data in events if event_type == "net.orcest.task.killed"), None
    )
    assert killed_data is not None and killed_data["trigger"] in ("stuck", "looping")


class _RaisingTickTracker(LivenessTracker):
    """I2 regression fixture: a tracker whose tick() always raises, to
    prove the watchdog thread falls back to an inline wall-clock ceiling
    check rather than silently losing kill protection for the rest of the
    attempt."""

    def tick(self) -> str | None:  # type: ignore[override]
        raise RuntimeError("tracker.tick() exploded")


@pytest.mark.integration
def test_tick_exception_falls_back_to_wall_clock_ceiling_kill(tmp_path, fake_redis_client):
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    script = _write_script(tmp_path / "agent.sh", "sleep 30\n")

    events: list = []

    def _emit(event_type: str, data: dict) -> None:
        events.append((event_type, data))

    def _factory(root_pid: int) -> LivenessTracker:
        return _RaisingTickTracker(
            _watchdog_cfg(),
            60.0,
            redis=fake_redis_client,
            emit=_emit,
            worker_id="test-worker",
            task_id="test-task",
            root_pid=root_pid,
            workspace=work_dir,
        )

    start = time.monotonic()
    # Small runner timeout: the ladder can never kill (tick() always
    # raises), so only the inline wall-clock fallback can end this attempt.
    result = _run(work_dir, tmp_path, script, timeout=2, tracker_factory=_factory)
    elapsed = time.monotonic() - start

    # The ladder never got a chance to fire (tick() always raises); the
    # inline wall-clock fallback treats this exactly like the no-tracker
    # ceiling path: "ceiling" -> a generic timed-out result, not a
    # STALLED(stuck/looping) verdict.
    assert result.success is False
    assert result.transient is True
    assert result.summary == "Timed out after 2s"
    # Killed promptly at ~2s, not left to run the full 30s sleep.
    assert elapsed < 15


@pytest.mark.integration
def test_disabled_watchdog_preserves_wall_clock_timeout(tmp_path):
    script = _write_script(tmp_path / "agent.sh", "sleep 30\n")

    start = time.monotonic()
    # tracker_factory omitted -> None: today's fixed wall-clock watchdog,
    # byte-for-byte (the rollback lever per global-constraints.md).
    result = _run(tmp_path, tmp_path, script, timeout=1)
    elapsed = time.monotonic() - start

    assert result.success is False
    assert result.summary == "Timed out after 1s"
    assert result.transient is True
    # Killed promptly at ~1s, not left to run the full 30s sleep.
    assert elapsed < 10
