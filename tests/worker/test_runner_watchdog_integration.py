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


def _make_tracker_factory(fake_redis_client, workspace: Path, events: list, ceiling: float = 60.0):
    def _emit(event_type: str, data: dict) -> None:
        events.append((event_type, data))

    def _factory(root_pid: int) -> LivenessTracker:
        return LivenessTracker(
            _watchdog_cfg(),
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
sleep 1.5
echo '{"type":"assistant","message":{"content":[]}}'
exit 0
""",
    )
    events: list = []
    tracker_factory = _make_tracker_factory(fake_redis_client, work_dir, events)

    result = _run(work_dir, tmp_path, script, tracker_factory=tracker_factory)

    assert result.success is True
    assert not any(event_type == "net.orcest.task.killed" for event_type, _ in events)


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
