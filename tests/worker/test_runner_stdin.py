"""Regression tests for stdin delivery in ``_run_cli_agent``.

CodexRunner sets ``prompt_via_stdin=True``. A blocking stdin write on the
main thread deadlocks when the child fills its stdout pipe before reading
stdin, because the stdout/stderr drains have not started yet. These tests
spawn a real subprocess so the kernel pipe buffers are in play.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import ClassVar

import pytest

from orcest.worker._runner_base import _BaseCliRunner, _run_cli_agent
from orcest.worker.runner import RunnerResult

# Larger than the typical 64 KiB pipe capacity so both directions fill.
_PIPE_FILL = 256 * 1024


class _StdinCliRunner(_BaseCliRunner):
    """Minimal runner that delivers the prompt on stdin."""

    prompt_via_stdin: ClassVar[bool] = True

    def __init__(self, script: Path, max_retries: int = 1, retry_backoff: int = 0) -> None:
        super().__init__(max_retries=max_retries, retry_backoff=retry_backoff)
        self._script = script

    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        return [sys.executable, str(self._script)]

    def extract_summary(self, stdout: str) -> str:
        lines = [line for line in stdout.splitlines() if line]
        return lines[-1] if lines else ""

    def extract_agent_text(self, stdout: str) -> str:
        return stdout

    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        return False, 0

    def detect_overload(self, stdout: str, stderr: str) -> bool:
        return False


def _run(
    work_dir: Path,
    home_dir: Path,
    script: Path,
    *,
    prompt: str,
    timeout: int,
) -> RunnerResult:
    return _run_cli_agent(
        _StdinCliRunner(script),
        prompt,
        work_dir,
        "tok",
        timeout,
        binary=sys.executable,
        env_var_name="",
        credential="",
        model="",
        home_dir=home_dir,
        logger=None,
        on_output=None,
        on_stderr=None,
        abort_event=threading.Event(),
    )


@pytest.mark.timeout(15)
def test_stdin_write_does_not_deadlock_when_child_fills_stdout_first(tmp_path: Path) -> None:
    """Child emits a large stdout preamble before reading stdin.

    The pre-fix driver wrote stdin on the main thread before starting any
    drain. A 256 KiB prompt plus a 256 KiB preamble fills both pipes and
    deadlocks permanently. With the writer on a background thread the
    main thread can drain stdout and the child unblocks.
    """
    script = tmp_path / "agent.py"
    script.write_text(
        "\n".join(
            [
                "import sys",
                f"sys.stdout.write('P' * {_PIPE_FILL} + '\\n')",
                "sys.stdout.flush()",
                "prompt = sys.stdin.read()",
                "sys.stdout.write('GOT ' + str(len(prompt)) + '\\n')",
                "sys.stdout.flush()",
            ]
        )
        + "\n"
    )
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    prompt = "Q" * _PIPE_FILL

    result = _run(work_dir, tmp_path, script, prompt=prompt, timeout=10)

    assert result.success is True
    assert result.summary == f"GOT {_PIPE_FILL}"


@pytest.mark.timeout(15)
def test_timeout_joins_stdin_writer_when_child_never_reads_stdin(tmp_path: Path) -> None:
    """A hung child that never reads stdin must still time out.

    The stdin writer is blocked on the full pipe. Killing the child unblocks
    it; the driver joins that thread before returning the timeout result.
    """
    script = tmp_path / "agent.py"
    script.write_text(
        "\n".join(
            [
                "import sys, time",
                f"sys.stdout.write('P' * {_PIPE_FILL} + '\\n')",
                "sys.stdout.flush()",
                "time.sleep(1000)",
            ]
        )
        + "\n"
    )
    work_dir = tmp_path / "wd"
    work_dir.mkdir()
    prompt = "Q" * _PIPE_FILL

    result = _run(work_dir, tmp_path, script, prompt=prompt, timeout=2)

    assert result.success is False
    assert "Timed out" in result.summary
    assert result.transient is True
