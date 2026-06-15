"""Unit tests for the interactive Claude runner."""

from __future__ import annotations

import os
import stat

from orcest.worker.claude_interactive_runner import ClaudeInteractiveRunner


def test_build_argv_uses_interactive_claude_without_print_flags() -> None:
    runner = ClaudeInteractiveRunner()

    argv = runner.build_argv("claude", "opus")

    assert argv == ["claude", "--dangerously-skip-permissions", "--model", "opus"]
    assert "-p" not in argv
    assert "--print" not in argv


def test_build_argv_does_not_include_model_when_empty() -> None:
    runner = ClaudeInteractiveRunner()

    argv = runner.build_argv("claude", "")

    assert argv == ["claude", "--dangerously-skip-permissions"]
    assert "-p" not in argv
    assert "--print" not in argv


def test_run_provides_controlling_tty_and_reads_result(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import sys
import time

try:
    fd = os.open("/dev/tty", os.O_RDWR)
except OSError:
    print("NO_CONTROLLING_TTY", flush=True)
    time.sleep(10)
    raise
else:
    os.close(fd)

buf = "\\n".join(sys.argv[1:])
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", buf)
if match:
    with open(match.group(1), "w", encoding="utf-8") as result:
        result.write("interactive fake ok\\n")
    time.sleep(10)
    raise SystemExit(0)

print("NO_RESULT_PATH", flush=True)
time.sleep(10)
raise SystemExit(1)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    runner = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0)

    result = runner.run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=3,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "interactive fake ok"
