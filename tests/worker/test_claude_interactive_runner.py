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


def test_workspace_trust_prompt_detector_handles_tui_output() -> None:
    runner = ClaudeInteractiveRunner()

    text = (
        "\x1b[?25lAccessingworkspace:/tmp/repo"
        "Quicksafetycheck:Isthisaprojectyoucreatedoroneyoutrust?"
        "1.Yes,Itrustthisfolder2.No,exitEntertoconfirm/Esctocancel"
    )

    assert runner._looks_like_workspace_trust_prompt(text) is True


def test_bypass_permissions_prompt_detector_handles_tui_output() -> None:
    runner = ClaudeInteractiveRunner()

    text = (
        "\x1b[38;5;211mWARNING: Claude Code running in Bypass Permissions mode"
        "In Bypass Permissions mode, Claude Codewillnotaskforyourapproval"
        "1. No, exit 2. Yes, I accept Enter to confirm / Esc to cancel"
    )

    assert runner._looks_like_bypass_permissions_prompt(text) is True


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


def test_run_confirms_workspace_trust_prompt(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import sys
import time

print(
    "Quick safety check: Is this a project you created or one you trust?\\n"
    "> 1. Yes, I trust this folder\\n"
    "  2. No, exit\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
confirmation = os.read(0, 16)
if b"\\r" not in confirmation and b"\\n" not in confirmation:
    print("NO_CONFIRMATION", flush=True)
    time.sleep(10)
    raise SystemExit(1)

buf = "\\n".join(sys.argv[1:])
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", buf)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(10)
    raise SystemExit(1)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("trust prompt confirmed\\n")
time.sleep(10)
raise SystemExit(0)
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
    assert result.summary == "trust prompt confirmed"


def test_run_confirms_bypass_permissions_prompt(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import sys
import time

print(
    "WARNING: Claude Code running in Bypass Permissions mode\\n"
    "1. No, exit\\n"
    "2. Yes, I accept\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
confirmation = os.read(0, 16)
if b"\\x1b[B" not in confirmation or (b"\\r" not in confirmation and b"\\n" not in confirmation):
    print(f"BAD_CONFIRMATION={confirmation!r}", flush=True)
    time.sleep(10)
    raise SystemExit(1)

buf = "\\n".join(sys.argv[1:])
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", buf)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(10)
    raise SystemExit(1)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("bypass prompt confirmed\\n")
time.sleep(10)
raise SystemExit(0)
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
    assert result.summary == "bypass prompt confirmed"
