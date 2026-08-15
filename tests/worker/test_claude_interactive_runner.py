"""Unit tests for the interactive Claude runner."""

from __future__ import annotations

import os
import stat

from orcest.worker.claude_interactive_runner import (
    ClaudeInteractiveRunner,
    _is_interactive_usage_exhausted,
)


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


def test_usage_detector_ignores_submitted_prompt_echo() -> None:
    prompt = "Error: usage limit reached for this billing period"
    terminal = f"❯ {prompt}\r\n"

    assert _is_interactive_usage_exhausted(terminal, prompt) is False
    assert _is_interactive_usage_exhausted(terminal + "Error: quota exceeded\n", prompt) is True


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


def test_mcp_server_prompt_detector_handles_tui_output() -> None:
    runner = ClaudeInteractiveRunner()

    text = (
        "\x1b[38;5;220mNew MCP server found in this project: supabase"
        "1. Use this MCP server"
        "2. Use this and all future MCP servers in this project"
        "3. Continue without using this MCP server"
        "Enter to confirm / Esc to cancel"
    )

    assert runner._looks_like_mcp_server_prompt(text) is True


def test_mcp_server_prompt_detector_handles_plural_tui_output() -> None:
    runner = ClaudeInteractiveRunner()

    text = (
        "\x1b[38;5;220m2 new MCP servers found in this project"
        "1. Use these MCP servers"
        "2. Use these and all future MCP servers in this project"
        "3. Continue without using these MCP servers"
        "Enter to confirm / Esc to cancel"
    )

    assert runner._looks_like_mcp_server_prompt(text) is True


def test_main_input_prompt_detector_rejects_setup_menus() -> None:
    runner = ClaudeInteractiveRunner()

    assert runner._looks_like_main_input_prompt("Claude Code\n❯ ") is True
    assert runner._looks_like_main_input_prompt(
        "Quick safety check\n> 1. Yes, I trust this folder\nEnter to confirm"
    ) is False


def test_run_provides_controlling_tty_and_reads_result(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
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

if "say hello" in "\\n".join(sys.argv[1:]):
    print("PROMPT_IN_ARGV", flush=True)
    time.sleep(10)
    raise SystemExit(1)

print("❯ ", flush=True)

def read_prompt():
    buf = b""
    deadline = time.time() + 5
    while time.time() < deadline:
        readable, _, _ = select.select([0], [], [], 0.1)
        if not readable:
            continue
        chunk = os.read(0, 4096)
        if not chunk:
            break
        buf += chunk
        text = buf.decode("utf-8", errors="replace")
        if "ORCEST_WORKER_RESULT_CONTRACT" in text and ".txt" in text:
            return text
    return buf.decode("utf-8", errors="replace")

buf = read_prompt()
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
import select
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

print("❯ ", flush=True)

def read_prompt():
    buf = b""
    deadline = time.time() + 5
    while time.time() < deadline:
        readable, _, _ = select.select([0], [], [], 0.1)
        if not readable:
            continue
        chunk = os.read(0, 4096)
        if not chunk:
            break
        buf += chunk
        text = buf.decode("utf-8", errors="replace")
        if "ORCEST_WORKER_RESULT_CONTRACT" in text and ".txt" in text:
            return text
    return buf.decode("utf-8", errors="replace")

buf = read_prompt()
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


def test_run_resends_prompt_after_delayed_workspace_trust_prompt(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import sys
import time

time.sleep(3.5)
discard_deadline = time.time() + 0.3
while time.time() < discard_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if readable:
        os.read(0, 65536)

print(
    "Quick safety check: Is this a project you created or one you trust?\\n"
    "> 1. Yes, I trust this folder\\n"
    "  2. No, exit\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
confirmed = False
deadline = time.time() + 3
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 16)
    if b"\\r" in chunk or b"\\n" in chunk:
        confirmed = True
        break
if not confirmed:
    print("NO_CONFIRMATION", flush=True)
    time.sleep(10)
    raise SystemExit(1)

print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 4096)
    if not chunk:
        break
    buf += chunk
    text = buf.decode("utf-8", errors="replace")
    if "ORCEST_WORKER_RESULT_CONTRACT" in text and ".txt" in text:
        match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", text)
        if match:
            with open(match.group(1), "w", encoding="utf-8") as result:
                result.write("delayed trust prompt confirmed\\n")
            time.sleep(10)
            raise SystemExit(0)

print("NO_RESENT_PROMPT", flush=True)
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
        timeout=7,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "delayed trust prompt confirmed"


def test_run_waits_for_main_prompt_across_sequential_setup_menus(
    tmp_path, monkeypatch
) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time

print(
    "Quick safety check: Is this a project you created or one you trust?\\n"
    "> 1. Yes, I trust this folder\\n"
    "  2. No, exit\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
first = os.read(0, 16)
if b"\\r" not in first and b"\\n" not in first:
    raise SystemExit(1)

# This gap is intentionally longer than the old 0.5s fixed delay. Task text
# sent during it would be consumed as the next menu's answer.
time.sleep(1.0)
print(
    "WARNING: Claude Code running in Bypass Permissions mode\\n"
    "1. No, exit\\n"
    "2. Yes, I accept\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
second = os.read(0, 64)
if b"2" not in second or b"ORCEST" in second:
    print(f"BAD_SECOND_CONFIRMATION={second!r}", flush=True)
    raise SystemExit(1)

print("❯ ", flush=True)
buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    buf += os.read(0, 4096)
    text = buf.decode("utf-8", errors="replace")
    match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", text)
    if match:
        with open(match.group(1), "w", encoding="utf-8") as result:
            result.write("sequential setup confirmed\\n")
        time.sleep(10)
        raise SystemExit(0)
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
        timeout=5,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "sequential setup confirmed"


def test_run_confirms_bypass_permissions_prompt(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
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
if b"2" not in confirmation or (b"\\r" not in confirmation and b"\\n" not in confirmation):
    print(f"BAD_CONFIRMATION={confirmation!r}", flush=True)
    time.sleep(10)
    raise SystemExit(1)

print("❯ ", flush=True)

def read_prompt():
    buf = b""
    deadline = time.time() + 5
    while time.time() < deadline:
        readable, _, _ = select.select([0], [], [], 0.1)
        if not readable:
            continue
        chunk = os.read(0, 4096)
        if not chunk:
            break
        buf += chunk
        text = buf.decode("utf-8", errors="replace")
        if "ORCEST_WORKER_RESULT_CONTRACT" in text and ".txt" in text:
            return text
    return buf.decode("utf-8", errors="replace")

buf = read_prompt()
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


def test_run_declines_mcp_server_prompt(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import sys
import time

print(
    "New MCP server found in this project: supabase\\n"
    "1. Use this MCP server\\n"
    "2. Use this and all future MCP servers in this project\\n"
    "3. Continue without using this MCP server\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
confirmation = os.read(0, 16)
if b"3" not in confirmation or (b"\\r" not in confirmation and b"\\n" not in confirmation):
    print(f"BAD_CONFIRMATION={confirmation!r}", flush=True)
    time.sleep(10)
    raise SystemExit(1)

print("❯ ", flush=True)

def read_prompt():
    buf = b""
    deadline = time.time() + 5
    while time.time() < deadline:
        readable, _, _ = select.select([0], [], [], 0.1)
        if not readable:
            continue
        chunk = os.read(0, 4096)
        if not chunk:
            break
        buf += chunk
        text = buf.decode("utf-8", errors="replace")
        if "ORCEST_WORKER_RESULT_CONTRACT" in text and ".txt" in text:
            return text
    return buf.decode("utf-8", errors="replace")

buf = read_prompt()
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", buf)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(10)
    raise SystemExit(1)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("mcp prompt declined\\n")
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
    assert result.summary == "mcp prompt declined"


def test_run_classifies_fast_usage_limit_exit(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
print("Error: usage limit reached for this billing period", flush=True)
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

    assert result.success is False
    assert result.usage_exhausted is True
    assert result.summary == "Claude usage limit reached"


def test_run_does_not_classify_plain_terminal_text_as_usage_limit(
    tmp_path,
    monkeypatch,
) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
print("The request discusses usage and limit handling.", flush=True)
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

    assert result.success is False
    assert result.usage_exhausted is False
    assert result.transient is True


def test_run_classifies_prompt_write_timeout_as_transient(
    tmp_path,
    monkeypatch,
) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import time
print("❯ ", flush=True)
time.sleep(30)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    runner = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0)

    def fail_send_prompt(*args, **kwargs) -> None:
        raise TimeoutError("timed out writing prompt to Claude PTY")

    monkeypatch.setattr(runner, "_send_prompt", fail_send_prompt)

    result = runner.run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=3,
        credential="claude-token",
    )

    assert result.success is False
    assert result.transient is True
    assert result.summary == "Failed to write prompt: timed out writing prompt to Claude PTY"
