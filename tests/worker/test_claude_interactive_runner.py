"""Unit tests for the interactive Claude runner."""

from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path

from orcest.shared.provider_versions import PROVIDER_CLI_DESIRED_VERSIONS
from orcest.worker.claude_interactive_runner import (
    _SUBMISSION_ATTEMPT_LIMIT,
    ClaudeInteractiveRunner,
    _is_interactive_usage_exhausted,
    _PostSubmitState,
    _PtyOutputDecoder,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _interactive_frames() -> dict:
    return json.loads((FIXTURES / "claude_interactive_2.1.235_frames.json").read_text())


def _record_submit_keystrokes(monkeypatch) -> list[int]:
    calls: list[int] = []
    original_submit = ClaudeInteractiveRunner._send_submit_keystroke

    def counting_submit(self, *args, **kwargs):
        calls.append(1)
        return original_submit(self, *args, **kwargs)

    monkeypatch.setattr(ClaudeInteractiveRunner, "_send_submit_keystroke", counting_submit)
    return calls


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


def test_pty_decoder_preserves_utf8_codepoint_split_across_reads(monkeypatch) -> None:
    runner = ClaudeInteractiveRunner()
    decoder = _PtyOutputDecoder()
    terminal_output: list[str] = []
    callback_output: list[str] = []
    encoded = "❯ ".encode("utf-8")
    chunks = iter([encoded[:1], encoded[1:2], encoded[2:]])

    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner.select.select",
        lambda *args, **kwargs: ([123], [], []),
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner.os.read",
        lambda *args, **kwargs: next(chunks),
    )

    for _ in range(3):
        assert runner._read_available(
            123,
            terminal_output,
            callback_output.append,
            None,
            decoder,
        )

    runner._finish_output_decoder(
        decoder,
        terminal_output,
        callback_output.append,
        None,
    )
    assert "".join(terminal_output) == "❯ "
    assert "".join(callback_output) == "❯ "
    assert "�" not in "".join(terminal_output)


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
    assert (
        runner._looks_like_main_input_prompt(
            "Quick safety check\n> 1. Yes, I trust this folder\nEnter to confirm"
        )
        is False
    )


def test_main_input_prompt_detector_tolerates_box_drawing_borders() -> None:
    """The composer may be framed inside a box border ('│ ❯ …'); the detector
    must still recognize it or every attempt burns the full runner timeout."""
    runner = ClaudeInteractiveRunner()

    assert runner._looks_like_main_input_prompt('│ ❯ Try "edit <filepath> to..."') is True
    assert runner._looks_like_main_input_prompt('┃ ❯ Try "fix the failing test"') is True
    assert runner._looks_like_main_input_prompt("║ ❯ ") is True
    assert runner._looks_like_main_input_prompt('| ❯ Try "help"') is True
    # A bordered line without the composer glyph is still not an input prompt.
    assert runner._looks_like_main_input_prompt("│ Welcome to Claude Code │") is False
    # Excluded setup dialogs stay excluded even with borders present.
    assert (
        runner._looks_like_main_input_prompt(
            "│ Quick safety check\n│ > 1. Yes, I trust this folder\n│ Enter to confirm"
        )
        is False
    )


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


def test_run_waits_for_main_prompt_across_sequential_setup_menus(tmp_path, monkeypatch) -> None:
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


def test_run_classifies_usage_limit_while_waiting_for_startup_prompt(
    tmp_path,
    monkeypatch,
) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import time
print("Error: usage limit reached for this billing period", flush=True)
time.sleep(30)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    runner = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0)

    started = time.monotonic()
    result = runner.run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=5,
        credential="claude-token",
    )

    assert time.monotonic() - started < 3
    assert result.success is False
    assert result.usage_exhausted is True
    assert result.summary == "Claude usage limit reached"


def test_startup_wait_consumes_the_attempt_timeout(tmp_path, monkeypatch) -> None:
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import time
time.sleep(30)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    runner = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0)

    started = time.monotonic()
    result = runner.run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=1,
        credential="claude-token",
    )

    assert time.monotonic() - started < 3
    assert result.success is False
    assert result.transient is True
    assert result.summary == "Timed out waiting for interactive Claude input prompt"


def test_prompt_write_uses_only_remaining_attempt_budget(monkeypatch) -> None:
    runner = ClaudeInteractiveRunner()
    captured: dict[str, float] = {}

    def capture_write(
        fd: int,
        data: bytes,
        abort_event=None,
        timeout: float = 10.0,
    ) -> None:
        del fd, data, abort_event
        captured["timeout"] = timeout

    monkeypatch.setattr(runner, "_write_all", capture_write)

    runner._send_prompt(123, "prompt", None, timeout=0.25)

    assert captured["timeout"] == 0.25


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

    captured: dict[str, float] = {}

    def fail_send_prompt(*args, **kwargs) -> None:
        captured["timeout"] = kwargs["timeout"]
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
    assert 0 < captured["timeout"] <= 3


def test_mcp_server_prompt_detector_survives_cursor_positioning_gaps() -> None:
    """Real TUI output repositions the cursor mid-word, so a literal option
    string can lose a character ('...MCP servr').  Captured from a production
    trace where the dropped 'e' made the detector miss the menu, which then
    satisfied the main-composer gate and burned the whole runner timeout.
    """
    runner = ClaudeInteractiveRunner()

    text = (
        "\x1b[38;5;220mNew MCP server found in this project: supabase"
        "\x1b[38;5;153m❯\x1b[39m \x1b[38;5;246m1. \x1b[38;5;153mUse this MCP server\x1b[39m"
        "\x1b[38;5;246m2. \x1b[39mUse this and all future MCP servers in this project"
        "   \x1b[38;5;246m3. \x1b[39mContinue without using this MCP serv\x1b[45Gr"
        "\x1b[38;5;246m\x1b[3mEnter to confirm \xb7 Esc to cancel"
    )

    assert runner._looks_like_mcp_server_prompt(text) is True
    # And it must never be mistaken for the composer.
    assert runner._looks_like_main_input_prompt(text) is False


def test_main_input_prompt_detector_rejects_caret_selected_numbered_menu() -> None:
    """Any selection dialog renders numbered options with an 'Enter to confirm'
    footer and marks the selected row with the same glyph as the composer.
    The gate must reject that shape generically, without enumerating dialogs.
    """
    runner = ClaudeInteractiveRunner()

    assert (
        runner._looks_like_main_input_prompt(
            "Some future dialog we have never seen\n"
            "❯ 1. Do the thing\n"
            "  2. Do not do the thing\n"
            "Enter to confirm \xb7 Esc to cancel"
        )
        is False
    )
    # The real composer still passes.
    assert runner._looks_like_main_input_prompt('│ ❯ Try "fix the failing test"') is True


def test_run_confirms_bypass_prompt_appearing_after_prompt_sent(tmp_path, monkeypatch) -> None:
    """Production sequence: trust -> MCP menu -> bypass warning.  The bypass
    dialog can appear after the task prompt has been sent; if the runner stops
    answering setup dialogs at that point the session hangs until the wall
    clock kills it (observed as ~27% of tasks dying at 'Timed out after 5400s').
    """
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import sys
import time

def read_confirmation():
    deadline = time.time() + 5
    buf = b""
    while time.time() < deadline:
        readable, _, _ = select.select([0], [], [], 0.1)
        if not readable:
            continue
        chunk = os.read(0, 4096)
        if not chunk:
            break
        buf += chunk
        if b"\\r" in buf or b"\\n" in buf:
            return buf
    return buf

print(
    "Quick safety check: Is this a project you created or one you trust?\\n"
    "1. Yes, I trust this folder\\n"
    "2. No, exit\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
read_confirmation()

# Decline option loses a character to a cursor-position escape, exactly as
# captured in the production trace.
print(
    "New MCP server found in this project: supabase\\n"
    "\\u276f 1. Use this MCP server\\n"
    "2. Use this and all future MCP servers in this project\\n"
    "3. Continue without using this MCP serv\\x1b[45Gr\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
mcp = read_confirmation()
if b"3" not in mcp:
    print(f"BAD_MCP_CONFIRMATION={mcp!r}", flush=True)
    time.sleep(10)
    raise SystemExit(1)

print("\\u276f ", flush=True)

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
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

# Drain the rest of the pasted prompt so the confirmation read below cannot
# swallow prompt bytes; the real CLI is idle when it raises this dialog.
while True:
    readable, _, _ = select.select([0], [], [], 0.5)
    if not readable:
        break
    extra = os.read(0, 4096)
    if not extra:
        break
    buf += extra

# The bypass warning arrives only after the task prompt was submitted.
print(
    "WARNING: Claude Code running in Bypass Permissions mode\\n"
    "1. No, exit\\n"
    "2. Yes, I accept\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
confirmation = read_confirmation()
if b"2" not in confirmation:
    print(f"BAD_BYPASS_CONFIRMATION={confirmation!r}", flush=True)
    time.sleep(30)
    raise SystemExit(1)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(30)
    raise SystemExit(1)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("late bypass prompt confirmed\\n")
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
        timeout=15,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "late bypass prompt confirmed"


def test_looks_like_activity_state_detects_esc_to_interrupt() -> None:
    runner = ClaudeInteractiveRunner()

    assert runner._looks_like_activity_state("Cerebrating... (esc to interrupt)") is True
    assert runner._looks_like_activity_state("❯ ") is False


def test_pinned_claude_frames_document_post_submit_states() -> None:
    """Keep accepted evidence tied to the deployed interactive TUI version."""
    capture = _interactive_frames()
    frames = capture["frames"]
    runner = ClaudeInteractiveRunner()

    assert capture["cli_version"] == PROVIDER_CLI_DESIRED_VERSIONS["claude"]
    assert capture["accepted_execution_signals"] == {
        "activity": "esc to interrupt",
        "composer_cleared": ("latest settled non-menu composer has no pasted-text placeholder"),
    }
    assert (
        runner._classify_post_submit_state(frames["explicitly_stuck"])
        is _PostSubmitState.EXPLICITLY_STUCK
    )
    assert (
        runner._classify_post_submit_state(frames["explicitly_stuck_with_footer"])
        is _PostSubmitState.EXPLICITLY_STUCK
    )
    assert (
        runner._classify_post_submit_state(frames["stuck_then_cleared"])
        is _PostSubmitState.EXECUTING
    )
    assert (
        runner._classify_post_submit_state(frames["stuck_then_cleared_with_footer"])
        is _PostSubmitState.EXECUTING
    )
    assert (
        runner._classify_post_submit_state(frames["stuck_then_unrecognized"])
        is _PostSubmitState.AMBIGUOUS
    )
    assert runner._classify_post_submit_state(frames["executing"]) is _PostSubmitState.EXECUTING
    assert runner._classify_post_submit_state(frames["unknown_menu"]) is _PostSubmitState.AMBIGUOUS
    assert runner._classify_post_submit_state(frames["unrecognized"]) is _PostSubmitState.AMBIGUOUS


def test_looks_like_pending_paste_composer_detects_placeholder() -> None:
    runner = ClaudeInteractiveRunner()

    assert runner._looks_like_pending_paste_composer("❯ [Pasted text #1 +12 lines]") is True
    assert runner._looks_like_pending_paste_composer("│ ❯ [Pasted text #2 +3 lines]") is True
    assert runner._looks_like_pending_paste_composer("❯ ") is False


def test_run_confirms_submission_on_first_enter_and_sends_exactly_one_enter(
    tmp_path, monkeypatch
) -> None:
    """A composer that accepts the initial Enter must submit exactly once."""
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

# A real interactive TUI puts the tty in raw mode immediately, disabling
# input echo and CR/NL translation. Match that so the fixture behaves like
# the real Claude Code composer instead of a canonical-mode shell.
tty.setraw(0)

print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

# Drain any trailing submission-keystroke bytes still in flight.
drain_deadline = time.time() + 0.3
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if not readable:
        break
    os.read(0, 65536)

# The composer clears and Claude starts working immediately.
print("Cerebrating... (esc to interrupt)", flush=True)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(10)
    raise SystemExit(1)
time.sleep(0.5)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("submitted on first enter\\n")
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

    submit_calls: list[int] = []
    original_submit = ClaudeInteractiveRunner._send_submit_keystroke

    def counting_submit(self, *args, **kwargs):
        submit_calls.append(1)
        return original_submit(self, *args, **kwargs)

    monkeypatch.setattr(ClaudeInteractiveRunner, "_send_submit_keystroke", counting_submit)

    result = runner.run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=8,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "submitted on first enter"
    assert len(submit_calls) == 1


def test_run_recovers_stuck_placeholder_with_later_enter(tmp_path, monkeypatch) -> None:
    """A composer that keeps the pasted placeholder after the first Enter must
    be recovered by a bounded, Enter-only retry -- never a repaste.
    """
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._SUBMISSION_RETRY_SETTLE_SECONDS",
        0.5,
    )
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

# A real interactive TUI puts the tty in raw mode immediately, disabling
# input echo and CR/NL translation. Match that so the fixture behaves like
# the real Claude Code composer instead of a canonical-mode shell.
tty.setraw(0)

print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

# Drain any bytes from the initial submission still in flight (e.g. its
# trailing Enter) so they cannot be mistaken for the later retry keystroke.
drain_deadline = time.time() + 0.5
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        break
    os.read(0, 65536)

# The composer stays stuck on the pasted placeholder: the first Enter did
# not land.
print("❯ [Pasted text #1 +12 lines]", flush=True)
print("? for shortcuts  Context left until auto-compact: 87%", flush=True)

retry = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    retry += chunk
    if b"\\r" in retry:
        break

if retry.strip(b"\\r") != b"":
    print(f"UNEXPECTED_RETRY_BYTES={retry!r}", flush=True)
    time.sleep(10)
    raise SystemExit(1)

print("❯ ", flush=True)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(10)
    raise SystemExit(1)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("recovered by later enter\\n")
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
        timeout=10,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "recovered by later enter"


def test_run_requires_fresh_stuck_evidence_before_each_retry(tmp_path, monkeypatch) -> None:
    """One placeholder authorizes one retry; later silence authorizes nothing."""
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._PRE_EXECUTION_CONFIRM_DEADLINE_SECONDS",
        0.5,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._SUBMISSION_RETRY_SETTLE_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._COMPOSER_SETTLE_TICKS",
        1,
    )
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

tty.setraw(0)
print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

drain_deadline = time.time() + 0.3
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if not readable:
        break
    os.read(0, 65536)

print("❯ [Pasted text #1 +12 lines]", flush=True)
retry = b""
deadline = time.time() + 5
while time.time() < deadline and b"\\r" not in retry:
    readable, _, _ = select.select([0], [], [], 0.1)
    if readable:
        retry += os.read(0, 65536)
if b"\\r" not in retry or retry.strip(b"\\r"):
    raise SystemExit(2)

# After that evidence-backed retry, render nothing. A historical placeholder
# must not authorize another Enter.
unexpected = b""
deadline = time.time() + 1.2
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if readable:
        unexpected += os.read(0, 65536)
if unexpected:
    print(f"UNEXPECTED_AMBIGUOUS_INPUT={unexpected!r}", flush=True)
    raise SystemExit(3)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    raise SystemExit(4)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("one evidence frame authorized one retry\\n")
time.sleep(10)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    submit_calls = _record_submit_keystrokes(monkeypatch)
    result = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0).run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=6,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "one evidence frame authorized one retry"
    assert len(submit_calls) == 2


def test_run_fresh_stuck_epoch_after_ambiguity_gets_new_recovery_window(
    tmp_path, monkeypatch
) -> None:
    """An expired earlier stuck window cannot kill a later fresh stuck epoch."""
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._PRE_EXECUTION_CONFIRM_DEADLINE_SECONDS",
        0.75,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._SUBMISSION_RETRY_SETTLE_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._COMPOSER_SETTLE_TICKS",
        1,
    )
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

tty.setraw(0)
print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

drain_deadline = time.time() + 0.3
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if not readable:
        break
    os.read(0, 65536)

def read_enter():
    received = b""
    deadline = time.time() + 3
    while time.time() < deadline and b"\\r" not in received:
        readable, _, _ = select.select([0], [], [], 0.1)
        if readable:
            received += os.read(0, 65536)
    return received

print("❯ [Pasted text #1 +12 lines]", flush=True)
first_retry = read_enter()
if b"\\r" not in first_retry or first_retry.strip(b"\\r"):
    raise SystemExit(2)

# Leave the explicit-stuck state and remain ambiguous past its old short
# deadline. No recovery input is safe during this interval.
print("Working…", flush=True)
unexpected = b""
deadline = time.time() + 1.0
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if readable:
        unexpected += os.read(0, 65536)
if unexpected:
    print(f"UNEXPECTED_AMBIGUOUS_INPUT={unexpected!r}", flush=True)
    raise SystemExit(3)

# A newly rendered placeholder is a new explicit-stuck epoch. It receives a
# fresh short deadline and can authorize the final budgeted Enter.
print("❯ [Pasted text #2 +12 lines]", flush=True)
second_retry = read_enter()
if b"\\r" not in second_retry or second_retry.strip(b"\\r"):
    raise SystemExit(4)
print("\\x1b[2K\\r│ ❯ ", flush=True)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    raise SystemExit(5)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("fresh stuck epoch recovered after ambiguity\\n")
time.sleep(10)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    submit_calls = _record_submit_keystrokes(monkeypatch)
    result = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0).run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=6,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "fresh stuck epoch recovered after ambiguity"
    assert len(submit_calls) == _SUBMISSION_ATTEMPT_LIMIT == 3


def test_run_stuck_frame_cleared_before_settle_sends_no_retry(tmp_path, monkeypatch) -> None:
    """The latest repaint wins over a historical placeholder frame."""
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

tty.setraw(0)
print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

drain_deadline = time.time() + 0.3
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if not readable:
        break
    os.read(0, 65536)

print("❯ [Pasted text #1 +12 lines]", flush=True)
time.sleep(0.2)
print("\\x1b[2K\\r│ ❯ ", flush=True)

unexpected = b""
deadline = time.time() + 1.2
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if readable:
        unexpected += os.read(0, 65536)
if unexpected:
    print(f"UNEXPECTED_CLEARED_COMPOSER_INPUT={unexpected!r}", flush=True)
    raise SystemExit(2)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    raise SystemExit(3)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("latest composer frame was clear\\n")
time.sleep(10)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    submit_calls = _record_submit_keystrokes(monkeypatch)
    result = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0).run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=6,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "latest composer frame was clear"
    assert len(submit_calls) == 1


def test_run_stuck_frame_followed_by_unrecognized_output_sends_no_retry(
    tmp_path, monkeypatch
) -> None:
    """Newer unrecognized output makes an old stuck composer ambiguous."""
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._SUBMISSION_RETRY_SETTLE_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._COMPOSER_SETTLE_TICKS",
        1,
    )
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

tty.setraw(0)
print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

drain_deadline = time.time() + 0.3
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if not readable:
        break
    os.read(0, 65536)

print("❯ [Pasted text #1 +12 lines]", flush=True)
print("Working…", flush=True)

unexpected = b""
deadline = time.time() + 1.2
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if readable:
        unexpected += os.read(0, 65536)
if unexpected:
    print(f"UNEXPECTED_UNRECOGNIZED_INPUT={unexpected!r}", flush=True)
    raise SystemExit(2)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    raise SystemExit(3)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("newer output made the old placeholder ambiguous\\n")
time.sleep(10)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    submit_calls = _record_submit_keystrokes(monkeypatch)
    result = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0).run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=6,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "newer output made the old placeholder ambiguous"
    assert len(submit_calls) == 1


def test_run_unknown_post_submit_menu_receives_no_bare_enter(tmp_path, monkeypatch) -> None:
    """An unrecognized menu is ambiguous, never a submission-retry target."""
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._SUBMISSION_RETRY_SETTLE_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._COMPOSER_SETTLE_TICKS",
        1,
    )
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

tty.setraw(0)
print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

drain_deadline = time.time() + 0.3
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if not readable:
        break
    os.read(0, 65536)

print(
    "A future prompt changed in a new CLI release\\n"
    "❯ 1. Accept the default\\n"
    "  2. Cancel\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)

unexpected = b""
deadline = time.time() + 1.2
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if readable:
        unexpected += os.read(0, 65536)
if unexpected:
    print(f"UNEXPECTED_MENU_INPUT={unexpected!r}", flush=True)
    raise SystemExit(2)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    raise SystemExit(3)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("unknown menu left untouched\\n")
time.sleep(10)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    submit_calls = _record_submit_keystrokes(monkeypatch)
    result = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0).run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=6,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "unknown menu left untouched"
    assert len(submit_calls) == 1


def test_run_permanently_stuck_composer_fails_fast(tmp_path, monkeypatch) -> None:
    """Fresh stuck evidence spends the documented total-attempt budget."""
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._PRE_EXECUTION_CONFIRM_DEADLINE_SECONDS",
        1.5,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._SUBMISSION_RETRY_SETTLE_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._COMPOSER_SETTLE_TICKS",
        1,
    )
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import select
import time
import tty

# A real interactive TUI puts the tty in raw mode immediately, disabling
# input echo and CR/NL translation. Match that so the fixture behaves like
# the real Claude Code composer instead of a canonical-mode shell.
tty.setraw(0)

print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

# Drain the initial Enter, which is written separately from the paste.
drain_deadline = time.time() + 0.3
while time.time() < drain_deadline:
    readable, _, _ = select.select([0], [], [], 0.05)
    if not readable:
        break
    os.read(0, 65536)

def read_enter():
    deadline = time.time() + 5
    received = b""
    while time.time() < deadline:
        readable, _, _ = select.select([0], [], [], 0.1)
        if not readable:
            continue
        received += os.read(0, 65536)
        if b"\\r" in received:
            return received
    return received

# There are three TOTAL submission attempts: the initial Enter and two
# evidence-backed retries. Render fresh stuck evidence before each retry.
print("❯ [Pasted text #1 +12 lines]", flush=True)
first_retry = read_enter()
if b"\\r" not in first_retry or first_retry.strip(b"\\r"):
    raise SystemExit(2)
print("❯ [Pasted text #2 +12 lines]", flush=True)
second_retry = read_enter()
if b"\\r" not in second_retry or second_retry.strip(b"\\r"):
    raise SystemExit(3)

# Fresh evidence after the final allowed attempt keeps the explicit-stuck
# deadline applicable, but no fourth Enter may be sent.
print("❯ [Pasted text #3 +12 lines]", flush=True)
time.sleep(30)
""",
        encoding="utf-8",
    )
    fake_claude.chmod(fake_claude.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    runner = ClaudeInteractiveRunner(max_retries=1, retry_backoff=0)
    submit_calls = _record_submit_keystrokes(monkeypatch)

    started = time.monotonic()
    result = runner.run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=30,
        credential="claude-token",
    )

    assert time.monotonic() - started < 10
    assert result.success is False
    assert result.transient is True
    assert (
        result.summary == "Timed out confirming interactive Claude accepted the prompt submission"
    )
    assert len(submit_calls) == _SUBMISSION_ATTEMPT_LIMIT == 3


def test_run_stops_recovery_once_activity_begins(tmp_path, monkeypatch) -> None:
    """Silence before activity is ambiguous and must not trigger another Enter."""
    monkeypatch.setattr(
        "orcest.worker.claude_interactive_runner._SUBMISSION_RETRY_SETTLE_SECONDS",
        0.5,
    )
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import time
import tty

# A real interactive TUI puts the tty in raw mode immediately, disabling
# input echo and CR/NL translation. Match that so the fixture behaves like
# the real Claude Code composer instead of a canonical-mode shell.
tty.setraw(0)

print("❯ ", flush=True)

buf = b""
deadline = time.time() + 5
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

# Stay silent beyond the retry-settle interval before revealing activity. The
# runner must not treat silence as proof that the initial Enter failed.
silent_deadline = time.time() + 2.0
while time.time() < silent_deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if readable:
        os.read(0, 65536)

print("Cerebrating... (esc to interrupt)", flush=True)

# Nothing further should ever arrive: no duplicate Enter, no repaste.
extra = b""
watch_deadline = time.time() + 2.5
while time.time() < watch_deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    extra += chunk

if extra:
    print(f"UNEXPECTED_POST_ACTIVITY_BYTES={extra!r}", flush=True)
    time.sleep(10)
    raise SystemExit(1)

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(10)
    raise SystemExit(1)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("delayed activity confirmed\\n")
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
    submit_calls = _record_submit_keystrokes(monkeypatch)

    result = runner.run(
        "say hello",
        work_dir,
        token="github-token",
        timeout=12,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "delayed activity confirmed"
    assert len(submit_calls) == 1


def test_run_does_not_send_prompt_into_a_partially_rendered_menu(tmp_path, monkeypatch) -> None:
    """The composer gate must not fire mid-render.

    A selection dialog emits its caret marker before the option numbers, so a
    buffer sampled between those writes holds a bare "❯" line that is
    indistinguishable from the composer.  Production evidence (worker-10000,
    2026-08-18): the task prompt was pasted into the MCP menu, its trailing
    carriage return accepted the menu default -- `settings.local.json` came out
    with `enabledMcpjsonServers` populated, which only option 1 does -- and
    Claude then sat at an empty composer until the wall clock killed it, with
    no session transcript ever created.
    """
    fake_claude = tmp_path / "claude"
    fake_claude.write_text(
        """#!/usr/bin/env python3
import os
import re
import select
import sys
import time

def read_confirmation(timeout=6):
    deadline = time.time() + timeout
    buf = b""
    while time.time() < deadline:
        readable, _, _ = select.select([0], [], [], 0.1)
        if not readable:
            continue
        chunk = os.read(0, 65536)
        if not chunk:
            break
        buf += chunk
        if b"\\r" in buf or b"\\n" in buf:
            return buf
    return buf

print(
    "Quick safety check: Is this a project you created or one you trust?\\n"
    "1. Yes, I trust this folder\\n"
    "2. No, exit\\n"
    "Enter to confirm / Esc to cancel",
    flush=True,
)
read_confirmation()

# Render the MCP menu the way a real TUI does: the caret row lands first, the
# numbered options only after further cursor positioning.
sys.stdout.write("New MCP server found in this project: supabase\\n\\u276f ")
sys.stdout.flush()
time.sleep(0.4)
sys.stdout.write(
    "1. Use this MCP server\\n"
    "2. Use this and all future MCP servers in this project\\n"
    "3. Continue without using this MCP serv\\x1b[45Gr\\n"
    "Enter to confirm / Esc to cancel\\n"
)
sys.stdout.flush()

mcp = read_confirmation()
if b"3" not in mcp:
    print(f"BAD_MCP_CONFIRMATION={mcp!r}", flush=True)
    time.sleep(30)
    raise SystemExit(1)

print("WARNING: Claude Code running in Bypass Permissions mode\\n"
      "1. No, exit\\n2. Yes, I accept\\nEnter to confirm / Esc to cancel", flush=True)
bypass = read_confirmation()
if b"2" not in bypass:
    print(f"BAD_BYPASS_CONFIRMATION={bypass!r}", flush=True)
    time.sleep(30)
    raise SystemExit(1)

# Only now does the real composer appear.
print("\\u276f ", flush=True)

buf = b""
deadline = time.time() + 8
while time.time() < deadline:
    readable, _, _ = select.select([0], [], [], 0.1)
    if not readable:
        continue
    chunk = os.read(0, 65536)
    if not chunk:
        break
    buf += chunk
    if b"ORCEST_WORKER_RESULT_CONTRACT" in buf and b".txt" in buf:
        break

decoded = buf.decode("utf-8", errors="replace")
match = re.search(r"write your final one-line summary to (\\S+?\\.txt)", decoded)
if not match:
    print("NO_RESULT_PATH", flush=True)
    time.sleep(30)
    raise SystemExit(1)
with open(match.group(1), "w", encoding="utf-8") as result:
    result.write("prompt reached the composer\\n")
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
        timeout=30,
        credential="claude-token",
    )

    assert result.success is True
    assert result.summary == "prompt reached the composer"
