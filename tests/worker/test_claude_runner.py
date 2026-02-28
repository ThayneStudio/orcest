"""Unit tests for the Claude CLI runner (worker/claude_runner.py).

Mocks ``subprocess.Popen`` so that no real ``claude`` process is spawned.
The mock target is ``orcest.worker.claude_runner.subprocess.Popen``.

Also mocks ``time.sleep`` and ``time.monotonic`` to avoid real delays
and to control timing for duration calculations.
"""

import json
import subprocess

import pytest

from orcest.worker.claude_runner import ClaudeResult, _extract_summary, run_claude

TOKEN = "test-token-runner"
PROMPT = "Fix the failing CI"


def _stream_json_assistant(text: str) -> str:
    """Build a single stream-json JSONL line for an assistant message."""
    obj = {
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": text}],
    }
    return json.dumps(obj)


@pytest.fixture
def mock_popen(mocker):
    """Patch subprocess.Popen with a controllable mock process."""
    mock_proc = mocker.MagicMock()
    mock_proc.communicate.return_value = ("", "")
    mock_proc.returncode = 0
    mock_proc.pid = 12345
    mock_cls = mocker.patch(
        "orcest.worker.claude_runner.subprocess.Popen",
        return_value=mock_proc,
    )
    return mock_cls, mock_proc


# ---------------------------------------------------------------------------
# Successful run
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_success(mock_popen, mocker, tmp_path):
    """returncode=0 with valid stream-json -> ClaudeResult(success=True)."""
    mock_cls, mock_proc = mock_popen
    stdout_text = _stream_json_assistant("All tests pass now.")
    mock_proc.communicate.return_value = (stdout_text, "")
    mock_proc.returncode = 0

    mocker.patch("orcest.worker.claude_runner.time.sleep")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=[100.0, 110.0],
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=1)

    assert isinstance(result, ClaudeResult)
    assert result.success is True
    assert "All tests pass now." in result.summary
    assert result.raw_output == stdout_text
    mock_cls.assert_called_once()


# ---------------------------------------------------------------------------
# Retry on crash
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_failure_retries(mock_popen, mocker, tmp_path):
    """First call fails (returncode=1), second succeeds -> two Popen calls."""
    mock_cls, mock_proc = mock_popen

    success_stdout = _stream_json_assistant("Fixed.")

    # First call: failure, second call: success
    mock_proc.communicate.side_effect = [
        ("", "segfault"),  # attempt 1
        (success_stdout, ""),  # attempt 2
    ]
    # returncode changes between calls: 1 then 0
    type(mock_proc).returncode = mocker.PropertyMock(side_effect=[1, 0])

    mocker.patch("orcest.worker.claude_runner.time.sleep")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=[100.0, 105.0, 110.0],
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=2)

    assert result.success is True
    assert mock_cls.call_count == 2


# ---------------------------------------------------------------------------
# Timeout -- no retry
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_timeout_no_retry(mock_popen, mocker, tmp_path):
    """TimeoutExpired from communicate -> no retry, success=False."""
    mock_cls, mock_proc = mock_popen

    mock_proc.communicate.side_effect = subprocess.TimeoutExpired(cmd=["claude"], timeout=60)
    # Mock _kill_process_tree to avoid real os.killpg on fake PID
    mocker.patch("orcest.worker.claude_runner._kill_process_tree")

    mocker.patch("orcest.worker.claude_runner.time.sleep")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=[100.0, 160.0],
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, timeout=60, max_retries=3)

    assert result.success is False
    assert "Timed out" in result.summary
    # Only one Popen call -- timeouts are NOT retried
    assert mock_cls.call_count == 1


# ---------------------------------------------------------------------------
# Usage exhaustion -- no retry
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_usage_exhausted_no_retry(mock_popen, mocker, tmp_path):
    """stderr containing usage limit pattern -> no retry, success=False."""
    mock_cls, mock_proc = mock_popen

    mock_proc.communicate.return_value = (
        "",
        "Error: usage limit reached for this billing period",
    )
    mock_proc.returncode = 1

    mocker.patch("orcest.worker.claude_runner.time.sleep")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=[100.0, 102.0],
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=3)

    assert result.success is False
    assert "usage limit" in result.summary.lower()
    # Only one Popen call -- usage exhaustion is NOT retried
    assert mock_cls.call_count == 1


# ---------------------------------------------------------------------------
# Environment allowlist
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_env_allowlist(mock_popen, monkeypatch, mocker, tmp_path):
    """SECRET_KEY must NOT appear in Popen env; PATH must."""
    mock_cls, mock_proc = mock_popen

    mock_proc.communicate.return_value = (
        _stream_json_assistant("done"),
        "",
    )
    mock_proc.returncode = 0

    monkeypatch.setenv("SECRET_KEY", "super_secret_value")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

    mocker.patch("orcest.worker.claude_runner.time.sleep")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=[100.0, 101.0],
    )

    run_claude(PROMPT, tmp_path, TOKEN, max_retries=1)

    _, kwargs = mock_cls.call_args
    env = kwargs["env"]
    assert "SECRET_KEY" not in env
    assert env.get("PATH") == "/usr/bin:/bin"
    # Token vars must be set
    assert env["GITHUB_TOKEN"] == TOKEN
    assert env["GH_TOKEN"] == TOKEN


# ---------------------------------------------------------------------------
# Command arguments
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_command_args(mock_popen, mocker, tmp_path):
    """Popen receives the expected claude CLI arguments."""
    mock_cls, mock_proc = mock_popen

    mock_proc.communicate.return_value = (
        _stream_json_assistant("ok"),
        "",
    )
    mock_proc.returncode = 0

    mocker.patch("orcest.worker.claude_runner.time.sleep")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=[100.0, 101.0],
    )

    run_claude(PROMPT, tmp_path, TOKEN, max_retries=1)

    cmd = mock_cls.call_args[0][0]
    assert cmd == [
        "claude",
        "--print",
        "--output-format",
        "stream-json",
        "-p",
        PROMPT,
    ]

    _, kwargs = mock_cls.call_args
    assert kwargs["cwd"] == tmp_path
    assert kwargs["text"] is True
    assert kwargs["start_new_session"] is True


# ---------------------------------------------------------------------------
# _extract_summary truncation
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_truncates():
    """_extract_summary truncates output to 500 characters."""
    long_text = "A" * 1000
    line = _stream_json_assistant(long_text)
    result = _extract_summary(line)
    assert len(result) == 500
    assert result == "A" * 500
