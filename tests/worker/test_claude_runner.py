"""Unit tests for the Claude CLI runner (worker/claude_runner.py).

Mocks ``subprocess.Popen`` so that no real ``claude`` process is spawned.
The mock target is ``orcest.worker.claude_runner.subprocess.Popen``.

Also mocks ``time.monotonic`` to avoid real delays and to control
timing for duration calculations.
"""

import json
import logging
import subprocess
import threading

import pytest

from orcest.worker.claude_runner import (
    ClaudeResult,
    ClaudeRunner,
    _extract_summary,
    _is_usage_exhausted,
    run_claude,
)
from orcest.worker.runner import RunnerResult

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


def _make_stdout_lines(*texts: str) -> list[str]:
    """Build stdout lines from text messages (each becomes a JSONL line)."""
    return [_stream_json_assistant(t) + "\n" for t in texts]


def _monotonic_seq(*values: float):
    """Return a side_effect function for time.monotonic that repeats the last value.

    The exact number of ``time.monotonic()`` calls made by ``run_claude``
    can vary depending on code-path taken and timing of background
    threads.  This helper returns a callable that yields values in order,
    then repeats the last value indefinitely so that unexpected extra
    calls never raise ``StopIteration``.

    Uses a lock to ensure thread-safe access to the index counter,
    since the main thread and background threads (stderr drain, watchdog
    join) may trigger ``time.monotonic()`` calls concurrently.
    """
    vals = list(values)
    lock = threading.Lock()
    idx = {"n": 0}

    def _next():
        with lock:
            i = idx["n"]
            if i < len(vals):
                idx["n"] += 1
                return vals[i]
            return vals[-1]

    return _next


@pytest.fixture
def mock_popen(mocker):
    """Patch subprocess.Popen with a controllable mock process.

    The mock provides:
    - proc.stdout: iterable of lines (set directly on mock_proc.stdout)
    - proc.stderr: iterable of lines (set directly on mock_proc.stderr)
    - proc.wait(): returns None (sets returncode)
    - proc.returncode: int
    - proc.pid: int
    """
    mock_proc = mocker.MagicMock()
    mock_proc.pid = 12345
    mock_proc.returncode = 0

    # Default empty output
    mock_proc.stdout = iter([])
    mock_proc.stderr = iter([])
    mock_proc.wait.return_value = None

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
    lines = _make_stdout_lines("All tests pass now.")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line), duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 110.0, 110.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=1)

    assert isinstance(result, ClaudeResult)
    assert result.success is True
    assert "All tests pass now." in result.summary
    mock_cls.assert_called_once()


# ---------------------------------------------------------------------------
# Retry on crash
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_failure_retries(mock_popen, mocker, tmp_path):
    """First call fails (returncode=1), second succeeds -> two Popen calls."""
    mock_cls, mock_proc = mock_popen

    success_lines = _make_stdout_lines("Fixed.")

    # Alternate stdout/stderr per attempt via side_effect on Popen
    attempts = [
        {"stdout": iter([]), "stderr": iter(["segfault\n"]), "rc": 1},
        {"stdout": iter(success_lines), "stderr": iter([]), "rc": 0},
    ]
    call_idx = {"n": 0}

    def popen_side_effect(*args, **kwargs):
        i = call_idx["n"]
        call_idx["n"] += 1
        attempt = attempts[i]
        mock_proc.stdout = attempt["stdout"]
        mock_proc.stderr = attempt["stderr"]
        mock_proc.returncode = attempt["rc"]
        return mock_proc

    mock_cls.side_effect = popen_side_effect

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt1_start, watchdog_remaining1, duration1,
        # attempt2_start, watchdog_remaining2, timeout_check(1 line), duration2
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 105.0, 105.0, 105.0, 110.0, 110.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=2, retry_backoff=0)

    assert result.success is True
    assert mock_cls.call_count == 2


# ---------------------------------------------------------------------------
# Timeout -- detected during streaming
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_timeout_no_retry(mock_popen, mocker, tmp_path):
    """Timeout detected during stdout reading -> no retry, success=False."""
    mock_cls, mock_proc = mock_popen

    lines = _make_stdout_lines("line 1", "line 2", "line 3")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])

    mocker.patch("orcest.worker.claude_runner._kill_process_tree")

    # start_time, attempt_start, watchdog_remaining, check_line1,
    # check_line2, check_line3(>timeout), duration
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 110.0, 120.0, 200.0, 200.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, timeout=60, max_retries=3)

    assert result.success is False
    assert "Timed out" in result.summary
    assert mock_cls.call_count == 1


# ---------------------------------------------------------------------------
# Usage exhaustion -- no retry
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_usage_exhausted_no_retry(mock_popen, mocker, tmp_path):
    """stderr containing usage limit pattern -> no retry, success=False."""
    mock_cls, mock_proc = mock_popen

    mock_proc.stdout = iter([])
    mock_proc.stderr = iter(["Error: usage limit reached for this billing period\n"])
    mock_proc.returncode = 1

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 102.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=3)

    assert result.success is False
    assert "usage limit" in result.summary.lower()
    assert mock_cls.call_count == 1


# ---------------------------------------------------------------------------
# Environment allowlist
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_env_allowlist(mock_popen, monkeypatch, mocker, tmp_path):
    """SECRET_KEY must NOT appear in Popen env; PATH must."""
    mock_cls, mock_proc = mock_popen

    mock_proc.stdout = iter(_make_stdout_lines("done"))
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    monkeypatch.setenv("SECRET_KEY", "super_secret_value")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line), duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 101.0),
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

    mock_proc.stdout = iter(_make_stdout_lines("ok"))
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line), duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 101.0),
    )

    run_claude(PROMPT, tmp_path, TOKEN, max_retries=1)

    cmd = mock_cls.call_args[0][0]
    assert cmd == [
        "claude",
        "--print",
        "--verbose",
        "--output-format",
        "stream-json",
        "--dangerously-skip-permissions",
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
def test_run_claude_max_retries_zero_raises(tmp_path):
    """max_retries=0 raises ValueError (must be >= 1)."""
    with pytest.raises(ValueError, match="max_retries must be >= 1"):
        run_claude(PROMPT, tmp_path, TOKEN, max_retries=0)


@pytest.mark.unit
def test_extract_summary_truncates():
    """_extract_summary truncates output to 500 characters."""
    long_text = "A" * 1000
    line = _stream_json_assistant(long_text)
    result = _extract_summary(line)
    assert len(result) == 500
    assert result == "A" * 500


# ---------------------------------------------------------------------------
# on_output streaming callback
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_on_output_called_per_line(mock_popen, mocker, tmp_path):
    """on_output callback is invoked once per stdout line."""
    mock_cls, mock_proc = mock_popen

    lines = _make_stdout_lines("hello", "world", "done")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, 3x timeout_check, duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 101.0, 101.0, 101.0),
    )

    captured: list[str] = []
    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=1, on_output=captured.append)

    assert result.success is True
    assert len(captured) == 3
    assert captured[0] == lines[0]
    assert captured[1] == lines[1]
    assert captured[2] == lines[2]


@pytest.mark.unit
def test_on_output_none_still_works(mock_popen, mocker, tmp_path):
    """on_output=None (default) does not crash."""
    _, mock_proc = mock_popen

    lines = _make_stdout_lines("ok")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line), duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 101.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=1, on_output=None)

    assert result.success is True
    assert "ok" in result.summary


@pytest.mark.unit
def test_timeout_during_streaming_calls_on_output(mock_popen, mocker, tmp_path):
    """on_output called for lines before timeout, then process is killed."""
    _, mock_proc = mock_popen

    lines = _make_stdout_lines("line1", "line2", "line3")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])

    kill_mock = mocker.patch("orcest.worker.claude_runner._kill_process_tree")

    # start_time=0, attempt_start=0, watchdog_remaining_calc=0(->60s left),
    # check_line1=10(<60 ok), check_line2=70(>=60 timeout; line2 is still
    # appended and on_output called BEFORE the timeout check fires), duration=70
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        side_effect=_monotonic_seq(0.0, 0.0, 0.0, 10.0, 70.0, 70.0),
    )

    captured: list[str] = []
    result = run_claude(
        PROMPT,
        tmp_path,
        TOKEN,
        timeout=60,
        max_retries=1,
        on_output=captured.append,
    )

    assert result.success is False
    assert "Timed out" in result.summary
    # on_output was called for lines before and including the timeout boundary
    assert len(captured) >= 1
    assert captured[0] == lines[0]
    kill_mock.assert_called_once()


@pytest.mark.unit
def test_on_output_exception_disables_callback(mock_popen, mocker, tmp_path):
    """on_output raising an exception disables it; remaining lines still read."""
    _, mock_proc = mock_popen

    lines = _make_stdout_lines("line1", "line2", "line3")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, 3x timeout_check, duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 101.0, 101.0, 101.0),
    )

    call_count = {"n": 0}

    def bad_callback(line: str) -> None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("callback exploded")

    result = run_claude(
        PROMPT,
        tmp_path,
        TOKEN,
        max_retries=1,
        on_output=bad_callback,
    )

    # Task should still succeed -- callback failure is non-fatal
    assert result.success is True
    # Callback was only invoked once (then disabled after the exception)
    assert call_count["n"] == 1
    # All 3 lines were still read into the output
    assert "line3" in result.raw_output


@pytest.mark.unit
def test_stderr_captured_via_thread(mock_popen, mocker, tmp_path):
    """stderr is captured in background thread for usage detection."""
    _, mock_proc = mock_popen

    mock_proc.stdout = iter([])
    mock_proc.stderr = iter(["rate limit exceeded\n"])
    mock_proc.returncode = 1

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 102.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=1)

    assert result.success is False
    assert result.usage_exhausted is True


# ---------------------------------------------------------------------------
# Popen OSError (binary not found)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_popen_oserror(mocker, tmp_path):
    """subprocess.Popen raising OSError -> ClaudeResult(success=False), no retry."""
    mock_cls = mocker.patch(
        "orcest.worker.claude_runner.subprocess.Popen",
        side_effect=OSError("No such file or directory: 'claude'"),
    )

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, duration
        side_effect=_monotonic_seq(100.0, 100.0, 102.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=3)

    assert result.success is False
    assert "Failed to start" in result.summary
    assert result.usage_exhausted is False
    # OSError is not retried -- only one Popen call
    assert mock_cls.call_count == 1


# ---------------------------------------------------------------------------
# All retries exhausted (both attempts fail)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_all_retries_exhausted(mock_popen, mocker, tmp_path):
    """All attempts fail (returncode != 0) with max_retries=2 -> 'Failed after 2 attempts'."""
    mock_cls, mock_proc = mock_popen

    attempts = [
        {"stdout": iter([]), "stderr": iter(["crash 1\n"]), "rc": 1},
        {"stdout": iter([]), "stderr": iter(["crash 2\n"]), "rc": 1},
    ]
    call_idx = {"n": 0}

    def popen_side_effect(*args, **kwargs):
        i = call_idx["n"]
        call_idx["n"] += 1
        attempt = attempts[i]
        mock_proc.stdout = attempt["stdout"]
        mock_proc.stderr = attempt["stderr"]
        mock_proc.returncode = attempt["rc"]
        return mock_proc

    mock_cls.side_effect = popen_side_effect

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt1_start, watchdog_remaining1, duration1,
        # attempt2_start, watchdog_remaining2, duration2,
        # final_duration (all retries exhausted fallthrough)
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 105.0, 105.0, 105.0, 110.0, 110.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=2, retry_backoff=0)

    assert result.success is False
    assert "Failed after 2 attempts" in result.summary
    assert mock_cls.call_count == 2


# ---------------------------------------------------------------------------
# proc.wait() timeout after stdout finishes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_wait_timeout_after_stdout(mock_popen, mocker, tmp_path):
    """proc.wait(timeout=30) raises TimeoutExpired -> _kill_process_tree called."""
    mock_cls, mock_proc = mock_popen

    lines = _make_stdout_lines("output line")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])
    # After kill, returncode is set (simulating SIGKILL)
    mock_proc.returncode = -9

    # First wait() raises TimeoutExpired, second wait() (after kill) succeeds
    mock_proc.wait.side_effect = [
        subprocess.TimeoutExpired(cmd="claude", timeout=30),
        None,
    ]

    kill_mock = mocker.patch("orcest.worker.claude_runner._kill_process_tree")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line),
        # duration (non-zero rc path), final_duration (all retries exhausted)
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 110.0, 110.0),
    )

    run_claude(PROMPT, tmp_path, TOKEN, max_retries=1)

    kill_mock.assert_called_once_with(mock_proc)


# ---------------------------------------------------------------------------
# proc.returncode is None (D-state) -- no retry
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_returncode_none_no_retry(mock_popen, mocker, tmp_path):
    """proc.returncode stays None after SIGKILL -> immediate failure, no retry."""
    mock_cls, mock_proc = mock_popen

    lines = _make_stdout_lines("partial output")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter(["some error\n"])
    # returncode stays None -- process stuck in D-state
    mock_proc.returncode = None

    # Both wait() calls raise TimeoutExpired
    mock_proc.wait.side_effect = [
        subprocess.TimeoutExpired(cmd="claude", timeout=30),
        subprocess.TimeoutExpired(cmd="claude", timeout=5),
    ]

    mocker.patch("orcest.worker.claude_runner._kill_process_tree")
    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line), duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 110.0),
    )

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=3)

    assert result.success is False
    assert "D-state" in result.summary
    # Must NOT retry -- only one Popen call
    assert mock_cls.call_count == 1


# ---------------------------------------------------------------------------
# Logger parameter
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_claude_with_logger(mock_popen, mocker, tmp_path):
    """Passing a logging.Logger does not raise; logger branches execute."""
    mock_cls, mock_proc = mock_popen

    lines = _make_stdout_lines("done with logging")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line), duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 101.0),
    )

    logger = logging.getLogger("test_claude_runner")
    logger.setLevel(logging.DEBUG)

    result = run_claude(PROMPT, tmp_path, TOKEN, max_retries=1, logger=logger)

    assert result.success is True
    assert "done with logging" in result.summary


# ---------------------------------------------------------------------------
# _extract_summary: invalid JSON lines mixed with valid
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_invalid_json_lines():
    """Non-JSON lines are skipped; valid assistant message is extracted."""
    lines = [
        "not json at all\n",
        "another garbage line {{{}\n",
        _stream_json_assistant("The real summary") + "\n",
        "trailing nonsense\n",
    ]
    output = "".join(lines)
    result = _extract_summary(output)
    assert result == "The real summary"


# ---------------------------------------------------------------------------
# _extract_summary: top-level "result" key
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_result_key():
    """Line with top-level 'result' key -> returned as summary."""
    obj = {"result": "Task completed successfully"}
    output = json.dumps(obj) + "\n"
    result = _extract_summary(output)
    assert result == "Task completed successfully"


# ---------------------------------------------------------------------------
# _extract_summary: multi-result stream -> last result wins (regression #111)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_last_result_in_multi_result_stream():
    """Multi-result stream -> last top-level 'result' value is returned.

    Regression test for issue #111: _extract_summary must scan all lines and
    return the *last* result, not early-return on the first one.
    """
    lines = [
        json.dumps({"result": "intermediate result"}) + "\n",
        _stream_json_assistant("assistant message in between") + "\n",
        json.dumps({"result": "final summary"}) + "\n",
    ]
    output = "".join(lines)
    result = _extract_summary(output)
    assert result == "final summary"


# ---------------------------------------------------------------------------
# _extract_summary: assistant message with "result" key not misinterpreted
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_result_key_on_assistant_message():
    """Assistant message with 'result' key -> extract text from content, not result."""
    obj = {
        "type": "message",
        "role": "assistant",
        "result": "should be ignored",
        "content": [{"type": "text", "text": "The actual summary"}],
    }
    output = json.dumps(obj) + "\n"
    result = _extract_summary(output)
    assert result == "The actual summary"


# ---------------------------------------------------------------------------
# _extract_summary: multiple assistant messages -> last one wins
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_multiple_assistant_messages():
    """Multiple assistant messages -> last one's text wins."""
    lines = [
        _stream_json_assistant("First message") + "\n",
        _stream_json_assistant("Second message") + "\n",
        _stream_json_assistant("Third and final message") + "\n",
    ]
    output = "".join(lines)
    result = _extract_summary(output)
    assert result == "Third and final message"


# ---------------------------------------------------------------------------
# _extract_summary: empty input
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_empty_input():
    """Empty string -> 'No summary available'."""
    assert _extract_summary("") == "No summary available"
    assert _extract_summary("   ") == "No summary available"
    assert _extract_summary("\n\n") == "No summary available"


# ---------------------------------------------------------------------------
# _extract_summary: no assistant messages
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_no_assistant_messages():
    """Valid JSON but no role=='assistant' -> 'No summary available'."""
    system_msg = json.dumps({"role": "system", "cost_usd": 0.05})
    user_msg = json.dumps({"role": "user", "content": "hello"})
    output = system_msg + "\n" + user_msg + "\n"
    result = _extract_summary(output)
    assert result == "No summary available"


# ---------------------------------------------------------------------------
# _extract_summary: non-text content blocks skipped
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_non_text_content_blocks():
    """Content blocks with type=='tool_use' are skipped; only type=='text' extracted."""
    obj = {
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "tool_use", "name": "bash", "input": {"cmd": "ls"}},
            {"type": "text", "text": "Here is the result"},
            {"type": "tool_use", "name": "read", "input": {"path": "/tmp"}},
        ],
    }
    output = json.dumps(obj) + "\n"
    result = _extract_summary(output)
    assert result == "Here is the result"


# ---------------------------------------------------------------------------
# _is_usage_exhausted: all 5 patterns
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_is_usage_exhausted_all_patterns():
    """Test all 5 pattern pairs from _USAGE_EXHAUSTION_PATTERNS."""
    # ("usage", "limit")
    assert _is_usage_exhausted("usage limit reached") is True
    # _RATE_LIMIT_RE (anchored regex)
    assert _is_usage_exhausted("rate limit exceeded") is True
    # ("quota", "exceeded")
    assert _is_usage_exhausted("quota exceeded for account") is True
    # ("token limit", "")
    assert _is_usage_exhausted("token limit hit") is True
    # ("billing", "limit")
    assert _is_usage_exhausted("billing limit reached") is True
    # No match
    assert _is_usage_exhausted("everything is fine") is False


# ---------------------------------------------------------------------------
# _is_usage_exhausted: pattern in stdout is NOT detected (only stderr checked)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_is_usage_exhausted_rate_limit_mid_sentence():
    """'exceeded' mid-sentence (not EOL) must not trigger the anchored regex."""
    assert _is_usage_exhausted("") is False
    # "exceeded" here is not at end-of-line — the \s*$ anchor rejects it.
    assert _is_usage_exhausted("rate limit exceeded in user-authored code") is False


# ---------------------------------------------------------------------------
# ClaudeRunner.run() wraps run_claude -> RunnerResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_claude_runner_class_run(mock_popen, mocker, tmp_path):
    """ClaudeRunner.run() wraps run_claude and returns RunnerResult."""
    mock_cls, mock_proc = mock_popen

    lines = _make_stdout_lines("Runner class result")
    mock_proc.stdout = iter(lines)
    mock_proc.stderr = iter([])
    mock_proc.returncode = 0

    mocker.patch(
        "orcest.worker.claude_runner.time.monotonic",
        # start_time, attempt_start, watchdog_remaining, timeout_check(1 line), duration
        side_effect=_monotonic_seq(100.0, 100.0, 100.0, 101.0, 101.0),
    )

    runner = ClaudeRunner(max_retries=1, retry_backoff=0)
    result = runner.run(
        prompt=PROMPT,
        work_dir=tmp_path,
        token=TOKEN,
        timeout=1800,
    )

    assert isinstance(result, RunnerResult)
    assert result.success is True
    assert "Runner class result" in result.summary
    assert result.usage_exhausted is False
