"""Claude CLI subprocess manager with timeout, retry, and output parsing.

Executes Claude Code in non-interactive (--print) mode with stream-json
output format, parses the result summary, and handles retries on crash.
Timeouts and usage exhaustion are NOT retried.
"""

import json
import logging
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from orcest.worker.runner import RunnerResult

# Patterns that indicate Claude usage/rate limit exhaustion.
# Checked against stderr only (case-insensitive).
_USAGE_EXHAUSTION_PATTERNS: list[tuple[str, str]] = [
    ("usage", "limit"),
    ("rate", "limit"),
    ("quota", "exceeded"),
    ("token limit", ""),
    ("billing", "limit"),
]

# Environment variables that are safe to forward to the Claude subprocess.
# We use an allowlist rather than os.environ.copy() to avoid leaking secrets
# (database passwords, API keys for other services, SSH credentials, etc.).
_ENV_ALLOWLIST: set[str] = {
    # Basic system vars needed by most programs
    "PATH",
    "HOME",
    "USER",
    "SHELL",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TERM",
    "TMPDIR",
    "TZ",
    # Node.js / Claude Code specific
    "NODE_PATH",
    "NODE_OPTIONS",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_CACHE_HOME",
    # Git config (needed for commits inside Claude)
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
    # Claude CLI auth
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
}


@dataclass
class ClaudeResult:
    """Parsed result from a Claude CLI invocation."""

    success: bool
    summary: str
    duration_seconds: int
    raw_output: str
    usage_exhausted: bool = False


def _build_env(token: str) -> dict[str, str]:
    """Build a minimal environment for the Claude subprocess.

    Uses an allowlist of safe variables from the parent process, then
    injects GITHUB_TOKEN (also as GH_TOKEN for gh CLI compatibility).
    """
    env: dict[str, str] = {}
    for key in _ENV_ALLOWLIST:
        val = os.environ.get(key)
        if val is not None:
            env[key] = val
    # Always set GITHUB_TOKEN and GH_TOKEN for gh CLI compatibility
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token
    return env


def _is_usage_exhausted(stderr: str) -> bool:
    """Check whether stderr indicates Claude usage/rate limit exhaustion.

    Examines only stderr (case-insensitive) against known patterns to avoid
    false positives when Claude is working on code that mentions rate limiting,
    tokens, or billing (e.g. implementing a rate-limiter or auth system).
    Returns True if any pattern matches.
    """
    text = stderr.lower()
    for primary, secondary in _USAGE_EXHAUSTION_PATTERNS:
        # When secondary is empty, only the primary keyword is required.
        if primary in text and (not secondary or secondary in text):
            return True
    return False


def _kill_process_tree(proc: subprocess.Popen[str], sigterm_timeout: float = 2.0) -> None:
    """Kill a subprocess and all its children via process group signal.

    Because we launch with start_new_session=True, the subprocess is
    the leader of its own process group.  Sending signals to the group
    ensures child processes (e.g. Node.js subprocesses spawned by Claude
    CLI) are also terminated.

    Sends SIGTERM first to allow Claude to exit cleanly, waits up to
    ``sigterm_timeout`` seconds, then sends SIGKILL if still alive.
    """
    try:
        pgid = os.getpgid(proc.pid)
    except (ProcessLookupError, PermissionError):
        # Process already exited or we lost permission -- nothing to do.
        return

    try:
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return

    try:
        proc.wait(timeout=sigterm_timeout)
        # Leader exited; fall through to SIGKILL the group for any remaining children.
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        # Process already exited between SIGTERM and SIGKILL -- nothing to do.
        pass


def _close_pipes(proc: subprocess.Popen[str]) -> None:
    """Close stdout and stderr pipes to avoid file descriptor leaks.

    Safe to call multiple times; idempotent.
    """
    for pipe in (proc.stdout, proc.stderr):
        if pipe is not None and hasattr(pipe, "close"):
            try:
                pipe.close()
            except OSError:
                pass


def _drain_stderr(
    proc: subprocess.Popen[str],
) -> tuple[list[str], threading.Thread]:
    """Read stderr in a background thread to avoid pipe deadlock.

    Returns a (lines, thread) tuple.  The caller should
    ``thread.join(timeout=...)`` before reading the list.

    Thread safety note: if ``join()`` times out while the thread is
    still appending, reading ``lines`` is safe on CPython (the GIL
    serialises ``list.append`` and ``list.__iter__``).  In the worst
    case we get a partial stderr, which is acceptable.
    """
    lines: list[str] = []

    def _reader() -> None:
        if proc.stderr is None:  # pragma: no cover
            return
        try:
            for line in proc.stderr:
                lines.append(line)
        except (OSError, ValueError):
            # Pipe closed or invalid -- nothing more to read.
            pass

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()
    return lines, thread


def run_claude(
    prompt: str,
    work_dir: Path,
    token: str,
    timeout: int = 1800,
    max_retries: int = 3,
    retry_backoff: int = 10,
    logger: logging.Logger | None = None,
    on_output: Callable[[str], None] | None = None,
    abort_event: threading.Event | None = None,
) -> ClaudeResult:
    """Execute Claude CLI and return parsed result.

    Runs: claude --print --output-format stream-json -p <prompt>

    Stdout is read line-by-line so that on_output can stream each line
    to external consumers (e.g. Redis) as it arrives.  Stderr is drained
    in a background thread to prevent pipe deadlock.

    The prompt is passed as a subprocess argument (list form), so it is
    never interpreted by a shell.  No prompt-injection risk exists at the
    subprocess layer (Claude itself may still act on instructions within
    the prompt text, but that is by design).

    Args:
        prompt: The full prompt text.
        work_dir: Working directory (cloned repo).
        token: GitHub token (passed as GITHUB_TOKEN env var).
        timeout: Max seconds to wait for Claude.
        max_retries: Maximum number of total attempts (including the first).
        retry_backoff: Seconds between retries.
        logger: Optional logger for status messages.
        on_output: Optional callback invoked with each stdout line.
        abort_event: Optional event that, when set, interrupts retry backoff
            and aborts the running subprocess so the worker can respond to a
            lost lock without waiting the full delay.

    Returns:
        ClaudeResult with success flag, summary, and timing.
    """
    if max_retries < 1:
        raise ValueError(f"max_retries must be >= 1, got {max_retries}")

    # Use a dedicated event for interruptible backoff sleeps.  If no
    # external abort event is provided, create a local one that is
    # never set so event.wait(timeout=N) behaves like time.sleep(N).
    _abort = abort_event if abort_event is not None else threading.Event()

    env = _build_env(token)

    cmd = [
        "claude",
        "--print",
        "--verbose",
        "--output-format",
        "stream-json",
        "--dangerously-skip-permissions",
        "-p",
        prompt,
    ]

    start_time = time.monotonic()
    # Initialise outside the loop so the "all retries exhausted" fallthrough
    # can report output from the last attempt.
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    for attempt in range(1, max_retries + 1):
        proc: subprocess.Popen[str] | None = None
        attempt_start = time.monotonic()

        if logger:
            logger.info(f"Claude attempt {attempt}/{max_retries}")

        # start_new_session=True puts the child in its own process
        # group so we can kill the entire tree on timeout.
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=work_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
        except (OSError, ValueError) as e:
            # Process creation failed (e.g. claude binary not found,
            # or invalid Popen arguments).  Not retryable.
            duration = int(time.monotonic() - start_time)
            if logger:
                logger.error(f"Failed to start Claude: {e}")
            return ClaudeResult(
                success=False,
                summary=f"Failed to start: {e}",
                duration_seconds=duration,
                raw_output="",
            )

        # Drain stderr in background to avoid pipe deadlock
        try:
            stderr_lines, stderr_thread = _drain_stderr(proc)
        except RuntimeError:
            # Thread creation failed (e.g. system resource limit).
            # Kill the process and treat as a retryable crash.
            _kill_process_tree(proc)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass  # Zombie; will be reaped on process exit
            _close_pipes(proc)
            if attempt < max_retries:
                if logger:
                    logger.warning(
                        f"Failed to create stderr drain thread; retrying in {retry_backoff}s...",
                    )
                _abort.wait(timeout=retry_backoff)
            continue

        # Read stdout line-by-line, streaming to on_output
        stdout_lines = []
        timed_out = False
        if proc.stdout is None:  # pragma: no cover
            _kill_process_tree(proc)
            stderr_thread.join(timeout=5)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            _close_pipes(proc)
            raise RuntimeError("Popen stdout pipe is None despite PIPE flag")

        # Watchdog: kill the process tree if no output arrives
        # within the timeout.  Without this, the ``for line in
        # proc.stdout`` loop would block indefinitely when the
        # subprocess hangs without closing its stdout pipe.
        # We compute remaining time here (main thread) so the
        # watchdog thread doesn't need to call time.monotonic().
        watchdog_cancelled = threading.Event()
        watchdog_killed = threading.Event()
        watchdog_remaining = max(0.0, timeout - (time.monotonic() - attempt_start))
        assert proc is not None

        def _watchdog(
            _proc: subprocess.Popen[str] = proc,
            _remaining: float = watchdog_remaining,
            _cancelled: threading.Event = watchdog_cancelled,
            _killed: threading.Event = watchdog_killed,
        ) -> None:
            if _remaining > 0:
                _cancelled.wait(timeout=_remaining)
            if not _cancelled.is_set():
                # Timeout expired -- kill the process so the stdout
                # iterator unblocks with EOF / BrokenPipeError.
                _killed.set()
                _kill_process_tree(_proc)

        watchdog_thread = threading.Thread(target=_watchdog, daemon=True)
        try:
            watchdog_thread.start()
        except RuntimeError:
            # Thread creation failed -- kill the process and retry.
            _kill_process_tree(proc)
            stderr_thread.join(timeout=5)
            _close_pipes(proc)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            if attempt < max_retries:
                if logger:
                    logger.warning(
                        f"Failed to create watchdog thread; retrying in {retry_backoff}s...",
                    )
                _abort.wait(timeout=retry_backoff)
            continue

        try:
            for line in proc.stdout:
                stdout_lines.append(line)
                if on_output:
                    try:
                        on_output(line)
                    except Exception:
                        # Callback failure is non-fatal; log once
                        # and disable permanently (across retries)
                        # to avoid repeated errors.
                        if logger:
                            logger.warning(
                                "on_output callback raised; "
                                "disabling streaming for remaining attempts",
                                exc_info=True,
                            )
                        # Disable streaming for all remaining retry attempts to avoid log spam
                        on_output = None
                if abort_event is not None and abort_event.is_set():
                    watchdog_cancelled.set()
                    watchdog_thread.join(timeout=5)
                    _kill_process_tree(proc)
                    stderr_thread.join(timeout=5)
                    _close_pipes(proc)
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pass
                    duration = int(time.monotonic() - start_time)
                    if logger:
                        logger.warning("Claude subprocess killed: lock lost")
                    return ClaudeResult(
                        success=False,
                        summary="Aborted: lock lost",
                        duration_seconds=duration,
                        raw_output="".join(stdout_lines),
                    )
                if time.monotonic() - attempt_start >= timeout:
                    timed_out = True
                    break
        except Exception as stdout_exc:
            # If stdout iteration raises (broken pipe, etc.), kill
            # the process tree and treat it as a retryable crash
            # rather than propagating the exception to the caller.
            watchdog_cancelled.set()
            watchdog_thread.join(timeout=5)
            _kill_process_tree(proc)
            stderr_thread.join(timeout=5)
            _close_pipes(proc)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

            # If the watchdog killed the process, this is a timeout
            # -- not a retryable crash.
            if watchdog_killed.is_set():
                duration = int(time.monotonic() - start_time)
                if logger:
                    logger.error(f"Claude timed out after {timeout}s")
                return ClaudeResult(
                    success=False,
                    summary=f"Timed out after {timeout}s",
                    duration_seconds=duration,
                    raw_output="".join(stdout_lines),
                )

            if logger:
                logger.warning(
                    f"stdout read failed: {stdout_exc}",
                    exc_info=True,
                )
            # Skip the normal returncode analysis and go straight
            # to the retry backoff at the bottom of the loop.
            if attempt < max_retries:
                if logger:
                    logger.info(f"Retrying in {retry_backoff}s...")
                _abort.wait(timeout=retry_backoff)
            continue

        # Cancel the watchdog -- stdout reading finished (normally
        # or via the per-line timeout check).
        watchdog_cancelled.set()
        watchdog_thread.join(timeout=5)

        # Detect timeout: either the per-line check fired, or the
        # watchdog killed the process (stdout hit EOF).
        if not timed_out and watchdog_killed.is_set():
            timed_out = True

        if timed_out:
            duration = int(time.monotonic() - start_time)
            if logger:
                logger.error(f"Claude timed out after {timeout}s")
            _kill_process_tree(proc)
            stderr_thread.join(timeout=5)
            _close_pipes(proc)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # _kill_process_tree already sent SIGKILL to the
                # process group; this direct kill is a last-ditch
                # attempt in case the pgid lookup failed above.
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass  # Zombie; will be reaped on process exit
            return ClaudeResult(
                success=False,
                summary=f"Timed out after {timeout}s",
                duration_seconds=duration,
                raw_output="".join(stdout_lines),
            )

        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            _kill_process_tree(proc)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Process stuck in uninterruptible state (D-state / NFS).
                # Nothing more we can do; proceed with what we have.
                if logger:
                    logger.warning(
                        "Process did not exit after SIGKILL; proceeding with partial output"
                    )
        stderr_thread.join(timeout=5)
        _close_pipes(proc)

        stdout = "".join(stdout_lines)
        stderr = "".join(stderr_lines)
        duration = int(time.monotonic() - start_time)

        if proc.returncode == 0:
            summary = _extract_summary(stdout)
            return ClaudeResult(
                success=True,
                summary=summary,
                duration_seconds=duration,
                raw_output=stdout,
            )
        else:
            rc = proc.returncode
            if logger:
                if rc is None:
                    logger.warning(f"Claude process did not exit; stderr: {stderr[:500]}")
                else:
                    logger.warning(f"Claude exited with code {rc}: {stderr[:500]}")
            # Process stuck in D-state -- do NOT retry; the zombie
            # would leak resources and a fresh attempt is unlikely
            # to help if the system is in this state.
            if rc is None:
                return ClaudeResult(
                    success=False,
                    summary="Process did not exit (stuck in D-state)",
                    duration_seconds=duration,
                    raw_output=stderr or stdout,
                )
            # Check for usage exhaustion -- do NOT retry
            if _is_usage_exhausted(stderr):
                return ClaudeResult(
                    success=False,
                    summary="Claude usage limit reached",
                    duration_seconds=duration,
                    raw_output=stderr,
                    usage_exhausted=True,
                )

        # Retry with backoff on non-zero exit (crash)
        if attempt < max_retries:
            if logger:
                logger.info(f"Retrying in {retry_backoff}s...")
            _abort.wait(timeout=retry_backoff)

    # All retries exhausted -- include stderr from the most recent
    # attempt that successfully started a drain thread.
    duration = int(time.monotonic() - start_time)
    last_stderr = "".join(stderr_lines) if stderr_lines else ""
    last_stdout = "".join(stdout_lines) if stdout_lines else ""
    return ClaudeResult(
        success=False,
        summary=f"Failed after {max_retries} attempts",
        duration_seconds=duration,
        raw_output=last_stderr or last_stdout,
    )


class ClaudeRunner:
    """Runner implementation that executes tasks via the Claude CLI."""

    def __init__(
        self,
        max_retries: int = 3,
        retry_backoff: int = 10,
    ):
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff

    def run(
        self,
        prompt: str,
        work_dir: Path,
        token: str,
        timeout: int,
        logger: logging.Logger | None = None,
        on_output: Callable[[str], None] | None = None,
        abort_event: threading.Event | None = None,
    ) -> RunnerResult:

        result = run_claude(
            prompt=prompt,
            work_dir=work_dir,
            token=token,
            timeout=timeout,
            max_retries=self.max_retries,
            retry_backoff=self.retry_backoff,
            logger=logger,
            on_output=on_output,
            abort_event=abort_event,
        )
        return RunnerResult(
            success=result.success,
            summary=result.summary,
            usage_exhausted=result.usage_exhausted,
        )


def _extract_summary(stream_json_output: str) -> str:
    """Extract a human-readable summary from Claude's stream-json output.

    The stream-json format emits one JSON object per line (JSONL).  Each
    line is a message object.  The format we care about:

      - Assistant messages: {"type": "message", "role": "assistant",
        "content": [{"type": "text", "text": "..."}], ...}
      - System messages: {"role": "system", "cost_usd": ..., ...}

    We extract the text from the last assistant message's content blocks.
    If the output also contains a top-level "result" field (as in the
    --output-format json single-object format), we handle that too for
    forward compatibility.

    Returns the summary truncated to 500 characters.
    """
    if not stream_json_output or not stream_json_output.strip():
        return "No summary available"

    lines = stream_json_output.strip().splitlines()
    last_text = ""
    last_result = ""

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue

        # Forward-compat: top-level "result" key (--output-format json).
        # Guard with ``"role" not in obj`` so that an assistant message
        # that happens to contain a "result" key isn't misinterpreted.
        if "result" in obj and isinstance(obj["result"], str) and "role" not in obj:
            last_result = obj["result"]

        # stream-json assistant message with content array
        # stream-json wraps messages: {"type":"assistant","message":{"role":...,"content":[...]}}
        msg = obj.get("message", obj)
        if msg.get("role") == "assistant" and isinstance(msg.get("content"), list):
            for block in msg["content"]:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text", "")
                    if text:
                        last_text = text

        # stream-json system message (final line, has cost_usd)
        # -- not useful for summary, skip

    if last_result:
        return last_result[:500]
    return last_text[:500] if last_text else "No summary available"
