"""Claude CLI subprocess manager with timeout, retry, and output parsing.

Executes Claude Code in non-interactive (--print) mode with stream-json
output format, parses the result summary, and handles retries on crash.
Timeouts and usage exhaustion are NOT retried.

The shared worker conventions (NEEDS_HUMAN regex, env allowlist, credential
injection) live in ``_runner_base``; Claude-specific parsing stays here.
``ClaudeRunner`` inherits the ``_BaseCliRunner`` hook contract so the worker
dispatch in ``loop.py`` can pick a runner per task.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from orcest.worker._runner_base import (
    _BaseCliRunner,
    _build_env,
    _check_needs_human,
)
from orcest.worker.runner import (
    ProviderRecipe,
    RunnerResult,
    get_provider_recipe,
)

# Re-exports of helpers that tests import directly from this module.
__all__ = [
    "ClaudeResult",
    "ClaudeRunner",
    "_build_env",
    "_check_overloaded_event",
    "_check_rate_limit_event",
    "_extract_summary",
    "_is_usage_exhausted",
    "run_claude",
]

# Patterns that indicate Claude usage/rate limit exhaustion.
# Checked against stderr only (case-insensitive).
_USAGE_EXHAUSTION_PATTERNS: list[tuple[str, str]] = [
    ("usage", "limit"),
    ("quota", "exceeded"),
    ("token limit", ""),
    ("billing", "limit"),
]

# Rate-limit regex. Two intentional constraints:
#  1. Word order: indicator (exceeded/reached/hit/error) must follow "rate
#     limit" — inverted phrasings like "You've hit the rate limit" won't match.
#     False positives are the bigger risk; this is deliberate.
#  2. Trailing lookahead: indicator must be followed by non-alpha or EOL, so
#     real API error suffixes ("Retry after 60 seconds.", "for your plan")
#     still match. Only ever checked against stderr.
_RATE_LIMIT_RE = re.compile(
    r"\brate\s+limit\b.{0,20}(?:exceeded|reached|hit|error)(?=[^a-zA-Z]|$)",
    re.IGNORECASE | re.MULTILINE,
)


@dataclass
class ClaudeResult:
    """Parsed result from a Claude CLI invocation."""

    success: bool
    summary: str
    duration_seconds: int
    raw_output: str
    usage_exhausted: bool = False
    rate_limit_resets_at: int = 0  # Unix timestamp from rate_limit_event, 0 = not set
    transient: bool = False


def _agent_text_from_stream_json(output: str) -> str:
    """Return only the agent-authored text from Claude stream-json output.

    Concatenates assistant message text blocks and any top-level ``result``
    string, JSON-decoded so newlines are real (anchored matching needs them).
    Critically, ``user`` message lines -- which echo the prompt we sent, and
    therefore the prompt's own ``NEEDS_HUMAN:`` instruction -- are excluded, so
    the instruction can never be mistaken for the agent emitting the signal.
    If the input is not stream-json (e.g. plain stderr), it is returned
    unchanged so it is still scannable.
    """
    if not output or not output.strip():
        return ""
    parts: list[str] = []
    saw_json = False
    for line in output.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        saw_json = True
        if "result" in obj and isinstance(obj["result"], str) and "role" not in obj:
            parts.append(obj["result"])
        msg = obj.get("message", obj)
        if msg.get("role") == "assistant" and isinstance(msg.get("content"), list):
            for block in msg["content"]:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(str(block.get("text", "")))
    if not saw_json:
        return output
    return "\n".join(parts)


def _parse_needs_human(output: str) -> tuple[bool, str]:
    """Detect a worker-reported human-decision blocker in Claude's output.

    The fix prompt instructs Claude to end its final message with a standalone
    ``NEEDS_HUMAN: <reason>`` line, only for a genuine human-decision blocker.
    This extracts Claude-specific agent text from the raw stream-json, then
    delegates the regex match to the shared ``_check_needs_human`` so the
    NEEDS_HUMAN convention lives in exactly one place across all providers.
    Returns (flag, reason); reason is empty when the signal is absent.
    """
    text = _agent_text_from_stream_json(output)
    return _check_needs_human(text)


def _build_claude_argv(binary: str, prompt: str, model: str) -> list[str]:
    """Construct the Claude CLI argv. Single source of truth shared by
    ``ClaudeRunner.build_argv`` (the per-provider hook) and ``run_claude``'s
    legacy inline path, so the two cannot drift. Credential is never on argv.
    """
    cmd = [
        binary,
        "--print",
        "--verbose",
        "--output-format",
        "stream-json",
        "--dangerously-skip-permissions",
    ]
    if model:
        cmd += ["--model", model]
    cmd += ["-p", prompt]
    return cmd


def _is_usage_exhausted(stderr: str) -> bool:
    """Check whether stderr indicates Claude usage/rate limit exhaustion.

    Only examines stderr (case-insensitive).  Stdout is intentionally
    excluded because stream-json output contains ``"usage": {...}`` in
    every API response message, causing false positives when the word
    "limit" also appears anywhere in Claude's text output or the prompt.
    Returns True if any pattern matches.
    """
    if _RATE_LIMIT_RE.search(stderr):
        return True
    text = stderr.lower()
    for primary, secondary in _USAGE_EXHAUSTION_PATTERNS:
        # When secondary is empty, only the primary keyword is required.
        if primary in text and (not secondary or secondary in text):
            return True
    return False


def _check_rate_limit_event(stdout: str) -> tuple[bool, int]:
    """Check stream-json stdout for Claude usage/rate-limit exhaustion.

    Claude Code emits ``rate_limit_event`` objects in stream-json output
    when usage limits are approached or hit.  The ``status`` field is
    ``"allowed"`` normally; exhausted tokens have been observed as either
    ``"blocked"`` or ``"rejected"``.  Some CLI versions also emit a final
    ``result`` object with ``api_error_status=429`` and no useful stderr.

    Returns (is_exhausted, resets_at_unix) where resets_at_unix is the
    Unix timestamp from the event (0 if not available).
    """

    def _parse_resets_at(value: object) -> int:
        if value is None or value == "" or isinstance(value, bool):
            return 0
        try:
            return int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError, OverflowError):
            return 0

    resets_at = 0
    for line in stdout.splitlines():
        line = line.strip()
        if not line or (
            "rate_limit_event" not in line
            and "api_error_status" not in line
            and '"error"' not in line
        ):
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if obj.get("type") == "rate_limit_event":
            info = obj.get("rate_limit_info", {})
            if info.get("resetsAt"):
                resets_at = _parse_resets_at(info["resetsAt"])
            if info.get("status") in {"blocked", "rejected"}:
                return True, resets_at
        if obj.get("api_error_status") == 429 or obj.get("error") == "rate_limit":
            return True, resets_at
    return False, 0


def _check_overloaded_event(stdout: str) -> bool:
    """Check stream-json stdout for Claude server overload errors."""
    for line in stdout.splitlines():
        line = line.strip()
        if not line or (
            "api_error_status" not in line and '"error"' not in line and "Overloaded" not in line
        ):
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if obj.get("api_error_status") == 529:
            return True
        message = str(obj.get("message") or obj.get("result") or "")
        if obj.get("error") == "server_error" and (
            "529" in message or "overloaded" in message.lower()
        ):
            return True
        if "529" in message and "overloaded" in message.lower():
            return True
    return False


def _timeout_claude_result(
    timeout: int,
    duration: int,
    stdout: str,
    stderr: str,
) -> ClaudeResult:
    """Build the ClaudeResult for a run the watchdog killed at the wall clock.

    A watchdog kill is usually a genuine timeout, but Claude can also stall at
    the limit because it hit a usage / rate limit and the CLI never exited on
    its own. Inspect the partial output for that signal first: if present,
    report usage exhaustion so the orchestrator waits for the reset instead of
    retrying straight into the same wall. Otherwise it is a real timeout —
    transient (retryable with backoff), not a code defect.
    """
    rate_blocked, resets_at = _check_rate_limit_event(stdout)
    if _is_usage_exhausted(stderr) or rate_blocked:
        return ClaudeResult(
            success=False,
            summary="Claude usage limit reached",
            duration_seconds=duration,
            raw_output=stderr or stdout,
            usage_exhausted=True,
            rate_limit_resets_at=resets_at,
        )
    return ClaudeResult(
        success=False,
        summary=f"Timed out after {timeout}s",
        duration_seconds=duration,
        raw_output=stdout,
        transient=True,
    )


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
        # Leader exited cleanly; remaining children already received SIGTERM via killpg above.
        return
    except subprocess.TimeoutExpired:
        # Leader ignored SIGTERM; SIGKILL the whole group.
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
    on_stderr: Callable[[str], None] | None = None,
) -> tuple[list[str], threading.Thread]:
    """Read stderr in a background thread to avoid pipe deadlock.

    Returns a (lines, thread) tuple.  The caller should
    ``thread.join(timeout=...)`` before reading the list.

    If ``on_stderr`` is provided, each stderr line is also forwarded to it
    so the worker can stream stderr to Redis alongside stdout. The callback
    must not raise; the reader silently ignores callback failures so a
    flaky consumer cannot break stderr collection (which the local code
    still relies on for usage-exhaustion detection).

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
                if on_stderr is not None:
                    try:
                        on_stderr(line)
                    except Exception:
                        pass
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
    on_stderr: Callable[[str], None] | None = None,
    abort_event: threading.Event | None = None,
    claude_token: str = "",
    model: str = "",
    provider: str = "claude",
    credential: str = "",
    cmd_argv: list[str] | None = None,
) -> ClaudeResult:
    """Execute the provider CLI (selected via local registry) and return parsed result.

    The binary and credential env var name are looked up from the worker's
    image-baked PROVIDER_REGISTRY using `provider` as the key. This makes the
    CLI execution provider-agnostic while keeping the actual subprocess + output
    parsing logic in one place for the initial rollout (claude-specific parsing
    will be extended or split when additional providers are added).

    Runs: <binary> --print --output-format stream-json [--model M] -p <prompt>
    (flags may be provider-specific; registry + future per-provider recipes
    will drive them).

    Stdout is read line-by-line so that on_output can stream each line
    to external consumers (e.g. Redis) as it arrives.  Stderr is drained
    in a background thread to prevent pipe deadlock, and each line is
    forwarded to on_stderr when provided — workers stream stderr alongside
    stdout so postmortem investigations of failed tasks (e.g. claude-cli
    crashes that exit non-zero without an explicit error envelope) have a
    surviving record after the worker VM is destroyed.

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
        on_stderr: Optional callback invoked with each stderr line.
        abort_event: Optional event that, when set, interrupts retry backoff
            and aborts the running subprocess so the worker can respond to a
            lost lock without waiting the full delay.
        claude_token: Optional Claude OAuth token.
        model: Optional model identifier passed to the CLI as --model. When
            empty the CLI uses the token's default model.

    Returns:
        ClaudeResult with success flag, summary, and timing.
    """
    if max_retries < 1:
        raise ValueError(f"max_retries must be >= 1, got {max_retries}")

    # Use a dedicated event for interruptible backoff sleeps.  If no
    # external abort event is provided, create a local one that is
    # never set so event.wait(timeout=N) behaves like time.sleep(N).
    _abort = abort_event if abort_event is not None else threading.Event()

    # Support legacy callers that only pass claude_token
    if not credential and claude_token:
        credential = claude_token
    if not provider:
        provider = "claude"

    # Local registry lookup (never from orchestrator payload).
    # Defensive fallback: this is run_claude — its job is to run the Claude
    # CLI — so a missing recipe defaults to the Claude binary + env var. In
    # production this branch is unreachable: the loop.py early-reject filters
    # unknown providers, and PROVIDER_REGISTRY is always seeded by the worker
    # package __init__ before any task is dispatched. It exists only for
    # direct/ad-hoc callers of run_claude (tests). A non-Claude task can never
    # reach here because only ClaudeRunner.run (claude + grok-placeholder)
    # calls run_claude, and both providers carry a recipe.
    recipe = get_provider_recipe(provider)
    if recipe is None:
        recipe = ProviderRecipe(
            binary="claude",
            env_var="CLAUDE_CODE_OAUTH_TOKEN",
            runner_cls=ClaudeRunner,
        )

    env = _build_env(token, credential=credential, env_var_name=recipe.env_var)

    if cmd_argv is not None:
        # Caller (typically ClaudeRunner.run) supplied the argv via the
        # build_argv hook. Trust it. This is the path that exercises the
        # per-provider hook contract; the branch below is the legacy
        # direct-call fallback (tests, ad-hoc callers) and builds from the
        # same _build_claude_argv source of truth so they cannot drift.
        cmd = list(cmd_argv)
    else:
        cmd = _build_claude_argv(recipe.binary, prompt, model)

    if logger:
        env_keys = sorted(env.keys())
        logger.info(
            "Launching Claude: cwd=%s, timeout=%ds, env_vars=%s",
            work_dir,
            timeout,
            env_keys,
        )

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

        if logger:
            logger.info("Claude process started (pid=%d)", proc.pid)

        # Drain stderr in background to avoid pipe deadlock
        try:
            stderr_lines, stderr_thread = _drain_stderr(proc, on_stderr=on_stderr)
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
                if _abort.is_set():
                    break
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
            _proc: subprocess.Popen[str] = proc,  # type: ignore[assignment]
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
                if _abort.is_set():
                    break
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
                # NOTE (known limitation): abort latency is unbounded when
                # Claude produces no stdout.  This check only fires between
                # lines, so if the subprocess is silent (e.g. long tool-call
                # or network wait), the abort won't be detected until the
                # next stdout line arrives or the watchdog fires its hard
                # kill.  The watchdog fires a hard SIGKILL at most
                # timeout - elapsed seconds after lock loss is detected
                # (i.e. before the total execution timeout), but there is
                # no prompt/graceful signal to Claude on lock loss -- just an
                # eventual SIGKILL.
                #
                # If prompt abort on lock loss becomes a requirement, consider
                # switching to a non-blocking read loop (e.g. select-based) that
                # can interleave abort checks without blocking on stdout.  See
                # GitHub issue #144 for context.
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
                        transient=True,
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
                    stderr_text = "".join(stderr_lines).strip()
                    logger.error(
                        "Claude timed out after %ds (watchdog kill, stdout_lines=%d, stderr=%s)",
                        timeout,
                        len(stdout_lines),
                        stderr_text[:1000] if stderr_text else "(empty)",
                    )
                return _timeout_claude_result(
                    timeout, duration, "".join(stdout_lines), "".join(stderr_lines)
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
                if _abort.is_set():
                    break
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
                    pass  # Zombie; will be reaped on process exit
            if logger:
                stderr_text = "".join(stderr_lines).strip()
                logger.error(
                    "Claude timed out after %ds (pid=%d, stdout_lines=%d, stderr=%s)",
                    timeout,
                    proc.pid,
                    len(stdout_lines),
                    stderr_text[:1000] if stderr_text else "(empty)",
                )
            return _timeout_claude_result(
                timeout, duration, "".join(stdout_lines), "".join(stderr_lines)
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

        # Check for usage exhaustion before normal returncode handling. Claude
        # Code can emit a stream-json rate_limit_event/result and still exit
        # through paths that do not give us a useful stderr signal.
        rate_blocked, resets_at = _check_rate_limit_event(stdout)
        if _is_usage_exhausted(stderr) or rate_blocked:
            return ClaudeResult(
                success=False,
                summary="Claude usage limit reached",
                duration_seconds=duration,
                raw_output=stderr or stdout,
                usage_exhausted=True,
                rate_limit_resets_at=resets_at,
            )
        overloaded = _check_overloaded_event(stdout)

        if overloaded:
            return ClaudeResult(
                success=False,
                summary="Claude overloaded (529)",
                duration_seconds=duration,
                raw_output=stderr or stdout,
                transient=True,
            )

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
                    transient=True,
                )

        # Retry with backoff on non-zero exit (crash)
        if attempt < max_retries:
            if logger:
                logger.info(f"Retrying in {retry_backoff}s...")
            _abort.wait(timeout=retry_backoff)
            if _abort.is_set():
                break

    # All retries exhausted -- include stderr from the most recent
    # attempt that successfully started a drain thread.
    #
    # Repeated non-zero exits without any of the explicit transient signals
    # above (timeout, 529, usage exhaustion, D-state, lock loss) are almost
    # always infrastructure: network blip, auth/token hiccup, claude-cli
    # stream-json crash, OOM. There is nothing a human can fix; mark
    # transient so the orchestrator retries with exponential backoff
    # instead of escalating the PR to needs-human.
    duration = int(time.monotonic() - start_time)
    last_stderr = "".join(stderr_lines) if stderr_lines else ""
    last_stdout = "".join(stdout_lines) if stdout_lines else ""
    return ClaudeResult(
        success=False,
        summary=f"Failed after {max_retries} attempts",
        duration_seconds=duration,
        raw_output=last_stderr or last_stdout,
        transient=True,
    )


class ClaudeRunner(_BaseCliRunner):
    """Runner implementation that executes tasks via the Claude CLI."""

    def __init__(
        self,
        max_retries: int = 3,
        retry_backoff: int = 10,
        model: str = "",
    ):
        super().__init__(max_retries=max_retries, retry_backoff=retry_backoff, model=model)

    # --- _BaseCliRunner hook implementations -------------------------------

    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        # work_dir is ignored — Claude takes the workspace via subprocess cwd,
        # not an explicit flag. The parameter is in the signature so Codex /
        # Grok runners (later PRs) can use it via the same hook.
        del work_dir
        return _build_claude_argv(binary, prompt, model)

    def extract_summary(self, stdout: str) -> str:
        return _extract_summary(stdout)

    def extract_agent_text(self, stdout: str) -> str:
        return _agent_text_from_stream_json(stdout)

    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        rate_blocked, resets_at = _check_rate_limit_event(stdout)
        if _is_usage_exhausted(stderr) or rate_blocked:
            return True, resets_at
        return False, 0

    def detect_overload(self, stdout: str, stderr: str) -> bool:
        return _check_overloaded_event(stdout)

    # --- Runner protocol ---------------------------------------------------

    def run(
        self,
        prompt: str,
        work_dir: Path,
        token: str,
        timeout: int,
        logger: logging.Logger | None = None,
        on_output: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        abort_event: threading.Event | None = None,
        claude_token: str = "",
        provider: str = "claude",
        credential: str = "",
        model: str = "",
    ) -> RunnerResult:
        effective_model = model or self.model
        # Resolve the binary from the registry so build_argv sees the same
        # binary that run_claude would. Falls back defensively if the
        # registry hasn't been seeded for this provider.
        recipe = get_provider_recipe(provider)
        binary = recipe.binary if recipe is not None else "claude"
        cmd_argv = self.build_argv(binary, prompt, effective_model, work_dir)
        result = run_claude(
            prompt=prompt,
            work_dir=work_dir,
            token=token,
            timeout=timeout,
            max_retries=self.max_retries,
            retry_backoff=self.retry_backoff,
            logger=logger,
            on_output=on_output,
            on_stderr=on_stderr,
            abort_event=abort_event,
            claude_token=claude_token,
            provider=provider,
            credential=credential,
            cmd_argv=cmd_argv,
        )
        needs_human, needs_human_reason = _parse_needs_human(result.raw_output)
        return RunnerResult(
            success=result.success,
            summary=result.summary,
            usage_exhausted=result.usage_exhausted,
            rate_limit_resets_at=result.rate_limit_resets_at,
            transient=result.transient,
            needs_human=needs_human,
            needs_human_reason=needs_human_reason,
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
