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
import time
from dataclasses import dataclass
from pathlib import Path

from orcest.worker.runner import RunnerResult

# Patterns that indicate Claude usage/rate limit exhaustion.
# Checked against both stderr and stdout (case-insensitive).
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


def _is_usage_exhausted(stderr: str, stdout: str) -> bool:
    """Check whether the output indicates Claude usage/rate limit exhaustion.

    Examines both stderr and stdout (case-insensitive) against known
    patterns. Returns True if any pattern matches.
    """
    combined = (stderr + "\n" + stdout).lower()
    for primary, secondary in _USAGE_EXHAUSTION_PATTERNS:
        if primary in combined and (not secondary or secondary in combined):
            return True
    return False


def _kill_process_tree(proc: subprocess.Popen[str]) -> None:
    """Kill a subprocess and all its children via process group signal.

    Because we launch with start_new_session=True, the subprocess is
    the leader of its own process group.  Sending SIGKILL to the group
    ensures child processes (e.g. Node.js subprocesses spawned by Claude
    CLI) are also terminated.
    """
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        # Process already exited or we lost permission -- nothing to do.
        pass


def run_claude(
    prompt: str,
    work_dir: Path,
    token: str,
    timeout: int = 1800,
    max_retries: int = 3,
    retry_backoff: int = 10,
    logger: logging.Logger | None = None,
) -> ClaudeResult:
    """Execute Claude CLI and return parsed result.

    Runs: claude --print --output-format stream-json -p <prompt>

    The prompt is passed as a subprocess argument (list form), so it is
    never interpreted by a shell.  No prompt-injection risk exists at the
    subprocess layer (Claude itself may still act on instructions within
    the prompt text, but that is by design).

    Args:
        prompt: The full prompt text.
        work_dir: Working directory (cloned repo).
        token: GitHub token (passed as GITHUB_TOKEN env var).
        timeout: Max seconds to wait for Claude.
        max_retries: Number of retry attempts on crash.
        retry_backoff: Seconds between retries.
        logger: Optional logger for status messages.

    Returns:
        ClaudeResult with success flag, summary, and timing.
    """
    env = _build_env(token)

    cmd = [
        "claude",
        "--print",
        "--verbose",
        "--output-format",
        "stream-json",
        "--dangerouslySkipPermissions",
        "-p",
        prompt,
    ]

    start_time = time.monotonic()

    for attempt in range(1, max_retries + 1):
        proc: subprocess.Popen[str] | None = None
        try:
            if logger:
                logger.info(f"Claude attempt {attempt}/{max_retries}")

            # start_new_session=True puts the child in its own process
            # group so we can kill the entire tree on timeout.
            proc = subprocess.Popen(
                cmd,
                cwd=work_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            stdout, stderr = proc.communicate(timeout=timeout)

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
                if logger:
                    logger.warning(
                        f"Claude exited with code {proc.returncode}: "
                        f"{stderr[:500]}"
                    )
                # Check for usage exhaustion -- do NOT retry
                if _is_usage_exhausted(stderr, stdout):
                    return ClaudeResult(
                        success=False,
                        summary="Claude usage limit reached",
                        duration_seconds=duration,
                        raw_output=stderr,
                        usage_exhausted=True,
                    )

        except subprocess.TimeoutExpired:
            duration = int(time.monotonic() - start_time)
            if logger:
                logger.error(f"Claude timed out after {timeout}s")
            # Kill the entire process tree, not just the leader.
            if proc is not None:
                _kill_process_tree(proc)
                # Drain pipes to avoid ResourceWarning / zombie.
                # The secondary communicate() can itself time out if a
                # child somehow survived SIGKILL (e.g. D-state / NFS),
                # so we catch that rather than propagating.
                try:
                    proc.communicate(timeout=5)
                except (subprocess.TimeoutExpired, OSError):
                    proc.kill()  # Last-ditch direct kill
            # Timeout is NOT retried -- suggests the task is too large
            return ClaudeResult(
                success=False,
                summary=f"Timed out after {timeout}s",
                duration_seconds=duration,
                raw_output="",
            )

        # Retry with backoff on non-zero exit (crash)
        if attempt < max_retries:
            if logger:
                logger.info(f"Retrying in {retry_backoff}s...")
            time.sleep(retry_backoff)

    # All retries exhausted
    duration = int(time.monotonic() - start_time)
    return ClaudeResult(
        success=False,
        summary=f"Failed after {max_retries} attempts",
        duration_seconds=duration,
        raw_output="",
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
    ) -> RunnerResult:

        result = run_claude(
            prompt=prompt,
            work_dir=work_dir,
            token=token,
            timeout=timeout,
            max_retries=self.max_retries,
            retry_backoff=self.retry_backoff,
            logger=logger,
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

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue

        # Forward-compat: top-level "result" key (--output-format json)
        if "result" in obj and isinstance(obj["result"], str):
            return obj["result"][:500]

        # stream-json assistant message with content array
        if obj.get("role") == "assistant" and isinstance(obj.get("content"), list):
            for block in obj["content"]:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text", "")
                    if text:
                        last_text = text

        # stream-json system message (final line, has cost_usd)
        # -- not useful for summary, skip

    return last_text[:500] if last_text else "No summary available"
