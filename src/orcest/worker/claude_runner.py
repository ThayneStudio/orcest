"""Claude CLI subprocess manager with timeout, retry, and output parsing.

Executes Claude Code in non-interactive (--print) mode with stream-json
output format, parses the result summary, and handles retries on crash.
Timeouts and usage exhaustion are NOT retried.
"""

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ClaudeResult:
    """Parsed result from a Claude CLI invocation."""

    success: bool
    summary: str
    duration_seconds: int
    raw_output: str


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
    env = os.environ.copy()
    env["GITHUB_TOKEN"] = token

    cmd = [
        "claude",
        "--print",
        "--output-format",
        "stream-json",
        "-p",
        prompt,
    ]

    start_time = time.monotonic()

    for attempt in range(1, max_retries + 1):
        try:
            if logger:
                logger.info(f"Claude attempt {attempt}/{max_retries}")

            proc = subprocess.run(
                cmd,
                cwd=work_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            duration = int(time.monotonic() - start_time)

            if proc.returncode == 0:
                summary = _extract_summary(proc.stdout)
                return ClaudeResult(
                    success=True,
                    summary=summary,
                    duration_seconds=duration,
                    raw_output=proc.stdout,
                )
            else:
                if logger:
                    logger.warning(
                        f"Claude exited with code {proc.returncode}: "
                        f"{proc.stderr[:500]}"
                    )
                # Check for usage exhaustion -- do NOT retry
                if (
                    "usage" in proc.stderr.lower()
                    and "limit" in proc.stderr.lower()
                ):
                    return ClaudeResult(
                        success=False,
                        summary="Claude usage limit reached",
                        duration_seconds=duration,
                        raw_output=proc.stderr,
                    )

        except subprocess.TimeoutExpired:
            duration = int(time.monotonic() - start_time)
            if logger:
                logger.error(f"Claude timed out after {timeout}s")
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


def _extract_summary(stream_json_output: str) -> str:
    """Extract a human-readable summary from Claude's stream-json output.

    The stream-json format emits one JSON object per line. We look for
    the final 'result' message which contains the assistant's summary.
    Falls back to last N characters of text content if no result found.
    Truncates to 500 chars.
    """
    lines = stream_json_output.strip().splitlines()
    last_text = ""

    for line in lines:
        try:
            obj = json.loads(line)
            # stream-json has 'type' field: 'text', 'result', etc.
            if obj.get("type") == "result":
                return obj.get("result", "")[:500]
            if obj.get("type") == "text":
                last_text = obj.get("text", "")
        except json.JSONDecodeError:
            continue

    # Fallback: return last chunk of text output
    return last_text[:500] if last_text else "No summary available"
