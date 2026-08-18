"""Claude CLI runner: stream-json parsing hooks for the generic driver.

Execution (subprocess spawn, stdout streaming, stderr drain, watchdog
timeout, retry-with-backoff, activity-watchdog integration) lives in
``_runner_base._run_cli_agent``; this module only supplies the
Claude-specific pieces: argv construction, stream-json summary / agent-text
extraction, usage-exhaustion and overload detection, timeout
reclassification (``classify_timeout``), and the post-run ``NEEDS_HUMAN``
scan (``postprocess_result``).

The shared worker conventions (NEEDS_HUMAN regex, env allowlist, credential
injection) live in ``_runner_base``; Claude-specific parsing stays here.
``ClaudeRunner`` inherits the full ``_BaseCliRunner`` contract — including
``run`` itself — so the worker dispatch in ``loop.py`` can pick a runner per
task and the activity watchdog wires up exactly as it does for Grok/Codex.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from orcest.worker._runner_base import (
    _BaseCliRunner,
    _build_env,
    _check_needs_human,
)
from orcest.worker.runner import RunnerResult

# Re-exports of helpers that tests and sibling modules import from here
# (claude_interactive_runner uses _is_usage_exhausted; stream_liveness uses
# _check_rate_limit_event).
__all__ = [
    "ClaudeRunner",
    "_build_env",
    "_check_overloaded_event",
    "_check_rate_limit_event",
    "_extract_summary",
    "_is_usage_exhausted",
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
    """Construct the Claude CLI argv (single source of truth for
    ``ClaudeRunner.build_argv``). Credential is never on argv.
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


class ClaudeRunner(_BaseCliRunner):
    """Runner implementation that executes tasks via the Claude CLI.

    Uses the generic ``_run_cli_agent`` driver via the inherited
    ``_BaseCliRunner.run`` — this class deliberately does NOT override
    ``run``, so the worker loop's activity-watchdog guard
    (``type(runner).run is _BaseCliRunner.run``) wires a tracker for Claude
    tasks exactly as it does for Grok/Codex.
    """

    # --- _BaseCliRunner hook implementations -------------------------------

    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        # work_dir is ignored — Claude takes the workspace via subprocess cwd,
        # not an explicit flag. The parameter is in the signature so Codex /
        # Grok runners can use it via the same hook.
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

    def classify_timeout(
        self, stdout_lines: list[str], stderr_lines: list[str], timeout: int
    ) -> RunnerResult | None:
        """Reclassify a wall-clock timeout that is really a usage-cap stall.

        A watchdog kill is usually a genuine timeout, but Claude can also
        stall at the limit because it hit a usage / rate limit and the CLI
        never exited on its own. Inspect the partial output for that signal
        first: if present, report usage exhaustion so the orchestrator waits
        for the reset instead of retrying straight into the same wall.
        Otherwise return None — the driver builds the generic transient
        timeout result. (Ports the legacy ``_timeout_claude_result``; the
        driver's exhaustion-first dispatch usually catches this earlier via
        ``detect_exhaustion``, so this is the belt to that suspender.)
        """
        stdout = "".join(stdout_lines)
        stderr = "".join(stderr_lines)
        rate_blocked, resets_at = _check_rate_limit_event(stdout)
        if _is_usage_exhausted(stderr) or rate_blocked:
            return RunnerResult(
                success=False,
                summary="Claude usage limit reached",
                usage_exhausted=True,
                rate_limit_resets_at=resets_at,
            )
        return None

    def postprocess_result(
        self, result: RunnerResult, stdout_lines: list[str], stderr_lines: list[str]
    ) -> RunnerResult:
        """Scan every outgoing result's stdout for the NEEDS_HUMAN signal.

        The base driver only checks NEEDS_HUMAN on the rc==0 success path;
        the legacy Claude wrapper parsed it from the raw output of *every*
        result (so an agent that emitted the signal and then exited non-zero,
        timed out, or was aborted still surfaced it). Preserve that: the
        signal lives in stream-json assistant/result lines on stdout, so
        stdout is the scan target (``_parse_needs_human`` strips the prompt
        echo exactly as on the success path).
        """
        if not result.needs_human:
            flag, reason = _parse_needs_human("".join(stdout_lines))
            if flag:
                result.needs_human = True
                result.needs_human_reason = reason
        return result

    # --- Defaults for direct (provider-less) invocation ---------------------

    def _default_provider(self) -> str:
        return "claude"

    def _default_binary(self) -> str:
        return "claude"

    def _default_env_var(self) -> str:
        return "CLAUDE_CODE_OAUTH_TOKEN"


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
