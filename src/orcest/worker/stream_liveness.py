"""Provider-tolerant stream liveness classifier for the activity watchdog.

Turns a single stdout line from a coding-agent CLI (Claude Code stream-json,
raw Claude API stream events, Grok ACP, or anything unrecognized) into a
``StreamSignal`` that the liveness ladder samples on each poll.

Classification is deliberately conservative: only lines that positively
match a known "real activity" or "known waiting" shape are marked
``progress``/``waiting``; everything else -- non-JSON text, malformed JSON,
JSON of an unrecognized shape -- is ``output`` (weak liveness). See spec §4
degradation clause: the strong/weak distinction only matters for triage
snapshots, not for freshening S1.

``classify_line`` must never raise, regardless of input: huge lines,
truncated/malformed JSON, or JSON of an unexpected shape (wrong types for
expected fields) are all handled defensively and fall through to
``output``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from orcest.worker.claude_runner import _check_rate_limit_event

# Raw Claude API stream event types (as opposed to Claude Code's
# message-level wrapper types like "assistant"/"user"/"system"). These carry
# no useful tool information themselves -- they just indicate the model is
# actively streaming.
_RAW_STREAM_EVENT_TYPES = frozenset(
    {"content_block_delta", "content_block_start", "content_block_stop", "message_start"}
)

_ERROR_CLASS_MAX_LEN = 120


@dataclass(frozen=True)
class StreamSignal:
    """Classification of a single stdout line for the liveness watchdog.

    ``tool_args`` is LOCAL use only (fed to the repetition hasher elsewhere
    in the ladder) -- per spec §8's redaction rule, raw tool args must never
    be emitted (logged, persisted, or sent to Redis). Only ``tool_name``,
    hashes derived from ``tool_args``, and ``tool_error_class`` may leave
    this process.
    """

    kind: str  # "progress" | "waiting" | "output"
    reason: str = ""  # waiting only: "api_retry" | "rate_limit"
    tool_name: str = ""  # progress only, when a tool_use block was parsed
    tool_args: dict | None = None  # parsed args (LOCAL use only -- never emitted)
    tool_error_class: str = ""  # when a tool_result with is_error was parsed


_OUTPUT_SIGNAL = StreamSignal(kind="output")


def _first_tool_use_block(content: Any) -> tuple[str, dict | None] | None:
    """Return (name, args) for the first tool_use block in a content list, if any."""
    if not isinstance(content, list):
        return None
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_use":
            name = block.get("name", "")
            args = block.get("input")
            return (
                str(name) if name is not None else "",
                args if isinstance(args, dict) else None,
            )
    return None


def _first_error_tool_result(content: Any) -> str | None:
    """Return the truncated first line of the first erroring tool_result's content, if any."""
    if not isinstance(content, list):
        return None
    for block in content:
        if (
            isinstance(block, dict)
            and block.get("type") == "tool_result"
            and block.get("is_error") is True
        ):
            return _truncate_error_class(_result_content_text(block.get("content")))
    return None


def _result_content_text(content: Any) -> str:
    """Normalize a tool_result's ``content`` (str, or list of content blocks) to text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                parts.append(block["text"])
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts)
    return ""


def _truncate_error_class(text: str) -> str:
    first_line = text.splitlines()[0] if text else ""
    return first_line[:_ERROR_CLASS_MAX_LEN]


def _is_grok_acp_progress(obj: Any, _depth: int = 0) -> bool:
    """True if a Grok ACP session/update or agent_message_chunk shape appears
    anywhere in the object (top-level or nested under e.g. "params").

    Depth-bounded to keep this cheap and to guarantee termination even on
    pathological (but JSON-valid) nesting.
    """
    if _depth > 8 or not isinstance(obj, dict):
        return False
    if obj.get("method") == "session/update":
        return True
    if obj.get("sessionUpdate") == "agent_message_chunk":
        return True
    for value in obj.values():
        if isinstance(value, dict) and _is_grok_acp_progress(value, _depth + 1):
            return True
    return False


def classify_line(line: str) -> StreamSignal:
    """Classify a single stdout line from a coding-agent CLI.

    Never raises. Anything that isn't recognized JSON of a known shape --
    including non-JSON text, malformed JSON, and JSON with an unexpected
    field layout -- is classified as ``output`` (weak liveness).
    """
    try:
        return _classify_line(line)
    except Exception:  # noqa: BLE001 - classify_line must never raise
        return _OUTPUT_SIGNAL


def _classify_line(line: str) -> StreamSignal:
    if not line:
        return _OUTPUT_SIGNAL

    try:
        obj = json.loads(line)
    except (json.JSONDecodeError, ValueError, RecursionError):
        return _OUTPUT_SIGNAL

    if not isinstance(obj, dict):
        return _OUTPUT_SIGNAL

    obj_type = obj.get("type")

    if obj_type == "system" and obj.get("subtype") == "api_retry":
        return StreamSignal(kind="waiting", reason="api_retry")

    rate_blocked, _resets_at = _check_rate_limit_event(line)
    if rate_blocked:
        return StreamSignal(kind="waiting", reason="rate_limit")

    if obj_type in _RAW_STREAM_EVENT_TYPES:
        return StreamSignal(kind="progress")

    if obj_type == "assistant":
        message = obj.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        tool = _first_tool_use_block(content)
        if tool is not None:
            name, args = tool
            return StreamSignal(kind="progress", tool_name=name, tool_args=args)
        return StreamSignal(kind="progress")

    if obj_type == "user":
        message = obj.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        error_class = _first_error_tool_result(content)
        if error_class is not None:
            return StreamSignal(kind="progress", tool_error_class=error_class)
        return _OUTPUT_SIGNAL

    if _is_grok_acp_progress(obj):
        return StreamSignal(kind="progress")

    return _OUTPUT_SIGNAL
