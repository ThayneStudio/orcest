"""OpenAI Codex CLI runner.

Drives the ``codex exec`` non-interactive subcommand and parses its
``--experimental-json`` event stream. Verified live against codex-cli 0.131.0;
stdout carries this event vocabulary (one JSON object per line):

    {"type":"thread.started","thread_id":"..."}
    {"type":"turn.started"}
    {"type":"item.started","item":{"id":"...","type":"file_change"|"command_execution"|...,"status":"in_progress",...}}
    {"type":"item.completed","item":{"id":"...","type":"agent_message"|"file_change"|"command_execution"|"reasoning",...,
                                     "text":"..." (when agent_message)}}
    {"type":"turn.completed","usage":{"input_tokens":N,"output_tokens":N,...}}
    {"type":"turn.failed","error":{"message":"..."}}      # error path
    {"type":"error","message":"..."}                       # also seen

Agent text = concatenation of ``item.text`` across every ``item.completed``
whose ``item.type == "agent_message"``. Tool/file-change/command items are
internal-state events and are intentionally ignored by the text extractors.
The prompt arrives on STDIN (trailing ``-`` argv), so codex never echoes it
back — NEEDS_HUMAN extraction needs no prompt-echo stripping.

Auth — Path B (ChatGPT subscription OAuth blob): the credential is the
contents of ``~/.codex/auth.json`` (``auth_mode: chatgpt`` + ``tokens``
dict with ``access_token`` + ``refresh_token`` + ``account_id``; the CLI
refreshes the access token automatically and may rotate the refresh token,
which is why the orchestrator-side credential write-back exists). The
``codex exec`` subcommand specifically reads ``~/.codex/auth.json``; it does
NOT honor ``OPENAI_API_KEY`` on its own. A plain (non-JSON) credential
falls back to the ``CODEX_API_KEY`` env-var path for funded-API users.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import ClassVar

from orcest.worker._runner_base import CredentialContext, _BaseCliRunner

logger = logging.getLogger(__name__)

# Rate-limit / quota signals → usage exhaustion. The canonical codex-exec
# message is "exceeded retry limit, last status: 429 Too Many Requests".
# Deliberately NOT matching 403 / billing-related auth errors (those are
# permanent config problems, not transient rate limits).
_CODEX_RATE_LIMIT_RE = re.compile(
    r"\b(429|rate[\s_-]?limit|quota\s+exceeded|too\s+many\s+requests|exceeded\s+retry\s+limit)\b",
    re.IGNORECASE,
)

# Transient server overload (5xx / explicit overload). Conservative so a
# generic "Internal error" from codex (e.g. max-turns) is not treated as
# transient and silently retried.
_CODEX_OVERLOAD_RE = re.compile(
    r"(\b5[0-9][0-9]\b.*\b(error|unavailable|overloaded|bad gateway)\b"
    r"|\boverloaded\b|\bservice unavailable\b|status\s+5[0-9][0-9]\b)",
    re.IGNORECASE,
)


def _has_refresh_token(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            (key == "refresh_token" and isinstance(nested, str) and bool(nested.strip()))
            or (isinstance(nested, (dict, list)) and _has_refresh_token(nested))
            for key, nested in value.items()
        )
    if isinstance(value, list):
        return any(_has_refresh_token(item) for item in value)
    return False


def _remove_stale_codex_auth(home_dir: Path) -> None:
    auth_path = home_dir / ".codex" / "auth.json"
    try:
        auth_path.unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.error("Failed to remove stale Codex auth file %s", auth_path, exc_info=True)
        raise RuntimeError(f"Failed to remove stale Codex auth file {auth_path}: {exc}") from exc


def _iter_events(stdout: str) -> Iterator[dict]:
    """Yield parsed JSON event dicts from codex --experimental-json stdout."""
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, dict):
            yield obj


def _agent_messages(stdout: str) -> list[str]:
    """Texts of every ``item.completed`` whose item is an ``agent_message``.

    A codex turn typically emits multiple agent_message items (interleaved
    with tool/file_change items). We collect them all so the summary captures
    the agent's full final narrative and so NEEDS_HUMAN detection cannot be
    bypassed by appearing in an earlier message.
    """
    out: list[str] = []
    for e in _iter_events(stdout):
        if e.get("type") != "item.completed":
            continue
        item = e.get("item") or {}
        if not isinstance(item, dict):
            continue
        if item.get("type") == "agent_message":
            text = item.get("text")
            if isinstance(text, str):
                out.append(text)
    return out


def _codex_error_text(stdout: str, stderr: str) -> str:
    """All error-bearing text to scan for rate-limit / overload signals:
    ``error`` events + ``turn.failed.error.message`` from stdout, plus raw
    stderr."""
    parts: list[str] = []
    for e in _iter_events(stdout):
        t = e.get("type")
        if t == "error":
            msg = e.get("message")
            if isinstance(msg, str):
                parts.append(msg)
        elif t == "turn.failed":
            err = e.get("error") or {}
            if isinstance(err, dict):
                msg = err.get("message")
                if isinstance(msg, str):
                    parts.append(msg)
    return "\n".join(parts) + "\n" + stderr


class CodexRunner(_BaseCliRunner):
    """Runner that executes tasks via the OpenAI ``codex exec`` CLI."""

    # Codex reads the prompt from stdin when the trailing ``-`` arg is
    # present (see build_argv). The shared driver in ``_run_cli_agent``
    # branches on this to pipe the prompt on stdin instead of argv.
    prompt_via_stdin: ClassVar[bool] = True

    def _default_provider(self) -> str:
        return "codex"

    def _default_binary(self) -> str:
        return "codex"

    def _default_env_var(self) -> str:
        return "CODEX_API_KEY"

    # --- argv / parsing hooks ---------------------------------------------

    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        # Prompt is delivered via stdin (the driver does the pipe). No prompt
        # arg here; the trailing ``-`` tells codex exec to read stdin.
        # Sandbox + bypass flags match the grok ``--always-approve`` posture:
        # the agent must edit files without prompting, identical to Claude's
        # ``--dangerously-skip-permissions``.
        cmd = [
            binary,
            "exec",
            "--experimental-json",
            "--sandbox",
            "danger-full-access",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
            "--cd",
            str(work_dir),
        ]
        if model:
            cmd += ["-m", model]
        cmd += ["-"]
        return cmd

    def extract_summary(self, stdout: str) -> str:
        msgs = _agent_messages(stdout)
        # Codex's last agent_message is the canonical final reply; earlier
        # ones are narration ("I'll create X..."). The last is the summary.
        return msgs[-1][:500] if msgs else "No summary available"

    def extract_agent_text(self, stdout: str) -> str:
        # Concatenate every agent_message so NEEDS_HUMAN: detection cannot
        # be bypassed by hiding the signal in a non-final message. Codex
        # never echoes the prompt (it arrives on stdin), so no echo strip.
        return "\n".join(_agent_messages(stdout))

    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        # codex emits no reset timestamp in its events, so resets_at == 0
        # (unknown) and the orchestrator applies its default cooldown.
        if _CODEX_RATE_LIMIT_RE.search(_codex_error_text(stdout, stderr)):
            return True, 0
        return False, 0

    def detect_overload(self, stdout: str, stderr: str) -> bool:
        return bool(_CODEX_OVERLOAD_RE.search(_codex_error_text(stdout, stderr)))

    # --- credential hooks (Path B: ChatGPT OAuth blob) --------------------

    def prepare_credential(
        self, credential: str, work_dir: Path, home_dir: Path, env_var_name: str
    ) -> CredentialContext:
        blob = credential.strip()
        if blob.startswith("{"):
            # OAuth-blob mode: write ~/.codex/auth.json so codex authenticates
            # unattended from the ChatGPT session. No CODEX_API_KEY injected.
            try:
                parsed = json.loads(blob)
            except (json.JSONDecodeError, ValueError):
                # Looks like JSON but isn't valid — fall through to API-key
                # path rather than write a corrupt auth file.
                _remove_stale_codex_auth(home_dir)
                blob = ""
            if blob and not _has_refresh_token(parsed):
                _remove_stale_codex_auth(home_dir)
                return CredentialContext()
            if blob:
                codex_dir = home_dir / ".codex"
                codex_dir.mkdir(parents=True, exist_ok=True)
                auth_path = codex_dir / "auth.json"
                # Create at 0o600 atomically — no world-readable window for
                # the refresh token. fchmod also narrows a pre-existing
                # looser file (cheap belt-and-braces).
                fd = os.open(str(auth_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "w") as fh:
                    os.fchmod(fh.fileno(), 0o600)
                    fh.write(blob)
                return CredentialContext(extra_env={}, watch_path=auth_path)
        if credential:
            # API-key path (funded OpenAI API): inject CODEX_API_KEY. Note:
            # OPENAI_API_KEY is intentionally NOT used here — ``codex exec``
            # only honors CODEX_API_KEY for non-interactive auth.
            _remove_stale_codex_auth(home_dir)
            return CredentialContext(extra_env={env_var_name: credential})
        _remove_stale_codex_auth(home_dir)
        return CredentialContext()

    def extract_credential_update(self, watch_path: Path, original: str) -> str | None:
        try:
            current = watch_path.read_text()
        except OSError:
            return None
        # Only surface a change (token refreshed/rotated in place).
        if not current.strip() or current == original:
            return None
        # Guard against a partial/corrupt write (e.g. codex killed mid-refresh):
        # never propagate an invalid blob — it would be persisted to Redis
        # and brick the credential for every subsequent task until cleared.
        try:
            parsed = json.loads(current)
        except (json.JSONDecodeError, ValueError):
            return None
        if not _has_refresh_token(parsed):
            return None
        return current
