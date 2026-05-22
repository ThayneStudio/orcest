"""Grok (xAI Grok Build) CLI runner.

Drives the ``grok`` CLI in unattended single-shot mode and parses its
``streaming-json`` output. Verified live against grok 0.1.216; stdout carries
exactly four event types (one JSON object per line):

    {"type":"thought","data":"..."}   reasoning chunk            (ignored)
    {"type":"text","data":"..."}      agent answer chunk         (concatenated)
    {"type":"end","stopReason":"...","sessionId":"...","requestId":"..."}
    {"type":"error","message":"..."}  error envelope (may embed an http_status)

Tool execution (file edits, shell) produces NO stdout events — it is internal
to grok — so the parsers only deal with the four types above. ``text`` events
are pure agent output (the prompt is never echoed), so NEEDS_HUMAN detection
needs no echo-stripping.

Auth — Path B (SuperGrok OAuth blob): the credential is the contents of
``~/.grok/auth.json`` (access token + refresh_token + expires_at). The grok
CLI authenticates from that file alone, fully unattended, with no
``XAI_API_KEY`` and no interactive login. ``prepare_credential`` writes the
blob to ``$HOME/.grok/auth.json``; ``extract_credential_update`` reads it back
if grok refreshed the access token in place. A plain (non-JSON) credential is
treated as an ``XAI_API_KEY`` for the API-key path.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator
from pathlib import Path

from orcest.worker._runner_base import CredentialContext, _BaseCliRunner

# Rate-limit / quota signals (→ usage exhaustion). Deliberately NOT matching
# 403 "no credits/licenses" (a permanent billing/config problem, not a
# transient rate limit) so a misconfigured key isn't mistaken for exhaustion.
_GROK_RATE_LIMIT_RE = re.compile(
    r"\b(429|rate[\s_-]?limit|quota\s+exceeded|too many requests)\b",
    re.IGNORECASE,
)

# Transient server overload (5xx / explicit overload). Conservative so generic
# "Internal error" text (e.g. a max-turns error) is not treated as transient.
_GROK_OVERLOAD_RE = re.compile(
    r"(\b5[0-9][0-9]\b.*\b(error|unavailable|overloaded|bad gateway)\b"
    r"|\boverloaded\b|\bservice unavailable\b|status\s+5[0-9][0-9]\b)",
    re.IGNORECASE,
)


def _iter_events(stdout: str) -> Iterator[dict]:
    """Yield parsed JSON event dicts from grok streaming-json stdout."""
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


def _concat_text(stdout: str) -> str:
    """Concatenate ``data`` from all ``text`` events — the agent's answer."""
    parts = [str(e.get("data", "")) for e in _iter_events(stdout) if e.get("type") == "text"]
    return "".join(parts)


def _grok_error_text(stdout: str, stderr: str) -> str:
    """All error-bearing text to scan for rate-limit / overload signals:
    ``error`` event messages from stdout plus raw stderr."""
    msgs = [str(e.get("message", "")) for e in _iter_events(stdout) if e.get("type") == "error"]
    return "\n".join(msgs) + "\n" + stderr


class GrokRunner(_BaseCliRunner):
    """Runner that executes tasks via the xAI ``grok`` CLI (Grok Build)."""

    def _default_provider(self) -> str:
        return "grok"

    def _default_binary(self) -> str:
        return "grok"

    def _default_env_var(self) -> str:
        return "XAI_API_KEY"

    # --- argv / parsing hooks ---------------------------------------------

    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        cmd = [
            binary,
            "-p",
            prompt,
            "--output-format",
            "streaming-json",
            "--always-approve",
            "--cwd",
            str(work_dir),
        ]
        if model:
            cmd += ["-m", model]
        return cmd

    def extract_summary(self, stdout: str) -> str:
        text = _concat_text(stdout)
        return text[:500] if text else "No summary available"

    def extract_agent_text(self, stdout: str) -> str:
        # grok ``text`` events are pure agent output (no prompt echo), so this
        # is safe to feed directly to the NEEDS_HUMAN detector.
        return _concat_text(stdout)

    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        # grok events carry no reset timestamp, so resets_at is always 0
        # (unknown) → the orchestrator applies its default cooldown.
        if _GROK_RATE_LIMIT_RE.search(_grok_error_text(stdout, stderr)):
            return True, 0
        return False, 0

    def detect_overload(self, stdout: str, stderr: str) -> bool:
        return bool(_GROK_OVERLOAD_RE.search(_grok_error_text(stdout, stderr)))

    # --- credential hooks (Path B: OAuth blob) ----------------------------

    def prepare_credential(
        self, credential: str, work_dir: Path, home_dir: Path, env_var_name: str
    ) -> CredentialContext:
        blob = credential.strip()
        if blob.startswith("{"):
            # OAuth-blob mode: write ~/.grok/auth.json so grok authenticates
            # unattended from the SuperGrok session. No XAI_API_KEY injected.
            try:
                json.loads(blob)
            except (json.JSONDecodeError, ValueError):
                # Looks like JSON but isn't valid — fall through to API-key path
                # rather than write a corrupt auth file.
                blob = ""
            if blob:
                grok_dir = home_dir / ".grok"
                grok_dir.mkdir(parents=True, exist_ok=True)
                auth_path = grok_dir / "auth.json"
                # Create at 0o600 atomically — no world-readable window for the
                # refresh token. fchmod also narrows a pre-existing looser file.
                fd = os.open(str(auth_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "w") as fh:
                    os.fchmod(fh.fileno(), 0o600)
                    fh.write(blob)
                return CredentialContext(extra_env={}, watch_path=auth_path)
        if credential:
            # API-key path (funded xAI API team): inject XAI_API_KEY.
            return CredentialContext(extra_env={env_var_name: credential})
        return CredentialContext()

    def extract_credential_update(self, watch_path: Path, original: str) -> str | None:
        try:
            current = watch_path.read_text()
        except OSError:
            return None
        # Only surface a change (token refreshed/rotated in place).
        if not current.strip() or current == original:
            return None
        # Guard against a partial/corrupt write (e.g. grok killed mid-refresh):
        # never propagate an invalid blob — it would be persisted to Redis and
        # brick the credential for every subsequent task until cleared by hand.
        try:
            json.loads(current)
        except (json.JSONDecodeError, ValueError):
            return None
        return current
