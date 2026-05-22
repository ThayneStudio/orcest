"""Generic CLI runner base.

Hosts the small pieces of the worker's execution contract that every provider
shares: the ``NEEDS_HUMAN:`` worker convention, the env allowlist and
credential injection helper, and the abstract ``_BaseCliRunner`` ABC that
declares the hooks each per-provider Runner overrides.

Subprocess driving (Popen, watchdog, abort, retry) currently lives in
``claude_runner.py`` alongside Claude's parsers. As additional providers land
(PR 2: Codex, PR 3: Grok production) and share enough subprocess plumbing, a
shared ``_run_one_subprocess_attempt`` helper will be lifted here. PR 1
deliberately stops short of that extraction so the existing Claude tests can
keep their tight coupling to ``run_claude``'s monotonic-call sequencing.

Per-provider Runners subclass ``_BaseCliRunner`` and override:
    build_argv(binary, prompt, model, work_dir) -> list[str]
    extract_summary(stdout) -> str
    extract_agent_text(stdout) -> str   # feeds the shared _check_needs_human
    detect_exhaustion(stdout, stderr) -> tuple[bool, int]
    detect_overload(stdout, stderr) -> bool

Class-attribute hooks:
    prompt_via_stdin: bool  # True ⇒ the prompt is piped on stdin instead of
                             # appearing in argv. Codex uses this; Claude/Grok
                             # take ``-p <prompt>`` on argv.

NEEDS_HUMAN contract: ``extract_agent_text`` returns only agent-authored text
that the shared ``NEEDS_HUMAN:`` regex may scan. For CLIs whose output echoes
the prompt back (e.g. Claude's stream-json ``user`` messages), the override
MUST strip that echo — otherwise every task will match the prompt's own
example ``NEEDS_HUMAN: <reason>`` line. For CLIs that take the prompt on
stdin and never echo it (Codex), no stripping is needed.

Exhaustion-first dispatch invariant: callers check ``detect_exhaustion``
and ``detect_overload`` BEFORE inspecting the exit code, because some CLIs
(Claude) exit non-zero on silent rate-limit and others (Codex likely) exit 0
with an error envelope embedded in stdout. ``detect_exhaustion`` is free to
scan stdout, stderr, or both — Claude's implementation deliberately scans
stderr only to avoid false-positives on stream-json ``"usage": {...}``
blocks, but other providers may need stdout scanning.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orcest.worker.runner import RunnerResult


# Environment variables safe to forward to provider subprocesses.
# Whitelist (not os.environ.copy()) to avoid leaking secrets for other services.
# Credential env vars themselves are NOT whitelisted: they would cross-leak
# (a grok subprocess inheriting ANTHROPIC_API_KEY, etc.). The credential for
# the current task's provider is injected explicitly by _build_env using the
# recipe's env_var_name.
_ENV_WHITELIST: set[str] = {
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
    "NODE_PATH",
    "NODE_OPTIONS",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_CACHE_HOME",
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
    # Claude provider routing flags. Harmless to forward to other CLIs.
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
}


# NEEDS_HUMAN: <reason> — orcest worker convention, emitted by the fix
# prompt. Anchored to start-of-line so a mid-sentence mention does not trip
# it. Matched only against agent-authored text (each runner's
# extract_agent_text strips the user-role echo).
_NEEDS_HUMAN_RE = re.compile(r"(?m)^[ \t>]*NEEDS_HUMAN:[ \t]*([^\n]{1,300})")


def _build_env(
    token: str,
    credential: str = "",
    env_var_name: str = "CLAUDE_CODE_OAUTH_TOKEN",
    extra_env_keys: set[str] | None = None,
) -> dict[str, str]:
    """Minimal environment for a provider subprocess.

    - Whitelisted parent env vars only.
    - GITHUB_TOKEN + GH_TOKEN (gh CLI compat) always set from ``token``.
    - Provider credential injected under ``env_var_name``. If ``credential``
      is empty, falls back to the parent process's value for that env var
      (supports orchestrator ``credential: ''`` + worker ``/opt/orcest/.env``).
    - ``extra_env_keys`` lets a per-provider runner forward additional safe
      keys without polluting the base whitelist.

    The credential is never placed on argv.
    """
    keys = _ENV_WHITELIST | (extra_env_keys or set())
    env: dict[str, str] = {}
    for key in keys:
        val = os.environ.get(key)
        if val is not None:
            env[key] = val
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token
    if credential:
        env[env_var_name] = credential
    else:
        parent_val = os.environ.get(env_var_name)
        if parent_val:
            env[env_var_name] = parent_val
    return env


def _check_needs_human(agent_text: str) -> tuple[bool, str]:
    """Detect a worker-reported human-decision blocker.

    Callers MUST pass already-filtered agent text — never the raw transcript
    or stderr blob. Each runner's ``extract_agent_text`` is responsible for
    returning only agent-authored content (stripping any prompt echo that
    contains the literal ``NEEDS_HUMAN:`` instruction the prompt itself uses
    as an example).

    Named distinctly from ``claude_runner._parse_needs_human`` (which takes
    raw stream-json and extracts agent text internally) to avoid a
    same-name-different-signature footgun for future runner authors.

    Returns (flag, reason); reason is empty when absent. A reason starting
    with ``<`` is treated as the prompt's placeholder example and ignored.
    """
    if not agent_text:
        return False, ""
    m = _NEEDS_HUMAN_RE.search(agent_text)
    if not m:
        return False, ""
    reason = m.group(1).strip()
    if reason.startswith("<"):
        return False, ""
    return True, reason


class _BaseCliRunner(ABC):
    """Base for CLI-driven provider runners.

    Declares the hook surface every Runner subclass implements. PR 1 leaves
    the concrete ``run()`` method to subclasses (ClaudeRunner delegates to
    the legacy ``run_claude`` function so the existing test suite's mock
    points stay intact). A shared ``run()`` will land when there's a second
    runner (PR 2 Codex) to validate the abstraction against.
    """

    # Subclasses may opt to forward extra env keys (CLI-specific routing
    # flags) without polluting the global whitelist.
    extra_env_keys: set[str] = set()

    # When True, the prompt is piped on stdin rather than placed in argv.
    # Codex's `codex exec ... -` uses this. Claude/Grok use ``-p <prompt>``
    # on argv (False, the default). The shared subprocess driver (PR 2)
    # will branch on this to decide between argv and stdin delivery.
    prompt_via_stdin: bool = False

    def __init__(
        self,
        max_retries: int = 3,
        retry_backoff: int = 10,
        model: str = "",
    ) -> None:
        if max_retries < 1:
            raise ValueError(f"max_retries must be >= 1, got {max_retries}")
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.model = model

    @abstractmethod
    def build_argv(self, binary: str, prompt: str, model: str, work_dir: Path) -> list[str]:
        """Return the argv list. Credential MUST NOT appear here — env only.

        ``work_dir`` is passed so providers that take a workspace flag
        (Codex's ``--cd``, Grok's ``--cwd``) can include it directly.
        Providers that don't (Claude — workspace is the subprocess cwd)
        ignore the argument. ``prompt`` may be empty when
        ``prompt_via_stdin`` is True; the driver pipes it on stdin instead.
        """

    @abstractmethod
    def extract_summary(self, stdout: str) -> str:
        """Human-readable summary from the CLI's stdout."""

    @abstractmethod
    def extract_agent_text(self, stdout: str) -> str:
        """Agent-authored text only — strip any user-role echo of the prompt.
        Returned text feeds the shared NEEDS_HUMAN: detector."""

    @abstractmethod
    def detect_exhaustion(self, stdout: str, stderr: str) -> tuple[bool, int]:
        """(exhausted?, resets_at_unix). resets_at == 0 means unknown."""

    @abstractmethod
    def detect_overload(self, stdout: str, stderr: str) -> bool:
        """Transient 5xx / overload — orchestrator retries with backoff."""

    @abstractmethod
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
        provider: str = "",
        credential: str = "",
        model: str = "",
    ) -> "RunnerResult":
        """Execute one task end-to-end and return a RunnerResult."""
