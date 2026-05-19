"""Runner protocol and factory for pluggable worker backends.

Each backend (Claude, Gemini, Codex, etc.) implements the Runner protocol.
Workers are configured with a backend (which stream to subscribe to) and
a runner (how to execute tasks).
"""

from __future__ import annotations

import logging
import math
import shutil
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from orcest.shared.config import RunnerConfig


@dataclass(frozen=True)
class ProviderRecipe:
    """Image-baked execution recipe for a given provider (local to worker).

    The orchestrator never sends recipes; `task.provider` is the sole opaque
    lookup key. This lives in the worker image (or worker-local config) only.
    """

    binary: str
    env_var: str


# PROVIDER_REGISTRY: the worker's local, image-baked dispatch table.
# Keys are the opaque `task.provider` values sent by the orchestrator in Task.
# The orchestrator is completely agnostic to binaries, env vars, flags, and
# output formats — it only ever round-robins ProviderEntry(provider, credential, model).
# Execution contract lives ONLY here and in the corresponding runner code
# (claude_runner.py for the generic stream-json CLI path today).
#
# Adding a provider (see also provision/setup-worker.sh):
#   1. Pick the provider name (e.g. "grok").
#   2. Add a ProviderRecipe here with its baked binary name and the env var
#      that will receive the per-task credential (injected via env, NEVER argv).
#   3. Ensure the binary + any deps are installed by setup-worker.sh (and thus
#      present after rebake).
#   4. If the CLI uses different flags or produces different output for parsing
#      exhaustion / results, extend the generic logic or add a dedicated runner.
#
# Grok execution contract (v1, Task 7):
#   - provider name: "grok"
#   - binary: "grok" (must be on $PATH inside the worker image)
#   - env_var: "XAI_API_KEY" (receives the credential from Task.credential)
#   - invocation (reuses generic path):
#       grok --print --verbose --output-format stream-json \
#            --dangerously-skip-permissions -p "<prompt>"
#   - stdout: stream-json JSONL (or "result" envelope) for _extract_summary
#   - exhaustion detection: reuses _is_usage_exhausted (stderr patterns) +
#     _check_rate_limit_event (rate_limit_event + api_error_status 429) from
#     claude_runner.  Real Grok CLI may emit different signals; extend the
#     checks when the production binary contract is known.
#   - credential handling: only via os.environ["XAI_API_KEY"] in the child;
#     the parent worker process never logs it (see redaction in Task).
#
# Future providers follow the exact same pattern; no orchestrator changes.
PROVIDER_REGISTRY: dict[str, ProviderRecipe] = {
    "claude": ProviderRecipe(binary="claude", env_var="CLAUDE_CODE_OAUTH_TOKEN"),
    "grok": ProviderRecipe(binary="grok", env_var="XAI_API_KEY"),
}


def get_provider_recipe(provider: str) -> ProviderRecipe | None:
    """Return the baked recipe for `provider`, or None if unknown to this image."""
    return PROVIDER_REGISTRY.get(provider)


def get_unsupported_reason(provider: str) -> str | None:
    """Return a short reason if the provider cannot be executed here, else None.

    Covers both "unknown provider" (not in registry) and "missing binary"
    (in registry but CLI not found in $PATH). Used for early graceful reject.
    """
    recipe = PROVIDER_REGISTRY.get(provider)
    if recipe is None:
        return f'unknown provider "{provider}"'
    if shutil.which(recipe.binary) is None:
        return f'missing binary "{recipe.binary}" for provider "{provider}"'
    return None


@dataclass
class RunnerResult:
    """Result from a runner execution."""

    success: bool
    summary: str
    usage_exhausted: bool = False
    rate_limit_resets_at: int = 0  # Unix timestamp when rate limit resets (0 = unknown)
    transient: bool = False
    # Set when the worker's agent explicitly reported a genuine human-decision
    # blocker (a `NEEDS_HUMAN:` line). This is the only signal that warrants the
    # orcest:needs-human label -- orcest never infers it from failure counts.
    needs_human: bool = False
    needs_human_reason: str = ""


class Runner(Protocol):
    """Protocol for task execution backends.

    Generalized for multi-provider: callers now pass `provider` + `credential`
    (the lean Task surface). Legacy `claude_token` is still accepted for
    transition. Implementations must use the local PROVIDER_REGISTRY (via
    provider name) to select binary + env var for credential injection.
    Credentials are *never* passed on argv, only via environment.
    """

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
    ) -> RunnerResult: ...


def create_runner(config: RunnerConfig) -> Runner:
    """Create a runner instance from configuration."""
    if config.type == "claude":
        from orcest.worker.claude_runner import ClaudeRunner

        return ClaudeRunner(config.max_retries, config.retry_backoff, config.model)
    elif config.type == "noop":
        from orcest.worker.noop_runner import NoopRunner

        duration_str = config.extra.get("duration", "0.01")
        try:
            duration = float(duration_str)
        except (ValueError, TypeError) as e:
            raise ValueError(f"NoopRunner 'duration' must be numeric, got {duration_str!r}") from e
        if math.isnan(duration) or math.isinf(duration) or duration < 0:
            raise ValueError(
                f"NoopRunner 'duration' must be a finite non-negative number, got {duration}"
            )
        return NoopRunner(duration)
    else:
        raise ValueError(f"Unknown runner type: {config.type!r}")
