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
# Keys are the opaque `task.provider` values. Add new providers here + the
# corresponding CLI to the worker image (setup-worker.sh + rebake) to support them.
# This is deliberately *not* populated from orchestrator ProviderEntry fields.
PROVIDER_REGISTRY: dict[str, ProviderRecipe] = {
    "claude": ProviderRecipe(binary="claude", env_var="CLAUDE_CODE_OAUTH_TOKEN"),
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
