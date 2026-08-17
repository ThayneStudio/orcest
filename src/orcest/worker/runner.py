"""Runner protocol and factory for pluggable worker backends.

Each backend (Claude, Codex, Grok, ...) ships its own ``Runner`` subclass and
its own ``ProviderRecipe`` entry in PROVIDER_REGISTRY. Workers are configured
with a backend (which stream to subscribe to); per-task dispatch chooses the
runner from the registry using ``task.provider``.

The orchestrator never imports the registry — it only round-robins lean
``ProviderEntry(provider, credential, model)`` and publishes tasks. Execution
contracts live in this module + the per-provider runner files only.
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
    from orcest.worker._runner_base import _BaseCliRunner


@dataclass(frozen=True)
class ProviderRecipe:
    """Image-baked execution recipe for a given provider (local to worker).

    The orchestrator never sends recipes; ``task.provider`` is the sole
    opaque lookup key. This lives in the worker image (or worker-local
    config) only.

    ``runner_cls`` is the Runner subclass responsible for executing tasks
    for this provider. Allowing None preserves backwards-compat for any
    third-party recipe construction while the multi-runner refactor lands;
    in practice every shipped entry sets it.
    """

    binary: str
    env_var: str
    runner_cls: type[_BaseCliRunner] | None = None


# PROVIDER_REGISTRY: the worker's local, image-baked dispatch table.
# Keys are the opaque ``task.provider`` values sent by the orchestrator.
# The orchestrator is completely agnostic to binaries, env vars, flags, and
# output formats — it only ever round-robins ProviderEntry(provider,
# credential, model).
#
# This dict is declared empty here and SEEDED IN ``orcest/worker/__init__.py``
# on package import. Doing the seeding in the package init avoids a circular
# import: ``claude_runner`` imports ``ProviderRecipe`` from this module, so
# importing ``ClaudeRunner`` from inside this module's load would deadlock.
#
# Adding a provider (see also provision/setup-worker.sh and
# docs/adding-a-provider.md):
#   1. Pick the provider name (e.g. "codex").
#   2. Implement a Runner subclass in src/orcest/worker/<name>_runner.py.
#   3. Add a registry entry in src/orcest/worker/__init__.py with the baked
#      binary name, env var, and Runner class.
#   4. Ensure the binary + any deps are installed by setup-worker.sh and
#      rebake the worker template.
PROVIDER_REGISTRY: dict[str, ProviderRecipe] = {}


def get_provider_recipe(provider: str) -> ProviderRecipe | None:
    """Return the baked recipe for ``provider``, or None if unknown."""
    return PROVIDER_REGISTRY.get(provider)


def get_unsupported_reason(provider: str) -> str | None:
    """Return a short reason if the provider cannot be executed here, else None.

    Covers both "unknown provider" (not in registry) and "missing binary"
    (in registry but CLI not found in $PATH). Used for early graceful reject.

    A registered entry with ``binary == ""`` is treated as "no baked binary
    required" — useful for in-process runners like ``noop`` that don't shell
    out at all.
    """
    recipe = PROVIDER_REGISTRY.get(provider)
    if recipe is None:
        return f'unknown provider "{provider}"'
    if recipe.binary and shutil.which(recipe.binary) is None:
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
    # blocker (a ``NEEDS_HUMAN:`` line). This is the only signal that warrants
    # the orcest:needs-human label -- orcest never infers it from failure counts.
    needs_human: bool = False
    needs_human_reason: str = ""
    # OAuth-blob providers (Grok/Codex) may refresh their token mid-run; the
    # worker surfaces the rotated blob here so the orchestrator can persist it.
    # None for the common case (env-var credentials never rotate in place).
    credential_update: str | None = None
    credential_update_minted_at: float = 0.0


class Runner(Protocol):
    """Protocol for task execution backends.

    Callers pass ``provider`` + ``credential`` (the lean Task surface). Legacy
    ``claude_token`` is still accepted for transition. Implementations must
    use the local PROVIDER_REGISTRY to select binary + env var for credential
    injection. Credentials are never passed on argv, only via environment.

    ``model`` is the optional per-task model override. When empty, the runner
    falls back to its instance default (configured via ``RunnerConfig.model``).
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
        model: str = "",
    ) -> RunnerResult: ...


def create_runner(config: RunnerConfig) -> Runner:
    """Create a runner instance from configuration.

    This factory still drives the worker's *default* runner (used by tests
    and the ``noop`` path). Per-task dispatch via PROVIDER_REGISTRY happens
    in ``worker/loop.py:_execute_task`` and supersedes this for real tasks.
    """
    if config.type == "claude":
        if config.extra.get("mode") == "interactive":
            from orcest.worker.claude_interactive_runner import ClaudeInteractiveRunner

            return ClaudeInteractiveRunner(
                config.max_retries,
                config.retry_backoff,
                config.model,
            )
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
    recipe = PROVIDER_REGISTRY.get(config.type)
    if recipe is not None and recipe.runner_cls is not None:
        return recipe.runner_cls(config.max_retries, config.retry_backoff, config.model)
    raise ValueError(f"Unknown runner type: {config.type!r}")


# Registry seeding happens in ``orcest/worker/__init__.py`` (see comment on
# PROVIDER_REGISTRY above for why).
