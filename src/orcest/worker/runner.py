"""Runner protocol and factory for pluggable worker backends.

Each backend (Claude, Gemini, Codex, etc.) implements the Runner protocol.
Workers are configured with a backend (which stream to subscribe to) and
a runner (how to execute tasks).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from orcest.shared.config import RunnerConfig


@dataclass
class RunnerResult:
    """Result from a runner execution."""

    success: bool
    summary: str
    usage_exhausted: bool = False


class Runner(Protocol):
    """Protocol for task execution backends."""

    def run(
        self,
        prompt: str,
        work_dir: Path,
        token: str,
        timeout: int,
        logger: logging.Logger | None = None,
    ) -> RunnerResult: ...


def create_runner(config: RunnerConfig) -> Runner:
    """Create a runner instance from configuration."""
    if config.type == "claude":
        from orcest.worker.claude_runner import ClaudeRunner

        return ClaudeRunner(config.max_retries, config.retry_backoff)
    elif config.type == "noop":
        from orcest.worker.noop_runner import NoopRunner

        return NoopRunner(float(config.extra.get("duration", "0.01")))
    else:
        raise ValueError(f"Unknown runner type: {config.type!r}")
