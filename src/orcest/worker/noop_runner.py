"""No-op runner for testing and development.

Simulates any backend by sleeping for a configured duration.
Used in stress/integration tests to exercise the real worker loop
without calling external tools.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from orcest.worker.runner import RunnerResult


class NoopRunner:
    """Runner that sleeps for a configured duration."""

    def __init__(self, duration: float = 0.01):
        self.duration = duration

    def run(
        self,
        prompt: str,
        work_dir: Path,
        token: str,
        timeout: int,
        logger: logging.Logger | None = None,
    ) -> RunnerResult:
        if logger:
            logger.debug(f"NoopRunner sleeping {self.duration}s")
        time.sleep(self.duration)
        return RunnerResult(success=True, summary="noop")
