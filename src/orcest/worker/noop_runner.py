"""No-op runner for testing and development.

Simulates any backend by sleeping for a configured duration.
Used in stress/integration tests to exercise the real worker loop
without calling external tools.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable
from pathlib import Path

from orcest.worker.runner import RunnerResult


class NoopRunner:
    """Runner that sleeps for a configured duration."""

    def __init__(self, duration: float = 0.01):
        if math.isnan(duration) or math.isinf(duration) or duration < 0:
            raise ValueError(f"duration must be a finite non-negative number, got {duration}")
        self.duration = duration

    def run(
        self,
        prompt: str,
        work_dir: Path,
        token: str,
        timeout: int,
        logger: logging.Logger | None = None,
        on_output: Callable[[str], None] | None = None,
        shutdown_event: threading.Event | None = None,
    ) -> RunnerResult:
        sleep_duration = max(0.0, min(self.duration, max(timeout, 0)))
        if logger:
            logger.debug(f"NoopRunner sleeping {sleep_duration}s")
        time.sleep(sleep_duration)
        if on_output:
            try:
                on_output("noop\n")
            except Exception:
                if logger:
                    logger.warning("on_output callback raised; ignoring", exc_info=True)
        return RunnerResult(success=True, summary="noop")
