"""Lock TTL refresh via background daemon thread.

Keeps a Redis lock alive while a long-running Claude process executes.
Uses threading.Event.wait() for responsive shutdown instead of time.sleep().
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orcest.shared.coordination import RedisLock


class Heartbeat:
    """Background thread that refreshes a Redis lock's TTL."""

    def __init__(
        self,
        lock: RedisLock,
        interval: float | None = None,
        logger: logging.Logger | None = None,
        on_lock_lost: Callable[[], None] | None = None,
    ):
        """
        Args:
            lock: The RedisLock to keep alive.
            interval: Refresh interval in seconds.
                      Defaults to lock.ttl / 3.
            logger: Optional logger.
            on_lock_lost: Optional callback invoked when lock refresh fails.
                          Called once from the heartbeat thread before it stops.
        """
        self.lock = lock
        self.interval = lock.ttl / 3 if interval is None else interval
        self.logger = logger
        self._on_lock_lost = on_lock_lost
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the heartbeat thread.

        Raises RuntimeError if a heartbeat thread is already running.
        Call stop() before starting a new heartbeat.
        """
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError(
                f"Heartbeat thread for {self.lock.key} is already running. "
                "Call stop() before starting a new heartbeat."
            )
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"heartbeat-{self.lock.key}",
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the heartbeat thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self.interval + 1)
            self._thread = None

    def _run(self) -> None:
        """Heartbeat loop: refresh TTL until stopped."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.interval)
            if self._stop_event.is_set():
                break
            refreshed = self.lock.refresh()
            if self.logger:
                if refreshed:
                    self.logger.debug(f"Heartbeat: refreshed {self.lock.key}")
                else:
                    self.logger.warning(
                        f"Heartbeat: failed to refresh {self.lock.key} (lock lost?)"
                    )
            if not refreshed:
                if self._on_lock_lost is not None:
                    self._on_lock_lost()
                self._stop_event.set()
                break
