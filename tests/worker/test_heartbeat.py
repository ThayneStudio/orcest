"""Unit tests for the Heartbeat background lock-refresh thread."""

from __future__ import annotations

import logging
import threading
import time

import pytest

from orcest.worker.heartbeat import Heartbeat


@pytest.fixture
def mock_lock(mocker):
    """A mock RedisLock with default attributes for heartbeat tests."""
    lock = mocker.MagicMock()
    lock.ttl = 30
    lock.key = "test-lock"
    lock.refresh.return_value = True
    return lock


@pytest.mark.unit
class TestHeartbeat:
    """Tests for the Heartbeat daemon thread."""

    def test_heartbeat_start_stop(self, mock_lock):
        """Start and stop a heartbeat cleanly with no errors."""
        hb = Heartbeat(mock_lock, interval=0.1)
        hb.start()
        # Poll until at least one refresh, then stop
        deadline = time.monotonic() + 5
        while mock_lock.refresh.call_count < 1 and time.monotonic() < deadline:
            time.sleep(0.05)
        hb.stop()
        assert hb._thread is None

    def test_heartbeat_refreshes_lock(self, mock_lock):
        """Heartbeat refreshes the lock multiple times over its interval."""
        mock_lock.ttl = 3
        hb = Heartbeat(mock_lock, interval=0.1)
        hb.start()
        # Poll until we see enough refreshes rather than fixed sleep
        deadline = time.monotonic() + 5
        while mock_lock.refresh.call_count < 3 and time.monotonic() < deadline:
            time.sleep(0.05)
        hb.stop()
        assert mock_lock.refresh.call_count >= 3

    def test_heartbeat_default_interval(self, mock_lock):
        """When no explicit interval is given, default to lock.ttl / 3."""
        mock_lock.ttl = 30
        hb = Heartbeat(mock_lock)
        assert hb.interval == pytest.approx(10.0)

    def test_heartbeat_failed_refresh_logged(self, mock_lock, caplog):
        """A failed refresh (returns False) produces a warning log."""
        mock_lock.refresh.return_value = False
        logger = logging.getLogger("test.heartbeat.failed_refresh")
        hb = Heartbeat(mock_lock, interval=0.1, logger=logger)

        with caplog.at_level(logging.WARNING, logger="test.heartbeat.failed_refresh"):
            hb.start()
            # Poll until we see at least one warning logged
            deadline = time.monotonic() + 5
            has_warning = lambda: any(  # noqa: E731
                r.levelno == logging.WARNING for r in caplog.records
            )
            while not has_warning() and time.monotonic() < deadline:
                time.sleep(0.05)
            hb.stop()

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) >= 1
        assert "failed to refresh" in warnings[0].message.lower()

    def test_heartbeat_double_start_raises(self, mock_lock):
        """Starting a heartbeat that is already running raises RuntimeError."""
        hb = Heartbeat(mock_lock, interval=0.1)
        hb.start()
        try:
            with pytest.raises(RuntimeError, match="already running"):
                hb.start()
        finally:
            hb.stop()

    def test_heartbeat_stop_before_start(self, mock_lock):
        """Calling stop() without start() does not raise an exception."""
        hb = Heartbeat(mock_lock, interval=0.1)
        hb.stop()  # Should be a no-op, no error.

    def test_heartbeat_daemon_thread(self, mock_lock):
        """The heartbeat thread must be a daemon thread."""
        hb = Heartbeat(mock_lock, interval=0.1)
        hb.start()
        try:
            assert hb._thread is not None
            assert hb._thread.daemon is True
        finally:
            hb.stop()

    def test_heartbeat_on_lock_lost_callback_called(self, mock_lock):
        """When lock refresh fails, on_lock_lost is called once."""
        mock_lock.refresh.return_value = False
        lock_lost = threading.Event()

        hb = Heartbeat(mock_lock, interval=0.1, on_lock_lost=lock_lost.set)
        hb.start()

        assert lock_lost.wait(timeout=5), "on_lock_lost was not called within 5 seconds"
        hb.stop()

    def test_heartbeat_stops_after_lock_lost(self, mock_lock):
        """After lock refresh fails, the heartbeat thread stops itself."""
        mock_lock.refresh.return_value = False
        lock_lost = threading.Event()

        hb = Heartbeat(mock_lock, interval=0.1, on_lock_lost=lock_lost.set)
        hb.start()
        thread = hb._thread

        # Wait for the callback to fire
        assert lock_lost.wait(timeout=5), "on_lock_lost was not called within 5 seconds"
        # Give the thread time to exit naturally
        assert thread is not None
        thread.join(timeout=5)
        assert not thread.is_alive(), "Heartbeat thread should have stopped after lock loss"
        hb.stop()

    def test_heartbeat_no_callback_still_stops_on_lock_lost(self, mock_lock):
        """Even without a callback, heartbeat stops itself when lock is lost."""
        mock_lock.refresh.return_value = False

        hb = Heartbeat(mock_lock, interval=0.1)
        hb.start()
        thread = hb._thread

        assert thread is not None
        thread.join(timeout=5)
        assert not thread.is_alive(), "Heartbeat thread should stop after failed refresh"
        hb.stop()
