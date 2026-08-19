"""Dual-listener entrypoint for the monitor service.

Runs the ingest listener (private, write-token authenticated) and the query
listener (public read API, scoped bearer tokens) as two independent
``uvicorn.Server`` instances in daemon threads, and blocks the main thread
until SIGTERM/SIGINT.

Each server manages its own event loop (one per thread). ``uvicorn.Server``
only installs OS signal handlers when it detects it is running on the main
thread (``Server.capture_signals``); since both servers here run in daemon
threads, neither installs its own handlers, avoiding a clash over
SIGTERM/SIGINT with the main thread. Shutdown is instead coordinated
manually: this module's own signal handlers set a ``threading.Event`` that
the main thread waits on, then both servers' ``should_exit`` flags are set
and their threads joined.

A server thread can also end on its own -- e.g. ``Server.run()`` hits
``sys.exit(1)`` internally when the port is already in use at startup.
Python's default ``threading.excepthook`` silently discards ``SystemExit``
raised inside a thread, so an unwrapped ``server.run()`` target would just
vanish: the process hangs forever on ``shutdown_event.wait()`` while the
surviving listener keeps serving, with no visible failure. Each thread target
below is wrapped so that *any* exit (return or exception, ``SystemExit``
included) sets ``shutdown_event`` too. The main thread distinguishes a
graceful signal-initiated shutdown from a listener dying on its own via a
separate ``signal_shutdown`` flag set only inside the signal handler, and
re-raises ``SystemExit(1)`` in the listener-death case after both threads
have been stopped and joined.
"""

from __future__ import annotations

import logging
import signal
import threading
from types import FrameType

import uvicorn

from orcest.monitor.config import MonitorConfig
from orcest.monitor.ingest_app import create_ingest_app
from orcest.monitor.query_app import create_query_app

logger = logging.getLogger(__name__)


def run_monitor(cfg: MonitorConfig) -> None:
    """Start the ingest and query listeners and block until shutdown.

    Blocks the calling (main) thread until SIGTERM/SIGINT is received (the
    normal case) or either listener thread ends on its own -- e.g. a startup
    bind failure. In the latter case, once both threads have been stopped and
    joined, raises ``SystemExit(1)`` so the crash is not silently swallowed.
    """
    ingest_app = create_ingest_app(cfg)
    query_app = create_query_app(cfg)

    ingest_config = uvicorn.Config(
        ingest_app,
        host=cfg.ingest_host,
        port=cfg.ingest_port,
    )
    query_config = uvicorn.Config(
        query_app,
        host=cfg.query_host,
        port=cfg.query_port,
    )
    ingest_server = uvicorn.Server(ingest_config)
    query_server = uvicorn.Server(query_config)

    shutdown_event = threading.Event()
    signal_shutdown = threading.Event()

    def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
        logger.info("monitor service received signal %d, shutting down", signum)
        signal_shutdown.set()
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)

    def _run_server(server: uvicorn.Server, name: str) -> None:
        try:
            server.run()
        except BaseException:
            # Catches SystemExit too (uvicorn calls sys.exit(1) on a startup
            # bind failure) -- must not propagate to threading's default
            # excepthook, which silently drops SystemExit and would leave
            # the main thread blocked on shutdown_event.wait() forever.
            logger.exception("monitor %s listener terminated unexpectedly", name)
        finally:
            shutdown_event.set()

    ingest_thread = threading.Thread(
        target=_run_server, args=(ingest_server, "ingest"), name="monitor-ingest", daemon=True
    )
    query_thread = threading.Thread(
        target=_run_server, args=(query_server, "query"), name="monitor-query", daemon=True
    )
    ingest_thread.start()
    query_thread.start()

    listener_crashed = False
    try:
        shutdown_event.wait()
        listener_crashed = not signal_shutdown.is_set()
    finally:
        ingest_server.should_exit = True
        query_server.should_exit = True
        ingest_thread.join()
        query_thread.join()

    if listener_crashed:
        raise SystemExit(1)
