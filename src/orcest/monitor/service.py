"""Dual-listener entrypoint for the monitor service.

Runs the ingest listener (private, write-token authenticated) and the query
listener (public read API, scoped bearer tokens) as two independent
``uvicorn.Server`` instances in daemon threads, and blocks the main thread
until SIGTERM/SIGINT.

Each server manages its own event loop (one per thread), so
``install_signal_handlers=False`` is required -- uvicorn's default signal
handling only works on the main thread of the main interpreter, and here two
servers are sharing the process. Shutdown is coordinated manually: the signal
handlers set a ``threading.Event`` that the main thread waits on, then both
servers' ``should_exit`` flags are set and their threads joined.
"""

from __future__ import annotations

import logging
import signal
import threading

import uvicorn

from orcest.monitor.config import MonitorConfig
from orcest.monitor.ingest_app import create_ingest_app
from orcest.monitor.query_app import create_query_app

logger = logging.getLogger(__name__)


def run_monitor(cfg: MonitorConfig) -> None:
    """Start the ingest and query listeners and block until shutdown.

    Blocks the calling (main) thread until SIGTERM or SIGINT is received,
    then signals both uvicorn servers to stop and waits for their threads to
    exit before returning.
    """
    ingest_app = create_ingest_app(cfg)
    query_app = create_query_app(cfg)

    ingest_config = uvicorn.Config(
        ingest_app,
        host=cfg.ingest_host,
        port=cfg.ingest_port,
        install_signal_handlers=False,
    )
    query_config = uvicorn.Config(
        query_app,
        host=cfg.query_host,
        port=cfg.query_port,
        install_signal_handlers=False,
    )
    ingest_server = uvicorn.Server(ingest_config)
    query_server = uvicorn.Server(query_config)

    shutdown_event = threading.Event()

    def _handle_shutdown_signal(signum, frame) -> None:  # noqa: ANN001 - stdlib signature
        logger.info("monitor service received signal %d, shutting down", signum)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)

    ingest_thread = threading.Thread(
        target=ingest_server.run, name="monitor-ingest", daemon=True
    )
    query_thread = threading.Thread(target=query_server.run, name="monitor-query", daemon=True)
    ingest_thread.start()
    query_thread.start()

    try:
        shutdown_event.wait()
    finally:
        ingest_server.should_exit = True
        query_server.should_exit = True
        ingest_thread.join()
        query_thread.join()
