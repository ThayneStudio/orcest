"""Structured logging with Rich for orchestrator and worker components.

Each component gets a named logger (orcest.{component}.{identifier}) with
a Rich handler writing to stderr. The format includes a component:identifier
prefix for grep-ability when logs from multiple processes are interleaved.
"""

import logging

from rich.console import Console
from rich.logging import RichHandler


def setup_logging(
    component: str,
    identifier: str,
    level: str = "INFO",
) -> logging.Logger:
    """Configure structured logging with Rich.

    Args:
        component: "orchestrator" or "worker"
        identifier: worker ID or "main" for orchestrator
        level: log level string (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logger instance.
    """
    console = Console(stderr=True)
    handler = RichHandler(
        console=console,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
    )

    # Format includes component and identifier for grep-ability
    fmt = f"[{component}:{identifier}] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))

    numeric_level = getattr(logging, level.upper(), None)
    if numeric_level is None or not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level!r}")

    logger = logging.getLogger(f"orcest.{component}.{identifier}")
    logger.setLevel(numeric_level)
    logger.handlers.clear()
    logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate output)
    logger.propagate = False

    return logger
