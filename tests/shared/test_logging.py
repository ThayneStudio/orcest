"""Unit tests for structured logging setup."""

import logging

from orcest.shared.logging import setup_logging


def test_setup_logging_returns_logger():
    logger = setup_logging("worker", "w1")
    assert isinstance(logger, logging.Logger)


def test_setup_logging_logger_name():
    logger = setup_logging("orchestrator", "main")
    assert logger.name == "orcest.orchestrator.main"


def test_setup_logging_level():
    logger = setup_logging("worker", "w2", level="DEBUG")
    assert logger.level == logging.DEBUG

    logger_warn = setup_logging("worker", "w3", level="WARNING")
    assert logger_warn.level == logging.WARNING
