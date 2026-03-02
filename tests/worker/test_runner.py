"""Unit tests for the runner factory (worker/runner.py)."""

from __future__ import annotations

import pytest

from orcest.shared.config import RunnerConfig
from orcest.worker.claude_runner import ClaudeRunner
from orcest.worker.noop_runner import NoopRunner
from orcest.worker.runner import create_runner


@pytest.mark.unit
def test_create_runner_claude() -> None:
    """RunnerConfig(type='claude') returns a ClaudeRunner instance."""
    config = RunnerConfig(type="claude")
    runner = create_runner(config)
    assert isinstance(runner, ClaudeRunner)


@pytest.mark.unit
def test_create_runner_noop() -> None:
    """Noop runner config with duration=0.5 returns NoopRunner."""
    config = RunnerConfig(type="noop", extra={"duration": "0.5"})
    runner = create_runner(config)
    assert isinstance(runner, NoopRunner)
    assert runner.duration == 0.5


@pytest.mark.unit
def test_create_runner_noop_default_duration() -> None:
    """RunnerConfig(type='noop', extra={}) returns NoopRunner with default duration 0.01."""
    config = RunnerConfig(type="noop", extra={})
    runner = create_runner(config)
    assert isinstance(runner, NoopRunner)
    assert runner.duration == 0.01


@pytest.mark.unit
def test_create_runner_unknown_type_raises() -> None:
    """RunnerConfig(type='gemini') raises ValueError."""
    config = RunnerConfig(type="gemini")
    with pytest.raises(ValueError, match="Unknown runner type"):
        create_runner(config)


@pytest.mark.unit
def test_create_runner_noop_negative_duration_raises() -> None:
    """RunnerConfig(type='noop', extra={'duration': '-1'}) raises ValueError."""
    config = RunnerConfig(type="noop", extra={"duration": "-1"})
    with pytest.raises(ValueError, match="finite non-negative"):
        create_runner(config)


@pytest.mark.unit
def test_create_runner_noop_non_numeric_duration_raises() -> None:
    """RunnerConfig(type='noop', extra={'duration': 'abc'}) raises ValueError."""
    config = RunnerConfig(type="noop", extra={"duration": "abc"})
    with pytest.raises(ValueError, match="must be numeric"):
        create_runner(config)


@pytest.mark.unit
def test_create_runner_noop_inf_duration_raises() -> None:
    """RunnerConfig(type='noop', extra={'duration': 'inf'}) raises ValueError."""
    config = RunnerConfig(type="noop", extra={"duration": "inf"})
    with pytest.raises(ValueError, match="finite non-negative"):
        create_runner(config)


@pytest.mark.unit
def test_create_runner_noop_nan_duration_raises() -> None:
    """RunnerConfig(type='noop', extra={'duration': 'nan'}) raises ValueError."""
    config = RunnerConfig(type="noop", extra={"duration": "nan"})
    with pytest.raises(ValueError, match="finite non-negative"):
        create_runner(config)
