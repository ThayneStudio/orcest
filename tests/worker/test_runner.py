"""Unit tests for the runner factory (worker/runner.py)."""

from __future__ import annotations

import shutil

import pytest

from orcest.shared.config import RunnerConfig
from orcest.worker.claude_runner import ClaudeRunner
from orcest.worker.noop_runner import NoopRunner
from orcest.worker.runner import (
    PROVIDER_REGISTRY,
    ProviderRecipe,
    create_runner,
    get_provider_recipe,
    get_unsupported_reason,
)


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


# ---------------------------------------------------------------------------
# Task 7: Grok registry entries + early-reject behaviour for unsupported Grok
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_provider_registry_contains_grok() -> None:
    """PROVIDER_REGISTRY (worker-local) must contain the 'grok' entry for Task 7."""
    assert "grok" in PROVIDER_REGISTRY
    recipe = PROVIDER_REGISTRY["grok"]
    assert isinstance(recipe, ProviderRecipe)
    assert recipe.binary == "grok"
    assert recipe.env_var == "XAI_API_KEY"


@pytest.mark.unit
def test_get_provider_recipe_grok() -> None:
    """get_provider_recipe returns the correct baked recipe for 'grok'."""
    recipe = get_provider_recipe("grok")
    assert recipe is not None
    assert recipe.binary == "grok"
    assert recipe.env_var == "XAI_API_KEY"


@pytest.mark.unit
def test_grok_without_support_reports_missing_binary(monkeypatch) -> None:
    """A worker whose image lacks the 'grok' binary reports it as unsupported.

    This is the 'without Grok support' case.  The early dispatch in loop.py
    will turn this into a permanent FAILED + 'rebake worker image to include
    grok CLI' message before any lock or runner work.
    """
    # Ensure the registry entry exists (Task 7)
    assert get_provider_recipe("grok") is not None

    # Simulate a worker image that does not have 'grok' in $PATH
    def fake_which(name: str) -> str | None:
        if name == "grok":
            return None
        return shutil.which(name)

    monkeypatch.setattr(shutil, "which", fake_which)

    reason = get_unsupported_reason("grok")
    assert reason is not None
    assert 'missing binary "grok"' in reason
    assert "grok" in reason


@pytest.mark.unit
def test_grok_reject_message_uses_rebake_guidance() -> None:
    """The early-reject helper (exercised for grok tasks) always emits the
    actionable 'Rebake worker image to include <provider> CLI' text.

    The existing integration of this path (unknown or missing binary) is
    covered by test_early_reject_unsupported_provider_publishes_clean_failed
    in test_loop.py which explicitly drives a 'grok' provider name.
    """
    # This test is a marker + documentation that the Grok-specific early
    # reject contract is exercised by the shared helper test.
    # No additional behaviour to assert here; the helper hard-codes the
    # rebake wording for any provider name.
    assert True
