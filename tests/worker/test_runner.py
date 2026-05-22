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
    # PR 1 multi-runner refactor: every shipped entry carries a runner_cls.
    # Grok currently points at ClaudeRunner as a placeholder until the
    # dedicated GrokRunner ships in PR 3.
    assert recipe.runner_cls is ClaudeRunner


@pytest.mark.unit
def test_get_provider_recipe_grok() -> None:
    """get_provider_recipe returns the correct baked recipe for 'grok'."""
    recipe = get_provider_recipe("grok")
    assert recipe is not None
    assert recipe.binary == "grok"
    assert recipe.env_var == "XAI_API_KEY"


@pytest.mark.unit
def test_provider_registry_claude_has_runner_cls() -> None:
    """The claude entry maps to ClaudeRunner via runner_cls (PR 1)."""
    recipe = PROVIDER_REGISTRY["claude"]
    assert recipe.binary == "claude"
    assert recipe.env_var == "CLAUDE_CODE_OAUTH_TOKEN"
    assert recipe.runner_cls is ClaudeRunner


@pytest.mark.unit
def test_provider_registry_contains_noop() -> None:
    """The noop registry entry is present and shaped for in-process dispatch.

    Locks in PR 1's design choice: 'noop' is a real registry entry (so the
    early-reject's 'known provider' check passes) but has no binary and no
    runner_cls (the dispatch always uses the worker's pre-instantiated
    NoopRunner fallback whenever ``task.provider == config.runner.type``).
    """
    assert "noop" in PROVIDER_REGISTRY
    recipe = PROVIDER_REGISTRY["noop"]
    assert recipe.binary == ""
    assert recipe.runner_cls is None


@pytest.mark.unit
def test_get_unsupported_reason_noop_is_supported() -> None:
    """get_unsupported_reason('noop') is None — noop has no binary requirement.

    Pins the new ``binary == ''`` carve-out in get_unsupported_reason. Without
    this assertion, a future tightening of that function could silently route
    every noop integration test through the early-reject path.
    """
    assert get_unsupported_reason("noop") is None


@pytest.mark.unit
def test_provider_recipe_default_runner_cls_is_none() -> None:
    """ProviderRecipe(binary, env_var) constructs with runner_cls=None.

    Documents the backwards-compat surface: any third-party recipe built with
    the two-argument form keeps working — runner_cls defaults to None, which
    the dispatch handles by falling back.
    """
    recipe = ProviderRecipe(binary="x", env_var="Y")
    assert recipe.runner_cls is None


@pytest.mark.unit
def test_claude_runner_hooks_match_module_functions() -> None:
    """ClaudeRunner's _BaseCliRunner hook overrides match the module-level
    helpers they delegate to.

    The hooks themselves are not yet on every code path (PR 1 wires
    ``build_argv`` through ``run_claude``; the other parsers will follow as
    further refactor lands). This test pins the equivalence so PR 2's
    CodexRunner, which will subclass ``_BaseCliRunner`` and copy the pattern,
    has a working reference. A silent break in this plumbing would only
    surface when Codex ships.
    """
    from pathlib import Path

    from orcest.worker.claude_runner import (
        _agent_text_from_stream_json,
        _check_overloaded_event,
        _check_rate_limit_event,
        _extract_summary,
        _is_usage_exhausted,
    )

    runner = ClaudeRunner()

    # build_argv produces a Claude-shaped argv ending with -p <prompt>.
    argv = runner.build_argv("claude", "hello", "claude-3-opus", Path("/tmp"))
    assert argv[0] == "claude"
    assert "--print" in argv
    assert "--output-format" in argv
    assert "stream-json" in argv
    assert argv[-2:] == ["-p", "hello"]
    assert "claude-3-opus" in argv

    # No --model when model is empty.
    argv_no_model = runner.build_argv("claude", "hi", "", Path("/tmp"))
    assert "--model" not in argv_no_model

    # extract_summary / extract_agent_text mirror the module-level functions.
    sample_stream_json = (
        '{"type":"assistant","message":{"role":"assistant","content":'
        '[{"type":"text","text":"summary text"}]}}\n'
    )
    assert runner.extract_summary(sample_stream_json) == _extract_summary(sample_stream_json)
    assert runner.extract_agent_text(sample_stream_json) == _agent_text_from_stream_json(
        sample_stream_json
    )

    # detect_exhaustion fuses stderr usage check + stdout rate_limit_event.
    rate_limited_stdout = (
        '{"type":"rate_limit_event","rate_limit_info":{"status":"blocked","resetsAt":1700000000}}\n'
    )
    exhausted, resets_at = runner.detect_exhaustion(rate_limited_stdout, "")
    assert exhausted is True
    assert resets_at == 1700000000
    # Module-level helpers agree on the components.
    expected_stdout_blocked, expected_resets = _check_rate_limit_event(rate_limited_stdout)
    assert expected_stdout_blocked is True
    assert _is_usage_exhausted("") is False

    # detect_overload delegates to _check_overloaded_event.
    overloaded_stdout = '{"api_error_status":529}\n'
    assert runner.detect_overload(overloaded_stdout, "") is True
    assert runner.detect_overload(overloaded_stdout, "") == _check_overloaded_event(
        overloaded_stdout
    )


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
