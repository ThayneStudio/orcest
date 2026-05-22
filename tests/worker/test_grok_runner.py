"""Unit tests for GrokRunner — parsing, exhaustion/overload detection, and the
Path B OAuth-blob credential hooks. Fixtures in tests/worker/fixtures/ were
captured from a live grok 0.1.216 run."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from orcest.worker._runner_base import CredentialContext
from orcest.worker.grok_runner import GrokRunner

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


# ---------------------------------------------------------------------------
# build_argv
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_argv_headless_streaming_json() -> None:
    argv = GrokRunner().build_argv("grok", "fix the failing test", "grok-build", Path("/wd"))
    assert argv == [
        "grok",
        "-p",
        "fix the failing test",
        "--output-format",
        "streaming-json",
        "--always-approve",
        "--cwd",
        "/wd",
        "-m",
        "grok-build",
    ]


@pytest.mark.unit
def test_build_argv_omits_model_when_empty() -> None:
    argv = GrokRunner().build_argv("grok", "p", "", Path("/wd"))
    assert "-m" not in argv
    assert argv[-2:] == ["--cwd", "/wd"]


@pytest.mark.unit
def test_build_argv_never_contains_credential() -> None:
    # Credential is delivered via auth.json/env, never argv.
    argv = GrokRunner().build_argv("grok", "p", "grok-build", Path("/wd"))
    assert not any("auth" in a.lower() or a.startswith("xai-") or a.startswith("{") for a in argv)


# ---------------------------------------------------------------------------
# Output parsing (real fixtures)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_simple_reply() -> None:
    assert GrokRunner().extract_summary(_fixture("grok_simple_reply.jsonl")) == "hello from grok"


@pytest.mark.unit
def test_extract_summary_tool_use_returns_agent_answer() -> None:
    summary = GrokRunner().extract_summary(_fixture("grok_tool_use.jsonl"))
    assert summary.startswith("Done. The file")
    assert "world" in summary


@pytest.mark.unit
def test_extract_summary_ignores_thought_events() -> None:
    # The simple-reply fixture has 17 'thought' events and 4 'text' events;
    # the summary must contain only the concatenated text, no reasoning.
    out = _fixture("grok_simple_reply.jsonl")
    assert "The user query" not in GrokRunner().extract_summary(out)


@pytest.mark.unit
def test_extract_summary_empty_output() -> None:
    assert GrokRunner().extract_summary("") == "No summary available"


@pytest.mark.unit
def test_extract_agent_text_equals_concatenated_text() -> None:
    out = _fixture("grok_simple_reply.jsonl")
    assert GrokRunner().extract_agent_text(out) == "hello from grok"


# ---------------------------------------------------------------------------
# NEEDS_HUMAN — grok text events are pure agent output (no prompt echo)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_needs_human_detected_in_text_events() -> None:
    from orcest.worker._runner_base import _check_needs_human

    text_data = "I cannot proceed.\nNEEDS_HUMAN: rotate the API key"
    stdout = "\n".join(
        [
            json.dumps({"type": "thought", "data": "considering"}),
            json.dumps({"type": "text", "data": text_data}),
            json.dumps({"type": "end", "stopReason": "EndTurn"}),
        ]
    )
    agent_text = GrokRunner().extract_agent_text(stdout)
    flag, reason = _check_needs_human(agent_text)
    assert flag is True
    assert reason == "rotate the API key"


@pytest.mark.unit
def test_needs_human_not_triggered_by_thought_events() -> None:
    from orcest.worker._runner_base import _check_needs_human

    # A NEEDS_HUMAN mention only in 'thought' (reasoning) must NOT count —
    # extract_agent_text drops thoughts.
    stdout = "\n".join(
        [
            json.dumps({"type": "thought", "data": "Maybe I should emit NEEDS_HUMAN: foo"}),
            json.dumps({"type": "text", "data": "All done."}),
            json.dumps({"type": "end", "stopReason": "EndTurn"}),
        ]
    )
    flag, _ = _check_needs_human(GrokRunner().extract_agent_text(stdout))
    assert flag is False


# ---------------------------------------------------------------------------
# Exhaustion / overload detection
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_detect_exhaustion_429() -> None:
    stdout = json.dumps({"type": "error", "message": "API error (status 429 Too Many Requests)"})
    exhausted, resets_at = GrokRunner().detect_exhaustion(stdout, "")
    assert exhausted is True
    assert resets_at == 0  # grok events carry no reset timestamp


@pytest.mark.unit
def test_detect_exhaustion_from_stderr_rate_limit() -> None:
    exhausted, _ = GrokRunner().detect_exhaustion("", "responses API error: rate limit exceeded")
    assert exhausted is True


@pytest.mark.unit
def test_detect_exhaustion_403_no_credits_is_not_exhaustion() -> None:
    # A 403 "no credits/licenses" is a permanent billing/config problem, NOT a
    # transient rate limit — must not be mistaken for exhaustion.
    err = _fixture("grok_error_403.jsonl")
    assert GrokRunner().detect_exhaustion(err, "") == (False, 0)


@pytest.mark.unit
def test_detect_exhaustion_clean_output_is_false() -> None:
    assert GrokRunner().detect_exhaustion(_fixture("grok_simple_reply.jsonl"), "") == (False, 0)


@pytest.mark.unit
def test_detect_overload_5xx() -> None:
    stdout = json.dumps({"type": "error", "message": "API error (status 503 Service Unavailable)"})
    assert GrokRunner().detect_overload(stdout, "") is True


@pytest.mark.unit
def test_detect_overload_does_not_match_generic_internal_error() -> None:
    # "Internal error: max_turns exceeded" is not a server overload.
    stdout = json.dumps({"type": "error", "message": 'Internal error: "max_turns exceeded"'})
    assert GrokRunner().detect_overload(stdout, "") is False


@pytest.mark.unit
def test_detect_overload_clean_output_is_false() -> None:
    assert GrokRunner().detect_overload(_fixture("grok_tool_use.jsonl"), "") is False


# ---------------------------------------------------------------------------
# Credential hooks (Path B: OAuth blob)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_prepare_credential_blob_writes_auth_json(tmp_path) -> None:
    blob = json.dumps({"https://auth.x.ai::scope": {"key": "tok", "refresh_token": "rt"}})
    home = tmp_path / "home"
    home.mkdir()
    ctx = GrokRunner().prepare_credential(blob, tmp_path / "wd", home, "XAI_API_KEY")

    assert isinstance(ctx, CredentialContext)
    # Blob mode: no XAI_API_KEY in env (auth is the file).
    assert ctx.extra_env == {}
    auth = home / ".grok" / "auth.json"
    assert ctx.watch_path == auth
    assert auth.exists()
    assert json.loads(auth.read_text()) == json.loads(blob)
    # 0600 perms (owner-only) for the secret.
    assert (auth.stat().st_mode & 0o777) == 0o600


@pytest.mark.unit
def test_prepare_credential_plain_key_uses_env_var(tmp_path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    ctx = GrokRunner().prepare_credential("xai-abc123", tmp_path / "wd", home, "XAI_API_KEY")
    assert ctx.extra_env == {"XAI_API_KEY": "xai-abc123"}
    assert ctx.watch_path is None
    assert not (home / ".grok" / "auth.json").exists()


@pytest.mark.unit
def test_prepare_credential_empty_is_noop(tmp_path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    ctx = GrokRunner().prepare_credential("", tmp_path / "wd", home, "XAI_API_KEY")
    assert ctx.extra_env == {}
    assert ctx.watch_path is None


@pytest.mark.unit
def test_extract_credential_update_detects_refresh(tmp_path) -> None:
    auth = tmp_path / "auth.json"
    original = json.dumps({"key": "old-token", "refresh_token": "rt"})
    refreshed = json.dumps({"key": "new-token", "refresh_token": "rt"})
    auth.write_text(refreshed)
    # original (what we injected) differs from the refreshed file → surfaced.
    assert GrokRunner().extract_credential_update(auth, original) == refreshed


@pytest.mark.unit
def test_extract_credential_update_none_when_unchanged(tmp_path) -> None:
    auth = tmp_path / "auth.json"
    blob = json.dumps({"key": "tok"})
    auth.write_text(blob)
    assert GrokRunner().extract_credential_update(auth, blob) is None


@pytest.mark.unit
def test_extract_credential_update_none_when_missing(tmp_path) -> None:
    assert GrokRunner().extract_credential_update(tmp_path / "nope.json", "x") is None
