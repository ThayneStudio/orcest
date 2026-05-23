"""Unit tests for CodexRunner — parsing, exhaustion/overload detection, and
the Path B (ChatGPT OAuth blob) credential hooks. Fixtures in
tests/worker/fixtures/codex_*.jsonl were captured from a live codex-cli
0.131.0 run; codex_rate_limit.jsonl is a synthetic turn.failed sample
matching the documented 429 message shape (no easy way to force a real
rate-limit during dev)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from orcest.worker._runner_base import CredentialContext
from orcest.worker.codex_runner import CodexRunner

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


# ---------------------------------------------------------------------------
# build_argv — prompt arrives on STDIN via the trailing `-`, never on argv
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_prompt_via_stdin_is_true() -> None:
    """Class-level flag must be True so the driver pipes the prompt on stdin."""
    assert CodexRunner.prompt_via_stdin is True


@pytest.mark.unit
def test_build_argv_codex_exec_with_stdin_dash() -> None:
    argv = CodexRunner().build_argv("codex", "PROMPT-WONT-APPEAR", "gpt-5-codex", Path("/wd"))
    assert argv[:2] == ["codex", "exec"]
    assert "--experimental-json" in argv
    assert "--sandbox" in argv and "danger-full-access" in argv
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    assert "--skip-git-repo-check" in argv
    assert argv[-1] == "-", "trailing '-' makes codex read the prompt from stdin"
    assert "--cd" in argv and "/wd" in argv
    assert "-m" in argv and "gpt-5-codex" in argv


@pytest.mark.unit
def test_build_argv_omits_model_when_empty() -> None:
    argv = CodexRunner().build_argv("codex", "p", "", Path("/wd"))
    assert "-m" not in argv
    assert argv[-1] == "-"


@pytest.mark.unit
def test_build_argv_never_contains_prompt_or_credential() -> None:
    """Prompt is delivered via stdin, credential via auth.json / env — never argv."""
    argv = CodexRunner().build_argv("codex", "leak me", "gpt-5-codex", Path("/wd"))
    assert "leak me" not in argv
    assert not any(a.startswith("sk-") or a.startswith("{") for a in argv)


# ---------------------------------------------------------------------------
# Output parsing (real fixtures)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_simple_reply() -> None:
    assert CodexRunner().extract_summary(_fixture("codex_simple_reply.jsonl")) == "codex hello"


@pytest.mark.unit
def test_extract_summary_tool_use_returns_final_agent_message() -> None:
    """Codex emits multiple agent_message items per turn (narration + final).
    The summary is the LAST agent_message — the canonical reply."""
    summary = CodexRunner().extract_summary(_fixture("codex_tool_use.jsonl"))
    assert summary == "Done."


@pytest.mark.unit
def test_extract_agent_text_concatenates_all_agent_messages() -> None:
    """For NEEDS_HUMAN safety the extractor concatenates ALL agent_messages,
    so the signal can't be hidden by appearing in a non-final message."""
    text = CodexRunner().extract_agent_text(_fixture("codex_tool_use.jsonl"))
    assert "Done." in text
    assert "CODEX.txt" in text  # the earlier narration message
    # Multiple lines (one per agent_message), so the NEEDS_HUMAN ^-anchored
    # regex can match any of them.
    assert text.count("\n") >= 1


@pytest.mark.unit
def test_extract_summary_ignores_non_agent_message_items() -> None:
    """item.completed of type file_change / command_execution must NOT appear
    in the summary or agent text."""
    out = _fixture("codex_tool_use.jsonl")
    text = CodexRunner().extract_agent_text(out)
    assert "file_change" not in text
    assert "in_progress" not in text


@pytest.mark.unit
def test_extract_summary_empty_output() -> None:
    assert CodexRunner().extract_summary("") == "No summary available"


# ---------------------------------------------------------------------------
# NEEDS_HUMAN — codex prompt is on stdin, never echoed, so concat is safe
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_needs_human_detected_in_agent_message() -> None:
    from orcest.worker._runner_base import _check_needs_human

    stdout = "\n".join(
        [
            json.dumps({"type": "thread.started", "thread_id": "x"}),
            json.dumps({"type": "turn.started"}),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "i0",
                        "type": "agent_message",
                        "text": "I can't proceed.\nNEEDS_HUMAN: rotate the auth blob",
                    },
                }
            ),
            json.dumps({"type": "turn.completed", "usage": {}}),
        ]
    )
    flag, reason = _check_needs_human(CodexRunner().extract_agent_text(stdout))
    assert flag is True
    assert reason == "rotate the auth blob"


# ---------------------------------------------------------------------------
# Exhaustion / overload detection
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_detect_exhaustion_429_in_turn_failed() -> None:
    """The canonical codex-exec rate-limit signal: turn.failed with
    'exceeded retry limit, last status: 429 Too Many Requests'."""
    exhausted, resets_at = CodexRunner().detect_exhaustion(_fixture("codex_rate_limit.jsonl"), "")
    assert exhausted is True
    assert resets_at == 0  # codex events carry no reset timestamp


@pytest.mark.unit
def test_detect_exhaustion_from_stderr_rate_limit() -> None:
    exhausted, _ = CodexRunner().detect_exhaustion("", "rate limit exceeded; retry later")
    assert exhausted is True


@pytest.mark.unit
def test_detect_exhaustion_clean_output_is_false() -> None:
    assert CodexRunner().detect_exhaustion(_fixture("codex_simple_reply.jsonl"), "") == (False, 0)
    assert CodexRunner().detect_exhaustion(_fixture("codex_tool_use.jsonl"), "") == (False, 0)


@pytest.mark.unit
def test_detect_overload_5xx() -> None:
    stdout = json.dumps(
        {
            "type": "turn.failed",
            "error": {"message": "API error (status 503 Service Unavailable)"},
        }
    )
    assert CodexRunner().detect_overload(stdout, "") is True


@pytest.mark.unit
def test_detect_overload_does_not_match_generic_internal_error() -> None:
    """A bare 'Internal error' (e.g. max-turns) must NOT be flagged transient."""
    stdout = json.dumps(
        {"type": "turn.failed", "error": {"message": 'Internal error: "max_turns exceeded"'}}
    )
    assert CodexRunner().detect_overload(stdout, "") is False


@pytest.mark.unit
def test_detect_overload_clean_output_is_false() -> None:
    assert CodexRunner().detect_overload(_fixture("codex_tool_use.jsonl"), "") is False


# ---------------------------------------------------------------------------
# Credential hooks (Path B: ChatGPT OAuth blob)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_prepare_credential_blob_writes_auth_json(tmp_path) -> None:
    blob = json.dumps(
        {
            "auth_mode": "chatgpt",
            "tokens": {"access_token": "tok", "refresh_token": "rt", "account_id": "acct"},
        }
    )
    home = tmp_path / "home"
    home.mkdir()
    ctx = CodexRunner().prepare_credential(blob, tmp_path / "wd", home, "CODEX_API_KEY")

    assert isinstance(ctx, CredentialContext)
    # Blob mode: no CODEX_API_KEY in env (auth is the file).
    assert ctx.extra_env == {}
    auth = home / ".codex" / "auth.json"
    assert ctx.watch_path == auth
    assert auth.exists()
    assert json.loads(auth.read_text()) == json.loads(blob)
    # 0o600 (owner-only) for the secret.
    assert (auth.stat().st_mode & 0o777) == 0o600


@pytest.mark.unit
def test_prepare_credential_plain_key_uses_codex_api_key_env(tmp_path) -> None:
    """codex exec specifically reads CODEX_API_KEY (NOT OPENAI_API_KEY) —
    the env var name must be exactly CODEX_API_KEY when injecting an API key."""
    home = tmp_path / "home"
    home.mkdir()
    ctx = CodexRunner().prepare_credential("sk-abc123", tmp_path / "wd", home, "CODEX_API_KEY")
    assert ctx.extra_env == {"CODEX_API_KEY": "sk-abc123"}
    assert ctx.watch_path is None
    assert not (home / ".codex" / "auth.json").exists()


@pytest.mark.unit
def test_prepare_credential_invalid_json_blob_falls_back_to_api_key(tmp_path) -> None:
    """Looks like JSON but isn't valid → don't write a corrupt auth.json;
    fall back to the API-key path (better to fail loudly on bad config than
    persist garbage to disk)."""
    home = tmp_path / "home"
    home.mkdir()
    ctx = CodexRunner().prepare_credential("{not-json", tmp_path / "wd", home, "CODEX_API_KEY")
    # blob.strip() truthy → reset to "" by the JSON-validate branch → falls
    # through. The "if credential:" below it is the ORIGINAL credential which
    # is still truthy → API-key path.
    assert ctx.extra_env == {"CODEX_API_KEY": "{not-json"}
    assert not (home / ".codex" / "auth.json").exists()


@pytest.mark.unit
def test_prepare_credential_empty_is_noop(tmp_path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    ctx = CodexRunner().prepare_credential("", tmp_path / "wd", home, "CODEX_API_KEY")
    assert ctx.extra_env == {}
    assert ctx.watch_path is None


@pytest.mark.unit
def test_extract_credential_update_detects_refresh(tmp_path) -> None:
    auth = tmp_path / "auth.json"
    original = json.dumps({"auth_mode": "chatgpt", "tokens": {"access_token": "old"}})
    refreshed = json.dumps({"auth_mode": "chatgpt", "tokens": {"access_token": "new"}})
    auth.write_text(refreshed)
    assert CodexRunner().extract_credential_update(auth, original) == refreshed


@pytest.mark.unit
def test_extract_credential_update_none_when_unchanged(tmp_path) -> None:
    auth = tmp_path / "auth.json"
    blob = json.dumps({"tokens": {"access_token": "tok"}})
    auth.write_text(blob)
    assert CodexRunner().extract_credential_update(auth, blob) is None


@pytest.mark.unit
def test_extract_credential_update_rejects_corrupt_blob(tmp_path) -> None:
    """A partial/corrupt auth.json (codex killed mid-refresh) must NOT be
    propagated — persisting it would brick the credential for all subsequent
    tasks on this identity."""
    auth = tmp_path / "auth.json"
    auth.write_text('{"tokens":{"access_token":"truncat')
    assert (
        CodexRunner().extract_credential_update(auth, '{"tokens":{"access_token":"old"}}') is None
    )


@pytest.mark.unit
def test_extract_credential_update_none_when_missing(tmp_path) -> None:
    assert CodexRunner().extract_credential_update(tmp_path / "nope.json", "x") is None
