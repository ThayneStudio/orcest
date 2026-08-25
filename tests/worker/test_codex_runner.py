"""Unit tests for CodexRunner — parsing, exhaustion/overload detection, and
the Path B (ChatGPT OAuth blob) credential hooks.

Fixture provenance (also labeled in-file):
- ``codex_simple_reply.jsonl`` / ``codex_tool_use.jsonl``: 0.149.1 JSONL
  contract (``codex exec --json``; rust-v0.149.1 ``exec_events.rs``).
  Authenticated live recapture is #620.
- ``codex_failure.jsonl``: captured from ``@openai/codex@0.149.1``
  ``codex exec --json`` (unauthenticated 401).
- ``codex_rate_limit.jsonl``, ``codex_overload.jsonl``,
  ``codex_exhaustion.jsonl``, ``codex_auth_*.json``: synthetic, labeled
  ``# provenance: synthetic`` in the fixture files.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from orcest.worker._runner_base import CredentialContext
from orcest.worker.codex_runner import CodexRunner

FIXTURES = Path(__file__).parent / "fixtures"

CAPTURED_JSONL = (
    "codex_simple_reply.jsonl",
    "codex_tool_use.jsonl",
    "codex_failure.jsonl",
)
SYNTHETIC_FIXTURES = (
    "codex_rate_limit.jsonl",
    "codex_overload.jsonl",
    "codex_exhaustion.jsonl",
    "codex_auth_original.json",
    "codex_auth_refreshed.json",
)


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def _json_body(name: str) -> str:
    """Return fixture contents with provenance comment lines stripped."""
    return "\n".join(
        line for line in _fixture(name).splitlines() if not line.lstrip().startswith("#")
    ).strip()


# ---------------------------------------------------------------------------
# Pin / flag / provenance
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_synthetic_fixtures_label_provenance() -> None:
    for name in SYNTHETIC_FIXTURES:
        first = next(line for line in _fixture(name).splitlines() if line.strip())
        assert first.startswith("# provenance: synthetic"), name


@pytest.mark.unit
def test_captured_jsonl_fixtures_label_provenance() -> None:
    for name in CAPTURED_JSONL:
        first = next(line for line in _fixture(name).splitlines() if line.strip())
        assert first.startswith("# provenance:"), name
        assert "0.149.1" in first, name


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
    assert "--json" in argv
    assert "--experimental-json" not in argv
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


@pytest.mark.unit
@pytest.mark.parametrize("effort", ["max", "ultra", "xhigh", "custom-effort"])
def test_build_argv_does_not_add_reasoning_effort_flag(effort: str) -> None:
    """Codex 0.149.1 accepts ``max`` / ``ultra`` / unknown custom strings via
    ``config.toml`` or ``-c model_reasoning_effort=...``. Orcest must not add
    its own reasoning-effort flag that would reject them."""
    argv = CodexRunner().build_argv("codex", "p", "gpt-5-codex", Path("/wd"))
    joined = " ".join(argv)
    assert "--reasoning-effort" not in argv
    assert "model_reasoning_effort" not in joined
    assert effort not in argv


# ---------------------------------------------------------------------------
# Output parsing (0.149.1 fixtures)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_summary_simple_reply() -> None:
    assert CodexRunner().extract_summary(_fixture("codex_simple_reply.jsonl")) == "codex hello"


@pytest.mark.unit
def test_extract_summary_ignores_reasoning_items() -> None:
    summary = CodexRunner().extract_summary(_fixture("codex_simple_reply.jsonl"))
    assert summary == "codex hello"
    assert "short greeting" not in summary


@pytest.mark.unit
def test_extract_agent_text_excludes_reasoning_items() -> None:
    text = CodexRunner().extract_agent_text(_fixture("codex_simple_reply.jsonl"))
    assert text == "codex hello"
    assert "short greeting" not in text


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
    assert "command_execution" not in text
    assert "in_progress" not in text


@pytest.mark.unit
def test_extract_summary_ordinary_failure_has_no_agent_message() -> None:
    assert CodexRunner().extract_summary(_fixture("codex_failure.jsonl")) == "No summary available"
    assert CodexRunner().extract_agent_text(_fixture("codex_failure.jsonl")) == ""


@pytest.mark.unit
def test_extract_summary_empty_output() -> None:
    assert CodexRunner().extract_summary("") == "No summary available"


@pytest.mark.unit
def test_provenance_comments_are_ignored_by_parser() -> None:
    assert CodexRunner().extract_summary(_fixture("codex_simple_reply.jsonl")) == "codex hello"
    assert "# provenance" not in CodexRunner().extract_agent_text(_fixture("codex_tool_use.jsonl"))


@pytest.mark.unit
@pytest.mark.parametrize("effort", ["max", "ultra", "custom-effort"])
def test_parser_accepts_custom_reasoning_effort_strings(effort: str) -> None:
    """Reasoning items that mention custom effort strings must not break
    summary extraction or be mistaken for exhaustion/overload."""
    stdout = "\n".join(
        [
            json.dumps({"type": "thread.started", "thread_id": "t"}),
            json.dumps({"type": "turn.started"}),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"id": "item_0", "type": "reasoning", "text": f"effort={effort}"},
                }
            ),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"id": "item_1", "type": "agent_message", "text": "ok"},
                }
            ),
            json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {
                        "input_tokens": 1,
                        "cached_input_tokens": 0,
                        "cache_write_input_tokens": 0,
                        "output_tokens": 1,
                        "reasoning_output_tokens": 0,
                    },
                }
            ),
        ]
    )
    assert CodexRunner().extract_summary(stdout) == "ok"
    assert effort not in CodexRunner().extract_summary(stdout)
    assert CodexRunner().detect_exhaustion(stdout, "") == (False, 0)
    assert CodexRunner().detect_overload(stdout, "") is False


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


@pytest.mark.unit
def test_needs_human_not_triggered_by_reasoning_or_error_items() -> None:
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
                        "type": "reasoning",
                        "text": "Maybe emit NEEDS_HUMAN: ignore me",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "i1",
                        "type": "error",
                        "message": "NEEDS_HUMAN: not agent text",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"id": "i2", "type": "agent_message", "text": "All done."},
                }
            ),
            json.dumps({"type": "turn.completed", "usage": {}}),
        ]
    )
    flag, _ = _check_needs_human(CodexRunner().extract_agent_text(stdout))
    assert flag is False


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
def test_detect_exhaustion_quota_fixture() -> None:
    exhausted, resets_at = CodexRunner().detect_exhaustion(_fixture("codex_exhaustion.jsonl"), "")
    assert exhausted is True
    assert resets_at == 0


@pytest.mark.unit
def test_detect_exhaustion_from_stderr_rate_limit() -> None:
    exhausted, _ = CodexRunner().detect_exhaustion("", "rate limit exceeded; retry later")
    assert exhausted is True


@pytest.mark.unit
def test_detect_exhaustion_clean_output_is_false() -> None:
    assert CodexRunner().detect_exhaustion(_fixture("codex_simple_reply.jsonl"), "") == (False, 0)
    assert CodexRunner().detect_exhaustion(_fixture("codex_tool_use.jsonl"), "") == (False, 0)
    assert CodexRunner().detect_exhaustion(_fixture("codex_failure.jsonl"), "") == (False, 0)


@pytest.mark.unit
def test_detect_overload_5xx_fixture() -> None:
    assert CodexRunner().detect_overload(_fixture("codex_overload.jsonl"), "") is True


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
    assert CodexRunner().detect_overload(_fixture("codex_failure.jsonl"), "") is False
    assert CodexRunner().detect_overload(_fixture("codex_rate_limit.jsonl"), "") is False


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
def test_prepare_credential_plain_key_removes_stale_oauth_file(tmp_path) -> None:
    home = tmp_path / "home"
    stale_auth = home / ".codex" / "auth.json"
    stale_auth.parent.mkdir(parents=True)
    stale_auth.write_text(json.dumps({"tokens": {"refresh_token": "old"}}))

    ctx = CodexRunner().prepare_credential("sk-new", tmp_path / "wd", home, "CODEX_API_KEY")

    assert ctx.extra_env == {"CODEX_API_KEY": "sk-new"}
    assert not stale_auth.exists()


@pytest.mark.unit
def test_prepare_credential_fails_closed_if_stale_auth_cannot_be_removed(
    tmp_path, monkeypatch
) -> None:
    home = tmp_path / "home"
    stale_auth = home / ".codex" / "auth.json"
    stale_auth.parent.mkdir(parents=True)
    stale_auth.write_text(json.dumps({"tokens": {"refresh_token": "old"}}))
    original_unlink = Path.unlink

    def fail_auth_unlink(path: Path, *args, **kwargs):
        if path == stale_auth:
            raise OSError("permission denied")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_auth_unlink)

    with pytest.raises(RuntimeError, match="Failed to remove stale Codex auth file"):
        CodexRunner().prepare_credential("sk-new", tmp_path / "wd", home, "CODEX_API_KEY")


@pytest.mark.unit
def test_prepare_credential_oauth_without_refresh_token_removes_stale_file(tmp_path) -> None:
    home = tmp_path / "home"
    stale_auth = home / ".codex" / "auth.json"
    stale_auth.parent.mkdir(parents=True)
    stale_auth.write_text(json.dumps({"tokens": {"refresh_token": "old"}}))

    ctx = CodexRunner().prepare_credential(
        json.dumps({"tokens": {"access_token": "access-only"}}),
        tmp_path / "wd",
        home,
        "CODEX_API_KEY",
    )

    assert ctx == CredentialContext()
    assert not stale_auth.exists()


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
def test_prepare_credential_empty_removes_stale_oauth_file(tmp_path) -> None:
    home = tmp_path / "home"
    stale_auth = home / ".codex" / "auth.json"
    stale_auth.parent.mkdir(parents=True)
    stale_auth.write_text(json.dumps({"tokens": {"refresh_token": "old"}}))

    CodexRunner().prepare_credential("", tmp_path / "wd", home, "CODEX_API_KEY")

    assert not stale_auth.exists()


@pytest.mark.unit
def test_extract_credential_update_detects_refresh(tmp_path) -> None:
    original = _json_body("codex_auth_original.json")
    refreshed = _json_body("codex_auth_refreshed.json")
    auth = tmp_path / "auth.json"
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
def test_extract_credential_update_rejects_blob_without_refresh_token(tmp_path) -> None:
    auth = tmp_path / "auth.json"
    auth.write_text(json.dumps({"tokens": {"access_token": "new"}}))
    assert CodexRunner().extract_credential_update(auth, "original") is None


@pytest.mark.unit
def test_extract_credential_update_none_when_missing(tmp_path) -> None:
    assert CodexRunner().extract_credential_update(tmp_path / "nope.json", "x") is None
