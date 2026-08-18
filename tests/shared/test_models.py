"""Unit tests for Task and TaskResult dataclasses."""

import pytest

from orcest.shared.models import (
    REDACTED_FIELDS,
    ResultStatus,
    Task,
    TaskResult,
    TaskType,
    is_claude_provider,
    task_stream_name,
)


def _make_task(**overrides) -> Task:
    """Helper to build a Task with sensible defaults."""
    defaults = {
        "task_type": TaskType.FIX_CI,
        "repo": "acme/widget",
        "token": "ghp_fake123",
        "resource_type": "pr",
        "resource_id": 42,
        "prompt": "Fix the failing lint check",
        "branch": "fix/lint",
    }
    defaults.update(overrides)
    return Task.create(**defaults)


def _make_task_result(**overrides) -> TaskResult:
    """Helper to build a TaskResult with sensible defaults."""
    defaults = {
        "task_id": "aaa-bbb-ccc",
        "worker_id": "worker-7",
        "status": ResultStatus.COMPLETED,
        "branch": "fix/lint",
        "summary": "Fixed 3 ruff errors",
        "duration_seconds": 120,
        "resource_type": "pr",
        "resource_id": 42,
        "repo": "acme/widget",
    }
    defaults.update(overrides)
    return TaskResult(**defaults)


def test_task_stream_name_builds_provider_specific_streams():
    assert task_stream_name("claude") == "tasks:claude"
    assert task_stream_name("clauder") == "tasks:clauder"
    assert task_stream_name("grok", issue=True) == "tasks:issue:grok"


def test_task_stream_name_rejects_empty_provider():
    with pytest.raises(ValueError, match="provider"):
        task_stream_name(" ")


@pytest.mark.parametrize(
    "provider", ["issue:grok", "GROK", "grok name", "grok\nname", "my-provider", "my.provider"]
)
def test_task_stream_name_rejects_unsafe_provider(provider):
    with pytest.raises(ValueError, match="Invalid provider name"):
        task_stream_name(provider)


def test_task_create_rejects_unsafe_provider():
    with pytest.raises(ValueError, match="Invalid provider name"):
        _make_task(provider="issue:grok")


def test_task_create_generates_unique_ids():
    t1 = _make_task()
    t2 = _make_task()
    assert t1.id != t2.id


def test_clauder_is_a_claude_backed_provider_alias():
    task = _make_task(provider="clauder", claude_token="claude-oauth-token")

    assert is_claude_provider("clauder") is True
    assert task.provider == "clauder"
    assert task.credential == "claude-oauth-token"
    assert task.claude_token == "claude-oauth-token"


def test_task_to_dict_values_are_strings():
    task = _make_task()
    d = task.to_dict()
    for key, value in d.items():
        assert isinstance(value, str), f"to_dict()[{key!r}] is {type(value).__name__}, expected str"


def test_task_to_dict_from_dict_round_trip():
    original = _make_task(
        snapshot_head_sha="abc123",
        decision_reason="ci_failure",
        snapshot_failed_checks=["tests"],
        snapshot_review_thread_ids=["thread-1"],
        snapshot_review_thread_fingerprints=["thread-1:fingerprint"],
        provider_account="grok:0123456789ab",
    )
    rebuilt = Task.from_dict(original.to_dict())

    assert rebuilt.id == original.id
    assert rebuilt.type == original.type
    assert rebuilt.repo == original.repo
    assert rebuilt.token == original.token
    assert rebuilt.resource_type == original.resource_type
    assert rebuilt.resource_id == original.resource_id
    assert rebuilt.prompt == original.prompt
    assert rebuilt.branch == original.branch
    assert rebuilt.snapshot_head_sha == "abc123"
    assert rebuilt.decision_reason == "ci_failure"
    assert rebuilt.snapshot_failed_checks == ["tests"]
    assert rebuilt.snapshot_review_thread_ids == ["thread-1"]
    assert rebuilt.snapshot_review_thread_fingerprints == ["thread-1:fingerprint"]
    assert rebuilt.provider_account == "grok:0123456789ab"
    # Datetime round-trip through isoformat loses sub-microsecond
    # precision but should be equal within a second.
    assert abs((rebuilt.created_at - original.created_at).total_seconds()) < 1


def test_task_none_branch_serializes_to_empty_string():
    task = _make_task(branch=None)
    d = task.to_dict()
    assert d["branch"] == ""

    rebuilt = Task.from_dict(d)
    # from_dict uses `data["branch"] or None`, so empty string -> None
    assert rebuilt.branch is None


def test_task_type_enum_round_trip():
    for member in TaskType:
        assert TaskType(member.value) is member


def test_task_result_credential_update_round_trip():
    """credential_update survives serialization (worker -> Redis -> orchestrator)."""
    original = _make_task_result(credential_update='{"key":"rotated","refresh_token":"rt"}')
    rebuilt = TaskResult.from_dict(original.to_dict())
    assert rebuilt.credential_update == '{"key":"rotated","refresh_token":"rt"}'


def test_task_result_credential_update_timestamp_round_trip():
    original = _make_task_result(
        credential_update='{"refresh_token":"rt"}',
        credential_update_minted_at=123.5,
    )
    rebuilt = TaskResult.from_dict(original.to_dict())
    assert rebuilt.credential_update_minted_at == 123.5


def test_task_result_provider_account_round_trip_is_non_secret_metadata():
    original = _make_task_result(
        provider_account="grok:0123456789ab",
    )

    wire = original.to_dict()
    rebuilt = TaskResult.from_dict(wire)

    assert wire["provider_account"] == "grok:0123456789ab"
    assert rebuilt.provider_account == "grok:0123456789ab"


def test_task_result_repo_does_not_shift_historic_positional_arguments():
    result = TaskResult(
        "task-1",
        "worker-1",
        ResultStatus.COMPLETED,
        None,
        "done",
        5,
        "pr",
        42,
        123,
    )
    assert result.rate_limit_resets_at == 123
    assert result.repo == ""


def test_task_result_credential_update_absent_by_default():
    d = _make_task_result().to_dict()
    assert "credential_update" not in d
    assert TaskResult.from_dict(d).credential_update == ""


def test_task_result_to_safe_dict_redacts_credential_update():
    """The blob is a secret — to_safe_dict masks it for logging."""
    r = _make_task_result(credential_update='{"key":"secret-token"}')
    safe = r.to_safe_dict()
    assert safe["credential_update"] == "[REDACTED]"
    assert "secret-token" not in str(safe)
    # to_dict (the Redis payload, trusted channel) keeps the real value.
    assert r.to_dict()["credential_update"] == '{"key":"secret-token"}'


def test_task_result_to_dict_from_dict_round_trip():
    original = _make_task_result(
        snapshot_head_sha="abc123",
        decision_reason="changes_requested",
        snapshot_review_thread_ids=["thread-1"],
        snapshot_review_thread_fingerprints=["thread-1:fingerprint"],
    )
    rebuilt = TaskResult.from_dict(original.to_dict())

    assert rebuilt.task_id == original.task_id
    assert rebuilt.worker_id == original.worker_id
    assert rebuilt.status == original.status
    assert rebuilt.branch == original.branch
    assert rebuilt.summary == original.summary
    assert rebuilt.duration_seconds == original.duration_seconds
    assert rebuilt.resource_type == original.resource_type
    assert rebuilt.resource_id == original.resource_id
    assert rebuilt.repo == original.repo
    assert rebuilt.snapshot_head_sha == "abc123"
    assert rebuilt.decision_reason == "changes_requested"
    assert rebuilt.snapshot_review_thread_ids == ["thread-1"]
    assert rebuilt.snapshot_review_thread_fingerprints == ["thread-1:fingerprint"]


def test_result_status_enum_round_trip():
    for member in ResultStatus:
        assert ResultStatus(member.value) is member


def test_task_claude_token_round_trip():
    task = _make_task(claude_token="sk-ant-oat01-test")
    d = task.to_dict()
    assert d["claude_token"] == "sk-ant-oat01-test"
    rebuilt = Task.from_dict(d)
    assert rebuilt.claude_token == "sk-ant-oat01-test"


def test_task_from_dict_missing_claude_token_defaults_empty():
    task = _make_task()
    d = task.to_dict()
    del d["claude_token"]
    rebuilt = Task.from_dict(d)
    assert rebuilt.claude_token == ""


def test_task_from_dict_missing_key_raises():
    task = _make_task()
    d = task.to_dict()
    del d["repo"]
    with pytest.raises(KeyError):
        Task.from_dict(d)


def test_task_from_dict_invalid_type_raises():
    task = _make_task()
    d = task.to_dict()
    d["type"] = "invalid_type"
    with pytest.raises(ValueError):
        Task.from_dict(d)


def test_task_result_from_dict_invalid_status_raises():
    result = _make_task_result()
    d = result.to_dict()
    d["status"] = "invalid_status"
    with pytest.raises(ValueError):
        TaskResult.from_dict(d)


def test_task_result_branch_none_roundtrip():
    result = _make_task_result(branch=None)
    d = result.to_dict()
    assert d["branch"] == ""

    rebuilt = TaskResult.from_dict(d)
    assert rebuilt.branch is None


def test_task_result_from_dict_legacy_repo_defaults_empty():
    result = _make_task_result()
    d = result.to_dict()
    del d["repo"]

    rebuilt = TaskResult.from_dict(d)

    assert rebuilt.repo == ""


def test_task_result_needs_human_round_trip():
    """needs_human / needs_human_reason survive to_dict -> from_dict."""
    original = _make_task_result(
        status=ResultStatus.FAILED,
        needs_human=True,
        needs_human_reason="product owner must pick the canonical role name",
    )
    rebuilt = TaskResult.from_dict(original.to_dict())
    assert rebuilt.needs_human is True
    assert rebuilt.needs_human_reason == ("product owner must pick the canonical role name")


def test_task_result_needs_human_defaults_false():
    """A result without the needs_human signal deserializes to False."""
    rebuilt = TaskResult.from_dict(_make_task_result().to_dict())
    assert rebuilt.needs_human is False
    assert rebuilt.needs_human_reason == ""


# ---------------------------------------------------------------------------
# Task 2: new fields, to_safe_dict redaction, legacy from_dict, __repr__, DL paths
# These are the TDD failing tests added in Step 2.1 before implementation.
# ---------------------------------------------------------------------------


def test_task_new_provider_credential_model_fields_with_defaults():
    """Task exposes provider/credential/model (for multi-provider) with compat defaults."""
    # No claude_token passed -> credential empty, provider defaults to claude
    task = _make_task()
    assert task.provider == "claude"
    assert task.credential == ""
    assert task.model is None
    assert hasattr(task, "claude_token")  # kept for transition

    # When claude_token passed via create (current call sites), it populates credential too
    task_ct = _make_task(claude_token="sk-ant-oat01-test123")
    assert task_ct.provider == "claude"
    assert task_ct.credential == "sk-ant-oat01-test123"
    assert task_ct.claude_token == "sk-ant-oat01-test123"
    assert task_ct.model is None

    # Explicit new-style creation (for future providers)
    task_grok = _make_task(provider="grok", credential="xai-secret-abc987", model="grok-3-latest")
    assert task_grok.provider == "grok"
    assert task_grok.credential == "xai-secret-abc987"
    assert task_grok.model == "grok-3-latest"
    # claude_token remains empty for non-claude (current design keeps them separate)
    assert task_grok.claude_token == ""


def test_task_to_safe_dict_redacts_secrets():
    """to_safe_dict returns flat dict with REDACTED_FIELDS masked; used by DL, logs, repr."""
    secret = "sk-ant-oat01-verysecretvalue"
    task = _make_task(claude_token=secret, token="ghp_ghsecret456")
    safe = task.to_safe_dict()

    # Must be dict[str,str] like to_dict
    assert isinstance(safe, dict)
    for k, v in safe.items():
        assert isinstance(k, str) and isinstance(v, str)

    # Secrets must be redacted; marker must not leak the value
    for f in ("token", "claude_token", "credential"):
        assert f in safe, f"REDACTED field {f} must be present in safe dict"
        assert secret not in safe[f]
        assert "ghsecret" not in safe[f]
        assert safe[f] == "[REDACTED]"

    # Non-secrets preserved
    assert safe["id"] == task.id
    assert safe["repo"] == task.repo
    assert safe["type"] == task.type.value
    assert safe["provider"] == "claude"
    assert safe["model"] == ""

    # REDACTED_FIELDS is the canonical set
    assert "token" in REDACTED_FIELDS
    assert "claude_token" in REDACTED_FIELDS
    assert "credential" in REDACTED_FIELDS
    # model is not secret
    assert "model" not in REDACTED_FIELDS


def test_task_from_dict_tolerates_legacy_claude_token_payload():
    """from_dict must accept old payloads (only claude_token, no provider/credential/model)."""
    legacy_d = {
        "id": "legacy-task-001",
        "type": "fix_ci",
        "repo": "acme/legacy",
        "token": "ghp_legacy_pat",
        "claude_token": "sk-legacy-claude-999999",
        "resource_type": "pr",
        "resource_id": "99",
        "prompt": "legacy prompt",
        "branch": "fix/legacy",
        "base_branch": "main",
        "key_prefix": "proj:",
        "created_at": "2026-05-01T12:00:00+00:00",
        # deliberately no 'provider', 'credential', 'model' keys (old orchestrator)
        "snapshot_head_sha": "",
        "decision_reason": "",
        "snapshot_failed_checks": "[]",
        "snapshot_review_thread_ids": "[]",
        "snapshot_review_thread_fingerprints": "[]",
    }
    task = Task.from_dict(legacy_d)

    assert task.id == "legacy-task-001"
    assert task.claude_token == "sk-legacy-claude-999999"
    # Must synthesize for new fields from legacy claude_token
    assert task.provider == "claude"
    assert task.credential == "sk-legacy-claude-999999"
    assert task.model is None

    # Round-trip via to_dict should now emit the new fields + keep claude_token for workers
    d2 = task.to_dict()
    assert d2["provider"] == "claude"
    assert d2["credential"] == "sk-legacy-claude-999999"
    assert d2["claude_token"] == "sk-legacy-claude-999999"
    assert d2["model"] == ""


def test_task_from_dict_explicit_empty_credential_takes_precedence():
    """from_dict must treat explicit 'credential': '' as present (not fallback to claude_token).

    This was the precedence bug: `get("credential") or claude...` treated "" as absent.
    """
    payload = {
        "id": "p1",
        "type": "fix_ci",
        "repo": "a/b",
        "token": "ghp_x",
        "claude_token": "sk-claude-present",
        "credential": "",  # explicit empty for new field (precedence)
        "provider": "claude",
        "resource_type": "pr",
        "resource_id": "1",
        "prompt": "p",
        "branch": "",
        "base_branch": "",
        "key_prefix": "",
        "created_at": "2026-01-01T00:00:00+00:00",
        "snapshot_head_sha": "",
        "decision_reason": "",
        "snapshot_failed_checks": "[]",
        "snapshot_review_thread_ids": "[]",
        "snapshot_review_thread_fingerprints": "[]",
        "model": "",
    }
    task = Task.from_dict(payload)
    assert task.credential == "", (
        "explicit empty credential must be kept (not replaced by claude_token)"
    )
    assert task.claude_token == "sk-claude-present", (
        "claude_token from input preserved when cred explicit empty"
    )
    assert task.provider == "claude"


def test_task_repr_redacts_sensitive_fields():
    """__repr__ must never contain raw secrets (security requirement from subagent review)."""
    secret_ct = "sk-ant-repr-secret-00112233"
    secret_gh = "ghp_reprgh_445566"
    task = _make_task(claude_token=secret_ct, token=secret_gh)

    r = repr(task)
    assert secret_ct not in r, "raw claude credential leaked in Task.__repr__"
    assert secret_gh not in r, "raw github token leaked in Task.__repr__"
    # Should contain redaction marker or masked form
    assert "[REDACTED]" in r or "..." in r


def test_safe_dict_supports_dead_letter_redaction_paths():
    """to_safe_dict is the projection used (or to be used) by dead-letter writers to avoid
    leaking credentials into DL stream, logs, and exception contexts.
    """
    secret = "sk-dl-redact-me-778899"
    task = _make_task(claude_token=secret)

    # This is the shape used in DL writes: {**task.to_xxx(), "dead_letter_reason": ..., ...}
    dl_fields = {
        **task.to_safe_dict(),
        "dead_letter_reason": "Exceeded max delivery count (5)",
        "tasks_stream": "tasks:claude",
        "original_entry_id": "1234-0",
        "delivery_count": "6",
    }
    # No secret value anywhere in the DL payload
    dl_str = str(dl_fields)
    assert secret not in dl_str
    assert dl_fields["credential"] == "[REDACTED]"
    assert dl_fields["claude_token"] == "[REDACTED]"
    assert dl_fields["token"] == "[REDACTED]"
    # Metadata and non-secrets still present for recovery/display
    assert dl_fields["dead_letter_reason"].startswith("Exceeded")
    assert dl_fields["id"] == task.id


def test_task_attempt_roundtrip_and_legacy_default():
    task = _make_task(attempt=3)
    assert task.attempt == 3
    d = task.to_dict()
    assert d["attempt"] == "3"
    assert Task.from_dict(d).attempt == 3
    # legacy payload without the key
    del d["attempt"]
    assert Task.from_dict(d).attempt == 0
