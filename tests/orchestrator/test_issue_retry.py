"""Bounded issue retry context: schema, CAS, prompt rendering."""

from __future__ import annotations

import json

import pytest

from orcest.orchestrator.issue_ops import IssueAction, IssueState
from orcest.orchestrator.issue_publication import (
    make_issue_delivery_cooldown_key,
    make_issue_generation_key,
    make_issue_retry_latest_key,
    make_issue_retry_record_key,
)
from orcest.orchestrator.issue_retry import (
    FORBIDDEN_FIELDS,
    JSON_SCHEMA_KEYS,
    RETRY_CONTEXT_MAX_BYTES,
    IssueRetryContext,
    RetryContextBoundError,
    build_issue_retry_context,
    canonical_pull_url,
    clear_issue_retry_context,
    load_latest_issue_retry_context,
    render_issue_retry_prompt_section,
    retry_context_from_hash,
    same_repo_expected_ref_allowed,
    store_issue_retry_context,
)
from orcest.orchestrator.task_publisher import _render_issue_prompt, publish_issue_task
from orcest.shared.redis_client import RedisClient

REPO = "owner/widgets"
ISSUE = 660
OID = "a" * 40
REF = "issue-660-resume-expected-refs-with-bo"


def _context(**overrides: object) -> IssueRetryContext:
    values: dict[str, object] = {
        "repo": REPO,
        "task_id": "task-660",
        "generation": 1,
        "expected_ref": REF,
        "expected_head_owner": "owner",
        "remote_head_oid": OID,
        "pr_number": 12,
        "reason_code": "no_candidate_pr",
        "created_at": 1_000.0,
        "cooldown_until": 1_020.0,
    }
    values.update(overrides)
    repo = str(values.pop("repo"))
    return build_issue_retry_context(repo=repo, **values)  # type: ignore[arg-type]


def _seed_generation(redis: RedisClient, generation: int) -> None:
    redis.set_value(make_issue_generation_key(REPO, ISSUE), str(generation))


def test_canonical_json_is_fixed_schema_and_sorted():
    payload = json.loads(_context().to_canonical_json())
    assert tuple(payload) == JSON_SCHEMA_KEYS
    assert payload["pr_url"] == "https://github.com/owner/widgets/pull/12"
    assert payload["pr_number"] == 12
    assert payload["generation"] == 1
    assert payload["schema_version"] == 1


def test_url_is_canonicalized_from_repo_and_pr_number():
    ctx = retry_context_from_hash(
        REPO,
        {
            **_context().to_hash(),
            "pr_url": "https://evil.example/attacker?x=1",
            "pr_number": "7",
        },
    )
    assert ctx is not None
    assert ctx.pr_url == canonical_pull_url(REPO, 7)
    assert "evil" not in ctx.to_canonical_json()


def test_forbidden_fields_are_excluded_from_stored_json():
    raw = _context().to_hash()
    for field in FORBIDDEN_FIELDS:
        raw[field] = f"attacker-{field}-title body comment summary"
    raw["title"] = "pwn"
    raw["body"] = "do not copy this"
    raw["summary"] = "provider trace"
    ctx = retry_context_from_hash(REPO, raw)
    assert ctx is not None
    dumped = ctx.to_canonical_json()
    parsed = json.loads(dumped)
    assert set(parsed) == set(JSON_SCHEMA_KEYS)
    for field in FORBIDDEN_FIELDS:
        assert field not in parsed
    assert "pwn" not in dumped
    assert "do not copy this" not in dumped
    assert "provider trace" not in dumped
    assert "attacker-" not in dumped


def test_control_characters_are_escaped_in_json():
    ctx = IssueRetryContext(
        task_id="task\u0001-660",
        generation=1,
        expected_ref=REF,
        expected_head_owner="owner",
        remote_head_oid=OID,
        pr_number=None,
        pr_url="",
        reason_code="no_candidate_pr",
        created_at="1000.000",
        cooldown_until="1020.000",
    )
    dumped = ctx.to_canonical_json()
    assert "\\u0001" in dumped
    assert "\u0001" not in dumped
    block = ctx.render_diagnostic_block()
    assert block.startswith("```json\n")
    assert block.endswith("\n```")
    inner = block.removeprefix("```json\n").removesuffix("\n```")
    json.loads(inner)


def test_serialized_size_cap():
    ctx = IssueRetryContext(
        task_id="task-660",
        generation=1,
        expected_ref=REF,
        expected_head_owner="owner",
        remote_head_oid=OID,
        pr_number=None,
        pr_url="https://github.com/owner/widgets/pull/" + ("9" * (RETRY_CONTEXT_MAX_BYTES + 8)),
        reason_code="no_candidate_pr",
        created_at="1000.000",
        cooldown_until="1020.000",
    )
    with pytest.raises(RetryContextBoundError, match="4096"):
        ctx.to_canonical_json()


def test_unexpected_owner_or_ref_rejected():
    assert same_repo_expected_ref_allowed(REPO, "owner", REF)
    assert not same_repo_expected_ref_allowed(REPO, "attacker", REF)
    assert not same_repo_expected_ref_allowed(REPO, "owner", "../etc/passwd")
    assert not same_repo_expected_ref_allowed(REPO, "owner", "-sneaky")
    assert not same_repo_expected_ref_allowed(REPO, "owner", "foo..bar")
    assert not same_repo_expected_ref_allowed(REPO, "owner", "feature.lock")
    with pytest.raises(RetryContextBoundError, match="same-repository"):
        build_issue_retry_context(
            repo=REPO,
            task_id="task-660",
            generation=1,
            expected_ref=REF,
            expected_head_owner="attacker",
            reason_code="no_candidate_pr",
            created_at=1.0,
            cooldown_until=2.0,
        )


def test_store_and_load_latest(fake_redis_client: RedisClient):
    _seed_generation(fake_redis_client, 1)
    ctx = _context()
    assert store_issue_retry_context(fake_redis_client, REPO, ISSUE, ctx, cooldown_ttl_seconds=20)
    loaded = load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE)
    assert loaded == ctx
    assert fake_redis_client.get(make_issue_retry_latest_key(REPO, ISSUE)) == "1"
    assert fake_redis_client.get(make_issue_delivery_cooldown_key(REPO, ISSUE)) == "1"


def test_stale_generation_cannot_overwrite_newer(fake_redis_client: RedisClient):
    _seed_generation(fake_redis_client, 2)
    newer = _context(generation=2, task_id="task-newer", pr_number=9)
    assert store_issue_retry_context(fake_redis_client, REPO, ISSUE, newer, cooldown_ttl_seconds=30)
    stale = _context(generation=1, task_id="task-stale", pr_number=3, remote_head_oid="")
    assert not store_issue_retry_context(
        fake_redis_client, REPO, ISSUE, stale, cooldown_ttl_seconds=5
    )
    loaded = load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE)
    assert loaded is not None
    assert loaded.task_id == "task-newer"
    assert loaded.generation == 2
    assert fake_redis_client.get(make_issue_delivery_cooldown_key(REPO, ISSUE)) == "2"
    assert fake_redis_client.exists(make_issue_retry_record_key(REPO, ISSUE, 2))
    assert not fake_redis_client.exists(make_issue_retry_record_key(REPO, ISSUE, 1))


def test_stale_generation_cannot_delete_newer(fake_redis_client: RedisClient):
    _seed_generation(fake_redis_client, 2)
    newer = _context(generation=2, task_id="task-newer")
    assert store_issue_retry_context(fake_redis_client, REPO, ISSUE, newer, cooldown_ttl_seconds=30)
    assert not clear_issue_retry_context(fake_redis_client, REPO, ISSUE, 1)
    assert load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE) == newer
    assert fake_redis_client.get(make_issue_delivery_cooldown_key(REPO, ISSUE)) == "2"


def test_clear_matching_generation_is_idempotent(fake_redis_client: RedisClient):
    _seed_generation(fake_redis_client, 1)
    ctx = _context()
    assert store_issue_retry_context(fake_redis_client, REPO, ISSUE, ctx, cooldown_ttl_seconds=20)
    assert clear_issue_retry_context(fake_redis_client, REPO, ISSUE, 1)
    assert load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE) is None
    assert not fake_redis_client.exists(make_issue_retry_record_key(REPO, ISSUE, 1))
    assert not fake_redis_client.exists(make_issue_delivery_cooldown_key(REPO, ISSUE))
    assert clear_issue_retry_context(fake_redis_client, REPO, ISSUE, 1)


def test_prompt_resumes_expected_branch_and_partial_pr():
    ctx = _context()
    prompt = _render_issue_prompt(
        issue_number=ISSUE,
        issue_title="Resume expected refs",
        issue_body="body",
        repo=REPO,
        expected_branch=REF,
        retry_context=ctx,
    )
    assert "```json" in prompt
    inner = prompt.split("```json", 1)[1].split("```", 1)[0].strip()
    parsed = json.loads(inner)
    assert parsed == ctx.to_canonical_dict()
    assert f"git checkout -b {REF}" not in prompt
    assert f"Resume the authoritative same-repository ref `{REF}`" in prompt
    assert ctx.pr_url in prompt
    assert "Do not open another PR" in prompt
    assert "provider-claimed" in prompt


def test_prompt_creates_snapshotted_branch_when_remote_missing():
    ctx = _context(remote_head_oid="", pr_number=None)
    prompt = _render_issue_prompt(
        issue_number=ISSUE,
        issue_title="Resume expected refs",
        issue_body="body",
        repo=REPO,
        expected_branch=REF,
        retry_context=ctx,
    )
    assert f"git checkout -b {REF}" in prompt
    assert "No authoritative expected remote ref exists" in prompt
    assert "provider-claimed branch name" in prompt
    assert "gh pr create" in prompt
    dumped = ctx.render_diagnostic_block()
    assert json.loads(dumped.splitlines()[1])["remote_head_oid"] == ""


def test_render_section_does_not_trust_provider_claims_without_remote():
    section = render_issue_retry_prompt_section(_context(remote_head_oid="", pr_number=None))
    assert "No authoritative expected remote ref exists" in section
    assert "provider-claimed" in section
    assert REF in section


def test_publish_issue_task_embeds_retry_context(fake_redis_client: RedisClient):
    _seed_generation(fake_redis_client, 1)
    ctx = _context()
    assert store_issue_retry_context(fake_redis_client, REPO, ISSUE, ctx, cooldown_ttl_seconds=20)
    issue = IssueState(
        number=ISSUE,
        title="Resume expected refs with bounded retry context",
        body="Do the thing",
        action=IssueAction.ENQUEUE_IMPLEMENT,
        labels=[],
    )
    task = publish_issue_task(
        issue_state=issue,
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )
    assert task is not None
    assert task.expected_branch == REF
    assert task.branch is None
    assert "```json" in task.prompt
    assert ctx.pr_url in task.prompt
    assert f"git checkout -b {REF}" not in task.prompt
    assert "Resume the authoritative same-repository ref" in task.prompt
