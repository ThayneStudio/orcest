"""Generation-scoped idempotent publication for issue tasks."""

from __future__ import annotations

import json

import pytest

from orcest.orchestrator.issue_ops import IssueAction, IssueState, get_attempt_count
from orcest.orchestrator.issue_publication import (
    _IDEMPOTENT_XADD_SCRIPT,
    AmbiguousTaskPublishError,
    IssuePublicationState,
    cas_issue_publication_state,
    expected_branch_name,
    expected_head_owner,
    gc_issue_publication,
    get_issue_generation,
    get_issue_publication,
    get_task_receipt,
    hash_prompt_inputs,
    make_issue_publication_key,
    make_issue_result_ref_key,
    make_issue_retry_record_key,
    make_issue_verification_job_key,
    make_task_receipt_key,
    reserve_issue_publication,
    rollback_prepared_issue_publication,
    xadd_task_idempotent,
)
from orcest.orchestrator.task_publisher import _render_issue_prompt, publish_issue_task
from orcest.shared.coordination import clear_pending_task, get_pending_task
from orcest.shared.models import Task, TaskType, task_stream_name
from orcest.shared.redis_client import RedisClient


def _issue_state(
    number: int = 42,
    title: str = "Fix the widget",
    body: str = "Do the thing",
) -> IssueState:
    return IssueState(
        number=number,
        title=title,
        body=body,
        action=IssueAction.ENQUEUE_IMPLEMENT,
        labels=[],
    )


def _task_fields(task_id: str = "task-1") -> dict[str, str]:
    return {
        "id": task_id,
        "type": TaskType.IMPLEMENT_ISSUE.value,
        "repo": "acme/widget",
        "prompt": "implement it",
    }


def test_expected_branch_name_matches_prompt_instruction():
    title = "Add generation-scoped idempotent publication"
    branch = expected_branch_name(655, title)
    prompt = _render_issue_prompt(
        issue_number=655,
        issue_title=title,
        issue_body="body",
        repo="acme/widget",
        expected_branch=branch,
    )
    assert branch == "issue-655-add-generation-scoped-idempotent-publica"
    assert f"git checkout -b {branch}" in prompt
    assert f"--head {branch}" in prompt
    assert f"git push -u origin {branch}" in prompt


def test_expected_head_owner_parses_owner_repo():
    assert expected_head_owner("ThayneStudio/orcest") == "ThayneStudio"
    with pytest.raises(ValueError, match="owner/repo"):
        expected_head_owner("not-a-repo")


def test_publish_issue_task_round_trip_prompt_and_expected_branch(fake_redis_client):
    issue = _issue_state(number=12, title="Ship the feature")
    task = publish_issue_task(
        issue_state=issue,
        repo="acme/widget",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )
    assert task is not None
    assert task.branch is None
    assert task.expected_branch == expected_branch_name(12, "Ship the feature")
    assert task.expected_branch in task.prompt
    assert task.issue_generation == 1
    assert f"git checkout -b {task.expected_branch}" in task.prompt

    rebuilt = Task.from_dict(task.to_dict())
    assert rebuilt.branch is None
    assert rebuilt.expected_branch == task.expected_branch
    assert rebuilt.issue_generation == 1
    assert rebuilt.prompt == task.prompt

    record = get_issue_publication(fake_redis_client, "acme/widget", 12, 1)
    assert record is not None
    assert record.state is IssuePublicationState.PUBLISHED
    assert record.expected_branch == task.expected_branch
    assert record.expected_head_owner == "acme"
    assert record.prompt_input_hash == hash_prompt_inputs(
        repo="acme/widget",
        issue_number=12,
        issue_title="Ship the feature",
        issue_body="Do the thing",
        expected_branch=task.expected_branch or "",
    )
    assert record.task_id == task.id
    assert record.stream_id
    assert get_task_receipt(fake_redis_client, task.id) == record.stream_id


def test_duplicate_publish_returns_exactly_one_stream_entry(fake_redis_client):
    stream = task_stream_name("claude", issue=True)
    fields = _task_fields("dup-task")
    first = xadd_task_idempotent(fake_redis_client, stream, fields, "dup-task")
    second = xadd_task_idempotent(fake_redis_client, stream, fields, "dup-task")
    assert first == second
    entries = fake_redis_client.xrange(stream)
    assert len(entries) == 1
    assert entries[0][0] == first
    assert entries[0][1]["id"] == "dup-task"


def test_lost_response_after_successful_append_reconciles_to_original_entry(
    fake_redis_client,
):
    stream = task_stream_name("claude", issue=True)
    fields = _task_fields("lost-task")
    real_eval = fake_redis_client.client.eval
    lost = {"once": False}

    def eval_lost_reply(script, numkeys, *args):
        result = real_eval(script, numkeys, *args)
        if script == _IDEMPOTENT_XADD_SCRIPT and not lost["once"]:
            lost["once"] = True
            raise ConnectionError("response lost after Redis executed EVAL")
        return result

    fake_redis_client.client.eval = eval_lost_reply  # type: ignore[method-assign]
    try:
        entry_id = xadd_task_idempotent(fake_redis_client, stream, fields, "lost-task")
    finally:
        fake_redis_client.client.eval = real_eval  # type: ignore[method-assign]

    entries = fake_redis_client.xrange(stream)
    assert len(entries) == 1
    assert entries[0][0] == entry_id
    assert get_task_receipt(fake_redis_client, "lost-task") == entry_id

    retry_id = xadd_task_idempotent(fake_redis_client, stream, fields, "lost-task")
    assert retry_id == entry_id
    assert len(fake_redis_client.xrange(stream)) == 1


def test_lost_response_during_issue_publish_marks_published(fake_redis_client):
    real_eval = fake_redis_client.client.eval

    def eval_lost_reply(script, numkeys, *args):
        result = real_eval(script, numkeys, *args)
        if script == _IDEMPOTENT_XADD_SCRIPT:
            raise ConnectionError("response lost after Redis executed EVAL")
        return result

    fake_redis_client.client.eval = eval_lost_reply  # type: ignore[method-assign]
    try:
        task = publish_issue_task(
            issue_state=_issue_state(number=88),
            repo="acme/widget",
            token="fake-token",
            redis=fake_redis_client,
            default_runner="claude",
        )
    finally:
        fake_redis_client.client.eval = real_eval  # type: ignore[method-assign]

    assert task is not None
    record = get_issue_publication(fake_redis_client, "acme/widget", 88, 1)
    assert record is not None
    assert record.state is IssuePublicationState.PUBLISHED
    stream = task_stream_name("claude", issue=True)
    entries = fake_redis_client.xrange(stream)
    assert len(entries) == 1
    assert entries[0][0] == record.stream_id
    assert get_pending_task(fake_redis_client, "acme/widget", "issue", 88) == task.id


def test_definite_pre_publish_failure_cleans_up(fake_redis_client):
    real_eval = fake_redis_client.client.eval

    def eval_never_runs_idempotent(script, numkeys, *args):
        if script == _IDEMPOTENT_XADD_SCRIPT:
            raise ConnectionError("Redis down before XADD")
        return real_eval(script, numkeys, *args)

    fake_redis_client.client.eval = eval_never_runs_idempotent  # type: ignore[method-assign]
    try:
        with pytest.raises(ConnectionError, match="before XADD"):
            publish_issue_task(
                issue_state=_issue_state(number=21),
                repo="acme/widget",
                token="fake-token",
                redis=fake_redis_client,
                default_runner="claude",
            )
    finally:
        fake_redis_client.client.eval = real_eval  # type: ignore[method-assign]

    assert get_issue_publication(fake_redis_client, "acme/widget", 21, 1) is None
    assert get_pending_task(fake_redis_client, "acme/widget", "issue", 21) is None
    assert get_attempt_count(fake_redis_client, "acme/widget", 21) == 0
    assert get_issue_generation(fake_redis_client, "acme/widget", 21) == 1
    assert fake_redis_client.xrange(task_stream_name("claude", issue=True)) == []


def test_ambiguous_outcome_preserves_prepared_state(fake_redis_client):
    real_eval = fake_redis_client.client.eval
    real_get = fake_redis_client.get

    def eval_never_runs_idempotent(script, numkeys, *args):
        if script == _IDEMPOTENT_XADD_SCRIPT:
            raise ConnectionError("Redis down during XADD")
        return real_eval(script, numkeys, *args)

    def get_receipt_also_fails(key: str) -> str | None:
        if key.startswith("task-receipt:"):
            raise ConnectionError("cannot read receipt")
        return real_get(key)

    fake_redis_client.client.eval = eval_never_runs_idempotent  # type: ignore[method-assign]
    fake_redis_client.get = get_receipt_also_fails  # type: ignore[method-assign]
    try:
        with pytest.raises(AmbiguousTaskPublishError):
            publish_issue_task(
                issue_state=_issue_state(number=22),
                repo="acme/widget",
                token="fake-token",
                redis=fake_redis_client,
                default_runner="claude",
            )
    finally:
        fake_redis_client.client.eval = real_eval  # type: ignore[method-assign]
        fake_redis_client.get = real_get  # type: ignore[method-assign]

    record = get_issue_publication(fake_redis_client, "acme/widget", 22, 1)
    assert record is not None
    assert record.state is IssuePublicationState.AMBIGUOUS
    assert get_pending_task(fake_redis_client, "acme/widget", "issue", 22) == record.task_id
    assert get_attempt_count(fake_redis_client, "acme/widget", 22) == 1
    assert fake_redis_client.xrange(task_stream_name("claude", issue=True)) == []


def test_generation_monotonicity_and_stale_cas_refusal(fake_redis_client):
    repo = "acme/widget"
    first = reserve_issue_publication(
        fake_redis_client,
        repo=repo,
        issue_number=9,
        task_id="t-1",
        prompt_input_hash="hash-1",
        expected_head_owner="acme",
        expected_branch="issue-9-one",
        pending_ttl=600,
        created_at="2026-01-01T00:00:00+00:00",
    )
    assert first is not None
    assert first.generation == 1
    assert first.attempt == 1
    assert get_issue_generation(fake_redis_client, repo, 9) == 1

    clear_pending_task(fake_redis_client, repo, "issue", 9)
    second = reserve_issue_publication(
        fake_redis_client,
        repo=repo,
        issue_number=9,
        task_id="t-2",
        prompt_input_hash="hash-2",
        expected_head_owner="acme",
        expected_branch="issue-9-two",
        pending_ttl=600,
        created_at="2026-01-01T00:00:01+00:00",
    )
    assert second is not None
    assert second.generation == 2
    assert second.attempt == 2
    assert get_issue_generation(fake_redis_client, repo, 9) == 2

    assert (
        cas_issue_publication_state(
            fake_redis_client,
            repo,
            9,
            1,
            IssuePublicationState.PREPARED,
            IssuePublicationState.PUBLISHED,
        )
        is False
    )
    leftover = get_issue_publication(fake_redis_client, repo, 9, 1)
    assert leftover is not None
    assert leftover.state is IssuePublicationState.PREPARED
    current = get_issue_publication(fake_redis_client, repo, 9, 2)
    assert current is not None
    assert current.state is IssuePublicationState.PREPARED

    assert cas_issue_publication_state(
        fake_redis_client,
        repo,
        9,
        2,
        IssuePublicationState.PREPARED,
        IssuePublicationState.PUBLISHED,
        extra_fields={"stream": "tasks:issue:claude", "stream_id": "1-0"},
    )
    published = get_issue_publication(fake_redis_client, repo, 9, 2)
    assert published is not None
    assert published.state is IssuePublicationState.PUBLISHED
    assert published.stream_id == "1-0"


def test_rollback_prepared_refuses_stale_generation(fake_redis_client):
    repo = "acme/widget"
    first = reserve_issue_publication(
        fake_redis_client,
        repo=repo,
        issue_number=3,
        task_id="old",
        prompt_input_hash="h",
        expected_head_owner="acme",
        expected_branch="issue-3-x",
        pending_ttl=600,
        created_at="2026-01-01T00:00:00+00:00",
    )
    assert first is not None
    clear_pending_task(fake_redis_client, repo, "issue", 3)
    second = reserve_issue_publication(
        fake_redis_client,
        repo=repo,
        issue_number=3,
        task_id="new",
        prompt_input_hash="h2",
        expected_head_owner="acme",
        expected_branch="issue-3-y",
        pending_ttl=600,
        created_at="2026-01-01T00:00:01+00:00",
    )
    assert second is not None
    assert rollback_prepared_issue_publication(fake_redis_client, repo, 3, 1, "old") is False
    assert get_issue_publication(fake_redis_client, repo, 3, 1) is not None
    assert get_issue_publication(fake_redis_client, repo, 3, 2) is not None


def test_reference_aware_cleanup_never_deletes_active_publication_state(
    fake_redis_server,
):
    import fakeredis

    project_fake = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    project_redis = RedisClient.from_client(project_fake, key_prefix="project")
    stream_fake = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    stream_redis = RedisClient.from_client(stream_fake, key_prefix="shared")

    repo = "acme/widget"
    reservation = reserve_issue_publication(
        project_redis,
        repo=repo,
        issue_number=5,
        task_id="live-task",
        prompt_input_hash="hash",
        expected_head_owner="acme",
        expected_branch="issue-5-live",
        pending_ttl=600,
        created_at="2026-01-01T00:00:00+00:00",
    )
    assert reservation is not None
    stream_id = xadd_task_idempotent(
        stream_redis,
        task_stream_name("claude", issue=True),
        _task_fields("live-task"),
        "live-task",
    )
    assert stream_id

    pub_key = make_issue_publication_key(repo, 5, 1)
    assert project_redis.expire(pub_key, 1)
    assert project_redis.ttl(pub_key) > 0

    assert gc_issue_publication(project_redis, stream_redis, repo, 5, 1) is False, (
        "pending marker and stream receipt must keep the record"
    )
    assert get_issue_publication(project_redis, repo, 5, 1) is not None
    assert project_redis.ttl(pub_key) == -1

    clear_pending_task(project_redis, repo, "issue", 5)
    assert gc_issue_publication(project_redis, stream_redis, repo, 5, 1) is False
    assert stream_redis.exists(make_task_receipt_key("live-task"))

    stream_redis.delete(make_task_receipt_key("live-task"))
    project_redis.set_value(make_issue_result_ref_key(repo, 5, 1), "1")
    assert gc_issue_publication(project_redis, stream_redis, repo, 5, 1) is False

    project_redis.delete(make_issue_result_ref_key(repo, 5, 1))
    project_redis.set_value(make_issue_verification_job_key(repo, 5, 1), "1")
    assert gc_issue_publication(project_redis, stream_redis, repo, 5, 1) is False

    project_redis.delete(make_issue_verification_job_key(repo, 5, 1))
    project_redis.set_value(make_issue_retry_record_key(repo, 5, 1), "1")
    assert gc_issue_publication(project_redis, stream_redis, repo, 5, 1) is False

    project_redis.delete(make_issue_retry_record_key(repo, 5, 1))
    assert gc_issue_publication(project_redis, stream_redis, repo, 5, 1) is True
    assert get_issue_publication(project_redis, repo, 5, 1) is None
    assert get_issue_generation(project_redis, repo, 5) == 1


def test_reserve_skips_when_pending_exists(fake_redis_client):
    repo = "acme/widget"
    first = reserve_issue_publication(
        fake_redis_client,
        repo=repo,
        issue_number=4,
        task_id="first",
        prompt_input_hash="h",
        expected_head_owner="acme",
        expected_branch="issue-4-x",
        pending_ttl=600,
        created_at="2026-01-01T00:00:00+00:00",
    )
    assert first is not None
    skipped = reserve_issue_publication(
        fake_redis_client,
        repo=repo,
        issue_number=4,
        task_id="second",
        prompt_input_hash="h2",
        expected_head_owner="acme",
        expected_branch="issue-4-y",
        pending_ttl=600,
        created_at="2026-01-01T00:00:01+00:00",
    )
    assert skipped is None
    assert get_issue_generation(fake_redis_client, repo, 4) == 1
    pending = fake_redis_client.get("pending:issue:acme/widget:4")
    assert pending is not None
    assert json.loads(pending)["task_id"] == "first"
