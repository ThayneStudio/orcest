"""Tests for task.enqueued event emission from the orchestrator's task publishers."""

import json

from orcest.orchestrator.pr_ops import PRAction, PRState
from orcest.orchestrator.task_publisher import publish_fix_task
from orcest.shared.events import EVENTS_STREAM


def _make_pr_state(
    number: int = 42,
    title: str = "Fix the widget",
    branch: str = "fix/widget",
    ci_failures: list[dict] | None = None,
    review_threads: list[dict] | None = None,
) -> PRState:
    """Build a PRState suitable for publish_fix_task."""
    return PRState(
        number=number,
        title=title,
        branch=branch,
        head_sha="abc123",
        action=PRAction.ENQUEUE_FIX,
        ci_failures=ci_failures or [],
        review_threads=review_threads or [],
        labels=[],
    )


def _setup_gh_defaults(gh_mock):
    """Set sensible default return values for gh mock functions."""
    gh_mock.get_pr_diff.return_value = "diff --git a/foo.py b/foo.py\n+pass"
    gh_mock.get_unresolved_review_threads.return_value = []
    gh_mock.post_comment.return_value = None


def test_enqueued_event_emitted_after_publish(gh_mock, fake_redis_client):
    """publish_fix_task emits net.orcest.task.enqueued to the events spool."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=42)

    # Distinct literal secret values so the assertions below can prove they
    # are genuinely absent from the envelope, not just that a field named
    # "token" is missing (a stricter check than the substring "token").
    secret_gh_token = "ghp_SuperSecretGithubToken000111"  # noqa: S105 (test fixture, not real)
    secret_claude_token = "claude-oauth-SecretValue222333"  # noqa: S105
    secret_credential = "cred-SecretValue444555"  # noqa: S105
    secret_diff_marker = "SECRET_DIFF_LINE_marker_666777"
    gh_mock.get_pr_diff.return_value = f"diff --git a/foo.py b/foo.py\n+{secret_diff_marker}"

    publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token=secret_gh_token,
        redis=fake_redis_client,
        default_runner="claude",
        claude_token=secret_claude_token,
        credential=secret_credential,
    )

    entries = fake_redis_client.xrevrange(EVENTS_STREAM, count=10)
    envs = [json.loads(fields["envelope"]) for _id, fields in entries]
    enq = [e for e in envs if e["type"] == "net.orcest.task.enqueued"]
    assert len(enq) == 1
    assert enq[0]["data"]["work"]["resource_id"] == pr_state.number
    assert enq[0]["data"]["attempt"] == 0
    assert enq[0]["data"]["decision_reason"] != ""

    # secrets must not leak: the task's literal GitHub token, Claude OAuth
    # token, provider credential, or any rendered-prompt content (diff text)
    # must never appear in the serialized envelope.
    serialized = json.dumps(enq[0])
    assert secret_gh_token not in serialized
    assert secret_claude_token not in serialized
    assert secret_credential not in serialized
    assert secret_diff_marker not in serialized
