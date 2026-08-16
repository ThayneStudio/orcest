import json

import pytest

from orcest.canary_evidence import CanaryEvidenceError, collect_canary_evidence

pytestmark = pytest.mark.unit


def test_collect_canary_evidence_projects_only_non_secret_fields(fake_redis_client):
    task_id = "task-codex-1"
    fake_redis_client.client.xadd(
        "orcest:tasks:codex",
        {
            "id": task_id,
            "provider": "codex",
            "credential": "oauth-secret",
            "token": "github-secret",
        },
    )
    fake_redis_client.client.xadd(
        "test:results",
        {
            "task_id": task_id,
            "worker_id": "orcest-worker-10001",
            "status": "completed",
            "credential_update": "rotated-secret",
            "summary": "potentially sensitive output",
        },
    )

    evidence = collect_canary_evidence(
        fake_redis_client,
        task_prefix="orcest",
        canaries={"codex": task_id},
    )

    assert evidence["ok"] is True
    assert evidence["canaries"][0]["provider"] == "codex"
    rendered = json.dumps(evidence)
    assert "oauth-secret" not in rendered
    assert "github-secret" not in rendered
    assert "rotated-secret" not in rendered
    assert "potentially sensitive output" not in rendered


def test_collect_canary_evidence_requires_exactly_one_source_and_result(fake_redis_client):
    with pytest.raises(CanaryEvidenceError, match="expected one source entry, found 0"):
        collect_canary_evidence(
            fake_redis_client,
            task_prefix="orcest",
            canaries={"grok": "task-grok-1"},
        )


def test_collect_canary_evidence_accepts_issue_stream_canary(fake_redis_client):
    task_id = "task-grok-issue-1"
    fake_redis_client.client.xadd(
        "orcest:tasks:issue:grok",
        {"id": task_id, "provider": "grok", "credential": "secret"},
    )
    fake_redis_client.client.xadd(
        "test:results",
        {"task_id": task_id, "worker_id": "orcest-worker-10002", "status": "completed"},
    )

    evidence = collect_canary_evidence(
        fake_redis_client,
        task_prefix="orcest",
        canaries={"grok": task_id},
    )

    assert evidence["canaries"][0]["source_stream"] == "orcest:tasks:issue:grok"


def test_collect_canary_evidence_rejects_duplicate_across_pr_and_issue_streams(
    fake_redis_client,
):
    task_id = "task-codex-duplicate"
    for stream in ("orcest:tasks:codex", "orcest:tasks:issue:codex"):
        fake_redis_client.client.xadd(stream, {"id": task_id, "provider": "codex"})
    fake_redis_client.client.xadd(
        "test:results",
        {"task_id": task_id, "worker_id": "orcest-worker-10001", "status": "completed"},
    )

    with pytest.raises(CanaryEvidenceError, match="expected one source entry, found 2"):
        collect_canary_evidence(
            fake_redis_client,
            task_prefix="orcest",
            canaries={"codex": task_id},
        )


def test_collect_canary_evidence_requires_unique_task_ids(fake_redis_client):
    with pytest.raises(CanaryEvidenceError, match="unique task ID"):
        collect_canary_evidence(
            fake_redis_client,
            task_prefix="orcest",
            canaries={"codex": "same-task", "grok": "same-task"},
        )
