"""Dashboard evidence follows scheduling; it must never drive or break it."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

from orcest.shared import work_observations as view


def issue(number=12, action="skip_dependency"):
    return SimpleNamespace(
        number=number,
        title="Ship alerts",
        body="Depends on #11",
        action=SimpleNamespace(value=action),
        open_blockers=["org/repo#11"],
    )


def task():
    return SimpleNamespace(
        repo="org/repo",
        resource_type="issue",
        resource_id=12,
        id="attempt-1",
        provider="codex",
        model="test-model",
        provider_account="safe-account-id",
        credential="SECRET",
        token="GH_SECRET",
        branch="feature",
        snapshot_head_sha="abc",
    )


def test_dependency_and_first_execution_survive_retries_without_inventing_delivery(
    fake_redis_client,
):
    r = fake_redis_client
    view.observe(r, "org/repo", "issue", issue())
    key = view.work_key("org/repo", "issue", 12)
    assert not r.hgetall(key).get("started_at")
    view.queued(r, task())
    view.attempt_started(r, task(), "vm-worker")
    first = r.hgetall(key)["started_at"]
    view.attempt_finished(r, task(), "completed")
    assert not r.hgetall(key).get("outcome")
    view.observe(r, "org/repo", "issue", issue(action="skip_backoff"))
    view.attempt_started(r, task(), "vm-worker")
    assert r.hgetall(key)["started_at"] == first
    records = [
        r.client.hgetall(k) for k in r.client.scan_iter(view.full_key(r, "dashboard:attempt:*"))
    ]
    assert "SECRET" not in json.dumps(records)
    assert records[0]["account_id"] == "safe-account-id"


def test_observation_failure_is_not_a_scheduler_failure(fake_redis_client):
    fake_redis_client.hset_mapping = Mock(side_effect=RuntimeError("redis down SECRET"))
    assert view.observe(fake_redis_client, "org/repo", "issue", issue()) is None


def test_missing_ready_label_does_not_mean_done_and_closed_issue_can_reopen(
    fake_redis_client, monkeypatch
):
    r = fake_redis_client
    view.observe(r, "org/repo", "issue", issue())
    source = Mock(return_value={"state": "OPEN", "title": "Ship alerts"})
    monkeypatch.setattr("orcest.orchestrator.gh.get_issue", source)
    key = view.work_key("org/repo", "issue", 12)
    view.reconcile_missing(r, "org/repo", "secret", set())
    assert r.hgetall(key)["action"] == "not_in_discovery"
    assert not r.hgetall(key)["outcome"]
    source.return_value = {"state": "CLOSED"}
    view.reconcile_missing(r, "org/repo", "secret", set())
    assert r.hgetall(key)["outcome"] == "closed"
    source.return_value = {"state": "OPEN"}
    view.reconcile_missing(r, "org/repo", "secret", set())
    assert not r.hgetall(key)["outcome"]
    source.side_effect = RuntimeError("network failed")
    old = r.hgetall(key)
    view.reconcile_missing(r, "org/repo", "secret", set())
    assert r.hgetall(key) == old


def test_verified_publication_is_a_link_not_completion(fake_redis_client):
    r = fake_redis_client
    view.observe(r, "org/repo", "issue", issue())
    view.link_publication(r, "org/repo", 12, "15")
    state = r.hgetall(view.work_key("org/repo", "issue", 12))
    assert state["related_pr"] == "15"
    assert not state["outcome"]
