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
    assert r.hgetall(key)["action"] == "skip_dependency"
    assert r.hgetall(key)["discovery_missing"] == "1"
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
    after = r.hgetall(key)
    assert after["observed_at"] == old["observed_at"]
    assert {k: v for k, v in after.items() if k != "last_reconcile_attempt_at"} == {
        k: v for k, v in old.items() if k != "last_reconcile_attempt_at"
    }


def test_worker_blocker_survives_poll_until_acknowledged_or_restarted(fake_redis_client):
    r = fake_redis_client
    key = view.work_key("org/repo", "issue", 12)
    view.observe(r, "org/repo", "issue", issue())
    view.human_reason(r, task(), "Access denied SECRET ROTATED", credential_update="ROTATED")
    view.observe(r, "org/repo", "issue", issue())
    assert r.hgetall(key)["worker_needs_human"] == "1"
    assert r.hgetall(key)["human_reason"] == "Access denied [REDACTED] [REDACTED]"
    view.observe(r, "org/repo", "issue", issue(action="skip_labeled"))
    assert r.hgetall(key)["needs_human"] == "1"
    assert r.hgetall(key)["worker_needs_human"] == "0"
    view.observe(r, "org/repo", "issue", issue())
    assert r.hgetall(key)["needs_human"] == "0"
    view.human_reason(r, task(), "Access denied")
    view.attempt_started(r, task(), "new-worker")
    assert r.hgetall(key)["worker_needs_human"] == "0"
    assert r.hgetall(key)["needs_human"] == "0"


def test_discovery_gap_preserves_queue_evidence_and_clears_on_rediscovery(
    fake_redis_client, monkeypatch
):
    r = fake_redis_client
    key = view.work_key("org/repo", "issue", 12)
    view.observe(r, "org/repo", "issue", issue())
    view.queued(r, task())
    monkeypatch.setattr("orcest.orchestrator.gh.get_issue", Mock(return_value={"state": "OPEN"}))
    view.reconcile_missing(r, "org/repo", "secret", set())
    assert r.hgetall(key)["action"] == "skip_queued"
    assert r.hgetall(key)["discovery_missing"] == "1"
    view.observe(r, "org/repo", "issue", issue(action="skip_queued"))
    assert r.hgetall(key)["discovery_missing"] == "0"


def test_verified_publication_is_a_link_not_completion(fake_redis_client):
    r = fake_redis_client
    view.observe(r, "org/repo", "issue", issue())
    view.link_publication(r, "org/repo", 12, "15")
    state = r.hgetall(view.work_key("org/repo", "issue", 12))
    assert state["related_pr"] == "15"
    assert not state["outcome"]


def test_retained_refresh_budget_excludes_merged_and_discovered_records(
    fake_redis_client, monkeypatch
):
    """The live 82-record fleet has only 11 retained resources needing reads."""
    r = fake_redis_client
    clock = [1000.0]
    monkeypatch.setattr(view.time, "time", lambda: clock[0])
    seen = set()
    for number in range(1, 83):
        view.observe(r, "org/repo", "issue", issue(number))
        key = view.work_key("org/repo", "issue", number)
        if number <= 29:
            r.hset_mapping(key, {"outcome": "merged", "completed_at": "1000"})
        elif number <= 71:
            seen.add(key)
    merged_before = r.hgetall(view.work_key("org/repo", "issue", 1))
    source = Mock(return_value={"state": "OPEN"})
    monkeypatch.setattr("orcest.orchestrator.gh.get_issue", source)
    for _ in range(3):
        clock[0] += 80
        before = source.call_count
        view.reconcile_missing(r, "org/repo", "secret", seen)
        assert source.call_count - before == 5
    reads = {call.args[1] for call in source.call_args_list}
    assert reads == set(range(72, 83))
    assert r.hgetall(view.work_key("org/repo", "issue", 1)) == merged_before
    assert (
        max(
            clock[0] - float(r.hgetall(view.work_key("org/repo", "issue", n))["observed_at"])
            for n in range(72, 83)
        )
        <= 160
    )


def test_changing_discovery_membership_cannot_starve_old_retained_work(
    fake_redis_client, monkeypatch
):
    r = fake_redis_client
    clock = [1000.0]
    monkeypatch.setattr(view.time, "time", lambda: clock[0])
    for number in range(1, 17):
        view.observe(r, "org/repo", "issue", issue(number))
    # Legacy numeric cursor should have no influence on stable record fairness.
    r.hset("dashboard:project", "reconcile_cursor", "142535")
    source = Mock(return_value={"state": "OPEN"})
    monkeypatch.setattr("orcest.orchestrator.gh.get_issue", source)
    for recently_seen in [1, 2, 3, 4]:
        clock[0] += 80
        key = view.work_key("org/repo", "issue", recently_seen)
        view.observe(r, "org/repo", "issue", issue(recently_seen))
        view.reconcile_missing(r, "org/repo", "secret", {key})
    assert set(range(5, 17)) <= {call.args[1] for call in source.call_args_list}


def test_failed_or_unknown_reads_stay_stale_but_do_not_monopolize_budget(
    fake_redis_client, monkeypatch
):
    r = fake_redis_client
    clock = [1000.0]
    monkeypatch.setattr(view.time, "time", lambda: clock[0])
    for number in range(1, 12):
        view.observe(r, "org/repo", "issue", issue(number))

    def read(_repo, number, _token):
        if number <= 5:
            raise RuntimeError("GitHub unavailable SECRET")
        if number == 6:
            return {"state": "UNKNOWN"}
        return {"state": "OPEN"}

    source = Mock(side_effect=read)
    monkeypatch.setattr("orcest.orchestrator.gh.get_issue", source)
    for _ in range(3):
        clock[0] += 80
        view.reconcile_missing(r, "org/repo", "secret", set())
    assert {call.args[1] for call in source.call_args_list} == set(range(1, 12))
    for n in range(1, 7):
        fields = r.hgetall(view.work_key("org/repo", "issue", n))
        assert fields["observed_at"] == "1000.0"
        assert float(fields["last_reconcile_attempt_at"]) > 1000
        assert "SECRET" not in json.dumps(fields)


def test_expired_missing_and_invalid_records_consume_no_github_read_slots(
    fake_redis_client, monkeypatch
):
    r = fake_redis_client
    clock = [1000.0]
    monkeypatch.setattr(view.time, "time", lambda: clock[0])
    for n in range(1, 16):
        view.observe(r, "org/repo", "issue", issue(n))
        key = view.work_key("org/repo", "issue", n)
        if n <= 5:
            r.hset_mapping(key, {"outcome": "closed", "completed_at": "1000"})
        elif n <= 10:
            r.hset(key, "number", "invalid")
    clock[0] += 31 * 86400
    absent = view.work_key("org/repo", "issue", 16)
    r.client.zadd(view.full_key(r, "dashboard:tracked"), {absent: 1000})
    source = Mock(return_value={"state": "OPEN"})
    monkeypatch.setattr("orcest.orchestrator.gh.get_issue", source)
    view.reconcile_missing(r, "org/repo", "secret", set())
    assert source.call_count == 5
    assert {call.args[1] for call in source.call_args_list} == set(range(11, 16))
    assert r.client.zscore(view.full_key(r, "dashboard:tracked"), absent) is None
    for n in range(1, 6):
        key = view.work_key("org/repo", "issue", n)
        assert r.client.zscore(view.full_key(r, "dashboard:tracked"), key) is None
        assert r.client.ttl(view.full_key(r, key)) > 0


def test_invalid_completion_timestamp_does_not_expire_or_block_other_records(
    fake_redis_client, monkeypatch
):
    r = fake_redis_client
    monkeypatch.setattr(view.time, "time", lambda: 4_000_000.0)
    view.observe(r, "org/repo", "issue", issue(1))
    key = view.work_key("org/repo", "issue", 1)
    r.hset_mapping(key, {"outcome": "merged", "completed_at": "invalid", "observed_at": "NaN"})
    view.observe(r, "org/repo", "issue", issue(2))
    source = Mock(return_value={"state": "OPEN"})
    monkeypatch.setattr("orcest.orchestrator.gh.get_issue", source)
    view.reconcile_missing(r, "org/repo", "secret", set())
    source.assert_called_once_with("org/repo", 2, "secret")
    assert r.client.zscore(view.full_key(r, "dashboard:tracked"), key) is not None
    assert r.hgetall(key)["observed_at"] == "NaN"
