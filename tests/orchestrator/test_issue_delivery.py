"""Durable issue-result admission and delivery verification."""

from __future__ import annotations

import json
import logging

from orcest.orchestrator.github_delivery_verifier import (
    CandidatePullRequest,
    ClosingIssueReference,
    DeliveryErrorKind,
    DeliveryFailureReason,
    HandoffObservation,
)
from orcest.orchestrator.issue_delivery import (
    ACTIVE_JOBS_KEY,
    DUE_INDEX_KEY,
    GC_DUE_INDEX_KEY,
    METRICS_KEY,
    QUARANTINE_KEY,
    ROUTE_COMPLETED_VERIFY,
    ROUTE_NONSUCCESS,
    UNVERIFIABLE_INDEX_KEY,
    AdmissionKind,
    SagaPhase,
    VerificationState,
    admit_completed_verification_job,
    admit_issue_result,
    apply_admission_conflict,
    count_ineffective_delivery_generations,
    delivery_state_blocks_old_orchestrator_rollback,
    gc_issue_delivery_state,
    get_admission,
    get_delivery_metrics,
    get_verification_job,
    has_issue_dispatch_barrier,
    job_member,
    list_quarantined_conflicts,
    process_due_delivery_state_gc,
    process_due_verification_jobs,
    quarantine_job_admission_mismatch,
    reconcile_verification_due_index,
    result_fingerprint,
)
from orcest.orchestrator.issue_ops import (
    IssueAction,
    clear_attempts,
    discover_actionable_issues,
    get_attempt_count,
    increment_attempts,
)
from orcest.orchestrator.issue_publication import (
    make_issue_admission_key,
    make_issue_delivery_cooldown_key,
    make_issue_dispatch_barrier_key,
    make_issue_generation_key,
    make_issue_result_ref_key,
    make_issue_retry_latest_key,
    make_issue_retry_record_key,
    make_issue_verification_job_key,
    reserve_issue_publication,
)
from orcest.orchestrator.issue_retry import (
    clear_issue_retry_context,
    load_issue_retry_context,
    load_latest_issue_retry_context,
)
from orcest.shared.config import IssueDeliveryVerifierConfig, LabelConfig
from orcest.shared.coordination import make_pending_task_key
from orcest.shared.events import EVENTS_STREAM
from orcest.shared.models import ResultStatus, TaskResult
from orcest.shared.redis_client import RedisClient

REPO = "owner/testrepo"
ISSUE = 659
TOKEN = "fake-token"
OID = "a" * 40
BRANCH = "issue-659-gate-completion"


def _result(
    status: ResultStatus = ResultStatus.COMPLETED,
    task_id: str = "task-659",
    branch: str = BRANCH,
    snapshot_head_sha: str = OID,
    summary: str = "implemented",
) -> TaskResult:
    return TaskResult(
        task_id=task_id,
        worker_id="worker-1",
        status=status,
        branch=branch,
        summary=summary,
        duration_seconds=12,
        resource_type="issue",
        resource_id=ISSUE,
        snapshot_head_sha=snapshot_head_sha,
        repo=REPO,
    )


def _publish(redis: RedisClient, task_id: str = "task-659", branch: str = BRANCH) -> int:
    reservation = reserve_issue_publication(
        redis,
        repo=REPO,
        issue_number=ISSUE,
        task_id=task_id,
        prompt_input_hash="hash",
        expected_head_owner="owner",
        expected_branch=branch,
        pending_ttl=600,
        created_at="2026-01-01T00:00:00+00:00",
    )
    assert reservation is not None
    return reservation.generation


def _observation(
    *,
    verified: bool = True,
    kind: DeliveryErrorKind = DeliveryErrorKind.NONE,
    reason: DeliveryFailureReason = DeliveryFailureReason.VERIFIED,
    complete: bool = True,
    echo_mismatch: bool = False,
    ambiguous: bool = False,
    selected_number: int | None = 10,
    message: str = "ok",
    live_head_oid: str = OID,
    candidate_prs: tuple[CandidatePullRequest, ...] | None = None,
    head_repository: str = REPO,
) -> HandoffObservation:
    selected = None
    qualifying = ()
    if selected_number is not None:
        selected = CandidatePullRequest(
            number=selected_number,
            url=f"https://github.com/{REPO}/pull/{selected_number}",
            state="OPEN",
            is_draft=False,
            base_repository=REPO,
            base_ref_name="master",
            head_repository=head_repository,
            head_ref_name=BRANCH,
            head_oid=OID,
            closing_issues_references=(
                ClosingIssueReference(repository=REPO, number=ISSUE, url="https://x"),
            ),
        )
        qualifying = (selected,)
    prs = qualifying if candidate_prs is None else candidate_prs
    return HandoffObservation(
        verified=verified,
        error_kind=kind,
        reason=reason,
        repo=REPO,
        issue_number=ISSUE,
        default_branch="master",
        default_branch_oid="b" * 40,
        expected_head_ref=BRANCH,
        claimed_head_oid=OID,
        live_head_oid=live_head_oid,
        complete=complete,
        echo_mismatch=echo_mismatch,
        ambiguous=ambiguous,
        selected_pr=selected,
        qualifying_prs=qualifying,
        candidate_prs=prs,
        message=message,
    )


def _config(**overrides: object) -> IssueDeliveryVerifierConfig:
    values = {
        "grace_seconds": 30,
        "backoff_initial_seconds": 10,
        "backoff_max_seconds": 300,
        "scheduler_batch_size": 20,
        "oldest_pending_alert_seconds": 1800,
        "page_cap": 50,
        "ineffective_cooldown_seconds": 15,
    }
    values.update(overrides)
    return IssueDeliveryVerifierConfig(**values)  # type: ignore[arg-type]


def _admit_completed(redis: RedisClient, result: TaskResult | None = None) -> TaskResult:
    result = result or _result()
    gen = _publish(redis, result.task_id, result.branch or BRANCH)
    decision = admit_issue_result(redis, REPO, result, now=lambda: 1_000.0)
    assert decision.kind is AdmissionKind.ADMITTED
    assert decision.generation == gen
    created = admit_completed_verification_job(
        redis, REPO, result, decision, _config(), now=lambda: 1_000.0
    )
    assert created is True
    return result


def _events(redis: RedisClient) -> list[dict]:
    entries = redis.xrange(EVENTS_STREAM)
    out: list[dict] = []
    for _eid, fields in entries:
        raw = fields.get("envelope")
        if not raw:
            continue
        out.append(json.loads(raw))
    return out


def test_first_payload_cas_and_identical_replay(fake_redis_client):
    result = _result()
    _publish(fake_redis_client, result.task_id)
    first = admit_issue_result(fake_redis_client, REPO, result, now=lambda: 10.0)
    replay = admit_issue_result(fake_redis_client, REPO, result, now=lambda: 11.0)
    assert first.kind is AdmissionKind.ADMITTED
    assert replay.kind is AdmissionKind.REPLAY
    assert replay.route == ROUTE_COMPLETED_VERIFY
    admission = get_admission(fake_redis_client, result.task_id)
    assert admission is not None
    assert admission.fingerprint == result_fingerprint(result)
    envelopes = [e for e in _events(fake_redis_client) if e["type"].endswith("result.admitted")]
    assert len(envelopes) == 1


def test_conflicting_status_cannot_both_mutate(fake_redis_client):
    failed = _result(status=ResultStatus.FAILED, summary="boom")
    completed = _result(status=ResultStatus.COMPLETED, summary="done")
    _publish(fake_redis_client, failed.task_id)
    first = admit_issue_result(fake_redis_client, REPO, failed, now=lambda: 10.0)
    assert first.route == ROUTE_NONSUCCESS
    second = admit_issue_result(fake_redis_client, REPO, completed, now=lambda: 11.0)
    assert second.kind is AdmissionKind.CONFLICT
    apply_admission_conflict(fake_redis_client, REPO, completed, second, now=lambda: 11.0)
    assert get_admission(fake_redis_client, failed.task_id).status == "failed"
    assert list_quarantined_conflicts(fake_redis_client)


def test_completed_then_failed_freezes_pending_job(fake_redis_client):
    _admit_completed(fake_redis_client)
    failed = _result(status=ResultStatus.FAILED, summary="nope")
    conflict = admit_issue_result(fake_redis_client, REPO, failed, now=lambda: 20.0)
    assert conflict.kind is AdmissionKind.CONFLICT
    apply_admission_conflict(fake_redis_client, REPO, failed, conflict, now=lambda: 20.0)
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job is not None
    assert job.state is VerificationState.UNVERIFIABLE
    assert has_issue_dispatch_barrier(fake_redis_client, REPO, ISSUE)
    assert fake_redis_client.sismember(UNVERIFIABLE_INDEX_KEY, job_member(REPO, ISSUE, 1))


def test_post_terminal_conflict_is_quarantined(fake_redis_client, mocker):
    result = _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job is not None
    assert job.state is VerificationState.VERIFIED
    failed = _result(status=ResultStatus.FAILED, summary="late")
    conflict = admit_issue_result(fake_redis_client, REPO, failed, now=lambda: 3_000.0)
    apply_admission_conflict(fake_redis_client, REPO, failed, conflict, now=lambda: 3_000.0)
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.VERIFIED
    items = list_quarantined_conflicts(fake_redis_client)
    assert items[0]["existing_status"] == "completed"
    assert result.task_id == items[0]["task_id"]


def test_atomic_job_and_due_index_and_replay(fake_redis_client):
    result = _admit_completed(fake_redis_client)
    member = job_member(REPO, ISSUE, 1)
    assert fake_redis_client.zscore(DUE_INDEX_KEY, member) == 1_000.0
    assert fake_redis_client.exists(make_issue_dispatch_barrier_key(REPO, ISSUE))
    decision = admit_issue_result(fake_redis_client, REPO, result, now=lambda: 1_001.0)
    created = admit_completed_verification_job(
        fake_redis_client, REPO, result, decision, _config(), now=lambda: 1_001.0
    )
    assert created is False
    assert fake_redis_client.zcard(DUE_INDEX_KEY) == 1
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job is not None
    assert job.expected_branch == BRANCH
    assert job.expected_head_owner == "owner"


def test_oversized_provider_echo_is_bounded_in_admission_and_job(fake_redis_client):
    oversized_oid = "f" * 5000
    oversized_branch = "b" * 5000
    result = _result(snapshot_head_sha=oversized_oid, branch=oversized_branch)
    _publish(fake_redis_client, result.task_id, oversized_branch)
    decision = admit_issue_result(fake_redis_client, REPO, result, now=lambda: 1_000.0)
    assert decision.kind is AdmissionKind.ADMITTED

    admission = get_admission(fake_redis_client, result.task_id)
    assert admission is not None
    payload = json.loads(admission.payload)
    assert len(payload["snapshot_head_sha"]) == 64
    assert payload["snapshot_head_sha"] == oversized_oid[:64]
    assert len(payload["branch"]) == 120
    assert payload["branch"] == oversized_branch[:120]

    created = admit_completed_verification_job(
        fake_redis_client, REPO, result, decision, _config(), now=lambda: 1_000.0
    )
    assert created is True
    job = get_verification_job(fake_redis_client, REPO, ISSUE, decision.generation)
    assert job is not None
    assert len(job.claimed_head_oid) == 64
    assert job.claimed_head_oid == oversized_oid[:64]
    assert len(job.claimed_branch) == 120
    assert job.claimed_branch == oversized_branch[:120]


def test_normal_oid_and_branch_are_unchanged_by_bounding(fake_redis_client):
    result = _admit_completed(fake_redis_client)
    admission = get_admission(fake_redis_client, result.task_id)
    assert admission is not None
    payload = json.loads(admission.payload)
    assert payload["snapshot_head_sha"] == OID
    assert payload["branch"] == BRANCH

    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job is not None
    assert job.claimed_head_oid == OID
    assert job.claimed_branch == BRANCH


def test_job_admission_mismatch_is_quarantined(fake_redis_client):
    first = _result(task_id="legacy-task-1", branch="")
    first_decision = admit_issue_result(fake_redis_client, REPO, first, now=lambda: 1_000.0)
    assert first_decision.pre_schema is True
    assert first_decision.generation == 0
    assert (
        admit_completed_verification_job(
            fake_redis_client, REPO, first, first_decision, _config(), now=lambda: 1_000.0
        )
        is True
    )

    second = _result(task_id="legacy-task-2", branch="")
    second_decision = admit_issue_result(fake_redis_client, REPO, second, now=lambda: 1_001.0)
    assert second_decision.pre_schema is True
    assert second_decision.generation == 0
    assert (
        admit_completed_verification_job(
            fake_redis_client, REPO, second, second_decision, _config(), now=lambda: 1_001.0
        )
        is None
    )

    quarantine_job_admission_mismatch(
        fake_redis_client, REPO, second, second_decision, now=lambda: 1_002.0
    )

    items = list_quarantined_conflicts(fake_redis_client)
    assert items[0]["task_id"] == "legacy-task-2"
    assert items[0]["generation"] == 0
    assert items[0]["reason"] == "verification_job_mismatch"
    events = [
        e
        for e in _events(fake_redis_client)
        if e["type"].endswith("delivery.alert")
        and e["data"].get("alert") == "verification_job_mismatch"
    ]
    assert len(events) == 1


def test_pending_survives_marker_and_stream_expiry(fake_redis_client, label_config, mocker):
    _admit_completed(fake_redis_client)
    fake_redis_client.delete(f"pending_task:{REPO}:issue:{ISSUE}")
    fake_redis_client.client.delete(fake_redis_client._prefixed("results"))
    assert gc_issue_delivery_state(fake_redis_client, REPO, ISSUE, 1) is False
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job is not None
    assert job.state is VerificationState.PENDING
    assert fake_redis_client.ttl(make_issue_verification_job_key(REPO, ISSUE, 1)) == -1
    assert fake_redis_client.ttl(make_issue_admission_key("task-659")) == -1
    assert fake_redis_client.ttl(make_issue_dispatch_barrier_key(REPO, ISSUE)) == -1
    mocker.patch(
        "orcest.orchestrator.gh.list_labeled_issues",
        return_value=[
            {
                "number": ISSUE,
                "title": "x",
                "body": "",
                "labels": [{"name": "orcest:ready"}],
            }
        ],
    )
    actions = discover_actionable_issues(REPO, TOKEN, fake_redis_client, label_config)
    assert actions[0].action is IssueAction.SKIP_VERIFYING


def test_scheduler_verifies_and_runs_saga(fake_redis_client, mocker):
    _admit_completed(fake_redis_client)
    increment_attempts(fake_redis_client, REPO, ISSUE)
    mocker.patch("orcest.orchestrator.gh.remove_issue_label")
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job is not None
    assert job.state is VerificationState.VERIFIED
    assert job.saga_phase is SagaPhase.BARRIER_REMOVED
    assert job.selected_pr_number == "10"
    assert not has_issue_dispatch_barrier(fake_redis_client, REPO, ISSUE)
    from orcest.orchestrator.gh import remove_issue_label

    remove_issue_label.assert_called_once()
    metrics = get_delivery_metrics(fake_redis_client)
    assert metrics["verified"] == "1"


def test_barrier_removed_state_is_queued_for_gc_and_collected(fake_redis_client, mocker):
    _admit_completed(fake_redis_client)
    mocker.patch("orcest.orchestrator.gh.remove_issue_label")
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job is not None
    assert job.saga_phase is SagaPhase.BARRIER_REMOVED

    member = job_member(REPO, ISSUE, 1)
    assert fake_redis_client.zscore(GC_DUE_INDEX_KEY, member) is not None

    collected = process_due_delivery_state_gc(fake_redis_client, REPO)
    assert collected == 1
    assert fake_redis_client.zscore(GC_DUE_INDEX_KEY, member) is None
    assert get_verification_job(fake_redis_client, REPO, ISSUE, 1) is None
    assert not fake_redis_client.exists(make_issue_admission_key("task-659"))
    assert not fake_redis_client.exists(make_issue_result_ref_key(REPO, ISSUE, 1))


def test_echo_mismatch_still_verifies(fake_redis_client, mocker):
    _admit_completed(fake_redis_client)
    mocker.patch("orcest.orchestrator.gh.remove_issue_label")
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(echo_mismatch=True),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.VERIFIED
    assert job.echo_mismatch is True
    assert get_delivery_metrics(fake_redis_client)["echo_mismatch"] == "1"
    echo_events = [
        e
        for e in _events(fake_redis_client)
        if e["type"].endswith("delivery.phase") and e["data"].get("alert") == "echo_mismatch"
    ]
    assert len(echo_events) == 1
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 2_001.0,
        observe=lambda *a, **k: _observation(echo_mismatch=True),
    )
    assert get_delivery_metrics(fake_redis_client)["echo_mismatch"] == "1"
    echo_events = [
        e
        for e in _events(fake_redis_client)
        if e["type"].endswith("delivery.phase") and e["data"].get("alert") == "echo_mismatch"
    ]
    assert len(echo_events) == 1


def test_ambiguity_increments_once(fake_redis_client, mocker):
    _admit_completed(fake_redis_client)
    mocker.patch("orcest.orchestrator.gh.remove_issue_label")
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(ambiguous=True, selected_number=4),
    )
    assert get_delivery_metrics(fake_redis_client)["ambiguity"] == "1"
    ambiguity_events = [
        e
        for e in _events(fake_redis_client)
        if e["type"].endswith("delivery.phase") and e["data"].get("alert") == "ambiguous_handoff"
    ]
    assert len(ambiguity_events) == 1
    assert ambiguity_events[0]["data"]["selected_pr_number"] == "4"
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 2_001.0,
        observe=lambda *a, **k: _observation(ambiguous=True, selected_number=4),
    )
    assert get_delivery_metrics(fake_redis_client)["ambiguity"] == "1"
    ambiguity_events = [
        e
        for e in _events(fake_redis_client)
        if e["type"].endswith("delivery.phase") and e["data"].get("alert") == "ambiguous_handoff"
    ]
    assert len(ambiguity_events) == 1


def test_grace_keeps_mismatch_pending(fake_redis_client):
    _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=100),
        now=lambda: 1_010.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.MISMATCH,
            reason=DeliveryFailureReason.NO_CANDIDATE_PR,
            selected_number=None,
        ),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.PENDING


def test_grace_expiry_becomes_ineffective(fake_redis_client):
    _admit_completed(fake_redis_client)
    now = 1_040.0
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=30, ineffective_cooldown_seconds=20),
        stream_redis=fake_redis_client,
        now=lambda: now,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.MISMATCH,
            reason=DeliveryFailureReason.NO_CANDIDATE_PR,
            selected_number=None,
        ),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.INEFFECTIVE
    assert job.saga_phase is SagaPhase.BARRIER_REMOVED
    retry_record = fake_redis_client.hgetall(make_issue_retry_record_key(REPO, ISSUE, 1))
    assert retry_record["created_at"] == f"{now:.3f}"
    assert retry_record["cooldown_until"] == f"{now + 20:.3f}"
    context = load_issue_retry_context(fake_redis_client, REPO, ISSUE, 1)
    assert context is not None
    assert context.task_id == "task-659"
    assert context.generation == 1
    assert context.expected_ref == BRANCH
    assert context.expected_head_owner == "owner"
    assert context.reason_code == DeliveryFailureReason.NO_CANDIDATE_PR.value
    assert "title" not in retry_record
    assert "body" not in retry_record
    assert "summary" not in retry_record
    assert fake_redis_client.zscore(GC_DUE_INDEX_KEY, job_member(REPO, ISSUE, 1)) == now
    assert not has_issue_dispatch_barrier(fake_redis_client, REPO, ISSUE)


def _observe_no_handoff(*_args: object, **_kwargs: object) -> HandoffObservation:
    return _observation(
        verified=False,
        kind=DeliveryErrorKind.MISMATCH,
        reason=DeliveryFailureReason.NO_CANDIDATE_PR,
        selected_number=None,
    )


def test_ineffective_unbound_retry_context_still_counts_generation(
    fake_redis_client, label_config, mocker
):
    """RetryContextBoundError must still persist the generation retry hash.

    Empty expected_head_owner rejects resume context, and with
    ``ineffective_cooldown_seconds == 0`` there is no cooldown key either.
    The hash is what keeps INEFFECTIVE generations in the attempt budget.
    """
    _admit_completed(fake_redis_client)
    fake_redis_client.hset(
        make_issue_verification_job_key(REPO, ISSUE, 1),
        "expected_head_owner",
        "",
    )
    now = 1_040.0
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0, ineffective_cooldown_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: now,
        observe=_observe_no_handoff,
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.INEFFECTIVE
    assert job.saga_phase is SagaPhase.BARRIER_REMOVED
    retry_record = fake_redis_client.hgetall(make_issue_retry_record_key(REPO, ISSUE, 1))
    assert retry_record["generation"] == "1"
    assert retry_record["task_id"] == "task-659"
    assert retry_record["reason_code"] == DeliveryFailureReason.NO_CANDIDATE_PR.value
    assert retry_record["created_at"] == f"{now:.3f}"
    assert retry_record["cooldown_until"] == f"{now:.3f}"
    assert retry_record["expected_ref"] == ""
    assert retry_record["expected_head_owner"] == ""
    assert load_issue_retry_context(fake_redis_client, REPO, ISSUE, 1) is None
    assert not fake_redis_client.exists(make_issue_delivery_cooldown_key(REPO, ISSUE))
    assert count_ineffective_delivery_generations(fake_redis_client, REPO, ISSUE) == 1

    mocker.patch(
        "orcest.orchestrator.gh.list_labeled_issues",
        return_value=[
            {
                "number": ISSUE,
                "title": "x",
                "body": "",
                "labels": [{"name": "orcest:ready"}],
            }
        ],
    )
    fake_redis_client.delete(make_pending_task_key(REPO, "issue", ISSUE))
    clear_attempts(fake_redis_client, REPO, ISSUE)
    actions = discover_actionable_issues(
        REPO, TOKEN, fake_redis_client, label_config, max_attempts=1
    )
    assert get_attempt_count(fake_redis_client, REPO, ISSUE) == 0
    assert actions[0].action is IssueAction.SKIP_MAX_ATTEMPTS


def test_count_ineffective_delivery_generations_walks_prior_gens(fake_redis_client):
    fake_redis_client.set_value(make_issue_generation_key(REPO, ISSUE), "4")
    for gen in (1, 3):
        fake_redis_client.hset_mapping(
            make_issue_retry_record_key(REPO, ISSUE, gen),
            {"reason": "ineffective_delivery", "generation": str(gen)},
        )
    assert count_ineffective_delivery_generations(fake_redis_client, REPO, ISSUE) == 2
    assert count_ineffective_delivery_generations(fake_redis_client, REPO, ISSUE, limit=1) == 1


def test_ineffective_loop_exhausts_max_attempts_after_pending_and_attempts_cleared(
    fake_redis_client, label_config, mocker
):
    """COMPLETED without a qualifying PR cannot refresh max_attempts every generation.

    Admission clears the pending marker, and workers delete the issue attempts
    hash on terminal cleanup. After INEFFECTIVE + cooldown, discovery must still
    count those generations against the retry budget.
    """
    mocker.patch(
        "orcest.orchestrator.gh.list_labeled_issues",
        return_value=[
            {
                "number": ISSUE,
                "title": "x",
                "body": "",
                "labels": [{"name": "orcest:ready"}],
            }
        ],
    )
    for i in range(3):
        _admit_completed(fake_redis_client, _result(task_id=f"task-ineffective-{i}"))
        fake_redis_client.delete(make_pending_task_key(REPO, "issue", ISSUE))
        clear_attempts(fake_redis_client, REPO, ISSUE)
        process_due_verification_jobs(
            fake_redis_client,
            REPO,
            TOKEN,
            LabelConfig(),
            _config(grace_seconds=0, ineffective_cooldown_seconds=20),
            stream_redis=fake_redis_client,
            now=lambda: 2_000.0 + i,
            observe=_observe_no_handoff,
        )
        fake_redis_client.delete(make_issue_delivery_cooldown_key(REPO, ISSUE))
        process_due_delivery_state_gc(fake_redis_client, REPO, now=lambda: 3_000.0 + i)
        actions = discover_actionable_issues(
            REPO, TOKEN, fake_redis_client, label_config, max_attempts=3
        )
        assert get_attempt_count(fake_redis_client, REPO, ISSUE) == 0
        if i < 2:
            assert actions[0].action is IssueAction.ENQUEUE_IMPLEMENT
        else:
            assert actions[0].action is IssueAction.SKIP_MAX_ATTEMPTS
    assert count_ineffective_delivery_generations(fake_redis_client, REPO, ISSUE) == 3


def test_transport_backoff_and_auth_unverifiable(fake_redis_client):
    _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(),
        now=lambda: 1_001.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.TRANSPORT,
            reason=DeliveryFailureReason.GH_TRANSPORT_ERROR,
            complete=False,
            selected_number=None,
        ),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.PENDING
    assert job.attempt_count == 1
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(),
        now=lambda: 1_100.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.AUTHENTICATION,
            reason=DeliveryFailureReason.AUTHENTICATION_FAILED,
            complete=False,
            selected_number=None,
        ),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.UNVERIFIABLE
    assert has_issue_dispatch_barrier(fake_redis_client, REPO, ISSUE)


def test_pre_schema_unproven_is_unverifiable(fake_redis_client):
    result = _result(task_id="legacy-task")
    decision = admit_issue_result(fake_redis_client, REPO, result, now=lambda: 5.0)
    assert decision.pre_schema is True
    admit_completed_verification_job(
        fake_redis_client, REPO, result, decision, _config(), now=lambda: 5.0
    )
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 50.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.MISMATCH,
            reason=DeliveryFailureReason.NO_CANONICAL_CLOSING_REFERENCE,
            selected_number=None,
        ),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 0)
    assert job.state is VerificationState.UNVERIFIABLE
    assert has_issue_dispatch_barrier(fake_redis_client, REPO, ISSUE)


def test_reconcile_repairs_missing_due_membership(fake_redis_client):
    _admit_completed(fake_redis_client)
    fake_redis_client.zrem(DUE_INDEX_KEY, job_member(REPO, ISSUE, 1))
    repaired = reconcile_verification_due_index(fake_redis_client, now=lambda: 1_500.0)
    assert repaired == 1
    assert fake_redis_client.zscore(DUE_INDEX_KEY, job_member(REPO, ISSUE, 1)) is not None


def test_verified_saga_replay_is_idempotent(fake_redis_client, mocker):
    _admit_completed(fake_redis_client)
    mocker.patch("orcest.orchestrator.gh.remove_issue_label")
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(),
    )
    from orcest.orchestrator.gh import remove_issue_label

    first_phases = [e for e in _events(fake_redis_client) if e["type"].endswith("delivery.phase")]
    assert first_phases
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: 2_001.0,
        observe=lambda *a, **k: _observation(),
    )
    later = [e for e in _events(fake_redis_client) if e["type"].endswith("delivery.phase")]
    assert [e["id"] for e in later] == [e["id"] for e in first_phases]
    assert remove_issue_label.call_count == 1


def test_crash_between_verified_phases(fake_redis_client, mocker):
    _admit_completed(fake_redis_client)
    mocker.patch(
        "orcest.orchestrator.gh.remove_issue_label",
        side_effect=[RuntimeError("github down"), RuntimeError("github down"), None],
    )
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.VERIFIED
    assert job.saga_phase is SagaPhase.TERMINAL_PERSISTED
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: 2_010.0,
        observe=lambda *a, **k: _observation(),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.saga_phase is SagaPhase.BARRIER_REMOVED


def test_unverifiable_survives_ttl_and_blocks_dispatch(fake_redis_client, label_config, mocker):
    result = _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(),
        now=lambda: 1_100.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.SCHEMA,
            reason=DeliveryFailureReason.MALFORMED_RESPONSE,
            complete=False,
            selected_number=None,
        ),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 1)
    assert job.state is VerificationState.UNVERIFIABLE
    fake_redis_client.delete("pending-task")
    assert gc_issue_delivery_state(fake_redis_client, REPO, ISSUE, 1) is False
    assert fake_redis_client.ttl(make_issue_verification_job_key(REPO, ISSUE, 1)) == -1
    mocker.patch(
        "orcest.orchestrator.gh.list_labeled_issues",
        return_value=[
            {
                "number": ISSUE,
                "title": "x",
                "body": "",
                "labels": [{"name": "orcest:ready"}],
            }
        ],
    )
    actions = discover_actionable_issues(REPO, TOKEN, fake_redis_client, label_config)
    assert actions[0].action is IssueAction.SKIP_VERIFYING
    assert result.task_id


def test_gc_due_member_stays_until_state_is_collected(fake_redis_client):
    _admit_completed(fake_redis_client)
    member = job_member(REPO, ISSUE, 1)
    fake_redis_client.zadd(GC_DUE_INDEX_KEY, {member: 1.0})

    collected = process_due_delivery_state_gc(fake_redis_client, REPO, now=lambda: 2_000.0)

    assert collected == 0
    assert fake_redis_client.zscore(GC_DUE_INDEX_KEY, member) is not None
    assert "1 delivery GC job(s)" in delivery_state_blocks_old_orchestrator_rollback(
        fake_redis_client
    )


def test_events_are_secret_free(fake_redis_client, mocker):
    secret_result = _result(summary="token ghp_secret123")
    _publish(fake_redis_client, secret_result.task_id)
    decision = admit_issue_result(fake_redis_client, REPO, secret_result, now=lambda: 1.0)
    admit_completed_verification_job(
        fake_redis_client, REPO, secret_result, decision, _config(), now=lambda: 1.0
    )
    mocker.patch("orcest.orchestrator.gh.remove_issue_label")
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        now=lambda: 50.0,
        observe=lambda *a, **k: _observation(),
    )
    dumped = json.dumps(_events(fake_redis_client))
    assert "ghp_secret123" not in dumped
    assert TOKEN not in dumped or TOKEN == "fake-token"
    assert "ghp_secret123" not in json.dumps(get_delivery_metrics(fake_redis_client))


def test_rollback_requires_drain(fake_redis_client):
    assert delivery_state_blocks_old_orchestrator_rollback(fake_redis_client) == []
    _admit_completed(fake_redis_client)
    reasons = delivery_state_blocks_old_orchestrator_rollback(fake_redis_client)
    assert reasons
    fake_redis_client.delete(DUE_INDEX_KEY, ACTIVE_JOBS_KEY, UNVERIFIABLE_INDEX_KEY, QUARANTINE_KEY)
    fake_redis_client.delete(make_issue_verification_job_key(REPO, ISSUE, 1))
    fake_redis_client.delete(make_issue_dispatch_barrier_key(REPO, ISSUE))
    # Active set was deleted; remaining job hash does not count without indexes.
    # Recreate the drain contract: operator must remove jobs and indexes.
    fake_redis_client.delete(METRICS_KEY)
    assert delivery_state_blocks_old_orchestrator_rollback(fake_redis_client) == []


def test_oldest_pending_alert(fake_redis_client, caplog):
    _admit_completed(fake_redis_client)
    caplog.set_level(logging.WARNING)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(oldest_pending_alert_seconds=10, grace_seconds=10_000),
        now=lambda: 2_000.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.TRANSPORT,
            reason=DeliveryFailureReason.GH_TRANSPORT_ERROR,
            complete=False,
            selected_number=None,
        ),
    )
    assert "Oldest PENDING" in caplog.text
    assert get_delivery_metrics(fake_redis_client).get("oldest_pending_alerts") == "1"


def _draft_pr(
    *,
    number: int = 4,
    head_repository: str = REPO,
    state: str = "OPEN",
) -> CandidatePullRequest:
    return CandidatePullRequest(
        number=number,
        url=f"https://evil.example/{head_repository}/pull/{number}",
        state=state,
        is_draft=True,
        base_repository=REPO,
        base_ref_name="master",
        head_repository=head_repository,
        head_ref_name=BRANCH,
        head_oid=OID,
        closing_issues_references=(),
    )


def test_ineffective_retry_record_resumes_partial_pr(fake_redis_client):
    _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0, ineffective_cooldown_seconds=20),
        stream_redis=fake_redis_client,
        now=lambda: 1_040.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.MISMATCH,
            reason=DeliveryFailureReason.NO_CANDIDATE_PR,
            selected_number=None,
            live_head_oid=OID,
            candidate_prs=(_draft_pr(number=8), _draft_pr(number=4)),
        ),
    )
    context = load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE)
    assert context is not None
    assert context.remote_head_oid == OID
    assert context.pr_number == 4
    assert context.pr_url == f"https://github.com/{REPO}/pull/4"
    dumped = context.to_canonical_json()
    assert "evil.example" not in dumped
    assert "is_draft" not in dumped


def test_ineffective_retry_context_ignores_closed_partial_pr(fake_redis_client):
    _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0, ineffective_cooldown_seconds=20),
        stream_redis=fake_redis_client,
        now=lambda: 1_040.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.MISMATCH,
            reason=DeliveryFailureReason.NO_CANDIDATE_PR,
            selected_number=None,
            live_head_oid=OID,
            candidate_prs=(_draft_pr(number=4, state="CLOSED"), _draft_pr(number=8)),
        ),
    )
    context = load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE)
    assert context is not None
    assert context.pr_number == 8
    assert context.pr_url == f"https://github.com/{REPO}/pull/8"


def test_fork_pr_is_not_stored_in_retry_context(fake_redis_client):
    _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0, ineffective_cooldown_seconds=20),
        stream_redis=fake_redis_client,
        now=lambda: 1_040.0,
        observe=lambda *a, **k: _observation(
            verified=False,
            kind=DeliveryErrorKind.MISMATCH,
            reason=DeliveryFailureReason.NO_CANDIDATE_PR,
            selected_number=None,
            live_head_oid=OID,
            candidate_prs=(_draft_pr(number=9, head_repository="attacker/fork"),),
        ),
    )
    context = load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE)
    assert context is not None
    assert context.pr_number is None
    assert context.pr_url == ""


def test_verified_clears_matching_retry_context_and_cooldown(fake_redis_client, mocker):
    _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0, ineffective_cooldown_seconds=20),
        stream_redis=fake_redis_client,
        now=lambda: 1_040.0,
        observe=_observe_no_handoff,
    )
    assert load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE) is not None
    assert fake_redis_client.exists(make_issue_delivery_cooldown_key(REPO, ISSUE))
    mocker.patch("orcest.orchestrator.gh.remove_issue_label")
    fake_redis_client.delete(make_pending_task_key(REPO, "issue", ISSUE))
    result = _result(task_id="task-verified-2", branch=BRANCH)
    gen = _publish(fake_redis_client, result.task_id, BRANCH)
    assert gen == 2
    decision = admit_issue_result(fake_redis_client, REPO, result, now=lambda: 2_000.0)
    assert decision.generation == 2
    assert admit_completed_verification_job(
        fake_redis_client, REPO, result, decision, _config(grace_seconds=0), now=lambda: 2_000.0
    )
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: 2_010.0,
        observe=lambda *a, **k: _observation(),
    )
    job = get_verification_job(fake_redis_client, REPO, ISSUE, 2)
    assert job.state is VerificationState.VERIFIED
    assert job.saga_phase is SagaPhase.BARRIER_REMOVED
    assert load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE) is None
    assert not fake_redis_client.exists(make_issue_retry_record_key(REPO, ISSUE, 1))
    assert not fake_redis_client.exists(make_issue_delivery_cooldown_key(REPO, ISSUE))
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0),
        stream_redis=fake_redis_client,
        now=lambda: 2_011.0,
        observe=lambda *a, **k: _observation(),
    )
    assert load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE) is None


def test_stale_verified_clear_does_not_drop_newer_retry(fake_redis_client):
    _admit_completed(fake_redis_client)
    process_due_verification_jobs(
        fake_redis_client,
        REPO,
        TOKEN,
        LabelConfig(),
        _config(grace_seconds=0, ineffective_cooldown_seconds=20),
        stream_redis=fake_redis_client,
        now=lambda: 1_040.0,
        observe=_observe_no_handoff,
    )
    fake_redis_client.set_value(make_issue_generation_key(REPO, ISSUE), "2")
    fake_redis_client.set_value(make_issue_retry_latest_key(REPO, ISSUE), "2")
    fake_redis_client.hset_mapping(
        make_issue_retry_record_key(REPO, ISSUE, 2),
        {
            "schema_version": "1",
            "task_id": "task-newer",
            "generation": "2",
            "expected_ref": BRANCH,
            "expected_head_owner": "owner",
            "remote_head_oid": OID,
            "pr_number": "6",
            "pr_url": f"https://github.com/{REPO}/pull/6",
            "reason_code": "no_candidate_pr",
            "created_at": "2000.000",
            "cooldown_until": "2020.000",
        },
    )
    fake_redis_client.set_ex(make_issue_delivery_cooldown_key(REPO, ISSUE), "2", 30)
    assert not clear_issue_retry_context(fake_redis_client, REPO, ISSUE, 1)
    newer = load_latest_issue_retry_context(fake_redis_client, REPO, ISSUE)
    assert newer is not None
    assert newer.generation == 2
    assert newer.task_id == "task-newer"
    assert fake_redis_client.get(make_issue_delivery_cooldown_key(REPO, ISSUE)) == "2"
