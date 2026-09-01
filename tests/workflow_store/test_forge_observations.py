"""Durable Forge Observation Schedule/Request/Outbox/Observation substrate."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import failure_evidence_digest
from orcest.workflow_store.store import (
    CasMismatchError,
    ForgeObservationInput,
    IdempotencyConflictError,
    RunStore,
    RunStoreError,
)

pytestmark = pytest.mark.unit


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def run_store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


class Project:
    def __init__(self, project_id: str, forge_instance_id: str, source_read_secret_id: str) -> None:
        self.project_id = project_id
        self.forge_instance_id = forge_instance_id
        self.source_read_secret_id = source_read_secret_id


def _seed_project(store: RunStore) -> Project:
    """Insert a Forge Instance + Project directly (bypasses the full HTTPS
    onboarding flow tested elsewhere) so the Forge Observation Schedule/
    Request/Observation foreign keys have something real to point at."""
    now = _now_ms()

    def _secret(purpose: str) -> str:
        secret_id = _uid()
        store.conn.execute(
            "INSERT INTO secret_current_versions(secret_id, purpose, owner_scope_kind, "
            "owner_scope_id, provider_account_ref, current_version, last_operation_id, "
            "created_at_ms, updated_at_ms) VALUES (?, ?, 'PROJECT', ?, NULL, 1, ?, ?, ?)",
            (secret_id, purpose, _uid(), _uid(), now, now),
        )
        return secret_id

    forge_api_secret = _secret("FORGE_API")
    source_read_secret = _secret("SOURCE_READ")
    publication_secret = _secret("PUBLICATION")

    forge_instance_id = _uid()
    store.conn.execute(
        "INSERT INTO forge_instances(forge_instance_id, adapter_kind, canonical_origin, "
        "credential_secret_id, registration_provenance_version, created_at_ms) "
        "VALUES (?, 'GITHUB', ?, ?, 1, ?)",
        (forge_instance_id, f"github.com/{_uid()}", forge_api_secret, now),
    )

    project_id = _uid()
    store.conn.execute(
        "INSERT INTO projects(project_id, forge_instance_id, installation_or_account_ref, "
        "repository_external_id, repository_locator, default_ref, trusted_base_policy_ref, "
        "budget_policy_ref, budget_reset_window_ref, source_read_secret_id, "
        "publication_secret_id, registration_source_read_secret_version, "
        "registration_publication_secret_version, registration_revision, "
        "registration_operation_id, work_item_discovery_schedule_id, registration_state) "
        "VALUES (?, ?, 'inst', ?, 'org/repo', 'main', 'default', 'default', 'default', "
        "?, ?, 1, 1, 1, ?, ?, 'ACTIVE')",
        (
            project_id,
            forge_instance_id,
            _uid(),
            source_read_secret,
            publication_secret,
            _uid(),
            _uid(),
        ),
    )
    store.conn.commit()
    return Project(project_id, forge_instance_id, source_read_secret)


def _due_request(
    store: RunStore,
    project: Project,
    schedule_id: str,
    *,
    controller_mode: str = "RUNNING",
):
    return store.create_due_forge_observation_request(
        forge_observation_request_id=_uid(),
        forge_observation_schedule_id=schedule_id,
        now_ms=_now_ms(),
        controller_mode=controller_mode,
        controller_mode_revision=1,
        credential_purpose="PROJECT_SOURCE_READ",
        credential_secret_id=project.source_read_secret_id,
        credential_secret_version=1,
        outbox_id=_uid(),
    )


def _work_item_poll_schedule(store: RunStore, project: Project, target_id: str = "issue-1"):
    return store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind="WORK_ITEM_POLL",
        project_id=project.project_id,
        forge_instance_id=project.forge_instance_id,
        target_kind="WORK_ITEM",
        target_id=target_id,
        minimum_interval_ms=1,
        next_due_at_ms=0,
    )


def test_create_forge_observation_schedule_is_idempotent_by_identity(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    first = _work_item_poll_schedule(run_store, project)
    second = run_store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind="WORK_ITEM_POLL",
        project_id=project.project_id,
        forge_instance_id=project.forge_instance_id,
        target_kind="WORK_ITEM",
        target_id="issue-1",
        minimum_interval_ms=999,
        next_due_at_ms=12345,
    )
    assert second.forge_observation_schedule_id == first.forge_observation_schedule_id
    assert second.minimum_interval_ms == first.minimum_interval_ms


def test_schedule_kind_target_kind_matrix_is_enforced(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    with pytest.raises(Exception):
        run_store.create_forge_observation_schedule(
            forge_observation_schedule_id=_uid(),
            schedule_kind="WORK_ITEM_POLL",
            project_id=project.project_id,
            forge_instance_id=project.forge_instance_id,
            target_kind="PUBLICATION",
            target_id="pub-1",
            minimum_interval_ms=1,
            next_due_at_ms=0,
        )


def test_create_due_request_is_idempotent_while_pending(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    first = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    second = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert first.forge_observation_request_id == second.forge_observation_request_id
    assert first.state == "PENDING"
    outbox = run_store.conn.execute(
        "SELECT state FROM outbox WHERE outbox_id = ?", (first.outbox_id,)
    ).fetchone()
    assert outbox["state"] == "PENDING"


def test_maintenance_mode_creates_no_request(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    request = _due_request(
        run_store, project, schedule.forge_observation_schedule_id, controller_mode="MAINTENANCE"
    )
    assert request is None


def test_success_produces_one_observation_and_delivered_outbox(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    request = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert request is not None
    run_store.record_forge_observation_request_attempt(request.forge_observation_request_id)

    completion = run_store.complete_forge_observation_request(
        forge_observation_request_id=request.forge_observation_request_id,
        observations=[
            ForgeObservationInput(
                kind="WORK_ITEM_SNAPSHOT", external_revision="rev-1", fact={"state": "OPEN"}
            )
        ],
    )

    assert completion.request.state == "COMPLETED"
    assert len(completion.observation_ids) == 1
    observation = run_store.get_forge_observation(completion.observation_ids[0])
    assert observation is not None
    assert observation.kind == "WORK_ITEM_SNAPSHOT"
    assert observation.observation_sequence == 1
    outbox = run_store.conn.execute(
        "SELECT state FROM outbox WHERE outbox_id = ?", (request.outbox_id,)
    ).fetchone()
    assert outbox["state"] == "DELIVERED"


def test_transient_failure_leaves_request_and_outbox_pending(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    request = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert request is not None
    ordinal = run_store.record_forge_observation_request_attempt(
        request.forge_observation_request_id
    )

    fact = run_store.record_forge_request_failure_fact(
        forge_request_failure_fact_id=_uid(),
        forge_observation_request_id=request.forge_observation_request_id,
        request_attempt_ordinal=ordinal,
        failure_kind="TIMEOUT",
        failure_code="ETIMEDOUT",
        failure_evidence_digest=failure_evidence_digest({"note": "timed out"}),
        retry_not_before_ms=_now_ms() + 5_000,
    )
    assert fact.failure_kind == "TIMEOUT"

    after = run_store.get_forge_observation_request(request.forge_observation_request_id)
    assert after is not None
    assert after.state == "PENDING"
    assert after.last_failure_fact_id == fact.forge_request_failure_fact_id
    assert after.next_retry_ms == fact.retry_not_before_ms
    outbox = run_store.conn.execute(
        "SELECT state FROM outbox WHERE outbox_id = ?", (request.outbox_id,)
    ).fetchone()
    assert outbox["state"] == "PENDING"

    # Exact retry with identical content replays the same Fact.
    replay = run_store.record_forge_request_failure_fact(
        forge_request_failure_fact_id=_uid(),
        forge_observation_request_id=request.forge_observation_request_id,
        request_attempt_ordinal=ordinal,
        failure_kind="TIMEOUT",
        failure_code="ETIMEDOUT",
        failure_evidence_digest=failure_evidence_digest({"note": "timed out"}),
        retry_not_before_ms=fact.retry_not_before_ms,
    )
    assert replay.forge_request_failure_fact_id == fact.forge_request_failure_fact_id


def test_late_failure_fact_after_completion_is_rejected(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    request = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert request is not None
    ordinal = run_store.record_forge_observation_request_attempt(
        request.forge_observation_request_id
    )
    run_store.complete_forge_observation_request(
        forge_observation_request_id=request.forge_observation_request_id,
        observations=[
            ForgeObservationInput(
                kind="WORK_ITEM_SNAPSHOT", external_revision="rev-1", fact={"state": "OPEN"}
            )
        ],
    )

    with pytest.raises(CasMismatchError):
        run_store.record_forge_request_failure_fact(
            forge_request_failure_fact_id=_uid(),
            forge_observation_request_id=request.forge_observation_request_id,
            request_attempt_ordinal=ordinal,
            failure_kind="TIMEOUT",
            failure_code="ETIMEDOUT",
            failure_evidence_digest=failure_evidence_digest({"note": "too late"}),
            retry_not_before_ms=_now_ms() + 1_000,
        )


def test_a_b_a_sequence_records_three_observations_but_adjacent_repeat_coalesces(
    run_store: RunStore,
) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    seen_ids: list[str] = []
    for value in ("A", "B", "A", "A"):
        request = _due_request(run_store, project, schedule.forge_observation_schedule_id)
        assert request is not None
        run_store.record_forge_observation_request_attempt(request.forge_observation_request_id)
        completion = run_store.complete_forge_observation_request(
            forge_observation_request_id=request.forge_observation_request_id,
            observations=[
                ForgeObservationInput(
                    kind="WORK_ITEM_SNAPSHOT", external_revision=value, fact={"v": value}
                )
            ],
        )
        seen_ids.append(completion.observation_ids[0])

    observations = run_store.list_forge_observations_for_target(
        project_id=project.project_id, target_kind="WORK_ITEM", target_id="issue-1"
    )
    assert [o.external_revision for o in observations] == ["A", "B", "A"]
    # The immediate repeat of the final "A" coalesced into the same row.
    assert seen_ids[2] == seen_ids[3]
    assert seen_ids[0] != seen_ids[2]


def test_adapter_event_id_replay_is_idempotent_and_conflict_is_rejected(
    run_store: RunStore,
) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    request = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert request is not None
    run_store.record_forge_observation_request_attempt(request.forge_observation_request_id)
    completion = run_store.complete_forge_observation_request(
        forge_observation_request_id=request.forge_observation_request_id,
        observations=[
            ForgeObservationInput(
                kind="WORK_ITEM_SNAPSHOT",
                external_revision="rev-1",
                fact={"state": "OPEN"},
                adapter_event_id="delivery-1",
            )
        ],
    )

    request2 = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert request2 is not None
    run_store.record_forge_observation_request_attempt(request2.forge_observation_request_id)
    replay = run_store.complete_forge_observation_request(
        forge_observation_request_id=request2.forge_observation_request_id,
        observations=[
            ForgeObservationInput(
                kind="WORK_ITEM_SNAPSHOT",
                external_revision="rev-1",
                fact={"state": "OPEN"},
                adapter_event_id="delivery-1",
            )
        ],
    )
    assert replay.observation_ids == completion.observation_ids

    request3 = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert request3 is not None
    run_store.record_forge_observation_request_attempt(request3.forge_observation_request_id)
    with pytest.raises(IdempotencyConflictError):
        run_store.complete_forge_observation_request(
            forge_observation_request_id=request3.forge_observation_request_id,
            observations=[
                ForgeObservationInput(
                    kind="WORK_ITEM_SNAPSHOT",
                    external_revision="rev-2",
                    fact={"state": "CLOSED"},
                    adapter_event_id="delivery-1",
                )
            ],
        )


def test_close_schedule_supersedes_pending_request_and_late_response_still_delivers_outbox(
    run_store: RunStore,
) -> None:
    project = _seed_project(run_store)
    schedule = _work_item_poll_schedule(run_store, project)
    request = _due_request(run_store, project, schedule.forge_observation_schedule_id)
    assert request is not None

    current = run_store.get_forge_observation_schedule(schedule.forge_observation_schedule_id)
    assert current is not None
    closed = run_store.close_forge_observation_schedule(
        schedule.forge_observation_schedule_id, expected_revision=current.schedule_revision
    )
    assert closed.state == "CLOSED"

    superseded = run_store.get_forge_observation_request(request.forge_observation_request_id)
    assert superseded is not None
    assert superseded.state == "SUPERSEDED"
    assert superseded.result_observation_ids_digest is not None
    outbox_before = run_store.conn.execute(
        "SELECT state FROM outbox WHERE outbox_id = ?", (request.outbox_id,)
    ).fetchone()
    assert outbox_before["state"] == "SUPERSEDED"

    # A late adapter response for the now-superseded Request still proves I/O
    # happened: the Request stays SUPERSEDED, but the Outbox is (re)delivered.
    late = run_store.complete_forge_observation_request(
        forge_observation_request_id=request.forge_observation_request_id,
        observations=[
            ForgeObservationInput(
                kind="WORK_ITEM_SNAPSHOT", external_revision="late", fact={"v": "late"}
            )
        ],
    )
    assert late.request.state == "SUPERSEDED"
    outbox_after = run_store.conn.execute(
        "SELECT state FROM outbox WHERE outbox_id = ?", (request.outbox_id,)
    ).fetchone()
    assert outbox_after["state"] == "DELIVERED"

    # Closing again is an idempotent no-op.
    again = run_store.close_forge_observation_schedule(
        schedule.forge_observation_schedule_id, expected_revision=closed.schedule_revision
    )
    assert again.state == "CLOSED"


def _discover(run_store: RunStore, project: Project, schedule_id: str, items, revision: str):
    request = _due_request(run_store, project, schedule_id)
    assert request is not None
    run_store.record_forge_observation_request_attempt(request.forge_observation_request_id)
    return run_store.complete_work_item_discovery_request(
        forge_observation_request_id=request.forge_observation_request_id,
        discovery_search_revision=revision,
        work_items=items,
    )


def test_discovery_completion_creates_children_deterministically(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    discovery_schedule = run_store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind="WORK_ITEM_DISCOVERY",
        project_id=project.project_id,
        forge_instance_id=project.forge_instance_id,
        target_kind="PROJECT",
        target_id=project.project_id,
        minimum_interval_ms=1,
        next_due_at_ms=0,
    )

    items = [
        ForgeObservationInput(
            kind="WORK_ITEM_SNAPSHOT", external_revision="r2", fact={"n": 2}, target_id="issue-2"
        ),
        ForgeObservationInput(
            kind="WORK_ITEM_SNAPSHOT", external_revision="r1", fact={"n": 1}, target_id="issue-1"
        ),
    ]
    completion = _discover(
        run_store, project, discovery_schedule.forge_observation_schedule_id, items, "search-1"
    )
    assert completion.request.state == "COMPLETED"
    assert len(completion.observation_ids) == 2
    assert completion.request.result_discovery_search_revision == "search-1"

    children = run_store.conn.execute(
        "SELECT schedule_kind, target_id, state FROM forge_observation_schedules "
        "WHERE schedule_kind IN ('WORK_ITEM_POLL', 'BASE_HEAD_POLL') "
        "ORDER BY target_id, schedule_kind"
    ).fetchall()
    assert [(r["schedule_kind"], r["target_id"], r["state"]) for r in children] == [
        ("BASE_HEAD_POLL", "issue-1", "ACTIVE"),
        ("WORK_ITEM_POLL", "issue-1", "ACTIVE"),
        ("BASE_HEAD_POLL", "issue-2", "ACTIVE"),
        ("WORK_ITEM_POLL", "issue-2", "ACTIVE"),
    ]

    # Observation sequence is committed in bytewise stable-ID order (issue-1
    # before issue-2) regardless of the adapter response order above.
    issue1_obs = run_store.list_forge_observations_for_target(
        project_id=project.project_id, target_kind="WORK_ITEM", target_id="issue-1"
    )
    issue2_obs = run_store.list_forge_observations_for_target(
        project_id=project.project_id, target_kind="WORK_ITEM", target_id="issue-2"
    )
    assert issue1_obs[0].external_revision == "r1"
    assert issue2_obs[0].external_revision == "r2"

    # Re-running discovery with the identical set is stable replay: no new
    # child Schedules, same discovery pair recorded again.
    completion2 = _discover(
        run_store, project, discovery_schedule.forge_observation_schedule_id, items, "search-1"
    )
    assert completion2.request.result_discovery_search_revision == "search-1"
    children2 = run_store.conn.execute(
        "SELECT COUNT(*) AS n FROM forge_observation_schedules "
        "WHERE schedule_kind IN ('WORK_ITEM_POLL', 'BASE_HEAD_POLL')"
    ).fetchone()
    assert children2["n"] == 4

    # A later discovery that drops issue-1 (with no active Run) closes its
    # Run-null children.
    completion3 = _discover(
        run_store,
        project,
        discovery_schedule.forge_observation_schedule_id,
        [items[0]],
        "search-2",
    )
    assert completion3.request.state == "COMPLETED"
    remaining = run_store.conn.execute(
        "SELECT schedule_kind, target_id, state FROM forge_observation_schedules "
        "WHERE schedule_kind IN ('WORK_ITEM_POLL', 'BASE_HEAD_POLL') "
        "ORDER BY target_id, schedule_kind"
    ).fetchall()
    assert {(r["target_id"], r["state"]) for r in remaining} == {
        ("issue-1", "CLOSED"),
        ("issue-2", "ACTIVE"),
    }

    # An empty discovered set (every Work Item disappeared) must still close
    # every remaining Run-null child, not silently retain them.
    completion4 = _discover(
        run_store, project, discovery_schedule.forge_observation_schedule_id, [], "search-3"
    )
    assert completion4.request.state == "COMPLETED"
    final = run_store.conn.execute(
        "SELECT state FROM forge_observation_schedules "
        "WHERE schedule_kind IN ('WORK_ITEM_POLL', 'BASE_HEAD_POLL')"
    ).fetchall()
    assert {r["state"] for r in final} == {"CLOSED"}


def test_discovery_completion_preserves_paused_children_and_never_reactivates(
    run_store: RunStore,
) -> None:
    project = _seed_project(run_store)
    discovery_schedule = run_store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind="WORK_ITEM_DISCOVERY",
        project_id=project.project_id,
        forge_instance_id=project.forge_instance_id,
        target_kind="PROJECT",
        target_id=project.project_id,
        minimum_interval_ms=1,
        next_due_at_ms=0,
    )
    request = _due_request(run_store, project, discovery_schedule.forge_observation_schedule_id)
    assert request is not None
    run_store.record_forge_observation_request_attempt(request.forge_observation_request_id)
    # Simulate the discovery Schedule pausing (e.g. Project suspension) while
    # this Request's read is already in flight: a PAUSED Schedule may still
    # finish an already-pending read, per persistence-and-recovery.
    run_store.conn.execute(
        "UPDATE forge_observation_schedules SET state = 'PAUSED' "
        "WHERE forge_observation_schedule_id = ?",
        (discovery_schedule.forge_observation_schedule_id,),
    )
    run_store.conn.commit()

    items = [
        ForgeObservationInput(
            kind="WORK_ITEM_SNAPSHOT", external_revision="r1", fact={"n": 1}, target_id="issue-1"
        )
    ]
    completion = run_store.complete_work_item_discovery_request(
        forge_observation_request_id=request.forge_observation_request_id,
        discovery_search_revision="search-1",
        work_items=items,
    )
    assert completion.request.state == "COMPLETED"
    children = run_store.conn.execute(
        "SELECT schedule_kind, state FROM forge_observation_schedules "
        "WHERE target_id = 'issue-1' AND schedule_kind IN ('WORK_ITEM_POLL', 'BASE_HEAD_POLL')"
    ).fetchall()
    assert {r["state"] for r in children} == {"PAUSED"}


def test_discovery_request_kind_cannot_use_ordinary_completion(run_store: RunStore) -> None:
    project = _seed_project(run_store)
    discovery_schedule = run_store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind="WORK_ITEM_DISCOVERY",
        project_id=project.project_id,
        forge_instance_id=project.forge_instance_id,
        target_kind="PROJECT",
        target_id=project.project_id,
        minimum_interval_ms=1,
        next_due_at_ms=0,
    )
    request = _due_request(run_store, project, discovery_schedule.forge_observation_schedule_id)
    assert request is not None
    with pytest.raises(ValueError):
        run_store.complete_forge_observation_request(
            forge_observation_request_id=request.forge_observation_request_id,
            observations=[],
        )


def test_unknown_request_id_raises(run_store: RunStore) -> None:
    with pytest.raises(RunStoreError):
        run_store.complete_forge_observation_request(
            forge_observation_request_id=_uid(), observations=[]
        )
