from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from orcest.workflow_store import (
    ActivityReviewAssignmentInput,
    AttemptOfferInput,
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
    activity_offer_protocol,
)

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
ACTIVITY_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"
POLICY_HASH = "sha256:" + "0" * 64
SEMANTIC_DIGEST = "sha256:" + "1" * 64
IDEMPOTENCY_KEY = "sha256:" + "2" * 64


def _offer(
    *, generation: int = 1, attempt_id: str = ATTEMPT_ID, worker_profile: str = "codex"
) -> AttemptOfferInput:
    return AttemptOfferInput(
        attempt_id=attempt_id,
        generation=generation,
        protocol_version=activity_offer_protocol(),
        worker_profile=worker_profile,
        offered_at_ms=1_000,
        claim_timeout_ms=300_000,
    )


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.create_run(
                run_id=RUN_ID,
                project_id="project-a",
                work_item_key="work-1",
                state="ADMITTED",
                specification_generation=1,
            )
        yield store


def _create(store: RunStore, **overrides):
    kwargs = dict(
        activity_id=ACTIVITY_ID,
        run_id=RUN_ID,
        activity_ordinal=1,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="BUILD",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={"a": 1},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key=IDEMPOTENCY_KEY,
        attempt=_offer(),
        outbox_id=OUTBOX_ID,
    )
    kwargs.update(overrides)
    return store.create_activity(**kwargs)


def test_create_activity_commits_activity_attempt_and_outbox_atomically(store: RunStore) -> None:
    activity, attempt, outbox = _create(store)

    assert activity.activity_id == ACTIVITY_ID
    assert activity.state == "READY"
    assert attempt is not None
    assert attempt.state == "OFFERED"
    assert attempt.claim_deadline_ms == attempt.offered_at_ms + attempt.claim_timeout_ms
    assert outbox is not None
    assert outbox.source_kind == "ACTIVITY"
    assert outbox.attempt_id == attempt.attempt_id
    assert outbox.attempt_generation == 1
    assert outbox.state == "PENDING"

    assert store.get_activity(ACTIVITY_ID) is not None
    assert store.get_attempt(attempt.attempt_id).state == "OFFERED"
    open_offers = store.list_open_activity_offers()
    assert len(open_offers) == 1
    assert open_offers[0][0].attempt_id == attempt.attempt_id
    assert open_offers[0][1].outbox_id == outbox.outbox_id


def test_offer_outbox_payload_contains_no_secret_fields(store: RunStore) -> None:
    import json

    _activity, _attempt, outbox = _create(store)
    payload = json.loads(outbox.payload_json)
    assert set(payload) == {
        "outbox_id",
        "attempt_id",
        "activity_id",
        "generation",
        "worker_profile",
        "claim_deadline_ms",
    }


def test_replaying_same_idempotency_key_returns_existing_activity_without_new_outbox(
    store: RunStore,
) -> None:
    activity1, attempt1, outbox1 = _create(store)
    activity2, attempt2, outbox2 = _create(store)

    assert activity2.activity_id == activity1.activity_id
    assert attempt2.attempt_id == attempt1.attempt_id
    assert outbox2 is None
    assert store.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM activities").fetchone()[0] == 1


def test_replaying_idempotency_key_with_different_content_conflicts(store: RunStore) -> None:
    _create(store)
    with pytest.raises(IdempotencyConflictError):
        _create(store, kind="VERIFY")


@pytest.mark.parametrize(
    "overrides",
    [
        {"policy_hash": "sha256:" + "9" * 64},
        {"activity_ordinal": 2},
        {"specification_generation": 2},
        {"role": "reviewer"},
        {"slot": "slot-a"},
    ],
)
def test_replaying_idempotency_key_with_different_activity_field_conflicts(
    store: RunStore, overrides: dict
) -> None:
    _create(store)
    with pytest.raises(IdempotencyConflictError):
        _create(store, **overrides)


def test_replaying_idempotency_key_with_different_attempt_offer_conflicts(store: RunStore) -> None:
    _create(store)
    with pytest.raises(IdempotencyConflictError):
        _create(store, attempt=_offer(worker_profile="grok"))


def test_replaying_idempotency_key_with_attempt_added_conflicts(store: RunStore) -> None:
    kwargs = dict(
        activity_id=ACTIVITY_ID,
        run_id=RUN_ID,
        activity_ordinal=1,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="BUILD",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={"a": 1},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key=IDEMPOTENCY_KEY,
    )
    store.create_activity(**kwargs)
    with pytest.raises(IdempotencyConflictError):
        store.create_activity(**kwargs, attempt=_offer(), outbox_id=OUTBOX_ID)


def test_replaying_idempotency_key_with_different_review_assignment_conflicts(
    store: RunStore,
) -> None:
    review = ActivityReviewAssignmentInput(
        assignment_kind="REVIEW",
        panel_round=1,
        role="reviewer",
        context_digest="sha256:" + "3" * 64,
        subject_refs=("snapshot:overall",),
        reviewer_slot="slot-a",
    )
    _create(store, kind="REVIEW", role="reviewer", review_assignment=review)
    other_review = ActivityReviewAssignmentInput(
        assignment_kind="REVIEW",
        panel_round=2,
        role="reviewer",
        context_digest="sha256:" + "3" * 64,
        subject_refs=("snapshot:overall",),
        reviewer_slot="slot-a",
    )
    with pytest.raises(IdempotencyConflictError):
        _create(store, kind="REVIEW", role="reviewer", review_assignment=other_review)


def test_create_activity_requires_outbox_id_when_attempt_given(store: RunStore) -> None:
    with pytest.raises(ValueError):
        _create(store, outbox_id=None)


def test_activity_ordinal_and_idempotency_key_are_unique_per_run(store: RunStore) -> None:
    _create(store)
    with pytest.raises(sqlite3.IntegrityError):
        with store.transaction():
            store.conn.execute(
                "INSERT INTO activities(activity_id, run_id, activity_ordinal, "
                "specification_generation, policy_hash, kind, execution_class, state, "
                "input_ref_json, candidate_id, forge_observation_id, "
                "change_request_head_observation_id, observed_change_request_head_json, role, "
                "repair_cycle, recovery_cycle, strategy_index, recovery_tactic, "
                "recovery_evidence_id, rescue_epoch, created_transition_sequence, "
                "semantic_input_json, semantic_input_digest, idempotency_key, slot, "
                "created_at_ms, updated_at_ms) "
                "VALUES ('55555555-5555-4555-8555-555555555555', ?, 1, 1, ?, 'BUILD', 'WORKER', "
                "'PLANNED', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, NULL, NULL, 0, 1, '{}', "
                "?, ?, NULL, 0, 0)",
                (RUN_ID, POLICY_HASH, SEMANTIC_DIGEST, IDEMPOTENCY_KEY),
            )


def test_only_one_nonterminal_attempt_per_activity(store: RunStore) -> None:
    _create(store)
    with pytest.raises(sqlite3.IntegrityError):
        with store.transaction():
            store.conn.execute(
                "INSERT INTO attempts(attempt_id, activity_id, generation, state, "
                "protocol_version, worker_profile, offered_at_ms, claim_timeout_ms, "
                "claim_deadline_ms, created_at_ms) VALUES "
                "('66666666-6666-4666-8666-666666666666', ?, 2, 'OFFERED', ?, 'codex', "
                "2000, 300000, 302000, 2000)",
                (ACTIVITY_ID, activity_offer_protocol()),
            )


def test_create_next_attempt_requires_prior_generation_terminal(store: RunStore) -> None:
    _create(store)
    with pytest.raises(CasMismatchError):
        store.create_next_attempt(
            activity_id=ACTIVITY_ID,
            prior_attempt_terminal_state="FAILED",
            offer=_offer(generation=2, attempt_id="77777777-7777-4777-8777-777777777777"),
            outbox_id="88888888-8888-4888-8888-888888888888",
        )


def test_create_next_attempt_requires_terminal_state_to_match_actual_state(
    store: RunStore,
) -> None:
    _activity, attempt1, _outbox1 = _create(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'EXPIRED' WHERE attempt_id = ?", (attempt1.attempt_id,)
        )
    with pytest.raises(CasMismatchError):
        store.create_next_attempt(
            activity_id=ACTIVITY_ID,
            prior_attempt_terminal_state="FAILED",
            offer=_offer(generation=2, attempt_id="99999999-9999-4999-8999-999999999999"),
            outbox_id="88888888-8888-4888-8888-888888888888",
        )


def test_create_next_attempt_offers_the_successor_generation(store: RunStore) -> None:
    _activity, attempt1, _outbox1 = _create(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'FAILED' WHERE attempt_id = ?", (attempt1.attempt_id,)
        )
    attempt2, outbox2 = store.create_next_attempt(
        activity_id=ACTIVITY_ID,
        prior_attempt_terminal_state="FAILED",
        offer=_offer(generation=2, attempt_id="99999999-9999-4999-8999-999999999999"),
        outbox_id="88888888-8888-4888-8888-888888888888",
    )
    assert attempt2.generation == 2
    assert attempt2.state == "OFFERED"
    assert outbox2.attempt_generation == 2
    open_offers = store.list_open_activity_offers()
    assert len(open_offers) == 1
    assert open_offers[0][0].attempt_id == attempt2.attempt_id


def test_review_assignment_and_ordered_subjects_persist_and_reproduce_digest(
    store: RunStore,
) -> None:
    review = ActivityReviewAssignmentInput(
        assignment_kind="REVIEW",
        panel_round=1,
        role="reviewer",
        context_digest="sha256:" + "3" * 64,
        subject_refs=("snapshot:overall", "plan:requirement:r1", "plan:requirement:r2"),
        reviewer_slot="slot-a",
    )
    activity, _attempt, _outbox = _create(
        store, kind="REVIEW", role="reviewer", review_assignment=review
    )

    assert activity.review_assignment is not None
    assert activity.review_assignment.subject_refs == (
        "snapshot:overall",
        "plan:requirement:r1",
        "plan:requirement:r2",
    )

    reloaded = store.get_activity(ACTIVITY_ID)
    assert (
        reloaded.review_assignment.assignment_digest == activity.review_assignment.assignment_digest
    )
    assert reloaded.review_assignment.subject_refs == activity.review_assignment.subject_refs


def test_adjudicate_assignment_requires_disputed_findings(store: RunStore) -> None:
    review = ActivityReviewAssignmentInput(
        assignment_kind="ADJUDICATE",
        panel_round=1,
        role="adjudicator",
        context_digest="sha256:" + "3" * 64,
        subject_refs=("snapshot:overall",),
        adjudication_round=1,
        adjudicator_slot="default",
        disputed_finding_ids=("finding-a", "finding-b"),
    )
    activity, _attempt, _outbox = _create(
        store, kind="ADJUDICATE", role="adjudicator", review_assignment=review
    )
    assert activity.review_assignment.disputed_finding_ids == ("finding-a", "finding-b")

    with pytest.raises(sqlite3.IntegrityError):
        with store.transaction():
            store.conn.execute(
                "INSERT INTO activity_review_assignments(activity_id, assignment_kind, "
                "panel_round, reviewer_slot, adjudication_round, adjudicator_slot, role, "
                "subject_refs_digest, context_digest, disputed_finding_ids_digest, "
                "assignment_digest, created_at_ms) VALUES "
                "('99999999-0000-4000-8000-000000000000', 'ADJUDICATE', 1, NULL, 1, 'default', "
                "'adjudicator', 'sha256:' || printf('%064d', 0), 'sha256:' || printf('%064d', 0), "
                "NULL, 'sha256:' || printf('%064d', 0), 0)"
            )
