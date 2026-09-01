"""Worker Loss Report ledger and Attempt terminalization (issue #680)."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_store import (
    AttemptOfferInput,
    AttemptUnknownError,
    IdempotencyConflictError,
    RunStore,
    activity_offer_protocol,
)

pytestmark = pytest.mark.unit

POOL_MANAGER_ID = "pool-manager-1"
RUN_ID = "11111111-1111-4111-8111-111111111111"
ACTIVITY_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"
WORKER_ID = "orcest-worker-1"
WORKER_SESSION_ID = "55555555-5555-4555-8555-555555555555"
POLICY_HASH = "sha256:" + "0" * 64
SEMANTIC_DIGEST = "sha256:" + "1" * 64
IDEMPOTENCY_KEY = "sha256:" + "2" * 64
FUTURE_OFFERED_AT_MS = 4_102_444_800_000
OBSERVED_AT_MS = 1_700_000_000_000


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.create_run(
                run_id=RUN_ID,
                project_id="project-a",
                work_item_key="work-1",
                state="BUILDING",
                specification_generation=1,
            )
        yield store


def _claim_activity(store: RunStore, *, worker_session_id: str = WORKER_SESSION_ID) -> None:
    """Create a READY Activity + OFFERED Attempt, then simulate the (not yet
    implemented, #679) claim transition directly, matching the existing
    ``test_activity_attempts.py`` convention of raw SQL for state transitions
    not yet owned by a dedicated store method."""
    store.create_activity(
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
        attempt=AttemptOfferInput(
            attempt_id=ATTEMPT_ID,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile="codex",
            offered_at_ms=FUTURE_OFFERED_AT_MS,
            claim_timeout_ms=300_000,
        ),
        outbox_id=OUTBOX_ID,
    )
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = ? WHERE attempt_id = ?",
            (WORKER_ID, worker_session_id, _now_ms(), ATTEMPT_ID),
        )
        store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (ACTIVITY_ID,)
        )


def _submit(store: RunStore, **overrides):
    kwargs = dict(
        worker_loss_report_id=_uid(),
        pool_manager_id=POOL_MANAGER_ID,
        idempotency_key=_uid(),
        worker_id=WORKER_ID,
        worker_session_id=WORKER_SESSION_ID,
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        attempt_generation=1,
        reason="VM_DESTROYED",
        observed_at_ms=OBSERVED_AT_MS,
        authenticated_principal_id="pool-manager-principal",
        authorization_context_digest="sha256:" + "3" * 64,
    )
    kwargs.update(overrides)
    return store.submit_worker_loss_report(**kwargs)


def test_accepted_report_terminalizes_attempt_and_returns_activity_to_planned(
    store: RunStore,
) -> None:
    _claim_activity(store)

    result = _submit(store)

    assert result.outcome == "ACCEPTED"
    assert result.health_observation_id is not None
    assert result.attempt_terminal_fact_id is not None
    assert result.replayed is False

    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.state == "FAILED"
    assert attempt.terminal_reason == "WORKER_LOST"

    activity = store.get_activity(ACTIVITY_ID)
    assert activity.state == "PLANNED"

    observation = store.get_latest_health_observation("WORKER_SESSION", WORKER_SESSION_ID)
    assert observation is not None
    assert observation.kind == "LOST"
    assert observation.source_kind == "WORKER_LOSS_REPORT"
    assert observation.source_id == result.worker_loss_report_id

    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (RUN_ID,)).fetchone()
    assert run_row["state"] == "RECOVERING"


def test_mismatched_worker_session_yields_stale_and_does_not_terminalize(
    store: RunStore,
) -> None:
    _claim_activity(store)

    result = _submit(store, worker_session_id=_uid())

    assert result.outcome == "STALE"
    assert result.health_observation_id is None
    assert result.attempt_terminal_fact_id is None

    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.state == "CLAIMED"
    assert attempt.terminal_reason is None

    activity = store.get_activity(ACTIVITY_ID)
    assert activity.state == "ACTIVE"


def test_already_terminal_attempt_yields_stale(store: RunStore) -> None:
    _claim_activity(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'FAILED', terminal_reason = 'INFRASTRUCTURE' "
            "WHERE attempt_id = ?",
            (ATTEMPT_ID,),
        )

    result = _submit(store)

    assert result.outcome == "STALE"
    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.terminal_reason == "INFRASTRUCTURE"


def test_unknown_attempt_triple_raises_and_creates_no_row(store: RunStore) -> None:
    with pytest.raises(AttemptUnknownError):
        _submit(store, attempt_id=_uid())
    assert store.conn.execute("SELECT COUNT(*) FROM worker_loss_reports").fetchone()[0] == 0


def test_wrong_generation_is_unknown_not_stale(store: RunStore) -> None:
    _claim_activity(store)
    with pytest.raises(AttemptUnknownError):
        _submit(store, attempt_generation=2)


def test_replaying_same_idempotency_key_returns_stored_response(store: RunStore) -> None:
    _claim_activity(store)
    idempotency_key = _uid()
    first = _submit(store, idempotency_key=idempotency_key)
    second = _submit(store, idempotency_key=idempotency_key)

    assert second.replayed is True
    assert second.outcome == first.outcome
    assert second.attempt_terminal_fact_id == first.attempt_terminal_fact_id
    assert store.conn.execute("SELECT COUNT(*) FROM worker_loss_reports").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_terminal_facts").fetchone()[0] == 1


def test_reusing_idempotency_key_with_different_body_conflicts(store: RunStore) -> None:
    _claim_activity(store)
    idempotency_key = _uid()
    _submit(store, idempotency_key=idempotency_key)
    with pytest.raises(IdempotencyConflictError):
        _submit(store, idempotency_key=idempotency_key, reason="VM_MISSING")


def test_replay_after_acceptance_does_not_reterminalize(store: RunStore) -> None:
    _claim_activity(store)
    idempotency_key = _uid()
    _submit(store, idempotency_key=idempotency_key)
    _submit(store, idempotency_key=idempotency_key)
    assert store.conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 1
