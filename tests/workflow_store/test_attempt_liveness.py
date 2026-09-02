"""Attempt Liveness: sequence validation, control delivery, ack (issue #683).

worker-protocol.md "Liveness, control, and deadlines": liveness has no
idempotency key, durable request row, or original-response replay -- every
call is freshly derived from current durable state.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.protocol import validate_envelope
from orcest.workflow_store import (
    AttemptOfferInput,
    CasMismatchError,
    RunStore,
    activity_offer_protocol,
)
from orcest.workflow_store.store import AttemptUnknownError

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
ACTIVITY_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"
WORKER_ID = "orcest-worker-1"
WORKER_SESSION_ID = "55555555-5555-4555-8555-555555555555"
CAPABILITY_DIGEST = "sha256:" + "3" * 64
POLICY_HASH = "sha256:" + "0" * 64
SEMANTIC_DIGEST = "sha256:" + "1" * 64
IDEMPOTENCY_KEY = "sha256:" + "2" * 64


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


def _claim_activity(store: RunStore, *, execution_deadline_ms: int) -> None:
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
            offered_at_ms=_now_ms(),
            claim_timeout_ms=300_000,
        ),
        outbox_id=OUTBOX_ID,
    )
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = ?, execution_deadline_ms = ?, "
            "attempt_capability_digest = ? WHERE attempt_id = ?",
            (
                WORKER_ID,
                WORKER_SESSION_ID,
                _now_ms(),
                execution_deadline_ms,
                CAPABILITY_DIGEST,
                ATTEMPT_ID,
            ),
        )
        store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (ACTIVITY_ID,)
        )


def _liveness(store: RunStore, **overrides):
    kwargs = dict(
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        generation=1,
        worker_id=WORKER_ID,
        worker_session_id=WORKER_SESSION_ID,
        attempt_capability_digest=CAPABILITY_DIGEST,
        sequence=1,
        observed_at_ms=_now_ms(),
        state="ACTIVE",
    )
    kwargs.update(overrides)
    return store.submit_attempt_liveness(**kwargs)


def test_liveness_continues_and_records_observed_state(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)

    result = _liveness(store)

    assert result.control == "CONTINUE"
    assert result.sequence_advanced is True
    assert result.liveness_recorded is False
    assert result.response_http_status == 202
    body = json.loads(result.response_json)
    validate_envelope(body)
    assert "replayed" not in body

    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.last_liveness_sequence == 1
    assert attempt.last_liveness_observed_ms is not None


def test_replayed_sequence_never_rewinds_state(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)
    _liveness(store, sequence=5, observed_at_ms=1_000)

    replay = _liveness(store, sequence=5, observed_at_ms=2_000)

    assert replay.control == "CONTINUE"
    assert replay.sequence_advanced is False
    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.last_liveness_sequence == 5
    assert attempt.last_liveness_observed_ms == 1_000


def test_skipped_sequence_is_accepted_and_advances(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)
    _liveness(store, sequence=1)

    skipped = _liveness(store, sequence=18)

    assert skipped.sequence_advanced is True
    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.last_liveness_sequence == 18


def test_lower_sequence_after_a_higher_one_does_not_rewind(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)
    _liveness(store, sequence=10)

    ambiguous_retry = _liveness(store, sequence=3)

    assert ambiguous_retry.control == "CONTINUE"
    assert ambiguous_retry.sequence_advanced is False
    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.last_liveness_sequence == 10


def test_missing_redis_liveness_storage_is_the_only_supported_path(store: RunStore) -> None:
    """This store has no Redis client: every accepted call is the wiki's
    documented "SQLite available, Redis liveness storage not" branch."""
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)

    result = _liveness(store)

    assert result.liveness_recorded is False
    assert result.response_http_status == 202


def test_wrong_worker_session_is_stale(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)

    with pytest.raises(CasMismatchError):
        _liveness(store, worker_session_id=_uid())


def test_wrong_generation_is_stale(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)

    with pytest.raises(CasMismatchError):
        _liveness(store, generation=2)


def test_unknown_attempt_raises(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)

    with pytest.raises(AttemptUnknownError):
        _liveness(store, attempt_id=_uid())


def test_liveness_at_or_after_execution_deadline_is_refused(store: RunStore) -> None:
    """Source access, upload, credential rotation, liveness, and launch all
    end at execution_deadline_ms (domain-model.md "Attempt") -- equality
    belongs to the deadline, not to this call arriving "first"."""
    _claim_activity(store, execution_deadline_ms=_now_ms())

    with pytest.raises(CasMismatchError):
        _liveness(store)


def test_cancellation_pending_run_delivers_cancel(store: RunStore) -> None:
    _claim_activity(store, execution_deadline_ms=_now_ms() + 3_600_000)
    from orcest.workflow_reducer.ledger import apply, load_view
    from orcest.workflow_reducer.types import Trigger

    with store.transaction():
        view = load_view(store, RUN_ID)
        assert view is not None
        apply(
            store,
            view,
            Trigger(kind="MANAGEMENT_COMMAND", trigger_id=_uid(), facts={"kind": "CANCEL"}),
            run_id=RUN_ID,
        )

    result = _liveness(store, sequence=2)

    assert result.control == "CANCEL"
