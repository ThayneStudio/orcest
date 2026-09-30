"""Durable source-unique Attempt Claim schema.

Population of this table (capability issuance, launch nonce allocation,
source-access materialization) is the fenced worker claim leaf's job; this
issue owns only the durable, source-unique persistence shape so that leaf
can write into it. These tests exercise the schema's own constraints
directly against the DB, the way that leaf's writer eventually will.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from orcest.workflow_store import AttemptOfferInput, RunStore, activity_offer_protocol

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
ACTIVITY_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"


@pytest.fixture
def store_with_offered_attempt(tmp_path: Path):
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.create_run(
                run_id=RUN_ID,
                project_id="project-a",
                work_item_key="work-1",
                state="ADMITTED",
                specification_generation=1,
            )
        _activity, attempt, outbox = store.create_activity(
            activity_id=ACTIVITY_ID,
            run_id=RUN_ID,
            activity_ordinal=1,
            specification_generation=1,
            policy_hash="sha256:" + "0" * 64,
            kind="BUILD",
            execution_class="WORKER",
            state="READY",
            created_transition_sequence=1,
            semantic_input={},
            semantic_input_digest="sha256:" + "1" * 64,
            idempotency_key="sha256:" + "2" * 64,
            attempt=AttemptOfferInput(
                attempt_id=ATTEMPT_ID,
                generation=1,
                protocol_version=activity_offer_protocol(),
                worker_profile="codex",
                offered_at_ms=1_000,
                claim_timeout_ms=300_000,
            ),
            outbox_id=OUTBOX_ID,
        )
        yield store, attempt, outbox


def _claim_row(*, attempt_claim_id: str, attempt_id: str, worker_id: str, worker_session_id: str):
    now = int(time.time() * 1000)
    return (
        attempt_claim_id,
        "orcest.attempt-claim/1",
        attempt_id,
        ACTIVITY_ID,
        1,
        OUTBOX_ID,
        worker_id,
        worker_session_id,
        "codex",
        "git-sha",
        "sha256:" + "d" * 64,
        now,
        now + 1_000,
        now + 2_000,
        f"jti-{worker_session_id}",
        "sha256:" + "e" * 64,
        "signing-key-1",
        "ED25519",
        1,
        "SCOPED_CREDENTIAL",
        "{}",
        "sha256:" + "f" * 64,
        "sha256:" + "a" * 64,
        now,
    )


_INSERT_CLAIM_SQL = (
    "INSERT INTO attempt_claims(attempt_claim_id, protocol_version, attempt_id, activity_id, "
    "attempt_generation, offer_outbox_id, worker_id, worker_session_id, worker_profile, "
    "worker_build_revision, request_digest, claimed_at_ms, execution_deadline_ms, "
    "capability_auth_expires_at_ms, attempt_capability_jti, attempt_capability_digest, "
    "attempt_capability_signing_key_id, attempt_capability_signature_algorithm, "
    "capability_key_registry_revision, source_access_kind, source_access_descriptor_json, "
    "source_access_descriptor_digest, response_contract_digest, created_at_ms) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)


def test_attempt_claim_persists_and_is_retrievable_by_attempt(store_with_offered_attempt) -> None:
    store, attempt, _outbox = store_with_offered_attempt
    with store.transaction():
        store.conn.execute(
            _INSERT_CLAIM_SQL,
            _claim_row(
                attempt_claim_id="c1111111-1111-4111-8111-111111111111",
                attempt_id=attempt.attempt_id,
                worker_id="worker-1",
                worker_session_id="session-1",
            ),
        )
    row = store.conn.execute(
        "SELECT * FROM attempt_claims WHERE attempt_id = ?", (attempt.attempt_id,)
    ).fetchone()
    assert row["attempt_claim_id"] == "c1111111-1111-4111-8111-111111111111"
    assert row["worker_session_id"] == "session-1"


def test_attempt_claim_is_source_unique_on_attempt_id(store_with_offered_attempt) -> None:
    store, attempt, _outbox = store_with_offered_attempt
    with store.transaction():
        store.conn.execute(
            _INSERT_CLAIM_SQL,
            _claim_row(
                attempt_claim_id="c1111111-1111-4111-8111-111111111111",
                attempt_id=attempt.attempt_id,
                worker_id="worker-1",
                worker_session_id="session-1",
            ),
        )
    with pytest.raises(sqlite3.IntegrityError):
        with store.transaction():
            store.conn.execute(
                _INSERT_CLAIM_SQL,
                _claim_row(
                    attempt_claim_id="c2222222-2222-4222-8222-222222222222",
                    attempt_id=attempt.attempt_id,
                    worker_id="worker-2",
                    worker_session_id="session-2",
                ),
            )


def test_attempt_claim_fences_to_the_offer_it_claims(store_with_offered_attempt) -> None:
    store, attempt, outbox = store_with_offered_attempt
    with store.transaction():
        store.conn.execute(
            _INSERT_CLAIM_SQL,
            _claim_row(
                attempt_claim_id="c1111111-1111-4111-8111-111111111111",
                attempt_id=attempt.attempt_id,
                worker_id="worker-1",
                worker_session_id="session-1",
            ),
        )
    row = store.conn.execute(
        "SELECT offer_outbox_id, activity_id, attempt_generation FROM attempt_claims "
        "WHERE attempt_id = ?",
        (attempt.attempt_id,),
    ).fetchone()
    assert row["offer_outbox_id"] == outbox.outbox_id
    assert row["activity_id"] == ACTIVITY_ID
    assert row["attempt_generation"] == 1
