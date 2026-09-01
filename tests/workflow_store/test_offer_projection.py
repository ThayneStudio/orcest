"""Redis activity-offer dispatch and post-flush reconstruction."""

from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_contract.v1.protocol import get_envelope_schema, validate_object
from orcest.workflow_store import (
    AttemptOfferInput,
    RunStore,
    activity_offer_protocol,
    dispatch_pending_offers,
    offer_stream_key,
    reconstruct_open_offers,
)

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"


def _offer(
    *, generation: int = 1, attempt_id: str, worker_profile: str = "codex"
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


def _create_offer(
    store: RunStore, *, activity_id: str, attempt_id: str, outbox_id: str, activity_ordinal: int = 1
):
    return store.create_activity(
        activity_id=activity_id,
        run_id=RUN_ID,
        activity_ordinal=activity_ordinal,
        specification_generation=1,
        policy_hash="sha256:" + "0" * 64,
        kind="BUILD",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={},
        semantic_input_digest="sha256:" + "1" * 64,
        idempotency_key="sha256:" + activity_id.replace("-", "")[:64].ljust(64, "0"),
        attempt=_offer(attempt_id=attempt_id),
        outbox_id=outbox_id,
    )


def test_dispatch_publishes_a_valid_envelope_and_marks_delivered(store, fake_redis_client) -> None:
    _activity, attempt, outbox = _create_offer(
        store,
        activity_id="22222222-2222-4222-8222-222222222222",
        attempt_id="33333333-3333-4333-8333-333333333333",
        outbox_id="44444444-4444-4444-8444-444444444444",
    )

    dispatched = dispatch_pending_offers(store, fake_redis_client, redis_epoch=1)
    assert dispatched == 1

    entries = fake_redis_client.xrange(offer_stream_key("codex"), count=10)
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert fields["protocol"] == activity_offer_protocol()
    assert fields["protocol_version"] == "1"
    assert fields["redis_epoch"] == "1"
    assert fields["attempt_id"] == attempt.attempt_id
    assert fields["activity_id"] == "22222222-2222-4222-8222-222222222222"
    assert fields["generation"] == "1"
    assert fields["worker_profile"] == "codex"
    assert fields["claim_deadline_ms"] == str(attempt.claim_deadline_ms)

    # The module validates the typed envelope (real ints, not wire strings)
    # against the registered schema before every publish -- reproduce that
    # here from the same wire fields to prove the published entry conforms.
    schema = get_envelope_schema(activity_offer_protocol())
    typed = dict(fields)
    for int_field in ("redis_epoch", "generation", "claim_deadline_ms"):
        typed[int_field] = int(typed[int_field])
    validate_object(typed, schema.schema, path=activity_offer_protocol())

    assert store.get_outbox(outbox.outbox_id).state == "DELIVERED"


def test_offer_envelope_never_carries_a_secret_or_prompt_field(store, fake_redis_client) -> None:
    _create_offer(
        store,
        activity_id="22222222-2222-4222-8222-222222222222",
        attempt_id="33333333-3333-4333-8333-333333333333",
        outbox_id="44444444-4444-4444-8444-444444444444",
    )
    dispatch_pending_offers(store, fake_redis_client, redis_epoch=1)
    _entry_id, fields = fake_redis_client.xrange(offer_stream_key("codex"), count=10)[0]
    forbidden = {
        "prompt",
        "issue_body",
        "provider_credential",
        "repository_token",
        "attempt_capability",
        "candidate_download_url",
        "secret",
    }
    assert forbidden.isdisjoint(fields)


def test_dispatch_only_publishes_pending_rows_once(store, fake_redis_client) -> None:
    _create_offer(
        store,
        activity_id="22222222-2222-4222-8222-222222222222",
        attempt_id="33333333-3333-4333-8333-333333333333",
        outbox_id="44444444-4444-4444-8444-444444444444",
    )
    first = dispatch_pending_offers(store, fake_redis_client, redis_epoch=1)
    second = dispatch_pending_offers(store, fake_redis_client, redis_epoch=1)
    assert first == 1
    assert second == 0
    assert len(fake_redis_client.xrange(offer_stream_key("codex"), count=10)) == 1


def test_reconstruction_republishes_only_current_offered_attempts(store, fake_redis_client) -> None:
    _activity_a, attempt_a, _outbox_a = _create_offer(
        store,
        activity_id="22222222-2222-4222-8222-222222222222",
        attempt_id="33333333-3333-4333-8333-333333333333",
        outbox_id="44444444-4444-4444-8444-444444444444",
    )
    _activity_b, attempt_b, _outbox_b = _create_offer(
        store,
        activity_id="55555555-5555-4555-8555-555555555555",
        attempt_id="66666666-6666-4666-8666-666666666666",
        outbox_id="77777777-7777-4777-8777-777777777777",
        activity_ordinal=2,
    )
    dispatch_pending_offers(store, fake_redis_client, redis_epoch=1)

    # Claim one Attempt -- it must never be republished as schedulable work.
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED' WHERE attempt_id = ?", (attempt_b.attempt_id,)
        )

    fake_redis_client.client.flushall()
    republished = reconstruct_open_offers(store, fake_redis_client, redis_epoch=2)
    assert republished == 1

    entries = fake_redis_client.xrange(offer_stream_key("codex"), count=10)
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert fields["attempt_id"] == attempt_a.attempt_id
    assert fields["redis_epoch"] == "2"


def test_reconstruction_is_reachable_from_a_fresh_process_boundary(
    store, fake_redis_client
) -> None:
    """Reconstruction reads only durable SQLite state -- never Redis's prior contents."""
    _create_offer(
        store,
        activity_id="22222222-2222-4222-8222-222222222222",
        attempt_id="33333333-3333-4333-8333-333333333333",
        outbox_id="44444444-4444-4444-8444-444444444444",
    )
    # No prior dispatch ever ran: the PENDING outbox row alone still counts as
    # a durable open offer that reconstruction must (re)publish.
    republished = reconstruct_open_offers(store, fake_redis_client, redis_epoch=1)
    assert republished == 1
    assert store.get_outbox("44444444-4444-4444-8444-444444444444").state == "DELIVERED"
