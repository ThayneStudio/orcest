"""Timer Facts, the claim-deadline capacity classifier, and Attempt
terminalization by deadline (issue #683).

domain-model.md "Timer Fact" / "Attempt Terminal Fact": a Timer Fact is
scope/deadline-unique evidence that the controller evaluated one durable
deadline; it never itself terminalizes an Attempt. Only the classified
Attempt Terminal Fact does, reusing the same idempotent
``T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`` reduction #680/#682 already
established for ``WORKER_LOST``/``EXECUTION_DEADLINE``.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest
from orcest.workflow_store import (
    ActivityReviewAssignmentInput,
    AttemptOfferInput,
    AttemptUnknownError,
    CapacityReportEntryInput,
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
AUTHZ_DIGEST = "sha256:" + "a" * 64


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


def _offer_expired_claim_window(store: RunStore, *, worker_profile: str = "codex") -> None:
    """A ``READY`` Activity whose ``OFFERED`` Attempt's claim window already passed."""
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
            worker_profile=worker_profile,
            offered_at_ms=_now_ms() - 400_000,
            claim_timeout_ms=300_000,
        ),
        outbox_id=OUTBOX_ID,
    )


def _activate_controller(store: RunStore) -> None:
    key_id = _uid()
    store.apply_capability_key_operation(
        capability_key_operation_id=_uid(),
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        register_public_verification_key=bytes([1]) * 32,
        register_public_key_digest=capability_public_key_digest(bytes([1]) * 32),
        register_private_signing_secret_ref="bootstrap:0",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    store.apply_capability_key_operation(
        capability_key_operation_id=_uid(),
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="SET_MODE",
        expected_mode_revision=1,
        expected_mode="MAINTENANCE",
        requested_mode="RUNNING",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )


def _report_capacity(store: RunStore, *, worker_profile: str = "codex") -> None:
    store.submit_capacity_report(
        capacity_report_id=_uid(),
        pool_manager_id="pool-manager-1",
        report_id=_uid(),
        idempotency_key=_uid(),
        report_sequence=1,
        observed_at_ms=_now_ms(),
        expires_at_ms=_now_ms() + 600_000,
        configured_max_ttl_ms=900_000,
        entries=[
            CapacityReportEntryInput(
                scope_kind="WORKER_PROFILE",
                scope_id=worker_profile,
                capacity_pool_id="default",
                worker_profile=worker_profile,
                available_slots=2,
            ),
        ],
        authenticated_principal_id="pool-manager-principal",
        authorization_context_digest=AUTHZ_DIGEST,
    )


# -- Timer Fact -------------------------------------------------------------


def test_timer_fact_is_scope_deadline_unique(store: RunStore) -> None:
    first = store.record_timer_fact(
        scope_kind="ATTEMPT_CLAIM_DEADLINE",
        scope_id=ATTEMPT_ID,
        fired_for_ms=1_000,
        source_kind="SCHEDULED_SWEEP",
        source_id=_uid(),
        now_ms=1_000,
    )
    second = store.record_timer_fact(
        scope_kind="ATTEMPT_CLAIM_DEADLINE",
        scope_id=ATTEMPT_ID,
        fired_for_ms=1_000,
        source_kind="SCHEDULED_SWEEP",
        source_id=_uid(),
        now_ms=2_000,
    )
    assert second.timer_fact_id == first.timer_fact_id

    count = store.conn.execute("SELECT COUNT(*) c FROM timer_facts").fetchone()["c"]
    assert count == 1


def test_timer_fact_rejects_a_deadline_that_has_not_occurred(store: RunStore) -> None:
    with pytest.raises(ValueError):
        store.record_timer_fact(
            scope_kind="ATTEMPT_CLAIM_DEADLINE",
            scope_id=ATTEMPT_ID,
            fired_for_ms=5_000,
            source_kind="SCHEDULED_SWEEP",
            source_id=_uid(),
            now_ms=1_000,
        )


# -- Claim-deadline capacity classifier --------------------------------------


def test_claim_deadline_expires_offered_attempt_and_replans_activity(store: RunStore) -> None:
    _offer_expired_claim_window(store)

    result = store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)

    assert result.outcome == "EXPIRED"
    assert result.attempt_terminal_fact_id is not None

    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.state == "EXPIRED"
    assert attempt.terminal_reason == "CLAIM_DEADLINE"

    activity = store.get_activity(ACTIVITY_ID)
    assert activity.state == "PLANNED"

    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (RUN_ID,)).fetchone()
    assert run_row["state"] == "RECOVERING"


def test_claim_deadline_never_creates_an_offer_when_mode_gate_fails(store: RunStore) -> None:
    """Claim deadline never creates an offer when mode/key/capacity gates
    fail (acceptance criterion): a fresh controller starts MAINTENANCE-blocked."""
    _offer_expired_claim_window(store)

    result = store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)

    assert result.replacement_offer_disposition == "MODE_BLOCKED"
    fact = store.conn.execute(
        "SELECT * FROM attempt_terminal_facts WHERE attempt_terminal_fact_id = ?",
        (result.attempt_terminal_fact_id,),
    ).fetchone()
    assert fact["replacement_offer_disposition"] == "MODE_BLOCKED"
    assert fact["capacity_disposition"] == "NO_COMPATIBLE_AVAILABLE"
    assert fact["controller_mode"] is None or fact["controller_mode"] == "MAINTENANCE"


def test_claim_deadline_never_creates_an_offer_when_no_issuance_key(store: RunStore) -> None:
    store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="SET_MODE",
        expected_mode_revision=1,
        expected_mode="MAINTENANCE",
        requested_mode="RUNNING",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    _offer_expired_claim_window(store)

    result = store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)

    assert result.replacement_offer_disposition == "ISSUANCE_KEY_UNAVAILABLE"


def test_claim_deadline_allows_offer_and_records_compatible_capacity(store: RunStore) -> None:
    _activate_controller(store)
    _report_capacity(store)
    _offer_expired_claim_window(store)

    result = store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)

    assert result.replacement_offer_disposition == "OFFER_ALLOWED"
    assert result.capacity_disposition == "COMPATIBLE_AVAILABLE"

    membership = store.conn.execute(
        "SELECT * FROM attempt_terminal_fact_health_observations "
        "WHERE attempt_terminal_fact_id = ? ORDER BY observation_ordinal",
        (result.attempt_terminal_fact_id,),
    ).fetchall()
    assert len(membership) == 1

    fact = store.conn.execute(
        "SELECT * FROM attempt_terminal_facts WHERE attempt_terminal_fact_id = ?",
        (result.attempt_terminal_fact_id,),
    ).fetchone()
    assert fact["selected_issuance_key_id"] is not None
    assert fact["controller_mode"] == "RUNNING"
    assert fact["health_observation_ids_digest"] is not None


def test_claim_deadline_replay_is_idempotent_no_duplicate_fact(store: RunStore) -> None:
    _offer_expired_claim_window(store)

    first = store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)
    second = store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)

    assert second.attempt_terminal_fact_id == first.attempt_terminal_fact_id
    fact_count = store.conn.execute("SELECT COUNT(*) c FROM attempt_terminal_facts").fetchone()["c"]
    timer_count = store.conn.execute("SELECT COUNT(*) c FROM timer_facts").fetchone()["c"]
    assert fact_count == 1
    assert timer_count == 1


def test_claim_deadline_is_stale_once_the_attempt_was_already_claimed(store: RunStore) -> None:
    """Equality belongs to deadline expiry, never first Result acceptance:
    once a claim has actually landed, a late sweep pass over the same
    deadline is bounded stale-evidence audit input, not a second Fact."""
    _offer_expired_claim_window(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = ?, execution_deadline_ms = ? "
            "WHERE attempt_id = ?",
            ("worker-1", _uid(), _now_ms(), _now_ms() + 3_600_000, ATTEMPT_ID),
        )
        store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (ACTIVITY_ID,)
        )

    result = store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)

    assert result.outcome == "STALE"
    assert result.attempt_terminal_fact_id is None
    fact_count = store.conn.execute("SELECT COUNT(*) c FROM attempt_terminal_facts").fetchone()["c"]
    assert fact_count == 0
    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.state == "CLAIMED"


def test_claim_deadline_unknown_attempt_raises(store: RunStore) -> None:
    with pytest.raises(AttemptUnknownError):
        store.expire_attempt_claim_deadline(attempt_id=_uid())


def test_claim_deadline_rejects_a_deadline_that_has_not_occurred(store: RunStore) -> None:
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
            claim_timeout_ms=3_600_000,
        ),
        outbox_id=OUTBOX_ID,
    )
    with pytest.raises(ValueError):
        store.expire_attempt_claim_deadline(attempt_id=ATTEMPT_ID)


# -- Execution-deadline sweep -------------------------------------------------


def test_execution_deadline_expires_claimed_attempt_and_replans_activity(store: RunStore) -> None:
    _offer_expired_claim_window(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = ?, execution_deadline_ms = ? "
            "WHERE attempt_id = ?",
            ("worker-1", _uid(), _now_ms() - 500_000, _now_ms() - 1_000, ATTEMPT_ID),
        )
        store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (ACTIVITY_ID,)
        )

    result = store.expire_attempt_execution_deadline(attempt_id=ATTEMPT_ID)

    assert result.outcome == "EXPIRED"
    assert result.capacity_disposition is None
    assert result.replacement_offer_disposition is None

    attempt = store.get_attempt(ATTEMPT_ID)
    assert attempt.state == "EXPIRED"
    assert attempt.terminal_reason == "EXECUTION_DEADLINE"
    activity = store.get_activity(ACTIVITY_ID)
    assert activity.state == "PLANNED"


def test_execution_deadline_replay_is_idempotent(store: RunStore) -> None:
    _offer_expired_claim_window(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = ?, execution_deadline_ms = ? "
            "WHERE attempt_id = ?",
            ("worker-1", _uid(), _now_ms() - 500_000, _now_ms() - 1_000, ATTEMPT_ID),
        )
        store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (ACTIVITY_ID,)
        )

    first = store.expire_attempt_execution_deadline(attempt_id=ATTEMPT_ID)
    second = store.expire_attempt_execution_deadline(attempt_id=ATTEMPT_ID)

    assert second.attempt_terminal_fact_id == first.attempt_terminal_fact_id


def test_execution_deadline_never_fires_for_a_not_yet_claimed_attempt(store: RunStore) -> None:
    _offer_expired_claim_window(store)
    with pytest.raises(ValueError):
        store.expire_attempt_execution_deadline(attempt_id=ATTEMPT_ID)


# -- Panel claim-deadline coalescing ------------------------------------------


def _create_review_slot(
    store: RunStore, *, activity_id: str, attempt_id: str, outbox_id: str, slot: str
) -> None:
    assignment = ActivityReviewAssignmentInput(
        assignment_kind="REVIEW",
        panel_round=1,
        role="reviewer",
        context_digest="sha256:" + "4" * 64,
        subject_refs=("subject-1",),
        reviewer_slot=slot,
    )
    store.create_activity(
        activity_id=activity_id,
        run_id=RUN_ID,
        activity_ordinal=1 if slot == "slot-a" else 2,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="REVIEW",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={"slot": slot},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key="sha256:" + ("5" if slot == "slot-a" else "6") * 64,
        slot=slot,
        role="reviewer",
        review_assignment=assignment,
        attempt=AttemptOfferInput(
            attempt_id=attempt_id,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile="codex",
            offered_at_ms=_now_ms() - 400_000,
            claim_timeout_ms=300_000,
        ),
        outbox_id=outbox_id,
    )


def test_panel_claim_deadline_coalesces_when_a_peer_is_still_claimed(store: RunStore) -> None:
    """Peer panel expiry cannot create partial staffing or duplicate
    continuations (acceptance criterion): with slot-b still claimed, the
    Run stays REVIEWING and only the coalesced staffing pointer moves."""
    with store.transaction():
        store.conn.execute("UPDATE runs SET state = 'REVIEWING' WHERE run_id = ?", (RUN_ID,))
    slot_a_activity, slot_a_attempt, slot_a_outbox = _uid(), _uid(), _uid()
    slot_b_activity, slot_b_attempt, slot_b_outbox = _uid(), _uid(), _uid()
    _create_review_slot(
        store,
        activity_id=slot_a_activity,
        attempt_id=slot_a_attempt,
        outbox_id=slot_a_outbox,
        slot="slot-a",
    )
    _create_review_slot(
        store,
        activity_id=slot_b_activity,
        attempt_id=slot_b_attempt,
        outbox_id=slot_b_outbox,
        slot="slot-b",
    )
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = ? WHERE attempt_id = ?",
            ("reviewer-b", _uid(), _now_ms(), slot_b_attempt),
        )
        store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (slot_b_activity,)
        )

    result = store.expire_attempt_claim_deadline(attempt_id=slot_a_attempt)

    assert result.outcome == "EXPIRED"
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (RUN_ID,)).fetchone()
    assert run_row["state"] == "REVIEWING"

    slot_a = store.get_activity(slot_a_activity)
    assert slot_a.state == "PLANNED"
    slot_b = store.get_activity(slot_b_activity)
    assert slot_b.state == "ACTIVE"

    from orcest.workflow_reducer.ledger import load_view

    view = load_view(store, RUN_ID)
    assert view is not None
    assert view.latest_staffing_recheck_transition_sequence is not None

    # A second, later claim-deadline sweep pass over the SAME still-CLAIMED
    # peer must coalesce into the same pending recheck, never a duplicate one.
    view_before = load_view(store, RUN_ID)
    resolved = store.resolve_panel_staffing_recheck(
        run_id=RUN_ID, assignment_kind="REVIEW", panel_round=1
    )
    assert resolved is not None
    assert resolved.reduction is not None
    assert resolved.reduction.reason_code == "STAFFING_OR_STATE"
    view_after = load_view(store, RUN_ID)
    assert view_after.state == "REVIEWING"
    assert (
        view_after.latest_staffing_recheck_transition_sequence
        == view_before.latest_staffing_recheck_transition_sequence
    )
