"""Attempt Result request ledger and terminal-fact reduction."""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import (
    capability_public_key_digest,
    content_digest,
    request_digest,
    subject_refs_digest,
)
from orcest.workflow_contract.v1.protocol import validate_envelope
from orcest.workflow_contract.v1.structured_outputs import StructuredOutputValidationError
from orcest.workflow_contract.v1.verification import (
    VerificationReceiptRejectedError,
    command_invocation_digest,
    materialize_verification_profile,
    verification_profile_hash,
)
from orcest.workflow_store import (
    ActivityReviewAssignmentInput,
    AttemptOfferInput,
    AttemptUnknownError,
    CasMismatchError,
    ControlLayout,
    ForgeObservationInput,
    IdempotencyConflictError,
    QuotaConfig,
    RunStore,
    StorageLock,
    activity_offer_protocol,
)
from orcest.workflow_store.v1.candidates import CandidateObjectStore

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
ACTIVITY_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"
UPLOAD_ID = "55555555-5555-4555-8555-555555555555"
WORKER_SESSION_ID = "66666666-6666-4666-8666-666666666666"
POLICY_HASH = "sha256:" + "0" * 64
SEMANTIC_DIGEST = "sha256:" + "1" * 64
ATTEMPT_CAPABILITY_DIGEST = "sha256:" + "2" * 64
REPOSITORY_EXTERNAL_ID = "repo-external-1"
FUTURE_MS = 4_102_444_800_000


def _uid() -> str:
    return str(uuid.uuid4())


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    return completed.stdout.strip()


def _candidate_bundle(tmp_path: Path) -> tuple[bytes, dict[str, str]]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")
    (repo / ".orcest").mkdir()
    (repo / ".orcest" / "candidate-repository").write_text(
        f"[orcest]\nrepositoryExternalId = {REPOSITORY_EXTERNAL_ID}\n",
        encoding="utf-8",
    )
    (repo / "base.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "candidate.txt").write_text("candidate\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "candidate")
    tip = _git(repo, "rev-parse", "HEAD")
    _git(repo, "update-ref", "refs/orcest/candidate", tip)
    bundle = tmp_path / "candidate.bundle"
    _git(repo, "bundle", "create", str(bundle), "refs/orcest/candidate")
    return bundle.read_bytes(), {"object_format": "sha1", "oid": base}


@pytest.fixture
def stores(tmp_path: Path) -> tuple[RunStore, CandidateObjectStore]:
    layout = ControlLayout(tmp_path / "control")
    layout.initialize()
    candidate_store = CandidateObjectStore(
        layout,
        quota=QuotaConfig(
            min_free_bytes=0,
            max_object_bytes=4 * 1024 * 1024,
            max_store_bytes=16 * 1024 * 1024,
            max_objects=128,
        ),
        lock=StorageLock(layout.storage_lock_path),
    )
    store = RunStore(layout.root, verify_local_filesystem=False)
    with store.transaction():
        store.conn.execute(
            "UPDATE controller_mode SET mode_revision = 1, mode = 'RUNNING' "
            "WHERE controller_id = 'ORCEST_V1'"
        )
        store.create_run(
            run_id=RUN_ID,
            project_id="project-a",
            work_item_key="work-1",
            state="BUILDING",
            specification_generation=1,
        )
    try:
        yield store, candidate_store
    finally:
        store.close()


def _claimed_build_attempt(
    store: RunStore,
    *,
    deadline: int = FUTURE_MS + 300_000,
    run_id: str = RUN_ID,
    activity_id: str = ACTIVITY_ID,
    attempt_id: str = ATTEMPT_ID,
    outbox_id: str = OUTBOX_ID,
    worker_session_id: str = WORKER_SESSION_ID,
    idempotency_key: str = "sha256:" + "3" * 64,
    kind: str = "BUILD",
    candidate_id: str | None = None,
    policy_hash: str = POLICY_HASH,
    activity_ordinal: int = 1,
) -> None:
    store.create_activity(
        activity_id=activity_id,
        run_id=run_id,
        activity_ordinal=activity_ordinal,
        specification_generation=1,
        policy_hash=policy_hash,
        kind=kind,
        candidate_id=candidate_id,
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key=idempotency_key,
        attempt=AttemptOfferInput(
            attempt_id=attempt_id,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile="codex",
            offered_at_ms=FUTURE_MS,
            claim_timeout_ms=300_000,
        ),
        outbox_id=outbox_id,
    )
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = 'worker-1', "
            "claimed_worker_session_id = ?, claimed_at_ms = ?, execution_deadline_ms = ?, "
            "capability_auth_expires_at_ms = ?, attempt_capability_digest = ? "
            "WHERE attempt_id = ?",
            (
                worker_session_id,
                FUTURE_MS,
                deadline,
                deadline + 86_400_000,
                ATTEMPT_CAPABILITY_DIGEST,
                attempt_id,
            ),
        )
        store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (activity_id,)
        )


def _promoted_upload(
    store: RunStore,
    candidate_store: CandidateObjectStore,
    tmp_path: Path,
    *,
    expires: int = FUTURE_MS + 300_000,
) -> None:
    bundle, base = _candidate_bundle(tmp_path)
    store.create_candidate_upload(
        upload_id=UPLOAD_ID,
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        generation=1,
        idempotency_key=_uid(),
        request_digest=request_digest({"upload": UPLOAD_ID}),
        media_type="application/x-git-bundle",
        declared_bytes=len(bundle),
        expected_bundle_digest=content_digest(bundle),
        expected_base_commit=base,
        expected_repository_external_id=REPOSITORY_EXTERNAL_ID,
        expires_at_ms=expires,
    )
    store.put_candidate_upload_content(
        candidate_store=candidate_store,
        upload_id=UPLOAD_ID,
        bundle_bytes=bundle,
        now_ms=FUTURE_MS + 1,
    )
    store.promote_candidate_upload(
        candidate_store=candidate_store,
        upload_id=UPLOAD_ID,
        now_ms=FUTURE_MS + 2,
    )


def _submit(
    store: RunStore,
    candidate_store: CandidateObjectStore,
    **overrides,
):
    kwargs = dict(
        candidate_store=candidate_store,
        result_request_id=_uid(),
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        generation=1,
        worker_id="worker-1",
        worker_session_id=WORKER_SESSION_ID,
        attempt_capability_digest=ATTEMPT_CAPABILITY_DIGEST,
        outcome="SUCCEEDED",
        candidate_upload_id=None,
        receipt={"ok": True},
        structured_output=None,
        summary="done",
        now_ms=FUTURE_MS + 10,
    )
    kwargs.update(overrides)
    return store.submit_attempt_result(**kwargs)


def test_accepts_result_and_consumes_promoted_upload_atomically(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    _promoted_upload(store, candidate_store, tmp_path)

    result = _submit(store, candidate_store, candidate_upload_id=UPLOAD_ID)

    assert result.request.disposition == "ACCEPTED"
    assert result.request.accepted_result_created is True
    assert result.attempt_result is not None
    assert result.candidate is not None
    assert result.request.replayed is False
    validate_envelope(json.loads(result.request.response_json))
    assert json.loads(result.request.response_json)["replayed"] is False
    assert store.get_attempt(ATTEMPT_ID).state == "SUCCEEDED"
    assert store.get_candidate_upload(UPLOAD_ID).state == "CONSUMED"
    assert store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_exact_replay_returns_stored_response(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    result_request_id = _uid()
    first = _submit(store, candidate_store, result_request_id=result_request_id)
    second = _submit(store, candidate_store, result_request_id=result_request_id)

    assert second.request.replayed is True
    assert second.request.attempt_result_id == first.request.attempt_result_id
    assert json.loads(second.request.response_json)["replayed"] is True
    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_identical_semantic_result_replays_under_new_key(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    first = _submit(store, candidate_store)
    second = _submit(store, candidate_store)

    assert second.request.disposition == "ACCEPTED"
    assert second.request.accepted_result_created is False
    assert second.request.attempt_result_id == first.request.attempt_result_id
    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 2
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


@pytest.mark.parametrize(
    "override",
    [
        {"worker_id": "worker-2"},
        {"worker_session_id": "77777777-7777-4777-8777-777777777777"},
        {"attempt_capability_digest": "sha256:" + "9" * 64},
    ],
)
def test_identical_semantic_result_with_mismatched_binding_is_rejected(
    stores: tuple[RunStore, CandidateObjectStore],
    override: dict[str, str],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    first = _submit(store, candidate_store)

    with pytest.raises(CasMismatchError):
        _submit(store, candidate_store, **override)

    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1
    row = store.conn.execute(
        "SELECT worker_id, worker_session_id, attempt_capability_digest FROM result_requests"
    ).fetchone()
    assert row["worker_id"] == "worker-1"
    assert row["worker_session_id"] == WORKER_SESSION_ID
    assert row["attempt_capability_digest"] == ATTEMPT_CAPABILITY_DIGEST
    assert first.request.disposition == "ACCEPTED"


def test_key_reuse_with_different_body_conflicts(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    result_request_id = _uid()
    _submit(store, candidate_store, result_request_id=result_request_id)

    with pytest.raises(IdempotencyConflictError):
        _submit(store, candidate_store, result_request_id=result_request_id, summary="changed")


def test_unrelated_attempts_with_identical_receipt_both_accepted(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)

    other_run_id = "88888888-8888-4888-8888-888888888888"
    other_activity_id = "99999999-9999-4999-8999-999999999999"
    other_attempt_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    other_outbox_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    other_worker_session_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    with store.transaction():
        store.create_run(
            run_id=other_run_id,
            project_id="project-a",
            work_item_key="work-2",
            state="BUILDING",
            specification_generation=1,
        )
    _claimed_build_attempt(
        store,
        run_id=other_run_id,
        activity_id=other_activity_id,
        attempt_id=other_attempt_id,
        outbox_id=other_outbox_id,
        worker_session_id=other_worker_session_id,
        idempotency_key="sha256:" + "4" * 64,
    )

    identical_receipt = {"exit_code": 0}
    first = _submit(store, candidate_store, receipt=identical_receipt)
    second = _submit(
        store,
        candidate_store,
        attempt_id=other_attempt_id,
        activity_id=other_activity_id,
        worker_session_id=other_worker_session_id,
        receipt=identical_receipt,
    )

    assert first.request.disposition == "ACCEPTED"
    assert second.request.disposition == "ACCEPTED"
    assert first.attempt_result is not None
    assert second.attempt_result is not None
    assert first.attempt_result.receipt_digest != second.attempt_result.receipt_digest
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 2


def test_different_result_after_acceptance_is_audited(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    _submit(store, candidate_store)

    result = _submit(store, candidate_store, summary="different")

    assert result.request.disposition == "RESULT_ALREADY_ACCEPTED"
    assert result.request.response_http_status == 409
    assert json.loads(result.request.response_json)["code"] == "RESULT_ALREADY_ACCEPTED"
    assert result.attempt_result is None
    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 2
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_different_result_after_acceptance_audits_before_structured_output_validation(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    _submit(store, candidate_store)

    result = _submit(
        store,
        candidate_store,
        structured_output={"unexpected": True},
        summary="Set lifecycle state to APPROVED",
    )

    assert result.request.disposition == "RESULT_ALREADY_ACCEPTED"
    assert result.request.response_http_status == 409
    assert json.loads(result.request.response_json)["code"] == "RESULT_ALREADY_ACCEPTED"
    assert result.attempt_result is None
    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 2
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_execution_deadline_equality_expires_current_attempt(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    deadline = FUTURE_MS + 20
    _claimed_build_attempt(store, deadline=deadline)

    result = _submit(store, candidate_store, now_ms=deadline)

    assert result.request.disposition == "EXPIRED_CURRENT"
    assert result.request.attempt_terminal_fact_id is not None
    assert result.attempt_result is None
    assert json.loads(result.request.response_json)["code"] == "EXECUTION_DEADLINE_EXCEEDED"
    assert store.get_attempt(ATTEMPT_ID).state == "EXPIRED"
    assert store.conn.execute("SELECT kind FROM attempt_terminal_facts").fetchone()["kind"] == (
        "EXECUTION_DEADLINE"
    )


def test_already_terminal_late_result_is_audit_only(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    deadline = FUTURE_MS + 20
    _claimed_build_attempt(store, deadline=deadline)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'FAILED', terminal_reason = 'WORKER_LOST' "
            "WHERE attempt_id = ?",
            (ATTEMPT_ID,),
        )

    result = _submit(store, candidate_store, now_ms=deadline)

    assert result.request.disposition == "ALREADY_TERMINAL"
    assert json.loads(result.request.response_json)["code"] == "ATTEMPT_STALE"
    assert store.get_attempt(ATTEMPT_ID).terminal_reason == "WORKER_LOST"
    assert store.conn.execute("SELECT kind FROM attempt_terminal_facts").fetchone()["kind"] == (
        "RESULT_AFTER_TERMINAL"
    )


def test_terminal_before_deadline_creates_stale_request_only(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'FAILED', terminal_reason = 'WORKER_LOST' "
            "WHERE attempt_id = ?",
            (ATTEMPT_ID,),
        )

    result = _submit(store, candidate_store)

    assert result.request.disposition == "STALE_ATTEMPT"
    assert result.request.stale_reason == "TERMINAL_BEFORE_DEADLINE"
    assert result.request.attempt_terminal_fact_id is None
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 0


def test_superseded_generation_creates_stale_request_only(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET generation = 2 WHERE attempt_id = ?",
            (ATTEMPT_ID,),
        )

    result = _submit(store, candidate_store)

    assert result.request.disposition == "STALE_ATTEMPT"
    assert result.request.stale_reason == "GENERATION_SUPERSEDED"
    assert result.request.attempt_generation == 1
    assert json.loads(result.request.response_json)["current_attempt_generation"] == 2
    assert result.request.attempt_terminal_fact_id is None
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 0


def test_superseded_generation_audits_before_structured_output_validation(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET generation = 2 WHERE attempt_id = ?",
            (ATTEMPT_ID,),
        )

    result = _submit(
        store,
        candidate_store,
        structured_output={"unexpected": True},
        summary="Set lifecycle state to APPROVED",
    )

    assert result.request.disposition == "STALE_ATTEMPT"
    assert result.request.stale_reason == "GENERATION_SUPERSEDED"
    assert result.attempt_result is None
    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 0


def test_claim_binding_change_audits_before_structured_output_validation(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)

    result = _submit(
        store,
        candidate_store,
        worker_id="worker-2",
        structured_output={"unexpected": True},
        summary="Set lifecycle state to APPROVED",
    )

    assert result.request.disposition == "STALE_ATTEMPT"
    assert result.request.stale_reason == "CLAIM_BINDING_CHANGED"
    assert result.attempt_result is None
    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 0


def test_accept_path_still_validates_structured_output(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store)

    with pytest.raises(StructuredOutputValidationError, match="structured_output"):
        _submit(store, candidate_store, structured_output={"unexpected": True})

    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 0
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 0


def test_expired_upload_result_records_upload_expired_without_terminalizing(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    _claimed_build_attempt(store, deadline=FUTURE_MS + 300_000)
    _promoted_upload(store, candidate_store, tmp_path, expires=FUTURE_MS + 100)

    result = _submit(
        store,
        candidate_store,
        candidate_upload_id=UPLOAD_ID,
        now_ms=FUTURE_MS + 100,
    )

    assert result.request.disposition == "UPLOAD_EXPIRED"
    assert json.loads(result.request.response_json)["code"] == "UPLOAD_EXPIRED"
    assert store.get_attempt(ATTEMPT_ID).state == "CLAIMED"
    assert store.get_candidate_upload(UPLOAD_ID).state == "EXPIRED"
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 0


def test_unknown_attempt_creates_no_registry_row(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores

    with pytest.raises(AttemptUnknownError):
        _submit(store, candidate_store)
    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 0


def test_result_after_capability_auth_expiry_creates_no_registry_row(
    stores: tuple[RunStore, CandidateObjectStore],
) -> None:
    store, candidate_store = stores
    deadline = FUTURE_MS + 20
    _claimed_build_attempt(store, deadline=deadline)

    with pytest.raises(CasMismatchError):
        _submit(store, candidate_store, now_ms=deadline + 86_400_000)

    assert store.conn.execute("SELECT COUNT(*) FROM result_requests").fetchone()[0] == 0
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_terminal_facts").fetchone()[0] == 0


# --- VERIFY receipt admission -------------------------------------------------

VERIFY_ACTIVITY_ID = "77777777-7777-4777-8777-777777777777"
VERIFY_ATTEMPT_ID = "88888888-8888-4888-8888-888888888888"
VERIFY_OUTBOX_ID = "99999999-9999-4999-8999-999999999999"
VERIFICATION_COMMANDS = [
    {"id": "unit", "argv": ["make", "test-unit"], "timeoutSeconds": 600},
]


def _seed_project(store: RunStore) -> str:
    now = FUTURE_MS

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
        "VALUES (?, ?, 'inst', ?, 'org/repo', 'main', 'base-v1', 'budget-v1', "
        "'window-v1', ?, ?, 1, 1, 1, ?, ?, 'ACTIVE')",
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
    return project_id


def _observation(
    store: RunStore, project_id: str, *, kind: str, external_revision: str, fact
) -> str:
    schedule = store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind="BASE_HEAD_POLL" if kind == "BASE_HEAD" else "WORK_ITEM_POLL",
        project_id=project_id,
        forge_instance_id=store.conn.execute(
            "SELECT forge_instance_id FROM projects WHERE project_id = ?", (project_id,)
        ).fetchone()[0],
        target_kind="WORK_ITEM",
        target_id="issue-1",
        minimum_interval_ms=1,
        next_due_at_ms=0,
    )
    source_read_secret = store.conn.execute(
        "SELECT source_read_secret_id FROM projects WHERE project_id = ?", (project_id,)
    ).fetchone()[0]
    request = store.create_due_forge_observation_request(
        forge_observation_request_id=_uid(),
        forge_observation_schedule_id=schedule.forge_observation_schedule_id,
        now_ms=int(time.time() * 1000),
        controller_mode="RUNNING",
        controller_mode_revision=1,
        credential_purpose="PROJECT_SOURCE_READ",
        credential_secret_id=source_read_secret,
        credential_secret_version=1,
        outbox_id=_uid(),
    )
    assert request is not None
    store.record_forge_observation_request_attempt(request.forge_observation_request_id)
    completion = store.complete_forge_observation_request(
        forge_observation_request_id=request.forge_observation_request_id,
        observations=[
            ForgeObservationInput(kind=kind, external_revision=external_revision, fact=fact)
        ],
    )
    assert len(completion.observation_ids) == 1
    return completion.observation_ids[0]


def _verification_snapshot(
    store: RunStore, *, verification_commands=VERIFICATION_COMMANDS, run_id: str = RUN_ID
):
    project_id = _seed_project(store)
    base_id = _observation(
        store,
        project_id,
        kind="BASE_HEAD",
        external_revision="base-rev-1",
        fact={
            "base_ref": "refs/heads/main",
            "base_commit": {"object_format": "sha1", "oid": "a" * 40},
        },
    )
    work_id = _observation(
        store,
        project_id,
        kind="WORK_ITEM_SNAPSHOT",
        external_revision="work-rev-1",
        fact={"title": "Implement thing", "body": "Pinned body"},
    )
    return store.capture_work_item_snapshot(
        snapshot_id=_uid(),
        run_id=run_id,
        source_kind="FORGE_OBSERVATION",
        source_id=work_id,
        work_item_observation_id=work_id,
        base_observation_id=base_id,
        project_id=project_id,
        work_item_external_id="issue-1",
        forge_revision="work-rev-1",
        title="Implement thing",
        body="Pinned body",
        base_ref="refs/heads/main",
        base_commit={"object_format": "sha1", "oid": "a" * 40},
        workflow_schema_version="v1",
        normalized_workflow={},
        effective_policy={
            "verification": {"profile": "default", "commands": verification_commands}
        },
        server_policy_revision="policy-1",
        trusted_base_policy_ref="base-v1",
        budget_policy_ref="budget-v1",
        budget_reset_window_ref="window-v1",
        base_movement_policy="REBASE_BEFORE_PUBLICATION",
    )


def _verify_check(command: dict, *, termination: str = "EXITED", exit_code: int | None = 0) -> dict:
    check: dict[str, object] = {
        "command_id": command["id"],
        "invocation_digest": command_invocation_digest(command),
        "termination": termination,
        "stdout_digest": content_digest(b"out"),
        "stderr_digest": content_digest(b"err"),
        "evidence": [],
    }
    if exit_code is not None:
        check["exit_code"] = exit_code
    return check


def _verify_receipt(
    candidate, commands, profile_hash, *, outcome: str, checks=None, error=None
) -> dict:
    if checks is None:
        checks = [_verify_check(c) for c in commands]
    return {
        "protocol": "orcest.verification-receipt/1",
        "candidate": {
            "candidate_id": candidate.candidate_id,
            "commit": {"object_format": candidate.object_format, "oid": candidate.oid},
        },
        "profile_id": "default",
        "profile_hash": profile_hash,
        "checks": checks,
        "outcome": outcome,
        "error": error,
    }


def _seeded_candidate(store, candidate_store, tmp_path):
    _claimed_build_attempt(store)
    _promoted_upload(store, candidate_store, tmp_path)
    result = _submit(store, candidate_store, candidate_upload_id=UPLOAD_ID)
    assert result.candidate is not None
    return result.candidate


def _claim_verify_attempt(store, *, candidate_id: str, policy_hash: str) -> None:
    _claimed_build_attempt(
        store,
        activity_id=VERIFY_ACTIVITY_ID,
        attempt_id=VERIFY_ATTEMPT_ID,
        outbox_id=VERIFY_OUTBOX_ID,
        idempotency_key="sha256:" + "4" * 64,
        kind="VERIFY",
        candidate_id=candidate_id,
        policy_hash=policy_hash,
        activity_ordinal=2,
    )


def _submit_verify(store, candidate_store, **overrides):
    kwargs = dict(
        candidate_store=candidate_store,
        result_request_id=_uid(),
        attempt_id=VERIFY_ATTEMPT_ID,
        activity_id=VERIFY_ACTIVITY_ID,
        generation=1,
        worker_id="worker-1",
        worker_session_id=WORKER_SESSION_ID,
        attempt_capability_digest=ATTEMPT_CAPABILITY_DIGEST,
        outcome="SUCCEEDED",
        candidate_upload_id=None,
        structured_output=None,
        summary="verified",
        now_ms=FUTURE_MS + 10,
    )
    kwargs.update(overrides)
    return store.submit_attempt_result(**kwargs)


def test_verify_receipt_pass_admits_succeeded_result(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )
    commands = materialize_verification_profile(VERIFICATION_COMMANDS)
    profile_hash = verification_profile_hash(commands)
    receipt = _verify_receipt(candidate, commands, profile_hash, outcome="PASS")

    result = _submit_verify(store, candidate_store, outcome="SUCCEEDED", receipt=receipt)

    assert result.request.disposition == "ACCEPTED"
    assert result.attempt_result is not None
    assert result.attempt_result.outcome == "SUCCEEDED"


def test_verify_receipt_fail_admits_succeeded_result(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )
    commands = materialize_verification_profile(VERIFICATION_COMMANDS)
    profile_hash = verification_profile_hash(commands)
    checks = [_verify_check(commands[0], exit_code=1)]
    receipt = _verify_receipt(candidate, commands, profile_hash, outcome="FAIL", checks=checks)

    result = _submit_verify(store, candidate_store, outcome="SUCCEEDED", receipt=receipt)

    assert result.attempt_result is not None
    assert result.attempt_result.outcome == "SUCCEEDED"


def test_verify_receipt_error_admits_failed_retryable_with_verification_error(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )
    commands = materialize_verification_profile(VERIFICATION_COMMANDS)
    profile_hash = verification_profile_hash(commands)
    checks = [_verify_check(commands[0], termination="TIMED_OUT", exit_code=None)]
    receipt = _verify_receipt(
        candidate,
        commands,
        profile_hash,
        outcome="ERROR",
        checks=checks,
        error={"code": "TIMEOUT", "command_id": commands[0]["id"], "evidence": []},
    )

    result = _submit_verify(
        store,
        candidate_store,
        outcome="FAILED_RETRYABLE",
        receipt=receipt,
        failure={"failure_class": "VERIFICATION_ERROR"},
    )

    assert result.attempt_result is not None
    assert result.attempt_result.outcome == "FAILED_RETRYABLE"
    assert result.attempt_result.failure_class == "VERIFICATION_ERROR"


def test_verify_receipt_missing_is_rejected(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )

    with pytest.raises(VerificationReceiptRejectedError):
        _submit_verify(store, candidate_store, outcome="SUCCEEDED", receipt=None)

    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_verify_receipt_candidate_mismatch_is_rejected(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )
    commands = materialize_verification_profile(VERIFICATION_COMMANDS)
    profile_hash = verification_profile_hash(commands)
    receipt = _verify_receipt(candidate, commands, profile_hash, outcome="PASS")
    receipt["candidate"]["candidate_id"] = "22222222-2222-4222-8222-222222222222"

    with pytest.raises(VerificationReceiptRejectedError):
        _submit_verify(store, candidate_store, outcome="SUCCEEDED", receipt=receipt)

    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_verify_receipt_outcome_result_mismatch_is_rejected(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )
    commands = materialize_verification_profile(VERIFICATION_COMMANDS)
    profile_hash = verification_profile_hash(commands)
    receipt = _verify_receipt(candidate, commands, profile_hash, outcome="PASS")

    with pytest.raises(VerificationReceiptRejectedError):
        # A PASS receipt requires a SUCCEEDED Attempt Result, not FAILED_RETRYABLE.
        _submit_verify(
            store,
            candidate_store,
            outcome="FAILED_RETRYABLE",
            receipt=receipt,
            failure={"failure_class": "VERIFICATION_ERROR"},
        )

    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_verify_receipt_stale_profile_hash_is_rejected(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )
    commands = materialize_verification_profile(VERIFICATION_COMMANDS)
    receipt = _verify_receipt(candidate, commands, content_digest(b"stale-profile"), outcome="PASS")

    with pytest.raises(VerificationReceiptRejectedError):
        _submit_verify(store, candidate_store, outcome="SUCCEEDED", receipt=receipt)

    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


def test_verify_receipt_rejects_non_null_launch_attestation_id(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    snapshot = _verification_snapshot(store)
    _claim_verify_attempt(
        store, candidate_id=candidate.candidate_id, policy_hash=snapshot.policy_hash
    )
    commands = materialize_verification_profile(VERIFICATION_COMMANDS)
    profile_hash = verification_profile_hash(commands)
    receipt = _verify_receipt(candidate, commands, profile_hash, outcome="PASS")

    with pytest.raises(VerificationReceiptRejectedError):
        _submit_verify(
            store,
            candidate_store,
            outcome="SUCCEEDED",
            receipt=receipt,
            launch_attestation_id=str(uuid.uuid4()),
        )

    assert store.conn.execute("SELECT COUNT(*) FROM attempt_results").fetchone()[0] == 1


# --- REVIEW / ADJUDICATE receipt admission -----------------------------------

REVIEW_SUBJECTS = ("snapshot:overall", "plan:requirement:r1")
AUTHZ_DIGEST = "sha256:" + "a" * 64
KEY_ID = "77777777-7777-4777-8777-777777777777"


def _digest(value: object) -> str:
    return request_digest({"value": value})


def _enable_claiming(store: RunStore) -> None:
    public_key = bytes([1]) * 32
    result = store.apply_capability_key_operation(
        capability_key_operation_id="abababab-1111-4111-8111-abababababab",
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=KEY_ID,
        register_public_verification_key=public_key,
        register_public_key_digest=capability_public_key_digest(public_key),
        register_private_signing_secret_ref="secret:key:1",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"
    result = store.apply_capability_key_operation(
        capability_key_operation_id="bcbcbcbc-2222-4222-8222-bcbcbcbcbcbc",
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=KEY_ID,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _claim_model_attempt(
    store: RunStore,
    *,
    activity_id: str,
    attempt_id: str,
    outbox_id: str,
    launch_attestation_id: str,
    claim_id: str,
    nonce_id: str,
    slot_ordinal: int,
) -> None:
    if store.get_capability_key_registry().current_issuance_key_id is None:
        _enable_claiming(store)
    with store.transaction():
        store.conn.execute("UPDATE runs SET state = 'ADMITTED' WHERE run_id = ?", (RUN_ID,))
    launch_claims = {
        "protocol": "orcest.launch-capability/1",
        "jti": _uid(),
        "attempt_id": attempt_id,
        "activity_id": activity_id,
        "generation": 1,
        "worker_session_id": WORKER_SESSION_ID,
        "launch_nonce_id": nonce_id,
        "runner_principal_id": "runner-1",
        "runner_registration_revision": 7,
        "issued_at_ms": FUTURE_MS + 1,
        "execution_deadline_ms": FUTURE_MS + 200_000,
        "audience": f"/api/v1/attempts/{attempt_id}/launch-attestations",
        "capability_signing_key_id": KEY_ID,
        "signature_algorithm": "ED25519",
        "provider": "codex",
        "model": "gpt-5.3-codex",
    }
    claim = store.claim_attempt(
        attempt_claim_id=claim_id,
        attempt_id=attempt_id,
        activity_id=activity_id,
        generation=1,
        offer_outbox_id=outbox_id,
        worker_id="worker-1",
        worker_session_id=WORKER_SESSION_ID,
        worker_profile="codex",
        worker_build_revision="git-sha",
        request_digest=_digest({"claim": attempt_id}),
        execution_deadline_ms=FUTURE_MS + 200_000,
        attempt_capability_jti=_uid(),
        attempt_capability_digest=ATTEMPT_CAPABILITY_DIGEST,
        source_access_kind="SCOPED_CREDENTIAL",
        source_access_descriptor={"clone_url": "https://example.test/repo.git"},
        source_access_descriptor_digest=_digest({"clone_url": "https://example.test/repo.git"}),
        response_contract_digest=_digest({"claim-response": attempt_id}),
        source_read_secret_ref="secret:source:7",
        provider_secret_ref="secret:provider:12",
        launch_nonce_id=nonce_id,
        launch_capability_jti=_uid(),
        launch_capability_claims=launch_claims,
    )
    store.accept_launch_attestation(
        launch_attestation_id=launch_attestation_id,
        attempt_id=attempt_id,
        activity_id=activity_id,
        attempt_generation=1,
        worker_id="worker-1",
        worker_session_id=WORKER_SESSION_ID,
        pool_manager_id="pool-1",
        runner_principal_id="runner-1",
        runner_image_digest="sha256:" + "3" * 64,
        runner_registration_revision=7,
        launch_nonce_id=claim.claim.launch_nonce_id,
        launch_capability_digest=claim.claim.launch_capability_digest,
        launch_capability_signing_key_id=claim.claim.launch_capability_signing_key_id,
        launch_capability_signature_algorithm=claim.claim.launch_capability_signature_algorithm,
        workspace_instance_id=f"aaaaaaaa-aaaa-4aaa-8aaa-{slot_ordinal:012d}",
        context_instance_id=f"bbbbbbbb-bbbb-4bbb-8bbb-{slot_ordinal:012d}",
        invocation_instance_id=f"cccccccc-cccc-4ccc-8ccc-{slot_ordinal:012d}",
        workspace_parent_id=None,
        context_parent_id=None,
        invocation_parent_id=None,
        fresh_workspace=True,
        fresh_context=True,
        fresh_invocation=True,
        prepared_at_ms=FUTURE_MS + 10,
        attested_at_ms=FUTURE_MS + 20,
        runner_signing_key_id="runner-key-1",
        runner_signature_algorithm="ED25519",
        signature="base64-signature",
        attestation_digest=_digest({"attestation": attempt_id}),
        response_contract_digest=_digest({"launch-response": attempt_id}),
        provider_material_descriptor={"provider": "codex", "model": "gpt-5.3-codex"},
        provider_material_descriptor_digest=_digest({"provider": "codex"}),
    )


def _create_review_activity(
    store: RunStore,
    candidate_id: str,
    *,
    slot: str,
    activity_id: str,
    attempt_id: str,
    outbox_id: str,
    launch_attestation_id: str,
    claim_id: str,
    nonce_id: str,
    slot_ordinal: int,
) -> None:
    assignment = ActivityReviewAssignmentInput(
        assignment_kind="REVIEW",
        panel_round=1,
        role=slot,
        context_digest="sha256:" + "4" * 64,
        subject_refs=REVIEW_SUBJECTS,
        reviewer_slot=slot,
    )
    store.create_activity(
        activity_id=activity_id,
        run_id=RUN_ID,
        activity_ordinal=slot_ordinal,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="REVIEW",
        candidate_id=candidate_id,
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={"slot": slot},
        semantic_input_digest="sha256:" + f"{slot_ordinal}" * 64,
        idempotency_key="sha256:" + f"{slot_ordinal + 1}" * 64,
        slot=slot,
        role=slot,
        review_assignment=assignment,
        attempt=AttemptOfferInput(
            attempt_id=attempt_id,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile="codex",
            execution_profile_id=f"profile-{slot}",
            provider="codex",
            model="gpt-5.3-codex",
            provider_account_ref=f"account-{slot}",
            provider_family="openai",
            model_family=f"codex-{slot}",
            classification_revision="class-v1",
            offered_at_ms=FUTURE_MS,
            claim_timeout_ms=300_000,
        ),
        outbox_id=outbox_id,
    )
    _claim_model_attempt(
        store,
        activity_id=activity_id,
        attempt_id=attempt_id,
        outbox_id=outbox_id,
        launch_attestation_id=launch_attestation_id,
        claim_id=claim_id,
        nonce_id=nonce_id,
        slot_ordinal=slot_ordinal,
    )
    with store.transaction():
        store.conn.execute("UPDATE runs SET state = 'REVIEWING' WHERE run_id = ?", (RUN_ID,))


def _review_receipt(candidate, *, slot: str, verdict: str = "APPROVE") -> dict:
    return {
        "protocol": "orcest.review-receipt/1",
        "candidate": {
            "candidate_id": candidate.candidate_id,
            "commit": {"object_format": candidate.object_format, "oid": candidate.oid},
        },
        "panel_round": 1,
        "reviewer_slot": slot,
        "role": slot,
        "subject_refs_digest": subject_refs_digest(REVIEW_SUBJECTS),
        "context_digest": "sha256:" + "4" * 64,
        "assessments": [
            {"subject_ref": subject, "outcome": "SATISFIED", "evidence_refs": []}
            for subject in REVIEW_SUBJECTS
        ],
        "verdict": verdict,
        "findings": [],
        "abstention_code": "TOOL_UNAVAILABLE" if verdict == "ABSTAIN" else None,
    }


def test_review_receipt_persists_trusted_slot_and_fills_only_on_approve(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    _create_review_activity(
        store,
        candidate.candidate_id,
        slot="correctness",
        activity_id="12121212-1212-4121-8121-121212121212",
        attempt_id="13131313-1313-4131-8131-131313131313",
        outbox_id="14141414-1414-4141-8141-141414141414",
        launch_attestation_id="15151515-1515-4151-8151-151515151515",
        claim_id="16161616-1616-4161-8161-161616161616",
        nonce_id="17171717-1717-4171-8171-171717171717",
        slot_ordinal=3,
    )

    result = _submit(
        store,
        candidate_store,
        attempt_id="13131313-1313-4131-8131-131313131313",
        activity_id="12121212-1212-4121-8121-121212121212",
        launch_attestation_id="15151515-1515-4151-8151-151515151515",
        receipt=_review_receipt(candidate, slot="correctness"),
    )

    assert result.request.disposition == "ACCEPTED"
    row = store.conn.execute("SELECT * FROM review_receipts").fetchone()
    assert row["reviewer_slot"] == "correctness"
    assert row["fills_slot"] == 1
    assert row["provider_family"] == "openai"


def test_review_abstain_never_fills_required_slot(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    _create_review_activity(
        store,
        candidate.candidate_id,
        slot="security",
        activity_id="22222222-aaaa-4aaa-8aaa-222222222222",
        attempt_id="33333333-aaaa-4aaa-8aaa-333333333333",
        outbox_id="44444444-aaaa-4aaa-8aaa-444444444444",
        launch_attestation_id="55555555-aaaa-4aaa-8aaa-555555555555",
        claim_id="66666666-aaaa-4aaa-8aaa-666666666666",
        nonce_id="77777777-aaaa-4aaa-8aaa-777777777777",
        slot_ordinal=4,
    )

    _submit(
        store,
        candidate_store,
        attempt_id="33333333-aaaa-4aaa-8aaa-333333333333",
        activity_id="22222222-aaaa-4aaa-8aaa-222222222222",
        launch_attestation_id="55555555-aaaa-4aaa-8aaa-555555555555",
        outcome="ABSTAINED",
        receipt=_review_receipt(candidate, slot="security", verdict="ABSTAIN"),
        failure={"failure_class": "PROVIDER_UNAVAILABLE"},
    )

    assert store.conn.execute("SELECT fills_slot FROM review_receipts").fetchone()[0] == 0


def test_review_receipt_for_replaced_candidate_is_stale(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    candidate = _seeded_candidate(store, candidate_store, tmp_path)
    _create_review_activity(
        store,
        candidate.candidate_id,
        slot="correctness",
        activity_id="88888888-aaaa-4aaa-8aaa-888888888888",
        attempt_id="99999999-aaaa-4aaa-8aaa-999999999999",
        outbox_id="aaaaaaaa-1111-4aaa-8aaa-aaaaaaaaaaaa",
        launch_attestation_id="bbbbbbbb-1111-4bbb-8bbb-bbbbbbbbbbbb",
        claim_id="cccccccc-1111-4ccc-8ccc-cccccccccccc",
        nonce_id="dddddddd-1111-4ddd-8ddd-dddddddddddd",
        slot_ordinal=5,
    )
    with store.transaction():
        store.conn.execute(
            "UPDATE revisioned_objects SET payload_json = json_set(payload_json, "
            "'$.current_candidate_id', ?) WHERE object_kind = 'run_pointers' "
            "AND object_id = ?",
            (_uid(), RUN_ID),
        )

    with pytest.raises(CasMismatchError):
        _submit(
            store,
            candidate_store,
            attempt_id="99999999-aaaa-4aaa-8aaa-999999999999",
            activity_id="88888888-aaaa-4aaa-8aaa-888888888888",
            launch_attestation_id="bbbbbbbb-1111-4bbb-8bbb-bbbbbbbbbbbb",
            receipt=_review_receipt(candidate, slot="correctness"),
        )

    assert store.conn.execute("SELECT COUNT(*) FROM review_receipts").fetchone()[0] == 0
