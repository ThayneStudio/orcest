from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import content_digest, request_digest
from orcest.workflow_store import (
    AttemptOfferInput,
    CasMismatchError,
    ControlLayout,
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
IDEMPOTENCY_KEY = "66666666-6666-4666-8666-666666666666"
POLICY_HASH = "sha256:" + "0" * 64
SEMANTIC_DIGEST = "sha256:" + "1" * 64
FUTURE_MS = 4_102_444_800_000
REPOSITORY_EXTERNAL_ID = "repo-external-1"


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


def _candidate_bundle(
    tmp_path: Path, *, repository_external_id: str = REPOSITORY_EXTERNAL_ID
) -> tuple[bytes, dict[str, str], str]:
    repo = tmp_path / f"repo-{repository_external_id}"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")
    (repo / ".orcest").mkdir()
    (repo / ".orcest" / "candidate-repository").write_text(
        f"[orcest]\nrepositoryExternalId = {repository_external_id}\n",
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
    bundle = tmp_path / f"{repository_external_id}.bundle"
    _git(repo, "bundle", "create", str(bundle), "refs/orcest/candidate")
    return bundle.read_bytes(), {"object_format": "sha1", "oid": base}, tip


@pytest.fixture
def stores(tmp_path: Path) -> tuple[RunStore, CandidateObjectStore]:
    layout = ControlLayout(tmp_path / "control")
    layout.initialize()
    quota = QuotaConfig(
        min_free_bytes=0,
        max_object_bytes=4 * 1024 * 1024,
        max_store_bytes=16 * 1024 * 1024,
        max_objects=128,
    )
    candidate_store = CandidateObjectStore(
        layout, quota=quota, lock=StorageLock(layout.storage_lock_path)
    )
    store = RunStore(layout.root, verify_local_filesystem=False)
    with store.transaction():
        store.create_run(
            run_id=RUN_ID,
            project_id="project-a",
            work_item_key="work-1",
            state="ADMITTED",
            specification_generation=1,
        )
    try:
        yield store, candidate_store
    finally:
        store.close()


def _offer() -> AttemptOfferInput:
    return AttemptOfferInput(
        attempt_id=ATTEMPT_ID,
        generation=1,
        protocol_version=activity_offer_protocol(),
        worker_profile="codex",
        offered_at_ms=FUTURE_MS,
        claim_timeout_ms=300_000,
    )


def _claimed_build_attempt(store: RunStore) -> None:
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
        semantic_input={},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key="sha256:" + "2" * 64,
        attempt=_offer(),
        outbox_id=OUTBOX_ID,
    )
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = 'worker-1', "
            "claimed_worker_session_id = 'session-1', claimed_at_ms = ?, "
            "execution_deadline_ms = ?, capability_auth_expires_at_ms = ? "
            "WHERE attempt_id = ?",
            (FUTURE_MS, FUTURE_MS + 300_000, FUTURE_MS + 86_700_000, ATTEMPT_ID),
        )


def _create_upload(store: RunStore, bundle: bytes, base: dict[str, str], *, expires: int) -> None:
    store.create_candidate_upload(
        upload_id=UPLOAD_ID,
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        generation=1,
        idempotency_key=IDEMPOTENCY_KEY,
        request_digest=request_digest({"upload": UPLOAD_ID}),
        media_type="application/x-git-bundle",
        declared_bytes=len(bundle),
        expected_bundle_digest=content_digest(bundle),
        expected_base_commit=base,
        expected_repository_external_id=REPOSITORY_EXTERNAL_ID,
        expires_at_ms=expires,
    )


def test_candidate_upload_validates_real_bundle_and_promotes_without_candidate_row(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    bundle, base, tip = _candidate_bundle(tmp_path)
    _claimed_build_attempt(store)
    _create_upload(store, bundle, base, expires=FUTURE_MS + 300_000)

    validated = store.put_candidate_upload_content(
        candidate_store=candidate_store,
        upload_id=UPLOAD_ID,
        bundle_bytes=bundle,
        now_ms=FUTURE_MS + 1,
    )
    assert validated.state == "VALIDATED"
    assert validated.verified_tip == {"object_format": "sha1", "oid": tip}

    promoted = store.promote_candidate_upload(
        candidate_store=candidate_store,
        upload_id=UPLOAD_ID,
        now_ms=FUTURE_MS + 2,
    )
    assert promoted.state == "PROMOTED"
    assert promoted.artifact_bundle_digest == content_digest(bundle)
    assert store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0] == 0
    assert candidate_store.read(content_digest(bundle)) == bundle


def test_expiry_at_equality_wins_for_put_and_replays_exact_body(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    bundle, base, _tip = _candidate_bundle(tmp_path)
    _claimed_build_attempt(store)
    expires = FUTURE_MS + 100
    _create_upload(store, bundle, base, expires=expires)

    expired = store.put_candidate_upload_content(
        candidate_store=candidate_store,
        upload_id=UPLOAD_ID,
        bundle_bytes=bundle,
        now_ms=expires,
    )
    assert expired.state == "EXPIRED"
    assert store.candidate_upload_expired_body(expired) == {
        "protocol": "orcest.candidate-upload-expired/1",
        "upload_id": UPLOAD_ID,
        "state": "EXPIRED",
        "code": "UPLOAD_EXPIRED",
        "expires_at_ms": expires,
    }
    replay = store.put_candidate_upload_content(
        candidate_store=candidate_store,
        upload_id=UPLOAD_ID,
        bundle_bytes=b"changed",
        now_ms=expires + 1,
    )
    assert store.candidate_upload_expired_body(replay) == store.candidate_upload_expired_body(
        expired
    )


def test_corrupt_wrong_base_and_wrong_repository_uploads_do_not_promote(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    bundle, base, _tip = _candidate_bundle(tmp_path)
    _claimed_build_attempt(store)
    _create_upload(store, bundle, base, expires=FUTURE_MS + 300_000)

    with pytest.raises(CasMismatchError):
        store.put_candidate_upload_content(
            candidate_store=candidate_store,
            upload_id=UPLOAD_ID,
            bundle_bytes=bundle[:-1] + b"x",
            now_ms=FUTURE_MS + 1,
        )
    assert store.get_candidate_upload(UPLOAD_ID).state == "RECEIVING"

    wrong_bundle, wrong_base, _wrong_tip = _candidate_bundle(
        tmp_path, repository_external_id="other-repo"
    )
    wrong_repo_upload = "77777777-7777-4777-8777-777777777777"
    store.create_candidate_upload(
        upload_id=wrong_repo_upload,
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        generation=1,
        idempotency_key="88888888-8888-4888-8888-888888888888",
        request_digest=request_digest({"upload": wrong_repo_upload}),
        media_type="application/x-git-bundle",
        declared_bytes=len(wrong_bundle),
        expected_bundle_digest=content_digest(wrong_bundle),
        expected_base_commit=wrong_base,
        expected_repository_external_id=REPOSITORY_EXTERNAL_ID,
        expires_at_ms=FUTURE_MS + 300_000,
    )
    with pytest.raises(CasMismatchError, match="repository identity"):
        store.put_candidate_upload_content(
            candidate_store=candidate_store,
            upload_id=wrong_repo_upload,
            bundle_bytes=wrong_bundle,
            now_ms=FUTURE_MS + 2,
        )
    assert store.conn.execute("SELECT COUNT(*) FROM artifact_objects").fetchone()[0] == 0

    other_upload = "99999999-9999-4999-8999-999999999999"
    store.create_candidate_upload(
        upload_id=other_upload,
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        generation=1,
        idempotency_key="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        request_digest=request_digest({"upload": other_upload}),
        media_type="application/x-git-bundle",
        declared_bytes=len(bundle),
        expected_bundle_digest=content_digest(bundle),
        expected_base_commit={"object_format": "sha1", "oid": "0" * 40},
        expected_repository_external_id=REPOSITORY_EXTERNAL_ID,
        expires_at_ms=FUTURE_MS + 300_000,
    )
    with pytest.raises(CasMismatchError):
        store.put_candidate_upload_content(
            candidate_store=candidate_store,
            upload_id=other_upload,
            bundle_bytes=bundle,
            now_ms=FUTURE_MS + 3,
        )


def test_validated_upload_rejects_different_complete_body_replay(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    bundle, base, _tip = _candidate_bundle(tmp_path)
    _claimed_build_attempt(store)
    _create_upload(store, bundle, base, expires=FUTURE_MS + 300_000)
    store.put_candidate_upload_content(
        candidate_store=candidate_store,
        upload_id=UPLOAD_ID,
        bundle_bytes=bundle,
        now_ms=FUTURE_MS + 1,
    )

    with pytest.raises(IdempotencyConflictError):
        store.put_candidate_upload_content(
            candidate_store=candidate_store,
            upload_id=UPLOAD_ID,
            bundle_bytes=bundle + b"x",
            now_ms=FUTURE_MS + 2,
        )


def test_controller_import_admits_candidate_and_download_is_attempt_scoped(
    stores: tuple[RunStore, CandidateObjectStore], tmp_path: Path
) -> None:
    store, candidate_store = stores
    bundle, base, tip = _candidate_bundle(tmp_path)
    import_activity = "99999999-9999-4999-8999-999999999999"
    store.create_activity(
        activity_id=import_activity,
        run_id=RUN_ID,
        activity_ordinal=1,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="IMPORT",
        execution_class="CONTROLLER",
        state="ACTIVE",
        created_transition_sequence=1,
        semantic_input={},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key="sha256:" + "3" * 64,
    )

    candidate, fact = store.admit_controller_import_candidate(
        candidate_store=candidate_store,
        candidate_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        controller_operation_fact_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        activity_id=import_activity,
        forge_observation_id="forge-observation-1",
        operation_digest=request_digest({"operation": "import"}),
        fact_digest=request_digest({"fact": "import"}),
        bundle_bytes=bundle,
        expected_base_commit=base,
        expected_repository_external_id=REPOSITORY_EXTERNAL_ID,
        now_ms=FUTURE_MS + 1,
    )
    assert candidate.provenance_kind == "FORGE_IMPORT"
    assert candidate.oid == tip
    assert fact.operation_kind == "IMPORT"

    consumer_activity = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    consumer_attempt = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    store.create_activity(
        activity_id=consumer_activity,
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="VERIFY",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key="sha256:" + "4" * 64,
        candidate_id=candidate.candidate_id,
        attempt=AttemptOfferInput(
            attempt_id=consumer_attempt,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile="codex",
            offered_at_ms=FUTURE_MS,
            claim_timeout_ms=300_000,
        ),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    with store.transaction():
        store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = 'worker-1', "
            "claimed_worker_session_id = 'session-1' WHERE attempt_id = ?",
            (consumer_attempt,),
        )
    download = store.get_candidate_download_for_attempt(
        attempt_id=consumer_attempt,
        candidate_id=candidate.candidate_id,
    )
    assert download.bundle.bundle_digest == candidate.bundle_digest
    with pytest.raises(CasMismatchError):
        store.get_candidate_download_for_attempt(
            attempt_id=ATTEMPT_ID,
            candidate_id=candidate.candidate_id,
        )
