"""Candidate object identity, storage-key shape, and exact-object APIs."""

from __future__ import annotations

import pytest

from orcest.workflow_contract.v1.digest import content_digest
from orcest.workflow_store.v1.candidates import CandidateObjectRecord, CandidateObjectStore
from orcest.workflow_store.v1.errors import IntegrityConflictError, ObjectNotFoundError
from orcest.workflow_store.v1.fs import FILE_MODE, ControlLayout, digest_hex

BUNDLE = b"PACK\x00git-bundle-bytes-for-v1-tests\n"


def test_candidate_digest_is_sha256_of_exact_bytes(
    candidate_store: CandidateObjectStore,
) -> None:
    record = candidate_store.install(BUNDLE)
    hex_part = digest_hex(record.bundle_digest)
    assert record.bundle_digest == content_digest(BUNDLE)
    assert record.storage_key == f"objects/sha256/{hex_part[:2]}/{hex_part}.bundle"
    assert candidate_store.read(record.bundle_digest) == BUNDLE


def test_same_bytes_as_workflow_blob_do_not_share_candidate_identity(
    candidate_store: CandidateObjectStore,
) -> None:
    from orcest.workflow_contract.v1.digest import workflow_blob_digest

    record = candidate_store.identity(BUNDLE)
    blob_digest = workflow_blob_digest("CONFIG_JSON", BUNDLE)
    assert record.bundle_digest != blob_digest


def test_candidate_install_is_idempotent(candidate_store: CandidateObjectStore) -> None:
    first = candidate_store.install(BUNDLE)
    second = candidate_store.install(BUNDLE)
    assert first == second
    assert list(candidate_store.iter_objects()) == [first]


def test_no_clobber_mismatch_does_not_replace_dest(
    candidate_store: CandidateObjectStore, layout: ControlLayout
) -> None:
    record = candidate_store.identity(BUNDLE)
    dest = layout.candidates_root / record.storage_key
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"not-the-bundle")
    dest.chmod(FILE_MODE)
    with pytest.raises(IntegrityConflictError, match="no-clobber"):
        candidate_store.install(BUNDLE)
    assert dest.read_bytes() == b"not-the-bundle"


def test_missing_candidate_verify_fails_closed(candidate_store: CandidateObjectStore) -> None:
    digest = content_digest(b"absent-bundle-bytes")
    with pytest.raises(ObjectNotFoundError):
        candidate_store.verify(digest)


def test_write_before_reference_order(candidate_store: CandidateObjectStore) -> None:
    live: dict[str, CandidateObjectRecord] = {}

    def reference(record: CandidateObjectRecord) -> None:
        candidate_store.verify(record.bundle_digest)
        live[record.bundle_digest] = record

    installed = candidate_store.install(BUNDLE, reference=reference)
    assert installed.bundle_digest in live


def test_corrupt_candidate_fails_verify(
    candidate_store: CandidateObjectStore, layout: ControlLayout
) -> None:
    record = candidate_store.install(BUNDLE)
    dest = layout.candidates_root / record.storage_key
    dest.write_bytes(BUNDLE + b"x")
    dest.chmod(FILE_MODE)
    with pytest.raises(IntegrityConflictError):
        candidate_store.verify(record.bundle_digest)
