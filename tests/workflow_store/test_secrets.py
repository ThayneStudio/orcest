"""Secret Store references, keyed integrity, and non-leakage."""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import (
    content_digest,
    secret_version_integrity_preimage,
    secret_version_integrity_tag,
    sha256_hex,
)
from orcest.workflow_store.v1.errors import IntegrityConflictError, ObjectNotFoundError
from orcest.workflow_store.v1.fs import FILE_MODE
from orcest.workflow_store.v1.secrets import SecretStore, SecretVersionHandle

SECRET = b"super-secret-credential-value-xyz"
OTHER = b"other-secret-credential-value-aaa"


def _secret_id() -> str:
    return str(uuid.uuid4())


def test_put_version_returns_opaque_handle(secret_store: SecretStore) -> None:
    secret_id = _secret_id()
    handle = secret_store.put_version(secret_id, 1, SECRET)
    assert handle.reference.secret_id == secret_id
    assert handle.reference.version == 1
    assert handle.byte_length == len(SECRET)
    assert uuid.UUID(handle.attestation_id)
    assert SECRET not in repr(handle).encode()
    assert SECRET.decode() not in repr(handle)
    assert SECRET.decode() not in json.dumps(handle.to_json())
    assert secret_store.read_value(secret_id, 1) == SECRET
    verified = secret_store.verify(secret_id, 1)
    assert verified.attestation_id == handle.attestation_id


def test_idempotent_put_of_same_bytes_keeps_attestation(secret_store: SecretStore) -> None:
    secret_id = _secret_id()
    first = secret_store.put_version(secret_id, 1, SECRET)
    second = secret_store.put_version(secret_id, 1, SECRET)
    assert first.attestation_id == second.attestation_id
    assert secret_store.read_value(secret_id, 1) == SECRET


def test_no_clobber_different_bytes_fail_closed(secret_store: SecretStore) -> None:
    secret_id = _secret_id()
    secret_store.put_version(secret_id, 1, SECRET)
    with pytest.raises(IntegrityConflictError):
        secret_store.put_version(secret_id, 1, OTHER)
    assert secret_store.read_value(secret_id, 1) == SECRET


def test_integrity_mismatch_fails_closed(secret_store: SecretStore, layout: object) -> None:
    from orcest.workflow_store.v1.fs import ControlLayout

    assert isinstance(layout, ControlLayout)
    secret_id = _secret_id()
    secret_store.put_version(secret_id, 2, SECRET)
    dest = layout.secrets_root / secret_id / "versions" / "2"
    dest.write_bytes(OTHER)
    dest.chmod(FILE_MODE)
    with pytest.raises(IntegrityConflictError, match="integrity mismatch"):
        secret_store.verify(secret_id, 2)
    with pytest.raises(IntegrityConflictError):
        secret_store.read_value(secret_id, 2)


def test_missing_version_is_not_found(secret_store: SecretStore) -> None:
    with pytest.raises(ObjectNotFoundError):
        secret_store.verify(_secret_id(), 1)


def test_keyed_tag_is_not_unkeyed_sha256() -> None:
    key = b"controller-integrity-key-32b!!!!"
    secret_id = _secret_id()
    tag = secret_version_integrity_tag(key, secret_id=secret_id, version=1, secret_bytes=SECRET)
    assert tag != sha256_hex(SECRET)
    assert tag != content_digest(SECRET)
    other_id = secret_version_integrity_tag(
        key, secret_id=_secret_id(), version=1, secret_bytes=SECRET
    )
    assert tag != other_id
    other_ver = secret_version_integrity_tag(
        key, secret_id=secret_id, version=2, secret_bytes=SECRET
    )
    assert tag != other_ver
    preimage = secret_version_integrity_preimage(
        secret_id=secret_id, version=1, secret_bytes=SECRET
    )
    assert SECRET in preimage
    assert tag not in SECRET.decode()


def test_secret_absent_from_logs_db_pages_redis_and_exceptions(
    secret_store: SecretStore, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    secret_id = _secret_id()
    db_page = tmp_path / "workflow.db"
    redis_payloads: list[str] = []
    live: list[dict[str, object]] = []

    def reference(handle: SecretVersionHandle) -> None:
        body = handle.to_json()
        live.append(body)
        db_page.write_text(json.dumps(body), encoding="utf-8")
        redis_payloads.append(json.dumps({"secret_ref": handle.reference.to_json()}))

    handle = secret_store.put_version(secret_id, 1, SECRET, reference=reference)
    leaked = SECRET.decode()
    assert leaked not in caplog.text
    assert leaked not in db_page.read_text(encoding="utf-8")
    assert leaked.encode() not in db_page.read_bytes()
    assert all(leaked not in payload for payload in redis_payloads)
    assert leaked not in json.dumps(live)
    assert leaked not in repr(handle)
    with pytest.raises(IntegrityConflictError) as exc:
        secret_store.put_version(secret_id, 1, OTHER)
    message = str(exc.value)
    assert leaked not in message
    assert OTHER.decode() not in message


def test_precheck_runs_under_lock_before_install(secret_store: SecretStore) -> None:
    secret_id = _secret_id()
    calls: list[str] = []

    def precheck() -> None:
        calls.append("precheck")
        with pytest.raises(ObjectNotFoundError):
            secret_store.verify(secret_id, 1)

    def reference(handle: SecretVersionHandle) -> None:
        calls.append("reference")
        assert handle.reference.version == 1

    secret_store.put_version(secret_id, 1, SECRET, precheck=precheck, reference=reference)
    assert calls == ["precheck", "reference"]


def test_precheck_failure_does_not_create_version(secret_store: SecretStore) -> None:
    secret_id = _secret_id()

    def precheck() -> None:
        raise IntegrityConflictError("CAS lost")

    with pytest.raises(IntegrityConflictError, match="CAS lost"):
        secret_store.put_version(secret_id, 1, SECRET, precheck=precheck)
    with pytest.raises(ObjectNotFoundError):
        secret_store.verify(secret_id, 1)


def test_write_before_reference_failure_has_no_live_ref(secret_store: SecretStore) -> None:
    secret_id = _secret_id()
    live: list[str] = []

    def reference(handle: SecretVersionHandle) -> None:
        raise RuntimeError("crash after secret promotion before sqlite reference")

    with pytest.raises(RuntimeError, match="before sqlite"):
        secret_store.put_version(secret_id, 1, SECRET, reference=reference)
    assert live == []
    # Bytes are durable; a later retry of the exact version/bytes completes the ref.
    handle = secret_store.put_version(
        secret_id, 1, SECRET, reference=lambda h: live.append(h.attestation_id)
    )
    assert live == [handle.attestation_id]
    assert secret_store.read_value(secret_id, 1) == SECRET


def test_missing_integrity_meta_is_integrity_conflict_not_oserror(
    secret_store: SecretStore, layout: object
) -> None:
    from orcest.workflow_store.v1.fs import ControlLayout

    assert isinstance(layout, ControlLayout)
    secret_id = _secret_id()
    secret_store.put_version(secret_id, 1, SECRET)
    meta = layout.secrets_root / secret_id / "integrity" / "1"
    meta.unlink()
    with pytest.raises(IntegrityConflictError, match="integrity metadata"):
        secret_store.verify(secret_id, 1)
    with pytest.raises(IntegrityConflictError, match="integrity metadata"):
        secret_store.read_value(secret_id, 1)


def test_value_without_meta_is_repaired_on_retry(secret_store: SecretStore, layout: object) -> None:
    from orcest.workflow_store.v1.fs import ControlLayout

    assert isinstance(layout, ControlLayout)
    secret_id = _secret_id()
    secret_store.put_version(secret_id, 1, SECRET)
    meta = layout.secrets_root / secret_id / "integrity" / "1"
    meta.unlink()
    with pytest.raises(IntegrityConflictError):
        secret_store.verify(secret_id, 1)
    repaired = secret_store.put_version(secret_id, 1, SECRET)
    assert secret_store.read_value(secret_id, 1) == SECRET
    assert repaired.byte_length == len(SECRET)
    assert secret_store.verify(secret_id, 1).attestation_id == repaired.attestation_id


def test_meta_without_value_completes_on_retry(secret_store: SecretStore, layout: object) -> None:
    from orcest.workflow_store.v1.fs import ControlLayout

    assert isinstance(layout, ControlLayout)
    secret_id = _secret_id()
    first = secret_store.put_version(secret_id, 1, SECRET)
    dest = layout.secrets_root / secret_id / "versions" / "1"
    dest.unlink()
    with pytest.raises(ObjectNotFoundError):
        secret_store.verify(secret_id, 1)
    second = secret_store.put_version(secret_id, 1, SECRET)
    assert secret_store.read_value(secret_id, 1) == SECRET
    assert second.attestation_id == first.attestation_id


def test_promote_missing_staging_is_not_found(secret_store: SecretStore) -> None:
    with pytest.raises(ObjectNotFoundError, match="staged secret"):
        secret_store.promote_version(staging_id=_secret_id(), secret_id=_secret_id(), version=1)
