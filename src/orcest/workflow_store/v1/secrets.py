"""Controller-only Secret Store adapter.

A Secret Reference is ``(secret_id, version)``. Secret values, keyed
integrity tags, and unkeyed secret-derived digests never enter SQLite,
Redis, logs, exception messages, or ordinary API bodies. SQLite may store
only the opaque attestation UUID returned here.

Write-before-reference: the controller-only integrity metadata is made
durable first, then the immutable version file, both via no-clobber
promotion under ``storage.lock``, before the optional reference callback
(the later SQLite Secret Version insert) runs. A crash cannot leave a live
reference to missing bytes. If a version value is later present without its
mandatory metadata, the store fails closed so operator repair can preserve
the original opaque attestation identity.
"""

from __future__ import annotations

import hmac
import json
import os
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from orcest.workflow_contract.v1.canonical import canonical_json_bytes, canonical_json_text
from orcest.workflow_contract.v1.digest import (
    secret_staging_attestation,
    secret_version_integrity_tag,
)
from orcest.workflow_contract.v1.identity import require_lowercase_uuid
from orcest.workflow_store.v1.errors import IntegrityConflictError, ObjectNotFoundError
from orcest.workflow_store.v1.fs import (
    FILE_MODE,
    ControlLayout,
    QuotaConfig,
    StorageLock,
    default_free_bytes,
    fsync_dir,
    lstat_regular_file,
    mkdir_durable,
    promote_no_clobber,
    quarantine_file,
    read_exact_file,
    trusted_join,
    write_exclusive_file,
    write_incoming_bytes,
)

INTEGRITY_KEY_BYTES = 32
INTEGRITY_KEY_NAME = "integrity.key"


def _require_positive_version(version: int) -> int:
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise IntegrityConflictError("secret version must be a positive integer")
    return version


def _require_int(value: object, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise IntegrityConflictError(f"{field} must be an integer")
    return value


@dataclass(frozen=True, slots=True)
class SecretReference:
    """Versioned opaque Secret Reference. Never carries the secret value."""

    secret_id: str
    version: int

    def __post_init__(self) -> None:
        require_lowercase_uuid(self.secret_id, field="secret_id")
        _require_positive_version(self.version)

    def to_json(self) -> dict[str, str | int]:
        return {"secret_id": self.secret_id, "version": self.version}

    def __repr__(self) -> str:
        return f"SecretReference(secret_id={self.secret_id!r}, version={self.version})"


@dataclass(frozen=True, slots=True)
class SecretVersionHandle:
    """Installed Secret Version metadata suitable for SQLite/API/logs."""

    reference: SecretReference
    byte_length: int
    attestation_id: str
    storage_key: str

    def __post_init__(self) -> None:
        require_lowercase_uuid(self.attestation_id, field="attestation_id")
        if self.byte_length < 1:
            raise IntegrityConflictError("secret byte_length must be positive")

    def to_json(self) -> dict[str, str | int]:
        return {
            "secret_id": self.reference.secret_id,
            "version": self.reference.version,
            "byte_length": self.byte_length,
            "attestation_id": self.attestation_id,
            "storage_key": self.storage_key,
        }

    def __repr__(self) -> str:
        return (
            "SecretVersionHandle("
            f"secret_id={self.reference.secret_id!r}, version={self.reference.version}, "
            f"byte_length={self.byte_length}, attestation_id={self.attestation_id!r})"
        )


@dataclass(frozen=True, slots=True)
class SecretStagingHandle:
    """Incoming staging identity. Contains no secret bytes or keyed tag."""

    staging_id: str
    byte_length: int
    attestation_id: str

    def __post_init__(self) -> None:
        require_lowercase_uuid(self.staging_id, field="staging_id")
        require_lowercase_uuid(self.attestation_id, field="attestation_id")
        if self.byte_length < 1:
            raise IntegrityConflictError("staged secret byte_length must be positive")

    def __repr__(self) -> str:
        return (
            "SecretStagingHandle("
            f"staging_id={self.staging_id!r}, byte_length={self.byte_length}, "
            f"attestation_id={self.attestation_id!r})"
        )


class SecretStore:
    """Local durable Secret Store. Values are revealed only by :meth:`read_value`."""

    def __init__(
        self,
        layout: ControlLayout,
        *,
        quota: QuotaConfig,
        lock: StorageLock,
        free_space: Callable[[Path], int] = default_free_bytes,
    ) -> None:
        self._layout = layout
        self._quota = quota
        self._lock = lock
        self._free_space = free_space
        self._root = layout.secrets_root
        self._incoming = trusted_join(self._root, "incoming")
        self._quarantine = trusted_join(self._root, "quarantine")
        self._key_path = trusted_join(self._root, INTEGRITY_KEY_NAME)
        self._integrity_key = self._load_or_create_key()

    def _load_or_create_key(self) -> bytes:
        mkdir_durable(self._root, stop=self._layout.root)
        try:
            write_exclusive_file(self._key_path, os.urandom(INTEGRITY_KEY_BYTES), mode=FILE_MODE)
        except FileExistsError:
            pass
        # Persist the dirent even if we found a key from a previous start that
        # fsynced the file but crashed before the directory fsync.
        fsync_dir(self._root)
        key = read_exact_file(self._key_path, max_bytes=INTEGRITY_KEY_BYTES)
        if len(key) != INTEGRITY_KEY_BYTES:
            raise IntegrityConflictError("secret store integrity key has the wrong length")
        lstat_regular_file(self._key_path, expected_mode=FILE_MODE)
        return key

    def stage(self, value: bytes) -> SecretStagingHandle:
        """Write incoming bytes outside the live-reference path.

        Staging cannot create a live Secret Version. Quota is enforced before
        the incoming file is accepted.
        """
        staging_id = str(uuid.uuid4())
        attestation_id = str(uuid.uuid4())
        incoming = write_incoming_bytes(
            self._incoming,
            value,
            store_root=self._root,
            quota=self._quota,
            usage_root=self._root,
            free_space=self._free_space,
        )
        staged = trusted_join(self._incoming, staging_id)
        authenticator = secret_staging_attestation(
            self._integrity_key, staging_id=staging_id, secret_bytes=value
        )
        meta = {
            "staging_id": staging_id,
            "attestation_id": attestation_id,
            "byte_length": len(value),
            "authenticator": authenticator,
        }
        meta_path = trusted_join(self._incoming, f"{staging_id}.integrity")
        write_exclusive_file(meta_path, canonical_json_bytes(meta), mode=FILE_MODE)
        fsync_dir(self._incoming)
        os.rename(incoming, staged)
        fsync_dir(self._incoming)
        return SecretStagingHandle(
            staging_id=staging_id, byte_length=len(value), attestation_id=attestation_id
        )

    def put_version(
        self,
        secret_id: str,
        version: int,
        value: bytes,
        *,
        precheck: Callable[[], None] | None = None,
        reference: Callable[[SecretVersionHandle], None] | None = None,
    ) -> SecretVersionHandle:
        """Install one immutable version, then run the live-reference callback."""
        staging = self.stage(value)
        try:
            return self.promote_version(
                staging_id=staging.staging_id,
                secret_id=secret_id,
                version=version,
                precheck=precheck,
                reference=reference,
            )
        except Exception:
            try:
                self.quarantine_staging(staging.staging_id)
            except Exception:
                pass
            raise

    def promote_version(
        self,
        *,
        staging_id: str,
        secret_id: str,
        version: int,
        precheck: Callable[[], None] | None = None,
        reference: Callable[[SecretVersionHandle], None] | None = None,
    ) -> SecretVersionHandle:
        """Under ``storage.lock``: recheck, no-clobber install, then reference."""
        require_lowercase_uuid(staging_id, field="staging_id")
        require_lowercase_uuid(secret_id, field="secret_id")
        _require_positive_version(version)
        with self._lock:
            if precheck is not None:
                precheck()
            staged_path = trusted_join(self._incoming, staging_id)
            meta_path = trusted_join(self._incoming, f"{staging_id}.integrity")
            if not staged_path.is_file():
                raise ObjectNotFoundError("staged secret is not installed")
            if not meta_path.is_file():
                raise IntegrityConflictError("staged secret integrity metadata is missing")
            value = read_exact_file(staged_path, max_bytes=self._quota.max_object_bytes)
            meta = _load_integrity_meta(
                meta_path, max_bytes=self._quota.max_object_bytes, kind="staging"
            )
            expected = secret_staging_attestation(
                self._integrity_key, staging_id=staging_id, secret_bytes=value
            )
            authenticator = meta["authenticator"]
            if not isinstance(authenticator, str) or not hmac.compare_digest(
                expected, authenticator
            ):
                raise IntegrityConflictError("staged secret integrity attestation mismatch")
            if _require_int(meta["byte_length"], field="byte_length") != len(value):
                raise IntegrityConflictError("staged secret length mismatch")
            if str(meta["staging_id"]) != staging_id:
                raise IntegrityConflictError("staged secret identity mismatch")
            handle = self._install_version_locked(
                secret_id=secret_id,
                version=version,
                value=value,
                staged_path=staged_path,
                attestation_id=str(meta["attestation_id"]),
            )
            try:
                os.unlink(meta_path)
            except FileNotFoundError:
                pass
            fsync_dir(self._incoming)
            if reference is not None:
                reference(handle)
            return handle

    def stage_for_request(self, request_id: str, value: bytes) -> SecretStagingHandle:
        """Idempotently stage bytes under a caller-assigned request identity.

        Unlike :meth:`stage`, ``request_id`` -- the caller's own idempotency
        key, e.g. a Credential Rotation Request id -- replaces the internal
        random staging id. The returned attestation is then a pure function
        of ``(request_id, value)``: an exact retry after a crash or lost
        response reproduces the identical attestation without ever comparing
        or exposing raw bytes outside this store, while different bytes
        replayed under the same id fail closed as an integrity conflict.
        """
        require_lowercase_uuid(request_id, field="request_id")
        staged = trusted_join(self._incoming, request_id)
        meta_path = trusted_join(self._incoming, f"{request_id}.integrity")
        attestation_id = str(uuid.uuid4())
        authenticator = secret_staging_attestation(
            self._integrity_key, staging_id=request_id, secret_bytes=value
        )
        if meta_path.is_file():
            existing_meta = _load_integrity_meta(
                meta_path, max_bytes=self._quota.max_object_bytes, kind="staging"
            )
            existing_authenticator = existing_meta.get("authenticator")
            if not isinstance(existing_authenticator, str) or not hmac.compare_digest(
                authenticator, existing_authenticator
            ):
                raise IntegrityConflictError("staged request integrity attestation mismatch")
            if _require_int(existing_meta.get("byte_length"), field="byte_length") != len(value):
                raise IntegrityConflictError("staged request length mismatch")
            attestation_id = str(existing_meta["attestation_id"])
        else:
            meta_body = canonical_json_bytes(
                {
                    "staging_id": request_id,
                    "attestation_id": attestation_id,
                    "byte_length": len(value),
                    "authenticator": authenticator,
                }
            )
            self._promote_incoming_no_clobber(meta_path, meta_body)
        self._promote_incoming_no_clobber(staged, value)
        return SecretStagingHandle(
            staging_id=request_id, byte_length=len(value), attestation_id=attestation_id
        )

    def quarantine_request_value(self, request_id: str) -> None:
        """Reclaim only the staged value bytes for ``request_id``.

        The integrity metadata is deliberately retained (unlike
        :meth:`quarantine_staging`) so a later replay of the same
        Credential Rotation Request can still reproduce the original
        keyed request-attestation identity for audit.
        """
        require_lowercase_uuid(request_id, field="request_id")
        with self._lock:
            staged = trusted_join(self._incoming, request_id)
            if staged.exists():
                quarantine_file(src=staged, quarantine_dir=self._quarantine, store_root=self._root)

    def _promote_incoming_no_clobber(self, dest: Path, data: bytes) -> None:
        incoming = write_incoming_bytes(
            self._incoming,
            data,
            store_root=self._root,
            quota=self._quota,
            usage_root=self._root,
            free_space=self._free_space,
        )
        promote_no_clobber(
            incoming=incoming,
            dest=dest,
            incoming_dir=self._incoming,
            store_root=self._root,
            expected=data,
        )

    def quarantine_staging(self, staging_id: str) -> None:
        require_lowercase_uuid(staging_id, field="staging_id")
        with self._lock:
            staged_path = trusted_join(self._incoming, staging_id)
            meta_path = trusted_join(self._incoming, f"{staging_id}.integrity")
            if staged_path.exists():
                quarantine_file(
                    src=staged_path, quarantine_dir=self._quarantine, store_root=self._root
                )
            if meta_path.exists():
                quarantine_file(
                    src=meta_path, quarantine_dir=self._quarantine, store_root=self._root
                )

    def verify(self, secret_id: str, version: int) -> SecretVersionHandle:
        require_lowercase_uuid(secret_id, field="secret_id")
        _require_positive_version(version)
        dest = self._version_path(secret_id, version)
        meta_dest = self._integrity_path(secret_id, version)
        if not dest.is_file():
            raise ObjectNotFoundError("secret version is not installed")
        if not meta_dest.is_file():
            raise IntegrityConflictError("secret version integrity metadata is missing")
        value = read_exact_file(dest, max_bytes=self._quota.max_object_bytes)
        meta = _load_integrity_meta(
            meta_dest, max_bytes=self._quota.max_object_bytes, kind="version"
        )
        expected = secret_version_integrity_tag(
            self._integrity_key, secret_id=secret_id, version=version, secret_bytes=value
        )
        authenticator = meta["authenticator"]
        if not isinstance(authenticator, str) or not hmac.compare_digest(expected, authenticator):
            raise IntegrityConflictError("secret version integrity mismatch")
        if _require_int(meta["byte_length"], field="byte_length") != len(value):
            raise IntegrityConflictError("secret version length mismatch")
        if (
            str(meta["secret_id"]) != secret_id
            or _require_int(meta["version"], field="version") != version
        ):
            raise IntegrityConflictError("secret version identity mismatch")
        return SecretVersionHandle(
            reference=SecretReference(secret_id=secret_id, version=version),
            byte_length=len(value),
            attestation_id=str(meta["attestation_id"]),
            storage_key=f"{secret_id}/versions/{version}",
        )

    def read_value(self, secret_id: str, version: int) -> bytes:
        """Controller-only reveal. Callers MUST NOT log, persist, or emit the return."""
        handle = self.verify(secret_id, version)
        dest = self._version_path(secret_id, version)
        value = read_exact_file(dest, max_bytes=self._quota.max_object_bytes)
        if len(value) != handle.byte_length:
            raise IntegrityConflictError("secret version length changed during read")
        return value

    def iter_versions(self) -> Iterator[SecretVersionHandle]:
        for secret_dir in sorted(self._root.iterdir()):
            if not secret_dir.is_dir() or secret_dir.is_symlink():
                continue
            if secret_dir.name in {"incoming", "quarantine"}:
                continue
            versions = secret_dir / "versions"
            if not versions.is_dir() or versions.is_symlink():
                continue
            for child in sorted(versions.iterdir()):
                if not child.is_file() or child.is_symlink():
                    continue
                if not child.name.isdigit():
                    continue
                yield self.verify(secret_dir.name, int(child.name))

    def _install_version_locked(
        self,
        *,
        secret_id: str,
        version: int,
        value: bytes,
        staged_path: Path,
        attestation_id: str,
    ) -> SecretVersionHandle:
        dest = self._version_path(secret_id, version)
        meta_dest = self._integrity_path(secret_id, version)
        authenticator = secret_version_integrity_tag(
            self._integrity_key, secret_id=secret_id, version=version, secret_bytes=value
        )
        mkdir_durable(dest.parent, stop=self._root)
        mkdir_durable(meta_dest.parent, stop=self._root)
        meta_body = canonical_json_bytes(
            {
                "secret_id": secret_id,
                "version": version,
                "byte_length": len(value),
                "attestation_id": attestation_id,
                "authenticator": authenticator,
            }
        )
        if dest.is_file():
            existing_bytes = read_exact_file(dest, max_bytes=self._quota.max_object_bytes)
            if existing_bytes != value:
                raise IntegrityConflictError("secret version already exists with different bytes")
            if not meta_dest.is_file():
                raise IntegrityConflictError("secret version integrity metadata is missing")
            try:
                os.unlink(staged_path)
            except FileNotFoundError:
                pass
            return self.verify(secret_id, version)
        # dest is the completeness marker: make meta durable first so a crash
        # cannot leave a value that verify()/retry cannot read.
        if meta_dest.is_file():
            existing_meta = _load_integrity_meta(
                meta_dest, max_bytes=self._quota.max_object_bytes, kind="version"
            )
            tag = existing_meta.get("authenticator")
            if not isinstance(tag, str) or not hmac.compare_digest(authenticator, tag):
                raise IntegrityConflictError(
                    "secret version integrity metadata does not match value"
                )
            if _require_int(existing_meta.get("byte_length"), field="byte_length") != len(value):
                raise IntegrityConflictError("secret version length mismatch")
            if (
                str(existing_meta.get("secret_id", "")) != secret_id
                or _require_int(existing_meta.get("version"), field="version") != version
            ):
                raise IntegrityConflictError("secret version identity mismatch")
        else:
            self._promote_integrity_file(meta_dest, meta_body)
        promote_no_clobber(
            incoming=staged_path,
            dest=dest,
            incoming_dir=self._incoming,
            store_root=self._root,
            expected=value,
        )
        return self.verify(secret_id, version)

    def _promote_integrity_file(self, dest: Path, meta_body: bytes) -> None:
        incoming = write_incoming_bytes(
            self._incoming,
            meta_body,
            store_root=self._root,
            quota=self._quota,
            usage_root=self._root,
            free_space=self._free_space,
        )
        promote_no_clobber(
            incoming=incoming,
            dest=dest,
            incoming_dir=self._incoming,
            store_root=self._root,
            expected=meta_body,
        )

    def _version_path(self, secret_id: str, version: int) -> Path:
        return trusted_join(self._root, secret_id, "versions", str(version))

    def _integrity_path(self, secret_id: str, version: int) -> Path:
        return trusted_join(self._root, secret_id, "integrity", str(version))


def _load_integrity_meta(
    path: Path, *, max_bytes: int, kind: Literal["staging", "version"]
) -> dict[str, object]:
    if not path.is_file():
        raise IntegrityConflictError("integrity metadata is missing")
    raw = read_exact_file(path, max_bytes=max_bytes)
    text = raw.decode("utf-8")
    # Round-trip through canonical JSON so unknown/unordered fields fail closed
    # the same way other v1 envelopes do: only the exact keys we persist.
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise IntegrityConflictError("integrity metadata is not JSON") from exc
    if not isinstance(parsed, dict):
        raise IntegrityConflictError("integrity metadata is not an object")
    allowed_by_kind = {
        "staging": {"staging_id", "byte_length", "attestation_id", "authenticator"},
        "version": {"secret_id", "version", "byte_length", "attestation_id", "authenticator"},
    }
    allowed = allowed_by_kind[kind]
    extra = set(parsed) - allowed
    if extra:
        raise IntegrityConflictError("integrity metadata has unknown fields")
    if not allowed.issubset(parsed):
        raise IntegrityConflictError("integrity metadata is missing fields")
    # Re-canonicalize so a future reader can compare exact bytes if needed.
    canonical_json_text(parsed)
    return parsed
