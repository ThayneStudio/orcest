"""Content-addressed Candidate artifact object store.

Artifact objects are SHA-256 of the exact installed bundle bytes (not the
Workflow Blob domain-separated formula). Storage keys have the exact form
``objects/sha256/<first-two-hex>/<64-hex>.bundle`` and stay below the
Candidate root. Inventory on disk is not Candidate authority; a later SQLite
``candidates`` / ``artifact_objects`` reference may be created only after
this store has promoted the exact durable file.

Git bundle structure, tip, and base-ancestry checks belong to Candidate
transfer (issue #681). This leaf owns byte identity, no-clobber promotion,
quota, and exact-object read/verify.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from orcest.workflow_contract.v1.digest import content_digest, require_valid_content_digest
from orcest.workflow_store.v1.errors import (
    IntegrityConflictError,
    ObjectNotFoundError,
    QuotaExceededError,
)
from orcest.workflow_store.v1.fs import (
    ControlLayout,
    QuotaConfig,
    StorageLock,
    default_free_bytes,
    digest_hex,
    fsync_dir,
    promote_no_clobber,
    quarantine_file,
    read_exact_file,
    trusted_join,
    write_incoming_bytes,
)


@dataclass(frozen=True, slots=True)
class CandidateObjectRecord:
    """Exact-object identity for an installed Candidate bundle file."""

    bundle_digest: str
    byte_length: int
    storage_key: str

    def __post_init__(self) -> None:
        require_valid_content_digest(self.bundle_digest, field="bundle_digest")
        if self.byte_length < 1:
            raise IntegrityConflictError("Candidate byte_length must be positive")
        hex_part = digest_hex(self.bundle_digest)
        expected_key = f"objects/sha256/{hex_part[:2]}/{hex_part}.bundle"
        if self.storage_key != expected_key:
            raise IntegrityConflictError("Candidate storage_key does not match digest")


class CandidateObjectStore:
    """Durable CAS for Candidate bundle bytes, keyed by SHA-256 of those bytes."""

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
        self._root = layout.candidates_root
        self._incoming = trusted_join(self._root, "incoming")
        self._objects = trusted_join(self._root, "objects")

    def identity(self, bundle_bytes: bytes) -> CandidateObjectRecord:
        digest = content_digest(bundle_bytes)
        hex_part = digest_hex(digest)
        return CandidateObjectRecord(
            bundle_digest=digest,
            byte_length=len(bundle_bytes),
            storage_key=f"objects/sha256/{hex_part[:2]}/{hex_part}.bundle",
        )

    def _dest(self, record: CandidateObjectRecord) -> Path:
        hex_part = digest_hex(record.bundle_digest)
        return trusted_join(self._root, "objects", "sha256", hex_part[:2], f"{hex_part}.bundle")

    def stage_upload_bytes(self, bundle_bytes: bytes) -> tuple[str, CandidateObjectRecord]:
        """Durably stage complete upload bytes without making them a live artifact."""
        if len(bundle_bytes) < 1:
            raise QuotaExceededError("object byte length must be positive")
        record = self.identity(bundle_bytes)
        incoming = write_incoming_bytes(
            self._incoming,
            bundle_bytes,
            store_root=self._root,
            quota=self._quota,
            usage_root=self._root,
            free_space=self._free_space,
        )
        return (incoming.relative_to(self._root).as_posix(), record)

    def read_staged(self, incoming_path: str) -> bytes:
        path = self._incoming_path(incoming_path)
        return read_exact_file(path, max_bytes=self._quota.max_object_bytes)

    def discard_staged(self, incoming_path: str) -> None:
        path = self._incoming_path(incoming_path)
        try:
            os.unlink(path)
        except FileNotFoundError:
            return
        fsync_dir(path.parent)

    def promote_staged(
        self, incoming_path: str, expected: CandidateObjectRecord
    ) -> CandidateObjectRecord:
        """Promote a previously staged upload with no clobber and exact-byte verify."""
        data = self.read_staged(incoming_path)
        actual = self.identity(data)
        if actual != expected:
            raise IntegrityConflictError("staged Candidate upload does not match expected identity")
        with self._lock:
            dest = self._dest(expected)
            promote_no_clobber(
                incoming=self._incoming_path(incoming_path),
                dest=dest,
                incoming_dir=self._incoming,
                store_root=self._root,
                expected=data,
            )
            return self._verify_at(dest, expected)

    def promote_staged_with_reference(
        self,
        incoming_path: str,
        expected: CandidateObjectRecord,
        *,
        reference: Callable[[CandidateObjectRecord], None],
    ) -> CandidateObjectRecord:
        """Promote staged bytes and create the SQLite reference under the same lock."""
        data = self.read_staged(incoming_path)
        actual = self.identity(data)
        if actual != expected:
            raise IntegrityConflictError("staged Candidate upload does not match expected identity")
        with self._lock:
            dest = self._dest(expected)
            result = promote_no_clobber(
                incoming=self._incoming_path(incoming_path),
                dest=dest,
                incoming_dir=self._incoming,
                store_root=self._root,
                expected=data,
            )
            verified = self._verify_at(dest, expected)
            try:
                reference(verified)
            except BaseException:
                if result.created:
                    try:
                        os.unlink(dest)
                    except FileNotFoundError:
                        pass
                    fsync_dir(dest.parent)
                raise
            return verified

    def _incoming_path(self, incoming_path: str) -> Path:
        parts = incoming_path.split("/")
        if len(parts) != 2 or parts[0] != "incoming":
            raise IntegrityConflictError("Candidate upload incoming path is invalid")
        return trusted_join(self._root, parts[0], parts[1])

    def install(
        self,
        bundle_bytes: bytes,
        *,
        reference: Callable[[CandidateObjectRecord], None] | None = None,
    ) -> CandidateObjectRecord:
        """Write-before-reference install of one immutable bundle file."""
        if len(bundle_bytes) < 1:
            raise QuotaExceededError("object byte length must be positive")
        record = self.identity(bundle_bytes)
        incoming = write_incoming_bytes(
            self._incoming,
            bundle_bytes,
            store_root=self._root,
            quota=self._quota,
            usage_root=self._root,
            free_space=self._free_space,
        )
        with self._lock:
            dest = self._dest(record)
            promote_no_clobber(
                incoming=incoming,
                dest=dest,
                incoming_dir=self._incoming,
                store_root=self._root,
                expected=bundle_bytes,
            )
            verified = self._verify_at(dest, record)
            if reference is not None:
                reference(verified)
            return verified

    def verify(self, bundle_digest: str) -> CandidateObjectRecord:
        require_valid_content_digest(bundle_digest, field="bundle_digest")
        hex_part = digest_hex(bundle_digest)
        path = trusted_join(self._root, "objects", "sha256", hex_part[:2], f"{hex_part}.bundle")
        if not path.is_file():
            raise ObjectNotFoundError("Candidate object is not installed")
        data = read_exact_file(path, max_bytes=self._quota.max_object_bytes)
        actual = self.identity(data)
        if actual.bundle_digest != bundle_digest:
            raise IntegrityConflictError("Candidate bundle digest mismatch")
        return actual

    def read(self, bundle_digest: str) -> bytes:
        record = self.verify(bundle_digest)
        return read_exact_file(self._dest(record), max_bytes=self._quota.max_object_bytes)

    def installed_mtime_ms(self, bundle_digest: str) -> int:
        """Filesystem modification time of an installed object, for GC's
        age-based orphan grace period (persistence-and-recovery.md
        "Retention and garbage collection"). Never trusted as lifecycle
        authority -- only as a physical age floor before the reference
        recheck that must gate every deletion."""
        record = self.verify(bundle_digest)
        return int(self._dest(record).stat().st_mtime * 1000)

    def quarantine(self, bundle_digest: str) -> None:
        """Move an installed object out of the live CAS into quarantine.

        Callers MUST already hold ``self._lock`` (the shared storage
        mutation lock) and MUST have just rechecked there is no live
        database reference to ``bundle_digest`` -- this method performs no
        reference check of its own.
        """
        record = self.verify(bundle_digest)
        dest = self._dest(record)
        quarantine_dir = trusted_join(self._root, "quarantine")
        quarantine_file(src=dest, quarantine_dir=quarantine_dir, store_root=self._root)

    def iter_objects(self) -> Iterator[CandidateObjectRecord]:
        objects_sha = trusted_join(self._root, "objects", "sha256")
        if not objects_sha.is_dir():
            return
        for shard in sorted(objects_sha.iterdir()):
            if not shard.is_dir() or shard.is_symlink():
                continue
            for child in sorted(shard.iterdir()):
                if not child.is_file() or child.is_symlink():
                    continue
                if not child.name.endswith(".bundle"):
                    continue
                digest = f"sha256:{child.name[: -len('.bundle')]}"
                yield self.verify(digest)

    def _verify_at(self, path: Path, expected: CandidateObjectRecord) -> CandidateObjectRecord:
        data = read_exact_file(path, max_bytes=self._quota.max_object_bytes)
        actual = self.identity(data)
        if (
            actual.bundle_digest != expected.bundle_digest
            or actual.byte_length != expected.byte_length
        ):
            raise IntegrityConflictError("Candidate object failed exact-object verify")
        return actual
