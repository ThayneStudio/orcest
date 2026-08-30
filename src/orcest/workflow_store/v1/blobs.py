"""Domain-separated Workflow Blob durable object store.

Identity (domain-model.md):

    blob_digest = sha256:hex(SHA256(
        ascii("orcest-workflow-blob-v1") || 0x00 ||
        utf8(media_kind) || 0x00 ||
        uint64_be(byte_length) ||
        normalized_bytes
    ))

Identical bytes under different media kinds have different identities and
must not alias. The blob file is made durable (no-clobber, fsync-file,
fsync-directory, validate, atomic promotion) before any reference callback
runs, so a crash cannot leave a live reference to missing bytes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.digest import (
    require_valid_content_digest,
    workflow_blob_digest,
)
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
    promote_no_clobber,
    read_exact_file,
    trusted_join,
    write_incoming_bytes,
)

_MEDIA_KINDS = frozenset(member.value for member in enums.get_enum("workflow_blob.media_kind"))


def _parse_media_kind(media_kind: str) -> str:
    parsed = enums.parse_enum("workflow_blob.media_kind", media_kind)
    return str(parsed.value)


@dataclass(frozen=True, slots=True)
class WorkflowBlobRecord:
    """Exact-object identity for an installed Workflow Blob. Contains no payload."""

    blob_digest: str
    media_kind: str
    byte_length: int
    storage_key: str

    def __post_init__(self) -> None:
        require_valid_content_digest(self.blob_digest, field="blob_digest")
        if self.media_kind not in _MEDIA_KINDS:
            raise IntegrityConflictError("unknown Workflow Blob media kind")
        if self.byte_length < 1:
            raise IntegrityConflictError("Workflow Blob byte_length must be positive")


class WorkflowBlobStore:
    """Durable CAS for Workflow Blob bytes, keyed by domain-separated digest."""

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
        self._root = layout.blobs_root
        self._incoming = trusted_join(self._root, "incoming")
        self._objects = trusted_join(self._root, "objects")

    def identity(self, media_kind: str, normalized_bytes: bytes) -> WorkflowBlobRecord:
        kind = _parse_media_kind(media_kind)
        digest = workflow_blob_digest(kind, normalized_bytes)
        hex_part = digest_hex(digest)
        storage_key = f"objects/{kind}/sha256/{hex_part[:2]}/{hex_part}"
        return WorkflowBlobRecord(
            blob_digest=digest,
            media_kind=kind,
            byte_length=len(normalized_bytes),
            storage_key=storage_key,
        )

    def _dest(self, record: WorkflowBlobRecord) -> Path:
        hex_part = digest_hex(record.blob_digest)
        return trusted_join(
            self._root, "objects", record.media_kind, "sha256", hex_part[:2], hex_part
        )

    def install(
        self,
        media_kind: str,
        normalized_bytes: bytes,
        *,
        reference: Callable[[WorkflowBlobRecord], None] | None = None,
    ) -> WorkflowBlobRecord:
        """Write-before-reference install. ``reference`` runs only after fsync/promotion."""
        if len(normalized_bytes) < 1:
            raise QuotaExceededError("object byte length must be positive")
        record = self.identity(media_kind, normalized_bytes)
        incoming = write_incoming_bytes(
            self._incoming,
            normalized_bytes,
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
                expected=normalized_bytes,
            )
            verified = self._verify_at(dest, record)
            if reference is not None:
                reference(verified)
            return verified

    def verify(self, blob_digest: str) -> WorkflowBlobRecord:
        """Recompute the domain-separated digest; a readable file is not enough."""
        require_valid_content_digest(blob_digest, field="blob_digest")
        path, kind = self._locate(blob_digest)
        data = read_exact_file(path, max_bytes=self._quota.max_object_bytes)
        record = self.identity(kind, data)
        if record.blob_digest != blob_digest:
            raise IntegrityConflictError("Workflow Blob digest mismatch")
        return record

    def read(self, blob_digest: str) -> bytes:
        record = self.verify(blob_digest)
        return read_exact_file(self._dest(record), max_bytes=self._quota.max_object_bytes)

    def iter_objects(self) -> Iterator[WorkflowBlobRecord]:
        for kind in sorted(_MEDIA_KINDS):
            kind_root = self._root / "objects" / kind / "sha256"
            if not kind_root.is_dir():
                continue
            for shard in sorted(kind_root.iterdir()):
                if not shard.is_dir() or shard.is_symlink():
                    continue
                for child in sorted(shard.iterdir()):
                    if not child.is_file() or child.is_symlink():
                        continue
                    yield self.verify(f"sha256:{child.name}")

    def _locate(self, blob_digest: str) -> tuple[Path, str]:
        hex_part = digest_hex(blob_digest)
        found: list[tuple[Path, str]] = []
        for kind in _MEDIA_KINDS:
            path = trusted_join(self._root, "objects", kind, "sha256", hex_part[:2], hex_part)
            if path.is_file() and not path.is_symlink():
                found.append((path, kind))
        if not found:
            raise ObjectNotFoundError("Workflow Blob is not installed")
        if len(found) != 1:
            raise IntegrityConflictError("Workflow Blob digest aliases across media kinds")
        return found[0]

    def _verify_at(self, path: Path, expected: WorkflowBlobRecord) -> WorkflowBlobRecord:
        data = read_exact_file(path, max_bytes=self._quota.max_object_bytes)
        actual = self.identity(expected.media_kind, data)
        if (
            actual.blob_digest != expected.blob_digest
            or actual.media_kind != expected.media_kind
            or actual.byte_length != expected.byte_length
        ):
            raise IntegrityConflictError("Workflow Blob failed exact-object verify")
        return actual
