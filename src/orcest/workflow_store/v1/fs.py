"""Filesystem layout, storage lock, quota, fsync, and no-clobber promotion.

Normative layout (persistence-and-recovery.md):

    /var/lib/orcest/control/                 mode 0700
    ├── controller.lock                     mode 0600
    ├── storage.lock                        mode 0600
    ├── workflow.db                         mode 0600   (owned by the run-store leaf)
    ├── candidates/                         mode 0700
    │   ├── objects/sha256/ab/<64-hex>.bundle
    │   ├── incoming/
    │   └── quarantine/
    └── secrets/                            mode 0700
        ├── <secret-id>/versions/<version>
        ├── incoming/
        └── quarantine/

This module also creates ``blobs/`` with the same incoming/objects/quarantine
shape so Workflow Blob bytes can be made durable with the same no-clobber
fsync/promotion protocol before any later SQLite Snapshot reference. Blob
*identity* remains the domain-separated digest from the contract registry.

Lock order: acquire ``storage.lock`` before any SQLite transaction that
creates, removes, or audits reachability. Never wait for ``storage.lock``
while a SQLite transaction is open. Byte staging that cannot create a live
reference MAY run outside the lock; every authority/current-reference check
is repeated after acquiring it.
"""

from __future__ import annotations

import fcntl
import os
import re
import shutil
import stat
import threading
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from orcest.workflow_store.v1.errors import (
    IntegrityConflictError,
    LayoutError,
    QuotaExceededError,
    StorageLockError,
)

DEFAULT_CONTROL_ROOT = Path("/var/lib/orcest/control")

_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9._-]+$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")

DIR_MODE = 0o700
FILE_MODE = 0o600

_PATH_THREAD_LOCKS_GUARD = threading.Lock()
_PATH_THREAD_LOCKS: dict[str, threading.Lock] = {}


def _thread_lock_for(path: Path) -> threading.Lock:
    """One in-process lock per storage.lock path so concurrent writers serialize.

    Re-entry is tracked per ``StorageLock`` instance (``_depth``), not via
    ``RLock``, so a second instance on the same path cannot sneak in on the
    same thread.
    """
    key = str(Path(path).absolute())
    with _PATH_THREAD_LOCKS_GUARD:
        lock = _PATH_THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PATH_THREAD_LOCKS[key] = lock
        return lock


def digest_hex(digest: str) -> str:
    """Return the 64-hex suffix of a ``sha256:<hex>`` digest."""
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise IntegrityConflictError("content digest is not sha256-prefixed")
    hex_part = digest[7:]
    if not _HEX64.fullmatch(hex_part):
        raise IntegrityConflictError("content digest hex is not 64 lowercase chars")
    return hex_part


def trusted_join(root: Path, *parts: str) -> Path:
    """Join ``parts`` under ``root``, rejecting ``..``, absolute, and empty components."""
    if not parts:
        raise LayoutError("trusted_join requires at least one path component")
    for part in parts:
        if not isinstance(part, str) or not _SAFE_COMPONENT.fullmatch(part):
            raise LayoutError("unsafe path component")
    dest = root.joinpath(*parts)
    try:
        dest.relative_to(root)
    except ValueError as exc:
        raise LayoutError("path escapes store root") from exc
    return dest


def assert_no_symlink(path: Path) -> None:
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(st.st_mode):
        raise LayoutError("symlink is forbidden on the control filesystem")


def lstat_regular_file(path: Path, *, expected_mode: int | None = FILE_MODE) -> os.stat_result:
    try:
        st = os.lstat(path)
    except FileNotFoundError as exc:
        raise IntegrityConflictError("object file is missing") from exc
    if stat.S_ISLNK(st.st_mode):
        raise IntegrityConflictError("object path is a symlink")
    if not stat.S_ISREG(st.st_mode):
        raise IntegrityConflictError("object path is not a regular file")
    if expected_mode is not None and stat.S_IMODE(st.st_mode) != expected_mode:
        raise IntegrityConflictError("object file mode is not 0600")
    return st


def fsync_fd(fd: int) -> None:
    os.fsync(fd)


def fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def mkdir_durable(path: Path, *, stop: Path, mode: int = DIR_MODE) -> None:
    """Create ``path`` and missing parents under ``stop``, fsyncing after each mkdir."""
    if path == stop:
        return
    try:
        path.relative_to(stop)
    except ValueError as exc:
        raise LayoutError("mkdir path escapes stop directory") from exc
    missing: list[Path] = []
    current = path
    while current != stop:
        assert_no_symlink(current)
        try:
            st = os.lstat(current)
        except FileNotFoundError:
            missing.append(current)
            current = current.parent
            continue
        if not stat.S_ISDIR(st.st_mode):
            raise LayoutError("expected a directory")
        break
    for directory in reversed(missing):
        try:
            os.mkdir(directory, mode)
        except FileExistsError:
            assert_no_symlink(directory)
            if not directory.is_dir():
                raise LayoutError("expected a directory")
        os.chmod(directory, mode)
        fsync_dir(directory.parent)


def write_exclusive_file(path: Path, data: bytes, *, mode: int = FILE_MODE) -> None:
    """O_EXCL create, write all bytes, fchmod, fsync-file, close, fsync-dir.

    Unlinks on a write/fchmod/fsync-file failure. The parent directory is
    fsynced after a successful close so a crash cannot drop the new dirent
    (and, for files like the Secret Store integrity key, silently recreate
    a different object on the next start).
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, mode)
    try:
        view = memoryview(data)
        written = 0
        while written < len(view):
            written += os.write(fd, view[written:])
        os.fchmod(fd, mode)
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    os.close(fd)
    fsync_dir(path.parent)


def read_exact_file(path: Path, *, max_bytes: int) -> bytes:
    """Read a regular 0600 file through O_NOFOLLOW, rejecting oversize objects."""
    if max_bytes < 1:
        raise QuotaExceededError("max_bytes must be positive")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except FileNotFoundError as exc:
        raise IntegrityConflictError("object file is missing") from exc
    try:
        st = os.fstat(fd)
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
            raise IntegrityConflictError("object path is not a regular file")
        if st.st_size > max_bytes:
            raise IntegrityConflictError("object exceeds max_object_bytes")
        if st.st_size < 1:
            raise IntegrityConflictError("object byte length must be positive")
        chunks: list[bytes] = []
        remaining = st.st_size
        while remaining > 0:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) != st.st_size:
            raise IntegrityConflictError("short read of durable object")
        return data
    finally:
        os.close(fd)


def no_clobber_link(src: Path, dest: Path) -> None:
    """Atomically publish ``src`` at ``dest``. Fails if ``dest`` already exists."""
    os.link(src, dest, follow_symlinks=False)


@dataclass(frozen=True, slots=True)
class PromoteResult:
    dest: Path
    created: bool
    byte_length: int


def promote_no_clobber(
    *,
    incoming: Path,
    dest: Path,
    incoming_dir: Path,
    store_root: Path,
    expected: bytes,
) -> PromoteResult:
    """Link ``incoming`` onto ``dest`` without replacement, then fsync directories.

    If ``dest`` already exists, reopen it through the trusted path and require
    exact byte identity. A mismatch fails closed and never clobbers ``dest``.
    """
    mkdir_durable(dest.parent, stop=store_root)
    try:
        no_clobber_link(incoming, dest)
    except FileExistsError:
        existing = read_exact_file(dest, max_bytes=max(len(expected), 1))
        if existing != expected:
            raise IntegrityConflictError("no-clobber destination already holds different bytes")
        try:
            os.unlink(incoming)
        except FileNotFoundError:
            pass
        fsync_dir(incoming_dir)
        return PromoteResult(dest=dest, created=False, byte_length=len(expected))
    fsync_file(dest)
    fsync_dir(dest.parent)
    fsync_dir(incoming_dir)
    try:
        os.unlink(incoming)
    except FileNotFoundError:
        pass
    fsync_dir(incoming_dir)
    return PromoteResult(dest=dest, created=True, byte_length=len(expected))


def unique_incoming_path(incoming_dir: Path) -> Path:
    return trusted_join(incoming_dir, str(uuid.uuid4()))


def quarantine_file(*, src: Path, quarantine_dir: Path, store_root: Path) -> Path:
    """Rename ``src`` into quarantine with a fresh name; fsync the directories."""
    mkdir_durable(quarantine_dir, stop=store_root)
    dest = trusted_join(quarantine_dir, str(uuid.uuid4()))
    os.rename(src, dest)
    fsync_dir(quarantine_dir)
    fsync_dir(src.parent)
    return dest


def directory_usage(path: Path) -> tuple[int, int]:
    """Return ``(total_bytes, regular_file_count)`` under ``path`` without following links."""
    total = 0
    files = 0
    if not path.exists():
        return 0, 0
    for dirpath, dirnames, filenames in os.walk(path, followlinks=False):
        base = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not (base / name).is_symlink()]
        for name in filenames:
            child = base / name
            try:
                st = os.lstat(child)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(st.st_mode):
                total += st.st_size
                files += 1
    return total, files


def default_free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


@dataclass(frozen=True, slots=True)
class QuotaConfig:
    """Reject bytes before they are accepted into incoming or live storage."""

    min_free_bytes: int
    max_object_bytes: int
    max_store_bytes: int | None = None
    max_objects: int | None = None

    def __post_init__(self) -> None:
        if self.min_free_bytes < 0:
            raise ValueError("min_free_bytes must be nonnegative")
        if self.max_object_bytes < 1:
            raise ValueError("max_object_bytes must be positive")
        if self.max_store_bytes is not None and self.max_store_bytes < 1:
            raise ValueError("max_store_bytes must be positive when set")
        if self.max_objects is not None and self.max_objects < 1:
            raise ValueError("max_objects must be positive when set")


def check_quota(
    *,
    incoming_bytes: int,
    quota: QuotaConfig,
    free_bytes: int,
    current_store_bytes: int,
    current_objects: int,
) -> None:
    """Fail closed before writing if the object would violate quota or free space."""
    if incoming_bytes < 1:
        raise QuotaExceededError("object byte length must be positive")
    if incoming_bytes > quota.max_object_bytes:
        raise QuotaExceededError("object exceeds max_object_bytes")
    if free_bytes < incoming_bytes:
        raise QuotaExceededError("insufficient free space for object")
    if free_bytes - incoming_bytes < quota.min_free_bytes:
        raise QuotaExceededError("free space would fall below the configured safety floor")
    if (
        quota.max_store_bytes is not None
        and current_store_bytes + incoming_bytes > quota.max_store_bytes
    ):
        raise QuotaExceededError("store quota exceeded")
    if quota.max_objects is not None and current_objects + 1 > quota.max_objects:
        raise QuotaExceededError("object count quota exceeded")


@dataclass(frozen=True, slots=True)
class ControlLayout:
    """Resolved control-root paths. Call :meth:`initialize` before use."""

    root: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root).absolute())

    @property
    def storage_lock_path(self) -> Path:
        return self.root / "storage.lock"

    @property
    def controller_lock_path(self) -> Path:
        return self.root / "controller.lock"

    @property
    def candidates_root(self) -> Path:
        return self.root / "candidates"

    @property
    def secrets_root(self) -> Path:
        return self.root / "secrets"

    @property
    def blobs_root(self) -> Path:
        return self.root / "blobs"

    def initialize(self) -> None:
        root = self.root
        if root.exists():
            assert_no_symlink(root)
            if not root.is_dir():
                raise LayoutError("control root exists and is not a directory")
        else:
            parent = root.parent
            if not parent.exists():
                raise LayoutError("control root parent does not exist")
            os.mkdir(root, DIR_MODE)
        os.chmod(root, DIR_MODE)
        rel_dirs = (
            ("candidates",),
            ("candidates", "objects"),
            ("candidates", "objects", "sha256"),
            ("candidates", "incoming"),
            ("candidates", "quarantine"),
            ("secrets",),
            ("secrets", "incoming"),
            ("secrets", "quarantine"),
            ("blobs",),
            ("blobs", "objects"),
            ("blobs", "objects", "sha256"),
            ("blobs", "incoming"),
            ("blobs", "quarantine"),
        )
        for parts in rel_dirs:
            path = trusted_join(root, *parts)
            if path.exists():
                assert_no_symlink(path)
                if not path.is_dir():
                    raise LayoutError("layout path exists and is not a directory")
            else:
                os.mkdir(path, DIR_MODE)
            os.chmod(path, DIR_MODE)
            fsync_dir(path.parent)
        for name in ("storage.lock", "controller.lock"):
            lock_path = trusted_join(root, name)
            if lock_path.exists():
                lstat_regular_file(lock_path, expected_mode=FILE_MODE)
            else:
                write_exclusive_file(lock_path, b"", mode=FILE_MODE)
            os.chmod(lock_path, FILE_MODE)
        fsync_dir(root)


class StorageLock:
    """Controller-owned storage mutation lock backed by ``storage.lock``.

    Combines a process-level ``fcntl.flock`` with a reentrant thread lock so
    concurrent in-process writers and cross-process writers both fail closed
    (they serialize or, in nonblocking mode, error) rather than clobbering.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._thread = _thread_lock_for(self._path)
        self._fd: int | None = None
        self._depth = 0

    def acquire(self, *, blocking: bool = True) -> bool:
        if self._depth > 0:
            self._depth += 1
            return True
        if not self._thread.acquire(blocking=blocking):
            return False
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
        fd = os.open(self._path, flags, FILE_MODE)
        try:
            os.fchmod(fd, FILE_MODE)
            flock_flags = fcntl.LOCK_EX if blocking else (fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, flock_flags)
        except BlockingIOError:
            os.close(fd)
            self._thread.release()
            return False
        except BaseException:
            os.close(fd)
            self._thread.release()
            raise
        self._fd = fd
        self._depth = 1
        return True

    def release(self) -> None:
        if self._depth < 1:
            raise StorageLockError("storage.lock is not held")
        if self._depth == 1:
            fd = self._fd
            self._fd = None
            try:
                if fd is not None:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
            finally:
                self._depth = 0
                self._thread.release()
            return
        self._depth -= 1

    def held(self) -> bool:
        return self._depth > 0

    def __enter__(self) -> StorageLock:
        if not self.acquire(blocking=True):
            raise StorageLockError("failed to acquire storage.lock")
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


def write_incoming_bytes(
    incoming_dir: Path,
    data: bytes,
    *,
    store_root: Path,
    quota: QuotaConfig,
    usage_root: Path,
    free_space: Callable[[Path], int],
) -> Path:
    """Quota-check, then durable-write ``data`` to a unique incoming file."""
    current_bytes, current_objects = directory_usage(usage_root)
    check_quota(
        incoming_bytes=len(data),
        quota=quota,
        free_bytes=free_space(usage_root),
        current_store_bytes=current_bytes,
        current_objects=current_objects,
    )
    mkdir_durable(incoming_dir, stop=store_root)
    incoming = unique_incoming_path(incoming_dir)
    write_exclusive_file(incoming, data, mode=FILE_MODE)
    fsync_dir(incoming_dir)
    return incoming


def iter_regular_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not (base / name).is_symlink()]
        for name in filenames:
            child = base / name
            try:
                st = os.lstat(child)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(st.st_mode):
                yield child
