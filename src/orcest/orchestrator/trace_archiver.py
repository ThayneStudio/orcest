"""Verbatim per-task trace archiver.

Tails the per-worker Redis output streams (``output:<worker-id>``) and
materializes one ``.jsonl`` file per task plus a ``.meta.json`` sidecar
on a filesystem path owned by the operator (NFS, local disk, anything).

Redis entries are byte-capped: lines larger than the live-tail budget are
split across consecutive stream entries (``part`` / ``parts``). This
archiver concatenates those parts so the ``.jsonl`` is the original
stream-json, not the Redis-sized fragments.

Progress is a versioned per-project state file on the archive volume, not
a Redis hash. Redis ``trace_archiver:cursors`` is read once to seed a
missing local file and is never written by the active pump. A cursor is
committed only after the archive file has been flushed and fsynced.

After a committed cursor advance, the archiver issues ``XTRIM MINID`` at
the older of that cursor and the 128th-newest stream entry so archived
prefix entries can be reclaimed under Redis ``noeviction`` while a tail
stays available for late dashboard attach. Trim is skipped when archival
is disabled, an entry was skipped, local cursor persist failed, or cursor
state is corrupt. Trim failures are retried on later passes and warn only
on state transitions. Local cursor records for streams absent longer than
32 hours are pruned, along with the matching legacy Redis hash fields.

Orcest is filesystem-agnostic: the archiver only knows about a writable
directory. ``OrchestratorConfig.trace_archive_path`` being ``None`` is the
documented "fall back to Redis logging" mode — :meth:`TraceArchiver.start`
logs the disable and returns without launching a thread.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import IO, Any

from orcest.shared.output_streams import OutputLineAssembler
from orcest.shared.redis_client import RedisClient

_CURSOR_HASH_KEY = "trace_archiver:cursors"
_CURSOR_FILENAME = ".orcest-archiver-state.json"
_CURSOR_STATE_VERSION = 1
_ALIVE_FILENAME = ".orcest_archiver_alive"
_WRITABILITY_RECHECK_SECONDS = 60
_SLEEP_BETWEEN_PASSES_SECONDS = 1.0
_XREAD_COUNT = 500
_FILE_MODE = 0o600  # archived files contain verbatim model output; restrict to owner
_DEFAULT_PROJECT = "unknown"
# Keep this many newest entries after a durable trim so a late dashboard
# attach still has a live tail. Must stay well under OUTPUT_STREAM_MAXLEN.
_RETAIN_NEWEST = 128
_STALE_CURSOR_AGE = timedelta(hours=32)
_LAST_SEEN_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# task_id is worker-supplied and used to construct filesystem paths. Reject
# anything outside a strict allowlist so a hostile or buggy task_id cannot
# escape the archive root via "../.." path-traversal. The same allowlist
# names the per-project cursor directory (Redis key_prefix).
_TASK_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime(_LAST_SEEN_FORMAT)


def _entry_id_to_iso(entry_id: str) -> str:
    """Convert a Redis stream entry-id (``"1779558629469-0"``) to UTC ISO."""
    try:
        ms = int(entry_id.split("-", 1)[0])
    except (ValueError, IndexError):
        return _now_iso()
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _entry_id_tuple(entry_id: str) -> tuple[int, int] | None:
    """Parse a Redis stream id into a comparable tuple, or None if malformed."""
    ms_str, sep, seq_str = entry_id.partition("-")
    if not sep:
        return None
    try:
        return (int(ms_str), int(seq_str or "0"))
    except ValueError:
        return None


def _id_at_or_before(candidate: str, highwater: str) -> bool:
    parsed_candidate = _entry_id_tuple(candidate)
    parsed_highwater = _entry_id_tuple(highwater)
    if parsed_candidate is None or parsed_highwater is None:
        return False
    return parsed_candidate <= parsed_highwater


def _older_entry_id(left: str, right: str) -> str | None:
    """Return the older of two Redis stream ids, or None if either is malformed."""
    parsed_left = _entry_id_tuple(left)
    parsed_right = _entry_id_tuple(right)
    if parsed_left is None or parsed_right is None:
        return None
    return left if parsed_left <= parsed_right else right


def _parse_last_seen(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, _LAST_SEEN_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def cursor_state_path(archive_path: Path, project: str) -> Path:
    """Return the per-project cursor state file path."""
    return archive_path / project / _CURSOR_FILENAME


def _project_dirname(key_prefix: str) -> str:
    if _TASK_ID_RE.match(key_prefix):
        return key_prefix
    return _DEFAULT_PROJECT


def _write_fd(fd: int, data: bytes) -> None:
    """Write ``data`` to ``fd``, looping through short writes."""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write while persisting cursor state")
        view = view[written:]


def _fsync_fd(fd: int) -> None:
    os.fsync(fd)


def _replace_file(src: Path, dst: Path) -> None:
    os.replace(src, dst)


def _fsync_directory(directory: Path) -> None:
    """Fsync a directory so a rename is durable, when the OS supports it."""
    directory_flag = getattr(os, "O_DIRECTORY", None)
    if directory_flag is None:
        return
    fd = os.open(str(directory), os.O_RDONLY | directory_flag)
    try:
        _fsync_fd(fd)
    finally:
        os.close(fd)


@dataclass(frozen=True)
class StreamCursor:
    last_id: str
    last_seen: str


class _ArchiveStatus(str, Enum):
    ARCHIVED = "archived"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class _EntryResult:
    status: _ArchiveStatus
    appended: bool = False


class CursorStateError(Exception):
    """Local cursor state is unusable; advancement is disabled."""


def _parse_cursor_state(raw: str) -> dict[str, StreamCursor]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CursorStateError(f"cursor state is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise CursorStateError("cursor state must be a JSON object")
    version = data.get("version")
    if version != _CURSOR_STATE_VERSION:
        raise CursorStateError(f"unsupported cursor state version {version!r}")
    streams = data.get("streams")
    if not isinstance(streams, dict):
        raise CursorStateError("cursor state 'streams' must be an object")
    parsed: dict[str, StreamCursor] = {}
    for name, body in streams.items():
        if not isinstance(name, str) or not name:
            raise CursorStateError("cursor state contains an invalid stream name")
        if not isinstance(body, dict):
            raise CursorStateError(f"cursor record for {name} must be an object")
        last_id = body.get("last_id")
        last_seen = body.get("last_seen")
        if not isinstance(last_id, str) or not last_id:
            raise CursorStateError(f"cursor record for {name} has invalid last_id")
        if not isinstance(last_seen, str) or not last_seen:
            raise CursorStateError(f"cursor record for {name} has invalid last_seen")
        parsed[name] = StreamCursor(last_id=last_id, last_seen=last_seen)
    return parsed


def _encode_cursor_state(streams: dict[str, StreamCursor]) -> str:
    payload = {
        "version": _CURSOR_STATE_VERSION,
        "streams": {
            name: {"last_id": cursor.last_id, "last_seen": cursor.last_seen}
            for name, cursor in sorted(streams.items())
        },
    }
    return json.dumps(payload, indent=2) + "\n"


def persist_cursor_state_file(path: Path, streams: dict[str, StreamCursor]) -> None:
    """Atomically persist cursor state with fsync of the file and parent dir.

    Order: create temp in the final file's directory; write and fsync the
    temp file; rename it; fsync the parent directory where supported.
    The file is created with mode ``0600``.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _encode_cursor_state(streams).encode("utf-8")
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _FILE_MODE)
    try:
        os.fchmod(fd, _FILE_MODE)
        _write_fd(fd, payload)
        _fsync_fd(fd)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    os.close(fd)
    try:
        _replace_file(tmp, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


class TraceArchiver:
    """Background thread that drains ``output:*`` streams to per-task files."""

    def __init__(
        self,
        redis: RedisClient,
        archive_path: str | None,
        repo_to_project: dict[str, str],
        logger: logging.Logger,
    ) -> None:
        self._redis = redis
        self._archive_path = Path(archive_path) if archive_path else None
        self._repo_to_project = dict(repo_to_project)
        self._logger = logger
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None
        self._open_files: dict[str, IO[str]] = {}
        self._open_metas: dict[str, dict[str, Any]] = {}
        self._task_to_path: dict[str, Path] = {}
        self._stream_to_current_task: dict[str, str] = {}
        self._archiver_paused = False
        self._last_writability_check: float = 0.0
        self._line_assembler = OutputLineAssembler()
        self._project = _project_dirname(redis.key_prefix)
        self._committed: dict[str, StreamCursor] = {}
        self._resume_from: dict[str, str] = {}
        self._written_highwater: dict[str, str] = {}
        self._cursors_loaded = False
        self._cursor_corrupt = False
        self._health_error: str | None = None
        self._trim_failing: set[str] = set()

    @property
    def health_error(self) -> str | None:
        """Visible failure that disables cursor advancement, if any."""
        return self._health_error

    def committed_last_id(self, stream: str) -> str | None:
        """Durably persisted last_id for ``stream``, or None if none/corrupt.

        None means the stream is not trim-eligible: either nothing has been
        committed to the local state file, or cursor state is corrupt.
        """
        if self._cursor_corrupt:
            return None
        cursor = self._committed.get(stream)
        return None if cursor is None else cursor.last_id

    def start(self) -> None:
        if self._archive_path is None:
            self._logger.info("Trace archiver disabled (trace_archive_path unset).")
            return
        if not self._probe_writability():
            self._logger.error(
                "Trace archiver disabled: %s is not writable. Verify the mount and "
                "permissions; orchestrator otherwise unaffected.",
                self._archive_path,
            )
            return
        self._logger.info("Trace archiver enabled, archiving to %s", self._archive_path)
        self._thread = threading.Thread(target=self._run, name="trace-archiver", daemon=True)
        self._thread.start()

    def shutdown(self, timeout: float = 5.0) -> None:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._close_all_open_files(status_on_unclosed="incomplete")

    def _probe_writability(self) -> bool:
        path = self._archive_path
        if path is None:
            return False
        try:
            path.mkdir(parents=True, exist_ok=True)
            alive = path / _ALIVE_FILENAME
            alive.write_text(_now_iso(), encoding="utf-8")
            try:
                alive.unlink()
            except FileNotFoundError:
                pass
            return True
        except OSError as exc:
            self._logger.warning("Trace archive path %s probe failed: %s", path, exc)
            return False

    def _run(self) -> None:
        try:
            while not self._shutdown.is_set():
                try:
                    self._maybe_recheck_writability()
                    if not self._archiver_paused:
                        self._pump_output_streams()
                except Exception:
                    self._logger.error("Trace archiver pass raised", exc_info=True)
                self._shutdown.wait(timeout=_SLEEP_BETWEEN_PASSES_SECONDS)
        finally:
            self._close_all_open_files(status_on_unclosed="incomplete")

    def _maybe_recheck_writability(self) -> None:
        now = time.monotonic()
        if now - self._last_writability_check < _WRITABILITY_RECHECK_SECONDS:
            return
        self._last_writability_check = now
        ok = self._probe_writability()
        if ok and self._archiver_paused:
            self._logger.info("Trace archive path %s recovered; resuming.", self._archive_path)
            self._archiver_paused = False
        elif not ok and not self._archiver_paused:
            self._logger.warning(
                "Trace archive path %s no longer writable; pausing archiver.",
                self._archive_path,
            )
            self._archiver_paused = True

    def _cursor_path(self) -> Path | None:
        if self._archive_path is None:
            return None
        return cursor_state_path(self._archive_path, self._project)

    def _set_health_error(self, message: str, *, exc_info: bool = False) -> None:
        self._health_error = message
        self._logger.error("%s", message, exc_info=exc_info)

    def _ensure_cursors_loaded(self) -> None:
        if self._cursors_loaded:
            return
        self._cursors_loaded = True
        path = self._cursor_path()
        if path is None:
            return
        if path.exists():
            try:
                raw = path.read_text(encoding="utf-8")
                self._committed = _parse_cursor_state(raw)
            except (OSError, CursorStateError) as exc:
                self._cursor_corrupt = True
                self._committed = {}
                self._set_health_error(
                    f"Trace archiver cursor state is corrupt at {path}; "
                    f"refusing to advance or replay: {exc}"
                )
            return
        self._seed_cursors_from_legacy_hash(path)

    def _seed_cursors_from_legacy_hash(self, path: Path) -> None:
        try:
            legacy = self._redis.hgetall(_CURSOR_HASH_KEY) or {}
        except Exception as exc:
            self._logger.warning(
                "Legacy cursor hash read failed; starting streams from 0-0: %s",
                exc,
                exc_info=True,
            )
            legacy = {}
        now = _now_iso()
        seeded: dict[str, StreamCursor] = {}
        for stream, last_id in legacy.items():
            if not isinstance(stream, str) or not stream:
                continue
            if not isinstance(last_id, str) or not last_id:
                continue
            seeded[stream] = StreamCursor(last_id=last_id, last_seen=now)
        if not seeded:
            return
        try:
            persist_cursor_state_file(path, seeded)
        except OSError as exc:
            self._logger.warning(
                "Failed to persist seeded cursor state at %s; using in-memory "
                "seed for this process only: %s",
                path,
                exc,
                exc_info=True,
            )
            self._set_health_error(f"Cursor seed persist failed at {path}: {exc}")
            self._resume_from = {name: cursor.last_id for name, cursor in seeded.items()}
            return
        self._committed = seeded

    def _xread_last_id(self, stream: str) -> str:
        cursor = self._committed.get(stream)
        if cursor is not None:
            return cursor.last_id
        return self._resume_from.get(stream, "0-0")

    def _already_written(self, stream: str, entry_id: str) -> bool:
        highwater = self._written_highwater.get(stream)
        if highwater is None:
            return False
        return _id_at_or_before(entry_id, highwater)

    def _mark_written(self, stream: str, entry_id: str) -> None:
        current = self._written_highwater.get(stream)
        if current is None or not _id_at_or_before(entry_id, current):
            self._written_highwater[stream] = entry_id

    def _persist_stream_cursor(self, stream: str, entry_id: str) -> None:
        path = self._cursor_path()
        if path is None:
            raise RuntimeError("cursor persist requested without an archive path")
        snapshot = dict(self._committed)
        snapshot[stream] = StreamCursor(last_id=entry_id, last_seen=_now_iso())
        persist_cursor_state_file(path, snapshot)
        self._committed = snapshot
        if self._health_error and not self._cursor_corrupt:
            self._health_error = None

    def _trim_minid(self, stream: str) -> str | None:
        """Return the exclusive-lower MINID bound, or None if trim is unsafe.

        The bound is the older of the committed durable cursor and the
        128th-newest stream entry. XTRIM MINID keeps that id and everything
        newer, so a late dashboard attach still sees at least 128 entries
        and trim cannot walk past unarchived data.
        """
        if self._archive_path is None or self._archiver_paused or self._cursor_corrupt:
            return None
        cursor_id = self.committed_last_id(stream)
        if cursor_id is None:
            return None
        try:
            newest = self._redis.xrevrange(stream, count=_RETAIN_NEWEST)
        except Exception:
            return None
        if len(newest) < _RETAIN_NEWEST:
            return None
        tail_id = newest[-1][0]
        return _older_entry_id(cursor_id, tail_id)

    def _note_trim_failure(self, stream: str, exc: BaseException) -> None:
        if stream in self._trim_failing:
            return
        self._trim_failing.add(stream)
        self._logger.warning(
            "Trace archiver trim failed for %s; will retry without blocking archival: %s",
            stream,
            exc,
        )

    def _note_trim_success(self, stream: str) -> None:
        if stream not in self._trim_failing:
            return
        self._trim_failing.discard(stream)
        self._logger.info("Trace archiver trim recovered for %s", stream)

    def _trim_archived_stream(self, stream: str) -> None:
        """XTRIM MINID using only the committed durable cursor plus the tail.

        Failures are retried on later passes and must not halt archival.
        """
        minid = self._trim_minid(stream)
        if minid is None:
            return
        try:
            self._redis.xtrim_minid(stream, minid)
        except Exception as exc:
            self._note_trim_failure(stream, exc)
            return
        self._note_trim_success(stream)

    def _gc_stale_cursors(self, live_streams: set[str]) -> None:
        """Drop local (and legacy Redis) cursor records for long-absent streams."""
        if self._cursor_corrupt or self._archive_path is None:
            return
        now = datetime.now(timezone.utc)
        stale: list[str] = []
        for name, cursor in self._committed.items():
            if name in live_streams:
                continue
            try:
                if self._redis.exists(name):
                    continue
            except Exception:
                continue
            last_seen = _parse_last_seen(cursor.last_seen)
            if last_seen is None:
                continue
            if now - last_seen <= _STALE_CURSOR_AGE:
                continue
            stale.append(name)
        if not stale:
            return
        path = self._cursor_path()
        if path is None:
            return
        snapshot = {name: cursor for name, cursor in self._committed.items() if name not in stale}
        try:
            persist_cursor_state_file(path, snapshot)
        except OSError as exc:
            self._logger.warning(
                "Failed to persist pruned cursor state at %s; leaving stale records: %s",
                path,
                exc,
                exc_info=True,
            )
            return
        self._committed = snapshot
        try:
            self._redis.hdel(_CURSOR_HASH_KEY, *stale)
        except Exception as exc:
            self._logger.warning(
                "Failed to remove legacy cursor hash fields %s: %s",
                stale,
                exc,
                exc_info=True,
            )

    def _pump_output_streams(self) -> None:
        self._ensure_cursors_loaded()
        if self._cursor_corrupt:
            self._logger.error(
                "Trace archiver cursor state is corrupt; not processing streams: %s",
                self._health_error,
            )
            return
        streams = self._redis.scan_iter(match="output:*")
        self._gc_stale_cursors(set(streams))
        for stream in streams:
            last_id = self._xread_last_id(stream)
            entries = self._redis.xread_after(stream, last_id, count=_XREAD_COUNT)
            trim_ok = True
            for entry_id, fields in entries:
                skip_append = self._already_written(stream, entry_id)
                try:
                    result = self._process_entry(stream, entry_id, fields, skip_append=skip_append)
                except Exception:
                    self._logger.error(
                        "Failed to archive stream=%s entry=%s",
                        stream,
                        entry_id,
                        exc_info=True,
                    )
                    result = _EntryResult(_ArchiveStatus.FAILED, appended=False)
                if result.status == _ArchiveStatus.FAILED:
                    trim_ok = False
                    if result.appended or skip_append:
                        self._mark_written(stream, entry_id)
                    self._set_health_error(
                        f"Archive failed for {stream} at {entry_id}; "
                        "halting this stream without advancing the committed cursor"
                    )
                    break
                if result.status == _ArchiveStatus.SKIPPED:
                    trim_ok = False
                if result.status == _ArchiveStatus.ARCHIVED or result.appended:
                    self._mark_written(stream, entry_id)
                try:
                    self._persist_stream_cursor(stream, entry_id)
                except Exception as exc:
                    trim_ok = False
                    if result.status == _ArchiveStatus.ARCHIVED or result.appended:
                        self._mark_written(stream, entry_id)
                    self._set_health_error(
                        f"Cursor persist failed for {stream} at {entry_id}; "
                        f"halting this stream without advancing the committed cursor: {exc}",
                        exc_info=True,
                    )
                    break
            # Trim cannot outrun archive-file and cursor fsync, and is unsafe
            # after a skip, drop, or local persist failure.
            if trim_ok:
                self._trim_archived_stream(stream)

    def _process_entry(
        self,
        stream: str,
        entry_id: str,
        fields: dict[str, str],
        skip_append: bool = False,
    ) -> _EntryResult:
        msg_type = fields.get("type", "")
        if msg_type == "task_start":
            return self._handle_task_start(stream, entry_id, fields, skip_append=skip_append)
        if msg_type == "task_end":
            return self._handle_task_end(stream, entry_id, fields, skip_append=skip_append)
        return self._handle_line(stream, fields, skip_append=skip_append)

    def _handle_task_start(
        self,
        stream: str,
        entry_id: str,
        fields: dict[str, str],
        skip_append: bool = False,
    ) -> _EntryResult:
        if self._archive_path is None:
            return _EntryResult(_ArchiveStatus.SKIPPED)
        if skip_append:
            return self._fsync_stream_archive(stream)
        self._flush_pending_line(stream)
        task_id = fields.get("task_id", "")
        if not _TASK_ID_RE.match(task_id):
            self._logger.warning(
                "Rejecting task_start with invalid task_id %r on stream %s", task_id, stream
            )
            return _EntryResult(_ArchiveStatus.SKIPPED)
        # Finalize any existing open handle for this task_id (covers duplicate
        # task_start re-delivery from cursor replay) AND any prior task on the
        # same stream that never saw a task_end (worker crash).
        if task_id in self._open_files:
            try:
                self._flush_and_fsync_archive(self._open_files[task_id])
            except OSError as exc:
                self._logger.error("fsync of prior %s before task_start failed: %s", task_id, exc)
                return _EntryResult(_ArchiveStatus.FAILED, appended=False)
            self._finalize_task(task_id, status="incomplete")
        prev = self._stream_to_current_task.get(stream)
        if prev and prev != task_id:
            prev_handle = self._open_files.get(prev)
            if prev_handle is not None:
                try:
                    self._flush_and_fsync_archive(prev_handle)
                except OSError as exc:
                    self._logger.error("fsync of prior %s before task_start failed: %s", prev, exc)
                    return _EntryResult(_ArchiveStatus.FAILED, appended=False)
            self._finalize_task(prev, status="incomplete")
        repo = fields.get("repo", "")
        project = self._repo_to_project.get(repo, "unknown")
        started_at = _entry_id_to_iso(entry_id)
        date_parts = started_at[:10].split("-")
        if len(date_parts) != 3:
            date_parts = _now_iso()[:10].split("-")
        rel = Path(project) / date_parts[0] / date_parts[1] / date_parts[2] / f"{task_id}.jsonl"
        abs_path = self._archive_path / rel
        try:
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            handle = self._open_archive_file(abs_path)
        except OSError as exc:
            self._logger.error("Open archive file %s failed: %s", abs_path, exc)
            return _EntryResult(_ArchiveStatus.FAILED, appended=False)
        self._open_files[task_id] = handle
        self._task_to_path[task_id] = abs_path
        self._stream_to_current_task[stream] = task_id
        self._open_metas[task_id] = {
            "task_id": task_id,
            "worker_id": fields.get("worker_id", ""),
            "project": project,
            "repo": repo,
            "resource_type": fields.get("resource_type", ""),
            "resource_id": fields.get("resource_id", ""),
            "provider": fields.get("provider", ""),
            "branch": fields.get("branch", ""),
            "started_at": started_at,
            "archive_path": str(rel),
        }
        self._write_index_pointer(task_id, rel.parent)
        try:
            self._flush_and_fsync_archive(handle)
        except OSError as exc:
            self._logger.error("fsync of archive file %s failed: %s", abs_path, exc)
            return _EntryResult(_ArchiveStatus.FAILED, appended=True)
        return _EntryResult(_ArchiveStatus.ARCHIVED, appended=True)

    def _handle_task_end(
        self,
        stream: str,
        entry_id: str,
        fields: dict[str, str],
        skip_append: bool = False,
    ) -> _EntryResult:
        if not skip_append:
            self._flush_pending_line(stream)
        task_id = fields.get("task_id", "")
        if not _TASK_ID_RE.match(task_id):
            return _EntryResult(_ArchiveStatus.SKIPPED)
        handle = self._open_files.get(task_id)
        if handle is None:
            return _EntryResult(_ArchiveStatus.SKIPPED)
        try:
            self._flush_and_fsync_archive(handle)
        except OSError as exc:
            self._logger.error("fsync of %s on task_end failed: %s", task_id, exc)
            return _EntryResult(_ArchiveStatus.FAILED, appended=True)
        status = fields.get("status", "")
        ended_at = _entry_id_to_iso(entry_id)
        meta = self._open_metas.get(task_id)
        if meta is not None:
            started = meta.get("started_at", "")
            meta["ended_at"] = ended_at
            if started:
                try:
                    delta = datetime.strptime(ended_at, "%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(
                        started, "%Y-%m-%dT%H:%M:%SZ"
                    )
                    meta["duration_seconds"] = int(delta.total_seconds())
                except ValueError:
                    pass
        self._finalize_task(task_id, status=status)
        if self._stream_to_current_task.get(stream) == task_id:
            self._stream_to_current_task.pop(stream, None)
        return _EntryResult(_ArchiveStatus.ARCHIVED, appended=True)

    def _handle_line(
        self, stream: str, fields: dict[str, str], skip_append: bool = False
    ) -> _EntryResult:
        if skip_append:
            return self._fsync_stream_archive(stream)
        wrote = False
        try:
            for complete in self._line_assembler.push(stream, fields):
                wrote = self._write_line(stream, complete) or wrote
        except OSError as exc:
            self._logger.error("Write to archive for stream %s failed: %s", stream, exc)
            return _EntryResult(_ArchiveStatus.FAILED, appended=wrote)
        if not wrote:
            return _EntryResult(_ArchiveStatus.SKIPPED)
        handle = self._open_handle_for_stream(stream)
        if handle is None:
            return _EntryResult(_ArchiveStatus.SKIPPED)
        try:
            self._flush_and_fsync_archive(handle)
        except OSError as exc:
            self._logger.error("fsync of archive for stream %s failed: %s", stream, exc)
            return _EntryResult(_ArchiveStatus.FAILED, appended=True)
        return _EntryResult(_ArchiveStatus.ARCHIVED, appended=True)

    def _flush_pending_line(self, stream: str) -> None:
        leftover = self._line_assembler.flush(stream)
        if leftover is not None:
            self._write_line(stream, leftover)

    def _open_handle_for_stream(self, stream: str) -> IO[str] | None:
        task_id = self._stream_to_current_task.get(stream)
        if not task_id:
            return None
        return self._open_files.get(task_id)

    def _fsync_stream_archive(self, stream: str) -> _EntryResult:
        handle = self._open_handle_for_stream(stream)
        if handle is None:
            return _EntryResult(_ArchiveStatus.ARCHIVED)
        try:
            self._flush_and_fsync_archive(handle)
        except OSError as exc:
            self._logger.error("fsync of archive for stream %s failed: %s", stream, exc)
            return _EntryResult(_ArchiveStatus.FAILED, appended=True)
        return _EntryResult(_ArchiveStatus.ARCHIVED, appended=True)

    def _write_line(self, stream: str, fields: dict[str, str]) -> bool:
        task_id = self._stream_to_current_task.get(stream)
        if not task_id:
            return False
        handle = self._open_files.get(task_id)
        if handle is None:
            return False
        line = fields.get("line", "")
        stream_tag = fields.get("stream", "")
        if stream_tag == "stderr":
            text = json.dumps({"stderr": line}) + "\n"
        else:
            text = line if line.endswith("\n") else line + "\n"
        self._append_archive_text(handle, text)
        return True

    def _open_archive_file(self, path: Path) -> IO[str]:
        # Open via os.open so we can set 0o600 atomically (the umask-respecting
        # default would be world-readable). ``buffering=1`` gives line-buffered
        # text mode so each model output line is flushed to disk immediately —
        # the whole point of the archive is crash-survivable trace recovery.
        fd = os.open(
            str(path),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            _FILE_MODE,
        )
        return os.fdopen(fd, "a", buffering=1, encoding="utf-8")

    def _append_archive_text(self, handle: IO[str], text: str) -> None:
        handle.write(text)

    def _flush_archive(self, handle: IO[str]) -> None:
        handle.flush()

    def _fsync_archive(self, handle: IO[str]) -> None:
        os.fsync(handle.fileno())

    def _flush_and_fsync_archive(self, handle: IO[str]) -> None:
        self._flush_archive(handle)
        self._fsync_archive(handle)

    def _finalize_task(self, task_id: str, status: str) -> None:
        handle = self._open_files.pop(task_id, None)
        if handle is not None:
            try:
                self._flush_and_fsync_archive(handle)
            except OSError:
                self._logger.warning("fsync of %s failed", task_id, exc_info=True)
            try:
                handle.close()
            except OSError:
                self._logger.warning("Close of %s failed", task_id, exc_info=True)
        path = self._task_to_path.pop(task_id, None)
        meta = self._open_metas.pop(task_id, None)
        if meta is None or path is None:
            return
        meta["status"] = status
        meta["archived_at"] = _now_iso()
        meta_path = path.with_suffix(".meta.json")
        try:
            self._write_file_0o600(meta_path, json.dumps(meta, indent=2) + "\n")
        except OSError:
            self._logger.warning("Writing meta sidecar %s failed", meta_path, exc_info=True)

    def _write_index_pointer(self, task_id: str, rel_dir: Path) -> None:
        if self._archive_path is None or not _TASK_ID_RE.match(task_id):
            return
        idx_dir = self._archive_path / "index" / "by-task-id" / task_id[:2]
        try:
            idx_dir.mkdir(parents=True, exist_ok=True)
            self._write_file_0o600(idx_dir / task_id, str(rel_dir) + "\n")
        except OSError:
            self._logger.warning("Index pointer write for %s failed", task_id, exc_info=True)

    @staticmethod
    def _write_file_0o600(path: Path, content: str) -> None:
        """Atomically write ``content`` to ``path`` with mode 0o600.

        ``os.open(..., 0o600)`` sets file perms atomically at creation (umask
        only restricts further; never grants more). Existing files keep their
        prior mode — we additionally ``fchmod`` to guarantee 0o600 on rewrites.
        """
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _FILE_MODE)
        try:
            os.fchmod(fd, _FILE_MODE)
        except OSError:
            os.close(fd)
            raise
        # os.fdopen takes ownership of fd; its context manager handles close.
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)

    def _close_all_open_files(self, status_on_unclosed: str) -> None:
        for stream, leftover in self._line_assembler.flush_all():
            self._write_line(stream, leftover)
        for task_id in list(self._open_files.keys()):
            self._finalize_task(task_id, status=status_on_unclosed)
