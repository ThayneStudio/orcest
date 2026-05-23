"""Verbatim per-task trace archiver.

Tails the per-worker Redis output streams (``output:<worker-id>``) and
materializes one ``.jsonl`` file per task plus a ``.meta.json`` sidecar
on a filesystem path owned by the operator (NFS, local disk, anything).

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
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

from orcest.shared.redis_client import RedisClient

_CURSOR_HASH_KEY = "trace_archiver:cursors"
_ALIVE_FILENAME = ".orcest_archiver_alive"
_WRITABILITY_RECHECK_SECONDS = 60
_SLEEP_BETWEEN_PASSES_SECONDS = 1.0
_XREAD_COUNT = 500
_FILE_MODE = 0o600  # archived files contain verbatim model output; restrict to owner

# task_id is worker-supplied and used to construct filesystem paths. Reject
# anything outside a strict allowlist so a hostile or buggy task_id cannot
# escape the archive root via "../.." path-traversal.
_TASK_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _entry_id_to_iso(entry_id: str) -> str:
    """Convert a Redis stream entry-id (``"1779558629469-0"``) to UTC ISO."""
    try:
        ms = int(entry_id.split("-", 1)[0])
    except (ValueError, IndexError):
        return _now_iso()
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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

    def _pump_output_streams(self) -> None:
        streams = self._redis.scan_iter(match="output:*")
        cursors = self._redis.hgetall(_CURSOR_HASH_KEY) or {}
        for stream in streams:
            last_id = cursors.get(stream, "0-0")
            entries = self._redis.xread_after(stream, last_id, count=_XREAD_COUNT)
            if not entries:
                continue
            # Per-entry cursor persist: if cursor write fails we stop advancing
            # this stream for the rest of this pump rather than processing more
            # entries (which the next pump would then re-process, causing
            # duplicate writes and leaking file handles).
            for entry_id, fields in entries:
                try:
                    self._process_entry(stream, entry_id, fields)
                except Exception:
                    self._logger.error(
                        "Failed to archive stream=%s entry=%s",
                        stream,
                        entry_id,
                        exc_info=True,
                    )
                try:
                    self._redis.hset(_CURSOR_HASH_KEY, stream, entry_id)
                except Exception:
                    self._logger.warning(
                        "Cursor persist failed for %s at %s; pausing this stream until next pump",
                        stream,
                        entry_id,
                        exc_info=True,
                    )
                    break

    def _process_entry(self, stream: str, entry_id: str, fields: dict[str, str]) -> None:
        msg_type = fields.get("type", "")
        if msg_type == "task_start":
            self._handle_task_start(stream, entry_id, fields)
        elif msg_type == "task_end":
            self._handle_task_end(stream, entry_id, fields)
        else:
            self._handle_line(stream, fields)

    def _handle_task_start(self, stream: str, entry_id: str, fields: dict[str, str]) -> None:
        if self._archive_path is None:
            return
        task_id = fields.get("task_id", "")
        if not _TASK_ID_RE.match(task_id):
            self._logger.warning(
                "Rejecting task_start with invalid task_id %r on stream %s", task_id, stream
            )
            return
        # Finalize any existing open handle for this task_id (covers duplicate
        # task_start re-delivery from cursor replay) AND any prior task on the
        # same stream that never saw a task_end (worker crash).
        if task_id in self._open_files:
            self._finalize_task(task_id, status="incomplete")
        prev = self._stream_to_current_task.get(stream)
        if prev and prev != task_id:
            self._finalize_task(prev, status="incomplete")
        repo = fields.get("repo", "")
        project = self._repo_to_project.get(repo, "unknown")
        started_at = _entry_id_to_iso(entry_id)
        date_parts = started_at[:10].split("-")
        if len(date_parts) != 3:
            date_parts = _now_iso()[:10].split("-")
        rel = (
            Path(project)
            / date_parts[0]
            / date_parts[1]
            / date_parts[2]
            / f"{task_id}.jsonl"
        )
        abs_path = self._archive_path / rel
        try:
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            # Open via os.open so we can set 0o600 atomically (the umask-respecting
            # default would be world-readable). ``buffering=1`` gives line-buffered
            # text mode so each model output line is flushed to disk immediately —
            # the whole point of the archive is crash-survivable trace recovery.
            fd = os.open(
                str(abs_path),
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                _FILE_MODE,
            )
            handle = os.fdopen(fd, "a", buffering=1, encoding="utf-8")
        except OSError as exc:
            self._logger.error("Open archive file %s failed: %s", abs_path, exc)
            return
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

    def _handle_task_end(self, stream: str, entry_id: str, fields: dict[str, str]) -> None:
        task_id = fields.get("task_id", "")
        if not _TASK_ID_RE.match(task_id):
            return
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

    def _handle_line(self, stream: str, fields: dict[str, str]) -> None:
        task_id = self._stream_to_current_task.get(stream)
        if not task_id:
            return
        handle = self._open_files.get(task_id)
        if handle is None:
            return
        line = fields.get("line", "")
        stream_tag = fields.get("stream", "")
        if stream_tag == "stderr":
            handle.write(json.dumps({"stderr": line}) + "\n")
        else:
            handle.write(line if line.endswith("\n") else line + "\n")

    def _finalize_task(self, task_id: str, status: str) -> None:
        handle = self._open_files.pop(task_id, None)
        if handle is not None:
            try:
                handle.flush()
                os.fsync(handle.fileno())
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
        for task_id in list(self._open_files.keys()):
            self._finalize_task(task_id, status=status_on_unclosed)
