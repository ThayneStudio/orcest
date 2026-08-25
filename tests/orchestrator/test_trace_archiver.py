"""Tests for the verbatim per-task trace archiver."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fakeredis
import pytest
import redis

from orcest.orchestrator import trace_archiver as trace_archiver_mod
from orcest.orchestrator.trace_archiver import (
    _CURSOR_FILENAME,
    _CURSOR_HASH_KEY,
    _CURSOR_STATE_VERSION,
    _RETAIN_NEWEST,
    StreamCursor,
    TraceArchiver,
    _entry_id_to_iso,
    _entry_id_tuple,
    cursor_state_path,
    persist_cursor_state_file,
)
from orcest.shared.output_streams import iter_capped_output_fields
from orcest.shared.redis_client import RedisClient


@pytest.fixture
def archive_root(tmp_path: Path) -> Path:
    p = tmp_path / "traces"
    p.mkdir()
    return p


@pytest.fixture
def logger() -> logging.Logger:
    return logging.getLogger("test_trace_archiver")


def _xadd(redis_client, stream: str, fields: dict[str, str]) -> str:
    """Add an entry directly to a stream and return its entry id."""
    # The fakeredis client lives behind RedisClient with a "test:" key_prefix;
    # use xadd_capped to mirror what the worker does.
    return redis_client.xadd_capped(stream, fields, maxlen=20000)


class TestArchiverDisabledModes:
    def test_disabled_when_archive_path_is_none(self, fake_redis_client, logger, archive_root):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=None,
            repo_to_project={},
            logger=logger,
        )
        archiver.start()
        assert archiver._thread is None
        # Stream activity should not touch the filesystem.
        _xadd(
            fake_redis_client,
            "output:worker-1",
            {"type": "task_start", "task_id": "t1", "repo": "owner/r"},
        )
        # No pump call possible because the thread didn't start; the archiver
        # exposes only public surface here. Confirm nothing got written.
        assert list(archive_root.iterdir()) == []

    def test_self_disables_on_unwritable_archive_path(self, fake_redis_client, logger, tmp_path):
        read_only = tmp_path / "ro"
        read_only.mkdir()
        os.chmod(read_only, 0o500)  # read+execute only
        try:
            archiver = TraceArchiver(
                redis=fake_redis_client,
                archive_path=str(read_only / "nested-must-create"),
                repo_to_project={},
                logger=logger,
            )
            archiver.start()
            # Probe should fail and no thread should run.
            assert archiver._thread is None
        finally:
            os.chmod(read_only, 0o700)


class TestArchiverHappyPath:
    def test_single_task_writes_jsonl_meta_and_index(self, fake_redis_client, logger, archive_root):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        # Manually probe writability (start() would spawn a thread; here we
        # drive the loop synchronously for determinism).
        assert archiver._probe_writability()
        _xadd(
            fake_redis_client,
            "output:worker-1",
            {
                "type": "task_start",
                "task_id": "task-abc",
                "repo": "owner/r",
                "resource_type": "pr",
                "resource_id": "42",
                "provider": "claude",
                "worker_id": "worker-1",
                "branch": "fix/bug",
            },
        )
        _xadd(fake_redis_client, "output:worker-1", {"line": '{"hello": "world"}'})
        _xadd(fake_redis_client, "output:worker-1", {"line": '{"hello": "again"}'})
        _xadd(
            fake_redis_client,
            "output:worker-1",
            {"type": "task_end", "task_id": "task-abc", "status": "completed"},
        )

        archiver._pump_output_streams()

        # Locate the materialized file.
        jsonls = list(archive_root.rglob("task-abc.jsonl"))
        assert len(jsonls) == 1, f"expected 1 .jsonl, found {jsonls}"
        jsonl = jsonls[0]
        assert "myproj" in jsonl.parts
        body = jsonl.read_text()
        assert '{"hello": "world"}' in body
        assert '{"hello": "again"}' in body

        meta_path = jsonl.with_suffix(".meta.json")
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["task_id"] == "task-abc"
        assert meta["project"] == "myproj"
        assert meta["status"] == "completed"
        assert meta["resource_type"] == "pr"
        assert meta["resource_id"] == "42"
        assert meta["worker_id"] == "worker-1"
        assert "started_at" in meta
        assert "ended_at" in meta
        assert "archived_at" in meta

        # Index pointer must resolve back to the directory.
        pointer = archive_root / "index" / "by-task-id" / "ta" / "task-abc"
        assert pointer.exists()
        rel_dir = pointer.read_text().strip()
        assert (archive_root / rel_dir / "task-abc.jsonl") == jsonl

    def test_unknown_repo_falls_back_to_unknown_project(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={},  # empty map
            logger=logger,
        )
        archiver._probe_writability()
        _xadd(
            fake_redis_client,
            "output:worker-1",
            {
                "type": "task_start",
                "task_id": "t-1",
                "repo": "mystery/repo",
                "worker_id": "worker-1",
            },
        )
        _xadd(
            fake_redis_client,
            "output:worker-1",
            {"type": "task_end", "task_id": "t-1", "status": "completed"},
        )
        archiver._pump_output_streams()
        assert (archive_root / "unknown").exists()

    def test_cursor_persists_across_pumps(self, fake_redis_client, logger, archive_root):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()

        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "t-1", "repo": "owner/r"},
        )
        _xadd(fake_redis_client, "output:w1", {"line": "line-a"})
        archiver._pump_output_streams()

        state_path = cursor_state_path(archive_root, fake_redis_client.key_prefix)
        assert state_path.exists()
        first_state = json.loads(state_path.read_text())
        assert first_state["version"] == _CURSOR_STATE_VERSION
        first_cursor = first_state["streams"]["output:w1"]["last_id"]
        assert first_cursor
        assert first_state["streams"]["output:w1"]["last_seen"]
        assert fake_redis_client.hgetall(_CURSOR_HASH_KEY) == {}

        # Add new entries; cursor must advance.
        _xadd(fake_redis_client, "output:w1", {"line": "line-b"})
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "t-1", "status": "completed"},
        )
        archiver._pump_output_streams()

        second_state = json.loads(state_path.read_text())
        assert second_state["streams"]["output:w1"]["last_id"] != first_cursor
        assert fake_redis_client.hgetall(_CURSOR_HASH_KEY) == {}

        # Both lines made it to the file.
        jsonl = next(archive_root.rglob("t-1.jsonl"))
        body = jsonl.read_text()
        assert "line-a" in body
        assert "line-b" in body


class TestArchiverPartialTraces:
    def test_task_start_with_no_task_end_finalizes_as_incomplete_on_shutdown(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "crashed", "repo": "owner/r"},
        )
        _xadd(fake_redis_client, "output:w1", {"line": "halfway through"})
        archiver._pump_output_streams()
        # File should be open but meta not yet written.
        jsonl = next(archive_root.rglob("crashed.jsonl"))
        assert jsonl.exists()
        assert not jsonl.with_suffix(".meta.json").exists()

        # Simulate orchestrator shutdown.
        archiver.shutdown(timeout=0.5)
        meta = json.loads(jsonl.with_suffix(".meta.json").read_text())
        assert meta["status"] == "incomplete"

    def test_second_task_start_closes_first_as_incomplete(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "first", "repo": "owner/r"},
        )
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "second", "repo": "owner/r"},
        )
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "second", "status": "completed"},
        )
        archiver._pump_output_streams()

        first_meta = next(archive_root.rglob("first.meta.json"))
        second_meta = next(archive_root.rglob("second.meta.json"))
        assert json.loads(first_meta.read_text())["status"] == "incomplete"
        assert json.loads(second_meta.read_text())["status"] == "completed"


class TestEntryIdToIso:
    def test_converts_ms_entry_id(self):
        # 1700000000000 ms = 2023-11-14T22:13:20Z
        assert _entry_id_to_iso("1700000000000-0") == "2023-11-14T22:13:20Z"

    def test_falls_back_on_bad_input(self):
        # Should not raise; should return *some* iso-formatted string
        result = _entry_id_to_iso("not-a-real-id")
        assert result.endswith("Z")
        assert "T" in result


class TestArchiverSecurity:
    def test_rejects_path_traversal_task_id(self, fake_redis_client, logger, archive_root):
        """A worker emitting a task_id with '..' must not write outside archive_root."""
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        for bad in ("../../etc/pwn", "..", "/abs/path", "has/slash", "a/../b", ""):
            _xadd(
                fake_redis_client,
                "output:w1",
                {
                    "type": "task_start",
                    "task_id": bad,
                    "repo": "owner/r",
                },
            )
        archiver._pump_output_streams()
        # No project subdirs, no rogue files outside archive_root anywhere.
        # The per-project cursor state directory is the only allowed entry.
        contents = {p.name for p in archive_root.iterdir()}
        assert contents == {fake_redis_client.key_prefix}, (
            f"unexpected entries under archive root: {contents}"
        )
        cursor_dir = archive_root / fake_redis_client.key_prefix
        assert {p.name for p in cursor_dir.iterdir()} == {_CURSOR_FILENAME}

    def test_archived_files_are_mode_0o600(self, fake_redis_client, logger, archive_root):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "secure-task", "repo": "owner/r"},
        )
        _xadd(fake_redis_client, "output:w1", {"line": "secret payload"})
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "secure-task", "status": "completed"},
        )
        archiver._pump_output_streams()
        jsonl = next(archive_root.rglob("secure-task.jsonl"))
        meta = jsonl.with_suffix(".meta.json")
        idx = archive_root / "index" / "by-task-id" / "se" / "secure-task"
        cursor = cursor_state_path(archive_root, fake_redis_client.key_prefix)
        for f in (jsonl, meta, idx, cursor):
            mode = f.stat().st_mode & 0o777
            assert mode == 0o600, f"{f} has perms {oct(mode)}, expected 0o600"

    def test_archive_contains_only_what_redis_streamed(
        self, fake_redis_client, logger, archive_root
    ):
        """No amplification: a sentinel that appears once in Redis appears once on disk."""
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        sentinel = "SENTINEL_CREDENTIAL_FAKE_kj2dh3jh"
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "t-sentinel", "repo": "owner/r"},
        )
        _xadd(fake_redis_client, "output:w1", {"line": f"agent text {sentinel} here"})
        _xadd(fake_redis_client, "output:w1", {"line": "unrelated"})
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "t-sentinel", "status": "completed"},
        )
        archiver._pump_output_streams()
        # Verify exactly-one occurrence of the sentinel across the whole archive,
        # i.e. the archiver isn't injecting credentials from anywhere besides
        # the verbatim stream content.
        count = 0
        for f in archive_root.rglob("*"):
            if f.is_file():
                try:
                    count += f.read_text(encoding="utf-8").count(sentinel)
                except OSError:
                    continue
        assert count == 1, f"expected sentinel exactly once in archive, found {count}"


class TestArchiverDuplicateTaskStart:
    def test_duplicate_task_start_does_not_leak_handle_or_double_write(
        self, fake_redis_client, logger, archive_root
    ):
        """A re-delivered task_start for the same task_id must close the prior handle.

        Without the fix, the prior handle is leaked and subsequent lines are
        appended *twice* (once via the new handle, once via stale state in the
        old handle if it ever flushed).
        """
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "dup-task", "repo": "owner/r"},
        )
        _xadd(fake_redis_client, "output:w1", {"line": "before duplicate"})
        # Simulate cursor replay: same task_start arrives a second time.
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "dup-task", "repo": "owner/r"},
        )
        _xadd(fake_redis_client, "output:w1", {"line": "after duplicate"})
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "dup-task", "status": "completed"},
        )
        archiver._pump_output_streams()
        # Exactly one open handle remains in self._open_files for any active
        # task — and after task_end, none.
        assert "dup-task" not in archiver._open_files
        # The duplicate task_start finalizes the prior file as "incomplete"
        # and starts a fresh one. So we should see two archive runs.
        jsonls = list(archive_root.rglob("dup-task.jsonl"))
        # Both writes target the same path; the second open() in append mode
        # adds onto the first. Both "before" and "after" must appear.
        assert len(jsonls) == 1
        body = jsonls[0].read_text()
        assert "before duplicate" in body
        assert "after duplicate" in body


class TestArchiverChunkReassembly:
    def test_chunked_stream_json_is_archived_verbatim(
        self, fake_redis_client, logger, archive_root
    ):
        """Redis-capped fragments must reassemble into the original line (#585)."""
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        original = json.dumps({"type": "user", "payload": "x" * (48 * 1024)})
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "t-chunk", "repo": "owner/r"},
        )
        chunks = iter_capped_output_fields({"line": original, "task_id": "t-chunk"})
        assert len(chunks) > 1
        for chunk in chunks:
            _xadd(fake_redis_client, "output:w1", chunk)
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "t-chunk", "status": "completed"},
        )
        archiver._pump_output_streams()
        jsonl = next(archive_root.rglob("t-chunk.jsonl"))
        body = jsonl.read_text()
        assert body.count(original) == 1
        assert body.strip() == original

    def test_chunked_stderr_is_archived_as_single_json_object(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=str(archive_root),
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver._probe_writability()
        original = "stderr-payload-" + ("z" * (48 * 1024))
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_start", "task_id": "t-err", "repo": "owner/r"},
        )
        for chunk in iter_capped_output_fields(
            {"line": original, "stream": "stderr", "task_id": "t-err"}
        ):
            _xadd(fake_redis_client, "output:w1", chunk)
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "t-err", "status": "completed"},
        )
        archiver._pump_output_streams()
        jsonl = next(archive_root.rglob("t-err.jsonl"))
        records = [json.loads(line) for line in jsonl.read_text().splitlines() if line]
        assert records == [{"stderr": original}]


def _archiver(redis_client, logger, archive_root, repo_to_project=None) -> TraceArchiver:
    archiver = TraceArchiver(
        redis=redis_client,
        archive_path=str(archive_root),
        repo_to_project=repo_to_project or {"owner/r": "myproj"},
        logger=logger,
    )
    archiver._probe_writability()
    return archiver


def _count_in_archive(archive_root: Path, needle: str) -> int:
    count = 0
    for path in archive_root.rglob("*"):
        if path.is_file():
            try:
                count += path.read_text(encoding="utf-8").count(needle)
            except OSError:
                continue
    return count


def _refuse_redis_writes(redis_client: RedisClient) -> None:
    def boom(*_args: object, **_kwargs: object) -> None:
        raise redis.ResponseError("OOM command not allowed when used memory > 'maxmemory'")

    redis_client.hset = boom  # type: ignore[method-assign]
    redis_client.hdel = boom  # type: ignore[method-assign]
    redis_client.set = boom  # type: ignore[method-assign]
    redis_client.delete = boom  # type: ignore[method-assign]
    redis_client.xadd = boom  # type: ignore[method-assign]
    redis_client.xadd_capped = boom  # type: ignore[method-assign]


def _start_and_line(
    redis_client: RedisClient,
    *,
    stream: str = "output:w1",
    task_id: str = "t-dur",
    line: str = "unique-payload",
) -> None:
    _xadd(
        redis_client,
        stream,
        {"type": "task_start", "task_id": task_id, "repo": "owner/r"},
    )
    _xadd(redis_client, stream, {"line": line})


def _commit_start_then_enqueue_line(
    redis_client: RedisClient,
    archiver: TraceArchiver,
    line: str,
    *,
    stream: str = "output:w1",
    task_id: str = "t-dur",
) -> tuple[str, str]:
    """Commit task_start, then enqueue a line that has not been persisted yet."""
    start_id = _xadd(
        redis_client,
        stream,
        {"type": "task_start", "task_id": task_id, "repo": "owner/r"},
    )
    archiver._pump_output_streams()
    line_id = _xadd(redis_client, stream, {"line": line})
    return start_id, line_id


class TestArchiverCursorDurability:
    def test_redis_write_refusal_does_not_duplicate_on_retry(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "redis-write-refusal-payload"
        _start_and_line(fake_redis_client, line=payload)
        _refuse_redis_writes(fake_redis_client)

        archiver._pump_output_streams()
        archiver._pump_output_streams()

        assert _count_in_archive(archive_root, payload) == 1
        state = json.loads(
            cursor_state_path(archive_root, fake_redis_client.key_prefix).read_text()
        )
        assert "output:w1" in state["streams"]
        assert archiver.committed_last_id("output:w1") == state["streams"]["output:w1"]["last_id"]

    def test_archive_open_failure_does_not_commit_cursor(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        _start_and_line(fake_redis_client, line="open-fail-payload")

        def fail_open(_path: Path):
            raise OSError("open failed")

        monkeypatch.setattr(archiver, "_open_archive_file", fail_open)
        archiver._pump_output_streams()

        assert archiver.committed_last_id("output:w1") is None
        assert archiver.health_error is not None
        assert "open-fail-payload" not in "".join(
            p.read_text(encoding="utf-8") for p in archive_root.rglob("*.jsonl")
        )

    def test_archive_write_failure_does_not_commit_past_unwritten_entry(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "write-fail-payload"
        _start_and_line(fake_redis_client, line=payload)

        orig = archiver._append_archive_text

        def fail_write(handle, text: str) -> None:
            if "write-fail-payload" in text:
                raise OSError("write failed")
            orig(handle, text)

        monkeypatch.setattr(archiver, "_append_archive_text", fail_write)
        archiver._pump_output_streams()

        assert payload not in "".join(
            p.read_text(encoding="utf-8") for p in archive_root.rglob("*.jsonl")
        )
        committed = archiver.committed_last_id("output:w1")
        assert committed is not None
        # task_start committed; the line that failed to write did not.
        start_id, line_id = [eid for eid, _ in fake_redis_client.xread_after("output:w1", "0-0")]
        assert committed == start_id
        assert committed != line_id
        assert archiver.health_error is not None

    def test_archive_flush_failure_does_not_commit_cursor(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "flush-fail-payload"
        _start_and_line(fake_redis_client, line=payload)

        def fail_flush(_handle) -> None:
            raise OSError("flush failed")

        monkeypatch.setattr(archiver, "_flush_archive", fail_flush)
        archiver._pump_output_streams()

        assert archiver.committed_last_id("output:w1") is None
        assert archiver.health_error is not None
        assert "flush" in archiver.health_error.lower() or "archive failed" in (
            archiver.health_error.lower()
        )

    def test_archive_fsync_failure_does_not_commit_cursor(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "fsync-fail-payload"
        _start_and_line(fake_redis_client, line=payload)

        def fail_fsync(_handle) -> None:
            raise OSError("fsync failed")

        monkeypatch.setattr(archiver, "_fsync_archive", fail_fsync)
        archiver._pump_output_streams()

        assert archiver.committed_last_id("output:w1") is None
        assert archiver.health_error is not None

    def test_cursor_temp_write_failure_halts_without_commit(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "cursor-write-fail-payload"
        start_id, line_id = _commit_start_then_enqueue_line(fake_redis_client, archiver, payload)

        def fail_write(_fd: int, _data: bytes) -> None:
            raise OSError("cursor temp write failed")

        monkeypatch.setattr(trace_archiver_mod, "_write_fd", fail_write)
        archiver._pump_output_streams()

        assert _count_in_archive(archive_root, payload) == 1
        assert archiver.committed_last_id("output:w1") == start_id
        assert archiver.committed_last_id("output:w1") != line_id
        assert archiver.health_error is not None

    def test_cursor_temp_fsync_failure_halts_without_commit(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "cursor-fsync-fail-payload"
        start_id, line_id = _commit_start_then_enqueue_line(fake_redis_client, archiver, payload)

        def fail_fsync(_fd: int) -> None:
            raise OSError("cursor temp fsync failed")

        monkeypatch.setattr(trace_archiver_mod, "_fsync_fd", fail_fsync)
        archiver._pump_output_streams()

        assert _count_in_archive(archive_root, payload) == 1
        assert archiver.committed_last_id("output:w1") == start_id
        assert archiver.committed_last_id("output:w1") != line_id
        assert archiver.health_error is not None

    def test_cursor_rename_failure_halts_without_commit(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "cursor-rename-fail-payload"
        start_id, line_id = _commit_start_then_enqueue_line(fake_redis_client, archiver, payload)

        def fail_replace(_src: Path, _dst: Path) -> None:
            raise OSError("cursor rename failed")

        monkeypatch.setattr(trace_archiver_mod, "_replace_file", fail_replace)
        archiver._pump_output_streams()

        assert _count_in_archive(archive_root, payload) == 1
        assert archiver.committed_last_id("output:w1") == start_id
        assert archiver.committed_last_id("output:w1") != line_id
        assert archiver.health_error is not None

    def test_cursor_directory_fsync_failure_halts_without_commit(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "cursor-dir-fsync-fail-payload"
        start_id, line_id = _commit_start_then_enqueue_line(fake_redis_client, archiver, payload)

        def fail_dir_fsync(_directory: Path) -> None:
            raise OSError("directory fsync failed")

        monkeypatch.setattr(trace_archiver_mod, "_fsync_directory", fail_dir_fsync)
        archiver._pump_output_streams()

        assert _count_in_archive(archive_root, payload) == 1
        assert archiver.committed_last_id("output:w1") == start_id
        assert archiver.committed_last_id("output:w1") != line_id
        assert archiver.health_error is not None

    def test_same_process_retry_does_not_duplicate_content(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        payload = "same-process-retry-payload"
        start_id, line_id = _commit_start_then_enqueue_line(fake_redis_client, archiver, payload)
        orig_write = trace_archiver_mod._write_fd
        persist_calls = {"n": 0}

        def fail_write(fd: int, data: bytes) -> None:
            persist_calls["n"] += 1
            raise OSError("cursor persist failed")

        monkeypatch.setattr(trace_archiver_mod, "_write_fd", fail_write)
        archiver._pump_output_streams()
        archiver._pump_output_streams()
        assert _count_in_archive(archive_root, payload) == 1
        assert archiver.committed_last_id("output:w1") == start_id
        assert persist_calls["n"] >= 2

        monkeypatch.setattr(trace_archiver_mod, "_write_fd", orig_write)
        archiver._pump_output_streams()
        assert _count_in_archive(archive_root, payload) == 1
        assert archiver.committed_last_id("output:w1") == line_id

    def test_legacy_hash_seeds_local_state_once(self, fake_redis_client, logger, archive_root):
        fake_redis_client.hset(_CURSOR_HASH_KEY, "output:w1", "123-4")
        archiver = _archiver(fake_redis_client, logger, archive_root)
        archiver._pump_output_streams()

        state_path = cursor_state_path(archive_root, fake_redis_client.key_prefix)
        state = json.loads(state_path.read_text())
        assert state["version"] == _CURSOR_STATE_VERSION
        assert state["streams"]["output:w1"]["last_id"] == "123-4"
        assert state["streams"]["output:w1"]["last_seen"]
        assert archiver.committed_last_id("output:w1") == "123-4"
        # Legacy hash is left in place; this change does not own cursor GC.
        assert fake_redis_client.hget(_CURSOR_HASH_KEY, "output:w1") == "123-4"

    def test_stale_legacy_hash_is_ignored_after_local_state_exists(
        self, fake_redis_client, logger, archive_root
    ):
        fake_redis_client.hset(_CURSOR_HASH_KEY, "output:w1", "5-0")
        first = _archiver(fake_redis_client, logger, archive_root)
        first._pump_output_streams()
        assert first.committed_last_id("output:w1") == "5-0"

        fake_redis_client.hset(_CURSOR_HASH_KEY, "output:w1", "999-0")
        second = _archiver(fake_redis_client, logger, archive_root)
        second._pump_output_streams()
        assert second.committed_last_id("output:w1") == "5-0"
        state = json.loads(
            cursor_state_path(archive_root, fake_redis_client.key_prefix).read_text()
        )
        assert state["streams"]["output:w1"]["last_id"] == "5-0"

    def test_project_namespaces_do_not_share_cursors(self, fake_redis_server, logger, archive_root):
        shared = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
        redis_a = RedisClient.from_client(shared, key_prefix="projA")
        redis_b = RedisClient.from_client(shared, key_prefix="projB")
        redis_a.hset(_CURSOR_HASH_KEY, "output:w1", "10-0")
        redis_b.hset(_CURSOR_HASH_KEY, "output:w1", "20-0")

        archiver_a = _archiver(redis_a, logger, archive_root)
        archiver_b = _archiver(redis_b, logger, archive_root)
        archiver_a._pump_output_streams()
        archiver_b._pump_output_streams()

        path_a = cursor_state_path(archive_root, "projA")
        path_b = cursor_state_path(archive_root, "projB")
        assert path_a != path_b
        assert json.loads(path_a.read_text())["streams"]["output:w1"]["last_id"] == "10-0"
        assert json.loads(path_b.read_text())["streams"]["output:w1"]["last_id"] == "20-0"
        assert archiver_a.committed_last_id("output:w1") == "10-0"
        assert archiver_b.committed_last_id("output:w1") == "20-0"

    def test_corrupt_state_disables_advancement_and_does_not_replay(
        self, fake_redis_client, logger, archive_root, caplog
    ):
        state_path = cursor_state_path(archive_root, fake_redis_client.key_prefix)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text("{not-json", encoding="utf-8")

        payload = "corrupt-state-payload"
        _start_and_line(fake_redis_client, line=payload)
        archiver = _archiver(fake_redis_client, logger, archive_root)
        with caplog.at_level(logging.ERROR):
            archiver._pump_output_streams()
            archiver._pump_output_streams()

        assert archiver.health_error is not None
        assert "corrupt" in archiver.health_error.lower()
        assert "corrupt" in caplog.text.lower()
        assert archiver.committed_last_id("output:w1") is None
        assert _count_in_archive(archive_root, payload) == 0
        assert state_path.read_text(encoding="utf-8") == "{not-json"

    def test_unsupported_version_is_corrupt_not_skipped(
        self, fake_redis_client, logger, archive_root
    ):
        state_path = cursor_state_path(archive_root, fake_redis_client.key_prefix)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps(
                {
                    "version": 99,
                    "streams": {"output:w1": {"last_id": "1-0", "last_seen": "t"}},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        _start_and_line(fake_redis_client, line="version-skip-payload")
        archiver = _archiver(fake_redis_client, logger, archive_root)
        archiver._pump_output_streams()
        assert archiver.health_error is not None
        assert archiver.committed_last_id("output:w1") is None
        assert _count_in_archive(archive_root, "version-skip-payload") == 0


def _stream_ids(redis_client: RedisClient, stream: str) -> list[str]:
    return [entry_id for entry_id, _ in redis_client.xrange(stream)]


def _enqueue_task_with_lines(
    redis_client: RedisClient,
    n_lines: int,
    *,
    stream: str = "output:w1",
    task_id: str = "t-trim",
) -> list[str]:
    ids = [
        _xadd(
            redis_client,
            stream,
            {"type": "task_start", "task_id": task_id, "repo": "owner/r"},
        )
    ]
    for i in range(n_lines):
        ids.append(_xadd(redis_client, stream, {"line": f"payload-{i}"}))
    ids.append(
        _xadd(
            redis_client,
            stream,
            {"type": "task_end", "task_id": task_id, "status": "completed"},
        )
    )
    return ids


def _hours_ago_iso(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


class TestArchiverTrimAndCursorGC:
    def test_retains_newest_tail_after_durable_archive(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)
        archiver._pump_output_streams()

        remaining = _stream_ids(fake_redis_client, "output:w1")
        assert len(remaining) == _RETAIN_NEWEST
        assert remaining == ids[-_RETAIN_NEWEST:]
        assert archiver.committed_last_id("output:w1") == ids[-1]

    def test_does_not_trim_past_committed_cursor(self, fake_redis_client, logger, archive_root):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        archived = _enqueue_task_with_lines(fake_redis_client, 5, task_id="t-early")
        archiver._pump_output_streams()
        cursor = archiver.committed_last_id("output:w1")
        assert cursor == archived[-1]

        extras = [
            _xadd(fake_redis_client, "output:w1", {"line": f"unarchived-{i}"}) for i in range(200)
        ]
        archiver._trim_archived_stream("output:w1")

        remaining = _stream_ids(fake_redis_client, "output:w1")
        assert cursor in remaining
        cursor_tuple = _entry_id_tuple(cursor)
        assert cursor_tuple is not None
        for entry_id in remaining:
            parsed = _entry_id_tuple(entry_id)
            assert parsed is not None
            assert parsed >= cursor_tuple
        assert extras[-1] in remaining
        assert len(remaining) > _RETAIN_NEWEST

    def test_disabled_archiver_does_not_trim(self, fake_redis_client, logger, archive_root):
        ids = _enqueue_task_with_lines(fake_redis_client, 200)
        archiver = TraceArchiver(
            redis=fake_redis_client,
            archive_path=None,
            repo_to_project={"owner/r": "myproj"},
            logger=logger,
        )
        archiver.start()
        assert archiver._thread is None
        archiver._trim_archived_stream("output:w1")
        assert _stream_ids(fake_redis_client, "output:w1") == ids

    def test_skipped_entry_does_not_trim(self, fake_redis_client, logger, archive_root):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = [_xadd(fake_redis_client, "output:w1", {"line": f"orphan-{i}"}) for i in range(200)]
        archiver._pump_output_streams()
        assert archiver.committed_last_id("output:w1") == ids[-1]
        assert _stream_ids(fake_redis_client, "output:w1") == ids

    def test_open_failure_does_not_trim(self, fake_redis_client, logger, archive_root, monkeypatch):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)

        def fail_open(_path: Path):
            raise OSError("open failed")

        monkeypatch.setattr(archiver, "_open_archive_file", fail_open)
        archiver._pump_output_streams()
        assert archiver.committed_last_id("output:w1") is None
        assert _stream_ids(fake_redis_client, "output:w1") == ids

    def test_write_failure_does_not_trim(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)
        orig = archiver._append_archive_text

        def fail_write(handle, text: str) -> None:
            if "payload-0" in text:
                raise OSError("write failed")
            orig(handle, text)

        monkeypatch.setattr(archiver, "_append_archive_text", fail_write)
        archiver._pump_output_streams()
        assert _stream_ids(fake_redis_client, "output:w1") == ids

    def test_fsync_failure_does_not_trim(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)

        def fail_fsync(_handle) -> None:
            raise OSError("fsync failed")

        monkeypatch.setattr(archiver, "_fsync_archive", fail_fsync)
        archiver._pump_output_streams()
        assert archiver.committed_last_id("output:w1") is None
        assert _stream_ids(fake_redis_client, "output:w1") == ids

    def test_cursor_persist_failure_does_not_trim(
        self, fake_redis_client, logger, archive_root, monkeypatch
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)

        def fail_write(_fd: int, _data: bytes) -> None:
            raise OSError("cursor temp write failed")

        monkeypatch.setattr(trace_archiver_mod, "_write_fd", fail_write)
        archiver._pump_output_streams()
        assert archiver.committed_last_id("output:w1") is None
        assert _stream_ids(fake_redis_client, "output:w1") == ids

    def test_corrupt_cursor_does_not_trim(self, fake_redis_client, logger, archive_root):
        state_path = cursor_state_path(archive_root, fake_redis_client.key_prefix)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text("{not-json", encoding="utf-8")
        ids = _enqueue_task_with_lines(fake_redis_client, 200)
        archiver = _archiver(fake_redis_client, logger, archive_root)
        archiver._pump_output_streams()
        assert archiver.committed_last_id("output:w1") is None
        assert _stream_ids(fake_redis_client, "output:w1") == ids

    def test_oom_compatible_trim_when_redis_refuses_writes(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)
        _refuse_redis_writes(fake_redis_client)
        archiver._pump_output_streams()
        remaining = _stream_ids(fake_redis_client, "output:w1")
        assert remaining == ids[-_RETAIN_NEWEST:]
        assert _count_in_archive(archive_root, "payload-199") == 1

    def test_trim_failure_retries_without_blocking_archival(
        self, fake_redis_client, logger, archive_root, monkeypatch, caplog
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)
        orig = fake_redis_client.xtrim_minid
        calls = {"n": 0}

        def fail_trim(stream: str, minid: str) -> int:
            calls["n"] += 1
            raise redis.ResponseError("OOM command not allowed when used memory > 'maxmemory'")

        monkeypatch.setattr(fake_redis_client, "xtrim_minid", fail_trim)
        with caplog.at_level(logging.WARNING):
            archiver._pump_output_streams()
            archiver._pump_output_streams()
        assert _count_in_archive(archive_root, "payload-199") == 1
        assert _stream_ids(fake_redis_client, "output:w1") == ids
        assert caplog.text.count("trim failed") == 1

        monkeypatch.setattr(fake_redis_client, "xtrim_minid", orig)
        archiver._pump_output_streams()
        remaining = _stream_ids(fake_redis_client, "output:w1")
        assert remaining == ids[-_RETAIN_NEWEST:]
        assert calls["n"] >= 2

    def test_late_dashboard_attach_reads_retained_tail(
        self, fake_redis_client, logger, archive_root
    ):
        archiver = _archiver(fake_redis_client, logger, archive_root)
        ids = _enqueue_task_with_lines(fake_redis_client, 200)
        archiver._pump_output_streams()

        # Dashboard live-tail starts from 0-0 on first attach.
        attached = fake_redis_client.xread_after("output:w1", "0-0", count=500)
        attached_ids = [entry_id for entry_id, _ in attached]
        assert attached_ids == ids[-_RETAIN_NEWEST:]
        payloads = [fields.get("line", "") for _, fields in attached]
        assert "payload-199" in payloads
        assert "payload-0" not in payloads

    def test_stale_absent_cursor_gc_after_32_hours(self, fake_redis_client, logger, archive_root):
        gone = "output:gone"
        fake_redis_client.hset(_CURSOR_HASH_KEY, gone, "1-0")
        fake_redis_client.hset(_CURSOR_HASH_KEY, "output:w1", "2-0")
        persist_cursor_state_file(
            cursor_state_path(archive_root, fake_redis_client.key_prefix),
            {
                gone: StreamCursor(last_id="1-0", last_seen=_hours_ago_iso(33)),
                "output:w1": StreamCursor(last_id="2-0", last_seen=_hours_ago_iso(33)),
            },
        )
        live_id = _xadd(fake_redis_client, "output:w1", {"line": "still-here"})
        archiver = _archiver(fake_redis_client, logger, archive_root)
        archiver._pump_output_streams()

        state = json.loads(
            cursor_state_path(archive_root, fake_redis_client.key_prefix).read_text()
        )
        assert gone not in state["streams"]
        assert "output:w1" in state["streams"]
        assert fake_redis_client.hget(_CURSOR_HASH_KEY, gone) is None
        assert fake_redis_client.hget(_CURSOR_HASH_KEY, "output:w1") == "2-0"
        assert live_id in _stream_ids(fake_redis_client, "output:w1")

    def test_cursor_gc_waits_full_32_hours(self, fake_redis_client, logger, archive_root):
        gone = "output:gone"
        fake_redis_client.hset(_CURSOR_HASH_KEY, gone, "1-0")
        persist_cursor_state_file(
            cursor_state_path(archive_root, fake_redis_client.key_prefix),
            {gone: StreamCursor(last_id="1-0", last_seen=_hours_ago_iso(31))},
        )
        archiver = _archiver(fake_redis_client, logger, archive_root)
        archiver._pump_output_streams()
        state = json.loads(
            cursor_state_path(archive_root, fake_redis_client.key_prefix).read_text()
        )
        assert gone in state["streams"]
        assert fake_redis_client.hget(_CURSOR_HASH_KEY, gone) == "1-0"

    def test_live_idle_stream_cursor_is_not_gc_even_if_last_seen_is_old(
        self, fake_redis_client, logger, archive_root
    ):
        stream = "output:idle"
        entry_id = _xadd(fake_redis_client, stream, {"line": "idle-tail"})
        persist_cursor_state_file(
            cursor_state_path(archive_root, fake_redis_client.key_prefix),
            {stream: StreamCursor(last_id=entry_id, last_seen=_hours_ago_iso(40))},
        )
        fake_redis_client.hset(_CURSOR_HASH_KEY, stream, entry_id)
        archiver = _archiver(fake_redis_client, logger, archive_root)
        archiver._pump_output_streams()
        state = json.loads(
            cursor_state_path(archive_root, fake_redis_client.key_prefix).read_text()
        )
        assert stream in state["streams"]
        assert fake_redis_client.hget(_CURSOR_HASH_KEY, stream) == entry_id
        assert _stream_ids(fake_redis_client, stream) == [entry_id]
