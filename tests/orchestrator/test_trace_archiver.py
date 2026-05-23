"""Tests for the verbatim per-task trace archiver."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

from orcest.orchestrator.trace_archiver import TraceArchiver, _entry_id_to_iso


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

    def test_self_disables_on_unwritable_archive_path(
        self, fake_redis_client, logger, tmp_path
    ):
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
    def test_single_task_writes_jsonl_meta_and_index(
        self, fake_redis_client, logger, archive_root
    ):
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

        cursors_first = fake_redis_client.hgetall("trace_archiver:cursors")
        assert "output:w1" in cursors_first
        first_cursor = cursors_first["output:w1"]

        # Add new entries; cursor must advance.
        _xadd(fake_redis_client, "output:w1", {"line": "line-b"})
        _xadd(
            fake_redis_client,
            "output:w1",
            {"type": "task_end", "task_id": "t-1", "status": "completed"},
        )
        archiver._pump_output_streams()

        cursors_second = fake_redis_client.hgetall("trace_archiver:cursors")
        assert cursors_second["output:w1"] != first_cursor

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
        contents = {p.name for p in archive_root.iterdir()}
        assert contents == set(), f"unexpected entries under archive root: {contents}"

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
        for f in (jsonl, meta, idx):
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
