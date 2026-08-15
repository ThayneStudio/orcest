"""Tests for the `orcest trace` CLI subcommand."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from orcest.cli import main


@pytest.fixture
def archive_with_one_task(tmp_path: Path) -> tuple[Path, str]:
    """Materialize a fake archive containing one task and return (root, task_id)."""
    task_id = "9a686e05-a81a-4d22-b17d-be7c17d17b0e"
    rel_dir = Path("bbr-platform/2026/05/23")
    jsonl_dir = tmp_path / rel_dir
    jsonl_dir.mkdir(parents=True)
    jsonl = jsonl_dir / f"{task_id}.jsonl"
    jsonl.write_text(
        '{"type":"assistant","message":{"role":"assistant","content":'
        '[{"type":"text","text":"hello human"}]}}\n'
    )
    meta = {
        "task_id": task_id,
        "worker_id": "orcest-worker-10001",
        "project": "bbr-platform",
        "repo": "bluebamboollc/bbr-platform",
        "resource_type": "pr",
        "resource_id": "3546",
        "status": "failed",
        "started_at": "2026-05-23T17:40:08Z",
        "ended_at": "2026-05-23T17:44:05Z",
        "archived_at": "2026-05-23T17:44:11Z",
        "archive_path": str(rel_dir / f"{task_id}.jsonl"),
    }
    (jsonl_dir / f"{task_id}.meta.json").write_text(json.dumps(meta, indent=2))

    # Index pointer
    idx_dir = tmp_path / "index" / "by-task-id" / task_id[:2]
    idx_dir.mkdir(parents=True)
    (idx_dir / task_id).write_text(str(rel_dir) + "\n")

    return tmp_path, task_id


class TestTraceCmd:
    def test_unknown_task_id_returns_nonzero(self, archive_with_one_task):
        root, _ = archive_with_one_task
        runner = CliRunner()
        result = runner.invoke(main, ["trace", "no-such-task", "--archive-root", str(root)])
        assert result.exit_code != 0
        assert "No archived trace" in (result.output + (result.stderr or ""))

    def test_meta_flag_prints_sidecar(self, archive_with_one_task):
        root, task_id = archive_with_one_task
        runner = CliRunner()
        result = runner.invoke(main, ["trace", task_id, "--meta", "--archive-root", str(root)])
        assert result.exit_code == 0, result.output
        assert task_id in result.output
        assert "failed" in result.output
        assert "bbr-platform" in result.output

    def test_default_prints_pretty_formatted(self, archive_with_one_task):
        root, task_id = archive_with_one_task
        runner = CliRunner()
        result = runner.invoke(main, ["trace", task_id, "--archive-root", str(root)])
        assert result.exit_code == 0, result.output
        assert "hello human" in result.output

    def test_raw_prints_jsonl_unchanged(self, archive_with_one_task):
        root, task_id = archive_with_one_task
        runner = CliRunner()
        result = runner.invoke(main, ["trace", task_id, "--raw", "--archive-root", str(root)])
        assert result.exit_code == 0, result.output
        assert '"role":"assistant"' in result.output

    def test_pr_resolution_finds_task(self, archive_with_one_task):
        root, task_id = archive_with_one_task
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["trace", "--pr", "bluebamboollc/bbr-platform#3546", "--archive-root", str(root)],
        )
        assert result.exit_code == 0, result.output
        assert "hello human" in result.output

    def test_pr_with_unknown_pr_fails_gracefully(self, archive_with_one_task):
        root, _ = archive_with_one_task
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["trace", "--pr", "bluebamboollc/bbr-platform#9999", "--archive-root", str(root)],
        )
        assert result.exit_code != 0

    def test_list_shows_archived_tasks(self, archive_with_one_task, monkeypatch):
        root, task_id = archive_with_one_task
        # Force a wide terminal so Rich doesn't truncate the task_id column.
        monkeypatch.setenv("COLUMNS", "200")
        runner = CliRunner()
        result = runner.invoke(
            main, ["trace", "--list", "bbr-platform", "--archive-root", str(root)]
        )
        assert result.exit_code == 0, result.output
        assert task_id in result.output
        assert "failed" in result.output

    def test_rejects_task_id_with_path_traversal(self, archive_with_one_task):
        root, _ = archive_with_one_task
        runner = CliRunner()
        result = runner.invoke(main, ["trace", "../../etc/passwd", "--archive-root", str(root)])
        assert result.exit_code != 0

    def test_hostile_index_pointer_redirecting_outside_root_is_refused(
        self, archive_with_one_task, tmp_path
    ):
        """An index pointer that points outside the archive must be refused."""
        root, _ = archive_with_one_task
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret-task.jsonl").write_text("DO NOT SHOW")
        (outside / "secret-task.meta.json").write_text("{}")
        # Plant a hostile index pointer for a syntactically-valid task_id
        # that resolves outside the archive root.
        idx_dir = root / "index" / "by-task-id" / "se"
        idx_dir.mkdir(parents=True, exist_ok=True)
        # Use a relative path that walks outside via ../
        (idx_dir / "secret-task").write_text("../../../outside")

        runner = CliRunner()
        result = runner.invoke(main, ["trace", "secret-task", "--archive-root", str(root)])
        assert result.exit_code != 0
        assert "DO NOT SHOW" not in result.output
