"""Tests for the `orcest project` CLI (src/orcest/cli_project.py)."""

from __future__ import annotations

import json
import os
import subprocess

import pytest
from click.testing import CliRunner

from orcest.cli import main


@pytest.fixture
def runner():
    try:
        return CliRunner(mix_stderr=False)
    except TypeError as exc:
        if "mix_stderr" not in str(exc):
            raise
        return CliRunner()


def _git(*args: str) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    result = subprocess.run(["git", *args], capture_output=True, env=env, timeout=30)
    assert result.returncode == 0, result.stderr.decode()
    return result


def test_project_help(runner) -> None:
    result = runner.invoke(main, ["project", "--help"])
    assert result.exit_code == 0
    assert "init" in result.output
    assert "lint" in result.output
    assert "explain" in result.output
    assert "simulate" in result.output


def test_init_lint_explain_simulate_round_trip(runner, tmp_path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        _git("init", "-q")

        init_result = runner.invoke(main, ["project", "init"])
        assert init_result.exit_code == 0, init_result.output
        created = json.loads(init_result.output)["created"]
        assert ".orcest/project.yaml" in created

        _git("add", ".orcest")
        _git("commit", "-q", "-m", "bundle")

        # Re-running init must refuse to overwrite and leave the tree untouched.
        second_init = runner.invoke(main, ["project", "init"])
        assert second_init.exit_code == 1

        lint_result = runner.invoke(main, ["project", "lint", "--revision", "HEAD"])
        assert lint_result.exit_code == 0, lint_result.output
        lint_payload = json.loads(lint_result.output)
        assert lint_payload["ok"] is True
        assert lint_payload["workflow_hash"].startswith("sha256:")

        explain_result = runner.invoke(main, ["project", "explain", "--revision", "HEAD"])
        assert explain_result.exit_code == 0, explain_result.output
        explain_payload = json.loads(explain_result.output)
        assert explain_payload["workflow_hash"] == lint_payload["workflow_hash"]
        assert explain_payload["project"]["kind"] == "Project"
        assert explain_payload["workflow"]["kind"] == "Workflow"
        assert explain_payload["server_constraints"]["available"] is False

        fixture_path = tmp_path / "fixture.json"
        fixture_path.write_text(json.dumps({"trigger": "ADMIT", "payload": {}}))
        simulate_result = runner.invoke(
            main, ["project", "simulate", "--revision", "HEAD", "--event", str(fixture_path)]
        )
        assert simulate_result.exit_code == 0, simulate_result.output
        simulate_payload = json.loads(simulate_result.output)
        assert simulate_payload["trigger"] == "ADMIT"
        assert simulate_payload["reducer"]["status"] == "deferred"


def test_lint_reports_diagnostics_on_invalid_bundle(runner, tmp_path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        _git("init", "-q")
        os.makedirs(".orcest", exist_ok=True)
        with open(".orcest/project.yaml", "w") as f:
            f.write(
                "apiVersion: orcest.dev/v1\nkind: Project\n"
                "spec:\n  workflow: .orcest/missing.yaml\n"
            )
        _git("add", ".orcest")
        _git("commit", "-q", "-m", "invalid bundle")

        result = runner.invoke(main, ["project", "lint", "--revision", "HEAD"])
        assert result.exit_code == 1
        diagnostics = json.loads(result.output)
        assert any(d["code"] == "FILE_NOT_FOUND" for d in diagnostics)
        for diagnostic in diagnostics:
            assert set(diagnostic) == {"code", "message", "file", "path"}


def test_simulate_rejects_unknown_trigger(runner, tmp_path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        _git("init", "-q")
        runner.invoke(main, ["project", "init"])
        _git("add", ".orcest")
        _git("commit", "-q", "-m", "bundle")

        fixture_path = tmp_path / "fixture.json"
        fixture_path.write_text(json.dumps({"trigger": "NOT_REAL", "payload": {}}))
        result = runner.invoke(
            main, ["project", "simulate", "--revision", "HEAD", "--event", str(fixture_path)]
        )
        assert result.exit_code == 1
        diagnostics = json.loads(result.output)
        assert diagnostics[0]["code"] == "FIXTURE_TRIGGER_INVALID"


def test_lint_without_revision_or_origin_fails_closed(runner, tmp_path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        _git("init", "-q")
        runner.invoke(main, ["project", "init"])
        _git("add", ".orcest")
        _git("commit", "-q", "-m", "bundle")

        result = runner.invoke(main, ["project", "lint"])
        assert result.exit_code == 2
