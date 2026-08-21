from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "master-verified.yml"

NO_RUN_SHA = "715e4f1f4e121f0c6196226128690b9e6fa9708e"
RED_PUSH_SHA = "178bfa855a8f106135fb9931b9466a69bc79e6d2"
DISPATCH_ONLY_SHA = "0f5d3dd0122091d3997596e00adfd368f6458ce4"
HEALTHY_SHA = "0123456789abcdef0123456789abcdef01234567"

REQUIRED_JOBS = (
    "lint",
    "typecheck",
    "test",
    "dashboard",
    "integration",
    "docker",
)


def _workflow() -> dict[str, Any]:
    return yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)


def _workflow_python() -> str:
    lines = WORKFLOW.read_text().splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.strip() == "python3 <<'PYEOF'":
            start = i + 1
            break
    assert start is not None, "master-verified.yml must embed a python3 heredoc"
    body: list[str] = []
    for line in lines[start:]:
        if line.strip() == "PYEOF":
            break
        body.append(line)
    else:
        raise AssertionError("unterminated python3 heredoc in master-verified.yml")
    return textwrap.dedent("\n".join(body) + "\n")


def _load_assert_ns() -> dict[str, Any]:
    source = _workflow_python()
    ns: dict[str, Any] = {"__name__": "assert_master_ci"}
    exec(compile(source, "master-verified.yml", "exec"), ns)
    return ns


def _job(name: str, conclusion: str | None, status: str = "completed") -> dict[str, Any]:
    return {"name": name, "conclusion": conclusion, "status": status}


def _green_jobs() -> list[dict[str, Any]]:
    return [_job(name, "success") for name in REQUIRED_JOBS]


def _run(
    run_id: int,
    event: str,
    *,
    path: str = ".github/workflows/ci.yml",
    conclusion: str | None = "success",
    status: str = "completed",
    sha: str = HEALTHY_SHA,
) -> dict[str, Any]:
    return {
        "id": run_id,
        "event": event,
        "path": path,
        "name": "CI",
        "conclusion": conclusion,
        "status": status,
        "head_sha": sha,
    }


def _install_api(
    ns: dict[str, Any],
    *,
    runs: list[dict[str, Any]],
    jobs: dict[int, list[dict[str, Any]]] | None = None,
) -> None:
    jobs = jobs or {}

    def gh_api(path: str) -> dict[str, Any]:
        if "/jobs" in path:
            after = path.split("/actions/runs/")[1]
            run_id = int(after.split("/")[0].split("?")[0])
            return {"jobs": jobs.get(run_id, [])}
        if "/actions/runs" in path:
            return {"total_count": len(runs), "workflow_runs": runs}
        raise AssertionError(f"unexpected gh api path: {path}")

    ns["gh_api"] = gh_api


def test_master_verified_workflow_exists_and_is_github_hosted() -> None:
    assert WORKFLOW.is_file()
    text = WORKFLOW.read_text()
    assert "self-hosted" not in text
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert jobs
    for job in jobs.values():
        assert job["runs-on"] == "ubuntu-latest"


def test_master_verified_workflow_schedules_off_the_hour_and_accepts_sha_override() -> None:
    workflow = _workflow()
    triggers = workflow["on"]
    assert "schedule" in triggers
    assert "workflow_dispatch" in triggers
    assert "head_sha" in triggers["workflow_dispatch"]["inputs"]
    crons = [item["cron"] for item in triggers["schedule"]]
    assert crons
    for cron in crons:
        minute = cron.split()[0]
        assert minute not in {"0", "00"}


def test_master_verified_workflow_resolves_master_via_gh_api() -> None:
    text = WORKFLOW.read_text()
    assert 'gh api "repos/${{ github.repository }}/commits/master"' in text
    assert "pull_request" not in _workflow()["on"]


def test_master_verified_embedded_python_is_valid() -> None:
    ast.parse(_workflow_python())


def test_no_run_is_failure_even_when_other_workflows_exist(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", NO_RUN_SHA)
    ns = _load_assert_ns()
    _install_api(
        ns,
        runs=[
            _run(
                1,
                "pull_request",
                path=".github/workflows/claude-review.yml",
                sha=NO_RUN_SHA,
            )
        ],
        jobs={1: _green_jobs()},
    )

    with pytest.raises(SystemExit) as exited:
        ns["main"]()

    assert exited.value.code == 1
    captured = capsys.readouterr().out
    assert "::error::" in captured
    assert NO_RUN_SHA in captured
    assert "no run" in captured


def test_pull_request_or_dispatch_run_is_wrong_event(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", DISPATCH_ONLY_SHA)
    ns = _load_assert_ns()
    _install_api(
        ns,
        runs=[
            _run(32309364353, "workflow_dispatch", conclusion="success", sha=DISPATCH_ONLY_SHA),
            _run(2, "pull_request", conclusion="success", sha=DISPATCH_ONLY_SHA),
        ],
        jobs={
            32309364353: _green_jobs(),
            2: _green_jobs(),
        },
    )

    with pytest.raises(SystemExit) as exited:
        ns["main"]()

    assert exited.value.code == 1
    captured = capsys.readouterr().out
    assert DISPATCH_ONLY_SHA in captured
    assert "wrong event" in captured
    assert "no run" not in captured


def test_red_jobs_are_failure_even_when_run_conclusion_is_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", RED_PUSH_SHA)
    ns = _load_assert_ns()
    jobs = _green_jobs()
    for job in jobs:
        if job["name"] in {"lint", "typecheck"}:
            job["conclusion"] = "failure"
    _install_api(
        ns,
        runs=[_run(32219239554, "push", conclusion="success", sha=RED_PUSH_SHA)],
        jobs={32219239554: jobs},
    )

    with pytest.raises(SystemExit) as exited:
        ns["main"]()

    assert exited.value.code == 1
    captured = capsys.readouterr().out
    assert RED_PUSH_SHA in captured
    assert "red jobs" in captured
    assert "lint=failure" in captured
    assert "typecheck=failure" in captured


def test_missing_integration_job_is_red_jobs(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", HEALTHY_SHA)
    ns = _load_assert_ns()
    jobs = [job for job in _green_jobs() if job["name"] != "integration"]
    _install_api(
        ns,
        runs=[_run(9, "push", conclusion="success")],
        jobs={9: jobs},
    )

    with pytest.raises(SystemExit) as exited:
        ns["main"]()

    assert exited.value.code == 1
    captured = capsys.readouterr().out
    assert "red jobs" in captured
    assert "integration=absent" in captured


def test_healthy_push_run_passes_with_no_annotations(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", HEALTHY_SHA)
    ns = _load_assert_ns()
    _install_api(
        ns,
        runs=[_run(11, "push", conclusion="failure")],
        jobs={11: _green_jobs()},
    )

    ns["main"]()

    captured = capsys.readouterr().out
    assert "::error::" not in captured
    assert "::warning::" not in captured
    assert HEALTHY_SHA in captured
    assert "11" in captured


def test_healthy_schedule_run_passes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", HEALTHY_SHA)
    ns = _load_assert_ns()
    _install_api(
        ns,
        runs=[_run(12, "schedule")],
        jobs={12: _green_jobs()},
    )

    ns["main"]()

    captured = capsys.readouterr().out
    assert "::error::" not in captured
    assert "schedule" in captured


def test_in_progress_push_run_is_not_red_jobs(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", HEALTHY_SHA)
    ns = _load_assert_ns()
    jobs = [_job(name, None, status="in_progress") for name in REQUIRED_JOBS]
    _install_api(
        ns,
        runs=[_run(13, "push", conclusion=None, status="in_progress")],
        jobs={13: jobs},
    )

    ns["main"]()

    captured = capsys.readouterr().out
    assert "::error::" not in captured
    assert "red jobs" not in captured
    assert "::notice::" in captured
    assert "in progress" in captured
    assert HEALTHY_SHA in captured
    assert "13" in captured


def test_completed_red_run_is_red_jobs_even_with_in_progress_sibling(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "ThayneStudio/orcest")
    monkeypatch.setenv("SHA", RED_PUSH_SHA)
    ns = _load_assert_ns()
    red_jobs = _green_jobs()
    for job in red_jobs:
        if job["name"] == "lint":
            job["conclusion"] = "failure"
    pending_jobs = [_job(name, None, status="in_progress") for name in REQUIRED_JOBS]
    _install_api(
        ns,
        runs=[
            _run(14, "push", conclusion=None, status="in_progress", sha=RED_PUSH_SHA),
            _run(15, "push", conclusion="failure", sha=RED_PUSH_SHA),
        ],
        jobs={
            14: pending_jobs,
            15: red_jobs,
        },
    )

    with pytest.raises(SystemExit) as exited:
        ns["main"]()

    assert exited.value.code == 1
    captured = capsys.readouterr().out
    assert RED_PUSH_SHA in captured
    assert "red jobs" in captured
    assert "lint=failure" in captured
    assert "::notice::" not in captured
