"""Canonical check DAG leaves, aggregates, and CI wiring."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
CI_PATH = ROOT / ".github" / "workflows" / "ci.yml"
MASTER_VERIFIED_PATH = ROOT / ".github" / "workflows" / "master-verified.yml"

REQUIRED_CI_JOBS = (
    "lint",
    "typecheck",
    "test",
    "dashboard",
    "integration",
    "docker",
)

PYTHON_CI_JOBS = ("lint", "typecheck", "test", "integration")

CORRECTNESS_TARGETS = (
    "lint-check",
    "typecheck",
    "test-unit",
    "test-integration",
    "test-stress",
    "test-dashboard",
    "check-fast",
    "check-full",
)

CI_ONLY_TARGETS = (
    "build-dashboard",
    "smoke-dashboard-image",
    "smoke-dashboard-compose",
    "redis-up",
    "redis-down",
    "audit-dashboard",
)

LOCKED_INSTALL = "pip install -r requirements-dev.lock"
LOCKED_EDITABLE = "pip install --no-deps --no-build-isolation -e ."


def _ci_workflow() -> dict[str, Any]:
    return yaml.load(CI_PATH.read_text(), Loader=yaml.BaseLoader)


def _job_run_commands(job: dict[str, Any]) -> list[str]:
    commands: list[str] = []
    for step in job["steps"]:
        run = step.get("run")
        if run:
            commands.append(run)
    return commands


def _normalize(text: str) -> str:
    return " ".join(text.split())


def _make_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("MAKEFLAGS", None)
    env.pop("MAKELEVEL", None)
    env.pop("MFLAGS", None)
    return env


def _make_database() -> str:
    proc = subprocess.run(
        [
            "make",
            "-qp",
            "--no-builtin-rules",
            "--no-builtin-variables",
            "check-full",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=_make_env(),
    )
    assert proc.returncode in {0, 1}, proc.stderr
    assert proc.stdout, proc.stderr
    return proc.stdout


def _direct_prerequisites(database: str, target: str) -> set[str]:
    header = f"{target}:"
    found: set[str] | None = None
    for line in database.splitlines():
        if not line.startswith(header):
            continue
        rest = line[len(header) :].lstrip()
        tokens = rest.split()
        if any(tok in {"=", ":=", "+=", "?=", "::="} for tok in tokens):
            continue
        if tokens and "=" in tokens[0]:
            continue
        prereqs: list[str] = []
        for tok in tokens:
            if tok == "|":
                break
            prereqs.append(tok)
        found = set(prereqs)
    assert found is not None, f"{target!r} missing from make database"
    return found


def _has_rule(database: str, target: str) -> bool:
    header = f"{target}:"
    for line in database.splitlines():
        if line.startswith(header) and "=" not in line.split(":", 1)[1].split()[:1]:
            return True
    return False


def _transitive_prerequisites(database: str, target: str) -> set[str]:
    seen: set[str] = set()
    stack = [target]
    while stack:
        current = stack.pop()
        for prereq in _direct_prerequisites(database, current):
            if prereq in seen:
                continue
            seen.add(prereq)
            if _has_rule(database, prereq):
                stack.append(prereq)
    return seen


def _make_dry_run(*targets: str) -> str:
    proc = subprocess.run(
        ["make", "-n", "--no-builtin-rules", "--no-builtin-variables", *targets],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=_make_env(),
    )
    return proc.stdout


def test_ci_keeps_required_job_names() -> None:
    jobs = _ci_workflow()["jobs"]
    assert tuple(jobs) == REQUIRED_CI_JOBS


def test_integration_trigger_is_master_push_schedule_or_dispatch() -> None:
    integration = _ci_workflow()["jobs"]["integration"]
    cond = _normalize(integration["if"])
    assert "github.event_name == 'workflow_dispatch'" in cond
    assert "github.event_name == 'schedule'" in cond
    assert "github.event_name == 'push'" in cond
    assert "github.ref == 'refs/heads/master'" in cond
    assert "pull_request" not in cond


def test_ci_invokes_named_leaf_targets_not_restated_commands() -> None:
    jobs = _ci_workflow()["jobs"]
    lint_runs = "\n".join(_job_run_commands(jobs["lint"]))
    typecheck_runs = "\n".join(_job_run_commands(jobs["typecheck"]))
    test_runs = "\n".join(_job_run_commands(jobs["test"]))
    dashboard_runs = "\n".join(_job_run_commands(jobs["dashboard"]))
    integration_runs = "\n".join(_job_run_commands(jobs["integration"]))

    assert "make lint-check" in lint_runs
    assert "ruff " not in lint_runs
    assert "make check-lock-dev" in lint_runs

    assert "make typecheck" in typecheck_runs
    assert "mypy" not in typecheck_runs

    assert "make test-unit" in test_runs
    assert "pytest" not in test_runs

    assert "make test-dashboard" in dashboard_runs
    assert "make audit-dashboard" in dashboard_runs

    assert "make test-integration" in integration_runs
    assert "pytest" not in integration_runs
    assert "tests.harness.supervisor" not in integration_runs

    ci_text = CI_PATH.read_text()
    assert "ruff check src/ tests/" not in ci_text
    assert "ruff format --check src/ tests/" not in ci_text
    assert "mypy src/" not in ci_text
    assert "pytest tests/" not in ci_text
    assert "python3 -m tests.harness.supervisor" not in ci_text
    assert "make check-fast" not in ci_text
    assert "make check-full" not in ci_text


def test_ci_python_jobs_use_locked_install_path() -> None:
    jobs = _ci_workflow()["jobs"]
    for name in PYTHON_CI_JOBS:
        runs = "\n".join(_job_run_commands(jobs[name]))
        assert LOCKED_INSTALL in runs, name
        assert LOCKED_EDITABLE in runs, name
        assert 'pip install -e ".[dev]"' not in runs


def test_ci_has_no_fixed_port_redis_service() -> None:
    jobs = _ci_workflow()["jobs"]
    for name, job in jobs.items():
        assert "services" not in job, name
    ci_text = CI_PATH.read_text()
    assert "6379:6379" not in ci_text
    assert "image: redis" not in ci_text


def test_ci_keeps_stress_local_only_and_audit_non_blocking() -> None:
    jobs = _ci_workflow()["jobs"]
    all_runs = "\n".join(cmd for job in jobs.values() for cmd in _job_run_commands(job))
    assert "test-stress" not in all_runs
    assert "pytest -m stress" not in all_runs

    audit_steps = [
        step for step in jobs["dashboard"]["steps"] if step.get("run") == "make audit-dashboard"
    ]
    assert len(audit_steps) == 1
    assert audit_steps[0]["continue-on-error"] == "true"


def test_ci_docker_job_keeps_image_builds_and_smokes() -> None:
    docker = _ci_workflow()["jobs"]["docker"]
    uses = [step.get("uses", "") for step in docker["steps"]]
    runs = "\n".join(_job_run_commands(docker))
    assert any(item.startswith("docker/build-push-action@") for item in uses)
    assert "dashboard/scripts/smoke-image.sh" in runs
    assert "dashboard/scripts/smoke-compose.sh" in runs
    assert "make check-full" not in runs
    assert "make test-dashboard" not in runs


def test_master_verified_inputs_and_required_jobs_unchanged() -> None:
    text = MASTER_VERIFIED_PATH.read_text()
    for name in REQUIRED_CI_JOBS:
        assert f'"{name}"' in text
    assert "head_sha" in text
    assert "715e4f1f4e121f0c6196226128690b9e6fa9708e" in text
    assert "178bfa85" in text
    assert "0f5d3dd0122091d3997596e00adfd368f6458ce4" in text
    assert 'ALLOWED_EVENTS = {"push", "schedule"}' in text


def test_check_aggregates_have_documented_prerequisites() -> None:
    database = _make_database()
    assert _direct_prerequisites(database, "check-fast") == {
        "lint-check",
        "typecheck",
        "test-unit",
    }
    assert _direct_prerequisites(database, "check-full") == {
        "check-fast",
        "test-integration",
        "test-stress",
        "test-dashboard",
    }

    fast = _transitive_prerequisites(database, "check-fast")
    full = _transitive_prerequisites(database, "check-full")
    assert {"lint-check", "typecheck", "test-unit"} <= fast
    assert "test-integration" not in fast
    assert "test-stress" not in fast
    assert "test-dashboard" not in fast
    assert {
        "check-fast",
        "lint-check",
        "typecheck",
        "test-unit",
        "test-integration",
        "test-stress",
        "test-dashboard",
    } <= full
    for name in CI_ONLY_TARGETS:
        assert name not in fast
        assert name not in full


def test_correctness_targets_do_not_depend_on_shared_redis_helpers() -> None:
    database = _make_database()
    for target in CORRECTNESS_TARGETS:
        prereqs = _transitive_prerequisites(database, target)
        prereqs.add(target)
        assert "redis-up" not in prereqs
        assert "redis-down" not in prereqs
        recipe = _make_dry_run(target)
        assert "redis-up" not in recipe
        assert "redis-down" not in recipe
        assert "docker-compose.redis.yml" not in recipe


def test_lint_check_leaf_runs_ruff_check_and_format_check() -> None:
    recipe = _make_dry_run("lint-check")
    lines = [line.strip() for line in recipe.splitlines() if line.strip()]
    assert lines == [
        "ruff check src/ tests/",
        "ruff format --check src/ tests/",
    ]
    assert _direct_prerequisites(_make_database(), "lint-check") == set()


def test_typecheck_leaf_runs_mypy_src() -> None:
    recipe = _make_dry_run("typecheck")
    assert recipe.strip() == "mypy src/"
    assert _direct_prerequisites(_make_database(), "typecheck") == set()


def test_test_unit_leaf_preserves_marker_and_coverage() -> None:
    recipe = _make_dry_run("test-unit")
    assert recipe.strip() == ("pytest -m unit --cov=src/orcest --cov-report=term-missing")
    assert "--ignore=tests/integration" not in recipe
    assert _direct_prerequisites(_make_database(), "test-unit") == set()


def test_test_integration_leaf_uses_managed_redis_and_marker() -> None:
    recipe = _make_dry_run("test-integration")
    assert recipe.strip() == (
        "python3 -m tests.harness.supervisor python3 -m pytest -m integration "
        "--cov=src/orcest --cov-report=term-missing"
    )
    assert "tests/integration" not in recipe
    assert _direct_prerequisites(_make_database(), "test-integration") == set()


def test_test_stress_leaf_uses_managed_redis_and_marker() -> None:
    recipe = _make_dry_run("test-stress")
    assert recipe.strip() == (
        "python3 -m tests.harness.supervisor python3 -m pytest -m stress "
        "--cov=src/orcest --cov-report=term-missing"
    )
    assert _direct_prerequisites(_make_database(), "test-stress") == set()


def test_test_dashboard_leaf_uses_clean_copy_wrapper() -> None:
    recipe = _make_dry_run("test-dashboard")
    assert "dashboard/scripts/check-tracked-files.sh" in recipe
    assert "tar -C" in recipe
    assert "--exclude='./node_modules'" in recipe
    assert "docker run" in recipe
    assert "tar -C /app -xf -" in recipe
    dashboard_cmd = (
        "npm ci && npm run typecheck && npm test && npm run build && npm run check:bundle-runtime"
    )
    assert dashboard_cmd in recipe
    assert "docker build" not in recipe
    assert _direct_prerequisites(_make_database(), "test-dashboard") == {"check-dashboard-tracked"}


def test_check_fast_dry_run_executes_only_fast_leaves() -> None:
    recipe = _make_dry_run("check-fast")
    assert "ruff check src/ tests/" in recipe
    assert "ruff format --check src/ tests/" in recipe
    assert "mypy src/" in recipe
    assert "pytest -m unit --cov=src/orcest --cov-report=term-missing" in recipe
    assert "pytest -m integration" not in recipe
    assert "pytest -m stress" not in recipe
    assert "npm ci" not in recipe
    assert "tests.harness.supervisor" not in recipe


def test_check_full_dry_run_executes_local_leaves_not_image_builds() -> None:
    recipe = _make_dry_run("check-full")
    assert "ruff check src/ tests/" in recipe
    assert "ruff format --check src/ tests/" in recipe
    assert "mypy src/" in recipe
    assert "pytest -m unit --cov=src/orcest --cov-report=term-missing" in recipe
    assert "python3 -m tests.harness.supervisor python3 -m pytest -m integration" in recipe
    assert "python3 -m tests.harness.supervisor python3 -m pytest -m stress" in recipe
    dashboard_cmd = (
        "npm ci && npm run typecheck && npm test && npm run build && npm run check:bundle-runtime"
    )
    assert dashboard_cmd in recipe
    assert "docker build" not in recipe
    assert "smoke-image.sh" not in recipe
    assert "smoke-compose.sh" not in recipe
    assert "npm audit" not in recipe
