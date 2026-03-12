"""Tests for orcest.fleet.cli."""

import pytest
import yaml
from click.testing import CliRunner

from orcest.fleet.cli import fleet
from orcest.fleet.inventory import (
    FleetInventory,
    ProjectEntry,
    WorkerEntry,
    save_inventory,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def runner():
    try:
        return CliRunner(mix_stderr=False)
    except TypeError:
        return CliRunner()


@pytest.fixture
def inv_path(tmp_path):
    """Path to a temporary fleet inventory file."""
    return str(tmp_path / "fleet.yaml")


def _save(inv, path):
    save_inventory(inv, path)


def test_status_no_projects(runner, inv_path):
    """fleet status with no projects shows empty message."""
    _save(FleetInventory(), inv_path)
    result = runner.invoke(fleet, ["status", "--inventory", inv_path])
    assert result.exit_code == 0
    assert "No projects" in result.output


def test_status_shows_projects(runner, inv_path):
    """fleet status lists projects with their details."""
    inv = FleetInventory(
        projects=[
            ProjectEntry(
                name="alpha",
                repo="Org/alpha",
                redis_port=6379,
                workers=[WorkerEntry(vm_id=200)],
            ),
        ]
    )
    _save(inv, inv_path)
    result = runner.invoke(fleet, ["status", "--inventory", inv_path])
    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "Org/alpha" in result.output
    assert "200" in result.output


def test_onboard_creates_project(runner, inv_path, mocker):
    """fleet onboard creates a new project entry in the inventory."""
    _save(FleetInventory(orchestrator_host="10.20.0.23"), inv_path)
    mocker.patch("orcest.fleet.orchestrator_deploy.deploy_project_stack")
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "ThayneStudio/my-project",
            "--inventory",
            inv_path,
            "--github-token",
            "ghp_fake",
            "--claude-token",
            "sk-fake",
        ],
    )
    assert result.exit_code == 0
    assert "my-project" in result.output

    # Verify inventory was updated
    with open(inv_path) as f:
        data = yaml.safe_load(f)
    assert len(data["projects"]) == 1
    assert data["projects"][0]["name"] == "my-project"
    assert data["projects"][0]["repo"] == "ThayneStudio/my-project"
    assert data["projects"][0]["redis_port"] == 6379


def test_onboard_custom_name(runner, inv_path, mocker):
    """fleet onboard --name overrides the derived project name."""
    _save(FleetInventory(orchestrator_host="10.20.0.23"), inv_path)
    mocker.patch("orcest.fleet.orchestrator_deploy.deploy_project_stack")
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "ThayneStudio/my-project",
            "--name",
            "custom-name",
            "--inventory",
            inv_path,
            "--github-token",
            "ghp_fake",
            "--claude-token",
            "sk-fake",
        ],
    )
    assert result.exit_code == 0
    with open(inv_path) as f:
        data = yaml.safe_load(f)
    assert data["projects"][0]["name"] == "custom-name"


def test_onboard_requires_orchestrator_host(runner, inv_path):
    """fleet onboard fails if orchestrator_host is not set."""
    _save(FleetInventory(), inv_path)
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "Org/repo",
            "--inventory",
            inv_path,
            "--github-token",
            "ghp_fake",
            "--claude-token",
            "sk-fake",
        ],
    )
    assert result.exit_code != 0
    assert "orchestrator_host not set" in result.output


def test_onboard_duplicate_fails(runner, inv_path):
    """fleet onboard fails if project already exists."""
    inv = FleetInventory(
        orchestrator_host="10.20.0.23",
        projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
    )
    _save(inv, inv_path)
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "Org/alpha",
            "--name",
            "alpha",
            "--inventory",
            inv_path,
            "--github-token",
            "ghp_fake",
            "--claude-token",
            "sk-fake",
        ],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_add_worker_appends_to_project(runner, inv_path):
    """fleet add-worker adds a new worker to an existing project."""
    inv = FleetInventory(
        projects=[
            ProjectEntry(
                name="alpha",
                repo="Org/alpha",
                redis_port=6379,
                workers=[WorkerEntry(vm_id=200)],
            )
        ]
    )
    _save(inv, inv_path)
    result = runner.invoke(
        fleet,
        [
            "add-worker",
            "alpha",
            "--inventory",
            inv_path,
            "--github-token",
            "ghp_fake",
            "--claude-token",
            "sk-fake",
        ],
    )
    assert result.exit_code == 0

    with open(inv_path) as f:
        data = yaml.safe_load(f)
    assert len(data["projects"][0]["workers"]) == 2
    assert data["projects"][0]["workers"][1]["vm_id"] == 201


def test_add_worker_missing_project(runner, inv_path):
    """fleet add-worker fails if project doesn't exist."""
    _save(FleetInventory(), inv_path)
    result = runner.invoke(
        fleet,
        [
            "add-worker",
            "nonexistent",
            "--inventory",
            inv_path,
            "--github-token",
            "ghp_fake",
            "--claude-token",
            "sk-fake",
        ],
    )
    assert result.exit_code != 0
    assert "not found" in result.output


def test_destroy_removes_project(runner, inv_path):
    """fleet destroy removes the project from inventory."""
    inv = FleetInventory(
        projects=[
            ProjectEntry(name="alpha", repo="Org/alpha"),
            ProjectEntry(name="beta", repo="Org/beta"),
        ]
    )
    _save(inv, inv_path)
    result = runner.invoke(
        fleet,
        ["destroy", "alpha", "--inventory", inv_path, "--yes"],
    )
    assert result.exit_code == 0

    with open(inv_path) as f:
        data = yaml.safe_load(f)
    assert len(data["projects"]) == 1
    assert data["projects"][0]["name"] == "beta"


def test_destroy_missing_project(runner, inv_path):
    """fleet destroy fails if project doesn't exist."""
    _save(FleetInventory(), inv_path)
    result = runner.invoke(
        fleet,
        ["destroy", "nonexistent", "--inventory", inv_path, "--yes"],
    )
    assert result.exit_code != 0
    assert "not found" in result.output
