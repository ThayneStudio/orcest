"""Tests for orcest.fleet.cli."""

import pytest
import yaml
from click.testing import CliRunner

from orcest.fleet.cli import fleet
from orcest.fleet.config import (
    FleetConfig,
    OrchestratorConfig,
    OrgEntry,
    ProjectEntry,
    ProxmoxConfig,
    save_config,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def runner():
    try:
        return CliRunner(mix_stderr=False)
    except TypeError:
        return CliRunner()


@pytest.fixture
def cfg_path(tmp_path):
    """Path to a temporary fleet config file."""
    return str(tmp_path / "config.yaml")


def _save(cfg, path):
    save_config(cfg, path)


def test_status_no_projects(runner, cfg_path):
    """fleet status with no projects shows empty message."""
    _save(FleetConfig(), cfg_path)
    result = runner.invoke(fleet, ["status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "No projects" in result.output


def test_status_shows_projects(runner, cfg_path):
    """fleet status lists projects with their details."""
    cfg = FleetConfig(
        projects=[
            ProjectEntry(
                name="alpha",
                repo="Org/alpha",
                redis_port=6379,
                workers=1,
            ),
        ]
    )
    _save(cfg, cfg_path)
    result = runner.invoke(fleet, ["status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "Org/alpha" in result.output


def test_onboard_creates_project(runner, cfg_path, mocker):
    """fleet onboard creates a new project entry in the config."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_token="sk-fake")},
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.deploy_stack")
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "ThayneStudio/my-project",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code == 0, result.output
    assert "my-project" in result.output

    # Verify config was updated
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert len(data["projects"]) == 1
    assert data["projects"][0]["name"] == "my-project"
    assert data["projects"][0]["repo"] == "ThayneStudio/my-project"
    assert data["projects"][0]["redis_port"] == 6379


def test_onboard_custom_name(runner, cfg_path, mocker):
    """fleet onboard --name overrides the derived project name."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_token="sk-fake")},
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.deploy_stack")
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "ThayneStudio/my-project",
            "--name",
            "custom-name",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code == 0, result.output
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["projects"][0]["name"] == "custom-name"


def test_onboard_requires_orchestrator_host(runner, cfg_path):
    """fleet onboard fails if orchestrator host is not set."""
    cfg = FleetConfig(
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_token="sk-fake")},
    )
    _save(cfg, cfg_path)
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "Org/repo",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code != 0
    assert "Orchestrator host not set" in result.output


def test_onboard_requires_org(runner, cfg_path):
    """fleet onboard fails if org is not registered."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
    )
    _save(cfg, cfg_path)
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "UnknownOrg/repo",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code != 0
    assert "not found" in result.output


def test_onboard_duplicate_fails(runner, cfg_path):
    """fleet onboard fails if project already exists."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_token="sk-fake")},
        projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
    )
    _save(cfg, cfg_path)
    result = runner.invoke(
        fleet,
        [
            "onboard",
            "Org/alpha",
            "--name",
            "alpha",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_add_worker_increments_count(runner, cfg_path, mocker):
    """fleet add-worker increments the worker count for a project."""
    cfg = FleetConfig(
        projects=[
            ProjectEntry(
                name="alpha",
                repo="Org/alpha",
                redis_port=6379,
                workers=1,
            )
        ]
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    result = runner.invoke(
        fleet,
        [
            "add-worker",
            "alpha",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code == 0, result.output

    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["projects"][0]["workers"] == 2


def test_add_worker_missing_project(runner, cfg_path):
    """fleet add-worker fails if project doesn't exist."""
    _save(FleetConfig(), cfg_path)
    result = runner.invoke(
        fleet,
        [
            "add-worker",
            "nonexistent",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code != 0
    assert "not found" in result.output


def test_destroy_removes_project(runner, cfg_path, mocker):
    """fleet destroy removes the project from config."""
    cfg = FleetConfig(
        projects=[
            ProjectEntry(name="alpha", repo="Org/alpha"),
            ProjectEntry(name="beta", repo="Org/beta"),
        ]
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    result = runner.invoke(
        fleet,
        ["destroy", "alpha", "--config", cfg_path, "--yes"],
    )
    assert result.exit_code == 0, result.output

    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert len(data["projects"]) == 1
    assert data["projects"][0]["name"] == "beta"


def test_destroy_missing_project(runner, cfg_path):
    """fleet destroy fails if project doesn't exist."""
    _save(FleetConfig(), cfg_path)
    result = runner.invoke(
        fleet,
        ["destroy", "nonexistent", "--config", cfg_path, "--yes"],
    )
    assert result.exit_code != 0
    assert "not found" in result.output


def test_add_org_registers_credentials(runner, cfg_path, mocker):
    """fleet add-org registers an org with credentials."""
    _save(FleetConfig(), cfg_path)
    mocker.patch("subprocess.run")  # mock gh auth status
    result = runner.invoke(
        fleet,
        [
            "add-org",
            "MyOrg",
            "--github-token",
            "ghp_test123",
            "--claude-token",
            "sk-test456",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code == 0, result.output
    assert "MyOrg" in result.output

    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert "MyOrg" in data["orgs"]
    assert data["orgs"]["MyOrg"]["github_token"] == "ghp_test123"
