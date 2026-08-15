"""Tests for orcest.fleet.cli."""

import subprocess
import time
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from orcest.fleet.cli import fleet
from orcest.fleet.config import (
    FleetConfig,
    OrchestratorConfig,
    OrgEntry,
    PoolConfig,
    ProjectEntry,
    ProxmoxConfig,
    load_config,
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


@pytest.fixture(autouse=True)
def _no_pending_workers_by_default(mocker):
    mocker.patch("orcest.fleet.orchestrator.get_workers_with_pending_tasks", return_value=set())
    mocker.patch("orcest.fleet.orchestrator.set_workers_draining")
    mocker.patch("orcest.fleet.orchestrator.get_deployed_pool_backend", return_value=None)
    mocker.patch("orcest.fleet.cli._wait_for_worker_drain_quiescence")


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
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.image_exists", return_value=True)
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


def test_onboard_custom_name(runner, cfg_path, mocker):
    """fleet onboard --name overrides the derived project name."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.image_exists", return_value=True)
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


def test_onboard_mints_and_passes_redis_password(runner, cfg_path, mocker):
    """C1: onboard must mint the Redis password and pass it to generate_env_file
    (read from the minted .redis.env) so the per-project .env carries the value
    the orchestrator container uses to AUTH. Round 1 left this caller without a
    redis_password arg, so the param was dead code."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
    )
    _save(cfg, cfg_path)
    gen = mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.image_exists", return_value=True)
    mocker.patch("orcest.fleet.orchestrator.deploy_stack")
    mint = mocker.patch(
        "orcest.fleet.orchestrator.ensure_redis_password", return_value="minted-pw-123"
    )
    result = runner.invoke(fleet, ["onboard", "ThayneStudio/my-project", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    mint.assert_called_once()
    assert gen.call_args.kwargs.get("redis_password") == "minted-pw-123"


def test_onboard_passes_pool_backend_as_generated_default_runner(runner, cfg_path, mocker):
    """A clauder worker pool must not be stranded by generated project configs
    that still publish legacy Claude work to tasks:claude."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        pool=PoolConfig(worker_backend="clauder"),
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    gen_config = mocker.patch(
        "orcest.fleet.orchestrator.generate_orchestrator_config",
        return_value="",
    )
    mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.image_exists", return_value=True)
    mocker.patch("orcest.fleet.orchestrator.deploy_stack")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")

    result = runner.invoke(fleet, ["onboard", "ThayneStudio/my-project", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert gen_config.call_args.kwargs["default_runner"] == "clauder"


def test_onboard_refuses_project_when_deployed_pool_backend_differs(runner, cfg_path, mocker):
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        pool=PoolConfig(worker_backend="clauder"),
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
    )
    _save(cfg, cfg_path)
    mocker.patch(
        "orcest.fleet.orchestrator.get_deployed_pool_backend",
        return_value="claude",
    )
    write_files = mocker.patch("orcest.fleet.orchestrator.write_project_files")

    result = runner.invoke(fleet, ["onboard", "ThayneStudio/my-project", "--config", cfg_path])

    assert result.exit_code != 0
    assert "uncoordinated worker backend change" in result.output
    write_files.assert_not_called()
    assert load_config(cfg_path).projects == []


def test_onboard_mints_password_before_starting_redis(runner, cfg_path, mocker):
    """KEY regression: the password must be minted BEFORE ensure_redis_stack so
    Redis never boots with an empty requirepass (FATAL boot / total outage)."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
    )
    _save(cfg, cfg_path)
    order: list[str] = []
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.image_exists", return_value=True)
    mocker.patch("orcest.fleet.orchestrator.deploy_stack")
    mocker.patch(
        "orcest.fleet.orchestrator.ensure_redis_password",
        side_effect=lambda *a, **k: (order.append("mint"), "pw")[1],
    )
    mocker.patch(
        "orcest.fleet.orchestrator.ensure_redis_stack",
        side_effect=lambda *a, **k: order.append("redis"),
    )
    result = runner.invoke(fleet, ["onboard", "ThayneStudio/my-project", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "mint" in order and "redis" in order
    assert order.index("mint") < order.index("redis"), (
        "password must be minted before the redis stack is started"
    )


def test_onboard_requires_orchestrator_host(runner, cfg_path):
    """fleet onboard fails if orchestrator host is not set."""
    cfg = FleetConfig(
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
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
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
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


def test_destroy_removes_project(runner, cfg_path, mocker):
    """fleet destroy removes the project from config."""
    cfg = FleetConfig(
        projects=[
            ProjectEntry(name="alpha", repo="Org/alpha"),
            ProjectEntry(name="beta", repo="Org/beta"),
        ]
    )
    _save(cfg, cfg_path)
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
    cfg = FleetConfig(orchestrator=OrchestratorConfig(host="10.20.0.23"))
    _save(cfg, cfg_path)
    mocker.patch(
        "orcest.fleet.cli._run_on_orchestrator",
        return_value=mocker.MagicMock(returncode=0, stdout="", stderr=""),
    )
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


def test_add_org_skips_validation_without_orchestrator(runner, cfg_path):
    """fleet add-org skips token validation when orchestrator is not configured."""
    _save(FleetConfig(), cfg_path)
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
    assert "skipped" in result.output
    # Credentials should still be saved
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert "MyOrg" in data["orgs"]


def test_add_org_saves_on_validation_failure(runner, cfg_path, mocker):
    """fleet add-org warns but saves credentials when token validation fails."""
    cfg = FleetConfig(orchestrator=OrchestratorConfig(host="10.20.0.23"))
    _save(cfg, cfg_path)
    mocker.patch(
        "orcest.fleet.cli._run_on_orchestrator",
        return_value=mocker.MagicMock(returncode=1, stdout="", stderr="bad token"),
    )
    result = runner.invoke(
        fleet,
        [
            "add-org",
            "MyOrg",
            "--github-token",
            "ghp_bad",
            "--claude-token",
            "sk-test456",
            "--config",
            cfg_path,
        ],
    )
    assert result.exit_code == 0, result.output
    assert "failed" in result.output
    assert "saving anyway" in result.output
    # Credentials should still be saved
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["orgs"]["MyOrg"]["github_token"] == "ghp_bad"


def test_add_org_skips_on_connection_error(runner, cfg_path, mocker):
    """fleet add-org skips validation when orchestrator is unreachable."""
    cfg = FleetConfig(orchestrator=OrchestratorConfig(host="10.20.0.23"))
    _save(cfg, cfg_path)
    mocker.patch(
        "orcest.fleet.cli._run_on_orchestrator",
        side_effect=OSError("Connection refused"),
    )
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
    assert "skipped" in result.output
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert "MyOrg" in data["orgs"]


def test_add_org_skips_on_timeout(runner, cfg_path, mocker):
    """fleet add-org skips validation when orchestrator command times out."""
    cfg = FleetConfig(orchestrator=OrchestratorConfig(host="10.20.0.23"))
    _save(cfg, cfg_path)
    mocker.patch(
        "orcest.fleet.cli._run_on_orchestrator",
        side_effect=subprocess.TimeoutExpired(cmd="ssh", timeout=30),
    )
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
    assert "skipped" in result.output
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert "MyOrg" in data["orgs"]


def test_create_orchestrator(runner, cfg_path, mocker):
    """fleet create-orchestrator creates VM and deploys Docker stack."""
    cfg = FleetConfig(
        proxmox=ProxmoxConfig(
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(ssh_key="ssh-ed25519 AAAA..."),
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.99")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")

    result = runner.invoke(
        fleet,
        ["create-orchestrator", "--vm-id", "199", "--storage", "local-lvm", "--config", cfg_path],
    )
    assert result.exit_code == 0, result.output
    assert "10.20.0.99" in result.output

    # Verify config was updated with orchestrator host
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["orchestrator"]["host"] == "10.20.0.99"


def test_create_orchestrator_starts_pool_manager_for_template_range_only(runner, cfg_path, mocker):
    cfg = FleetConfig(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(ssh_key="ssh-ed25519 AAAA..."),
        pool=PoolConfig(template_vm_id=0, template_vmid_range=[9000, 9009]),
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.99")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    upload_config = mocker.patch("orcest.fleet.orchestrator.upload_fleet_config")
    ensure_pool = mocker.patch("orcest.fleet.orchestrator.ensure_pool_manager")

    result = runner.invoke(
        fleet,
        ["create-orchestrator", "--vm-id", "199", "--storage", "local-lvm", "--config", cfg_path],
    )

    assert result.exit_code == 0, result.output
    upload_config.assert_called_once()
    ensure_pool.assert_called_once()


def test_create_orchestrator_ssh_timeout(runner, cfg_path, mocker):
    """fleet create-orchestrator saves config and exits if SSH times out."""
    cfg = FleetConfig(
        proxmox=ProxmoxConfig(
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.provisioner.generate_tfvars", return_value={})
    mocker.patch("orcest.fleet.provisioner.write_tfvars")
    mocker.patch("orcest.fleet.provisioner.apply")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.99")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=False)

    result = runner.invoke(
        fleet,
        ["create-orchestrator", "--vm-id", "199", "--storage", "local-lvm", "--config", cfg_path],
    )
    assert result.exit_code != 0

    # Config should still be saved with the IP
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["orchestrator"]["host"] == "10.20.0.99"


def test_update_rebuilds_and_restarts(runner, cfg_path, mocker):
    """fleet update uploads source, rebuilds image, and restarts stacks."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
        projects=[
            ProjectEntry(name="alpha", repo="Org/alpha"),
            ProjectEntry(name="beta", repo="Org/beta"),
        ],
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mock_write = mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mock_ensure_redis = mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mock_restart = mocker.patch("orcest.fleet.orchestrator.restart_stack")

    result = runner.invoke(fleet, ["update", "--config", cfg_path])
    assert result.exit_code == 0, result.output

    # Should update shared Redis stack and restart both project stacks
    mock_ensure_redis.assert_called_once()
    assert mock_write.call_count == 2
    assert mock_restart.call_count == 2


def test_update_regenerates_project_files_with_current_pool_backend(runner, cfg_path, mocker):
    """Existing project configs must follow backend changes such as claude -> clauder."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(worker_backend="clauder"),
        trace_archive_host_path="/mnt/orcest/traces",
        orgs={
            "Org": OrgEntry(
                github_token="ghp_fake",
                claude_oauth_tokens=["sk-claude"],
                provider_credentials={"grok": ["grok-json"]},
            )
        },
        projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="redis-pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    gen_env = mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="env")
    gen_config = mocker.patch(
        "orcest.fleet.orchestrator.generate_orchestrator_config",
        return_value="yaml",
    )
    write_files = mocker.patch("orcest.fleet.orchestrator.write_project_files")
    mocker.patch("orcest.fleet.orchestrator.restart_stack")

    result = runner.invoke(fleet, ["update", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert gen_env.call_args.kwargs == {
        "github_token": "ghp_fake",
        "key_prefix": "alpha",
        "project_name": "alpha",
        "claude_tokens": ["sk-claude"],
        "provider_credentials": {"grok": ["grok-json"]},
        "trace_archive_host_path": "/mnt/orcest/traces",
        "redis_password": "redis-pw",
    }
    assert gen_config.call_args.kwargs == {
        "repo": "Org/alpha",
        "key_prefix": "alpha",
        "extra_providers": ["grok"],
        "default_runner": "clauder",
        "trace_archive_enabled": True,
    }
    write_files.assert_called_once_with("orcest@10.20.0.23", "alpha", "env", "yaml")


def test_update_refuses_uncoordinated_worker_backend_change(runner, cfg_path, mocker):
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(worker_backend="clauder"),
    )
    _save(cfg, cfg_path)
    mocker.patch(
        "orcest.fleet.orchestrator.get_deployed_pool_backend",
        return_value="claude",
    )
    upload = mocker.patch("orcest.fleet.orchestrator.upload_source")

    result = runner.invoke(fleet, ["update", "--config", cfg_path])

    assert result.exit_code != 0
    assert "uncoordinated worker backend change" in result.output
    upload.assert_not_called()


def test_update_has_no_user_callable_backend_change_bypass(runner, cfg_path, mocker):
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(worker_backend="clauder"),
    )
    _save(cfg, cfg_path)
    upload = mocker.patch("orcest.fleet.orchestrator.upload_source")

    result = runner.invoke(
        fleet,
        ["update", "--allow-backend-change", "--config", cfg_path],
    )

    assert result.exit_code != 0
    assert "No such option: --allow-backend-change" in result.output
    upload.assert_not_called()


def test_update_does_not_regenerate_project_files_without_redis_password(
    runner,
    cfg_path,
    mocker,
):
    """Regenerating .env without ORCEST_REDIS_PASSWORD would break deployed stacks."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
        projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch(
        "orcest.fleet.orchestrator.ensure_redis_password",
        side_effect=RuntimeError("missing redis env"),
    )
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    write_files = mocker.patch("orcest.fleet.orchestrator.write_project_files")
    restart = mocker.patch("orcest.fleet.orchestrator.restart_stack")

    result = runner.invoke(fleet, ["update", "--config", cfg_path])

    assert result.exit_code != 0
    assert "Redis password unavailable" in result.output
    write_files.assert_not_called()
    restart.assert_not_called()


def test_update_refreshes_pool_manager_for_template_range_only(runner, cfg_path, mocker):
    """fleet update refreshes pool manager when range mode has no legacy template ID."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(template_vm_id=0, template_vmid_range=[9000, 9009]),
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
        projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    order: list[str] = []
    mocker.patch(
        "orcest.fleet.orchestrator.write_project_files",
        side_effect=lambda *_args, **_kwargs: order.append("write_project_files"),
    )
    mock_upload_config = mocker.patch("orcest.fleet.orchestrator.upload_fleet_config")
    mock_ensure_pool = mocker.patch("orcest.fleet.orchestrator.ensure_pool_manager")
    mock_upload_config.side_effect = lambda *_args, **_kwargs: order.append("upload_fleet_config")
    mock_ensure_pool.side_effect = lambda *_args, **_kwargs: order.append("ensure_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.restart_stack",
        side_effect=lambda *_args, **_kwargs: order.append("restart_stack"),
    )

    result = runner.invoke(fleet, ["update", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    mock_upload_config.assert_called_once()
    mock_ensure_pool.assert_called_once()
    assert order == [
        "write_project_files",
        "restart_stack",
        "upload_fleet_config",
        "ensure_pool_manager",
    ]


def test_update_requires_orchestrator_host(runner, cfg_path):
    """fleet update fails if orchestrator host is not set."""
    _save(FleetConfig(), cfg_path)
    result = runner.invoke(fleet, ["update", "--config", cfg_path])
    assert result.exit_code != 0
    assert "Orchestrator host not set" in result.output


# ── create-template tests ───────────────────────────────────


def _proxmox_cfg(**overrides):
    """Build a FleetConfig with Proxmox credentials for template tests."""
    defaults = dict(
        proxmox=ProxmoxConfig(
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
            node="pve",
        ),
        orchestrator=OrchestratorConfig(
            user="orcest",
            ssh_key="ssh-ed25519 AAAA...",
        ),
        pool=PoolConfig(storage="ssd-pool"),
    )
    defaults.update(overrides)
    return FleetConfig(**defaults)


def _mock_proxmox_client(mocker):
    """Create and return a mock ProxmoxClient, patching _create_proxmox_client."""
    mock_px = mocker.MagicMock()
    mock_px.next_free_vmid.return_value = 200
    mock_px.get_vm_ip.return_value = "10.20.0.50"
    mock_px.get_vm_status.return_value = "stopped"
    mock_px.list_vms.return_value = []
    _all_storage = [
        {
            "storage": "local-lvm",
            "type": "lvmthin",
            "content": "images,rootdir",
            "avail": 1e12,
            "enabled": 1,
            "active": 1,
        },
        {
            "storage": "local",
            "type": "dir",
            "content": "snippets,iso,backup",
            "avail": 1e11,
            "enabled": 1,
            "active": 1,
        },
    ]

    def _list_storage(content_type=None):
        if content_type:
            return [s for s in _all_storage if content_type in s["content"].split(",")]
        return _all_storage

    mock_px.list_storage.side_effect = _list_storage
    mocker.patch("orcest.fleet.cli._create_proxmox_client", return_value=mock_px)
    return mock_px


def test_create_template_success(runner, cfg_path, mocker):
    """create-template creates VM from cloud image, provisions, and converts to template."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.cli._install_source_on_worker_template", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
        input="\n",
    )
    assert result.exit_code == 0, result.output
    assert "Worker template created" in result.output

    # Verify Proxmox operations happened in order
    mock_px.start_vm.assert_called_once_with(200)
    mock_px.shutdown_vm.assert_called_once_with(200, timeout=60)
    mock_px.convert_to_template.assert_called_once_with(200)

    # Verify config was updated with template_vm_id
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["pool"]["template_vm_id"] == 200


def test_create_template_prompts_for_vm_id(runner, cfg_path, mocker):
    """create-template prompts for template VM ID when not provided."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._next_free_vmid", return_value=300)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.cli._install_source_on_worker_template", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )

    # Accept default template VM ID and default worker VM ID range start
    result = runner.invoke(
        fleet,
        ["create-template", "--config", cfg_path],
        input="\n\n",
    )
    assert result.exit_code == 0, result.output
    assert "VM ID for new template" in result.output


def test_create_template_no_proxmox_creds(runner, cfg_path):
    """create-template fails if Proxmox API credentials are missing."""
    cfg = FleetConfig()  # No proxmox credentials
    _save(cfg, cfg_path)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "Proxmox API credentials not configured" in result.output


def test_create_template_image_import_failure(runner, cfg_path, mocker):
    """create-template exits on cloud image creation failure."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch(
        "orcest.fleet.cli._create_vm_from_cloud_image",
        side_effect=RuntimeError("download failed"),
    )

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "download failed" in result.output
    # Should attempt best-effort cleanup
    mock_px.destroy_vm.assert_called_once_with(200)


def test_create_template_cloud_init_failure_cleans_up(runner, cfg_path, mocker):
    """create-template destroys the VM if cloud-init config fails."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch(
        "orcest.fleet.cli._set_vm_cloud_init",
        side_effect=RuntimeError("upload failed"),
    )

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    # Should attempt cleanup
    mock_px.destroy_vm.assert_called_once_with(200)


def test_create_template_ip_timeout(runner, cfg_path, mocker):
    """create-template aborts and cleans up if VM IP times out."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value=None)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "Could not get VM IP" in result.output
    mock_px.destroy_vm.assert_called_once_with(200)


def test_create_template_ssh_timeout(runner, cfg_path, mocker):
    """create-template aborts and cleans up if SSH times out."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=False)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "SSH not available" in result.output
    mock_px.destroy_vm.assert_called_once_with(200)


def test_create_template_cloud_init_timeout(runner, cfg_path, mocker):
    """create-template aborts and cleans up if cloud-init times out."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=False)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "Cloud-init timed out" in result.output
    mock_px.destroy_vm.assert_called_once_with(200)


def test_create_template_disable_cloud_init_failure(runner, cfg_path, mocker):
    """create-template aborts and cleans up if disabling cloud-init fails."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    # Let the provider-CLI smoke-check (step 6b) pass so this test exercises the
    # *cloud-init clean* step (step 7) failing -- the smoke-check shares the
    # _ssh_run mock and would otherwise abort first.
    mocker.patch("orcest.fleet.cli._verify_provider_clis", return_value=True)
    mocker.patch("orcest.fleet.cli._install_source_on_worker_template", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=1, stderr="permission denied"),
    )

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "permission denied" in result.output
    mock_px.destroy_vm.assert_called_once_with(200)


def test_create_template_stop_timeout_cleans_up(runner, cfg_path, mocker):
    """create-template aborts and cleans up if VM stop times out."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.cli._install_source_on_worker_template", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )
    # VM never reaches "stopped" state
    mock_px.get_vm_status.return_value = "running"
    # Patch time.monotonic to simulate deadline expiry without sleeping.
    # The base time is captured once; the first call returns it, all
    # subsequent calls jump far past any deadline (both the stop-wait
    # loop and the _cleanup_vm stop-wait).
    base = time.monotonic()
    call_count = 0

    def fast_monotonic():
        nonlocal call_count
        call_count += 1
        # Each call advances 120s, so any deadline (15s or 60s) is blown
        # on the second check.
        return base + (call_count - 1) * 120

    mocker.patch("orcest.fleet.cli.time.monotonic", new=fast_monotonic)
    mocker.patch("orcest.fleet.cli.time.sleep", new=lambda _: None)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "VM did not stop" in result.output
    mock_px.destroy_vm.assert_called_once_with(200)


# ── provider-CLI smoke-check tests (H1-infra repair 1) ──────


def test_create_template_fails_when_provider_cli_missing(runner, cfg_path, mocker):
    """H1-infra (repair 1): after cloud-init reports done, the bake SSHes a
    `command -v` check for every required provider CLI. If any is missing
    (e.g. a failed install mid-runcmd, which has no `set -e`), the bake must
    FAIL and CLEAN UP -- never convert-to-template / set the pointer.
    """
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    # The provider-CLI smoke-check reports a missing binary.
    mocker.patch("orcest.fleet.cli._verify_provider_clis", return_value=False)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
        input="\n",
    )
    assert result.exit_code != 0, result.output
    # Must never convert a half-baked VM to a template.
    mock_px.convert_to_template.assert_not_called()
    # Must clean up the half-built VM.
    mock_px.destroy_vm.assert_called_once_with(200)


def test_rebake_fails_and_keeps_pointer_when_provider_cli_missing(runner, cfg_path, mocker):
    """H1-infra (repair 1): a missing provider CLI must abort rebake BEFORE the
    pointer swap so a half-baked template can never become the active one.
    """
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest", ssh_key="ssh-ed25519 AAA"),
        pool=PoolConfig(
            template_vmid_range=[9000, 9009],
            template_vm_id=9000,
            storage="ssd-pool",
            snippet_storage="local",
        ),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True}
    ]
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.cli._verify_provider_clis", return_value=False)
    mock_set = mocker.patch("orcest.fleet.orchestrator.set_current_template_vmid")

    result = runner.invoke(fleet, ["rebake", "--config", cfg_path])

    assert result.exit_code != 0, result.output
    mock_px.convert_to_template.assert_not_called()
    mock_set.assert_not_called()
    mock_px.destroy_vm.assert_called_once_with(9001)


def test_create_template_fails_when_source_install_fails(runner, cfg_path, mocker):
    """A template must not be converted if it cannot install the deployed source."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.cli._verify_provider_clis", return_value=True)
    mocker.patch("orcest.fleet.cli._install_source_on_worker_template", return_value=False)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
        input="\n",
    )

    assert result.exit_code != 0, result.output
    assert "Source install failed" in result.output
    mock_px.convert_to_template.assert_not_called()
    mock_px.destroy_vm.assert_called_once_with(200)


def test_install_source_on_worker_template_uses_local_tarball(mocker, tmp_path):
    from rich.console import Console

    from orcest.fleet.cli import _install_source_on_worker_template

    tarball = tmp_path / "orcest-source.tar.gz"
    tarball.write_bytes(b"fake tarball")
    mocker.patch("orcest.fleet.orchestrator.create_source_tarball", return_value=str(tarball))
    scp = mocker.patch(
        "orcest.fleet.cli._scp_to_vm",
        return_value=mocker.MagicMock(returncode=0, stderr=""),
    )
    ssh = mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0, stderr=""),
    )

    assert _install_source_on_worker_template("10.20.0.50", "orcest", Console()) is True

    scp.assert_called_once_with(
        "10.20.0.50",
        "orcest",
        str(tarball),
        "/tmp/orcest-source.tar.gz",
    )
    install_cmd = ssh.call_args.args[2]
    assert "/tmp/orcest-template-source/requirements.lock" in install_cmd
    assert "--no-deps /tmp/orcest-template-source" in install_cmd
    assert "github.com/ThayneStudio/orcest.git" not in install_cmd
    assert not tarball.exists()


class TestVerifyProviderClis:
    """H1-infra (repair 1): the smoke-check itself."""

    def test_required_binaries_match_baked_set(self):
        """The required-binary list lives where the installs are defined
        (cloud_init.py) and enumerates the baked provider CLIs: claude, grok,
        codex.
        """
        from orcest.fleet.cloud_init import REQUIRED_PROVIDER_BINARIES

        assert set(REQUIRED_PROVIDER_BINARIES) == {"claude", "grok", "codex"}

    def test_passes_when_all_clis_present(self, mocker):
        from rich.console import Console

        from orcest.fleet.cli import _verify_provider_clis

        # Every `command -v <bin>` succeeds.
        mocker.patch(
            "orcest.fleet.cli._ssh_run",
            return_value=mocker.MagicMock(returncode=0, stdout="/usr/local/bin/x", stderr=""),
        )
        assert _verify_provider_clis("10.0.0.1", "orcest", Console()) is True

    def test_fails_when_a_cli_is_missing(self, mocker):
        from rich.console import Console

        from orcest.fleet.cli import _verify_provider_clis
        from orcest.fleet.cloud_init import REQUIRED_PROVIDER_BINARIES

        # First binary present, the rest missing (command -v returns non-zero).
        def fake_ssh(host, user, cmd, timeout=60):
            present = REQUIRED_PROVIDER_BINARIES[0]
            rc = 0 if present in cmd else 1
            return mocker.MagicMock(returncode=rc, stdout="", stderr="")

        mocker.patch("orcest.fleet.cli._ssh_run", side_effect=fake_ssh)
        assert _verify_provider_clis("10.0.0.1", "orcest", Console()) is False

    def test_checks_as_runtime_worker_user_on_path(self, mocker):
        """The check must verify the CLI is on PATH for the orcest *worker*
        user (the runtime user), so it catches the grok exec-permission
        regression -- not just root visibility. We assert the probe runs as
        the orcest user (sudo -u orcest) and uses `command -v`.
        """
        from rich.console import Console

        from orcest.fleet.cli import _verify_provider_clis

        seen_cmds: list[str] = []

        def fake_ssh(host, user, cmd, timeout=60):
            seen_cmds.append(cmd)
            return mocker.MagicMock(returncode=0, stdout="/x", stderr="")

        mocker.patch("orcest.fleet.cli._ssh_run", side_effect=fake_ssh)
        _verify_provider_clis("10.0.0.1", "orcest", Console())
        assert seen_cmds, "smoke-check ran no probes"
        for cmd in seen_cmds:
            assert "command -v" in cmd
            assert "orcest" in cmd  # probes the worker user, not root

    def test_transient_ssh_timeout_fails_closed(self, mocker):
        """A timed-out probe must fail closed (return False) rather than
        accept the template -- we cannot prove the CLI is installed.
        """
        from rich.console import Console

        from orcest.fleet.cli import _verify_provider_clis

        mocker.patch(
            "orcest.fleet.cli._ssh_run",
            side_effect=subprocess.TimeoutExpired(cmd="ssh", timeout=60),
        )
        assert _verify_provider_clis("10.0.0.1", "orcest", Console()) is False


# ── image-digest verification wiring tests (M5-infra repair 3) ──


def test_create_vm_from_cloud_image_passes_verified_checksum(mocker):
    """M5-infra (repair 3): the sole download caller must resolve a verified
    sha256 and forward it to download_image (checksum + algorithm), so the
    Proxmox node actually verifies the cloud image. Round 1 passed nothing,
    so download_image never verified.
    """
    from rich.console import Console

    from orcest.fleet.cli import _create_vm_from_cloud_image

    cfg = _proxmox_cfg()
    mock_px = mocker.MagicMock()
    # The resolver returns a verified digest (GPG-checked SHA256SUMS upstream).
    mocker.patch(
        "orcest.fleet.cli._resolve_image_checksum",
        return_value="deadbeef" * 8,
    )
    mocker.patch("orcest.fleet.cli.subprocess.run", return_value=mocker.MagicMock(returncode=0))

    _create_vm_from_cloud_image(
        mock_px,
        cfg,
        200,
        "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
        Console(),
        storage="ssd-pool",
        snippet_storage="local",
    )

    mock_px.download_image.assert_called_once()
    kwargs = mock_px.download_image.call_args.kwargs
    assert kwargs.get("checksum") == "deadbeef" * 8
    assert kwargs.get("checksum_algorithm") == "sha256"


def test_create_vm_from_cloud_image_aborts_when_checksum_unresolvable(mocker):
    """M5-infra (repair 3): verification is FAIL-CLOSED. If the digest cannot
    be resolved/verified, the bake must raise -- never download unverified.
    """
    from rich.console import Console

    from orcest.fleet.cli import _create_vm_from_cloud_image

    cfg = _proxmox_cfg()
    mock_px = mocker.MagicMock()
    mocker.patch(
        "orcest.fleet.cli._resolve_image_checksum",
        side_effect=RuntimeError("GPG signature verification failed"),
    )

    with pytest.raises(RuntimeError, match="GPG signature verification failed"):
        _create_vm_from_cloud_image(
            mock_px,
            cfg,
            200,
            "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
            Console(),
            storage="ssd-pool",
            snippet_storage="local",
        )
    mock_px.download_image.assert_not_called()


class TestResolveImageChecksum:
    """M5-infra (repair 3): the GPG-verified SHA256SUMS resolver."""

    def test_uses_config_pinned_sha256_without_network(self, mocker):
        """A pinned pool.expected_image_sha256 short-circuits the GPG fetch
        (air-gapped / offline bakes) and is returned directly. No subprocess
        (gpg/curl) is invoked.
        """
        from rich.console import Console

        from orcest.fleet.cli import _resolve_image_checksum

        cfg = _proxmox_cfg(pool=PoolConfig(expected_image_sha256="f" * 64))
        run = mocker.patch("orcest.fleet.cli.subprocess.run")
        digest = _resolve_image_checksum(
            "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
            cfg,
            Console(),
        )
        assert digest == "f" * 64
        run.assert_not_called()

    def test_gpg_verifies_and_extracts_sha_for_filename(self, mocker, tmp_path):
        """Without a pinned digest: fetch SHA256SUMS + .gpg, GPG-verify against
        the expected key, and extract the sha256 for the pinned image filename.
        """
        from rich.console import Console

        from orcest.fleet.cli import _resolve_image_checksum

        cfg = _proxmox_cfg(
            pool=PoolConfig(expected_image_gpg_key="D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81")
        )
        target_sha = "a1b2c3" + "0" * 58
        sums = (
            f"{target_sha} *noble-server-cloudimg-amd64.img\n"
            "ffff000000000000000000000000000000000000000000000000000000000000 *other.img\n"
        )

        # Sequence of subprocess.run calls inside the resolver:
        #   1. fetch SHA256SUMS, 2. fetch SHA256SUMS.gpg,
        #   3. gpg --recv-keys (import), 4. gpg --verify (VALIDSIG present)
        def fake_run(cmd, *args, **kwargs):
            joined = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
            if "curl" in joined and "-o" in cmd:
                # curl -f -o <path> writes the file on success. The .gpg
                # detached signature content is opaque to the resolver.
                out = cmd[cmd.index("-o") + 1]
                Path(out).write_text(sums if out.endswith("SHA256SUMS") else "SIG")
                return mocker.MagicMock(returncode=0, stdout="", stderr="")
            if "--recv-keys" in joined or "--list-keys" in joined:
                return mocker.MagicMock(returncode=0, stdout="", stderr="")
            if "--verify" in joined:
                return mocker.MagicMock(
                    returncode=0,
                    stdout="[GNUPG:] VALIDSIG X D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81\n",
                    stderr="",
                )
            return mocker.MagicMock(returncode=0, stdout="", stderr="")

        mocker.patch("orcest.fleet.cli.subprocess.run", side_effect=fake_run)
        mocker.patch("orcest.fleet.cli.tempfile.mkdtemp", return_value=str(tmp_path))

        digest = _resolve_image_checksum(
            "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
            cfg,
            Console(),
        )
        assert digest == target_sha

    def test_raises_when_gpg_signature_invalid(self, mocker, tmp_path):
        """Fail-closed: a bad/missing GPG signature must raise, never return a
        digest that could have been tampered with.
        """
        from rich.console import Console

        from orcest.fleet.cli import _resolve_image_checksum

        cfg = _proxmox_cfg(
            pool=PoolConfig(expected_image_gpg_key="D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81")
        )

        def fake_run(cmd, *args, **kwargs):
            joined = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
            if "SHA256SUMS" in joined and "curl" in joined and "-o" in cmd:
                out = cmd[cmd.index("-o") + 1]
                Path(out).write_text("deadbeef *noble-server-cloudimg-amd64.img\n")
                return mocker.MagicMock(returncode=0, stdout="", stderr="")
            if "--verify" in joined:
                # No VALIDSIG line / non-zero => signature failed.
                return mocker.MagicMock(returncode=1, stdout="", stderr="BADSIG")
            return mocker.MagicMock(returncode=0, stdout="", stderr="")

        mocker.patch("orcest.fleet.cli.subprocess.run", side_effect=fake_run)
        mocker.patch("orcest.fleet.cli.tempfile.mkdtemp", return_value=str(tmp_path))

        with pytest.raises(RuntimeError, match="GPG"):
            _resolve_image_checksum(
                "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
                cfg,
                Console(),
            )

    def test_raises_when_filename_absent_from_sums(self, mocker, tmp_path):
        """Fail-closed: if the pinned image filename is not present in the
        verified SHA256SUMS, raise rather than download unverified.
        """
        from rich.console import Console

        from orcest.fleet.cli import _resolve_image_checksum

        cfg = _proxmox_cfg(
            pool=PoolConfig(expected_image_gpg_key="D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81")
        )

        def fake_run(cmd, *args, **kwargs):
            joined = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
            if "SHA256SUMS" in joined and "curl" in joined and "-o" in cmd:
                out = cmd[cmd.index("-o") + 1]
                Path(out).write_text("aaaa *some-other-image.img\n")
                return mocker.MagicMock(returncode=0, stdout="", stderr="")
            if "--verify" in joined:
                return mocker.MagicMock(
                    returncode=0,
                    stdout="[GNUPG:] VALIDSIG X D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81\n",
                    stderr="",
                )
            return mocker.MagicMock(returncode=0, stdout="", stderr="")

        mocker.patch("orcest.fleet.cli.subprocess.run", side_effect=fake_run)
        mocker.patch("orcest.fleet.cli.tempfile.mkdtemp", return_value=str(tmp_path))

        with pytest.raises(RuntimeError, match="noble-server-cloudimg-amd64.img"):
            _resolve_image_checksum(
                "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
                cfg,
                Console(),
            )


# ── rebake / destroy-template / gc-templates tests ──────────


def _patch_template_bake(mocker):
    """Stub out the slow steps inside ``_create_template_at_vmid``.

    Used by rebake/create-template tests to avoid needing a real Proxmox
    or a full successful bake path.
    """
    mocker.patch("orcest.fleet.cli._create_vm_from_cloud_image")
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._get_vm_ip", return_value="10.20.0.50")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch("orcest.fleet.cli._install_source_on_worker_template", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )


def test_rebake_allocates_next_free_vmid_and_swaps_pointer(runner, cfg_path, mocker):
    """rebake picks the lowest free VMID in the range and SETs the Redis pointer."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest", ssh_key="ssh-ed25519 AAA"),
        pool=PoolConfig(
            template_vmid_range=[9000, 9009],
            template_vm_id=9000,  # current active
            storage="ssd-pool",
            snippet_storage="local",
        ),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    # 9000 is the current active template; rebake should skip it and pick 9001.
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
    ]
    _patch_template_bake(mocker)
    mock_set = mocker.patch("orcest.fleet.orchestrator.set_current_template_vmid")

    result = runner.invoke(fleet, ["rebake", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert "Rebake complete" in result.output
    mock_px.convert_to_template.assert_called_once_with(9001)
    mock_set.assert_called_once_with("orcest@10.20.0.1", 9001)


def test_rebake_no_range_configured_fails(runner, cfg_path, mocker):
    """rebake refuses without pool.template_vmid_range."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000),  # legacy single-VMID, no range
    )
    _save(cfg, cfg_path)
    _mock_proxmox_client(mocker)

    result = runner.invoke(fleet, ["rebake", "--config", cfg_path])

    assert result.exit_code != 0
    assert "template_vmid_range" in result.output


def test_rebake_range_exhausted_fails(runner, cfg_path, mocker):
    """rebake fails if every VMID in the range is already in use."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9001], template_vm_id=9000),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "template": True},
        {"vmid": 9001, "template": True},
    ]

    result = runner.invoke(fleet, ["rebake", "--config", cfg_path])

    assert result.exit_code != 0
    assert "exhausted" in result.output


def test_rebake_bake_failure_does_not_swap_pointer(runner, cfg_path, mocker):
    """If bake fails, rebake never SETs the Redis pointer (active stays unchanged)."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(
            template_vmid_range=[9000, 9009],
            template_vm_id=9000,
            storage="ssd-pool",
            snippet_storage="local",
        ),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [{"vmid": 9000, "template": True}]
    mocker.patch(
        "orcest.fleet.cli._create_vm_from_cloud_image",
        side_effect=RuntimeError("download failed"),
    )
    mock_set = mocker.patch("orcest.fleet.orchestrator.set_current_template_vmid")

    result = runner.invoke(fleet, ["rebake", "--config", cfg_path])

    assert result.exit_code != 0
    mock_set.assert_not_called()
    # Best-effort cleanup of the half-built VM
    mock_px.destroy_vm.assert_called_once_with(9001)


def test_rebake_pointer_swap_failure_prints_authenticated_redis_cli(
    runner,
    cfg_path,
    mocker,
):
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(
            template_vmid_range=[9000, 9009],
            template_vm_id=9000,
            storage="ssd-pool",
            snippet_storage="local",
        ),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [{"vmid": 9000, "template": True}]
    _patch_template_bake(mocker)
    mocker.patch(
        "orcest.fleet.orchestrator.set_current_template_vmid",
        side_effect=RuntimeError("redis auth failed"),
    )

    result = runner.invoke(fleet, ["rebake", "--config", cfg_path])

    assert result.exit_code != 0
    assert "pointer swap failed" in result.output
    normalized_output = " ".join(result.output.split())
    assert "docker exec orcest-redis-redis-1" in normalized_output
    assert 'redis-cli -a "$ORCEST_REDIS_PASSWORD"' in normalized_output
    assert "--no-auth-warning" in normalized_output


def test_destroy_template_refuses_active_pointer(runner, cfg_path, mocker):
    """destroy-template refuses to destroy the currently-active template."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009], template_vm_id=9000),
    )
    _save(cfg, cfg_path)

    _mock_proxmox_client(mocker)
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        return_value=9001,
    )

    result = runner.invoke(fleet, ["destroy-template", "9001", "--yes", "--config", cfg_path])

    assert result.exit_code != 0
    assert "currently-active template" in result.output


def test_destroy_template_refuses_with_live_clones(runner, cfg_path, mocker):
    """destroy-template refuses if any worker VMs (linked clones) still exist."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009], template_vm_id=9001),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
        {"vmid": 9001, "name": "orcest-worker-template", "template": True},
        {"vmid": 300, "name": "orcest-worker-300", "template": False},
    ]
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        return_value=9001,
    )

    result = runner.invoke(fleet, ["destroy-template", "9000", "--yes", "--config", cfg_path])

    assert result.exit_code != 0
    assert "linked clone" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_destroy_template_succeeds_when_safe(runner, cfg_path, mocker):
    """destroy-template destroys the template when no clones reference it."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009], template_vm_id=9001),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
        {"vmid": 9001, "name": "orcest-worker-template", "template": True},
    ]
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        return_value=9001,
    )

    result = runner.invoke(fleet, ["destroy-template", "9000", "--yes", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    mock_px.destroy_vm.assert_called_once_with(9000)


def test_gc_templates_destroys_inactive_only(runner, cfg_path, mocker):
    """gc-templates destroys old templates in range but skips the active one."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009], template_vm_id=9001),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
        {"vmid": 9001, "name": "orcest-worker-template", "template": True},
        {"vmid": 9002, "name": "orcest-worker-template", "template": True},
    ]
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        return_value=9001,
    )

    result = runner.invoke(fleet, ["gc-templates", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    # 9000 and 9002 destroyed; 9001 (active) preserved
    destroyed = {call.args[0] for call in mock_px.destroy_vm.call_args_list}
    assert destroyed == {9000, 9002}


def test_gc_templates_skips_template_that_becomes_active_mid_run(runner, cfg_path, mocker):
    """M3-conc: a concurrent rebake can swap the active pointer to a VMID that
    gc classified as a candidate. gc must re-read the pointer before each
    destroy and never destroy the freshly-active template.
    """
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009], template_vm_id=9001),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
        {"vmid": 9001, "name": "orcest-worker-template", "template": True},
        {"vmid": 9002, "name": "orcest-worker-template", "template": True},
    ]
    # Initial read (used to compute candidates) -> 9001 active, so candidates
    # are [9000, 9002]. Then a rebake swaps the pointer to 9002 before gc
    # reaches it: the per-destroy re-reads return 9002.
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        side_effect=[9001, 9002, 9002],
    )

    result = runner.invoke(fleet, ["gc-templates", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    destroyed = {call.args[0] for call in mock_px.destroy_vm.call_args_list}
    # 9000 destroyed; 9002 spared because it became the active template mid-run.
    assert destroyed == {9000}
    assert 9002 not in destroyed


def test_gc_templates_dry_run_destroys_nothing(runner, cfg_path, mocker):
    """gc-templates --dry-run reports candidates but does not destroy them."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009], template_vm_id=9001),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "template": True},
        {"vmid": 9001, "template": True},
    ]
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        return_value=9001,
    )

    result = runner.invoke(fleet, ["gc-templates", "--dry-run", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert "would destroy VM 9000" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_gc_templates_aborts_when_active_undeterminable(runner, cfg_path, mocker):
    """gc-templates fails closed when the active template cannot be determined."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009]),  # no template_vm_id fallback
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        side_effect=RuntimeError("redis unreachable"),
    )

    result = runner.invoke(fleet, ["gc-templates", "--config", cfg_path])

    assert result.exit_code == 1
    assert "Could not determine the active template" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_destroy_template_aborts_when_active_undeterminable(runner, cfg_path, mocker):
    """destroy-template fails closed when the active template cannot be determined."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vmid_range=[9000, 9009]),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        side_effect=RuntimeError("redis unreachable"),
    )

    result = runner.invoke(fleet, ["destroy-template", "9001", "--yes", "--config", cfg_path])

    assert result.exit_code == 1
    assert "Could not determine the active template" in result.output
    mock_px.destroy_vm.assert_not_called()


# ── pool-status tests ───────────────────────────────────────


def test_pool_status_no_template(runner, cfg_path):
    """pool-status shows config and warns when no template is set."""
    cfg = FleetConfig()
    _save(cfg, cfg_path)

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "No template configured" in result.output
    assert "Target Size" in result.output


def test_pool_status_shows_config(runner, cfg_path):
    """pool-status displays all pool configuration fields."""
    cfg = FleetConfig(
        pool=PoolConfig(
            size=6,
            storage="fast-pool",
            worker_memory=32768,
            worker_cores=16,
            worker_disk_size=50,
            max_task_duration=7200,
        ),
    )
    _save(cfg, cfg_path)

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "6" in result.output
    assert "fast-pool" in result.output
    assert "32768" in result.output
    assert "16" in result.output
    assert "50" in result.output
    assert "7200" in result.output


def test_pool_status_with_template_and_vms(runner, cfg_path, mocker):
    """pool-status shows template status and lists worker VMs."""
    cfg = _proxmox_cfg(
        pool=PoolConfig(template_vm_id=200, size=3),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.get_vm_status.return_value = "stopped"
    # Mock listing VMs -- list_vms(name_prefix="orcest-worker-") already
    # filters by prefix, so only return matching VMs.
    mock_px.list_vms.return_value = [
        {
            "vmid": 201,
            "name": "orcest-worker-1",
            "status": "running",
            "cpus": 8,
            "maxmem": 16384 * 1024 * 1024,
        },
        {
            "vmid": 202,
            "name": "orcest-worker-2",
            "status": "stopped",
            "cpus": 8,
            "maxmem": 16384 * 1024 * 1024,
        },
    ]

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "orcest-worker-1" in result.output
    assert "orcest-worker-2" in result.output
    # Unrelated VM should not appear
    assert "unrelated-vm" not in result.output
    assert "1 running" in result.output
    assert "1 stopped" in result.output


def test_pool_status_no_proxmox_creds(runner, cfg_path):
    """pool-status warns when Proxmox creds are missing but template is set."""
    cfg = FleetConfig(
        pool=PoolConfig(template_vm_id=200),
    )
    _save(cfg, cfg_path)

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "Proxmox API credentials not configured" in result.output


def test_pool_status_no_worker_vms(runner, cfg_path, mocker):
    """pool-status shows message when no worker VMs exist."""
    cfg = _proxmox_cfg(
        pool=PoolConfig(template_vm_id=200),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.get_vm_status.return_value = "stopped"
    mock_px.list_vms.return_value = []

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "No worker VMs found" in result.output


def test_pool_status_uses_redis_pointer(runner, cfg_path, mocker):
    """pool-status reads the active template from Redis when orchestrator host is set."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(template_vm_id=200),
    )
    _save(cfg, cfg_path)

    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        return_value=9003,
    )
    mock_px = _mock_proxmox_client(mocker)
    mock_px.get_vm_status.return_value = "stopped"
    mock_px.list_vms.return_value = []

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "9003" in result.output
    assert "Redis pointer" in result.output
    # The Proxmox status check should use the Redis-derived VMID, not the config one.
    mock_px.get_vm_status.assert_called_with(9003)


def test_pool_status_falls_back_to_config_when_redis_unset(runner, cfg_path, mocker):
    """pool-status falls back to cfg.pool.template_vm_id when Redis pointer is unset."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(template_vm_id=200),
    )
    _save(cfg, cfg_path)

    mocker.patch(
        "orcest.fleet.orchestrator.get_current_template_vmid",
        return_value=None,
    )
    mock_px = _mock_proxmox_client(mocker)
    mock_px.get_vm_status.return_value = "stopped"
    mock_px.list_vms.return_value = []

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "200" in result.output
    mock_px.get_vm_status.assert_called_with(200)


# ── set-pool-size tests ─────────────────────────────────────


def test_set_pool_size(runner, cfg_path):
    """set-pool-size updates the pool size in config."""
    cfg = FleetConfig(pool=PoolConfig(size=4))
    _save(cfg, cfg_path)

    result = runner.invoke(fleet, ["set-pool-size", "8", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "4" in result.output  # old size
    assert "8" in result.output  # new size

    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["pool"]["size"] == 8


def test_set_pool_size_zero(runner, cfg_path):
    """set-pool-size allows zero (drain the pool)."""
    cfg = FleetConfig(pool=PoolConfig(size=4))
    _save(cfg, cfg_path)

    result = runner.invoke(fleet, ["set-pool-size", "0", "--config", cfg_path])
    assert result.exit_code == 0, result.output

    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["pool"]["size"] == 0


def test_set_pool_size_negative(runner, cfg_path):
    """set-pool-size rejects negative values."""
    _save(FleetConfig(), cfg_path)

    result = runner.invoke(fleet, ["set-pool-size", "--config", cfg_path, "--", "-1"])
    assert result.exit_code != 0
    assert "non-negative" in result.output


# ── fleet stop ──────────────────────────────────────────────


def test_stop_destroys_idle_vms(runner, cfg_path, mocker):
    """stop destroys idle worker VMs and cleans Redis."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        {"vmid": 301, "name": "orcest-worker-301", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300", "301"}, {}),
    )
    mock_clean = mocker.patch("orcest.fleet.orchestrator.clean_pool_redis")

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])
    assert result.exit_code == 0
    assert mock_px.destroy_vm.call_count == 2
    mock_clean.assert_called_once()


def test_stop_refuses_named_worker_outside_configured_vmid_range(runner, cfg_path, mocker):
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)
    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 500, "name": "orcest-worker-500", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"500"}, {}),
    )

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert "outside configured worker VMID range" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_stop_final_pending_check_closes_idle_claim_race(runner, cfg_path, mocker):
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)
    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {}),
    )
    pending = mocker.patch(
        "orcest.fleet.orchestrator.get_workers_with_pending_tasks",
        side_effect=[set(), set(), {"orcest-worker-300"}],
    )

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert pending.call_count == 3
    assert "restarted VM for task recovery" in result.output
    mock_px.start_vm.assert_called_once_with(300)
    mock_px.destroy_vm.assert_not_called()


def test_stop_fails_when_late_claim_worker_cannot_restart(runner, cfg_path, mocker):
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)
    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
    ]
    mock_px.start_vm.side_effect = RuntimeError("start rejected")
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {}),
    )
    mocker.patch(
        "orcest.fleet.orchestrator.get_workers_with_pending_tasks",
        side_effect=[set(), set(), {"orcest-worker-300"}],
    )

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code != 0
    assert "could not be restarted" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_stop_does_not_clean_pending_task_markers(runner, cfg_path, mocker):
    """stop must not wipe task pending markers for still-running or recoverable work."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {}),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pool_redis")
    mock_clean_pending = mocker.patch("orcest.fleet.orchestrator.clean_pending_tasks")

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    mock_clean_pending.assert_not_called()
    assert "Pending task markers left intact" in result.output


def test_stop_aborts_when_pool_manager_stop_fails(runner, cfg_path, mocker):
    """stop must not destroy workers while the pool manager may still mutate state."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch(
        "orcest.fleet.orchestrator.stop_pool_manager",
        side_effect=RuntimeError("compose down failed"),
    )
    mock_members = mocker.patch("orcest.fleet.orchestrator.get_pool_redis_members")

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code != 0
    assert "pool manager may still be running" in result.output
    mock_members.assert_not_called()
    mock_px.destroy_vm.assert_not_called()


def test_stop_does_not_destroy_worker_templates(runner, cfg_path, mocker):
    """stop must not destroy blue/green worker templates as orphan workers."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(
            template_vm_id=9000,
            template_vmid_range=[9000, 9009],
            vm_id_start=300,
            vm_id_end=399,
        ),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
        {"vmid": 9001, "name": "orcest-worker-template"},
        {"vmid": 9002, "name": "orcest-worker-template-old"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {}),
    )
    mocker.patch("orcest.fleet.orchestrator.get_current_template_vmid", return_value=9001)
    mocker.patch("orcest.fleet.orchestrator.clean_pool_redis")

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])
    assert result.exit_code == 0
    mock_px.destroy_vm.assert_called_once_with(300)


def test_stop_leaves_active_vms(runner, cfg_path, mocker):
    """stop leaves active VMs running unless --drain-active."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        {"vmid": 301, "name": "orcest-worker-301", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {"301": "1000.0"}),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pool_redis")

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])
    assert result.exit_code == 0
    # Only idle VM 300 destroyed, active VM 301 left
    mock_px.destroy_vm.assert_called_once_with(300)
    assert "Leaving active VM 301" in result.output


def test_stop_refuses_to_destroy_when_pool_state_unavailable(runner, cfg_path, mocker):
    """stop must not classify active workers as orphans when Redis state reads fail."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        side_effect=RuntimeError("redis unavailable"),
    )

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code != 0
    assert "Refusing to destroy worker VMs" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_stop_refuses_when_pending_consumer_state_unavailable(runner, cfg_path, mocker):
    """stop must not destroy idle/orphan VMs if PEL state cannot be inspected."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {}),
    )
    mocker.patch(
        "orcest.fleet.orchestrator.get_workers_with_pending_tasks",
        side_effect=RuntimeError("xinfo failed"),
    )

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code != 0
    assert "pending-consumer state" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_stop_skips_idle_vm_with_pending_consumer(runner, cfg_path, mocker):
    """An idle-set VM that now has PEL entries is not destroyed during normal stop."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {}),
    )
    mocker.patch(
        "orcest.fleet.orchestrator.get_workers_with_pending_tasks",
        return_value={"orcest-worker-300"},
    )

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert "with pending task" in result.output
    mock_px.destroy_vm.assert_not_called()


def test_stop_drain_active_destroys_all(runner, cfg_path, mocker):
    """stop --drain-active also destroys active VMs."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        {"vmid": 301, "name": "orcest-worker-301", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {"301": "1000.0"}),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pool_redis")

    result = runner.invoke(fleet, ["stop", "--drain-active", "--yes", "--config", cfg_path])
    assert result.exit_code == 0
    assert mock_px.destroy_vm.call_count == 2


@pytest.mark.parametrize(
    ("prompt_input", "should_proceed", "expected_destroy_count"),
    [
        pytest.param("n\n", False, 0, id="abort"),
        pytest.param("y\n", True, 2, id="confirm"),
    ],
)
def test_stop_drain_active_prompt(
    runner,
    cfg_path,
    mocker,
    prompt_input,
    should_proceed,
    expected_destroy_count,
):
    """stop --drain-active follows the user's confirmation prompt answer."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=300, vm_id_end=399),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        {"vmid": 301, "name": "orcest-worker-301", "status": "running"},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=({"300"}, {"301": "1000.0"}),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pool_redis")

    result = runner.invoke(
        fleet,
        ["stop", "--drain-active", "--config", cfg_path],
        input=prompt_input,
    )
    if should_proceed:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0
    assert mock_px.destroy_vm.call_count == expected_destroy_count


def test_stop_requires_orchestrator_host(runner, cfg_path):
    """stop fails if orchestrator host not set."""
    _save(FleetConfig(), cfg_path)

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])
    assert result.exit_code != 0
    assert "Orchestrator host not set" in result.output


def test_stop_no_vms(runner, cfg_path, mocker):
    """stop succeeds cleanly when no worker VMs exist."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
    )
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = []
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=(set(), {}),
    )

    result = runner.invoke(fleet, ["stop", "--config", cfg_path])
    assert result.exit_code == 0
    assert "Destroyed 0 VMs" in result.output


# ── fleet start ─────────────────────────────────────────────


def test_start_uploads_config_and_starts(runner, cfg_path, mocker):
    """start uploads config and starts pool manager."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, size=4),
    )
    _save(cfg, cfg_path)

    mock_upload = mocker.patch("orcest.fleet.orchestrator.upload_fleet_config")
    mock_ensure = mocker.patch("orcest.fleet.orchestrator.ensure_pool_manager")
    mock_pw = mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")

    result = runner.invoke(fleet, ["start", "--config", cfg_path])
    assert result.exit_code == 0
    mock_upload.assert_called_once()
    mock_ensure.assert_called_once()
    # C1: the pool stack --env-file's the minted password file; ensure it exists.
    mock_pw.assert_called_once()
    assert "target size: 4" in result.output


def test_start_allows_template_range_without_legacy_template(runner, cfg_path, mocker):
    """start accepts range-mode pools where Redis pointer selects the active template."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=0, template_vmid_range=[9000, 9009], size=4),
    )
    _save(cfg, cfg_path)

    mock_upload = mocker.patch("orcest.fleet.orchestrator.upload_fleet_config")
    mock_ensure = mocker.patch("orcest.fleet.orchestrator.ensure_pool_manager")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")

    result = runner.invoke(fleet, ["start", "--config", cfg_path])

    assert result.exit_code == 0
    mock_upload.assert_called_once()
    mock_ensure.assert_called_once()


def test_start_refuses_uncoordinated_worker_backend_change(runner, cfg_path, mocker):
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, worker_backend="clauder"),
    )
    _save(cfg, cfg_path)
    mocker.patch(
        "orcest.fleet.orchestrator.get_deployed_pool_backend",
        return_value="claude",
    )
    upload = mocker.patch("orcest.fleet.orchestrator.upload_fleet_config")
    ensure = mocker.patch("orcest.fleet.orchestrator.ensure_pool_manager")

    result = runner.invoke(fleet, ["start", "--config", cfg_path])

    assert result.exit_code != 0
    assert "uncoordinated worker backend change" in result.output
    upload.assert_not_called()
    ensure.assert_not_called()


def test_start_requires_orchestrator_host(runner, cfg_path):
    """start fails if orchestrator host not set."""
    _save(FleetConfig(), cfg_path)

    result = runner.invoke(fleet, ["start", "--config", cfg_path])
    assert result.exit_code != 0
    assert "Orchestrator host not set" in result.output


def test_start_requires_template(runner, cfg_path):
    """start fails if no template VM configured."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=0),
    )
    _save(cfg, cfg_path)

    result = runner.invoke(fleet, ["start", "--config", cfg_path])
    assert result.exit_code != 0
    assert "template" in result.output.lower()


def test_start_rejects_localhost_endpoint(runner, cfg_path):
    """start fails if Proxmox endpoint is localhost."""
    cfg = FleetConfig(
        proxmox=ProxmoxConfig(
            endpoint="https://127.0.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000),
    )
    _save(cfg, cfg_path)

    result = runner.invoke(fleet, ["start", "--config", cfg_path])
    assert result.exit_code != 0
    assert "localhost" in result.output.lower()


# ── deploy tests ───────────────────────────────────────────


def test_deploy_runs_full_sequence(runner, cfg_path, mocker):
    """deploy runs stop, update, and start in order without self-upgrading."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(template_vm_id=9000),
    )
    _save(cfg, cfg_path)

    # Mock all external calls
    mock_upgrade = mocker.patch("orcest.fleet.cli._upgrade_cli")
    _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=(set(), {}),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pending_tasks", return_value=0)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mock_upload_cfg = mocker.patch(
        "orcest.fleet.orchestrator.upload_fleet_config",
    )
    mock_ensure_pool = mocker.patch(
        "orcest.fleet.orchestrator.ensure_pool_manager",
    )

    result = runner.invoke(fleet, ["deploy", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "Deploy complete" in result.output
    mock_upgrade.assert_not_called()

    # Start step should have uploaded config and started pool manager
    mock_upload_cfg.assert_called()
    mock_ensure_pool.assert_called()


def test_deploy_defers_pool_manager_until_after_project_restart(runner, cfg_path, mocker):
    """deploy must not start workers until regenerated project stacks are running."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, worker_backend="clauder"),
        orgs={"Org": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
        projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
    )
    _save(cfg, cfg_path)

    order: list[str] = []
    mocker.patch("orcest.fleet.cli._upgrade_cli")
    _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=(set(), {}),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pending_tasks", return_value=0)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.generate_env_file", return_value="")
    mocker.patch("orcest.fleet.orchestrator.generate_orchestrator_config", return_value="")
    mocker.patch(
        "orcest.fleet.orchestrator.write_project_files",
        side_effect=lambda *_args, **_kwargs: order.append("write_project_files"),
    )
    mocker.patch(
        "orcest.fleet.orchestrator.restart_stack",
        side_effect=lambda *_args, **_kwargs: order.append("restart_stack"),
    )
    mocker.patch(
        "orcest.fleet.orchestrator.upload_fleet_config",
        side_effect=lambda *_args, **_kwargs: order.append("upload_fleet_config"),
    )
    mocker.patch(
        "orcest.fleet.orchestrator.ensure_pool_manager",
        side_effect=lambda *_args, **_kwargs: order.append("ensure_pool_manager"),
    )

    result = runner.invoke(fleet, ["deploy", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    assert order == [
        "write_project_files",
        "restart_stack",
        "upload_fleet_config",
        "ensure_pool_manager",
    ]


def test_deploy_rebuild_template_uses_rebake_pointer_swap(runner, cfg_path, mocker):
    """deploy --rebuild-template must use rebake, not legacy create-template."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(
            host="10.20.0.23",
            user="orcest",
            ssh_key="ssh-ed25519 AAA",
        ),
        pool=PoolConfig(
            template_vm_id=9000,
            template_vmid_range=[9000, 9009],
            worker_backend="clauder",
            vm_id_start=300,
            vm_id_end=399,
            storage="ssd-pool",
            snippet_storage="local",
        ),
    )
    _save(cfg, cfg_path)

    mocker.patch("orcest.fleet.cli._upgrade_cli")
    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
    ]
    _patch_template_bake(mocker)
    call_order: list[str] = []
    mock_set = mocker.patch(
        "orcest.fleet.orchestrator.set_current_template_vmid",
        side_effect=lambda *_args, **_kwargs: call_order.append("set_current_template_vmid"),
    )
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=(set(), {}),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pending_tasks", return_value=0)
    mocker.patch(
        "orcest.fleet.orchestrator.get_deployed_pool_backend",
        return_value="claude",
    )
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.upload_fleet_config")
    mocker.patch(
        "orcest.fleet.orchestrator.ensure_pool_manager",
        side_effect=lambda *_args, **_kwargs: call_order.append("ensure_pool_manager"),
    )

    result = runner.invoke(
        fleet,
        [
            "deploy",
            "--rebuild-template",
            "--drain-active",
            "--config",
            cfg_path,
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Rebaking template" in result.output
    mock_px.convert_to_template.assert_called_once_with(9001)
    mock_set.assert_called_once_with("orcest@10.20.0.23", 9001)
    assert call_order == ["set_current_template_vmid", "ensure_pool_manager"]


def test_deploy_rebuild_template_requires_range_before_side_effects(runner, cfg_path, mocker):
    """deploy --rebuild-template must fail before stopping legacy single-template fleets."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(template_vm_id=9000),
    )
    _save(cfg, cfg_path)

    mock_upgrade = mocker.patch("orcest.fleet.cli._upgrade_cli")
    mock_stop_pool = mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")

    result = runner.invoke(fleet, ["deploy", "--rebuild-template", "--config", cfg_path])

    assert result.exit_code != 0
    assert "template_vmid_range" in result.output
    mock_upgrade.assert_not_called()
    mock_stop_pool.assert_not_called()


def test_deploy_drain_active_forwards_to_stop(runner, cfg_path, mocker):
    """deploy --drain-active destroys active workers through the stop step."""
    cfg = _proxmox_cfg(
        proxmox=ProxmoxConfig(
            endpoint="https://10.20.0.1:8006",
            api_token_id="root@pam!orcest",
            api_token_secret="secret",
        ),
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        pool=PoolConfig(template_vm_id=9000, vm_id_start=9001, vm_id_end=9999),
    )
    _save(cfg, cfg_path)

    mocker.patch("orcest.fleet.cli._upgrade_cli")
    mock_px = _mock_proxmox_client(mocker)
    mock_px.list_vms.return_value = [
        {"vmid": 9001, "name": "orcest-worker-9001", "template": False},
    ]
    mocker.patch("orcest.fleet.orchestrator.stop_pool_manager")
    mocker.patch(
        "orcest.fleet.orchestrator.get_pool_redis_members",
        return_value=(set(), {"9001": "orcest-worker-9001"}),
    )
    mocker.patch(
        "orcest.fleet.orchestrator.get_workers_with_pending_tasks",
        return_value=set(),
    )
    mocker.patch("orcest.fleet.orchestrator.clean_pool_redis")
    mocker.patch("orcest.fleet.orchestrator.clean_pending_tasks", return_value=0)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_password", return_value="pw")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mocker.patch("orcest.fleet.orchestrator.upload_fleet_config")
    mocker.patch("orcest.fleet.orchestrator.ensure_pool_manager")

    result = runner.invoke(fleet, ["deploy", "--drain-active", "--config", cfg_path])

    assert result.exit_code == 0, result.output
    mock_px.destroy_vm.assert_called_once_with(9001)


# ── _prompt_storage non-interactive tests (bug 8) ───────────


class TestPromptStorageNonInteractive:
    """Bug 8: when default matches an available storage, skip the prompt.

    ``orcest-rebake-template.timer`` invokes the CLI without a TTY; an
    interactive ``click.prompt`` would block forever, so ``_prompt_storage``
    must return the configured value directly when it is valid.
    """

    def _px(self, mocker, storages):
        from orcest.fleet.cli import _prompt_storage

        px = mocker.MagicMock()
        px.list_storage.return_value = storages
        return _prompt_storage, px

    def test_returns_default_without_prompt_when_match(self, mocker):
        from rich.console import Console

        mock_prompt = mocker.patch("orcest.fleet.cli.click.prompt")
        _prompt_storage, px = self._px(
            mocker,
            [
                {"storage": "ssd-pool", "type": "lvmthin", "avail": 1e12},
                {"storage": "hdd-pool", "type": "lvmthin", "avail": 1e12},
            ],
        )
        result = _prompt_storage(px, "images", "template VM disk", Console(), default="ssd-pool")
        assert result == "ssd-pool"
        mock_prompt.assert_not_called()

    def test_prompts_when_default_not_in_storages(self, mocker):
        """If the configured default is not available, fall back to the prompt."""
        from rich.console import Console

        mock_prompt = mocker.patch("orcest.fleet.cli.click.prompt", return_value=2)
        _prompt_storage, px = self._px(
            mocker,
            [
                {"storage": "ssd-pool", "type": "lvmthin", "avail": 1e12},
                {"storage": "hdd-pool", "type": "lvmthin", "avail": 1e12},
            ],
        )
        result = _prompt_storage(px, "images", "template VM disk", Console(), default="missing")
        assert result == "hdd-pool"
        mock_prompt.assert_called_once()

    def test_prompts_when_default_is_none(self, mocker):
        from rich.console import Console

        mock_prompt = mocker.patch("orcest.fleet.cli.click.prompt", return_value=1)
        _prompt_storage, px = self._px(
            mocker,
            [
                {"storage": "ssd-pool", "type": "lvmthin", "avail": 1e12},
                {"storage": "hdd-pool", "type": "lvmthin", "avail": 1e12},
            ],
        )
        result = _prompt_storage(px, "images", "template VM disk", Console())
        assert result == "ssd-pool"
        mock_prompt.assert_called_once()

    def test_single_option_short_circuits(self, mocker):
        """Single-option path ignores ``default`` and returns the only choice."""
        from rich.console import Console

        mock_prompt = mocker.patch("orcest.fleet.cli.click.prompt")
        _prompt_storage, px = self._px(
            mocker, [{"storage": "only-one", "type": "lvmthin", "avail": 1e12}]
        )
        assert _prompt_storage(px, "images", "x", Console(), default="missing") == "only-one"
        mock_prompt.assert_not_called()


# ── _wait_for_cloud_init polling (bug 5) ────────────────────


class TestWaitForCloudInit:
    """Bug 5: poll for ``/var/lib/cloud/instance/boot-finished`` to confirm
    cloud-final has actually exited; ``cloud-init status --wait`` returns
    early on recoverable errors while installs are still running."""

    def test_returns_true_when_boot_finished_appears(self, mocker):
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        completed_test = mocker.MagicMock(returncode=0)
        completed_status = mocker.MagicMock(returncode=0, stdout="status: done", stderr="")
        run = mocker.patch(
            "orcest.fleet.cli.subprocess.run",
            side_effect=[completed_test, completed_status],
        )
        mocker.patch("orcest.fleet.cli.time.sleep")
        result = _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=60)
        assert result is True
        # The first invocation must check the boot-finished marker file.
        first_cmd = run.call_args_list[0][0][0]
        assert "test -f /var/lib/cloud/instance/boot-finished" in " ".join(first_cmd)

    def test_polls_until_boot_finished(self, mocker):
        """Bug 5: keep polling while boot-finished is missing."""
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        # First two checks fail (file missing); third succeeds; then status check.
        results = [
            mocker.MagicMock(returncode=1),
            mocker.MagicMock(returncode=1),
            mocker.MagicMock(returncode=0),
            mocker.MagicMock(returncode=0, stdout="status: done", stderr=""),
        ]
        run = mocker.patch("orcest.fleet.cli.subprocess.run", side_effect=results)
        mocker.patch("orcest.fleet.cli.time.sleep")
        assert _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=60) is True
        # Should have polled at least three times.
        assert run.call_count >= 3

    def test_timeout_returns_false(self, mocker):
        """Bug 5: timeout returns False, never proceeds with a half-done bake."""
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        # Always returns "file not found".
        mocker.patch(
            "orcest.fleet.cli.subprocess.run",
            return_value=mocker.MagicMock(returncode=1),
        )
        mocker.patch("orcest.fleet.cli.time.sleep")
        # Make monotonic jump past the deadline immediately.
        base = [0.0]

        def fake_monotonic():
            base[0] += 30.0
            return base[0]

        mocker.patch("orcest.fleet.cli.time.monotonic", new=fake_monotonic)
        assert _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=10) is False

    def test_does_not_complete_on_status_error_alone(self, mocker):
        """Bug 5 regression: ``status: error`` without boot-finished must NOT
        be treated as completion. Old code returned True on any non-zero exit
        from ``cloud-init status --wait``; the verification agent saw this
        proceed to ``cloud-init clean`` while installs were still running.
        """
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        # boot-finished never shows up; we should time out, not complete.
        mocker.patch(
            "orcest.fleet.cli.subprocess.run",
            return_value=mocker.MagicMock(returncode=1),
        )
        mocker.patch("orcest.fleet.cli.time.sleep")
        base = [0.0]

        def fake_monotonic():
            base[0] += 100.0
            return base[0]

        mocker.patch("orcest.fleet.cli.time.monotonic", new=fake_monotonic)
        assert _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=10) is False

    def test_hard_fails_when_status_not_done(self, mocker):
        """H1-infra: boot-finished present but `cloud-init status` != done must
        HARD-FAIL (return False) so the caller aborts before the pointer swap.

        Old code printed a yellow warning and returned True, letting a
        half-baked template (e.g. a failed provider-CLI install in runcmd,
        which has no cross-entry `set -e`) flip
        orcest:pool:current_template_vmid.
        """
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        # boot-finished marker exists (returncode 0), but status reports error.
        boot_finished = mocker.MagicMock(returncode=0)
        status_error = mocker.MagicMock(returncode=1, stdout="status: error", stderr="")
        mocker.patch(
            "orcest.fleet.cli.subprocess.run",
            side_effect=[boot_finished, status_error],
        )
        mocker.patch("orcest.fleet.cli.time.sleep")
        assert _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=60) is False

    def test_transient_status_read_is_retried_not_aborted(self, mocker):
        """H1-infra (repair 2): a single flaky SSH read of `cloud-init status`
        AFTER boot-finished must NOT abort a good bake. Round 1 returned False
        when the status read timed out / was unreadable. The read must be
        retried a few times; if a later attempt reports `status: done`, the
        bake succeeds.
        """
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        boot_finished = mocker.MagicMock(returncode=0)
        # First status read times out (transient SSH blip), second succeeds.
        status_done = mocker.MagicMock(returncode=0, stdout="status: done", stderr="")
        mocker.patch(
            "orcest.fleet.cli.subprocess.run",
            side_effect=[
                boot_finished,
                subprocess.TimeoutExpired(cmd="ssh", timeout=15),
                status_done,
            ],
        )
        mocker.patch("orcest.fleet.cli.time.sleep")
        assert _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=60) is True

    def test_definitive_error_status_hard_fails_without_retry_loop(self, mocker):
        """H1-infra (repair 2): a DEFINITIVE non-done status (error/degraded)
        must hard-fail -- the retry budget is only for transient read failures,
        not for a genuine bad status. One `status: error` read => return False.
        """
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        boot_finished = mocker.MagicMock(returncode=0)
        status_error = mocker.MagicMock(returncode=1, stdout="status: error", stderr="")
        run = mocker.patch(
            "orcest.fleet.cli.subprocess.run",
            side_effect=[boot_finished, status_error],
        )
        mocker.patch("orcest.fleet.cli.time.sleep")
        assert _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=60) is False
        # boot-finished check + exactly one status read: a definitive error is
        # not retried (only transient read failures are).
        assert run.call_count == 2

    def test_all_status_reads_transient_eventually_fails(self, mocker):
        """H1-infra (repair 2): if EVERY status read is transiently unreadable
        (never a definitive status, never `done`), the bake must still fail
        closed after exhausting the retry budget -- a half-baked template must
        never be accepted just because its status was never readable.
        """
        from rich.console import Console

        from orcest.fleet.cli import _wait_for_cloud_init

        boot_finished = mocker.MagicMock(returncode=0)
        mocker.patch(
            "orcest.fleet.cli.subprocess.run",
            side_effect=[boot_finished] + [subprocess.TimeoutExpired(cmd="ssh", timeout=15)] * 20,
        )
        mocker.patch("orcest.fleet.cli.time.sleep")
        assert _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=60) is False
