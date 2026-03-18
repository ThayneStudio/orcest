"""Tests for orcest.fleet.cli."""

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
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_token="sk-fake")},
    )
    _save(cfg, cfg_path)
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
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")

    result = runner.invoke(fleet, ["create-orchestrator", "--config", cfg_path])
    assert result.exit_code == 0, result.output
    assert "10.20.0.99" in result.output

    # Verify config was updated with orchestrator host
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["orchestrator"]["host"] == "10.20.0.99"


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

    result = runner.invoke(fleet, ["create-orchestrator", "--config", cfg_path])
    assert result.exit_code != 0

    # Config should still be saved with the IP
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["orchestrator"]["host"] == "10.20.0.99"


def test_update_rebuilds_and_restarts(runner, cfg_path, mocker):
    """fleet update uploads source, rebuilds image, and restarts stacks."""
    cfg = FleetConfig(
        orchestrator=OrchestratorConfig(host="10.20.0.23"),
        projects=[
            ProjectEntry(name="alpha", repo="Org/alpha"),
            ProjectEntry(name="beta", repo="Org/beta"),
        ],
    )
    _save(cfg, cfg_path)
    mocker.patch("orcest.fleet.orchestrator.upload_source")
    mocker.patch("orcest.fleet.orchestrator.build_image")
    mock_ensure_redis = mocker.patch("orcest.fleet.orchestrator.ensure_redis_stack")
    mock_restart = mocker.patch("orcest.fleet.orchestrator.restart_stack")

    result = runner.invoke(fleet, ["update", "--config", cfg_path])
    assert result.exit_code == 0, result.output

    # Should update shared Redis stack and restart both project stacks
    mock_ensure_redis.assert_called_once()
    assert mock_restart.call_count == 2


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
    mocker.patch("orcest.fleet.cli._create_proxmox_client", return_value=mock_px)
    return mock_px


def test_create_template_success(runner, cfg_path, mocker):
    """create-template clones, boots, provisions, and converts to template."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code == 0, result.output
    assert "Worker template created" in result.output

    # Verify Proxmox operations happened in order
    mock_px.clone_vm.assert_called_once_with(
        template_id=9000,
        new_id=200,
        name="orcest-worker-template",
        storage="ssd-pool",
        linked=False,
    )
    mock_px.start_vm.assert_called_once_with(200)
    mock_px.get_vm_ip.assert_called_once_with(200, timeout=300)
    mock_px.stop_vm.assert_called_once_with(200)
    mock_px.convert_to_template.assert_called_once_with(200)

    # Verify config was updated with template_vm_id
    with open(cfg_path) as f:
        data = yaml.safe_load(f)
    assert data["pool"]["template_vm_id"] == 200


def test_create_template_auto_assigns_vmid(runner, cfg_path, mocker):
    """create-template gets next free VM ID when --vm-id is not provided."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.next_free_vmid.return_value = 300
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )

    result = runner.invoke(
        fleet,
        ["create-template", "--config", cfg_path],
    )
    assert result.exit_code == 0, result.output
    assert "Auto-assigned VM ID: 300" in result.output

    mock_px.next_free_vmid.assert_called_once()
    mock_px.clone_vm.assert_called_once()
    # Verify the auto-assigned ID was used
    assert mock_px.clone_vm.call_args.kwargs["new_id"] == 300


def test_create_template_custom_base_vm(runner, cfg_path, mocker):
    """create-template respects --base-vm-id option."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )

    result = runner.invoke(
        fleet,
        ["create-template", "--base-vm-id", "5000", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code == 0, result.output
    assert mock_px.clone_vm.call_args.kwargs["template_id"] == 5000


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


def test_create_template_clone_failure(runner, cfg_path, mocker):
    """create-template exits on clone failure."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.clone_vm.side_effect = RuntimeError("clone failed")

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "clone failed" in result.output


def test_create_template_cloud_init_failure_cleans_up(runner, cfg_path, mocker):
    """create-template destroys the cloned VM if cloud-init config fails."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
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
    """create-template aborts if VM IP times out."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    mock_px = _mock_proxmox_client(mocker)
    mock_px.get_vm_ip.return_value = None
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "timed out" in result.output


def test_create_template_ssh_timeout(runner, cfg_path, mocker):
    """create-template aborts if SSH times out."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=False)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "SSH not available" in result.output


def test_create_template_cloud_init_timeout(runner, cfg_path, mocker):
    """create-template aborts if cloud-init times out."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=False)

    result = runner.invoke(
        fleet,
        ["create-template", "--vm-id", "200", "--config", cfg_path],
    )
    assert result.exit_code != 0
    assert "Cloud-init timed out" in result.output


def test_create_template_disable_cloud_init_failure(runner, cfg_path, mocker):
    """create-template aborts if disabling cloud-init fails."""
    cfg = _proxmox_cfg()
    _save(cfg, cfg_path)

    _mock_proxmox_client(mocker)
    mocker.patch("orcest.fleet.cli._set_vm_cloud_init")
    mocker.patch("orcest.fleet.cli._wait_for_ssh", return_value=True)
    mocker.patch("orcest.fleet.cli._wait_for_cloud_init", return_value=True)
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
    # Mock listing VMs
    mock_px._api.nodes.return_value.qemu.get.return_value = [
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
        {
            "vmid": 100,
            "name": "unrelated-vm",
            "status": "running",
            "cpus": 4,
            "maxmem": 4096 * 1024 * 1024,
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
    mock_px._api.nodes.return_value.qemu.get.return_value = []

    result = runner.invoke(fleet, ["pool-status", "--config", cfg_path])
    assert result.exit_code == 0
    assert "No worker VMs found" in result.output


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
