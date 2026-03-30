"""Tests for orcest.fleet.cli."""

import time

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
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
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
        orgs={"ThayneStudio": OrgEntry(github_token="ghp_fake", claude_oauth_tokens=["sk-fake"])},
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
    import subprocess

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
        pool=PoolConfig(template_vm_id=9000),
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


def test_stop_leaves_active_vms(runner, cfg_path, mocker):
    """stop leaves active VMs running unless --drain-active."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000),
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


def test_stop_drain_active_destroys_all(runner, cfg_path, mocker):
    """stop --drain-active also destroys active VMs."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(host="10.20.0.1", user="orcest"),
        pool=PoolConfig(template_vm_id=9000),
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

    result = runner.invoke(fleet, ["stop", "--drain-active", "--config", cfg_path])
    assert result.exit_code == 0
    assert mock_px.destroy_vm.call_count == 2


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

    result = runner.invoke(fleet, ["start", "--config", cfg_path])
    assert result.exit_code == 0
    mock_upload.assert_called_once()
    mock_ensure.assert_called_once()
    assert "target size: 4" in result.output


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
    """deploy runs upgrade, stop, update, and start in order."""
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

    # Start step should have uploaded config and started pool manager
    mock_upload_cfg.assert_called()
    mock_ensure_pool.assert_called()
