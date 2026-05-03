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
    mocker.patch(
        "orcest.fleet.cli._ssh_run",
        return_value=mocker.MagicMock(returncode=0),
    )


def test_rebake_allocates_next_free_vmid_and_swaps_pointer(runner, cfg_path, mocker):
    """rebake picks the lowest free VMID in the range and SETs the Redis pointer."""
    cfg = _proxmox_cfg(
        orchestrator=OrchestratorConfig(
            host="10.20.0.1", user="orcest", ssh_key="ssh-ed25519 AAA"
        ),
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

    result = runner.invoke(
        fleet, ["destroy-template", "9001", "--yes", "--config", cfg_path]
    )

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

    result = runner.invoke(
        fleet, ["destroy-template", "9000", "--yes", "--config", cfg_path]
    )

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

    result = runner.invoke(
        fleet, ["destroy-template", "9000", "--yes", "--config", cfg_path]
    )

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

    result = runner.invoke(fleet, ["stop", "--drain-active", "--yes", "--config", cfg_path])
    assert result.exit_code == 0
    assert mock_px.destroy_vm.call_count == 2


def test_stop_drain_active_prompt_abort(runner, cfg_path, mocker):
    """stop --drain-active aborts when user answers 'n' at the confirmation prompt."""
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

    result = runner.invoke(fleet, ["stop", "--drain-active", "--config", cfg_path], input="n\n")
    assert result.exit_code != 0
    mock_px.destroy_vm.assert_not_called()


def test_stop_drain_active_prompt_confirm(runner, cfg_path, mocker):
    """stop --drain-active proceeds when user answers 'y' at the confirmation prompt."""
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

    result = runner.invoke(fleet, ["stop", "--drain-active", "--config", cfg_path], input="y\n")
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
        result = _prompt_storage(
            px, "images", "template VM disk", Console(), default="ssd-pool"
        )
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
        result = _prompt_storage(
            px, "images", "template VM disk", Console(), default="missing"
        )
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
        assert (
            _prompt_storage(px, "images", "x", Console(), default="missing") == "only-one"
        )
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
        assert (
            _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=60) is True
        )
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
        assert (
            _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=10) is False
        )

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
        assert (
            _wait_for_cloud_init("10.0.0.1", "orcest", Console(), timeout=10) is False
        )
