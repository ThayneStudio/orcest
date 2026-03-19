"""Tests for orcest.fleet.cloud_init."""

import pytest
import yaml

from orcest.fleet.cloud_init import (
    render_orchestrator_userdata,
    render_template_userdata,
    render_worker_userdata,
)

pytestmark = pytest.mark.unit


def _render(**overrides):
    defaults = {
        "redis_host": "10.20.0.23",
        "key_prefix": "orcest",
        "worker_id": "worker-200",
        "github_token": "ghp_fake",
        "claude_oauth_token": "sk-ant-oat01-fake",
        "repo": "ThayneStudio/orcest",
    }
    defaults.update(overrides)
    return render_worker_userdata(**defaults)


def test_output_starts_with_cloud_config_header():
    """The rendered user-data must start with #cloud-config."""
    output = _render()
    assert output.startswith("#cloud-config\n")


def test_output_is_valid_yaml():
    """The rendered user-data is valid YAML after the header."""
    output = _render()
    data = yaml.safe_load(output)
    assert isinstance(data, dict)


def test_worker_yaml_in_write_files():
    """The worker.yaml content is included in write_files."""
    output = _render(redis_host="10.0.0.1", key_prefix="myproject")
    data = yaml.safe_load(output)

    worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
    worker_cfg = yaml.safe_load(worker_file["content"])
    assert worker_cfg["redis"]["host"] == "10.0.0.1"
    assert worker_cfg["redis"]["port"] == 6379
    assert worker_cfg["redis"]["key_prefix"] == "myproject"
    assert worker_cfg["worker_id"] == "worker-200"


def test_env_file_in_write_files():
    """The .env file contains GITHUB_TOKEN and CLAUDE_CODE_OAUTH_TOKEN."""
    output = _render(github_token="ghp_test123", claude_oauth_token="sk-test")
    data = yaml.safe_load(output)

    env_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/.env")
    assert "GITHUB_TOKEN=ghp_test123" in env_file["content"]
    assert "CLAUDE_CODE_OAUTH_TOKEN=sk-test" in env_file["content"]
    assert env_file["permissions"] == "0600"


def test_systemd_unit_in_write_files():
    """The systemd unit file is written to the correct path."""
    output = _render()
    data = yaml.safe_load(output)

    unit_file = next(
        f for f in data["write_files"] if f["path"] == "/etc/systemd/system/orcest-worker.service"
    )
    assert "ExecStart=/opt/orcest/venv/bin/orcest work" in unit_file["content"]
    assert "ReadWritePaths=" in unit_file["content"]
    assert "/home/orcest/.cache" in unit_file["content"]


def test_claude_json_onboarding_bypass():
    """The .claude.json is written to bypass Claude CLI onboarding."""
    output = _render()
    data = yaml.safe_load(output)

    claude_file = next(f for f in data["write_files"] if f["path"] == "/home/orcest/.claude.json")
    assert "hasCompletedOnboarding" in claude_file["content"]


def test_runcmd_installs_key_tools():
    """runcmd entries install Node, Docker, Claude CLI, gh, Supabase, Playwright."""
    output = _render()
    data = yaml.safe_load(output)
    runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])

    assert "nodesource" in runcmd
    assert "docker-ce" in runcmd
    assert "claude-code" in runcmd
    assert "gh" in runcmd
    assert "supabase" in runcmd
    assert "playwright" in runcmd


def test_runcmd_installs_orcest_from_repo():
    """runcmd installs orcest from the given GitHub repo."""
    output = _render(repo="MyOrg/my-project")
    data = yaml.safe_load(output)
    runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])

    assert "git+https://github.com/MyOrg/my-project.git" in runcmd


def test_runcmd_enables_worker_service():
    """runcmd enables and starts the worker systemd service."""
    output = _render()
    data = yaml.safe_load(output)
    runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])

    assert "systemctl enable --now orcest-worker" in runcmd


def test_packages_include_golang():
    """The packages list includes Go for worker toolchain."""
    output = _render()
    data = yaml.safe_load(output)
    assert "golang-go" in data["packages"]


def test_worker_no_ssh_keys():
    """Workers are ephemeral — no SSH key injection."""
    output = _render()
    data = yaml.safe_load(output)
    assert "ssh_authorized_keys" not in data
    assert "ssh_authorized_keys" not in data["users"][1]


# ── Orchestrator userdata tests ─────────────────────────────


class TestOrchestratorUserdata:
    def test_valid_yaml_with_cloud_config_header(self):
        output = render_orchestrator_userdata()
        assert output.startswith("#cloud-config\n")
        data = yaml.safe_load(output)
        assert isinstance(data, dict)

    def test_has_orcest_user(self):
        data = yaml.safe_load(render_orchestrator_userdata())
        users = data["users"]
        assert users[0] == "default"
        orcest = users[1]
        assert orcest["name"] == "orcest"
        assert "docker" in orcest["groups"]
        assert "sudo" in orcest["groups"]

    def test_installs_docker_with_compose_plugin(self):
        data = yaml.safe_load(render_orchestrator_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "docker-compose-plugin" in runcmd

    def test_enables_qemu_guest_agent(self):
        data = yaml.safe_load(render_orchestrator_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "systemctl enable qemu-guest-agent" in runcmd
        assert "systemctl start qemu-guest-agent" in runcmd

    def test_ssh_key_injection(self):
        data = yaml.safe_load(render_orchestrator_userdata(ssh_public_key="ssh-ed25519 AAAA"))
        # Key on orcest user
        assert "ssh-ed25519 AAAA" in data["users"][1]["ssh_authorized_keys"]
        # Key on default user (top-level)
        assert "ssh-ed25519 AAAA" in data["ssh_authorized_keys"]

    def test_no_ssh_key_without_arg(self):
        data = yaml.safe_load(render_orchestrator_userdata())
        assert "ssh_authorized_keys" not in data
        assert "ssh_authorized_keys" not in data["users"][1]

    def test_does_not_install_worker_tools(self):
        data = yaml.safe_load(render_orchestrator_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "claude-code" not in runcmd
        assert "nodesource" not in runcmd


# ── Template userdata tests ─────────────────────────────────


class TestTemplateUserdata:
    def test_valid_yaml_with_cloud_config_header(self):
        output = render_template_userdata()
        assert output.startswith("#cloud-config\n")
        data = yaml.safe_load(output)
        assert isinstance(data, dict)

    def test_installs_worker_tooling(self):
        data = yaml.safe_load(render_template_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "claude-code" in runcmd
        assert "nodesource" in runcmd
        assert "docker-ce" in runcmd
        assert "gh" in runcmd
        assert "supabase" in runcmd
        assert "playwright" in runcmd

    def test_no_compose_plugin(self):
        data = yaml.safe_load(render_template_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "docker-compose-plugin" not in runcmd

    def test_no_worker_service(self):
        data = yaml.safe_load(render_template_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "orcest-worker" not in runcmd

    def test_netplan_dhcp_identifier_mac(self):
        data = yaml.safe_load(render_template_userdata())
        netplan_file = next(
            f for f in data["write_files"] if f["path"] == "/etc/netplan/99-orcest.yaml"
        )
        netplan = yaml.safe_load(netplan_file["content"])
        eth0 = netplan["network"]["ethernets"]["eth0"]
        assert eth0["dhcp4"] is True
        assert eth0["dhcp-identifier"] == "mac"

    def test_creates_venv_without_installing_orcest(self):
        data = yaml.safe_load(render_template_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "python3 -m venv /opt/orcest/venv" in runcmd
        assert "pip install" not in runcmd

    def test_ssh_key_injection(self):
        data = yaml.safe_load(render_template_userdata(ssh_public_key="ssh-ed25519 BBBB"))
        assert "ssh-ed25519 BBBB" in data["users"][1]["ssh_authorized_keys"]
        assert "ssh-ed25519 BBBB" in data["ssh_authorized_keys"]

    def test_includes_worker_packages(self):
        data = yaml.safe_load(render_template_userdata())
        assert "golang-go" in data["packages"]
        assert "python3" in data["packages"]
        assert "qemu-guest-agent" in data["packages"]
