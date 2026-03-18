"""Tests for orcest.fleet.provisioner."""

import json
import os
import stat

import pytest

from orcest.fleet.config import (
    FleetConfig,
    OrchestratorConfig,
    OrgEntry,
    ProjectEntry,
    ProxmoxConfig,
)
from orcest.fleet.provisioner import generate_tfvars, write_tfvars

pytestmark = pytest.mark.unit


def _cfg(**kwargs) -> FleetConfig:
    """Build a FleetConfig with reasonable defaults for testing."""
    defaults = dict(
        proxmox=ProxmoxConfig(
            node="pve",
            storage="local-lvm",
            api_token_id="root@pam!orcest",
            api_token_secret="secret123",
        ),
        orchestrator=OrchestratorConfig(
            vm_id=199,
            host="10.0.0.1",
            ssh_key="ssh-ed25519 AAAA...",
            disk_size=20,
        ),
        orgs={"Org": OrgEntry(github_token="ghp_abc", claude_oauth_token="sk_def")},
        projects=[],
    )
    defaults.update(kwargs)
    return FleetConfig(**defaults)


class TestGenerateTfvars:
    def test_basic_structure(self):
        cfg = _cfg()
        tfvars = generate_tfvars(cfg)

        assert tfvars["proxmox_endpoint"] == "https://127.0.0.1:8006"
        assert tfvars["proxmox_api_token"] == "root@pam!orcest=secret123"
        assert tfvars["proxmox_node"] == "pve"
        assert tfvars["proxmox_storage"] == "local-lvm"
        assert tfvars["orchestrator"]["vm_id"] == 199
        assert tfvars["orchestrator"]["disk_size"] == 20
        assert tfvars["workers"] == {}

    def test_workers_always_empty(self):
        """Workers are managed by the pool manager, not Terraform."""
        cfg = _cfg(
            projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
        )
        tfvars = generate_tfvars(cfg)
        assert tfvars["workers"] == {}

    def test_empty_token_raises(self):
        cfg = _cfg(
            proxmox=ProxmoxConfig(api_token_id="", api_token_secret=""),
        )
        with pytest.raises(ValueError, match="Proxmox API token not configured"):
            generate_tfvars(cfg)

    def test_empty_token_id_only_raises(self):
        cfg = _cfg(
            proxmox=ProxmoxConfig(api_token_id="", api_token_secret="secret"),
        )
        with pytest.raises(ValueError, match="Proxmox API token not configured"):
            generate_tfvars(cfg)

    def test_disk_size_is_int(self):
        cfg = _cfg(
            orchestrator=OrchestratorConfig(
                vm_id=199, host="10.0.0.1", disk_size=50,
            ),
        )
        tfvars = generate_tfvars(cfg)
        assert tfvars["orchestrator"]["disk_size"] == 50
        assert isinstance(tfvars["orchestrator"]["disk_size"], int)

    def test_orchestrator_cloud_init_content(self):
        cfg = _cfg()
        tfvars = generate_tfvars(cfg)
        assert "cloud_init_content" in tfvars["orchestrator"]
        assert "#cloud-config" in tfvars["orchestrator"]["cloud_init_content"]


class TestWriteTfvars:
    def test_writes_json(self, tmp_path):
        tfvars = {"proxmox_node": "pve", "workers": {}}
        write_tfvars(tfvars, config_dir=tmp_path)

        path = tmp_path / "terraform.tfvars.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["proxmox_node"] == "pve"

    def test_file_permissions(self, tmp_path):
        write_tfvars({"test": True}, config_dir=tmp_path)
        path = tmp_path / "terraform.tfvars.json"
        mode = stat.S_IMODE(os.stat(path).st_mode)
        assert mode == 0o600

    def test_atomic_overwrite(self, tmp_path):
        write_tfvars({"version": 1}, config_dir=tmp_path)
        write_tfvars({"version": 2}, config_dir=tmp_path)
        path = tmp_path / "terraform.tfvars.json"
        data = json.loads(path.read_text())
        assert data["version"] == 2

    def test_no_leftover_tmp_on_success(self, tmp_path):
        write_tfvars({"test": True}, config_dir=tmp_path)
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []
