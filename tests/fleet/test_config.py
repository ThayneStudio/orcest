"""Tests for orcest.fleet.config."""

import os
import stat

import pytest
import yaml

from orcest.fleet.config import (
    FleetConfig,
    OrchestratorConfig,
    OrgEntry,
    PoolConfig,
    ProjectEntry,
    ProxmoxConfig,
    _parse_disk_size,
    load_config,
    require_valid_project_name,
    save_config,
    validate_project_name,
)

pytestmark = pytest.mark.unit


# ── validate_project_name ────────────────────────────────────


class TestValidateProjectName:
    def test_simple_name(self):
        assert validate_project_name("my-project") is True

    def test_alphanumeric_with_dots_hyphens_underscores(self):
        assert validate_project_name("my.project_v2-beta") is True

    def test_rejects_empty(self):
        assert validate_project_name("") is False

    def test_rejects_starting_with_hyphen(self):
        assert validate_project_name("-bad") is False

    def test_rejects_starting_with_dot(self):
        assert validate_project_name(".bad") is False

    def test_rejects_shell_injection(self):
        assert validate_project_name('"; rm -rf /') is False

    def test_rejects_spaces(self):
        assert validate_project_name("has space") is False

    def test_rejects_over_64_chars(self):
        assert validate_project_name("a" * 65) is False

    def test_accepts_64_chars(self):
        assert validate_project_name("a" * 64) is True


# ── _parse_disk_size ─────────────────────────────────────────


class TestParseDiskSize:
    def test_int_passthrough(self):
        assert _parse_disk_size(20) == 20

    def test_string_with_G_suffix(self):
        assert _parse_disk_size("20G") == 20

    def test_string_with_GB_suffix(self):
        assert _parse_disk_size("20GB") == 20

    def test_string_lowercase(self):
        assert _parse_disk_size("30g") == 30

    def test_plain_numeric_string(self):
        assert _parse_disk_size("50") == 50

    def test_invalid_string(self):
        with pytest.raises(ValueError, match="Invalid disk_size"):
            _parse_disk_size("abc")


# ── FleetConfig helpers ──────────────────────────────────────


class TestFleetConfigHelpers:
    def test_get_project_found(self):
        cfg = FleetConfig(projects=[ProjectEntry(name="alpha", repo="Org/alpha")])
        assert cfg.get_project("alpha") is not None
        assert cfg.get_project("alpha").repo == "Org/alpha"

    def test_get_project_missing(self):
        cfg = FleetConfig()
        assert cfg.get_project("nope") is None

    def test_resolve_org_found(self):
        cfg = FleetConfig(
            orgs={"MyOrg": OrgEntry(github_token="ghp_x")},
            projects=[ProjectEntry(name="p", repo="MyOrg/p")],
        )
        org = cfg.resolve_org(cfg.projects[0])
        assert org.github_token == "ghp_x"

    def test_resolve_org_missing(self):
        cfg = FleetConfig(projects=[ProjectEntry(name="p", repo="Unknown/p")])
        with pytest.raises(KeyError, match="not registered"):
            cfg.resolve_org(cfg.projects[0])

    def test_ssh_target(self):
        cfg = FleetConfig(orchestrator=OrchestratorConfig(host="1.2.3.4", user="admin"))
        assert cfg.ssh_target() == "admin@1.2.3.4"

    def test_ssh_target_no_host(self):
        cfg = FleetConfig()
        with pytest.raises(RuntimeError, match="not set"):
            cfg.ssh_target()


# ── load_config / save_config round-trip ─────────────────────


class TestConfigPersistence:
    def test_round_trip(self, tmp_path):
        path = tmp_path / "config.yaml"
        original = FleetConfig(
            proxmox=ProxmoxConfig(node="mynode", storage="ceph", api_token_id="root@pam!t"),
            orchestrator=OrchestratorConfig(vm_id=200, host="10.0.0.1", disk_size=40),
            orgs={"Org": OrgEntry(github_token="ghp_abc", claude_oauth_token="sk_def")},
            projects=[
                ProjectEntry(name="proj", repo="Org/proj"),
            ],
            pool=PoolConfig(size=6, template_vm_id=9000, storage="nvme-pool"),
        )
        save_config(original, path)
        loaded = load_config(path)

        assert loaded.proxmox.node == "mynode"
        assert loaded.proxmox.storage == "ceph"
        assert loaded.orchestrator.vm_id == 200
        assert loaded.orchestrator.host == "10.0.0.1"
        assert loaded.orchestrator.disk_size == 40
        assert loaded.orgs["Org"].github_token == "ghp_abc"
        assert len(loaded.projects) == 1
        assert loaded.projects[0].name == "proj"
        assert loaded.pool.size == 6
        assert loaded.pool.template_vm_id == 9000
        assert loaded.pool.storage == "nvme-pool"

    def test_load_missing_file(self, tmp_path):
        cfg = load_config(tmp_path / "does-not-exist.yaml")
        assert isinstance(cfg, FleetConfig)
        assert cfg.projects == []

    def test_load_empty_file(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("")
        cfg = load_config(path)
        assert isinstance(cfg, FleetConfig)

    def test_load_disk_size_string_compat(self, tmp_path):
        """Old configs may store disk_size as '20G' — should parse correctly."""
        path = tmp_path / "config.yaml"
        path.write_text(
            yaml.dump(
                {
                    "orchestrator": {"disk_size": "30G"},
                }
            )
        )
        cfg = load_config(path)
        assert cfg.orchestrator.disk_size == 30

    def test_round_trip_pool_config(self, tmp_path):
        path = tmp_path / "config.yaml"
        original = FleetConfig(
            pool=PoolConfig(
                size=8,
                template_vm_id=9000,
                storage="ssd-pool",
                worker_memory=32768,
                worker_cores=16,
                worker_disk_size=100,
                max_task_duration=7200,
            ),
        )
        save_config(original, path)
        loaded = load_config(path)
        assert loaded.pool.size == 8
        assert loaded.pool.template_vm_id == 9000
        assert loaded.pool.worker_memory == 32768
        assert loaded.pool.worker_cores == 16
        assert loaded.pool.worker_disk_size == 100
        assert loaded.pool.max_task_duration == 7200

    def test_load_legacy_config_without_pool(self, tmp_path):
        """Old configs without pool section get correct defaults."""
        path = tmp_path / "config.yaml"
        path.write_text(
            yaml.dump(
                {
                    "projects": [
                        {"name": "old", "repo": "O/old"}
                    ]
                }
            )
        )
        cfg = load_config(path)
        assert cfg.pool.size == 4
        assert cfg.pool.template_vm_id == 0
        assert cfg.pool.worker_memory == 16384

    def test_save_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "sub" / "dir" / "config.yaml"
        save_config(FleetConfig(), path)
        assert path.exists()

    def test_save_file_permissions(self, tmp_path):
        path = tmp_path / "config.yaml"
        save_config(FleetConfig(), path)
        mode = stat.S_IMODE(os.stat(path).st_mode)
        assert mode == 0o600


# ── require_valid_project_name ────────────────────────────────


class TestRequireValidProjectName:
    def test_valid_name_does_not_raise(self):
        require_valid_project_name("my-project")  # should not raise

    def test_invalid_name_raises(self):
        with pytest.raises(ValueError, match="Invalid project name"):
            require_valid_project_name("")

    def test_shell_injection_raises(self):
        with pytest.raises(ValueError):
            require_valid_project_name("; rm -rf /")
