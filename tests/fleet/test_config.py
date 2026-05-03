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
            orgs={"Org": OrgEntry(github_token="ghp_abc", claude_oauth_tokens=["sk_def"])},
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
        path.write_text(yaml.dump({"projects": [{"name": "old", "repo": "O/old"}]}))
        cfg = load_config(path)
        assert cfg.pool.size == 4
        assert cfg.pool.template_vm_id == 0
        assert cfg.pool.worker_memory == 16384
        # template_vmid_range defaults to empty list (range mode disabled)
        assert cfg.pool.template_vmid_range == []
        assert cfg.pool.template_range() is None

    def test_round_trip_template_vmid_range(self, tmp_path):
        """template_vmid_range is persisted as a list of two ints."""
        path = tmp_path / "config.yaml"
        original = FleetConfig(
            pool=PoolConfig(template_vmid_range=[9000, 9009], template_vm_id=9001),
        )
        save_config(original, path)
        loaded = load_config(path)
        assert loaded.pool.template_vmid_range == [9000, 9009]
        assert loaded.pool.template_range() == (9000, 9009)
        # The single-VMID fallback also persists for backward compat.
        assert loaded.pool.template_vm_id == 9001

    def test_load_template_range_only(self, tmp_path):
        """A config with range but no template_vm_id parses correctly."""
        path = tmp_path / "config.yaml"
        path.write_text(
            yaml.dump({"pool": {"template_vmid_range": [9000, 9009]}})
        )
        cfg = load_config(path)
        assert cfg.pool.template_range() == (9000, 9009)
        assert cfg.pool.template_vm_id == 0

    def test_legacy_single_vmid_keeps_working(self, tmp_path):
        """A legacy config with only template_vm_id (no range) is backward-compat."""
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump({"pool": {"template_vm_id": 9000}}))
        cfg = load_config(path)
        assert cfg.pool.template_vm_id == 9000
        assert cfg.pool.template_range() is None  # range mode disabled

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


class TestPoolConfigTemplateRange:
    def test_empty_returns_none(self):
        assert PoolConfig().template_range() is None

    def test_valid_pair_returns_tuple(self):
        assert PoolConfig(template_vmid_range=[9000, 9009]).template_range() == (
            9000,
            9009,
        )

    def test_wrong_length_raises(self):
        with pytest.raises(ValueError, match=r"\[start, end\]"):
            PoolConfig(template_vmid_range=[9000]).template_range()

    def test_inverted_range_raises(self):
        with pytest.raises(ValueError, match="0 < start <= end"):
            PoolConfig(template_vmid_range=[9009, 9000]).template_range()

    def test_zero_start_raises(self):
        with pytest.raises(ValueError, match="0 < start <= end"):
            PoolConfig(template_vmid_range=[0, 9]).template_range()


class TestPoolConfigValidateVmidRanges:
    """Bug 4: workers and templates must use disjoint VMID ranges."""

    def test_disjoint_above_template_range_ok(self):
        # Workers above template range: classic deployment.
        cfg = PoolConfig(template_vmid_range=[9000, 9009], vm_id_start=10000)
        cfg.validate_vmid_ranges()  # no raise

    def test_disjoint_below_template_range_ok(self):
        cfg = PoolConfig(
            template_vmid_range=[9000, 9009],
            vm_id_start=300,
            vm_id_end=999,
        )
        cfg.validate_vmid_ranges()  # no raise

    def test_overlap_at_start_raises(self):
        # vm_id_start lands inside the template range — this is the live
        # production misconfiguration that triggered the bug.
        cfg = PoolConfig(template_vmid_range=[9000, 9009], vm_id_start=9001)
        with pytest.raises(ValueError, match=r"vm_id_start .9001. overlaps"):
            cfg.validate_vmid_ranges()

    def test_overlap_at_template_start_raises(self):
        cfg = PoolConfig(template_vmid_range=[9000, 9009], vm_id_start=9000)
        with pytest.raises(ValueError, match="overlaps"):
            cfg.validate_vmid_ranges()

    def test_overlap_at_template_end_raises(self):
        cfg = PoolConfig(template_vmid_range=[9000, 9009], vm_id_start=9009)
        with pytest.raises(ValueError, match="overlaps"):
            cfg.validate_vmid_ranges()

    def test_open_ended_worker_range_below_passes_when_disjoint(self):
        # Open-ended worker range (vm_id_end=0) is fine if vm_id_start is above
        # the template range.
        cfg = PoolConfig(template_vmid_range=[9000, 9009], vm_id_start=10000, vm_id_end=0)
        cfg.validate_vmid_ranges()

    def test_open_ended_worker_range_below_template_range_overlaps(self):
        # vm_id_start=300 with no upper bound spans across the template range.
        cfg = PoolConfig(template_vmid_range=[9000, 9009], vm_id_start=300, vm_id_end=0)
        with pytest.raises(ValueError, match="overlaps"):
            cfg.validate_vmid_ranges()

    def test_no_template_range_passes(self):
        # Legacy single-VMID mode: no range to overlap with.
        cfg = PoolConfig(template_vm_id=9000, vm_id_start=9001)
        cfg.validate_vmid_ranges()

    def test_zero_vm_id_start_passes(self):
        # Unconfigured worker range: nothing to validate.
        cfg = PoolConfig(template_vmid_range=[9000, 9009], vm_id_start=0)
        cfg.validate_vmid_ranges()

    def test_load_config_runs_validation(self, tmp_path):
        # End-to-end: an overlapping config in YAML refuses to load.
        path = tmp_path / "config.yaml"
        path.write_text(
            yaml.dump(
                {"pool": {"template_vmid_range": [9000, 9009], "vm_id_start": 9001}}
            )
        )
        with pytest.raises(ValueError, match="overlaps"):
            load_config(path)

    def test_load_config_with_disjoint_ranges_succeeds(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_text(
            yaml.dump(
                {"pool": {"template_vmid_range": [9000, 9009], "vm_id_start": 10000}}
            )
        )
        cfg = load_config(path)
        assert cfg.pool.vm_id_start == 10000
        assert cfg.pool.template_range() == (9000, 9009)


class TestProxmoxConfigIsLocalhost:
    def test_default_is_localhost(self):
        assert ProxmoxConfig().is_localhost() is True

    def test_localhost_hostname(self):
        assert ProxmoxConfig(endpoint="https://localhost:8006").is_localhost() is True

    def test_ipv6_loopback(self):
        assert ProxmoxConfig(endpoint="https://[::1]:8006").is_localhost() is True

    def test_real_ip_is_not_localhost(self):
        assert ProxmoxConfig(endpoint="https://10.20.0.1:8006").is_localhost() is False

    def test_hostname_is_not_localhost(self):
        assert ProxmoxConfig(endpoint="https://pve.local:8006").is_localhost() is False
