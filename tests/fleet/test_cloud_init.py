"""Tests for orcest.fleet.cloud_init."""

import re
from pathlib import Path

import pytest
import yaml

from orcest.fleet.cloud_init import (
    _CLAUDE_VERSION,
    _CODEX_VERSION,
    _GROK_VERSION,
    _NODE_MAJOR,
    _PLAYWRIGHT_MAJOR,
    _SUPABASE_VERSION,
    render_clone_userdata,
    render_orchestrator_userdata,
    render_template_userdata,
)

pytestmark = pytest.mark.unit


def test_manual_setup_codex_pin_matches_cloud_init() -> None:
    """Shell and Python Codex pins must stay identical; silent drift would
    ship a CLI the parser fixtures were not validated against."""
    repo = Path(__file__).resolve().parents[2]
    script = (repo / "provision" / "setup-worker.sh").read_text()
    cloud_init = (repo / "src" / "orcest" / "fleet" / "cloud_init.py").read_text()

    sh_match = re.search(r'^CODEX_VERSION="([^"]+)"', script, re.MULTILINE)
    py_match = re.search(r'^_CODEX_VERSION = "([^"]+)"', cloud_init, re.MULTILINE)
    assert sh_match is not None, "provision/setup-worker.sh is missing CODEX_VERSION"
    assert py_match is not None, "cloud_init.py is missing _CODEX_VERSION"
    assert sh_match.group(1) == py_match.group(1), (
        f"Codex pins diverged: setup-worker.sh={sh_match.group(1)!r} "
        f"cloud_init.py={py_match.group(1)!r}"
    )
    assert sh_match.group(1) == "0.149.1"
    assert _CODEX_VERSION == "0.149.1"


# NOTE: the dead, secret-leaking ``render_worker_userdata`` and its
# ``_render`` helper (plus ~13 tests that exercised it) were removed in M4 —
# that path baked long-lived GitHub + Claude tokens into retrievable
# cloud-init user-data. Equivalent coverage for the LIVE paths lives under
# TestTemplateUserdata and TestCloneUserdata, and the regression guard
# ``test_render_worker_userdata_removed`` keeps it deleted.


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
        assert f"setup_{_NODE_MAJOR}.x" in runcmd
        assert "docker-ce" in runcmd
        assert "gh" in runcmd
        assert f"v{_SUPABASE_VERSION}" in runcmd
        assert f"playwright@{_PLAYWRIGHT_MAJOR}" in runcmd
        assert "deno.land/install.sh" in runcmd
        assert "npm install -g bun" in runcmd
        assert "astral.sh/uv/install.sh" in runcmd
        assert "npm install -g wrangler" in runcmd
        # Grok CLI: the exact versioned binary is fetched directly, checksum
        # gated before installation, and made executable for the worker user.
        assert f"x.ai/cli/grok-{_GROK_VERSION}-linux-x86_64" in runcmd
        assert "x.ai/cli/install.sh" not in runcmd
        assert "/usr/local/bin/grok" in runcmd
        assert "install -m 0755 -o root -g root /tmp/grok /usr/local/bin/grok" in runcmd
        # The post-install check runs as the orcest worker user, so it catches
        # exec-permission regressions (not just root-visible presence).
        assert "sudo -u orcest -H bash -lc 'command -v grok && grok --version'" in runcmd
        # The checksum gate and the install must be ONE runcmd entry joined by
        # `&&` — cloud-init has no `set -e` across entries, so a separate gate
        # wouldn't actually block the install on a checksum mismatch.
        gated = [
            c
            for c in data["runcmd"]
            if "sha256sum" in str(c) and "install -m 0755" in str(c) and "&&" in str(c)
        ]
        assert gated, "Grok binary checksum gate and install must be one entry"
        # Codex CLI (OpenAI codex-cli): pinned npm-global install, no
        # silent-failure swallow, and verified as the orcest worker user so
        # any exec-perms regression (mirroring the grok /root/.local/bin
        # /root=0700 incident) surfaces at bake time, not first-task time.
        assert f"npm install -g @openai/codex@{_CODEX_VERSION}" in runcmd
        assert f"npm install -g @openai/codex@{_CODEX_VERSION} || true" not in runcmd
        assert "sudo -u orcest -H bash -lc 'command -v codex && codex --version'" in runcmd
        # Claude CLI: pinned like every other provider CLI. The interactive
        # (PTY) runner recognises Claude's setup dialogs by their rendered
        # text, so a floating upgrade silently changes a contract it depends
        # on -- that is how the 2026-08-14 prompt-eaten-by-the-MCP-menu
        # regression reached production and burned ~27% of clauder tasks.
        assert f"npm install -g @anthropic-ai/claude-code@{_CLAUDE_VERSION}" in runcmd
        assert 'npm install -g @anthropic-ai/claude-code"' not in runcmd
        assert "npm install -g @anthropic-ai/claude-code'" not in runcmd

    @pytest.mark.parametrize(
        "digest",
        ["not-a-digest", "a" * 63, "a" * 65, '"; touch /root/pwned; #', "a" * 32 + "\nwhoami"],
    )
    def test_rejects_unsafe_grok_binary_digest_override(self, monkeypatch, digest):
        monkeypatch.setenv("ORCEST_GROK_BINARY_SHA256", digest)

        with pytest.raises(ValueError, match="exactly 64 hexadecimal"):
            render_template_userdata()

    def test_template_packages_include_quality_of_life_tools(self):
        data = yaml.safe_load(render_template_userdata())
        packages = set(data.get("packages", []))
        assert "ripgrep" in packages
        assert "fd-find" in packages
        assert "jq" in packages
        assert "redis-tools" in packages
        assert "postgresql-client" in packages

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

    def test_creates_venv_without_fetching_orcest_from_github(self):
        data = yaml.safe_load(render_template_userdata())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "python3 -m venv /opt/orcest/venv" in runcmd
        assert "github.com/ThayneStudio/orcest.git" not in runcmd

    def test_ssh_key_injection(self):
        data = yaml.safe_load(render_template_userdata(ssh_public_key="ssh-ed25519 BBBB"))
        assert "ssh-ed25519 BBBB" in data["users"][1]["ssh_authorized_keys"]
        assert "ssh-ed25519 BBBB" in data["ssh_authorized_keys"]

    def test_includes_worker_packages(self):
        data = yaml.safe_load(render_template_userdata())
        assert "golang-go" in data["packages"]
        assert "python3" in data["packages"]
        assert "qemu-guest-agent" in data["packages"]

    def test_orcest_owned_writes_are_deferred(self):
        """Bug 6: write_files for orcest-owned paths must use ``defer: true``.

        cloud-init's cc_write_files runs in the config stage (before
        cc_users_groups creates the orcest user). Without defer, chowning
        to orcest:orcest fails with ``Unknown user or group: "orcest"``.
        """
        data = yaml.safe_load(render_template_userdata())
        for entry in data["write_files"]:
            owner = entry.get("owner", "")
            if owner.startswith("orcest"):
                assert entry.get("defer") is True, (
                    f"write_files entry for {entry['path']} owned by {owner!r} must set defer=true"
                )

    def test_template_versions_audit_file_written(self):
        """Bug 7: ``/etc/orcest/template.versions`` records baked-in versions."""
        data = yaml.safe_load(render_template_userdata())
        entry = next(f for f in data["write_files"] if f["path"] == "/etc/orcest/template.versions")
        # Owner is root:root — system metadata, not user data; not deferred.
        assert entry.get("owner") == "root:root"
        assert entry.get("permissions") == "0644"
        # No defer — the path doesn't depend on the orcest user.
        assert "defer" not in entry or entry["defer"] is False
        # Content lists the major version constants and a bumped_at timestamp.
        content = entry["content"]
        assert f"node_major={_NODE_MAJOR}" in content
        assert f"playwright_major={_PLAYWRIGHT_MAJOR}" in content
        assert f"supabase_version={_SUPABASE_VERSION}" in content
        assert f"grok_version={_GROK_VERSION}" in content
        assert f"codex_version={_CODEX_VERSION}" in content
        assert f"claude_version={_CLAUDE_VERSION}" in content
        assert "bumped_at=" in content

    def test_template_versions_bumped_at_is_iso_timestamp(self):
        """Bug 7: bumped_at is a parseable ISO 8601 UTC timestamp."""
        from datetime import datetime

        data = yaml.safe_load(render_template_userdata())
        entry = next(f for f in data["write_files"] if f["path"] == "/etc/orcest/template.versions")
        line = next(line for line in entry["content"].splitlines() if line.startswith("bumped_at="))
        ts = line.split("=", 1)[1].strip()
        # Must round-trip through fromisoformat without raising.
        parsed = datetime.fromisoformat(ts)
        # And must be timezone-aware (UTC).
        assert parsed.tzinfo is not None


# ── Clone userdata tests ──────────────────────────────────


class TestCloneUserdata:
    def _render(self, **overrides):
        defaults = {
            "redis_host": "10.20.0.23",
            "worker_id": "orcest-worker-10002",
            "key_prefix": "orcest",
        }
        defaults.update(overrides)
        return render_clone_userdata(**defaults)

    def test_valid_yaml_with_cloud_config_header(self):
        output = self._render()
        assert output.startswith("#cloud-config\n")
        data = yaml.safe_load(output)
        assert isinstance(data, dict)

    def test_worker_yaml_content(self):
        output = self._render(redis_host="10.0.0.1", key_prefix="myprefix", worker_id="w-99")
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["redis"]["host"] == "10.0.0.1"
        assert cfg["redis"]["port"] == 6379
        assert cfg["redis"]["key_prefix"] == "myprefix"
        assert cfg["worker_id"] == "w-99"
        assert cfg["ephemeral"] is True
        assert cfg["pool_managed"] is True
        assert cfg["backend"] == "claude"
        # C1c: interactive (PTY) mode has no ladder coverage, so its
        # wall-clock timeout is pinned to the pre-branch default rather
        # than inheriting the raised RunnerConfig ceiling.
        assert cfg["runner"] == {
            "type": "claude",
            "extra": {"mode": "interactive"},
            "timeout": 5400,
            "watchdog": {"enabled": True},
        }

    def test_worker_yaml_can_use_isolated_clauder_backend(self):
        output = self._render(
            worker_backend="clauder",
            worker_runner_type="claude",
            worker_runner_mode="interactive",
        )
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["backend"] == "clauder"
        assert cfg["runner"] == {
            "type": "claude",
            "extra": {"mode": "interactive"},
            "timeout": 5400,
            "watchdog": {"enabled": True},
        }

    def test_worker_yaml_defaults_clauder_backend_to_interactive_mode(self):
        output = self._render(worker_backend="clauder")
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["backend"] == "clauder"
        assert cfg["runner"] == {
            "type": "claude",
            "extra": {"mode": "interactive"},
            "timeout": 5400,
            "watchdog": {"enabled": True},
        }

    def test_worker_yaml_rejects_non_interactive_clauder_mode(self):
        with pytest.raises(ValueError, match="worker_runner_mode 'interactive'"):
            self._render(worker_backend="clauder", worker_runner_mode="batch")

    def test_worker_yaml_defaults_legacy_claude_backend_to_interactive_mode(self):
        # Legacy fleet configs have no worker_runner_mode field; the deployed
        # behavior before the field existed was the interactive PTY runner, so
        # unset must keep it rather than silently downgrading to `claude -p`.
        output = self._render(worker_runner_mode="")
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["backend"] == "claude"
        assert cfg["runner"] == {
            "type": "claude",
            "extra": {"mode": "interactive"},
            "timeout": 5400,
            "watchdog": {"enabled": True},
        }

    def test_worker_yaml_can_disable_interactive_mode_for_legacy_claude(self):
        # Explicit opt-out: 'headless' renders the legacy `claude -p` runner
        # section with no extra.mode key, and (C1c) no timeout pin either --
        # headless workers DO get ladder coverage, so they keep the raised
        # RunnerConfig default (rendering nothing here means worker.yaml
        # inherits it).
        output = self._render(worker_runner_mode="headless")
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["backend"] == "claude"
        assert cfg["runner"] == {"type": "claude", "watchdog": {"enabled": True}}

    def test_worker_yaml_rejects_unknown_claude_runner_mode(self):
        with pytest.raises(ValueError, match="'interactive' or 'headless'"):
            self._render(worker_backend="claude", worker_runner_mode="batch")

    # ── C1a: fleet watchdog toggle ──────────────────────────────

    def test_worker_yaml_watchdog_enabled_default_true(self):
        output = self._render(worker_runner_mode="headless")
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["runner"]["watchdog"] == {"enabled": True}

    def test_worker_yaml_watchdog_can_be_disabled(self):
        output = self._render(worker_runner_mode="headless", watchdog_enabled=False)
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["runner"]["watchdog"] == {"enabled": False}

    def test_worker_yaml_watchdog_toggle_applies_to_interactive_mode_too(self):
        # The toggle is rendered regardless of runner mode -- interactive
        # workers just also get the separate C1c timeout pin.
        output = self._render(worker_runner_mode="interactive", watchdog_enabled=False)
        data = yaml.safe_load(output)
        worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
        cfg = yaml.safe_load(worker_file["content"])
        assert cfg["runner"]["watchdog"] == {"enabled": False}
        assert cfg["runner"]["timeout"] == 5400

    def test_systemd_unit_written(self):
        data = yaml.safe_load(self._render())
        svc_path = "/etc/systemd/system/orcest-worker.service"
        unit_file = next(f for f in data["write_files"] if f["path"] == svc_path)
        assert "ExecStart=/opt/orcest/venv/bin/orcest work" in unit_file["content"]

    def test_runcmd_starts_service(self):
        data = yaml.safe_load(self._render())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "systemctl daemon-reload" in runcmd
        assert "systemctl enable --now orcest-worker" in runcmd

    def test_clone_does_not_reinstall_orcest_from_github(self):
        data = yaml.safe_load(self._render())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "github.com/ThayneStudio/orcest.git" not in runcmd
        assert "pip install" not in runcmd

    def test_no_package_installation(self):
        data = yaml.safe_load(self._render())
        assert "packages" not in data

    def test_no_tooling_commands(self):
        data = yaml.safe_load(self._render())
        runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
        assert "nodesource" not in runcmd
        assert "docker-ce" not in runcmd
        assert "claude-code" not in runcmd

    def test_systemd_env_file_optional(self):
        """EnvironmentFile uses - prefix so missing .env doesn't fail."""
        data = yaml.safe_load(self._render())
        svc_path = "/etc/systemd/system/orcest-worker.service"
        unit_file = next(f for f in data["write_files"] if f["path"] == svc_path)
        assert "EnvironmentFile=-/opt/orcest/.env" in unit_file["content"]

    def test_systemd_start_limit_hardened(self):
        """StartLimitBurst/IntervalSec let systemd retry through a brief Redis
        outage during ``orcest fleet update``.  10 restarts over 5 minutes
        combined with the in-process ~60s Redis-connect retry covers a normal
        deploy without manual ``systemctl reset-failed`` per VM."""
        data = yaml.safe_load(self._render())
        svc_path = "/etc/systemd/system/orcest-worker.service"
        unit_file = next(f for f in data["write_files"] if f["path"] == svc_path)
        content = unit_file["content"]
        assert "StartLimitBurst=10" in content
        assert "StartLimitIntervalSec=300" in content
        assert "Restart=on-failure" in content
        assert "RestartSec=10" in content

    def test_orcest_owned_writes_are_deferred(self):
        """Bug 6: clone write_files for orcest-owned paths set ``defer: true``."""
        data = yaml.safe_load(self._render())
        for entry in data["write_files"]:
            owner = entry.get("owner", "")
            if owner.startswith("orcest"):
                assert entry.get("defer") is True, (
                    f"write_files entry for {entry['path']} owned by {owner!r} must set defer=true"
                )


# ── M4: dead secret-leaking render_worker_userdata must stay deleted ──


def test_render_worker_userdata_removed():
    """M4: the dead render_worker_userdata (which baked long-lived GitHub +
    Claude tokens into retrievable cloud-init user-data) must stay deleted so
    it cannot be reintroduced. The live pool path is render_clone_userdata,
    which carries no secrets.
    """
    import orcest.fleet.cloud_init as ci

    assert not hasattr(ci, "render_worker_userdata"), (
        "render_worker_userdata was re-added — it embeds GITHUB_TOKEN/"
        "CLAUDE_CODE_OAUTH_TOKEN into cloud-init user-data retrievable from the VM"
    )
    # The shared systemd helper used by the live clone path must still exist.
    assert hasattr(ci, "_systemd_unit")
    assert hasattr(ci, "render_clone_userdata")

    # And the live clone path must not embed those secret env vars.
    clone = ci.render_clone_userdata(
        redis_host="10.0.0.1", worker_id="orcest-worker-10009", key_prefix="orcest"
    )
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in clone
    assert "GITHUB_TOKEN" not in clone


# ── M3: reconcile generated + static systemd unit to one hardened body ──


def test_systemd_unit_hardening_reconciled():
    """M3: the generated worker unit must carry BOTH the StartLimit fix AND the
    full hardening (ProtectSystem/ProtectHome/ReadWritePaths), reconciled with
    the static provision/systemd unit. ReadWritePaths must include the Grok and
    Codex credential dirs or those providers EROFS under ProtectHome=read-only.
    """
    from pathlib import Path

    from orcest.fleet.cloud_init import _systemd_unit

    unit = _systemd_unit(worker_id="orcest-worker-10005")

    # StartLimit fix (workers-die-on-Redis-restart bug).
    assert "StartLimitBurst=10" in unit
    assert "StartLimitIntervalSec=300" in unit
    assert "StartLimitBurst=5" not in unit

    # Filesystem hardening that the generated unit was missing.
    assert "ProtectSystem=strict" in unit
    assert "ProtectHome=read-only" in unit
    assert "NoNewPrivileges=yes" in unit
    assert "RestrictSUIDSGID=yes" in unit
    assert "PrivateTmp=yes" in unit

    # ReadWritePaths must cover every dir the worker + providers write at
    # runtime, or ProtectHome/ProtectSystem break them.
    rw_line = next(line for line in unit.splitlines() if line.startswith("ReadWritePaths="))
    for needed in (
        "/opt/orcest",
        "/home/orcest/.claude",
        "/home/orcest/.codex",
        "/home/orcest/.grok",
    ):
        assert needed in rw_line, f"ReadWritePaths missing {needed}: {rw_line}"

    # Optional EnvironmentFile so clone VMs (no .env) still start.
    assert "EnvironmentFile=-/opt/orcest/.env" in unit

    # The static provision unit must be reconciled to the same hardened values.
    static = Path(__file__).resolve().parents[2] / "provision" / "systemd" / "orcest-worker.service"
    static_text = static.read_text()
    assert "StartLimitBurst=10" in static_text
    assert "StartLimitBurst=5" not in static_text
    assert "ProtectSystem=strict" in static_text
    assert "ProtectHome=read-only" in static_text
    static_rw = next(
        line for line in static_text.splitlines() if line.startswith("ReadWritePaths=")
    )
    for needed in ("/home/orcest/.codex", "/home/orcest/.grok"):
        assert needed in static_rw, f"static ReadWritePaths missing {needed}: {static_rw}"


# ── M3: ReadWritePaths dirs must be CREATED, or systemd refuses to start ──


def _runcmd_text(userdata: str) -> str:
    """Join a rendered cloud-init's runcmd entries into one searchable string."""
    data = yaml.safe_load(userdata)
    return "\n".join(str(cmd) for cmd in data.get("runcmd", []))


def test_template_runcmd_creates_codex_and_grok_dirs():
    """M3: the template bake (render_template_userdata, via
    _worker_workspace_runcmd) must mkdir /home/orcest/.codex and
    /home/orcest/.grok. They are listed in the worker unit's ReadWritePaths,
    and under ProtectHome=read-only systemd (>=249) refuses to start a unit
    whose ReadWritePaths target is missing — so the dirs must pre-exist.
    """
    runcmd = _runcmd_text(render_template_userdata())
    # Match the existing .claude/.cache creation pattern.
    assert "mkdir -p /home/orcest/.claude" in runcmd
    assert "mkdir -p /home/orcest/.cache" in runcmd
    assert "mkdir -p /home/orcest/.codex" in runcmd, (
        "template must create /home/orcest/.codex (a ReadWritePaths target) "
        "or workers EROFS/fail to start under ProtectHome=read-only"
    )
    assert "mkdir -p /home/orcest/.grok" in runcmd, (
        "template must create /home/orcest/.grok (a ReadWritePaths target) "
        "or workers EROFS/fail to start under ProtectHome=read-only"
    )
    # Ownership must be fixed up so the orcest user can write at runtime.
    assert "chown -R orcest:orcest /home/orcest" in runcmd


def test_worker_workspace_runcmd_creates_all_readwrite_home_dirs():
    """M3: every /home/orcest ReadWritePaths target must be created by the
    shared workspace runcmd, so both the template and any caller stay in sync
    with the systemd unit's ReadWritePaths line."""
    from orcest.fleet.cloud_init import _WORKER_READ_WRITE_PATHS, _worker_workspace_runcmd

    cmds = _worker_workspace_runcmd()
    joined = "\n".join(cmds)
    home_targets = [p for p in _WORKER_READ_WRITE_PATHS.split() if p.startswith("/home/orcest/")]
    assert home_targets, "expected /home/orcest ReadWritePaths targets to exist"
    for target in home_targets:
        assert f"mkdir -p {target}" in joined, (
            f"ReadWritePaths target {target} is never created by "
            f"_worker_workspace_runcmd; systemd will refuse to start the unit"
        )


def test_clone_runcmd_creates_codex_and_grok_dirs():
    """M3: the LIVE pool path (render_clone_userdata) writes the hardened unit
    (ProtectHome=read-only, ReadWritePaths includes .codex/.grok) but the warm
    template may predate those dirs. The clone runcmd must (idempotently)
    create every /home/orcest ReadWritePaths target BEFORE
    ``systemctl enable --now orcest-worker`` so the unit can start.
    """
    output = render_clone_userdata(
        redis_host="10.0.0.1", worker_id="orcest-worker-10009", key_prefix="orcest"
    )
    data = yaml.safe_load(output)
    runcmd = [str(c) for c in data["runcmd"]]

    def mkdir_idx(path: str) -> int:
        # Tolerate both ``mkdir -p <path>`` and a combined
        # ``mkdir -p <a> <b> <path> ...`` form.
        return next(i for i, c in enumerate(runcmd) if "mkdir" in c and path in c)

    enable_idx = next(i for i, c in enumerate(runcmd) if "enable --now orcest-worker" in c)
    # The dirs must be created (and owned) BEFORE the unit is enabled/started,
    # otherwise the first start fails on the missing ReadWritePaths target.
    for path in ("/home/orcest/.codex", "/home/orcest/.grok"):
        idx = mkdir_idx(path)  # raises StopIteration -> test failure if absent
        assert idx < enable_idx, f"{path} must be created before enabling the worker unit"

    # And the orcest user must own them so runtime writes (auth.json) succeed.
    chown_idx = next(
        (
            i
            for i, c in enumerate(runcmd)
            if "chown" in c and "orcest:orcest" in c and "/home/orcest" in c
        ),
        None,
    )
    assert chown_idx is not None, "clone path must chown the created /home/orcest dirs to orcest"
    assert chown_idx < enable_idx, "chown must run before the unit is enabled"


def test_setup_worker_sh_installs_jq():
    """The merge-policy hook and cloud-init worker image both need jq.

    ``cloud_init._WORKER_PACKAGES`` already lists jq; the bare-VM path in
    setup-worker.sh must stay in parity so a freshly provisioned static
    worker does not fail-closed every Bash tool call.
    """
    setup = Path(__file__).resolve().parents[2] / "provision" / "setup-worker.sh"
    text = setup.read_text()
    assert re.search(r"(?m)^\s+jq\\?$", text), "setup-worker.sh must apt-install jq"


def test_setup_worker_sh_creates_codex_and_grok_dirs():
    """M3: the static-worker provisioning script must also create the
    /home/orcest ReadWritePaths targets (.codex/.grok), or static workers
    running the hardened unit fail to start the same way pool workers do.
    """
    from pathlib import Path

    setup = Path(__file__).resolve().parents[2] / "provision" / "setup-worker.sh"
    text = setup.read_text()
    assert "mkdir -p /home/orcest/.codex" in text, (
        "setup-worker.sh must create /home/orcest/.codex (a ReadWritePaths target)"
    )
    assert "mkdir -p /home/orcest/.grok" in text, (
        "setup-worker.sh must create /home/orcest/.grok (a ReadWritePaths target)"
    )
    # Owned by orcest so the worker can write auth.json under them at runtime.
    assert "chown -R orcest:orcest /home/orcest" in text


def test_setup_worker_sh_replaces_mismatched_grok_version():
    setup = Path(__file__).resolve().parents[2] / "provision" / "setup-worker.sh"
    text = setup.read_text()
    assert 'installed_grok_version="$([ -x /usr/local/bin/grok ]' in text
    assert "/usr/local/bin/grok --version" in text
    assert 'if [ "${installed_grok_version}" != "${GROK_VERSION}" ]; then' in text
    assert 'if [ "${installed_grok_version}" = "${GROK_VERSION}" ]; then' in text


# ── M2-infra: Grok binary SHA-256 gate must FAIL CLOSED when unset ──


def test_grok_binary_gate_fails_closed_when_sha_unset(monkeypatch):
    """With no trusted Grok binary SHA, the template must not install it.

    An empty digest must fail closed, not short-circuit the checksum to true.
    """
    import orcest.fleet.cloud_init as ci

    monkeypatch.delenv("ORCEST_GROK_BINARY_SHA256", raising=False)
    monkeypatch.setattr(ci, "_GROK_BINARY_SHA256", "", raising=True)

    runcmd = ci._worker_tooling_runcmd()
    joined = "\n".join(str(c) for c in runcmd)

    assert f"https://x.ai/cli/grok-{ci._GROK_VERSION}-linux-x86_64 -o /tmp/grok" in joined
    grok_entry = next(c for c in runcmd if "install -m 0755" in str(c))
    # The empty-digest short-circuit `[ -z "" ] ||` must be GONE — that is the bug.
    assert '[ -z "" ]' not in str(grok_entry)
    # Installation must be gated behind a non-empty SHA test that fails closed.
    assert 'if [ -n ""' in str(grok_entry) or "SKIPPING grok install" in str(grok_entry), (
        "empty SHA must skip the install"
    )


def test_grok_binary_gate_enforces_sha_before_install(monkeypatch):
    """An override digest gates installation of the exact downloaded binary."""
    import orcest.fleet.cloud_init as ci

    fake_sha = "a" * 64
    monkeypatch.setenv("ORCEST_GROK_BINARY_SHA256", fake_sha)

    runcmd = ci._worker_tooling_runcmd()
    grok_entry = next(c for c in runcmd if "install -m 0755" in str(c))
    s = str(grok_entry)
    assert fake_sha in s
    assert "sha256sum -c -" in s
    assert s.index("sha256sum -c -") < s.index("install -m 0755")


# ── M1: reproducible image builds — complete lock + Dockerfiles use it ──


def test_requirements_lock_and_dockerfiles_are_reproducible():
    """M1: requirements.lock must pin every declared runtime dependency
    (incl. proxmoxer + requests), and both Dockerfiles must install from the
    lock rather than re-resolving unpinned ranges out of pyproject.toml.
    """
    import re
    import tomllib
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    lock = (root / "requirements.lock").read_text().lower()
    pyproject = tomllib.loads((root / "pyproject.toml").read_text())
    deps = pyproject["project"]["dependencies"]

    # Every direct runtime dependency must appear (pinned with ==) in the lock.
    for spec in deps:
        name = re.split(r"[<>=!~\[ ]", spec, 1)[0].strip().lower()
        assert name in lock, f"requirements.lock is missing direct dependency {name!r}"
        assert f"{name}==" in lock, f"requirements.lock must PIN {name!r} with =="

    # Specifically the two the audit flagged as missing.
    assert "proxmoxer==" in lock
    assert "requests==" in lock

    # Both Dockerfiles must install from the lock, not parse pyproject deps.
    for df in (root / "Dockerfile", root / "src" / "orcest" / "fleet" / "deploy" / "Dockerfile"):
        text = df.read_text()
        assert "pip install --no-cache-dir -r requirements.lock" in text, (
            f"{df} must install pinned deps from requirements.lock"
        )
        assert "tomllib.load" not in text, (
            f"{df} still resolves unpinned deps from pyproject.toml — non-reproducible"
        )
        assert "ARG ORCEST_UID=1000" in text
        assert "ARG ORCEST_GID=1000" in text
        assert 'groupadd --gid "${ORCEST_GID}" orcest' in text
        assert 'useradd --uid "${ORCEST_UID}" --gid "${ORCEST_GID}"' in text

    for compose in (
        root / "docker-compose.yml",
        root / "src" / "orcest" / "fleet" / "deploy" / "docker-compose.yml",
    ):
        text = compose.read_text()
        assert "ORCEST_UID: ${ORCEST_UID:-1000}" in text
        assert "ORCEST_GID: ${ORCEST_GID:-1000}" in text


# ── C1: Redis AUTH password injected into worker .env (from fix/redis-security) ──


def test_clone_userdata_writes_redis_password_env():
    """C1: pool-manager-provisioned workers (render_clone_userdata) must get the
    Redis password in /opt/orcest/.env (build_redis_config reads it from env
    only, and systemd loads EnvironmentFile=-/opt/orcest/.env). Bug: today the
    clone path writes no .env at all."""
    data = yaml.safe_load(
        render_clone_userdata(
            redis_host="10.20.0.23",
            worker_id="orcest-worker-10002",
            key_prefix="orcest",
            redis_password="pool-pw",
        )
    )
    env_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/.env")
    assert "ORCEST_REDIS_PASSWORD='pool-pw'" in env_file["content"]
    # Secret-at-rest: the worker .env must be 0600 and orcest-owned + deferred.
    assert env_file["permissions"] == "0600"
    assert env_file["owner"] == "orcest:orcest"
    assert env_file.get("defer") is True
    # And it must NOT go into worker.yaml (config.py ignores yaml redis.password).
    worker_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/worker.yaml")
    assert "pool-pw" not in worker_file["content"]


def test_clone_userdata_no_env_when_password_absent():
    """C1: backward compat -- no redis_password means the clone writes no .env
    (preserves today's behaviour for unauthenticated/dev stacks)."""
    data = yaml.safe_load(
        render_clone_userdata(redis_host="10.0.0.1", worker_id="w-1", key_prefix="orcest")
    )
    assert all(f["path"] != "/opt/orcest/.env" for f in data["write_files"])


def test_clone_redis_password_is_single_quoted():
    """C1 hardening: the clone .env ORCEST_REDIS_PASSWORD line must be
    single-quoted (parity with generate_env_file) so the .env survives a value
    that systemd/shell would otherwise mangle."""
    data = yaml.safe_load(
        render_clone_userdata(
            redis_host="10.20.0.23",
            worker_id="orcest-worker-10002",
            key_prefix="orcest",
            redis_password="pool-pw",
        )
    )
    env_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/.env")
    assert "ORCEST_REDIS_PASSWORD='pool-pw'" in env_file["content"]


def test_clone_rejects_newline_in_redis_password():
    """C1 hardening: a newline (or hash) in the password must not be allowed to
    corrupt /opt/orcest/.env -- validated like generate_env_file."""
    with pytest.raises(ValueError, match="redis_password"):
        render_clone_userdata(
            redis_host="10.20.0.23",
            worker_id="w-1",
            key_prefix="orcest",
            redis_password="pw\nINJECTED=evil",
        )


def test_clone_rejects_single_quote_in_redis_password():
    """C1 hardening: single-quoted in .env, so a single quote breaks quoting."""
    with pytest.raises(ValueError, match="redis_password"):
        render_clone_userdata(
            redis_host="10.20.0.23",
            worker_id="w-1",
            key_prefix="orcest",
            redis_password="pw'injected",
        )
