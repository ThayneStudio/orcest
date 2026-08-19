"""Cloud-init user-data generation for orchestrator and worker VMs.

Generates cloud-init YAML documents that fully provision VMs at boot time —
no SSH provisioning step needed. The VM boots, installs all dependencies,
and configures the appropriate services.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

import yaml

from orcest.fleet.config import normalize_worker_runner_for_backend

# C1c: interactive (PTY) Claude workers (ClaudeInteractiveRunner) never
# route through _BaseCliRunner/tracker_factory -- they have no
# LivenessTracker/ladder coverage at all (see worker/claude_interactive_runner.py).
# Pin their wall-clock RunnerConfig.timeout to this pre-branch default
# rather than letting them silently inherit the raised 21600s ceiling,
# which has no ladder backstopping it for this runner mode. Headless
# profiles are unaffected and keep the new (raised) RunnerConfig default.
_INTERACTIVE_RUNNER_TIMEOUT_SECONDS = 5400

# ── Shared building blocks ──────────────────────────────────


def _validate_env_value(value: str, name: str) -> None:
    """Raise ValueError if *value* is unsafe to single-quote in an .env file.

    Parity with ``orchestrator.generate_env_file._validate_env_value`` (C1):
    the worker .env writes ``NAME='<value>'``, so a single quote breaks quoting,
    and a newline / carriage return / null byte could smuggle an extra line
    (e.g. inject another env var) into the file.
    """
    if any(c in value for c in ("\n", "\r", "\0")):
        raise ValueError(f"{name} must not contain newlines or null bytes")
    if "'" in value:
        raise ValueError(f"{name} must not contain single quotes")


_BASE_PACKAGES: list[str] = [
    "qemu-guest-agent",
    "curl",
    "ca-certificates",
    "gnupg",
    "lsb-release",
    "git",
]

_WORKER_PACKAGES: list[str] = _BASE_PACKAGES + [
    "python3",
    "python3-pip",
    "python3-venv",
    "golang-go",
    "unzip",
    # Quality-of-life utilities for shell-driven workflows.
    # ripgrep + fd-find: Claude's Grep tool uses ripgrep when available, and
    # fd is much faster than find for the agent's file-scanning passes.
    "ripgrep",
    "fd-find",
    # jq: JSON processing in shell pipelines (gh CLI output, API responses).
    "jq",
    # redis-tools: redis-cli for ad-hoc queue inspection from a worker shell.
    "redis-tools",
    # postgresql-client: psql for debugging Supabase connections.
    "postgresql-client",
]


# Major versions of tools baked into the worker template.
# Bumping these requires a template rebake (`orcest fleet create-template`).
# Patch versions float — they're picked up automatically on each rebake.
_NODE_MAJOR = "22"  # 22.x LTS, supported through April 2027 (replaces EOL 20.x)
_PLAYWRIGHT_MAJOR = "1"  # 1.x line — npx resolves latest 1.x at install time

# Hard-pinned tools (replace dynamic fetches / opaque defaults).
# Bump when rebaking; verify against https://github.com/supabase/cli/releases
_SUPABASE_VERSION = "2.95.4"

# Grok (xAI Grok Build) CLI — beta; the streaming-json event schema can shift
# between releases, so pin it and re-validate the GrokRunner parsers
# (tests/worker/test_grok_runner.py) on bump.
_GROK_VERSION = "0.1.216"
# SHA-256 of the exact linux-x86_64 Grok binary for _GROK_VERSION. The worker
# cloud image is amd64, so download the versioned artifact directly instead of
# executing xAI's mutable bootstrap as root. The x.ai primary and the official
# installer's GCS fallback returned identical 116,449,128-byte objects with
# this digest on 2026-08-16 (GCS generation 1779443477078136).
#
# FAIL CLOSED: if this is empty or mismatched, the downloaded binary is not
# installed or executed. Grok degrades gracefully when absent — grok-backed
# tasks early-reject with a rebake instruction, never a stuck task or secret
# leak.
#
# Re-pin deliberately with _GROK_VERSION after validating GrokRunner fixtures:
#   curl -fsSL https://x.ai/cli/grok-VERSION-linux-x86_64 | sha256sum
_GROK_BINARY_SHA256 = "01044edfadcddebdb1197195e692f351ad87569e079324b7feac6a08d692d8af"

# OpenAI Codex CLI — published as an npm package (@openai/codex). Pinned so
# template rebakes are reproducible; the experimental JSON event vocabulary
# parsed by CodexRunner (tests/worker/test_codex_runner.py) is tied to this
# version, so re-validate fixtures + parser on bump.
_CODEX_VERSION = "0.131.0"

# Anthropic Claude Code CLI — published as an npm package. Pinned for the same
# reason as the others, plus one specific to this provider: the interactive
# (PTY) runner drives Claude's terminal UI by recognising its setup dialogs, so
# a CLI upgrade that adds, reorders, or rewords a dialog silently changes a
# contract that runner depends on. An unpinned install is what let that happen
# on 2026-08-14, when a floating upgrade began pasting the task prompt into the
# MCP consent menu and burned ~27% of clauder tasks on the wall-clock timeout
# (see claude_interactive_runner's dialog handling + its regression tests).
# On bump: re-run those tests and smoke a real interactive task before shipping.
_CLAUDE_VERSION = "2.1.235"

# Provider CLIs that ``_worker_tooling_runcmd`` installs and which a worker
# MUST be able to exec (as the non-root ``orcest`` user) for any provider task
# to run. cloud-init's runcmd has no cross-entry ``set -e``: a failed install
# in the middle of the list still lets cloud-final exit ``status: done``, so a
# half-baked template can otherwise ship missing one of these. The bake path
# (``orcest fleet create-template`` / ``rebake``) runs a post-bake smoke-check
# of this exact list on PATH *before* the irreversible convert-to-template /
# pool-pointer swap — keep this in sync with the install commands above.
REQUIRED_PROVIDER_BINARIES: tuple[str, ...] = ("claude", "grok", "codex")
# systemd ReadWritePaths for the worker unit (M3). Every dir the worker +
# providers write at RUNTIME under ProtectHome=read-only/ProtectSystem=strict,
# else those writes EROFS. Single line (a systemd directive cannot wrap); kept
# as a constant so the unit f-strings stay under the line-length limit.
_WORKER_READ_WRITE_PATHS = (
    "/opt/orcest /home/orcest/.claude /home/orcest/.cache /home/orcest/.codex /home/orcest/.grok"  # noqa: E501
)


def _template_versions_write_file() -> dict:
    """Build the cloud-init write_files entry for ``/etc/orcest/template.versions``.

    System-level audit metadata recording which baked-in tool versions
    were on the template at rebake time. Owner ``root:root`` so it
    doesn't depend on the orcest user existing — this entry can run in
    the cc_write_files config stage without ``defer``.

    The ``bumped_at`` timestamp is captured at render time, which for the
    template-bake flow is when ``orcest fleet rebake`` (or
    ``create-template``) builds the userdata document. Patch versions
    floating in NodeSource / npm / Playwright registries are NOT recorded
    — only the major-version pins under our control.
    """
    bumped_at = datetime.now(timezone.utc).isoformat()
    content = (
        f"node_major={_NODE_MAJOR}\n"
        f"playwright_major={_PLAYWRIGHT_MAJOR}\n"
        f"supabase_version={_SUPABASE_VERSION}\n"
        f"claude_version={_CLAUDE_VERSION}\n"
        f"grok_version={_GROK_VERSION}\n"
        f"codex_version={_CODEX_VERSION}\n"
        f"bumped_at={bumped_at}\n"
    )
    return {
        "path": "/etc/orcest/template.versions",
        "owner": "root:root",
        "permissions": "0644",
        "content": content,
    }


def _orcest_user(ssh_public_key: str = "") -> dict:
    """Build the cloud-init user entry for the orcest user."""
    user: dict = {
        "name": "orcest",
        "shell": "/bin/bash",
        "groups": ["docker", "sudo"],
        "sudo": "ALL=(ALL) NOPASSWD:ALL",
        "lock_passwd": True,
    }
    if ssh_public_key:
        user["ssh_authorized_keys"] = [ssh_public_key]
    return user


def _guest_agent_runcmd() -> list[str]:
    """Commands to enable and start the QEMU guest agent."""
    return [
        "systemctl enable qemu-guest-agent",
        "systemctl start qemu-guest-agent",
    ]


def _docker_install_runcmd(*, include_compose_plugin: bool = False) -> list[str]:
    """Commands to install Docker Engine from the official repository."""
    pkgs = "docker-ce docker-ce-cli containerd.io"
    if include_compose_plugin:
        pkgs += " docker-compose-plugin"
    return [
        (
            "curl -fsSL https://download.docker.com/linux/ubuntu/gpg"
            " | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg"
        ),
        (
            'echo "deb [arch=$(dpkg --print-architecture)'
            " signed-by=/usr/share/keyrings/docker-archive-keyring.gpg]"
            " https://download.docker.com/linux/ubuntu"
            ' $(lsb_release -cs) stable"'
            " | tee /etc/apt/sources.list.d/docker.list > /dev/null"
        ),
        "apt-get update -qq",
        f"apt-get install -y -qq {pkgs}",
        "usermod -aG docker orcest",
    ]


def _worker_tooling_runcmd() -> list[str]:
    """Commands to install worker tooling.

    Installs (in order): Node, Docker, Claude CLI, Grok CLI, Codex CLI, gh,
    Supabase CLI, Playwright + Chromium, Deno, Bun, uv, wrangler.
    """
    # Effective Grok binary digest: an optional bake-time override falls back
    # to the pinned exact-version artifact. Empty => FAIL CLOSED (skip).
    grok_sha = os.environ.get("ORCEST_GROK_BINARY_SHA256", _GROK_BINARY_SHA256)
    if grok_sha and re.fullmatch(r"[0-9a-fA-F]{64}", grok_sha) is None:
        raise ValueError(
            "ORCEST_GROK_BINARY_SHA256 must be empty or exactly 64 hexadecimal characters"
        )
    grok_sha = grok_sha.lower()
    return [
        # Node.js: NodeSource channel for the configured major version.
        f"curl -fsSL https://deb.nodesource.com/setup_{_NODE_MAJOR}.x | bash -",
        "apt-get install -y -qq nodejs",
        # Docker Engine (no compose plugin for workers).
        *_docker_install_runcmd(),
        # Claude CLI: floats to npm-latest; rebakes pull current.
        f"npm install -g @anthropic-ai/claude-code@{_CLAUDE_VERSION}",
        # Grok CLI (xAI Grok Build): fetch the exact versioned, self-contained
        # amd64 binary. Auth is injected per-task to ~/.grok/auth.json by
        # GrokRunner (Path B), never baked.
        (
            "curl -fsSL --connect-timeout 30 --max-time 300"
            f" https://x.ai/cli/grok-{_GROK_VERSION}-linux-x86_64 -o /tmp/grok"
        ),
        # Integrity gate + install in ONE entry because cloud-init has no
        # cross-entry `set -e`. The binary is installed ONLY when a non-empty
        # digest matches; unlike the old bootstrap path, no unverified binary
        # is executed before this check.
        (
            f'if [ -n "{grok_sha}" ] && '
            f'echo "{grok_sha}  /tmp/grok" | sha256sum -c -; then '
            "install -m 0755 -o root -g root /tmp/grok /usr/local/bin/grok; "
            'else rm -f /usr/local/bin/grok; echo "grok binary SHA-256 unset/mismatch '
            f"— SKIPPING grok install (set ORCEST_GROK_BINARY_SHA256 to enable); "
            'grok tasks will be rejected"; fi'
        ),
        "rm -f /tmp/grok",
        # Surface a missing/broken binary in the cloud-init log. Run as the
        # orcest user (the worker's runtime user) so this also catches the
        # exec-permission regression above — not just root visibility.
        "sudo -u orcest -H bash -lc 'command -v grok && grok --version'",
        # Codex CLI (OpenAI codex-cli): published as an npm package
        # ``@openai/codex`` — version-pinned for reproducible rebakes. The
        # CodexRunner JSON event parser (tests/worker/test_codex_runner.py)
        # is tied to this version; re-validate fixtures on bump.
        # Auth is per-task: an OAuth blob written to ~/.codex/auth.json by
        # CodexRunner.prepare_credential (Path B), never baked into the
        # template. No silent-failure swallow (no `|| true`): a bad install
        # must surface in the cloud-init log rather than ship a broken image.
        f"npm install -g @openai/codex@{_CODEX_VERSION}",
        # Defense in depth: ``npm i -g`` symlinks /usr/local/bin/codex into
        # /usr/local/lib/node_modules — world-readable by default, but in
        # case a future Node packaging quirk drops perms (mirroring the grok
        # /root/.local/bin exec-perms regression), verify as the orcest
        # worker user, not root, so any such regression is loud at bake.
        "sudo -u orcest -H bash -lc 'command -v codex && codex --version'",
        # gh CLI: GitHub apt repo, stable channel.
        (
            "curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg"
            " | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg 2>/dev/null"
        ),
        (
            'echo "deb [arch=$(dpkg --print-architecture)'
            " signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg]"
            ' https://cli.github.com/packages stable main"'
            " | tee /etc/apt/sources.list.d/github-cli.list > /dev/null"
        ),
        "apt-get update -qq",
        "apt-get install -y -qq gh",
        # Supabase CLI: hard-pinned. Static URL = reproducible across rebakes.
        (
            "ARCH=$(dpkg --print-architecture)"
            f' && curl -fsSL "https://github.com/supabase/cli/releases/download/v{_SUPABASE_VERSION}'
            f'/supabase_{_SUPABASE_VERSION}_linux_${{ARCH}}.deb" -o /tmp/supabase.deb'
            " && dpkg -i /tmp/supabase.deb && rm -f /tmp/supabase.deb"
        ),
        # Playwright + Chromium browser binaries.
        f"npx -y playwright@{_PLAYWRIGHT_MAJOR} install --with-deps chromium",
        # Deno: Supabase Edge Functions runtime. System-wide install so all
        # users (including the orcest service) see the binary.
        "curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh -s -- -y",
        # Bun: fast Node-compatible runtime + package manager.
        "npm install -g bun",
        # uv: Rust-based fast Python package manager (10-50x faster than pip).
        ('curl -fsSL https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="/usr/local/bin" sh'),
        # wrangler: Cloudflare Workers CLI (transit-platform deploys here).
        "npm install -g wrangler",
    ]


def _worker_workspace_runcmd() -> list[str]:
    """Commands to set up the worker workspace directories.

    Every ``/home/orcest`` directory listed in the worker unit's
    ``ReadWritePaths`` (_WORKER_READ_WRITE_PATHS) MUST be created here: under
    ``ProtectHome=read-only`` systemd (>=249) refuses to start a unit whose
    ReadWritePaths target is missing, so an un-created .codex/.grok dir would
    make every worker fail ``systemctl enable --now orcest-worker``.
    """
    return [
        "mkdir -p /opt/orcest/workspaces",
        "chown -R orcest:orcest /opt/orcest",
        # ReadWritePaths home targets — keep in sync with _WORKER_READ_WRITE_PATHS.
        "mkdir -p /home/orcest/.claude",  # Claude CLI state
        "mkdir -p /home/orcest/.cache",  # generic caches
        "mkdir -p /home/orcest/.codex",  # CodexRunner writes auth.json
        "mkdir -p /home/orcest/.grok",  # GrokRunner writes auth.json
        "chown -R orcest:orcest /home/orcest",
    ]


def _base_cloud_config(
    *,
    ssh_public_key: str = "",
    packages: list[str],
    runcmd: list[str],
    write_files: list[dict] | None = None,
) -> dict:
    """Build the base cloud-config dict with common structure."""
    config: dict = {
        "users": ["default", _orcest_user(ssh_public_key)],
        "package_update": True,
        "packages": packages,
    }
    if write_files:
        config["write_files"] = write_files
    config["runcmd"] = runcmd
    if ssh_public_key:
        config["ssh_authorized_keys"] = [ssh_public_key]
    return config


def _render(cloud_config: dict) -> str:
    """Render a cloud-config dict to a YAML string with the #cloud-config header."""
    return "#cloud-config\n" + yaml.dump(cloud_config, default_flow_style=False, sort_keys=False)


# ── Orchestrator ────────────────────────────────────────────


def render_orchestrator_userdata(
    *,
    ssh_public_key: str = "",
) -> str:
    """Render cloud-init user-data for the orchestrator VM.

    Installs Docker Engine + Compose, creates the orcest user,
    and sets up the /opt/orcest directory structure. Does NOT start
    any compose stacks — that happens per-project via fleet onboard.

    Args:
        ssh_public_key: Optional SSH public key for the ``thayne`` user.
    """
    cloud_config = _base_cloud_config(
        ssh_public_key=ssh_public_key,
        packages=list(_BASE_PACKAGES),
        runcmd=[
            *_guest_agent_runcmd(),
            "mkdir -p /opt/orcest/projects",
            "chown -R orcest:orcest /opt/orcest",
            *_docker_install_runcmd(include_compose_plugin=True),
            # Open firewall port 6379 for Redis if ufw is active
            (
                "if command -v ufw >/dev/null 2>&1"
                " && sudo ufw status 2>/dev/null | grep -q 'Status: active';"
                " then sudo ufw allow 6379/tcp; fi"
            ),
        ],
    )
    return _render(cloud_config)


# ── Worker template ─────────────────────────────────────────


def render_template_userdata(
    *,
    ssh_public_key: str = "",
) -> str:
    """Render cloud-init user-data for a worker VM *template*.

    Installs all worker tooling (Python, Node, Docker, Claude CLI, gh CLI,
    etc.) but does NOT configure any worker service, Redis connection, or
    credentials.  Those are injected at clone time via cloud-init
    customisation on each ephemeral worker.

    Args:
        ssh_public_key: Optional SSH public key for the ``thayne`` user.
    """
    # Linked clones share the template's disk, including /etc/machine-id.
    # Ubuntu's default DHCP identifier (DUID) is derived from machine-id,
    # so all clones get the same DHCP lease.  Write a netplan config that
    # uses MAC-based DHCP identifiers — Proxmox assigns a unique MAC to
    # each clone.
    netplan_content = yaml.dump(
        {
            "network": {
                "version": 2,
                "ethernets": {
                    "eth0": {
                        "dhcp4": True,
                        "dhcp-identifier": "mac",
                    },
                },
            },
        },
        default_flow_style=False,
    )

    cloud_config = _base_cloud_config(
        ssh_public_key=ssh_public_key,
        packages=list(_WORKER_PACKAGES),
        write_files=[
            {
                "path": "/etc/netplan/99-orcest.yaml",
                "permissions": "0644",
                "content": netplan_content,
            },
            {
                # ``defer: true`` runs this write at the cc_scripts_user
                # stage (after cc_users_groups), so the ``orcest`` user
                # exists by the time cloud-init resolves ``orcest:orcest``.
                # Without defer, cc_write_files runs in the config stage
                # and logs ``Unknown user or group: "orcest"``.
                "path": "/home/orcest/.claude.json",
                "owner": "orcest:orcest",
                "permissions": "0644",
                "content": '{"hasCompletedOnboarding": true}',
                "defer": True,
            },
            _template_versions_write_file(),
        ],
        runcmd=[
            *_guest_agent_runcmd(),
            *_worker_workspace_runcmd(),
            *_worker_tooling_runcmd(),
            # Create Python virtualenv. The fleet CLI installs the active
            # source tarball into this venv after cloud-init finishes and
            # before converting the VM to a template.
            "sudo -u orcest python3 -m venv /opt/orcest/venv",
        ],
    )
    return _render(cloud_config)


# ── Warm-pool clone (lightweight cloud-init for cloned VMs) ──


def render_clone_userdata(
    *,
    redis_host: str,
    worker_id: str,
    key_prefix: str = "orcest",
    redis_password: str = "",
    worker_backend: str = "claude",
    worker_runner_type: str = "claude",
    worker_runner_mode: str = "",
    watchdog_enabled: bool = True,
) -> str:
    """Render cloud-init user-data for a warm-pool clone.

    This is a lightweight config — the template already has all tooling
    and orcest pre-installed. We just write the worker config and start
    the systemd service.

    Args:
        redis_host: Redis host (orchestrator VM IP).
        worker_id: Unique worker identifier (e.g. ``orcest-worker-10002``).
        key_prefix: Redis key prefix (shared across all projects).
        redis_password: Redis AUTH password. When set, written to
            ``/opt/orcest/.env`` (0600) so the worker can authenticate to the
            password-protected Redis. ``build_redis_config`` reads it from the
            env var ONLY (never worker.yaml), and the systemd unit loads
            ``/opt/orcest/.env`` via ``EnvironmentFile=-``.
        worker_backend: Task backend stream this clone should consume.
        worker_runner_type: Runner implementation type to instantiate.
        worker_runner_mode: Optional runner mode; ``interactive`` selects the
            PTY Claude runner (the default for Claude backends, matching the
            pre-``worker_runner_mode`` deployments), while ``headless``
            explicitly opts a ``claude`` backend into the legacy ``claude -p``
            runner.
        watchdog_enabled: Fleet-level activity-watchdog toggle (C1a,
            ``PoolConfig.watchdog_enabled``), rendered as
            ``runner.watchdog.enabled`` in the generated ``worker.yaml``.
            This is clone-time cloud-init data (not baked into the template
            image), so toggling it takes effect for the next clone with no
            rebake needed.
    """
    normalized_backend, normalized_runner_type, normalized_runner_mode = (
        normalize_worker_runner_for_backend(
            worker_backend,
            worker_runner_type,
            worker_runner_mode,
        )
    )
    redis_section: dict = {"host": redis_host, "port": 6379, "key_prefix": key_prefix}
    runner_section: dict[str, Any] = {"type": normalized_runner_type}
    if normalized_runner_mode == "interactive":
        runner_section["extra"] = {"mode": normalized_runner_mode}
        # C1c: no ladder coverage for this runner mode -- pin the
        # pre-branch wall-clock ceiling rather than inheriting the raised
        # default (see _INTERACTIVE_RUNNER_TIMEOUT_SECONDS above).
        runner_section["timeout"] = _INTERACTIVE_RUNNER_TIMEOUT_SECONDS
    runner_section["watchdog"] = {"enabled": watchdog_enabled}
    worker_yaml = yaml.dump(
        {
            "redis": redis_section,
            "worker_id": worker_id,
            "workspace_dir": "/opt/orcest/workspaces",
            "backend": normalized_backend,
            "runner": runner_section,
            "ephemeral": True,
            "pool_managed": True,
        },
        default_flow_style=False,
    )

    systemd_unit = _systemd_unit(worker_id=worker_id)

    clone_write_files: list[dict[str, Any]] = [
        {
            # ``defer`` ensures the orcest user exists before the file
            # is chowned to it (cc_write_files otherwise runs in the
            # config stage, before cc_users_groups).
            "path": "/opt/orcest/worker.yaml",
            "owner": "orcest:orcest",
            "permissions": "0600",
            "content": worker_yaml,
            "defer": True,
        },
        {
            "path": "/etc/systemd/system/orcest-worker.service",
            "permissions": "0644",
            "content": systemd_unit,
        },
        {
            "path": "/home/orcest/.claude.json",
            "owner": "orcest:orcest",
            "permissions": "0644",
            "content": '{"hasCompletedOnboarding": true}',
            "defer": True,
        },
    ]
    if redis_password:
        # C1: Redis AUTH for the worker. build_redis_config reads
        # ORCEST_REDIS_PASSWORD from the env ONLY (never worker.yaml), and the
        # systemd unit loads /opt/orcest/.env via EnvironmentFile=-. 0600 +
        # orcest-owned + defer. Single-quoted + validated (parity with
        # generate_env_file) so a newline/hash/quote can't corrupt the .env.
        _validate_env_value(redis_password, "redis_password")
        clone_write_files.append(
            {
                "path": "/opt/orcest/.env",
                "owner": "orcest:orcest",
                "permissions": "0600",
                "content": f"ORCEST_REDIS_PASSWORD='{redis_password}'\n",
                "defer": True,
            }
        )

    cloud_config = {
        "hostname": worker_id,
        "write_files": clone_write_files,
        "runcmd": [
            # Orcest itself is installed during template bake from the same
            # source tarball used for orchestrator deploys. Clones must not
            # fetch GitHub at boot; that can silently run a different revision
            # from the deployed orchestrator/pool-manager.
            # Ensure every /home/orcest ReadWritePaths target exists BEFORE the
            # unit is enabled. A warm template baked before .codex/.grok were
            # added would otherwise be missing them, and under
            # ProtectHome=read-only systemd refuses to start a unit whose
            # ReadWritePaths target is absent -> the worker never boots. Keep in
            # sync with _WORKER_READ_WRITE_PATHS / the systemd unit.
            "mkdir -p /home/orcest/.claude /home/orcest/.cache"
            " /home/orcest/.codex /home/orcest/.grok",
            "chown -R orcest:orcest /home/orcest",
            "systemctl daemon-reload",
            "systemctl enable --now orcest-worker",
        ],
    }
    return _render(cloud_config)


def _systemd_unit(worker_id: str = "%H") -> str:
    """Return the orcest-worker systemd unit file content.

    Args:
        worker_id: Worker identifier for ``--id``.  Pool clones pass
            their VM-based ID (e.g. ``orcest-worker-9001``); the default
            ``%H`` (hostname) is kept for legacy non-pool workers.
    """
    # StartLimitBurst=10 / StartLimitIntervalSec=300 lets systemd retry the
    # worker up to 10 times over 5 minutes before giving up.  Combined with
    # the in-process ~60 s Redis-connect retry in worker/loop.py, a brief
    # Redis container restart during ``orcest fleet update`` is well within
    # the recovery window without manual ``systemctl reset-failed`` per VM.
    #
    # Hardening (ProtectSystem=strict + ProtectHome=read-only + …) is the
    # reconciled superset of this generated unit and the static
    # provision/systemd/orcest-worker.service (M3). ReadWritePaths MUST list
    # every dir the worker + providers write at RUNTIME under the unit sandbox,
    # or ProtectHome/ProtectSystem turn those writes into EROFS:
    #   /opt/orcest          — clone-written worker.yaml + venv reinstall
    #   /home/orcest/.claude — Claude CLI state
    #   /home/orcest/.cache  — generic caches
    #   /home/orcest/.codex  — CodexRunner writes auth.json (codex_runner.py)
    #   /home/orcest/.grok   — GrokRunner writes auth.json (grok_runner.py)
    # Note: gh cannot refresh its token under ProtectHome=read-only, which is
    # why setup-worker.sh mandates a non-expiring PAT — accepted behaviour, do
    # NOT add ~/.config here to "fix" it.
    return f"""\
[Unit]
Description=Orcest Worker
After=network.target
StartLimitBurst=10
StartLimitIntervalSec=300

[Service]
Type=simple
User=orcest
WorkingDirectory=/opt/orcest
ExecStart=/opt/orcest/venv/bin/orcest work --id {worker_id} --config /opt/orcest/worker.yaml
Restart=on-failure
RestartSec=10
TimeoutStopSec=120
MemoryMax=8G
Environment=PYTHONUNBUFFERED=1
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths={_WORKER_READ_WRITE_PATHS}
PrivateTmp=yes
NoNewPrivileges=yes
RestrictSUIDSGID=yes
EnvironmentFile=-/opt/orcest/.env

[Install]
WantedBy=multi-user.target
"""
