"""Cloud-init user-data generation for orchestrator and worker VMs.

Generates cloud-init YAML documents that fully provision VMs at boot time —
no SSH provisioning step needed. The VM boots, installs all dependencies,
and configures the appropriate services.
"""

from __future__ import annotations

from datetime import datetime, timezone

import yaml

# ── Shared building blocks ──────────────────────────────────

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

    Installs (in order): Node, Docker, Claude CLI, gh, Supabase CLI,
    Playwright + Chromium, Deno, Bun, uv, wrangler.
    """
    return [
        # Node.js: NodeSource channel for the configured major version.
        f"curl -fsSL https://deb.nodesource.com/setup_{_NODE_MAJOR}.x | bash -",
        "apt-get install -y -qq nodejs",
        # Docker Engine (no compose plugin for workers).
        *_docker_install_runcmd(),
        # Claude CLI: floats to npm-latest; rebakes pull current.
        "npm install -g @anthropic-ai/claude-code",
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
        (
            "curl -fsSL https://astral.sh/uv/install.sh"
            ' | env UV_INSTALL_DIR="/usr/local/bin" sh'
        ),
        # wrangler: Cloudflare Workers CLI (transit-platform deploys here).
        "npm install -g wrangler",
    ]


def _worker_workspace_runcmd() -> list[str]:
    """Commands to set up the worker workspace directories."""
    return [
        "mkdir -p /opt/orcest/workspaces",
        "chown -R orcest:orcest /opt/orcest",
        "mkdir -p /home/orcest/.claude",
        "mkdir -p /home/orcest/.cache",
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
            # Create Python virtualenv and pre-install orcest
            "sudo -u orcest python3 -m venv /opt/orcest/venv",
            "sudo -u orcest /opt/orcest/venv/bin/pip install -q"
            " 'git+https://github.com/ThayneStudio/orcest.git'",
        ],
    )
    return _render(cloud_config)


# ── Warm-pool clone (lightweight cloud-init for cloned VMs) ──


def render_clone_userdata(
    *,
    redis_host: str,
    worker_id: str,
    key_prefix: str = "orcest",
) -> str:
    """Render cloud-init user-data for a warm-pool clone.

    This is a lightweight config — the template already has all tooling
    and orcest pre-installed. We just write the worker config and start
    the systemd service.

    Args:
        redis_host: Redis host (orchestrator VM IP).
        worker_id: Unique worker identifier (e.g. ``orcest-worker-10002``).
        key_prefix: Redis key prefix (shared across all projects).
    """
    redis_section: dict = {"host": redis_host, "port": 6379, "key_prefix": key_prefix}
    worker_yaml = yaml.dump(
        {
            "redis": redis_section,
            "worker_id": worker_id,
            "workspace_dir": "/opt/orcest/workspaces",
            "backend": "claude",
            "ephemeral": True,
        },
        default_flow_style=False,
    )

    systemd_unit = _systemd_unit(worker_id=worker_id)

    cloud_config = {
        "hostname": worker_id,
        "write_files": [
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
        ],
        "runcmd": [
            # Reinstall orcest from latest commit before starting the worker.
            # The template may have an older version baked in. --force-reinstall
            # is needed because the version number (0.1.0) doesn't change.
            "sudo -u orcest /opt/orcest/venv/bin/pip install -q --no-cache-dir"
            " --force-reinstall 'git+https://github.com/ThayneStudio/orcest.git'",
            "systemctl daemon-reload",
            "systemctl enable --now orcest-worker",
        ],
    }
    return _render(cloud_config)


# ── Worker (ephemeral, cloned from template) ────────────────


def render_worker_userdata(
    *,
    redis_host: str,
    key_prefix: str,
    worker_id: str,
    github_token: str,
    claude_oauth_token: str,
    repo: str,
) -> str:
    """Render a cloud-init user-data YAML for a worker VM.

    The generated user-data includes:
    - System package installation (Python, Node, Docker, Go, etc.)
    - Claude CLI and gh CLI installation
    - Orcest user creation and workspace setup
    - Worker config, env file, and systemd service
    - Claude Code authentication (headless OAuth token)
    - GitHub CLI authentication
    - Orcest package installation from PyPI or git

    Workers are ephemeral clones — SSH key injection is not needed.
    Credentials are injected via cloud-init write_files.

    Args:
        redis_host: Orchestrator Redis host (IP or hostname).
        key_prefix: Redis key prefix for namespace isolation.
        worker_id: Unique worker identifier (used in heartbeats).
        github_token: GitHub token for gh CLI and orcest.
        claude_oauth_token: Claude Code OAuth token from ``claude setup-token``.
        repo: GitHub repo in "owner/repo" format (for orcest install).
    """
    worker_yaml = yaml.dump(
        {
            "redis": {"host": redis_host, "port": 6379, "key_prefix": key_prefix},
            "worker_id": worker_id,
            "workspace_dir": "/opt/orcest/workspaces",
            "backend": "claude",
            "runner": {
                "type": "claude",
                "timeout": 1800,
                "max_retries": 3,
                "retry_backoff": 10,
            },
        },
        default_flow_style=False,
    )

    env_content = (
        f"GITHUB_TOKEN={github_token}\n"
        f"GH_TOKEN={github_token}\n"
        f"CLAUDE_CODE_OAUTH_TOKEN={claude_oauth_token}\n"
    )

    systemd_unit = _systemd_unit(worker_id=worker_id)

    claude_json = '{"hasCompletedOnboarding": true}'

    write_files = [
        # ``defer: true`` waits for cc_users_groups to create the orcest
        # user before chown/chmod — the config stage runs cc_write_files
        # before users exist, which logs ``Unknown user or group: "orcest"``.
        {
            "path": "/opt/orcest/worker.yaml",
            "owner": "orcest:orcest",
            "permissions": "0600",
            "content": worker_yaml,
            "defer": True,
        },
        {
            "path": "/opt/orcest/.env",
            "owner": "orcest:orcest",
            "permissions": "0600",
            "content": env_content,
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
            "content": claude_json,
            "defer": True,
        },
        {
            "path": "/opt/orcest/.gh-token",
            "owner": "orcest:orcest",
            "permissions": "0600",
            "content": f"{github_token}\n",
            "defer": True,
        },
    ]

    # Workers are ephemeral clones — SSH key injection is not needed
    # (credentials and config are injected via cloud-init write_files).
    cloud_config = _base_cloud_config(
        packages=list(_WORKER_PACKAGES),
        runcmd=[
            *_guest_agent_runcmd(),
            *_worker_workspace_runcmd(),
            *_worker_tooling_runcmd(),
            # Authenticate gh CLI for the orcest user using the pre-written token file
            "su - orcest -c 'gh auth login --with-token < /opt/orcest/.gh-token'",
            # Create Python virtualenv and install orcest
            "sudo -u orcest python3 -m venv /opt/orcest/venv",
            f"sudo -u orcest /opt/orcest/venv/bin/pip install 'git+https://github.com/{repo}.git'",
            # Enable and start the worker service
            "systemctl daemon-reload",
            "systemctl enable --now orcest-worker",
        ],
        write_files=write_files,
    )
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
MemoryMax=4G
Environment=PYTHONUNBUFFERED=1
PrivateTmp=yes
NoNewPrivileges=yes
RestrictSUIDSGID=yes
EnvironmentFile=-/opt/orcest/.env

[Install]
WantedBy=multi-user.target
"""
