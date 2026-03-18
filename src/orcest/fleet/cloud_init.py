"""Cloud-init user-data generation for orchestrator and worker VMs.

Generates cloud-init YAML documents that fully provision VMs at boot time —
no SSH provisioning step needed. The VM boots, installs all dependencies,
and configures the appropriate services.
"""

from __future__ import annotations

import yaml


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
    orcest_user: dict = {
        "name": "orcest",
        "system": True,
        "shell": "/bin/bash",
        "home": "/home/orcest",
        "groups": ["docker"],
    }

    # Inject SSH key via cloud-init's native ssh_authorized_keys directive
    # (avoids shell injection risk from runcmd echo)
    if ssh_public_key:
        orcest_user["ssh_authorized_keys"] = [ssh_public_key]

    cloud_config: dict = {
        "users": [
            "default",
            orcest_user,
        ],
        "package_update": True,
        "packages": [
            "qemu-guest-agent",
            "curl",
            "ca-certificates",
            "gnupg",
            "lsb-release",
            "git",
        ],
        "runcmd": _orchestrator_runcmd(),
    }

    # If an SSH public key is provided, inject it for the default (thayne) user too
    if ssh_public_key:
        cloud_config["ssh_authorized_keys"] = [ssh_public_key]

    return "#cloud-config\n" + yaml.dump(cloud_config, default_flow_style=False, sort_keys=False)


def _orchestrator_runcmd() -> list[str]:
    """Return the list of runcmd entries for orchestrator cloud-init."""
    return [
        # Start QEMU guest agent (installed via packages above) so Terraform
        # can read the VM's IP address via the Proxmox API.
        "systemctl enable qemu-guest-agent",
        "systemctl start qemu-guest-agent",
        # Create directory structure
        "mkdir -p /opt/orcest/projects",
        "chown -R orcest:orcest /opt/orcest",
        # Install Docker Engine
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
        "apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin",
        "usermod -aG docker orcest",
        # Open firewall port 6379 for Redis if ufw is active
        (
            "if command -v ufw >/dev/null 2>&1"
            " && sudo ufw status 2>/dev/null | grep -q 'Status: active';"
            " then sudo ufw allow 6379/tcp; fi"
        ),
    ]


def render_worker_userdata(
    *,
    redis_host: str,
    key_prefix: str,
    worker_id: str,
    github_token: str,
    claude_oauth_token: str,
    repo: str,
    ssh_public_key: str = "",
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

    Args:
        redis_host: Orchestrator Redis host (IP or hostname).
        key_prefix: Redis key prefix for namespace isolation.
        worker_id: Unique worker identifier (used in heartbeats).
        github_token: GitHub token for gh CLI and orcest.
        claude_oauth_token: Claude Code OAuth token from ``claude setup-token``.
        repo: GitHub repo in "owner/repo" format (for orcest install).
        ssh_public_key: Optional SSH public key for the ``thayne`` user.
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

    systemd_unit = _systemd_unit()

    claude_json = '{"hasCompletedOnboarding": true}'

    # Build the cloud-config document
    cloud_config: dict = {
        "users": [
            "default",
            {
                "name": "orcest",
                "system": True,
                "shell": "/bin/bash",
                "home": "/home/orcest",
            },
        ],
        "package_update": True,
        "packages": [
            "qemu-guest-agent",
            "python3",
            "python3-pip",
            "python3-venv",
            "git",
            "curl",
            "ca-certificates",
            "gnupg",
            "lsb-release",
            "golang-go",
            "unzip",
        ],
        "write_files": [
            {
                "path": "/opt/orcest/worker.yaml",
                "owner": "orcest:orcest",
                "permissions": "0644",
                "content": worker_yaml,
            },
            {
                "path": "/opt/orcest/.env",
                "owner": "orcest:orcest",
                "permissions": "0600",
                "content": env_content,
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
            },
            {
                "path": "/opt/orcest/.gh-token",
                "owner": "orcest:orcest",
                "permissions": "0600",
                "content": f"{github_token}\n",
            },
        ],
        "runcmd": _runcmd(repo=repo),
    }

    # Render as YAML with the #cloud-config header
    return "#cloud-config\n" + yaml.dump(cloud_config, default_flow_style=False, sort_keys=False)


def _systemd_unit() -> str:
    """Return the orcest-worker systemd unit file content."""
    return """\
[Unit]
Description=Orcest Worker
After=network.target
StartLimitBurst=5
StartLimitIntervalSec=300

[Service]
Type=simple
User=orcest
WorkingDirectory=/opt/orcest
ExecStart=/opt/orcest/venv/bin/orcest work --id %H --config /opt/orcest/worker.yaml
Restart=on-failure
RestartSec=10
TimeoutStopSec=120
MemoryMax=4G
Environment=PYTHONUNBUFFERED=1
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/opt/orcest/workspaces /home/orcest/.claude /home/orcest/.cache
PrivateTmp=yes
NoNewPrivileges=yes
RestrictSUIDSGID=yes
EnvironmentFile=/opt/orcest/.env

[Install]
WantedBy=multi-user.target
"""


def _runcmd(repo: str) -> list[str]:
    """Return the list of runcmd entries for cloud-init."""
    return [
        # Start QEMU guest agent so Terraform can read the VM's IP
        "systemctl enable qemu-guest-agent",
        "systemctl start qemu-guest-agent",
        # Create workspace directories
        "mkdir -p /opt/orcest/workspaces",
        "chown -R orcest:orcest /opt/orcest",
        "mkdir -p /home/orcest/.claude",
        "mkdir -p /home/orcest/.cache",
        "chown -R orcest:orcest /home/orcest",
        # Install Node.js 20.x
        "curl -fsSL https://deb.nodesource.com/setup_20.x | bash -",
        "apt-get install -y -qq nodejs",
        # Install Docker Engine
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
        "apt-get install -y -qq docker-ce docker-ce-cli containerd.io",
        "usermod -aG docker orcest",
        # Install Claude CLI
        "npm install -g @anthropic-ai/claude-code",
        # Install gh CLI
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
        # Install Supabase CLI (npm global install no longer supported)
        "ARCH=$(dpkg --print-architecture)"
        " && SUPA_VER=$(curl -fsSL https://api.github.com/repos/supabase/cli/releases/latest"
        ' | grep -oP \'"tag_name":\\s*"v\\K[^"]+\') '
        '&& curl -fsSL "https://github.com/supabase/cli/releases/download/v${SUPA_VER}'
        '/supabase_${SUPA_VER}_linux_${ARCH}.deb" -o /tmp/supabase.deb'
        " && dpkg -i /tmp/supabase.deb && rm -f /tmp/supabase.deb",
        # Install Playwright browsers
        "npx playwright install --with-deps chromium",
        # Authenticate gh CLI for the orcest user using the pre-written token file
        "su - orcest -c 'gh auth login --with-token < /opt/orcest/.gh-token'",
        # Create Python virtualenv and install orcest
        "sudo -u orcest python3 -m venv /opt/orcest/venv",
        (f"sudo -u orcest /opt/orcest/venv/bin/pip install 'git+https://github.com/{repo}.git'"),
        # Enable and start the worker service
        "systemctl daemon-reload",
        "systemctl enable --now orcest-worker",
    ]
