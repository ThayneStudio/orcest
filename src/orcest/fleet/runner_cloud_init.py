"""Cloud-init user-data generation for self-hosted GitHub Actions runner VMs.

Generates a cloud-init YAML document that provisions a GitHub Actions
runner at boot — installs the runner agent, fat toolchain, and registers
with GitHub. Runners are cattle: destroy and recreate to update.
"""

from __future__ import annotations

import shlex

import yaml


def render_runner_userdata(
    *,
    org_url: str,
    runner_token: str,
    runner_name: str = "",
    runner_labels: str = "self-hosted,linux",
) -> str:
    """Render cloud-init user-data for a self-hosted GitHub Actions runner VM.

    Args:
        org_url: GitHub organization URL (e.g. https://github.com/ThayneStudio).
        runner_token: GitHub Actions runner registration token.
        runner_name: Runner name (default: hostname).
        runner_labels: Comma-separated labels for the runner.
    """
    cloud_config: dict = {
        "users": [
            "default",
            {
                "name": "runner",
                "system": True,
                "shell": "/bin/bash",
                "home": "/home/runner",
                "groups": ["docker"],
            },
        ],
        "package_update": True,
        "packages": [
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
            "jq",
            "libatomic1",
        ],
        "runcmd": _runcmd(
            org_url=org_url,
            runner_token=runner_token,
            runner_name=runner_name,
            runner_labels=runner_labels,
        ),
    }

    return "#cloud-config\n" + yaml.dump(cloud_config, default_flow_style=False, sort_keys=False)


def _runcmd(
    org_url: str,
    runner_token: str,
    runner_name: str,
    runner_labels: str,
) -> list[str]:
    """Return runcmd entries for runner provisioning."""
    # Omit --name when not specified; config.sh defaults to the machine hostname.
    name_flag = f"--name {shlex.quote(runner_name)}" if runner_name else ""

    return [
        # Create runner user home
        "mkdir -p /home/runner",
        "chown runner:runner /home/runner",
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
        "usermod -aG docker runner",
        # Install Playwright browsers
        "npx playwright install --with-deps chromium",
        # Install Supabase CLI (npm global install no longer supported)
        "ARCH=$(dpkg --print-architecture)"
        " && SUPA_VER=$(curl -fsSL https://api.github.com/repos/supabase/cli/releases/latest"
        " | grep -oP '\"tag_name\":\\s*\"v\\K[^\"]+') "
        '&& curl -fsSL "https://github.com/supabase/cli/releases/download/v${SUPA_VER}'
        '/supabase_${SUPA_VER}_linux_${ARCH}.deb" -o /tmp/supabase.deb'
        " && dpkg -i /tmp/supabase.deb && rm -f /tmp/supabase.deb",
        # Download and configure GitHub Actions runner (version determined at boot time)
        "mkdir -p /opt/actions-runner",
        "chown runner:runner /opt/actions-runner",
        (
            "su - runner -c '"
            "cd /opt/actions-runner && "
            "RUNNER_VER=$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest"
            ' | grep \'"tag_name"\' | sed \'s/.*"v\\([^"]*\\)".*/\\1/\') && '
            "curl -o actions-runner-linux-x64.tar.gz -L "
            "https://github.com/actions/runner/releases/download/v${RUNNER_VER}/"
            "actions-runner-linux-x64-${RUNNER_VER}.tar.gz && "
            "tar xzf actions-runner-linux-x64.tar.gz && "
            "rm actions-runner-linux-x64.tar.gz"
            "'"
        ),
        # Configure the runner (non-interactive).
        # shlex.quote ensures values containing spaces or metacharacters are
        # safely quoted within the outer single-quoted shell argument.
        (
            f"su - runner -c 'cd /opt/actions-runner && "
            f"./config.sh --url {shlex.quote(org_url)} --token {shlex.quote(runner_token)} "
            f"{name_flag} --labels {shlex.quote(runner_labels)} "
            f"--unattended --replace'"
        ),
        # Install as systemd service
        "cd /opt/actions-runner && ./svc.sh install runner",
        "cd /opt/actions-runner && ./svc.sh start",
    ]
