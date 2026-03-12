"""Deploy and manage per-project orchestrator stacks on the orchestrator VM.

Each project gets its own directory under ``/opt/orcest/projects/{name}/``
with a docker-compose.yml (Redis + orchestrator), orchestrator config, and
.env file.  All stacks share a single pre-built ``orcest-orchestrator:latest``
Docker image.
"""

from __future__ import annotations

import re
import subprocess
import sys
import textwrap

import click
from rich.console import Console

_SSH_INPUT_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")

# StrictHostKeyChecking=yes requires known_hosts to be pre-populated (e.g. via
# ssh-keyscan during provisioning) but prevents MITM on initial connection when
# writing secrets to the remote host.
_SSH_OPTS = ["-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"]

_DOCKER_BUILD_CMD = "sudo -u orcest docker build -t orcest-orchestrator:latest /opt/orcest/"

_DOCKER_INSPECT_CMD = (
    "sudo -u orcest docker image inspect orcest-orchestrator:latest >/dev/null 2>&1"
)


def _validate_ssh_input(value: str, label: str) -> None:
    if ".." in value or not _SSH_INPUT_RE.match(value):
        raise click.BadParameter(
            f"Invalid {value!r}: only alphanumerics, dots, hyphens, and underscores are allowed.",
            param_hint=repr(label),
        )


def _ssh(target: str, cmd: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["ssh", *_SSH_OPTS, target, cmd],
        capture_output=True,
        text=True,
    )


def _ssh_check(
    target: str,
    cmd: str,
    description: str,
    console: Console,
) -> None:
    """Run an SSH command, print status, and exit on failure."""
    console.print(f"  {description}...", end=" ")
    result = _ssh(target, cmd)
    if result.returncode != 0:
        console.print("[red]failed[/red]")
        stderr = result.stderr.strip()
        if stderr:
            console.print(f"    {stderr}")
        sys.exit(1)
    console.print("[green]ok[/green]")


def _ssh_stdin_check(
    target: str,
    cmd: str,
    stdin_data: str,
    description: str,
    console: Console,
) -> None:
    """Run an SSH command with stdin input, print status, and exit on failure.

    Use this when secret values must be delivered via stdin rather than
    embedded in the shell command string.
    """
    console.print(f"  {description}...", end=" ")
    result = subprocess.run(
        ["ssh", *_SSH_OPTS, target, cmd],
        input=stdin_data,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console.print("[red]failed[/red]")
        stderr = result.stderr.strip()
        if stderr:
            console.print(f"    {stderr}")
        sys.exit(1)
    console.print("[green]ok[/green]")


def _ufw_cmd(action: str, port: int) -> str:
    """Build a ufw command that only runs if ufw is active.

    Returns 0 if ufw is not installed/active (nothing to do) or if the rule
    change succeeds; returns non-zero only if ufw IS active but the rule
    change fails.  This allows callers to use ``_ssh_check`` without false
    positives on hosts that don't use ufw.
    """
    return (
        f"if command -v ufw >/dev/null 2>&1"
        f" && sudo ufw status 2>/dev/null | grep -q 'Status: active';"
        f" then sudo ufw {action} {port}/tcp; fi"
    )


def render_project_compose(redis_port: int) -> str:
    """Return docker-compose.yml content for a per-project stack."""
    return textwrap.dedent(f"""\
        services:
          redis:
            image: redis:7-alpine
            ports:
              - "{redis_port}:6379"
            volumes:
              - redis-data:/data
            command: redis-server --appendonly yes
            restart: unless-stopped
            healthcheck:
              test: ["CMD", "redis-cli", "ping"]
              interval: 10s
              timeout: 3s
              retries: 3
            logging:
              driver: json-file
              options:
                max-size: "50m"
                max-file: "3"

          orchestrator:
            image: orcest-orchestrator:latest
            depends_on:
              redis:
                condition: service_healthy
            volumes:
              - ./config:/home/orcest/app/config:ro
            env_file:
              - .env
            restart: unless-stopped
            healthcheck:
              test: ["CMD", "python3", "-c", "import redis; redis.Redis('redis').ping()"]
              interval: 30s
              timeout: 5s
              retries: 3
            logging:
              driver: json-file
              options:
                max-size: "50m"
                max-file: "3"
            mem_limit: 1g

        volumes:
          redis-data:
    """)


def render_orchestrator_config(repo: str) -> str:
    """Return orchestrator.yaml content for a project."""
    return textwrap.dedent(f"""\
        redis:
          host: redis
          port: 6379

        github:
          repo: "{repo}"

        polling:
          interval: 60

        default_runner: "claude"

        labels:
          blocked: "orcest:blocked"
          needs_human: "orcest:needs-human"
          ready: "orcest:ready"
    """)


def _ensure_image(
    ssh_target: str,
    console: Console,
    *,
    force: bool,
) -> None:
    """Build the orchestrator image if missing (or forced)."""
    if force:
        console.print("  Rebuilding orcest-orchestrator image...")
        result = subprocess.run(
            ["ssh", *_SSH_OPTS, ssh_target, _DOCKER_BUILD_CMD],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            console.print("  Image build [red]failed[/red]")
            output = (result.stdout + result.stderr).strip()
            if output:
                console.print(f"    {output}")
            sys.exit(1)
        console.print("  Image build [green]ok[/green]")
        return

    result = _ssh(ssh_target, _DOCKER_INSPECT_CMD)
    if result.returncode != 0:
        console.print(
            "  Building orcest-orchestrator image (first time)...",
        )
        build_result = subprocess.run(
            ["ssh", *_SSH_OPTS, ssh_target, _DOCKER_BUILD_CMD],
            capture_output=True,
            text=True,
        )
        if build_result.returncode != 0:
            console.print("  Image build [red]failed[/red]")
            output = (build_result.stdout + build_result.stderr).strip()
            if output:
                console.print(f"    {output}")
            sys.exit(1)
        console.print("  Image build [green]ok[/green]")


def deploy_project_stack(
    host: str,
    user: str,
    project_name: str,
    redis_port: int,
    repo: str,
    github_token: str,
    claude_token: str,
    console: Console,
    *,
    rebuild_image: bool = False,
) -> None:
    """Deploy a per-project orchestrator stack on the orchestrator VM."""
    _validate_ssh_input(host, "host")
    _validate_ssh_input(user, "user")
    _validate_ssh_input(project_name, "project_name")
    if not _REPO_RE.match(repo):
        raise click.BadParameter(
            f"Invalid repo {repo!r}: expected 'owner/repo' format with alphanumerics, dots,"
            " hyphens, or underscores.",
            param_hint="'repo'",
        )

    ssh_target = f"{user}@{host}"
    pdir = f"/opt/orcest/projects/{project_name}"

    console.print(
        f"\n  [bold]Deploying orchestrator stack for '{project_name}'[/bold]",
    )

    _ensure_image(ssh_target, console, force=rebuild_image)

    # Create project directory
    _ssh_check(
        ssh_target,
        f"sudo -u orcest mkdir -p {pdir}/config",
        "Creating project directory",
        console,
    )

    # Write docker-compose.yml via stdin to avoid heredoc delimiter injection
    compose = render_project_compose(redis_port)
    _ssh_stdin_check(
        ssh_target,
        f"sudo -u orcest bash -c 'cat > {pdir}/docker-compose.yml'",
        compose,
        "Writing docker-compose.yml",
        console,
    )

    # Write orchestrator config.
    # Pass via stdin to avoid heredoc injection if repo contains a line
    # matching the delimiter.
    config = render_orchestrator_config(repo)
    _ssh_stdin_check(
        ssh_target,
        f"sudo -u orcest bash -c 'cat > {pdir}/config/orchestrator.yaml'",
        config,
        "Writing orchestrator config",
        console,
    )

    # Validate tokens do not contain newlines (would produce a malformed .env).
    if "\n" in github_token or "\n" in claude_token:
        raise click.BadParameter(
            "Token must not contain newlines.",
            param_hint="'github_token' / 'claude_token'",
        )

    # Write .env (contains secrets, restrict permissions).
    # Pass token via stdin so it never enters the shell command string,
    # avoiding any risk of injection if the token contains shell metacharacters.
    _ssh_stdin_check(
        ssh_target,
        f"sudo -u orcest bash -c 'umask 077 && cat > {pdir}/.env'",
        f"GITHUB_TOKEN={github_token}\nCLAUDE_TOKEN={claude_token}\n",
        "Writing .env",
        console,
    )

    # Open firewall port if ufw is active
    _ssh_check(
        ssh_target,
        _ufw_cmd("allow", redis_port),
        f"Opening firewall port {redis_port}",
        console,
    )

    # Start the stack
    _ssh_check(
        ssh_target,
        f"sudo -u orcest bash -c 'cd {pdir} && docker compose up -d'",
        "Starting containers",
        console,
    )

    # Verify Redis
    console.print("  Verifying Redis...", end=" ")
    result = _ssh(
        ssh_target,
        f"sudo -u orcest bash -c 'cd {pdir} && docker compose exec -T redis redis-cli ping'",
    )
    if result.stdout.strip() == "PONG":
        console.print("[green]PONG[/green]")
    else:
        console.print(
            "[yellow]no response (services may still be starting)[/yellow]",
        )


def destroy_project_stack(
    host: str,
    user: str,
    project_name: str,
    redis_port: int,
    console: Console,
) -> None:
    """Tear down a per-project orchestrator stack."""
    _validate_ssh_input(host, "host")
    _validate_ssh_input(user, "user")
    _validate_ssh_input(project_name, "project_name")

    ssh_target = f"{user}@{host}"
    pdir = f"/opt/orcest/projects/{project_name}"

    console.print(
        f"\n  [bold]Destroying orchestrator stack for '{project_name}'[/bold]",
    )

    # Stop and remove containers + volumes
    result = _ssh(
        ssh_target,
        f"sudo -u orcest bash -c 'cd {pdir} && docker compose down -v' 2>/dev/null",
    )
    if result.returncode == 0:
        console.print("  Containers stopped [green]ok[/green]")
    else:
        console.print(
            "  Containers [yellow]already stopped or missing[/yellow]",
        )

    # Remove project directory (run as orcest user to keep blast radius consistent)
    _ssh_check(
        ssh_target,
        f"sudo -u orcest rm -rf {pdir}",
        "Removing project directory",
        console,
    )

    # Close firewall port if ufw is active
    result = _ssh(ssh_target, _ufw_cmd("delete allow", redis_port))
    if result.returncode != 0:
        console.print(
            f"  [yellow]Warning: failed to remove ufw rule for port {redis_port}"
            " — the port may remain open.[/yellow]"
        )


def restart_project_stack(
    ssh_target: str,
    project_name: str,
    console: Console,
) -> None:
    """Restart a per-project orchestrator stack (docker compose up -d)."""
    _validate_ssh_input(project_name, "project_name")
    pdir = f"/opt/orcest/projects/{project_name}"
    console.print(f"  Restarting orchestrator stack for '{project_name}'...", end=" ")
    compose_up = f"sudo -u orcest bash -c 'cd {pdir} && docker compose up -d'"
    result = subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, compose_up],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        console.print("[green]ok[/green]")
    else:
        console.print("[yellow]failed (stack may not exist yet)[/yellow]")


def rebuild_image(host: str, user: str, console: Console) -> None:
    """Rebuild the shared orcest-orchestrator Docker image."""
    _validate_ssh_input(host, "host")
    _validate_ssh_input(user, "user")

    ssh_target = f"{user}@{host}"

    console.print(
        "\n  [bold]Rebuilding orcest-orchestrator image[/bold]",
    )
    result = subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, _DOCKER_BUILD_CMD],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console.print("  Image build [red]failed[/red]")
        output = (result.stdout + result.stderr).strip()
        if output:
            console.print(f"    {output}")
        sys.exit(1)
    console.print("  Image rebuild [green]ok[/green]")
