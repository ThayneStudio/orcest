"""Fleet management CLI commands.

Provides ``orcest fleet`` subcommands for managing the fleet of
orchestrator stacks and disposable worker VMs via Terraform and
Docker Compose, driven by a single config file.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from typing import TYPE_CHECKING

import click
from rich.console import Console
from rich.table import Table

from orcest.fleet.config import DEFAULT_CONFIG_PATH

if TYPE_CHECKING:
    from orcest.fleet.config import FleetConfig
    from orcest.fleet.proxmox_api import ProxmoxClient

_REPO_RE = re.compile(r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$")

_DEFAULT_CLOUD_IMAGE_URL = (
    "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
)


def _next_free_vmid() -> int | None:
    """Query Proxmox for the next available VM ID, or return None."""
    import json

    try:
        result = subprocess.run(
            ["pvesh", "get", "/cluster/nextid", "--output-format", "json"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return int(json.loads(result.stdout))
    except (FileNotFoundError, ValueError, json.JSONDecodeError):
        pass
    return None


def _get_vm_ip(vm_id: int, console: Console, timeout: int = 300) -> str | None:
    """Wait for a VM to get an IP address via the QEMU guest agent.

    Falls back to ARP table scanning if the guest agent is unavailable.
    """
    import json

    deadline = time.monotonic() + timeout
    console.print(f"  Waiting for VM {vm_id} to get an IP...", end=" ")

    while time.monotonic() < deadline:
        # Try QEMU guest agent first
        result = subprocess.run(
            ["qm", "guest", "cmd", str(vm_id), "network-get-interfaces"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            try:
                interfaces = json.loads(result.stdout)
                for iface in interfaces:
                    if iface.get("name") == "lo":
                        continue
                    for addr in iface.get("ip-addresses", []):
                        if addr.get("ip-address-type") == "ipv4":
                            ip = addr["ip-address"]
                            console.print(f"[green]{ip}[/green]")
                            return ip
            except (json.JSONDecodeError, KeyError):
                pass

        # Try ARP table as fallback — match the VM's MAC address
        mac_result = subprocess.run(
            ["qm", "config", str(vm_id)],
            capture_output=True,
            text=True,
        )
        if mac_result.returncode == 0:
            mac_match = re.search(r"([0-9A-Fa-f:]{17})", mac_result.stdout)
            if mac_match:
                mac = mac_match.group(1).lower()
                arp_result = subprocess.run(
                    ["ip", "neigh"], capture_output=True, text=True,
                )
                for line in arp_result.stdout.splitlines():
                    if mac in line.lower():
                        parts = line.split()
                        if parts:
                            ip = parts[0]
                            console.print(f"[green]{ip}[/green] (via ARP)")
                            return ip

        time.sleep(5)

    console.print("[yellow]timed out[/yellow]")
    return None


def _validate_project_name(name: str) -> None:
    """Validate project name, exit on failure."""
    from orcest.fleet.config import require_valid_project_name

    try:
        require_valid_project_name(name)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


def _validate_repo(repo: str) -> None:
    """Validate repo format (owner/repo)."""
    if not _REPO_RE.match(repo):
        click.echo(
            f"Error: Invalid repo format {repo!r}: expected 'owner/repo' with "
            "alphanumeric/dot/hyphen/underscore characters.",
            err=True,
        )
        sys.exit(1)


def _repo_to_project_name(repo: str) -> str:
    """Derive project name from repo (e.g. 'ThayneStudio/my-project' -> 'my-project')."""
    return repo.rsplit("/", 1)[-1]


_SSH_OPTS = [
    "-o", "ConnectTimeout=5",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "BatchMode=yes",
    "-o", "LogLevel=ERROR",
]


def _run_on_orchestrator(
    cfg: FleetConfig,
    cmd: list[str],
    input_data: str | None = None,
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    """Run an orcest CLI command on the orchestrator in a throwaway Docker container.

    SSHes to the orchestrator VM and executes ``docker run --rm -i orcest <cmd>``
    in an ephemeral container. Useful for self-test commands that need tools
    only available inside the orchestrator image (e.g. ``gh``).

    Args:
        cfg: Fleet config (must have orchestrator host set).
        cmd: Command arguments to run inside the container
            (e.g. ``["orcest", "check", "github-token"]``).
        input_data: Optional data to pipe to the container's stdin.
        timeout: Maximum seconds to wait for the command to complete.

    Returns:
        The completed process result.
    """
    import shlex

    ssh_target = cfg.ssh_target()
    quoted = " ".join(shlex.quote(c) for c in cmd)
    docker_cmd = f"docker run --rm -i orcest {quoted}"
    return subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, docker_cmd],
        input=input_data,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _create_proxmox_client(cfg: FleetConfig) -> ProxmoxClient:
    """Create a ProxmoxClient from fleet config."""
    from orcest.fleet.proxmox_api import ProxmoxClient

    return ProxmoxClient(
        endpoint=cfg.proxmox.endpoint,
        token_id=cfg.proxmox.api_token_id,
        token_secret=cfg.proxmox.api_token_secret,
        node=cfg.proxmox.node,
    )


def _wait_for_cloud_init(
    host: str, user: str, console: Console, timeout: int = 600,
) -> bool:
    """Wait for cloud-init to finish on a remote host. Returns True on success."""
    ssh_target = f"{user}@{host}"
    console.print(f"  Waiting for cloud-init to finish on {host}...", end=" ")
    try:
        result = subprocess.run(
            ["ssh", *_SSH_OPTS, ssh_target, "cloud-init status --wait"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode == 0:
            console.print("[green]ok[/green]")
            return True
        console.print("[yellow]warning[/yellow]")
        console.print(f"    cloud-init may have errors: {result.stderr.strip()}")
        return True  # cloud-init finished, possibly with errors
    except subprocess.TimeoutExpired:
        console.print("[red]timed out[/red]")
        return False


def _ssh_run(host: str, user: str, cmd: str) -> subprocess.CompletedProcess[str]:
    """Run a command over SSH and return the result."""
    ssh_target = f"{user}@{host}"
    return subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, cmd],
        capture_output=True,
        text=True,
    )


def _wait_for_ssh(host: str, user: str, console: Console, timeout: int = 300) -> bool:
    """Poll until SSH connects or timeout expires. Returns True on success."""
    ssh_target = f"{user}@{host}"
    deadline = time.monotonic() + timeout
    console.print(f"  Waiting for SSH on {host}...", end=" ")
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["ssh", *_SSH_OPTS, ssh_target, "true"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            console.print("[green]ok[/green]")
            return True
        time.sleep(5)
    console.print("[yellow]timed out[/yellow]")
    return False


@click.group()
def fleet() -> None:
    """Manage the orcest fleet: orchestrators, workers, and VMs."""


@fleet.command("add-org")
@click.argument("org_name")
@click.option(
    "--github-token", required=True,
    help="GitHub PAT (classic: repo+workflow scopes; "
    "fine-grained: contents, issues, pull-requests, actions R/W).",
)
@click.option("--claude-token", required=True, help="Claude OAuth token for this org.")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def add_org(org_name: str, github_token: str, claude_token: str, config: str) -> None:
    """Register a GitHub organization with its credentials.

    ORG_NAME is the GitHub org or user (e.g. 'ThayneStudio').

    \b
    GitHub token requirements:
      Classic PAT (ghp_): repo + workflow scopes
      Fine-grained PAT:   contents, issues, pull-requests, actions (R/W)
                          metadata (read)
    """
    from orcest.fleet.config import OrgEntry, load_config, save_config

    console = Console()
    cfg = load_config(config)

    if org_name in cfg.orgs:
        console.print(f"[yellow]Org '{org_name}' already exists, updating credentials.[/yellow]")

    # Validate the GitHub token via the orchestrator's Docker image (which has gh installed)
    console.print("  Validating GitHub token...", end=" ")
    if not cfg.orchestrator.host:
        console.print("[yellow]skipped (orchestrator not set up yet)[/yellow]")
    else:
        try:
            result = _run_on_orchestrator(
                cfg, ["orcest", "check", "github-token"], input_data=github_token + "\n",
            )
            if result.returncode != 0:
                console.print("[red]failed[/red]")
                output = (result.stderr or result.stdout or "").strip()
                if output:
                    console.print(f"    {output}")
                console.print("[yellow]Warning: token validation failed, saving anyway.[/yellow]")
            else:
                console.print("[green]ok[/green]")
        except (OSError, subprocess.SubprocessError) as exc:
            console.print(f"[yellow]skipped ({exc})[/yellow]")

    cfg.orgs[org_name] = OrgEntry(
        github_token=github_token,
        claude_oauth_token=claude_token,
    )
    save_config(cfg, config)
    console.print(f"\n[bold]Org '{org_name}' registered.[/bold]")


@fleet.command("create-orchestrator")
@click.option("--vm-id", type=int, default=None, help="Proxmox VM ID for the orchestrator.")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def create_orchestrator(vm_id: int | None, config: str) -> None:
    """Create the orchestrator VM via Terraform and deploy the Docker stack."""
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    # Prompt for VM ID
    if vm_id is None:
        default_id = _next_free_vmid() or cfg.orchestrator.vm_id
        vm_id = click.prompt("  VM ID for orchestrator", default=default_id, type=int)
    cfg.orchestrator.vm_id = vm_id

    console.print(f"\n[bold]Creating orchestrator VM (ID {vm_id})[/bold]\n")

    # Step 1: Generate and write tfvars
    console.print("  Generating Terraform variables...", end=" ")
    try:
        from orcest.fleet.provisioner import generate_tfvars, write_tfvars

        tfvars = generate_tfvars(cfg)
        write_tfvars(tfvars)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        sys.exit(1)

    # Step 2: Apply Terraform
    console.print("  Applying Terraform (this may take a few minutes)...")
    try:
        from orcest.fleet.provisioner import apply

        apply()
        console.print("  Terraform apply [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Terraform apply [red]failed[/red]: {exc}")
        sys.exit(1)

    # Step 3: Get orchestrator IP (via guest agent or ARP)
    orch_ip = _get_vm_ip(vm_id, console)
    if not orch_ip:
        console.print("  [yellow]Could not determine IP. VM may still be booting.[/yellow]")
        console.print("  Saving config. Re-run after VM is ready.")
        save_config(cfg, config)
        sys.exit(1)

    # Step 4: Wait for SSH
    if not _wait_for_ssh(orch_ip, cfg.orchestrator.user, console):
        console.print("[yellow]SSH not available yet. VM may still be booting.[/yellow]")
        console.print("  Saving config with the IP and exiting. Re-run after VM is ready.")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 5: Wait for cloud-init to finish (installs Docker, etc.)
    ssh_target = f"{cfg.orchestrator.user}@{orch_ip}"
    if not _wait_for_cloud_init(orch_ip, cfg.orchestrator.user, console):
        console.print("[red]Cloud-init timed out. Saving config with partial state.[/red]")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 6: Upload source and build Docker image
    try:
        from orcest.fleet.orchestrator import build_image, upload_source

        console.print("  Uploading orcest source...")
        upload_source(ssh_target)
        console.print("  Upload [green]ok[/green]")

        console.print("  Building Docker image (this may take a minute)...")
        build_image(ssh_target)
        console.print("  Docker build [green]ok[/green]")
    except Exception as exc:
        console.print(f"  [red]failed[/red]: {exc}")
        console.print("  Saving config with partial state.")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 7: Start shared Redis stack
    try:
        from orcest.fleet.orchestrator import ensure_redis_stack

        console.print("  Starting shared Redis stack...")
        ensure_redis_stack(ssh_target)
        console.print("  Redis stack [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Redis stack [red]failed[/red]: {exc}")
        console.print("  Saving config with partial state.")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 8: Update config with orchestrator host (before uploading to remote)
    cfg.orchestrator.host = orch_ip
    save_config(cfg, config)

    # Step 9: Start pool manager (if template and Proxmox creds are configured)
    if cfg.pool.template_vm_id and cfg.proxmox.api_token_id and cfg.proxmox.api_token_secret:
        try:
            from orcest.fleet.orchestrator import ensure_pool_manager, upload_fleet_config

            console.print("  Uploading fleet config and starting pool manager...")
            upload_fleet_config(ssh_target, config)
            ensure_pool_manager(ssh_target)
            console.print("  Pool manager [green]ok[/green]")
        except Exception as exc:
            console.print(f"  Pool manager [yellow]failed: {exc}[/yellow]")
            console.print("  (Pool manager can be started later with 'orcest fleet update')")

    console.print(f"\n[bold]Orchestrator created at {orch_ip}.[/bold]")
    console.print("\n  Next steps:")
    console.print("  1. Create template:  orcest fleet create-template")
    console.print("  2. Set pool size:    orcest fleet set-pool-size <N>")
    console.print(
        "  3. Register an org:  orcest fleet add-org <org> --github-token ... --claude-token ..."
    )
    console.print("  4. Onboard a repo:   orcest fleet onboard <owner/repo>")


@fleet.command()
@click.argument("repo")
@click.option("--name", default=None, help="Project name (default: derived from repo).")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def onboard(repo: str, name: str | None, config: str) -> None:
    """Onboard a new repo: register project and deploy orchestrator stack.

    REPO is in "owner/repo" format (e.g. ThayneStudio/my-project).
    Workers are managed by the pool manager, not per-project.
    Requires the orchestrator VM to be created first (fleet create-orchestrator).
    """
    from orcest.fleet.config import ProjectEntry, load_config, save_config

    console = Console()
    cfg = load_config(config)
    project_name = name or _repo_to_project_name(repo)

    # Validate inputs
    _validate_repo(repo)
    _validate_project_name(project_name)

    console.print(f"\n[bold]Onboarding {repo} as '{project_name}'[/bold]\n")

    # Validate orchestrator is set up
    if not cfg.orchestrator.host:
        console.print(
            "[red]Orchestrator host not set in fleet config.[/red]\n"
            "  Run 'orcest fleet create-orchestrator' first."
        )
        sys.exit(1)

    # Resolve org credentials
    org_name = repo.split("/")[0] if "/" in repo else ""
    org = cfg.orgs.get(org_name)
    if not org:
        console.print(
            f"[red]Org '{org_name}' not found in fleet config.[/red]\n"
            f"  Run 'orcest fleet add-org {org_name} --github-token ... --claude-token ...' first."
        )
        sys.exit(1)

    # Check for duplicate
    if cfg.get_project(project_name):
        console.print(f"[red]Project '{project_name}' already exists in fleet config.[/red]")
        sys.exit(1)

    # Add project to config
    project = ProjectEntry(
        name=project_name,
        repo=repo,
    )
    cfg.projects.append(project)

    console.print(f"  Project: {project_name}")
    console.print(f"  Repo: {repo}")

    # Step 1: Write project files to orchestrator
    ssh_target = cfg.ssh_target()
    console.print("\n  Deploying orchestrator stack...")
    try:
        from orcest.fleet.orchestrator import (
            generate_env_file,
            generate_orchestrator_config,
            write_project_files,
        )

        env_content = generate_env_file(
            github_token=org.github_token,
            key_prefix=project_name,
            project_name=project_name,
        )
        config_yaml = generate_orchestrator_config(
            repo=repo, key_prefix=project_name,
        )
        write_project_files(ssh_target, project_name, env_content, config_yaml)
        console.print("  Project files written [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Writing project files [red]failed[/red]: {exc}")
        cfg.projects = [p for p in cfg.projects if p.name != project_name]
        sys.exit(1)

    # Step 2: Ensure shared Redis stack is running, then deploy project stack
    try:
        from orcest.fleet.orchestrator import (
            deploy_stack,
            ensure_redis_stack,
            image_exists,
        )

        ensure_redis_stack(ssh_target)

        if not image_exists(ssh_target):
            from orcest.fleet.orchestrator import build_image

            console.print("  Docker image not found, building...")
            build_image(ssh_target)
            console.print("  Docker build [green]ok[/green]")

        deploy_stack(ssh_target, project_name)
        console.print("  Stack deployed [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Deploy stack [red]failed[/red]: {exc}")
        console.print(
            "  [yellow]Config saved with project entry. To retry, run:[/yellow]\n"
            f"  [yellow]  orcest fleet destroy {project_name} --yes && "
            f"orcest fleet onboard {repo}[/yellow]"
        )
        save_config(cfg, config)
        sys.exit(1)

    save_config(cfg, config)
    console.print(f"\n[bold]Project '{project_name}' onboarded.[/bold]")


@fleet.command()
@click.argument("project_name")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
def destroy(project_name: str, config: str, yes: bool) -> None:
    """Destroy a project: remove orchestrator stack and deregister.

    Tears down the Docker Compose stack on the orchestrator and removes
    the project from config. Workers are managed by the pool manager.
    """
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    project = cfg.get_project(project_name)
    if not project:
        console.print(f"[red]Project '{project_name}' not found.[/red]")
        sys.exit(1)

    if not yes:
        click.confirm(
            f"Destroy project '{project_name}'?",
            abort=True,
        )

    console.print(f"\n[bold]Destroying project '{project_name}'[/bold]")

    # Teardown orchestrator stack
    if cfg.orchestrator.host:
        ssh_target = cfg.ssh_target()
        console.print("  Tearing down orchestrator stack...", end=" ")
        try:
            from orcest.fleet.orchestrator import teardown_stack

            teardown_stack(ssh_target, project_name)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed: {exc}[/yellow]")

    # Remove project from config
    cfg.projects = [p for p in cfg.projects if p.name != project_name]
    save_config(cfg, config)
    console.print(f"\n[bold]Project '{project_name}' destroyed.[/bold]")


@fleet.command()
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def update(config: str) -> None:
    """Update the fleet: rebuild Docker image and restart stacks.

    Uploads fresh source to the orchestrator, rebuilds the Docker image,
    and restarts all project stacks. Worker VMs are managed by the pool
    manager and will pick up changes on next clone cycle.
    """
    from orcest.fleet.config import load_config

    console = Console()
    cfg = load_config(config)

    if not cfg.orchestrator.host:
        console.print("[red]Orchestrator host not set in fleet config.[/red]")
        sys.exit(1)

    ssh_target = cfg.ssh_target()

    console.print("\n[bold]Updating fleet[/bold]\n")

    # Step 1: Upload source and rebuild Docker image
    try:
        from orcest.fleet.orchestrator import build_image, upload_source

        console.print("  Uploading fresh source...")
        upload_source(ssh_target)
        console.print("  Upload [green]ok[/green]")

        console.print("  Rebuilding Docker image...")
        build_image(ssh_target)
        console.print("  Docker build [green]ok[/green]")
    except Exception as exc:
        console.print(f"  [red]failed[/red]: {exc}")
        sys.exit(1)

    # Step 2: Update shared Redis stack
    console.print("  Updating shared Redis stack...", end=" ")
    try:
        from orcest.fleet.orchestrator import ensure_redis_stack

        ensure_redis_stack(ssh_target)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[yellow]failed: {exc}[/yellow]")

    # Step 3: Update pool manager (if template and Proxmox creds are configured)
    if cfg.pool.template_vm_id and cfg.proxmox.api_token_id and cfg.proxmox.api_token_secret:
        console.print("  Updating pool manager...", end=" ")
        try:
            from orcest.fleet.orchestrator import ensure_pool_manager, upload_fleet_config

            upload_fleet_config(ssh_target, config)
            ensure_pool_manager(ssh_target)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed: {exc}[/yellow]")

    # Step 4: Restart all project stacks
    from orcest.fleet.orchestrator import restart_stack

    for project in cfg.projects:
        console.print(f"  Restarting stack for '{project.name}'...", end=" ")
        try:
            restart_stack(ssh_target, project.name)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed: {exc}[/yellow]")

    console.print("\n[bold]Fleet update complete.[/bold]")


@fleet.command()
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def status(config: str) -> None:
    """Show fleet status: orchestrator, projects, and workers."""
    from orcest.fleet.config import load_config, validate_project_name

    console = Console()
    cfg = load_config(config)

    # Orchestrator status
    orch_table = Table(title="Orchestrator")
    orch_table.add_column("Property", style="cyan")
    orch_table.add_column("Value", style="white")

    orch_table.add_row("Host", cfg.orchestrator.host or "[dim]not set[/dim]")
    orch_table.add_row("User", cfg.orchestrator.user)
    orch_table.add_row("VM ID", str(cfg.orchestrator.vm_id))

    # Try SSH ping to check status
    if cfg.orchestrator.host:
        ssh_target = cfg.ssh_target()
        result = subprocess.run(
            ["ssh", *_SSH_OPTS, ssh_target, "true"],
            capture_output=True,
            text=True,
        )
        ssh_status = (
            "[green]reachable[/green]" if result.returncode == 0 else "[red]unreachable[/red]"
        )
        orch_table.add_row("SSH Status", ssh_status)

    console.print(orch_table)

    # Orgs
    if cfg.orgs:
        org_table = Table(title="Registered Orgs")
        org_table.add_column("Org", style="cyan")
        org_table.add_column("GitHub Token", style="green")
        org_table.add_column("Claude Token", style="yellow")

        for org_name, org_entry in cfg.orgs.items():
            gh = f"{org_entry.github_token[:8]}..." if org_entry.github_token else "[dim]none[/dim]"
            cl = (
                f"{org_entry.claude_oauth_token[:8]}..."
                if org_entry.claude_oauth_token
                else "[dim]none[/dim]"
            )
            org_table.add_row(org_name, gh, cl)

        console.print(org_table)

    # Projects
    if not cfg.projects:
        console.print("\n[dim]No projects in fleet config.[/dim]")
        return

    proj_table = Table(title="Projects")
    proj_table.add_column("Project", style="cyan")
    proj_table.add_column("Repo", style="white")
    proj_table.add_column("Stack Status", style="magenta")

    for project in cfg.projects:
        if not validate_project_name(project.name):
            # Skip projects with invalid names rather than aborting the
            # entire status display.  This can happen if the config file
            # was hand-edited with an invalid name.
            proj_table.add_row(
                project.name,
                project.repo,
                "[red]invalid name[/red]",
            )
            continue
        stack_status = "[dim]unknown[/dim]"
        if cfg.orchestrator.host:
            ssh_target = cfg.ssh_target()
            result = subprocess.run(
                [
                    "ssh", *_SSH_OPTS, ssh_target,
                    f"cd /opt/orcest && docker compose"
                    f" -p orcest-{project.name}"
                    f" ps --format json 2>/dev/null",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout.strip():
                stack_status = "[green]running[/green]"
            elif result.returncode == 0:
                stack_status = "[yellow]stopped[/yellow]"
            else:
                stack_status = "[red]error[/red]"

        proj_table.add_row(
            project.name,
            project.repo,
            stack_status,
        )

    console.print(proj_table)

    # Pool info
    pool_table = Table(title="Worker Pool")
    pool_table.add_column("Property", style="cyan")
    pool_table.add_column("Value", style="white")
    pool_table.add_row("Target Size", str(cfg.pool.size))
    tmpl_id = str(cfg.pool.template_vm_id) if cfg.pool.template_vm_id else "[dim]not set[/dim]"
    pool_table.add_row("Template VM ID", tmpl_id)
    pool_table.add_row("Storage", cfg.pool.storage)
    pool_table.add_row("Worker Memory", f"{cfg.pool.worker_memory} MB")
    pool_table.add_row("Worker Cores", str(cfg.pool.worker_cores))
    console.print(pool_table)


def _create_vm_from_cloud_image(
    px: ProxmoxClient,
    cfg: FleetConfig,
    vm_id: int,
    image_url: str,
    console: Console,
) -> None:
    """Download a cloud image and create a VM with it as the boot disk.

    Uses the Proxmox ``download-url`` API to fetch the image, then creates
    a VM with ``import-from`` to use the downloaded image as the boot disk.
    Disk is resized to ``cfg.pool.worker_disk_size``.

    Raises on any failure — caller is responsible for destroying the VM.
    """
    from urllib.parse import urlparse

    parsed = urlparse(image_url)
    if parsed.scheme not in ("https", "http"):
        raise ValueError(f"Invalid image URL scheme: {parsed.scheme!r} (expected http or https)")

    storage = cfg.pool.storage
    # Derive filename from the URL path and sanitize it
    raw_filename = image_url.rsplit("/", 1)[-1].split("?")[0] or "cloud-image.img"
    filename = re.sub(r"[^a-zA-Z0-9._-]", "_", raw_filename)
    if not filename or filename.startswith("."):
        filename = "cloud-image.img"

    # Step 1: Download cloud image to Proxmox local storage (skip if already present)
    download_storage = "local"
    console.print("  Downloading cloud image...", end=" ")
    try:
        px.download_image(image_url, filename, storage=download_storage)
        console.print("[green]ok[/green]")
    except RuntimeError as exc:
        if "already exists" in str(exc) or "override existing" in str(exc):
            console.print("[yellow]already cached[/yellow]")
        else:
            raise

    # Step 2: Create VM with imported disk
    console.print("  Creating VM...", end=" ")
    px.create_vm(
        vm_id=vm_id,
        name="orcest-worker-template",
        memory=cfg.pool.worker_memory,
        cores=cfg.pool.worker_cores,
        scsihw="virtio-scsi-pci",
        scsi0=f"{storage}:0,import-from={download_storage}:iso/{filename}",
        ide2=f"{storage}:cloudinit",
        net0="virtio,bridge=vmbr0",
        boot="order=scsi0",
        serial0="socket",
        vga="serial0",
        agent="1",
    )
    console.print("[green]ok[/green]")

    # Step 3: Resize disk to configured worker size
    console.print(f"  Resizing disk to {cfg.pool.worker_disk_size}G...", end=" ")
    px.resize_disk(vm_id, "scsi0", f"{cfg.pool.worker_disk_size}G")
    console.print("[green]ok[/green]")


@fleet.command("create-template")
@click.option("--vm-id", type=int, default=None, help="VM ID for the new template.")
@click.option(
    "--image-url",
    default=_DEFAULT_CLOUD_IMAGE_URL,
    help="Cloud image URL to download.",
    show_default=True,
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def create_template(vm_id: int | None, image_url: str, config: str) -> None:
    """Create a worker VM template for the warm pool.

    Downloads a cloud image, creates a VM, installs worker tools
    via cloud-init, then converts to a template for fast linked cloning.
    """
    from orcest.fleet.cloud_init import render_template_userdata
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Proxmox API credentials not configured.[/red]")
        console.print("  Set proxmox.api_token_id and proxmox.api_token_secret in fleet config.")
        sys.exit(1)

    px = _create_proxmox_client(cfg)

    # Prompt for template VM ID
    if vm_id is None:
        default_id = _next_free_vmid()
        vm_id = click.prompt("  VM ID for new template", default=default_id, type=int)

    console.print(f"\n[bold]Creating worker template (VM {vm_id})[/bold]\n")

    # Step 1: Create VM from cloud image
    try:
        _create_vm_from_cloud_image(px, cfg, vm_id, image_url, console)
    except Exception as exc:
        console.print(f"  [red]failed[/red]: {exc}")
        # Best-effort cleanup of partially-created VM
        try:
            px.destroy_vm(vm_id)
        except Exception:
            pass
        sys.exit(1)

    # Steps 2-8 can all fail; on any failure we destroy the VM
    # to avoid leaving orphaned resources.
    def _cleanup_vm() -> None:
        console.print("  Cleaning up: destroying VM...")
        try:
            # Stop the VM first -- Proxmox refuses to delete running VMs.
            # Best-effort; the VM may already be stopped or never started.
            try:
                px.stop_vm(vm_id)
                # Brief wait for it to actually stop before destroying
                stop_deadline = time.monotonic() + 15
                while time.monotonic() < stop_deadline:
                    if px.get_vm_status(vm_id) == "stopped":
                        break
                    time.sleep(1)
            except Exception:
                pass  # VM may already be stopped or never started
            px.destroy_vm(vm_id)
        except Exception:
            console.print("  [yellow]Warning: cleanup failed; VM may need manual removal.[/yellow]")

    # Step 2: Configure cloud-init userdata
    console.print("  Configuring cloud-init...", end=" ")
    try:
        userdata = render_template_userdata(
            ssh_public_key=cfg.orchestrator.ssh_key,
        )
        _set_vm_cloud_init(px, vm_id, userdata)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)

    # Step 3: Start the VM
    console.print("  Starting VM...", end=" ")
    try:
        px.start_vm(vm_id)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)

    # Step 4: Wait for IP
    console.print("  Waiting for VM IP...", end=" ")
    vm_ip = px.get_vm_ip(vm_id, timeout=300)
    if not vm_ip:
        console.print("[red]timed out[/red]")
        console.print("  Could not get VM IP. Template creation aborted.")
        _cleanup_vm()
        sys.exit(1)
    console.print(f"[green]{vm_ip}[/green]")

    # Step 5: Wait for SSH
    if not _wait_for_ssh(vm_ip, cfg.orchestrator.user, console):
        console.print("[red]SSH not available. Template creation aborted.[/red]")
        _cleanup_vm()
        sys.exit(1)

    # Step 6: Wait for cloud-init to finish
    if not _wait_for_cloud_init(vm_ip, cfg.orchestrator.user, console):
        console.print("[red]Cloud-init timed out. Template creation aborted.[/red]")
        _cleanup_vm()
        sys.exit(1)

    # Step 7: Disable cloud-init so clones don't re-run it
    console.print("  Disabling cloud-init...", end=" ")
    result = _ssh_run(vm_ip, cfg.orchestrator.user, "sudo touch /etc/cloud/cloud-init.disabled")
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        _cleanup_vm()
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 8: Stop the VM
    console.print("  Stopping VM...", end=" ")
    try:
        px.stop_vm(vm_id)
        # Wait for it to actually stop
        deadline = time.monotonic() + 60
        stopped = False
        while time.monotonic() < deadline:
            vm_status = px.get_vm_status(vm_id)
            if vm_status == "stopped":
                stopped = True
                break
            time.sleep(2)
        if not stopped:
            console.print("[red]timed out waiting for VM to stop[/red]")
            _cleanup_vm()
            sys.exit(1)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)

    # Step 9: Convert to template
    console.print("  Converting to template...", end=" ")
    try:
        px.convert_to_template(vm_id)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)

    # Step 10: Save template_vm_id in config
    cfg.pool.template_vm_id = vm_id
    save_config(cfg, config)

    console.print(f"\n[bold]Worker template created (VM {vm_id}).[/bold]")
    console.print(f"  Saved template_vm_id={vm_id} to fleet config.")


def _set_vm_cloud_init(px: ProxmoxClient, vm_id: int, userdata: str) -> None:
    """Set cloud-init user-data on a VM via the Proxmox API.

    Delegates to :meth:`ProxmoxClient.set_cloud_init_userdata`.
    """
    px.set_cloud_init_userdata(vm_id, userdata)


@fleet.command("pool-status")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def pool_status(config: str) -> None:
    """Show worker pool status: template info, idle/active VMs."""
    from orcest.fleet.config import load_config

    console = Console()
    cfg = load_config(config)

    # Pool configuration table
    pool_table = Table(title="Worker Pool Configuration")
    pool_table.add_column("Property", style="cyan")
    pool_table.add_column("Value", style="white")

    pool_table.add_row("Target Size", str(cfg.pool.size))
    pool_table.add_row(
        "Template VM ID",
        str(cfg.pool.template_vm_id) if cfg.pool.template_vm_id else "[dim]not set[/dim]",
    )
    pool_table.add_row("Storage", cfg.pool.storage)
    pool_table.add_row("Worker Memory", f"{cfg.pool.worker_memory} MB")
    pool_table.add_row("Worker Cores", str(cfg.pool.worker_cores))
    pool_table.add_row("Worker Disk Size", f"{cfg.pool.worker_disk_size} GB")
    pool_table.add_row("Max Task Duration", f"{cfg.pool.max_task_duration}s")
    console.print(pool_table)

    if not cfg.pool.template_vm_id:
        console.print(
            "\n[yellow]No template configured.[/yellow]\n"
            "  Run 'orcest fleet create-template' first."
        )
        return

    # Check template status via Proxmox API
    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print(
            "\n[yellow]Proxmox API credentials not configured"
            " -- cannot query VMs.[/yellow]"
        )
        return

    px = _create_proxmox_client(cfg)

    # Check if template exists
    console.print(f"\n  Checking template VM {cfg.pool.template_vm_id}...", end=" ")
    try:
        tpl_status = px.get_vm_status(cfg.pool.template_vm_id)
        console.print(f"[green]{tpl_status}[/green]")
    except Exception as exc:
        console.print(f"[red]not found[/red]: {exc}")
        return

    # List worker VMs (VMs named orcest-worker-*)
    console.print("\n  Scanning for worker VMs...")
    try:
        worker_vms = [
            vm for vm in px.list_vms(name_prefix="orcest-worker-")
            if not vm.get("template", False)
        ]
    except Exception as exc:
        console.print(f"  [red]Failed to list VMs[/red]: {exc}")
        return

    if not worker_vms:
        console.print("  [dim]No worker VMs found.[/dim]")
        return

    vm_table = Table(title="Worker VMs")
    vm_table.add_column("VM ID", style="cyan")
    vm_table.add_column("Name", style="white")
    vm_table.add_column("Status", style="magenta")
    vm_table.add_column("CPU", style="yellow")
    vm_table.add_column("Memory", style="yellow")

    running = 0
    stopped = 0
    for vm in sorted(worker_vms, key=lambda v: v.get("vmid", 0)):
        vm_status = vm.get("status", "unknown")
        if vm_status == "running":
            running += 1
            status_str = "[green]running[/green]"
        elif vm_status == "stopped":
            stopped += 1
            status_str = "[dim]stopped[/dim]"
        else:
            status_str = f"[yellow]{vm_status}[/yellow]"

        mem_mb = vm.get("maxmem", 0) // (1024 * 1024)
        vm_table.add_row(
            str(vm.get("vmid", "?")),
            vm.get("name", "?"),
            status_str,
            str(vm.get("cpus", "?")),
            f"{mem_mb} MB",
        )

    console.print(vm_table)
    console.print(f"\n  Total: {len(worker_vms)} VMs ({running} running, {stopped} stopped)")
    console.print(f"  Target pool size: {cfg.pool.size}")


@fleet.command("set-pool-size")
@click.argument("size", type=int)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def set_pool_size(size: int, config: str) -> None:
    """Set the target warm pool size."""
    from orcest.fleet.config import load_config, save_config

    console = Console()

    if size < 0:
        console.print("[red]Pool size must be non-negative.[/red]")
        sys.exit(1)

    cfg = load_config(config)
    old_size = cfg.pool.size
    cfg.pool.size = size
    save_config(cfg, config)

    console.print(f"Pool size updated: {old_size} -> {size}")
