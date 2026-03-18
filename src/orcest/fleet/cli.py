"""Fleet management CLI commands.

Provides ``orcest fleet`` subcommands for managing the fleet of
orchestrator stacks and disposable worker VMs via Terraform and
Docker Compose, driven by a single config file.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time

import click
from rich.console import Console
from rich.table import Table

from orcest.fleet.config import DEFAULT_CONFIG_PATH

_REPO_RE = re.compile(r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$")


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
            import re

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


def _wait_for_ssh(host: str, user: str, console: Console, timeout: int = 300) -> bool:
    """Poll until SSH connects or timeout expires. Returns True on success."""
    ssh_target = f"{user}@{host}"
    deadline = time.monotonic() + timeout
    console.print(f"  Waiting for SSH on {host}...", end=" ")
    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "BatchMode=yes",
                ssh_target,
                "true",
            ],
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
@click.option("--github-token", required=True, help="GitHub PAT (classic: repo+workflow scopes; fine-grained: contents, issues, pull-requests, actions R/W).")
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

    # Validate the GitHub token (best-effort; gh may not be installed on the Proxmox host)
    console.print("  Validating GitHub token...", end=" ")
    try:
        result = subprocess.run(
            ["gh", "auth", "status"],
            capture_output=True,
            text=True,
            env={**os.environ, "GITHUB_TOKEN": github_token, "GH_TOKEN": github_token},
        )
        if result.returncode != 0:
            console.print("[red]failed[/red]")
            stderr = result.stderr.strip()
            if stderr:
                console.print(f"    {stderr}")
            console.print("[yellow]Warning: token validation failed, saving anyway.[/yellow]")
        else:
            console.print("[green]ok[/green]")
    except FileNotFoundError:
        console.print("[yellow]skipped (gh CLI not installed)[/yellow]")

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
        console.print(f"  Saving config. Re-run after VM is ready.")
        save_config(cfg, config)
        sys.exit(1)

    # Step 4: Wait for SSH
    if not _wait_for_ssh(orch_ip, cfg.orchestrator.user, console):
        console.print("[yellow]SSH not available yet. VM may still be booting.[/yellow]")
        console.print("  Saving config with the IP and exiting. Re-run after VM is ready.")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 5: Upload source and build Docker image
    ssh_target = f"{cfg.orchestrator.user}@{orch_ip}"
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

    # Step 6: Start shared Redis stack
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

    # Step 7: Update config with orchestrator host
    cfg.orchestrator.host = orch_ip
    save_config(cfg, config)

    console.print(f"\n[bold]Orchestrator created at {orch_ip}.[/bold]")
    console.print("\n  Next steps:")
    console.print(
        "  1. Register an org:  orcest fleet add-org <org> --github-token ... --claude-token ..."
    )
    console.print("  2. Onboard a repo:   orcest fleet onboard <owner/repo>")


@fleet.command()
@click.argument("repo")
@click.option("--name", default=None, help="Project name (default: derived from repo).")
@click.option("--vm-id", type=int, default=None, help="Proxmox VM ID for the worker.")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def onboard(repo: str, name: str | None, vm_id: int | None, config: str) -> None:
    """Onboard a new repo: deploy orchestrator stack + create worker VM(s).

    REPO is in "owner/repo" format (e.g. ThayneStudio/my-project).
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

    # Prompt for worker VM ID
    if vm_id is None:
        default_id = _next_free_vmid() or 200
        vm_id = click.prompt("  VM ID for worker", default=default_id, type=int)

    # Add project to config
    project = ProjectEntry(
        name=project_name,
        repo=repo,
        workers=1,
        worker_vm_ids=[vm_id],
    )
    cfg.projects.append(project)

    console.print(f"  Project: {project_name}")
    console.print(f"  Repo: {repo}")
    console.print(f"  Key prefix: {project_name}")
    console.print("  Workers: 1")

    # Step 1: Generate tfvars and apply Terraform (creates worker VM)
    console.print("\n  Provisioning worker VM(s) via Terraform...")
    try:
        from orcest.fleet.provisioner import apply, generate_tfvars, write_tfvars

        tfvars = generate_tfvars(cfg)
        write_tfvars(tfvars)
        apply()
        console.print("  Terraform apply [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Terraform apply [red]failed[/red]: {exc}")
        # Remove the project we just added since provisioning failed
        cfg.projects = [p for p in cfg.projects if p.name != project_name]
        sys.exit(1)

    # Save config now — worker VM exists in Terraform state, must be tracked
    save_config(cfg, config)

    # Step 2: Write project files to orchestrator
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
        console.print("  [yellow]Config saved. Re-run onboard to retry stack deployment.[/yellow]")
        sys.exit(1)

    # Step 3: Ensure shared Redis stack is running, then deploy project stack
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
        console.print("  [yellow]Config saved. Re-run onboard to retry stack deployment.[/yellow]")
        sys.exit(1)

    console.print(f"\n[bold]Project '{project_name}' onboarded.[/bold]")


@fleet.command("add-worker")
@click.argument("project_name")
@click.option("--vm-id", type=int, default=None, help="Proxmox VM ID for the new worker.")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def add_worker(project_name: str, vm_id: int | None, config: str) -> None:
    """Add a worker VM to an existing project.

    Increments the worker count for PROJECT_NAME and applies Terraform
    to create the new VM.
    """
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    project = cfg.get_project(project_name)
    if not project:
        console.print(f"[red]Project '{project_name}' not found in fleet config.[/red]")
        sys.exit(1)

    # Prompt for VM ID
    if vm_id is None:
        default_id = _next_free_vmid() or 200
        vm_id = click.prompt("  VM ID for new worker", default=default_id, type=int)

    old_count = project.workers
    project.workers += 1
    project.worker_vm_ids.append(vm_id)

    console.print(f"\n[bold]Adding worker to '{project_name}'[/bold]")
    console.print(f"  Workers: {old_count} -> {project.workers} (VM ID {vm_id})")

    # Generate tfvars and apply Terraform
    console.print("  Applying Terraform...")
    try:
        from orcest.fleet.provisioner import apply, generate_tfvars, write_tfvars

        tfvars = generate_tfvars(cfg)
        write_tfvars(tfvars)
        apply()
        console.print("  Terraform apply [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Terraform apply [red]failed[/red]: {exc}")
        project.workers = old_count  # rollback
        project.worker_vm_ids.pop()
        sys.exit(1)

    save_config(cfg, config)
    console.print(
        f"\n[bold]Worker added to '{project_name}' (now {project.workers} workers).[/bold]"
    )


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
    """Destroy a project: remove orchestrator stack and all worker VMs.

    Tears down the Docker Compose stack on the orchestrator, removes the
    project from config, and applies Terraform to destroy worker VMs.
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
            f"Destroy project '{project_name}' ({project.workers} worker(s))?",
            abort=True,
        )

    console.print(f"\n[bold]Destroying project '{project_name}'[/bold]")

    # Step 1: Teardown orchestrator stack
    if cfg.orchestrator.host:
        ssh_target = cfg.ssh_target()
        console.print("  Tearing down orchestrator stack...", end=" ")
        try:
            from orcest.fleet.orchestrator import teardown_stack

            teardown_stack(ssh_target, project_name)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed: {exc}[/yellow]")

    # Step 2: Remove project from config and apply Terraform
    # (worker VMs no longer in tfvars, will be destroyed)
    cfg.projects = [p for p in cfg.projects if p.name != project_name]
    console.print("  Applying Terraform to remove workers...")
    try:
        from orcest.fleet.provisioner import apply, generate_tfvars, write_tfvars

        tfvars = generate_tfvars(cfg)
        write_tfvars(tfvars)
        apply()
        console.print("  Terraform apply [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Terraform apply [yellow]failed: {exc}[/yellow]")
        console.print(
            "  [yellow]Workers may still be running. Run 'tofu destroy' manually.[/yellow]"
        )

    # Save config only after Terraform has been applied (or attempted)
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
    """Update the fleet: rebuild Docker image, restart stacks, recreate workers.

    Uploads fresh source to the orchestrator, rebuilds the Docker image,
    restarts all project stacks, and applies Terraform to recreate workers
    with fresh cloud-init.
    """
    from orcest.fleet.config import load_config, save_config

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

    # Step 3: Restart all project stacks
    from orcest.fleet.orchestrator import restart_stack

    for project in cfg.projects:
        console.print(f"  Restarting stack for '{project.name}'...", end=" ")
        try:
            restart_stack(ssh_target, project.name)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed: {exc}[/yellow]")

    # Step 4: Regenerate tfvars and apply Terraform (ensures workers match current config)
    console.print("\n  Applying Terraform to ensure workers match current config...")
    try:
        from orcest.fleet.provisioner import apply, generate_tfvars, write_tfvars

        tfvars = generate_tfvars(cfg)
        write_tfvars(tfvars)
        apply()
        console.print("  Terraform apply [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Terraform apply [yellow]failed: {exc}[/yellow]")

    save_config(cfg, config)
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
    from orcest.fleet.config import load_config

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
            [
                "ssh",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "BatchMode=yes",
                ssh_target,
                "true",
            ],
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
    proj_table.add_column("Workers", style="green")
    proj_table.add_column("Stack Status", style="magenta")

    for project in cfg.projects:
        _validate_project_name(project.name)
        stack_status = "[dim]unknown[/dim]"
        if cfg.orchestrator.host:
            ssh_target = cfg.ssh_target()
            result = subprocess.run(
                [
                    "ssh",
                    "-o",
                    "ConnectTimeout=5",
                    "-o",
                    "BatchMode=yes",
                    ssh_target,
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
            str(project.workers),
            stack_status,
        )

    console.print(proj_table)


