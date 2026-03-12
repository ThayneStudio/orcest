"""Fleet management CLI commands.

Provides ``orcest fleet`` subcommands for managing the fleet of
orchestrator stacks and disposable worker VMs from a laptop.
"""

from __future__ import annotations

import os
import sys

import click
from rich.console import Console
from rich.table import Table


def _repo_to_project_name(repo: str) -> str:
    """Derive project name from repo (e.g. 'ThayneStudio/my-project' -> 'my-project')."""
    return repo.rsplit("/", 1)[-1]


@click.group()
def fleet() -> None:
    """Manage the orcest fleet: orchestrators, workers, and VMs."""


@fleet.command()
@click.argument("repo")
@click.option("--name", default=None, help="Project name (default: derived from repo).")
@click.option("--inventory", default="/opt/orcest/fleet.yaml", help="Fleet inventory path.")
@click.option("--github-token", envvar="GITHUB_TOKEN", required=True, help="GitHub token.")
@click.option(
    "--claude-token",
    envvar="CLAUDE_CODE_OAUTH_TOKEN",
    required=True,
    help="Claude OAuth token.",
)
@click.option(
    "--rebuild-image",
    is_flag=True,
    help="Force rebuild of the orchestrator Docker image.",
)
def onboard(
    repo: str,
    name: str | None,
    inventory: str,
    github_token: str,
    claude_token: str,
    rebuild_image: bool,
) -> None:
    """Onboard a new repo: deploy orchestrator stack + create first worker VM.

    REPO is in "owner/repo" format (e.g. ThayneStudio/my-project).
    Requires the orchestrator host to be provisioned first (provision-orchestrator).
    """
    from orcest.fleet.cloud_init import render_worker_userdata
    from orcest.fleet.inventory import (
        FleetInventory,
        ProjectEntry,
        WorkerEntry,
        load_inventory,
        save_inventory,
    )
    from orcest.fleet.orchestrator_deploy import deploy_project_stack

    console = Console()
    project_name = name or _repo_to_project_name(repo)

    console.print(f"\n[bold]Onboarding {repo} as '{project_name}'[/bold]\n")

    # Load or create inventory
    inv_path = inventory
    if os.path.exists(inv_path):
        inv = load_inventory(inv_path)
    else:
        inv = FleetInventory()

    # Require orchestrator_host to be set
    if not inv.orchestrator_host:
        console.print(
            "[red]orchestrator_host not set in fleet inventory.[/red]\n"
            "  Run 'orcest provision-orchestrator' first,"
            " then set orchestrator_host in the inventory."
        )
        sys.exit(1)

    # Check for duplicate
    if inv.get_project(project_name):
        console.print(f"[red]Project '{project_name}' already exists in fleet inventory.[/red]")
        sys.exit(1)

    # Allocate resources
    redis_port = inv.next_redis_port()
    vm_id = inv.next_vm_id()
    worker_id = f"worker-{vm_id}"

    console.print(f"  Redis port: {redis_port}")
    console.print(f"  Worker VM ID: {vm_id}")
    console.print(f"  Worker ID: {worker_id}")

    # Add project to inventory
    project = ProjectEntry(
        name=project_name,
        repo=repo,
        redis_port=redis_port,
        workers=[WorkerEntry(vm_id=vm_id)],
    )
    inv.projects.append(project)
    save_inventory(inv, inv_path)
    console.print("  Inventory updated.")

    # Deploy orchestrator stack (Redis + orchestrator container)
    deploy_project_stack(
        host=inv.orchestrator_host,
        user=inv.orchestrator_user,
        project_name=project_name,
        redis_port=redis_port,
        repo=repo,
        github_token=github_token,
        console=console,
        rebuild_image=rebuild_image,
    )

    # Generate cloud-init user-data for the first worker VM
    userdata = render_worker_userdata(
        redis_host=inv.orchestrator_host,
        redis_port=redis_port,
        worker_id=worker_id,
        github_token=github_token,
        claude_oauth_token=claude_token,
        repo=repo,
    )

    # Write user-data to temp file for Proxmox upload (0600: contains secrets)
    userdata_path = f"/tmp/orcest-worker-{vm_id}-userdata.yaml"
    fd = os.open(userdata_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(userdata)

    console.print(f"\n  Cloud-init user-data written to {userdata_path}")

    # If Proxmox is configured, create the VM
    if inv.proxmox.host and inv.proxmox.token_id:
        from orcest.fleet.proxmox import ProxmoxClient

        console.print("\n  Creating worker VM via Proxmox API...")
        px = ProxmoxClient(inv.proxmox)

        # Upload snippet
        snippet_name = f"worker-{vm_id}.yaml"
        snippet_path = px.upload_snippet(snippet_name, userdata)
        console.print(f"  Uploaded cloud-init snippet: {snippet_path}")

        # Create VM
        px.create_vm(
            vm_id=vm_id,
            name=f"orcest-{project_name}-worker-{vm_id}",
            cicustom=f"user={snippet_path}",
        )
        console.print(f"  VM {vm_id} created and starting.")

        # Wait for IP
        console.print("  Waiting for VM IP...")
        ip = px.get_vm_ip(vm_id)
        if ip:
            console.print(f"  VM IP: [green]{ip}[/green]")
        else:
            console.print("  [yellow]Could not detect IP (VM may still be booting).[/yellow]")
    else:
        console.print("\n  [yellow]Proxmox not configured — skipping VM creation.[/yellow]")
        console.print(f"  Use the cloud-init file at {userdata_path} to provision manually.")

    console.print(f"\n[bold]Project '{project_name}' onboarded.[/bold]")


@fleet.command("add-worker")
@click.argument("project_name")
@click.option("--inventory", default="/opt/orcest/fleet.yaml", help="Fleet inventory path.")
@click.option("--github-token", envvar="GITHUB_TOKEN", required=True, help="GitHub token.")
@click.option(
    "--claude-token",
    envvar="CLAUDE_CODE_OAUTH_TOKEN",
    required=True,
    help="Claude OAuth token.",
)
def add_worker(project_name: str, inventory: str, github_token: str, claude_token: str) -> None:
    """Add a worker VM to an existing project."""
    from orcest.fleet.cloud_init import render_worker_userdata
    from orcest.fleet.inventory import WorkerEntry, load_inventory, save_inventory

    console = Console()
    inv = load_inventory(inventory)

    project = inv.get_project(project_name)
    if not project:
        console.print(f"[red]Project '{project_name}' not found in fleet inventory.[/red]")
        sys.exit(1)

    vm_id = inv.next_vm_id()
    worker_id = f"worker-{vm_id}"

    console.print(f"\n[bold]Adding worker to '{project_name}'[/bold]")
    console.print(f"  VM ID: {vm_id}")
    console.print(f"  Worker ID: {worker_id}")

    project.workers.append(WorkerEntry(vm_id=vm_id))
    save_inventory(inv, inventory)

    userdata = render_worker_userdata(
        redis_host=inv.orchestrator_host or "localhost",
        redis_port=project.redis_port,
        worker_id=worker_id,
        github_token=github_token,
        claude_oauth_token=claude_token,
        repo=project.repo,
    )

    if inv.proxmox.host and inv.proxmox.token_id:
        from orcest.fleet.proxmox import ProxmoxClient

        px = ProxmoxClient(inv.proxmox)
        snippet_name = f"worker-{vm_id}.yaml"
        snippet_path = px.upload_snippet(snippet_name, userdata)
        px.create_vm(
            vm_id=vm_id,
            name=f"orcest-{project_name}-worker-{vm_id}",
            cicustom=f"user={snippet_path}",
        )
        console.print(f"  VM {vm_id} created and starting.")

        ip = px.get_vm_ip(vm_id)
        if ip:
            console.print(f"  VM IP: [green]{ip}[/green]")
    else:
        userdata_path = f"/tmp/orcest-worker-{vm_id}-userdata.yaml"
        fd = os.open(userdata_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(userdata)
        console.print(f"  Cloud-init user-data written to {userdata_path}")

    console.print(f"\n[bold]Worker {vm_id} added to '{project_name}'.[/bold]")


@fleet.command()
@click.option("--inventory", default="/opt/orcest/fleet.yaml", help="Fleet inventory path.")
def status(inventory: str) -> None:
    """Show fleet status: projects, workers, and VM states."""
    from orcest.fleet.inventory import load_inventory

    console = Console()
    inv = load_inventory(inventory)

    if not inv.projects:
        console.print("[dim]No projects in fleet inventory.[/dim]")
        return

    table = Table(title="Fleet Status")
    table.add_column("Project", style="cyan")
    table.add_column("Repo", style="white")
    table.add_column("Redis Port", style="yellow")
    table.add_column("Workers", style="green")
    table.add_column("VM IDs", style="magenta")

    for project in inv.projects:
        vm_ids = ", ".join(str(w.vm_id) for w in project.workers) or "none"
        table.add_row(
            project.name,
            project.repo,
            str(project.redis_port),
            str(len(project.workers)),
            vm_ids,
        )

    console.print(table)

    # If Proxmox is configured, show VM statuses
    if inv.proxmox.host and inv.proxmox.token_id:
        try:
            from orcest.fleet.proxmox import ProxmoxClient

            px = ProxmoxClient(inv.proxmox)
            vm_table = Table(title="VM Status")
            vm_table.add_column("VM ID", style="cyan")
            vm_table.add_column("Project", style="white")
            vm_table.add_column("Status", style="yellow")
            vm_table.add_column("IP", style="green")

            for project in inv.projects:
                for worker in project.workers:
                    try:
                        vm_status = px.get_vm_status(worker.vm_id)
                        ip = ""
                        if vm_status == "running":
                            ip = px.get_vm_ip(worker.vm_id, timeout=5) or ""
                    except Exception:
                        vm_status = "unknown"
                        ip = ""
                    vm_table.add_row(str(worker.vm_id), project.name, vm_status, ip)

            console.print(vm_table)
        except ImportError:
            console.print("[yellow]proxmoxer not installed — skipping VM status.[/yellow]")


@fleet.command()
@click.argument("project_name")
@click.option("--inventory", default="/opt/orcest/fleet.yaml", help="Fleet inventory path.")
@click.confirmation_option(prompt="Are you sure you want to destroy this project?")
def destroy(project_name: str, inventory: str) -> None:
    """Destroy a project: remove orchestrator stack and all worker VMs."""
    from orcest.fleet.inventory import load_inventory, save_inventory
    from orcest.fleet.orchestrator_deploy import destroy_project_stack

    console = Console()
    inv = load_inventory(inventory)

    project = inv.get_project(project_name)
    if not project:
        console.print(f"[red]Project '{project_name}' not found.[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Destroying project '{project_name}'[/bold]")

    # Tear down orchestrator stack on the orchestrator host
    if inv.orchestrator_host:
        destroy_project_stack(
            host=inv.orchestrator_host,
            user=inv.orchestrator_user,
            project_name=project_name,
            redis_port=project.redis_port,
            console=console,
        )

    # Destroy worker VMs via Proxmox if configured
    if inv.proxmox.host and inv.proxmox.token_id:
        try:
            from orcest.fleet.proxmox import ProxmoxClient

            px = ProxmoxClient(inv.proxmox)
            for worker in project.workers:
                try:
                    console.print(f"  Destroying VM {worker.vm_id}...", end=" ")
                    px.destroy_vm(worker.vm_id)
                    console.print("[green]ok[/green]")
                except Exception as e:
                    console.print(f"[yellow]failed: {e}[/yellow]")
        except ImportError:
            console.print("[yellow]proxmoxer not installed — skipping VM destruction.[/yellow]")

    # Remove from inventory
    inv.projects = [p for p in inv.projects if p.name != project_name]
    save_inventory(inv, inventory)

    console.print(f"\n[bold]Project '{project_name}' destroyed.[/bold]")


@fleet.command()
@click.option("--inventory", default="/opt/orcest/fleet.yaml", help="Fleet inventory path.")
@click.option("--github-token", envvar="GITHUB_TOKEN", required=True, help="GitHub token.")
@click.option(
    "--claude-token",
    envvar="CLAUDE_CODE_OAUTH_TOKEN",
    required=True,
    help="Claude OAuth token.",
)
@click.option(
    "--rebuild-image",
    is_flag=True,
    help="Rebuild the orchestrator Docker image and restart all orchestrator stacks.",
)
def update(inventory: str, github_token: str, claude_token: str, rebuild_image: bool) -> None:
    """Rolling-replace all worker VMs with fresh cloud-init instances.

    For each project, creates new worker VMs, waits for them to come up,
    then destroys the old ones. With --rebuild-image, also rebuilds the
    shared orchestrator Docker image and restarts all orchestrator stacks.
    """
    from orcest.fleet.cloud_init import render_worker_userdata
    from orcest.fleet.inventory import WorkerEntry, load_inventory, save_inventory

    console = Console()
    inv = load_inventory(inventory)

    if not inv.projects:
        console.print("[dim]No projects to update.[/dim]")
        return

    if not (inv.proxmox.host and inv.proxmox.token_id):
        console.print("[red]Proxmox not configured — cannot perform rolling replace.[/red]")
        sys.exit(1)

    # Rebuild orchestrator image and restart stacks if requested
    if rebuild_image and inv.orchestrator_host:
        from orcest.fleet.orchestrator_deploy import rebuild_image as _rebuild_image

        _rebuild_image(inv.orchestrator_host, inv.orchestrator_user, console)
        ssh_target = f"{inv.orchestrator_user}@{inv.orchestrator_host}"
        for project in inv.projects:
            project_dir = f"/opt/orcest/projects/{project.name}"
            console.print(f"  Restarting orchestrator stack for '{project.name}'...", end=" ")
            import subprocess

            cmd = f"sudo -u orcest bash -c 'cd {project_dir} && docker compose up -d'"
            result = subprocess.run(
                ["ssh", ssh_target, cmd],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                console.print("[green]ok[/green]")
            else:
                console.print("[yellow]failed (stack may not exist yet)[/yellow]")

    from orcest.fleet.proxmox import ProxmoxClient

    px = ProxmoxClient(inv.proxmox)

    for project in inv.projects:
        console.print(f"\n[bold]Updating '{project.name}'[/bold]")
        old_workers = list(project.workers)
        new_workers: list[WorkerEntry] = []

        for old_worker in old_workers:
            new_vm_id = inv.next_vm_id()
            worker_id = f"worker-{new_vm_id}"

            console.print(f"  Creating replacement VM {new_vm_id} for VM {old_worker.vm_id}...")

            userdata = render_worker_userdata(
                redis_host=inv.orchestrator_host or "localhost",
                redis_port=project.redis_port,
                worker_id=worker_id,
                github_token=github_token,
                claude_oauth_token=claude_token,
                repo=project.repo,
            )

            snippet_name = f"worker-{new_vm_id}.yaml"
            snippet_path = px.upload_snippet(snippet_name, userdata)
            px.create_vm(
                vm_id=new_vm_id,
                name=f"orcest-{project.name}-worker-{new_vm_id}",
                cicustom=f"user={snippet_path}",
            )

            new_entry = WorkerEntry(vm_id=new_vm_id)
            new_workers.append(new_entry)
            # Track in inventory immediately so next_vm_id sees it
            project.workers.append(new_entry)

            ip = px.get_vm_ip(new_vm_id, timeout=60)
            if ip:
                console.print(f"  New VM {new_vm_id} up at {ip}")
            else:
                console.print(f"  [yellow]New VM {new_vm_id} booting (no IP yet)[/yellow]")

        # Destroy old VMs
        for old_worker in old_workers:
            try:
                console.print(f"  Destroying old VM {old_worker.vm_id}...", end=" ")
                px.destroy_vm(old_worker.vm_id)
                console.print("[green]ok[/green]")
            except Exception as e:
                console.print(f"[yellow]failed: {e}[/yellow]")

        # Update inventory: keep only new workers
        project.workers = new_workers

    save_inventory(inv, inventory)
    console.print("\n[bold]Fleet update complete.[/bold]")


@fleet.command("add-runner")
@click.option("--org-url", required=True, help="GitHub org URL.")
@click.option("--runner-token", required=True, help="Runner registration token.")
@click.option("--runner-name", default="", help="Runner name (default: hostname).")
@click.option("--labels", default="self-hosted,linux", help="Runner labels.")
@click.option("--inventory", default="/opt/orcest/fleet.yaml", help="Fleet inventory path.")
@click.option("--vm-id", default=None, type=int, help="VM ID (default: auto).")
def add_runner(
    org_url: str,
    runner_token: str,
    runner_name: str,
    labels: str,
    inventory: str,
    vm_id: int | None,
) -> None:
    """Create a self-hosted GitHub Actions runner VM via Proxmox."""
    from orcest.fleet.inventory import load_inventory
    from orcest.fleet.runner_cloud_init import render_runner_userdata

    console = Console()
    inv = load_inventory(inventory)

    if vm_id is None:
        vm_id = inv.next_vm_id()

    runner_vm_name = runner_name or f"orcest-runner-{vm_id}"

    console.print(f"\n[bold]Creating runner VM {vm_id}[/bold]")
    console.print(f"  Name: {runner_vm_name}")
    console.print(f"  Labels: {labels}")

    userdata = render_runner_userdata(
        org_url=org_url,
        runner_token=runner_token,
        runner_name=runner_vm_name,
        runner_labels=labels,
    )

    if inv.proxmox.host and inv.proxmox.token_id:
        from orcest.fleet.proxmox import ProxmoxClient

        px = ProxmoxClient(inv.proxmox)
        snippet_name = f"runner-{vm_id}.yaml"
        snippet_path = px.upload_snippet(snippet_name, userdata)
        px.create_vm(
            vm_id=vm_id,
            name=runner_vm_name,
            cicustom=f"user={snippet_path}",
        )
        console.print(f"  VM {vm_id} created and starting.")

        ip = px.get_vm_ip(vm_id)
        if ip:
            console.print(f"  VM IP: [green]{ip}[/green]")
    else:
        userdata_path = f"/tmp/orcest-runner-{vm_id}-userdata.yaml"
        fd = os.open(userdata_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(userdata)
        console.print(f"  Cloud-init user-data written to {userdata_path}")

    console.print(f"\n[bold]Runner VM {vm_id} created.[/bold]")
