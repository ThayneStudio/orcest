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
from pathlib import Path
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


def _humanize_bytes(n: float) -> str:
    """Format bytes as a human-readable string (e.g. '1.7 TiB')."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def _prompt_storage(
    px: ProxmoxClient,
    content_type: str,
    purpose: str,
    console: Console,
    default: str | None = None,
) -> str:
    """Interactively select a Proxmox storage pool.

    Queries available storage filtered by *content_type* (e.g. ``"images"``),
    displays a Rich table, and prompts the user to pick one.

    Args:
        px: Proxmox API client.
        content_type: Required content type (``"images"``, ``"snippets"``, etc.).
        purpose: Human description shown in the prompt (e.g. ``"template VM disk"``).
        console: Rich console for output.
        default: Pre-selected storage name (highlighted as default).

    Returns:
        The chosen storage name.
    """
    storages = px.list_storage(content_type=content_type)
    if not storages:
        console.print(f"[red]No storage found supporting '{content_type}' content.[/red]")
        raise SystemExit(1)

    if len(storages) == 1:
        name = storages[0]["storage"]
        console.print(f"  Storage for {purpose}: [green]{name}[/green] (only option)")
        return name

    # Build table
    table = Table(title=f"Available storage ({purpose})")
    table.add_column("#", style="bold", width=3)
    table.add_column("Name")
    table.add_column("Type")
    table.add_column("Free")

    default_idx = 0
    for i, s in enumerate(storages):
        if default and s["storage"] == default:
            default_idx = i
        table.add_row(
            str(i + 1),
            s["storage"],
            s.get("type", "?"),
            _humanize_bytes(s.get("avail", 0)),
        )

    console.print(table)
    choice = click.prompt(
        f"  Select storage for {purpose}",
        default=default_idx + 1,
        type=click.IntRange(1, len(storages)),
    )
    selected = storages[choice - 1]["storage"]
    return selected


def _find_snippet_storage(px: ProxmoxClient, console: Console) -> str:
    """Auto-detect a storage pool that supports cloud-init snippets.

    Returns the first enabled storage with ``snippets`` content type.
    """
    storages = px.list_storage(content_type="snippets")
    if not storages:
        console.print("[red]No storage found supporting 'snippets' content.[/red]")
        console.print("  Cloud-init requires a storage with snippets enabled (usually 'local').")
        raise SystemExit(1)
    name = storages[0]["storage"]
    console.print(f"  Snippet storage (cloud-init): [green]{name}[/green]")
    return name


def _get_vm_ip(vm_id: int, console: Console, timeout: int = 300) -> str | None:
    """Wait for a VM to get an IPv4 address via the QEMU guest agent.

    Polls ``qm guest cmd network-get-interfaces`` until a non-loopback
    IPv4 address appears or the timeout expires.  The guest agent must
    be installed and running — there is no fallback.
    """
    import json

    deadline = time.monotonic() + timeout
    console.print(f"  Waiting for VM {vm_id} to get an IP...", end=" ")

    while time.monotonic() < deadline:
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
    "-o",
    "ConnectTimeout=5",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "BatchMode=yes",
    "-o",
    "LogLevel=ERROR",
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
    docker_cmd = f"docker run --rm -i --entrypoint '' orcest {quoted}"
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
    host: str,
    user: str,
    console: Console,
    timeout: int = 600,
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


def _ensure_orchestrator_ssh(
    ssh_target: str,
    proxmox_ip: str,
    console: Console,
) -> None:
    """Ensure the orchestrator VM can SSH to the Proxmox host.

    The pool manager runs on the orchestrator and needs SSH access to the
    Proxmox host to write cloud-init snippets and run ``qm`` commands.

    Idempotent: skips if SSH already works.
    """
    # Quick check: does SSH already work?
    verify = subprocess.run(
        [
            "ssh",
            *_SSH_OPTS,
            ssh_target,
            f"ssh -o StrictHostKeyChecking=no -o BatchMode=yes"
            f" -o ConnectTimeout=3 root@{proxmox_ip} true",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if verify.returncode == 0:
        console.print("  Orchestrator SSH to Proxmox... [green]ok[/green]")
        return

    console.print("  Setting up orchestrator SSH to Proxmox...", end=" ")

    # Generate key if missing
    subprocess.run(
        [
            "ssh",
            *_SSH_OPTS,
            ssh_target,
            "test -f ~/.ssh/id_ed25519 || ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ''",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )

    # Read public key
    result = subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, "cat ~/.ssh/id_ed25519.pub"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0 or not result.stdout.strip():
        console.print("[red]failed (could not read key)[/red]")
        return
    pub_key = result.stdout.strip()

    # Add to Proxmox authorized_keys (local — this command runs on the Proxmox host)
    auth_keys = Path("/root/.ssh/authorized_keys")
    if auth_keys.exists() and pub_key in auth_keys.read_text():
        pass  # already present
    else:
        auth_keys.parent.mkdir(parents=True, exist_ok=True)
        with auth_keys.open("a") as f:
            f.write(f"\n{pub_key}\n")

    # Verify
    verify = subprocess.run(
        [
            "ssh",
            *_SSH_OPTS,
            ssh_target,
            f"ssh -o StrictHostKeyChecking=no -o BatchMode=yes"
            f" -o ConnectTimeout=5 root@{proxmox_ip} hostname",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if verify.returncode == 0:
        console.print("[green]ok[/green]")
    else:
        console.print("[yellow]failed (SSH verify failed)[/yellow]")
        console.print(f"    {verify.stderr.strip()}")


@click.group()
def fleet() -> None:
    """Manage the orcest fleet: orchestrators, workers, and VMs."""


@fleet.command("add-org")
@click.argument("org_name")
@click.option(
    "--github-token",
    required=True,
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
                cfg,
                ["orcest", "check", "github-token"],
                input_data=github_token + "\n",
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
    "--storage",
    default=None,
    help="Proxmox storage for VM disk (skip interactive prompt).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def create_orchestrator(vm_id: int | None, storage: str | None, config: str) -> None:
    """Create the orchestrator VM via Terraform and deploy the Docker stack."""
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    # Select storage for orchestrator VM disk
    if storage is None and cfg.proxmox.api_token_id and cfg.proxmox.api_token_secret:
        px = _create_proxmox_client(cfg)
        storage = _prompt_storage(
            px,
            "images",
            "orchestrator VM disk",
            console,
            default=cfg.proxmox.storage,
        )
    if storage:
        cfg.proxmox.storage = storage

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

    # Step 5b: Set up SSH from orchestrator to Proxmox host
    # (needed by pool manager to write cloud-init snippets)
    from urllib.parse import urlparse

    proxmox_ip = urlparse(cfg.proxmox.endpoint).hostname or "127.0.0.1"
    _ensure_orchestrator_ssh(ssh_target, proxmox_ip, console)

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
        if cfg.proxmox.is_localhost():
            console.print("  [yellow]Skipping pool manager: proxmox.endpoint is localhost[/yellow]")
            console.print(
                "  The pool manager runs on the orchestrator VM and needs the"
                " Proxmox host's real IP."
            )
            console.print("  Fix with: orcest init  (or edit /etc/orcest/config.yaml)")
        else:
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
            claude_token=org.claude_oauth_token,
        )
        config_yaml = generate_orchestrator_config(
            repo=repo,
            key_prefix=project_name,
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
        cfg.projects = [p for p in cfg.projects if p.name != project_name]
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
        if cfg.proxmox.is_localhost():
            console.print("  [yellow]Skipping pool manager: proxmox.endpoint is localhost[/yellow]")
            console.print(
                "  The pool manager runs on the orchestrator VM and needs the"
                " Proxmox host's real IP."
            )
            console.print("  Fix with: orcest init  (or edit /etc/orcest/config.yaml)")
        else:
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
                    "ssh",
                    *_SSH_OPTS,
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
    *,
    storage: str | None = None,
    snippet_storage: str = "local",
) -> None:
    """Download a cloud image and create a VM with it as the boot disk.

    Uses the Proxmox ``download-url`` API to fetch the image, then creates
    a VM with ``import-from`` to use the downloaded image as the boot disk.
    Disk is resized to ``cfg.pool.worker_disk_size``.

    Args:
        storage: Proxmox storage for the VM boot disk. Falls back to
            ``cfg.pool.storage``.
        snippet_storage: Proxmox storage for cloud-init drive.

    Raises on any failure — caller is responsible for destroying the VM.
    """
    from urllib.parse import urlparse

    parsed = urlparse(image_url)
    if parsed.scheme not in ("https", "http"):
        raise ValueError(f"Invalid image URL scheme: {parsed.scheme!r} (expected http or https)")

    if storage is None:
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

    # Step 2: Create VM (without disk — import-from requires root which
    # API tokens don't have, so we import the disk via qm CLI in step 3)
    console.print("  Creating VM...", end=" ")
    px.create_vm(
        vm_id=vm_id,
        name="orcest-worker-template",
        memory=cfg.pool.worker_memory,
        cores=cfg.pool.worker_cores,
        cpu="host",
        scsihw="virtio-scsi-pci",
        ide2=f"{storage}:cloudinit",
        net0="virtio,bridge=vmbr0",
        ipconfig0="ip=dhcp",
        serial0="socket",
        vga="serial0",
        agent="1",
    )
    console.print("[green]ok[/green]")

    # Step 3: Import cloud image as boot disk via qm CLI (runs as root
    # on the Proxmox host, bypassing API token filesystem path restrictions)
    image_path = f"/var/lib/vz/template/iso/{filename}"
    console.print("  Importing boot disk...", end=" ")
    result = subprocess.run(
        [
            "qm",
            "set",
            str(vm_id),
            "--scsi0",
            f"{storage}:0,import-from={image_path},discard=on,ssd=1",
            "--boot",
            "order=scsi0",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"qm set failed: {(result.stderr or result.stdout).strip()}")
    console.print("[green]ok[/green]")

    # Step 4: Resize disk to configured worker size
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
    "--storage",
    default=None,
    help="Proxmox storage for VM disk (skip interactive prompt).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def create_template(vm_id: int | None, image_url: str, storage: str | None, config: str) -> None:
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

    # Select storage for VM disk
    if storage is None:
        storage = _prompt_storage(
            px,
            "images",
            "template VM disk",
            console,
            default=cfg.pool.storage,
        )
    cfg.pool.storage = storage

    # Auto-detect snippet storage for cloud-init
    snippet_storage = _find_snippet_storage(px, console)
    cfg.pool.snippet_storage = snippet_storage

    # Prompt for template VM ID
    if vm_id is None:
        default_id = _next_free_vmid()
        vm_id = click.prompt("  VM ID for new template", default=default_id, type=int)

    # Check if VM already exists and offer to replace
    existing_vms = {int(v["vmid"]) for v in px.list_vms() if "vmid" in v}
    if vm_id in existing_vms:
        if not click.confirm(f"  VM {vm_id} already exists. Destroy and replace it?"):
            console.print("Aborted.")
            sys.exit(0)
        console.print(f"  Destroying existing VM {vm_id}...", end=" ")
        try:
            px.stop_vm(vm_id)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if px.get_vm_status(vm_id) == "stopped":
                    break
                time.sleep(1)
        except Exception:
            pass  # May already be stopped or a template
        px.destroy_vm(vm_id)
        console.print("[green]ok[/green]")

    console.print(f"\n[bold]Creating worker template (VM {vm_id})[/bold]\n")

    # Step 1: Create VM from cloud image
    try:
        _create_vm_from_cloud_image(
            px,
            cfg,
            vm_id,
            image_url,
            console,
            storage=storage,
            snippet_storage=snippet_storage,
        )
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
        _set_vm_cloud_init(px, vm_id, userdata, snippet_storage=snippet_storage)
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

    # Step 4: Wait for IP (uses ARP fallback so we don't need to wait
    # for cloud-init to install qemu-guest-agent first)
    vm_ip = _get_vm_ip(vm_id, console, timeout=600)
    if not vm_ip:
        console.print("  Could not get VM IP. Template creation aborted.")
        _cleanup_vm()
        sys.exit(1)

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

    # Step 7: Clean cloud-init state so clones run fresh cloud-init
    # (clones get per-VM cloud-init userdata from the pool manager)
    console.print("  Cleaning cloud-init state...", end=" ")
    result = _ssh_run(vm_ip, cfg.orchestrator.user, "sudo rm -rf /var/lib/cloud/*")
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        _cleanup_vm()
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 7b: Prepare template for unique clone identities.
    # Clear machine-id so each linked clone gets a unique one on first boot.
    # (The netplan DHCP fix is handled via cloud-init write_files in cloud_init.py.)
    console.print("  Preparing template for cloning...", end=" ")
    result = _ssh_run(
        vm_ip,
        cfg.orchestrator.user,
        "sudo truncate -s 0 /etc/machine-id && sudo rm -f /var/lib/dbus/machine-id",
    )
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        _cleanup_vm()
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 8: Flush filesystem and gracefully shut down the VM.
    # A hard stop (qm stop) can lose up to 30 seconds of unflushed ext4
    # writes (commit=30 in the Ubuntu cloud image fstab), which corrupts
    # the venv and other recently-written files on ZFS-backed storage.
    console.print("  Syncing filesystem...", end=" ")
    result = _ssh_run(vm_ip, cfg.orchestrator.user, "sudo sync")
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    console.print("  Shutting down VM...", end=" ")
    try:
        px.shutdown_vm(vm_id, timeout=60)
        if px.get_vm_status(vm_id) != "stopped":
            console.print("[red]VM did not stop[/red]")
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

    # Step 10: Save template_vm_id and prompt for worker VM ID range
    cfg.pool.template_vm_id = vm_id
    default_start = vm_id + 1
    vm_id_start = click.prompt(
        "  Worker VM ID range starts at",
        default=default_start,
        type=int,
    )
    cfg.pool.vm_id_start = vm_id_start
    save_config(cfg, config)

    console.print(f"\n[bold]Worker template created (VM {vm_id}).[/bold]")
    console.print(f"  Saved template_vm_id={vm_id}, vm_id_start={vm_id_start} to fleet config.")


def _set_vm_cloud_init(
    px: ProxmoxClient,
    vm_id: int,
    userdata: str,
    snippet_storage: str = "local",
) -> None:
    """Set cloud-init user-data on a VM.

    Writes the snippet directly to the Proxmox host filesystem and
    configures ``cicustom`` via ``qm set``. This avoids the snippet
    upload API which can fail with certain API token configurations.

    Args:
        snippet_storage: Proxmox storage name for snippets (default ``"local"``).
    """
    from pathlib import Path

    snippet_name = f"orcest-template-{vm_id}-user.yaml"
    snippets_dir = Path("/var/lib/vz/snippets")
    snippets_dir.mkdir(parents=True, exist_ok=True)
    (snippets_dir / snippet_name).write_text(userdata)
    result = subprocess.run(
        ["qm", "set", str(vm_id), "--cicustom", f"user={snippet_storage}:snippets/{snippet_name}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"qm set --cicustom failed: {(result.stderr or result.stdout).strip()}")


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
            "\n[yellow]Proxmox API credentials not configured -- cannot query VMs.[/yellow]"
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
            vm for vm in px.list_vms(name_prefix="orcest-worker-") if not vm.get("template", False)
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
@click.option("--vm-id-start", type=int, default=None, help="First VM ID for worker clones.")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def set_pool_size(size: int, vm_id_start: int | None, config: str) -> None:
    """Set the target warm pool size."""
    from orcest.fleet.config import load_config, save_config

    console = Console()

    if size < 0:
        console.print("[red]Pool size must be non-negative.[/red]")
        sys.exit(1)

    cfg = load_config(config)
    old_size = cfg.pool.size
    cfg.pool.size = size
    if vm_id_start is not None:
        cfg.pool.vm_id_start = vm_id_start
        console.print(f"Worker VM ID start: {vm_id_start}")
    save_config(cfg, config)

    console.print(f"Pool size updated: {old_size} -> {size}")


@fleet.command()
@click.option(
    "--drain-active",
    is_flag=True,
    help="Also destroy active workers (interrupts running tasks).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def stop(drain_active: bool, config: str) -> None:
    """Stop the pool manager and destroy idle worker VMs."""
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import (
        clean_pending_tasks,
        clean_pool_redis,
        get_pool_redis_members,
        stop_pool_manager,
    )

    console = Console()
    cfg = load_config(config)

    if not cfg.orchestrator.host:
        console.print("[red]Orchestrator host not set.[/red]")
        console.print("  Run: orcest fleet create-orchestrator")
        sys.exit(1)

    ssh_target = cfg.ssh_target()

    # Step 1: Stop pool manager
    console.print("  Stopping pool manager...", end=" ")
    try:
        stop_pool_manager(ssh_target)
        console.print("[green]ok[/green]")
    except RuntimeError as exc:
        console.print(f"[yellow]warning[/yellow]: {exc}")

    # Step 2: Read Redis state
    console.print("  Reading pool state...", end=" ")
    try:
        idle_ids, active_ids = get_pool_redis_members(ssh_target)
        console.print(f"[green]ok[/green] ({len(idle_ids)} idle, {len(active_ids)} active)")
    except Exception as exc:
        console.print(f"[yellow]warning[/yellow]: {exc}")
        idle_ids, active_ids = set(), {}

    # Step 3: Destroy worker VMs
    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print(
            "[yellow]Proxmox API credentials not configured — skipping VM destruction.[/yellow]"
        )
        console.print("  VMs must be destroyed manually or via Proxmox UI.")
        return

    px = _create_proxmox_client(cfg)
    worker_vms = px.list_vms(name_prefix="orcest-worker-")
    # Exclude the template itself
    worker_vms = [v for v in worker_vms if int(v.get("vmid", 0)) != cfg.pool.template_vm_id]

    destroyed: list[str] = []
    skipped: list[str] = []
    for vm in worker_vms:
        vm_id = int(vm["vmid"])
        vm_id_str = str(vm_id)
        is_idle = vm_id_str in idle_ids
        is_active = vm_id_str in active_ids

        if is_active and not drain_active:
            console.print(f"  Leaving active VM {vm_id} ({vm.get('name', '')})")
            skipped.append(vm_id_str)
            continue

        label = "active" if is_active else ("idle" if is_idle else "orphan")
        console.print(f"  Destroying {label} VM {vm_id}...", end=" ")
        try:
            try:
                px.stop_vm(vm_id)
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if px.get_vm_status(vm_id) == "stopped":
                        break
                    time.sleep(1)
            except Exception:
                pass
            px.destroy_vm(vm_id)
            console.print("[green]ok[/green]")
            destroyed.append(vm_id_str)
        except Exception as exc:
            console.print(f"[yellow]failed[/yellow]: {exc}")

    # Step 4: Clean Redis
    if destroyed:
        console.print("  Cleaning Redis state...", end=" ")
        try:
            clean_pool_redis(ssh_target, destroyed)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]warning[/yellow]: {exc}")

    # Step 5: Clean pending task markers
    console.print("  Cleaning pending tasks...", end=" ")
    try:
        count = clean_pending_tasks(ssh_target)
        console.print(f"[green]ok[/green] ({count} cleared)")
    except Exception as exc:
        console.print(f"[yellow]warning[/yellow]: {exc}")

    console.print(f"\n  Destroyed {len(destroyed)} VMs", end="")
    if skipped:
        console.print(f", left {len(skipped)} active", end="")
    console.print(".")


@fleet.command()
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def start(config: str) -> None:
    """Start the pool manager.

    Uploads the current fleet config and starts the pool manager, which
    will begin cloning worker VMs to reach the target pool size.
    """
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import ensure_pool_manager, upload_fleet_config

    console = Console()
    cfg = load_config(config)

    if not cfg.orchestrator.host:
        console.print("[red]Orchestrator host not set.[/red]")
        console.print("  Run: orcest fleet create-orchestrator")
        sys.exit(1)

    if not cfg.pool.template_vm_id:
        console.print("[red]No worker template configured.[/red]")
        console.print("  Run: orcest fleet create-template")
        sys.exit(1)

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Proxmox API credentials not configured.[/red]")
        console.print("  Run: orcest init")
        sys.exit(1)

    if cfg.proxmox.is_localhost():
        console.print(
            "[red]Proxmox endpoint is localhost — unreachable from orchestrator VM.[/red]"
        )
        console.print("  Run: orcest init")
        sys.exit(1)

    ssh_target = cfg.ssh_target()

    # Ensure orchestrator can SSH to Proxmox host (for cloud-init snippets)
    from urllib.parse import urlparse

    proxmox_ip = urlparse(cfg.proxmox.endpoint).hostname or "127.0.0.1"
    _ensure_orchestrator_ssh(ssh_target, proxmox_ip, console)

    console.print("  Uploading fleet config...", end=" ")
    try:
        upload_fleet_config(ssh_target, config)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        sys.exit(1)

    console.print("  Starting pool manager...", end=" ")
    try:
        ensure_pool_manager(ssh_target)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        sys.exit(1)

    console.print(f"\n  Pool manager started (target size: {cfg.pool.size}).")


@fleet.command()
@click.option(
    "--rebuild-template",
    is_flag=True,
    help="Also recreate the worker template VM.",
)
@click.option(
    "--drain-active",
    is_flag=True,
    help="Destroy active workers (interrupts running tasks).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@click.pass_context
def deploy(ctx: click.Context, rebuild_template: bool, drain_active: bool, config: str) -> None:
    """Full deploy: upgrade CLI, rebuild images, restart fleet.

    Runs the full deployment sequence in order:

    \b
      1. Upgrade CLI (pip install from GitHub)
      2. Stop fleet (stop pool manager, destroy workers, clean Redis)
      3. Update orchestrator (upload source, rebuild Docker image, restart stacks)
      4. Rebuild template (only with --rebuild-template)
      5. Start fleet (start pool manager, begin cloning workers)
    """
    console = Console()

    total = 5 if rebuild_template else 4
    step = 0

    # Step 1: Upgrade CLI
    step += 1
    console.print(f"\n[bold]Step {step}/{total}: Upgrading CLI[/bold]\n")
    _upgrade_cli(console)

    # Step 2: Stop fleet
    step += 1
    console.print(f"\n[bold]Step {step}/{total}: Stopping fleet[/bold]\n")
    ctx.invoke(stop, drain_active=drain_active, config=config)

    # Step 3: Update orchestrator
    step += 1
    console.print(f"\n[bold]Step {step}/{total}: Updating orchestrator[/bold]\n")
    ctx.invoke(update, config=config)

    # Step 4: Rebuild template (optional)
    if rebuild_template:
        step += 1
        console.print(f"\n[bold]Step {step}/{total}: Rebuilding template[/bold]\n")
        ctx.invoke(create_template, config=config)

    # Step 5: Start fleet
    step += 1
    console.print(f"\n[bold]Step {step}/{total}: Starting fleet[/bold]\n")
    ctx.invoke(start, config=config)

    console.print("\n[bold green]Deploy complete.[/bold green]")


def _upgrade_cli(console: Console) -> None:
    """Upgrade the orcest CLI from GitHub."""
    import subprocess
    from pathlib import Path

    console.print("  Installing latest version...", end=" ")
    pip = Path(sys.executable).parent / "pip"
    result = subprocess.run(
        [
            str(pip),
            "install",
            "--quiet",
            "--no-cache-dir",
            "--force-reinstall",
            "git+https://github.com/ThayneStudio/orcest.git",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console.print("[red]failed[/red]")
        console.print(f"    {result.stderr.strip()}")
        raise SystemExit(1)
    console.print("[green]ok[/green]")
