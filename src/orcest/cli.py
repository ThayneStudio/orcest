"""CLI entry point for orcest."""

import re
import sys

import click
from rich.console import Console
from rich.table import Table

from orcest.fleet.cli import fleet

_SSH_INPUT_RE = re.compile(r"^[a-zA-Z0-9._-]+$")


def _validate_ssh_input(value: str, label: str) -> None:
    """Raise click.BadParameter if value contains shell metacharacters."""
    if not _SSH_INPUT_RE.match(value):
        raise click.BadParameter(
            f"Invalid {value!r}: only alphanumerics, dots, hyphens, and underscores are allowed.",
            param_hint=repr(label),
        )


@click.group()
def main():
    """Orcest: Autonomous CI/CD orchestration system."""


@main.command()
@click.option("--config", default="config/orchestrator.yaml", help="Path to orchestrator config.")
def orchestrate(config):
    """Start the orchestrator loop."""
    from orcest.orchestrator.loop import run_orchestrator
    from orcest.shared.config import load_orchestrator_config

    cfg = load_orchestrator_config(config)
    run_orchestrator(cfg)


@main.command()
@click.option("--id", "worker_id", required=True, help="Unique worker identifier.")
@click.option("--config", default="config/worker.yaml", help="Path to worker config.")
@click.option("--runner", default=None, help="Runner type override (claude, noop, etc.)")
def work(worker_id, config, runner):
    """Start a worker loop."""
    from orcest.shared.config import load_worker_config
    from orcest.worker.loop import run_worker

    cfg = load_worker_config(config)
    cfg.worker_id = worker_id
    if runner:
        cfg.runner.type = runner
        cfg.backend = runner
    run_worker(cfg)


@main.command()
@click.argument("redis_host", required=False, default=None)
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for Redis).")
@click.option("--once", is_flag=True, help="Print status once and exit (no TUI).")
@click.option("--interval", default=3.0, type=float, help="TUI refresh interval in seconds.")
def status(redis_host, config, once, interval):
    """Show system status: workers, queue depth, active tasks.

    Connects to Redis directly via REDIS_HOST (e.g. 10.20.0.19 or 10.20.0.19:6380),
    or falls back to --config file. Launches a live TUI dashboard by default.
    Use --once for single-shot output.
    """
    from orcest.shared.config import RedisConfig
    from orcest.shared.redis_client import RedisClient

    if redis_host:
        if ":" in redis_host:
            host, port_str = redis_host.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                click.echo(f"Error: Invalid port number: {port_str}", err=True)
                raise SystemExit(1)
        else:
            host, port = redis_host, 6379
        redis_cfg = RedisConfig(host=host, port=port, db=0)
    else:
        from orcest.shared.config import load_orchestrator_config

        cfg = load_orchestrator_config(config)
        redis_cfg = cfg.redis

    redis = RedisClient(redis_cfg)

    if not redis.health_check():
        redis.close()
        click.echo("Error: Cannot connect to Redis.", err=True)
        raise SystemExit(1)

    try:
        if once:
            _status_once(redis)
        else:
            if interval <= 0:
                click.echo("Error: --interval must be positive.", err=True)
                raise SystemExit(1)
            from orcest.dashboard import run_dashboard

            run_dashboard(redis, refresh_interval=interval)
    finally:
        redis.close()


def _status_once(redis):
    """Print system status once and exit (original behavior)."""
    import redis as redis_lib

    console = Console(file=sys.stdout)
    client = redis.client

    task_streams = list(client.scan_iter(match="tasks:*"))
    try:
        results_len = client.xlen("results") or 0
    except redis_lib.ResponseError:
        # WRONGTYPE: results key exists but is not a stream
        results_len = "(not a stream)"

    lock_keys = list(client.scan_iter(match="lock:pr:*"))
    locks = []
    for key in lock_keys:
        owner = client.get(key) or "(expired)"
        ttl = client.ttl(key)
        pr_num = key.split(":")[-1]
        locks.append({"pr": pr_num, "owner": owner, "ttl": ttl})

    groups = []
    for stream_key in task_streams:
        try:
            for g in client.xinfo_groups(stream_key):
                groups.append({"stream": stream_key, **g})
        except redis_lib.ResponseError:
            pass  # Stream has no consumer groups
        except redis_lib.RedisError as e:
            console.print(f"  [yellow]Could not read groups for {stream_key}: {e}[/yellow]")

    console.print("\n[bold]Orcest System Status[/bold]\n")

    table = Table(title="Queue Depths")
    table.add_column("Stream", style="cyan")
    table.add_column("Pending", style="yellow")
    for stream_key in sorted(task_streams):
        try:
            table.add_row(stream_key, str(client.xlen(stream_key) or 0))
        except redis_lib.ResponseError:
            # WRONGTYPE: key exists but is not a stream
            table.add_row(stream_key, "(not a stream)")
    if not task_streams:
        table.add_row("tasks:*", "0")
    table.add_row("results", str(results_len))
    console.print(table)

    if locks:
        lock_table = Table(title="Active Locks")
        lock_table.add_column("PR", style="cyan")
        lock_table.add_column("Owner", style="green")
        lock_table.add_column("TTL (s)", style="yellow")
        for lock in locks:
            lock_table.add_row(lock["pr"], lock["owner"], str(lock["ttl"]))
        console.print(lock_table)
    else:
        console.print("[dim]No active locks.[/dim]")

    if groups:
        group_table = Table(title="Consumer Groups")
        group_table.add_column("Stream", style="magenta")
        group_table.add_column("Group", style="cyan")
        group_table.add_column("Consumers", style="green")
        group_table.add_column("Pending", style="yellow")
        for g in groups:
            group_table.add_row(g["stream"], g["name"], str(g["consumers"]), str(g["pending"]))
        console.print(group_table)

    console.print()


@main.command()
def init():
    """Initialize orcest on this Proxmox host.

    Auto-detects Proxmox settings, creates an API token, reads the SSH
    public key, writes /etc/orcest/config.yaml, copies Terraform HCL
    templates, and runs ``tofu init``.
    """
    import json
    import os
    import shutil
    import subprocess
    from pathlib import Path

    from orcest.fleet.config import (
        DEFAULT_CONFIG_DIR,
        DEFAULT_CONFIG_PATH,
        FleetConfig,
        OrchestratorConfig,
        ProxmoxConfig,
        save_config,
    )

    console = Console()
    console.print("\n[bold]Initializing orcest fleet management[/bold]\n")

    is_proxmox = False
    node_name = "pve"
    storage = "local-lvm"
    api_token_id = ""
    api_token_secret = ""
    ssh_key = ""

    # Step 1: Detect Proxmox
    console.print("  Detecting Proxmox...", end=" ")
    has_qm = shutil.which("qm") is not None
    has_pve_dir = Path("/etc/pve").is_dir()
    if has_qm and has_pve_dir:
        is_proxmox = True
        console.print("[green]yes[/green]")
    else:
        console.print("[yellow]not detected[/yellow]")
        if not has_qm:
            console.print("    [dim]'qm' command not found[/dim]")
        if not has_pve_dir:
            console.print("    [dim]/etc/pve/ directory not found[/dim]")
        console.print("    [yellow]Continuing with defaults (manual config needed).[/yellow]")

    # Step 2: Detect node name
    if is_proxmox:
        console.print("  Detecting node name...", end=" ")
        try:
            hostname_path = Path("/etc/hostname")
            if hostname_path.exists():
                node_name = hostname_path.read_text().strip()
            console.print(f"[green]{node_name}[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed ({exc}), using 'pve'[/yellow]")

    # Step 3: Detect storage
    if is_proxmox:
        console.print("  Detecting storage...", end=" ")
        try:
            result = subprocess.run(
                ["pvesh", "get", f"/nodes/{node_name}/storage", "--output-format", "json"],
                capture_output=True,
                text=True,
                check=True,
            )
            storages = json.loads(result.stdout)
            for stype in ("lvmthin", "lvm"):
                for s in storages:
                    if s.get("type") == stype:
                        storage = s.get("storage", storage)
                        break
                else:
                    continue
                break
            console.print(f"[green]{storage}[/green]")
        except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as exc:
            console.print(f"[yellow]failed ({exc}), using 'local-lvm'[/yellow]")

    # Step 4: Create API token
    if is_proxmox:
        console.print("  Creating Proxmox API token...", end=" ")
        try:
            result = subprocess.run(
                [
                    "pveum",
                    "user",
                    "token",
                    "add",
                    "root@pam",
                    "orcest",
                    "--privsep",
                    "0",
                    "--output-format",
                    "json",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                token_data = json.loads(result.stdout)
                api_token_id = token_data.get("full-tokenid", "root@pam!orcest")
                api_token_secret = token_data.get("value", "")
                console.print(f"[green]{api_token_id}[/green]")
            else:
                stderr = result.stderr.strip()
                if "already exists" in stderr:
                    console.print("[yellow]token 'orcest' already exists[/yellow]")
                    console.print(
                        "    [dim]Set api_token_id and api_token_secret manually in config.[/dim]"
                    )
                    api_token_id = "root@pam!orcest"
                else:
                    console.print(f"[yellow]failed: {stderr}[/yellow]")
        except FileNotFoundError:
            console.print("[yellow]pveum not found[/yellow]")
        except (json.JSONDecodeError, KeyError) as exc:
            console.print(f"[yellow]failed to parse response: {exc}[/yellow]")

    # Step 5: Read SSH public key
    console.print("  Reading SSH public key...", end=" ")
    home = Path(os.path.expanduser("~"))
    for key_name in ("id_ed25519.pub", "id_rsa.pub"):
        key_path = home / ".ssh" / key_name
        if key_path.exists():
            ssh_key = key_path.read_text().strip()
            console.print(f"[green]{key_name}[/green]")
            break
    else:
        console.print("[yellow]not found (check ~/.ssh/)[/yellow]")

    # Step 6: Write config
    console.print("  Writing config...", end=" ")
    try:
        config = FleetConfig(
            proxmox=ProxmoxConfig(
                node=node_name,
                storage=storage,
                api_token_id=api_token_id,
                api_token_secret=api_token_secret,
            ),
            orchestrator=OrchestratorConfig(
                ssh_key=ssh_key,
            ),
            orgs={},
            projects=[],
        )
        DEFAULT_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        save_config(config, DEFAULT_CONFIG_PATH)
        console.print(f"[green]{DEFAULT_CONFIG_PATH}[/green]")
    except PermissionError:
        console.print(f"[red]permission denied writing {DEFAULT_CONFIG_PATH}[/red]")
        console.print("    [dim]Run with sudo or create /etc/orcest/ manually.[/dim]")
        raise SystemExit(1)

    # Step 7: Copy Terraform HCL templates
    console.print("  Copying Terraform templates...", end=" ")
    terraform_src = Path(__file__).parent / "fleet" / "terraform"
    terraform_dest = DEFAULT_CONFIG_DIR / "terraform"
    if terraform_src.is_dir():
        terraform_dest.mkdir(parents=True, exist_ok=True)
        for hcl_file in terraform_src.iterdir():
            if hcl_file.is_file():
                shutil.copy2(hcl_file, terraform_dest / hcl_file.name)
        console.print(f"[green]{terraform_dest}[/green]")
    else:
        console.print("[yellow]no bundled templates found[/yellow]")
        console.print(f"    [dim]Expected at {terraform_src}[/dim]")

    # Step 8: Run tofu init
    if terraform_dest.is_dir():
        console.print("  Running tofu init...", end=" ")
        try:
            from orcest.fleet.provisioner import init as tf_init

            tf_init(config_dir=terraform_dest)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed: {exc}[/yellow]")
            console.print("    [dim]Run 'tofu init' manually in /etc/orcest/terraform/[/dim]")

    # Print summary
    console.print("\n[bold]Initialization complete.[/bold]\n")
    console.print(f"  Config: {DEFAULT_CONFIG_PATH}")
    if terraform_dest.is_dir():
        console.print(f"  Terraform: {terraform_dest}")
    console.print("\n  Next steps:")
    step = 1
    if not api_token_secret and is_proxmox:
        console.print(
            f"  {step}. Set proxmox.api_token_secret in config (edit {DEFAULT_CONFIG_PATH})"
        )
        step += 1
    console.print(
        f"  {step}. Register an org:           orcest fleet add-org <org>"
        " --github-token ... --claude-token ..."
    )
    console.print(f"  {step + 1}. Create orchestrator VM:    orcest fleet create-orchestrator")
    console.print(f"  {step + 2}. Onboard a repo:            orcest fleet onboard <owner/repo>")


@main.command("init-labels")
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for repo/token).")
def init_labels(config):
    """Create orcest labels on the target repo."""
    import os
    import subprocess

    from orcest.shared.config import load_orchestrator_config

    cfg = load_orchestrator_config(config)
    console = Console()
    labels = [
        (cfg.labels.blocked, "d93f0b", "Blocked — waiting for dependency"),
        (cfg.labels.needs_human, "b60205", "Orcest failed — needs manual review"),
        (cfg.labels.ready, "0e8a16", "Issue is ready for orcest to implement"),
    ]

    env = dict(os.environ)
    env["GITHUB_TOKEN"] = cfg.github.token
    env["GH_TOKEN"] = cfg.github.token

    failures = 0
    for name, color, description in labels:
        console.print(f"  Creating label [cyan]{name}[/cyan]...", end=" ")
        try:
            subprocess.run(
                [
                    "gh",
                    "label",
                    "create",
                    name,
                    "--repo",
                    cfg.github.repo,
                    "--color",
                    color,
                    "--description",
                    description,
                    "--force",
                ],
                capture_output=True,
                text=True,
                check=True,
                env=env,
            )
            console.print("[green]ok[/green]")
        except subprocess.CalledProcessError as exc:
            console.print(f"[red]failed[/red]: {exc.stderr.strip()}")
            failures += 1
        except FileNotFoundError:
            console.print("[red]gh CLI not found[/red]")
            raise SystemExit(1)

    if failures:
        console.print(f"\n[red]{failures} label(s) failed[/red] on [bold]{cfg.github.repo}[/bold].")
        raise SystemExit(1)
    console.print(f"\nLabels ready on [bold]{cfg.github.repo}[/bold].")


@main.command()
@click.argument("host")
@click.option("--user", default="root", help="SSH user for the target host.")
@click.option("--worker-config", default="config/worker.yaml", help="Worker config to deploy.")
@click.option("--env-file", default="provision/.env", help="Env file with secrets.")
def provision(host, user, worker_config, env_file):
    """Provision a worker VM via SSH.

    Copies setup script, config, and systemd service to the target host,
    runs the setup script, and starts the worker service.
    """
    import os
    import subprocess
    import sys

    _validate_ssh_input(host, "host")
    _validate_ssh_input(user, "user")

    console = Console()
    ssh_target = f"{user}@{host}" if user else host

    def _ssh(cmd: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["ssh", ssh_target, cmd],
            capture_output=True,
            text=True,
        )

    def _scp(src: str, dest: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["scp", src, f"{ssh_target}:{dest}"],
            capture_output=True,
            text=True,
        )

    # Verify files exist locally
    required_files = {
        "provision/setup-worker.sh": "setup script",
        "provision/systemd/orcest-worker.service": "systemd unit",
        worker_config: "worker config",
        env_file: "env file",
    }
    for path, desc in required_files.items():
        if not os.path.isfile(path):
            console.print(f"[red]Missing {desc}:[/red] {path}")
            sys.exit(1)

    # Step 1: Build wheel, copy files, run setup script
    console.print(f"\n[bold]Provisioning worker on {host}[/bold]\n")

    # Find the project root (where pyproject.toml lives)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    console.print("  Building orcest wheel...", end=" ")
    build_result = subprocess.run(
        ["python3", "-m", "build", "--wheel", "--outdir", "/tmp/orcest-dist"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if build_result.returncode != 0:
        console.print("[red]failed[/red]")
        console.print(build_result.stderr)
        sys.exit(1)
    # Find the built wheel
    import glob

    wheels = glob.glob("/tmp/orcest-dist/*.whl")
    if not wheels:
        console.print("[red]failed[/red]: no wheel produced")
        sys.exit(1)
    wheel_path = wheels[-1]
    console.print(f"[green]ok[/green] ({os.path.basename(wheel_path)})")

    console.print("  Uploading wheel...", end=" ")
    _ssh("mkdir -p /tmp/orcest-wheel")
    result = _scp(wheel_path, "/tmp/orcest-wheel/")
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    console.print("  Copying setup script...", end=" ")
    result = _scp("provision/setup-worker.sh", "/tmp/orcest-setup.sh")
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    console.print("  Running setup script (this may take a few minutes)...\n")
    result = subprocess.run(
        ["ssh", ssh_target, "sudo bash /tmp/orcest-setup.sh"],
        text=True,
    )
    if result.returncode != 0:
        console.print("\n  Setup script [red]failed[/red]")
        sys.exit(1)
    console.print("\n  Setup script [green]ok[/green]")

    # Step 2: Copy config and env files
    console.print("  Copying worker config...", end=" ")
    result = _scp(worker_config, "/tmp/orcest-worker.yaml")
    if result.returncode == 0:
        _ssh(
            "sudo cp /tmp/orcest-worker.yaml /opt/orcest/worker.yaml && "
            "sudo chown orcest:orcest /opt/orcest/worker.yaml"
        )
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    console.print("  Copying env file...", end=" ")
    result = _scp(env_file, "/tmp/orcest-env")
    if result.returncode == 0:
        _ssh(
            "sudo cp /tmp/orcest-env /opt/orcest/.env && "
            "sudo chmod 600 /opt/orcest/.env && "
            "sudo chown orcest:orcest /opt/orcest/.env"
        )
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    # Step 3: Install and start systemd service
    console.print("  Installing systemd service...", end=" ")
    result = _scp("provision/systemd/orcest-worker.service", "/tmp/orcest-worker.service")
    if result.returncode == 0:
        _ssh(
            "sudo cp /tmp/orcest-worker.service /etc/systemd/system/ && "
            "sudo systemctl daemon-reload && "
            "sudo systemctl enable orcest-worker"
        )
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    console.print("  Starting worker service...", end=" ")
    result = _ssh("sudo systemctl restart orcest-worker")
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    # Step 5: Verify
    console.print("  Checking service status...", end=" ")
    result = _ssh("systemctl is-active orcest-worker")
    status = result.stdout.strip()
    if status == "active":
        console.print(f"[green]{status}[/green]")
    else:
        console.print(f"[yellow]{status}[/yellow]")
        console.print(f"  Check logs: ssh {ssh_target} journalctl -u orcest-worker -f")

    console.print(f"\n[bold]Worker provisioned on {host}.[/bold]")
    console.print("\n  To authenticate Claude Code, run:")
    console.print(f"  ssh -t {ssh_target} 'sudo -u orcest claude login'")


main.add_command(fleet)
