"""CLI entry point for orcest."""

import re
import sys

import click
from rich.console import Console
from rich.table import Table

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
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for repo/token).")
def init(config):
    """Initialize the target repo: create orcest labels."""
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


# Register fleet subcommands
from orcest.fleet.cli import fleet  # noqa: E402

main.add_command(fleet)


@main.command("provision-orchestrator")
@click.argument("host")
@click.option("--user", default="root", help="SSH user for the target host.")
@click.option(
    "--orch-config",
    default="config/orchestrator.yaml",
    help="Orchestrator config to deploy.",
)
@click.option(
    "--env-file",
    default="provision/.env.orchestrator",
    help="Env file with GITHUB_TOKEN.",
)
def provision_orchestrator(host, user, orch_config, env_file):
    """Provision an orchestrator VM via SSH.

    Installs Docker, uploads the compose stack, config, and env to the target
    host, builds the orchestrator image, and starts the services.
    """
    import os
    import subprocess
    import sys
    import tempfile

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
        "provision/setup-orchestrator.sh": "setup script",
        "docker-compose.yml": "Docker Compose file",
        "Dockerfile": "Dockerfile",
        "pyproject.toml": "pyproject.toml",
        orch_config: "orchestrator config",
        env_file: "env file",
    }
    for path, desc in required_files.items():
        if not os.path.isfile(path):
            console.print(f"[red]Missing {desc}:[/red] {path}")
            sys.exit(1)
    if not os.path.isdir("src"):
        console.print("[red]Missing src/ directory[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Provisioning orchestrator on {host}[/bold]\n")

    # Step 1: Upload and run setup script
    console.print("  Copying setup script...", end=" ")
    result = _scp("provision/setup-orchestrator.sh", "/tmp/orcest-setup.sh")
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

    # Step 2: Create and upload source tarball
    console.print("  Creating source tarball...", end=" ")
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tarball = tmp.name
    tar_result = subprocess.run(
        ["tar", "czf", tarball, "Dockerfile", "docker-compose.yml", "pyproject.toml", "src/"],
        capture_output=True,
        text=True,
    )
    if tar_result.returncode != 0:
        console.print(f"[red]failed[/red]: {tar_result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    console.print("  Uploading source...", end=" ")
    result = _scp(tarball, "/tmp/orcest-source.tar.gz")
    os.unlink(tarball)
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    console.print("  Extracting source...", end=" ")
    result = _ssh("sudo -u orcest tar xzf /tmp/orcest-source.tar.gz -C /opt/orcest/")
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 3: Copy config
    console.print("  Copying orchestrator config...", end=" ")
    _ssh("sudo -u orcest mkdir -p /opt/orcest/config")
    result = _scp(orch_config, "/tmp/orcest-config.yaml")
    if result.returncode == 0:
        result = _ssh(
            "sudo cp /tmp/orcest-config.yaml /opt/orcest/config/orchestrator.yaml && "
            "sudo chown orcest:orcest /opt/orcest/config/orchestrator.yaml"
        )
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    # Step 4: Copy env file
    console.print("  Copying env file...", end=" ")
    result = _scp(env_file, "/tmp/orcest-env")
    if result.returncode == 0:
        result = _ssh(
            "sudo cp /tmp/orcest-env /opt/orcest/.env && "
            "sudo chmod 600 /opt/orcest/.env && "
            "sudo chown orcest:orcest /opt/orcest/.env"
        )
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    # Step 5: Build and start Docker Compose
    console.print("  Building orchestrator image (this may take a minute)...\n")
    result = subprocess.run(
        ["ssh", ssh_target, "sudo -u orcest bash -c 'cd /opt/orcest && docker compose build'"],
        text=True,
    )
    if result.returncode != 0:
        console.print("\n  Docker build [red]failed[/red]")
        sys.exit(1)
    console.print("\n  Docker build [green]ok[/green]")

    console.print("  Starting services...", end=" ")
    result = _ssh(
        "sudo -u orcest bash -c "
        "'cd /opt/orcest && docker compose down 2>/dev/null; docker compose up -d'"
    )
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 6: Verify
    console.print("  Checking services...", end=" ")
    result = _ssh("sudo -u orcest bash -c 'cd /opt/orcest && docker compose ps --format json'")
    if result.returncode == 0:
        console.print("[green]ok[/green]")
    else:
        console.print("[yellow]could not verify[/yellow]")

    console.print("  Pinging Redis...", end=" ")
    result = _ssh(
        "sudo -u orcest bash -c 'cd /opt/orcest && docker compose exec -T redis redis-cli ping'"
    )
    if result.stdout.strip() == "PONG":
        console.print("[green]PONG[/green]")
    else:
        console.print("[yellow]no response (services may still be starting)[/yellow]")

    console.print(f"\n[bold]Orchestrator provisioned on {host}.[/bold]")
    console.print(f"\n  Redis is accessible at {host}:6379")
    console.print(f"  Workers should set redis.host to {host} in their config.")
    console.print("\n  To check logs:")
    console.print(f"  ssh {ssh_target} 'cd /opt/orcest && sudo -u orcest docker compose logs -f'")
