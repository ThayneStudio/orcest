"""CLI entry point for orcest."""

import click
from rich.console import Console
from rich.table import Table


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
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for Redis).")
def status(config):
    """Show system status: workers, queue depth, active tasks."""
    from orcest.shared.config import load_orchestrator_config
    from orcest.shared.redis_client import RedisClient

    cfg = load_orchestrator_config(config)
    redis = RedisClient(cfg.redis)

    if not redis.health_check():
        click.echo("Error: Cannot connect to Redis.", err=True)
        raise SystemExit(1)

    console = Console()
    client = redis.client

    # Queue depth — scan for all tasks:* streams
    task_streams = list(client.scan_iter(match="tasks:*"))
    results_len = client.xlen("results") or 0

    # Active locks
    lock_keys = list(client.scan_iter(match="lock:pr:*"))
    locks = []
    for key in lock_keys:
        owner = client.get(key) or "(expired)"
        ttl = client.ttl(key)
        pr_num = key.split(":")[-1]
        locks.append({"pr": pr_num, "owner": owner, "ttl": ttl})

    # Consumer group info
    groups = []
    for stream_key in task_streams:
        try:
            for g in client.xinfo_groups(stream_key):
                groups.append({"stream": stream_key, **g})
        except Exception:
            pass

    # Display
    console.print("\n[bold]Orcest System Status[/bold]\n")

    table = Table(title="Queue Depths")
    table.add_column("Stream", style="cyan")
    table.add_column("Pending", style="yellow")
    for stream_key in sorted(task_streams):
        table.add_row(stream_key, str(client.xlen(stream_key) or 0))
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
    import subprocess

    from orcest.shared.config import load_orchestrator_config

    cfg = load_orchestrator_config(config)
    console = Console()
    labels = [
        (cfg.labels.queued, "0e8a16", "Task queued for orcest processing"),
        (cfg.labels.in_progress, "1d76db", "Orcest worker is processing this"),
        (cfg.labels.blocked, "d93f0b", "Blocked — waiting for dependency"),
        (cfg.labels.needs_human, "b60205", "Orcest failed — needs manual review"),
    ]

    env = {"GITHUB_TOKEN": cfg.github.token, "GH_TOKEN": cfg.github.token}
    env.update({k: v for k, v in __import__("os").environ.items()})

    for name, color, description in labels:
        console.print(f"  Creating label [cyan]{name}[/cyan]...", end=" ")
        try:
            subprocess.run(
                ["gh", "label", "create", name,
                 "--repo", cfg.github.repo,
                 "--color", color,
                 "--description", description,
                 "--force"],
                capture_output=True, text=True, check=True, env=env,
            )
            console.print("[green]ok[/green]")
        except subprocess.CalledProcessError as exc:
            console.print(f"[red]failed[/red]: {exc.stderr.strip()}")
        except FileNotFoundError:
            console.print("[red]gh CLI not found[/red]")
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

    console = Console()
    ssh_target = f"{user}@{host}" if user else host

    def _ssh(cmd: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["ssh", ssh_target, cmd],
            capture_output=True, text=True,
        )

    def _scp(src: str, dest: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["scp", src, f"{ssh_target}:{dest}"],
            capture_output=True, text=True,
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

    # Step 1: Copy and run setup script
    console.print(f"\n[bold]Provisioning worker on {host}[/bold]\n")

    console.print("  Copying setup script...", end=" ")
    result = _scp("provision/setup-worker.sh", "/tmp/orcest-setup.sh")
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        sys.exit(1)
    console.print("[green]ok[/green]")

    console.print("  Running setup script...", end=" ")
    result = _ssh("sudo bash /tmp/orcest-setup.sh")
    if result.returncode != 0:
        console.print("[red]failed[/red]")
        console.print(result.stderr)
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 2: Copy config and env files
    console.print("  Copying worker config...", end=" ")
    result = _scp(worker_config, "/tmp/orcest-worker.yaml")
    if result.returncode == 0:
        _ssh("sudo cp /tmp/orcest-worker.yaml /opt/orcest/worker.yaml && "
             "sudo chown orcest:orcest /opt/orcest/worker.yaml")
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    console.print("  Copying env file...", end=" ")
    result = _scp(env_file, "/tmp/orcest-env")
    if result.returncode == 0:
        _ssh("sudo cp /tmp/orcest-env /opt/orcest/.env && "
             "sudo chmod 600 /opt/orcest/.env && "
             "sudo chown orcest:orcest /opt/orcest/.env")
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    # Step 3: Install and start systemd service
    console.print("  Installing systemd service...", end=" ")
    result = _scp("provision/systemd/orcest-worker.service",
                   "/tmp/orcest-worker.service")
    if result.returncode == 0:
        _ssh("sudo cp /tmp/orcest-worker.service /etc/systemd/system/ && "
             "sudo systemctl daemon-reload && "
             "sudo systemctl enable orcest-worker")
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    console.print("  Starting worker service...", end=" ")
    result = _ssh("sudo systemctl restart orcest-worker")
    console.print("[green]ok[/green]" if result.returncode == 0 else "[red]failed[/red]")

    # Step 4: Verify
    console.print("  Checking service status...", end=" ")
    result = _ssh("systemctl is-active orcest-worker")
    status = result.stdout.strip()
    if status == "active":
        console.print(f"[green]{status}[/green]")
    else:
        console.print(f"[yellow]{status}[/yellow]")
        console.print("  Check logs: ssh {ssh_target} journalctl -u orcest-worker -f")

    console.print(f"\n[bold]Worker provisioned on {host}.[/bold]")
