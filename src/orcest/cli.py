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
def work(worker_id, config):
    """Start a worker loop."""
    from orcest.shared.config import load_worker_config
    from orcest.worker.loop import run_worker

    cfg = load_worker_config(config)
    cfg.worker_id = worker_id
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

    # Queue depth
    tasks_len = client.xlen("tasks") or 0
    results_len = client.xlen("results") or 0

    # Active locks
    lock_keys = list(client.scan_iter(match="lock:pr:*"))
    locks = []
    for key in lock_keys:
        owner = client.get(key)
        ttl = client.ttl(key)
        pr_num = key.split(":")[-1]
        locks.append({"pr": pr_num, "owner": owner, "ttl": ttl})

    # Consumer group info
    try:
        groups = client.xinfo_groups("tasks")
    except Exception:
        groups = []

    # Display
    console.print("\n[bold]Orcest System Status[/bold]\n")

    table = Table(title="Queue Depths")
    table.add_column("Stream", style="cyan")
    table.add_column("Pending", style="yellow")
    table.add_row("tasks", str(tasks_len))
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
        group_table.add_column("Group", style="cyan")
        group_table.add_column("Consumers", style="green")
        group_table.add_column("Pending", style="yellow")
        for g in groups:
            group_table.add_row(g["name"], str(g["consumers"]), str(g["pending"]))
        console.print(group_table)

    console.print()


@main.command()
@click.argument("host")
def provision(host):
    """Provision a worker VM via SSH."""
    click.echo(f"Provisioning worker on {host}")
    click.echo("Not yet implemented.")
