"""CLI entry point for orcest."""

import click


@click.group()
def main():
    """Orcest: Autonomous CI/CD orchestration system."""


@main.command()
@click.option("--config", default="config/orchestrator.yaml", help="Path to orchestrator config.")
def orchestrate(config):
    """Start the orchestrator loop."""
    click.echo(f"Starting orchestrator with config: {config}")
    click.echo("Not yet implemented.")


@main.command()
@click.option("--id", "worker_id", required=True, help="Unique worker identifier.")
@click.option("--config", default="config/worker.yaml", help="Path to worker config.")
def work(worker_id, config):
    """Start a worker loop."""
    click.echo(f"Starting worker {worker_id} with config: {config}")
    click.echo("Not yet implemented.")


@main.command()
def status():
    """Show system status: workers, queue, active tasks."""
    click.echo("Not yet implemented.")


@main.command()
@click.argument("host")
def provision(host):
    """Provision a worker VM via SSH."""
    click.echo(f"Provisioning worker on {host}")
    click.echo("Not yet implemented.")
