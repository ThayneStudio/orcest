"""CLI entry point for orcest."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import click
import redis as redis_lib
from rich.console import Console
from rich.table import Table

from orcest.dashboard import fetch_snapshot, truncate
from orcest.fleet.cli import fleet
from orcest.shared.models import (
    DEAD_LETTER_METADATA_FIELDS,
    DEAD_LETTER_STREAM,
    REDACTED_FIELDS,
)
from orcest.shared.provider_stream_health import StreamHealthState

if TYPE_CHECKING:
    from orcest.shared.config import RedisConfig
    from orcest.shared.redis_client import RedisClient

_SSH_INPUT_RE = re.compile(r"^[a-zA-Z0-9._-]+$")


def _validate_ssh_input(value: str, label: str) -> None:
    """Raise click.BadParameter if value contains shell metacharacters."""
    if not _SSH_INPUT_RE.match(value):
        raise click.BadParameter(
            f"Invalid {value!r}: only alphanumerics, dots, hyphens, and underscores are allowed.",
            param_hint=repr(label),
        )


def _parse_redis_host(redis_host: str) -> tuple[str, int]:
    """Parse a Redis host string into (host, port), defaulting port to 6379."""
    if ":" in redis_host:
        host, port_str = redis_host.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            click.echo(f"Error: Invalid port number: {port_str}", err=True)
            raise SystemExit(1)
    else:
        host, port = redis_host, 6379
    return host, port


def _resolve_redis_config(
    redis_host: str | None,
    config_path: str,
    prefix: str | None,
) -> RedisConfig:
    """Build a RedisConfig from a CLI redis_host argument or config file.

    When ``redis_host`` is provided (e.g. ``10.0.0.5`` or ``10.0.0.5:6380``),
    host/port are parsed from it (taking precedence over ORCEST_REDIS_HOST /
    ORCEST_REDIS_PORT env vars) while remaining fields (password, key_prefix)
    come from env vars via ``build_redis_config``.  Otherwise the YAML at
    ``config_path`` is loaded and its redis section is used (again via
    ``build_redis_config`` so env vars take precedence).

    Only the ``redis`` section of the config file is needed -- the full
    orchestrator validation (e.g. github.repo required) is intentionally
    skipped so that ``orcest status`` and ``orcest dead-letters`` work
    without a fully populated orchestrator config.

    The optional ``prefix`` overrides the key_prefix in either case.
    """
    from orcest.shared.config import _load_yaml, build_redis_config

    if redis_host:
        host, port = _parse_redis_host(redis_host)
        # Build config from env vars (picks up ORCEST_REDIS_PASSWORD,
        # ORCEST_REDIS_KEY_PREFIX, etc.), then override host/port with
        # the explicit CLI argument which should take highest precedence.
        redis_cfg = build_redis_config()
        redis_cfg.host = host
        redis_cfg.port = port
    else:
        raw = _load_yaml(config_path)
        redis_cfg = build_redis_config(raw)

    if prefix:
        redis_cfg.key_prefix = prefix
    return redis_cfg


@click.group()
def main() -> None:
    """Orcest: Autonomous CI/CD orchestration system."""


@main.command()
@click.option("--json", "json_output", is_flag=True, help="Emit machine-readable JSON.")
@click.option("--short", is_flag=True, help="Print only the revision value.")
def revision(json_output: bool, short: bool) -> None:
    """Show the exact source revision baked into this Orcest installation."""
    from orcest.revision import get_build_revision, revision_is_attested

    value = get_build_revision()
    if short:
        click.echo(value)
        return
    payload = {"revision": value, "attested": revision_is_attested(value)}
    if json_output:
        click.echo(json.dumps(payload, sort_keys=True))
        return
    state = "attested" if payload["attested"] else "unattested"
    click.echo(f"{value} ({state})")


@main.command("rollout-health")
@click.argument("redis_host", required=False, default=None)
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for Redis).")
@click.option("--prefix", required=True, help="Project Redis key prefix (project name).")
@click.option("--task-prefix", default="orcest", show_default=True, help="Shared task prefix.")
@click.option(
    "--pool-prefix",
    default=None,
    help="Redis key prefix for worker-pool state [default: --task-prefix].",
)
@click.option(
    "--expected-revision",
    required=True,
    help="Exact clean revision expected for this checker installation.",
)
@click.option("--expected-pool-size", type=click.IntRange(min=0), default=None)
@click.option(
    "--expected-vmid-start",
    type=click.IntRange(min=1),
    default=None,
    help="First managed worker VMID; required with --expected-backend.",
)
@click.option(
    "--expected-backend",
    "expected_backends",
    multiple=True,
    help="Required worker backend; repeat for mixed fleets.",
)
@click.option("--baseline-dead-letters", type=click.IntRange(min=0), default=None)
@click.option("--baseline-exhausted-skips", type=click.IntRange(min=0), default=None)
@click.option("--baseline-rebake-failures", type=click.IntRange(min=0), default=None)
@click.option("--max-private-recovery", type=click.IntRange(min=0), default=0, show_default=True)
@click.option("--require-quiescent", is_flag=True, help="Require empty queues and no active VMs.")
@click.option("--json", "json_output", is_flag=True, help="Emit machine-readable JSON.")
def rollout_health(
    redis_host: str | None,
    config: str,
    prefix: str,
    task_prefix: str,
    pool_prefix: str | None,
    expected_revision: str,
    expected_pool_size: int | None,
    expected_vmid_start: int | None,
    expected_backends: tuple[str, ...],
    baseline_dead_letters: int | None,
    baseline_exhausted_skips: int | None,
    baseline_rebake_failures: int | None,
    max_private_recovery: int,
    require_quiescent: bool,
    json_output: bool,
) -> None:
    """Run read-only deployment gates suitable for scripts and watch loops."""
    from orcest.revision import normalize_revision
    from orcest.rollout_health import collect_rollout_health
    from orcest.shared.redis_client import RedisClient

    normalized = normalize_revision(expected_revision)
    if normalized is None or normalized.endswith("-dirty"):
        raise click.BadParameter(
            "must be an exact clean hexadecimal revision",
            param_hint="--expected-revision",
        )
    from orcest.shared.models import require_valid_provider_name

    try:
        for backend in expected_backends:
            require_valid_provider_name(backend)
    except ValueError as exc:
        raise click.BadParameter(str(exc), param_hint="--expected-backend") from exc
    if expected_backends and expected_pool_size is None:
        raise click.BadParameter(
            "requires --expected-pool-size so worker heartbeats can be correlated "
            "to managed pool slots",
            param_hint="--expected-backend",
        )
    if expected_backends and expected_vmid_start is None:
        raise click.BadParameter(
            "requires --expected-vmid-start so the exact managed VMID/backend "
            "layout can be verified",
            param_hint="--expected-backend",
        )
    if expected_pool_size is not None and expected_backends:
        if len(expected_backends) != expected_pool_size:
            raise click.BadParameter(
                "must be repeated exactly once per expected pool slot",
                param_hint="--expected-backend",
            )

    redis_cfg = _resolve_redis_config(redis_host, config, prefix)
    redis = RedisClient(redis_cfg)
    try:
        report = collect_rollout_health(
            redis,
            expected_revision=normalized,
            task_prefix=task_prefix,
            pool_prefix=pool_prefix,
            expected_pool_size=expected_pool_size,
            expected_vmid_start=expected_vmid_start,
            expected_backends=expected_backends,
            baseline_dead_letters=baseline_dead_letters,
            baseline_exhausted_skips=baseline_exhausted_skips,
            baseline_rebake_failures=baseline_rebake_failures,
            max_private_recovery=max_private_recovery,
            require_quiescent=require_quiescent,
        )
    finally:
        redis.close()

    if json_output:
        click.echo(json.dumps(report, sort_keys=True))
    else:
        for check in report["checks"]:
            marker = "PASS" if check["passed"] else "FAIL"
            click.echo(
                f"{marker} {check['name']}: {check['actual']} (expected {check['expected']})"
            )
        click.echo(f"METRICS {json.dumps(report['metrics'], sort_keys=True)}")
    if not report["ok"]:
        raise SystemExit(1)


@main.command("canary-evidence")
@click.argument("redis_host", required=False, default=None)
@click.option("--config", default="config/orchestrator.yaml", help="Config file for Redis.")
@click.option("--prefix", required=True, help="Project Redis key prefix.")
@click.option("--task-prefix", default="orcest", show_default=True)
@click.option(
    "--canary",
    "canary_specs",
    multiple=True,
    required=True,
    help="Provider/task pair as PROVIDER=TASK_ID; repeat once per provider.",
)
def canary_evidence(
    redis_host: str | None,
    config: str,
    prefix: str,
    task_prefix: str,
    canary_specs: tuple[str, ...],
) -> None:
    """Emit secret-safe proof that provider canaries completed exactly once."""
    from orcest.canary_evidence import CanaryEvidenceError, collect_canary_evidence
    from orcest.shared.redis_client import RedisClient

    canaries: dict[str, str] = {}
    for spec in canary_specs:
        provider, separator, task_id = spec.partition("=")
        if not separator or not provider or not task_id:
            raise click.BadParameter("must use PROVIDER=TASK_ID", param_hint="--canary")
        if provider in canaries:
            raise click.BadParameter(
                f"provider {provider!r} was specified more than once",
                param_hint="--canary",
            )
        canaries[provider] = task_id

    redis_cfg = _resolve_redis_config(redis_host, config, prefix)
    redis = RedisClient(redis_cfg)
    try:
        evidence = collect_canary_evidence(
            redis,
            task_prefix=task_prefix,
            canaries=canaries,
        )
    except CanaryEvidenceError as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        redis.close()
    click.echo(json.dumps(evidence, sort_keys=True))


@main.group("task-streams")
def task_streams() -> None:
    """Fence or restore provider task streams during a controlled migration."""


def _task_stream_transition(
    *,
    operation: str,
    redis_host: str | None,
    config: str,
    task_prefix: str,
    quarantine_id: str,
    force: bool = False,
) -> None:
    from orcest.shared.redis_client import RedisClient
    from orcest.task_stream_quarantine import (
        TaskStreamQuarantineError,
        quarantine_task_streams,
        restore_task_streams,
    )

    redis_cfg = _resolve_redis_config(redis_host, config, None)
    redis = RedisClient(redis_cfg)
    try:
        if operation == "quarantine":
            report = quarantine_task_streams(
                redis,
                task_prefix=task_prefix,
                quarantine_id=quarantine_id,
                force=force,
            )
        else:
            report = restore_task_streams(
                redis,
                task_prefix=task_prefix,
                quarantine_id=quarantine_id,
            )
    except TaskStreamQuarantineError as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        redis.close()
    click.echo(json.dumps(report, sort_keys=True))


@task_streams.command("quarantine")
@click.argument("redis_host", required=False, default=None)
@click.option("--config", default="config/orchestrator.yaml", help="Config file for Redis.")
@click.option("--task-prefix", default="orcest", show_default=True)
@click.option("--quarantine-id", required=True, help="Unique release identifier.")
@click.option(
    "--force",
    is_flag=True,
    help="Fence anyway when live workers or pending deliveries are known-orphaned.",
)
def task_streams_quarantine(
    redis_host: str | None,
    config: str,
    task_prefix: str,
    quarantine_id: str,
    force: bool,
) -> None:
    """Atomically move active task streams behind a migration fence."""
    _task_stream_transition(
        operation="quarantine",
        redis_host=redis_host,
        config=config,
        task_prefix=task_prefix,
        quarantine_id=quarantine_id,
        force=force,
    )


@task_streams.command("restore")
@click.argument("redis_host", required=False, default=None)
@click.option("--config", default="config/orchestrator.yaml", help="Config file for Redis.")
@click.option("--task-prefix", default="orcest", show_default=True)
@click.option("--quarantine-id", required=True, help="Release identifier used to quarantine.")
def task_streams_restore(
    redis_host: str | None,
    config: str,
    task_prefix: str,
    quarantine_id: str,
) -> None:
    """Restore fenced task streams without overwriting any new work."""
    _task_stream_transition(
        operation="restore",
        redis_host=redis_host,
        config=config,
        task_prefix=task_prefix,
        quarantine_id=quarantine_id,
    )


@main.command()
@click.option("--config", default="config/orchestrator.yaml", help="Path to orchestrator config.")
def orchestrate(config: str) -> None:
    """Start the orchestrator loop."""
    from orcest.orchestrator.loop import run_orchestrator
    from orcest.shared.config import load_orchestrator_config

    cfg = load_orchestrator_config(config)
    run_orchestrator(cfg)


@main.command()
@click.option("--id", "worker_id", required=True, help="Unique worker identifier.")
@click.option("--config", default="config/worker.yaml", help="Path to worker config.")
@click.option("--runner", default=None, help="Runner type override (claude, noop, etc.)")
@click.option("--once", is_flag=True, help="Ephemeral mode: process one task and exit.")
def work(worker_id: str, config: str, runner: str | None, once: bool) -> None:
    """Start a worker loop."""
    from orcest.shared.config import load_worker_config
    from orcest.worker.loop import run_worker

    cfg = load_worker_config(config)
    cfg.worker_id = worker_id
    if runner:
        from orcest.fleet.config import normalize_worker_runner_for_backend

        backend, runner_type, runner_mode = normalize_worker_runner_for_backend(runner, "", "")
        cfg.runner.type = runner_type
        cfg.runner.extra.pop("mode", None)
        if runner_mode:
            cfg.runner.extra["mode"] = runner_mode
        cfg.backend = backend
    if once:
        cfg.ephemeral = True
    run_worker(cfg)


@main.command()
@click.argument("redis_host", required=False, default=None)
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for Redis).")
@click.option("--prefix", default=None, help="Redis key prefix (project name).")
@click.option("--once", is_flag=True, help="Print status once and exit (no TUI).")
@click.option("--interval", default=3.0, type=float, help="TUI refresh interval in seconds.")
def status(
    redis_host: str | None,
    config: str,
    prefix: str | None,
    once: bool,
    interval: float,
) -> None:
    """Show system status: workers, queue depth, active tasks.

    Connects to Redis directly via REDIS_HOST (e.g. 10.20.0.19 or 10.20.0.19:6380),
    or falls back to --config file. Launches a live TUI dashboard by default.
    Use --once for single-shot output. Use --prefix to specify the project's
    key prefix when connecting directly via REDIS_HOST.
    """
    from orcest.shared.redis_client import RedisClient

    redis_cfg = _resolve_redis_config(redis_host, config, prefix)
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


def _status_once(redis: RedisClient) -> None:
    """Print system status once and exit (original behavior)."""
    console = Console(file=sys.stdout)
    snapshot = fetch_snapshot(redis)

    if not snapshot.redis_ok:
        console.print("[red]Error: Cannot connect to Redis.[/red]")
        return

    console.print("\n[bold]Orcest System Status[/bold]\n")

    table = Table(title="Queue Depths")
    table.add_column("Stream", style="cyan")
    table.add_column("Pending", style="yellow")
    for stream_key, depth in sorted(snapshot.queue_depths.items()):
        table.add_row(str(stream_key), str(depth))
    if not snapshot.queue_depths:
        table.add_row("tasks:*", "0")
    table.add_row("results", str(snapshot.results_depth))
    table.add_row(DEAD_LETTER_STREAM, str(snapshot.dead_letter_count))
    console.print(table)

    if snapshot.dead_letter_entries:
        dl_detail_table = Table(
            title=f"Recent Dead-Lettered Tasks (last {len(snapshot.dead_letter_entries)})"
        )
        dl_detail_table.add_column("Time", style="dim")
        dl_detail_table.add_column("Type", style="magenta")
        dl_detail_table.add_column("Repo", style="green")
        dl_detail_table.add_column("Resource", style="yellow")
        dl_detail_table.add_column("Reason", style="red")
        for entry in snapshot.dead_letter_entries:
            ts = (
                datetime.fromtimestamp(entry.timestamp_ms / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M UTC"
                )
                if entry.timestamp_ms is not None
                else entry.entry_id
            )
            dl_detail_table.add_row(
                ts,
                entry.task_type,
                entry.repo,
                f"{entry.resource_type} #{entry.resource_id}",
                truncate(entry.reason) if entry.reason is not None else "?",
            )
        console.print(dl_detail_table)

    if snapshot.locks:
        lock_table = Table(title="Active Locks")
        lock_table.add_column("PR", style="cyan")
        lock_table.add_column("Owner", style="green")
        lock_table.add_column("TTL (s)", style="yellow")
        for lock in snapshot.locks:
            lock_table.add_row(lock.pr, lock.owner, str(lock.ttl))
        console.print(lock_table)
    else:
        console.print("[dim]No active locks.[/dim]")

    if snapshot.consumer_groups:
        group_table = Table(title="Consumer Groups")
        group_table.add_column("Stream", style="magenta")
        group_table.add_column("Group", style="cyan")
        group_table.add_column("Consumers", style="green")
        group_table.add_column("Pending", style="yellow")
        for g in snapshot.consumer_groups:
            group_table.add_row(str(g.stream), g.name, str(g.consumers), str(g.pending))
        console.print(group_table)

    # Task 8: surface per-provider health from counters (exhausted skips, rebake failures)
    if getattr(snapshot, "provider_health", None):
        ph = snapshot.provider_health
        if ph:
            ph_table = Table(title="Provider Health (per-provider counters)")
            ph_table.add_column("Provider", style="cyan")
            ph_table.add_column("exhausted_skip", style="yellow")
            ph_table.add_column("rebake_required_failures", style="red")
            for prov in sorted(ph.keys()):
                m = ph[prov]
                ph_table.add_row(
                    prov,
                    str(m.get("exhausted_skip", 0)),
                    str(m.get("rebake_required_failures", 0)),
                )
            console.print(ph_table)

    # issue #613: render PoolManager's canonical stranded-stream snapshots.
    # This CLI never recomputes stream health, only displays what was published.
    stream_health = getattr(snapshot, "stream_health", None)
    if stream_health:
        stranded = [h for h in stream_health if h.state == StreamHealthState.STRANDED]
        for h in stranded:
            console.print(
                f"[bold red]STRANDED[/bold red] provider stream {h.stream!r} "
                f"(provider={h.provider}): pending={h.pending} lag={h.lag} "
                f"registered_consumers={h.registered_consumers} "
                f"live_consumers={h.live_consumers} -- work is queued but no consumer "
                "has a live worker heartbeat"
            )
        state_labels = {
            StreamHealthState.STRANDED: "[bold red]stranded[/bold red]",
            StreamHealthState.HEALTHY: "[green]healthy[/green]",
            StreamHealthState.UNKNOWN: "[dim]unknown[/dim]",
        }
        sh_table = Table(title="Provider Stream Health")
        sh_table.add_column("Provider", style="cyan")
        sh_table.add_column("Stream", style="magenta")
        sh_table.add_column("State")
        sh_table.add_column("Pending", style="yellow")
        sh_table.add_column("Lag", style="yellow")
        sh_table.add_column("Registered", style="green")
        sh_table.add_column("Live", style="green")
        for h in stream_health:
            sh_table.add_row(
                h.provider,
                h.stream,
                state_labels[h.state],
                "?" if h.pending is None else str(h.pending),
                "?" if h.lag is None else str(h.lag),
                "?" if h.registered_consumers is None else str(h.registered_consumers),
                "?" if h.live_consumers is None else str(h.live_consumers),
            )
        console.print(sh_table)

    console.print()


@main.command("dead-letters")
@click.argument("redis_host", required=False, default=None)
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for Redis).")
@click.option("--prefix", default=None, help="Redis key prefix (project name).")
@click.option(
    "--replay",
    is_flag=True,
    help="Re-enqueue dead-lettered tasks to their original task streams.",
)
@click.option(
    "--count",
    default=100,
    type=int,
    help="Maximum number of entries to list (also caps replay scope when --replay is used).",
)
def dead_letters(
    redis_host: str | None,
    config: str,
    prefix: str | None,
    replay: bool,
    count: int,
) -> None:
    """List and optionally replay dead-lettered tasks.

    Reads entries from the orcest:dead-letter stream and displays them in a
    table. Use --replay to re-enqueue them back to their original task streams
    and remove them from the dead-letter stream.

    Connects to Redis directly via REDIS_HOST (e.g. 10.20.0.19 or
    10.20.0.19:6380), or falls back to --config file.
    """
    from orcest.shared.redis_client import RedisClient

    if count < 1:
        click.echo("Error: --count must be a positive integer.", err=True)
        raise SystemExit(1)

    redis_cfg = _resolve_redis_config(redis_host, config, prefix)
    redis = RedisClient(redis_cfg)

    if not redis.health_check():
        redis.close()
        click.echo("Error: Cannot connect to Redis.", err=True)
        raise SystemExit(1)

    try:
        _dead_letters_command(redis, replay=replay, count=count)
    finally:
        redis.close()


def _dead_letters_command(redis: RedisClient, *, replay: bool, count: int) -> None:
    """Implementation of orcest dead-letters, separated for testability."""
    console = Console(file=sys.stdout)

    entries = redis.xread_after(DEAD_LETTER_STREAM, last_id="0-0", count=count)

    if not entries:
        console.print(f"[green]No dead-lettered tasks in {DEAD_LETTER_STREAM!r}.[/green]")
        return

    noun = "entry" if len(entries) == 1 else "entries"
    table = Table(title=f"Dead-Lettered Tasks ({len(entries)} {noun})")
    table.add_column("Entry ID", style="dim")
    table.add_column("Task ID", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Resource", style="yellow")
    table.add_column("Repo", style="green")
    table.add_column("Deliveries", style="red")
    table.add_column("Original Stream", style="blue")

    for entry_id, fields in entries:
        table.add_row(
            entry_id,
            fields.get("id", "?"),
            fields.get("type", "?"),
            f"{fields.get('resource_type', '?')} #{fields.get('resource_id', '?')}",
            fields.get("repo", "?"),
            fields.get("delivery_count", "?"),
            fields.get("tasks_stream", "?"),
        )

    console.print(table)

    if len(entries) == count:
        console.print(
            f"[yellow]{count} entries shown; stream may have more "
            + (
                "— re-run with --replay to process remaining.[/yellow]"
                if replay
                else "— increase --count to see more.[/yellow]"
            )
        )

    if not replay:
        return

    replayed = 0
    skipped = 0
    errors = 0

    for entry_id, fields in entries:
        tasks_stream = fields.get("tasks_stream")
        if not tasks_stream:
            console.print(f"[yellow]Entry {entry_id}: missing tasks_stream, skipping[/yellow]")
            skipped += 1
            continue

        # Strip dead-letter metadata; keep only original task fields.
        task_fields = {k: v for k, v in fields.items() if k not in DEAD_LETTER_METADATA_FIELDS}

        # Task 8: display uses the (already redacted-in-DL) fields; replay checks
        # for redacted values and refuses (see below). DL write sites use to_safe_dict().
        # (No mutation here so test samples with real creds continue to replay in unit tests.)

        # Critical safety for post-Task-2 redaction...
        if any(task_fields.get(f) == "[REDACTED]" for f in REDACTED_FIELDS):
            redacted_secrets = [f for f in REDACTED_FIELDS if task_fields.get(f) == "[REDACTED]"]
            console.print(
                f"[red]Cannot replay entry {entry_id}: contains redacted values for "
                f"{redacted_secrets}. Original credentials are not persisted in "
                "dead-letters (security requirement). Re-trigger the task from the "
                "orchestrator (normal triage/issue flow) or supply the credential "
                "when manually re-enqueuing.[/red]"
            )
            errors += 1
            continue

        try:
            # Not atomic: if xdel fails after xadd the entry stays in the dead-letter stream
            # and will be replayed again on the next --replay run (at-least-once delivery).
            redis.xadd_raw(tasks_stream, task_fields)
            redis.xdel(DEAD_LETTER_STREAM, entry_id)
            replayed += 1
        except redis_lib.RedisError as exc:
            console.print(f"[red]Failed to replay entry {entry_id}: {exc}[/red]")
            errors += 1

    if replayed:
        console.print(f"\n[green]Replayed {replayed} task(s) to their original streams.[/green]")
    if skipped:
        console.print(f"\n[yellow]{skipped} skipped (no tasks_stream field).[/yellow]")
    if errors:
        console.print(f"\n[red]{errors} error(s) during replay.[/red]")


@main.command()
def init() -> None:
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
                    # Delete and recreate to get a fresh secret
                    console.print("[yellow]exists, recreating...[/yellow]", end=" ")
                    subprocess.run(
                        ["pveum", "user", "token", "remove", "root@pam", "orcest"],
                        capture_output=True,
                        text=True,
                    )
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
                        console.print(f"[red]failed: {result.stderr.strip()}[/red]")
                else:
                    console.print(f"[yellow]failed: {stderr}[/yellow]")
        except FileNotFoundError:
            console.print("[yellow]pveum not found[/yellow]")
        except (json.JSONDecodeError, KeyError) as exc:
            console.print(f"[yellow]failed to parse response: {exc}[/yellow]")

    # Step 5: Ensure SSH key exists (generate if needed) and authorize for local access
    home = Path(os.path.expanduser("~"))
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(mode=0o700, exist_ok=True)

    console.print("  SSH key...", end=" ")
    key_path = None
    for key_name in ("id_ed25519", "id_rsa"):
        if (ssh_dir / f"{key_name}.pub").exists():
            key_path = ssh_dir / key_name
            console.print(f"[green]found {key_name}[/green]")
            break

    if key_path is None:
        console.print("[yellow]not found, generating...[/yellow]", end=" ")
        key_path = ssh_dir / "id_ed25519"
        result = subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-N", "", "-f", str(key_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            console.print(f"[red]failed: {result.stderr.strip()}[/red]")
        else:
            console.print("[green]generated id_ed25519[/green]")

    pub_path = Path(f"{key_path}.pub")
    if pub_path.exists():
        ssh_key = pub_path.read_text().strip()

        # Authorize for local SSH (needed by Terraform bpg/proxmox provider)
        authorized_keys = ssh_dir / "authorized_keys"
        existing = authorized_keys.read_text() if authorized_keys.exists() else ""
        if ssh_key not in existing:
            console.print("  Authorizing key for local SSH...", end=" ")
            with open(authorized_keys, "a") as f:
                f.write(f"{ssh_key}\n")
            authorized_keys.chmod(0o600)
            console.print("[green]ok[/green]")
    else:
        console.print("  [red]No public key found — SSH setup incomplete.[/red]")

    # Step 6: Detect Proxmox host IP (for remote API access from orchestrator VM).
    # The pool manager runs in Docker on the orchestrator, so this must be a
    # real IP reachable from VMs — never 127.0.0.1.
    proxmox_ip = "127.0.0.1"
    if is_proxmox:
        console.print("  Detecting host IP...")
        detected_ip = None

        # Prefer the IP on vmbr0 (VM bridge) — guaranteed reachable from VMs.
        try:
            result = subprocess.run(
                ["ip", "-4", "-o", "addr", "show", "vmbr0"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout.strip():
                # Parse "2: vmbr0  inet 10.20.0.1/24 ..."
                import re as _re

                m = _re.search(r"inet (\d+\.\d+\.\d+\.\d+)", result.stdout)
                if m:
                    detected_ip = m.group(1)
        except FileNotFoundError:
            pass

        # Fall back to hostname -I if vmbr0 detection failed.
        all_ipv4: list[str] = []
        if not detected_ip:
            try:
                result = subprocess.run(
                    ["hostname", "-I"],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    all_ips = result.stdout.strip().split()
                    all_ipv4 = [ip for ip in all_ips if ":" not in ip]
                    if all_ipv4:
                        detected_ip = all_ipv4[0]
            except FileNotFoundError:
                pass

        if detected_ip:
            # Always prompt so the user can verify/override, especially on
            # multi-NIC hosts where auto-detection may pick the wrong IP.
            proxmox_ip = click.prompt(
                "    Proxmox API IP (must be reachable from VMs)",
                default=detected_ip,
            )
        else:
            proxmox_ip = click.prompt(
                "    Proxmox API IP (must be reachable from VMs)",
                default="127.0.0.1",
            )

    # Step 7: Write config
    console.print("  Writing config...", end=" ")
    try:
        config = FleetConfig(
            proxmox=ProxmoxConfig(
                endpoint=f"https://{proxmox_ip}:8006",
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

    # Step 8: Copy Terraform HCL templates
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

    # Step 9: Run tofu init
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
    console.print(f"  {step}. Create orchestrator VM:    orcest fleet create-orchestrator")
    console.print(f"  {step + 1}. Create worker template:    orcest fleet create-template")
    console.print(f"  {step + 2}. Set pool size:             orcest fleet set-pool-size <N>")
    org_step = step + 3
    console.print(
        f"  {org_step}. Register an org:           orcest fleet add-org <org>"
        " --github-token ... --claude-token ..."
    )
    # Indent the hint to align under the step text (e.g. "  4. " -> 5 chars).
    hint_indent = " " * (len(str(org_step)) + 4)
    console.print(
        f"{hint_indent}GitHub token: classic PAT with [bold]repo + workflow[/bold] scopes,"
        " or fine-grained with contents/issues/PRs/actions R/W"
    )
    console.print(f"  {step + 4}. Onboard a repo:            orcest fleet onboard <owner/repo>")


@main.command()
def upgrade() -> None:
    """Update the orcest CLI to the latest version from GitHub.

    Reinstalls the package and refreshes Terraform templates.
    """
    import shutil
    import subprocess
    from pathlib import Path

    from orcest.fleet.cli import _upgrade_cli
    from orcest.fleet.config import DEFAULT_CONFIG_DIR

    console = Console()
    console.print("\n[bold]Updating orcest[/bold]\n")

    # Step 1: Reinstall from GitHub
    _upgrade_cli(console)

    # Step 2: Copy Terraform templates if config dir exists
    terraform_dest = DEFAULT_CONFIG_DIR / "terraform"
    if terraform_dest.is_dir():
        console.print("  Updating Terraform templates...", end=" ")
        pip = Path(sys.executable).parent / "pip"
        # Use pip show to find the installed package location (importlib.reload
        # may not pick up the new path after force-reinstall).
        loc_result = subprocess.run(
            [str(pip), "show", "orcest"],
            capture_output=True,
            text=True,
        )
        pkg_location = None
        for line in loc_result.stdout.splitlines():
            if line.startswith("Location:"):
                pkg_location = Path(line.split(":", 1)[1].strip())
                break

        terraform_src = None
        if pkg_location:
            terraform_src = pkg_location / "orcest" / "fleet" / "terraform"
        if terraform_src and terraform_src.is_dir():
            for hcl_file in terraform_src.iterdir():
                if hcl_file.is_file():
                    shutil.copy2(hcl_file, terraform_dest / hcl_file.name)
            console.print("[green]ok[/green]")
        else:
            console.print("[yellow]source templates not found[/yellow]")

    console.print("\n[bold]Update complete.[/bold]")


@main.command("init-labels")
@click.option("--config", default="config/orchestrator.yaml", help="Config file (for repo/token).")
def init_labels(config: str) -> None:
    """Create orcest labels on all configured project repos."""
    import os
    import subprocess

    from orcest.shared.config import load_orchestrator_config

    cfg = load_orchestrator_config(config)
    console = Console()
    label_defs = [
        (cfg.labels.blocked, "d93f0b", "Blocked — waiting for dependency"),
        (cfg.labels.needs_human, "b60205", "Orcest failed — needs manual review"),
        (cfg.labels.ready, "0e8a16", "Issue is ready for orcest to implement"),
    ]

    if not cfg.projects:
        console.print("[red]No projects configured.[/red]")
        raise SystemExit(1)

    total_failures = 0
    for project in cfg.projects:
        repo = project.repo
        token = project.token
        console.print(f"\n[bold]{repo}[/bold]")

        env = dict(os.environ)
        env["GITHUB_TOKEN"] = token
        env["GH_TOKEN"] = token

        for name, color, description in label_defs:
            console.print(f"  Creating label [cyan]{name}[/cyan]...", end=" ")
            try:
                subprocess.run(
                    [
                        "gh",
                        "label",
                        "create",
                        name,
                        "--repo",
                        repo,
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
                total_failures += 1
            except FileNotFoundError:
                console.print("[red]gh CLI not found[/red]")
                raise SystemExit(1)

    if total_failures:
        console.print(f"\n[red]{total_failures} label(s) failed.[/red]")
        raise SystemExit(1)
    repos = ", ".join(p.repo for p in cfg.projects)
    console.print(f"\nLabels ready on: [bold]{repos}[/bold].")


@main.command()
@click.argument("host")
@click.option("--user", default="root", help="SSH user for the target host.")
@click.option("--worker-config", default="config/worker.yaml", help="Worker config to deploy.")
@click.option("--env-file", default="provision/.env", help="Env file with secrets.")
def provision(host: str, user: str, worker_config: str, env_file: str) -> None:
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


@main.command("pool-manage")
@click.option("--config", default="/etc/orcest/config.yaml", help="Fleet config path.")
@click.option("--interval", default=10.0, type=float, help="Reconciliation interval in seconds.")
def pool_manage(config: str, interval: float) -> None:
    """Run the warm pool manager (long-running service).

    Maintains a pool of pre-booted ephemeral worker VMs. Monitors for
    completed workers, destroys finished VMs, and clones replacements
    to maintain the target pool size.
    """
    import logging as _logging

    from orcest.fleet.config import load_config
    from orcest.fleet.pool_manager import PoolManager
    from orcest.fleet.proxmox_api import ProxmoxClient
    from orcest.shared.config import build_redis_config
    from orcest.shared.redis_client import RedisClient

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    console = Console()
    cfg = load_config(config)

    if not cfg.pool.template_vm_id and cfg.pool.template_range() is None:
        console.print(
            "[red]Error: neither pool.template_vm_id nor "
            "pool.template_vmid_range is configured in fleet config.[/red]"
        )
        raise SystemExit(1)

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Error: Proxmox API credentials not configured.[/red]")
        raise SystemExit(1)

    if interval <= 0:
        console.print("[red]Error: --interval must be positive.[/red]")
        raise SystemExit(1)

    proxmox = ProxmoxClient(
        endpoint=cfg.proxmox.endpoint,
        token_id=cfg.proxmox.api_token_id,
        token_secret=cfg.proxmox.api_token_secret,
        node=cfg.proxmox.node,
    )

    # Build Redis config from ORCEST_REDIS_* env vars (set by Docker Compose),
    # falling back to defaults (localhost:6379, prefix "orcest").
    redis_cfg = build_redis_config()
    redis = RedisClient(redis_cfg)

    if not redis.health_check():
        redis.close()
        console.print("[red]Error: Cannot connect to Redis.[/red]")
        raise SystemExit(1)

    console.print(
        f"[bold]Starting pool manager[/bold] (target={cfg.pool.size}, interval={interval}s)"
    )

    manager = PoolManager(
        config=cfg,
        proxmox=proxmox,
        redis=redis,
        key_prefix=redis_cfg.key_prefix,
    )
    try:
        manager.run(interval=interval)
    finally:
        redis.close()


@main.command("monitor")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
def monitor_cmd(config_path: str) -> None:
    """Run the read-only monitor service (ingest + query listeners)."""
    from orcest.monitor.config import load_monitor_config
    from orcest.monitor.service import run_monitor

    run_monitor(load_monitor_config(config_path))


main.add_command(fleet)


# ── check commands ──────────────────────────────────────────


@main.group()
def check() -> None:
    """Self-test commands for validating tokens, connectivity, etc."""


@check.command("github-token")
def check_github_token() -> None:
    """Validate a GitHub token read from stdin.

    Reads a token from stdin, sets GH_TOKEN, and runs ``gh api user``
    to verify validity against the GitHub API. Exits 0 on success, 1 on failure.
    """
    import json
    import os
    import subprocess

    token = sys.stdin.read().strip()
    if not token:
        click.echo("Error: no token provided on stdin", err=True)
        raise SystemExit(1)
    if "\n" in token or "\r" in token:
        click.echo("Error: token contains newlines; provide exactly one token on stdin", err=True)
        raise SystemExit(1)

    env = {**os.environ, "GH_TOKEN": token, "GITHUB_TOKEN": token}
    result = subprocess.run(
        ["gh", "api", "user"],
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode == 0:
        try:
            user = json.loads(result.stdout)
            click.echo(f"Token valid: authenticated as {user.get('login', 'unknown')}")
        except json.JSONDecodeError:
            click.echo("Token valid")
    else:
        stderr = (result.stderr or "").strip()
        if stderr:
            click.echo(stderr, err=True)
        else:
            click.echo("Token validation failed", err=True)
    raise SystemExit(result.returncode)


def _resolve_archive_root(archive_root: str | None) -> Path:
    """Return the archive root path or exit with a helpful message.

    Precedence: CLI flag > ORCEST_TRACE_ARCHIVE_ROOT env var > fleet config
    (``/etc/orcest/config.yaml``: ``trace_archive_host_path``).
    """
    import os as _os

    if archive_root:
        return Path(archive_root)
    env_value = _os.environ.get("ORCEST_TRACE_ARCHIVE_ROOT")
    if env_value:
        return Path(env_value)
    try:
        from orcest.fleet.config import load_config as _load_fleet_config

        cfg = _load_fleet_config()
        if cfg.trace_archive_host_path:
            return Path(cfg.trace_archive_host_path)
    except Exception:
        pass
    click.echo(
        "Error: archive root not found. Pass --archive-root, set "
        "ORCEST_TRACE_ARCHIVE_ROOT, or configure trace_archive_host_path in "
        "/etc/orcest/config.yaml.",
        err=True,
    )
    raise SystemExit(1)


_TRACE_TASK_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")


def _trace_paths_for_task(root: Path, task_id: str) -> tuple[Path, Path] | None:
    """Resolve (jsonl_path, meta_path) for a task_id using the index pointer.

    Returns None if the task is unknown OR if the index pointer would direct
    us outside ``root`` (defense against a hostile pointer file planted on a
    multi-tenant share).
    """
    if not _TRACE_TASK_ID_RE.match(task_id):
        return None
    pointer = root / "index" / "by-task-id" / task_id[:2] / task_id
    try:
        rel = pointer.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not rel:
        return None
    try:
        root_resolved = root.resolve(strict=False)
        jsonl = (root / rel / f"{task_id}.jsonl").resolve(strict=False)
        meta = (root / rel / f"{task_id}.meta.json").resolve(strict=False)
    except OSError:
        return None
    if not jsonl.is_relative_to(root_resolved) or not meta.is_relative_to(root_resolved):
        return None
    return jsonl, meta


@main.command("trace")
@click.argument("identifier", required=False)
@click.option("--meta", "show_meta", is_flag=True, help="Print the .meta.json sidecar.")
@click.option(
    "--pr",
    "pr_ref",
    default=None,
    help="Find the most recent task for a PR. Format: 'owner/repo#NUMBER'.",
)
@click.option(
    "--list",
    "list_project",
    default=None,
    help="List archived task IDs in this project (newest first).",
)
@click.option(
    "--archive-root",
    default=None,
    help="Override the archive root (default: ORCEST_TRACE_ARCHIVE_ROOT or fleet config).",
)
@click.option("--raw", is_flag=True, help="Print raw JSONL instead of pretty-formatting it.")
@click.option(
    "--limit",
    default=50,
    type=int,
    help="Max entries returned by --list (default: 50).",
    show_default=False,
)
def trace(
    identifier: str | None,
    show_meta: bool,
    pr_ref: str | None,
    list_project: str | None,
    archive_root: str | None,
    raw: bool,
    limit: int,
) -> None:
    """Inspect an archived worker trace.

    Examples:
        orcest trace 9a686e05-a81a-4d22-b17d-be7c17d17b0e
        orcest trace 9a686e05-... --meta
        orcest trace --pr bluebamboollc/bbr-platform#3546
        orcest trace --list bbr-platform --limit 20

    Note: this is not retroactive — only tasks completed after the archiver
    was enabled appear here. Older traces that lived only in Redis are gone
    once their stream MAXLEN trimmed them out.
    """
    import json as _json

    from orcest.dashboard import format_stream_json_line

    root = _resolve_archive_root(archive_root)
    if not root.exists():
        click.echo(f"Error: archive root {root} does not exist.", err=True)
        raise SystemExit(1)

    if list_project:
        if limit <= 0:
            click.echo("Error: --limit must be positive.", err=True)
            raise SystemExit(1)
        project_dir = root / list_project
        if not project_dir.is_dir():
            click.echo(f"Error: project '{list_project}' not found at {project_dir}", err=True)
            raise SystemExit(1)
        entries: list[tuple[float, Path]] = []
        for meta_file in project_dir.rglob("*.meta.json"):
            try:
                mtime = meta_file.stat().st_mtime
            except OSError:
                continue
            entries.append((mtime, meta_file))
        entries.sort(reverse=True)
        if not entries:
            click.echo(f"(no archived traces in {list_project})")
            return
        console = Console(file=sys.stdout)
        table = Table(show_header=True, header_style="bold")
        table.add_column("task_id")
        table.add_column("when")
        table.add_column("status")
        table.add_column("resource")
        for _, meta_file in entries[:limit]:
            try:
                meta = _json.loads(meta_file.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            task_id = str(meta.get("task_id", meta_file.stem.removesuffix(".meta")))
            ended = str(meta.get("ended_at") or meta.get("started_at", ""))
            status = str(meta.get("status", ""))
            resource = (
                f"{meta.get('resource_type', '')} #{meta.get('resource_id', '')}"
                if meta.get("resource_id")
                else ""
            )
            table.add_row(task_id, ended, status, resource)
        console.print(table)
        return

    if pr_ref:
        if "#" not in pr_ref:
            click.echo("Error: --pr expects 'owner/repo#NUMBER' format.", err=True)
            raise SystemExit(1)
        repo, num_str = pr_ref.split("#", 1)
        try:
            num = int(num_str)
        except ValueError:
            click.echo(f"Error: PR number '{num_str}' is not an integer.", err=True)
            raise SystemExit(1)
        best: tuple[float, Path] | None = None
        for meta_file in root.rglob("*.meta.json"):
            try:
                meta = _json.loads(meta_file.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if meta.get("repo") != repo:
                continue
            if meta.get("resource_type") != "pr":
                continue
            try:
                if int(meta.get("resource_id", 0)) != num:
                    continue
            except (TypeError, ValueError):
                continue
            try:
                mtime = meta_file.stat().st_mtime
            except OSError:
                continue
            if best is None or mtime > best[0]:
                best = (mtime, meta_file)
        if best is None:
            click.echo(f"No archived trace found for PR {pr_ref}.", err=True)
            raise SystemExit(1)
        identifier = best[1].stem.removesuffix(".meta")

    if not identifier:
        click.echo("Error: pass a task-id, --pr, or --list.", err=True)
        raise SystemExit(1)

    paths = _trace_paths_for_task(root, identifier)
    if paths is None:
        click.echo(f"No archived trace found for task {identifier}.", err=True)
        raise SystemExit(1)
    jsonl_path, meta_path = paths

    if show_meta:
        if not meta_path.exists():
            click.echo(f"No .meta.json sidecar for task {identifier}.", err=True)
            raise SystemExit(1)
        click.echo(meta_path.read_text(encoding="utf-8"), nl=False)
        return

    if not jsonl_path.exists():
        click.echo(f"No .jsonl file for task {identifier}.", err=True)
        raise SystemExit(1)

    if raw:
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                click.echo(line, nl=False)
        return

    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            formatted = format_stream_json_line(line.strip())
            if formatted:
                click.echo(formatted)
