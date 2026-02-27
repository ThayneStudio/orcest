# Orcest

Autonomous CI/CD orchestration system. Replaces the Ralph system.

## Architecture

- **Orchestrator**: Single instance, polls GitHub, runs heuristics, enqueues tasks to Redis. Deployed via Docker Compose on `thayne-claude-dev-01.home.prefixa.net`.
- **Workers**: N instances on bare VMs, block on Redis streams, clone repos, run Claude. Deployed via systemd.
- **Redis**: Task queue (streams), distributed locks (SET NX EX), operational memory.

Workers are repo-agnostic. Project context comes from each repo's `.claude/` directory.

## Project Structure

```
src/orcest/
  cli.py              # Click CLI: orchestrate, work, status, provision
  orchestrator/       # GitHub polling, CI triage, task publishing
  worker/             # XREADGROUP loop, workspace management, Claude runner
  shared/             # Config, Redis client, coordination, models, logging
```

## Development

```bash
# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src/

# Format
ruff format src/
```

## Conventions

- Python 3.12+, type hints everywhere
- Structured logging with `rich`
- Config via YAML files (dataclass schemas)
- Redis streams for task distribution, SET NX EX for locking
- All GitHub interaction via `gh` CLI (not API directly)
- Click for CLI, Rich for terminal output

## Dashboard

GitHub itself is the dashboard:
- Labels: `orcest:queued`, `orcest:in-progress`, `orcest:blocked`, `orcest:needs-human`
- Comments on PRs/issues for status updates
- `orcest status` CLI for system health

## Key Commands

- `orcest orchestrate` -- start orchestrator loop
- `orcest work --id <id>` -- start worker loop
- `orcest status` -- system health dashboard
- `orcest provision <host>` -- provision a worker VM
