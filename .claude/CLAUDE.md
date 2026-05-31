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

# Run all tests (starts Redis via Docker, runs everything, stops Redis)
make test

# Run unit tests only (no Redis needed)
make test-unit

# Lint
make lint

# Format
make format
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
- Labels: `orcest:ready`, `orcest:blocked`, `orcest:needs-human`
- Comments on PRs/issues for status updates
- `orcest status` CLI for system health

## Issue Dependencies

Issues labeled `orcest:ready` whose body declares a still-open
prerequisite are automatically deferred (no manual `orcest:blocked`
needed). Recognised body-text patterns (case-insensitive, same-repo
only, see `src/orcest/orchestrator/issue_deps.py`):
- `blocked by #N`
- `depends on #N`
- `requires #N`
- `prerequisite[s]: #N`
- `after #N {merges|lands|closes|ships|is done}`
- unchecked task-list item: `- [ ] #N`

`Closes #N` / `Fixes #N` / `Resolves #N` are **not** treated as
dependencies — those describe the PR's output. Bare `#N` mentions
without one of the prefixes above are ignored to avoid noise.

## Key Commands

- `orcest orchestrate` -- start orchestrator loop
- `orcest work --id <id>` -- start worker loop
- `orcest status` -- system health dashboard
- `orcest provision <host>` -- provision a worker VM

## Multi-Provider Architecture (Provider Registration & Invocation Boundary)

Orcest supports multiple coding agents ("providers") — starting with Claude and Grok (xAI) — while strictly separating concerns:

- **Orchestrator is provider-agnostic**: It only ever sees `ProviderEntry` objects (provider name + credential + model + optional extras). It performs round-robin selection + exhaustion tracking inside `ProviderPool`, then publishes lean `Task` records containing `provider`, `credential`, `model`. No orchestrator code path knows *how* any provider is executed (binary name, env var, CLI flags, output format).

- **Workers own execution**: Every worker image contains a local `PROVIDER_REGISTRY` (see `src/orcest/worker/runner.py`). The registry maps the opaque `task.provider` string to a `ProviderRecipe` (binary + env_var). The generic runner (`claude_runner.py`) uses the recipe to:
  - Locate the CLI on $PATH (must be baked into the image via `provision/setup-worker.sh`).
  - Inject the per-task credential under exactly the declared `env_var` (only into the child environment, never on argv or in logs).
  - Parse stream-json / rate-limit signals.

- **Graceful skew handling**: If a worker receives a `Task` whose `provider` is not present in its local registry (or the binary is missing), it immediately emits a permanent (non-transient) `FAILED` result whose summary tells the operator to "rebake worker image to include `<provider>` CLI". No secret leakage, no stuck tasks, no impact on other providers.

- **Configuration surface**: `providers:` lists (or legacy `claude_tokens` synthesis) live in `orchestrator.yaml` / fleet `orgs`. Credentials may be supplied inline or via conventional environment variables (`CLAUDE_CODE_OAUTH_TOKEN`, `XAI_API_KEY`, etc.) — see `_PROVIDER_ENV_CANDIDATES` and `_parse_provider_entry` in `shared/config.py`.

- **Adding a provider** (see `docs/adding-a-provider.md` and the runbook inside `provision/setup-worker.sh`): one line in the worker `PROVIDER_REGISTRY` + one install block in `setup-worker.sh` + rebake. The orchestrator YAML change is purely declarative.

This boundary (orchestrator = registration/selection/exhaustion; workers = baked execution) was the non-negotiable outcome of the architecture review and is enforced in every code path and document.

Update the Architecture bullet list above if the high-level description needs refreshing for new providers.
