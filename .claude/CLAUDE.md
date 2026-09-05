# Orcest

Autonomous CI/CD orchestration system. Replaces the Ralph system.

## Skills

Canonical skills live in `.agents/skills/` and are symlinked into `.claude/skills`, `.grok/skills`, `.codex/skills`, `.gemini/skills`, `.opencode/skills`, and `.cursor/skills`. Those symlinks need `core.symlinks` at checkout time; on a Windows clone without it they land as plain text files and the skill silently never loads — see [`.agents/skills/spec/references/packaging.md`](../.agents/skills/spec/references/packaging.md#symlinks-and-windows-checkouts).

- **`/spec`** — Fully specify a system/feature, then file a GitHub issue graph with `blocked-by` links so the fleet can pick unblocked `orcest:ready` work. Do **not** implement in that session.
- **`/review`** — Parallel review of local git changes (existing Claude skill).

## Architecture

- **Orchestrator**: Single instance, polls GitHub, runs heuristics, enqueues tasks to Redis. Deployed via Docker Compose on `thayne-claude-dev-01.home.prefixa.net`.
- **Workers**: N instances on bare VMs, block on Redis streams, clone repos, run Claude. Deployed via systemd.
- **Redis**: Task queue (streams), distributed locks (SET NX EX), operational memory.
- **Events + Monitor**: Orchestrator/workers emit CloudEvents-shaped lifecycle events onto a Redis stream (`events`, MAXLEN 50000); an orchestrator-side relay forwards them to the `orcest monitor` service (`Dockerfile.monitor`, `docker-compose.monitor.yml`) — a private write-token ingest listener (`:9091`) plus a public read-only, scope-authenticated query listener (`:9090`, SQLite `mode=ro`). See `docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md` and `docs/monitor-exposure-runbook.md` for external (Cloudflare Tunnel + Access) exposure.
- **Activity watchdog**: Each worker runs a per-task liveness ladder (stdout classification + process-tree/workspace sampling + tool-call repetition detection, `worker/liveness_tracker.py`) that escalates through SUSPECT/STUCK/LOOPING and can kill a stalled task early, subject to a fleet-wide kill budget and pressure gate. It writes a `workers:activity:{worker_id}` Redis hash every sample; `PoolManager._health_check` is activity-aware, destroying a VM below `max_task_duration` on `needs_reap`, or on an absent/stale activity record **only when the worker's `workers:heartbeat:{worker_id}` liveness heartbeat (written by every worker regardless of watchdog config) is also absent**, work is still pending, **and elapsed time is at least `activity_stale_min_elapsed` (default 600s)** — never on elapsed time alone, never because a young task has not written activity yet, and never just because the watchdog is off or an old image never wrote an activity record. `needs_reap` and the absolute duration ceiling bypass that floor. `watchdog.enabled: false` is the rollback lever, restoring wall-clock-only behavior (fixed runner timeout + ceiling-only reaper) at this migration's raised ceiling values, and `PoolConfig.watchdog_enabled` is the same lever at the fleet level (rendered clone-time, no rebake needed). `worker_runner_mode: interactive` (PTY Claude) workers never get ladder coverage at all and have their `runner.timeout` pinned to the pre-branch 5400s default instead of the raised ceiling — move a project's Claude profiles to `worker_runner_mode: headless` for real ladder coverage. See `docs/monitor-exposure-runbook.md`'s "Watchdog rollout" section for staged enablement.

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
# Install in dev mode (locked)
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps --no-build-isolation -e .

# Fast local aggregate: lint-check + typecheck + unit tests
make check-fast

# Full local aggregate: check-fast + integration + stress + dashboard
# Does not include CI-only image builds or dashboard Compose/image smokes.
make check-full

# Compatibility: unit, then invocation-scoped integration/stress Redis, then dashboard
make test

# Individual leaves
make lint-check
make typecheck
make test-unit
make test-integration
make test-stress
make test-dashboard

# Format (applies changes)
make format
```

## Conventions

- Python 3.12+, type hints everywhere
- Structured logging with `rich`
- Config via YAML files (dataclass schemas)
- Redis streams for task distribution, SET NX EX for locking
- All GitHub interaction via `gh` CLI (not API directly)
- Click for CLI, Rich for terminal output
- Never use the word "load-bearing". Say what actually depends on the thing,
  or what breaks without it. Applies to code comments, commit messages, PR and
  issue text, and chat.

## Dashboard

See [product vision](../docs/vision.md) and [dashboard runbook](../docs/fleet-dashboard.md).
The fleet dashboard projects observations without making scheduling decisions.
Configured provider accounts, VM allocation, and running agents are separate concepts.
GitHub remains an inspectable source of work and delivery evidence:
- Labels: `orcest:ready`, `orcest:needs-human`
- Comments on PRs/issues for status updates
- `orcest status` CLI for system health

## Issue Dependencies

Issues labeled `orcest:ready` with a still-open prerequisite are
automatically deferred. Two sources are checked (see
`src/orcest/orchestrator/issue_deps.py` and
`docs/issue-dependencies.md`); an open blocker in either defers:

1. **GitHub-native blocked-by relationships** (issue sidebar /
   `addBlockedBy` API). Fetched inline with the issue listing — zero
   extra API calls. Cross-repo blockers supported.
2. **Body-text patterns** (case-insensitive, same-repo only):
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
