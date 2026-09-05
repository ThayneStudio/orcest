# Orcest

Autonomous CI/CD orchestration. A single orchestrator watches GitHub, runs
heuristics, and hands work to a managed fleet of ephemeral worker VMs that
invoke coding agents — Claude, Grok, or Codex — against the target repo.

GitHub is the dashboard. Redis is coordination state. There are no
long-lived scripts.

---

## Overview

Orcest turns GitHub issues and PRs into work for coding agents (Claude,
Grok, Codex) running on a managed fleet of VMs. It replaces the older
Ralph system: instead of long-lived scripts, a single orchestrator
watches GitHub, decides what is actionable, and hands tasks to ephemeral,
repo-agnostic workers that clone the target repo and run an agent CLI
against it. GitHub itself is the dashboard — labels, comments, and PR
state are the source of truth; Redis is purely coordination state
(queues, locks, retry counters).

## Architecture

A single orchestrator container polls GitHub on a cadence, runs CI /
review / issue-dependency heuristics, and enqueues lean `Task` records
onto per-stream Redis queues. Workers are ephemeral VMs provisioned from
baked templates; each blocks on a Redis stream via `XREADGROUP`, claims
one task, clones the target repo into a workspace, and invokes the
appropriate agent CLI. PRs and issues remain the source of truth — Redis
can delay work, but never permanently redefines what GitHub says is
actionable (see [`docs/orchestrator-state-machine.md`](docs/orchestrator-state-machine.md)).

- **Orchestrator** — single instance per project, deployed via Docker
  Compose. Owns GitHub polling, CI triage, issue triage, task publishing,
  per-provider round-robin selection, and exhaustion tracking.
- **Workers** — pool of ephemeral VMs (one task per VM lifecycle),
  managed by the fleet pool manager on Proxmox. Cloned from a baked
  template that already contains the agent CLIs. Repo-agnostic: project
  context comes from each repo's own `.claude/` directory.
- **Redis** — task distribution via streams, distributed locks via
  `SET NX EX`, pending markers tied to PR head SHAs, attempt counters,
  per-provider exhaustion keys, and operational memory.
- **GitHub as dashboard** — labels (`orcest:ready`,
  `orcest:needs-human`), PR/issue comments for status, and `orcest status`
  for a Rich/Textual TUI of queue and worker health.
- **Snapshot validation** — every task carries the PR head SHA plus a
  decision reason (`ci_failure`, `changes_requested`,
  `merge_conflict_rebase`, ...). Workers cheap-validate before running;
  orchestrator re-validates before applying labels or escalating
  attempts. Stale tasks/results are dropped without mutating GitHub.
- **Events spool** — orchestrator and workers emit CloudEvents-shaped
  lifecycle events (task started/completed/failed, VM reaped, ...) onto a
  per-project Redis stream (`events`, `MAXLEN` 50000); emission never
  raises into the producer's main path (swallow-and-log). Events never
  carry raw tool arguments, tool output, prompts, or credentials.
- **Activity watchdog** — each worker runs a per-task liveness ladder
  (`worker/liveness_tracker.py`: stdout-line classification, process-tree
  and workspace sampling, tool-call repetition detection) that escalates
  BOOTSTRAP → ACTIVE/WAITING → SUSPECT → STUCK/LOOPING and can kill a
  stalled task early, gated by a fleet-wide kill budget and pressure gate
  so it never piles on during a fleet-wide incident. Every sample it writes
  a global `workers:activity:{worker_id}` Redis hash
  (`task_id`/`state`/`last_liveness_ts`/`needs_reap`). `PoolManager
  ._health_check` consumes it: below the hard `max_task_duration` ceiling
  it destroys a VM when `needs_reap` is set, or when the activity record is
  absent/stale **and** the worker's `workers:heartbeat:{worker_id}`
  liveness heartbeat (written by every worker unconditionally, watchdog on
  or off) is *also* absent **and** the consumer still has pending work —
  never on elapsed time alone, never on a missing activity record by
  itself (that's expected whenever the watchdog is off or the worker
  predates it), and a fresh record blocks destruction outright.
  `watchdog.enabled: false` is the rollback lever, restoring
  wall-clock-only behavior — the runner's fixed `timeout` plus a
  ceiling-only reaper — at this migration's raised default ceilings, not a
  return to the original pre-migration numbers. See
  [`docs/monitor-exposure-runbook.md`](docs/monitor-exposure-runbook.md)'s
  "Watchdog rollout" section for staged enablement.
- **Event relay** — an orchestrator-side loop tails the events stream and
  forwards batches to the monitor's ingest listener over HTTP, tracking
  its own Redis cursor (`event_relay:cursor`). Disabled unless
  `monitor_ingest_url` is configured.
- **Monitor** — a separate `orcest monitor` service with two listeners:
  a private ingest API (write-token authenticated) and a public read-only
  query API (SQLite opened read-only, scoped bearer tokens per reader:
  `events:read`, `traces:read`). Deployed via `docker-compose.monitor.yml`;
  see [`docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md`](docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md)
  for the design and [`docs/monitor-exposure-runbook.md`](docs/monitor-exposure-runbook.md)
  for standing it up and exposing it externally via Cloudflare Tunnel +
  Access.

## Multi-provider model

Orcest currently supports three coding agents — **Claude** (Anthropic),
**Grok** (xAI), and **Codex** (OpenAI) — behind a strict
registration/invocation boundary:

- **Orchestrator is provider-agnostic.** It only sees `ProviderEntry`
  objects (provider name + credential + model + extras), round-robins
  through them in `ProviderPool`, and publishes lean `Task` records
  containing `provider`, `credential`, and `model`. It does not know any
  CLI binary name, env var, flag, or output format.
- **Workers own execution.** Each worker image carries a local
  `PROVIDER_REGISTRY` in `src/orcest/worker/runner.py` that maps the
  opaque `task.provider` string to a `ProviderRecipe` (binary + env var +
  runner class). The credential is injected only into the child process
  environment under the declared env var — never on argv, never in logs.
- **Graceful skew handling.** A task for an unknown provider, or one
  whose CLI isn't baked into the image, fails permanently with an
  operator message ("rebake worker image to include `<provider>` CLI").
  No secret leakage, no stuck tasks, no impact on other providers.
- **Adding a provider** is one line in `PROVIDER_REGISTRY`, one install
  block in `provision/setup-worker.sh`, and a rebake (see
  [`docs/adding-a-provider.md`](docs/adding-a-provider.md) and
  [`docs/rollout-multi-provider.md`](docs/rollout-multi-provider.md)).

## Project layout

```text
src/orcest/
  cli.py              # Click CLI: orchestrate, work, status, dead-letters,
                      # init, upgrade, init-labels, provision, pool-manage,
                      # trace, plus the `fleet` subgroup
  dashboard.py        # Rich/Textual TUI snapshot for `orcest status`

  orchestrator/       # Single-instance orchestrator
    loop.py             # Main poll loop, decision cascade, result handling
    gh.py               # Thin `gh` CLI wrapper (PR/issue state, labels)
    ci_triage.py        # CI failure classification and task selection
    pr_ops.py           # PR-side discovery + snapshot construction
    issue_ops.py        # Issue discovery cascade (labels, attempts, deps)
    issue_deps.py       # Body-text prerequisite parser + resolver
    task_publisher.py   # Lean `Task` -> Redis stream
    provider_pool.py    # Round-robin + per-credential exhaustion tracking
    token_pool.py       # Legacy claude_tokens shim
    usage_check.py      # Rate-limit / exhaustion signal detection
    trace_archiver.py   # Verbatim per-task trace archiving
    deployment.py       # Orchestrator deployment helpers

  worker/             # Ephemeral worker process (one per VM lifecycle)
    loop.py             # XREADGROUP loop + early provider dispatch
    runner.py           # PROVIDER_REGISTRY, ProviderRecipe, dispatch
    _runner_base.py     # Shared runner scaffolding
    claude_runner.py    # Generic stream-json runner
    grok_runner.py      # Grok Path B OAuth blob handling
    codex_runner.py     # Codex Path B OAuth blob handling
    noop_runner.py      # Test/stress runner
    workspace.py        # Repo clone + worktree lifecycle
    heartbeat.py        # Worker liveness reporting

  fleet/              # Proxmox fleet management
    cli.py              # `orcest fleet ...` subcommands
    pool_manager.py     # Worker VM pool sizing + template pointer
    provisioner.py      # Template build / rebake
    proxmox_api.py      # Proxmox HTTP client
    cloud_init.py       # Per-VM cloud-init seeding
    orchestrator.py     # Orchestrator VM lifecycle
    config.py           # Fleet YAML schema
    deploy/             # Compose files, systemd units, install scripts
    terraform/          # Templates for VM provisioning

  shared/             # Cross-cutting primitives (no GitHub or agent knowledge)
    config.py           # YAML loading, ProviderEntry parsing, env fallbacks
    models.py           # Task, Result, dead-letter constants, redacted fields
    redis_client.py     # Wrapped Redis client + key prefixing
    coordination.py     # Locks (SET NX EX), pending markers, attempt counters
    providers.py        # Provider name / env-var conventions
    logging.py          # Structured logging via Rich
```

## Key features

- **GitHub polling + label-driven triage** — discovers actionable PRs and
  issues via labels (`orcest:ready` and terminal `orcest:needs-human`)
  and per-snapshot decision reasons.
- **CI triage** — captures failing check names at enqueue time, drops
  tasks whose CI predicate no longer applies before running.
- **Issue dependency deferral** — `orcest:ready` issues whose body
  declares an open prerequisite (`blocked by #N`, `depends on #N`,
  `requires #N`, `- [ ] #N`, ...) are auto-deferred without manual
  labels. See [`docs/issue-dependencies.md`](docs/issue-dependencies.md).
- **Per-task trace archive** — verbatim agent transcripts archived under
  a configurable path (e.g. `/mnt/truenas-logs/orcest-traces`) so any
  decision can be post-mortemed via `orcest trace`.
- **Fleet management on Proxmox** — `orcest fleet` subcommands for
  start / stop / deploy / update / onboard / rebake / pool sizing, with
  ephemeral worker VMs cloned from a baked template.
- **Multi-provider** — Claude, Grok, and Codex behind a
  provider-agnostic orchestrator and a worker-baked execution registry,
  with graceful version-skew handling.

---

## Installation

Orcest development uses **Python 3.12**, `pip==24.0`, and
`pip-tools==7.5.2` to regenerate the development lock. Clone the repo
and install the locked development environment in editable mode:

```bash
git clone https://github.com/ThayneStudio/orcest.git
cd orcest
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps --no-build-isolation -e .
```

Available extras:

- `dev` — pytest (+ coverage, mock, timeout), `fakeredis`, `ruff`,
  `mypy`, `build`, `pip-tools`. This is the only declared
  optional-dependency group in `pyproject.toml`.

The install registers a single console script entry point, `orcest`,
which dispatches all commands below.

## Development workflow

The `Makefile` provides the canonical developer targets:

| Target                               | What it does                                                                                              |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| `make lint-check`                    | `ruff check src/ tests/` and `ruff format --check src/ tests/`.                                           |
| `make typecheck`                     | `mypy src/`.                                                                                              |
| `make test-unit`                     | Runs only tests marked `unit` (uses `fakeredis` / mocks; no external services required).                  |
| `make test-integration`              | Starts an invocation-scoped Redis and runs `pytest -m integration` (including inline markers outside `tests/integration/`). |
| `make test-stress`                   | Starts an invocation-scoped Redis and runs `pytest -m stress`.                                            |
| `make test-dashboard`                | Runs dashboard install, typecheck, tests, build, and bundle-runtime check in the pinned Node Docker image. |
| `make check-fast`                    | Aggregate of `lint-check`, `typecheck`, and `test-unit`.                                                   |
| `make check-full`                    | Aggregate of `check-fast`, `test-integration`, `test-stress`, and `test-dashboard`. Does not include CI-only image builds or dashboard Compose/image smokes. |
| `make test`                          | Compatibility entry point: `test-unit`, managed `test-integration`, managed `test-stress`, then `test-dashboard`. Stops on the first failing phase. |
| `make lint`                          | `ruff check src/ tests/`                                                                                  |
| `make format`                        | `ruff format src/ tests/`                                                                                 |
| `make redis-up` / `make redis-down`  | Manual helpers for the shared `docker-compose.redis.yml` service used by local orchestrator development. Not used by correctness targets. |
| `make lock`                          | Regenerate the runtime `requirements.lock` via `pip-compile`.                                             |
| `make lock-dev`                      | Regenerate `requirements-dev.lock` from the `dev` extra and PEP 517 build requirements, constrained by the runtime lock. |
| `make check-lock-dev`                | Regenerate `requirements-dev.lock` into a temporary file and compare it with the committed lock.           |
| `make audit-dashboard`               | Runs `npm audit --audit-level=$(DASHBOARD_AUDIT_LEVEL)` on its own. Kept out of `test-dashboard` (and non-blocking in CI) so a new registry advisory cannot fail an unrelated PR. |
| `make build-dashboard`               | Builds the dashboard in the pinned Node Docker image.                                                      |
| `make smoke-dashboard-compose`       | Builds the dashboard Compose stack with an authenticated Redis container and verifies `/api/ready`.        |
| `make dev-dashboard`                 | Runs the dashboard dev server in the pinned Node Docker image at `http://127.0.0.1:5173/?token=dev-dashboard-token` unless `DASHBOARD_TOKEN` is set. Redis defaults to `host.docker.internal:6379`; override `DASHBOARD_DEV_REDIS_HOST` / `DASHBOARD_DEV_REDIS_PORT` when needed. `REDIS_PASSWORD` or `ORCEST_REDIS_PASSWORD` is forwarded into the container for authenticated Redis. Set `DASHBOARD_REDIS_PREFIXES=orcest,project-a` to restrict dashboard scans in shared Redis; add `unprefixed` to include legacy unprefixed keys. |

Pytest markers in use (declared in `pyproject.toml`): `unit`,
`integration`, `stress`. The default `--timeout=60` is applied to every
test.

## CLI reference

All commands are subcommands of `orcest`. Docstrings below are taken
verbatim from the implementations in `src/orcest/cli.py` and
`src/orcest/fleet/cli.py`.

### Top-level commands

| Command                     | Purpose                                                                                       |
| --------------------------- | --------------------------------------------------------------------------------------------- |
| `orcest orchestrate`        | Start the orchestrator loop.                                                                  |
| `orcest work`               | Start a worker loop. Requires `--id <worker_id>`; supports `--runner`, `--once`.              |
| `orcest status`             | Show system status: workers, queue depth, active tasks. Launches a live TUI by default; `--once` for single-shot output. |
| `orcest dead-letters`       | List and optionally replay dead-lettered tasks (`--replay`, `--count`).                       |
| `orcest init`               | Initialize orcest on a Proxmox host (writes `/etc/orcest/config.yaml`, copies Terraform templates, runs `tofu init`). |
| `orcest upgrade`            | Update the orcest CLI to the latest version from GitHub and refresh Terraform templates.      |
| `orcest init-labels`        | Create orcest labels (`orcest:ready`, `orcest:needs-human`) on every configured project repo. |
| `orcest provision <host>`   | Provision a worker VM via SSH: copy setup script, config, systemd service; start the worker. |
| `orcest pool-manage`        | Run the warm pool manager (long-running service that reconciles ephemeral worker VMs).        |
| `orcest trace`              | Inspect an archived worker trace. Supports `<task-id>`, `--pr owner/repo#N`, `--list <project>`, `--meta`, `--raw`. |
| `orcest check github-token` | Validate a GitHub token read from stdin against the GitHub API.                               |
| `orcest fleet ...`          | Proxmox fleet management subcommands (see [Fleet commands](#fleet-commands) below).           |

## Configuration

Orcest is configured by two YAML files plus a small set of environment
variables for secrets. The dataclass schemas live in
`src/orcest/shared/config.py`; commented examples ship at
`config/orchestrator.example.yaml` and `config/worker.example.yaml`.

### `orchestrator.yaml`

Loaded by `orcest orchestrate` (default path `config/orchestrator.yaml`).
Top-level keys:

- **`redis`** — `host`, `port`, optional `db`, `password`,
  `socket_timeout`, `socket_connect_timeout`, `key_prefix` (defaults to
  `orcest`).
- **`github`** — `token` (PAT; falls back to `GITHUB_TOKEN` env var) and
  `repo` (`owner/repo`). Used as the default project when no `projects:`
  list is given.
- **`polling`** — `interval` in seconds between GitHub polling cycles
  (default `60`).
- **`labels`** — names for the two orcest labels: `ready` and
  `needs_human`.
- **`runner`** — `type` (default `claude`), `timeout`, `max_retries`,
  `retry_backoff`, optional `model`. Used to compute pending-task marker
  TTLs and should match what workers are deployed with.
- **`default_runner`** — runner name applied to new tasks (default
  `claude`).
- **`providers`** — list of `ProviderEntry` maps (`provider`,
  `credential`, `model`, optional `cli_binary`, `env_var`, `extras`).
  Top-level providers are inherited by every project. See *Providers*
  below.
- **`projects`** — optional list of
  `{repo, token, claude_tokens, key_prefix, providers}` for
  multi-project orchestrators. In multi-project mode every project must
  declare a unique `key_prefix`.
- **`deployment`** — optional post-merge deploy hook: `enabled`,
  `command`, `health_check_url`, `health_check_timeout`,
  `rollback_command`.
- **Tuning knobs** — `max_attempts` (per-SHA, default `3`),
  `max_total_attempts` (default `50`), `max_transient_failures`
  (default `5`), `stale_pending_timeout_seconds` (default `7200`),
  `delete_branch_on_merge` (default `true`), `task_key_prefix` (defaults
  to `redis.key_prefix`).
- **`trace_archive_path`** — absolute path on the orchestrator process
  where verbatim per-task traces are archived. Omit to disable the
  archiver.
- **`workflow_state_root`** — optional workflow-control v1 state directory.
  When configured, the legacy PR selector reads `workflow.db` in query-only
  mode and excludes PRs owned by a live v1 Publication. Docker deployments
  must also set `ORCEST_WORKFLOW_STATE_HOST_PATH` to the host directory that
  contains `workflow.db`; fleet-managed deployments set
  `workflow_state_host_path` in fleet config and generate both settings.

### Providers

Each entry in `providers:` is a `ProviderEntry`:

```yaml
providers:
  - provider: claude
    credential: ""
    model: claude-3-5-sonnet-20241022
  - provider: grok
    model: grok-3-latest
    extras:
      temperature: "0.2"
```

If `credential` is empty in YAML, orcest resolves it from environment
variables in this order (see `_PROVIDER_ENV_CANDIDATES` in
`shared/config.py`):

| Provider | Env vars (first non-empty wins)                                  |
| -------- | ---------------------------------------------------------------- |
| `claude` | `CLAUDE_CODE_OAUTH_TOKEN`                                        |
| `clauder` | `CLAUDER_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`                     |
| `grok`   | `XAI_API_KEY`, `GROK_API_KEY`, `XAI_API_TOKEN`                   |
| `codex`  | `CODEX_API_KEY`, `OPENAI_API_KEY`                                |
| *any*    | falls back to `<PROVIDER>_TOKEN`, `<PROVIDER>_API_KEY`, `<PROVIDER>_KEY` |

`ANTHROPIC_API_KEY` is **not** a supported Claude runtime credential.
Workers inject the task credential as `CLAUDE_CODE_OAUTH_TOKEN`, and the
task schema does not carry authentication kind, so an API key cannot be
relabeled as an OAuth token. A Claude-enabled path that only has
`ANTHROPIC_API_KEY` fails validation with a migration instruction. An
unrelated `ANTHROPIC_API_KEY` in the environment does not fail
non-Claude providers.

The orchestrator only registers and round-robins providers; **workers
own execution**. To add a new provider end-to-end (worker registry +
setup script + rebake) see
[`docs/adding-a-provider.md`](docs/adding-a-provider.md).

### `worker.yaml`

Loaded by `orcest work --id <id>` (default path `config/worker.yaml`).
Top-level keys (from `WorkerConfig`):

- **`redis`** — same shape as the orchestrator's `redis` block.
- **`worker_id`** — overridden by `--id` on the CLI.
- **`workspace_dir`** — defaults to `/tmp/orcest-workspaces`.
- **`backend`** — runner type (default `claude`); also overridable via
  `--runner`.
- **`runner`** — `type`, `timeout` (default 5400s), `max_retries`,
  `retry_backoff`, optional `model`, `extra`.
- **`ephemeral`** — process one task and exit. Equivalent to `--once`.
- **`providers`** — optional per-worker provider declarations.

### Required secrets / environment variables

| Variable                                                 | Required when                                            |
| -------------------------------------------------------- | -------------------------------------------------------- |
| `GITHUB_TOKEN`                                           | `github.token` is empty in YAML.                         |
| `CLAUDE_CODE_OAUTH_TOKEN`                                | A `claude` provider entry has no inline `credential`.    |
| `CLAUDER_API_KEY` (checked before `CLAUDE_CODE_OAUTH_TOKEN`) | A `clauder` provider entry has no inline `credential`. |
| `XAI_API_KEY` / `GROK_API_KEY` / `XAI_API_TOKEN`         | A `grok` provider entry has no inline `credential`.     |
| `CODEX_API_KEY` (or `OPENAI_API_KEY`)                    | A `codex` provider entry has no inline `credential`.    |
| `ORCEST_REDIS_*`                                         | Used by `orcest pool-manage` inside the Compose stack.   |
| `ORCEST_TRACE_ARCHIVE_ROOT`                              | Override the trace archive root used by `orcest trace`.  |

Claude runtime credentials are OAuth tokens. The Actions-review secret
and the fleet runtime tokens are **two separate planes** — see
[Credential planes](#credential-planes).

## Quickstart (local single-project test)

This walks through running an orchestrator on your laptop against one
GitHub repo. It is **not** a production deploy — for that, see
[Deployment model](#deployment-model) below and
[`docs/rollout-multi-provider.md`](docs/rollout-multi-provider.md).

1. **Clone and install.**
   ```bash
   git clone https://github.com/ThayneStudio/orcest.git
   cd orcest
   python3.12 -m venv .venv
   . .venv/bin/activate
   python -m pip install -r requirements-dev.lock
   python -m pip install --no-deps --no-build-isolation -e .
   ```
2. **Start Redis.**
   ```bash
   docker compose -f docker-compose.redis.yml up -d redis
   ```
   `make redis-up` does the same thing and additionally waits for `PING`.
3. **Export secrets.**
   ```bash
   export GITHUB_TOKEN=ghp_...
   export CLAUDE_CODE_OAUTH_TOKEN=sk-ant-...
   ```
4. **Write `config/orchestrator.yaml`** by copying
   `config/orchestrator.example.yaml` and setting `github.repo`. Leave
   `github.token` empty to use `GITHUB_TOKEN`; keep the `providers:`
   block's `claude` entry with `credential: ""` so it picks up
   `CLAUDE_CODE_OAUTH_TOKEN`.
5. **Create labels:** `orcest init-labels`.
6. **Run:** `orcest orchestrate` in one terminal, `orcest status` in
   another.
7. **(Optional) Local worker:** copy `config/worker.example.yaml`, then
   `orcest work --id local-worker-1 --once`. The worker needs the
   provider CLI (e.g. `claude`) on `$PATH`; in production this is baked
   into worker VM images by `provision/setup-worker.sh`.

---

## Deployment model

Orcest runs as a small fleet on a single Proxmox host. There are two
classes of compute:

- **Orchestrator VM** — one long-lived VM (default user `orcest`,
  configurable VM ID) that hosts everything stateful and
  provider-agnostic:
  - **Shared Redis** stack (`docker-compose.redis.yml`) — the task
    queue, distributed locks, exhaustion bookkeeping, and the pool's
    `orcest:pool:current_template_vmid` pointer.
  - **One orchestrator container per project** (deployed as Compose
    project `orcest-<project>` from
    `src/orcest/fleet/deploy/docker-compose.yml`). Each instance polls
    one GitHub repo, runs heuristics, and enqueues tasks onto the
    shared Redis streams.
  - **Pool manager** (`src/orcest/fleet/deploy/docker-compose.pool.yml`)
    — a single host-network container that talks to the Proxmox API and
    reconciles the warm worker pool against `pool.size` in fleet
    config.
  - **Dashboard** container (`docker-compose.dashboard.yml`). It joins the
    shared `orcest` network and is deployed with `/opt/orcest/.redis.env`
    plus a dashboard token from `DASHBOARD_TOKEN` or `/opt/orcest/.dashboard.env`.
    `DASHBOARD_REDIS_PREFIXES` can limit the prefixes visible to the
    dashboard when the Redis DB is shared. Only an absent/unset variable keeps
    all prefixed and unprefixed Orcest keys visible. If the variable is present,
    it must contain at least one prefix (or `unprefixed`); empty, whitespace-only,
    and separator-only values fail startup instead of silently widening access.
- **Worker VMs** — ephemeral Proxmox linked-clones of a baked template.
  Each worker takes a single task from Redis, runs it, and is destroyed
  by the pool manager on completion (or after `pool.max_task_duration`).
  Workers do **not** run Redis; they connect to the Redis instance on
  the orchestrator VM.

The single source of truth for the whole fleet is a YAML file on the
Proxmox host at `/etc/orcest/config.yaml` (see
`src/orcest/fleet/config.py`). `orcest fleet` subcommands are invoked
from the Proxmox host and reach the orchestrator over SSH.

## Credential planes

Claude Code OAuth tokens (`CLAUDE_CODE_OAUTH_TOKEN`) are the supported
Claude runtime credential. There are **two separate credential planes**.
They may contain different tokens and rotation schedules, and they are
**not** synchronized.

### Actions review plane

The repository GitHub Actions secret named `CLAUDE_CODE_OAUTH_TOKEN`
exists for review automation only. It is consumed by
`.github/workflows/claude-review.yml` and the manually dispatched
`.github/workflows/claude-review-usage.yml`. Keep that secret for those
workflows.

That Actions secret does **not** satisfy or feed fleet runtime
deployment. Production/test deploys are not performed by GitHub
Actions, and the secret must not be copied into PVE.

### Fleet runtime plane

Operators stage a verified source bundle and run fleet commands over
SSH through `root@pve-test.lab.prefixa.net`. Runtime provider
credentials come from the root-owned, mode-`0600`
`/etc/orcest/config.yaml` (`claude_oauth_tokens` or
`provider_credentials.claude`). `orcest fleet update` generates
protected per-project `.env` files containing
`CLAUDE_CODE_OAUTH_TOKEN` on the orchestrator VM.

The protected PVE file is the deployment source of truth for runtime
credentials. GitHub Actions does not deploy those values.

## Fleet commands

All subcommands live under `orcest fleet` (`src/orcest/fleet/cli.py`):

| Command                                                          | Purpose                                                                                                    |
| ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `add-org <org> --github-token … --claude-token …`                | Register a GitHub org and its provider credentials in fleet config.                                        |
| `create-orchestrator`                                            | Provision the orchestrator VM via Terraform, install Docker, build the image, and bring up Redis + pool manager. |
| `create-template`                                                | Bake the initial worker template VM (downloads a cloud image, runs `setup-worker.sh`, converts to template). |
| `onboard <owner/repo>`                                           | Register a project and deploy its per-project orchestrator Compose stack.                                  |
| `destroy <project>`                                              | Tear down the project's Compose stack and remove it from fleet config.                                     |
| `start`                                                          | Start (or restart) the pool manager.                                                                       |
| `stop [--drain-active]`                                          | Stop the pool manager and destroy idle (or all) worker VMs; clean Redis pool state.                        |
| `update`                                                         | Upload fresh source, rebuild the orchestrator Docker image, restart Redis + pool manager + every project stack. |
| `deploy [--rebuild-template] [--drain-active]`                   | Coordinated fleet deploy: `stop` → `update` → optional pointer-safe `rebake` → `start`; it does not update the host CLI. |
| `pool-status`                                                    | Show pool config, the active template VMID (Redis pointer), and idle/active VM counts.                     |
| `status`                                                         | Show orchestrator host reachability, orgs, per-project stack status, and pool summary.                     |
| `rebake [--image-url …]`                                         | Bake a fresh template at the next free VMID from `pool.template_vmid_range` and atomically swap the Redis pointer. |
| `destroy-template <vm-id>`                                       | Destroy a non-active template VM. Refuses if it is the active pointer target or has live clones.           |
| `gc-templates [--dry-run] [--yes]`                               | Destroy orcest worker templates in the template VMID range that are no longer active and have no live clones. Only VMs that are actually templates named `orcest-worker-*` are eligible; anything else sharing the range is listed and skipped. Prompts before destroying unless `--yes`. |
| `set-pool-size <N> [--vm-id-start …]`                            | Set the target warm pool size.                                                                             |

## Onboarding a project

1. **Prep the repo on GitHub.** Add the `orcest:ready` label (the
   orchestrator uses it as the pickup signal).
2. **Register the org credentials** (one-time per GitHub org):
   ```bash
   orcest fleet add-org ThayneStudio \
     --github-token "$GITHUB_PAT" \
     --claude-token "$CLAUDE_OAUTH_TOKEN"
   ```
   Pass `--claude-token` multiple times for a round-robin pool. For
   other providers see
   [`docs/adding-a-provider.md`](docs/adding-a-provider.md).
3. **Onboard the repo:**
   ```bash
   orcest fleet onboard ThayneStudio/my-project
   ```
   This appends the project to `/etc/orcest/config.yaml`, generates the
   per-project `.env` and `orchestrator.yaml` on the orchestrator VM,
   ensures Redis is running, builds the orchestrator image if missing,
   and brings up the `orcest-<project>` Compose stack.
4. **Watch progress.** `orcest fleet status` shows stack health, the
   dashboard container shows live task state, and any open issue
   labeled `orcest:ready` should be picked up within one polling cycle.

## Deploying code changes

Orcest has **four deploy layers**, and which target you hit depends on
which directory you changed:

| Layer                   | What it ships                                                                                       | When to use it                                                                                          |
| ----------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Host CLI                | `pip install` of orcest on the Proxmox host                                                         | Changes to `src/orcest/cli.py` or `src/orcest/fleet/*` — anything you run as `orcest …` from the Proxmox host. |
| Orchestrator containers | `orcest fleet update` (uploads source, rebuilds the `orcest` Docker image, regenerates project `.env` / `orchestrator.yaml`, restarts Redis + project stacks, then refreshes the pool manager) | Changes to `src/orcest/orchestrator/*`, `src/orcest/shared/*`, generated project config, or worker stream routing such as `default_runner`. |
| Worker template         | `orcest fleet rebake` (builds a new template VM, atomically swaps the Redis pointer)                | Changes to `src/orcest/worker/*`, additions to `PROVIDER_REGISTRY`, or any `provision/setup-worker.sh` change. |
| Dashboard container     | `make deploy-dashboard` or `make deploy-dashboard-remote` (syncs when remote, builds/restarts `docker-compose.dashboard.yml`, and waits for `/api/ready`) | Changes to `dashboard/*` or `docker-compose.dashboard.yml`. Run locally with `deploy-dashboard-remote`, or from the host that owns the dashboard container with `deploy-dashboard`. |

Notes:

- `orcest fleet deploy` coordinates layer 2 and (with
  `--rebuild-template`) layer 3, including stopping and restarting the worker
  pool. Project orchestrators are stopped before the workerless interval and
  resume only after the exact VMID/backend/revision worker layout attests. This
  is a no-lost-work maintenance cutover, not a zero-worker-downtime rollout. It
  does **not** install a new host CLI; update layer 1 separately before invoking
  it when `src/orcest/fleet/*` changed.
- During `deploy --rebuild-template`, the pool manager stays stopped
  until the new template pointer has been swapped, so fresh clones use
  the rebaked template.
- `orcest fleet rebake` only swaps the active template pointer. Existing
  idle/active worker VMs are not replaced until they are drained,
  stopped, or the pool needs more capacity. For an immediate worker
  cutoff, use `orcest fleet deploy --rebuild-template --drain-active`
  or explicitly `stop --drain-active`, `rebake`, then `start`.
- The active template pointer is durable. If it is missing or dangling, the
  pool manager recovers it automatically only when exactly one live template
  exists in `pool.template_vmid_range`. With multiple candidates, restore
  `orcest:pool:current_template_vmid` explicitly or run a coordinated rebake;
  VMID order does not identify the newest generation.
- Done and drain markers are durable lifecycle handoffs. Worker destruction and
  VMID reuse clear and verify them; a failed verification is reported as an
  incomplete stop/allocation instead of risking the next VM generation.
  `stop --drain-active` also requires Proxmox credentials up front and changes
  no fleet state when that preflight fails.
- Dashboard deploys keep the last readiness-verified Compose configuration
  in `.dashboard-compose.last-known-good.yml` beside the Compose file (outside
  the rsync-deleted `dashboard/` tree). If a candidate image or configuration
  fails, rollback recreates the previous image with that known-good
  configuration. Override the local or remote state path with
  `DASHBOARD_COMPOSE_STATE_FILE` or `DASHBOARD_REMOTE_COMPOSE_STATE_FILE`.
  The remote state file must remain outside `DASHBOARD_REMOTE_DIR`, whose
  contents are replaced with `rsync --delete` during synchronization.
- To isolate interactive Claude Code from the legacy `claude -p` worker,
  configure the pool clones with `pool.worker_backend: clauder`,
  `pool.worker_runner_type: claude`, and
  `pool.worker_runner_mode: interactive`, and publish tasks with
  `default_runner: clauder` or an explicit `provider: clauder`.
  The `claude` backend also defaults to the interactive runner when
  `pool.worker_runner_mode` is unset; set `pool.worker_runner_mode: headless`
  to keep running the legacy `claude -p` prompt-mode worker.
- A fleet can schedule dedicated mixed-provider workers with an ordered
  round-robin layout. Consecutive VMID slots use consecutive profiles and the
  list wraps; repetition adds worker capacity for that backend:
  ```yaml
  pool:
    size: 4
    vm_id_start: 10000
    worker_profiles:
      - backend: clauder
      - backend: codex
      - backend: grok
  ```
  This produces `clauder`, `codex`, `grok`, `clauder` for VMIDs
  `10000`–`10003`. Each worker still consumes only its own PR and issue streams.
  Changing profile order/settings requires
  `fleet deploy --rebuild-template --drain-active`; other fleet commands reject
  a mixed-layout transition. Changing the worker VMID range in place is not
  supported: drain with the deployed configuration first, retain its checksummed
  backup, retire the deployed config only after every old worker is absent, then
  perform a first deployment with the new range. Repetition controls capacity only—provider task
  selection remains round-robin across configured credential entries.
- For the pve-test dashboard path, deploy from this repo with:
  ```bash
  make deploy-dashboard-remote \
    DASHBOARD_REMOTE=orcest@10.20.1.129 \
    DASHBOARD_REMOTE_RSYNC_SHELL='ssh -o BatchMode=yes root@10.20.1.18 ssh -T -o BatchMode=yes' \
    DASHBOARD_REMOTE_EXEC='ssh -o BatchMode=yes root@10.20.1.18 ssh -T -o BatchMode=yes orcest@10.20.1.129'
  ```
- A `pip install` on the Proxmox host updates the host CLI but **does
  not** touch the orchestrator container or worker template. Skipping
  `fleet update` is the most common cause of "I deployed my fix and it
  is not running."
- `fleet update` regenerates per-project `.env` and
  `orchestrator.yaml` from `/etc/orcest/config.yaml` before restarting
  project stacks. It refuses to rewrite project files if the shared
  Redis password cannot be read, because dropping
  `ORCEST_REDIS_PASSWORD` would break the restarted orchestrators.
- See [`docs/rollout-multi-provider.md`](docs/rollout-multi-provider.md)
  for the per-provider rollout recipe.
- For major releases, follow the checked-in
  [`deployment, rollback, and health-watch runbook`](docs/operations/reliability-milestone-rollout.md).

## Observability

- **`orcest status`** — overall system health from any host with Redis
  access (queue depth, locks, exhaustion windows).
- **`orcest fleet status`** — orchestrator SSH reachability, registered
  orgs, project stack status, and pool summary.
- **`orcest fleet pool-status`** — target size, active template VMID
  (from the Redis pointer, with the config value as fallback), and
  idle/active worker counts from Proxmox.
- **`orcest rollout-health`** — read-only, machine-readable deployment gates for
  exact revision, Redis, queue/pending state, dead-letter/provider-counter
  growth, pool size, private credential-recovery residue, and live
  backend/revision/stream-correlated worker heartbeats. With backend checks,
  also pass `--expected-pool-size` and `--expected-vmid-start`, then one
  `--expected-backend` per consecutive VMID slot, including duplicates.
  `--prefix` is **required**: the project-scoped gates read `{prefix}:results`
  and friends, so a wrong or missing prefix would inspect an empty keyspace and
  report healthy. Pass `--pool-prefix` too when the pool manager runs under a
  key prefix different from `--task-prefix`.
- **Dashboard container** on the orchestrator VM — the live task view.
- **Container logs on the orchestrator VM:**
  ```bash
  ssh orcest@<orchestrator-host>
  docker compose -p orcest-<project> logs -f orchestrator
  docker compose -p orcest-redis logs -f redis
  docker compose -p orcest-pool logs -f pool-manager
  ```
- **Verbatim per-task trace archive.** When `trace_archive_host_path`
  is set in fleet config, the orchestrator container mounts that path
  at `/var/lib/orcest/traces` and writes one `.jsonl` + `.meta.json`
  per task. On the production fleet this is mounted at
  `/mnt/truenas-logs/orcest-traces`. Inspect from anywhere with Redis
  access:
  ```bash
  orcest trace <task-id>           # raw stream-json
  orcest trace <task-id> --meta    # metadata only
  orcest trace --pr <num>          # all traces for a PR
  orcest trace --list              # recent traces
  ```
- **Workflow-Control v1 ownership fence.** Set the fleet config's
  `workflow_state_host_path` to the absolute host directory containing the v1
  `workflow.db`. Fleet generation mounts it read-only at
  `/var/lib/orcest/workflow` and emits the matching `workflow_state_root` for
  each legacy orchestrator.
- **Issue-dependency deferrals** log at INFO from the orchestrator as
  `Issue #<n>: deferred, waiting on open blocker(s): #<a>, #<b>`.
- **State machine semantics** for what the orchestrator is doing per
  task are in
  [`docs/orchestrator-state-machine.md`](docs/orchestrator-state-machine.md).

## Operational pitfalls

These are failure modes that have actually bitten the production
fleet. None require deep debugging once you know the shape.

### Workers die permanently on Redis restart

When `orcest fleet update` restarts the Redis container, every worker's
systemd unit can hit its restart-rate-limit and end up in
`failed (start-limit-hit)`. Symptom: workers appear in `pool-status` as
existing VMs but never claim tasks, and the orchestrator log fills with
"task queue has N pending entries, deferring." Recovery, per VM:

```bash
ssh orcest@<worker-ip> "sudo systemctl reset-failed orcest-worker && \
                        sudo systemctl start orcest-worker"
```

Prefer `orcest fleet deploy` over `orcest fleet update` when you also
need to bounce Redis — `deploy` drains the pool first.

### Pending consumer entries from destroyed workers stall the queue

Redis streams remember consumer-group PEL entries for workers that have
been destroyed. The orchestrator's discovery loop sees them as
in-flight and logs `task queue has N pending entries, deferring` while
workers sit idle. Clean up per stalled stream:

```bash
docker exec orcest-redis-redis-1 redis-cli \
  XGROUP DELCONSUMER orcest:tasks:<stream> workers <dead-worker-id>
```

You lose those PEL entries; the orchestrator's re-poll loop re-enqueues
the corresponding work from GitHub.

### `pool.max_task_duration` vs `RunnerConfig.timeout`

These are two independent timers. The pool manager reaps any worker VM
whose task has been running longer than `pool.max_task_duration`
**regardless of the runner's own timeout**. If you raise the per-task
runner timeout for long jobs and forget to raise the pool reaper, the
VM dies mid-run and the work shows up as an opaque failure. Always
raise both together.

### `_is_usage_exhausted` only checks stderr

The Claude provider's exhaustion detector deliberately ignores stdout —
stream-json metadata produces false positives. If you add a new
provider that signals exhaustion on stdout, don't paper over it by
extending the stderr-only check; mirror the boundary by giving the
provider its own runner (see `src/orcest/worker/grok_runner.py`).

### Pool manager refuses to start with a localhost Proxmox endpoint

The pool manager runs **inside** the orchestrator VM and reaches the
Proxmox API over the network. A `proxmox.endpoint` of `127.0.0.1` works
from the Proxmox host CLI but is unreachable from the orchestrator VM.
`fleet start` / `fleet create-orchestrator` / `fleet update` all detect
this and print a `Fix with: orcest init` hint — set the Proxmox host's
real IP (or DNS name) in fleet config and retry.

---

## Further reading

- [`docs/orchestrator-state-machine.md`](docs/orchestrator-state-machine.md) — per-task state transitions.
- [`docs/adding-a-provider.md`](docs/adding-a-provider.md) — end-to-end recipe for a new agent.
- [`docs/rollout-multi-provider.md`](docs/rollout-multi-provider.md) — provider rollout playbook.
- [`docs/issue-dependencies.md`](docs/issue-dependencies.md) — body-text dependency syntax.
- [`docs/ci-forensics.md`](docs/ci-forensics.md) — CI and `gh` investigation gotchas.
- [`docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md`](docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md) — events/monitor design.
- [`docs/monitor-exposure-runbook.md`](docs/monitor-exposure-runbook.md) — standing up and externally exposing the monitor service.
- [`docs/plans/`](docs/plans/) — design notes and roadmap.
