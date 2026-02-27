# Orcest Design Document

Orcest is a closed-loop autonomous CI/CD system that runs 24/7 across multiple VMs. It monitors GitHub repositories, triages CI failures, fixes PRs, implements issues, and identifies codebase improvements -- all without human intervention unless escalation is required.

GitHub itself is the dashboard. All visibility into what Orcest is doing comes from labels and comments on PRs and issues.

## Terminology

- **Orcest** -- the overall system/portfolio
- **Orchestrator** -- the brain; a single instance that polls GitHub, runs heuristics, and enqueues tasks
- **Worker** -- the hands; N instances that consume tasks and run Claude Code against repositories

## Architecture

```
                      GitHub (PRs, Issues, CI)
                              |
                              | polls via `gh` CLI
                              v
┌─────────────────────────────────────────────────────┐
│                    ORCHESTRATOR                      │
│              (single instance, no Claude)            │
│                                                      │
│  Polls GitHub → Runs heuristics → Enqueues tasks     │
│  Auto-merges clean PRs, auto-retries transient CI    │
│  Monitors task results, posts GitHub comments/labels │
└─────────────────┬───────────────────────────────────┘
                  │ Redis Stream (task queue)
    ┌─────────────┼─────────────┐
    │             │             │
┌───▼───┐   ┌───▼───┐   ┌───▼───┐
│Worker 0│   │Worker 1│   │Worker 2│
│  (VM)  │   │  (VM)  │   │  (VM)  │
│ XREAD  │   │ XREAD  │   │ XREAD  │
│ Claude │   │ Claude │   │ Claude │
└────────┘   └────────┘   └────────┘
```

### Orchestrator (single instance, project-specific)

- Polls GitHub for PR/issue state changes
- Runs all heuristics (CI triage, review parsing, merge eligibility)
- **Handles all non-AI actions itself** -- auto-merge, CI re-run, rebase, artifact cleanup
- Only enqueues tasks to Redis when **AI reasoning is needed**
- Stores all operational memory in Redis (CI retry counts, failure patterns, cooldowns)
- Manages GitHub labels and posts status comments on PRs/issues
- Stateless on disk -- only needs config file + Redis

### Workers (N instances, repo-agnostic)

- Block on `XREADGROUP` waiting for tasks (idle, zero CPU when no work)
- Receive task containing: repo URL, credentials/token, branch, prompt
- **Clone the repo**, configure `gh` auth from task credentials
- The repo's `.claude/` directory provides all context (skills, hooks, commands, CLAUDE.md)
- **Zero project-specific knowledge** -- fully generic, like GitHub Actions runners
- Any project can be served by the same worker pool

### Redis (dedicated instance)

- Stream `tasks` -- task queue (orchestrator publishes, workers consume via consumer group)
- Stream `results` -- workers publish completion/failure results
- Keys `lock:*` -- distributed locks via `SET NX EX` (atomic test-and-set)
- Keys `memory:*` -- orchestrator operational memory

## GitHub Visibility

GitHub is the dashboard. Orcest communicates all status through labels and comments.

### Labels

| Label | Meaning |
|-------|---------|
| `orcest:queued` | Task enqueued, waiting for a worker to pick it up |
| `orcest:in-progress` | A worker is actively working on this PR/issue |
| `orcest:blocked` | Work is blocked (dependency, merge conflict, etc.) |
| `orcest:needs-human` | Orcest cannot resolve this; human attention required |

### Comments

The orchestrator posts comments on PRs and issues at key transitions:

- **Work starts** -- "Orcest picked this up. Worker N is working on it."
- **Work completes** -- "Orcest finished. Summary of changes: ..."
- **Escalation** -- "Orcest could not resolve this. Reason: ... Labeling `orcest:needs-human`."
- **CI triage** -- "CI failure classified as transient. Re-running failed jobs."
- **Merge** -- "All checks green, reviews clean. Rebasing and merging."

Labels are **visual indicators only** -- they are set after the Redis lock is acquired, not used for coordination.

## Priority System

### Priority 1: PR Management

For each open PR (oldest first):

1. CI failing + transient pattern match -- auto-retry (no Claude needed)
2. All CI green + review clean -- rebase and merge (no Claude needed)
3. Actionable review feedback -- enqueue FIX_PR task
4. CI code failure -- enqueue FIX_CI task
5. Unclassifiable failure -- enqueue CLASSIFY_CI task (lightweight Claude)

### Priority 2: Issue Processing

- Pick up issues labeled `ai-ready`
- Dependency resolution + topological sort
- Claude implements + self-reviews (/review cycles) before pushing

### Priority 3: Codebase Improvement

- When idle, spawn parallel Claude subagents for analysis
- Categories: security, tech debt, test coverage, performance, accessibility
- Creates GitHub issues (without `ai-ready` label) -- human triage required

## CI Triage Escalation Chain

```
CI fails --> heuristic pattern match
  --> TRANSIENT?   --> auto-retry via gh run rerun (no Claude)
  --> CODE?        --> enqueue Claude fix task
  --> DEPENDENCY?  --> create/find blocking fix PR, skip blocked PRs
  --> no match?    --> enqueue CLASSIFY_CI task (lightweight Claude)
                      --> Claude returns: TRANSIENT, CODE, DEPENDENCY, or HUMAN_NEEDED
                      --> if HUMAN_NEEDED --> label orcest:needs-human, post summary, move on
```

## Task Model

```python
@dataclass
class Task:
    id: str                    # UUID
    type: TaskType             # fix_pr, fix_ci, classify_ci, implement_issue, improve_codebase
    repo: str                  # Repo URL (e.g. "https://github.com/org/repo.git")
    token: str                 # GitHub token for clone + gh CLI auth
    resource_type: str         # "pr" or "issue"
    resource_id: int           # PR/issue number
    prompt: str                # Full prompt text (rendered from template + context)
    branch: str | None         # Existing branch to work on (for PR fixes)
    created_at: datetime

@dataclass
class TaskResult:
    task_id: str
    worker_id: int
    status: ResultStatus       # completed, failed, blocked, usage_exhausted
    branch: str | None         # Branch with work (for resume)
    summary: str
    duration_seconds: int
```

## Error Handling

| Scenario | Handler |
|----------|---------|
| CI transient failure | Orchestrator auto-retries via `gh run rerun --failed` (no task) |
| CI dependency failure | Orchestrator enqueues DEPENDENCY_FIX task, skips blocked PRs |
| CI unclassifiable | Orchestrator enqueues CLASSIFY_CI task (lightweight) |
| CI code failure | Orchestrator enqueues FIX_CI task |
| Claude crash | Worker retries 3x with 10s backoff, then reports failure |
| Claude /usage exhausted | Worker reports `usage_exhausted` with branch name; next pickup resumes |
| Worker crash mid-task | Redis lock TTL expires (30min); XPENDING shows unclaimed message |
| Redis down | Both orchestrator and workers pause with backoff until Redis returns |
| Lock contention | Redis `SET NX` is atomic -- one wins, others skip |
| Merge conflict | Orchestrator posts comment, labels `orcest:blocked`, releases lock |
| GitHub rate limit | Orchestrator backs off exponentially |

## Redis Key Design

| Key pattern | Type | Purpose |
|-------------|------|---------|
| `tasks` | Stream | Task queue; orchestrator publishes, workers consume via consumer group |
| `results` | Stream | Workers publish completion/failure results |
| `lock:{repo}:{resource_type}:{resource_id}` | String (SET NX EX) | Distributed lock; TTL auto-expires stale locks |
| `memory:ci_retries:{repo}:{run_id}` | String | Retry count for a CI run; TTL auto-expires |
| `memory:cooldown:{repo}:{resource_id}` | String | Cooldown timer to avoid hammering a PR/issue |
| `memory:failure_pattern:{hash}` | String | Cached CI failure classification |

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Coordination | Redis `SET NX EX` | True atomic test-and-set, TTL auto-expires stale locks |
| Task distribution | Redis Streams + consumer groups | Exactly-once delivery, blocking reads, built-in redelivery |
| Dashboard | GitHub labels + comments | No separate UI to build/maintain; PRs/issues are the source of truth |
| System health | `orcest status` CLI subcommand | Quick check of orchestrator, workers, Redis, and queue depth |
| Worker architecture | Repo-agnostic | Clone repo from task payload, `.claude/` provides context |
| Merge rules | All CI green + review clean + rebased on master | No branch protection (free tier), must rebase for migration guard |
| CI triage | Heuristic -> Claude classify -> Claude fix -> human | Saves API costs for transient failures |
| Improvement output | GitHub issues requiring human review | No auto-label, human triage gate |
| Config | YAML files | Orchestrator config is project-specific, worker config is generic |
| Package | Single `orcest` Python package | One install, multiple subcommands; simplifies deployment and versioning |

## Deployment

### Orchestrator + Redis: Docker Compose

The orchestrator and Redis run together on `thayne-claude-dev-01.home.prefixa.net` via Docker Compose.

```yaml
# docker-compose.yml deploys:
#   - orcest-orchestrator (Python, polls GitHub, enqueues tasks)
#   - redis (task queue, locks, operational memory)
```

The orchestrator container runs `orcest orchestrate` and connects to the co-located Redis instance. Configuration is mounted from `config/` on the host.

### Workers: Systemd on Bare VMs

Each worker VM runs `orcest work` as a systemd service. Workers connect to Redis on the orchestrator host over the local network.

Bare-metal (no Docker) because workers need:
- Direct access to `claude` CLI (authenticated per-machine)
- Direct access to `gh` CLI
- Disk space for repo clones
- Native process management for long-running Claude sessions

### Provisioning

```bash
orcest provision <host>
```

The `provision` subcommand handles SSH-based setup of worker VMs:
- Installs system dependencies (Python 3.12+, git, gh, claude CLI)
- Deploys the `orcest` package
- Creates and enables the systemd unit for `orcest work`
- Configures Redis connection to the orchestrator host

Provisioning scripts and templates live in the `provision/` directory.

## Package Structure

Orcest is a single Python package with four subcommands:

```
orcest orchestrate   # Run the orchestrator loop
orcest work          # Run a worker (XREADGROUP consumer)
orcest status        # Show system health (orchestrator, workers, Redis, queue)
orcest provision     # Set up a worker VM via SSH
```

### Repository Layout

```
orcest/
├── pyproject.toml              # Package definition, dependencies, entry points
├── docker-compose.yml          # Orchestrator + Redis deployment
├── Dockerfile                  # Orchestrator container image
├── src/orcest/
│   ├── __init__.py
│   ├── cli.py                  # Click CLI with subcommands
│   ├── orchestrator/
│   │   ├── __init__.py
│   │   ├── loop.py             # Main orchestrator polling loop
│   │   ├── gh.py               # GitHub CLI wrapper
│   │   ├── git.py              # Git CLI wrapper
│   │   ├── pr_ops.py           # PR discovery, CI checking, merge, review parsing
│   │   ├── issue_ops.py        # Issue discovery, dependency resolution
│   │   ├── ci_triage.py        # CI failure classification + self-healing
│   │   ├── improvement.py      # Codebase improvement task creation
│   │   ├── labels.py           # GitHub label management (orcest:* labels)
│   │   └── task_publisher.py   # Render prompts + enqueue tasks to Redis Stream
│   ├── worker/
│   │   ├── __init__.py
│   │   ├── loop.py             # Worker entry point (XREADGROUP loop)
│   │   ├── workspace.py        # Clone/pull repo, checkout branch, configure gh auth
│   │   ├── claude_runner.py    # Claude CLI subprocess management
│   │   └── heartbeat.py        # TTL refresh while Claude runs
│   └── shared/
│       ├── __init__.py
│       ├── config.py           # YAML config loader + typed Config dataclass
│       ├── coordination.py     # Redis locking (claim/release/heartbeat)
│       ├── redis_client.py     # Redis connection + stream helpers
│       ├── models.py           # Task, TaskResult, PR, Issue dataclasses
│       └── logger.py           # Structured logging
├── prompts/                    # Prompt templates
│   ├── implement-issue.md
│   ├── fix-pr.md
│   ├── fix-ci.md
│   ├── classify-ci-failure.md
│   └── improve-*.md
├── config/                     # Configuration files
│   ├── orchestrator.example.yaml
│   └── worker.example.yaml
├── provision/                  # Worker VM provisioning
│   ├── setup.sh
│   └── orcest-worker.service
├── docs/plans/                 # Phase plans (see Phasing below)
└── .claude/
    └── CLAUDE.md               # Project context for Claude Code
```

## Dependencies

- Python 3.12+
- Redis server (dedicated instance)
- `gh` CLI (authenticated)
- `git` CLI
- `claude` CLI (authenticated, workers only)
- Python packages: `redis`, `pyyaml`, `click`, `rich`

## The `.claude/` Convention

Workers are fully repo-agnostic. Project context comes from the target repo itself:

1. Worker clones the repo using credentials from the task payload
2. Checks out the specified branch
3. Runs `claude --prompt-file <task-prompt>` in the repo working directory
4. Claude Code automatically loads `.claude/` -- skills, hooks, commands, `CLAUDE.md`
5. The repo's `.claude/` directory IS the project configuration, like `.github/` for Actions

**Any project can be served by the same worker pool** -- just add a `.claude/` directory.

## Phasing

Development is broken into four phases. Detailed plans for each phase live in `docs/plans/`.

### Phase 1: Core Loop (`docs/plans/phase-1-core-loop.md`)

Orchestrator + 2 workers. PR fixes only.

- Orchestrator polls for open PRs, detects CI failures and review feedback
- Workers consume FIX_PR and FIX_CI tasks
- Redis coordination (locks, streams, results)
- Basic GitHub label and comment posting
- End-to-end: CI fails -> orchestrator triages -> worker fixes -> PR updated

### Phase 2: Full PR Management (`docs/plans/phase-2-pr-management.md`)

Complete the CI triage chain. Auto-merge. Review feedback loop.

- Full CI triage escalation chain (heuristic -> classify -> fix -> human)
- Auto-merge when all CI green + reviews clean + rebased
- Review feedback parsing and FIX_PR task creation
- Transient CI auto-retry (no Claude needed)

### Phase 3: Issue Processing (`docs/plans/phase-3-issue-processing.md`)

Pick up `ai-ready` issues. Dependency resolution.

- Issue discovery and `ai-ready` label scanning
- Dependency resolution and topological sort
- Implementation tasks with self-review cycles
- Branch naming and PR creation from issues

### Phase 4: Codebase Improvement (`docs/plans/phase-4-codebase-improvement.md`)

Idle-time analysis. Issue creation for human triage.

- Idle detection (no PRs or issues to work on)
- Parallel Claude subagent analysis (security, tech debt, tests, performance, accessibility)
- GitHub issue creation (without `ai-ready` label -- human triage gate)

## Existing Code to Port

From `bbr-platform`:

- `scripts/ralph.sh` -- issue processing logic (dependency resolution, topological sort, idempotency, branch naming)
- `.claude/commands/implement.md` -- autonomous implementation prompt structure
- `.claude/commands/review.md` -- parallel subagent review pattern
- `scripts/ci/cleanup-runner.sh` -- cleanup patterns for CI triage
