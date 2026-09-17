# Local dashboard harness

Develop the dashboard without a VPN, a live fleet, provider accounts, or GitHub.
This is the actual dashboard server, browser bundle, sign-in/session handling,
Redis projection and output WebSocket. A small simulator uses Orcest's Python
observation writers to supply synthetic fleet events. It does not start agents,
create VMs, or call external APIs.

## Start

Requires macOS or Linux, Python 3.12+, the dashboard's pinned Node 24 runtime,
and `redis-server` on PATH. Install the project's locked Python dependencies in
`.venv` as described in `.claude/CLAUDE.md`. Then, from the repository root:

```sh
cd dashboard
npm ci
npm run build
npm run harness
```

Open **http://localhost:4318/sign-in**, and use **`local-harness`** as the access
token. This intentionally public fixture credential is only for the owned,
loopback-only harness with fake data. The dashboard shows a simulated-fleet
banner after sign-in. Source links reference fictional `demo/*` projects.

The harness starts its own Redis on a random loopback port with a generated
password, and starts the real dashboard on loopback. It ignores inherited
production Redis and dashboard credential settings. There is no option to point
it at an external Redis instance. An occupied dashboard port or state directory
causes startup to fail, without replacing another service.

The default persistent state directory is
`~/.local/state/orcest/dashboard-harness`. It holds AOF data, process logs,
status and a private command mailbox. Use `--state-dir /your/durable/path` to
change it. Avoid `/tmp` for ongoing development or observation evidence. Use
`ORCEST_TEST_REDIS_SERVER=/absolute/path/to/redis-server` and
`ORCEST_TEST_PYTHON=/absolute/path/to/python` for isolated toolchains.

## What to explore

- Upcoming → In progress → Done, with queued/executing/waiting activity inside
  each stage. Waiting for a dependency stays Upcoming; CI stays In progress.
- Click a running card to inspect its attempt and incrementally streamed output.
- A completed publication releases its dependent task into the queue.
- Fleet shows three provider accounts independently from ephemeral worker
  identities; usage windows and a cooldown are synthetic observations. A simulated usage
  reset makes the cooled-down account eligible again and releases queued work.
- An exceptional access problem appears under Needs you.
- The project selector contains 22 projects, including projects without work.

By default a scenario advances every 25 seconds and resets after five stages.
Output arrives every two seconds. Each loop creates fresh simulated worker IDs.
Reset removes only the harness data in its owned Redis. It does not preserve
historical scenario runs. Redis-outage recovery within a run does preserve data.

## Controls

In a second terminal, from `dashboard/` with the same Python environment:

```sh
npm run harness -- --command pause
npm run harness -- --command step
npm run harness -- --command resume
npm run harness -- --command reset
npm run harness -- --command redis-down
npm run harness -- --command redis-up
npm run harness -- --command restart-dashboard
npm run harness -- --command stop
```

Repeat `--state-dir` when using a custom directory. Pause stops progression and
output, while keeping observations and worker heartbeats fresh. Step advances
one stage. Redis down/up exercises unavailable data and reconnection. Restarting
the dashboard invalidates existing sign-in sessions. Commands are local files,
not unauthenticated HTTP control endpoints. Ctrl-C or `stop` shuts down the
owned dashboard and Redis; it does not touch other processes. A new invocation
starts a fresh scenario and discards stale commands. This is not an auto-start
service; after a machine restart, run it again.

## Validate

```sh
npm run check:local-harness
```

This starts a separate temporary instance on unused ports and checks isolation,
sign-in, inventory, dependency gating, execution/output, CI wait, completion,
Redis outage/recovery, session invalidation, reset and logout. It cleans up its
owned processes and temporary data. Existing `check:fleet-e2e` and
`scripts/check-recovery.py` remain narrower independent checks.

Harness testing establishes local application behavior. It does not establish
real provider execution, actual GitHub delivery or production VM health. The
live-fleet observation gate remains separate; no synthetic completion counts as
real delivery evidence.
