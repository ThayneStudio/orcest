# Monitor exposure runbook

The monitor service (`orcest monitor --config <path>`, see
[`docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md`](superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md))
runs two independent HTTP listeners in one container:

- **Ingest** (`:9091`, `POST /ingest/v1/events`) — private, single write
  token, receives batched CloudEvents-shaped envelopes from each project's
  orchestrator (`EventRelay`). **Never expose this port outside the compose
  network.**
- **Query** (`:9090`, `GET/HEAD /api/v1/*`) — read-only (SQLite opened
  `mode=ro` + `PRAGMA query_only=1`), scoped bearer tokens per reader
  (`events:read`, `traces:read`).

This runbook walks through standing the service up and, if you need it
reachable from outside the orchestrator host, exposing the query listener
through a Cloudflare Tunnel behind Cloudflare Access. Follow the steps in
order.

## 1. Mint tokens

Generate one write token (ingest) and one bearer token per reader (query),
and put them in the compose `.env` file next to `docker-compose.monitor.yml`:

```bash
echo "MONITOR_WRITE_TOKEN=$(openssl rand -hex 32)" >> .env
echo "MONITOR_TOKEN_ADMIN=$(openssl rand -hex 32)" >> .env
# one MONITOR_TOKEN_<NAME> per additional reader, matching config/monitor.yaml
```

Treat these like any other credential: `.env` stays out of git, and each
token maps 1:1 to a named reader in `config/monitor.yaml` (`token_env` field)
or, for the write token, to `write_token_env`.

**Adding a reader takes three edits, not two.** Besides the `.env` line and
the `config/monitor.yaml` reader entry, the matching `MONITOR_TOKEN_<NAME>`
variable must also be listed in `docker-compose.monitor.yml`'s
`environment:` block — Compose only forwards explicitly-named variables, so
without that line the container never sees the token and the reader can
never authenticate, even though `.env` and `monitor.yaml` both look right.
(`MONITOR_WRITE_TOKEN` and `MONITOR_TOKEN_ADMIN` are already listed; every
additional reader needs its own line.)

## 2. Point the orchestrator(s) at the ingest listener

For each project whose orchestrator should emit events, add the monitor
block to that project's `orchestrator.yaml` (see
`config/orchestrator.example.yaml`):

```yaml
monitor_ingest_url: "http://monitor:9091/ingest/v1/events"
monitor_write_token_env: "MONITOR_WRITE_TOKEN"
```

`monitor` here is the compose service name — it resolves over the shared
`orcest` Docker network, so the orchestrator container needs no port
published to reach it. Pass `MONITOR_WRITE_TOKEN` (the same value minted in
step 1) through the orchestrator's own compose `environment:` block so the
relay can authenticate to ingest. If `monitor_ingest_url` is unset, the
relay is disabled and orcest is otherwise unaffected.

## 3. Start the monitor service and verify locally

On the orchestrator VM, `Dockerfile.monitor`, `docker-compose.monitor.yml`,
`config/monitor.example.yaml`, and `README.md` (needed by the image build)
are delivered to `/opt/orcest/` by `orcest fleet deploy`/`update` as part of
the source tarball. There is **no** `orcest fleet` subcommand that builds or
starts the monitor container — this step is manual:

```bash
cd /opt/orcest
cp config/monitor.example.yaml config/monitor.yaml   # edit readers as needed
docker compose -f docker-compose.monitor.yml up -d
curl -s http://localhost:9090/api/v1/health
```

Note that every subsequent `fleet update` re-copies (overwrites) those four
files, so keep local state in the files it preserves — `.env` and
`config/monitor.yaml` — and never hand-edit `docker-compose.monitor.yml` on
the VM without also landing the change in the repo (this includes the
per-reader `MONITOR_TOKEN_<NAME>` lines from step 1).

Expected: `{"ok": true}` with no auth header — `/api/v1/health`
and `/api/v1/openapi.json` are the only unauthenticated routes on the query
listener. Everything else 401s without a valid `Authorization: Bearer
<token>` header and 403s if the token's reader lacks the required scope.

## 4. External exposure via Cloudflare Tunnel + Access

Only do this if the query API needs to be reachable from outside the
orchestrator host (e.g. a dashboard or a teammate's laptop). The ingest
listener (`9091`) is never exposed this way — see the warning below.

1. Add `cloudflared` as a sidecar in `docker-compose.monitor.yml` (or run it
   as its own compose stack on the same `orcest` network), pointed at the
   `monitor` service's query port:

   ```yaml
     cloudflared:
       image: cloudflare/cloudflared:latest
       restart: unless-stopped
       command: tunnel run
       environment:
         - TUNNEL_TOKEN=${CLOUDFLARE_TUNNEL_TOKEN:?set in .env}
       networks:
         - orcest
   ```

   Configure the tunnel's ingress rule (via the Cloudflare dashboard or
   `cloudflared tunnel ingress`) to route the chosen hostname to
   `http://monitor:9090` — the internal compose DNS name and port, not
   `localhost`.

2. Create the tunnel and hostname:

   ```bash
   cloudflared tunnel create orcest-monitor
   cloudflared tunnel route dns orcest-monitor monitor.example.com
   ```

3. In the Cloudflare Zero Trust dashboard, create an **Access application**
   for `monitor.example.com` with a **Service Auth** policy (not an
   interactive login policy — this endpoint is consumed by scripts/dashboards,
   not humans clicking through an SSO prompt).

4. Mint one **Access service token** per consumer (each dashboard, each
   teammate, each automation) under that application. Each service token is
   a `CF-Access-Client-Id` / `CF-Access-Client-Secret` pair, independent of
   the app-level bearer tokens minted in step 1.

**Important:** Cloudflare Access is the network perimeter, not the
authorization layer. A request that clears Access still needs a valid
`Authorization: Bearer <monitor reader token>` with the right scope — the
app-level token check in `orcest.monitor.auth` runs regardless of how the
request arrived. Do not treat "has an Access service token" as equivalent to
"is allowed to read events/traces."

**Ingest (`:9091`) must never get a tunnel hostname.** Only the query
listener (`:9090`) is exposed through Cloudflare. The ingest listener stays
reachable only on the compose network, authenticated by the single write
token from step 1 — there is no reader-scoped access control on it, so
exposing it publicly would let anyone with the write token (or a leaked
token) forge events for any project.

## 5. Verify external access

Once the tunnel and Access application are live, test with a minted service
token and a reader's bearer token:

```bash
curl -s https://monitor.example.com/api/v1/health
# (unauthenticated route -- should succeed even through the tunnel)

curl -s https://monitor.example.com/api/v1/events \
  -H "CF-Access-Client-Id: <service-token-client-id>" \
  -H "CF-Access-Client-Secret: <service-token-secret>" \
  -H "Authorization: Bearer <MONITOR_TOKEN_ADMIN value>"
```

Expect:
- Missing/invalid `CF-Access-Client-Id`/`Secret` → blocked by Access before
  it reaches the container (Cloudflare's own 403 page, not orcest's JSON).
- Valid Access headers but missing/invalid `Authorization: Bearer` → `401
  unauthorized` from `orcest.monitor.auth`.
- Valid Access headers, valid bearer token, but the reader lacks the
  endpoint's required scope → `403 forbidden`.
- All three satisfied → `200` with the JSON event/timeline/trace payload.

## 6. Revocation

Two independent layers to revoke, either or both depending on the situation:

- **App-level reader token** — remove the reader's entry from
  `config/monitor.yaml` (or rotate its `token_env` value in `.env`), then
  `docker compose -f docker-compose.monitor.yml restart monitor` to pick up
  the change. A plain `up -d` is **not** sufficient after editing only the
  bind-mounted `monitor.yaml`: Compose sees no change to the service
  definition and leaves the running container (and its in-memory reader
  set) untouched — use `restart` (or `up -d --force-recreate`) so the
  removal actually takes effect. This revokes that reader's ability to
  authenticate to the query API entirely, tunnel or not.
- **Cloudflare Access service token** — revoke it from the Access
  application's service token list in the Zero Trust dashboard (or `cloudflared
  access service-token revoke`). This blocks that consumer from reaching the
  tunnel hostname at all, even if it still holds a valid app-level bearer
  token.

Revoke both when fully offboarding a consumer; revoking just one still
leaves the other layer's authorization intact for anyone who retains it.

## 7. Watchdog rollout

The activity watchdog (per-task liveness ladder on workers, `PoolManager`
reaping on `needs_reap`/stale activity below the `max_task_duration`
ceiling) ships disabled and is turned up in stages, independent of the
monitor-exposure steps above. `watchdog.enabled: false` is the rollback
lever at every stage — it restores **wall-clock-only** behavior: the
runner's fixed `RunnerConfig.timeout` plus the pool reaper's ceiling-only
destroy (`_health_check`'s `needs_reap`/`activity_stale` paths never fire —
see below), with no other config change required. This is wall-clock-only
at the *raised* default values this migration ships (`timeout` 21600s,
`pool.max_task_duration` 25200s), not the original pre-migration numbers —
"restores today's behavior" would be imprecise, since those ceilings
themselves moved as part of this change (§7 of the design doc).

Why the reaper's activity-aware paths stay dark with the watchdog off: no
worker ever writes `workers:activity:{worker_id}` when
`watchdog.enabled: false` (same for an old worker image mid-rollout that
predates the tracker), so the record is always absent. But
`_health_check`'s `activity_stale` destroy path additionally requires the
worker's `workers:heartbeat:{worker_id}` liveness heartbeat — written by
every worker unconditionally — to be absent too, and a live worker without
the watchdog still writes that heartbeat. So an absent activity record with
a present heartbeat is read as "alive, just not running the watchdog," not
"dead," and the VM is left alone below the ceiling exactly like the
pre-watchdog reaper.

Every change here touches the three deploy layers the same way the rest of
orcest does: host CLI (`pip`/`pipx install -e .` or equivalent), `orcest
fleet update` to roll the new code into the orchestrator/pool-manager
containers, and `orcest fleet rebake` to bake it into the worker template.
The watchdog's worker-side pieces (liveness tracker, ladder, activity
record writes) only take effect after a rebake — pushing the code and
running `fleet update` alone updates the pool manager's *reaping* logic but
leaves already-cloned workers running the old runner until they cycle
through a fresh clone of the rebaked template.

**Rollout prerequisite: interactive Claude workers have no ladder coverage.**
`ClaudeInteractiveRunner` (pool `worker_runner_mode: interactive`, the PTY
Claude driver) never routes through `_BaseCliRunner`/`tracker_factory` — it
gets no `LivenessTracker`, no ladder, no `SUSPECT`/`STUCK`/`LOOPING`
evaluation, at any stage below, `watchdog.enabled` or not. Cloud-init
compensates by pinning those workers' wall-clock `runner.timeout` to the
pre-branch default (5400s) instead of letting them inherit the raised
21600s ceiling with nothing backstopping a genuine hang before it (see
`cloud_init.render_clone_userdata`'s `_INTERACTIVE_RUNNER_TIMEOUT_SECONDS`).
That is a mitigation, not parity: an interactive worker still only ever
gets the blunt fixed-timeout kill, never the ladder's earlier
stuck/looping detection. Before relying on this rollout's stage 2/3
observability or kill behavior for a project's Claude pool, move its
profile(s) to `worker_runner_mode: headless` (the non-PTY `claude -p`
runner, which *does* route through the generic tracked path) to get real
ladder coverage. Fleet configs that must stay on interactive PTY workers
keep today's wall-clock-only behavior for that pool regardless of what
stage the rest of this rollout is in.

1. **Ship dark.** Deploy with `watchdog.enabled: false` and the monitor
   container up (see steps 1-3 above). Events flow (enqueue/start/complete),
   the query API is queryable, and the pool reaper stays wall-clock/
   ceiling-only — every worker still writes its liveness heartbeat, which
   keeps `_health_check`'s `activity_stale` path from ever firing while no
   worker is writing an activity record (see above) — this is purely wiring
   verification before any behavior changes.

2. **Observation mode.** Flip `watchdog.enabled: true` fleet-wide with
   `max_kills_per_hour: 0`. At this budget the ladder still evaluates every
   tick, but a would-be `STUCK`/`LOOPING` transition is *gate-deferred*: the
   reported state stays at `SUSPECT` and the transition never happens, so
   **no `net.orcest.task.stuck`/`net.orcest.task.looping` event is emitted
   in observation mode at all** — only `net.orcest.task.suspect` (and the
   periodic `net.orcest.task.activity` snapshot). `needs_reap` never gets
   set, so no still-running task is killed early on the watchdog's say-so.
   Operators watching false-positive rate during this stage should watch
   two things instead: (a) `task.suspect` episodes that never recover into
   a normal completion — a task that reaches SUSPECT and then finishes
   normally anyway is a false positive; and (b) the `deferred_kill` flag on
   the activity record / periodic `task.activity` event, which is `true`
   exactly when the fleet gate suppressed a kill the ladder would otherwise
   have fired this tick — that is the direct "would have killed" count a
   gated-off `stuck`/`looping` event can't give you. The pool reaper's
   `ceiling` and `activity_stale` paths are **not** gated by this budget —
   `activity_stale` only ever fires for a worker whose liveness heartbeat
   has genuinely gone (see above), meaning the VM already died for an
   unrelated reason, so reclaiming it during observation mode is correct
   and is not itself an "early kill" of a live task. Watch both signals
   against real workloads via the monitor for several days before tuning
   the ladder thresholds (`idle_window`, `waiting_grace`, the loop
   thresholds) or moving to stage 3.

3. **Enable kills.** Once the false-positive rate is acceptable, raise
   `max_kills_per_hour` to the real budget (default 6). From this point a
   `STUCK`/`LOOPING` kill is no longer gate-deferred, so the ladder fires it
   and the runner's watchdog thread kills the process tree — but
   `needs_reap` itself is **not** set just because a kill fired. It's set
   only when the post-kill D-state verification (a 2s wait, then a
   process-tree check) finds the tree did NOT actually die — i.e. a failed
   `SIGKILL`, not every kill. A cleanly-verified kill lets the attempt exit
   normally; only an unverified one flags `needs_reap` so the pool reaper's
   fast (10s) loop force-destroys the VM instead of leaving a zombie
   process tree parked in the warm pool. Keep watching `task.reaped`
   events' `reason` field (`ceiling` / `needs_reap` / `activity_stale`) via
   the monitor for a few more days; a spike in `needs_reap`/`activity_stale`
   reaps relative to `ceiling` reaps is the signal to look at, since those
   are the two paths the watchdog adds.

If a stage regresses, drop back to `watchdog.enabled: false` rather than
tuning forward under pressure — it is a complete, tested rollback to
wall-clock-only reaping (at this migration's raised ceiling values), not a
partial mitigation.

**Multi-project note:** the kill-budget mirror (`orcest:fleet:kill_budget:limit`)
and the pressure key are both *global* (unprefixed) Redis keys shared by
every per-project orchestrator's `FleetHealthMonitor`, each refreshing the
same mirror from its own `fleet_health.max_kills_per_hour` on every pass —
last write wins, with no cross-project reconciliation. Keep each project's
`fleet_health:` block (`pressure_min_tasks`, `pressure_window`,
`pressure_hold`, `max_kills_per_hour`) identical across every
per-project `orchestrator.yaml` in the fleet; divergent values don't error,
they just flap the effective fleet-wide budget between whatever value each
project's monitor last wrote.

## 8. Operational gotchas

### Deploy order: `fleet update` before `fleet rebake`

Roll the containers **before** the workers: run `orcest fleet update` (which
ships the new pool manager honoring `pool.max_task_duration`) *before*
`orcest fleet rebake` (which bakes workers with the raised 21600s
`runner.timeout`). Rebaking first puts long-running workers under the old
pool manager, which will reap them at the old ceiling mid-task.

If any deployment's fleet config pinned `pool.max_task_duration` explicitly
(old guidance was 7200), it MUST be raised above the new worker timeout plus
grace (the shipped default is 25200 = 21600 + 3600) before rebaking —
otherwise every healthy long task gets its VM destroyed as a `ceiling` reap
at the stale pinned value, regardless of the watchdog's verdict.

### Redis memory sizing for the events spool

The events spool retains up to `DEFAULT_EVENTS_MAXLEN` (50000) envelopes
**per project stream** (`XADD ... MAXLEN ~ 50000`), and never trims below
that cap — activity snapshots make typical envelopes ~1KB or more, so a
single busy project can hold 50MB+ of spool indefinitely. The shipped Redis
runs with `--maxmemory 256mb --maxmemory-policy noeviction`, and that budget
is *shared* with the task streams, locks, and pending markers. Once Redis
hits the wall, `noeviction` fails **writes fleet-wide** — task `XADD`s
included, not just event spooling. Size `maxmemory` for
(projects × 50000 × envelope size) plus operational headroom, or lower the
events maxlen cap, before pointing multiple busy projects at the relay.

### Manual PEL surgery now needs the pending markers deleted too

The old recovery recipe for a stalled consumer group — `XGROUP DELCONSUMER
orcest:tasks:<stream> workers <dead-worker-id>` to drop a dead consumer's
PEL entries and let the orchestrator's re-poll loop re-enqueue — no longer
re-enqueues promptly. The pending-task marker TTL is derived from the runner
timeout (`compute_pending_task_ttl`), which at the raised 21600s ceiling is
now ~18h; until the marker expires, the poll loop still sees the work as
in-flight and will not re-publish it. When doing that surgery, also `DEL`
the relevant `pending:*` markers (`pending:{resource_type}:{repo}:{id}`,
key-prefix aware) so the next poll cycle re-enqueues immediately.
