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

```bash
cp config/monitor.example.yaml config/monitor.yaml   # edit readers as needed
docker compose -f docker-compose.monitor.yml up -d
curl -s http://localhost:9090/api/v1/health
```

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
  `docker compose -f docker-compose.monitor.yml up -d` to restart the
  `monitor` service and pick up the change. This revokes that reader's
  ability to authenticate to the query API entirely, tunnel or not.
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
lever at every stage — it restores exactly today's behavior (pure
wall-clock reaping at `max_task_duration`, no early kills) with no other
config change required.

Every change here touches the three deploy layers the same way the rest of
orcest does: host CLI (`pip`/`pipx install -e .` or equivalent), `orcest
fleet update` to roll the new code into the orchestrator/pool-manager
containers, and `orcest fleet rebake` to bake it into the worker template.
The watchdog's worker-side pieces (liveness tracker, ladder, activity
record writes) only take effect after a rebake — pushing the code and
running `fleet update` alone updates the pool manager's *reaping* logic but
leaves already-cloned workers running the old runner until they cycle
through a fresh clone of the rebaked template.

1. **Ship dark.** Deploy with `watchdog.enabled: false` and the monitor
   container up (see steps 1-3 above). Events flow (enqueue/start/complete),
   the query API is queryable, and pool-reaper behavior is unchanged —
   this is purely wiring verification before any behavior changes.

2. **Observation mode.** Flip `watchdog.enabled: true` fleet-wide with
   `max_kills_per_hour: 0`. At this budget the ladder still evaluates and
   emits `net.orcest.task.suspect`/`stuck`/`looping` transition events (and
   the activity-aware pool reaper still only reaps on the ceiling, since a
   `needs_reap`/`activity_stale` kill trigger is gated by the same budget),
   but no task is killed early. Watch the `task.suspect` (and `stuck`)
   false-positive rate against real workloads via the monitor for several
   days — a task that reaches SUSPECT/STUCK and then finishes normally
   anyway is a false positive and a signal the ladder thresholds
   (`idle_window`, `waiting_grace`, the loop thresholds) need tuning before
   any kill budget goes live.

3. **Enable kills.** Once the false-positive rate is acceptable, raise
   `max_kills_per_hour` to the real budget (default 6). From this point the
   activity-aware reaper can destroy a VM below the `max_task_duration`
   ceiling on `needs_reap` or on an absent/stale activity record with
   pending work — not just at the ceiling. Keep watching `task.reaped`
   events' `reason` field (`ceiling` / `needs_reap` / `activity_stale`) via
   the monitor for a few more days; a spike in `needs_reap`/`activity_stale`
   reaps relative to `ceiling` reaps is the signal to look at, since those
   are the two paths the watchdog adds.

If a stage regresses, drop back to `watchdog.enabled: false` rather than
tuning forward under pressure — it is a complete, tested rollback to the
pre-watchdog reaper, not a partial mitigation.
