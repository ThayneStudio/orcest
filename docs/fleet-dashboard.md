# Fleet dashboard

The dashboard shows observed work as **Upcoming → In progress → Done**. Waiting
is an activity within that lifecycle: a dependency can hold upcoming work, while
CI can hold an already-started change. An agent's successful exit does not mark
work Done; the initial delivery evidence is an observed merged PR. Verified
issue-to-PR handoffs preserve a single card through delivery.

This implements the [product vision](vision.md) and the legacy integration in
[the source mapping](dashboard-data-mapping.md). The standalone prototype remains
separate. Production deployment and the running fleet's v1/monitor configuration
must be verified before calling rollout complete.

## Sign in

Set `DASHBOARD_TOKEN` in the existing protected dashboard environment file. Open
the dashboard URL without a token query parameter; `/sign-in` accepts the token
in a POST body and creates a random, HttpOnly, SameSite=Strict session cookie.
Sessions expire after 12 hours. Sign out revokes the session and closes its open
WebSockets. Restarting the server or rotating `DASHBOARD_TOKEN` invalidates all
sessions. HTTPS requests receive a Secure cookie; preserve the external scheme
at a trusted reverse proxy. Use HTTPS or an SSH tunnel when accessing the fleet.

Sign-in currently uses a shared administrator access token, not individual
GitHub identities, roles, or OAuth. Incorrect attempts are rate limited per
connection IP. Existing bearer-token and legacy cookie/query clients remain
compatible; new sign-in never writes the token to browser storage or a URL.

`/api/health`, `/api/ready`, and sign-in assets remain public. Fleet data, detail,
output, the application, and diagnostic APIs require authentication. With no
configured token, data access fails closed. Keep `DASHBOARD_ALLOWED_ORIGINS` and
`DASHBOARD_REDIS_PREFIXES` configured for the intended browser origin and scope.

## Data path

The orchestrator publishes allowlisted issue/PR observations before discarding
selector results. Successful enqueue records queue evidence. Workers record
first execution and bounded attempt identities; finished attempts do not alter
verified work outcomes. Delivery verification records explicit issue/PR links;
merge confirmation records completion. Observation failures are best effort and
never change scheduler behavior.

Redis keys, under each existing project prefix:

- `dashboard:project`: last project poll and configured account inventory.
- `dashboard:work:{issue|pr}:{owner/repo}:{number}`: last source observation,
  first execution, dependencies, publication link, and outcome evidence.
- `dashboard:work:…:attempts`: latest 50 attempt IDs.
- `dashboard:attempt:{task_id}`: safe provider/account/worker identity and times;
  expires after 30 days. No task credentials are copied.
- `dashboard:tracked`: resources retained for reconciliation after they leave
  ready/open discovery. At most five missing resources are checked per poll.

First-execution evidence has no independent TTL; it depends on Redis persistence.
Merged records remain for 30 days before expiry. A missing observation, lost
history, closed issue, or failed source request never implies successful delivery.
Closed-without-merge resources are excluded from the active board rather than
misreported as delivered. Reopened issues can return to observation.

`GET /api/work` returns the fleet view with project/search filters and pagination
(`offset`, `limit`, max 500 per response). Counts and Fleet inventory cover the
configured dashboard scope; board filters change the visible cards. The exception
inbox independently fetches all attention items, unaffected by board filters.
`GET /api/work/:id` returns context and attempt history. The server resolves IDs
only within its configured scope. A short shared cache coalesces concurrent reads;
the browser refreshes every three seconds and retains its last snapshot on errors.
The inventory is capped at 5,000 records with an explicit coverage notice.

Output uses the existing authenticated `/ws/task-output` transport and its cursor,
retention, missing-output, and prefix-ambiguity protections. The initial timeline
shows retained execution attempts and the latest observed condition. The optional
monitor event timeline and archived-trace API are not yet connected to this UI.
Workflow v1 ownership is surfaced with an incomplete-coverage notice; the v1 run
projection still needs its own read-only adapter before a v1-owned fleet can be
fully represented. Never infer deployed v1 adoption from its presence in source.

## Fleet semantics

Configured provider accounts are distinct from worker processes and VM capacity.
Account IDs are the existing non-secret account keys; model variants share an
account and budget. Availability reflects Orcest's pool cooldown observations,
not a promise that the next provider request will succeed.

The existing reactive Claude quota probe now retains safe utilization windows
and their observation time. Other accounts or accounts not yet probed display
“Not reported.” Old percentages are explicitly last-probe values, not live
remaining credit. Provider tokens are never sent to the browser.

VM pools come from pool-manager records (allocated and warm). Worker rows come
from current heartbeat keys. A heartbeat indicates worker liveness, not agent
progress. Active execution requires a matching work attempt and lock/task
identity. Missing VM state does not become a fabricated capacity denominator.

## Local validation

Use the supported Node 20 runtime and the project's Python development environment:

```sh
cd dashboard
npm ci
npm run typecheck
npm test
npm run build
npm run check:bundle-runtime
ORCEST_TEST_PYTHON=../.venv/bin/python npm run check:fleet-e2e
```

The final command requires `redis-server` on PATH, or set
`ORCEST_TEST_REDIS_SERVER=/absolute/path/to/redis-server`. It creates an isolated,
nonpersistent Redis process on a loopback port, calls the real Python observation
writers, starts the built server, and verifies sign-in, dependency waiting,
queueing, execution, live output, CI waiting, verified merge, project filtering,
and sign-out. Its source/provider inputs are fixtures; passing it is not proof
of connectivity to the deployed fleet. It never starts an agent or changes GitHub.

Run `make check-fast` for Python checks. On macOS, disable tar metadata with
`COPYFILE_DISABLE=1`; the provider-probe tests also expect a tool PATH without
an `npm`-named directory. The unit test command is `pytest -q -m unit`.

For UI development, `npm run dev` proxies API, output, and sign-in to the local
Express service. For the built application, set `PORT`, Redis connection variables,
`DASHBOARD_TOKEN`, and the prefix allowlist, then run `npm run preview`.

## Rollout and rollback

1. Verify the current host, revisions, project prefixes, optional workflow v1
   ownership, and monitor deployment. Preserve protected environment files.
2. Deploy the orchestrator observation writers and the dashboard from one tested
   revision using the existing release workflow. The observation keys are additive;
   no scheduler migration or discovery-policy change is required.
3. Roll worker images through the normal pool process so new attempts write
   first-start/history evidence. Drain existing attempts normally. During mixed
   revisions, the dashboard must not claim missing attempt evidence is live work.
4. Open the actual browser URL, sign in, and confirm known source items, real
   executing tasks/output, provider cooldowns, VM allocation, and exceptional
   blockers. Verify dependency waiting stays Upcoming and CI waiting remains
   In progress. Observe a real handoff/merge before certifying that transition.
5. Sign out and verify protected APIs and existing session sockets lose access.

The existing `make deploy-dashboard-remote` validates the dashboard source and
release revision. It deploys only the dashboard; the orchestrator and worker
changes require their normal rollout too. Do not deploy a dashboard-only release
and describe an empty, unavailable feed as a successful fleet integration.

Rollback the affected images to the previous verified revision. The old runtime
ignores `dashboard:*` keys, which can remain in Redis. Do not delete coordination
keys or flush Redis. Restarting the dashboard requires users to sign in again.
