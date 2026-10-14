# Self-Hosted Orcest

Status: **proposal, not approved.** The build is paused until Austin signs off.

Revision 2 addressed the Critical and High findings of two reviews (Astra and Fable) and recorded the decisions Austin made on them. Revision 3 settles the second-round reviews (Astra-2 and Fable-2) with Austin's decisions: v1-aligned records, fresh approvals, protected-path globs, a test-count floor, same-Attempt checks, a reserved `main` CI share, a freeze on human merges, and a narrower M5. [Review findings addressed](#review-findings-addressed) maps each finding to the section that answers it. Medium findings are listed with their fixes in [Medium findings](#medium-findings-planned-fixes).

This doc covers three separate pieces that let orcest run its own development loop on pve-test without GitHub in the loop:

1. **Backlog and task state** owned by orcest, in its own database.
2. **Code hosting**: a plain git host plus diff review.
3. **CI** run by orcest's own Proxmox workers.

It also covers how the machines in the fleet trust each other, how today's GitHub-based fleet keeps running alongside the new one, how `selfhosted-runners` merges into orcest, and the migration path. The end state is that GitHub is a private mirror.

**Placement.** Everything here runs on **pve-test** (`pve-test.lab.prefixa.net`). Nothing in this design touches **pve.lab**; promotion to pve.lab is Austin's call and his step.

This builds on Workflow Control v1 ([`docs/wiki/`](../wiki/README.md)), which is implemented but not yet adopted (epic #667). v1 already provides much of what is needed:

- the SQLite single-writer Run Store;
- forge-neutral Work Items;
- separate Activity and Attempt records, with fenced claims and generations;
- the fenced candidate upload (a git bundle) and controller-only publication;
- signed, attempt-bound worker capabilities;
- an audited Run-command endpoint;
- controller modes (`INTAKE_PAUSED`, `DISPATCH_PAUSED`, `DRAINING`);
- `VERIFY` activities on workers;
- budget reports.

Revision 1 re-derived looser versions of several of these. Revision 2 uses the v1 contract as written wherever one exists. Where this doc needs v1 to change, it says so under **v1 amendment**. Those changes are spec amendments to the wiki, made before any code is written.

## Deployment shape on pve-test

The **self-hosted fleet** (called fleet B below) is a separate orcest install. It shares nothing with the live GitHub fleet (fleet A: VM 199, `/etc/orcest`, workers from 10000).

| Piece | Where | Notes |
|---|---|---|
| Controller (Run Store, API, MCP, dashboard, monitor), Redis, pool manager | VM 20000 | One HTTPS listener on `:443`, bound to the LAN and Tailscale interfaces only; no public tunnel. Nothing else on the VM publishes a port except Redis (see [Fleet trust](#fleet-trust)). |
| Git host | VM 20100 | See [Code hosting](#2-code-hosting). |
| Dependency mirror (PyPI and npm proxies) | VM 20101 (from the reserve) | See [Reproducible builds](#reproducible-builds). |
| Workers | Agent workers 20010–20059; CI pool (checks and deploys) 20060–20099; templates 20001–20009 | Named `orcest-sh-worker-<vmid>`. |
| GitHub Actions runners | 9000–9999, run by `selfhosted-runners` until GitHub is off | See [selfhosted-runners merges into orcest](#selfhosted-runners-merges-into-orcest). |
| Fleet config | `/etc/orcest-sh/config.yaml` on the host | Its own CLI venv under `/opt/orcest-sh`. `/usr/local/bin/orcest` and `/etc/orcest` are never touched. |

### VM ID ownership

Each fleet gets its own bounded VM ID range, and every cleanup path checks ownership, not just a range or a name. This is a **precondition before M0**.

- **Fleet A:** `vm_id_end: 19999` is a hard precondition, and config validation rejects `0`.
- **Ranges:**

| Owner | Range |
|---|---|
| Runner fleet (`selfhosted-runners`) | 9000–9999 |
| Fleet A orchestrator | 199 (stays) |
| Fleet A workers | 10000–10899 |
| Fleet A templates | 10900–10999 |
| Fleet B controller | 20000 |
| Fleet B templates | 20001–20009 |
| Fleet B agent workers | 20010–20059 |
| Fleet B CI pool | 20060–20099 |
| Fleet B git host | 20100 |
| Fleet B reserve | 20101–20119 (the dependency mirror takes 20101) |

- **Before any destroy, stop or disk cleanup, require all of these:**
  - the ID is inside that fleet's explicit start and end;
  - the marker `orcest fleet=<id> role=worker|ci|template vmid=<vmid>` in the VM description, plus the tag `orcest-<fleet>`, with the marker's vmid matching the VM's own;
  - the name prefix matches;
  - templates are destroyed only by template cleanup;
  - nothing happens while a config can't be proven gone;
  - startup fails if any two fleets' ranges overlap.
- **Allocation checks the whole cluster.** `_next_vm_id` in `src/orcest/fleet/pool_manager.py` builds its occupied set from VMs named `orcest-worker-*` only. It is replaced by the cluster-wide reservation ported from `selfhosted-runners` (see [carry-over](#selfhosted-runners-merges-into-orcest)).
- **Fleet B gets its own scoped Proxmox token**, so its pool manager cannot act outside fleet B's VMs.

**Fleet A's templates sit inside the runner fleet's range.** Fleet A's current worker template is VM 9003, and `orcest-worker-10002` was cloned from VM 9002. Both are inside the runner fleet's 9000–9999, not the 10900–10999 fleet A templates range above. Left alone, `selfhosted-runners` with `MAX_VMID=9999` would treat them as its own to clean up, and fleet A's ownership checks would reject its own templates. Fleet A's `rebake` also rotates through every VMID in `pool.template_vmid_range`, not just the current one: the CLI's suggested value is `[9000, 9009]`, and template 9001 was in live use earlier (`docs/operations/fleet-dashboard-preflight.md`). One of these two fixes is part of the prerequisite:

1. **Move (preferred).** At fleet A's next rebake, set `pool.template_vmid_range` to a range inside 10900–10999 and bake there. The old templates in 9000–9999 are destroyed only after no fleet A worker is still a linked clone of them, and only by fleet A's template cleanup.
2. **Named temporary exception.** Until the move, list 9002 and 9003, plus every other VMID in fleet A's deployed `template_vmid_range`, as fleet A templates on loan from the runner range. Fleet B's ownership checks and `selfhosted-runners`' `MAX_VMID` reservation and cleanup must never touch them. The exception is removed once fleet A's templates are in 10900–10999.

The pve.lab side, meaning which template VMIDs fleet A uses there and whether they collide with a runner range, is **Austin to confirm at promotion**.

**Not verified yet.** These checks are read-only and run only on pve-test or the runner hosts. If a value lives on pve.lab, nobody logs in there: the item is marked **Austin to confirm at promotion**, and the work keeps moving. A first attempt on 2026-10-14 could not run. The machine doing the revision had no trusted SSH host key or usable credential for pve-test, and accepting an unknown host key was out of scope, so all four items are still open:

- the live `/etc/github-runners.conf` values;
- fleet A's deployed template range and `vm_id_end`, including whether `template_vmid_range` is `[9000, 9009]` and which template VMIDs exist;
- which cluster the runner fleet runs on;
- whether the Proxmox API covers host-side disk cleanup.

### Code changes needed before fleet B can coexist with fleet A

Their defaults stay as today, so fleet A is unaffected.

- **Configurable worker-name prefix.** `orcest-worker-` is a literal in more places than `_VM_NAME_PREFIX`. The M0 change covers every call site:
  - `src/orcest/fleet/pool_manager.py` (`_VM_NAME_PREFIX`, worker ID build and parse);
  - `src/orcest/fleet/cli.py` (worker listing, `fleet stop`, orphan checks, the `orcest-worker-template-build-*` name);
  - `src/orcest/fleet/orchestrator.py` (worker IDs);
  - `src/orcest/rollout_health.py` (the `orcest-worker-[1-9][0-9]*` match and tracked worker IDs).
- **Per-fleet Terraform dir, VM name and snippet name.** `create-orchestrator` uses fixed state at `/etc/orcest/terraform`. Running it for a second fleet would replace VM 199.
- **Whole-cluster VMID allocation**, **ownership checks on destroy**, config validation that rejects `vm_id_end: 0`, and the startup check for overlapping fleet ranges, as above.

## Fleet trust

This section defines how each machine in fleet B proves who it is, and what each one may do. Fleet B's trust is separate from fleet A's: no key, token or CA is shared.

**Fleet CA.** Fleet B has its own certificate authority. Its key lives only on VM 20000 (backed up encrypted with the Run Store backup). Its certificate is baked into every fleet B template and installed with the fleet B CLI. Every HTTPS and Redis TLS connection in fleet B validates against it, and clients fail closed on a validation error. Bearer tokens over this TLS are enough on pve-test; mTLS can wait for pve.lab (this answers revision 1's open question 4).

| Machine | Proves itself by | May do | May not do |
|---|---|---|---|
| Controller (VM 20000) | Server certificate from the fleet CA. | Write to the git host (the only write key). Own the Run Store. Publish to GitHub through the bot token (see [migration](#coexistence-and-migration)). Hold deploy credentials. | — |
| Pool manager (on VM 20000) | Fleet B's own scoped Proxmox API token. | Clone, start, stop and destroy fleet B's VMs, subject to the [ownership checks](#vm-id-ownership). Inject a one-time enrollment token per VM through its cloud-init snippet. | Touch any VM that fails the ownership checks. |
| Worker VM | At boot, exchanges its one-time enrollment token for a **worker session** bound to its VMID and boot ID. The enrollment token is single-use and expires 10 minutes after the clone. Per task, it receives v1's signed attempt capability. | Fetch from the git host with a read-only key (forced command: `git-upload-pack` only). Upload candidate bundles (intermediate and final) and results for its own current Attempt. Start checks on commits the controller published from those bundles (agent tasks only). Write its own heartbeat and activity keys in Redis. | Push to the git host. Hold any person's or bot's token. Read or write another attempt's state. |
| Git host (VM 20100) | SSH host key, pinned in every template's `known_hosts` and in the controller. | Serve fetches. Accept pushes from the controller's key only. | — |
| Dependency mirror (VM 20101) | Server certificate from the fleet CA. | Proxy PyPI, npm and Docker pulls. | Accept uploads. |
| Redis (on VM 20000) | TLS with the fleet CA certificate. **One ACL user per worker**, created at enrollment with its own password and deleted when the worker session is revoked. | A worker's ACL user may write only its own keys (`workers:heartbeat:<worker-id>`, `workers:activity:<worker-id>`) and read its own stream consumer group. No worker can write another worker's heartbeat. | Redis is never authority. No task is failed, fenced or retried because of a Redis value alone (see [the reaper](#the-reaper)). |
| Actions runner VMs (9000–9999, `selfhosted-runners`) | GitHub registration token, per VM, one job. | Run GitHub Actions jobs. | Reach VM 20000's API or Redis, or hold any fleet B token. |

A worker session, and with it the worker's Redis ACL user, is revoked when its VM is destroyed or when the pool manager reports the boot gone. An attempt capability is revoked the moment its attempt is fenced (see [Termination](#termination-fence-first)).

## 1. Backlog and task state

### Ownership

The backlog lives in the controller's Run Store, not in GitHub labels.

- **Backlog item.** A backlog item is a v1 **Work Item** whose source is `native`: the controller creates it and it has no forge issue behind it. An imported item is a native item with a GitHub URL kept as provenance.
- **Run.** Admitting a backlog item creates a **Run**.
- **Task.** A task is a v1 **Activity**: one unit of work, such as "implement this item" or "run the `unit` pipeline on this head".
- **Attempt.** Each execution of a task on one worker is a v1 **Attempt**, with its own generation number, claim fence, capability and deadline.

**Activity and Attempt are separate records.** An Activity has many Attempts. An Attempt is terminal once it ends, and a retry is a new Attempt with the next generation under the same Activity. This is what gives retries a legal path without ever reopening a terminal record.

The forge-label intake path (`list_work_items_by_labels`) stays for GitHub projects. Native items reach the same snapshot and admission code through a `native` source adapter.

**v1 amendment:** add `native` as a Work Item source, with create, edit and close operations as authenticated commands, and an `owner_fleet` field (see [migration](#coexistence-and-migration)).

### One task model with types

Every task has a `type`. All types share one queue, one lifecycle (the v1 records below), one log store and one set of controls.

| Type | What runs | Examples |
|---|---|---|
| `agent` | A provider CLI (Claude, Codex or Grok) works a backlog item or a review follow-up. | Implement an item; address review comments; fix a failing check. |
| `deterministic` | Steps from a versioned pipeline, run by the trusted grading driver, with no AI. | A check, a build. |
| `deploy` | A deploy script from trusted code, with a scoped credential, in the deploy profile. | Deploy a built artifact to a named pve-test target. |

**What all types share:**

- **Queue and claims:** the same queue and fenced claims. Each type has its own reserved capacity (see [Capacity](#capacity)).
- **Lifecycle:** the same v1 Activity and Attempt records, reaper, deadlines and terminal guarantee.
- **Failures:** the same structured `reason_code` and `reason_detail` fields.
- **Logs:** the same log stream and retention. Deterministic and deploy tasks log per step, with each step's command and exit code.
- **Controls:** requeue, cancel and kill. Pausing a worker and draining a host apply to all types.
- **Audit:** the same audit events.

**What differs:**

| | `agent` | `deterministic` | `deploy` |
|---|---|---|---|
| Liveness | Heartbeat plus the liveness ladder. | Heartbeat plus per-step timeouts. | Heartbeat plus per-step timeouts. |
| Credentials | Provider credential for this attempt only. | None. | A short-lived credential scoped to one named target (see [Deploys](#deploys)). |
| Result | A final candidate bundle (plus any intermediate bundles uploaded during the Attempt), or an abstention for a human decision. | A v1 Verification Receipt: `PASS`, `FAIL` or `ERROR`, with per-step exit codes and test counts. | Deployed artifact digest and target. |
| Automatic retry | Per the [retry policy](#retry-policy). | Per the retry policy. | Never. |
| Recorded usage | Provider, CLI version, model, tokens. | Execution manifest (see [Reproducible builds](#reproducible-builds)), per-step exit codes and durations. | Artifact digest, target, credential ID (never the secret). |

**Fleet view.** The dashboard lists all tasks in one view, filterable by type, repo, worker and state. The API and MCP list operations take the same four filters, as catalogue parameters.

### v1 records and the dashboard view

Fleet B stores exactly v1's Activity and Attempt records, with v1's states and transitions. The dashboard shows a simpler view derived from them, and that view is never written back.

**Activity** (v1 `state`):

| State | Meaning |
|---|---|
| `PLANNED` | No nonterminal Attempt. The Activity is waiting to be offered (held by a mode, capacity, budget or stop-pending gate), waiting on something outside the fleet (the Run has a Wait Condition or a Human Boundary), or back here after an Attempt ended while recovery decides on a retry. |
| `READY` | Exactly one current `OFFERED` Attempt. |
| `ACTIVE` | Exactly one current `CLAIMED` Attempt. |
| `SUCCEEDED` | Terminal. |
| `FAILED` | Terminal and permanent: no retry is allowed or left. |
| `CANCELLED` | Terminal: ended by a cancel or kill command. |
| `SUPERSEDED` | Terminal: replaced by newer input, such as a new Change Request head. |

**Attempt** (v1 `state`):

| State | Meaning |
|---|---|
| `OFFERED` | Created in the same transaction as its dispatch outbox row, with a fixed `claim_deadline`. |
| `CLAIMED` | A worker session claimed it. The claim fence, the capability and the fixed execution deadline are in the store. |
| `SUCCEEDED` | Terminal: an accepted `SUCCEEDED` Result. |
| `FAILED` | Terminal: an accepted failure Result from the worker, or a failure the controller proved (see [the reaper](#the-reaper)). |
| `ABSTAINED` | Terminal: the worker handed the task back without finishing it, for example to wait on a human decision or a quota reset. |
| `EXPIRED` | Terminal: a deadline passed. For `OFFERED` that is the claim deadline; for `CLAIMED`, the execution deadline. |
| `SUPERSEDED` | Terminal: the controller fenced it, for a cancel, a kill or a newer head. |

**Transitions are v1's.**

- `PLANNED -> READY` offers generation n+1 as a new `OFFERED` Attempt.
- A claim moves `READY`/`OFFERED` to `ACTIVE`/`CLAIMED` in one step.
- An accepted success sets both records to `SUCCEEDED`.
- Any other end of an Attempt makes it terminal and returns the Activity to `PLANNED` in the same transaction. The exception is when the controller chooses a permanent `FAILED`, `CANCELLED` or `SUPERSEDED` for the Activity.
- A terminal Activity has no nonterminal Attempt and is never reopened.

**Retries have a legal path.** A retry is the next generation, offered through `PLANNED -> READY` when the [retry policy](#retry-policy) allows it. An operator's `REQUEUE` is different: it creates a new Activity that links back with `requeued_from`.

**An unclaimed offer is not a failure.** When an `OFFERED` Attempt passes its claim deadline, v1's capacity classifier expires it (`EXPIRED`) and the Activity returns to `PLANNED`. It is re-offered when capacity allows. Nothing fails because no worker was free.

**Dashboard view.**

| Shown as | Derived from |
|---|---|
| `queued` | Activity `READY`, or `PLANNED` held only by a mode, capacity or stop-pending gate. |
| `running` | Activity `ACTIVE`. |
| `waiting(<reason>)` | Activity `PLANNED`, with a Wait Condition on the Run (`dependency`, `budget`, `ci`, `capacity`) or a Human Boundary (`human`). |
| `done` | `SUCCEEDED`. A check also shows its verdict: `pass` or `fail`. |
| `failed` | `FAILED`. |
| `cancelled` | `CANCELLED`, with reason `cancel_requested`, `cancel_escalated` or `operator_kill`. |
| `superseded` | `SUPERSEDED`. |

Three flags are shown alongside these: `suspect` (a `CLAIMED` Attempt whose heartbeat is late or whose liveness ladder reports `STUCK`), `cancelling` (a cancel is in progress), and `stopping` (the latest Attempt is fenced but its stop is not yet confirmed).

**Resume after waiting.** A task that goes `waiting` gives up its worker, VM and workspace. When it resumes, it gets a new Attempt. For an agent task, the new Attempt's input includes the prior Attempt's last candidate bundle (if any) and a transcript summary; it does not resume the old conversation. An agent checking its own work does not go `waiting(ci)`. It stays `ACTIVE`, uploads intermediate bundles, and reads their check results within its own deadline (see [Same-Attempt checks](#same-attempt-checks)). `waiting(ci)` is only for a follow-up task created after an Attempt abstained.

**Deadlines are fixed, not reset.**

- Each `OFFERED` Attempt gets a claim deadline. Each `CLAIMED` Attempt gets an execution deadline at claim: for `agent`, the configured ceiling; for `deterministic` and `deploy`, the sum of the step timeouts plus a fixed margin. No transition extends either.
- Each Activity gets `expires_at` and `max_attempts` when it is planned. Retries and waits spend from these; they never reset them.
- Past `queue_alarm_after` (default 15 min) without a claim, the dashboard shows `waiting(capacity)` and alarms. The Activity still fails at `expires_at`.
- `waiting(human)` is exempt from `expires_at`. It is listed in a "waiting on humans" queue with its age, and alarms after `human_wait_alarm_after` (default 3 days). It never fails on time.

**Every task reaches a terminal state, or is visibly waiting on a human.** This is enforced, not just checked:

- The invariant, checkable from the store alone without Redis: every nonterminal Activity has `expires_at` or is waiting on a Human Boundary; every `OFFERED` Attempt has a claim deadline; every `CLAIMED` Attempt has an execution deadline.
- A deadline sweeper runs every 30 s and acts on every passed deadline.
- An overdue alarm fires for any nonterminal record more than two sweep intervals past its deadline.

### How an Attempt ends

An Attempt ends in one of two ways.

**The worker reports.** This is the normal path, including for failures. The worker submits a v1 Attempt Result under its current generation, with outcome `SUCCEEDED`, `FAILED_RETRYABLE`, `FAILED_PERMANENT` or `ABSTAINED`. A failure carries v1's `failure_class` and `failure_code`; for example, `PROVIDER_RATE_LIMIT` with `failure_retry_after_ms`, or `VERIFICATION_ERROR` with an `ERROR` receipt. The controller maps the code to a `reason_code` through the classifiers (see [Failure reasons](#failure-reasons)). The store accepts the Result only if the generation and claim are current. After the Result, the worker exits and the pool manager destroys the VM, which confirms the stop.

**The controller proves it.** This covers a worker that cannot report. See [the reaper](#the-reaper).

### The reaper

The reaper is a controller loop. It is not part of any worker, so a dead worker cannot block it. It reads heartbeats (`workers:heartbeat:*`), activity records, the liveness ladder, and the pool manager's inventory. **Heartbeats and activity records are signals; they never fail a task on their own.**

**A task fails only on proof.** When the worker has not reported, only three things may end a `CLAIMED` Attempt. Each one fences the Attempt first:

1. **The VM is confirmed gone.** A fresh, successful Proxmox inventory read shows the VMID absent, or present with a different boot ID than the Attempt's worker session (the VMID was reused). A failed or timed-out inventory read is "unknown", never "gone". This is v1's pool-manager worker-loss report. Attempt `FAILED`, reason `worker_lost`.
2. **The reaper confirmed a kill.** The reaper fenced the Attempt, killed the process tree (or destroyed the VM), and the kill was acknowledged: the guest agent reports the process tree gone, or a fresh inventory read shows the VM gone. Reasons: `no_progress`, `tool_loop`, `step_timeout`, or `worker_lost` when the VM was up but the runner process had died.
3. **The deadline passed.** The Attempt's fixed execution deadline: Attempt `EXPIRED`, reason `duration_ceiling`. Or the Activity's `expires_at`: reason `deadline_exceeded`.

| Signal | Result |
|---|---|
| No heartbeat for `heartbeat_timeout` (default 300 s) | The Attempt is flagged `suspect`, and stays `CLAIMED`. The reaper probes Proxmox VM status, then the runner process through the guest agent. |
| Probe: VM confirmed gone | Rule 1. |
| Probe: VM up, runner process gone | The reaper fences, destroys the VM and confirms it gone: rule 2, reason `worker_lost`. |
| Probe: VM up and runner alive, or probe inconclusive | Stays flagged `suspect`, with an alarm. Rule 3 still applies at the deadline. |
| Heartbeat resumes | The `suspect` flag clears. |
| Liveness ladder `SUSPECT` (its first rung) | No flag on the dashboard; the rung clears often. |
| Liveness ladder `STUCK` | Flagged `suspect`. |
| Liveness ladder `LOOPING`, or `STUCK` past the kill budget | Fence, kill, confirm: rule 2, reason `tool_loop` or `no_progress`. The fleet-wide kill budget and pressure gate still apply. |
| A deterministic or deploy step outlives its timeout | Fence, kill the step, confirm: rule 2, reason `step_timeout`, naming the step. |

**Outage guard.** The fleet-wide guard against reaping during an outage is restored from today's pool manager (`_activity_stale_infra_fault` in `src/orcest/fleet/pool_manager.py`). The reaper enters **outage hold** when either:

- Redis was unreachable or restarted within `outage_grace` (default 10 min); or
- more than `outage_fraction` of active workers (default 50%, and at least 2) lose their heartbeat in the same pass.

In outage hold:

- no heartbeat-driven probe leads to a kill;
- no Attempt is flagged `suspect` because of a missing heartbeat;
- the dashboard alarms, naming the cause;
- durable deadlines (rule 3) still apply, because they come from the store, not Redis.

**Startup reconciliation.** Claims and deadlines survive a controller restart, as v1 requires. At startup the reaper does not invalidate claims by age. It reconciles every `CLAIMED` Attempt against a fresh inventory read and the worker session's boot ID, and applies the rules above. This closes today's gap, where 8 tasks from August and September still show as active.

**Fleet A's stuck tasks.** No import into fleet B is planned. For fleet A, only the startup inventory reconciliation (VM confirmed gone while the task is still active) is offered as a fix to its legacy loop, for Austin to deploy. The heartbeat rules are not offered, because fleet A has no fencing to make them safe.

**v1 amendment:** the loss rules follow v1's "Liveness, control, and deadlines" and "Pool-manager worker-loss report" contracts. The amendments are the outage-hold parameters, the reaper-kill fact (rule 2), and the stop record below.

### Termination: fence first

Stopping an execution and recording its outcome are one durable operation with a defined order:

1. **Fence.** In one Run Store transaction:
   - make the Attempt terminal (`FAILED`, `EXPIRED` or `SUPERSEDED`, with its reason);
   - revoke its capability;
   - return the Activity to `PLANNED`, or to `CANCELLED` for a cancel or kill;
   - record the Attempt's **stop** as `PENDING`;
   - write a `stop` job to the durable outbox.

   From this point the store rejects any result, upload or check request from that generation.
2. **Stop.** The outbox worker kills the process tree through the guest agent, or destroys the VM. This is idempotent and retried with backoff. It survives controller restarts because the job is in the store.
3. **Confirm.** When the kill is acknowledged, the stop becomes `CONFIRMED`.

**The task fails only after the stop is proven.** While the latest Attempt's stop is `PENDING`, its Activity sits in `PLANNED` behind a stop-pending gate. No new generation is offered and no permanent `FAILED` is recorded. The gate lifts when either:

- the stop is `CONFIRMED`; or
- the old Attempt's execution deadline passes. The deadline is proof in its own right, and the stop stays `PENDING` while the outbox keeps trying.

Rule 1 confirms the stop by its own evidence.

After the gate lifts with the stop still `PENDING`:

- **Agent and check tasks** may retry. The old execution cannot publish (workers have no git write), cannot submit results (fenced), and cannot start checks (capability revoked). Its only remaining effect is provider spend, which is shown on the dashboard.
- **Deploy tasks** may not retry. The target environment is quarantined: no deploy to it is accepted until the stop is confirmed or a human clears the quarantine with an audited command. See [Deploys](#deploys) for the controller's own exemption.

The crash points before and after the fence, and before and after the kill acknowledgement, are each covered by a test.

### Cancellation is durable

- `CANCEL` is a Run Store command row, written before anything else happens. It survives controller restarts; the reaper resumes unfinished cancels at startup.
- A `PLANNED` Activity is cancelled at once. A `READY` one is too: its `OFFERED` Attempt becomes `SUPERSEDED`.
- An `ACTIVE` Activity gets `cancel_requested_at` and a cancel deadline (default 120 s), and the dashboard shows it `cancelling`. The worker sees the request on its control poll, stops cooperatively and acknowledges. The controller then fences: the Attempt becomes `SUPERSEDED`, and the Activity becomes `CANCELLED` with reason `cancel_requested`.
- If the worker has not stopped by the cancel deadline, the cancel escalates to the fence-first path with reason `cancel_escalated`.
- `KILL` skips the cooperative step and fences at once, with reason `operator_kill`.
- A newer head on the same Change Request is not a cancel. The old checks' Activities become `SUPERSEDED`.

### Retry policy

This is the only place retry rules are defined. The reason comes from the worker's Result when it reported, or from the controller's proof when it did not. A worker's `FAILED_PERMANENT` is never retried, whatever its code.

| Reason | Retry | Max attempts | Backoff |
|---|---|---|---|
| `worker_lost` | yes | 3 | 1 min, then 5 min |
| `clone_failed`, `image_unavailable` | yes | 3 | 2 min, then 10 min |
| `verification_error` (a check's `ERROR` receipt, per v1) and `step_timeout` | yes (checks only) | 2 | 2 min |
| `no_progress` | yes (agent only) | 2 | none |
| `tool_loop` | no | — | — |
| `pipeline_invalid`, `provider_cli_rejected`, `provider_auth_failed` | no | — | — |
| `provider_quota_exhausted` | not a failure: the Attempt abstains (or fails retryable with `PROVIDER_RATE_LIMIT`) and the task waits as `waiting(budget)` until the reset | — | — |
| `duration_ceiling`, `deadline_exceeded` | no | — | — |
| `merge_conflict` | no; a follow-up agent task is created | — | — |
| `unclassified` | no | — | — |
| any reason, `deploy` task | no | — | — |

A check that ran to completion and found a failing step is `SUCCEEDED` with a `FAIL` receipt. That is a verdict, not a failure, and it is not retried.

### Failure reasons

A `FAILED`, `CANCELLED` or `SUPERSEDED` Activity, and every terminal Attempt other than `SUCCEEDED`, has a structured reason. Free text never replaces it.

| Field | Contents |
|---|---|
| `reason_code` | A closed enum: `provider_cli_rejected`, `provider_auth_failed`, `provider_quota_exhausted`, `worker_lost`, `no_progress`, `tool_loop`, `duration_ceiling`, `deadline_exceeded`, `step_timeout`, `verification_error`, `test_count_below_baseline`, `image_unavailable`, `pipeline_invalid`, `superseded`, `merge_conflict`, `clone_failed`, `admission_rejected`, `operator_kill`, `cancel_requested`, `cancel_escalated`, `unclassified`. |
| `failure_class` / `failure_code` | For a worker-reported failure, v1's class and the worker's code, kept as reported. |
| `reason_detail` | Specific evidence, bounded and with secrets redacted. For example: `grok 0.1.216 rejected: HTTP 426 Upgrade Required (min 1.0.13)`, or the exit status plus the last classified stderr line. |
| `stop` | `CONFIRMED` or `PENDING`, for each terminal Attempt. |
| `attempts` | The Attempts under this Activity, each with its generation, state, reason and stop. |

The generic text "transient, failed after 3 attempts" is no longer used. When retries run out, the task fails with the last Attempt's reason, and every Attempt stays listed.

Classifiers map provider output and worker failure codes to reason codes. Some already exist, such as Grok HTTP 402 balance exhaustion and `_is_usage_exhausted`. An unclassified failure becomes `unclassified`, with its detail kept, and the dashboard counts these so new classifiers get written.

### Per-attempt usage

Usage is recorded per Attempt, because that is the unit that ran. Each agent Attempt stores:

- **Provider and CLI:** the provider and runner (`claude`, `codex` or `grok`) and the CLI version, using the existing provider-version probe.
- **Model:** the requested model and the model the CLI reported.
- **Tokens:** approximate input, output and cache-read tokens, plus `usage_source`: `reported` when the CLI's stream gives usage, or `estimated` when the count comes from bytes. The numbers are always labelled approximate.
- **Time:** wall time, and the worker VMID, boot ID and template version.
- **Stop:** an Attempt whose stop is `PENDING` keeps accruing wall time until the stop is confirmed, so its spend is visible.

A task's total is the sum of its Attempts, and a Run's is the sum of its tasks. These totals feed v1 `budget-reports`.

### Control and audit

**One service layer, three transports.** Every operation is defined once, in a command catalogue with an input schema, a required scope and an audit event. The transports are:

- **HTTPS API:** `/api/v1/...`, with an OpenAPI document.
- **MCP server:** streamable HTTP on the same listener, plus stdio for local use. Stdio authenticates with a token like every other client; it gets no implicit authority from being local. One tool per catalogue entry.
- **Dashboard:** static files served from the same listener. It calls the API and holds no logic of its own (see [Dashboard sign-in](#dashboard-sign-in)).

A parity test fails the build if any dashboard control, API route or MCP tool is missing from the catalogue, or exists outside it.

**Operations and scopes**

| Scope | Operations |
|---|---|
| `read` | List and get backlog items, runs, tasks (filtered by type, repo, worker and state) and workers; events; per-attempt usage; diffs and check results; the control log. |
| `checks` | Run a pipeline on a Change Request head; read its results. |
| `backlog` | Create, edit, reprioritize and close items, and change an item's state. |
| `task` | Requeue (creates a new linked task), cancel (durable, cooperative first), kill (fence first). |
| `worker` | Pause (finish the current task, then take no new claims), resume. |
| `host` | Drain: no new clones and no new claims; running tasks finish or hit their deadline. Undrain. This is per Proxmox host, alongside v1's fleet-wide `DRAINING` mode. |
| `review` | Comment; approve or request changes. **Human subjects only** for approve and request changes. |
| `merge` | Merge (or, before M4, publish to GitHub). **Human subjects only.** |
| `deploy` | Start a deploy of an exact artifact to a named target; clear a deploy quarantine. **Human subjects only.** |
| `admin` | Issue and revoke tokens; override a red `main`. **Human subjects only.** |

**v1 amendment:** the closed command set today is `CANCEL` and `RESOLVE_HUMAN_BOUNDARY`. Add `REQUEUE`, `KILL`, `PAUSE_WORKER`, `RESUME_WORKER`, `DRAIN_HOST`, `UNDRAIN_HOST`, `DEPLOY`, `CLEAR_QUARANTINE`, and the backlog, review, merge and token commands. Each new command uses v1's transactional audit, outbox and idempotency rules. `REQUEUE` is an operator command that creates a new task. It does not replace v1's rule that retry is a reducer outcome.

**Tokens: one per person or bot**

- A **subject** is one person or one bot: `austin`, `dashboard-session:<id>`, `mcp-agent-<name>`, `github-mirror-bot`. Each subject has a stable ID and a `human` flag. Only an `admin` subject can create a subject, and the `human` flag can only be set from the CLI on VM 20000.
- Each subject has **one active token**, with its own scope set, expiry and `revoked_at`. Revoking it affects that subject only. Rotation issues a new token and revokes the old one after a short overlap (default 10 min).
- Tokens are random 256-bit bearers. The server stores only their hashes.
- Outside agents' MCP tokens default to `read`, `backlog` and `checks`. `review` comments can be granted; approval and merge cannot, because they require a human subject.
- **Only humans approve or merge.** The merge gate also rejects an approval from the subject that created the task that produced the head.
- Workers never receive subject tokens. An agent Attempt gets v1's signed attempt capability, delivered in an environment variable that the runner redacts from logs. It is bound to the Attempt ID, task, generation, worker session and Change Request. It can upload intermediate bundles, start checks on commits the controller published from them, and read their results (see [Same-Attempt checks](#same-attempt-checks)). It cannot touch pipelines, reviews, merges or other tasks. It dies when the Attempt is fenced. For a normal finish, v1's result-submission grace applies.
- Transport is HTTPS only, validated against the fleet CA, including on the LAN.

### Dashboard sign-in

- Every dashboard page requires sign-in; there is no anonymous view.
- A person signs in with their own token. The server exchanges it for a session: an `HttpOnly`, `Secure`, `SameSite=Strict` cookie, 12-hour lifetime, bound to that subject. Control actions also need a CSRF token.
- A session is bound to the token it was exchanged from. **Revoking or rotating that token ends all of its dashboard sessions at once**: the session rows are deleted in the same transaction, and the next request gets 401.
- Every action from the dashboard is audited under the signed-in person's subject. The dashboard process holds no token of its own and cannot act for anyone.
- The listener binds the LAN and Tailscale interfaces of VM 20000 explicitly, and logs the real bind address at startup (as `bc179be` does for today's dashboard).

### Audit

Every control command emits an audit event onto the event stream and records a Run Store row. The row names:

- the subject ID, the token ID (never the bearer) and the transport;
- the command, the target, and the before and after state;
- the request ID and the result;
- for denied commands, the denial. v1 already audits these.

The dashboard's **Control log** view reads the Run Store rows, not the `events` stream, which is capped at 50000 entries. The API and MCP expose the same log. A test sends every control command with a `read`-only token through the API, the dashboard and MCP, and checks each is denied and audited.

## 2. Code hosting

Orcest needs from a git host what the v1 adapter boundary lists:

- read refs and files at an exact commit;
- create a ref only if it is absent;
- update a ref with compare-and-swap;
- publish commits.

Everything else about a change is a **Change Request**: its diff, review, CI status and merge. Under this design, the Change Request is a record in orcest's Run Store.

### Workers never push

Workers never push to the git host. This is v1's boundary (domain model invariants I11 and I12; the worker protocol's security requirements):

1. An agent Attempt ends by uploading a final **git bundle** through its capability, over the fenced result protocol.
2. The controller validates the bundle (v1 limits on size and object count, the exact pinned base, path rules, hooks disabled).
3. The controller publishes it with its own git credential to `orcest/cr/<cr-id>/<rev>`, with create-if-absent. That revision becomes the Change Request's head.
4. The controller is the only SSH principal with write access. The git host's `pre-receive` hook rejects every push not made with the controller's key, and lets `main` move only through the controller's merge path.

Worker VMs hold a read-only fetch key (forced command `git-upload-pack`). A fenced or compromised worker cannot write anything.

### Same-Attempt checks

An agent can check a new commit without ending its Attempt:

- During the Attempt, the worker uploads an **intermediate bundle** through its capability. The limit is 10 per Attempt, and each is validated like a final bundle.
- Only the controller publishes. Each intermediate bundle goes to `orcest/cr/<cr-id>/wip/<attempt-id>/<n>`. It does not move the Change Request's head, and it cannot be approved or merged.
- The capability can start a [Verification Set](#verification-sets) on that commit and read the result. Bundles from a fenced generation are rejected.
- Check evidence is bound to the commit, target tip and policy digest, not to the ref. If the final bundle's head is a commit that was already checked under the same target and policy, the evidence is reused and the checks do not run twice.

**v1 amendment:** v1 accepts one candidate per Attempt, in its Result. Add intermediate candidate uploads under the same fence, with the limit above.

### Change Request states

| State | Meaning |
|---|---|
| `open` | Has a head. Checks or review are not yet satisfied for the current [binding](#merge-rules). |
| `ready` | Required checks pass and a valid human approval exists, both for the current binding. |
| `needs_rebase` | The target moved and the head no longer descends from it. The controller is rebasing it, or the rebase conflicted. |
| `published` | Before M4 only: pushed to GitHub as a draft PR from `selfhosted/<cr-id>`; waiting for Austin to merge it there. |
| `merged` | Terminal. After M4: `main` was moved to this head. Before M4: the mirror shows the GitHub PR merged. |
| `closed` | Terminal. Abandoned or rejected. |

### Before cutover, `main` is a mirror

Until M4, the git host's `main` is a **fast-forward-only mirror of GitHub `master`**, and the merge command cannot move it.

- The controller fetches GitHub `master` on a schedule (default 2 min) and fast-forwards `main`. A non-fast-forward fetch is refused and alarms.
- In M1 to M3, "merge" means **publish**: the Change Request is `ready`, a human runs the merge command, and the controller pushes the head to GitHub as `selfhosted/<cr-id>` and opens a draft PR. The Change Request becomes `published`.
- It becomes `merged` when GitHub shows the PR merged. This is matched by PR number, not SHA, because squash merges change SHAs.
- Agents base their work on the mirror's `main`. An item that depends on an unmerged Change Request waits (`waiting(dependency)`) until that Change Request is `merged`; stacked changes are not supported before M4.
- At M4 the direction flips in one audited step (see [Migration](#coexistence-and-migration)), and the merge command gains the right to move `main`.

### Merge rules

**Merges are fast-forward or rebase only.** No merge commits are created.

- The gate requires that the head descends from the current `main` tip, so the tree that lands is exactly the tree that was checked.
- **The system rebases open changes after a merge.** After every fast-forward of `main` (and, before M4, every mirror advance), the controller rebases each open Change Request onto the new tip. It works in its own workspace with hooks disabled, publishes the result as a new revision, and starts that revision's checks. If a rebase conflicts, the Change Request is marked `merge_conflict` and a follow-up agent task is created. A human can also run the **rebase** command at any time. A rebased head keeps its original author for the approval rules below.
- `main` moves with compare-and-swap against the exact tip the evidence was bound to.

**Evidence is bound, and re-checked when the binding changes.** A check result and an approval are each bound to:

- the repository;
- the target branch and its tip SHA;
- the head SHA and tree;
- the set of required pipelines;
- the **policy digest**: the digest of the pipeline files and protected-paths list at the current target tip.

At merge time the gate recomputes the binding.

- **Checks re-run when the head, the target or the policy changes.** A pass graded by an older policy never satisfies a newer one.
- **A new approval is needed whenever the head, the target or the policy changes.** Approvals never carry over, including across a clean rebase. To make re-approval quick, the reviewer sees a `git range-diff` against the last approved head. It is a review aid only and counts for nothing at the gate.

**Red `main`.** If the post-merge `main` pipelines fail, further merges are blocked until `main` is green again or a human overrides with the audited `admin` command.

**Review.** A merge needs at least one human approval, from a subject that did not create the task that produced the head. Agent reviews (from outside agents with a `review`-scoped token) are comments; they never count toward approval. A change that touches protected paths needs an approval that explicitly acknowledges them (see [The grading driver](#the-grading-driver-is-trusted-code)).

### Option A: bare git over SSH (recommended)

- **What it is:** a small VM running `git` and `git-shell`. The controller holds the only write key; workers hold a read-only fetch key; see [Fleet trust](#fleet-trust).
- **Compare-and-swap:** `git push --force-with-lease=<ref>:<expected>`, or atomic `update-ref` through a hook. Both are real compare-and-swap.
- **Review:** orcest renders the diff (`base...head`) in the dashboard, the API and MCP. A review is a control command (`approve`, `request_changes`, or a comment on a file or line), audited like every other command.
- **Pros:**
  - One control plane, so no second user, permission or state model.
  - A small attack surface: no web app, no database, almost nothing to upgrade.
  - Backups are `git bundle` plus the Run Store backup v1 already specifies.
- **Cons:**
  - Orcest has to build diff review. A unified diff with file-level and run-level comments plus approve and reject is moderate work. Inline, line-anchored threads that survive rebases are the expensive part and can come later.
  - There is no web UI for browsing the code. `gitweb` or `cgit` in read-only mode can be added if wanted.

### Option B: Forgejo as a git host only

- **What it is:** Forgejo with issues, wiki, packages and Actions disabled, used for repositories, pull requests and their review UI.
- **Pros:** mature diff review out of the box, with line comments, suggestions and history; a web code browser; built-in push mirroring to GitHub.
- **Cons:**
  - The review and merge state lives in Forgejo, not orcest. That needs a Forgejo forge adapter, and orcest's audit log would record "approved per Forgejo PR N", not who approved.
  - Another application to run: a database, user accounts, upgrades and CVEs.
  - Its pull request merge flow would compete with the controller's.

### Recommendation

**Option A.** Section 1 already routes every decision through orcest's audited command catalogue, and review is one more such decision. The extra work, a diff view with run-level and file-level review, is bounded. Outside agents with a `review`-scoped token can comment through MCP without extra work.

Line-anchored comments can follow if Austin finds the first version too thin. If he wants Forgejo's review UI from day one, Option B works with the same controller, at the cost of a Forgejo adapter and the split state described above.

**Austin's decision.**

## 3. CI: deterministic check tasks

There is no separate CI product. A check is a `deterministic` task, run by orcest's own Proxmox workers. In v1 terms it is a `VERIFY` Activity ([review-and-consensus.md](../wiki/review-and-consensus.md)), grouped into a Verification Set (below).

**Pipelines live in the repo.** They are defined in versioned files under `.orcest/` (v1's repository configuration). Each pipeline lists:

- its trigger: Change Request head updates, `main` after a merge, or manual runs;
- whether it is **required** for merge;
- its image, pinned by digest (`…@sha256:…`), never by tag;
- its steps in order, each with a command (a literal argument array, run without a shell, as v1 requires), a timeout, and, for steps that run tests, the test report the driver reads (JUnit XML or Vitest JSON).

The policy also lists **protected paths** (see below).

**Triggers create Verification Sets automatically.**

- **Change Request:** every new head, including a system rebase, gets a Verification Set for the exact head SHA. Sets for an older head are superseded.
- **Intermediate bundle:** an agent's capability can request a set on a commit published from its bundles (see [Same-Attempt checks](#same-attempt-checks)), at most 10 per Attempt.
- **`main`:** each move of `main` gets a set of the `main` pipelines (the `master-verified` job moves here). These run in the CI share reserved for `main` (see [Capacity](#capacity)).
- Requests are deduplicated per (commit, target tip, policy digest).

### Verification Sets

One structure is used everywhere checks are run or gated:

- A **Verification Set** is keyed by (commit, target tip, policy digest). When it is created, it freezes the list of pipelines that apply, each marked `required` or `optional`.
- Each member is one v1 `VERIFY` Activity, that is, one `deterministic` task, with its own Attempts. It ends with one v1 Verification Receipt: `PASS`, `FAIL` or `ERROR`.
- Members keep v1's execution contract. Commands run in declared order, every command runs after an ordinary non-zero exit, and only a sandbox, tool, resource or integrity error may stop early. `ERROR` is not `FAIL`: it is a `FAILED_RETRYABLE` Result with `VERIFICATION_ERROR`, retried per the [retry policy](#retry-policy).
- **Set result:**
  - `PASS` when every required member is `SUCCEEDED` with a `PASS` receipt;
  - `FAIL` when any required member has a `FAIL` receipt;
  - `ERROR` when any required member ends `FAILED` after its retries;
  - `PENDING` otherwise.

  Optional members are shown but never gate.
- If any of the three keys changes for a Change Request, its set is superseded as a whole, and every unfinished member becomes `SUPERSEDED`.
- The merge gate reads exactly one thing: the result of the set keyed by the current head, the current target tip and the current policy digest.

**v1 amendment:** v1 has one `default` Verification Profile and one `VERIFY` Activity per Candidate. Add the Verification Set record, and one named Verification Profile per pipeline. Receipt validation is updated to match before implementation.

### The grading driver is trusted code

The **grading driver** is the program that reads the pipeline, runs each step, enforces timeouts, captures exit codes and writes the verification receipt. It never comes from the candidate:

- It is built from `main` by the controller and baked into the verification image, which the policy pins by digest.
- It reads the pipeline and the protected-paths list from the **current target tip**, never from the change's head. The candidate is checked out into a separate directory and is only ever input to the driver.
- A change that edits `.orcest/` is graded by the current policy. Its new policy takes effect only after merge.

**The pipeline is pinned, but its steps still run candidate code.** A step like `make check-fast` runs the candidate's `Makefile`, `tests/conftest.py`, `pyproject.toml` markers and package scripts. An agent could make a step pass by editing those without touching `.orcest/`. Two controls cover this:

- **Protected paths need human review.** The policy lists, as globs, the files the steps delegate to. `**` matches any depth, including the repo root. For this repo the initial list is:
  - pipeline and build entry points: `.orcest/**`, `Makefile`, `Dockerfile*`, `docker-compose*.yml`, `.github/workflows/**`;
  - Python test and lint config: `**/conftest.py` (today that is `tests/`, `tests/integration/`, `tests/stress/` and `tests/workflow_store/`), `pyproject.toml`, `setup.cfg`, `pytest.ini`, `tox.ini`, `mypy.ini`, `ruff.toml`, `.ruff.toml`, `.pre-commit-config.yaml`;
  - CI and dashboard scripts: `scripts/**`, `dashboard/scripts/**`;
  - dependency manifests and lockfiles: `requirements*.lock`, `**/package.json`, `**/package-lock.json`;
  - TypeScript, Vite and lint config: `**/tsconfig*.json`, `**/vite.config.*`, `**/vitest.config.*`, `**/eslint.config.*`, `**/.eslintrc*`, `**/.prettierrc*`.

  The controller computes from the diff whether a Change Request touches any of them. The flag comes from the diff, not from a branch name or task setting. Such a change needs a human approval that explicitly acknowledges the protected paths, from a subject that did not author it. The list itself is protected.
- **Deleting tests fails.** Each test step names its report, and the driver counts the tests that ran and passed or failed. Skipped tests do not count. The **floor** is that step's count from the same pipeline's last passing run on the target tip (the baseline), which is recorded with every `main` set. A candidate that runs fewer tests than the floor fails the step with `test_count_below_baseline`, showing both numbers. When tests are removed on purpose, a human with `admin` records a floor override for that exact head, target and policy, and the override is audited. A pipeline with no baseline yet uses the floor declared in the policy, and the set is labelled "no baseline". The required pipeline set must be non-empty, or the merge gate refuses.

The specific attacks are tests:

- replacing a delegated check target with `true` must raise the protected-path flag and fail the floor (no report, so a count of 0);
- deleting a test, or adding a skip marker, must fail the floor.

**Every run starts fresh.**

- A new linked-clone VM per task. Steps run inside the pinned image on that VM.
- The checkout is the exact commit SHA, not a branch name.
- Nothing carries over from an earlier run: no cache volume, workspace or credentials.
- The VM is destroyed afterwards.
- Outbound network reaches only the dependency mirror (VM 20101) and the Docker mirror at 10.20.1.19.

**Verdict and error are different results.**

- The driver runs every step, even after a failing one, so the receipt shows all failures at once.
- All steps exit 0 and every test floor holds: the member is `SUCCEEDED` with a `PASS` receipt.
- Any step exits non-zero, or a floor fails: `SUCCEEDED` with a `FAIL` receipt, listing each failing step and its exit code. It is not retried.
- An execution problem is not a verdict, whether it happens before or during the steps. Examples are `image_unavailable`, `worker_lost`, `pipeline_invalid`, `step_timeout`, or a failure to capture evidence. The member gets an `ERROR` receipt or a controller-proven failure. It may be retried per the [retry policy](#retry-policy), and it blocks merge until it passes.
- There is no log parsing, no AI judgement and no "allowed to fail" step.

**Visibility.** Check tasks appear in the fleet view (filter `type=deterministic`), the API and MCP, with per-step logs. Full logs go to the v1 blob store.

### Reproducible builds

The achievable guarantee is **repeatable declared inputs**: two runs with the same execution manifest get the same inputs. M3 shows this with reruns before it is claimed.

- **Images pinned by digest.** The verification image is pinned in the policy. Nested images are pinned too: `FROM python:3.12-slim` in `Dockerfile` and `Dockerfile.monitor` becomes `FROM python:3.12-slim@sha256:…`. The driver rejects a Dockerfile `FROM` without a digest.
- **Lockfiles with hashes.** `requirements-dev.lock` has no `--hash` lines today. It is regenerated with hashes, and installs run with `pip install --require-hashes`. `dashboard/package-lock.json` already carries `integrity` hashes, and installs use `npm ci`.
- **Dependencies through a cache or mirror.** VM 20101 runs a PyPI pull-through proxy and an npm registry proxy; Docker pulls go through 10.20.1.19. CI VMs cannot reach `pypi.org` or `registry.npmjs.org` directly; only the mirror can. This also gives the `dashboard` check its npm path, which revision 1's network rule blocked.
- **Services in the image.** Redis, which the integration and stress targets need, is in the verification image.
- **Execution manifest.** Each check records: candidate SHA and tree, policy digest, VM template version, verification image digest, lockfile digests, a digest of the resolved package set (`pip freeze` and `npm ls` output), and nested base-image digests.

### Deploys

Deploys get a **separate profile**, the `deploy` task type. They are never a variant of a check.

- **Trigger:** a human subject with the `deploy` scope, naming an exact artifact digest and a named pve-test target. The artifact must come from a passing `main` build; builds keep their artifacts by digest in the v1 blob store before the build VM is destroyed.
- **Credentials:** the controller holds the deploy credential for each target and mints a short-lived credential scoped to that one target for each deploy. Check tasks and agent capabilities can never select the deploy profile or obtain its credentials.
- **Network:** the deploy VM reaches only its target and the dependency mirror.
- **Execution:** the deploy script comes from trusted code at the `main` tip, as the grading driver does. Deploys to one target are serialized by a per-target lock. A deploy is never retried automatically; a failure leaves the target as it is and alarms.
- **Rollback:** a deploy of the previous artifact digest, through the same command.
- **The controller deploying to itself is exempt from quarantine.** When the target is VM 20000, a stop left `PENDING` by an earlier self-deploy does not quarantine the target. Otherwise one failed self-deploy would leave no way to deploy a fix or roll the controller back. The exemption covers only that target, and every use of it is audited.

### Capacity

The reserved CI pool is sized from GitHub Actions history for this repo (the last 100 `ci.yml` runs, 2026-09-12 to 2026-10-14, read on 2026-10-14):

| Job | Runs on | p50 | p95 |
|---|---|---|---|
| `test` | PR and push | 9.1 min | 10.4 min |
| `docker` | PR and push | 1.9 min | 2.3 min |
| `dashboard` | PR and push | 1.4 min | 1.7 min |
| `lint` | PR and push | 1.3 min | 1.6 min |
| `typecheck` | PR and push | 1.2 min | 1.4 min |
| `integration` | push and schedule only | 3.1 min | 3.5 min |
| Whole run (wall time) | | 8.3 min | 15.1 min |

Merged PRs: 99 between 2026-08-25 and 2026-10-14, a peak of 22 in one day (2026-09-01). The current runners' queue wait was 0.7 min at p95.

From these numbers:

- **Two required pipelines per head, on two VMs:** `unit` (the `test` job, about 10.4 min at p95) and `static` (`lint`, `typecheck`, `dashboard`, `docker` and `integration` in sequence, about 10.5 min at p95). Grouping keeps one push at two VM boots instead of six, and the two take about the same time. Running `integration` on every head is stricter than today and is recorded as an intended difference for M3.
- **Seats and VMs are separate numbers.** Claude concurrency (seats) is 2: fleet B shares the subscription quota with fleet A. VM slots are counted separately below.
- **Reserved slots:**
  - 2 agent slots;
  - **4 CI slots for change checks**, so both agents can push at the same time and both heads start checking at once;
  - **1 CI slot reserved for `main` sets**, so post-merge checks and test baselines are never starved by change checks;
  - 1 deploy slot.

  That is at most 8 worker VMs at once, plus VMs 20000, 20100 and 20101. `main` sets may also use idle change slots; change checks never use the `main` slot.
- **No borrowing at the start.** Agent tasks cannot use CI slots and checks cannot use agent slots. An agent Attempt waiting on its own checks holds only its agent slot, so there is no circular wait.
- **Priority within the change-check slots:** required members on the newest heads first, then same-Attempt checks, then optional members; ties by age.
- **Check-start latency target:** p95 under 5 min from push to first step, measured in M0 and M3.

Not yet measured, and an M0 exit check: linked-clone boot time on pve-test, and pve-test's CPU, RAM, storage and clone I/O headroom with fleet A and the Actions runners at their own peaks. If the headroom does not cover 8 worker VMs, the agent slots drop to 1 first. The `main` slot is never given up.

## Coexistence and migration

**Fleet A keeps running unchanged on pve-test.** That means VM 199, workers in 10000–10899 and templates in 10900–10999, `/etc/orcest` and `/usr/local/bin/orcest`, its Saturday rebake timer, and `orcest:ready` on GitHub. Fleet B never edits fleet A's config, CLI, VMs, Redis or labels.

**Shared resources and their handling:**

- **Claude subscription quota** is shared, so fleet B starts with 2 Claude seats.
- **OAuth refresh tokens (Grok, Codex)** are not shared. Rotation by one fleet can invalidate the other's copy, so fleet B gets its own Grok login.
- **pve-test hardware** (CPU, RAM, storage, clone I/O) is shared with fleet A and the Actions runners; see [Capacity](#capacity).
- **The Docker mirror** at 10.20.1.19 is shared, read-only.

### Imported issues are owned by one fleet at a time

- Each imported item has an `owner_fleet`. Fleet B admits only items it owns.
- The importer refuses any issue that currently carries `orcest:ready` or `orcest:working`, or that has an open fleet A PR, and prints the list. Austin removes the label and confirms no fleet A work remains; then the import records ownership as `fleet-b`, with the label state seen at import as provenance.
- While M1 to M4 run, the controller reads GitHub (read access only) and pauses any owned item that regains `orcest:ready`, with an alarm.
- Handing an item back is an explicit export: it is closed in fleet B with reason `returned`, and Austin relabels it.

### selfhosted-runners merges into orcest

The Actions runners (VMs 9000–9999) are a shared fleet. This repo needs them until GitHub is turned off: `ci.yml` and `claude-review.yml` both run on `[self-hosted, linux]`, and `claude-review.yml` keeps reviewing PRs on GitHub, including fleet B's published PRs, until M5. The VM-management code of `selfhosted-runners` merges into orcest; its GitHub-specific parts stay where they are. The shared runner fleet itself is not orcest's to retire (see [M5](#migration-stages)).

`claude-review.yml` triggers on `opened` and `synchronize`, so it also reviews draft PRs. Proposed: add `ready_for_review` to its triggers and skip drafts, so a fleet B PR is reviewed once, when Austin marks it ready. This touches a workflow fleet A's PRs also use, so it is Austin's decision.

**Carry-over from `selfhosted-runners`.** That repo is Bash running as root on the Proxmox host, and orcest uses the Proxmox API, so this is a rewrite, not a copy.

Port into orcest:

- Cluster-wide VMID reservation (`reserve_vmid`). orcest only checks VMs named `orcest-worker-*`.
- The check that refuses to treat a missing VM config as proof the VM is gone while the shared config storage isn't serving (`vm_config_path_checked`).
- The ownership marker in the VM description, which only counts when its vmid matches the VM's own.
- The limit on parallel clones.
- Cleanup of disks left by failed clones and old templates.
- The safety checks behind the template build tests.
- Destroy-on-stop then reclone (`recycle.sh`/`reclone.sh`), as the model for the CI pool's fresh VM per run.
- Host settings as fleet config keys: bridge, VLAN, DNS, Docker mirror.

Stays in `selfhosted-runners` until GitHub is off, fixes only: runner registration and PATs, per-org setup, the `runner` CLI, and the Actions parts of cloud-init. Add `MAX_VMID=9999` to its reservation and cleanup, since that protects the live fleet today.

Drop: the file-lock and drain plumbing (orcest drains through Redis), and everything above once the switchover is done.

At M5, "dropped" applies only to what orcest's own migration took over. The shared 9000–9999 runner fleet and the `selfhosted-runners` code that runs it stay.

Credentials: the ported cleanup needs Proxmox API equivalents or a small helper on the host. Fleet B gets its own scoped Proxmox token.

### GitHub ends up as a private mirror

After M5, GitHub holds a read-only copy: fleet B's controller pushes `main` to GitHub `master` through `github-mirror-bot`, Actions are disabled for this repo, and the repo is made private. Making the repo private and disabling Actions are Austin's steps.

### Migration stages

Each stage has an exit check, and nothing advances on a calendar.

| Stage | What happens | Exit check |
|---|---|---|
| **Prerequisite** | Austin deploys fleet A with `pool.vm_id_end: 19999`. Fleet A's templates (9002, 9003 and the rest of its `template_vmid_range`) are either moved into 10900–10999 at the next rebake or listed as a named temporary exception, **before** `selfhosted-runners` gets `MAX_VMID=9999`. The [ownership checks](#vm-id-ownership) are in place. | Fleet A's deployed config shows `vm_id_end: 19999`, and the unverified items under [VM ID ownership](#vm-id-ownership) are checked or marked "Austin to confirm at promotion". **M0 does not start without it.** |
| M0 | The coexistence code changes (prefix call sites, Terraform dir, whole-cluster allocation, ownership checks) and the Grok ≥ 1.0.13 pin merge to `master`. Fleet B's infrastructure is created on pve-test: controller, git host, dependency mirror, templates and workers. Snapshots are taken first, and fleet A's baseline is diffed after each step. Recovery point RP0 is recorded. | Fleet A's baseline is unchanged. Fleet B's health is green. Boot time and pve-test headroom are measured and the [capacity](#capacity) numbers confirmed. |
| M1 | The orcest repo is imported into the git host. `main` is a fast-forward-only mirror of GitHub `master`. Fleet B's ready changes are published to GitHub as draft PRs from `selfhosted/*`; fleet A's loop skips drafts. Austin reviews and merges them on GitHub. | Draft PRs pass GitHub CI, and after Austin merges one, the mirror fast-forwards and the Change Request shows `merged`. |
| M2 | The native backlog opens. Selected GitHub issues are imported under the [ownership rules](#imported-issues-are-owned-by-one-fleet-at-a-time). New work for fleet B is created through the API, MCP or dashboard. | Every imported item is owned by exactly one fleet, and reaches a terminal state with reasons. |
| M3 | CI parity: check tasks run alongside GitHub Actions on the same changes, against a frozen parity manifest. | The parity exit check in [Medium findings](#medium-findings-planned-fixes) passes. |
| M4 | Cutover for the orcest project, by Austin's decision, as a single-writer handoff (below). The git host becomes canonical; GitHub becomes a mirror. | The handoff checks pass, the rollback export has been rehearsed against a restored copy, and the mirror shows `master` equal to `main`. |
| M5 | GitHub becomes a private mirror: Actions disabled for this repo and the repo made private (Austin's steps). Orcest retires **only the runner VMs that its own migration took over**, as recorded in its ownership manifest. The shared 9000–9999 runner fleet stays; its fate is Austin's call at pve.lab promotion. | No workflow runs on GitHub for 7 days; `master` still tracks `main`. |
| M6 | Promotion to pve.lab, done by Austin outside this design. | — |

### M4: single-writer handoff

1. **Freeze every writer.** Fleet A stops taking `orcest:ready` for this repo (Austin's step). Fleet B goes `DISPATCH_PAUSED`, and the controller refuses publication and merge commands, **including from humans**. On GitHub, Austin locks `master` against human merges. The freeze holds until step 7.
2. Drain or fence in-flight work in both fleets. No fleet A run, PR merge or publication retry for this repo may remain.
3. Reconcile: every open fleet B Change Request is either merged on GitHub or closed; every open fleet A PR is merged, closed or handed to fleet B.
4. Record the canonical SHA: GitHub `master` and the git host's `main` must be equal, and that SHA is written to the audit log.
5. Remove the old write authority: GitHub branch protection on `master` allows only `github-mirror-bot`.
6. Enable exactly one mirror direction (git host to GitHub), and give the merge command the right to move `main`.
7. Lift the freeze and resume fleet B.

### Rollback never deletes data

Rollback is an ordered transfer of state, not a teardown. Nothing is destroyed as part of it.

**Recovery points.**

- **RP-stage:** at the start of each stage, a verified Run Store backup, a `git bundle --all` of the git host, and VM snapshots of 20000 and 20100, all copied off those VMs.
- **RP-rollback:** taken during the rollback itself (step 4 below). It is the recovery point a rollback guarantees: nothing accepted before the freeze in step 1 is lost.

**Steps.**

1. Freeze: fleet B goes `INTAKE_PAUSED` and `DISPATCH_PAUSED`, and the controller refuses publication and merge commands, including from humans.
2. Drain or fence live work, using the fence-first path; wait for every stop to be confirmed or listed.
3. Reconcile publications: push every open Change Request head to GitHub as a draft PR from `selfhosted/<cr-id>`.
4. Export and verify, to storage off VMs 20000 and 20100:
   - a Run Store backup, verified by restoring it to a scratch copy and running the store invariant check;
   - a `git bundle --all`, verified with `git bundle verify` and by comparing every ref with the git host;
   - the native backlog as JSON and as GitHub issues (created by Austin, or by `github-mirror-bot` with his approval).
5. Compare exact refs: GitHub `master` must equal the git host's `main` (after M4), or the git host's `main` must still be a fast-forward mirror of GitHub (before M4).
6. Hand the canonical writer back: after M4, re-enable GitHub as canonical (branch protection back to its pre-M4 state) and turn the mirror off. Before M4, GitHub never stopped being canonical.
   - **After M5, also restore GitHub CI** before anyone merges on GitHub. Austin re-enables Actions for the repo, confirms that runners matching `[self-hosted, linux]` are registered (the shared fleet stayed, per M5), and confirms a `ci.yml` run on `master` is green. Then `master-verified` is re-enabled.
7. Stop fleet B's VMs. **Do not destroy them.** They stay stopped until Austin has confirmed the restored state and the recovered work.

Deleting fleet B is a separate, later decision by Austin. It deletes only VMs that pass every [ownership check](#vm-id-ownership), then `/etc/orcest-sh`, `/opt/orcest-sh` and fleet B's snippets.

## Medium findings: planned fixes

| Finding | Planned fix |
|---|---|
| Astra 10, Fable H3 (shared principals) | Done above: one token per subject, human-only approve and merge, stdio authenticated. |
| Astra 11, Fable M4 (reproducibility) | Done above: [Reproducible builds](#reproducible-builds). |
| Astra 12 (not the v1 VERIFY contract) | Done above: [Verification Sets](#verification-sets). |
| Astra 13, Fable M7 (VM allocation and ownership) | Done above: [VM ID ownership](#vm-id-ownership). Remaining: the four unverified items listed there. |
| Astra 14, Fable L5 (parity exit check) | Before M3, freeze a **parity manifest** mapping every GitHub job and trigger (including the advisory dashboard audit, `integration`, `docker` and `master-verified`) to its fleet B counterpart, with each intended difference approved by Austin. Compare exact trees, not just SHAs (GitHub CI checks out `github.sha`, a merge ref, for PRs). N = 20 consecutive changes, including at least one each of: a failing test, a setup failure, a step timeout, a superseded head, a controller restart mid-check, and a saturated CI pool. Any unexplained or unapproved mismatch blocks M4. |
| Astra 15, Fable M3 (deploys) | Done above: [Deploys](#deploys). |
| Fable M1 (state and reason overlap) | Done above: v1's `CANCELLED` and `SUPERSEDED` with reasons in place of a `killed` state, `cancelling` as a flag, check verdict separate from failure, and ladder `SUSPECT` not flagged. For deterministic tasks the `suspect` flag is heartbeat-only, because a step timeout fences at once. |
| Fable M2 (human waits failing on deadline) | Done above: `waiting(human)` never fails on time. |
| Fable M5 (imported issues worked twice) | Done above: [ownership rules](#imported-issues-are-owned-by-one-fleet-at-a-time). |
| Fable M6 (post-M4 rollback) | Done above: [Rollback](#rollback-never-deletes-data). |
| Fable M8 (agent capability) | Done above: v1's attempt capability, delivery, binding and revocation in [Tokens](#control-and-audit). |
| Fable M9 (LAN TLS trust) | Done above: [Fleet trust](#fleet-trust). |
| Fable M10 (invariant on Redis) | Done above: the invariant reads the store only. |
| Fable H6 detail (supersede churn) | Debounce: a new head supersedes queued members at once, but a running member for the previous head finishes if its first step started within the last 60 s. Its receipt is kept for the record and never gates. To be tuned from M3 data. |
| Fable L1–L4 | Review quorum and author exclusion, MCP review scope, the control log reading the store, and the narrowed fleet A reaper offer are all done above. |

## Review findings addressed

### Second round (Astra-2, Fable-2)

The second-round review files were not available on the machine that made this revision. This table maps Austin's decisions on them to sections, rather than mapping review finding numbers.

| Decision | Section |
|---|---|
| A new approval whenever the head, target or policy changes; no carry-over by patch ID | [Merge rules](#merge-rules) |
| Protected-path globs (every `conftest.py`, CI and dashboard scripts, tsconfig, Vite and lint config); a test-count floor from the target's last baseline in place of `min_collected` | [The grading driver](#the-grading-driver-is-trusted-code) |
| Intermediate bundles so an Attempt can re-check without ending; only the controller publishes | [Same-Attempt checks](#same-attempt-checks) |
| M5 retires only the runner VMs orcest's migration took over; the shared 9000–9999 fleet is Austin's call at promotion | [selfhosted-runners](#selfhosted-runners-merges-into-orcest), [Migration stages](#migration-stages) |
| Activity and Attempt aligned with v1, including `OFFERED` before claim | [v1 records](#v1-records-and-the-dashboard-view), [How an Attempt ends](#how-an-attempt-ends), [Termination](#termination-fence-first) |
| One consistent verification-set structure | [Verification Sets](#verification-sets) |
| A reserved CI share for `main` checks | [Capacity](#capacity) |
| The M4 freeze also blocks publication and human merges | [M4 handoff](#m4-single-writer-handoff), [Rollback](#rollback-never-deletes-data) |
| Fable Mediums: a path for worker-reported failures | [How an Attempt ends](#how-an-attempt-ends), [Retry policy](#retry-policy) |
| Fable Mediums: a controller deploying to itself is exempt from quarantine | [Deploys](#deploys) |
| Fable Mediums: the system rebases open changes after a fast-forward merge | [Merge rules](#merge-rules) |
| Astra Mediums: revoking a token ends its dashboard sessions | [Dashboard sign-in](#dashboard-sign-in) |
| Astra Mediums: one Redis ACL user per worker | [Fleet trust](#fleet-trust) |
| Astra Mediums: a rollback after M5 restores GitHub CI | [Rollback](#rollback-never-deletes-data) |
| Lows: Fable's wording issues | A general wording pass; see the note above about the review files. |

### First round (Astra, Fable)

| Section | Findings |
|---|---|
| [Deployment shape](#deployment-shape-on-pve-test), [VM ID ownership](#vm-id-ownership) | Astra 13; Fable M7, H5 (runner row) |
| [Fleet trust](#fleet-trust) | Astra 4; Fable C1, M9 |
| [Activity and Attempt](#ownership), [v1 records](#v1-records-and-the-dashboard-view), [retry policy](#retry-policy) | Astra 7; Fable C4, M1, M2, M10 |
| [The reaper](#the-reaper) (outage guard, proof-only failure, startup reconciliation) | Astra 5; Fable C3, L4 |
| [Termination](#termination-fence-first), [Cancellation](#cancellation-is-durable) | Astra 6; Fable M1 |
| [Control and audit](#control-and-audit), [Dashboard sign-in](#dashboard-sign-in) | Astra 10; Fable H3, H4, M8, L2, L3 |
| [Workers never push](#workers-never-push), [Change Request states](#change-request-states) | Astra 4; Fable C1 |
| [`main` is a mirror before cutover](#before-cutover-main-is-a-mirror) | Astra 9; Fable C2 |
| [Merge rules](#merge-rules) | Astra 3; Fable H1, L1 |
| [Grading driver](#the-grading-driver-is-trusted-code) | Astra 2; Fable H2 |
| [Verdict and error](#3-ci-deterministic-check-tasks), [Medium findings](#medium-findings-planned-fixes) | Astra 12; Fable M1, L5 |
| [Reproducible builds](#reproducible-builds) | Astra 11; Fable M4 |
| [Deploys](#deploys) | Astra 15; Fable M3 |
| [Capacity](#capacity) | Astra 8; Fable H6 |
| [Imported issues](#imported-issues-are-owned-by-one-fleet-at-a-time), [M4 handoff](#m4-single-writer-handoff) | Astra 9; Fable M5 |
| [selfhosted-runners](#selfhosted-runners-merges-into-orcest), [private mirror](#github-ends-up-as-a-private-mirror) | Fable H5 |
| [Rollback](#rollback-never-deletes-data) | Astra 1; Fable M6 |

## Open questions for Austin

1. Git host: Option A (bare git with orcest review) or Option B (Forgejo as git host only)?
2. Are line-anchored review comments required on day one, or are run-level and file-level comments enough to start?
3. Should fleet B use the v1 run engine from the start? This revision depends on it: Activity and Attempt, fencing, capabilities, the bundle upload and audit all come from v1. The legacy loop is no longer an option for fleet B.
4. `claude-review.yml`: add `ready_for_review` and skip drafts, as proposed under [selfhosted-runners](#selfhosted-runners-merges-into-orcest)?
5. Fleet B needs its own Grok login, a Tailscale auth key if Tailscale access is wanted, and a `github-mirror-bot` identity for publishing and, after M4, mirroring.

(Revision 1's question on token issuance is answered by [Fleet trust](#fleet-trust): bearer tokens per subject over fleet-CA TLS on pve-test; mTLS for pve.lab.)
