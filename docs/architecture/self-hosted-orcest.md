# Self-Hosted Orcest

Status: **proposal, not approved.** The build is paused until Austin signs off.

This doc covers three separate pieces that let orcest run its own development loop on pve-test without GitHub in the loop:

1. **Backlog and task state** owned by orcest, in its own database.
2. **Code hosting**: a plain git host plus diff review.
3. **CI** run by orcest's own Proxmox workers.

It also covers how today's GitHub-based fleet keeps running alongside it, and the migration path.

**Placement.** Everything here runs on **pve-test** (`pve-test.lab.prefixa.net`). Nothing in this design touches **pve.lab**; promotion to pve.lab is Austin's call and his step.

This builds on Workflow Control v1 ([`docs/wiki/`](../wiki/README.md)), which is implemented but not yet adopted (epic #667). v1 already provides much of what is needed:

- the SQLite single-writer Run Store;
- forge-neutral Work Items;
- an audited Run-command endpoint;
- controller modes (`INTAKE_PAUSED`, `DISPATCH_PAUSED`, `DRAINING`);
- `VERIFY` activities on workers;
- budget reports.

Where this doc needs v1 to change, it says so. Those changes are spec amendments to the wiki, made before any code is written.

## Deployment shape on pve-test

The **self-hosted fleet** (called fleet B below) is a separate orcest install. It shares nothing with the live GitHub fleet (fleet A: VM 199, `/etc/orcest`, workers from 10000).

| Piece | Where | Notes |
|---|---|---|
| Controller (Run Store, API, MCP, dashboard, monitor), Redis, pool manager | VM 20000 | One HTTPS listener with LAN and Tailscale access only; no public tunnel. |
| Git host | VM 20100 | See [Code hosting](#2-code-hosting). |
| Workers (agent and CI) | Linked clones from 20010; templates at 20001–20009 | Named `orcest-sh-worker-<vmid>`. |
| Fleet config | `/etc/orcest-sh/config.yaml` on the host | Its own CLI venv under `/opt/orcest-sh`. `/usr/local/bin/orcest` and `/etc/orcest` are never touched. |

**Two code changes are needed before fleet B can coexist with fleet A.** Both were found by reading `bc179be`; their defaults stay as today, so fleet A is unaffected.

- **Configurable worker-name prefix.** Fleet A's startup orphan reaper and `fleet stop` destroy every VM whose name starts with `orcest-worker-`, and fleet A's open-ended `vm_id_end: 0` covers 20000+. A different prefix makes fleet B invisible to fleet A.
- **Per-fleet Terraform dir, VM name and snippet name.** `create-orchestrator` uses fixed state at `/etc/orcest/terraform`. Running it for a second fleet would replace VM 199.

Fleet B never reads GitHub issues or PRs, so the label and PR-scope changes considered earlier are not needed.

## 1. Backlog and task state

### Ownership

The backlog lives in the controller's Run Store, not in GitHub labels.

- **Backlog item.** A backlog item is a v1 **Work Item** whose source is `native`: the controller creates it and it has no forge issue behind it.
- **Run.** Admitting a backlog item creates a **Run**.
- **Task.** Each unit of work a worker executes is a **task**, which is v1's Activity and Attempt pair.

The forge-label intake path (`list_work_items_by_labels`) stays for GitHub projects. Native items reach the same snapshot and admission code through a `native` source adapter.

**v1 amendment:** add `native` as a Work Item source, with create, edit and close operations as authenticated commands.

### One task model with types

Every task has a `type`. All types share one queue, one lifecycle (the state machine below), one log store and one set of controls.

| Type | What runs | Examples |
|---|---|---|
| `agent` | A provider CLI (Claude, Codex or Grok) works a backlog item or a review follow-up. | Implement an item; address review comments; fix a failing check. |
| `deterministic` | A fixed script from a versioned pipeline file, with no AI. | A CI check, a build, a deploy. |

**What both types share:**

- **Queue and claims:** the same queue and fenced worker claims. The pool keeps a per-type capacity share, so checks never wait behind long agent tasks.
- **Lifecycle:** the same states, transitions, reaper, deadlines and terminal guarantee.
- **Failures:** the same structured `reason_code` and `reason_detail` fields.
- **Logs:** the same log stream and retention. A deterministic task logs per step, with each step's command and exit code.
- **Controls:**
  - requeue, cancel and kill. Requeueing a check re-runs it on the same commit as a new linked task.
  - Pausing a worker and draining a host apply to both types.
- **Audit:** the same audit events.

**What differs:**

| | `agent` | `deterministic` |
|---|---|---|
| Liveness | Heartbeat plus the liveness ladder (`SUSPECT`, `STUCK`, `LOOPING`). | Heartbeat plus a per-step timeout from the pipeline file. |
| Credentials | Provider credential for this task only. | None (see [section 3](#3-ci-deterministic-check-tasks)). |
| Result | A candidate commit, or a human-decision request. | Exit code only. |
| Recorded usage | Provider, CLI version, model, tokens. | Pipeline file digest, image digest, commit SHA, and per-step exit codes and durations. Model and token fields are empty. |

**Fleet view.** The dashboard lists all tasks in one view, filterable by type, repo, worker and state. The API and MCP list operations take the same four filters, as catalogue parameters.

### Task state machine

Every task is in exactly one state:

| State | Meaning |
|---|---|
| `queued` | Admitted and waiting for a worker claim. |
| `running` | Claimed by a worker that is heartbeating and making progress. |
| `waiting` | Blocked on something outside the worker: a dependency, a human decision, a budget or quota reset, or CI. |
| `stuck` | Claimed, but heartbeats are late, or there is no progress: the liveness ladder reports `SUSPECT`, `STUCK` or `LOOPING` for an agent task, or a step outlives its timeout for a deterministic task. |
| `done` | Terminal: succeeded. |
| `failed` | Terminal: did not succeed. Always carries a specific reason. |
| `killed` | Terminal: stopped by an operator or a control command. |

Allowed transitions (anything not listed is rejected by the store):

```
queued  -> running | waiting | killed | failed(admission/expired)
running -> waiting | stuck | done | failed | killed
waiting -> queued | killed | failed(deadline)
stuck   -> running (progress resumed) | failed | killed
```

**Terminal states are final.** A requeue never reopens a terminal task. It creates a new task that links back to the old one with `requeued_from`, so history is never rewritten.

**Every task reaches a terminal state.** This is a store invariant:

- `queued` and `waiting` carry a deadline. Past it, the task becomes `failed` with reason `deadline_exceeded`.
- `running` and `stuck` are bounded by the reaper below and by the absolute duration ceiling.

The store exposes a check that lists any non-terminal task with no live owner and no deadline. That list must always be empty, and the dashboard alarms if it is not.

### The reaper

The reaper is a controller loop. It is not part of any worker, so a dead worker cannot block it. It reads worker heartbeats (`workers:heartbeat:*`), the pool manager's VM inventory, and the liveness ladder's records.

| Condition | Result |
|---|---|
| The worker VM no longer exists (destroyed, crashed, or missing from Proxmox) | `failed`, reason `worker_lost`, with the VMID and last-seen time. |
| No heartbeat for longer than `heartbeat_timeout` (default 300 s) | `stuck`. If the heartbeat is still absent after `stuck_grace` (default 600 s), the task becomes `failed` with reason `heartbeat_lost`. |
| Agent task: the liveness ladder reports `LOOPING`, or `STUCK` past its kill budget | The worker is killed, then the task becomes `failed` with reason `no_progress` or `tool_loop`. |
| Deterministic task: a step exceeds its pipeline timeout | The step is killed, then the task becomes `failed` with reason `step_timeout`, naming the step. |
| The absolute duration ceiling is reached | `failed`, reason `duration_ceiling`. |

The reaper also runs once at controller startup. Every non-terminal task whose claim fence predates the restart is checked against live inventory. This closes today's gap, where 8 tasks from August and September still show as active.

**Migration of fleet A's stuck tasks.** A one-time import into fleet B is not planned. For fleet A, the same reaper rules can be offered as a fix to its legacy loop, which Austin would deploy.

### Failure reasons

A `failed` or `killed` task always has a structured `reason`. Free text never replaces it.

| Field | Contents |
|---|---|
| `reason_code` | A closed enum, for example `provider_cli_rejected`, `provider_auth_failed`, `provider_quota_exhausted`, `worker_lost`, `heartbeat_lost`, `no_progress`, `tool_loop`, `duration_ceiling`, `deadline_exceeded`, `check_failed` (with the step and exit code), `step_timeout`, `image_unavailable`, `pipeline_invalid`, `superseded`, `merge_conflict`, `clone_failed`, `operator_killed`. |
| `reason_detail` | Specific evidence, bounded and with secrets redacted. For example: `grok 0.1.216 rejected: HTTP 426 Upgrade Required (min 1.0.13)`, or the exit status plus the last classified stderr line. |
| `retryable` | Whether an automatic retry is allowed. Version and auth failures are not retryable. Quota failures wait for the reset, and only lost or late heartbeats are retried. |
| `attempts` | The list of previous attempts, each with its own `reason_code`. |

The generic text "transient, failed after 3 attempts" is no longer used. When retries run out, the task fails with the last attempt's reason, and the full list stays in `attempts`.

Classifiers map provider output to reason codes. Some already exist, such as Grok HTTP 402 balance exhaustion and `_is_usage_exhausted`. An unclassified failure becomes `unclassified`, with its detail kept, and the dashboard counts these so new classifiers get written.

### Per-task usage

Each agent task (attempt) stores the fields below. Deterministic tasks record what they ran instead (see [task types](#one-task-model-with-types)).

- **Provider and CLI:** the provider and runner (`claude`, `codex` or `grok`) and the CLI version, using the existing provider-version probe.
- **Model:** the requested model and the model the CLI reported.
- **Tokens:** approximate input, output and cache-read tokens, plus `usage_source`. `usage_source` is `reported` when the CLI's stream gives usage, or `estimated` when the count comes from bytes. The numbers are always labelled approximate.
- **Time:** wall time, and the worker VMID and template version.

The Run total is the sum of its tasks. These totals feed v1 `budget-reports`, so budgets and reporting use the same numbers.

### Control and audit

**One service layer, three transports.** Every operation is defined once, in a command catalogue with an input schema, a required scope and an audit event. The transports are:

- **HTTPS API:** `/api/v1/...`, with an OpenAPI document.
- **MCP server:** streamable HTTP on the same listener, plus stdio for local use. One tool per catalogue entry.
- **Dashboard:** it calls the API and holds no logic of its own.

A parity test fails the build if any dashboard control, API route or MCP tool is missing from the catalogue, or exists outside it.

**Operations**

| Group | Operations |
|---|---|
| Read (`read` scope) | List and get backlog items, runs, tasks (filtered by type, repo, worker and state) and workers; events; per-task usage; diffs and check results. |
| Checks (`control`, or an agent's task capability) | Run a pipeline on a commit; read its results. |
| Backlog (`control`) | Create, edit, reprioritize and close items, and change an item's state. |
| Task (`control`) | Requeue (creates a new linked task), cancel (cooperative), kill (immediate: the worker VM's process tree is killed, then the task becomes `killed`). |
| Worker (`control`) | Pause (finish the current task, then take no new claims), resume. |
| Host (`control`) | Drain: no new clones and no new claims; running tasks finish or hit their deadline. Undrain. This is per Proxmox host, alongside v1's fleet-wide `DRAINING` mode. |
| Review and merge (`control`) | See [Code hosting](#2-code-hosting). |

**v1 amendment:** the closed command set today is `CANCEL` and `RESOLVE_HUMAN_BOUNDARY`. Add `REQUEUE`, `KILL`, `PAUSE_WORKER`, `RESUME_WORKER`, `DRAIN_HOST`, `UNDRAIN_HOST`, and the backlog and review commands. `REQUEUE` is an operator command that creates a new task. It does not replace v1's rule that retry is a reducer outcome.

**Tokens and principals**

- There are two server-side principals: `reader` (`read` scope only) and `operator` (`read` and `control`).
- Tokens are random 256-bit bearers. The server stores only their hashes, and they rotate through the CLI.
- An operator's or outside agent's MCP session gets a token like any other client.
- Workers never receive `reader` or `operator` tokens. An agent task gets a capability that lasts only as long as the task. It can do two things for that task's own change: start check tasks and read their results. It cannot touch pipelines, reviews, merges or other tasks.
- Transport is HTTPS only, as v1 requires, including on the LAN.

**Audit**

Every control command emits an audit event onto the event stream and records a Run Store row. The event names:

- the principal (who), the command and the target;
- the before and after state;
- the request ID and the result;
- for denied commands, the denial. v1 already audits these.

The dashboard has a **Control log** view. The API and MCP expose the same log.

## 2. Code hosting

Orcest needs from a git host what the v1 adapter boundary lists:

- read refs and files at an exact commit;
- create a ref only if it is absent;
- update a ref with compare-and-swap;
- publish commits.

Everything else about a change is a **Change Request**: its diff, review, CI status and merge. Under this design, the Change Request is a record in orcest's Run Store.

### Option A: bare git over SSH (recommended)

- **What it is:** a small VM running `git` and `git-shell`, with SSH keys per principal. The controller has write access. Workers push only to `orcest/*` refs, enforced by a `pre-receive` hook. Only the controller's merge command can move `main`.
- **Compare-and-swap:** `git push --force-with-lease=<ref>:<expected>`, or atomic `update-ref` through a hook. Both are real compare-and-swap.
- **Review:** orcest renders the diff (`base...head`) in the dashboard, the API and MCP. A review is a control command (`approve`, `request_changes`, or a comment on a file or line), and it is audited like every other command. Merge is a control command, and it is gated on review and on CI.
- **Pros:**
  - One control plane, so no second user, permission or state model.
  - A small attack surface: no web app, no database, almost nothing to upgrade.
  - Backups are `git bundle` plus the Run Store backup v1 already specifies.
- **Cons:**
  - Orcest has to build diff review. A unified diff with file-level and run-level comments plus approve and reject is moderate work. Inline, line-anchored threads that survive force-pushes are the expensive part and can come later.
  - There is no web UI for browsing the code. `gitweb` or `cgit` in read-only mode can be added if wanted.

### Option B: Forgejo as a git host only

- **What it is:** Forgejo with issues, wiki, packages and Actions disabled. It is used for repositories, pull requests and their review UI.
- **Pros:** mature diff review out of the box, with line comments, suggestions and history; a web code browser; built-in push mirroring to GitHub.
- **Cons:**
  - The review and merge state lives in Forgejo, not orcest. That needs a Forgejo forge adapter and puts the "who approved" state outside the audit log in section 1. It works against "orcest owns state".
  - Another application to run: a database, user accounts, upgrades and CVEs.
  - Its pull request merge flow would compete with the controller's.

### Recommendation

**Option A.** Section 1 already routes every decision through orcest's audited command catalogue, and review is one more such decision. The extra work, a diff view with run-level and file-level review, is bounded, and agents get review through MCP for free.

Line-anchored comments can follow if Austin finds the first version too thin. If he wants Forgejo's review UI from day one, Option B works with the same controller, at the cost of a Forgejo adapter and the split state described above.

**Austin's decision.**

**GitHub link (both options).** During migration, merged `main` is pushed to GitHub as a branch, not as `master` (see [Migration](#coexistence-and-migration)).

## 3. CI: deterministic check tasks

There is no separate CI product. A check is a `deterministic` task (see [task types](#one-task-model-with-types)), run by orcest's own Proxmox workers. In v1 terms it is a `VERIFY` activity, already specified in [review-and-consensus.md](../wiki/review-and-consensus.md).

**Pipelines live in the repo.** They are defined in a versioned file in the repository (v1's repository configuration under `.orcest/`). Each pipeline lists:

- its trigger: Change Request head updates, `main` after a merge, or manual runs;
- its image, pinned by digest (`…@sha256:…`), never by tag;
- its steps in order, each with a command and a timeout.

Builds and deploys are pipelines too. They differ from checks only by trigger, and deploys are manual control commands.

**Triggers create check tasks automatically.**

- **Change Request:** every push to a Change Request ref, including new commits made in response to review, creates one check task per matching pipeline for the exact new head SHA. Check tasks still running for an older head are cancelled with reason `superseded`.
- **`main`:** each merge to `main` runs the `main` pipelines (the `master-verified` job moves here).

**Every run starts fresh.**

- A new linked-clone VM per task. Steps run inside the pinned image on that VM.
- The checkout is the exact commit SHA, not a branch name.
- Nothing carries over from an earlier run: no cache volume, workspace or credentials.
- The VM is destroyed afterwards.
- Outbound network reaches only the package mirrors (the Docker mirror at 10.20.1.19 and PyPI).

**Pass or fail is the exit code only.**

- All steps exit 0: the task ends `done` (pass).
- The first non-zero exit ends the task `failed`, with reason `check_failed`, the step name and the exit code.
- There is no log parsing, no AI judgement and no "allowed to fail" step.
- A failure before any step ran is not a verdict. Examples are an image that can't be pulled (`image_unavailable`), a lost VM (`worker_lost`) or an unparseable pipeline file (`pipeline_invalid`). The check shows as an error, blocks merge like a failure, and may be retried automatically, unlike a `check_failed`.

**An agent cannot grade its own pipeline change.**

- The pipeline that grades a change is always read from the target branch's commit (the merge base), never from the change's head. The run records its digest.
- An agent task can start checks on its own change and read their results, through its task capability. It cannot alter the pipeline those checks use.
- A change that edits pipeline files is graded by the old pipeline. Its new pipeline only takes effect after merge.
- The git host's `pre-receive` hook rejects pushes from an agent task's ref that touch pipeline files, unless an operator created the task as a pipeline-change task. Pipeline-change tasks need an operator's review to merge, and that review is audited.

**Merge is gated on checks.** The merge command refuses unless every required pipeline has a passing check task for the exact head SHA being merged, graded by the base branch's pipeline. A rebase or new commit invalidates earlier passes.

**Visibility.** Check tasks appear in the fleet view (filter `type=deterministic`), the API and MCP, with per-step logs. Full logs go to the v1 blob store.

## Coexistence and migration

**Fleet A keeps running unchanged on pve-test.** That means VM 199, workers from 10000, `/etc/orcest` and `/usr/local/bin/orcest`, its Saturday rebake timer, and `orcest:ready` on GitHub. Fleet B never edits fleet A's config, CLI, VMs, Redis or labels.

**Two shared resources, with their handling:**

- **Claude subscription quota** is shared, so fleet B starts with a pool of 2.
- **OAuth refresh tokens (Grok, Codex)** are not shared. Rotation by one fleet can invalidate the other's copy, so fleet B gets its own Grok login.

**Recommended to Austin, not done by fleet B:** set fleet A's `pool.vm_id_end: 19999` the next time he deploys fleet A.

**Migration stages.** Each stage has an exit check, and nothing advances on a calendar.

| Stage | What happens | Exit check |
|---|---|---|
| M0 | The two coexistence fixes (worker-name prefix, Terraform dir) and the Grok ≥ 1.0.13 pin merge to `master`. Fleet B's infrastructure is created on pve-test: controller VM, git host, templates and 2 workers. Snapshots are taken first, and fleet A's baseline is diffed after each step. | Fleet A's baseline is unchanged. Fleet B's health is green. |
| M1 | The orcest repo is imported into the git host. GitHub `master` is still canonical, and the git host fetches it on a schedule. Fleet B's merged work goes to GitHub as **draft PRs** from `selfhosted/*` branches; fleet A's deployed loop skips drafts. Austin reviews and merges them on GitHub. | Draft PRs pass GitHub CI. |
| M2 | The native backlog opens. Selected GitHub issues are imported once, with the GitHub URL kept as provenance, so there is no continuous sync. New work for fleet B is created through the API, MCP or dashboard. | Every imported item reaches a terminal state, with reasons. |
| M3 | CI parity: deterministic check tasks run alongside GitHub Actions on the same changes. | The results agree on N consecutive changes; mismatches are explained. |
| M4 | Cutover for the orcest project, by Austin's decision. The git host becomes canonical and GitHub becomes a push-mirror. Fleet A stops taking `orcest:ready` for this repo, which is a change to Austin's fleet and his step. | The mirror is current, and the rollback below has been rehearsed. |
| M5 | Promotion to pve.lab, done by Austin outside this design. | — |

**Rollback at any stage:**

- Fleet B is self-contained: destroy VMIDs 20000–20119, remove `/etc/orcest-sh` and `/opt/orcest-sh`, and remove fleet B's snippets.
- Before M4, GitHub was canonical the whole time, so nothing is lost.
- After M4, GitHub becomes canonical again by pointing the push-mirror the other way.

## Open questions for Austin

1. Git host: Option A (bare git with orcest review) or Option B (Forgejo as git host only)?
2. Are line-anchored review comments required on day one, or are run-level and file-level comments enough to start?
3. Should fleet B use the v1 run engine from the start, or the legacy loop with native intake? This doc assumes v1, which has the store, fences and audit.
4. Token issuance: static bearer tokens per principal (proposed), or the mTLS principals the v1 wiki describes for its controller endpoints?
5. Fleet B needs its own Grok login, and a Tailscale auth key if Tailscale access is wanted.
