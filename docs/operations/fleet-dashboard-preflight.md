# Fleet dashboard rollout preflight — 2026-09-05

This is a read-only inspection record, not deployment approval or evidence that
the new dashboard is live. Recheck dynamic state immediately before a rollout.

## Observed baseline

- Proxmox: `root@pve-test.lab.prefixa.net`, hostname `pve-test`.
- Orchestrator: `orcest@10.20.1.129`, reached through Proxmox.
- Orchestrator and pool-manager image revision:
  `be9fc9921665d347f24ab84e7b7e4ae56fed528e`.
- Dashboard image revision: `0efcb8d79aaa41d0fb8e07c8a725fade5e06e73c`.
- Four project orchestrators (`orcest`, `transit-platform`, `asemly`,
  `bbr-platform`), pool manager, Redis, monitor, and dashboard reported healthy.
- Worker VMs `10000`–`10003` were running and listed idle by the pool; the sampled
  dashboard reported zero task locks and zero queued tasks. This is a momentary
  observation, not permission to destroy or restart the workers.
- Template VM `9001` was selected by the live pool.
- No inspected project configured `workflow_state_root`; no workflow-state mount
  was present on these project containers. Monitor/traces are deployed separately.
- Dashboard binds only `127.0.0.1:8080`. Its authenticated snapshot returned HTTP
  200 with no degraded sections. The live prefix allowlist is unset.
- No service, VM, configuration, queue, or credential was changed during inspection.

## Release boundary

[Dashboard PR #814](https://github.com/ThayneStudio/orcest/pull/814) is based on a
newer source revision. At candidate `4b08e98`, there are 82 commits after the live
orchestrator revision, including substantive worker, pool-manager, and workflow
changes. Do not describe deploying that branch as a dashboard-only upgrade.

A local backport assessment found that the deployed source has no
`orchestrator/issue_delivery.py`. The new dashboard's verified issue-to-PR link
hook therefore cannot simply be cherry-picked onto that version. A backport
requires its own verified publication adapter and validation; omitting the hook
would weaken the approved single-card lifecycle.

## Required gates before live changes

1. Resolve the release scope: a validated newer runtime or a separately tested
   compatibility backport. Record an exact source and image revision for each
   affected component. Keep the original prototype separate.
2. Require passing Linux lint, typecheck, unit, dashboard, and image checks on the
   chosen revision. Run integration checks and the real Redis lifecycle/sign-in
   scenario. The first PR run exposed a Redis typing mismatch; `4b08e98` fixes it
   and local verification uses the locked dependencies.
3. Validate the candidate against protected live configuration in an isolated
   environment without provider execution, GitHub mutation, or production Redis
   writes. Check legacy task/result compatibility and the actual provider mix.
4. Capture exact rollback images and protected configuration backups. Verify
   recovery commands and persistent Redis/trace storage before changing services.
5. Recheck active tasks, queue/pending entries, locks, worker liveness, and pool
   state. Use normal draining; never flush coordination state or interrupt an
   active attempt to make the rollout easier.
6. Stage component changes with explicit health checks and rollback conditions.
   Dashboard sign-in and observations alone do not validate new worker behavior.
7. Verify through the actual browser endpoint: unauthenticated denial, sign-in,
   real project/task/account/VM data, attempt output, waiting transitions, verified
   delivery, and sign-out revocation. Distinguish missing historical evidence from
   empty work. An idle snapshot cannot prove an executing-agent output path.

See [dashboard runbook](../fleet-dashboard.md) and
[reliability rollout procedure](reliability-milestone-rollout.md) for operational
commands. Credentials remain in protected server configuration, never this record.

## Staging results and recovery prerequisite

The user selected validation and staging of the newer runtime. Candidate
`4b08e9872fbcef675ae2481192fa7a061e7a11bc` passed the
[full Linux CI run](https://github.com/ThayneStudio/orcest/actions/runs/33951952173),
including integration, unit, lint, typecheck, dashboard, and Docker checks.

The candidate parsed all four protected project configurations in network-isolated,
read-only containers. Repository, Redis/task prefixes, provider selection, and
runner timeout/retry settings matched the old runtime. The candidate pool parser
accepted the existing four-slot `clauder, codex, grok, clauder` profile with no
provider-routing mismatches.

Separately tagged runtime and dashboard images are staged on the orchestrator
host. The dashboard validation container binds `127.0.0.1:8081`, with a separate
temporary access token. It passed readiness, sign-in, authenticated live Redis
reads, and sign-out revocation. It sees four workers and one pool. It reports
missing work observations explicitly because production writers are still old.
This is staging evidence, not completed live lifecycle integration.

Two staging/configuration details must be preserved at rollout:

- Extract the source archive with its original permissions (`tar -xpf` inside a
  protected staging directory). Applying umask 077 to extracted source files made
  package metadata unreadable by the non-root dashboard user; the staged image
  was rebuilt with correct source permissions.
- Set the project-prefix allowlist explicitly to
  `orcest,transit-platform,asemly,bbr-platform`. The live environment's empty value
  is rejected by the newer dashboard's fail-closed scope parser.

Read-only rollout-health checks exposed pre-existing unacknowledged results:
`orcest` had 1 (42 deliveries), `asemly` had 10 (up to 18,444 deliveries), and
`bbr-platform` had 1 (21,483 deliveries). The error was repeated removal of an
already-absent ready label: `gh: Label does not exist (HTTP 404)`. No pending result
was acknowledged or removed during inspection. These figures are snapshots and
must be refreshed before recovery.

A separate recovery commit, `b3f8109978b98e67af169a723ff91e27895aaa60`, backports the
existing label fix onto the exact deployed `be9fc99` revision and uses the newer
strict absent-label classifier. Only `orchestrator/gh.py` changes at runtime.
249 focused tests pass, including result acknowledgement/replay; lint and helper
typecheck pass. A network-isolated image check confirms the known label response
is accepted while permission, authentication, and generic 404 failures propagate.
The original installed helper's SHA-256 matched the deployed source exactly.

Recovery image `orcest:recovery-b3f8109` is built but not applied. Rendered Compose
configuration for all three affected orchestrators preserves their environment
and mounts and changes only their image. Applying it requires controlled
orchestrator restarts; workers and the pool manager must remain running. Start
with one affected project, verify its normal consumer clears the pending results,
and stop/rollback on failure. Recheck all rollout gates afterward. The live
recovery action is awaiting user approval.

Protected backups are retained on the orchestrator host at
`/opt/orcest-backups/dashboard-4b08e98-20260905`:

- `deployment.tar`: `64f9e56f216b22d7b3f1fd106eb65fcace37d9f4dde24be1cfc93802606d86a7`
- validated `redis.rdb`: `d400d2ce5ad6a9c4134e0ac902eb5eefaac8b18070673810f02815489168d9a8`

The running production images, worker VMs, pool manager, and dashboard have not
been replaced. Do not bypass the failed result-handling/quiescence gates to
complete the broader runtime rollout.
