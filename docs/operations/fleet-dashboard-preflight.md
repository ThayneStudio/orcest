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
