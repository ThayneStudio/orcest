# Reliability Milestone Deployment, Rollback, and Watch Plan

This is the release runbook for the audit and reliability milestone. It is a
deliberate maintenance-window rollout designed to preserve in-flight work and
make every rollback input available before the first production mutation.

Do not merge or deploy the release until the draft PR has the required reviews,
all required checks are green, and a staging rehearsal of this runbook has been
recorded on the PR.

For the current `pve-test` environment, follow
[`pve-test-mixed-provider-rehearsal.md`](pve-test-mixed-provider-rehearsal.md).
That staging overlay is authoritative where it differs from this generic
production runbook, particularly for commit handoff, the credential-preserving
candidate config, the retained legacy backlog, project discovery, publisher
fencing, and the no-side-effect forced rollback.

## Release invariants

- Deploy one exact, clean commit. Record it as `RELEASE_SHA`; never deploy a
  branch name, `latest`, `unknown`, or a dirty checkout.
- A fleet-managed worker consumes exactly one provider stream. The pool may
  schedule an ordered `worker_profiles` mix; fleet preflight rejects any project
  provider without a live target slot.
- Freeze new intake, but keep orchestrators running until they have consumed and
  acknowledged all worker results. Then prove queues, pending entries, active
  workers, and private credential-recovery state are empty before replacement.
- Back up the complete Redis data volume and `/opt/orcest` deployment state
  before building the candidate.
- Keep the previous orchestrator image, dashboard image, worker template, active
  template pointer, fleet config, and Redis snapshot until the watch window ends.
- Any failed gate stops the rollout. Do not compensate by increasing a threshold
  unless the incident commander documents why the baseline was invalid.

## Roles and timing

Assign one release operator and one independent observer. The observer reads
every gate output and owns the go/no-go call. Reserve a 30-minute maintenance
window, a 15-minute initial observation window, a 60-minute active watch, and a
24-hour automated observation period. Notify users that new work will pause,
while already-running work is allowed to finish.

The examples below assume:

```bash
export FLEET_CONFIG=/etc/orcest/config.yaml
export ORCHESTRATOR_HOST=orcest@ORCHESTRATOR_IP
export ORCEST_SOURCE_ROOT="$PWD"
unset ORCEST_BUILD_REVISION
export RELEASE_SHA="$(git rev-parse HEAD)"
export RELEASE_ID="$(date -u +%Y%m%dT%H%M%SZ)-${RELEASE_SHA}"
export EXPECTED_POOL_SIZE=REPLACE_WITH_CONFIGURED_POOL_SIZE
export EXPECTED_VMID_START=REPLACE_WITH_CONFIGURED_WORKER_VMID_START
export BACKUP_DIR="$(cd .. && pwd)/orcest-release-evidence/${RELEASE_ID}"
export REMOTE_BACKUP_DIR="/opt/orcest-backups/${RELEASE_ID}"
release_orcest() {
  PYTHONPATH="$ORCEST_SOURCE_ROOT/src" \
    python3 -c 'from orcest.cli import main; main()' "$@"
}
mapfile -t PROJECTS < <(PYTHONPATH="$ORCEST_SOURCE_ROOT/src" \
  python3 - "$FLEET_CONFIG" <<'PY'
import sys
from orcest.fleet.config import load_config
for project in load_config(sys.argv[1]).projects:
    print(project.name)
PY
)
test "${#PROJECTS[@]}" -gt 0
```

Run commands in the environment that owns the resource. The release checkout
and `orcest fleet ...` commands must run on the Proxmox host (or another host
with API access plus the same protected fleet config); `qm ...` commands must
run on the Proxmox host; Docker/Redis commands must run on the orchestrator
host. For the current disposable staging topology these roles are:

```text
operator workstation
  -> root@pve-test.lab.prefixa.net       (Proxmox and candidate fleet CLI)
       -> orcest@10.20.1.129             (Docker, Redis, and project stacks)
```

Do not copy a credential-bearing config into the source checkout. Stage the
candidate source separately on the Proxmox host and keep `/etc/orcest/config.yaml`
root-owned with mode `0600`.

Every post-deploy `--expected-backend` flag represents one consecutive worker
slot beginning at `EXPECTED_VMID_START`; pass them in VMID order. The candidate
2/1/1 layout is `clauder`, `codex`, `grok`, `clauder`. The health check rejects
missing, excess, unexpected, or misplaced live workers.

Replace placeholders before use. Keep secrets in existing protected env files;
never paste them into a PR, terminal transcript, or health report.

The direct health command needs the authenticated Redis password. Load it into
the operator process without printing it, verify it is nonempty, and unset it
when the watch is over:

```bash
export ORCEST_REDIS_PASSWORD="$(
  ssh "$ORCHESTRATOR_HOST" \
    'set -a; . /opt/orcest/.redis.env; printf %s "$ORCEST_REDIS_PASSWORD"'
)"
test -n "$ORCEST_REDIS_PASSWORD"
```

## 1. Preflight and staging rehearsal

Run from the exact release checkout:

```bash
set -eu
test -z "$(git status --porcelain)"
test "$(git rev-parse HEAD)" = "$RELEASE_SHA"
release_orcest revision --short
pytest -q
ruff check src tests
make test-dashboard
```

The revision command must print `RELEASE_SHA` with no `-dirty` suffix. Run the
same release through this full procedure in staging, including a forced
dashboard readiness failure and a worker-template pointer rollback. Attach the
commands, timestamps, health JSON, and recovery result to the draft PR.

Before production, verify static fleet routing without changing state:

```bash
orcest fleet status --config "$FLEET_CONFIG"
orcest fleet pool-status --config "$FLEET_CONFIG"
```

`fleet deploy`, `fleet update`, `fleet start`, and `fleet onboard` now fail
before mutation if a configured provider stream has no managed worker. Fix the
configuration; do not bypass this guard.

## 2. Capture the baseline

For every project prefix, run the read-only health checker from the candidate
checkout and save its JSON outside the checkout. The `checker_revision` gate
attests this diagnostic program; it does not attest any deployed runtime.
Runtime attestation is a separate required gate below.

Do not pass `--expected-backend` to the project-level baseline command. Capture
the old template pointer, clone one disposable probe from that exact template,
and record its installed Orcest source digest with the configured VMID/backend
layout. The retained template—not the current app image or possibly stale live
workers—is the rollback authority.

```bash
set -eu
install -d -m 0700 "$BACKUP_DIR"
sudo install -m 0600 -o "$(id -un)" -g "$(id -gn)" \
  "$FLEET_CONFIG" "$BACKUP_DIR/fleet-config.candidate.yaml"
ssh "$ORCHESTRATOR_HOST" 'sudo cat /etc/orcest/config.yaml' \
  >"$BACKUP_DIR/fleet-config.deployed-old.yaml"
chmod 0600 "$BACKUP_DIR/fleet-config.deployed-old.yaml"
sha256sum \
  "$BACKUP_DIR/fleet-config.candidate.yaml" \
  "$BACKUP_DIR/fleet-config.deployed-old.yaml" \
  >"$BACKUP_DIR/fleet-configs.sha256"
ssh "$ORCHESTRATOR_HOST" 'docker exec orcest-redis-redis-1 sh -c '\''exec redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning GET orcest:pool:current_template_vmid'\''' \
  >"$BACKUP_DIR/old-template-vmid"
grep -Eq '^[0-9]+$' "$BACKUP_DIR/old-template-vmid"
old_template="$(tr -d '\n' <"$BACKUP_DIR/old-template-vmid")"
qm config "$old_template" | grep -Eq '^template: 1$'
probe_vmid="$(pvesh get /cluster/nextid)"
case "$probe_vmid" in ''|*[!0-9]*) exit 2 ;; esac
probe_created=0
cleanup_template_probe() {
  if [ "$probe_created" -eq 1 ]; then
    qm stop "$probe_vmid" >/dev/null 2>&1 || true
    qm destroy "$probe_vmid" --purge 1 >/dev/null 2>&1 || true
  fi
}
trap cleanup_template_probe EXIT
qm clone "$old_template" "$probe_vmid" \
  --name "orcest-template-probe-${probe_vmid}" --full 1
probe_created=1
qm start "$probe_vmid"
deadline=$((SECONDS + 180))
old_source_sha=""
while [ "$SECONDS" -lt "$deadline" ]; do
  if digest_result="$(qm guest exec "$probe_vmid" -- bash -eu -o pipefail -c \
    'pkg=$(/opt/orcest/venv/bin/python -c "import pathlib,orcest; print(pathlib.Path(orcest.__file__).parent)"); test -d "$pkg"; test -n "$(find "$pkg" -type f -name "*.py" -print -quit)"; find "$pkg" -type f -name "*.py" -print0 | sort -z | xargs -0 sha256sum | sha256sum' \
    2>/dev/null)"; then
    old_source_sha="$(python3 -c \
      'import json,sys; value=json.load(sys.stdin); assert value["exitcode"] == 0; print(value.get("out-data", "").split()[0])' \
      <<<"$digest_result")"
    printf '%s\n' "$old_source_sha" | grep -Eq '^[0-9a-f]{64}$' && break
    old_source_sha=""
  fi
  sleep 3
done
test -n "$old_source_sha"
qm stop "$probe_vmid"
qm destroy "$probe_vmid" --purge 1
if qm status "$probe_vmid" >/dev/null 2>&1; then
  echo "template probe VM ${probe_vmid} survived cleanup" >&2
  exit 1
fi
probe_created=0
trap - EXIT
PYTHONPATH="$ORCEST_SOURCE_ROOT/src" python3 - \
  "$BACKUP_DIR/fleet-config.deployed-old.yaml" "$old_source_sha" \
  >"$BACKUP_DIR/old-worker-layout.json" <<'PY'
import json
import sys

from orcest.fleet.config import load_config

cfg = load_config(sys.argv[1])
source_sha = sys.argv[2]
observed = {
    f"orcest-worker-{cfg.pool.vm_id_start + index}": {
        "backend": profile.backend,
        "source_sha256": source_sha,
    }
    for index, profile in enumerate(cfg.pool.scheduled_worker_profiles())
}
print(json.dumps(observed, sort_keys=True))
PY
chmod 0600 "$BACKUP_DIR/old-worker-layout.json"
python3 - "$BACKUP_DIR/old-worker-layout.json" <<'PY'
import json
import os
import sys
import time

import redis

with open(sys.argv[1], encoding="utf-8") as handle:
    layout = json.load(handle)
expected = {}
for worker, value in layout.items():
    expected.setdefault(value["backend"], set()).add(worker)
client = redis.Redis(
    host="ORCHESTRATOR_IP",
    port=6379,
    password=os.environ["ORCEST_REDIS_PASSWORD"],
    decode_responses=True,
)
deadline = time.monotonic() + 30
while True:
    fresh = True
    for backend, workers in expected.items():
        for stream in (f"orcest:tasks:{backend}", f"orcest:tasks:issue:{backend}"):
            try:
                consumers = client.xinfo_consumers(stream, "workers")
            except redis.ResponseError:
                fresh = False
                break
            observed = {str(item["name"]): item for item in consumers}
            if set(observed) != workers or any(
                int(item["idle"]) >= 15_000 for item in observed.values()
            ):
                fresh = False
                break
        if not fresh:
            break
    if fresh:
        break
    if time.monotonic() >= deadline:
        raise SystemExit("pre-cutover worker consumers were not fresh and exact")
    time.sleep(1)
print(f"pre-cutover-consumers-ok workers={len(layout)}")
PY
for project in "${PROJECTS[@]}"; do
  release_orcest rollout-health "ORCHESTRATOR_IP:6379" \
    --prefix "$project" \
    --expected-revision "$RELEASE_SHA" \
    --expected-pool-size "$EXPECTED_POOL_SIZE" \
    --json >"$BACKUP_DIR/baseline-${project}.json"
done
```

Record these values for each project:

```bash
jq '.metrics | {
  dead_letters,
  provider_exhausted_skips,
  provider_rebake_failures,
  queue_depth,
  pending,
  pool_idle,
  pool_active
}' "$BACKUP_DIR"/baseline-*.json
```

Also record image IDs, Compose state, and service status on the orchestrator
host:

```bash
ssh "$ORCHESTRATOR_HOST" 'docker image inspect orcest:latest --format "{{.Id}}"' \
  >"$BACKUP_DIR/old-orcest-image-id"
ssh "$ORCHESTRATOR_HOST" 'docker image inspect orcest-dashboard:latest --format "{{.Id}}"' \
  >"$BACKUP_DIR/old-dashboard-image-id"
ssh "$ORCHESTRATOR_HOST" 'docker image inspect orcest:latest --format "{{index .Config.Labels \"org.opencontainers.image.revision\"}}"' \
  >"$BACKUP_DIR/old-release-sha"
```

Verify `old-template-vmid` names a live template. `old-release-sha` is
`OLD_RELEASE_SHA` when it contains a clean hexadecimal revision; otherwise
record it as unattested and use the exact old image IDs as rollback authority.

```bash
if grep -Eq '^[0-9a-f]{7,64}$' "$BACKUP_DIR/old-release-sha"; then
  export OLD_RELEASE_SHA="$(cat "$BACKUP_DIR/old-release-sha")"
else
  unset OLD_RELEASE_SHA
fi
```

## 3. Pause intake and drain without losing work

Pause the upstream automation that applies `orcest:ready` labels and hold any
new ready work. Leave the per-project orchestrators and pool manager running
while work drains. Orchestrators consume and acknowledge the result streams;
stopping them earlier can strand completed results and make this gate
impossible to satisfy.

Poll every project until the gate passes twice, one minute apart:

```bash
set -eu
for project in "${PROJECTS[@]}"; do
  release_orcest rollout-health "ORCHESTRATOR_IP:6379" \
    --prefix "$project" \
    --expected-revision "$RELEASE_SHA" \
    --expected-pool-size "$EXPECTED_POOL_SIZE" \
    --max-private-recovery 0 \
    --require-quiescent
done
```

This requires queue depth `0`, pending plus lag `0`, active pool workers `0`, and
no private credential checkpoints or recovery intents. If it does not converge,
stop and diagnose; do not destroy active workers.

`--prefix` is mandatory: project results, provider counters, and credential
recovery state are all read under it, so a missing prefix would gate on an empty
keyspace. Worker pool state is read under `--pool-prefix`, which defaults to
`--task-prefix`; pass it explicitly if the pool manager runs with a different
`ORCEST_REDIS_KEY_PREFIX`. With `--require-quiescent`, both an absent project
result stream and an absent pool keyspace are reported as inspection failures
rather than as zero work, but they are escaped differently:

- An absent **pool** keyspace is overridable with `--expected-pool-size 0`,
  which is how you assert a deliberately empty fleet (the rehearsal uses this
  after the pool is destroyed).
- An absent **project result stream** is unconditionally fatal. There is no
  flag for it, and that is deliberate: the orchestrator creates
  `{prefix}:results` with `mkstream` on its first run, so its absence means
  either the wrong `--prefix` or an orchestrator that has never started —
  neither of which should ever gate a deploy as healthy.

If a legacy homogeneous staging pool has backlog on provider streams it cannot
consume, stop this generic flow. Do not delete, trim, ACK, replay, or allow new
workers to consume those entries before approval. Use the checked
`task-streams quarantine/restore` fence and `--keep-orchestrators-paused` flow
in the `pve-test` overlay. Production cutover requires the full quiescence gate;
there is no implicit backlog exception.

Only after the second pass, stop every project orchestrator and immediately run
the same quiescence gate one more time. No task or result work may appear after
the consumers stop:

```bash
set -eu
for project in "${PROJECTS[@]}"; do
  ssh "$ORCHESTRATOR_HOST" \
    "cd /opt/orcest && docker compose -p orcest-${project} \
     --env-file .redis.env --env-file projects/${project}/.env stop orchestrator"
done
for project in "${PROJECTS[@]}"; do
  release_orcest rollout-health "ORCHESTRATOR_IP:6379" \
    --prefix "$project" \
    --expected-revision "$RELEASE_SHA" \
    --expected-pool-size "$EXPECTED_POOL_SIZE" \
    --max-private-recovery 0 \
    --require-quiescent
done
```

Then stop the pool manager and remove only idle workers:

```bash
orcest fleet stop --config "$FLEET_CONFIG" --yes
```

## 4. Create rollback artifacts

Copy the protected, already-captured rollback inputs to the orchestrator host,
then retain durable image tags and deployment state:

```bash
ssh "$ORCHESTRATOR_HOST" \
  "install -d -m 0700 /tmp/orcest-rollback-inputs-${RELEASE_ID}"
scp "$BACKUP_DIR/old-worker-layout.json" \
  "$BACKUP_DIR/fleet-config.deployed-old.yaml" \
  "$BACKUP_DIR/old-template-vmid" \
  "$ORCHESTRATOR_HOST:/tmp/orcest-rollback-inputs-${RELEASE_ID}/"
ssh "$ORCHESTRATOR_HOST"
set -euo pipefail
export RELEASE_ID=REPLACE_WITH_RELEASE_ID
case "$RELEASE_ID" in
  ''|.|..|[!A-Za-z0-9]*|*[!A-Za-z0-9._-]*) exit 2 ;;
esac
export BACKUP_DIR="/opt/orcest-backups/${RELEASE_ID}"
input_dir="/tmp/orcest-rollback-inputs-${RELEASE_ID}"
sudo install -d -m 0700 -o "$(id -un)" -g "$(id -gn)" "$BACKUP_DIR"
install -m 0600 "$input_dir/old-worker-layout.json" \
  "$BACKUP_DIR/old-worker-layout.json"
install -m 0600 "$input_dir/fleet-config.deployed-old.yaml" \
  "$BACKUP_DIR/fleet-config.deployed-old.yaml"
install -m 0600 "$input_dir/old-template-vmid" \
  "$BACKUP_DIR/old-template-vmid"
rm -f "$input_dir/old-worker-layout.json" \
  "$input_dir/fleet-config.deployed-old.yaml" \
  "$input_dir/old-template-vmid"
rmdir "$input_dir"
sudo cmp -s /etc/orcest/config.yaml "$BACKUP_DIR/fleet-config.deployed-old.yaml"
current_template_vmid="$(docker exec orcest-redis-redis-1 sh -c \
  'redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw GET orcest:pool:current_template_vmid')"
test "$current_template_vmid" = \
  "$(tr -d '\n' <"$BACKUP_DIR/old-template-vmid")"
docker tag orcest:latest "orcest:rollback-${RELEASE_ID}"
docker tag orcest-dashboard:latest "orcest-dashboard:rollback-${RELEASE_ID}"
export ROLLBACK_RUNTIME="/var/lib/orcest-rollback-runtime/${RELEASE_ID}"
rollback_uid="$(docker run --rm --entrypoint id \
  "orcest:rollback-${RELEASE_ID}" -u orcest)"
rollback_gid="$(docker run --rm --entrypoint id \
  "orcest:rollback-${RELEASE_ID}" -g orcest)"
printf '%s:%s\n' "$rollback_uid" "$rollback_gid" | \
  grep -Eq '^[1-9][0-9]*:[1-9][0-9]*$'
sudo install -d -m 0700 -o root -g root /var/lib/orcest-rollback-runtime
sudo mkdir "$ROLLBACK_RUNTIME"
sudo chown "$rollback_uid:$rollback_gid" "$ROLLBACK_RUNTIME"
sudo chmod 0700 "$ROLLBACK_RUNTIME"
sudo install -d -m 0700 -o "$rollback_uid" -g "$rollback_gid" \
  "$ROLLBACK_RUNTIME/ssh"
sudo install -m 0600 -o "$rollback_uid" -g "$rollback_gid" \
  "$BACKUP_DIR/fleet-config.deployed-old.yaml" "$ROLLBACK_RUNTIME/config.yaml"
sudo install -m 0600 -o "$rollback_uid" -g "$rollback_gid" \
  "$BACKUP_DIR/old-worker-layout.json" "$ROLLBACK_RUNTIME/old-worker-layout.json"
sudo cp -a /home/orcest/.ssh/. "$ROLLBACK_RUNTIME/ssh/"
sudo chown -R "$rollback_uid:$rollback_gid" "$ROLLBACK_RUNTIME/ssh"
sudo find "$ROLLBACK_RUNTIME/ssh" -type d -exec chmod 0700 {} +
sudo find "$ROLLBACK_RUNTIME/ssh" -type f -exec chmod go-rwx {} +
sudo ssh-keygen -y -f "$ROLLBACK_RUNTIME/ssh/id_ed25519" | \
  ssh-keygen -lf - | awk '{print $2}' >"$BACKUP_DIR/rollback-ssh-key.fingerprint"
test -s "$BACKUP_DIR/rollback-ssh-key.fingerprint"
docker run --rm --entrypoint sh \
  -v "$ROLLBACK_RUNTIME/config.yaml":/tmp/orcest-fleet-config.yaml:ro \
  -v "$ROLLBACK_RUNTIME/old-worker-layout.json":/tmp/old-worker-layout.json:ro \
  -v "$ROLLBACK_RUNTIME/ssh":/home/orcest/.ssh:ro \
  "orcest:rollback-${RELEASE_ID}" -c \
  'test -r /tmp/orcest-fleet-config.yaml && test -r /tmp/old-worker-layout.json && test -r /home/orcest/.ssh && test -x /home/orcest/.ssh && test -f /home/orcest/.ssh/id_ed25519 && test -r /home/orcest/.ssh/id_ed25519'
proxmox_host="$(docker run --rm --entrypoint python \
  -v "$ROLLBACK_RUNTIME/config.yaml":/tmp/orcest-fleet-config.yaml:ro \
  "orcest:rollback-${RELEASE_ID}" -c \
  'import urllib.parse,yaml; print(urllib.parse.urlparse(yaml.safe_load(open("/tmp/orcest-fleet-config.yaml"))["proxmox"]["endpoint"]).hostname)')"
test -n "$proxmox_host"
docker run --rm --network host --entrypoint ssh \
  -v "$ROLLBACK_RUNTIME/ssh":/home/orcest/.ssh:ro \
  "orcest:rollback-${RELEASE_ID}" -o StrictHostKeyChecking=no \
  -o BatchMode=yes -o ConnectTimeout=5 "root@${proxmox_host}" true
docker image inspect orcest:latest >"$BACKUP_DIR/orcest-image.inspect.json"
docker image inspect orcest-dashboard:latest \
  >"$BACKUP_DIR/dashboard-image.inspect.json"
grep -Eq '^[0-9]+$' "$BACKUP_DIR/old-template-vmid"
sudo /opt/orcest/venv/bin/python -c \
  'import sys,yaml; print(int((yaml.safe_load(open(sys.argv[1])) or {}).get("pool", {}).get("size", 4)))' \
  /etc/orcest/config.yaml >"$BACKUP_DIR/old-pool-size"
grep -Eq '^[0-9]+$' "$BACKUP_DIR/old-pool-size"
sudo tar -C /opt -czf "$BACKUP_DIR/orcest-deployment.tgz" orcest
sudo chown "$(id -un):$(id -gn)" \
  "$BACKUP_DIR/fleet-config.deployed-old.yaml" \
  "$BACKUP_DIR/old-template-vmid" \
  "$BACKUP_DIR/old-pool-size" \
  "$BACKUP_DIR/rollback-ssh-key.fingerprint" \
  "$BACKUP_DIR/orcest-deployment.tgz"
(cd "$BACKUP_DIR" && \
  sha256sum orcest-deployment.tgz >orcest-deployment.tgz.sha256 && \
  sha256sum \
    fleet-config.deployed-old.yaml \
    old-template-vmid \
    old-pool-size \
    old-worker-layout.json \
    rollback-ssh-key.fingerprint \
    orcest-deployment.tgz \
    orcest-image.inspect.json \
    dashboard-image.inspect.json \
    >rollback-artifacts.sha256)
exit
```

The local `fleet-config.candidate.yaml` captured in step 2 documents the desired
release state. The remote `fleet-config.deployed-old.yaml` is the authoritative
rollback configuration, including the old backend; copy it into the local
release record and restore it on both hosts during rollback.

Take a complete, consistent Redis-volume backup. Intake and workers are already
stopped, so the brief Redis stop cannot lose task progress:

```bash
ssh "$ORCHESTRATOR_HOST"
set -eu
export BACKUP_DIR="/opt/orcest-backups/REPLACE_WITH_RELEASE_ID"
cd /opt/orcest
docker compose --env-file .redis.env -f docker-compose.redis.yml -p orcest-redis stop redis
docker run --rm \
  -v orcest-redis_redis-data:/source:ro \
  -v "$BACKUP_DIR":/backup \
  alpine:3.20 sh -c 'cd /source && tar -czf /backup/redis-data.tgz .'
(cd "$BACKUP_DIR" && sha256sum redis-data.tgz >redis-data.tgz.sha256)
tar -tzf "$BACKUP_DIR/redis-data.tgz" >/dev/null
docker compose --env-file .redis.env -f docker-compose.redis.yml -p orcest-redis start redis
docker compose --env-file .redis.env -f docker-compose.redis.yml -p orcest-redis exec -T redis \
  sh -c 'redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning ping'
exit
```

Require `PONG`, confirm every archive is non-empty, and copy the remote archive,
image inspections, configuration, and checksum file into the local release
record:

```bash
for artifact in \
  fleet-config.deployed-old.yaml \
  old-template-vmid old-pool-size old-worker-layout.json \
  orcest-deployment.tgz orcest-deployment.tgz.sha256 \
  redis-data.tgz redis-data.tgz.sha256 \
  orcest-image.inspect.json dashboard-image.inspect.json \
  rollback-ssh-key.fingerprint \
  rollback-artifacts.sha256
do
  scp "$ORCHESTRATOR_HOST:$REMOTE_BACKUP_DIR/$artifact" "$BACKUP_DIR/$artifact"
done
(cd "$BACKUP_DIR" && \
  sha256sum -c rollback-artifacts.sha256 && \
  sha256sum -c orcest-deployment.tgz.sha256 && \
  sha256sum -c redis-data.tgz.sha256)
```

Do not run template garbage collection during the rollout.

## 5. Deploy the candidate

Build and hash a candidate wheel, then install it into an isolated release venv.
The production host CLI remains untouched, so application rollback does not
depend on reconstructing the old package:

```bash
set -eu
install -d -m 0700 "$BACKUP_DIR/candidate-wheel"
python3 -m pip wheel --no-deps --wheel-dir "$BACKUP_DIR/candidate-wheel" .
candidate_wheel="$(find "$BACKUP_DIR/candidate-wheel" -maxdepth 1 \
  -type f -name 'orcest-*.whl' -print -quit)"
test -n "$candidate_wheel"
sha256sum "$candidate_wheel" >"$candidate_wheel.sha256"
python3 -m venv --system-site-packages "$BACKUP_DIR/candidate-venv"
"$BACKUP_DIR/candidate-venv/bin/python" -m pip install \
  --force-reinstall --no-index --no-deps "$candidate_wheel"
candidate_package="$("$BACKUP_DIR/candidate-venv/bin/python" -c \
  'from pathlib import Path; import orcest; print(Path(orcest.__file__).resolve())')"
case "$candidate_package" in
  "$BACKUP_DIR/candidate-venv"/*) ;;
  *) echo "Candidate venv imported unexpected package: $candidate_package" >&2; exit 1 ;;
esac
candidate_orcest() {
  ORCEST_SOURCE_ROOT="$ORCEST_SOURCE_ROOT" \
    "$BACKUP_DIR/candidate-venv/bin/orcest" "$@"
}
resolved_source_revision="$(ORCEST_SOURCE_ROOT="$ORCEST_SOURCE_ROOT" \
  "$BACKUP_DIR/candidate-venv/bin/python" -c \
  'from orcest.fleet.orchestrator import _resolve_deploy_revision; print(_resolve_deploy_revision())')"
test "$resolved_source_revision" = "$RELEASE_SHA"
```

Then run the coordinated deployment. The prior quiescence gate makes
`--drain-active` non-disruptive; it also permits a backend/template transition.
The command stops all project orchestrators first, keeps them stopped while the
pool is rebuilt, waits for the exact VMID/backend/revision heartbeat layout, and
only then resumes publishers:

```bash
candidate_orcest fleet deploy \
  --rebuild-template \
  --drain-active \
  --config "$FLEET_CONFIG"
```

The deploy must reject `unknown` or `-dirty` revisions, build the orchestrator
image with the OCI revision label, bake the same revision into workers, and keep
the old template intact. Confirm the image and every project container:

```bash
ssh "$ORCHESTRATOR_HOST" 'docker image inspect orcest:latest --format "{{index .Config.Labels \"org.opencontainers.image.revision\"}}"'
for project in "${PROJECTS[@]}"; do
  ssh "$ORCHESTRATOR_HOST" \
    "cd /opt/orcest && docker compose -p orcest-${project} \
     --env-file .redis.env --env-file projects/${project}/.env \
     exec -T orchestrator orcest revision --short"
done
```

Every line must equal `RELEASE_SHA`. Use `orcest fleet pool-status` to select an
idle worker VMID, then verify it through the Proxmox guest agent before allowing
production tasks:

```bash
qm guest exec IDLE_WORKER_VMID -- /opt/orcest/venv/bin/orcest revision --short
```

The command's `out-data` must contain only `RELEASE_SHA` plus a newline.

Deploy the dashboard only from the same clean checkout. Its supported Make
targets fail on a dirty tree and bake `RELEASE_SHA` into the image. The runtime
reads an immutable image file, and `/api/ready` must return that exact value:

```bash
make deploy-dashboard-remote \
  ORCEST_BUILD_REVISION="$RELEASE_SHA" \
  DASHBOARD_REMOTE="$ORCHESTRATOR_HOST"
ssh "$ORCHESTRATOR_HOST" curl -fsS http://127.0.0.1:8080/api/ready | \
  jq -e --arg revision "$RELEASE_SHA" \
  '.ok == true and .redis_ok == true and .revision == $revision'
```

Define a gate that attests every deployed runtime. `rollout-health` is not a
substitute for this function: it proves Redis/data health and the revision of
the checker itself, while this function proves orchestrator, dashboard, and
worker revisions:

```bash
attest_release() {
  for project in "${PROJECTS[@]}"; do
    actual="$(ssh "$ORCHESTRATOR_HOST" \
      "cd /opt/orcest && docker compose -p orcest-${project} \
       --env-file .redis.env --env-file projects/${project}/.env \
       exec -T orchestrator orcest revision --short")"
    test "$actual" = "$RELEASE_SHA" || return 1
  done

  ssh "$ORCHESTRATOR_HOST" curl -fsS http://127.0.0.1:8080/api/ready | \
    jq -e --arg revision "$RELEASE_SHA" \
      '.ok == true and .redis_ok == true and .revision == $revision' >/dev/null || \
    return 1

  worker_vmids="$(
    {
      ssh "$ORCHESTRATOR_HOST" \
        "docker exec orcest-redis-redis-1 sh -c 'redis-cli -a \"\$ORCEST_REDIS_PASSWORD\" --no-auth-warning --raw SMEMBERS orcest:pool:idle'"
      ssh "$ORCHESTRATOR_HOST" \
        "docker exec orcest-redis-redis-1 sh -c 'redis-cli -a \"\$ORCEST_REDIS_PASSWORD\" --no-auth-warning --raw HKEYS orcest:pool:active'"
    } | sort -nu
  )"
  test -n "$worker_vmids" || test "$EXPECTED_POOL_SIZE" -eq 0 || return 1
  for vmid in $worker_vmids; do
    qm guest exec "$vmid" -- /opt/orcest/venv/bin/orcest revision --short | \
      jq -e --arg revision "$RELEASE_SHA" \
        '.exitcode == 0 and ."out-data" == ($revision + "\n")' >/dev/null || \
      return 1
  done
}

attest_release
```

Run the function on the Proxmox host, where `qm` is available. Treat an empty
worker list as a failure unless the configured target size is zero; the health
gate below separately requires the exact target size.

## 6. Canary and sustained-health watch

The coordinated deploy has restarted all project orchestrators, but the
upstream ready-label automation remains paused. Run sequential low-risk
canaries in a dedicated throwaway repository until the recorded provider set is
exactly `clauder`, `codex`, and `grok`. Record each task ID, source stream,
worker ID, and result status using the safe projection below. Record candidate
revision separately with `attest_release`, and bracket the canary cycle with
operator UTC timestamps. Each canary's expected GitHub side effect must contain
a unique, non-secret marker:

```bash
set -eu
date -u +%Y-%m-%dT%H:%M:%SZ >"$BACKUP_DIR/canary-started-at"
attest_release
export CANARY_PROJECT=PROJECT_A
export CLAUDER_CANARY_TASK_ID=REPLACE_WITH_PUBLISHED_TASK_UUID
export CODEX_CANARY_TASK_ID=REPLACE_WITH_PUBLISHED_TASK_UUID
export GROK_CANARY_TASK_ID=REPLACE_WITH_PUBLISHED_TASK_UUID
export CLAUDER_CANARY_MARKER=REPLACE_WITH_UNIQUE_EXPECTED_COMMENT_MARKER
export CODEX_CANARY_MARKER=REPLACE_WITH_UNIQUE_EXPECTED_COMMENT_MARKER
export GROK_CANARY_MARKER=REPLACE_WITH_UNIQUE_EXPECTED_COMMENT_MARKER
test -n "$CLAUDER_CANARY_TASK_ID"
test -n "$CODEX_CANARY_TASK_ID"
test -n "$GROK_CANARY_TASK_ID"
```

After all three complete, use the checked-in safe projector to prove each task
appeared exactly once on its expected shared provider stream and has exactly one
completed project result. It emits only provider/task/stream/worker IDs and
status; task credentials, GitHub tokens, credential updates, prompts, and
summaries are never returned:

```bash
candidate_orcest canary-evidence ORCHESTRATOR_IP:6379 \
  --prefix "$CANARY_PROJECT" \
  --canary "clauder=$CLAUDER_CANARY_TASK_ID" \
  --canary "codex=$CODEX_CANARY_TASK_ID" \
  --canary "grok=$GROK_CANARY_TASK_ID" \
  >"$BACKUP_DIR/canary-provider-evidence.json"
jq -e \
  '.ok == true and ([.canaries[].provider] | sort) == ["clauder","codex","grok"]' \
  "$BACKUP_DIR/canary-provider-evidence.json"
attest_release
date -u +%Y-%m-%dT%H:%M:%SZ >"$BACKUP_DIR/canary-completed-at"
```

Query the three canary issues/PRs through `gh api` and require exactly one
comment or other terminal side effect containing each provider's unique marker;
save the JSON as release evidence. Then rerun that project's health command with its saved baselines to
prove result PEL/lag, private checkpoints, recovery intents, DLQ, and provider
failure counters are clean. Only then release held work one project at a time:

```bash
gh api repos/OWNER/REPO/issues/NUMBER/comments --paginate --jq '.[]' \
  >"$BACKUP_DIR/canary-comments.pages.json" || exit 1
jq -s '.' "$BACKUP_DIR/canary-comments.pages.json" \
  >"$BACKUP_DIR/canary-comments.json" || exit 1
for marker in \
  "$CLAUDER_CANARY_MARKER" "$CODEX_CANARY_MARKER" "$GROK_CANARY_MARKER"
do
  jq -e --arg marker "$marker" \
    '[.[] | select(.body | contains($marker))] | length == 1' \
    "$BACKUP_DIR/canary-comments.json" || exit 1
done

baseline="$BACKUP_DIR/baseline-${CANARY_PROJECT}.json"
release_orcest rollout-health ORCHESTRATOR_IP:6379 \
  --prefix "$CANARY_PROJECT" \
  --expected-revision "$RELEASE_SHA" \
  --expected-pool-size "$EXPECTED_POOL_SIZE" \
  --expected-vmid-start "$EXPECTED_VMID_START" \
  --expected-backend clauder \
  --expected-backend codex \
  --expected-backend grok \
  --expected-backend clauder \
  --baseline-dead-letters "$(jq -r '.metrics.dead_letters' "$baseline")" \
  --baseline-exhausted-skips "$(jq -r '.metrics.provider_exhausted_skips' "$baseline")" \
  --baseline-rebake-failures "$(jq -r '.metrics.provider_rebake_failures' "$baseline")" \
  --max-private-recovery 0 || exit 1
```

The dead-letter stream is shared under the task prefix, so each per-project
sample intentionally observes the same DLQ baseline; provider counters and
result state remain project-scoped. For each project, derive the three baseline
counters from its saved JSON. Run
the following one-minute sampling loop for the 60-minute active watch; keep the
JSON so the observer can verify queue, pending, and lag trends rather than only
the final value:

```bash
set -eu
for sample in $(seq 1 60); do
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  attest_release || exit 1
  for project in "${PROJECTS[@]}"; do
    output="$BACKUP_DIR/watch-${timestamp}-${project}.json"
    baseline="$BACKUP_DIR/baseline-${project}.json"
    baseline_dlq="$(jq -r '.metrics.dead_letters' "$baseline")"
    baseline_exhausted="$(jq -r '.metrics.provider_exhausted_skips' "$baseline")"
    baseline_rebake="$(jq -r '.metrics.provider_rebake_failures' "$baseline")"
    release_orcest rollout-health ORCHESTRATOR_IP:6379 \
      --prefix "$project" \
      --expected-revision "$RELEASE_SHA" \
      --expected-pool-size "$EXPECTED_POOL_SIZE" \
      --expected-vmid-start "$EXPECTED_VMID_START" \
      --expected-backend clauder \
      --expected-backend codex \
      --expected-backend grok \
      --expected-backend clauder \
      --baseline-dead-letters "$baseline_dlq" \
      --baseline-exhausted-skips "$baseline_exhausted" \
      --baseline-rebake-failures "$baseline_rebake" \
      --max-private-recovery 0 \
      --json >"$output" || { jq . "$output"; exit 1; }
    jq '{ok, metrics: (.metrics | {
      queue_depth, pending, lag, result_work, dead_letters,
      private_credential_checkpoints, credential_recovery_intents,
      provider_exhausted_skips, provider_rebake_failures,
      pool_idle, pool_active, backend_consumers, backend_heartbeats,
      expected_backend_counts, worker_revision_mismatches, missing_worker_backends,
      unexpected_worker_backends, expected_worker_layout,
      worker_layout_mismatches, inspection_errors
    })}' "$output"
  done
  sleep 60
done
```

Also watch `orcest fleet status`, `orcest fleet pool-status`, dashboard
`/api/ready`, and orchestrator/pool/Redis logs. Roll back immediately for any of:

- a revision mismatch or `unknown`/dirty runtime revision;
- Redis unready, repeated reconnects, or an unhealthy container;
- any dead-letter, exhausted-skip, or rebake-failure increase over baseline;
- any private checkpoint or recovery intent surviving a polling interval;
- queue depth or pending/lag that grows for three consecutive samples;
- pool size not returning to target within 10 minutes;
- any expected worker slot losing its revision heartbeat or correlated PR/issue consumers;
- duplicate terminal results, credential leakage, or a stuck task.

After 15 clean minutes, re-enable the ready-label automation. After 60 clean
minutes under normal traffic, declare the rollout complete. Keep rollback
artifacts for at least the normal incident-retention period. Continue running
the same `attest_release` plus per-project JSON health sample every 15 minutes
for 24 hours, alert on any nonzero exit, and record explicit 1-hour and 24-hour
go/no-go verdicts. A scheduler must run on the Proxmox host or otherwise have
both `qm` access and the candidate checkout. Unset `ORCEST_REDIS_PASSWORD` when
the observation period ends.

## 7. Application rollback

At the first rollback trigger, freeze new intake and repeat step 3 in the same
order: keep orchestrators alive until all task and result work is acknowledged,
then stop orchestrators, recheck quiescence, and stop the pool. Do not downgrade
while a private credential checkpoint or recovery intent exists.

On the operator host, restore the captured canonical fleet configuration:

```bash
sudo install -m 0600 -o root -g root \
  "$BACKUP_DIR/fleet-config.deployed-old.yaml" "$FLEET_CONFIG"
```

On the orchestrator host, validate the deployment archive, preserve the failed
candidate tree, restore the remote pool-manager configuration, and retag the
exact retained images:

```bash
set -euo pipefail
export RELEASE_ID=REPLACE_WITH_RELEASE_ID
case "$RELEASE_ID" in
  ''|.|..|[!A-Za-z0-9]*|*[!A-Za-z0-9._-]*) exit 2 ;;
esac
export REMOTE_BACKUP_DIR="/opt/orcest-backups/${RELEASE_ID}"
cd "$REMOTE_BACKUP_DIR"
sha256sum -c rollback-artifacts.sha256
sha256sum -c orcest-deployment.tgz.sha256
tar -tzf "$REMOTE_BACKUP_DIR/orcest-deployment.tgz" >/dev/null
recorded_orcest_id="$(jq -r '.[0].Id' orcest-image.inspect.json)"
recorded_dashboard_id="$(jq -r '.[0].Id' dashboard-image.inspect.json)"
rollback_orcest_id="$(docker image inspect "orcest:rollback-${RELEASE_ID}" \
  --format '{{.Id}}')"
rollback_dashboard_id="$(docker image inspect "orcest-dashboard:rollback-${RELEASE_ID}" \
  --format '{{.Id}}')"
test "$rollback_orcest_id" = "$recorded_orcest_id"
test "$rollback_dashboard_id" = "$recorded_dashboard_id"
recorded_orcest_revision="$(jq -r \
  '.[0].Config.Labels["org.opencontainers.image.revision"] // ""' \
  orcest-image.inspect.json)"
recorded_dashboard_revision="$(jq -r \
  '.[0].Config.Labels["org.opencontainers.image.revision"] // ""' \
  dashboard-image.inspect.json)"
if printf '%s\n' "$recorded_orcest_revision" | grep -Eq '^[0-9a-f]{7,64}$'; then
  test "$(docker image inspect "orcest:rollback-${RELEASE_ID}" \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" \
    = "$recorded_orcest_revision"
fi
if printf '%s\n' "$recorded_dashboard_revision" | grep -Eq '^[0-9a-f]{7,64}$'; then
  test "$(docker image inspect "orcest-dashboard:rollback-${RELEASE_ID}" \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" \
    = "$recorded_dashboard_revision"
fi
export ROLLBACK_RUNTIME="/var/lib/orcest-rollback-runtime/${RELEASE_ID}"
sudo test -d "$ROLLBACK_RUNTIME/ssh"
sudo test ! -L "$ROLLBACK_RUNTIME"
test "$(sudo readlink -f -- "$ROLLBACK_RUNTIME")" = "$ROLLBACK_RUNTIME"
sudo cmp -s "$REMOTE_BACKUP_DIR/fleet-config.deployed-old.yaml" \
  "$ROLLBACK_RUNTIME/config.yaml"
sudo cmp -s "$REMOTE_BACKUP_DIR/old-worker-layout.json" \
  "$ROLLBACK_RUNTIME/old-worker-layout.json"
test "$(sudo ssh-keygen -y -f "$ROLLBACK_RUNTIME/ssh/id_ed25519" | \
  ssh-keygen -lf - | awk '{print $2}')" = \
  "$(cat "$REMOTE_BACKUP_DIR/rollback-ssh-key.fingerprint")"
docker run --rm --entrypoint sh \
  -v "$ROLLBACK_RUNTIME/config.yaml":/tmp/orcest-fleet-config.yaml:ro \
  -v "$ROLLBACK_RUNTIME/old-worker-layout.json":/tmp/old-worker-layout.json:ro \
  -v "$ROLLBACK_RUNTIME/ssh":/home/orcest/.ssh:ro \
  "orcest:rollback-${RELEASE_ID}" -c \
  'test -r /tmp/orcest-fleet-config.yaml && test -r /tmp/old-worker-layout.json && test -r /home/orcest/.ssh && test -x /home/orcest/.ssh && test -f /home/orcest/.ssh/id_ed25519 && test -r /home/orcest/.ssh/id_ed25519'
proxmox_host="$(docker run --rm --entrypoint python \
  -v "$ROLLBACK_RUNTIME/config.yaml":/tmp/orcest-fleet-config.yaml:ro \
  "orcest:rollback-${RELEASE_ID}" -c \
  'import urllib.parse,yaml; print(urllib.parse.urlparse(yaml.safe_load(open("/tmp/orcest-fleet-config.yaml"))["proxmox"]["endpoint"]).hostname)')"
test -n "$proxmox_host"
docker run --rm --network host --entrypoint ssh \
  -v "$ROLLBACK_RUNTIME/ssh":/home/orcest/.ssh:ro \
  "orcest:rollback-${RELEASE_ID}" -o StrictHostKeyChecking=no \
  -o BatchMode=yes -o ConnectTimeout=5 "root@${proxmox_host}" true
sudo mv /opt/orcest "/opt/orcest.failed-${RELEASE_ID}"
sudo tar -C /opt -xzf "$REMOTE_BACKUP_DIR/orcest-deployment.tgz"
sudo install -m 0600 -o root -g root \
  "$REMOTE_BACKUP_DIR/fleet-config.deployed-old.yaml" /etc/orcest/config.yaml
docker tag "orcest:rollback-${RELEASE_ID}" orcest:latest
docker tag "orcest-dashboard:rollback-${RELEASE_ID}" orcest-dashboard:latest
```

Restore the prior active template pointer using the recorded VMID, then recreate
the stopped application containers from retained images without invoking a
build. Create the pool manager in a stopped state so its image can be verified
before it can clone any worker:

```bash
set -eu
cd /opt/orcest
export OLD_TEMPLATE_VMID="$(tr -d '\n' <"$REMOTE_BACKUP_DIR/old-template-vmid")"
case "$OLD_TEMPLATE_VMID" in ""|*[!0-9]*) exit 2 ;; esac
export OLD_POOL_SIZE="$(tr -d '\n' <"$REMOTE_BACKUP_DIR/old-pool-size")"
case "$OLD_POOL_SIZE" in ""|*[!0-9]*) exit 2 ;; esac
export ROLLBACK_RUNTIME="/var/lib/orcest-rollback-runtime/${RELEASE_ID}"
export FLEET_CONFIG="$ROLLBACK_RUNTIME/config.yaml"
export SSH_KEY="$ROLLBACK_RUNTIME/ssh"
export ORCEST_FLEET_CONFIG_PATH="$ROLLBACK_RUNTIME/config.yaml"
PROJECT_NAMES="$(docker run --rm -i --entrypoint python \
  -v "$FLEET_CONFIG":/tmp/orcest-fleet-config.yaml:ro \
  orcest:latest - /tmp/orcest-fleet-config.yaml <<'PY'
import sys
import yaml
for project in (yaml.safe_load(open(sys.argv[1])) or {}).get('projects', []):
    print(project['name'])
PY
)"
test -n "$PROJECT_NAMES"
docker compose --env-file .redis.env -f docker-compose.redis.yml \
  -p orcest-redis up -d --no-build --force-recreate redis
docker exec orcest-redis-redis-1 sh -c \
  'exec redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning SET orcest:pool:current_template_vmid '"$OLD_TEMPLATE_VMID"

for project in $PROJECT_NAMES; do
  ORCEST_IMAGE=orcest:latest docker compose -p "orcest-${project}" \
    --env-file .redis.env --env-file "projects/${project}/.env" \
    create --no-build --force-recreate orchestrator
done
DASHBOARD_IMAGE=orcest-dashboard:latest docker compose \
  --env-file .redis.env --env-file .dashboard.env \
  -f docker-compose.dashboard.yml \
  up -d --no-build --force-recreate dashboard
ORCEST_IMAGE=orcest:latest docker compose \
  --env-file .redis.env -f docker-compose.pool.yml -p orcest-pool \
  create --no-build --force-recreate pool-manager
```

Verify every container uses the exact old image ID, Redis has the old template
pointer, and the dashboard is ready. Then start the already-verified pool
manager:

```bash
set -eu
export ROLLBACK_RUNTIME="/var/lib/orcest-rollback-runtime/${RELEASE_ID}"
export FLEET_CONFIG="$ROLLBACK_RUNTIME/config.yaml"
export SSH_KEY="$ROLLBACK_RUNTIME/ssh"
old_orcest_id="$(docker image inspect "orcest:rollback-${RELEASE_ID}" \
  --format '{{.Id}}')"
old_dashboard_id="$(docker image inspect "orcest-dashboard:rollback-${RELEASE_ID}" \
  --format '{{.Id}}')"
test "$old_orcest_id" = \
  "$(jq -r '.[0].Id' "$REMOTE_BACKUP_DIR/orcest-image.inspect.json")"
test "$old_dashboard_id" = \
  "$(jq -r '.[0].Id' "$REMOTE_BACKUP_DIR/dashboard-image.inspect.json")"
for project in $PROJECT_NAMES; do
  cid="$(docker compose -p "orcest-${project}" \
    --env-file .redis.env --env-file "projects/${project}/.env" \
    ps -aq orchestrator)"
  test "$(docker inspect -f '{{.Image}}' "$cid")" = "$old_orcest_id"
done
dashboard_cid="$(docker compose --env-file .redis.env --env-file .dashboard.env \
  -f docker-compose.dashboard.yml ps -q dashboard)"
test "$(docker inspect -f '{{.Image}}' "$dashboard_cid")" = "$old_dashboard_id"
pool_cid="$(docker compose --env-file .redis.env -f docker-compose.pool.yml \
  -p orcest-pool ps -aq pool-manager)"
test "$(docker inspect -f '{{.Image}}' "$pool_cid")" = "$old_orcest_id"
test "$(docker exec orcest-redis-redis-1 sh -c \
  'redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw GET orcest:pool:current_template_vmid')" \
  = "$OLD_TEMPLATE_VMID"
deadline=$((SECONDS + 120))
until curl -fsS http://127.0.0.1:8080/api/ready | jq -e \
  '.ok == true and .redis_ok == true' >/dev/null; do
  [ "$SECONDS" -lt "$deadline" ] || exit 1
  sleep 2
done
docker compose --env-file .redis.env -f docker-compose.pool.yml \
  -p orcest-pool start pool-manager
sleep 2
test "$(docker inspect -f '{{.State.Running}}' "$pool_cid")" = true
test "$(docker inspect -f '{{.RestartCount}}' "$pool_cid")" = 0
docker exec "$pool_cid" sh -c \
  'test -r /home/orcest/app/config/fleet.yaml && test -r /home/orcest/.ssh && test -x /home/orcest/.ssh && test -f /home/orcest/.ssh/id_ed25519 && test -r /home/orcest/.ssh/id_ed25519'

deadline=$((SECONDS + 600))
while [ "$SECONDS" -lt "$deadline" ]; do
  test "$(docker inspect -f '{{.State.Running}}' "$pool_cid")" = true
  test "$(docker inspect -f '{{.RestartCount}}' "$pool_cid")" = 0
  idle="$(docker exec orcest-redis-redis-1 sh -c \
    'redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw SCARD orcest:pool:idle')"
  active="$(docker exec orcest-redis-redis-1 sh -c \
    'redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw HLEN orcest:pool:active')"
  [ $((idle + active)) -eq "$OLD_POOL_SIZE" ] && break
  sleep 5
done
[ $((idle + active)) -eq "$OLD_POOL_SIZE" ] || exit 1

docker run --rm -i --network host --env-file .redis.env \
  -v "$ROLLBACK_RUNTIME/old-worker-layout.json":/tmp/old-worker-layout.json:ro \
  --entrypoint python orcest:latest - \
  /tmp/old-worker-layout.json "$OLD_POOL_SIZE" <<'PY'
import json
import os
import re
import sys

import redis

recorded_layout_path, pool_size_raw = sys.argv[1:]
pool_size = int(pool_size_raw)
with open(recorded_layout_path, encoding="utf-8") as handle:
    recorded_layout = json.load(handle)
assert len(recorded_layout) == pool_size
assert all(re.fullmatch(r"orcest-worker-[0-9]+", worker) for worker in recorded_layout)
assert all(isinstance(value.get("backend"), str) and value["backend"] for value in recorded_layout.values())
assert all(
    re.fullmatch(r"[0-9a-f]{64}", value.get("source_sha256", ""))
    for value in recorded_layout.values()
)
assert len({value["source_sha256"] for value in recorded_layout.values()}) == 1
expected_vmids = {
    worker.removeprefix("orcest-worker-") for worker in recorded_layout
}
client = redis.Redis(
    host="127.0.0.1",
    port=6379,
    password=os.environ["ORCEST_REDIS_PASSWORD"],
    decode_responses=True,
)
idle = set(client.smembers("orcest:pool:idle"))
active = set(client.hkeys("orcest:pool:active"))
assert active == set(), active
assert idle == expected_vmids, (idle, expected_vmids)
print(f"rollback-pool-membership-ok workers={len(expected_vmids)}")
PY

exit
```

On the Proxmox host, reload recorded values rather than relying on variables
from the orchestrator shell. Redis capacity is not sufficient proof: every
tracked guest must also have a running worker service and a registered consumer
on the restored backend stream.

```bash
set -eu
export OLD_POOL_SIZE="$(ssh "$ORCHESTRATOR_HOST" \
  "cat /opt/orcest-backups/${RELEASE_ID}/old-pool-size")"
rolled_back_vmids="$(
  ssh "$ORCHESTRATOR_HOST" \
    "docker exec orcest-redis-redis-1 sh -c 'redis-cli -a \"\$ORCEST_REDIS_PASSWORD\" --no-auth-warning --raw SMEMBERS orcest:pool:idle'"
)"
test "$(printf '%s\n' "$rolled_back_vmids" | sed '/^$/d' | wc -l)" \
  -eq "$OLD_POOL_SIZE"
expected_vmids="$(python3 - "$BACKUP_DIR/old-worker-layout.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    layout = json.load(handle)
for worker in sorted(layout):
    print(worker.removeprefix("orcest-worker-"))
PY
)"
test "$(printf '%s\n' "$expected_vmids" | sort -n)" = \
  "$(printf '%s\n' "$rolled_back_vmids" | sort -n)"
while read -r vmid backend expected_sha; do
  digest_result="$(qm guest exec "$vmid" -- bash -eu -o pipefail -c \
    'systemctl is-active --quiet orcest-worker; pkg=$(/opt/orcest/venv/bin/python -c "import pathlib,orcest; print(pathlib.Path(orcest.__file__).parent)"); test -d "$pkg"; test -n "$(find "$pkg" -type f -name "*.py" -print -quit)"; find "$pkg" -type f -name "*.py" -print0 | sort -z | xargs -0 sha256sum | sha256sum')"
  actual_sha="$(python3 -c \
    'import json,sys; value=json.load(sys.stdin); assert value["exitcode"] == 0; print(value.get("out-data", "").split()[0])' \
    <<<"$digest_result")"
  test "$actual_sha" = "$expected_sha"
done < <(python3 - "$BACKUP_DIR/old-worker-layout.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    layout = json.load(handle)
for worker, value in sorted(layout.items()):
    print(
        worker.removeprefix("orcest-worker-"),
        value["backend"],
        value["source_sha256"],
    )
PY
)
python3 - "$BACKUP_DIR/old-worker-layout.json" <<'PY'
import json
import os
import sys
import time

import redis

with open(sys.argv[1], encoding="utf-8") as handle:
    layout = json.load(handle)
expected = {}
for worker, value in layout.items():
    expected.setdefault(value["backend"], set()).add(worker)
client = redis.Redis(
    host="ORCHESTRATOR_IP",
    port=6379,
    password=os.environ["ORCEST_REDIS_PASSWORD"],
    decode_responses=True,
)
deadline = time.monotonic() + 30
while True:
    fresh = True
    for backend, workers in expected.items():
        for stream in (f"orcest:tasks:{backend}", f"orcest:tasks:issue:{backend}"):
            try:
                consumers = client.xinfo_consumers(stream, "workers")
            except redis.ResponseError:
                fresh = False
                break
            observed = {str(item["name"]): item for item in consumers}
            if set(observed) != workers or any(
                int(item["idle"]) >= 15_000 for item in observed.values()
            ):
                fresh = False
                break
        if not fresh:
            break
    if fresh:
        break
    if time.monotonic() >= deadline:
        raise SystemExit("rollback worker consumers were not fresh and exact")
    time.sleep(1)
print(f"rollback-consumers-ok workers={len(layout)}")
PY

# Start orchestrators remotely only after every Proxmox check passes.
for project in "${PROJECTS[@]}"; do
  ssh "$ORCHESTRATOR_HOST" \
    "cd /opt/orcest && docker compose -p orcest-${project} \
     --env-file .redis.env --env-file projects/${project}/.env start orchestrator"
done

deadline=$((SECONDS + 120))
while [ "$SECONDS" -lt "$deadline" ]; do
  all_running=1
  for project in "${PROJECTS[@]}"; do
    ssh "$ORCHESTRATOR_HOST" \
      "cd /opt/orcest && test \"\$(docker compose -p orcest-${project} \
       --env-file .redis.env --env-file projects/${project}/.env \
       ps --status running -q orchestrator | wc -l)\" -eq 1" || all_running=0
  done
  [ "$all_running" -eq 1 ] && break
  sleep 2
done
[ "$all_running" -eq 1 ]
```

The old worker pool reaches its recorded capacity and every guest worker
service is active before orchestrators resume, so rollback does not publish
into a workerless interval.

The root-shielded `$ROLLBACK_RUNTIME` contains duplicate config and SSH-key
material needed by the retained image. Keep it only while that rollback pool
or its stopped project containers may be reused. After they are retired,
validate that the path is exactly
`/var/lib/orcest-rollback-runtime/$RELEASE_ID`, remove that release-specific
directory with `sudo rm -rf -- "$ROLLBACK_RUNTIME"`, and verify it is absent.

```bash
set -euo pipefail
release_id="$RELEASE_ID"
case "$release_id" in
  ''|.|..|[!A-Za-z0-9]*|*[!A-Za-z0-9._-]*) exit 2 ;;
esac
runtime_root=/var/lib/orcest-rollback-runtime
runtime="$runtime_root/$release_id"
test "$(dirname -- "$runtime")" = "$runtime_root"
test "$(basename -- "$runtime")" = "$release_id"
sudo test -d "$runtime"
sudo test ! -L "$runtime"
test "$(sudo readlink -f -- "$runtime")" = "$runtime"
sudo rm -rf -- "$runtime"
sudo test ! -e "$runtime"
```

If an old image has a clean revision label, compare it with the recorded
`OLD_RELEASE_SHA`; for an older unlabeled image, the captured image ID is
authoritative. Never inject the candidate revision into a rollback container.

Keep the candidate checkout and CLI until rollback coordination is complete.
Use `release_orcest rollout-health --prefix "$project" --expected-revision
"$RELEASE_SHA"` with the saved baselines to check Redis/data state—the expected
value identifies the candidate checker, not the rolled-back services. Verify old runtime image IDs
and the old template pointer separately as above. The production host CLI was
never replaced; retain the isolated candidate venv through diagnosis, then
resume intake one project at a time.

For a mixed-to-homogeneous worker rollback, do not start the old pool manager
against the mixed live VMs. Keep the candidate CLI/pool manager long enough to
drain every mixed worker, restore the captured scalar `worker_backend` config
and old template pointer, and only then start the old pool manager. The old
manager counts aggregate capacity and cannot preserve a mixed layout.

## 8. Redis data rollback (last resort)

Use this only for confirmed state corruption or an incompatible Redis migration.
It discards all Redis changes since the backup and therefore requires explicit
incident-commander approval and a fresh archive of the failed state.

With intake, orchestrators, and the pool stopped, first validate and fully stage
the backup while the current Redis volume is untouched. Abort if the staging
volume name already exists or the archive cannot be inspected and extracted.
Run all three blocks below in the same orchestrator-host shell so the validated
paths and staging-volume name cannot drift between steps:

```bash
set -eu
export RELEASE_ID=REPLACE_WITH_RELEASE_ID
export BACKUP_DIR="/opt/orcest-backups/${RELEASE_ID}"
case "$RELEASE_ID" in
  ""|*[!0-9A-Za-z._-]*) exit 2 ;;
esac
restore_volume="orcest-redis-restore-${RELEASE_ID}"
cd "$BACKUP_DIR"
sha256sum -c redis-data.tgz.sha256
tar -tzf redis-data.tgz >/dev/null
if docker volume inspect "$restore_volume" >/dev/null 2>&1; then
  echo "Refusing to reuse staging volume $restore_volume" >&2
  exit 1
fi
docker volume create "$restore_volume" >/dev/null
docker run --rm \
  -v "$restore_volume":/staged \
  -v "$BACKUP_DIR":/backup:ro \
  alpine:3.20 sh -ec '
    tar -xzf /backup/redis-data.tgz -C /staged
    test -n "$(find /staged -mindepth 1 -print -quit)"
  '
```

Only after staging succeeds, stop Redis and archive the complete failed state.
The checksum and archive listing must both succeed before changing the target:

```bash
set -eu
failed_id="$(date -u +%Y%m%dT%H%M%SZ)"
docker compose --env-file /opt/orcest/.redis.env \
  -f /opt/orcest/docker-compose.redis.yml -p orcest-redis stop redis
docker run --rm \
  -v orcest-redis_redis-data:/failed:ro \
  -v "$BACKUP_DIR":/backup \
  alpine:3.20 sh -ec \
    "cd /failed && tar -czf /backup/redis-data.failed-${failed_id}.tgz ."
(cd "$BACKUP_DIR" && \
  sha256sum "redis-data.failed-${failed_id}.tgz" \
    >"redis-data.failed-${failed_id}.tgz.sha256" && \
  tar -tzf "redis-data.failed-${failed_id}.tgz" >/dev/null)
```

Now replace the target contents from the already-verified staging volume. If
this copy fails, leave Redis stopped; the failed-state archive and staging
volume are both recoverable inputs:

```bash
set -eu
docker run --rm \
  -v orcest-redis_redis-data:/target \
  -v "$restore_volume":/staged:ro \
  alpine:3.20 sh -ec '
    find /target -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    cp -a /staged/. /target/
    test -n "$(find /target -mindepth 1 -print -quit)"
    (cd /staged && find . -type f -exec sha256sum {} +) >/tmp/staged.manifest
    (cd /target && find . -type f -exec sha256sum {} +) >/tmp/target.manifest
    test -s /tmp/staged.manifest
    sort /tmp/staged.manifest >/tmp/staged.sorted
    sort /tmp/target.manifest >/tmp/target.sorted
    cmp /tmp/staged.sorted /tmp/target.sorted
  '
docker compose --env-file /opt/orcest/.redis.env \
  -f /opt/orcest/docker-compose.redis.yml -p orcest-redis start redis
```

Verify `PONG`, the old template pointer, stream/group counts, dead-letter
baseline, and private-state count before starting any application service.
Remove the staging volume only after the restored system passes verification;
retain the timestamped `redis-data.failed-*.tgz` with the incident record.

## Release evidence checklist

- [ ] Required PR reviews and all checks green
- [ ] Exact clean `RELEASE_SHA` recorded
- [ ] Staging rehearsal and forced rollback evidence attached
- [ ] Provider-stream fleet preflight passed
- [ ] Baseline health JSON saved for every project
- [ ] Quiescence passed twice, one minute apart
- [ ] Deployment/config archive and Redis-volume archive checksummed
- [ ] Prior images and prior worker template retained
- [ ] Orchestrator, worker, and dashboard revisions equal `RELEASE_SHA`
- [ ] All-provider canary cycle succeeded without counter growth or private residue
- [ ] 15-minute initial and 60-minute sustained watch passed
- [ ] 24-hour automated observation and final verdict passed
