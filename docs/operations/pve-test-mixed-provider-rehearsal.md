# pve-test Mixed-Provider Rehearsal

This is the executable staging overlay for
`reliability-milestone-rollout.md`. It is intentionally more conservative than
the production flow because `pve-test` is disposable but its configuration and
GitHub credentials are not, and its Redis currently contains legacy provider
backlog that must not execute merely because new workers become available.

The rehearsal proves backup/restore, a clean commit handoff, image/template
construction, exact `clauder,codex,grok,clauder` worker placement, pinned CLI
versions, Redis connectivity, sustained idle health, and a forced rollback. It
does **not** resume project orchestrators or execute provider tasks until a
dedicated throwaway GitHub repository is explicitly approved.

## Host boundaries and release inputs

- Workstation: checkout, tests, artifact construction, off-host evidence.
- `root@pve-test.lab.prefixa.net`: Proxmox, protected fleet config, candidate
  CLI, and every mutating `orcest fleet`/`qm` command.
- `orcest@10.20.1.129`: Docker, Redis, and the four project stacks. Reach this
  host through `pve-test` when direct routing is unavailable.

Only one operator may run mutating fleet commands. The CLI also holds
`/run/lock/orcest-fleet-operation.lock` across the complete deployment,
including nested stop/update/rebake/start steps.

On the workstation, after the candidate is committed and its draft-PR checks
are green:

```bash
set -eu
export PVE_HOST=root@pve-test.lab.prefixa.net
export RELEASE_SHA="$(git rev-parse HEAD)"
export RELEASE_ID="$(date -u +%Y%m%dT%H%M%SZ)-${RELEASE_SHA}"
export EVIDENCE_DIR="$(cd .. && pwd)/orcest-release-evidence/${RELEASE_ID}"
test -z "$(git status --porcelain)"
test "$RELEASE_SHA" = "$(git rev-parse HEAD)"
install -d -m 0700 "$EVIDENCE_DIR"
git bundle create "$EVIDENCE_DIR/orcest.bundle" HEAD
git bundle verify "$EVIDENCE_DIR/orcest.bundle"
git bundle list-heads "$EVIDENCE_DIR/orcest.bundle" | \
  awk -v sha="$RELEASE_SHA" '$1 == sha { found=1 } END { exit !found }'
printf '%s\n' "$RELEASE_SHA" >"$EVIDENCE_DIR/release.sha"
(
  cd "$EVIDENCE_DIR"
  sha256sum orcest.bundle release.sha >handoff.sha256
)
ssh "$PVE_HOST" "install -d -m 0700 /root/orcest-releases/${RELEASE_ID}/incoming"
scp "$EVIDENCE_DIR/orcest.bundle" "$EVIDENCE_DIR/release.sha" \
  "$EVIDENCE_DIR/handoff.sha256" \
  "$PVE_HOST:/root/orcest-releases/${RELEASE_ID}/incoming/"
```

On `pve-test`, verify and materialize the exact commit without using GitHub
credentials:

```bash
set -eu
export RELEASE_ID=REPLACE_WITH_RELEASE_ID
export RELEASE_ROOT="/root/orcest-releases/${RELEASE_ID}"
cd "$RELEASE_ROOT/incoming"
sha256sum -c handoff.sha256
export RELEASE_SHA="$(tr -d '\n' <release.sha)"
printf '%s\n' "$RELEASE_SHA" | grep -Eq '^[0-9a-f]{40}$'
git clone orcest.bundle "$RELEASE_ROOT/source"
git -C "$RELEASE_ROOT/source" checkout --detach "$RELEASE_SHA"
test "$(git -C "$RELEASE_ROOT/source" rev-parse HEAD)" = "$RELEASE_SHA"
test -z "$(git -C "$RELEASE_ROOT/source" status --porcelain)"
export ORCEST_SOURCE_ROOT="$RELEASE_ROOT/source"
export ORCEST_BUILD_REVISION="$RELEASE_SHA"
candidate_orcest() {
  PYTHONPATH="$ORCEST_SOURCE_ROOT/src" python3 -c \
    'from orcest.cli import main; main()' "$@"
}
test "$(candidate_orcest revision --short)" = "$RELEASE_SHA"
```

CI/workstation validation owns `pytest`, Ruff, mypy, and dashboard tests;
`pve-test` intentionally does not install those tools. The bundle checksum,
detached checkout, clean status, and revision command are its handoff gates.

## Preserve credentials and build the candidate config

Create a release-specific backup before any service or Redis mutation. This is
in addition to the verified pre-change bundle already copied off-host:

```bash
set -eu
export RELEASE_BACKUP="/root/orcest-backups/${RELEASE_ID}"
install -d -m 0700 "$RELEASE_BACKUP"
chown root:root /root/.env /root/.orcest-redis.env /etc/orcest/config.yaml
chmod 0600 /root/.env /root/.orcest-redis.env /etc/orcest/config.yaml
find /root/.env /root/.orcest-redis.env /etc/orcest/config.yaml \
  -maxdepth 0 -user root -group root -perm 0600 | grep -c . | grep -Fx 3
install -m 0600 -o root -g root /etc/orcest/config.yaml \
  "$RELEASE_BACKUP/fleet-config.deployed-old.yaml"
install -m 0600 -o root -g root /root/.env "$RELEASE_BACKUP/pve-root.env"
install -m 0600 -o root -g root /root/.orcest-redis.env \
  "$RELEASE_BACKUP/pve-redis.env"
ssh orcest@10.20.1.129 'sudo cat /etc/orcest/config.yaml' \
  >"$RELEASE_BACKUP/orchestrator-fleet-config.deployed-old.yaml"
chmod 0600 "$RELEASE_BACKUP/orchestrator-fleet-config.deployed-old.yaml"
ssh orcest@10.20.1.129 \
  "sudo tar -C /opt -czf /tmp/orcest-${RELEASE_ID}.tgz orcest && \
   sudo chown orcest:orcest /tmp/orcest-${RELEASE_ID}.tgz"
scp "orcest@10.20.1.129:/tmp/orcest-${RELEASE_ID}.tgz" \
  "$RELEASE_BACKUP/orchestrator-opt-orcest.tgz"
ssh orcest@10.20.1.129 "rm -f /tmp/orcest-${RELEASE_ID}.tgz"
qm config 199 >"$RELEASE_BACKUP/vm-199.conf"
qm list >"$RELEASE_BACKUP/qm-list.txt"
ssh orcest@10.20.1.129 \
  'docker exec orcest-redis-redis-1 sh -c '\''redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw GET orcest:pool:current_template_vmid'\''' \
  >"$RELEASE_BACKUP/old-template-vmid"
grep -Eq '^[0-9]+$' "$RELEASE_BACKUP/old-template-vmid"
qm config "$(cat "$RELEASE_BACKUP/old-template-vmid")" | \
  tee "$RELEASE_BACKUP/old-template.conf" | grep -Eq '^template: 1$'
(
  cd "$RELEASE_BACKUP"
  sha256sum fleet-config.deployed-old.yaml pve-root.env pve-redis.env \
    orchestrator-fleet-config.deployed-old.yaml orchestrator-opt-orcest.tgz \
    vm-199.conf old-template.conf qm-list.txt \
    old-template-vmid \
    >release-backup.sha256
  sha256sum -c release-backup.sha256
)
```

On the workstation, copy that directory into the protected evidence directory
and verify it before continuing:

```bash
export PVE_HOST=root@pve-test.lab.prefixa.net
export RELEASE_ID=REPLACE_WITH_RELEASE_ID
export EVIDENCE_DIR="$(cd .. && pwd)/orcest-release-evidence/${RELEASE_ID}"
install -d -m 0700 "$EVIDENCE_DIR/release-backup"
scp -rp "$PVE_HOST:/root/orcest-backups/${RELEASE_ID}/." \
  "$EVIDENCE_DIR/release-backup/"
chmod -R go-rwx "$EVIDENCE_DIR/release-backup"
test "$(stat -c '%a' "$EVIDENCE_DIR/release-backup")" = 700
for secret in \
  fleet-config.deployed-old.yaml \
  orchestrator-fleet-config.deployed-old.yaml \
  pve-root.env pve-redis.env
do
  test "$(stat -c '%a' "$EVIDENCE_DIR/release-backup/$secret")" = 600
done
(
  cd "$EVIDENCE_DIR/release-backup"
  sha256sum -c release-backup.sha256
)
```

Never attach env/config files to the PR; attach only their filenames, modes,
sizes, and hashes.

Build a separate credential-preserving candidate. The canonical
`/etc/orcest/config.yaml` stays byte-for-byte old so rollback does not depend on
reconstructing it:

```bash
set -eu
export OLD_FLEET_CONFIG="$RELEASE_BACKUP/fleet-config.deployed-old.yaml"
export CANDIDATE_FLEET_CONFIG="$RELEASE_ROOT/fleet-config.candidate.yaml"
install -m 0600 -o root -g root "$OLD_FLEET_CONFIG" "$CANDIDATE_FLEET_CONFIG"
PYTHONPATH="$ORCEST_SOURCE_ROOT/src" \
  python3 - "$OLD_FLEET_CONFIG" "$CANDIDATE_FLEET_CONFIG" <<'PY'
import os
import sys
from pathlib import Path
import yaml

old_path, candidate_path = map(Path, sys.argv[1:])
old = yaml.safe_load(old_path.read_text()) or {}
candidate = yaml.safe_load(candidate_path.read_text()) or {}
candidate.setdefault("pool", {})["worker_profiles"] = [
    {"backend": "clauder"},
    {"backend": "codex"},
    {"backend": "grok"},
    {"backend": "clauder"},
]
tmp = candidate_path.with_suffix(".tmp")
tmp.write_text(yaml.safe_dump(candidate, sort_keys=False))
os.chmod(tmp, 0o600)
os.replace(tmp, candidate_path)

# Prove the transformation changed only worker_profiles. This compares secret
# values in memory but never prints them.
rendered = yaml.safe_load(candidate_path.read_text()) or {}
expected = yaml.safe_load(old_path.read_text()) or {}
expected.setdefault("pool", {})["worker_profiles"] = candidate["pool"]["worker_profiles"]
assert rendered == expected
PY
chmod 0600 "$CANDIDATE_FLEET_CONFIG"
sha256sum "$OLD_FLEET_CONFIG" "$CANDIDATE_FLEET_CONFIG" \
  >"$RELEASE_ROOT/fleet-configs.sha256"
candidate_orcest fleet status --config "$CANDIDATE_FLEET_CONFIG"
PYTHONPATH="$ORCEST_SOURCE_ROOT/src" \
  python3 - "$CANDIDATE_FLEET_CONFIG" <<'PY'
import sys
from orcest.fleet.config import load_config
cfg = load_config(sys.argv[1])
assert cfg.provider_stream_mismatches() == {}
assert [p.backend for p in cfg.pool.scheduled_worker_profiles()] == [
    "clauder", "codex", "grok", "clauder"
]
assert cfg.pool.size == 4 and cfg.pool.vm_id_start == 10000
print("candidate-layout-ok")
PY
```

Derive the complete project set once from the protected config rather than
using placeholder names:

```bash
mapfile -t PROJECTS < <(PYTHONPATH="$ORCEST_SOURCE_ROOT/src" \
  python3 - "$CANDIDATE_FLEET_CONFIG" <<'PY'
import sys
from orcest.fleet.config import load_config
for project in load_config(sys.argv[1]).projects:
    print(project.name)
PY
)
test "${#PROJECTS[@]}" -gt 0
printf 'project=%s\n' "${PROJECTS[@]}" | tee "$RELEASE_ROOT/projects.txt"
```

## Fence the retained backlog before candidate workers exist

There is no approved throwaway canary repository in the current fleet. First
pause the upstream automation that applies `orcest:ready` labels and save its
disabled-state evidence under `$RELEASE_ROOT`. Do not continue based on a
verbal assumption: the operator and observer must both verify that new intake
is disabled.

Leave the old orchestrators and workers running until the consumable Clauder
work and every project result stream have drained. The Codex/Grok backlog is
the deliberate exception and remains untouched. This helper emits counts only:

```bash
set -a
. /root/.orcest-redis.env
set +a
drain_inventory() {
  PYTHONPATH="$ORCEST_SOURCE_ROOT/src" \
    python3 - 10.20.1.129 "${PROJECTS[@]}" <<'PY'
import json
import os
import sys

import redis

host, *projects = sys.argv[1:]
client = redis.Redis(
    host=host,
    port=6379,
    password=os.environ["ORCEST_REDIS_PASSWORD"],
    decode_responses=True,
)

def group_counts(key, expected_group):
    kind = client.type(key)
    if kind != "stream":
        raise SystemExit(f"unexpected Redis type for {key}")
    groups = client.xinfo_groups(key)
    if sorted(str(group.get("name")) for group in groups) != [expected_group]:
        raise SystemExit(f"unexpected consumer groups for {key}")
    if any(group.get("pending") is None or group.get("lag") is None for group in groups):
        raise SystemExit(f"unavailable group state for {key}")
    length = client.xlen(key)
    return {
        "key": key,
        "length": length,
        "groups": len(groups),
        "pending": sum(int(group["pending"]) for group in groups),
        # A work-bearing stream without a group is entirely unconsumed.
        "lag": (
            sum(max(int(group["lag"]), 0) for group in groups)
            if groups
            else length
        ),
    }

report = {
    "pool_active": client.hlen("orcest:pool:active"),
    "private_recovery": len(list(client.scan_iter("*:private-credential-recovery:*"))),
    "recovery_intents": len(list(client.scan_iter("*:credential-recovery-intent:*"))),
    "streams": [
        group_counts("orcest:tasks:clauder", "workers"),
        group_counts("orcest:tasks:issue:clauder", "workers"),
        *(group_counts(f"{project}:results", "orchestrator") for project in projects),
    ],
}
print(json.dumps(report, sort_keys=True))
PY
}

clean_samples=0
for attempt in $(seq 1 120); do
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  sample="$RELEASE_ROOT/drain-${timestamp}.json"
  drain_inventory >"$sample"
  if jq -e '
    .pool_active == 0 and .private_recovery == 0 and .recovery_intents == 0 and
    all(.streams[]; .pending == 0 and .lag == 0)
  ' "$sample" >/dev/null; then
    clean_samples=$((clean_samples + 1))
  else
    clean_samples=0
  fi
  [ "$clean_samples" -eq 2 ] && break
  sleep 60
done
[ "$clean_samples" -eq 2 ]
```

Only after those two clean samples, stop every project orchestrator and leave it
stopped for the rest of the rehearsal. Take one more identical clean sample to
prove no result or Clauder work appeared after the consumers stopped:

```bash
for project in "${PROJECTS[@]}"; do
  ssh orcest@10.20.1.129 \
    "cd /opt/orcest && docker compose -p orcest-${project} \
     --env-file .redis.env --env-file projects/${project}/.env stop orchestrator"
done
sleep 5
drain_inventory >"$RELEASE_ROOT/drain-after-publisher-stop.json"
jq -e '
  .pool_active == 0 and .private_recovery == 0 and .recovery_intents == 0 and
  all(.streams[]; .pending == 0 and .lag == 0)
' "$RELEASE_ROOT/drain-after-publisher-stop.json"

# This is a non-forced idle stop. Any reappearing active worker makes it fail.
candidate_orcest fleet stop --yes --config "$OLD_FLEET_CONFIG"
qm_output="$(qm list)"
test -z "$(awk '$2 ~ /^orcest-worker-/ && $2 !~ /template/ { print $1 }' \
  <<<"$qm_output")"
```

With publishers and workers stopped, create every remote rollback artifact
before quarantine or deploy. This retains the old image under an immutable tag,
records its ID, captures the old pool size, and takes a consistent Redis-volume
snapshot:

```bash
ssh orcest@10.20.1.129 bash -s -- "$RELEASE_ID" <<'SH'
set -eu
release_id="$1"
backup="/opt/orcest-backups/${release_id}"
sudo install -d -m 0700 -o orcest -g orcest "$backup"
docker tag orcest:latest "orcest:rollback-${release_id}"
docker image inspect orcest:latest >"$backup/orcest-image.inspect.json"
docker run --rm --user 0:0 --entrypoint python \
  -v /etc/orcest/config.yaml:/tmp/orcest-fleet-config.yaml:ro \
  "orcest:rollback-${release_id}" - /tmp/orcest-fleet-config.yaml \
  >"$backup/old-pool-size" <<'PY'
import sys
import yaml
config = yaml.safe_load(open(sys.argv[1])) or {}
print(int(config.get("pool", {}).get("size", 4)))
PY
grep -Eq '^[0-9]+$' "$backup/old-pool-size"
cd /opt/orcest
docker run --rm --user 0:0 --entrypoint /bin/sh "orcest:rollback-${release_id}" \
  -c 'command -v tar >/dev/null'
redis_stopped=0
restart_redis() {
  if [ "$redis_stopped" -eq 1 ]; then
    docker compose --env-file .redis.env -f docker-compose.redis.yml \
      -p orcest-redis start redis
  fi
}
trap restart_redis EXIT
docker compose --env-file .redis.env -f docker-compose.redis.yml \
  -p orcest-redis stop redis
redis_stopped=1
docker run --rm --user 0:0 \
  -v orcest-redis_redis-data:/source:ro \
  -v "$backup":/backup \
  --entrypoint /bin/sh "orcest:rollback-${release_id}" \
  -c 'cd /source && tar -czf /backup/redis-data.tgz .'
tar -tzf "$backup/redis-data.tgz" >/dev/null
docker compose --env-file .redis.env -f docker-compose.redis.yml \
  -p orcest-redis start redis
redis_stopped=0
trap - EXIT
docker compose --env-file .redis.env -f docker-compose.redis.yml \
  -p orcest-redis exec -T redis \
  sh -c 'redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning ping' | grep -Fx PONG
(
  cd "$backup"
  sha256sum orcest-image.inspect.json old-pool-size redis-data.tgz \
    >rollback-artifacts.sha256
  sha256sum -c rollback-artifacts.sha256
)
test "$(docker image inspect "orcest:rollback-${release_id}" --format '{{.Id}}')" = \
  "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[0]["Id"])' \
    "$backup/orcest-image.inspect.json")"
SH
install -d -m 0700 "$RELEASE_BACKUP/orchestrator-rollback"
scp -r "orcest@10.20.1.129:/opt/orcest-backups/${RELEASE_ID}/." \
  "$RELEASE_BACKUP/orchestrator-rollback/"
(
  cd "$RELEASE_BACKUP/orchestrator-rollback"
  sha256sum -c rollback-artifacts.sha256
)
```

On the workstation, repeat the protected `scp -rp` and `chmod -R go-rwx` block
above, then require both checksum sets:

```bash
scp -rp "$PVE_HOST:/root/orcest-backups/${RELEASE_ID}/." \
  "$EVIDENCE_DIR/release-backup/"
chmod -R go-rwx "$EVIDENCE_DIR/release-backup"
(
  cd "$EVIDENCE_DIR/release-backup"
  sha256sum -c release-backup.sha256
  cd orchestrator-rollback
  sha256sum -c rollback-artifacts.sha256
)
```

On `pve-test`, prove the old template and remote rollback image still exist
before continuing. Do not run template garbage collection during the rehearsal.

Now move every active PR/issue provider stream behind a release-specific fence.
The command renames whole Redis stream values, so entries, consumer groups, and
PEL state remain intact; its JSON projection contains counts and key names but
never task fields or credentials:

```bash
set -a
. /root/.orcest-redis.env
set +a
candidate_orcest task-streams quarantine 10.20.1.129:6379 \
  --task-prefix orcest \
  --quarantine-id "$RELEASE_ID" \
  >"$RELEASE_ROOT/quarantined-task-streams.json"
jq -e '.ok == true and .operation == "quarantine"' \
  "$RELEASE_ROOT/quarantined-task-streams.json"
jq -e '
  (.streams | length) == 6 and
  ([.streams[].source] | sort) == [
    "orcest:tasks:clauder",
    "orcest:tasks:codex",
    "orcest:tasks:grok",
    "orcest:tasks:issue:clauder",
    "orcest:tasks:issue:codex",
    "orcest:tasks:issue:grok"
  ]
' "$RELEASE_ROOT/quarantined-task-streams.json"
jq -S '[.streams[] | {source,length,groups,pending,lag}] | sort_by(.source)' \
  "$RELEASE_ROOT/quarantined-task-streams.json" \
  >"$RELEASE_ROOT/quarantined-task-stream-inventory.json"
```

If this step fails, restore/verify the Redis snapshot or use the checked
`task-streams restore` operation; do not improvise key deletion.

## Deploy and observe without GitHub side effects

Capture secret-safe DLQ and project-counter baselines while all task streams are
fenced:

```bash
for project in "${PROJECTS[@]}"; do
  candidate_orcest rollout-health 10.20.1.129:6379 \
    --prefix "$project" \
    --expected-revision "$RELEASE_SHA" \
    --max-private-recovery 0 \
    --require-quiescent \
    --json >"$RELEASE_ROOT/baseline-${project}.json"
  jq -e '.ok == true' "$RELEASE_ROOT/baseline-${project}.json"
done
```

Deploy the exact candidate while keeping all project publishers paused:

```bash
candidate_orcest fleet deploy \
  --rebuild-template \
  --drain-active \
  --keep-orchestrators-paused \
  --config "$CANDIDATE_FLEET_CONFIG"
```

Require exact consecutive VMID/backend/revision placement and exact pinned CLI
versions. Each guest command writes only service/revision/version output:

```bash
set -o pipefail
attest_worker_slots() {
  local evidence_suffix="$1"
  qm guest exec 10000 -- /bin/bash -lc \
    "systemctl is-active --quiet orcest-worker && \
     test \"\$(cat /etc/orcest/source-revision)\" = '$RELEASE_SHA' && \
     sudo -u orcest -H claude --version >/dev/null" | \
    tee "$RELEASE_ROOT/guest-${evidence_suffix}-10000.json" | \
    jq -e '.exitcode == 0' >/dev/null
  qm guest exec 10001 -- /bin/bash -lc \
    "systemctl is-active --quiet orcest-worker && \
     test \"\$(cat /etc/orcest/source-revision)\" = '$RELEASE_SHA' && \
     test \"\$(sudo -u orcest -H codex --version | awk '{print \$NF}')\" = 0.131.0" | \
    tee "$RELEASE_ROOT/guest-${evidence_suffix}-10001.json" | \
    jq -e '.exitcode == 0' >/dev/null
  qm guest exec 10002 -- /bin/bash -lc \
    "systemctl is-active --quiet orcest-worker && \
     test \"\$(cat /etc/orcest/source-revision)\" = '$RELEASE_SHA' && \
     test \"\$(sudo -u orcest -H grok --version | grep -Eo '[0-9]+\\.[0-9]+\\.[0-9]+' | head -1)\" = 0.1.216" | \
    tee "$RELEASE_ROOT/guest-${evidence_suffix}-10002.json" | \
    jq -e '.exitcode == 0' >/dev/null
  qm guest exec 10003 -- /bin/bash -lc \
    "systemctl is-active --quiet orcest-worker && \
     test \"\$(cat /etc/orcest/source-revision)\" = '$RELEASE_SHA' && \
     sudo -u orcest -H claude --version >/dev/null" | \
    tee "$RELEASE_ROOT/guest-${evidence_suffix}-10003.json" | \
    jq -e '.exitcode == 0' >/dev/null
}
attest_worker_slots initial
```

Run this artifact-producing idle watch once per minute for 15 minutes. It checks
the exact layout, worker revision, both provider consumer groups, shared DLQ,
project counters/results, private recovery state, and that publishers remain
stopped:

```bash
watch_started_epoch="$(date -u +%s)"
for sample in $(seq 0 15); do
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  attest_worker_slots "$timestamp"
  for project in "${PROJECTS[@]}"; do
    baseline="$RELEASE_ROOT/baseline-${project}.json"
    output="$RELEASE_ROOT/watch-${timestamp}-${project}.json"
    candidate_orcest rollout-health 10.20.1.129:6379 \
      --prefix "$project" \
      --expected-revision "$RELEASE_SHA" \
      --expected-pool-size 4 \
      --expected-vmid-start 10000 \
      --expected-backend clauder \
      --expected-backend codex \
      --expected-backend grok \
      --expected-backend clauder \
      --baseline-dead-letters "$(jq -r '.metrics.dead_letters' "$baseline")" \
      --baseline-exhausted-skips \
        "$(jq -r '.metrics.provider_exhausted_skips' "$baseline")" \
      --baseline-rebake-failures \
        "$(jq -r '.metrics.provider_rebake_failures' "$baseline")" \
      --max-private-recovery 0 \
      --require-quiescent \
      --json >"$output"
    jq -e '.ok == true' "$output"
    ssh orcest@10.20.1.129 \
      "set -eu; cd /opt/orcest; \
       running=\$(docker compose -p orcest-${project} \
         --env-file .redis.env --env-file projects/${project}/.env \
       ps --status running -q orchestrator); \
       test -z \"\$running\""
    printf '{"project":"%s","running":0,"timestamp":"%s"}\n' \
      "$project" "$timestamp" \
      >"$RELEASE_ROOT/publisher-${timestamp}-${project}.json"
  done
  [ "$sample" -eq 15 ] || sleep 60
done
watch_finished_epoch="$(date -u +%s)"
test $((watch_finished_epoch - watch_started_epoch)) -ge 900
```

Do not run provider canaries, restore backlog, or start orchestrators during
this rehearsal. End-to-end authorization remains a separate go/no-go gate that
requires an approved throwaway GitHub repo and three unique PR/issue task IDs.

## Forced rollback with publishers still paused

Stop candidate workers using the candidate ownership boundary. Leave task
streams quarantined while the old runtime and pool are restored:

```bash
candidate_orcest fleet stop --drain-active --yes --config "$CANDIDATE_FLEET_CONFIG"
```

On `pve-test`, restore the protected old configs and verify the old template:

```bash
(cd "$RELEASE_BACKUP" && sha256sum -c release-backup.sha256)
install -m 0600 -o root -g root \
  "$RELEASE_BACKUP/fleet-config.deployed-old.yaml" /etc/orcest/config.yaml
old_template_vmid="$(tr -d '\n' <"$RELEASE_BACKUP/old-template-vmid")"
qm config "$old_template_vmid" | grep -Eq '^template: 1$'
```

Stage the three verified rollback inputs from `pve-test` to the orchestrator,
then restore the exact deployment tree, remote fleet config, old image ID, and
template pointer. The failed candidate tree is retained for diagnosis. Project
containers are recreated but remain stopped; only Redis and the old pool
manager start:

```bash
scp "$RELEASE_BACKUP/orchestrator-opt-orcest.tgz" \
  "orcest@10.20.1.129:/tmp/orchestrator-opt-orcest-${RELEASE_ID}.tgz"
scp "$RELEASE_BACKUP/orchestrator-fleet-config.deployed-old.yaml" \
  "orcest@10.20.1.129:/tmp/orchestrator-fleet-config-${RELEASE_ID}.yaml"
scp "$RELEASE_BACKUP/old-template-vmid" \
  "orcest@10.20.1.129:/tmp/old-template-vmid-${RELEASE_ID}"
archive_sha="$(sha256sum "$RELEASE_BACKUP/orchestrator-opt-orcest.tgz" | awk '{print $1}')"
config_sha="$(sha256sum \
  "$RELEASE_BACKUP/orchestrator-fleet-config.deployed-old.yaml" | awk '{print $1}')"
template_sha="$(sha256sum "$RELEASE_BACKUP/old-template-vmid" | awk '{print $1}')"
ssh orcest@10.20.1.129 bash -s -- \
  "$RELEASE_ID" "$archive_sha" "$config_sha" "$template_sha" <<'SH'
set -eu
release_id="$1"
archive_sha="$2"
config_sha="$3"
template_sha="$4"
backup="/opt/orcest-backups/${release_id}"
archive="/tmp/orchestrator-opt-orcest-${release_id}.tgz"
old_config="/tmp/orchestrator-fleet-config-${release_id}.yaml"
old_template_file="/tmp/old-template-vmid-${release_id}"
printf '%s  %s\n' "$archive_sha" "$archive" | sha256sum -c -
printf '%s  %s\n' "$config_sha" "$old_config" | sha256sum -c -
printf '%s  %s\n' "$template_sha" "$old_template_file" | sha256sum -c -
tar -tzf "$archive" >/dev/null
cd "$backup"
sha256sum -c rollback-artifacts.sha256
recorded_image="$(python3 -c \
  'import json,sys; print(json.load(open(sys.argv[1]))[0]["Id"])' \
  orcest-image.inspect.json)"
test "$(docker image inspect "orcest:rollback-${release_id}" --format '{{.Id}}')" = \
  "$recorded_image"
test ! -e "/opt/orcest.failed-${release_id}"
sudo mv /opt/orcest "/opt/orcest.failed-${release_id}"
sudo tar -C /opt -xzf "$archive"
sudo install -m 0600 -o root -g root "$old_config" /etc/orcest/config.yaml
docker tag "orcest:rollback-${release_id}" orcest:latest
cd /opt/orcest
docker compose --env-file .redis.env -f docker-compose.redis.yml \
  -p orcest-redis up -d --no-build --force-recreate redis
deadline=$((SECONDS + 120))
while :; do
  pong="$(docker exec orcest-redis-redis-1 sh -c \
    'redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw PING' \
    2>/dev/null || true)"
  [ "$pong" = PONG ] && break
  [ "$SECONDS" -lt "$deadline" ] || exit 1
  sleep 2
done
old_template="$(tr -d '\n' <"$old_template_file")"
case "$old_template" in ''|*[!0-9]*) exit 2 ;; esac
docker exec orcest-redis-redis-1 sh -c \
  'redis-cli -e -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw SET orcest:pool:current_template_vmid '"$old_template" | \
  grep -Fx OK >/dev/null
restored_template="$(docker exec orcest-redis-redis-1 sh -c \
  'redis-cli -e -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw GET orcest:pool:current_template_vmid')"
test "$restored_template" = "$old_template"
projects="$(docker run --rm --user 0:0 --entrypoint python \
  -v /etc/orcest/config.yaml:/tmp/orcest-fleet-config.yaml:ro \
  "orcest:rollback-${release_id}" - /tmp/orcest-fleet-config.yaml <<'PY'
import sys
import yaml
for project in (yaml.safe_load(open(sys.argv[1])) or {}).get('projects', []):
    print(project['name'])
PY
)"
test -n "$projects"
for project in $projects; do
  ORCEST_IMAGE=orcest:latest docker compose -p "orcest-${project}" \
    --env-file .redis.env --env-file "projects/${project}/.env" \
    create --no-build --force-recreate orchestrator
  cid="$(docker compose -p "orcest-${project}" \
    --env-file .redis.env --env-file "projects/${project}/.env" \
    ps -aq orchestrator)"
  test "$(docker inspect -f '{{.Image}}' "$cid")" = "$recorded_image"
  test "$(docker inspect -f '{{.State.Running}}' "$cid")" = false
done
FLEET_CONFIG=/etc/orcest/config.yaml ORCEST_IMAGE=orcest:latest docker compose \
  --env-file .redis.env -f docker-compose.pool.yml -p orcest-pool \
  up -d --no-build --force-recreate pool-manager
pool_cid="$(docker compose --env-file .redis.env -f docker-compose.pool.yml \
  -p orcest-pool ps -q pool-manager)"
test "$(docker inspect -f '{{.Image}}' "$pool_cid")" = "$recorded_image"
sudo rm -f "$archive" "$old_config" "$old_template_file"
SH
```

Before any backlog is restored, verify the old pool reaches its recorded size,
all tracked VMIDs equal the old consecutive ownership slots, every guest worker
service is active, and every project orchestrator has zero running containers:

```bash
set -eu
old_pool_size="$(ssh orcest@10.20.1.129 \
  "cat /opt/orcest-backups/${RELEASE_ID}/old-pool-size")"
case "$old_pool_size" in ''|*[!0-9]*) exit 2 ;; esac
deadline=$((SECONDS + 600))
while [ "$SECONDS" -lt "$deadline" ]; do
  idle_vmids="$(ssh orcest@10.20.1.129 \
    'docker exec orcest-redis-redis-1 sh -c '\''redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw SMEMBERS orcest:pool:idle'\''')"
  active_vmids="$(ssh orcest@10.20.1.129 \
    'docker exec orcest-redis-redis-1 sh -c '\''redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw HKEYS orcest:pool:active'\''')"
  rolled_back_vmids="$(printf '%s\n' "$idle_vmids" | sed '/^$/d' | sort -nu)"
  if [ -z "$active_vmids" ] && \
    [ "$(printf '%s\n' "$rolled_back_vmids" | sed '/^$/d' | wc -l)" \
      -eq "$old_pool_size" ]; then
    break
  fi
  sleep 5
done
test -z "$active_vmids"
mapfile -t expected_vmids < <(PYTHONPATH="$ORCEST_SOURCE_ROOT/src" \
  python3 - "$OLD_FLEET_CONFIG" <<'PY'
import sys
from orcest.fleet.config import load_config
cfg = load_config(sys.argv[1])
for vmid in range(cfg.pool.vm_id_start, cfg.pool.vm_id_start + cfg.pool.size):
    print(vmid)
PY
)
test "$(printf '%s\n' "${expected_vmids[@]}" | sort -n)" = "$rolled_back_vmids"
for vmid in $rolled_back_vmids; do
  qm guest exec "$vmid" -- systemctl is-active --quiet orcest-worker | \
    jq -e '.exitcode == 0' >/dev/null
  for stream in orcest:tasks:clauder orcest:tasks:issue:clauder; do
    ssh orcest@10.20.1.129 \
      "docker exec orcest-redis-redis-1 sh -c \
       'redis-cli -a \"\$ORCEST_REDIS_PASSWORD\" --no-auth-warning --raw \
        XINFO CONSUMERS $stream workers'" | \
      grep -Fx "orcest-worker-${vmid}" >/dev/null
  done
done
for project in "${PROJECTS[@]}"; do
  ssh orcest@10.20.1.129 \
    "set -eu; cd /opt/orcest; \
     running=\$(docker compose -p orcest-${project} \
       --env-file .redis.env --env-file projects/${project}/.env \
       ps --status running -q orchestrator); \
     test -z \"\$running\""
done

# Prove rollback capacity, then remove it again before exposing retained work.
candidate_orcest fleet stop --yes --config "$OLD_FLEET_CONFIG"
qm_output="$(qm list)"
test -z "$(awk '$2 ~ /^orcest-worker-/ && $2 !~ /template/ { print $1 }' \
  <<<"$qm_output")"
```

This proves the rollback path without exposing retained tasks to a worker.

Finally restore the quarantined streams. Restore refuses to overwrite any
active work. Require the exact six-stream inventory to match byte-independent
length/group/pending/lag counts, and prove no quarantine key remains:

```bash
candidate_orcest task-streams restore 10.20.1.129:6379 \
  --task-prefix orcest \
  --quarantine-id "$RELEASE_ID" \
  >"$RELEASE_ROOT/restored-task-streams.json"
jq -e '.ok == true and .operation == "restore" and (.streams | length) == 6' \
  "$RELEASE_ROOT/restored-task-streams.json"
jq -S '[.streams[] | {source,length,groups,pending,lag}] | sort_by(.source)' \
  "$RELEASE_ROOT/restored-task-streams.json" \
  >"$RELEASE_ROOT/restored-task-stream-inventory.json"
cmp "$RELEASE_ROOT/quarantined-task-stream-inventory.json" \
  "$RELEASE_ROOT/restored-task-stream-inventory.json"
remaining_quarantine_keys="$(ssh orcest@10.20.1.129 \
  'docker exec orcest-redis-redis-1 sh -c '\''redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning --raw --scan --pattern "orcest:quarantine:'"$RELEASE_ID"':tasks:*"'\''')"
test -z "$remaining_quarantine_keys"
```

Keep every project orchestrator and upstream ready-label automation paused
after rollback, and leave the old worker pool stopped. The old homogeneous
`clauder` fleet is a known degraded state for the restored Codex/Grok streams;
this is a data-preserving rollback target, not a state in which publishers or
workers may safely resume.

Copy the secret-safe health, quarantine/restore inventory, service checks,
template pointer, artifact hashes, and timestamps off-host. Attach only those
safe projections to the draft PR. Never attach the Redis archive, configs, env
files, task payloads, prompts, results, or credential fields.
