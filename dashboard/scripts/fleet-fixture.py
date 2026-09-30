"""Isolated Redis-protocol fixture for the fleet end-to-end contract check.

Calls the real Python observation writers. No provider or GitHub requests run.
The parent process controls lifecycle steps over stdin; nothing is persisted.
"""

import json
import sys
import os
import time
from types import SimpleNamespace

import redis
from orcest.orchestrator.provider_pool import ProviderPool
from orcest.shared import work_observations as view
from orcest.shared.coordination import set_pending_task
from orcest.shared.models import Task, TaskType
from orcest.shared.providers import ProviderEntry
from orcest.shared.redis_client import RedisClient

port = int(os.environ["REDIS_PORT"])
client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
for _ in range(50):
    try:
        client.ping()
        break
    except redis.ConnectionError:
        time.sleep(0.1)
r = RedisClient.from_client(client, key_prefix="fleet-e2e")
repo = "test/fleet-contract"
account = ProviderEntry("codex", "FIXTURE_PROVIDER_SECRET", model="test-model")
pool = ProviderPool([account])
view.project_observation(r, repo, 30, pool)


def observe(number, kind, action, title):
    view.observe(
        r,
        repo,
        kind,
        SimpleNamespace(
            number=number,
            title=title,
            body="Contract fixture",
            action=SimpleNamespace(value=action),
            open_blockers=["#1"] if number == 2 else [],
        ),
    )


observe(2, "issue", "skip_dependency", "Dependency waiting")
observe(3, "issue", "enqueue_implement", "Execute and publish")
task = Task.create(
    TaskType.IMPLEMENT_ISSUE,
    repo,
    "FIXTURE_GITHUB_SECRET",
    "issue",
    3,
    "Fixture prompt",
    provider="codex",
    credential="FIXTURE_PROVIDER_SECRET",
    model="test-model",
    key_prefix=r.key_prefix,
    provider_account=account.account_key(),
)
stream = view.full_key(r, "tasks:issue:codex")
r.client.xgroup_create(stream, "workers", id="0", mkstream=True)
r.client.xadd(stream, task.to_dict())
view.queued(r, task)
print(json.dumps({"port": port, "taskId": task.id}), flush=True)
for line in sys.stdin:
    command = json.loads(line)["phase"]
    if command == "start":
        r.client.xreadgroup("workers", "vm-e2e", {stream: ">"}, count=1)
        set_pending_task(r, repo, "issue", 3, task.id)
        r.set_ex("lock:issue:test/fleet-contract:3", "vm-e2e", 120)
        r.set_ex_raw(
            "fleet-shared:workers:heartbeat:vm-e2e",
            json.dumps({"backend": "proxmox", "revision": "abc1234"}),
            150,
        )
        view.attempt_started(r, task, "vm-e2e", worker_prefix="fleet-shared")
        r.client.xadd(view.full_key(r, "output:vm-e2e"), {"type": "task_start", "task_id": task.id})
        r.client.xadd(
            view.full_key(r, "output:vm-e2e"),
            {"task_id": task.id, "line": "E2E agent output received"},
        )
    elif command == "finish":
        view.attempt_finished(r, task, "completed")
        r.delete("lock:issue:test/fleet-contract:3")
        r.client.xadd(
            view.full_key(r, "output:vm-e2e"),
            {"type": "task_end", "task_id": task.id, "status": "completed"},
        )
        observe(3, "issue", "skip_verifying", "Execute and publish")
        observe(7, "pr", "skip_pending", "Published change")
        view.link_publication(r, repo, 3, "7")
    elif command == "merge":
        view.merged(r, repo, 7)
    print(json.dumps({"phase": command}), flush=True)
r.close()
