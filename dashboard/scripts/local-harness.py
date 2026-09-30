"""Own an isolated fake fleet; drive the real writers, Redis and dashboard.

No external Redis URL, provider credentials, GitHub client, or deployment config
is accepted. Commands use a private local mailbox, never a network control API.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import secrets
import shutil
import signal
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import redis

from orcest.orchestrator.provider_pool import ProviderPool
from orcest.shared import work_observations as view
from orcest.shared.coordination import clear_pending_task, set_pending_task
from orcest.shared.models import Task, TaskType
from orcest.shared.providers import ProviderEntry
from orcest.shared.redis_client import RedisClient

DASHBOARD = Path(__file__).resolve().parents[1]
COMMANDS = (
    "pause",
    "resume",
    "step",
    "reset",
    "redis-down",
    "redis-up",
    "restart-dashboard",
    "source-down",
    "source-up",
    "stop",
)
TOKEN = "local-harness"  # Public fixture credential: synthetic data, loopback only.


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


class Fleet:
    def __init__(self, client):
        self.client = client
        self.entries = [
            ProviderEntry(p, f"FAKE_{p}_TOKEN", model=f"{p}-demo")
            for p in ("claude", "codex", "grok")
        ]
        self.pool = ProviderPool(self.entries)
        self.projects = [
            RedisClient.from_client(client, key_prefix=f"harness-{i}") for i in range(22)
        ]
        self.repos = ["demo/orcest", "demo/sparkmaw", "demo/transit"] + [
            f"demo/project-{i:02}" for i in range(4, 23)
        ]
        self.running = {}
        self.records = {}
        self.phase = 0
        self.generation = 0
        self.sequence = 0
        self.source_online = True

    def observe(self, project, number, action, title, blockers=None, kind="issue"):
        r, repo = self.projects[project], self.repos[project]
        state = SimpleNamespace(
            number=number,
            title=title,
            body="Simulated local harness work. No external actions occur.",
            action=SimpleNamespace(value=action),
            open_blockers=blockers or [],
        )
        self.records[(project, kind, number)] = state
        view.observe(r, repo, kind, state)

    def queue(self, project, number, title, provider=0):
        self.observe(project, number, "enqueue_implement", title)
        r, entry = self.projects[project], self.entries[provider]
        task = Task.create(
            TaskType.IMPLEMENT_ISSUE,
            self.repos[project],
            "FAKE_GITHUB_TOKEN",
            "issue",
            number,
            "Local simulation only",
            provider=entry.provider,
            credential=entry.credential,
            model=entry.model,
            key_prefix=r.key_prefix,
            provider_account=entry.account_key(),
        )
        stream = view.full_key(r, f"tasks:issue:{entry.provider}")
        try:
            self.client.xgroup_create(stream, "workers", id="0", mkstream=True)
        except redis.ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise
        message = self.client.xadd(stream, task.to_dict())
        view.queued(r, task)
        self.records[(project, "issue", number)].action.value = "skip_queued"
        return project, task, stream, message

    def start(self, item):
        project, task, stream, message = item
        worker = f"vm-demo-{self.generation}-{task.resource_id}"
        self.client.xreadgroup("workers", worker, {stream: ">"}, count=1)
        r = self.projects[project]
        set_pending_task(r, task.repo, "issue", task.resource_id, task.id)
        view.attempt_started(r, task, worker, worker_prefix="harness-pool")
        self.running[task.id] = (item, worker)
        self.client.xadd(
            view.full_key(r, f"output:{worker}"),
            {"type": "task_start", "task_id": task.id},
            maxlen=500,
        )
        self.refresh()

    def finish(self, item):
        project, task, stream, message = item
        _, worker = self.running.pop(task.id)
        r = self.projects[project]
        view.attempt_finished(r, task, "completed")
        clear_pending_task(r, task.repo, "issue", task.resource_id)
        r.delete(f"lock:issue:{task.repo}:{task.resource_id}")
        self.client.xack(stream, "workers", message)
        self.client.xdel(stream, message)
        self.client.delete(f"harness-pool:workers:heartbeat:{worker}")
        self.client.hdel("harness-pool:pool:active", worker)
        self.client.xadd(
            view.full_key(r, f"output:{worker}"),
            {"type": "task_end", "task_id": task.id, "status": "completed"},
            maxlen=500,
        )
        self.observe(project, task.resource_id, "skip_verifying", "Improve task history")
        self.observe(project, 101, "skip_pending", "Improve task history", kind="pr")
        view.link_publication(r, task.repo, task.resource_id, "101")

    def reset(self):
        # Only this invocation's private, authenticated Redis is reachable here.
        keys = list(self.client.scan_iter("harness-*"))
        if keys:
            self.client.delete(*keys)
        self.running.clear()
        self.records.clear()
        self.phase = 0
        self.generation += 1
        self.main = self.queue(0, 10, "Improve task history")
        self.observe(0, 11, "skip_dependency", "Add history search", ["#10"])
        self.observe(2, 30, "skip_usage_cooldown", "Handle delayed arrivals")
        self.observe(2, 31, "skip_labeled", "Restore repository access")
        self.observe(1, 40, "skip_green", "Fix project switching", kind="pr")
        view.merged(self.projects[1], self.repos[1], 40)
        self.pool.mark_account_exhausted(
            self.entries[2].account_key(), datetime.now(timezone.utc) + timedelta(minutes=5)
        )
        background = self.queue(1, 20, "Add command-menu navigation", provider=1)
        self.start(background)
        self.refresh()

    def step(self):
        if self.phase == 0:
            self.start(self.main)
        elif self.phase == 1:
            self.finish(self.main)
        elif self.phase == 2:
            view.merged(self.projects[0], self.repos[0], 101)
            self.dependent = self.queue(0, 11, "Add history search", provider=1)
        elif self.phase == 3:
            self.start(self.dependent)
            self.pool = ProviderPool(self.entries)  # Simulate a fresh successful usage probe.
            self.queue(2, 30, "Handle delayed arrivals", provider=2)
        else:
            self.reset()
            return
        self.phase += 1

    def refresh(self, *, output=False):
        if self.source_online:
            for i, r in enumerate(self.projects):
                for entry in self.entries:
                    self.pool.record_usage(
                        entry.account_key(),
                        {
                            "five_hour": {
                                "utilization": 100
                                if entry.provider == "grok" and self.phase < 4
                                else 35
                            },
                            "seven_day": {
                                "utilization": 100
                                if entry.provider == "grok" and self.phase < 4
                                else 62
                            },
                        },
                    )
                view.project_observation(r, self.repos[i], 10, self.pool)
            # Touch observation freshness without clearing queued/start/merge evidence.
            for (project, kind, number), state in self.records.items():
                self.projects[project].hset(
                    view.work_key(self.repos[project], kind, number),
                    "observed_at",
                    str(time.time()),
                )
        for item, worker in self.running.values():
            project, task, _, _ = item
            r = self.projects[project]
            r.set_ex(f"lock:issue:{task.repo}:{task.resource_id}", worker, 120)
            self.client.set(
                f"harness-pool:workers:heartbeat:{worker}",
                json.dumps({"backend": "simulated-vm", "revision": "abcdef0"}),
                ex=150,
            )
            self.client.hsetnx("harness-pool:pool:active", worker, str(time.time()))
            if output:
                self.sequence += 1
                line = [
                    "Inspecting project conventions",
                    "Editing the implementation",
                    "Running focused tests",
                    "Checking the change",
                ][self.sequence % 4]
                self.client.xadd(
                    view.full_key(r, f"output:{worker}"),
                    {"task_id": task.id, "line": f"[simulation {self.sequence}] {line}"},
                    maxlen=500,
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir", type=Path, default=Path.home() / ".local/state/orcest/dashboard-harness"
    )
    parser.add_argument("--port", type=int, default=4318)
    parser.add_argument("--interval", type=float, default=25)
    parser.add_argument("--paused", action="store_true")
    parser.add_argument("--command", choices=COMMANDS)
    args = parser.parse_args()
    root = args.state_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    commands = root / "commands"
    commands.mkdir(exist_ok=True, mode=0o700)
    lock = (root / "owner.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        owns = True
    except BlockingIOError:
        owns = False
    if args.command:
        if owns:
            raise SystemExit("No harness is running in that state directory.")
        atomic_json(commands / f"{time.time_ns()}.json", {"command": args.command})
        print("Queued:", args.command)
        return
    if not owns:
        raise SystemExit("A harness already owns this state directory.")
    if not math.isfinite(args.interval) or args.interval < 1 or not 0 <= args.port <= 65535:
        raise SystemExit("Interval must be finite and >= 1 second; port must be 0..65535.")
    if not (DASHBOARD / "build/server/index.js").exists():
        raise SystemExit("Build first: cd dashboard && npm ci && npm run build")
    node = shutil.which("node")
    redis_binary = shutil.which(os.environ.get("ORCEST_TEST_REDIS_SERVER", "redis-server"))
    if not node or not redis_binary:
        raise SystemExit(
            "Node and redis-server must be installed; see docs/dashboard-local-harness.md"
        )
    subprocess.run([node, "scripts/check-node-version.mjs"], cwd=DASHBOARD, check=True)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", args.port))
        port = probe.getsockname()[1]
    redis_port, password = free_port(), secrets.token_hex(24)
    data_dir = root / "redis"
    data_dir.mkdir(exist_ok=True, mode=0o700)
    config = root / "redis.conf"
    config.write_text(
        f"bind 127.0.0.1\nport {redis_port}\nrequirepass {password}\n"
        'save ""\nappendonly yes\nappendfsync everysec\n'
    )
    config.chmod(0o600)
    # Stale commands from a crashed run must not affect a new invocation.
    for old in commands.glob("*.json"):
        old.unlink()
    processes = {"redis": None, "dashboard": None}
    log = (root / "processes.log").open("a")
    client = redis.Redis(
        host="127.0.0.1",
        port=redis_port,
        password=password,
        decode_responses=True,
        socket_timeout=2,
        socket_connect_timeout=2,
    )

    def start_redis():
        processes["redis"] = subprocess.Popen(
            [redis_binary, str(config)], cwd=data_dir, stdout=log, stderr=log
        )
        for _ in range(100):
            if processes["redis"].poll() is not None:
                raise RuntimeError("Owned Redis failed to start; inspect processes.log")
            try:
                if client.ping():
                    return
            except redis.ConnectionError:
                time.sleep(0.1)
        raise RuntimeError("Owned Redis startup timed out")

    def start_dashboard():
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(root),
            "REDIS_HOST": "127.0.0.1",
            "REDIS_PORT": str(redis_port),
            "REDIS_PASSWORD": password,
            "DASHBOARD_TOKEN": TOKEN,
            "ORCEST_DASHBOARD_MODE": "local-harness",
            "DASHBOARD_REDIS_PREFIXES": ",".join(
                [f"harness-{i}" for i in range(22)] + ["harness-pool"]
            ),
        }
        code = (
            "import {createDashboardServer} from './build/server/index.js';"
            f"const app=createDashboardServer({{port:{port}}});"
            f"app.server.listen({port},'127.0.0.1');"
            "process.on('SIGTERM',app.shutdown);"
        )
        processes["dashboard"] = subprocess.Popen(
            [node, "--input-type=module", "-e", code],
            cwd=DASHBOARD,
            env=env,
            stdout=log,
            stderr=log,
        )

    running = True

    def terminate(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        start_redis()
        fleet = Fleet(client)
        fleet.reset()
        start_dashboard()
        paused, offline, next_step, next_output = (
            args.paused,
            False,
            time.monotonic() + args.interval,
            0,
        )
        print(f"Local harness: http://localhost:{port}/sign-in | access token: {TOKEN}", flush=True)
        next_status = 0
        last_command = None
        while running:
            for command_file in sorted(commands.glob("*.json")):
                command = json.loads(command_file.read_text())["command"]
                command_file.unlink()
                last_command = command
                next_status = 0
                if command == "stop":
                    running = False
                elif command in ("pause", "resume"):
                    paused = command == "pause"
                elif command == "redis-down":
                    stop(processes["redis"])
                    offline = True
                elif command == "redis-up" and offline:
                    start_redis()
                    offline = False
                elif command in ("source-down", "source-up"):
                    fleet.source_online = command == "source-up"
                elif command == "restart-dashboard":
                    stop(processes["dashboard"])
                    start_dashboard()
                elif command in ("step", "reset") and not offline:
                    getattr(fleet, command)()
                next_step = time.monotonic() + args.interval
            if processes["dashboard"].poll() is not None:
                raise RuntimeError("Dashboard exited; inspect processes.log")
            if not offline:
                if processes["redis"].poll() is not None:
                    raise RuntimeError("Redis exited unexpectedly")
                if not paused and fleet.source_online and time.monotonic() >= next_step:
                    fleet.step()
                    next_step = time.monotonic() + args.interval
                if time.monotonic() >= next_output:
                    fleet.refresh(output=not paused)
                    next_output = time.monotonic() + 2
            if time.monotonic() >= next_status:
                atomic_json(
                    root / "status.json",
                    {
                        "url": f"http://localhost:{port}",
                        "pid": os.getpid(),
                        "redisPid": processes["redis"].pid,
                        "dashboardPid": processes["dashboard"].pid,
                        "phase": fleet.phase,
                        "paused": paused,
                        "redisOffline": offline,
                        "sourceOnline": fleet.source_online,
                        "updatedAt": time.time(),
                        "lastCommand": last_command,
                        "synthetic": True,
                    },
                )
                next_status = time.monotonic() + 2
            time.sleep(0.2)
    finally:
        for proc in reversed(list(processes.values())):
            stop(proc)
        client.close()
        log.close()
        atomic_json(
            root / "status.json", {"stopped": True, "synthetic": True, "updatedAt": time.time()}
        )


if __name__ == "__main__":
    main()
