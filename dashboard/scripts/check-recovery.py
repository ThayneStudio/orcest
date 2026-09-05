"""Local process-failure rehearsal with synthetic data and owned Redis directories.

Run after npm run build, with the project's Python environment and Node on PATH.
No live service, provider account, GitHub API, or deployment config is used.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import redis

from orcest.shared import work_observations as observations
from orcest.shared.models import Task, TaskType
from orcest.shared.redis_client import RedisClient


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually(check, *, seconds=15):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (redis.RedisError, URLError, ConnectionError):
            pass
        time.sleep(0.1)
    raise AssertionError("Recovery condition did not become true before deadline")


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def run():
    dashboard = Path(__file__).resolve().parents[1]
    redis_binary = os.environ.get("ORCEST_TEST_REDIS_SERVER", "redis-server")
    node = shutil.which("node")
    assert node, "Node must be on PATH"
    assert (dashboard / "build/server/index.js").exists(), "Run npm run build first"
    subprocess.run([node, "scripts/check-node-version.mjs"], cwd=dashboard, check=True)
    token = secrets.token_hex(24)
    owned = []
    clients = []
    checks = []
    with tempfile.TemporaryDirectory(prefix="orcest-recovery-") as temporary:
        root = Path(temporary)
        log = (root / "processes.log").open("w")

        def start_redis(directory, port, *, aof=True):
            directory.mkdir(exist_ok=True)
            process = subprocess.Popen(
                [
                    redis_binary,
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--dir",
                    str(directory),
                    "--save",
                    "",
                    "--appendonly",
                    "yes" if aof else "no",
                    "--appendfsync",
                    "always",
                ],
                stdout=log,
                stderr=log,
            )
            owned.append(process)
            client = redis.Redis(
                host="127.0.0.1",
                port=port,
                decode_responses=True,
                socket_timeout=2,
                socket_connect_timeout=2,
            )
            clients.append(client)
            eventually(lambda: process.poll() is None and client.ping())
            assert process.poll() is None
            return process, client

        redis_port, http_port = free_port(), free_port()
        while http_port == redis_port:
            http_port = free_port()
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(root),
            "REDIS_HOST": "127.0.0.1",
            "REDIS_PORT": str(redis_port),
            "DASHBOARD_TOKEN": token,
            "DASHBOARD_REDIS_PREFIXES": "recovery",
        }

        def start_dashboard():
            # The container entry point intentionally binds all interfaces;
            # this local rehearsal explicitly binds loopback instead.
            process = subprocess.Popen(
                [
                    node,
                    "--input-type=module",
                    "-e",
                    "import {createDashboardServer} from './build/server/index.js';"
                    f"const app=createDashboardServer({{port:{http_port}}});"
                    f"app.server.listen({http_port},'127.0.0.1');"
                    "process.on('SIGTERM',app.shutdown);",
                ],
                cwd=dashboard,
                env=env,
                stdout=log,
                stderr=log,
            )
            owned.append(process)
            eventually(lambda: request("/api/ready")[0] == 200)
            return process

        def request(path, cookie="", body=None):
            headers = {"Cookie": cookie, "Content-Type": "application/json"}
            req = Request(
                f"http://127.0.0.1:{http_port}{path}",
                headers=headers,
                data=json.dumps(body).encode() if body is not None else None,
            )
            try:
                with urlopen(req, timeout=8) as response:
                    return response.status, json.load(response), response.headers
            except HTTPError as error:
                return error.code, json.load(error), error.headers

        def login():
            status, _, headers = request("/api/auth/login", body={"token": token})
            assert status == 200
            return headers["Set-Cookie"].split(";")[0]

        try:
            redis_process, client = start_redis(root / "redis", redis_port)
            rc = RedisClient.from_client(client, key_prefix="recovery")
            observations.project_observation(rc, "test/recovery", 30, None)
            state = SimpleNamespace(
                number=1,
                title="Recovery fixture",
                body="Synthetic",
                action=SimpleNamespace(value="enqueue_implement"),
                open_blockers=[],
            )
            observations.observe(rc, "test/recovery", "issue", state)
            task = Task.create(
                TaskType.IMPLEMENT_ISSUE,
                "test/recovery",
                "fake",
                "issue",
                1,
                "Synthetic recovery fixture",
                provider="codex",
            )
            stream = "recovery:tasks:issue:codex"
            client.xgroup_create(stream, "workers", id="0", mkstream=True)
            entry = client.xadd(stream, task.to_dict())
            observations.queued(rc, task)

            # Kill a real claimant process after it has received the task and
            # before it can acknowledge it. No provider execution is simulated.
            claimant = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    "import redis,sys,time; r=redis.Redis(port=int(sys.argv[1]));"
                    "assert r.xreadgroup('workers','replacement-vm',{sys.argv[2]:'>'},count=1);"
                    "print('claimed',flush=True); time.sleep(60)",
                    str(redis_port),
                    stream,
                ],
                stdout=subprocess.PIPE,
                stderr=log,
                text=True,
            )
            owned.append(claimant)
            eventually(lambda: client.xpending(stream, "workers")["pending"] == 1)
            claimant.kill()
            claimant.wait(timeout=5)
            checks.append("claimant killed before ACK; task retained in pending list")
            app = start_dashboard()
            cookie = login()
            assert request("/api/work", cookie)[1]["total"] == 1
            client.save()
            backup = root / "backup"
            backup.mkdir()
            shutil.copy2(root / "redis/dump.rdb", backup / "dump.rdb")

            redis_process.kill()
            redis_process.wait(timeout=5)
            eventually(lambda: request("/api/ready")[0] == 503, seconds=20)
            assert request("/api/health")[0] == 200
            # Work reads may still use the bounded two-second coalescing cache.
            eventually(lambda: request("/api/work", cookie)[0] == 503, seconds=20)
            checks.append("Redis crash reports unavailable data while dashboard remains alive")

            redis_process, client = start_redis(root / "redis", redis_port)
            eventually(lambda: request("/api/ready")[0] == 200)
            recovered = client.xreadgroup("workers", "replacement-vm", {stream: "0"}, count=1)
            assert recovered[0][1][0][0] == entry
            assert recovered[0][1][0][1]["id"] == task.id
            assert client.xack(stream, "workers", entry) == 1
            assert client.xpending(stream, "workers")["pending"] == 0
            eventually(lambda: request("/api/work", cookie)[0] == 200)
            checks.append(
                "AOF restart preserves task identity and pending ownership; replay ACK succeeds"
            )

            # Restore the saved RDB into a DIFFERENT owned Redis, proving the
            # backup contains both observation hashes and pending stream state.
            restored_process, restored = start_redis(backup, free_port(), aof=False)
            assert restored.xpending(stream, "workers")["pending"] == 1
            assert restored.xrange(stream)[0][1]["id"] == task.id
            assert (
                restored.hget("recovery:dashboard:work:issue:test/recovery:1", "title")
                == state.title
            )
            checks.append(
                "independent RDB restore preserves observations, task payload and consumer group"
            )
            stop(restored_process)

            app.kill()
            app.wait(timeout=5)
            app = start_dashboard()
            assert request("/api/work", cookie)[0] == 401
            cookie = login()
            assert request("/api/work", cookie)[1]["total"] == 1
            checks.append(
                "dashboard restart invalidates old session; fresh sign-in restores same work"
            )
            print(json.dumps({"passed": True, "checks": checks}, indent=2))
        finally:
            for process in reversed(owned):
                stop(process)
            for client in clients:
                client.close()
            log.close()


if __name__ == "__main__":
    run()
