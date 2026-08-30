"""Live acceptance tests for the invocation-scoped Redis test supervisor."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import redis

from tests.harness.constants import (
    CHILD_EXIT_ENV,
    GRACE_SECS_ENV,
    HARNESS_LABEL,
    HARNESS_LABEL_VALUE,
    IDENTITY_FILE_ENV,
    IGNORE_SIGNALS_ENV,
    MARKER_KEY,
    NONCE_ENV,
    READY_FILE_ENV,
    RELEASE_FILE_ENV,
    SPAWN_GRANDCHILD_ENV,
    URL_ENV,
    repo_root,
)
from tests.harness.proof import RedisProofError, guarded_flushdb
from tests.harness.supervisor import compose_cleanup_cmd, list_stale_resources

pytestmark = pytest.mark.timeout(120)

ROOT = repo_root()
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"


def _wait_json(path: Path, timeout: float = 40.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for JSON file {path}")


def _ids(args: list[str]) -> set[str]:
    proc = subprocess.run(args, check=True, capture_output=True, text=True)
    return {line.strip() for line in proc.stdout.splitlines() if line.strip()}


def _proc_running(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            stat = handle.read()
    except FileNotFoundError:
        return False
    state = stat.rsplit(")", 1)[-1].split()[0]
    return state not in {"Z", "X"}


def snapshot_project(project: str) -> dict[str, set[str]]:
    containers = _ids(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label={COMPOSE_PROJECT_LABEL}={project}",
        ]
    )
    networks = _ids(
        [
            "docker",
            "network",
            "ls",
            "-q",
            "--filter",
            f"label={COMPOSE_PROJECT_LABEL}={project}",
        ]
    )
    volumes = _ids(
        [
            "docker",
            "volume",
            "ls",
            "-q",
            "--filter",
            f"label={COMPOSE_PROJECT_LABEL}={project}",
        ]
    )
    volumes |= _ids(
        [
            "docker",
            "volume",
            "ls",
            "-q",
            "--filter",
            f"label={HARNESS_LABEL}={HARNESS_LABEL_VALUE}",
            "--filter",
            f"label=com.orcest.test.project={project}",
        ]
    )
    for cid in list(containers):
        inspect = json.loads(subprocess.check_output(["docker", "inspect", cid], text=True))
        for mount in inspect[0].get("Mounts", []):
            if mount.get("Type") == "volume" and mount.get("Name"):
                volumes.add(mount["Name"])
    return {"containers": containers, "networks": networks, "volumes": volumes}


def assert_gone(snapshot: dict[str, set[str]]) -> None:
    for cid in snapshot["containers"]:
        proc = subprocess.run(
            ["docker", "inspect", cid], check=False, capture_output=True, text=True
        )
        assert proc.returncode != 0, f"container {cid} still exists"
    for nid in snapshot["networks"]:
        proc = subprocess.run(
            ["docker", "network", "inspect", nid],
            check=False,
            capture_output=True,
            text=True,
        )
        assert proc.returncode != 0, f"network {nid} still exists"
    for vid in snapshot["volumes"]:
        proc = subprocess.run(
            ["docker", "volume", "inspect", vid],
            check=False,
            capture_output=True,
            text=True,
        )
        assert proc.returncode != 0, f"volume {vid} still exists"


@contextmanager
def harness(
    tmp_path: Path,
    extra_env: dict[str, str] | None = None,
) -> Iterator[tuple[subprocess.Popen[Any], dict[str, Any], dict[str, Any], Path]]:
    identity = tmp_path / "identity.json"
    ready = tmp_path / "ready.json"
    release = tmp_path / "release"
    env = os.environ.copy()
    env.update(extra_env or {})
    env[IDENTITY_FILE_ENV] = str(identity)
    env[READY_FILE_ENV] = str(ready)
    env[RELEASE_FILE_ENV] = str(release)
    env.setdefault(GRACE_SECS_ENV, "1")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.harness.supervisor",
            "--grace-secs",
            env[GRACE_SECS_ENV],
            sys.executable,
            "-m",
            "tests.harness.blocking_client",
        ],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        ident = _wait_json(identity)
        ready_payload = _wait_json(ready)
        yield proc, ident, ready_payload, release
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


def test_overlapping_harnesses_are_isolated(tmp_path: Path) -> None:
    first_dir = tmp_path / "a"
    second_dir = tmp_path / "b"
    first_dir.mkdir()
    second_dir.mkdir()
    with harness(first_dir) as (proc_a, ident_a, ready_a, _release_a):
        with harness(second_dir) as (proc_b, ident_b, ready_b, _release_b):
            assert ident_a["project"] != ident_b["project"]
            assert ident_a["nonce"] != ident_b["nonce"]
            assert ident_a["port"] != ident_b["port"]
            assert ident_a["url"] != ident_b["url"]
            snap_a = snapshot_project(ident_a["project"])
            snap_b = snapshot_project(ident_b["project"])
            assert snap_a["containers"]
            assert snap_b["containers"]
            assert snap_a["containers"].isdisjoint(snap_b["containers"])
            assert snap_a["networks"].isdisjoint(snap_b["networks"])

            client_a = redis.from_url(ident_a["url"], decode_responses=True)
            client_b = redis.from_url(ident_b["url"], decode_responses=True)
            try:
                assert client_a.get(ready_a["key"]) == ident_a["nonce"]
                assert client_b.get(ready_b["key"]) == ident_b["nonce"]
                assert client_a.get(ready_b["key"]) is None
                assert client_b.get(ready_a["key"]) is None
                client_a.flushdb()
                assert client_a.get(ready_a["key"]) is None
                assert client_b.get(ready_b["key"]) == ident_b["nonce"]
            finally:
                client_a.close()
                client_b.close()

            proc_a.send_signal(signal.SIGTERM)
            assert proc_a.wait(timeout=20) == 143
            assert_gone(snap_a)
            still_b = snapshot_project(ident_b["project"])
            assert still_b["containers"] == snap_b["containers"]
            client_b = redis.from_url(ident_b["url"], decode_responses=True)
            try:
                assert client_b.get(ready_b["key"]) == ident_b["nonce"]
            finally:
                client_b.close()

        assert proc_b.wait(timeout=20) in {0, 143}


def test_success_failure_int_and_term_cleanup(tmp_path: Path) -> None:
    cases = [
        ("success", {CHILD_EXIT_ENV: "0"}, None, 0),
        ("failure", {CHILD_EXIT_ENV: "7"}, None, 7),
        ("int", {SPAWN_GRANDCHILD_ENV: "1"}, signal.SIGINT, 130),
        ("term", {SPAWN_GRANDCHILD_ENV: "1"}, signal.SIGTERM, 143),
    ]
    for name, extra, sig, expected in cases:
        case_dir = tmp_path / name
        case_dir.mkdir()
        with harness(case_dir, extra) as (proc, ident, ready, release):
            snap = snapshot_project(ident["project"])
            assert snap["containers"]
            assert snap["networks"]
            grandchild = ready.get("grandchild_pid")
            if sig is None:
                release.touch()
            else:
                assert grandchild is not None
                assert _proc_running(int(grandchild))
                proc.send_signal(sig)
            rc = proc.wait(timeout=20)
            assert rc == expected, f"{name}: expected {expected}, got {rc}"
            assert_gone(snap)
            leftover = subprocess.run(
                compose_cleanup_cmd(ident["project"], Path(ident["compose_file"])),
                check=False,
                capture_output=True,
                text=True,
            )
            assert leftover.returncode == 0
            assert_gone(snap)
            if grandchild is not None:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and _proc_running(int(grandchild)):
                    time.sleep(0.05)
                assert not _proc_running(int(grandchild))


def test_escalation_kills_signal_immune_process_group(tmp_path: Path) -> None:
    extra = {
        SPAWN_GRANDCHILD_ENV: "1",
        IGNORE_SIGNALS_ENV: "1",
        GRACE_SECS_ENV: "1",
    }
    with harness(tmp_path, extra) as (proc, ident, ready, _release):
        snap = snapshot_project(ident["project"])
        grandchild = int(ready["grandchild_pid"])
        child_pid = int(ready["pid"])
        proc.send_signal(signal.SIGTERM)
        time.sleep(0.3)
        assert _proc_running(child_pid)
        assert _proc_running(grandchild)
        rc = proc.wait(timeout=20)
        assert rc == 143
        assert not _proc_running(child_pid)
        assert not _proc_running(grandchild)
        assert_gone(snap)


def test_wrong_url_nonce_and_db0_refuse_flush(tmp_path: Path) -> None:
    with harness(tmp_path) as (_proc, ident, ready, _release):
        url = ident["url"]
        nonce = ident["nonce"]
        key = ready["key"]
        raw = redis.from_url(url, decode_responses=True)
        try:
            assert raw.get(key) == nonce
            with pytest.raises(RedisProofError):
                guarded_flushdb(raw, url, "not-the-nonce")
            assert raw.get(key) == nonce

            db0 = url.rsplit("/", 1)[0] + "/0"
            with pytest.raises(RedisProofError):
                guarded_flushdb(raw, db0, nonce)
            assert raw.get(key) == nonce

            marker = redis.from_url(db0, decode_responses=True)
            try:
                assert marker.get(MARKER_KEY) == nonce
            finally:
                marker.close()
        finally:
            raw.close()

        with pytest.raises(RedisProofError, match=URL_ENV):
            from tests.harness.proof import require_test_redis_proof

            require_test_redis_proof({NONCE_ENV: nonce})
        with pytest.raises(RedisProofError, match=NONCE_ENV):
            from tests.harness.proof import require_test_redis_proof

            require_test_redis_proof({URL_ENV: url})


def test_stale_labels_are_discoverable_without_broad_delete(tmp_path: Path) -> None:
    with harness(tmp_path) as (_proc, ident, _ready, _release):
        listed = list_stale_resources()
        names = " ".join(
            str(item.get("Names") or item.get("Name") or item.get("ID") or "")
            for group in listed.values()
            for item in group
        )
        assert ident["project"] in names or any(
            ident["project"] in str(item) for item in listed["containers"]
        )
        stale_proc = subprocess.run(
            [sys.executable, "-m", "tests.harness.supervisor", "--list-stale"],
            cwd=str(ROOT),
            check=False,
            capture_output=True,
            text=True,
        )
        assert stale_proc.returncode == 0
        assert ident["project"] in stale_proc.stdout
        snap = snapshot_project(ident["project"])
        assert snap["containers"], "--list-stale must not delete running harness resources"
        assert compose_cleanup_cmd(ident["project"], Path(ident["compose_file"]))[2:6] == [
            "-p",
            ident["project"],
            "-f",
            ident["compose_file"],
        ]
