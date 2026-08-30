"""Run a child command against an invocation-scoped Docker Redis."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import redis

from tests.harness.constants import (
    COMPOSE_FILE_ENV,
    DEFAULT_SIGNAL_GRACE_SECS,
    GRACE_SECS_ENV,
    HARNESS_LABEL,
    HARNESS_LABEL_VALUE,
    IDENTITY_FILE_ENV,
    MARKER_DB,
    MARKER_KEY,
    NONCE_ENV,
    PROJECT_PREFIX,
    TEST_DB,
    URL_ENV,
    default_compose_file,
)

SIGINT_EXIT = 128 + signal.SIGINT
SIGTERM_EXIT = 128 + signal.SIGTERM


def compose_cleanup_cmd(project: str, compose_file: Path) -> list[str]:
    """Exact cleanup command for one invocation-owned Compose project."""
    return [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(compose_file),
        "down",
        "--volumes",
        "--remove-orphans",
    ]


def generate_nonce() -> str:
    return secrets.token_hex(16)


def generate_project_name(nonce: str) -> str:
    return f"{PROJECT_PREFIX}{nonce[:12]}"


def resolve_exit_status(
    child_returncode: int | None,
    received_signal: int | None,
    cleanup_ok: bool,
) -> int:
    """Map child status + supervisor signal + cleanup to the process exit code.

    INT/TERM on the supervisor become 130/143. A cleanup failure overrides the
    result only when the child succeeded (status 0).
    """
    if received_signal == signal.SIGINT:
        status = SIGINT_EXIT
    elif received_signal == signal.SIGTERM:
        status = SIGTERM_EXIT
    elif child_returncode is None:
        status = 1
    elif child_returncode < 0:
        status = 128 + (-child_returncode)
    else:
        status = child_returncode
    if not cleanup_ok and status == 0:
        return 1
    return status


def _docker_json_list(args: Sequence[str]) -> list[dict[str, Any]]:
    proc = subprocess.run(
        ["docker", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"docker {' '.join(args)} failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def list_stale_resources() -> dict[str, list[dict[str, Any]]]:
    """Discover labeled harness resources. Never deletes anything."""
    filt = f"label={HARNESS_LABEL}={HARNESS_LABEL_VALUE}"
    return {
        "containers": _docker_json_list(["ps", "-a", "--filter", filt, "--format", "{{json .}}"]),
        "networks": _docker_json_list(
            ["network", "ls", "--filter", filt, "--format", "{{json .}}"]
        ),
        "volumes": _docker_json_list(["volume", "ls", "--filter", filt, "--format", "{{json .}}"]),
    }


class CleanupOnce:
    """Run a cleanup callable at most once."""

    def __init__(self, fn: Callable[[], bool]) -> None:
        self._fn = fn
        self._done = False
        self.calls = 0

    def __call__(self) -> bool:
        if self._done:
            return True
        self._done = True
        self.calls += 1
        return bool(self._fn())


class RedisTestSupervisor:
    """Start one labeled Redis Compose project, run a child, clean that project."""

    def __init__(
        self,
        *,
        compose_file: Path | None = None,
        nonce: str | None = None,
        grace_secs: float | None = None,
        identity_file: Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self.environ = dict(os.environ if environ is None else environ)
        self.compose_file = Path(
            compose_file or self.environ.get(COMPOSE_FILE_ENV) or default_compose_file()
        ).resolve()
        self.nonce = nonce or generate_nonce()
        self.project = generate_project_name(self.nonce)
        if grace_secs is None:
            raw = self.environ.get(GRACE_SECS_ENV, str(DEFAULT_SIGNAL_GRACE_SECS))
            grace_secs = float(raw)
        self.grace_secs = grace_secs
        identity_raw = identity_file or self.environ.get(IDENTITY_FILE_ENV)
        self.identity_file = Path(identity_raw) if identity_raw else None
        self.port: int | None = None
        self.url: str | None = None
        self._child: subprocess.Popen[str] | None = None
        self._received: int | None = None
        self._cleanup = CleanupOnce(self._compose_down)

    def child_env(self) -> dict[str, str]:
        if self.url is None:
            raise RuntimeError("test Redis URL is not available before startup")
        env = dict(self.environ)
        env[URL_ENV] = self.url
        env[NONCE_ENV] = self.nonce
        return env

    def compose_up_cmd(self) -> list[str]:
        return [
            "docker",
            "compose",
            "--progress",
            "quiet",
            "-p",
            self.project,
            "-f",
            str(self.compose_file),
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "30",
        ]

    def _compose_env(self) -> dict[str, str]:
        env = dict(self.environ)
        env["ORCEST_TEST_REDIS_NONCE"] = self.nonce
        env["ORCEST_TEST_REDIS_PROJECT"] = self.project
        env["COMPOSE_PROJECT_NAME"] = self.project
        return env

    def start_redis(self) -> None:
        proc = subprocess.run(
            self.compose_up_cmd(),
            check=False,
            capture_output=True,
            text=True,
            env=self._compose_env(),
        )
        if proc.returncode != 0:
            raise RuntimeError(
                "failed to start invocation-scoped test Redis: "
                f"{proc.stderr.strip() or proc.stdout.strip()}"
            )
        self.port = self._discover_port()
        self.url = f"redis://127.0.0.1:{self.port}/{TEST_DB}"
        self._seed_marker()
        self._write_identity()

    def _discover_port(self) -> int:
        proc = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "-f",
                str(self.compose_file),
                "port",
                "redis",
                "6379",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=self._compose_env(),
        )
        if proc.returncode != 0:
            raise RuntimeError(f"failed to discover published Redis port: {proc.stderr.strip()}")
        line = proc.stdout.strip().splitlines()[-1]
        try:
            return int(line.rsplit(":", 1)[1])
        except (IndexError, ValueError) as exc:
            raise RuntimeError(f"unexpected `docker compose port` output: {proc.stdout!r}") from exc

    def _seed_marker(self) -> None:
        if self.port is None:
            raise RuntimeError("Redis port is not available")
        client = redis.Redis(
            host="127.0.0.1",
            port=self.port,
            db=MARKER_DB,
            decode_responses=True,
        )
        try:
            client.set(MARKER_KEY, self.nonce)
            if client.get(MARKER_KEY) != self.nonce:
                raise RuntimeError("failed to seed invocation marker")
        finally:
            client.close()

    def _write_identity(self) -> None:
        payload = {
            "project": self.project,
            "nonce": self.nonce,
            "url": self.url,
            "port": self.port,
            "compose_file": str(self.compose_file),
            "pid": os.getpid(),
            "marker_key": MARKER_KEY,
        }
        line = json.dumps(payload, sort_keys=True)
        print(f"orcest-test-redis: {line}", file=sys.stderr, flush=True)
        if self.identity_file is not None:
            tmp = self.identity_file.with_suffix(self.identity_file.suffix + ".tmp")
            tmp.write_text(line + "\n", encoding="utf-8")
            tmp.replace(self.identity_file)

    def _compose_down(self) -> bool:
        proc = subprocess.run(
            compose_cleanup_cmd(self.project, self.compose_file),
            check=False,
            capture_output=True,
            text=True,
            env=self._compose_env(),
        )
        if proc.returncode != 0:
            print(
                f"test Redis cleanup failed: {proc.stderr.strip() or proc.stdout.strip()}",
                file=sys.stderr,
            )
            return False
        return True

    def cleanup(self) -> bool:
        return self._cleanup()

    def _handle_signal(self, signum: int, _frame: object | None) -> None:
        self._received = signum
        self._forward_signal(signum)

    def _forward_signal(self, signum: int) -> None:
        child = self._child
        if child is None or child.poll() is not None:
            return
        try:
            pgid = os.getpgid(child.pid)
        except (ProcessLookupError, PermissionError):
            return
        try:
            os.killpg(pgid, signum)
        except (ProcessLookupError, PermissionError):
            return

    def _kill_group(self) -> None:
        child = self._child
        if child is None or child.poll() is not None:
            return
        try:
            pgid = os.getpgid(child.pid)
        except (ProcessLookupError, PermissionError):
            return
        try:
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            return

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def run_child(self, argv: Sequence[str]) -> int:
        self._child = subprocess.Popen(
            list(argv),
            env=self.child_env(),
            start_new_session=True,
            text=True,
        )
        if self._received is not None:
            self._forward_signal(self._received)
        deadline_after_signal: float | None = None
        while True:
            if self._received is not None and deadline_after_signal is None:
                deadline_after_signal = time.monotonic() + self.grace_secs
                self._forward_signal(self._received)
            try:
                timeout = 0.2
                if deadline_after_signal is not None:
                    remaining = deadline_after_signal - time.monotonic()
                    if remaining <= 0:
                        self._kill_group()
                        self._child.wait()
                        break
                    timeout = min(timeout, max(remaining, 0.01))
                self._child.wait(timeout=timeout)
                break
            except subprocess.TimeoutExpired:
                continue
        rc = self._child.returncode
        return 1 if rc is None else rc

    def run(self, argv: Sequence[str]) -> int:
        child_rc: int | None = None
        self._install_signal_handlers()
        try:
            self.start_redis()
            child_rc = self.run_child(argv)
        except Exception as exc:
            print(f"test Redis supervisor failed: {exc}", file=sys.stderr)
            if child_rc is None:
                child_rc = 1
        finally:
            cleanup_ok = self.cleanup()
        return resolve_exit_status(child_rc, self._received, cleanup_ok)


def _parse_argv(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tests.harness.supervisor",
        description=(
            "Start an invocation-scoped test Redis, export "
            f"{URL_ENV} and {NONCE_ENV}, and run a child command."
        ),
    )
    parser.add_argument(
        "--list-stale",
        action="store_true",
        help=(
            "Print containers/networks/volumes labeled "
            f"{HARNESS_LABEL}={HARNESS_LABEL_VALUE}. Does not delete."
        ),
    )
    parser.add_argument(
        "--grace-secs",
        type=float,
        default=None,
        help=(
            "Seconds to wait after INT/TERM before SIGKILL "
            f"(env {GRACE_SECS_ENV}, default {DEFAULT_SIGNAL_GRACE_SECS})."
        ),
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser.parse_args(list(argv))


def main(argv: Sequence[str] | None = None) -> int:
    ns = _parse_argv(sys.argv[1:] if argv is None else argv)
    if ns.list_stale:
        json.dump(list_stale_resources(), sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0
    command: list[str] = list(ns.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        print(
            "usage: python -m tests.harness.supervisor [--grace-secs N] COMMAND [ARGS...]",
            file=sys.stderr,
        )
        return 2
    supervisor = RedisTestSupervisor(grace_secs=ns.grace_secs)
    return supervisor.run(command)


if __name__ == "__main__":
    raise SystemExit(main())
