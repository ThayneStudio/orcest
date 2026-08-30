"""Small blocking child used by harness acceptance tests."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import redis

from tests.harness.constants import (
    CHILD_EXIT_ENV,
    CLIENT_KEY_PREFIX,
    IGNORE_SIGNALS_ENV,
    NONCE_ENV,
    READY_FILE_ENV,
    RELEASE_FILE_ENV,
    SPAWN_GRANDCHILD_ENV,
    URL_ENV,
)


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip() in {"1", "true", "yes"}


def _spawn_grandchild(ignore_signals: bool) -> int:
    if ignore_signals:
        code = (
            "import signal, time\n"
            "signal.signal(signal.SIGINT, signal.SIG_IGN)\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "time.sleep(3600)\n"
        )
    else:
        code = "import time; time.sleep(3600)\n"
    proc = subprocess.Popen([sys.executable, "-c", code])
    return proc.pid


def _write_ready(payload: dict[str, object]) -> None:
    line = json.dumps(payload, sort_keys=True)
    print(f"READY {line}", flush=True)
    ready_path = os.environ.get(READY_FILE_ENV, "").strip()
    if ready_path:
        path = Path(ready_path)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(line + "\n", encoding="utf-8")
        tmp.replace(path)


def _wait_for_release_or_sleep() -> None:
    release = os.environ.get(RELEASE_FILE_ENV, "").strip()
    if release:
        path = Path(release)
        while not path.exists():
            time.sleep(0.05)
        return
    while True:
        time.sleep(3600)


def main() -> int:
    url = os.environ.get(URL_ENV, "").strip()
    nonce = os.environ.get(NONCE_ENV, "").strip()
    if not url or not nonce:
        print("blocking client missing test Redis URL or nonce", file=sys.stderr)
        return 2
    ignore_signals = _env_flag(IGNORE_SIGNALS_ENV)
    if ignore_signals:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    grandchild_pid: int | None = None
    if _env_flag(SPAWN_GRANDCHILD_ENV):
        grandchild_pid = _spawn_grandchild(ignore_signals)
    key = f"{CLIENT_KEY_PREFIX}{nonce}"
    client = redis.from_url(url, decode_responses=True)
    try:
        client.set(key, nonce)
        _write_ready(
            {
                "pid": os.getpid(),
                "pgid": os.getpgid(os.getpid()),
                "grandchild_pid": grandchild_pid,
                "url": url,
                "nonce": nonce,
                "key": key,
            }
        )
    finally:
        client.close()
    _wait_for_release_or_sleep()
    raw_exit = os.environ.get(CHILD_EXIT_ENV, "0").strip() or "0"
    return int(raw_exit)


if __name__ == "__main__":
    raise SystemExit(main())
