import contextlib
import os
import signal
import subprocess
import time

import pytest

from orcest.worker.liveness_signals import ProcessTreeSampler, WorkspaceSampler


def _kill_group(proc: subprocess.Popen) -> None:
    """Kill the whole process group, not just the direct child.

    These fixtures background a CPU burner with ``&``; ``proc.kill()`` reaps only
    the ``sh`` parent and leaves the grandchild spinning forever under init.
    ``start_new_session=True`` makes ``proc`` a process-group leader, so signalling
    the group takes the whole tree down.
    """
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait()


@pytest.mark.integration
def test_cpu_sampler_sees_busy_child():
    # parent sh spawns a python child that burns CPU: tree sampling must see it
    proc = subprocess.Popen(
        ["sh", "-c", "python3 -c 'x=0\nwhile True: x+=1' & wait"],
        start_new_session=True,
    )
    try:
        s = ProcessTreeSampler(proc.pid)
        a = s.sample()
        time.sleep(1.0)
        b = s.sample()  # noqa: E702
        assert a is not None and b is not None and b - a > 0.2
    finally:
        _kill_group(proc)


@pytest.mark.integration
def test_cpu_sampler_idle_process_near_zero():
    proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        s = ProcessTreeSampler(proc.pid)
        a = s.sample()
        time.sleep(0.5)
        b = s.sample()  # noqa: E702
        assert b - a < 0.05
    finally:
        _kill_group(proc)


@pytest.mark.integration
def test_cpu_sampler_counts_reaped_children():
    # sh runs a short CPU burst child to completion; cutime must capture it
    proc = subprocess.Popen(
        ["sh", "-c", "python3 -c 'x=0\nfor i in range(10**7): x+=1'; sleep 30"],
        start_new_session=True,
    )
    try:
        s = ProcessTreeSampler(proc.pid)
        deadline = time.time() + 15
        while time.time() < deadline:
            v = s.sample()
            if v is not None and v > 0.1:
                break
            time.sleep(0.3)
        assert v is not None and v > 0.1
    finally:
        _kill_group(proc)


@pytest.mark.unit
def test_cpu_sampler_gone_process_returns_none():
    proc = subprocess.Popen(["true"])
    proc.wait()  # noqa: E702
    assert ProcessTreeSampler(proc.pid).sample() is None


@pytest.mark.unit
def test_workspace_sampler(tmp_path):
    (tmp_path / "a.txt").write_text("x")
    ws = WorkspaceSampler(tmp_path)
    ts = time.time()
    time.sleep(0.05)
    assert ws.changed_since(ts) is False
    (tmp_path / "b.txt").write_text("y")
    assert ws.changed_since(ts) is True
