from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from orcest.shared.provider_versions import (
    MAX_VERSION_OUTPUT_BYTES,
    _read_bounded_probe_output,
    collect_provider_cli_probe,
    normalize_cli_version_output,
)

pytestmark = pytest.mark.unit


def _write_probe(tmp_path: Path, source: str) -> str:
    path = tmp_path / "provider-probe"
    path.write_text(f"#!/usr/bin/env python3\n{source}", encoding="utf-8")
    path.chmod(0o755)
    return str(path)


def _collect_codex(tmp_path: Path, binary_path: str, *, timeout_seconds: float = 1.0):
    metadata = tmp_path / "template.versions"
    metadata.write_text("codex_version=0.149.1\n", encoding="utf-8")
    return collect_provider_cli_probe(
        "codex",
        binary="codex",
        binary_path=binary_path,
        template_path=metadata,
        timeout_seconds=timeout_seconds,
    )


def test_normalizes_known_provider_version_outputs():
    assert normalize_cli_version_output("claude 2.1.235") == "2.1.235"
    assert normalize_cli_version_output("grok version 0.1.216\n") == "0.1.216"
    assert normalize_cli_version_output("codex-cli 0.149.1") == "0.149.1"


def test_collect_provider_cli_probe_matches_desired_template_and_executable(tmp_path):
    binary_path = _write_probe(
        tmp_path,
        'import sys\nassert sys.argv[1:] == ["--version"]\nprint("codex-cli 0.149.1")\n',
    )

    probe = _collect_codex(tmp_path, binary_path)

    assert probe.to_heartbeat() == {
        "schema": 1,
        "provider": "codex",
        "desired_version": "0.149.1",
        "template_version": "0.149.1",
        "observed_version": "0.149.1",
        "status": "ok",
    }


def test_collect_provider_cli_probe_accepts_exact_combined_output_limit(tmp_path):
    stdout_size = MAX_VERSION_OUTPUT_BYTES // 2
    stderr_size = MAX_VERSION_OUTPUT_BYTES - stdout_size
    version = b"codex-cli 0.149.1\n"
    binary_path = _write_probe(
        tmp_path,
        "import os\n"
        f"os.write(1, {version!r} + b'x' * ({stdout_size} - {len(version)}))\n"
        f"os.write(2, b'y' * {stderr_size})\n",
    )

    probe = _collect_codex(tmp_path, binary_path)

    assert probe.status == "ok"
    assert probe.observed_version == "0.149.1"


@pytest.mark.parametrize("file_descriptor", [1, 2])
def test_collect_provider_cli_probe_bounds_each_output_stream_and_reaps(tmp_path, file_descriptor):
    pid_file = tmp_path / "probe.pid"
    binary_path = _write_probe(
        tmp_path,
        "import os, pathlib, signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid()))\n"
        f"os.write({file_descriptor}, b'x' * {MAX_VERSION_OUTPUT_BYTES + 1})\n"
        "time.sleep(30)\n",
    )

    probe = _collect_codex(tmp_path, binary_path)

    assert probe.status == "probe_output_oversized"
    assert probe.observed_version is None
    with pytest.raises(ChildProcessError):
        os.waitpid(int(pid_file.read_text()), os.WNOHANG)


def test_collect_provider_cli_probe_bounds_simultaneous_stdout_and_stderr(tmp_path):
    binary_path = _write_probe(
        tmp_path,
        "import os, threading, time\n"
        "def write_output(fd):\n"
        "    while True:\n"
        "        os.write(fd, b'x' * 1024)\n"
        "threads = [threading.Thread(target=write_output, args=(fd,)) for fd in (1, 2)]\n"
        "for thread in threads:\n"
        "    thread.start()\n"
        "time.sleep(30)\n",
    )

    probe = _collect_codex(tmp_path, binary_path)

    assert probe.status == "probe_output_oversized"
    assert probe.observed_version is None


def test_probe_output_reader_retains_only_the_limit_plus_one_sentinel_byte(tmp_path):
    binary_path = _write_probe(
        tmp_path,
        f"import os, time\nos.write(1, b'x' * {MAX_VERSION_OUTPUT_BYTES * 4})\ntime.sleep(30)\n",
    )
    process = subprocess.Popen(
        [binary_path, "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    stdout, stderr, status = _read_bounded_probe_output(process, timeout_seconds=1)

    assert status == "probe_output_oversized"
    assert len(stdout) + len(stderr) == MAX_VERSION_OUTPUT_BYTES + 1
    with pytest.raises(ChildProcessError):
        os.waitpid(process.pid, os.WNOHANG)


def test_collect_provider_cli_probe_rejects_malformed_output(tmp_path):
    binary_path = _write_probe(tmp_path, 'print("not a version")\n')

    probe = _collect_codex(tmp_path, binary_path)

    assert probe.status == "probe_output_unparseable"
    assert probe.observed_version is None


def test_collect_provider_cli_probe_reports_nonzero_exit_as_failed(tmp_path):
    binary_path = _write_probe(
        tmp_path,
        'print("codex-cli 0.149.1")\nraise SystemExit(2)\n',
    )

    probe = _collect_codex(tmp_path, binary_path)

    assert probe.status == "probe_failed"
    assert probe.observed_version is None


def test_collect_provider_cli_probe_reports_process_start_failure(tmp_path, mocker):
    popen = mocker.patch(
        "orcest.shared.provider_versions.subprocess.Popen",
        side_effect=OSError("exec failed"),
    )

    probe = _collect_codex(tmp_path, "/usr/bin/codex")

    assert probe.status == "probe_failed"
    assert probe.observed_version is None
    popen.assert_called_once()


def test_collect_provider_cli_probe_fails_closed_for_missing_template_metadata(tmp_path, mocker):
    popen = mocker.patch("orcest.shared.provider_versions.subprocess.Popen")

    probe = collect_provider_cli_probe(
        "claude",
        binary="claude",
        binary_path="/usr/bin/claude",
        template_path=tmp_path / "missing",
    )

    assert probe.status == "missing_template_metadata"
    assert probe.observed_version is None
    popen.assert_not_called()


def test_collect_provider_cli_probe_reports_timeout_and_reaps(tmp_path):
    pid_file = tmp_path / "probe.pid"
    binary_path = _write_probe(
        tmp_path,
        "import os, pathlib, signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(30)\n",
    )

    probe = _collect_codex(tmp_path, binary_path, timeout_seconds=0.1)

    assert probe.status == "probe_timeout"
    assert probe.observed_version is None
    with pytest.raises(ChildProcessError):
        os.waitpid(int(pid_file.read_text()), os.WNOHANG)


def test_collect_provider_cli_probe_reports_missing_binary(tmp_path, mocker):
    metadata = tmp_path / "template.versions"
    metadata.write_text("codex_version=0.149.1\n", encoding="utf-8")
    popen = mocker.patch("orcest.shared.provider_versions.subprocess.Popen")

    probe = collect_provider_cli_probe(
        "codex",
        binary="codex",
        binary_path=None,
        template_path=metadata,
    )

    assert probe.status == "missing_binary"
    popen.assert_not_called()
