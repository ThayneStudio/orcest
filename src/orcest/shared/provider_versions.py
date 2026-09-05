"""Canonical expected provider CLI versions and bounded runtime probes."""

from __future__ import annotations

import os
import re
import selectors
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

PROVIDER_CLI_DESIRED_VERSIONS = MappingProxyType(
    {
        "claude": "2.1.235",
        "clauder": "2.1.235",
        "grok": "0.1.216",
        "codex": "0.149.1",
    }
)

PROVIDER_CLI_TEMPLATE_KEYS = MappingProxyType(
    {
        "claude": "claude_version",
        "clauder": "claude_version",
        "grok": "grok_version",
        "codex": "codex_version",
    }
)

TEMPLATE_VERSIONS_PATH = Path("/etc/orcest/template.versions")
PROVIDER_CLI_HEARTBEAT_SCHEMA = 1
PROVIDER_CLI_PROBE_STATUSES = frozenset(
    {
        "missing_desired_version",
        "missing_template_metadata",
        "template_metadata_unreadable",
        "template_metadata_oversized",
        "template_metadata_unparseable",
        "missing_template_version",
        "missing_binary",
        "ok",
        "version_mismatch",
        "probe_timeout",
        "probe_failed",
        "probe_output_oversized",
        "probe_output_unparseable",
    }
)
MAX_VERSION_OUTPUT_BYTES = 4096
MAX_TEMPLATE_VERSIONS_BYTES = 8192
VERSION_RE = re.compile(
    r"(?<![0-9A-Za-z.])v?(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)(?![0-9A-Za-z.])"
)


@dataclass(frozen=True)
class ProviderCliProbe:
    """Secret-free, bounded provider CLI version observation."""

    provider: str
    desired_version: str | None
    template_version: str | None
    observed_version: str | None
    status: str

    def to_heartbeat(self) -> dict[str, Any]:
        return {
            "schema": PROVIDER_CLI_HEARTBEAT_SCHEMA,
            "provider": self.provider,
            "desired_version": self.desired_version,
            "template_version": self.template_version,
            "observed_version": self.observed_version,
            "status": self.status,
        }


def normalize_cli_version_output(output: str) -> str | None:
    """Return the first semver-like token from bounded CLI output."""
    match = VERSION_RE.search(output)
    return match.group(1) if match else None


def desired_provider_cli_version(provider: str) -> str | None:
    return PROVIDER_CLI_DESIRED_VERSIONS.get(provider)


def parse_template_versions_file(
    path: Path = TEMPLATE_VERSIONS_PATH,
) -> tuple[dict[str, str], str | None]:
    """Parse known non-secret template version metadata.

    Returns ``(versions, error_status)``. Unknown keys are ignored and values
    must be plain single-line version tokens for the provider keys we use.
    """
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return {}, "missing_template_metadata"
    except OSError:
        return {}, "template_metadata_unreadable"
    if len(raw) > MAX_TEMPLATE_VERSIONS_BYTES:
        return {}, "template_metadata_oversized"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {}, "template_metadata_unparseable"
    versions: dict[str, str] = {}
    wanted = set(PROVIDER_CLI_TEMPLATE_KEYS.values())
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if not sep or key not in wanted:
            continue
        value = value.strip()
        if "\0" in value or normalize_cli_version_output(value) != value:
            return {}, "template_metadata_unparseable"
        versions[key] = value
    return versions, None


def collect_provider_cli_probe(
    provider: str,
    *,
    binary: str,
    binary_path: str | None,
    template_path: Path = TEMPLATE_VERSIONS_PATH,
    timeout_seconds: float = 5.0,
) -> ProviderCliProbe:
    """Probe one assigned provider CLI without preserving command output."""
    desired = desired_provider_cli_version(provider)
    versions, template_error = parse_template_versions_file(template_path)
    template_key = PROVIDER_CLI_TEMPLATE_KEYS.get(provider)
    template_version = versions.get(template_key, None) if template_key else None

    if desired is None:
        status = "missing_desired_version"
    elif template_error is not None:
        status = template_error
    elif template_key is None or template_version is None:
        status = "missing_template_version"
    elif not binary:
        status = "missing_binary"
    elif binary_path is None:
        status = "missing_binary"
    else:
        observed = _probe_binary_version(binary_path, timeout_seconds=timeout_seconds)
        if isinstance(observed, str):
            status = "ok" if desired == template_version == observed else "version_mismatch"
            return ProviderCliProbe(provider, desired, template_version, observed, status)
        status = observed["status"]
    return ProviderCliProbe(provider, desired, template_version, None, status)


def _probe_binary_version(binary_path: str, *, timeout_seconds: float) -> str | dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }
    try:
        process = subprocess.Popen(
            [binary_path, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            start_new_session=True,
        )
    except OSError:
        return {"status": "probe_failed"}

    stdout, stderr, error_status = _read_bounded_probe_output(
        process,
        timeout_seconds=timeout_seconds,
    )
    if error_status is not None:
        return {"status": error_status}
    if process.returncode != 0:
        return {"status": "probe_failed"}
    output = stdout + b"\n" + stderr
    try:
        text = output.decode("utf-8", errors="replace")
    except AttributeError:
        text = str(output)
    version = normalize_cli_version_output(text)
    if version is None:
        return {"status": "probe_output_unparseable"}
    return version


def _read_bounded_probe_output(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: float,
) -> tuple[bytes, bytes, str | None]:
    """Drain both probe pipes without retaining more than the configured bound."""
    stdout_buffer = bytearray()
    stderr_buffer = bytearray()
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    selector = selectors.DefaultSelector()
    streams = (
        (process.stdout, stdout_buffer),
        (process.stderr, stderr_buffer),
    )
    try:
        for stream, buffer in streams:
            if stream is None:
                continue
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, (stream, buffer))

        while selector.get_map():
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                _terminate_and_reap_probe(process)
                return bytes(stdout_buffer), bytes(stderr_buffer), "probe_timeout"
            events = selector.select(remaining_seconds)
            if not events:
                _terminate_and_reap_probe(process)
                return bytes(stdout_buffer), bytes(stderr_buffer), "probe_timeout"
            for key, _events in events:
                stream, buffer = key.data
                retained = len(stdout_buffer) + len(stderr_buffer)
                read_size = min(64 * 1024, MAX_VERSION_OUTPUT_BYTES + 1 - retained)
                try:
                    chunk = os.read(key.fd, read_size)
                except BlockingIOError:
                    continue
                except OSError:
                    _terminate_and_reap_probe(process)
                    return bytes(stdout_buffer), bytes(stderr_buffer), "probe_failed"
                if not chunk:
                    selector.unregister(stream)
                    stream.close()
                    continue
                buffer.extend(chunk)
                if len(stdout_buffer) + len(stderr_buffer) > MAX_VERSION_OUTPUT_BYTES:
                    _terminate_and_reap_probe(process)
                    return (
                        bytes(stdout_buffer),
                        bytes(stderr_buffer),
                        "probe_output_oversized",
                    )

        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            _terminate_and_reap_probe(process)
            return bytes(stdout_buffer), bytes(stderr_buffer), "probe_timeout"
        try:
            process.wait(timeout=remaining_seconds)
        except subprocess.TimeoutExpired:
            _terminate_and_reap_probe(process)
            return bytes(stdout_buffer), bytes(stderr_buffer), "probe_timeout"
        return bytes(stdout_buffer), bytes(stderr_buffer), None
    finally:
        selector.close()
        for stream, _buffer in streams:
            if stream is not None and not stream.closed:
                stream.close()


def _terminate_and_reap_probe(process: subprocess.Popen[bytes]) -> None:
    """Stop the isolated probe process group and always reap its leader."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=0.5)
    except subprocess.TimeoutExpired:
        pass
    # Signal the group even if the leader exited after SIGTERM: descendants
    # may still hold inherited output pipes or ignore the graceful signal.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()
