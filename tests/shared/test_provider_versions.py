from __future__ import annotations

import subprocess

import pytest

from orcest.shared.provider_versions import (
    MAX_VERSION_OUTPUT_BYTES,
    collect_provider_cli_probe,
    normalize_cli_version_output,
)

pytestmark = pytest.mark.unit


def test_normalizes_known_provider_version_outputs():
    assert normalize_cli_version_output("claude 2.1.235") == "2.1.235"
    assert normalize_cli_version_output("grok version 0.1.216\n") == "0.1.216"
    assert normalize_cli_version_output("codex-cli 0.149.1") == "0.149.1"


def test_collect_provider_cli_probe_matches_desired_template_and_executable(tmp_path, mocker):
    metadata = tmp_path / "template.versions"
    metadata.write_text("codex_version=0.149.1\n", encoding="utf-8")
    run = mocker.patch(
        "orcest.shared.provider_versions.subprocess.run",
        return_value=subprocess.CompletedProcess(
            ["/usr/bin/codex", "--version"],
            0,
            stdout=b"codex-cli 0.149.1\n",
            stderr=b"",
        ),
    )

    probe = collect_provider_cli_probe(
        "codex",
        binary="codex",
        binary_path="/usr/bin/codex",
        template_path=metadata,
        timeout_seconds=1,
    )

    assert probe.to_heartbeat() == {
        "schema": 1,
        "provider": "codex",
        "desired_version": "0.149.1",
        "template_version": "0.149.1",
        "observed_version": "0.149.1",
        "status": "ok",
    }
    run.assert_called_once()
    args, kwargs = run.call_args
    assert args[0] == ["/usr/bin/codex", "--version"]
    assert kwargs["check"] is False
    assert "npm" not in str(run.call_args)
    assert "apt-get" not in str(run.call_args)


@pytest.mark.parametrize(
    ("stdout", "status"),
    [
        (b"not a version", "probe_output_unparseable"),
        (b"x" * (MAX_VERSION_OUTPUT_BYTES + 1), "probe_output_oversized"),
    ],
)
def test_collect_provider_cli_probe_rejects_malformed_or_oversized_output(
    tmp_path, mocker, stdout, status
):
    metadata = tmp_path / "template.versions"
    metadata.write_text("grok_version=0.1.216\n", encoding="utf-8")
    mocker.patch(
        "orcest.shared.provider_versions.subprocess.run",
        return_value=subprocess.CompletedProcess(["/usr/bin/grok", "--version"], 0, stdout, b""),
    )

    probe = collect_provider_cli_probe(
        "grok",
        binary="grok",
        binary_path="/usr/bin/grok",
        template_path=metadata,
    )

    assert probe.status == status
    assert probe.observed_version is None


def test_collect_provider_cli_probe_fails_closed_for_missing_template_metadata(tmp_path, mocker):
    run = mocker.patch("orcest.shared.provider_versions.subprocess.run")

    probe = collect_provider_cli_probe(
        "claude",
        binary="claude",
        binary_path="/usr/bin/claude",
        template_path=tmp_path / "missing",
    )

    assert probe.status == "missing_template_metadata"
    assert probe.observed_version is None
    run.assert_not_called()


def test_collect_provider_cli_probe_reports_timeout(tmp_path, mocker):
    metadata = tmp_path / "template.versions"
    metadata.write_text("codex_version=0.149.1\n", encoding="utf-8")
    mocker.patch(
        "orcest.shared.provider_versions.subprocess.run",
        side_effect=subprocess.TimeoutExpired(["/usr/bin/codex", "--version"], 1),
    )

    probe = collect_provider_cli_probe(
        "codex",
        binary="codex",
        binary_path="/usr/bin/codex",
        template_path=metadata,
        timeout_seconds=1,
    )

    assert probe.status == "probe_timeout"
    assert probe.observed_version is None


def test_collect_provider_cli_probe_reports_missing_binary(tmp_path, mocker):
    metadata = tmp_path / "template.versions"
    metadata.write_text("codex_version=0.149.1\n", encoding="utf-8")
    run = mocker.patch("orcest.shared.provider_versions.subprocess.run")

    probe = collect_provider_cli_probe(
        "codex",
        binary="codex",
        binary_path=None,
        template_path=metadata,
    )

    assert probe.status == "missing_binary"
    run.assert_not_called()
