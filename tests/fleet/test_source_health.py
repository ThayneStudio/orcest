"""Desired-source resolution and read-only fleet revision health tests."""

from __future__ import annotations

import json
import sys

import pytest

from orcest.fleet.config import (
    FleetConfig,
    OrchestratorConfig,
    PoolConfig,
    ProjectEntry,
    SourceConfig,
)
from orcest.fleet.source_health import (
    DesiredRevision,
    _active_template,
    _CommandResult,
    _run_bounded,
    _worker_revisions,
    collect_source_health,
    resolve_desired_revision,
)

pytestmark = pytest.mark.unit

NEW = "a" * 40
OLD = "b" * 40


def _configured_fleet() -> FleetConfig:
    return FleetConfig(
        source=SourceConfig(sha=NEW),
        orchestrator=OrchestratorConfig(host="10.20.0.23", user="orcest"),
        projects=[ProjectEntry(name="alpha", repo="Org/alpha")],
        pool=PoolConfig(size=2, template_vm_id=9001, vm_id_start=10000),
    )


def _patch_runtime_reads(
    mocker,
    *,
    project: str | None = NEW,
    pool: str | None = NEW,
    template: str | None = NEW,
    workers: list[tuple[str, str | None]] | None = None,
):
    from orcest.fleet import source_health

    if workers is None:
        workers = [("orcest-worker-10000", NEW), ("orcest-worker-10001", NEW)]

    def container(_target, compose_project, _service):
        return project if compose_project == "orcest-alpha" else pool

    mocker.patch.object(source_health, "_container_revision", side_effect=container)
    mocker.patch.object(source_health, "_active_template", return_value=("vm-9001", template))
    mocker.patch.object(source_health, "_worker_revisions", return_value=workers)


def test_immutable_sha_resolves_without_git(mocker):
    run = mocker.patch("orcest.fleet.source_health._run_bounded")

    desired = resolve_desired_revision(SourceConfig(sha=NEW.upper()))

    assert desired.resolved is True
    assert desired.revision == NEW
    run.assert_not_called()


@pytest.mark.parametrize(
    "source",
    [
        SourceConfig(repository="Org/orcest"),
        SourceConfig(ref="refs/heads/master"),
        SourceConfig(repository="Org/orcest", sha=NEW),
        SourceConfig(ref="refs/heads/master", sha=NEW),
        SourceConfig(repository="Org/orcest", ref="refs/heads/master", sha=NEW),
    ],
)
def test_source_policy_requires_exactly_repo_and_ref_or_sha(mocker, source):
    run = mocker.patch("orcest.fleet.source_health._run_bounded")

    desired = resolve_desired_revision(source)

    assert desired.revision is None
    assert desired.status == "invalid configuration"
    run.assert_not_called()


def test_moving_ref_uses_one_bounded_exact_lookup(mocker):
    run = mocker.patch(
        "orcest.fleet.source_health._run_bounded",
        return_value=_CommandResult("ok", f"{NEW}\trefs/heads/master\n".encode()),
    )

    desired = resolve_desired_revision(
        SourceConfig(
            repository="https://github.com/ThayneStudio/orcest.git",
            ref="refs/heads/master",
        )
    )

    assert desired.revision == NEW
    run.assert_called_once()
    argv = run.call_args.args[0]
    assert argv[-2:] == ["refs/heads/master", "refs/heads/master^{}"]
    assert run.call_args.kwargs["timeout"] > 0
    assert run.call_args.kwargs["max_output"] <= 4096


def test_bounded_command_enforces_output_limit_and_discards_stderr():
    secret = "secret-auth-diagnostic"
    result = _run_bounded(
        [
            sys.executable,
            "-c",
            f"import sys; sys.stderr.write({secret!r}); print('x' * 10000)",
        ],
        timeout=2,
        max_output=128,
    )

    assert result.status == "oversized response"
    assert result.output == b""
    assert secret.encode() not in result.output


def test_bounded_command_enforces_timeout():
    result = _run_bounded(
        [sys.executable, "-c", "import time; time.sleep(10)"],
        timeout=0.05,
        max_output=128,
    )

    assert result.status == "timeout"
    assert result.output == b""


@pytest.mark.parametrize(
    ("command_status", "expected_status"),
    [
        ("timeout", "timeout"),
        ("failed", "failed"),
        ("oversized response", "oversized response"),
    ],
)
def test_ref_failures_are_unknown_and_secret_safe(mocker, command_status, expected_status):
    mocker.patch(
        "orcest.fleet.source_health._run_bounded",
        return_value=_CommandResult(command_status, b""),
    )
    desired = resolve_desired_revision(
        SourceConfig(repository="Org/orcest", ref="refs/heads/master")
    )

    assert desired.revision is None
    assert desired.status == expected_status


@pytest.mark.parametrize(
    "output",
    [
        b"not-a-sha\trefs/heads/master\n",
        f"{NEW} refs/heads/master\n".encode(),
        f"{NEW}\trefs/heads/other\n".encode(),
        b"\xff\n",
    ],
)
def test_malformed_ref_response_never_resolves(mocker, output):
    mocker.patch(
        "orcest.fleet.source_health._run_bounded",
        return_value=_CommandResult("ok", output),
    )

    desired = resolve_desired_revision(
        SourceConfig(repository="Org/orcest", ref="refs/heads/master")
    )

    assert desired.resolved is False
    assert desired.status == "malformed response"


def test_repository_credentials_are_rejected_and_never_reported():
    secret = "very-secret-token"
    desired = resolve_desired_revision(
        SourceConfig(
            repository=f"https://{secret}@github.com/Org/orcest.git",
            ref="refs/heads/master",
        )
    )

    rendered = json.dumps(desired.__dict__)
    assert desired.status == "invalid configuration"
    assert desired.repository is None
    assert secret not in rendered


def test_malformed_repository_url_returns_fixed_invalid_configuration():
    desired = resolve_desired_revision(
        SourceConfig(
            repository="https://[bad/path",
            ref="refs/heads/master",
        )
    )

    assert desired.revision is None
    assert desired.repository is None
    assert desired.status == "invalid configuration"


def test_secret_in_repository_port_is_rejected_and_redacted():
    secret = "very-secret-token"
    desired = resolve_desired_revision(
        SourceConfig(
            repository=f"https://github.com:{secret}/Org/orcest.git",
            ref="refs/heads/master",
        )
    )

    assert desired.repository is None
    assert desired.status == "invalid configuration"
    assert secret not in json.dumps(desired.__dict__)


def test_unconfigured_source_is_explicit_non_green_without_remote_reads(mocker):
    read = mocker.patch("orcest.fleet.source_health._read_ssh")

    report = collect_source_health(FleetConfig())

    assert report.healthy is False
    assert report.desired.status == "desired revision unconfigured"
    assert report.runtimes == ()
    read.assert_not_called()


def test_exact_desired_runtime_match_is_healthy(mocker):
    _patch_runtime_reads(mocker)

    report = collect_source_health(_configured_fleet())

    assert report.healthy is True
    assert {runtime.runtime_class for runtime in report.runtimes} == {
        "project-orchestrator",
        "pool-manager",
        "active-template",
        "worker",
    }
    assert all(runtime.status == "current" for runtime in report.runtimes)


@pytest.mark.parametrize("surface", ["project", "pool", "template", "workers"])
def test_each_runtime_class_independently_makes_health_non_green(mocker, surface):
    values = {"project": NEW, "pool": NEW, "template": NEW}
    workers = [("orcest-worker-10000", NEW), ("orcest-worker-10001", NEW)]
    if surface == "workers":
        workers[0] = ("orcest-worker-10000", OLD)
    else:
        values[surface] = OLD
    _patch_runtime_reads(mocker, workers=workers, **values)

    report = collect_source_health(_configured_fleet())

    assert report.healthy is False
    stale = [runtime for runtime in report.runtimes if runtime.status == "stale"]
    assert len(stale) == 1
    assert stale[0].revision == OLD
    assert NEW in report.diagnostics[0]
    assert OLD in report.diagnostics[0]


def test_four_coherent_old_workers_fail_against_new_desired(mocker):
    workers = [(f"orcest-worker-{10000 + index}", OLD) for index in range(4)]
    _patch_runtime_reads(mocker, workers=workers)

    report = collect_source_health(_configured_fleet())

    worker_rows = [row for row in report.runtimes if row.runtime_class == "worker"]
    assert len(worker_rows) == 4
    assert all(row.status == "stale" for row in worker_rows)
    assert report.healthy is False
    assert all(NEW in diagnostic and OLD in diagnostic for diagnostic in report.diagnostics)


def test_stale_template_remains_visible_without_live_workers(mocker):
    _patch_runtime_reads(mocker, template=OLD, workers=[])

    report = collect_source_health(_configured_fleet())

    assert report.healthy is False
    assert any(
        row.runtime_class == "active-template" and row.status == "stale" for row in report.runtimes
    )
    assert any(row.runtime_id == "no-live-workers" for row in report.runtimes)


def test_active_template_uses_matching_config_revision_when_redis_metadata_is_legacy(
    mocker,
):
    redis = mocker.patch(
        "orcest.fleet.source_health._redis_read",
        side_effect=[
            _CommandResult("ok", b"9001\n"),
            _CommandResult("ok", b"\n"),
        ],
    )

    assert _active_template("orcest@host", 9001, OLD) == ("vm-9001", OLD)
    assert redis.call_count == 2


@pytest.mark.parametrize(
    "pointer",
    [
        _CommandResult("timeout", b""),
        _CommandResult("failed", b""),
        _CommandResult("ok", b"not-a-vmid\n"),
        _CommandResult("ok", b"-1\n"),
    ],
)
def test_active_template_pointer_failure_never_uses_config_fallback(mocker, pointer):
    redis = mocker.patch("orcest.fleet.source_health._redis_read", return_value=pointer)

    assert _active_template("orcest@host", 9001, NEW) == ("active-template", None)
    redis.assert_called_once()


@pytest.mark.parametrize(
    "metadata",
    [
        _CommandResult("timeout", b""),
        _CommandResult("failed", b""),
        _CommandResult("ok", b"not-a-revision\n"),
        _CommandResult("ok", f"{NEW}-dirty\n".encode()),
        _CommandResult("ok", f"{NEW}\n{OLD}\n".encode()),
    ],
)
def test_active_template_bad_metadata_never_uses_config_fallback(mocker, metadata):
    mocker.patch(
        "orcest.fleet.source_health._redis_read",
        side_effect=[_CommandResult("ok", b"9001\n"), metadata],
    )

    assert _active_template("orcest@host", 9001, NEW) == ("vm-9001", None)


def test_worker_snapshot_uses_one_batched_mget(mocker):
    heartbeat_one = json.dumps({"revision": NEW})
    heartbeat_two = json.dumps({"revision": OLD})
    redis = mocker.patch(
        "orcest.fleet.source_health._redis_read",
        side_effect=[
            _CommandResult(
                "ok",
                b"orcest:workers:heartbeat:orcest-worker-10000\n"
                b"orcest:workers:heartbeat:orcest-worker-10001\n",
            ),
            _CommandResult("ok", f"{heartbeat_one}\n{heartbeat_two}\n".encode()),
        ],
    )

    assert _worker_revisions("orcest@host") == [
        ("orcest-worker-10000", NEW),
        ("orcest-worker-10001", OLD),
    ]
    assert redis.call_count == 2
    assert redis.call_args_list[1].args[1].startswith("MGET ")


def test_worker_inspection_failure_is_distinct_from_verified_empty_pool(mocker):
    _patch_runtime_reads(mocker, workers=[])
    mocker.patch("orcest.fleet.source_health._worker_revisions", return_value=None)

    report = collect_source_health(_configured_fleet())

    worker = next(row for row in report.runtimes if row.runtime_class == "worker")
    assert worker.runtime_id == "inspection-unavailable"
    assert worker.status == "unknown"


def test_mixed_rolling_generation_and_busy_old_worker_remain_non_green(mocker):
    _patch_runtime_reads(
        mocker,
        workers=[
            ("orcest-worker-10000", NEW),
            ("orcest-worker-10001", OLD),
            ("orcest-worker-10002", NEW),
        ],
    )

    report = collect_source_health(_configured_fleet())

    assert report.healthy is False
    assert [row.status for row in report.runtimes if row.runtime_class == "worker"] == [
        "current",
        "stale",
        "current",
    ]


def test_health_collection_does_not_call_mutation_methods(mocker):
    _patch_runtime_reads(mocker)
    upload = mocker.patch("orcest.fleet.orchestrator.upload_source")
    restart = mocker.patch("orcest.fleet.orchestrator.restart_stack")
    destroy = mocker.patch("orcest.fleet.proxmox_api.ProxmoxClient.destroy_vm")

    assert collect_source_health(_configured_fleet()).healthy is True

    upload.assert_not_called()
    restart.assert_not_called()
    destroy.assert_not_called()


def test_unknown_and_dirty_runtime_revisions_fail_closed(mocker):
    _patch_runtime_reads(
        mocker,
        project=None,
        workers=[("orcest-worker-10000", None)],
    )

    report = collect_source_health(_configured_fleet())

    assert report.healthy is False
    assert sum(row.status == "unknown" for row in report.runtimes) == 2


def test_unresolved_desired_ref_cannot_be_healthy(mocker):
    _patch_runtime_reads(mocker)
    unresolved = DesiredRevision(
        configured=True,
        repository="Org/orcest",
        ref="refs/heads/master",
        revision=None,
        status="timeout",
    )

    report = collect_source_health(_configured_fleet(), desired=unresolved)

    assert report.healthy is False
    assert report.diagnostics[0] == "timeout"
    assert all(row.status == "unverified" for row in report.runtimes)
