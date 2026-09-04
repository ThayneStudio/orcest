import json

import pytest
import redis as redis_lib

from orcest.rollout_health import collect_rollout_health
from orcest.shared.provider_versions import desired_provider_cli_version
from orcest.shared.result_stream_health import (
    RESULT_PENDING_STALE_DELIVERIES,
    RESULT_PENDING_STALE_IDLE_SECONDS,
)

pytestmark = pytest.mark.unit


def _provider_cli(provider: str, **overrides):
    version = desired_provider_cli_version(provider)
    payload = {
        "schema": 1,
        "provider": provider,
        "desired_version": version,
        "template_version": version,
        "observed_version": version,
        "status": "ok",
    }
    payload.update(overrides)
    return payload


def test_rollout_health_passes_clean_quiescent_snapshot(fake_redis_client, mocker):
    revision = "a" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        expected_pool_size=0,
        baseline_dead_letters=0,
        baseline_exhausted_skips=0,
        baseline_rebake_failures=0,
        require_quiescent=True,
    )

    assert report["ok"] is True
    assert all(check["passed"] for check in report["checks"])


def test_rollout_health_fails_on_private_state_and_dlq_growth(fake_redis_client, mocker):
    revision = "b" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.set("test:results:private-credential-recovery:secret", "opaque")
    fake_redis_client.client.xadd(
        "orcest:dead-letter",
        {"id": "task-1", "type": "review", "repo": "org/repo"},
    )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        baseline_dead_letters=0,
    )

    assert report["ok"] is False
    failed = {check["name"] for check in report["checks"] if not check["passed"]}
    assert failed == {"private_recovery_state", "dead_letters"}


def test_rollout_health_counts_shared_prefix_credential_checkpoints(fake_redis_client, mocker):
    """Checkpoints for tasks without a project key prefix live under the shared
    task prefix; a project-prefixed health check must still count them."""
    revision = "9" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.set("orcest:results:private-credential-recovery:abc", "opaque")
    fake_redis_client.client.set("orcest:results:credential-recovery-intent:abc", "opaque")

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["metrics"]["private_credential_checkpoints"] == 1
    assert report["metrics"]["credential_recovery_intents"] == 1
    recovery = next(c for c in report["checks"] if c["name"] == "private_recovery_state")
    assert recovery["passed"] is False
    assert report["ok"] is False


def test_rollout_health_counts_each_credential_checkpoint_once(fake_redis_client, mocker):
    """When the project prefix and task prefix coincide, the two scans overlap
    and each key must still count exactly once."""
    revision = "9" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.set("test:results:private-credential-recovery:abc", "opaque")

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        max_private_recovery=1,
    )

    assert report["metrics"]["private_credential_checkpoints"] == 1
    recovery = next(c for c in report["checks"] if c["name"] == "private_recovery_state")
    assert recovery["passed"] is True


def test_rollout_health_allows_fresh_pending_result(fake_redis_client, mocker):
    revision = "1" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    fake_redis_client.xadd("results", {"task_id": "task-1", "summary": "secret body"})
    fake_redis_client.xreadgroup("orchestrator", "orchestrator-main", "results", block_ms=None)

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is True
    assert report["metrics"]["result_pending"] == 1
    assert report["metrics"]["result_lag"] == 0
    assert report["metrics"]["result_max_delivery_count"] == 1
    fresh = next(c for c in report["checks"] if c["name"] == "result_handling_fresh")
    assert fresh["passed"] is True


def test_rollout_health_fails_on_stale_result_idle_age(fake_redis_client, mocker):
    revision = "2" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    fake_redis_client.xadd("results", {"task_id": "task-1"})
    fake_redis_client.xreadgroup("orchestrator", "orchestrator-main", "results", block_ms=None)
    mocker.patch.object(
        fake_redis_client.client,
        "xpending_range",
        return_value=[
            {
                "message_id": "1-0",
                "consumer": "orchestrator-main",
                "time_since_delivered": RESULT_PENDING_STALE_IDLE_SECONDS * 1000,
                "times_delivered": 1,
            }
        ],
    )

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is False
    assert report["metrics"]["result_oldest_pending_idle_seconds"] == (
        RESULT_PENDING_STALE_IDLE_SECONDS
    )
    fresh = next(c for c in report["checks"] if c["name"] == "result_handling_fresh")
    assert fresh["passed"] is False


def test_rollout_health_fails_on_stale_result_delivery_count(fake_redis_client, mocker):
    revision = "3" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    fake_redis_client.xadd("results", {"task_id": "task-1"})
    fake_redis_client.xreadgroup("orchestrator", "orchestrator-main", "results", block_ms=None)
    mocker.patch.object(
        fake_redis_client.client,
        "xpending_range",
        return_value=[
            {
                "message_id": "1-0",
                "consumer": "orchestrator-main",
                "time_since_delivered": 0,
                "times_delivered": RESULT_PENDING_STALE_DELIVERIES,
            }
        ],
    )

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is False
    assert report["metrics"]["result_max_delivery_count"] == RESULT_PENDING_STALE_DELIVERIES
    fresh = next(c for c in report["checks"] if c["name"] == "result_handling_fresh")
    assert fresh["passed"] is False


def test_rollout_health_treats_acked_retained_results_as_no_work(fake_redis_client, mocker):
    revision = "4" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    fake_redis_client.xadd("results", {"task_id": "task-1"})
    [(entry_id, _fields)] = fake_redis_client.xreadgroup(
        "orchestrator", "orchestrator-main", "results", block_ms=None
    )
    fake_redis_client.xack("results", "orchestrator", entry_id)

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is True
    assert report["metrics"]["result_retained_entries"] == 1
    assert report["metrics"]["result_work"] == 0
    assert report["metrics"]["result_pending"] == 0


def test_rollout_health_allows_missing_result_stream_without_quiescence(mocker, fake_redis_client):
    revision = "5" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is True
    assert report["metrics"]["result_retained_entries"] == 0
    assert report["metrics"]["result_work"] == 0
    assert report["metrics"]["result_stream_warning"] is None


def test_rollout_health_fails_revision_mismatch(fake_redis_client, mocker):
    mocker.patch("orcest.rollout_health.get_build_revision", return_value="c" * 40)

    report = collect_rollout_health(fake_redis_client, expected_revision="d" * 40)

    revision_check = next(c for c in report["checks"] if c["name"] == "checker_revision")
    assert revision_check["passed"] is False
    assert report["ok"] is False


def test_rollout_health_fails_for_unconsumed_shared_task_stream(fake_redis_client, mocker):
    revision = "e" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.xadd("orcest:tasks:grok", {"id": "task-1"})

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        require_quiescent=True,
    )

    assert report["metrics"]["unconsumed_task_streams"] == ["orcest:tasks:grok"]
    failed = {check["name"] for check in report["checks"] if not check["passed"]}
    assert {"consumer_groups", "queue_quiescent"} <= failed


def test_rollout_health_fails_when_group_has_work_but_no_consumers(fake_redis_client, mocker):
    revision = "5" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.xadd("orcest:tasks:codex", {"id": "task-1"})
    mocker.patch.object(
        fake_redis_client.client,
        "xinfo_groups",
        return_value=[
            {
                "name": "workers",
                "consumers": 0,
                "pending": 0,
                "lag": 1,
            }
        ],
    )

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["metrics"]["unconsumed_task_streams"] == ["orcest:tasks:codex"]
    consumer_groups = next(c for c in report["checks"] if c["name"] == "consumer_groups")
    assert consumer_groups["passed"] is False
    assert report["ok"] is False


def test_rollout_health_fails_when_result_group_has_work_but_no_consumers(
    fake_redis_client, mocker
):
    revision = "7" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    mocker.patch.object(
        fake_redis_client.client,
        "xinfo_groups",
        return_value=[
            {
                "name": "orchestrator",
                "consumers": 0,
                "pending": 0,
                "lag": 1,
            }
        ],
    )

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["metrics"]["unconsumed_results"] is True
    consumer_groups = next(c for c in report["checks"] if c["name"] == "consumer_groups")
    assert consumer_groups["passed"] is False
    assert report["ok"] is False


def test_rollout_health_requires_each_expected_backend_consumer(fake_redis_client, mocker):
    revision = "6" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    worker_ids = {
        "clauder": "orcest-worker-300",
        "codex": "orcest-worker-301",
        "grok": "orcest-worker-302",
    }
    for backend in ("clauder", "codex", "grok"):
        worker_id = worker_ids[backend]
        fake_redis_client.set_ex(
            f"workers:heartbeat:{worker_id}",
            json.dumps({"backend": backend, "revision": revision}),
            ttl=150,
        )
        for stream in (f"tasks:{backend}", f"tasks:issue:{backend}"):
            fake_redis_client.ensure_consumer_group(stream, "workers")
            fake_redis_client.xreadgroup(
                group="workers",
                consumer=worker_id,
                stream=stream,
                count=1,
                block_ms=None,
            )
    fake_redis_client.client.xgroup_delconsumer(
        "test:tasks:issue:grok", "workers", "orcest-worker-302"
    )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("clauder", "codex", "grok"),
    )

    backend_check = next(c for c in report["checks"] if c["name"] == "worker_backends")
    assert backend_check["passed"] is False
    assert backend_check["actual"] == ["grok"]
    assert report["metrics"]["backend_consumers"]["codex"] == {"pr": 1, "issue": 1}


def test_rollout_health_rejects_stale_consumer_without_live_heartbeat(fake_redis_client, mocker):
    revision = "7" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    for stream in ("tasks:grok", "tasks:issue:grok"):
        fake_redis_client.ensure_consumer_group(stream, "workers")
        fake_redis_client.xreadgroup(
            group="workers",
            consumer="orcest-worker-grok",
            stream=stream,
            count=1,
            block_ms=None,
        )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("grok",),
    )

    backend_check = next(c for c in report["checks"] if c["name"] == "worker_backends")
    assert backend_check["passed"] is False
    assert report["metrics"]["backend_consumers"]["grok"] == {"pr": 0, "issue": 0}
    assert report["metrics"]["backend_heartbeats"]["grok"] == 0


def test_rollout_health_requires_heartbeat_from_candidate_revision(fake_redis_client, mocker):
    revision = "8" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.set_ex(
        "workers:heartbeat:orcest-worker-300",
        json.dumps({"backend": "grok", "revision": "9" * 40}),
        ttl=150,
    )
    for stream in ("tasks:grok", "tasks:issue:grok"):
        fake_redis_client.ensure_consumer_group(stream, "workers")
        fake_redis_client.xreadgroup(
            group="workers",
            consumer="orcest-worker-300",
            stream=stream,
            count=1,
            block_ms=None,
        )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("grok",),
    )

    assert report["ok"] is False
    assert report["metrics"]["worker_revision_mismatches"] == ["orcest-worker-300"]


def test_rollout_health_rejects_old_worker_even_when_candidate_backend_is_present(
    fake_redis_client, mocker
):
    revision = "a" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    for worker_id, worker_revision in (
        ("orcest-worker-300", revision),
        ("orcest-worker-301", "b" * 40),
    ):
        fake_redis_client.set_ex(
            f"workers:heartbeat:{worker_id}",
            json.dumps({"backend": "grok", "revision": worker_revision}),
            ttl=150,
        )
    for stream in ("tasks:grok", "tasks:issue:grok"):
        fake_redis_client.ensure_consumer_group(stream, "workers")
        fake_redis_client.xreadgroup(
            group="workers",
            consumer="orcest-worker-300",
            stream=stream,
            count=1,
            block_ms=None,
        )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("grok",),
    )

    revision_check = next(c for c in report["checks"] if c["name"] == "worker_revisions")
    assert revision_check["passed"] is False
    assert revision_check["actual"] == ["orcest-worker-301"]


def test_rollout_health_enforces_repeated_backend_capacity(fake_redis_client, mocker):
    revision = "c" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.set_ex(
        "workers:heartbeat:orcest-worker-300",
        json.dumps({"backend": "clauder", "revision": revision}),
        ttl=150,
    )
    for stream in ("tasks:clauder", "tasks:issue:clauder"):
        fake_redis_client.ensure_consumer_group(stream, "workers")
        fake_redis_client.xreadgroup(
            group="workers",
            consumer="orcest-worker-300",
            stream=stream,
            count=1,
            block_ms=None,
        )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("clauder", "clauder"),
    )

    backend_check = next(c for c in report["checks"] if c["name"] == "worker_backends")
    assert backend_check["passed"] is False
    assert report["metrics"]["expected_backend_counts"] == {"clauder": 2}


def test_rollout_health_rejects_excess_backend_capacity(fake_redis_client, mocker):
    revision = "d" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    for vmid in (300, 301):
        worker_id = f"orcest-worker-{vmid}"
        fake_redis_client.set_ex(
            f"workers:heartbeat:{worker_id}",
            json.dumps({"backend": "codex", "revision": revision}),
            ttl=150,
        )
        for stream in ("tasks:codex", "tasks:issue:codex"):
            fake_redis_client.ensure_consumer_group(stream, "workers")
            fake_redis_client.xreadgroup(
                group="workers",
                consumer=worker_id,
                stream=stream,
                count=1,
                block_ms=None,
            )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("codex",),
    )

    backend_check = next(c for c in report["checks"] if c["name"] == "worker_backends")
    assert backend_check["passed"] is False
    assert backend_check["actual"] == ["codex"]


def test_rollout_health_rejects_unexpected_live_backend(fake_redis_client, mocker):
    revision = "e" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    for vmid, backend in ((300, "codex"), (301, "grok")):
        worker_id = f"orcest-worker-{vmid}"
        fake_redis_client.set_ex(
            f"workers:heartbeat:{worker_id}",
            json.dumps({"backend": backend, "revision": revision}),
            ttl=150,
        )
        for stream in (f"tasks:{backend}", f"tasks:issue:{backend}"):
            fake_redis_client.ensure_consumer_group(stream, "workers")
            fake_redis_client.xreadgroup(
                group="workers",
                consumer=worker_id,
                stream=stream,
                count=1,
                block_ms=None,
            )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("codex",),
    )

    backend_check = next(c for c in report["checks"] if c["name"] == "worker_backends")
    assert backend_check["passed"] is False
    assert backend_check["actual"] == ["orcest-worker-301:grok"]
    assert report["metrics"]["unexpected_worker_backends"] == ["orcest-worker-301:grok"]


def test_rollout_health_accepts_matching_provider_cli_versions(fake_redis_client, mocker):
    revision = "1" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    for index, backend in enumerate(("claude", "grok", "codex"), start=300):
        worker_id = f"orcest-worker-{index}"
        fake_redis_client.set_ex(
            f"workers:heartbeat:{worker_id}",
            json.dumps(
                {
                    "backend": backend,
                    "revision": revision,
                    "provider_cli": _provider_cli(backend),
                }
            ),
            ttl=150,
        )
        for stream in (f"tasks:{backend}", f"tasks:issue:{backend}"):
            fake_redis_client.ensure_consumer_group(stream, "workers")
            fake_redis_client.xreadgroup(
                group="workers",
                consumer=worker_id,
                stream=stream,
                count=1,
                block_ms=None,
            )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("claude", "grok", "codex"),
    )

    check = next(c for c in report["checks"] if c["name"] == "provider_cli_versions")
    assert check["passed"] is True
    assert report["metrics"]["provider_cli_diagnostics"] == []


@pytest.mark.parametrize(
    ("payload_overrides", "expected_fragment"),
    [
        (
            {"template_version": "0.131.0", "status": "version_mismatch"},
            "desired provider CLI version 0.149.1 != baked template 0.131.0",
        ),
        (
            {"observed_version": "0.150.0", "status": "version_mismatch"},
            "desired provider CLI version 0.149.1 != observed executable 0.150.0",
        ),
        (
            {
                "template_version": "0.150.0",
                "observed_version": "0.149.1",
                "status": "version_mismatch",
            },
            "baked template provider CLI version 0.150.0 != observed executable 0.149.1",
        ),
    ],
)
def test_rollout_health_fails_provider_cli_pairwise_mismatch(
    fake_redis_client, mocker, payload_overrides, expected_fragment
):
    revision = "2" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.set_ex(
        "workers:heartbeat:orcest-worker-300",
        json.dumps(
            {
                "backend": "codex",
                "revision": revision,
                "provider_cli": _provider_cli("codex", **payload_overrides),
            }
        ),
        ttl=150,
    )
    for stream in ("tasks:codex", "tasks:issue:codex"):
        fake_redis_client.ensure_consumer_group(stream, "workers")
        fake_redis_client.xreadgroup(
            group="workers",
            consumer="orcest-worker-300",
            stream=stream,
            count=1,
            block_ms=None,
        )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("codex",),
    )

    check = next(c for c in report["checks"] if c["name"] == "provider_cli_versions")
    assert check["passed"] is False
    assert any(expected_fragment in item for item in check["actual"])
    assert report["ok"] is False


@pytest.mark.parametrize(
    "status",
    ["missing_template_metadata", "probe_timeout", "probe_output_unparseable"],
)
def test_rollout_health_fails_provider_cli_probe_status(fake_redis_client, mocker, status):
    revision = "3" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.set_ex(
        "workers:heartbeat:orcest-worker-300",
        json.dumps(
            {
                "backend": "grok",
                "revision": revision,
                "provider_cli": _provider_cli(
                    "grok",
                    template_version=None if status == "missing_template_metadata" else "0.1.216",
                    observed_version=None if status != "ok" else "0.1.216",
                    status=status,
                ),
            }
        ),
        ttl=150,
    )
    for stream in ("tasks:grok", "tasks:issue:grok"):
        fake_redis_client.ensure_consumer_group(stream, "workers")
        fake_redis_client.xreadgroup(
            group="workers",
            consumer="orcest-worker-300",
            stream=stream,
            count=1,
            block_ms=None,
        )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("grok",),
    )

    assert report["ok"] is False
    assert any(status in item for item in report["metrics"]["provider_cli_diagnostics"])


def test_rollout_health_aggregates_repeated_provider_cli_diagnostics(fake_redis_client, mocker):
    revision = "4" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    for vmid in (300, 301):
        worker_id = f"orcest-worker-{vmid}"
        fake_redis_client.set_ex(
            f"workers:heartbeat:{worker_id}",
            json.dumps(
                {
                    "backend": "codex",
                    "revision": revision,
                    "provider_cli": _provider_cli(
                        "codex",
                        observed_version=None,
                        status="missing_binary",
                    ),
                }
            ),
            ttl=150,
        )
        for stream in ("tasks:codex", "tasks:issue:codex"):
            fake_redis_client.ensure_consumer_group(stream, "workers")
            fake_redis_client.xreadgroup(
                group="workers",
                consumer=worker_id,
                stream=stream,
                count=1,
                block_ms=None,
            )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("codex", "codex"),
    )

    diagnostics = report["metrics"]["provider_cli_diagnostics"]
    status_lines = [
        item for item in diagnostics if "provider CLI probe status missing_binary" in item
    ]
    assert status_lines == [
        "provider CLI probe status missing_binary; rebake required: "
        "orcest-worker-300/codex, orcest-worker-301/codex"
    ]


def test_rollout_health_skips_provider_cli_for_mixed_revision_worker(fake_redis_client, mocker):
    revision = "5" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.set_ex(
        "workers:heartbeat:orcest-worker-300",
        json.dumps(
            {
                "backend": "codex",
                "revision": "6" * 40,
                "provider_cli": _provider_cli(
                    "codex",
                    template_version="0.131.0",
                    observed_version="0.131.0",
                    status="version_mismatch",
                ),
            }
        ),
        ttl=150,
    )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_backends=("codex",),
    )

    assert report["metrics"]["provider_cli_diagnostics"] == []
    assert report["metrics"]["worker_revision_mismatches"] == ["orcest-worker-300"]


def test_rollout_health_does_not_let_stray_heartbeat_mask_dead_pool_slot(fake_redis_client, mocker):
    revision = "f" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.sadd("test:pool:idle", "300")
    fake_redis_client.set_ex(
        "workers:heartbeat:orcest-worker-999",
        json.dumps({"backend": "codex", "revision": revision}),
        ttl=150,
    )
    for stream in ("tasks:codex", "tasks:issue:codex"):
        fake_redis_client.ensure_consumer_group(stream, "workers")
        fake_redis_client.xreadgroup(
            group="workers",
            consumer="orcest-worker-999",
            stream=stream,
            count=1,
            block_ms=None,
        )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_pool_size=1,
        expected_vmid_start=300,
        expected_backends=("codex",),
    )

    backend_check = next(c for c in report["checks"] if c["name"] == "worker_backends")
    assert backend_check["passed"] is False
    assert report["metrics"]["backend_heartbeats"]["codex"] == 0
    assert report["metrics"]["unexpected_worker_backends"] == ["orcest-worker-999:codex"]
    assert report["metrics"]["worker_layout_mismatches"] == ["orcest-worker-300:missing!=codex"]


def test_rollout_health_rejects_malformed_heartbeat_key_impersonating_slot(
    fake_redis_client, mocker
):
    revision = "f" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.sadd("orcest:pool:idle", "300")
    fake_redis_client.client.set(
        "orcest:workers:heartbeat:other:orcest-worker-300",
        json.dumps({"backend": "codex", "revision": revision}),
    )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="orcest",
        expected_pool_size=1,
        expected_vmid_start=300,
        expected_backends=("codex",),
    )

    assert report["ok"] is False
    assert report["metrics"]["backend_heartbeats"]["codex"] == 0
    assert report["metrics"]["worker_layout_mismatches"] == ["orcest-worker-300:missing!=codex"]
    assert report["metrics"]["inspection_errors"] == [
        "orcest:workers:heartbeat:other:orcest-worker-300: malformed heartbeat key"
    ]


def test_rollout_health_fails_closed_when_stream_inspection_is_denied(fake_redis_client, mocker):
    revision = "f" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.xadd("orcest:tasks:claude", {"id": "task-1"})
    mocker.patch.object(
        fake_redis_client.client,
        "xinfo_groups",
        side_effect=redis_lib.ResponseError("NOPERM secret detail"),
    )

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        require_quiescent=True,
    )

    failed = {check["name"] for check in report["checks"] if not check["passed"]}
    assert "redis_inspection" in failed
    assert report["ok"] is False
    # The quiescence gate also refuses to read an absent project result stream
    # or an absent worker pool keyspace as "nothing is running".
    assert report["metrics"]["inspection_errors"] == [
        "orcest:pool:idle/orcest:pool:active: worker pool state is absent",
        "orcest:tasks:claude: ResponseError",
        "test:results: stream is absent",
    ]
    assert "secret detail" not in str(report)


def test_rollout_health_fails_closed_when_consumer_lag_is_unknown(fake_redis_client, mocker):
    revision = "1" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.xadd("orcest:tasks:claude", {"id": "task-1"})
    mocker.patch.object(
        fake_redis_client.client,
        "xinfo_groups",
        return_value=[{"name": "workers", "pending": 0, "lag": None}],
    )

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    inspection = next(c for c in report["checks"] if c["name"] == "redis_inspection")
    assert inspection["passed"] is False
    assert report["ok"] is False


def test_rollout_health_fails_closed_when_dead_letter_length_is_denied(fake_redis_client, mocker):
    revision = "2" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.xadd("orcest:dead-letter", {"id": "task-1"})
    original_xlen = fake_redis_client.client.xlen

    def denied_dead_letter(key):
        if key == "orcest:dead-letter":
            raise redis_lib.ResponseError("NOPERM hidden detail")
        return original_xlen(key)

    mocker.patch.object(fake_redis_client.client, "xlen", side_effect=denied_dead_letter)

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        baseline_dead_letters=1,
    )

    assert report["ok"] is False
    assert "orcest:dead-letter: ResponseError" in report["metrics"]["inspection_errors"]
    assert "hidden detail" not in str(report)


def test_rollout_health_fails_closed_when_provider_counter_is_malformed(fake_redis_client, mocker):
    revision = "3" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.set_value("providers:claude:exhausted_skip", "not-a-number")

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        baseline_exhausted_skips=0,
    )

    assert report["ok"] is False
    assert "providers:*:exhausted_skip: ValueError" in report["metrics"]["inspection_errors"]


def test_rollout_health_fails_closed_when_results_key_has_wrong_type(fake_redis_client, mocker):
    revision = "4" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.set("test:results", "corrupt")

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        require_quiescent=True,
    )

    assert report["ok"] is False
    assert "test:results: expected stream, found string" in report["metrics"]["inspection_errors"]


def test_rollout_health_fails_closed_when_result_stream_has_no_group(fake_redis_client, mocker):
    revision = "4" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.xadd("results", {"task_id": "task-1"})

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is False
    assert (
        "test:results: results consumer group 'orchestrator' is missing"
        in report["metrics"]["inspection_errors"]
    )


def test_rollout_health_fails_closed_when_result_group_inspection_errors(fake_redis_client, mocker):
    revision = "5" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    original_xinfo_groups = fake_redis_client.client.xinfo_groups

    def fail_results_group(stream):
        if stream == "test:results":
            raise redis_lib.ResponseError("NOPERM secret detail")
        return original_xinfo_groups(stream)

    mocker.patch.object(fake_redis_client.client, "xinfo_groups", side_effect=fail_results_group)

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is False
    assert "test:results: ResponseError" in report["metrics"]["inspection_errors"]
    assert "secret detail" not in str(report)


def test_rollout_health_fails_closed_when_result_pending_metadata_is_malformed(
    fake_redis_client, mocker
):
    revision = "5" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    fake_redis_client.xadd("results", {"task_id": "task-1"})
    fake_redis_client.xreadgroup("orchestrator", "orchestrator-main", "results", block_ms=None)
    mocker.patch.object(
        fake_redis_client.client,
        "xpending_range",
        return_value=[{"message_id": "1-0", "time_since_delivered": "bad"}],
    )

    report = collect_rollout_health(fake_redis_client, expected_revision=revision)

    assert report["ok"] is False
    assert (
        "test:results: pending result metadata is malformed"
        in report["metrics"]["inspection_errors"]
    )


def test_rollout_health_reads_pool_state_under_the_pool_prefix(fake_redis_client, mocker):
    revision = "5" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    fake_redis_client.client.hset("fleet:pool:active", "10000", "1750000000.0")
    fake_redis_client.client.sadd("fleet:pool:idle", "10001")

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        pool_prefix="fleet",
        expected_pool_size=2,
        require_quiescent=True,
    )

    assert report["metrics"]["pool_active"] == 1
    assert report["metrics"]["pool_idle"] == 1
    assert report["metrics"]["inspection_errors"] == []
    pool_size = next(c for c in report["checks"] if c["name"] == "pool_size")
    assert pool_size["passed"] is True
    quiescent = next(c for c in report["checks"] if c["name"] == "pool_quiescent")
    assert quiescent["passed"] is False
    assert report["ok"] is False


def test_rollout_health_fails_closed_when_pool_state_is_absent(fake_redis_client, mocker):
    """A mismatched pool prefix must not read as an empty, safe-to-destroy fleet."""
    revision = "6" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")
    # Real pool state lives under the pool manager's own prefix.
    fake_redis_client.client.hset("fleet:pool:active", "10000", "1750000000.0")

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        task_prefix="test",
        expected_pool_size=1,
        require_quiescent=True,
    )

    assert report["metrics"]["pool_active"] == 0
    assert report["metrics"]["inspection_errors"] == [
        "test:pool:idle/test:pool:active: worker pool state is absent"
    ]
    inspection = next(c for c in report["checks"] if c["name"] == "redis_inspection")
    assert inspection["passed"] is False
    assert report["ok"] is False


def test_rollout_health_allows_absent_pool_state_for_an_explicitly_empty_pool(
    fake_redis_client, mocker
):
    revision = "7" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.ensure_consumer_group("results", "orchestrator")

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        expected_pool_size=0,
        require_quiescent=True,
    )

    assert report["metrics"]["inspection_errors"] == []
    assert report["ok"] is True


def test_rollout_health_fails_closed_when_project_result_stream_is_absent(
    fake_redis_client, mocker
):
    """A wrong project prefix must not report a backlogged project as quiescent."""
    revision = "8" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)
    fake_redis_client.client.sadd("orcest:pool:idle", "10000")
    # Real results live under the project prefix, not the one being inspected.
    fake_redis_client.client.xadd("otherproject:results", {"task_id": "task-1"})

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        require_quiescent=True,
    )

    assert report["metrics"]["result_work"] == 0
    assert report["metrics"]["inspection_errors"] == ["test:results: stream is absent"]
    inspection = next(c for c in report["checks"] if c["name"] == "redis_inspection")
    assert inspection["passed"] is False
    assert report["ok"] is False
