import pytest
import redis as redis_lib

from orcest.rollout_health import collect_rollout_health

pytestmark = pytest.mark.unit


def test_rollout_health_passes_clean_quiescent_snapshot(fake_redis_client, mocker):
    revision = "a" * 40
    mocker.patch("orcest.rollout_health.get_build_revision", return_value=revision)

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
    fake_redis_client.xadd(
        "dead-letter",
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
    assert report["metrics"]["inspection_errors"] == ["orcest:tasks:claude: ResponseError"]
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
    fake_redis_client.xadd("dead-letter", {"id": "task-1"})
    original_xlen = fake_redis_client.client.xlen

    def denied_dead_letter(key):
        if key == "test:dead-letter":
            raise redis_lib.ResponseError("NOPERM hidden detail")
        return original_xlen(key)

    mocker.patch.object(fake_redis_client.client, "xlen", side_effect=denied_dead_letter)

    report = collect_rollout_health(
        fake_redis_client,
        expected_revision=revision,
        baseline_dead_letters=1,
    )

    assert report["ok"] is False
    assert "test:dead-letter: ResponseError" in report["metrics"]["inspection_errors"]
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
