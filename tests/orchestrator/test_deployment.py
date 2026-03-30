"""Tests for orcest.orchestrator.deployment."""

import logging
import subprocess

import pytest

from orcest.orchestrator.deployment import DeploymentError, _wait_for_healthy, run_deployment
from orcest.shared.config import DeploymentConfig


def _config(**kwargs: object) -> DeploymentConfig:
    """Build a DeploymentConfig with sensible defaults for testing."""
    defaults: dict = {
        "enabled": True,
        "command": "true",  # no-op shell command that succeeds
        "health_check_url": "",
        "health_check_timeout": 1,
        "rollback_command": "",
    }
    defaults.update(kwargs)
    return DeploymentConfig(**defaults)


logger = logging.getLogger("test")


# ---------------------------------------------------------------------------
# run_deployment — disabled / no command
# ---------------------------------------------------------------------------


def test_run_deployment_disabled_is_noop():
    """run_deployment does nothing when deployment is disabled."""
    config = _config(enabled=False, command="false")  # 'false' exits 1 — would raise
    result = run_deployment(config, pr_number=1, logger=logger)
    assert result is False


def test_run_deployment_no_command_is_noop():
    """run_deployment does nothing when command is empty string."""
    config = _config(enabled=True, command="")
    result = run_deployment(config, pr_number=2, logger=logger)
    assert result is False


# ---------------------------------------------------------------------------
# run_deployment — deploy command success/failure
# ---------------------------------------------------------------------------


def test_run_deployment_success():
    """run_deployment succeeds when the deploy command exits 0 and no health check is configured."""
    config = _config(command="true")
    result = run_deployment(config, pr_number=3, logger=logger)
    assert result is True


def test_run_deployment_command_failure():
    """run_deployment raises DeploymentError when deploy command exits non-zero."""
    config = _config(command="false")  # exits 1
    with pytest.raises(DeploymentError, match="deployment command failed"):
        run_deployment(config, pr_number=4, logger=logger)


def test_run_deployment_command_timeout(mocker):
    """run_deployment raises DeploymentError when deploy command times out."""
    mocker.patch(
        "orcest.orchestrator.deployment.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="deploy.sh", timeout=300),
    )
    config = _config(command="deploy.sh")
    with pytest.raises(DeploymentError, match="timed out"):
        run_deployment(config, pr_number=5, logger=logger)


# ---------------------------------------------------------------------------
# run_deployment — health check
# ---------------------------------------------------------------------------


def test_run_deployment_invalid_health_check_url_scheme():
    """run_deployment raises DeploymentError when health_check_url is not http/https."""
    config = _config(command="true", health_check_url="file:///etc/shadow")
    with pytest.raises(DeploymentError, match="health_check_url must use http"):
        run_deployment(config, pr_number=6, logger=logger)


def test_run_deployment_skips_health_check_when_no_url():
    """No health check is performed when health_check_url is empty."""
    config = _config(command="true", health_check_url="")
    # Must complete without error (no HTTP call attempted)
    run_deployment(config, pr_number=6, logger=logger)


def test_run_deployment_health_check_passes(mocker):
    """run_deployment succeeds when health check returns 2xx."""
    mocker.patch(
        "orcest.orchestrator.deployment._wait_for_healthy",
        return_value=True,
    )
    config = _config(command="true", health_check_url="http://localhost/health")
    run_deployment(config, pr_number=7, logger=logger)


def test_run_deployment_health_check_fails_raises(mocker):
    """run_deployment raises DeploymentError when health check fails (no rollback configured)."""
    mocker.patch(
        "orcest.orchestrator.deployment._wait_for_healthy",
        return_value=False,
    )
    config = _config(
        command="true",
        health_check_url="http://localhost/health",
        rollback_command="",
    )
    with pytest.raises(DeploymentError, match="Health check failed"):
        run_deployment(config, pr_number=8, logger=logger)


def test_run_deployment_health_check_fails_runs_rollback(mocker):
    """When health check fails, the rollback command is executed."""
    mocker.patch(
        "orcest.orchestrator.deployment._wait_for_healthy",
        return_value=False,
    )
    mock_run = mocker.patch("orcest.orchestrator.deployment.subprocess.run")
    mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

    config = _config(
        command="deploy.sh",
        health_check_url="http://localhost/health",
        rollback_command="rollback.sh",
    )
    with pytest.raises(DeploymentError, match="Health check failed"):
        run_deployment(config, pr_number=9, logger=logger)

    # Both deploy and rollback commands should have been run
    calls = [c.args[0] for c in mock_run.call_args_list]
    assert "deploy.sh" in calls
    assert "rollback.sh" in calls


def test_run_deployment_health_check_fails_rollback_also_fails(mocker):
    """When health check fails and rollback command also fails, DeploymentError is still raised."""
    mocker.patch(
        "orcest.orchestrator.deployment._wait_for_healthy",
        return_value=False,
    )
    # Make both deploy (first call) succeed and rollback (second call) fail
    mock_run = mocker.patch("orcest.orchestrator.deployment.subprocess.run")
    mock_run.side_effect = [
        subprocess.CompletedProcess(args=[], returncode=0),  # deploy succeeds
        subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="rollback err"),
    ]

    config = _config(
        command="deploy.sh",
        health_check_url="http://localhost/health",
        rollback_command="rollback.sh",
    )
    with pytest.raises(DeploymentError, match="Health check failed"):
        run_deployment(config, pr_number=10, logger=logger)


# ---------------------------------------------------------------------------
# _wait_for_healthy
# ---------------------------------------------------------------------------


def test_wait_for_healthy_returns_true_on_200(mocker):
    """_wait_for_healthy returns True when the URL responds with 200."""
    mock_resp = mocker.MagicMock()
    mock_resp.__enter__ = mocker.MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = mocker.MagicMock(return_value=False)
    mock_resp.status = 200
    mocker.patch("orcest.orchestrator.deployment.urlopen", return_value=mock_resp)

    result = _wait_for_healthy("http://localhost/health", timeout_seconds=5, logger=logger)
    assert result is True


def test_wait_for_healthy_returns_false_on_timeout(mocker):
    """_wait_for_healthy returns False when connection errors prevent success before timeout.

    Mocks time.monotonic to simulate: one failed poll attempt inside the deadline,
    then the deadline expiring on the next check, to verify the loop actually
    exercises error handling and not just the zero-timeout early-exit path.
    """
    from urllib.error import URLError

    mocker.patch(
        "orcest.orchestrator.deployment.urlopen",
        side_effect=URLError("connection refused"),
    )
    mocker.patch("orcest.orchestrator.deployment.time.sleep")
    # Calls in order:
    #   1. deadline = time.monotonic() + 2  →  0.0 + 2 = 2.0
    #   2. remaining = 2.0 - time.monotonic()  →  2.0 - 0.5 = 1.5  (inside deadline, poll)
    #   3. urlopen raises URLError
    #   4. time.sleep arg: deadline - time.monotonic()  →  2.0 - 1.0 = 1.0
    #   5. remaining = 2.0 - time.monotonic()  →  2.0 - 3.0 = -1.0  (past deadline → break)
    mocker.patch(
        "orcest.orchestrator.deployment.time.monotonic",
        side_effect=[0.0, 0.5, 1.0, 3.0],
    )

    result = _wait_for_healthy("http://localhost/health", timeout_seconds=2, logger=logger)
    assert result is False


# ---------------------------------------------------------------------------
# loop integration: deployment called after successful merge
# ---------------------------------------------------------------------------


def test_poll_cycle_runs_deployment_after_merge(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """After a successful merge, run_deployment is called when deployment is enabled."""
    from orcest.orchestrator.loop import RESULTS_GROUP, RESULTS_STREAM, _poll_cycle
    from orcest.orchestrator.pr_ops import PRAction, PRState
    from orcest.shared.config import DeploymentConfig

    orchestrator_config.deployment = DeploymentConfig(
        enabled=True,
        command="true",
    )

    pr_state = PRState(
        number=200,
        title="PR #200",
        branch="feat/200",
        head_sha="abc200",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )
    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[pr_state])
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mock_deploy = mocker.patch("orcest.orchestrator.loop.run_deployment")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_deploy.assert_called_once_with(orchestrator_config.deployment, 200, mocker.ANY)


def test_poll_cycle_deployment_failure_creates_issue(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """A DeploymentError after merge triggers gh.create_issue."""
    from orcest.orchestrator.loop import RESULTS_GROUP, RESULTS_STREAM, _poll_cycle
    from orcest.orchestrator.pr_ops import PRAction, PRState
    from orcest.shared.config import DeploymentConfig

    orchestrator_config.deployment = DeploymentConfig(
        enabled=True,
        command="false",
    )

    pr_state = PRState(
        number=201,
        title="PR #201",
        branch="feat/201",
        head_sha="abc201",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )
    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[pr_state])
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch(
        "orcest.orchestrator.loop.run_deployment",
        side_effect=DeploymentError("deploy failed"),
    )
    gh_mock.create_issue.return_value = 999
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.create_issue.assert_called_once()
    title, body = (
        gh_mock.create_issue.call_args[0][1],
        gh_mock.create_issue.call_args[0][2],
    )
    assert "201" in title
    assert "deploy failed" in body


def test_poll_cycle_deployment_skipped_when_disabled(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """Deployment is not triggered when deployment.enabled is False."""
    from orcest.orchestrator.loop import RESULTS_GROUP, RESULTS_STREAM, _poll_cycle
    from orcest.orchestrator.pr_ops import PRAction, PRState
    from orcest.shared.config import DeploymentConfig

    orchestrator_config.deployment = DeploymentConfig(enabled=False, command="false")

    pr_state = PRState(
        number=202,
        title="PR #202",
        branch="feat/202",
        head_sha="abc202",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )
    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[pr_state])
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mock_deploy = mocker.patch("orcest.orchestrator.loop.run_deployment")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # Merge happened; run_deployment is called unconditionally but is a no-op when disabled
    gh_mock.merge_pr.assert_called_once()
    mock_deploy.assert_called_once()
    gh_mock.create_issue.assert_not_called()
