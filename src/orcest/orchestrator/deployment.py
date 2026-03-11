"""Post-merge deployment step.

Runs a configurable shell command on the orchestrator host after a PR merges,
verifies success via an optional HTTP health check, and executes a rollback
command if the health check fails.
"""

import logging
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen

from orcest.shared.config import DeploymentConfig

_DEPLOY_TIMEOUT_SECONDS = 300  # 5 minutes max for deploy/rollback commands


class DeploymentError(Exception):
    """Raised when deployment or health check fails."""


def run_deployment(
    config: DeploymentConfig,
    pr_number: int,
    logger: logging.Logger,
) -> None:
    """Run the deployment command, health check, and rollback if needed.

    Does nothing when deployment is disabled or no command is configured.

    Raises:
        DeploymentError: If the deploy command fails or health check does not
            pass within the configured timeout (rollback is attempted first).
    """
    if not config.enabled or not config.command:
        return

    logger.info("PR #%d: running deployment: %s", pr_number, config.command)
    _run_command(config.command, "deployment", pr_number, logger)
    logger.info("PR #%d: deployment command succeeded", pr_number)

    if not config.health_check_url:
        return

    logger.info(
        "PR #%d: health check %s (timeout: %ds)",
        pr_number,
        config.health_check_url,
        config.health_check_timeout,
    )
    healthy = _wait_for_healthy(config.health_check_url, config.health_check_timeout, logger)
    if healthy:
        logger.info("PR #%d: health check passed", pr_number)
        return

    logger.error(
        "PR #%d: health check failed after %ds — %s",
        pr_number,
        config.health_check_timeout,
        config.health_check_url,
    )

    if config.rollback_command:
        logger.info("PR #%d: running rollback: %s", pr_number, config.rollback_command)
        try:
            _run_command(config.rollback_command, "rollback", pr_number, logger)
            logger.info("PR #%d: rollback succeeded", pr_number)
        except DeploymentError as exc:
            logger.error("PR #%d: rollback also failed: %s", pr_number, exc)

    raise DeploymentError(
        f"Health check failed: {config.health_check_url} did not respond healthy "
        f"within {config.health_check_timeout}s"
    )


def _run_command(command: str, label: str, pr_number: int, logger: logging.Logger) -> None:
    """Run a shell command, raising DeploymentError on failure or timeout."""
    try:
        result = subprocess.run(
            command,
            # shell=True is intentional: command is operator-controlled YAML;
            # do not source this value from untrusted input (e.g. PR labels).
            shell=True,  # noqa: S603 S605
            capture_output=True,
            text=True,
            timeout=_DEPLOY_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise DeploymentError(f"{label} command timed out after {_DEPLOY_TIMEOUT_SECONDS}s")

    if result.returncode != 0:
        stderr_excerpt = (result.stderr.strip() or result.stdout.strip())[:300]
        raise DeploymentError(
            f"{label} command failed (exit {result.returncode}): {stderr_excerpt}"
        )


def _wait_for_healthy(url: str, timeout_seconds: int, logger: logging.Logger) -> bool:
    """Poll the health check URL until it returns 2xx or the timeout expires.

    Returns True if healthy, False if the timeout was reached.
    """
    deadline = time.monotonic() + timeout_seconds
    poll_interval = 2  # seconds between attempts

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            with urlopen(url, timeout=min(5, remaining)) as resp:  # noqa: S310
                if 200 <= resp.status < 300:
                    return True
        except (URLError, OSError):
            pass

        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(poll_interval, remaining))

    return False
