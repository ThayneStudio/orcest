"""Orchestrator stack management via SSH.

Provides helpers to manage per-project Docker Compose stacks on the
orchestrator VM. All operations are performed via SSH from the Proxmox
host (where ``orcest fleet`` commands run) to the orchestrator VM.
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile

import yaml

from orcest.fleet.config import require_valid_project_name as _validate_project_name

logger = logging.getLogger(__name__)


_SSH_OPTS = [
    "-o", "ConnectTimeout=5",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "BatchMode=yes",
    "-o", "LogLevel=ERROR",
]


def _ssh(ssh_target: str, cmd: str) -> subprocess.CompletedProcess[str]:
    """Run a command on the orchestrator VM via SSH."""
    logger.debug("ssh %s: %s", ssh_target, cmd)
    return subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, cmd],
        capture_output=True,
        text=True,
    )


def _scp(src: str, dest_target: str, dest_path: str) -> subprocess.CompletedProcess[str]:
    """Copy a local file to the orchestrator VM via SCP."""
    logger.debug("scp %s -> %s:%s", src, dest_target, dest_path)
    return subprocess.run(
        ["scp", *_SSH_OPTS, src, f"{dest_target}:{dest_path}"],
        capture_output=True,
        text=True,
    )


def build_image(ssh_target: str) -> None:
    """Build the orcest:latest Docker image on the orchestrator VM.

    Expects the source tarball to already be extracted at /opt/orcest/.
    """
    logger.info("Building orcest:latest image on %s", ssh_target)
    result = _ssh(ssh_target, "cd /opt/orcest && docker compose build")
    if result.returncode != 0:
        logger.error("Image build failed: %s", result.stderr.strip())
        raise RuntimeError(f"Docker image build failed on {ssh_target}: {result.stderr.strip()}")
    logger.info("Image build succeeded on %s", ssh_target)


def image_exists(ssh_target: str, image: str = "orcest:latest") -> bool:
    """Check whether a Docker image exists on the orchestrator VM."""
    result = _ssh(ssh_target, f"docker image inspect {image} >/dev/null 2>&1")
    return result.returncode == 0


def upload_source(ssh_target: str) -> None:
    """Create a source tarball locally and upload+extract it on the orchestrator.

    Assembles a Docker build context from the installed package:
    deploy files (Dockerfile, compose files, pyproject.toml) from package data,
    and source code from the installed orcest package.

    Extracts to /opt/orcest/ on the orchestrator VM.
    """
    import shutil

    logger.info("Uploading source to %s", ssh_target)

    # Locate the deploy files bundled as package data
    fleet_dir = os.path.dirname(os.path.abspath(__file__))
    deploy_dir = os.path.join(fleet_dir, "deploy")

    # Locate the installed orcest package source
    orcest_pkg_dir = os.path.dirname(fleet_dir)  # .../site-packages/orcest/

    # Assemble build context in a temp directory
    staging = tempfile.mkdtemp(prefix="orcest-upload-")
    try:
        # Copy deploy files (Dockerfile, compose files, pyproject.toml) to staging root
        for fname in ("Dockerfile", "docker-compose.yml", "docker-compose.redis.yml", "pyproject.toml"):
            src_path = os.path.join(deploy_dir, fname)
            if not os.path.exists(src_path):
                raise RuntimeError(f"Missing deploy file: {src_path}")
            shutil.copy2(src_path, os.path.join(staging, fname))

        # Copy orcest source to staging/src/orcest/
        dest_src = os.path.join(staging, "src", "orcest")
        shutil.copytree(orcest_pkg_dir, dest_src, ignore=shutil.ignore_patterns("__pycache__"))

        # Create tarball
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tarball_path = tmp.name

        tar_result = subprocess.run(
            [
                "tar", "czf", tarball_path,
                "Dockerfile", "docker-compose.yml", "docker-compose.redis.yml",
                "pyproject.toml", "src/",
            ],
            cwd=staging,
            capture_output=True,
            text=True,
        )
        if tar_result.returncode != 0:
            raise RuntimeError(f"Failed to create tarball: {tar_result.stderr.strip()}")

        # SCP to orchestrator
        result = _scp(tarball_path, ssh_target, "/tmp/orcest-source.tar.gz")
        if result.returncode != 0:
            raise RuntimeError(f"Failed to upload tarball: {result.stderr.strip()}")

        # Ensure /opt/orcest exists and extract on orchestrator
        mkdir_result = _ssh(ssh_target, "mkdir -p /opt/orcest")
        if mkdir_result.returncode != 0:
            raise RuntimeError(
                f"Failed to create /opt/orcest on {ssh_target}: {mkdir_result.stderr.strip()}"
            )
        result = _ssh(ssh_target, "tar xzf /tmp/orcest-source.tar.gz -C /opt/orcest/")
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to extract tarball on {ssh_target}: {result.stderr.strip()}"
            )

        # Clean up remote tarball
        _ssh(ssh_target, "rm -f /tmp/orcest-source.tar.gz")

        logger.info("Source uploaded and extracted on %s", ssh_target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        if "tarball_path" in locals():
            with open(os.devnull, "w"):
                try:
                    os.unlink(tarball_path)
                except OSError:
                    pass


def ensure_redis_stack(ssh_target: str) -> None:
    """Ensure the shared Redis stack is running.

    Starts (or updates) the shared Redis service from docker-compose.redis.yml.
    This creates the ``orcest`` Docker network that per-project stacks join.
    Idempotent — safe to call if Redis is already running.
    """
    logger.info("Ensuring shared Redis stack on %s", ssh_target)
    result = _ssh(
        ssh_target,
        "cd /opt/orcest && docker compose"
        " -f docker-compose.redis.yml"
        " -p orcest-redis"
        " up -d",
    )
    if result.returncode != 0:
        logger.error("Redis stack failed: %s", result.stderr.strip())
        raise RuntimeError(
            f"Failed to start shared Redis stack: {result.stderr.strip()}"
        )
    logger.info("Shared Redis stack running on %s", ssh_target)


def ensure_pool_manager(ssh_target: str, fleet_config_path: str = "/etc/orcest/fleet.yaml") -> None:
    """Ensure the pool manager stack is running.

    Starts (or updates) the pool manager service from docker-compose.pool.yml.
    Requires the Redis stack to be running first.
    """
    logger.info("Ensuring pool manager on %s", ssh_target)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && FLEET_CONFIG={fleet_config_path} docker compose"
        " -f docker-compose.pool.yml"
        " -p orcest-pool"
        " up -d",
    )
    if result.returncode != 0:
        logger.error("Pool manager failed: %s", result.stderr.strip())
        raise RuntimeError(
            f"Failed to start pool manager: {result.stderr.strip()}"
        )
    logger.info("Pool manager running on %s", ssh_target)


def deploy_stack(ssh_target: str, project_name: str) -> None:
    """Start/update a per-project Docker Compose stack.

    Runs docker compose from /opt/orcest/ using the main docker-compose.yml
    with a project-specific env file and compose project name.
    """
    _validate_project_name(project_name)
    logger.info("Deploying stack orcest-%s on %s", project_name, ssh_target)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && docker compose"
        f" -p orcest-{project_name}"
        f" --env-file projects/{project_name}/.env"
        f" up -d",
    )
    if result.returncode != 0:
        logger.error("Deploy failed: %s", result.stderr.strip())
        raise RuntimeError(f"Failed to deploy stack orcest-{project_name}: {result.stderr.strip()}")
    logger.info("Stack orcest-%s deployed on %s", project_name, ssh_target)


def teardown_stack(ssh_target: str, project_name: str) -> None:
    """Stop and remove a per-project Docker Compose stack."""
    _validate_project_name(project_name)
    logger.info("Tearing down stack orcest-%s on %s", project_name, ssh_target)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && docker compose"
        f" -p orcest-{project_name}"
        f" --env-file projects/{project_name}/.env"
        f" down -v",
    )
    if result.returncode != 0:
        logger.error("Teardown failed: %s", result.stderr.strip())
        raise RuntimeError(
            f"Failed to teardown stack orcest-{project_name}: {result.stderr.strip()}"
        )
    logger.info("Stack orcest-%s torn down on %s", project_name, ssh_target)


def restart_stack(ssh_target: str, project_name: str) -> None:
    """Force-recreate the orchestrator container for a project."""
    _validate_project_name(project_name)
    logger.info("Restarting stack orcest-%s on %s", project_name, ssh_target)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && docker compose"
        f" -p orcest-{project_name}"
        f" --env-file projects/{project_name}/.env"
        f" up -d --force-recreate",
    )
    if result.returncode != 0:
        logger.error("Restart failed: %s", result.stderr.strip())
        raise RuntimeError(
            f"Failed to restart stack orcest-{project_name}: {result.stderr.strip()}"
        )
    logger.info("Stack orcest-%s restarted on %s", project_name, ssh_target)


def write_project_files(
    ssh_target: str,
    project_name: str,
    env_content: str,
    config_yaml: str,
) -> None:
    """Write per-project .env and config files on the orchestrator VM.

    Creates:
      /opt/orcest/projects/{project_name}/.env
      /opt/orcest/projects/{project_name}/config/orchestrator.yaml

    Uses a temporary file + scp + ssh mv pattern to avoid partial writes.
    """
    _validate_project_name(project_name)
    logger.info("Writing project files for %s on %s", project_name, ssh_target)
    pdir = f"/opt/orcest/projects/{project_name}"

    # Ensure project directory structure exists
    result = _ssh(ssh_target, f"mkdir -p {pdir}/config")
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create project directory: {result.stderr.strip()}")

    # Write .env file via temp file + scp + mv
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as tmp:
        tmp.write(env_content)
        tmp_env_path = tmp.name

    try:
        remote_tmp_env = f"/tmp/orcest-{project_name}-env"
        result = _scp(tmp_env_path, ssh_target, remote_tmp_env)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to upload .env: {result.stderr.strip()}")
        result = _ssh(
            ssh_target,
            f"mv {remote_tmp_env} {pdir}/.env && chmod 600 {pdir}/.env",
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to install .env: {result.stderr.strip()}")
    finally:
        os.unlink(tmp_env_path)

    # Write config/orchestrator.yaml via temp file + scp + mv
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        tmp.write(config_yaml)
        tmp_config_path = tmp.name

    try:
        remote_tmp_config = f"/tmp/orcest-{project_name}-config.yaml"
        result = _scp(tmp_config_path, ssh_target, remote_tmp_config)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to upload config: {result.stderr.strip()}")
        result = _ssh(
            ssh_target,
            f"mv {remote_tmp_config} {pdir}/config/orchestrator.yaml"
            f" && chmod 644 {pdir}/config/orchestrator.yaml",
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to install config: {result.stderr.strip()}")
    finally:
        os.unlink(tmp_config_path)

    logger.info("Project files written for %s on %s", project_name, ssh_target)


def generate_env_file(
    github_token: str,
    key_prefix: str,
    project_name: str,
) -> str:
    """Generate .env file content for a project's Docker Compose stack."""
    if any(c in github_token for c in ("\n", "\r", "\0")):
        raise ValueError("github_token must not contain newlines or null bytes")
    return (
        f"GITHUB_TOKEN={github_token}\n"
        f"GH_TOKEN={github_token}\n"
        f"ORCEST_REDIS_KEY_PREFIX={key_prefix}\n"
        f"ORCEST_IMAGE=orcest:latest\n"
        f"ORCEST_CONFIG_DIR=/opt/orcest/projects/{project_name}/config\n"
    )


def generate_orchestrator_config(repo: str, key_prefix: str) -> str:
    """Generate orchestrator.yaml content for a project.

    Uses redis host 'redis' (Docker network service name), port 6379,
    and the project's key prefix for namespace isolation.
    """
    config = {
        "redis": {"host": "redis", "port": 6379, "key_prefix": key_prefix},
        "github": {"repo": repo},
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)
