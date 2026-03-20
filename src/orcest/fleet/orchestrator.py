"""Orchestrator stack management via SSH.

Provides helpers to manage per-project Docker Compose stacks on the
orchestrator VM. All operations are performed via SSH from the Proxmox
host (where ``orcest fleet`` commands run) to the orchestrator VM.
"""

from __future__ import annotations

import logging
import os
import re
import shlex
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


_DOCKER_IMAGE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$")


def image_exists(ssh_target: str, image: str = "orcest:latest") -> bool:
    """Check whether a Docker image exists on the orchestrator VM."""
    if not _DOCKER_IMAGE_RE.match(image) or len(image) > 256:
        raise ValueError(f"Invalid Docker image reference: {image!r}")
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
        deploy_files = (
            "Dockerfile", "docker-compose.yml",
            "docker-compose.redis.yml", "docker-compose.pool.yml",
            "pyproject.toml",
        )
        for fname in deploy_files:
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
            ["tar", "czf", tarball_path, *deploy_files, "src/"],
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

        # Ensure /opt/orcest exists, clean stale build-context files (but
        # preserve the projects/ directory which holds per-project config),
        # then extract the fresh tarball.
        mkdir_result = _ssh(ssh_target, "mkdir -p /opt/orcest")
        if mkdir_result.returncode != 0:
            raise RuntimeError(
                f"Failed to create /opt/orcest on {ssh_target}: {mkdir_result.stderr.strip()}"
            )
        clean_result = _ssh(
            ssh_target,
            "cd /opt/orcest"
            " && rm -rf src/ Dockerfile docker-compose*.yml pyproject.toml",
        )
        if clean_result.returncode != 0:
            raise RuntimeError(
                f"Failed to clean /opt/orcest on {ssh_target}: {clean_result.stderr.strip()}"
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
            try:
                os.unlink(tarball_path)
            except OSError:
                pass


def ensure_redis_stack(ssh_target: str) -> None:
    """Ensure the shared Redis stack is running.

    Starts (or updates) the shared Redis service from docker-compose.redis.yml.
    This creates the ``orcest`` Docker network that per-project stacks join.
    Idempotent -- safe to call if Redis is already running.
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


def upload_fleet_config(
    ssh_target: str, local_config_path: str = "/etc/orcest/config.yaml",
) -> None:
    """Upload the fleet config from the Proxmox host to the orchestrator VM.

    Copies to ``/etc/orcest/config.yaml`` on the orchestrator so the pool
    manager container can mount it.  Uses temp file + scp + mv for atomicity.
    """
    logger.info("Uploading fleet config to %s", ssh_target)

    if not os.path.isfile(local_config_path):
        raise FileNotFoundError(
            f"Fleet config not found: {local_config_path}"
        )

    remote_dest = "/etc/orcest/config.yaml"
    # SCP as the orcest user to a writable location, then sudo mv into place.
    remote_tmp = "/tmp/.orcest-config.yaml.tmp"

    # Ensure target directory exists on the orchestrator VM
    result = _ssh(ssh_target, "sudo mkdir -p /etc/orcest")
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to create /etc/orcest on orchestrator: {result.stderr.strip()}"
        )

    result = _scp(local_config_path, ssh_target, remote_tmp)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to upload fleet config: {result.stderr.strip()}")

    result = _ssh(
        ssh_target,
        f"sudo mv {shlex.quote(remote_tmp)} {shlex.quote(remote_dest)}"
        f" && sudo chmod 600 {shlex.quote(remote_dest)}",
    )
    if result.returncode != 0:
        _ssh(ssh_target, f"rm -f {shlex.quote(remote_tmp)}")
        raise RuntimeError(f"Failed to install fleet config: {result.stderr.strip()}")

    logger.info("Fleet config uploaded to %s:%s", ssh_target, remote_dest)


def ensure_pool_manager(
    ssh_target: str, fleet_config_path: str = "/etc/orcest/config.yaml",
) -> None:
    """Ensure the pool manager stack is running.

    Starts (or updates) the pool manager service from docker-compose.pool.yml.
    Requires the Redis stack to be running first.
    """
    logger.info("Ensuring pool manager on %s", ssh_target)
    quoted_path = shlex.quote(fleet_config_path)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && FLEET_CONFIG={quoted_path} docker compose"
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


def stop_pool_manager(ssh_target: str) -> None:
    """Stop the pool manager stack.

    Counterpart to :func:`ensure_pool_manager`. Idempotent — safe to
    call when the pool manager is already stopped.
    """
    logger.info("Stopping pool manager on %s", ssh_target)
    result = _ssh(
        ssh_target,
        "cd /opt/orcest && docker compose"
        " -f docker-compose.pool.yml"
        " -p orcest-pool"
        " down",
    )
    if result.returncode != 0:
        logger.error("Pool manager stop failed: %s", result.stderr.strip())
        raise RuntimeError(
            f"Failed to stop pool manager: {result.stderr.strip()}"
        )
    logger.info("Pool manager stopped on %s", ssh_target)


def get_pool_redis_members(
    ssh_target: str,
) -> tuple[set[str], dict[str, str]]:
    """Read pool tracking sets from Redis on the orchestrator via SSH.

    Returns ``(idle_vm_ids, active_vm_id_to_timestamp)``.

    Uses ``redis-cli --raw`` for predictable line-per-value output.
    """
    # Read idle set
    result = _ssh(ssh_target, "redis-cli --raw SMEMBERS orcest:pool:idle")
    idle: set[str] = set()
    if result.returncode == 0:
        for line in result.stdout.strip().splitlines():
            stripped = line.strip()
            if stripped:
                idle.add(stripped)

    # Read active hash (returns alternating key, value lines)
    result = _ssh(ssh_target, "redis-cli --raw HGETALL orcest:pool:active")
    active: dict[str, str] = {}
    if result.returncode == 0:
        lines = [ln.strip() for ln in result.stdout.strip().splitlines() if ln.strip()]
        for i in range(0, len(lines) - 1, 2):
            active[lines[i]] = lines[i + 1]

    return idle, active


def clean_pool_redis(ssh_target: str, vm_ids: list[str]) -> None:
    """Remove VM IDs from pool:idle and pool:active in Redis."""
    if not vm_ids:
        return
    cmds: list[str] = []
    for vm_id in vm_ids:
        quoted = shlex.quote(vm_id)
        cmds.append(f"redis-cli SREM orcest:pool:idle {quoted}")
        cmds.append(f"redis-cli HDEL orcest:pool:active {quoted}")
    _ssh(ssh_target, " && ".join(cmds))


def clean_pending_tasks(ssh_target: str) -> int:
    """Delete all pending task markers from Redis. Returns count deleted."""
    result = _ssh(
        ssh_target,
        "redis-cli --raw KEYS 'orcest:pending:*'",
    )
    keys = [k.strip() for k in result.stdout.strip().splitlines() if k.strip()]
    if not keys:
        return 0
    quoted = " ".join(shlex.quote(k) for k in keys)
    _ssh(ssh_target, f"redis-cli DEL {quoted}")
    return len(keys)


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
            _ssh(ssh_target, f"rm -f {remote_tmp_env}")
            raise RuntimeError(f"Failed to install .env: {result.stderr.strip()}")
    finally:
        try:
            os.unlink(tmp_env_path)
        except OSError:
            pass

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
            _ssh(ssh_target, f"rm -f {remote_tmp_config}")
            raise RuntimeError(f"Failed to install config: {result.stderr.strip()}")
    finally:
        try:
            os.unlink(tmp_config_path)
        except OSError:
            pass

    logger.info("Project files written for %s on %s", project_name, ssh_target)


def _validate_env_value(value: str, name: str) -> None:
    """Raise ValueError if *value* contains characters unsafe for .env files.

    Values are single-quoted in the generated .env file, so single quotes
    within the value would break quoting.  Newlines, carriage returns, and
    null bytes are always forbidden.
    """
    if any(c in value for c in ("\n", "\r", "\0")):
        raise ValueError(f"{name} must not contain newlines or null bytes")
    if "'" in value:
        raise ValueError(f"{name} must not contain single quotes")


def generate_env_file(
    github_token: str,
    key_prefix: str,
    project_name: str,
    claude_token: str = "",
) -> str:
    """Generate .env file content for a project's Docker Compose stack.

    Values are single-quoted to prevent Docker Compose from performing
    variable interpolation (``$`` references) or word splitting.
    """
    _validate_project_name(project_name)
    _validate_env_value(github_token, "github_token")
    _validate_env_value(key_prefix, "key_prefix")
    _validate_env_value(project_name, "project_name")
    lines = [
        f"GITHUB_TOKEN='{github_token}'",
        f"GH_TOKEN='{github_token}'",
        f"ORCEST_REDIS_KEY_PREFIX='{key_prefix}'",
        "ORCEST_IMAGE='orcest:latest'",
        f"ORCEST_CONFIG_DIR='/opt/orcest/projects/{project_name}/config'",
    ]
    if claude_token:
        _validate_env_value(claude_token, "claude_token")
        lines.append(f"CLAUDE_CODE_OAUTH_TOKEN='{claude_token}'")
    return "\n".join(lines) + "\n"


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
