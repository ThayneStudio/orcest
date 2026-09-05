"""Orchestrator stack management via SSH.

Provides helpers to manage per-project Docker Compose stacks on the
orchestrator VM. All operations are performed via SSH from the Proxmox
host (where ``orcest fleet`` commands run) to the orchestrator VM.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shlex
import subprocess
import tempfile
from pathlib import Path

import yaml

from orcest.fleet.config import require_valid_project_name as _validate_project_name

logger = logging.getLogger(__name__)


_SSH_OPTS = [
    "-o",
    "ConnectTimeout=5",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "BatchMode=yes",
    "-o",
    "LogLevel=ERROR",
]

# Path to the 0600 env file on the orchestrator VM that holds the minted
# ORCEST_REDIS_PASSWORD. It is the single source of truth for the Redis AUTH
# secret: --env-file'd into the redis / pool compose stacks (so
# ${ORCEST_REDIS_PASSWORD} interpolates) and read back by the fleet CLI to wire
# the per-project .env and the worker clones.
REDIS_ENV_PATH = "/opt/orcest/.redis.env"

# The orchestrator VM does not have ``redis-cli`` installed natively —
# Redis runs inside the ``orcest-redis-redis-1`` container started by the
# ``docker-compose.redis.yml`` stack. All redis-cli invocations from the
# fleet CLI must be routed through ``docker exec`` against that container.
_REDIS_CONTAINER = "orcest-redis-redis-1"
# C1: Redis now requires AUTH, so the CLI must authenticate. The password is
# read from the container's own environment (delivered via the redis stack's
# --env-file) rather than interpolated by the outer ssh shell — that keeps the
# secret off the fleet host's argv / process listing / ssh debug logs.
#
# Form: ``docker exec C sh -c 'exec redis-cli -a "$ORCEST_REDIS_PASSWORD"
# --no-auth-warning "$@"' redis-cli``. Call sites append a flat argument string
# after the prefix (e.g. ``--raw SMEMBERS key``); the outer ssh shell word-splits
# it and docker passes the tokens as separate argv to the in-container ``sh``,
# where ``"$@"`` forwards them to redis-cli. The trailing ``redis-cli`` token
# becomes ``$0`` so appended args line up at ``$1`` onward. ``exec`` hands the
# real redis-cli exit status back through ``sh``/``docker exec``.
_REDIS_CLI_PREFIX = (
    f"sudo docker exec {_REDIS_CONTAINER} "
    'sh -c \'exec redis-cli -a "$ORCEST_REDIS_PASSWORD" --no-auth-warning -e "$@"\' redis-cli'
)

# Older redis-cli builds, wrappers, and test doubles can still report a Redis
# command error in output while returning zero.  Every fleet lifecycle read is
# a safety boundary, so reject recognizable server errors defensively instead
# of parsing them as an empty set/hash or a legitimate value.
_REDIS_ERROR_LINE_RE = re.compile(
    r"(?im)^\s*(?:\(error\)\s*)?"
    r"(?:AUTH failed:|ERR\b|NOAUTH\b|WRONGPASS\b|WRONGTYPE\b|NOGROUP\b|"
    r"LOADING\b|READONLY\b|MISCONF\b)"
)


def _redis_cli_failure(result: subprocess.CompletedProcess[str]) -> str | None:
    """Return a Redis/transport diagnostic when *result* is not trustworthy."""
    for output in (result.stderr or "", result.stdout or ""):
        match = _REDIS_ERROR_LINE_RE.search(output)
        if match is not None:
            return output[match.start() :].splitlines()[0].strip()
    if result.returncode != 0:
        return (result.stderr or result.stdout or f"exit status {result.returncode}").strip()
    return None


def _require_redis_cli_success(
    result: subprocess.CompletedProcess[str],
    context: str,
) -> None:
    """Raise when a redis-cli invocation failed or emitted a server error."""
    failure = _redis_cli_failure(result)
    if failure is not None:
        raise RuntimeError(f"{context}: {failure}")


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


def _remote_private_tmp(ssh_target: str, template: str) -> str:
    """Create a unique 0600 temporary file owned by the deploy SSH user."""
    if not re.fullmatch(r"/tmp/[a-zA-Z0-9._-]+\.XXXXXX", template):
        raise ValueError("Unsafe remote temporary-file template")
    result = _ssh(ssh_target, f"umask 077 && mktemp {shlex.quote(template)}")
    if result.returncode != 0:
        raise RuntimeError("Failed to allocate a private remote temporary file")
    path = result.stdout.strip()
    if not re.fullmatch(r"/tmp/[a-zA-Z0-9._-]+", path):
        raise RuntimeError("Remote mktemp returned an unsafe path")
    return path


def build_image(ssh_target: str) -> None:
    """Build the orcest:latest Docker image on the orchestrator VM.

    Expects the source tarball to already be extracted at /opt/orcest/.
    """
    logger.info("Building orcest:latest image on %s", ssh_target)
    result = _ssh(
        ssh_target,
        "cd /opt/orcest && "
        "revision=$(cat .orcest-revision) && "
        "orcest_uid=$(id -u orcest) && orcest_gid=$(id -g orcest) && "
        "printf '%s\\n' \"$revision\" | grep -Eq '^[0-9a-f]{7,64}$' || "
        "{ echo 'Build requires an exact clean .orcest-revision' >&2; exit 2; }; "
        'printf \'%s:%s\\n\' "$orcest_uid" "$orcest_gid" | '
        "grep -Eq '^[1-9][0-9]*:[1-9][0-9]*$' || "
        "{ echo 'Host orcest UID/GID must be positive integers' >&2; exit 2; }; "
        'ORCEST_BUILD_REVISION="$revision" ORCEST_UID="$orcest_uid" '
        'ORCEST_GID="$orcest_gid" docker compose build --no-cache && '
        'test "$(docker run --rm --entrypoint id orcest:latest -u orcest)" '
        '= "$orcest_uid" && '
        'test "$(docker run --rm --entrypoint id orcest:latest -g orcest)" '
        '= "$orcest_gid"',
    )
    if result.returncode != 0:
        logger.error("Image build failed: %s", result.stderr.strip())
        raise RuntimeError(f"Docker image build failed on {ssh_target}: {result.stderr.strip()}")
    logger.info("Image build succeeded on %s", ssh_target)


_DOCKER_IMAGE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$")
_DEPLOY_FILES = (
    "Dockerfile",
    "docker-compose.yml",
    "docker-compose.redis.yml",
    "docker-compose.pool.yml",
    "pyproject.toml",
    "requirements.lock",
)
# Monitor stack files live at the repo root (not src/orcest/fleet/deploy/),
# so they are only shipped when deploying from a source checkout; the
# installed-package fallback layout omits them. There is no `orcest fleet`
# subcommand that builds or starts the monitor container — the operator runs
# `docker compose -f docker-compose.monitor.yml up -d` on the orchestrator VM
# (see docs/monitor-exposure-runbook.md) — and `fleet update` re-copies
# (overwrites) these files on every deploy.
_MONITOR_DEPLOY_FILES = (
    "Dockerfile.monitor",
    "docker-compose.monitor.yml",
    "config/monitor.example.yaml",
    # Dockerfile.monitor's `COPY pyproject.toml README.md ./` needs README.md
    # in the build context or the image build fails.
    "README.md",
)


def image_exists(ssh_target: str, image: str = "orcest:latest") -> bool:
    """Check whether a Docker image exists on the orchestrator VM."""
    if not _DOCKER_IMAGE_RE.match(image) or len(image) > 256:
        raise ValueError(f"Invalid Docker image reference: {image!r}")
    result = _ssh(ssh_target, f"docker image inspect {image} >/dev/null 2>&1")
    return result.returncode == 0


def upload_source(ssh_target: str, source_root: str | os.PathLike[str] | None = None) -> None:
    """Create a source tarball locally and upload+extract it on the orchestrator.

    Assembles a Docker build context from the active source checkout when
    available, falling back to the installed package only when no checkout is
    present. ``ORCEST_SOURCE_ROOT`` can be set to force a specific checkout.

    Extracts to /opt/orcest/ on the orchestrator VM.
    """
    logger.info("Uploading source to %s", ssh_target)
    tarball_path = create_source_tarball(source_root=source_root)
    remote_tarball: str | None = None
    try:
        # Use a unique, mode-0600 destination so concurrent deploy attempts
        # cannot overwrite or remove each other's candidate source archive.
        remote_tarball = _remote_private_tmp(ssh_target, "/tmp/orcest-source.XXXXXX")
        result = _scp(tarball_path, ssh_target, remote_tarball)
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
            "cd /opt/orcest && rm -rf src/ Dockerfile docker-compose.yml"
            " docker-compose.redis.yml docker-compose.pool.yml pyproject.toml"
            " requirements.lock .orcest-revision Dockerfile.monitor"
            " docker-compose.monitor.yml config/monitor.example.yaml README.md",
        )
        if clean_result.returncode != 0:
            raise RuntimeError(
                f"Failed to clean /opt/orcest on {ssh_target}: {clean_result.stderr.strip()}"
            )
        result = _ssh(
            ssh_target,
            f"tar xzf {shlex.quote(remote_tarball)} -C /opt/orcest/",
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to extract tarball on {ssh_target}: {result.stderr.strip()}"
            )

        logger.info("Source uploaded and extracted on %s", ssh_target)
    finally:
        if remote_tarball is not None:
            _ssh(ssh_target, f"rm -f {shlex.quote(remote_tarball)}")
        try:
            os.unlink(tarball_path)
        except OSError:
            pass


def create_source_tarball(source_root: str | os.PathLike[str] | None = None) -> str:
    """Package the active Orcest source tree into a deploy build-context tarball.

    The returned path is owned by the caller and must be deleted after use.
    """
    import shutil

    layout = _resolve_source_layout(source_root)
    revision = _resolve_deploy_revision(source_root)
    staging = tempfile.mkdtemp(prefix="orcest-source-")
    try:
        for fname, src_path in layout.deploy_files.items():
            if not src_path.exists():
                raise RuntimeError(f"Missing deploy file: {src_path}")
            dest = Path(staging) / fname
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dest)

        dest_src = Path(staging) / "src" / "orcest"
        shutil.copytree(
            layout.package_dir,
            dest_src,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        (Path(staging) / ".orcest-revision").write_text(f"{revision}\n", encoding="utf-8")
        (dest_src / "_build_revision.py").write_text(
            f"# Generated by create_source_tarball; do not edit.\nBUILD_REVISION = {revision!r}\n",
            encoding="utf-8",
        )

        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tarball_path = tmp.name

        tar_result = subprocess.run(
            ["tar", "czf", tarball_path, *layout.deploy_files, ".orcest-revision", "src/"],
            cwd=staging,
            capture_output=True,
            text=True,
        )
        if tar_result.returncode != 0:
            raise RuntimeError(f"Failed to create tarball: {tar_result.stderr.strip()}")
        return tarball_path
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _resolve_deploy_revision(source_root: str | os.PathLike[str] | None = None) -> str:
    """Return an exact checkout revision, explicitly marking modified trees."""
    from orcest.revision import UNKNOWN_REVISION, normalize_revision

    root = _resolve_source_root(source_root)
    if root is None or not (root / ".git").exists():
        return normalize_revision(os.environ.get("ORCEST_BUILD_REVISION")) or UNKNOWN_REVISION
    try:
        head_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=10,
        )
        status_result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return UNKNOWN_REVISION
    if head_result.returncode != 0 or status_result.returncode != 0:
        return UNKNOWN_REVISION
    revision = normalize_revision(head_result.stdout)
    if revision is None:
        return UNKNOWN_REVISION
    return f"{revision}-dirty" if status_result.stdout.strip() else revision


class _SourceLayout:
    def __init__(self, package_dir: Path, deploy_files: dict[str, Path]) -> None:
        self.package_dir = package_dir
        self.deploy_files = deploy_files


def _resolve_source_layout(source_root: str | os.PathLike[str] | None = None) -> _SourceLayout:
    root = _resolve_source_root(source_root)
    if root is not None:
        deploy_dir = root / "src" / "orcest" / "fleet" / "deploy"
        deploy_files = {
            "Dockerfile": deploy_dir / "Dockerfile",
            "docker-compose.yml": deploy_dir / "docker-compose.yml",
            "docker-compose.redis.yml": deploy_dir / "docker-compose.redis.yml",
            "docker-compose.pool.yml": deploy_dir / "docker-compose.pool.yml",
            "pyproject.toml": root / "pyproject.toml",
            "requirements.lock": root / "requirements.lock",
        }
        # Monitor stack files ship from the repo root (checkout deploys only).
        for fname in _MONITOR_DEPLOY_FILES:
            deploy_files[fname] = root / fname
        return _SourceLayout(
            package_dir=root / "src" / "orcest",
            deploy_files=deploy_files,
        )

    fleet_dir = Path(__file__).resolve().parent
    deploy_dir = fleet_dir / "deploy"
    package_dir = fleet_dir.parent
    return _SourceLayout(
        package_dir=package_dir,
        deploy_files={fname: deploy_dir / fname for fname in _DEPLOY_FILES},
    )


def _resolve_source_root(source_root: str | os.PathLike[str] | None = None) -> Path | None:
    forced = source_root or os.environ.get("ORCEST_SOURCE_ROOT")
    if forced:
        root = Path(forced).expanduser().resolve()
        if not _is_source_root(root):
            raise RuntimeError(
                f"ORCEST source root is invalid: {root} "
                "(expected pyproject.toml and src/orcest/fleet/deploy/)"
            )
        return root

    candidates = (*[Path.cwd(), *Path.cwd().parents], *Path(__file__).resolve().parents)
    for candidate in dict.fromkeys(candidates):
        if _is_source_root(candidate):
            return candidate
    return None


def _is_source_root(path: Path) -> bool:
    return (
        (path / "pyproject.toml").is_file()
        and (path / "requirements.lock").is_file()
        and (path / "src" / "orcest" / "__init__.py").is_file()
        and (path / "src" / "orcest" / "fleet" / "deploy" / "Dockerfile").is_file()
    )


def ensure_redis_password(ssh_target: str) -> str:
    """Mint + persist the Redis AUTH password on the orchestrator VM (idempotent).

    C1: ``docker-compose.redis.yml`` runs ``redis-server --requirepass
    ${ORCEST_REDIS_PASSWORD}``. That variable must come from somewhere, or Redis
    boots with an *empty* requirepass — which Compose renders as
    ``--requirepass --appendonly yes`` (the next flag is consumed as the
    password's value), a FATAL misconfiguration / total outage. This helper is
    the single source of truth for that secret.

    Behaviour:
      * If :data:`REDIS_ENV_PATH` already exists and is non-empty, its value is
        **reused** — the password is NEVER rotated. Rotating would orphan every
        running worker VM and the persisted Redis AOF (workers can no longer
        AUTH, and the existing data is locked behind the old password).
      * Otherwise a strong CSPRNG password (``openssl rand -hex 32``) is minted
        and written ``0600`` (secret at rest).

    The minting is done in-band by an idempotent shell command (``[ -s file ]``
    guard under ``umask 077``), so concurrent/repeat deploys converge on one
    value. Returns the persisted password (read back from the file).

    Must be called BEFORE :func:`ensure_redis_stack` / :func:`ensure_pool_manager`
    so the ``--env-file`` they pass actually contains a value.
    """
    logger.info("Ensuring Redis password on %s", ssh_target)
    quoted = shlex.quote(REDIS_ENV_PATH)
    # Idempotent mint: only generate when the file is missing or empty.
    # umask 077 makes the freshly-created file 0600 even before the explicit
    # chmod (closes the brief world-readable window). Single quotes around the
    # inner script keep $(openssl ...) from expanding on the fleet host.
    # NOTE: the file is owned by the SSH/deploy user ($SUDO_USER), not root.
    # ensure_redis_stack / ensure_pool_manager / restart_stack run
    # ``docker compose --env-file`` WITHOUT sudo (the deploy user is in the
    # docker group), so a root-owned 0600 file would be unreadable to them
    # ("open .redis.env: permission denied"). chown 0600 to the deploy user keeps
    # it secret-at-rest AND readable by the (non-sudo) compose invocations.
    mint_cmd = (
        "sudo mkdir -p /opt/orcest && "
        "sudo sh -c '" + "umask 077; "
        f"[ -s {quoted} ] || "
        f'printf "ORCEST_REDIS_PASSWORD=%s\\n" "$(openssl rand -hex 32)" > {quoted}; '
        f"chmod 600 {quoted}; "
        f'chown "${{SUDO_USER:-root}}:${{SUDO_USER:-root}}" {quoted}'
        "'"
    )
    result = _ssh(ssh_target, mint_cmd)
    if result.returncode != 0:
        logger.error("Redis password mint failed: %s", result.stderr.strip())
        raise RuntimeError(f"Failed to mint Redis password: {result.stderr.strip()}")

    # Read the persisted value back (the single source of truth).
    read = _ssh(ssh_target, f"sudo cat {quoted}")
    if read.returncode != 0:
        raise RuntimeError(f"Failed to read Redis password: {read.stderr.strip()}")
    password = ""
    for line in read.stdout.splitlines():
        line = line.strip()
        if line.startswith("ORCEST_REDIS_PASSWORD="):
            password = line.split("=", 1)[1]
            break
    if not password:
        raise RuntimeError(f"Minted Redis password file at {REDIS_ENV_PATH} is empty")
    return password


def ensure_redis_stack(ssh_target: str) -> None:
    """Ensure the shared Redis stack is running.

    Starts (or updates) the shared Redis service from docker-compose.redis.yml.
    This creates the ``orcest`` Docker network that per-project stacks join.
    Idempotent -- safe to call if Redis is already running.

    C1: ``--env-file`` supplies ${ORCEST_REDIS_PASSWORD} (minted by
    :func:`ensure_redis_password`) so ``redis-server --requirepass`` gets a real
    value instead of booting unauthenticated / mis-parsing the next flag.
    """
    logger.info("Ensuring shared Redis stack on %s", ssh_target)
    result = _ssh(
        ssh_target,
        "cd /opt/orcest && docker compose"
        f" --env-file {REDIS_ENV_PATH}"
        " -f docker-compose.redis.yml -p orcest-redis up -d",
    )
    if result.returncode != 0:
        logger.error("Redis stack failed: %s", result.stderr.strip())
        raise RuntimeError(f"Failed to start shared Redis stack: {result.stderr.strip()}")
    logger.info("Shared Redis stack running on %s", ssh_target)


def upload_fleet_config(
    ssh_target: str,
    local_config_path: str = "/etc/orcest/config.yaml",
) -> None:
    """Upload the fleet config from the Proxmox host to the orchestrator VM.

    Copies to ``/etc/orcest/config.yaml`` on the orchestrator so the pool
    manager container can mount it.  Uses temp file + scp + mv for atomicity.
    """
    logger.info("Uploading fleet config to %s", ssh_target)

    if not os.path.isfile(local_config_path):
        raise FileNotFoundError(f"Fleet config not found: {local_config_path}")

    remote_dest = "/etc/orcest/config.yaml"

    # Ensure target directory exists on the orchestrator VM
    result = _ssh(ssh_target, "sudo mkdir -p /etc/orcest")
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create /etc/orcest on orchestrator: {result.stderr.strip()}")

    remote_tmp = _remote_private_tmp(ssh_target, "/tmp/orcest-config.XXXXXX")
    try:
        result = _scp(local_config_path, ssh_target, remote_tmp)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to upload fleet config: {result.stderr.strip()}")

        # The deployed image is built with the host ``orcest`` UID/GID and the
        # bind-mounted config is consumed by that unprivileged user. Keep it
        # private to the service identity (and root), matching the SSH-key
        # mount used by the same container.
        result = _ssh(
            ssh_target,
            f"sudo install -m 600 -o orcest -g orcest {shlex.quote(remote_tmp)} "
            f"{shlex.quote(remote_dest)}",
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to install fleet config: {result.stderr.strip()}")
    finally:
        _ssh(ssh_target, f"rm -f {shlex.quote(remote_tmp)}")

    logger.info("Fleet config uploaded to %s:%s", ssh_target, remote_dest)


def _get_deployed_pool_config(ssh_target: str) -> dict | None:
    """Read and validate the deployed pool mapping, or return None when absent."""
    result = _ssh(ssh_target, "sudo cat /etc/orcest/config.yaml")
    if result.returncode != 0:
        error = (result.stderr or "").strip().lower()
        if "no such file" in error or "not found" in error:
            return None
        raise RuntimeError(
            f"Could not read deployed fleet config on {ssh_target}: {result.stderr.strip()}"
        )

    try:
        data = yaml.safe_load(result.stdout) or {}
    except yaml.YAMLError as exc:
        raise RuntimeError("Deployed fleet config is not valid YAML") from exc
    if not isinstance(data, dict):
        raise RuntimeError("Deployed fleet config must contain a YAML mapping")
    pool = data.get("pool") or {}
    if not isinstance(pool, dict):
        raise RuntimeError("Deployed fleet config pool section must be a mapping")
    return pool


def get_deployed_pool_vmid_range(ssh_target: str) -> tuple[int, int] | None:
    """Return the deployed worker VMID range, or None on a first deployment."""
    pool = _get_deployed_pool_config(ssh_target)
    if pool is None:
        return None
    try:
        return int(pool.get("vm_id_start", 0) or 0), int(pool.get("vm_id_end", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Deployed pool worker VMID range must contain integers") from exc


def get_deployed_pool_backend(ssh_target: str) -> str | None:
    """Return the worker-layout signature in the deployed fleet config.

    ``None`` means this is a first deployment and no remote fleet config exists.
    Other read or parse failures are surfaced so callers cannot unknowingly
    perform a backend transition with stale workers. Legacy single-backend
    configurations return the backend name exactly; heterogeneous layouts
    return their ordered backend/runner/mode signature.
    """
    pool = _get_deployed_pool_config(ssh_target)
    if pool is None:
        return None
    raw_profiles = pool.get("worker_profiles") or []
    if raw_profiles:
        if not isinstance(raw_profiles, list):
            raise RuntimeError("Deployed pool.worker_profiles must be a list")
        from orcest.fleet.config import WorkerProfileConfig

        profiles: list[WorkerProfileConfig] = []
        for index, raw_profile in enumerate(raw_profiles):
            if isinstance(raw_profile, str):
                if not raw_profile.strip():
                    raise RuntimeError(
                        f"Deployed pool.worker_profiles[{index}] backend must not be empty"
                    )
                profiles.append(WorkerProfileConfig(backend=raw_profile))
                continue
            if not isinstance(raw_profile, dict):
                raise RuntimeError(
                    f"Deployed pool.worker_profiles[{index}] must be a string or mapping"
                )
            backend = str(raw_profile.get("backend", "") or "")
            if not backend.strip():
                raise RuntimeError(
                    f"Deployed pool.worker_profiles[{index}].backend must not be empty"
                )
            profiles.append(
                WorkerProfileConfig(
                    backend=backend,
                    runner_type=str(raw_profile.get("runner_type", "") or ""),
                    runner_mode=str(raw_profile.get("runner_mode", "") or ""),
                )
            )
        profile_signature = ",".join(
            f"{profile.backend}:{profile.runner_type}:{profile.runner_mode}" for profile in profiles
        )
        try:
            vm_id_start = int(pool.get("vm_id_start", 0) or 0)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Deployed pool.vm_id_start must be an integer") from exc
        try:
            vm_id_end = int(pool.get("vm_id_end", 0) or 0)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Deployed pool.vm_id_end must be an integer") from exc
        return f"vm_id_start={vm_id_start};vm_id_end={vm_id_end};profiles={profile_signature}"
    backend = str(pool.get("worker_backend") or "claude").strip()
    from orcest.fleet.config import WorkerProfileConfig

    profile = WorkerProfileConfig(
        backend=backend or "claude",
        runner_type=str(pool.get("worker_runner_type") or ""),
        runner_mode=str(pool.get("worker_runner_mode") or ""),
    )
    try:
        vm_id_start = int(pool.get("vm_id_start", 0) or 0)
        vm_id_end = int(pool.get("vm_id_end", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Deployed pool worker VMID range must contain integers") from exc
    return (
        f"vm_id_start={vm_id_start};vm_id_end={vm_id_end};backend={profile.backend};"
        f"runner={profile.runner_type};mode={profile.runner_mode}"
    )


def ensure_pool_manager(
    ssh_target: str,
    fleet_config_path: str = "/etc/orcest/config.yaml",
) -> None:
    """Ensure the pool manager stack is running.

    Starts (or updates) the pool manager service from docker-compose.pool.yml.
    Requires the Redis stack to be running first.
    """
    logger.info("Ensuring pool manager on %s", ssh_target)
    quoted_path = shlex.quote(fleet_config_path)
    # C1: --env-file delivers ORCEST_REDIS_PASSWORD into the pool-manager
    # container's environment (docker-compose.pool.yml passes it through), so the
    # pool manager can AUTH to Redis and forward the value to worker clones.
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && FLEET_CONFIG={quoted_path} docker compose"
        f" --env-file {REDIS_ENV_PATH} -f docker-compose.pool.yml -p orcest-pool"
        " run --rm --no-deps --entrypoint sh pool-manager"
        " -c 'test -r /home/orcest/app/config/fleet.yaml &&"
        " test -r /home/orcest/.ssh && test -x /home/orcest/.ssh &&"
        " test -f /home/orcest/.ssh/id_ed25519 &&"
        " test -r /home/orcest/.ssh/id_ed25519' &&"
        f" FLEET_CONFIG={quoted_path} docker compose"
        f" --env-file {REDIS_ENV_PATH} -f docker-compose.pool.yml -p orcest-pool"
        " up -d --force-recreate pool-manager && sleep 2 &&"
        f" cid=$(FLEET_CONFIG={quoted_path} docker compose"
        f" --env-file {REDIS_ENV_PATH} -f docker-compose.pool.yml -p orcest-pool"
        ' ps -q pool-manager) && test -n "$cid" &&'
        ' test "$(docker inspect -f \'{{.State.Running}}\' "$cid")" = true &&'
        ' test "$(docker inspect -f \'{{.RestartCount}}\' "$cid")" = 0',
    )
    if result.returncode != 0:
        logger.error("Pool manager failed: %s", result.stderr.strip())
        raise RuntimeError(f"Failed to start pool manager: {result.stderr.strip()}")
    logger.info("Pool manager running on %s", ssh_target)


def stop_pool_manager(ssh_target: str) -> None:
    """Stop the pool manager stack.

    Counterpart to :func:`ensure_pool_manager`. Idempotent — safe to
    call when the pool manager is already stopped.
    """
    logger.info("Stopping pool manager on %s", ssh_target)
    result = _ssh(
        ssh_target,
        "cd /opt/orcest && docker compose -f docker-compose.pool.yml -p orcest-pool down",
    )
    if result.returncode != 0:
        logger.error("Pool manager stop failed: %s", result.stderr.strip())
        raise RuntimeError(f"Failed to stop pool manager: {result.stderr.strip()}")
    logger.info("Pool manager stopped on %s", ssh_target)


def get_pool_redis_members(
    ssh_target: str,
) -> tuple[set[str], dict[str, str]]:
    """Read pool tracking sets from Redis on the orchestrator via SSH.

    Returns ``(idle_vm_ids, active_vm_id_to_timestamp)``.

    Uses ``redis-cli --raw`` for predictable line-per-value output.
    """
    # Read idle set
    result = _ssh(ssh_target, f"{_REDIS_CLI_PREFIX} --raw SMEMBERS orcest:pool:idle")
    _require_redis_cli_success(result, "Failed to read pool idle set")
    idle: set[str] = set()
    for line in result.stdout.strip().splitlines():
        stripped = line.strip()
        if stripped:
            idle.add(stripped)

    # Read active hash (returns alternating key, value lines)
    result = _ssh(ssh_target, f"{_REDIS_CLI_PREFIX} --raw HGETALL orcest:pool:active")
    _require_redis_cli_success(result, "Failed to read pool active hash")
    active: dict[str, str] = {}
    lines = [ln.strip() for ln in result.stdout.strip().splitlines() if ln.strip()]
    if len(lines) % 2 != 0:
        raise RuntimeError(
            "Failed to read pool active hash: redis-cli returned an odd number of HGETALL fields"
        )
    for i in range(0, len(lines) - 1, 2):
        active[lines[i]] = lines[i + 1]

    return idle, active


def _parse_xinfo_consumers_with_pending(stdout: str) -> set[str]:
    """Parse ``redis-cli --raw XINFO CONSUMERS`` output for busy consumers."""
    lines = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
    if len(lines) % 2 != 0:
        raise RuntimeError("Malformed XINFO CONSUMERS output: incomplete field/value pair")
    consumers: set[str] = set()
    current_name: str | None = None
    current_pending_seen = False
    for i in range(0, len(lines) - 1, 2):
        field = lines[i]
        value = lines[i + 1]
        if field == "name":
            if current_name is not None and not current_pending_seen:
                raise RuntimeError(
                    f"Malformed XINFO CONSUMERS output: consumer {current_name!r} "
                    "has no pending count"
                )
            if not value:
                raise RuntimeError("Malformed XINFO CONSUMERS output: empty consumer name")
            current_name = value
            current_pending_seen = False
        elif field == "pending":
            if current_name is None or current_pending_seen:
                raise RuntimeError(
                    "Malformed XINFO CONSUMERS output: pending count has no unique consumer"
                )
            try:
                pending = int(value)
            except ValueError as exc:
                raise RuntimeError(
                    f"Malformed XINFO CONSUMERS output: invalid pending count {value!r}"
                ) from exc
            if pending < 0:
                raise RuntimeError(
                    f"Malformed XINFO CONSUMERS output: invalid pending count {value!r}"
                )
            current_pending_seen = True
            if pending > 0:
                consumers.add(current_name)
    if current_name is None and lines:
        raise RuntimeError("Malformed XINFO CONSUMERS output: no consumer name")
    if current_name is not None and not current_pending_seen:
        raise RuntimeError(
            f"Malformed XINFO CONSUMERS output: consumer {current_name!r} has no pending count"
        )
    return consumers


def get_workers_with_pending_tasks(
    ssh_target: str,
    *,
    task_stream_pattern: str | None = None,
) -> set[str]:
    """Return worker consumer names with pending task-stream entries.

    The fleet CLI uses this as a last-moment safety check before destroying
    VMs. It mirrors the pool manager's PEL guard but uses the existing
    orchestrator-VM ``redis-cli`` path instead of requiring Redis access from
    the host where the CLI runs.
    """
    patterns = [task_stream_pattern] if task_stream_pattern else ["tasks:*", "*:tasks:*"]
    streams: set[str] = set()
    for pattern in patterns:
        result = _ssh(
            ssh_target,
            f"{_REDIS_CLI_PREFIX} --raw --scan --pattern {shlex.quote(pattern)}",
        )
        _require_redis_cli_success(result, "Failed to scan task streams")
        streams.update(ln.strip() for ln in result.stdout.splitlines() if ln.strip())

    busy: set[str] = set()
    for stream_name in sorted(streams):
        result = _ssh(
            ssh_target,
            f"{_REDIS_CLI_PREFIX} --raw TYPE {shlex.quote(stream_name)}",
        )
        _require_redis_cli_success(
            result,
            f"Failed to inspect Redis type for task-stream candidate {stream_name}",
        )
        redis_type = result.stdout.strip()
        if redis_type not in {"none", "string", "list", "set", "zset", "hash", "stream"}:
            raise RuntimeError(
                f"Failed to inspect Redis type for task-stream candidate {stream_name}: "
                f"unexpected TYPE output {redis_type!r}"
            )
        if redis_type != "stream":
            continue
        result = _ssh(
            ssh_target,
            f"{_REDIS_CLI_PREFIX} --raw XINFO CONSUMERS {shlex.quote(stream_name)} workers",
        )
        failure = _redis_cli_failure(result)
        if failure is not None:
            diagnostic = "\n".join(part for part in (result.stderr, result.stdout) if part).strip()
            lower = diagnostic.lower()
            if "nogroup" in lower or "no such key" in lower:
                continue
            raise RuntimeError(
                f"Failed to inspect task-stream consumers for {stream_name}: {failure}"
            )
        busy.update(_parse_xinfo_consumers_with_pending(result.stdout))
    return busy


def get_current_template_vmid(ssh_target: str) -> int | None:
    """Return the active worker template VMID from Redis, or ``None`` if unset.

    Reads ``orcest:pool:current_template_vmid`` via redis-cli on the
    orchestrator VM. Returns ``None`` for missing/empty/non-integer values.
    """
    result = _ssh(ssh_target, f"{_REDIS_CLI_PREFIX} --raw GET orcest:pool:current_template_vmid")
    _require_redis_cli_success(result, "Failed to read template pointer")
    raw = result.stdout.strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def set_current_template_vmid(ssh_target: str, vm_id: int) -> None:
    """Atomically swap the active worker template pointer in Redis.

    Sets ``orcest:pool:current_template_vmid`` to *vm_id*. The pool manager
    picks this up on its next reconciliation cycle (~10s).
    """
    result = _ssh(
        ssh_target,
        f"{_REDIS_CLI_PREFIX} SET orcest:pool:current_template_vmid {shlex.quote(str(vm_id))}",
    )
    _require_redis_cli_success(result, "Failed to set template pointer")


def get_current_template_revision(ssh_target: str) -> str | None:
    """Return the active worker template's baked source revision, or ``None``.

    Reads ``orcest:pool:current_template_revision`` -- set once by ``rebake``
    from the exact revision installed into the template -- so the template's
    revision remains visible for health reporting even while no worker VM
    from it is currently running.
    """
    from orcest.revision import normalize_revision

    result = _ssh(
        ssh_target, f"{_REDIS_CLI_PREFIX} --raw GET orcest:pool:current_template_revision"
    )
    _require_redis_cli_success(result, "Failed to read template revision")
    return normalize_revision(result.stdout.strip())


def set_current_template_revision(ssh_target: str, revision: str) -> None:
    """Persist the active worker template's baked source revision in Redis."""
    from orcest.revision import normalize_revision

    normalized = normalize_revision(revision)
    if normalized is None or normalized.endswith("-dirty"):
        raise ValueError(f"Refusing to persist a non-attested template revision: {revision!r}")
    result = _ssh(
        ssh_target,
        f"{_REDIS_CLI_PREFIX} SET orcest:pool:current_template_revision {shlex.quote(normalized)}",
    )
    _require_redis_cli_success(result, "Failed to set template revision")


def get_container_revision(ssh_target: str, compose_project: str, service: str) -> str | None:
    """Return the source revision baked into a running compose service's image.

    Reads the ``org.opencontainers.image.revision`` OCI label off the
    container's own image (not the ``orcest:latest`` tag, which may have
    moved since the container was created), so a coherent-but-not-yet
    recreated container reports the revision it is actually running rather
    than whatever was most recently built. Returns ``None`` when the service
    has no running container or the label is missing/unattested.
    """
    from orcest.revision import normalize_revision

    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", compose_project):
        raise ValueError(f"Invalid compose project name: {compose_project!r}")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", service):
        raise ValueError(f"Invalid compose service name: {service!r}")
    result = _ssh(
        ssh_target,
        f"cid=$(docker compose -p {shlex.quote(compose_project)} ps -q {shlex.quote(service)}) && "
        '[ -n "$cid" ] && docker inspect "$cid" '
        "--format '{{index .Config.Labels \"org.opencontainers.image.revision\"}}'",
    )
    if result.returncode != 0:
        return None
    return normalize_revision(result.stdout.strip())


def get_project_orchestrator_revision(ssh_target: str, project_name: str) -> str | None:
    """Return a project orchestrator container's baked source revision."""
    return get_container_revision(ssh_target, f"orcest-{project_name}", "orchestrator")


def get_pool_manager_revision(ssh_target: str) -> str | None:
    """Return the pool manager container's baked source revision."""
    return get_container_revision(ssh_target, "orcest-pool", "pool-manager")


def get_draining_worker_ids(ssh_target: str) -> set[str]:
    """Return worker IDs currently marked draining (retained for drain grace)."""
    result = _ssh(ssh_target, f"{_REDIS_CLI_PREFIX} --raw SMEMBERS orcest:pool:draining")
    _require_redis_cli_success(result, "Failed to read pool draining set")
    return {line.strip() for line in result.stdout.strip().splitlines() if line.strip()}


def clean_pool_redis(ssh_target: str, vm_ids: list[str]) -> None:
    """Remove destroyed VM generations and verify lifecycle markers are gone."""
    if not vm_ids:
        return
    cmds: list[str] = []
    for vm_id in vm_ids:
        quoted = shlex.quote(vm_id)
        worker_id = shlex.quote(f"orcest-worker-{vm_id}")
        cmds.append(f"{_REDIS_CLI_PREFIX} SREM orcest:pool:idle {quoted}")
        cmds.append(f"{_REDIS_CLI_PREFIX} HDEL orcest:pool:active {quoted}")
        cmds.append(f"{_REDIS_CLI_PREFIX} SREM orcest:pool:draining {worker_id}")
        cmds.append(f"{_REDIS_CLI_PREFIX} SREM orcest:pool:provisioning {quoted}")
        cmds.append(f"{_REDIS_CLI_PREFIX} SREM orcest:pool:ambiguous-clones {quoted}")
        cmds.append(f"{_REDIS_CLI_PREFIX} DEL orcest:pool:done:{worker_id}")
        cmds.append(f"{_REDIS_CLI_PREFIX} DEL orcest:workers:heartbeat:{worker_id}")
    result = _ssh(ssh_target, " && ".join(cmds))
    _require_redis_cli_success(result, "Failed to clean pool Redis state")

    # A stale durable done key or drain membership can target a later clone
    # after Proxmox reuses the same VMID. Verify both generation markers rather
    # than trusting a successful-looking cleanup response.
    for vm_id in vm_ids:
        worker_id = shlex.quote(f"orcest-worker-{vm_id}")
        verify = _ssh(
            ssh_target,
            f"{_REDIS_CLI_PREFIX} --raw EXISTS orcest:pool:done:{worker_id}"
            f" && {_REDIS_CLI_PREFIX} --raw EXISTS orcest:workers:heartbeat:{worker_id}"
            f" && {_REDIS_CLI_PREFIX} --raw SISMEMBER orcest:pool:draining {worker_id}"
            f" && {_REDIS_CLI_PREFIX} --raw SISMEMBER orcest:pool:provisioning {vm_id}"
            f" && {_REDIS_CLI_PREFIX} --raw SISMEMBER orcest:pool:ambiguous-clones {vm_id}",
        )
        _require_redis_cli_success(
            verify,
            f"Failed to verify pool Redis cleanup for VM {vm_id}",
        )
        states = [line.strip() for line in verify.stdout.splitlines() if line.strip()]
        if states != ["0", "0", "0", "0", "0"]:
            raise RuntimeError(
                f"Failed to verify pool Redis cleanup for VM {vm_id}: "
                f"expected lifecycle markers to be absent, got {states!r}"
            )


def set_workers_draining(
    ssh_target: str,
    worker_ids: list[str],
    *,
    draining: bool,
) -> None:
    """Add or remove worker consumer IDs from the shared drain set."""
    if not worker_ids:
        return
    command = "SADD" if draining else "SREM"
    members = " ".join(shlex.quote(worker_id) for worker_id in worker_ids)
    result = _ssh(
        ssh_target,
        f"{_REDIS_CLI_PREFIX} {command} orcest:pool:draining {members}",
    )
    failure = _redis_cli_failure(result)
    if failure is not None:
        action = "mark" if draining else "clear"
        raise RuntimeError(f"Failed to {action} worker drain state: {failure}")


def get_worker_heartbeat_details(ssh_target: str) -> dict[str, dict[str, object]]:
    """Return live worker heartbeat records with bounded provider CLI metadata."""
    pattern = "orcest:workers:heartbeat:*"
    result = _ssh(
        ssh_target,
        f"{_REDIS_CLI_PREFIX} --raw --scan --pattern {shlex.quote(pattern)}",
    )
    _require_redis_cli_success(result, "Failed to scan worker heartbeats")
    keys = sorted(line.strip() for line in result.stdout.splitlines() if line.strip())
    heartbeats: dict[str, dict[str, object]] = {}
    for key in keys:
        if not key.startswith("orcest:workers:heartbeat:"):
            raise RuntimeError("Worker heartbeat scan returned an unexpected key")
        value_result = _ssh(
            ssh_target,
            f"{_REDIS_CLI_PREFIX} --raw GET {shlex.quote(key)}",
        )
        _require_redis_cli_success(value_result, "Failed to read worker heartbeat")
        raw = value_result.stdout.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Worker heartbeat is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("Worker heartbeat must contain a JSON mapping")
        backend = payload.get("backend")
        revision = payload.get("revision")
        if not isinstance(backend, str) or not isinstance(revision, str):
            raise RuntimeError("Worker heartbeat is missing backend or revision")
        worker_id = key.removeprefix("orcest:workers:heartbeat:")
        if not worker_id:
            raise RuntimeError("Worker heartbeat is missing its worker ID")
        provider_cli = payload.get("provider_cli")
        heartbeats[worker_id] = {
            "backend": backend,
            "revision": revision,
            "provider_cli": provider_cli if isinstance(provider_cli, dict) else None,
        }
    return heartbeats


def get_worker_heartbeats(ssh_target: str) -> dict[str, tuple[str, str]]:
    """Return live ``worker_id -> (backend, revision)`` heartbeat records."""
    details = get_worker_heartbeat_details(ssh_target)
    return {
        worker_id: (str(record["backend"]), str(record["revision"]))
        for worker_id, record in details.items()
    }


def clean_pending_tasks(ssh_target: str) -> int:
    """Delete all pending task markers from Redis. Returns count deleted."""
    result = _ssh(
        ssh_target,
        f"{_REDIS_CLI_PREFIX} --scan --pattern 'orcest:pending:*'",
    )
    _require_redis_cli_success(result, "Failed to scan pending task markers")
    keys = [k.strip() for k in result.stdout.strip().splitlines() if k.strip()]
    if not keys:
        return 0
    quoted = " ".join(shlex.quote(k) for k in keys)
    result = _ssh(ssh_target, f"{_REDIS_CLI_PREFIX} DEL {quoted}")
    _require_redis_cli_success(result, "Failed to delete pending task markers")
    return len(keys)


def _project_compose_args(project_name: str) -> str:
    """Return ``-p`` value + ``--env-file`` flags for a per-project compose run.

    Layers two env files (Compose merges them; later wins on interpolation):
      1. :data:`REDIS_ENV_PATH` -- guarantees ${ORCEST_REDIS_PASSWORD} is present
         for the orchestrator container's AUTH even if a project's .env predates
         the C1 password write (e.g. an old onboarding).
      2. ``projects/<name>/.env`` -- the project's own credentials/config; takes
         precedence, and (post-C1) also carries ORCEST_REDIS_PASSWORD from
         :func:`generate_env_file`.

    ``_validate_project_name`` restricts to ``[a-zA-Z0-9._-]``, but the values
    are shell-quoted for defense-in-depth in case that validation is relaxed.
    """
    qname = shlex.quote(f"orcest-{project_name}")
    qredis = shlex.quote(REDIS_ENV_PATH)
    qenv = shlex.quote(f"projects/{project_name}/.env")
    return f"{qname} --env-file {qredis} --env-file {qenv}"


def deploy_stack(ssh_target: str, project_name: str) -> None:
    """Start/update a per-project Docker Compose stack.

    Runs docker compose from /opt/orcest/ using the main docker-compose.yml
    with a project-specific env file and compose project name.
    """
    _validate_project_name(project_name)
    logger.info("Deploying stack orcest-%s on %s", project_name, ssh_target)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && docker compose -p {_project_compose_args(project_name)} up -d",
    )
    if result.returncode != 0:
        logger.error("Deploy failed: %s", result.stderr.strip())
        raise RuntimeError(f"Failed to deploy stack orcest-{project_name}: {result.stderr.strip()}")
    logger.info("Stack orcest-%s deployed on %s", project_name, ssh_target)


def stop_stack(ssh_target: str, project_name: str) -> None:
    """Stop a project orchestrator without deleting its containers or volumes."""
    _validate_project_name(project_name)
    logger.info("Stopping stack orcest-%s on %s", project_name, ssh_target)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && docker compose -p {_project_compose_args(project_name)} stop",
    )
    if result.returncode != 0:
        logger.error("Stop failed: %s", result.stderr.strip())
        raise RuntimeError(f"Failed to stop stack orcest-{project_name}: {result.stderr.strip()}")
    logger.info("Stack orcest-%s stopped on %s", project_name, ssh_target)


def teardown_stack(ssh_target: str, project_name: str) -> None:
    """Stop and remove a per-project Docker Compose stack."""
    _validate_project_name(project_name)
    logger.info("Tearing down stack orcest-%s on %s", project_name, ssh_target)
    result = _ssh(
        ssh_target,
        f"cd /opt/orcest && docker compose -p {_project_compose_args(project_name)} down -v",
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
        f"cd /opt/orcest && docker compose -p {_project_compose_args(project_name)}"
        " up -d --force-recreate",
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
    result = _ssh(ssh_target, f"mkdir -p {shlex.quote(f'{pdir}/config')}")
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create project directory: {result.stderr.strip()}")

    # Write .env file via temp file + scp + mv
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as tmp:
        tmp.write(env_content)
        tmp_env_path = tmp.name

    try:
        remote_tmp_env = _remote_private_tmp(ssh_target, f"/tmp/orcest-{project_name}-env.XXXXXX")
        qremote_tmp_env = shlex.quote(remote_tmp_env)
        try:
            result = _scp(tmp_env_path, ssh_target, remote_tmp_env)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to upload .env: {result.stderr.strip()}")
            qenv_dest = shlex.quote(f"{pdir}/.env")
            result = _ssh(
                ssh_target,
                f"mv {qremote_tmp_env} {qenv_dest} && chmod 600 {qenv_dest}",
            )
            if result.returncode != 0:
                raise RuntimeError(f"Failed to install .env: {result.stderr.strip()}")
        finally:
            _ssh(ssh_target, f"rm -f {qremote_tmp_env}")
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
        remote_tmp_config = _remote_private_tmp(
            ssh_target, f"/tmp/orcest-{project_name}-config.XXXXXX"
        )
        qremote_tmp_config = shlex.quote(remote_tmp_config)
        try:
            result = _scp(tmp_config_path, ssh_target, remote_tmp_config)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to upload config: {result.stderr.strip()}")
            qconfig_dest = shlex.quote(f"{pdir}/config/orchestrator.yaml")
            result = _ssh(
                ssh_target,
                f"mv {qremote_tmp_config} {qconfig_dest} && chmod 644 {qconfig_dest}",
            )
            if result.returncode != 0:
                raise RuntimeError(f"Failed to install config: {result.stderr.strip()}")
        finally:
            _ssh(ssh_target, f"rm -f {qremote_tmp_config}")
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
    claude_tokens: list[str] | None = None,
    claude_token: str = "",
    provider_credentials: dict[str, list[str]] | None = None,
    trace_archive_host_path: str | None = None,
    workflow_state_host_path: str | None = None,
    redis_password: str = "",
    monitor_write_token: str = "",
) -> str:
    """Generate .env file content for a project's Docker Compose stack.

    Values are single-quoted to prevent Docker Compose from performing
    variable interpolation (``$`` references) or word splitting.

    Accepts either ``claude_tokens`` (list, preferred) or ``claude_token``
    (single string, backward compat) for the claude provider, plus an
    optional ``provider_credentials`` map for all providers (including
    "claude" overrides). This generalizes the old claude-only behaviour
    so that fleet can emit XAI_API_KEY, etc. for providers: entries that
    rely on env-var fallback (see shared/config.py _parse_provider_entry
    and rollout-multi-provider.md).

    Canonical env var names for first credential:
      claude -> CLAUDE_CODE_OAUTH_TOKEN (+ _TOKENS comma list)
      grok   -> XAI_API_KEY (also accepts GROK_API_KEY in parsers)
      other  -> <UPPER>_API_KEY
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
    if redis_password:
        # C1: forwarded to the orchestrator container via docker-compose.yml so
        # it can AUTH to the now-password-protected Redis. The same value backs
        # the redis stack's --requirepass; the .env is written 0600.
        _validate_env_value(redis_password, "redis_password")
        lines.append(f"ORCEST_REDIS_PASSWORD='{redis_password}'")
    if monitor_write_token:
        _validate_env_value(monitor_write_token, "monitor_write_token")
        lines.append(f"MONITOR_WRITE_TOKEN='{monitor_write_token}'")
    if trace_archive_host_path:
        _validate_env_value(trace_archive_host_path, "trace_archive_host_path")
        if not trace_archive_host_path.startswith("/"):
            # Compose bind-mount sources must be absolute; a relative value
            # would silently bind ``./<value>`` from the per-project compose
            # cwd, which is a footgun rather than a useful behavior.
            raise ValueError(
                "trace_archive_host_path must be an absolute path "
                f"(got {trace_archive_host_path!r})"
            )
        lines.append(f"ORCEST_TRACE_HOST_PATH='{trace_archive_host_path}'")
        # Mount the fleet config into the orchestrator container so the
        # archiver can build a global repo→project map. /etc/orcest/config.yaml
        # is the canonical location on the orchestrator VM (already populated
        # by ``upload_fleet_config`` for the pool manager).
        lines.append("ORCEST_FLEET_CONFIG_PATH='/etc/orcest/config.yaml'")
    if workflow_state_host_path:
        _validate_env_value(workflow_state_host_path, "workflow_state_host_path")
        if not workflow_state_host_path.startswith("/"):
            raise ValueError(
                "workflow_state_host_path must be an absolute path "
                f"(got {workflow_state_host_path!r})"
            )
        lines.append(f"ORCEST_WORKFLOW_STATE_HOST_PATH='{workflow_state_host_path}'")

    # Build a unified map: provider -> list of credentials
    creds: dict[str, list[str]] = {}
    # Legacy claude path
    tokens = claude_tokens if claude_tokens else ([claude_token] if claude_token else [])
    if tokens:
        for i, t in enumerate(tokens):
            _validate_env_value(t, f"claude_tokens[{i}]")
        creds["claude"] = tokens

    # New generalized path (may override claude or add grok/others)
    for prov, clist in (provider_credentials or {}).items():
        if not clist:
            continue
        for i, t in enumerate(clist):
            _validate_env_value(t, f"provider_credentials[{prov}][{i}]")
        # merge: explicit provider_credentials wins for that provider
        creds[prov] = clist

    # Emit in deterministic order (claude first for diff stability, then others)
    for prov in sorted(creds.keys(), key=lambda p: (0 if p == "claude" else 1, p)):
        toks = creds[prov]
        if prov == "claude":
            lines.append(f"CLAUDE_CODE_OAUTH_TOKEN='{toks[0]}'")
            lines.append(f"CLAUDE_CODE_OAUTH_TOKENS='{','.join(toks)}'")
        else:
            # Canonical env var name used by config fallback logic.
            # Only the singular form is emitted: no parser, worker, or compose
            # file currently reads a plural <PROV>_API_KEYS env var, so emitting
            # one would just produce dead output in generated .env files. Add a
            # reader (mirroring CLAUDE_CODE_OAUTH_TOKENS consumption in
            # shared/config.py) before reintroducing the plural form. See
            # docs/rollout-multi-provider.md#fleet-credential-multiplicity.
            env_name = {"grok": "XAI_API_KEY"}.get(prov, f"{prov.upper()}_API_KEY")
            lines.append(f"{env_name}='{toks[0]}'")

    return "\n".join(lines) + "\n"


def generate_orchestrator_config(
    repo: str,
    key_prefix: str,
    task_key_prefix: str = "orcest",
    extra_providers: list[str] | None = None,
    default_runner: str | None = None,
    trace_archive_enabled: bool = False,
    workflow_state_enabled: bool = False,
    monitor_ingest_url: str | None = None,
) -> str:
    """Generate orchestrator.yaml content for a project.

    Uses redis host 'redis' (Docker network service name), port 6379,
    and the project's key prefix for namespace isolation. The
    ``task_key_prefix`` is the shared prefix used for the task stream
    that workers read from.

    ``extra_providers`` lists additional provider names the org has
    credentials for (e.g. ["grok", "clauder"]). Each is emitted as a declarative
    ``providers:`` entry with an empty credential, so the orchestrator
    resolves the value from the generated ``.env`` via the env-var fallback
    (``_PROVIDER_ENV_CANDIDATES`` in shared/config.py — e.g. XAI_API_KEY for
    grok). Legacy ``claude`` is intentionally omitted: it is synthesized from
    CLAUDE_CODE_OAUTH_TOKENS by the legacy path and coexists with this list.
    ``default_runner`` lets fleet deployment align the generated orchestrator
    with the configured worker pool backend, so a pool consuming ``tasks:clauder``
    receives Claude-backed work even when the org only uses legacy Claude tokens.
    """
    config: dict = {
        "redis": {"host": "redis", "port": 6379, "key_prefix": key_prefix},
        "task_key_prefix": task_key_prefix,
        "github": {"repo": repo},
    }
    providers = [p for p in sorted(extra_providers or []) if p and p != "claude"]
    normalized_default_runner = (default_runner or "").strip()
    if not normalized_default_runner and "clauder" in providers:
        normalized_default_runner = "clauder"
    if normalized_default_runner and normalized_default_runner != "claude":
        config["default_runner"] = normalized_default_runner
    if providers:
        config["providers"] = [{"provider": p, "credential": "", "model": ""} for p in providers]
    if trace_archive_enabled:
        # In-container path; the operator bind-mounts whatever filesystem they
        # want at ORCEST_TRACE_HOST_PATH on the host side (see docker-compose.yml).
        config["trace_archive_path"] = "/var/lib/orcest/traces"
    if workflow_state_enabled:
        config["workflow_state_root"] = "/var/lib/orcest/workflow"
    if monitor_ingest_url:
        config["monitor_ingest_url"] = monitor_ingest_url
        config["monitor_write_token_env"] = "MONITOR_WRITE_TOKEN"
    return yaml.dump(config, default_flow_style=False, sort_keys=False)
