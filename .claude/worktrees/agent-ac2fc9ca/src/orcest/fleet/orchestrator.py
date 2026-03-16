"""Orchestrator VM management: source upload, Docker build, stack lifecycle.

Manages the Docker Compose stacks on the orchestrator VM via SSH.
Each project gets its own stack directory under ``/opt/orcest/projects/{name}/``.

NOTE: This is a stub that will be replaced by the full implementation.
"""

from __future__ import annotations

import subprocess
import textwrap

_SSH_OPTS = ["-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new"]


def _ssh(ssh_target: str, cmd: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, cmd],
        capture_output=True,
        text=True,
    )


def build_image(ssh_target: str) -> None:
    """Build the orcest-orchestrator Docker image on the remote host."""
    result = _ssh(ssh_target, "cd /opt/orcest && docker compose build")
    if result.returncode != 0:
        raise RuntimeError(f"Docker build failed: {result.stderr.strip()}")


def upload_source(ssh_target: str) -> None:
    """Create and upload a source tarball to the orchestrator VM."""
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tarball = tmp.name

    try:
        subprocess.run(
            ["tar", "czf", tarball, "Dockerfile", "docker-compose.yml", "pyproject.toml", "src/"],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["scp", *_SSH_OPTS, tarball, f"{ssh_target}:/tmp/orcest-source.tar.gz"],
            check=True,
            capture_output=True,
            text=True,
        )
        result = _ssh(
            ssh_target,
            "sudo -u orcest tar xzf /tmp/orcest-source.tar.gz -C /opt/orcest/",
        )
        if result.returncode != 0:
            raise RuntimeError(f"Source extraction failed: {result.stderr.strip()}")
    finally:
        os.unlink(tarball)


def deploy_stack(ssh_target: str, project_name: str) -> None:
    """Start a per-project Docker Compose stack."""
    pdir = f"/opt/orcest/projects/{project_name}"
    result = _ssh(
        ssh_target,
        f"sudo -u orcest bash -c 'cd {pdir} && docker compose up -d'",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Stack deploy failed: {result.stderr.strip()}")


def teardown_stack(ssh_target: str, project_name: str) -> None:
    """Stop and remove a per-project Docker Compose stack."""
    pdir = f"/opt/orcest/projects/{project_name}"
    result = _ssh(
        ssh_target,
        f"if [ -d {pdir} ]; then"
        f" sudo -u orcest bash -c 'cd {pdir} && docker compose down -v';"
        f" sudo -u orcest rm -rf {pdir}; fi",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Stack teardown failed: {result.stderr.strip()}")


def restart_stack(ssh_target: str, project_name: str) -> None:
    """Restart a per-project Docker Compose stack."""
    pdir = f"/opt/orcest/projects/{project_name}"
    result = _ssh(
        ssh_target,
        f"sudo -u orcest bash -c 'cd {pdir} && docker compose up -d'",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Stack restart failed: {result.stderr.strip()}")


def write_project_files(
    ssh_target: str,
    project_name: str,
    env_content: str,
    config_yaml: str,
) -> None:
    """Write .env and config/orchestrator.yaml for a project on the orchestrator."""
    pdir = f"/opt/orcest/projects/{project_name}"

    # Create project directory
    result = _ssh(ssh_target, f"sudo -u orcest mkdir -p {pdir}/config")
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create project dir: {result.stderr.strip()}")

    # Write docker-compose.yml
    compose = _render_project_compose(project_name)
    compose_cmd = f"sudo -u orcest bash -c 'cat > {pdir}/docker-compose.yml'"
    result = subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, compose_cmd],
        input=compose,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to write compose file: {result.stderr.strip()}")

    # Write .env
    result = subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, f"sudo -u orcest bash -c 'umask 077 && cat > {pdir}/.env'"],
        input=env_content,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to write .env: {result.stderr.strip()}")

    # Write orchestrator config
    result = subprocess.run(
        [
            "ssh",
            *_SSH_OPTS,
            ssh_target,
            f"sudo -u orcest bash -c 'cat > {pdir}/config/orchestrator.yaml'",
        ],
        input=config_yaml,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to write config: {result.stderr.strip()}")


def generate_env_file(github_token: str, redis_port: int, project_name: str) -> str:
    """Generate .env file content for a project stack."""
    return f"GITHUB_TOKEN={github_token}\nGH_TOKEN={github_token}\n"


def generate_orchestrator_config(repo: str, redis_port: int = 6379) -> str:
    """Generate orchestrator.yaml content for a project."""
    return textwrap.dedent(f"""\
        redis:
          host: redis
          port: 6379

        github:
          repo: "{repo}"

        polling:
          interval: 60

        default_runner: "claude"

        labels:
          blocked: "orcest:blocked"
          needs_human: "orcest:needs-human"
          ready: "orcest:ready"
    """)


def _render_project_compose(project_name: str) -> str:
    """Return a minimal docker-compose.yml for a project stack."""
    return textwrap.dedent("""\
        services:
          redis:
            image: redis:7-alpine
            restart: unless-stopped
          orchestrator:
            image: orcest-orchestrator:latest
            depends_on:
              - redis
            restart: unless-stopped
    """)
