"""Provisioning bridge: Python wrapper around OpenTofu.

This is the abstraction boundary between orcest fleet commands and the
underlying infrastructure provisioner. If we want to swap OpenTofu for
raw qm commands or another tool later, only this module changes.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from orcest.fleet.config import FleetConfig

logger = logging.getLogger(__name__)

TERRAFORM_DIR = Path("/etc/orcest/terraform")


def init(config_dir: Path = TERRAFORM_DIR) -> None:
    """Run ``tofu init`` in the terraform directory."""
    _run_tofu(["init", "-input=false"], cwd=config_dir)


def plan(config_dir: Path = TERRAFORM_DIR) -> str:
    """Run ``tofu plan`` and return stdout."""
    result = _run_tofu(["plan", "-input=false", "-no-color"], cwd=config_dir)
    return result.stdout


def apply(config_dir: Path = TERRAFORM_DIR) -> None:
    """Run ``tofu apply -auto-approve``.

    Automatically runs ``tofu init`` first if the ``.terraform`` directory
    does not exist yet (e.g. first run after copying HCL templates).
    """
    if not (config_dir / ".terraform").is_dir():
        logger.info("No .terraform directory found, running init first")
        init(config_dir)
    _run_tofu(["apply", "-auto-approve", "-input=false"], cwd=config_dir)


def destroy_resource(resource_addr: str, config_dir: Path = TERRAFORM_DIR) -> None:
    """Destroy a specific resource by address."""
    _run_tofu(
        ["destroy", "-auto-approve", "-target", resource_addr, "-input=false"],
        cwd=config_dir,
    )


def get_output(name: str, config_dir: Path = TERRAFORM_DIR) -> Any:
    """Get a terraform output value."""
    result = _run_tofu(["output", "-json", name], cwd=config_dir)
    return json.loads(result.stdout)


def generate_tfvars(config: FleetConfig) -> dict[str, Any]:
    """Convert a :class:`FleetConfig` into a dict suitable for ``terraform.tfvars.json``.

    This is the key translation layer between the orcest YAML config and the
    Terraform variable schema.  It:

    1. Renders cloud-init user-data for the orchestrator VM.
    2. Renders cloud-init user-data for each worker VM (one per project x worker index).
    3. Allocates VM IDs for workers (orchestrator VM ID + 1, incrementing).
    4. Returns a dict whose keys match ``variables.tf``.
    """
    from orcest.fleet.cloud_init import render_orchestrator_userdata, render_worker_userdata

    # Render orchestrator user-data
    orchestrator_userdata = render_orchestrator_userdata(
        ssh_public_key=config.orchestrator.ssh_key,
    )

    # Build worker entries: one per (project, worker-index) pair.
    # VM IDs start at orchestrator.vm_id + 1 and increment.
    next_vm_id = config.orchestrator.vm_id + 1
    workers: dict[str, dict[str, Any]] = {}

    for project in config.projects:
        org = config.resolve_org(project)

        for i in range(project.workers):
            key = f"{project.name}-{i}"
            worker_id = f"worker-{next_vm_id}"

            worker_userdata = render_worker_userdata(
                redis_host=config.orchestrator.host or "localhost",
                key_prefix=project.name,
                worker_id=worker_id,
                github_token=org.github_token,
                claude_oauth_token=org.claude_oauth_token,
                repo=project.repo,
                ssh_public_key=config.orchestrator.ssh_key,
            )

            workers[key] = {
                "vm_id": next_vm_id,
                "project_name": project.name,
                "memory": project.worker_memory,
                "cores": project.worker_cores,
                "disk_size": project.worker_disk_size,
                "cloud_init_content": worker_userdata,
            }
            next_vm_id += 1

    if not config.proxmox.api_token_id or not config.proxmox.api_token_secret:
        raise ValueError(
            "Proxmox API token not configured — set api_token_id and api_token_secret "
            "in the config, or run: orcest init"
        )

    return {
        "proxmox_endpoint": config.proxmox.endpoint,
        "proxmox_api_token": (f"{config.proxmox.api_token_id}={config.proxmox.api_token_secret}"),
        "proxmox_node": config.proxmox.node,
        "proxmox_storage": config.proxmox.storage,
        "orchestrator": {
            "vm_id": config.orchestrator.vm_id,
            "memory": config.orchestrator.memory,
            "cores": config.orchestrator.cores,
            "disk_size": config.orchestrator.disk_size,
            "cloud_init_content": orchestrator_userdata,
        },
        "workers": workers,
    }


def write_tfvars(tfvars: dict[str, Any], config_dir: Path = TERRAFORM_DIR) -> None:
    """Write a tfvars dict as ``terraform.tfvars.json``."""
    import contextlib

    path = config_dir / "terraform.tfvars.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(tfvars, f, indent=2)
        tmp_path.rename(path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise
    logger.info("Wrote %s", path)


def _run_tofu(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a ``tofu`` command, raising on failure."""
    cmd = ["tofu", *args]
    logger.info("Running: %s (cwd=%s)", " ".join(cmd), cwd)
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("tofu failed:\n%s", result.stderr)
        raise RuntimeError(f"tofu {args[0]} failed: {result.stderr.strip()}")
    return result
