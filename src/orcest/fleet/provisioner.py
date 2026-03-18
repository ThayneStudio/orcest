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

    Always runs ``tofu init`` first to ensure any new .tf files (e.g.
    outputs.tf added by ``orcest upgrade``) are picked up by the state.
    """
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

    Terraform manages only the orchestrator VM. Worker VMs are managed by the
    pool manager via the Proxmox API (ephemeral VMs cloned from a template).
    """
    from orcest.fleet.cloud_init import render_orchestrator_userdata

    orchestrator_userdata = render_orchestrator_userdata(
        ssh_public_key=config.orchestrator.ssh_key,
    )

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
        "workers": {},
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


def _ensure_ssh_agent() -> dict[str, str]:
    """Ensure ssh-agent is running and has at least one key loaded.

    Returns environment variables to pass to subprocesses so they can
    reach the agent.  Starts a new agent and loads the default key if
    one is not already available.
    """
    env = dict(os.environ)

    # Check if an agent is already usable
    check = subprocess.run(
        ["ssh-add", "-l"], capture_output=True, text=True, env=env,
    )
    if check.returncode == 0:
        return env  # agent running with keys

    # Start a new agent
    result = subprocess.run(
        ["ssh-agent", "-s"], capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.warning("Could not start ssh-agent: %s", result.stderr)
        return env

    # Parse SSH_AUTH_SOCK and SSH_AGENT_PID from agent output
    for line in result.stdout.splitlines():
        for var in ("SSH_AUTH_SOCK", "SSH_AGENT_PID"):
            if line.startswith(f"{var}="):
                value = line.split("=", 1)[1].split(";", 1)[0]
                env[var] = value
                os.environ[var] = value

    # Load default key
    add = subprocess.run(
        ["ssh-add"], capture_output=True, text=True, env=env,
    )
    if add.returncode != 0:
        logger.warning("ssh-add failed: %s", add.stderr)

    return env


def _run_tofu(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a ``tofu`` command, raising on failure."""
    env = _ensure_ssh_agent()
    cmd = ["tofu", *args]
    logger.info("Running: %s (cwd=%s)", " ".join(cmd), cwd)
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        logger.error("tofu failed:\n%s", result.stderr)
        raise RuntimeError(f"tofu {args[0]} failed: {result.stderr.strip()}")
    return result
