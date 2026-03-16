"""Terraform/OpenTofu provisioner for fleet VMs.

Wraps ``tofu`` CLI commands for managing Proxmox VMs via Terraform.
This module is called by the fleet CLI commands to create, update,
and destroy infrastructure.

NOTE: This is a stub that will be replaced by the full implementation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from orcest.fleet.config import FleetConfig

TERRAFORM_DIR = Path("/etc/orcest/terraform")


def init(config_dir: Path = TERRAFORM_DIR) -> None:
    """Run ``tofu init`` in the Terraform directory."""
    import subprocess

    subprocess.run(
        ["tofu", "init"],
        cwd=str(config_dir),
        check=True,
        capture_output=True,
        text=True,
    )


def apply(config_dir: Path = TERRAFORM_DIR) -> None:
    """Run ``tofu apply -auto-approve`` in the Terraform directory."""
    import subprocess

    subprocess.run(
        ["tofu", "apply", "-auto-approve"],
        cwd=str(config_dir),
        check=True,
        capture_output=True,
        text=True,
    )


def destroy_resource(resource_addr: str, config_dir: Path = TERRAFORM_DIR) -> None:
    """Destroy a specific Terraform resource."""
    import subprocess

    subprocess.run(
        ["tofu", "destroy", "-target", resource_addr, "-auto-approve"],
        cwd=str(config_dir),
        check=True,
        capture_output=True,
        text=True,
    )


def plan(config_dir: Path = TERRAFORM_DIR) -> str:
    """Run ``tofu plan`` and return the output."""
    import subprocess

    result = subprocess.run(
        ["tofu", "plan", "-no-color"],
        cwd=str(config_dir),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def get_output(name: str, config_dir: Path = TERRAFORM_DIR) -> Any:
    """Get a Terraform output value."""
    import json
    import subprocess

    result = subprocess.run(
        ["tofu", "output", "-json", name],
        cwd=str(config_dir),
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def generate_tfvars(config: FleetConfig) -> dict:
    """Generate terraform.tfvars content from a FleetConfig."""
    return {
        "proxmox_node": config.proxmox.node,
        "proxmox_storage": config.proxmox.storage,
        "proxmox_api_token_id": config.proxmox.api_token_id,
        "proxmox_api_token_secret": config.proxmox.api_token_secret,
        "proxmox_endpoint": config.proxmox.endpoint,
        "orchestrator_vm_id": config.orchestrator.vm_id,
        "orchestrator_memory": config.orchestrator.memory,
        "orchestrator_cores": config.orchestrator.cores,
        "orchestrator_disk_size": config.orchestrator.disk_size,
        "orchestrator_ssh_key": config.orchestrator.ssh_key,
        "projects": [
            {
                "name": p.name,
                "repo": p.repo,
                "redis_port": p.redis_port,
                "workers": p.workers,
            }
            for p in config.projects
        ],
    }


def write_tfvars(tfvars: dict, config_dir: Path = TERRAFORM_DIR) -> None:
    """Write tfvars dict as terraform.tfvars.json."""
    import json

    config_dir.mkdir(parents=True, exist_ok=True)
    tfvars_path = config_dir / "terraform.tfvars.json"
    with open(tfvars_path, "w") as f:
        json.dump(tfvars, f, indent=2)
