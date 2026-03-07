"""Fleet inventory schema and I/O.

The fleet inventory is a YAML file (typically ``/opt/orcest/fleet.yaml`` on the
orchestrator VM) that describes all projects, their orchestrator stacks, and
worker VMs managed by orcest.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class WorkerEntry:
    """A single worker VM in the fleet."""

    vm_id: int


@dataclass
class ProjectEntry:
    """A project managed by orcest (one orchestrator stack + N workers)."""

    name: str
    repo: str  # "owner/repo" format
    redis_port: int = 6379
    workers: list[WorkerEntry] = field(default_factory=list)


@dataclass
class ProxmoxConfig:
    """Proxmox API connection details."""

    host: str = ""
    node: str = "pve"
    storage: str = "local-lvm"
    token_id: str = ""  # e.g. "root@pam!orcest"
    token_secret: str = ""
    verify_ssl: bool = False


@dataclass
class FleetInventory:
    """Top-level fleet inventory."""

    orchestrator_host: str = ""
    orchestrator_user: str = "thayne"
    proxmox: ProxmoxConfig = field(default_factory=ProxmoxConfig)
    projects: list[ProjectEntry] = field(default_factory=list)

    def get_project(self, name: str) -> ProjectEntry | None:
        """Look up a project by name."""
        for p in self.projects:
            if p.name == name:
                return p
        return None

    def next_redis_port(self) -> int:
        """Return the next available Redis port (max existing + 1, or 6379)."""
        if not self.projects:
            return 6379
        return max(p.redis_port for p in self.projects) + 1

    def next_vm_id(self) -> int:
        """Return the next available VM ID (max existing + 1, or 200)."""
        all_ids = [w.vm_id for p in self.projects for w in p.workers]
        if not all_ids:
            return 200
        return max(all_ids) + 1

    def all_vm_ids(self) -> set[int]:
        """Return all VM IDs in use across the fleet."""
        return {w.vm_id for p in self.projects for w in p.workers}


def load_inventory(path: str | Path) -> FleetInventory:
    """Load a fleet inventory from a YAML file."""
    path = Path(path)
    if not path.exists():
        return FleetInventory()

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    proxmox_data = data.get("proxmox") or {}
    proxmox = ProxmoxConfig(
        host=proxmox_data.get("host", ""),
        node=proxmox_data.get("node", "pve"),
        storage=proxmox_data.get("storage", "local-lvm"),
        token_id=proxmox_data.get("token_id", ""),
        token_secret=proxmox_data.get("token_secret", ""),
        verify_ssl=proxmox_data.get("verify_ssl", False),
    )

    projects: list[ProjectEntry] = []
    for proj_data in data.get("projects") or []:
        workers = [WorkerEntry(vm_id=w["vm_id"]) for w in (proj_data.get("workers") or [])]
        projects.append(
            ProjectEntry(
                name=proj_data["name"],
                repo=proj_data["repo"],
                redis_port=proj_data.get("redis_port", 6379),
                workers=workers,
            )
        )

    return FleetInventory(
        orchestrator_host=data.get("orchestrator_host", ""),
        orchestrator_user=data.get("orchestrator_user", "thayne"),
        proxmox=proxmox,
        projects=projects,
    )


def save_inventory(inventory: FleetInventory, path: str | Path) -> None:
    """Save a fleet inventory to a YAML file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data: dict = {
        "orchestrator_host": inventory.orchestrator_host,
        "orchestrator_user": inventory.orchestrator_user,
        "proxmox": {
            "host": inventory.proxmox.host,
            "node": inventory.proxmox.node,
            "storage": inventory.proxmox.storage,
            "token_id": inventory.proxmox.token_id,
            "token_secret": inventory.proxmox.token_secret,
            "verify_ssl": inventory.proxmox.verify_ssl,
        },
        "projects": [],
    }

    for proj in inventory.projects:
        proj_data: dict = {
            "name": proj.name,
            "repo": proj.repo,
            "redis_port": proj.redis_port,
            "workers": [{"vm_id": w.vm_id} for w in proj.workers],
        }
        data["projects"].append(proj_data)

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
