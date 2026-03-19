"""Fleet configuration schema and I/O.

Supports multiple orgs (each with independent tokens), Proxmox
auto-detection fields, and an orchestrator VM managed via OpenTofu.

Config lives at ``/etc/orcest/config.yaml`` on the Proxmox host.
"""

from __future__ import annotations

import contextlib
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


def validate_project_name(name: str) -> bool:
    """Return True if *name* is a valid project name (safe for use in shell commands)."""
    return bool(SAFE_NAME_RE.match(name)) and len(name) <= 64


def require_valid_project_name(name: str) -> None:
    """Raise ValueError if *name* is not a valid project name."""
    if not validate_project_name(name):
        raise ValueError(
            f"Invalid project name {name!r}: must be 1-64 chars, "
            "alphanumeric/dot/hyphen/underscore, starting with alphanumeric."
        )


@dataclass
class ProxmoxConfig:
    """Proxmox connection details (auto-detected by ``orcest init``)."""

    endpoint: str = "https://127.0.0.1:8006"  # Proxmox API URL
    node: str = "pve"
    storage: str = "local-lvm"
    api_token_id: str = ""  # e.g. "root@pam!orcest"
    api_token_secret: str = ""

    def is_localhost(self) -> bool:
        """Return True if the endpoint points to localhost (unreachable from VMs)."""
        from urllib.parse import urlparse

        host = urlparse(self.endpoint).hostname or ""
        return host in ("127.0.0.1", "localhost", "::1")


@dataclass
class OrchestratorConfig:
    """Orchestrator VM settings."""

    vm_id: int = 199
    host: str = ""  # filled after create-orchestrator
    user: str = "orcest"
    ssh_key: str = ""
    memory: int = 4096
    cores: int = 2
    disk_size: int = 20  # GB


@dataclass
class OrgEntry:
    """An organisation registered with the fleet."""

    github_token: str = ""
    claude_oauth_token: str = ""


@dataclass
class ProjectEntry:
    """A project managed by orcest."""

    name: str = ""
    repo: str = ""  # "org/repo" format


@dataclass
class PoolConfig:
    """Ephemeral worker VM pool settings."""

    size: int = 4  # Target warm pool size
    template_vm_id: int = 0  # Template to clone from (0 = not configured)
    storage: str = "ssd-pool"  # ZFS pool for linked clones
    worker_memory: int = 16384  # MB per worker VM
    worker_cores: int = 8
    worker_disk_size: int = 30  # GB
    max_task_duration: int = 3600  # seconds before force-kill


@dataclass
class FleetConfig:
    """Top-level fleet configuration."""

    proxmox: ProxmoxConfig = field(default_factory=ProxmoxConfig)
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)
    orgs: dict[str, OrgEntry] = field(default_factory=dict)
    projects: list[ProjectEntry] = field(default_factory=list)
    pool: PoolConfig = field(default_factory=PoolConfig)

    # ── helpers ──────────────────────────────────────────────

    def get_project(self, name: str) -> ProjectEntry | None:
        for p in self.projects:
            if p.name == name:
                return p
        return None

    def resolve_org(self, project: ProjectEntry) -> OrgEntry:
        """Resolve the org entry for a project by extracting the owner from its repo field."""
        org_name = project.repo.split("/")[0]
        if org_name not in self.orgs:
            raise KeyError(
                f"Org '{org_name}' not registered — run: orcest fleet add-org {org_name}"
            )
        return self.orgs[org_name]

    def ssh_target(self) -> str:
        """Return user@host for the orchestrator VM."""
        if not self.orchestrator.host:
            raise RuntimeError("Orchestrator host not set — run: orcest fleet create-orchestrator")
        return f"{self.orchestrator.user}@{self.orchestrator.host}"


# ── persistence ──────────────────────────────────────────────

DEFAULT_CONFIG_DIR = Path("/etc/orcest")
DEFAULT_CONFIG_PATH = DEFAULT_CONFIG_DIR / "config.yaml"


def _parse_disk_size(value: int | str) -> int:
    """Convert a disk size value to an integer (GB).

    Accepts plain ints, numeric strings, or strings with a 'G'/'GB' suffix
    for backward compatibility with older config files.
    """
    if isinstance(value, int):
        return value
    s = str(value).strip().upper().removesuffix("GB").removesuffix("G")
    try:
        return int(s)
    except ValueError:
        raise ValueError(f"Invalid disk_size {value!r}: expected an integer (GB)") from None


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> FleetConfig:
    """Load fleet config from a YAML file."""
    path = Path(path)
    if not path.exists():
        return FleetConfig()

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    px = data.get("proxmox") or {}
    proxmox = ProxmoxConfig(
        endpoint=px.get("endpoint", "https://127.0.0.1:8006"),
        node=px.get("node", "pve"),
        storage=px.get("storage", "local-lvm"),
        api_token_id=px.get("api_token_id", ""),
        api_token_secret=px.get("api_token_secret", ""),
    )

    orch = data.get("orchestrator") or {}
    orchestrator = OrchestratorConfig(
        vm_id=orch.get("vm_id", 199),
        host=orch.get("host", ""),
        user=orch.get("user", "orcest"),
        ssh_key=orch.get("ssh_key", ""),
        memory=orch.get("memory", 4096),
        cores=orch.get("cores", 2),
        disk_size=_parse_disk_size(orch.get("disk_size", 20)),
    )

    orgs: dict[str, OrgEntry] = {}
    for name, entry in (data.get("orgs") or {}).items():
        orgs[name] = OrgEntry(
            github_token=entry.get("github_token", ""),
            claude_oauth_token=entry.get("claude_oauth_token", ""),
        )

    projects: list[ProjectEntry] = []
    for proj in data.get("projects") or []:
        projects.append(
            ProjectEntry(
                name=proj["name"],
                repo=proj["repo"],
            )
        )

    pl = data.get("pool") or {}
    pool = PoolConfig(
        size=pl.get("size", 4),
        template_vm_id=pl.get("template_vm_id", 0),
        storage=pl.get("storage", "ssd-pool"),
        worker_memory=pl.get("worker_memory", 16384),
        worker_cores=pl.get("worker_cores", 8),
        worker_disk_size=pl.get("worker_disk_size", 30),
        max_task_duration=pl.get("max_task_duration", 3600),
    )

    return FleetConfig(
        proxmox=proxmox,
        orchestrator=orchestrator,
        orgs=orgs,
        projects=projects,
        pool=pool,
    )


def save_config(config: FleetConfig, path: str | Path = DEFAULT_CONFIG_PATH) -> None:
    """Save fleet config to a YAML file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data: dict = {
        "proxmox": {
            "endpoint": config.proxmox.endpoint,
            "node": config.proxmox.node,
            "storage": config.proxmox.storage,
            "api_token_id": config.proxmox.api_token_id,
            "api_token_secret": config.proxmox.api_token_secret,
        },
        "orchestrator": {
            "vm_id": config.orchestrator.vm_id,
            "host": config.orchestrator.host,
            "user": config.orchestrator.user,
            "ssh_key": config.orchestrator.ssh_key,
            "memory": config.orchestrator.memory,
            "cores": config.orchestrator.cores,
            "disk_size": config.orchestrator.disk_size,
        },
        "orgs": {
            name: {
                "github_token": org.github_token,
                "claude_oauth_token": org.claude_oauth_token,
            }
            for name, org in config.orgs.items()
        },
        "projects": [
            {
                "name": p.name,
                "repo": p.repo,
            }
            for p in config.projects
        ],
        "pool": {
            "size": config.pool.size,
            "template_vm_id": config.pool.template_vm_id,
            "storage": config.pool.storage,
            "worker_memory": config.pool.worker_memory,
            "worker_cores": config.pool.worker_cores,
            "worker_disk_size": config.pool.worker_disk_size,
            "max_task_duration": config.pool.max_task_duration,
        },
    }

    # Atomic write: write to temp file then rename, with restrictive permissions
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        os.chmod(tmp_path, 0o600)
        os.rename(tmp_path, str(path))
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
