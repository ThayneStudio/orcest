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


def normalize_worker_runner_for_backend(
    worker_backend: str,
    worker_runner_type: str,
    worker_runner_mode: str,
) -> tuple[str, str, str]:
    """Normalize and validate worker runner settings for a pool backend."""
    backend = worker_backend.strip() or "claude"
    runner_type = worker_runner_type.strip() or "claude"
    runner_mode = worker_runner_mode.strip()
    if backend == "clauder":
        if runner_type != "claude":
            raise ValueError("pool.worker_backend 'clauder' requires worker_runner_type 'claude'")
        if not runner_mode:
            runner_mode = "interactive"
        elif runner_mode != "interactive":
            raise ValueError(
                "pool.worker_backend 'clauder' requires worker_runner_mode 'interactive'"
            )
    return backend, runner_type, runner_mode


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
    # Verify the Proxmox API server's TLS certificate. Defaults to False for
    # self-signed lab deployments (no behavior change); set True (and use a
    # CA-trusted endpoint) to defend the root API token against MITM on the
    # management network.
    verify_ssl: bool = False

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
    claude_oauth_tokens: list[str] = field(default_factory=list)
    # Generalized multi-provider support (Task 10). Keys are provider names
    # ("grok", etc.); values are lists of credentials (first is primary).
    # These are emitted by generate_env_file into the orchestrator .env under
    # the canonical env var for that provider (XAI_API_KEY, etc.) so that
    # providers: entries with empty credential can fall back.
    provider_credentials: dict[str, list[str]] = field(default_factory=dict)

    @property
    def claude_oauth_token(self) -> str:
        """First token in the pool (backward compat for single-token callers)."""
        return self.claude_oauth_tokens[0] if self.claude_oauth_tokens else ""


@dataclass
class ProjectEntry:
    """A project managed by orcest."""

    name: str = ""
    repo: str = ""  # "org/repo" format


@dataclass
class PoolConfig:
    """Ephemeral worker VM pool settings."""

    size: int = 4  # Target warm pool size
    template_vm_id: int = 0  # Single-VMID fallback (used only if template_vmid_range is empty)
    # Inclusive [start, end] range of VMIDs reserved for worker templates.
    # The "currently active" template within this range is named by the
    # Redis pointer ``orcest:pool:current_template_vmid`` (set by ``rebake``).
    # Empty list means range mode is disabled — fall back to ``template_vm_id``.
    template_vmid_range: list[int] = field(default_factory=list)
    vm_id_start: int = 0  # First VM ID for worker clones (0 = not configured)
    vm_id_end: int = 0  # Last VM ID for worker clones (0 = no upper bound)
    storage: str = "ssd-pool"  # ZFS pool for linked clones
    worker_memory: int = 16384  # MB per worker VM
    worker_cores: int = 8
    worker_disk_size: int = 30  # GB
    worker_backend: str = "claude"
    worker_runner_type: str = "claude"
    worker_runner_mode: str = ""
    # Force-kill threshold for an active worker VM. MUST exceed the worker's
    # RunnerConfig.timeout (default 5400s) plus a grace window, otherwise the
    # pool reaps HEALTHY long-running tasks before they can finish. Default =
    # 5400 (runner timeout) + 1800 (grace) = 7200s. Raise both together if you
    # raise the runner timeout (see project memory: pool_max_task_duration_vs_runner_timeout).
    max_task_duration: int = 7200  # seconds before force-kill (> runner timeout + grace)
    snippet_storage: str = "local"  # storage for cloud-init snippets (auto-detected)
    # Image-integrity verification for the template cloud image (M5-infra).
    # By default the bake fetches the image's published ``SHA256SUMS`` +
    # ``SHA256SUMS.gpg``, GPG-verifies them against ``expected_image_gpg_key``,
    # extracts the sha256 for the pinned image filename, and passes it to the
    # Proxmox download so the node verifies the bytes. Set
    # ``expected_image_sha256`` to a 64-hex digest to PIN it instead (offline /
    # air-gapped bakes) -- the digest is then used directly with no network
    # GPG fetch. Either way verification is fail-closed: an unresolvable /
    # unverifiable digest aborts the bake rather than downloading unverified.
    expected_image_sha256: str = ""
    # GPG signing-key fingerprint the SHA256SUMS signature must validate
    # against. Defaults to Ubuntu's UEC Image Automatic Signing Key
    # (cdimage@ubuntu.com) -- the same key provision/create-vm.sh pins.
    expected_image_gpg_key: str = "D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81"

    def __post_init__(self) -> None:
        (
            self.worker_backend,
            self.worker_runner_type,
            self.worker_runner_mode,
        ) = normalize_worker_runner_for_backend(
            self.worker_backend,
            self.worker_runner_type,
            self.worker_runner_mode,
        )

    def contains_worker_vmid(self, vm_id: int) -> bool:
        """Return whether *vm_id* is inside the configured worker range.

        Destructive lifecycle operations must fail closed when the range is
        unconfigured.  ``vm_id_end == 0`` intentionally retains the legacy
        open-ended range semantics above ``vm_id_start``.
        """
        if self.vm_id_start <= 0 or vm_id < self.vm_id_start:
            return False
        return self.vm_id_end <= 0 or vm_id <= self.vm_id_end

    def template_range(self) -> tuple[int, int] | None:
        """Return ``(start, end)`` template VMID range, or ``None`` if not configured.

        The range field is the source of truth for blue/green template
        rotation; ``template_vm_id`` is only consulted as a single-VMID
        fallback when the range is empty.
        """
        r = self.template_vmid_range
        if not r:
            return None
        if len(r) != 2:
            raise ValueError(f"pool.template_vmid_range must be [start, end], got {r!r}")
        start, end = int(r[0]), int(r[1])
        if start <= 0 or end < start:
            raise ValueError(
                f"pool.template_vmid_range must be [start, end] with 0 < start <= end,"
                f" got [{start}, {end}]"
            )
        return start, end

    def validate_vmid_ranges(self) -> None:
        """Verify the template and worker VMID ranges are disjoint.

        Templates and worker clones must use distinct VMID ranges, otherwise
        the pool manager will allocate worker VMIDs that collide with
        templates (and either fail the clone or destroy a freshly-baked
        template). See bug 4 in the blue/green design notes.

        Raises ``ValueError`` if ``template_vmid_range`` is set and
        ``vm_id_start`` falls within (or below) it. Worker ranges with
        ``vm_id_end == 0`` (open-ended) are also validated against the
        template range start.
        """
        rng = self.template_range()
        if rng is None or self.vm_id_start <= 0:
            return
        tpl_start, tpl_end = rng
        worker_start = self.vm_id_start
        worker_end = self.vm_id_end if self.vm_id_end > 0 else None
        # Disjoint iff worker_end < tpl_start OR worker_start > tpl_end.
        worker_above = worker_start > tpl_end
        worker_below = worker_end is not None and worker_end < tpl_start
        if not (worker_above or worker_below):
            raise ValueError(
                f"pool.vm_id_start ({worker_start}) overlaps"
                f" pool.template_vmid_range ({tpl_start}, {tpl_end});"
                " workers must use a disjoint VMID range from templates"
                f" (e.g. set vm_id_start to {tpl_end + 1} or higher)"
            )


@dataclass
class FleetConfig:
    """Top-level fleet configuration."""

    proxmox: ProxmoxConfig = field(default_factory=ProxmoxConfig)
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)
    orgs: dict[str, OrgEntry] = field(default_factory=dict)
    projects: list[ProjectEntry] = field(default_factory=list)
    pool: PoolConfig = field(default_factory=PoolConfig)
    # Optional absolute path on the orchestrator VM where verbatim per-task
    # traces are archived. ``None`` disables archiving (orchestrator falls back
    # to today's Redis-only output stream). When set, ``generate_env_file``
    # emits ``ORCEST_TRACE_HOST_PATH`` and ``generate_orchestrator_config``
    # emits ``trace_archive_path`` per project.
    trace_archive_host_path: str | None = None

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

    def provider_stream_mismatches(self) -> dict[str, list[str]]:
        """Return project providers not consumed by the managed worker pool.

        The fleet creates one homogeneous worker pool and every clone consumes
        only ``tasks:<pool.worker_backend>`` plus its matching issue stream.
        Orchestrators publish explicit providers to provider-specific streams,
        so accepting a different provider here would create durable work that
        no fleet-managed worker can claim. Non-fleet installations may still
        run multiple specialized worker groups; this check is intentionally
        scoped to fleet deployment paths.
        """
        from orcest.shared.models import is_claude_provider

        backend = self.pool.worker_backend.strip() or "claude"
        mismatches: dict[str, list[str]] = {}
        for project in self.projects:
            org = self.resolve_org(project)
            providers = {
                str(provider).strip()
                for provider in org.provider_credentials
                if str(provider).strip() and str(provider).strip() != "claude"
            }
            has_claude_credentials = bool(
                org.claude_oauth_tokens or org.provider_credentials.get("claude")
            )
            if has_claude_credentials:
                providers.add(backend if is_claude_provider(backend) else "claude")
            unsupported = sorted(provider for provider in providers if provider != backend)
            if unsupported:
                mismatches[project.name] = unsupported
        return mismatches

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
        verify_ssl=bool(px.get("verify_ssl", False)),
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
        # Support both list (claude_oauth_tokens) and single string (claude_oauth_token)
        raw_tokens = entry.get("claude_oauth_tokens")
        if isinstance(raw_tokens, list):
            tokens = [str(t) for t in raw_tokens if t]
        else:
            single = entry.get("claude_oauth_token", "")
            tokens = [single] if single else []
        # provider_credentials: dict[str, list[str]]
        raw_pc = entry.get("provider_credentials") or {}
        if isinstance(raw_pc, dict):
            provider_credentials = {
                str(k): [str(x) for x in (v or []) if str(x).strip()]
                for k, v in raw_pc.items()
                if v
            }
        else:
            provider_credentials = {}
        orgs[name] = OrgEntry(
            github_token=entry.get("github_token", ""),
            claude_oauth_tokens=tokens,
            provider_credentials=provider_credentials,
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
    raw_range = pl.get("template_vmid_range") or []
    if raw_range and not isinstance(raw_range, list):
        raise ValueError(f"pool.template_vmid_range must be a list, got {type(raw_range).__name__}")
    template_range_list = [int(v) for v in raw_range] if raw_range else []
    pool = PoolConfig(
        size=pl.get("size", 4),
        template_vm_id=pl.get("template_vm_id", 0),
        template_vmid_range=template_range_list,
        vm_id_start=pl.get("vm_id_start", 0),
        vm_id_end=pl.get("vm_id_end", 0),
        storage=pl.get("storage", "ssd-pool"),
        worker_memory=pl.get("worker_memory", 16384),
        worker_cores=pl.get("worker_cores", 8),
        worker_disk_size=pl.get("worker_disk_size", 30),
        worker_backend=str(pl.get("worker_backend", "claude") or "claude"),
        worker_runner_type=str(pl.get("worker_runner_type", "claude") or "claude"),
        worker_runner_mode=str(pl.get("worker_runner_mode", "") or ""),
        max_task_duration=pl.get("max_task_duration", 7200),
        snippet_storage=pl.get("snippet_storage", "local"),
        expected_image_sha256=str(pl.get("expected_image_sha256", "") or ""),
        expected_image_gpg_key=str(
            pl.get("expected_image_gpg_key", "") or "D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81"
        ),
    )
    # Surface VMID-range overlap at load time rather than at clone time:
    # an overlap means the pool manager will eventually destroy a
    # freshly-baked template, which is silent until you watch the live VM
    # list churn. ``template_range()`` itself raises on a bad range tuple,
    # so call ``validate_vmid_ranges`` only when we have a usable range.
    pool.validate_vmid_ranges()

    trace_archive_host_path_raw = data.get("trace_archive_host_path")
    if isinstance(trace_archive_host_path_raw, str) and trace_archive_host_path_raw.strip():
        trace_archive_host_path: str | None = trace_archive_host_path_raw.strip()
    else:
        trace_archive_host_path = None

    return FleetConfig(
        proxmox=proxmox,
        orchestrator=orchestrator,
        orgs=orgs,
        projects=projects,
        pool=pool,
        trace_archive_host_path=trace_archive_host_path,
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
            "verify_ssl": config.proxmox.verify_ssl,
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
                "claude_oauth_tokens": org.claude_oauth_tokens,
                "provider_credentials": org.provider_credentials,
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
            "template_vmid_range": list(config.pool.template_vmid_range),
            "vm_id_start": config.pool.vm_id_start,
            "vm_id_end": config.pool.vm_id_end,
            "storage": config.pool.storage,
            "worker_memory": config.pool.worker_memory,
            "worker_cores": config.pool.worker_cores,
            "worker_disk_size": config.pool.worker_disk_size,
            "worker_backend": config.pool.worker_backend,
            "worker_runner_type": config.pool.worker_runner_type,
            "worker_runner_mode": config.pool.worker_runner_mode,
            "max_task_duration": config.pool.max_task_duration,
            "snippet_storage": config.pool.snippet_storage,
            "expected_image_sha256": config.pool.expected_image_sha256,
            "expected_image_gpg_key": config.pool.expected_image_gpg_key,
        },
    }

    if config.trace_archive_host_path:
        data["trace_archive_host_path"] = config.trace_archive_host_path

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
