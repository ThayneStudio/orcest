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

from orcest.shared.models import require_valid_provider_name

SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


def normalize_worker_runner_for_backend(
    worker_backend: str,
    worker_runner_type: str,
    worker_runner_mode: str,
) -> tuple[str, str, str]:
    """Normalize and validate worker runner settings for a pool backend."""
    backend = worker_backend.strip() or "claude"
    require_valid_provider_name(backend)
    runner_type = worker_runner_type.strip() or (
        "claude" if backend in {"claude", "clauder"} else backend
    )
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
    elif runner_type != backend:
        raise ValueError(
            f"pool worker backend {backend!r} requires matching runner_type {backend!r}"
        )
    if backend == "claude":
        if not runner_mode:
            # Legacy fleet configs predate worker_runner_mode entirely, and the
            # pre-field deployment wrote every claude-backend clone with the
            # interactive PTY runner. Default unset to 'interactive' so those
            # configs keep their deployed runner instead of silently
            # downgrading to headless `claude -p` after a fleet update.
            runner_mode = "interactive"
        elif runner_mode not in {"interactive", "headless"}:
            raise ValueError(
                "pool.worker_backend 'claude' requires worker_runner_mode "
                "'interactive' or 'headless'"
            )
    if backend not in {"claude", "clauder"} and runner_mode:
        raise ValueError(f"pool worker backend {backend!r} does not support worker_runner_mode")
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

    def __post_init__(self) -> None:
        for provider, credentials in self.provider_credentials.items():
            require_valid_provider_name(provider)
            if not isinstance(credentials, list):
                raise ValueError(f"provider_credentials.{provider} must be a list of strings")
            if not credentials:
                raise ValueError(f"provider_credentials.{provider} must not be empty")
            for index, credential in enumerate(credentials):
                if not isinstance(credential, str) or not credential.strip():
                    raise ValueError(
                        f"provider_credentials.{provider}[{index}] must be a non-empty string"
                    )

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
class WorkerProfileConfig:
    """One worker kind in the pool's ordered round-robin layout."""

    backend: str = "claude"
    runner_type: str = ""
    runner_mode: str = ""

    def __post_init__(self) -> None:
        self.backend, self.runner_type, self.runner_mode = normalize_worker_runner_for_backend(
            self.backend,
            self.runner_type,
            self.runner_mode,
        )

    def signature(self) -> tuple[str, str, str]:
        """Return a stable representation used by deployment skew checks."""
        return self.backend, self.runner_type, self.runner_mode


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
    worker_runner_type: str = ""
    worker_runner_mode: str = ""
    # Optional heterogeneous worker layout. Each consecutive worker VMID is
    # assigned the next profile and the list wraps at the end. Repeating a
    # profile gives that backend additional worker capacity. Empty preserves the legacy single
    # worker_backend/worker_runner_* configuration exactly.
    worker_profiles: list[WorkerProfileConfig] = field(default_factory=list)
    # Force-kill threshold for an active worker VM. MUST exceed the worker's
    # RunnerConfig.timeout (default 21600s) plus a grace window, otherwise the
    # pool reaps HEALTHY long-running tasks before they can finish. Default =
    # 21600 (runner timeout) + 3600 (grace) = 25200s. Raise both together if you
    # raise the runner timeout (see project memory: pool_max_task_duration_vs_runner_timeout).
    max_task_duration: int = 25200  # seconds before force-kill (> runner timeout + grace)
    # Seconds since the last activity-watchdog sample write to
    # workers:activity:{worker_id} before PoolManager._health_check treats
    # that record as stale. Distinct from max_task_duration's hard
    # force-kill ceiling: below the ceiling, a stale-or-absent activity
    # record is a *kill-decision input*, not just observability -- but only
    # when corroborated by the worker's workers:heartbeat:{worker_id}
    # liveness heartbeat also being absent (proving the worker process
    # itself is gone). A present heartbeat with a stale/absent activity
    # record just means the worker isn't running the watchdog (disabled, or
    # an old pre-watchdog image) and is left to the ceiling, exactly like
    # the pre-watchdog reaper (see spec §6 and fleet/pool_manager.py's
    # _activity_reap_reason).
    activity_stale_after: int = 300
    # Minimum pool:active elapsed seconds before an absent-or-stale
    # activity record (plus a missing liveness heartbeat and pending
    # work) can destroy a VM. Distinct from activity_stale_after, which
    # ages the activity *record*; this ages the *task*. Below the floor,
    # a young VM may not have written either Redis key yet, so missing
    # activity + heartbeat is not proof of death. needs_reap and the
    # max_task_duration ceiling bypass this floor. Default 600s matches
    # WatchdogConfig.startup_grace so the reaper does not outrun the
    # worker's own bootstrap window.
    activity_stale_min_elapsed: int = 600
    # Fleet-level activity-watchdog rollback lever (final review, C1a).
    # Rendered into every newly-cloned worker's ``worker.yaml`` as
    # ``runner.watchdog.enabled`` (see ``cloud_init.render_clone_userdata``).
    # This is clone-time cloud-init data, not baked into the template
    # image, so toggling it takes effect for the NEXT clone with no rebake
    # required -- only ``orcest fleet update`` (to pick up the new pool
    # value) followed by normal pool churn (or a manual clone cycle) to
    # roll it out fleet-wide. Existing already-cloned workers keep whatever
    # value their own user-data was rendered with until they're replaced.
    watchdog_enabled: bool = True
    # Continuous stranded-provider-stream detector (issue #613/#639):
    # PoolManager alerts when a configured provider PR-task or issue-task
    # stream carries pending/lag work but has zero heartbeat-backed live
    # consumers, after a stream_health_dwell_seconds dwell. Each stream is
    # evaluated independently. Disabling only turns off the detector/alert;
    # it never affects reaping or task scheduling.
    stream_health_enabled: bool = True
    stream_health_dwell_seconds: int = 300
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
        normalized_profiles: list[WorkerProfileConfig] = []
        seen_backends: dict[str, tuple[str, str]] = {}
        for raw_profile in self.worker_profiles:
            if not isinstance(raw_profile, WorkerProfileConfig):
                raise ValueError("pool.worker_profiles entries must be WorkerProfileConfig values")
            profile = WorkerProfileConfig(
                backend=raw_profile.backend,
                runner_type=raw_profile.runner_type,
                runner_mode=raw_profile.runner_mode,
            )
            runner_settings = (profile.runner_type, profile.runner_mode)
            previous = seen_backends.get(profile.backend)
            if previous is not None and previous != runner_settings:
                raise ValueError(
                    "pool.worker_profiles cannot configure backend "
                    f"{profile.backend!r} with conflicting runner settings"
                )
            seen_backends[profile.backend] = runner_settings
            normalized_profiles.append(profile)
        self.worker_profiles = normalized_profiles
        if self.worker_profiles:
            primary = self.worker_profiles[0]
            self.worker_backend = primary.backend
            self.worker_runner_type = primary.runner_type
            self.worker_runner_mode = primary.runner_mode

    def effective_worker_profiles(self) -> tuple[WorkerProfileConfig, ...]:
        """Return the configured layout, including the legacy fallback profile."""
        if self.worker_profiles:
            return tuple(self.worker_profiles)
        return (
            WorkerProfileConfig(
                backend=self.worker_backend,
                runner_type=self.worker_runner_type,
                runner_mode=self.worker_runner_mode,
            ),
        )

    def scheduled_worker_profiles(self) -> tuple[WorkerProfileConfig, ...]:
        """Return the exact profile sequence occupied at the current pool size."""
        profiles = self.effective_worker_profiles()
        return tuple(profiles[index % len(profiles)] for index in range(max(0, self.size)))

    def worker_profile_for_vmid(self, vm_id: int) -> WorkerProfileConfig:
        """Assign a stable round-robin profile to a worker VMID."""
        if self.vm_id_start <= 0 or vm_id < self.vm_id_start:
            raise ValueError(
                f"worker VMID {vm_id} is below configured pool.vm_id_start {self.vm_id_start}"
            )
        profiles = self.effective_worker_profiles()
        return profiles[(vm_id - self.vm_id_start) % len(profiles)]

    def worker_backends(self) -> set[str]:
        """Return backends that have at least one slot at the target pool size."""
        return {profile.backend for profile in self.scheduled_worker_profiles()}

    def default_task_backend(self) -> str:
        """Return the backend that should receive legacy Claude-token work."""
        from orcest.shared.models import is_claude_provider

        for profile in self.scheduled_worker_profiles():
            if is_claude_provider(profile.backend):
                return profile.backend
        return "claude"

    def worker_layout_signature(self) -> str:
        """Return the deployed-layout signature used to fence unsafe transitions."""
        if not self.worker_profiles:
            return (
                f"vm_id_start={self.vm_id_start};vm_id_end={self.vm_id_end};"
                f"backend={self.worker_backend};runner={self.worker_runner_type};"
                f"mode={self.worker_runner_mode}"
            )
        profiles = ",".join(
            f"{profile.backend}:{profile.runner_type}:{profile.runner_mode}"
            for profile in self.worker_profiles
        )
        return f"vm_id_start={self.vm_id_start};vm_id_end={self.vm_id_end};profiles={profiles}"

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
        if self.size < 0:
            raise ValueError(f"pool.size must be non-negative, got {self.size}")
        if (
            self.vm_id_start > 0
            and self.vm_id_end > 0
            and self.size > 0
            and self.vm_id_start + self.size - 1 > self.vm_id_end
        ):
            raise ValueError(
                "pool worker VMID range cannot fit the target size: "
                f"{self.vm_id_start}-{self.vm_id_end} has fewer than {self.size} slots"
            )

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
class DesiredSourceConfig:
    """Declared desired Orcest source revision -- explicit operator policy.

    Either ``ref`` (a fully qualified, possibly moving ref such as
    ``refs/heads/master``) or ``sha`` (an immutable full 40-character commit
    hash) may be set, never both. Neither is inferred from the ambient
    checkout branch: an unset ``repo`` means no desired revision is declared
    at all, and fleet health reporting must say so explicitly rather than
    treating any deployed revision as current.
    """

    repo: str = ""
    ref: str = ""
    sha: str = ""

    def __post_init__(self) -> None:
        if self.ref and self.sha:
            raise ValueError("desired_source: set only one of ref or sha, not both")
        if (self.ref or self.sha) and not self.repo.strip():
            raise ValueError("desired_source.repo is required when ref or sha is set")
        if self.sha and not re.fullmatch(r"[0-9a-fA-F]{40}", self.sha.strip()):
            raise ValueError("desired_source.sha must be a full 40-character commit hash")

    @property
    def is_configured(self) -> bool:
        """Return whether a desired repo plus ref or sha has been declared."""
        return bool(self.repo.strip() and (self.ref.strip() or self.sha.strip()))


@dataclass
class FleetConfig:
    """Top-level fleet configuration."""

    proxmox: ProxmoxConfig = field(default_factory=ProxmoxConfig)
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)
    orgs: dict[str, OrgEntry] = field(default_factory=dict)
    projects: list[ProjectEntry] = field(default_factory=list)
    pool: PoolConfig = field(default_factory=PoolConfig)
    desired_source: DesiredSourceConfig = field(default_factory=DesiredSourceConfig)
    # Optional absolute path on the orchestrator VM where verbatim per-task
    # traces are archived. ``None`` disables archiving (orchestrator falls back
    # to today's Redis-only output stream). When set, ``generate_env_file``
    # emits ``ORCEST_TRACE_HOST_PATH`` and ``generate_orchestrator_config``
    # emits ``trace_archive_path`` per project.
    trace_archive_host_path: str | None = None
    # Optional absolute path on the orchestrator VM containing the v1
    # workflow.db. It is mounted read-only into each legacy orchestrator so
    # the unconditional ownership fence can be evaluated per repository.
    workflow_state_host_path: str | None = None
    # Optional shared monitor ingest wiring. The fleet config is already a
    # root-only credential store for provider/GitHub tokens; keeping the write
    # token here lets every regenerated per-project .env remain consistent
    # across `fleet update` instead of relying on fragile hand edits.
    monitor_ingest_url: str | None = None
    monitor_write_token: str = ""

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

        Each worker profile consumes its backend's PR and issue streams.
        Orchestrators publish explicit providers to provider-specific streams,
        so accepting a provider without a scheduled profile would create
        durable work that no fleet-managed worker can claim.
        """
        backends = self.pool.worker_backends()
        claude_backend = self.pool.default_task_backend()
        mismatches: dict[str, list[str]] = {}
        for project in self.projects:
            org = self.resolve_org(project)
            providers = set()
            for raw_provider in org.provider_credentials:
                provider = str(raw_provider).strip()
                if not provider:
                    continue
                providers.add(claude_backend if provider == "claude" else provider)
            has_claude_credentials = bool(
                org.claude_oauth_tokens or org.provider_credentials.get("claude")
            )
            if has_claude_credentials:
                providers.add(claude_backend)
            unsupported = sorted(provider for provider in providers if provider not in backends)
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
        if raw_tokens is not None:
            if not isinstance(raw_tokens, list):
                raise ValueError(f"orgs.{name}.claude_oauth_tokens must be a list of strings")
            tokens = []
            for index, token in enumerate(raw_tokens):
                if not isinstance(token, str) or not token.strip():
                    raise ValueError(
                        f"orgs.{name}.claude_oauth_tokens[{index}] must be a non-empty string"
                    )
                tokens.append(token)
        else:
            single = entry.get("claude_oauth_token", "")
            if single and not isinstance(single, str):
                raise ValueError(f"orgs.{name}.claude_oauth_token must be a string")
            tokens = [single] if single else []
        # provider_credentials: dict[str, list[str]]
        raw_pc = entry.get("provider_credentials", {})
        if raw_pc is None:
            raw_pc = {}
        if not isinstance(raw_pc, dict):
            raise ValueError(f"orgs.{name}.provider_credentials must be a mapping of lists")
        provider_credentials: dict[str, list[str]] = {}
        for raw_provider, raw_credentials in raw_pc.items():
            if not isinstance(raw_provider, str):
                raise ValueError(f"orgs.{name}.provider_credentials keys must be strings")
            provider = require_valid_provider_name(raw_provider)
            if not isinstance(raw_credentials, list):
                raise ValueError(
                    f"orgs.{name}.provider_credentials.{provider} must be a list of strings"
                )
            if not raw_credentials:
                raise ValueError(f"orgs.{name}.provider_credentials.{provider} must not be empty")
            credentials: list[str] = []
            for index, credential in enumerate(raw_credentials):
                if not isinstance(credential, str) or not credential.strip():
                    raise ValueError(
                        f"orgs.{name}.provider_credentials.{provider}[{index}] "
                        "must be a non-empty string"
                    )
                credentials.append(credential)
            provider_credentials[provider] = credentials
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
    raw_worker_profiles = pl.get("worker_profiles") or []
    if raw_worker_profiles and not isinstance(raw_worker_profiles, list):
        raise ValueError(
            f"pool.worker_profiles must be a list, got {type(raw_worker_profiles).__name__}"
        )
    worker_profiles: list[WorkerProfileConfig] = []
    for index, raw_profile in enumerate(raw_worker_profiles):
        if isinstance(raw_profile, str):
            if not raw_profile.strip():
                raise ValueError(f"pool.worker_profiles[{index}] backend must not be empty")
            worker_profiles.append(WorkerProfileConfig(backend=raw_profile))
            continue
        if not isinstance(raw_profile, dict):
            raise ValueError(f"pool.worker_profiles[{index}] must be a backend string or mapping")
        backend = str(raw_profile.get("backend", "") or "")
        if not backend.strip():
            raise ValueError(f"pool.worker_profiles[{index}].backend must not be empty")
        worker_profiles.append(
            WorkerProfileConfig(
                backend=backend,
                runner_type=str(raw_profile.get("runner_type", "") or ""),
                runner_mode=str(raw_profile.get("runner_mode", "") or ""),
            )
        )
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
        worker_runner_type=str(pl.get("worker_runner_type", "") or ""),
        worker_runner_mode=str(pl.get("worker_runner_mode", "") or ""),
        worker_profiles=worker_profiles,
        max_task_duration=pl.get("max_task_duration", 25200),
        activity_stale_after=pl.get("activity_stale_after", 300),
        activity_stale_min_elapsed=pl.get("activity_stale_min_elapsed", 600),
        watchdog_enabled=bool(pl.get("watchdog_enabled", True)),
        stream_health_enabled=bool(pl.get("stream_health_enabled", True)),
        stream_health_dwell_seconds=pl.get("stream_health_dwell_seconds", 300),
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

    workflow_state_host_path_raw = data.get("workflow_state_host_path")
    if isinstance(workflow_state_host_path_raw, str) and workflow_state_host_path_raw.strip():
        workflow_state_host_path: str | None = workflow_state_host_path_raw.strip()
    else:
        workflow_state_host_path = None

    monitor_ingest_url_raw = data.get("monitor_ingest_url")
    if isinstance(monitor_ingest_url_raw, str) and monitor_ingest_url_raw.strip():
        monitor_ingest_url: str | None = monitor_ingest_url_raw.strip()
    else:
        monitor_ingest_url = None
    monitor_write_token_raw = data.get("monitor_write_token", "")
    if monitor_write_token_raw is None:
        monitor_write_token = ""
    elif not isinstance(monitor_write_token_raw, str):
        raise ValueError("monitor_write_token must be a string")
    else:
        monitor_write_token = monitor_write_token_raw.strip()

    ds = data.get("desired_source") or {}
    if not isinstance(ds, dict):
        raise ValueError("desired_source must be a mapping")
    desired_source = DesiredSourceConfig(
        repo=str(ds.get("repo", "") or ""),
        ref=str(ds.get("ref", "") or ""),
        sha=str(ds.get("sha", "") or ""),
    )

    return FleetConfig(
        proxmox=proxmox,
        orchestrator=orchestrator,
        orgs=orgs,
        projects=projects,
        pool=pool,
        desired_source=desired_source,
        trace_archive_host_path=trace_archive_host_path,
        workflow_state_host_path=workflow_state_host_path,
        monitor_ingest_url=monitor_ingest_url,
        monitor_write_token=monitor_write_token,
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
            "worker_profiles": [
                {
                    "backend": profile.backend,
                    "runner_type": profile.runner_type,
                    "runner_mode": profile.runner_mode,
                }
                for profile in config.pool.worker_profiles
            ],
            "max_task_duration": config.pool.max_task_duration,
            "activity_stale_after": config.pool.activity_stale_after,
            "activity_stale_min_elapsed": config.pool.activity_stale_min_elapsed,
            "watchdog_enabled": config.pool.watchdog_enabled,
            "stream_health_enabled": config.pool.stream_health_enabled,
            "stream_health_dwell_seconds": config.pool.stream_health_dwell_seconds,
            "snippet_storage": config.pool.snippet_storage,
            "expected_image_sha256": config.pool.expected_image_sha256,
            "expected_image_gpg_key": config.pool.expected_image_gpg_key,
        },
    }

    if config.desired_source.is_configured:
        data["desired_source"] = {
            "repo": config.desired_source.repo,
            "ref": config.desired_source.ref,
            "sha": config.desired_source.sha,
        }
    if config.trace_archive_host_path:
        data["trace_archive_host_path"] = config.trace_archive_host_path
    if config.workflow_state_host_path:
        data["workflow_state_host_path"] = config.workflow_state_host_path
    if config.monitor_ingest_url:
        data["monitor_ingest_url"] = config.monitor_ingest_url
    if config.monitor_write_token:
        data["monitor_write_token"] = config.monitor_write_token

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
