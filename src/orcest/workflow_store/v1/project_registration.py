"""Authenticated Project REGISTER / REVALIDATE composition.

Resolves installation/account identity and the three forge Secret References
before the single writer transaction that inserts Project, reciprocal
registration provenance, and the revision-0 WORK_ITEM_DISCOVERY Schedule.
Repository files never create authority, principals, credentials,
installations, or controller policy.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_contract.v1.identity import is_lowercase_uuid, require_lowercase_uuid
from orcest.workflow_contract.v1.project_bundle import BundleValidationError, Diagnostic
from orcest.workflow_contract.v1.project_bundle_compile import CompiledBundle, compute_policy_hash
from orcest.workflow_contract.v1.protocol import ProtocolValidationError, validate_envelope
from orcest.workflow_contract.v1.protocol_registry import (
    ERROR_PROTOCOL,
    PROJECT_REGISTRATION_PROTOCOL,
)
from orcest.workflow_store.store import (
    CasMismatchError,
    IdempotencyConflictError,
    ProjectRegistrationOperationResult,
    RunStore,
    SecretCurrentVersionProjection,
)

AUTHORITY_PROJECT_REGISTER = "PROJECT_REGISTER"
AUTHORITY_PROJECT_REVALIDATE = "PROJECT_REVALIDATE"
AUTHORITY_INSTALLATION_USE = "INSTALLATION_USE"

MAX_REQUEST_BYTES = 16 * 1024
MAX_STRING_BYTES = 256
MAX_REF_BYTES = 256

_FORBIDDEN_REQUEST_KEYS = frozenset(
    {
        "token",
        "secret",
        "password",
        "credential",
        "credentials",
        "bearer",
        "private_key",
        "secret_ref",
        "secret_id",
        "secret_store",
        "api_token",
        "oauth",
        "ssh",
        "proxmox",
    }
)
_FORBIDDEN_BUNDLE_AUTHORITY_KEYS = frozenset(
    {
        "principal",
        "principals",
        "credential",
        "credentials",
        "secret",
        "secrets",
        "installation",
        "installations",
        "rbac",
        "controllerpolicy",
        "controller_policy",
        "authority",
        "apitoken",
        "token",
        "ssh",
        "proxmox",
        "privatekey",
        "private_key",
    }
)
_SECRET_VALUE_RE = re.compile(
    r"(?:ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|xox[baprs]-[A-Za-z0-9-]{20,}"
    r"|-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class _Rejection:
    code: str
    diagnostics: list[dict[str, str]]


@dataclass(frozen=True, slots=True)
class _ResolvedSecrets:
    forge_api: SecretCurrentVersionProjection
    source_read: SecretCurrentVersionProjection
    publication: SecretCurrentVersionProjection


class TransportError(Exception):
    """Auth/schema/CAS failure that must not create a registration Operation."""

    def __init__(self, http_status: int, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.retryable = retryable

    def body(self) -> dict[str, Any]:
        return {
            "protocol": ERROR_PROTOCOL,
            "code": self.code,
            "retryable": self.retryable,
            "message": self.message,
        }

    def body_json(self) -> str:
        return canonical_json_text(self.body())


@dataclass(frozen=True, slots=True)
class PrincipalRecord:
    principal_id: str
    authorities: frozenset[str]
    allowed_installations: frozenset[str]
    allowed_policy_refs: frozenset[str]


@dataclass(frozen=True, slots=True)
class InstallationRecord:
    installation_or_account_ref: str
    adapter_kind: str
    canonical_origin: str


@dataclass(frozen=True, slots=True)
class ExecutionProfileRecord:
    execution_profile_id: str
    worker_profile: str
    provider: str
    model: str
    provider_account_ref: str
    provider_family: str
    model_family: str
    classification_revision: int
    capacity_pool: str
    runner_shim_principal: str
    runner_image_digest: str
    runner_signature_algorithm: str
    runner_signing_key_id: str
    runner_registration_revision: int


@dataclass(frozen=True, slots=True)
class BudgetPolicyRecord:
    budget_policy_ref: str
    accounting_scope_id: str
    micro_unit: str
    limit_microunits: int
    max_budget_report_age_ms: int
    authorized_principal_id: str


@dataclass(frozen=True, slots=True)
class BudgetResetWindowRecord:
    budget_reset_window_ref: str
    window_id: str


@dataclass(frozen=True, slots=True)
class TrustedBasePolicyRecord:
    trusted_base_policy_ref: str
    allowed_default_refs: frozenset[str]


@dataclass(frozen=True, slots=True)
class ServerRegistrationCatalog:
    """Server-owned objects that repository files cannot create or grant."""

    principals: Mapping[str, PrincipalRecord]
    installations: Mapping[str, InstallationRecord]
    execution_profiles: Mapping[str, ExecutionProfileRecord]
    budget_policies: Mapping[str, BudgetPolicyRecord]
    budget_reset_windows: Mapping[str, BudgetResetWindowRecord]
    trusted_base_policies: Mapping[str, TrustedBasePolicyRecord]
    server_policy_revision: int = 1
    claim_timeout_ms: int = 300_000
    max_provider_rate_limit_wait_ms: int = 86_400_000
    mandatory_publication_checks: tuple[str, ...] = ()
    require_distinct_provider_family: bool = False
    require_distinct_model_family: bool = False


@dataclass(frozen=True, slots=True)
class ForgeResolution:
    repository_external_id: str
    repository_locator: str
    trusted_base_commit: dict[str, str]
    compiled_bundle: CompiledBundle
    ready: bool = True
    diagnostics: tuple[dict[str, str], ...] = ()


class ForgeResolver(Protocol):
    def resolve(
        self,
        *,
        adapter_kind: str,
        canonical_origin: str,
        repository_locator: str,
        default_ref: str,
    ) -> ForgeResolution: ...


@dataclass(frozen=True, slots=True)
class RegistrationHttpResult:
    http_status: int
    body_json: str
    operation: ProjectRegistrationOperationResult | None = None
    transport_error: TransportError | None = None

    @property
    def replayed(self) -> bool:
        return self.operation.replayed if self.operation is not None else False


def normalize_canonical_origin(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}:
        raise TransportError(422, "SCHEMA_INVALID", "canonical_origin must be an https origin")
    return f"https://{parsed.netloc.lower()}"


def _bounded_string(value: object, *, field: str, limit: int = MAX_STRING_BYTES) -> str:
    if not isinstance(value, str) or not value:
        raise TransportError(422, "SCHEMA_INVALID", f"{field} must be a nonempty string")
    if len(value.encode("utf-8")) > limit:
        raise TransportError(422, "SCHEMA_INVALID", f"{field} exceeds the v1 size bound")
    if _SECRET_VALUE_RE.search(value):
        raise TransportError(422, "SCHEMA_INVALID", f"{field} must not carry a secret value")
    return value


def _collect_mapping_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str):
                keys.add(key.lower())
            keys.update(_collect_mapping_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.update(_collect_mapping_keys(item))
    return keys


def _reject_forbidden_keys(payload: Mapping[str, Any]) -> None:
    keys = {key.lower() for key in payload if isinstance(key, str)}
    if keys & _FORBIDDEN_REQUEST_KEYS:
        raise TransportError(
            422, "SCHEMA_INVALID", "request must not carry credentials or secret material"
        )


def _reject_bundle_authority(bundle: CompiledBundle) -> list[dict[str, str]]:
    keys = _collect_mapping_keys(bundle.project) | _collect_mapping_keys(bundle.workflow)
    forbidden = sorted(keys & _FORBIDDEN_BUNDLE_AUTHORITY_KEYS)
    if not forbidden:
        return []
    return [
        {
            "code": "REPOSITORY_AUTHORITY_FORBIDDEN",
            "message": "repository files cannot create authority, principals, "
            "credentials, installations, or controller policy",
            "path": f"$.{name}",
        }
        for name in forbidden
    ]


def _referenced_profile_ids(workflow: Mapping[str, Any]) -> list[str]:
    spec = workflow["spec"]
    profiles: list[str] = [
        spec["implementation"]["profile"],
        *spec["implementation"]["alternateProfiles"],
        spec["verification"]["repair"]["profile"],
        *spec["verification"]["repair"]["alternateProfiles"],
        spec["review"]["adjudicator"]["profile"],
        *spec["review"]["adjudicator"]["alternates"],
    ]
    for slot in spec["review"]["slots"]:
        profiles.append(slot["profile"])
        profiles.extend(slot["alternates"])
    return profiles


def _authorization_context_digest(
    *,
    principal_id: str,
    authorities: Sequence[str],
    installation_or_account_ref: str,
    trusted_base_policy_ref: str,
    budget_policy_ref: str,
    budget_reset_window_ref: str,
) -> str:
    return request_digest(
        {
            "principal_id": principal_id,
            "authorities": sorted(authorities),
            "installation_or_account_ref": installation_or_account_ref,
            "trusted_base_policy_ref": trusted_base_policy_ref,
            "budget_policy_ref": budget_policy_ref,
            "budget_reset_window_ref": budget_reset_window_ref,
        }
    )


def parse_registration_request(raw: bytes, *, idempotency_key_header: str | None) -> dict[str, Any]:
    if len(raw) > MAX_REQUEST_BYTES:
        raise TransportError(422, "SCHEMA_INVALID", "request exceeds the v1 size bound")
    if not raw:
        raise TransportError(400, "MALFORMED", "request body is required")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TransportError(400, "MALFORMED", "request body must be UTF-8 JSON") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise TransportError(400, "MALFORMED", "request body must be UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise TransportError(400, "MALFORMED", "request body must be a JSON object")
    _reject_forbidden_keys(payload)
    try:
        validate_envelope(payload)
    except ProtocolValidationError as exc:
        raise TransportError(422, "SCHEMA_INVALID", str(exc)) from exc
    if payload.get("protocol") != PROJECT_REGISTRATION_PROTOCOL:
        raise TransportError(422, "SCHEMA_INVALID", "unsupported registration protocol")
    header = idempotency_key_header.strip().lower() if idempotency_key_header else ""
    if not header or not is_lowercase_uuid(header):
        raise TransportError(400, "MALFORMED", "Idempotency-Key must be a lowercase UUID")
    if header != payload["idempotency_key"]:
        raise TransportError(
            400, "MALFORMED", "Idempotency-Key header must equal the request idempotency_key"
        )
    return payload


def _require_principal(
    catalog: ServerRegistrationCatalog, principal_id: str | None
) -> PrincipalRecord:
    if not principal_id:
        raise TransportError(401, "AUTH_INVALID", "transport authentication is required")
    principal = catalog.principals.get(principal_id)
    if principal is None:
        raise TransportError(401, "AUTH_INVALID", "unknown transport principal")
    return principal


def _resolve_installation_secrets(
    run_store: RunStore, installation_or_account_ref: str
) -> _ResolvedSecrets | _Rejection:
    secrets = run_store.list_current_secrets_for_owner(
        owner_scope_kind="FORGE_INSTALLATION", owner_scope_id=installation_or_account_ref
    )
    by_purpose: dict[str, list[SecretCurrentVersionProjection]] = {
        "FORGE_API": [],
        "SOURCE_READ": [],
        "PUBLICATION": [],
    }
    for secret in secrets:
        if secret.owner_scope_kind != "FORGE_INSTALLATION":
            continue
        if secret.provider_account_ref != installation_or_account_ref:
            continue
        if secret.purpose in by_purpose:
            by_purpose[secret.purpose].append(secret)
    diagnostics: list[dict[str, str]] = []
    for purpose, rows in by_purpose.items():
        if len(rows) != 1:
            diagnostics.append(
                {
                    "code": "SECRET_RESOLUTION_FAILED",
                    "message": f"installation must have exactly one current {purpose} Secret",
                    "path": f"installation.{purpose}",
                }
            )
    if diagnostics:
        return _Rejection("POLICY_VALIDATION_FAILED", diagnostics)
    forge_api, source_read, publication = (
        by_purpose["FORGE_API"][0],
        by_purpose["SOURCE_READ"][0],
        by_purpose["PUBLICATION"][0],
    )
    if source_read.secret_id == publication.secret_id:
        return _Rejection(
            "POLICY_VALIDATION_FAILED",
            [
                {
                    "code": "SECRET_RESOLUTION_FAILED",
                    "message": "source-read and publication Secret IDs must differ",
                    "path": "installation.SOURCE_READ",
                }
            ],
        )
    for secret in (forge_api, source_read, publication):
        version = run_store.get_secret_version(secret.secret_id, secret.current_version)
        if version is None:
            return _Rejection(
                "POLICY_VALIDATION_FAILED",
                [
                    {
                        "code": "SECRET_RESOLUTION_FAILED",
                        "message": "resolved Secret is missing a creation Receipt",
                        "path": f"installation.{secret.purpose}",
                    }
                ],
            )
        receipt = run_store.get_credential_rotation_receipt(version.creation_receipt_id)
        if (
            receipt is None
            or receipt.purpose != secret.purpose
            or receipt.owner_scope_kind != "FORGE_INSTALLATION"
            or receipt.owner_scope_id != installation_or_account_ref
            or receipt.new_version != secret.current_version
        ):
            return _Rejection(
                "POLICY_VALIDATION_FAILED",
                [
                    {
                        "code": "SECRET_RESOLUTION_FAILED",
                        "message": "resolved Secret is missing a verified creation Receipt",
                        "path": f"installation.{secret.purpose}",
                    }
                ],
            )
    return _ResolvedSecrets(forge_api, source_read, publication)


def _materialize_server_policy(
    catalog: ServerRegistrationCatalog,
    bundle: CompiledBundle,
    *,
    trusted_base_policy_ref: str,
    budget_policy_ref: str,
    budget_reset_window_ref: str,
) -> tuple[dict[str, Any], str] | _Rejection:
    authority_diags = _reject_bundle_authority(bundle)
    if authority_diags:
        return _Rejection("POLICY_VALIDATION_FAILED", authority_diags)
    profile_ids = _referenced_profile_ids(bundle.workflow)
    unresolved = sorted(
        {profile for profile in profile_ids if profile not in catalog.execution_profiles}
    )
    if unresolved:
        return _Rejection(
            "CAPABILITY_UNSUPPORTED",
            [
                {
                    "code": "CAPABILITY_UNSUPPORTED",
                    "message": f"execution profile {profile!r} is not registered for this project",
                    "path": "spec.implementation.profile",
                }
                for profile in unresolved
            ],
        )
    budget = catalog.budget_policies[budget_policy_ref]
    reset = catalog.budget_reset_windows[budget_reset_window_ref]
    profiles = {
        profile_id: {
            "worker_profile": catalog.execution_profiles[profile_id].worker_profile,
            "provider": catalog.execution_profiles[profile_id].provider,
            "model": catalog.execution_profiles[profile_id].model,
            "provider_account_ref": catalog.execution_profiles[profile_id].provider_account_ref,
            "provider_family": catalog.execution_profiles[profile_id].provider_family,
            "model_family": catalog.execution_profiles[profile_id].model_family,
            "classification_revision": catalog.execution_profiles[
                profile_id
            ].classification_revision,
            "capacity_pool": catalog.execution_profiles[profile_id].capacity_pool,
            "runner_shim_principal": catalog.execution_profiles[profile_id].runner_shim_principal,
            "runner_image_digest": catalog.execution_profiles[profile_id].runner_image_digest,
            "runner_signature_algorithm": catalog.execution_profiles[
                profile_id
            ].runner_signature_algorithm,
            "runner_signing_key_id": catalog.execution_profiles[profile_id].runner_signing_key_id,
            "runner_registration_revision": catalog.execution_profiles[
                profile_id
            ].runner_registration_revision,
        }
        for profile_id in sorted(set(profile_ids))
    }
    review = bundle.workflow["spec"]["review"]
    server_policy = {
        "server_policy_revision": catalog.server_policy_revision,
        "trusted_base_policy_ref": trusted_base_policy_ref,
        "budget_policy_ref": budget_policy_ref,
        "budget_reset_window_ref": budget_reset_window_ref,
        "execution_profiles": profiles,
        "claim_timeout_ms": catalog.claim_timeout_ms,
        "max_provider_rate_limit_wait_ms": catalog.max_provider_rate_limit_wait_ms,
        "require_distinct_provider_family": bool(
            review["requireDistinctProviderFamily"] or catalog.require_distinct_provider_family
        ),
        "require_distinct_model_family": bool(
            review["requireDistinctModelFamily"] or catalog.require_distinct_model_family
        ),
        "mandatory_publication_checks": list(catalog.mandatory_publication_checks),
        "budget": {
            "accounting_scope_id": budget.accounting_scope_id,
            "micro_unit": budget.micro_unit,
            "limit_microunits": budget.limit_microunits,
            "reset_window_id": reset.window_id,
            "max_budget_report_age_ms": budget.max_budget_report_age_ms,
            "authorized_principal_id": budget.authorized_principal_id,
        },
    }
    return server_policy, compute_policy_hash(
        workflow_hash=bundle.workflow_hash, server_policy=server_policy
    )


def register_or_revalidate_project(
    run_store: RunStore,
    *,
    catalog: ServerRegistrationCatalog,
    resolver: ForgeResolver,
    raw_body: bytes,
    idempotency_key_header: str | None,
    authenticated_principal_id: str | None,
) -> RegistrationHttpResult:
    """Run one authenticated registration request to a terminal public response."""
    gates = run_store.controller_gate_permissions()
    if not gates.management_operations:
        raise TransportError(
            503, "CONTROLLER_UNAVAILABLE", "controller mode is not initialized", retryable=True
        )

    request = parse_registration_request(raw_body, idempotency_key_header=idempotency_key_header)
    principal = _require_principal(catalog, authenticated_principal_id)
    forge = request["forge"]
    installation_ref = _bounded_string(
        forge["installation_or_account_ref"], field="installation_or_account_ref"
    )
    origin = normalize_canonical_origin(
        _bounded_string(forge["canonical_origin"], field="canonical_origin")
    )
    locator = _bounded_string(forge["repository_locator"], field="repository_locator")
    default_ref = _bounded_string(
        request["requested_default_ref"], field="requested_default_ref", limit=MAX_REF_BYTES
    )
    trusted_base_policy_ref = _bounded_string(
        request["trusted_base_policy_ref"], field="trusted_base_policy_ref"
    )
    budget_policy_ref = _bounded_string(request["budget_policy_ref"], field="budget_policy_ref")
    budget_reset_window_ref = _bounded_string(
        request["budget_reset_window_ref"], field="budget_reset_window_ref"
    )
    adapter_kind = forge["adapter_kind"]
    if adapter_kind != "GITHUB":
        raise TransportError(422, "SCHEMA_INVALID", "adapter_kind has the sole v1 value GITHUB")

    mode = "REGISTER" if request["project_id"] is None else "REVALIDATE"
    needed = AUTHORITY_PROJECT_REGISTER if mode == "REGISTER" else AUTHORITY_PROJECT_REVALIDATE
    if needed not in principal.authorities:
        raise TransportError(403, "CAPABILITY_DENIED", f"principal lacks {needed} authority")
    if AUTHORITY_INSTALLATION_USE not in principal.authorities:
        raise TransportError(403, "CAPABILITY_DENIED", "principal lacks INSTALLATION_USE authority")
    if installation_ref not in principal.allowed_installations:
        raise TransportError(403, "CAPABILITY_DENIED", "principal cannot use this installation")
    if installation_ref not in catalog.installations:
        raise TransportError(403, "CAPABILITY_DENIED", "installation is not registered")
    installation = catalog.installations[installation_ref]
    if installation.canonical_origin != origin or installation.adapter_kind != adapter_kind:
        raise TransportError(403, "CAPABILITY_DENIED", "installation origin does not match request")
    for policy_ref in (trusted_base_policy_ref, budget_policy_ref, budget_reset_window_ref):
        if policy_ref not in principal.allowed_policy_refs:
            raise TransportError(403, "CAPABILITY_DENIED", "principal cannot use this policy ref")
    if trusted_base_policy_ref not in catalog.trusted_base_policies:
        raise TransportError(422, "SCHEMA_INVALID", "trusted_base_policy_ref is not registered")
    if budget_policy_ref not in catalog.budget_policies:
        raise TransportError(422, "SCHEMA_INVALID", "budget_policy_ref is not registered")
    if budget_reset_window_ref not in catalog.budget_reset_windows:
        raise TransportError(422, "SCHEMA_INVALID", "budget_reset_window_ref is not registered")
    allowed_refs = catalog.trusted_base_policies[trusted_base_policy_ref].allowed_default_refs
    if default_ref not in allowed_refs:
        raise TransportError(
            422, "SCHEMA_INVALID", "requested_default_ref is not allowed by policy"
        )
    if mode == "REVALIDATE":
        require_lowercase_uuid(request["project_id"], field="project_id")
        existing = run_store.get_project(request["project_id"])
        if existing is None:
            raise TransportError(409, "CAS_LOST", "project does not exist for revalidation")
        if AUTHORITY_PROJECT_REVALIDATE not in principal.authorities:
            raise TransportError(403, "CAPABILITY_DENIED", "principal lacks PROJECT_REVALIDATE")

    authz_digest = _authorization_context_digest(
        principal_id=principal.principal_id,
        authorities=sorted(principal.authorities),
        installation_or_account_ref=installation_ref,
        trusted_base_policy_ref=trusted_base_policy_ref,
        budget_policy_ref=budget_policy_ref,
        budget_reset_window_ref=budget_reset_window_ref,
    )

    existing_op = run_store.get_project_registration_operation(
        authenticated_principal_id=principal.principal_id,
        idempotency_key=request["idempotency_key"],
    )
    if existing_op is not None:
        if existing_op.request_digest == request_digest(request):
            replayed = _replay(existing_op)
            return RegistrationHttpResult(
                http_status=replayed.response_http_status,
                body_json=replayed.public_response_json(),
                operation=replayed,
            )
        raise TransportError(
            409, "IDEMPOTENCY_CONFLICT", "idempotency key was reused with a different body"
        )

    secret_resolution = _resolve_installation_secrets(run_store, installation_ref)
    if isinstance(secret_resolution, _Rejection):
        return _commit_rejection(
            run_store,
            request=request,
            principal_id=principal.principal_id,
            authz_digest=authz_digest,
            adapter_kind=adapter_kind,
            canonical_origin=origin,
            installation_ref=installation_ref,
            default_ref=default_ref,
            trusted_base_policy_ref=trusted_base_policy_ref,
            budget_policy_ref=budget_policy_ref,
            budget_reset_window_ref=budget_reset_window_ref,
            code=secret_resolution.code,
            diagnostics=secret_resolution.diagnostics,
        )
    forge_api = secret_resolution.forge_api
    source_read = secret_resolution.source_read
    publication = secret_resolution.publication

    try:
        resolution = resolver.resolve(
            adapter_kind=adapter_kind,
            canonical_origin=origin,
            repository_locator=locator,
            default_ref=default_ref,
        )
    except BundleValidationError as exc:
        return _commit_rejection(
            run_store,
            request=request,
            principal_id=principal.principal_id,
            authz_digest=authz_digest,
            adapter_kind=adapter_kind,
            canonical_origin=origin,
            installation_ref=installation_ref,
            default_ref=default_ref,
            trusted_base_policy_ref=trusted_base_policy_ref,
            budget_policy_ref=budget_policy_ref,
            budget_reset_window_ref=budget_reset_window_ref,
            code="WORKFLOW_INVALID",
            diagnostics=[_diagnostic_to_json(item) for item in exc.diagnostics],
        )

    policy = _materialize_server_policy(
        catalog,
        resolution.compiled_bundle,
        trusted_base_policy_ref=trusted_base_policy_ref,
        budget_policy_ref=budget_policy_ref,
        budget_reset_window_ref=budget_reset_window_ref,
    )
    if isinstance(policy, _Rejection):
        return _commit_rejection(
            run_store,
            request=request,
            principal_id=principal.principal_id,
            authz_digest=authz_digest,
            adapter_kind=adapter_kind,
            canonical_origin=origin,
            installation_ref=installation_ref,
            default_ref=default_ref,
            trusted_base_policy_ref=trusted_base_policy_ref,
            budget_policy_ref=budget_policy_ref,
            budget_reset_window_ref=budget_reset_window_ref,
            code=policy.code,
            diagnostics=policy.diagnostics,
            resolved_repository_external_id=resolution.repository_external_id,
            resolved_repository_locator=resolution.repository_locator,
            resolved_base_commit=resolution.trusted_base_commit,
        )
    _server_policy, policy_hash = policy
    del _server_policy

    try:
        operation = run_store.commit_project_registration(
            authenticated_principal_id=principal.principal_id,
            idempotency_key=request["idempotency_key"],
            request=request,
            authorization_context_digest=authz_digest,
            adapter_kind=adapter_kind,
            canonical_origin=origin,
            installation_or_account_ref=installation_ref,
            default_ref=default_ref,
            trusted_base_policy_ref=trusted_base_policy_ref,
            budget_policy_ref=budget_policy_ref,
            budget_reset_window_ref=budget_reset_window_ref,
            resolved_repository_external_id=resolution.repository_external_id,
            resolved_repository_locator=resolution.repository_locator,
            resolved_base_commit=resolution.trusted_base_commit,
            resolved_forge_api_secret_id=forge_api.secret_id,
            resolved_forge_api_secret_version=forge_api.current_version,
            resolved_source_read_secret_id=source_read.secret_id,
            resolved_source_read_secret_version=source_read.current_version,
            resolved_publication_secret_id=publication.secret_id,
            resolved_publication_secret_version=publication.current_version,
            workflow_hash=resolution.compiled_bundle.workflow_hash,
            policy_hash=policy_hash,
            readiness={
                "ready": resolution.ready,
                "diagnostics": list(resolution.diagnostics),
            },
        )
    except IdempotencyConflictError as exc:
        raise TransportError(409, "IDEMPOTENCY_CONFLICT", str(exc)) from exc
    except CasMismatchError as exc:
        raise TransportError(409, "CAS_LOST", str(exc)) from exc
    return RegistrationHttpResult(
        http_status=operation.response_http_status,
        body_json=operation.public_response_json(),
        operation=operation,
    )


def _replay(existing: ProjectRegistrationOperationResult) -> ProjectRegistrationOperationResult:
    return ProjectRegistrationOperationResult(
        project_registration_operation_id=existing.project_registration_operation_id,
        authenticated_principal_id=existing.authenticated_principal_id,
        idempotency_key=existing.idempotency_key,
        mode=existing.mode,
        status=existing.status,
        response_http_status=existing.response_http_status,
        response_json=existing.response_json,
        response_digest=existing.response_digest,
        resolution_digest=existing.resolution_digest,
        completed_at_ms=existing.completed_at_ms,
        request_digest=existing.request_digest,
        authorization_context_digest=existing.authorization_context_digest,
        installation_or_account_ref=existing.installation_or_account_ref,
        requested_project_id=existing.requested_project_id,
        expected_registration_revision=existing.expected_registration_revision,
        rejection_code=existing.rejection_code,
        result_project_id=existing.result_project_id,
        result_registration_revision=existing.result_registration_revision,
        result_work_item_discovery_schedule_id=existing.result_work_item_discovery_schedule_id,
        resolved_forge_instance_id=existing.resolved_forge_instance_id,
        resolved_repository_external_id=existing.resolved_repository_external_id,
        resolved_base_commit_json=existing.resolved_base_commit_json,
        resolved_forge_api_secret_id=existing.resolved_forge_api_secret_id,
        resolved_forge_api_secret_version=existing.resolved_forge_api_secret_version,
        resolved_source_read_secret_id=existing.resolved_source_read_secret_id,
        resolved_source_read_secret_version=existing.resolved_source_read_secret_version,
        resolved_publication_secret_id=existing.resolved_publication_secret_id,
        resolved_publication_secret_version=existing.resolved_publication_secret_version,
        replayed=True,
    )


def _diagnostic_to_json(item: Diagnostic) -> dict[str, str]:
    return {"code": item.code, "message": item.message, "path": item.path}


def _commit_rejection(
    run_store: RunStore,
    *,
    request: Mapping[str, Any],
    principal_id: str,
    authz_digest: str,
    adapter_kind: str,
    canonical_origin: str,
    installation_ref: str,
    default_ref: str,
    trusted_base_policy_ref: str,
    budget_policy_ref: str,
    budget_reset_window_ref: str,
    code: str,
    diagnostics: list[dict[str, str]],
    resolved_repository_external_id: str | None = None,
    resolved_repository_locator: str | None = None,
    resolved_base_commit: Mapping[str, str] | None = None,
) -> RegistrationHttpResult:
    try:
        operation = run_store.commit_project_registration(
            authenticated_principal_id=principal_id,
            idempotency_key=request["idempotency_key"],
            request=request,
            authorization_context_digest=authz_digest,
            adapter_kind=adapter_kind,
            canonical_origin=canonical_origin,
            installation_or_account_ref=installation_ref,
            default_ref=default_ref,
            trusted_base_policy_ref=trusted_base_policy_ref,
            budget_policy_ref=budget_policy_ref,
            budget_reset_window_ref=budget_reset_window_ref,
            resolved_repository_external_id=resolved_repository_external_id,
            resolved_repository_locator=resolved_repository_locator,
            resolved_base_commit=resolved_base_commit,
            resolved_forge_api_secret_id=None,
            resolved_forge_api_secret_version=None,
            resolved_source_read_secret_id=None,
            resolved_source_read_secret_version=None,
            resolved_publication_secret_id=None,
            resolved_publication_secret_version=None,
            business_rejection_code=code,
            diagnostics=diagnostics,
        )
    except IdempotencyConflictError as exc:
        raise TransportError(409, "IDEMPOTENCY_CONFLICT", str(exc)) from exc
    except CasMismatchError as exc:
        raise TransportError(409, "CAS_LOST", str(exc)) from exc
    return RegistrationHttpResult(
        http_status=operation.response_http_status,
        body_json=operation.public_response_json(),
        operation=operation,
    )
