"""Authenticated Project REGISTER/REVALIDATE storage and HTTPS onboarding."""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from orcest.cli import main
from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.digest import capability_public_key_digest, request_digest
from orcest.workflow_contract.v1.identity import CommitId
from orcest.workflow_contract.v1.project_bundle_compile import compile_bundle
from orcest.workflow_contract.v1.project_bundle_source import GitBundleSource, resolve_commit
from orcest.workflow_contract.v1.protocol import validate_envelope
from orcest.workflow_contract.v1.protocol_registry import (
    ERROR_PROTOCOL,
    PROJECT_REGISTRATION_PROTOCOL,
)
from orcest.workflow_store import (
    FaultInjectionPoint,
    RunStore,
    TransactionFault,
)
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.project_registration import (
    BudgetPolicyRecord,
    BudgetResetWindowRecord,
    ExecutionProfileRecord,
    ForgeResolution,
    InstallationRecord,
    PrincipalRecord,
    ServerRegistrationCatalog,
    TransportError,
    TrustedBasePolicyRecord,
    register_or_revalidate_project,
)
from orcest.workflow_store.v1.registration_http import handle_registration_http
from orcest.workflow_store.v1.secret_provision import provision_or_adopt_secret
from orcest.workflow_store.v1.secrets import SecretStore

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64
INSTALLATION = "installation-1"
PRINCIPAL = "onboard-operator"
DIGEST = "sha256:" + "b" * 64
OID = "a" * 40


def _git(repo: Path, *args: str) -> None:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, env=env, timeout=30
    )
    assert result.returncode == 0, result.stderr.decode()


def _bundle_repo(tmp_path: Path) -> tuple[CompiledLike, str]:
    from orcest.cli_project import (
        _PROMPT_STUBS,
        _project_yaml_template,
        _workflow_yaml_template,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    files = {
        ".orcest/project.yaml": _project_yaml_template(),
        ".orcest/workflows/implementation.yaml": _workflow_yaml_template("codex-default"),
        **_PROMPT_STUBS,
    }
    for relpath, content in files.items():
        path = repo / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    _git(repo, "add", ".orcest")
    _git(repo, "commit", "-q", "-m", "bundle")
    commit = resolve_commit(str(repo), "HEAD")
    compiled = compile_bundle(GitBundleSource(str(repo), commit))
    return compiled, commit.oid


CompiledLike = object


@pytest.fixture
def run_store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


@pytest.fixture
def secret_store(tmp_path: Path) -> SecretStore:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    quota = QuotaConfig(
        min_free_bytes=0,
        max_object_bytes=1024 * 1024,
        max_store_bytes=8 * 1024 * 1024,
        max_objects=1024,
    )
    return SecretStore(layout, quota=quota, lock=StorageLock(layout.storage_lock_path))


def _initialize(store: RunStore) -> None:
    result = store.apply_controller_mode_operation(
        controller_mode_operation_id=str(uuid.uuid4()),
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="bootstrap-service",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"
    key_id = str(uuid.uuid4())
    public_key = bytes([7]) * 32
    result = store.apply_capability_key_operation(
        capability_key_operation_id=str(uuid.uuid4()),
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        register_public_verification_key=public_key,
        register_public_key_digest=capability_public_key_digest(public_key),
        register_private_signing_secret_ref="bootstrap:0",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"
    result = store.apply_capability_key_operation(
        capability_key_operation_id=str(uuid.uuid4()),
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _provision_three(run_store: RunStore, secret_store: SecretStore) -> tuple[str, str, str]:
    ids = (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()))
    for secret_id, purpose, blob in zip(
        ids, ("FORGE_API", "SOURCE_READ", "PUBLICATION"), (b"api", b"read", b"pub"), strict=True
    ):
        result = provision_or_adopt_secret(
            run_store,
            secret_store,
            secret_provision_operation_id=str(uuid.uuid4()),
            mode="PROVISION",
            secret_id=secret_id,
            expected_prior_version=None,
            purpose=purpose,
            owner_scope_kind="FORGE_INSTALLATION",
            owner_scope_id=INSTALLATION,
            authenticated_principal_id="operator-1",
            authorization_context_digest=AUTHZ_DIGEST,
            secret_bytes=blob,
            provider_account_ref=INSTALLATION,
        )
        assert result.state == "COMPLETED"
    return ids


def _profile(profile_id: str) -> ExecutionProfileRecord:
    return ExecutionProfileRecord(
        execution_profile_id=profile_id,
        worker_profile="codex",
        provider="openai",
        model="gpt",
        provider_account_ref="acct-1",
        provider_family="openai",
        model_family="gpt",
        classification_revision=1,
        capacity_pool="default",
        runner_shim_principal="runner-1",
        runner_image_digest=DIGEST,
        runner_signature_algorithm="ED25519",
        runner_signing_key_id=str(uuid.uuid4()),
        runner_registration_revision=1,
    )


def _catalog() -> ServerRegistrationCatalog:
    policies = frozenset({"trusted-base/v1", "budget/v1", "budget-reset/v1"})
    return ServerRegistrationCatalog(
        principals={
            PRINCIPAL: PrincipalRecord(
                principal_id=PRINCIPAL,
                authorities=frozenset(
                    {"PROJECT_REGISTER", "PROJECT_REVALIDATE", "INSTALLATION_USE"}
                ),
                allowed_installations=frozenset({INSTALLATION}),
                allowed_policy_refs=policies,
            )
        },
        installations={
            INSTALLATION: InstallationRecord(
                installation_or_account_ref=INSTALLATION,
                adapter_kind="GITHUB",
                canonical_origin="https://github.com",
            )
        },
        execution_profiles={
            "codex-default": _profile("codex-default"),
            "claude-review": _profile("claude-review"),
            "codex-review": _profile("codex-review"),
        },
        budget_policies={
            "budget/v1": BudgetPolicyRecord(
                budget_policy_ref="budget/v1",
                accounting_scope_id="scope-1",
                micro_unit="usd_micros",
                limit_microunits=1_000_000,
                max_budget_report_age_ms=3_600_000,
                authorized_principal_id="budget-accountant",
            )
        },
        budget_reset_windows={
            "budget-reset/v1": BudgetResetWindowRecord(
                budget_reset_window_ref="budget-reset/v1", window_id="weekly"
            )
        },
        trusted_base_policies={
            "trusted-base/v1": TrustedBasePolicyRecord(
                trusted_base_policy_ref="trusted-base/v1",
                allowed_default_refs=frozenset({"refs/heads/main"}),
            )
        },
    )


def _real_catalog() -> ServerRegistrationCatalog:
    return _catalog()


class _Resolver:
    def __init__(self, compiled, oid: str, locator: str = "owner/repository") -> None:
        self.compiled = compiled
        self.oid = oid
        self.locator = locator
        self.external_id = "repo-external-1"

    def resolve(self, **_kwargs: object) -> ForgeResolution:
        return ForgeResolution(
            repository_external_id=self.external_id,
            repository_locator=self.locator,
            trusted_base_commit=CommitId("sha1", self.oid).to_json(),
            compiled_bundle=self.compiled,
        )


def _request(
    *, key: str, locator: str = "owner/repository", project_id=None, revision=None
) -> dict:
    return {
        "protocol": PROJECT_REGISTRATION_PROTOCOL,
        "idempotency_key": key,
        "project_id": project_id,
        "expected_registration_revision": revision,
        "forge": {
            "adapter_kind": "GITHUB",
            "canonical_origin": "https://github.com",
            "installation_or_account_ref": INSTALLATION,
            "repository_locator": locator,
        },
        "requested_default_ref": "refs/heads/main",
        "trusted_base_policy_ref": "trusted-base/v1",
        "budget_policy_ref": "budget/v1",
        "budget_reset_window_ref": "budget-reset/v1",
    }


def _post(run_store, catalog, resolver, request: dict, *, principal: str = PRINCIPAL):
    raw = json.dumps(request).encode("utf-8")
    return register_or_revalidate_project(
        run_store,
        catalog=catalog,
        resolver=resolver,
        raw_body=raw,
        idempotency_key_header=request["idempotency_key"],
        authenticated_principal_id=principal,
    )


def test_register_creates_project_schedule_and_pins_secrets(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    api_id, read_id, pub_id = _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    catalog = _real_catalog()
    resolver = _Resolver(compiled, oid)
    key = str(uuid.uuid4())
    result = _post(run_store, catalog, resolver, _request(key=key))
    assert result.http_status == 200
    body = json.loads(result.body_json)
    validate_envelope(body)
    assert body["replayed"] is False
    assert body["mode"] == "REGISTER"
    assert body["status"] == "SUCCEEDED"
    assert body["registration_revision"] == 1
    assert body["registration_state"] == "ACTIVE"
    assert "secret" not in json.dumps(body).lower() or "secret_id" not in body
    assert "resolution_digest" not in body
    op = result.operation
    assert op is not None
    assert op.result_work_item_discovery_schedule_id is not None
    assert op.resolved_forge_api_secret_id == api_id
    assert op.resolved_source_read_secret_id == read_id
    assert op.resolved_publication_secret_id == pub_id
    project = run_store.get_project(body["project_id"])
    assert project is not None
    assert project.registration_revision == 1
    assert project.registration_operation_id == op.project_registration_operation_id
    assert project.work_item_discovery_schedule_id == op.result_work_item_discovery_schedule_id
    assert project.source_read_secret_id == read_id
    assert project.publication_secret_id == pub_id
    schedule = run_store.get_forge_observation_schedule(project.work_item_discovery_schedule_id)
    assert schedule is not None
    assert schedule.schedule_kind == "WORK_ITEM_DISCOVERY"
    assert schedule.state == "ACTIVE"
    assert schedule.schedule_revision == 0
    assert schedule.target_kind == "PROJECT"
    assert schedule.target_id == project.project_id
    assert schedule.next_due_at_ms == op.completed_at_ms
    forge = run_store.get_forge_instance(project.forge_instance_id)
    assert forge is not None
    assert forge.credential_secret_id == api_id
    assert op.resolution_digest.startswith("sha256:")
    # Internal resolution must not leak into the public digest preimage.
    public_digest_body = json.loads(op.response_json)
    assert "resolved_forge_api_secret_id" not in public_digest_body
    assert public_digest_body["replayed"] is False


def test_lost_response_replays_byte_identically(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    catalog = _real_catalog()
    resolver = _Resolver(compiled, oid)
    key = str(uuid.uuid4())
    first = _post(run_store, catalog, resolver, _request(key=key))
    second = _post(run_store, catalog, resolver, _request(key=key))
    assert second.http_status == 200
    assert json.loads(second.body_json)["replayed"] is True
    first_obj = json.loads(first.body_json)
    second_obj = json.loads(second.body_json)
    first_obj.pop("replayed")
    second_obj.pop("replayed")
    assert canonical_json_text(first_obj) == canonical_json_text(second_obj)
    third = _post(run_store, catalog, resolver, _request(key=key))
    assert third.body_json == second.body_json
    assert run_store.conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0] == 1
    assert (
        run_store.conn.execute("SELECT COUNT(*) FROM project_registration_operations").fetchone()[0]
        == 1
    )


def test_same_key_different_body_conflicts_without_mutation(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    catalog = _real_catalog()
    resolver = _Resolver(compiled, oid)
    key = str(uuid.uuid4())
    first = _post(run_store, catalog, resolver, _request(key=key))
    project_id = json.loads(first.body_json)["project_id"]
    with pytest.raises(TransportError) as exc:
        _post(run_store, catalog, resolver, _request(key=key, locator="other/repo"))
    assert exc.value.http_status == 409
    assert exc.value.code == "IDEMPOTENCY_CONFLICT"
    assert json.loads(exc.value.body_json())["protocol"] == ERROR_PROTOCOL
    still = run_store.get_project(project_id)
    assert still is not None
    assert still.repository_locator == "owner/repository"
    assert (
        run_store.conn.execute("SELECT COUNT(*) FROM project_registration_operations").fetchone()[0]
        == 1
    )


def test_stale_revision_revalidate_is_cas_without_operation(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    catalog = _real_catalog()
    resolver = _Resolver(compiled, oid)
    first = _post(run_store, catalog, resolver, _request(key=str(uuid.uuid4())))
    project_id = json.loads(first.body_json)["project_id"]
    with pytest.raises(TransportError) as exc:
        _post(
            run_store,
            catalog,
            resolver,
            _request(key=str(uuid.uuid4()), project_id=project_id, revision=99),
        )
    assert exc.value.http_status == 409
    assert exc.value.code == "CAS_LOST"
    project = run_store.get_project(project_id)
    assert project is not None
    assert project.registration_revision == 1
    assert (
        run_store.conn.execute("SELECT COUNT(*) FROM project_registration_operations").fetchone()[0]
        == 1
    )


def test_revalidate_refreshes_locator_and_retains_schedule(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    catalog = _real_catalog()
    resolver = _Resolver(compiled, oid, locator="owner/renamed")
    first = _post(
        run_store,
        catalog,
        _Resolver(compiled, oid, locator="owner/repository"),
        _request(key=str(uuid.uuid4())),
    )
    body = json.loads(first.body_json)
    second = _post(
        run_store,
        catalog,
        resolver,
        _request(key=str(uuid.uuid4()), project_id=body["project_id"], revision=1),
    )
    replay = json.loads(second.body_json)
    assert replay["status"] == "SUCCEEDED"
    assert replay["mode"] == "REVALIDATE"
    assert replay["registration_revision"] == 2
    assert replay["repository_locator"] == "owner/renamed"
    project = run_store.get_project(body["project_id"])
    assert project is not None
    assert first.operation is not None
    assert second.operation is not None
    schedule_id = first.operation.result_work_item_discovery_schedule_id
    assert project.work_item_discovery_schedule_id == schedule_id
    assert second.operation.result_work_item_discovery_schedule_id == schedule_id
    count = run_store.conn.execute(
        "SELECT COUNT(*) FROM forge_observation_schedules WHERE schedule_kind='WORK_ITEM_DISCOVERY'"
    ).fetchone()[0]
    assert count == 1


def test_concurrent_register_cannot_steal_repository(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    catalog = _real_catalog()
    resolver = _Resolver(compiled, oid)
    first = _post(run_store, catalog, resolver, _request(key=str(uuid.uuid4())))
    other = PrincipalRecord(
        principal_id="other-operator",
        authorities=frozenset({"PROJECT_REGISTER", "PROJECT_REVALIDATE", "INSTALLATION_USE"}),
        allowed_installations=frozenset({INSTALLATION}),
        allowed_policy_refs=frozenset({"trusted-base/v1", "budget/v1", "budget-reset/v1"}),
    )
    catalog = ServerRegistrationCatalog(
        principals={**catalog.principals, other.principal_id: other},
        installations=catalog.installations,
        execution_profiles=catalog.execution_profiles,
        budget_policies=catalog.budget_policies,
        budget_reset_windows=catalog.budget_reset_windows,
        trusted_base_policies=catalog.trusted_base_policies,
    )
    stolen = _post(
        run_store,
        catalog,
        resolver,
        _request(key=str(uuid.uuid4())),
        principal="other-operator",
    )
    body = json.loads(stolen.body_json)
    assert stolen.http_status == 409
    assert body["status"] == "REJECTED"
    assert body["rejection_code"] == "STABLE_REPOSITORY_OWNERSHIP_CONFLICT"
    assert run_store.conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0] == 1
    assert run_store.get_project(json.loads(first.body_json)["project_id"]) is not None


def test_fault_before_commit_leaves_no_project_or_schedule(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    api_id, read_id, pub_id = _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    request = _request(key=str(uuid.uuid4()))
    with pytest.raises(TransactionFault):
        run_store.commit_project_registration(
            authenticated_principal_id=PRINCIPAL,
            idempotency_key=request["idempotency_key"],
            request=request,
            authorization_context_digest=AUTHZ_DIGEST,
            adapter_kind="GITHUB",
            canonical_origin="https://github.com",
            installation_or_account_ref=INSTALLATION,
            default_ref="refs/heads/main",
            trusted_base_policy_ref="trusted-base/v1",
            budget_policy_ref="budget/v1",
            budget_reset_window_ref="budget-reset/v1",
            resolved_repository_external_id="repo-external-1",
            resolved_repository_locator="owner/repository",
            resolved_base_commit={"object_format": "sha1", "oid": oid},
            resolved_forge_api_secret_id=api_id,
            resolved_forge_api_secret_version=1,
            resolved_source_read_secret_id=read_id,
            resolved_source_read_secret_version=1,
            resolved_publication_secret_id=pub_id,
            resolved_publication_secret_version=1,
            workflow_hash=compiled.workflow_hash,
            policy_hash=request_digest({"policy": 1}),
            readiness={"ready": True, "diagnostics": []},
            fault=FaultInjectionPoint.BEFORE_COMMIT,
        )
    assert run_store.conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0] == 0
    schedules = run_store.conn.execute(
        "SELECT COUNT(*) FROM forge_observation_schedules"
    ).fetchone()[0]
    assert schedules == 0
    assert (
        run_store.conn.execute("SELECT COUNT(*) FROM project_registration_operations").fetchone()[0]
        == 0
    )


def test_request_secret_fields_are_rejected_before_operation(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    request = _request(key=str(uuid.uuid4()))
    request["token"] = "ghp_not_a_real_secret_value_but_forbidden"
    with pytest.raises(TransportError) as exc:
        _post(run_store, _real_catalog(), _Resolver(compiled, oid), request)
    assert exc.value.http_status == 422
    assert run_store.conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0] == 0


def test_unauthenticated_request_is_401(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    compiled, oid = _bundle_repo(tmp_path)
    with pytest.raises(TransportError) as exc:
        register_or_revalidate_project(
            run_store,
            catalog=_real_catalog(),
            resolver=_Resolver(compiled, oid),
            raw_body=json.dumps(_request(key=str(uuid.uuid4()))).encode(),
            idempotency_key_header=None,
            authenticated_principal_id=None,
        )
    assert exc.value.http_status in {400, 401}


def test_http_handler_round_trip(
    run_store: RunStore, secret_store: SecretStore, tmp_path: Path
) -> None:
    _initialize(run_store)
    _provision_three(run_store, secret_store)
    compiled, oid = _bundle_repo(tmp_path)
    request = _request(key=str(uuid.uuid4()))
    status, headers, payload = handle_registration_http(
        method="POST",
        path="/api/v1/projects/registrations",
        headers={"Idempotency-Key": request["idempotency_key"]},
        body=json.dumps(request).encode("utf-8"),
        principal_id=PRINCIPAL,
        run_store=run_store,
        catalog=_real_catalog(),
        resolver=_Resolver(compiled, oid),
    )
    assert status == 200
    assert headers["Content-Type"] == "application/json"
    body = json.loads(payload)
    assert body["status"] == "SUCCEEDED"
    status, _, payload = handle_registration_http(
        method="POST",
        path="/api/v1/projects/registrations",
        headers={"Idempotency-Key": request["idempotency_key"]},
        body=json.dumps(request).encode("utf-8"),
        principal_id=PRINCIPAL,
        run_store=run_store,
        catalog=_real_catalog(),
        resolver=_Resolver(compiled, oid),
    )
    assert json.loads(payload)["replayed"] is True


def test_cli_onboard_rejects_plaintext_and_disabled_tls(tmp_path: Path, monkeypatch) -> None:
    profiles = tmp_path / "servers"
    profiles.mkdir()
    monkeypatch.setenv("ORCEST_SERVER_PROFILES", str(profiles))
    (profiles / "insecure.yaml").write_text(
        "url: http://127.0.0.1:1\nca_file: x\nclient_cert: x\nclient_key: x\n"
        "installation_or_account_ref: i\ntrusted_base_policy_ref: t\n"
        "budget_policy_ref: b\nbudget_reset_window_ref: r\ncanonical_origin: https://github.com\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(main, ["project", "onboard", "--server", "insecure", "--repo", "o/r"])
    assert result.exit_code != 0
    (profiles / "novalidate.yaml").write_text(
        "url: https://controller.example\nverify: false\nca_file: x\nclient_cert: x\n"
        "client_key: x\ninstallation_or_account_ref: i\ntrusted_base_policy_ref: t\n"
        "budget_policy_ref: b\nbudget_reset_window_ref: r\ncanonical_origin: https://github.com\n",
        encoding="utf-8",
    )
    result = runner.invoke(main, ["project", "onboard", "--server", "novalidate", "--repo", "o/r"])
    assert result.exit_code != 0


def test_project_help_lists_onboard() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["project", "--help"])
    assert result.exit_code == 0
    assert "onboard" in result.output
