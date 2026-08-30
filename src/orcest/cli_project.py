"""``orcest project`` -- the local CLI contract for repository-owned ``.orcest`` bundles.

Implements the four commands ``docs/wiki/repository-configuration.md``'s
"Local CLI contract" assigns to this leaf (workflow-control-v1-id V1-06,
issue #673): ``init``, ``lint``, ``explain``, ``simulate``. ``onboard`` (the
authenticated project-registration endpoint) belongs to a separate leaf
(#674, "add authenticated project onboarding and registration") and is not
implemented here -- these four commands never contact a server and never
mutate one.

Canonical output/exit contract for ``lint``/``explain``/``simulate``:

- success: the command's JSON result object on stdout, exit code ``0``.
- compile/validation failure: a JSON array of secret-free diagnostic objects
  (``{"code", "message", "file", "path"}``) on stdout, exit code ``1``.
- an unresolvable git/environment precondition (no ``git`` on PATH, no
  resolvable trusted default branch, unreadable fixture file): a single
  human-readable line on stderr, exit code ``2``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.project_bundle import BUNDLE_ROOT, BundleValidationError
from orcest.workflow_contract.v1.project_bundle_compile import DEFAULT_PROJECT_PATH, compile_bundle
from orcest.workflow_contract.v1.project_bundle_source import (
    GitBundleSource,
    GitSourceError,
    resolve_commit,
    resolve_default_branch_revision,
)
from orcest.workflow_contract.v1.project_bundle_yaml import YamlParseError

_EXIT_OK = 0
_EXIT_INVALID = 1
_EXIT_ENVIRONMENT = 2


def _resolve_repo_root() -> str:
    import subprocess

    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, timeout=10
    )
    if result.returncode != 0:
        raise click.ClickException(
            "not inside a git repository (git rev-parse --show-toplevel failed)"
        )
    return result.stdout.decode("utf-8").strip()


def _open_source(repo_root: str, revision: str | None) -> GitBundleSource:
    try:
        resolved_revision = (
            revision if revision is not None else resolve_default_branch_revision(repo_root)
        )
        commit = resolve_commit(repo_root, resolved_revision)
    except GitSourceError as exc:
        click.echo(f"error: {exc}", err=True)
        raise SystemExit(_EXIT_ENVIRONMENT) from exc
    return GitBundleSource(repo_root, commit)


def _print_json(value: Any) -> None:
    click.echo(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False))


def _emit_diagnostics_and_exit(diagnostics: list) -> None:
    _print_json([d.to_json() for d in diagnostics])
    raise SystemExit(_EXIT_INVALID)


@click.group()
def project() -> None:
    """Compile, pin, and inspect the repository-owned .orcest workflow bundle."""


_PROMPT_STUBS: dict[str, str] = {
    ".orcest/prompts/implement.md": (
        "# Implement\n\nDescribe the implementation task for the agent here.\n"
    ),
    ".orcest/prompts/repair.md": (
        "# Repair\n\nDescribe how to react to a failed verification or review here.\n"
    ),
    ".orcest/prompts/review-correctness.md": (
        "# Review: correctness\n\nDescribe the correctness review checklist here.\n"
    ),
    ".orcest/prompts/review-security.md": (
        "# Review: security\n\nDescribe the security review checklist here.\n"
    ),
    ".orcest/prompts/adjudicate.md": (
        "# Adjudicate\n\nDescribe how to resolve disputed review findings here.\n"
    ),
}


def _project_yaml_template() -> str:
    return (
        "apiVersion: orcest.dev/v1\n"
        "kind: Project\n"
        "spec:\n"
        "  workflow: .orcest/workflows/implementation.yaml\n"
        "  base:\n"
        "    changePolicy: rebase-before-publication\n"
        "  intake:\n"
        "    readyLabel: orcest:ready\n"
        "    workingLabel: orcest:working\n"
        "    specificationComments: none\n"
    )


def _workflow_yaml_template(profile: str) -> str:
    return (
        "apiVersion: orcest.dev/v1\n"
        "kind: Workflow\n"
        "metadata:\n"
        "  name: implementation\n"
        "spec:\n"
        "  implementation:\n"
        f"    profile: {profile}\n"
        "    prompt: .orcest/prompts/implement.md\n"
        "    timeoutSeconds: 7200\n"
        "  verification:\n"
        "    profile: default\n"
        "    commands:\n"
        "      - id: unit\n"
        "        argv: [make, test]\n"
        "        cwd: .\n"
        "        timeoutSeconds: 1800\n"
        "    repair:\n"
        f"      profile: {profile}\n"
        "      prompt: .orcest/prompts/repair.md\n"
        "  review:\n"
        "    approvalsRequired: 2\n"
        "    slots:\n"
        "      - id: correctness\n"
        f"        profile: {profile}\n"
        "        prompt: .orcest/prompts/review-correctness.md\n"
        "      - id: security\n"
        f"        profile: {profile}\n"
        "        prompt: .orcest/prompts/review-security.md\n"
        "    adjudicator:\n"
        f"      profile: {profile}\n"
        "      prompt: .orcest/prompts/adjudicate.md\n"
    )


@project.command("init")
@click.option(
    "--profile",
    default="codex-default",
    show_default=True,
    help="Execution profile ID to seed the generated workflow with.",
)
def project_init(profile: str) -> None:
    """Create the minimal .orcest layout in the current repository, without credentials."""
    repo_root = Path(_resolve_repo_root())
    targets: dict[Path, str] = {
        repo_root / DEFAULT_PROJECT_PATH: _project_yaml_template(),
        repo_root / BUNDLE_ROOT / "workflows" / "implementation.yaml": _workflow_yaml_template(
            profile
        ),
    }
    for relpath, content in _PROMPT_STUBS.items():
        targets[repo_root / relpath] = content

    existing = sorted(str(path.relative_to(repo_root)) for path in targets if path.exists())
    if existing:
        click.echo("refusing to overwrite existing file(s):", err=True)
        for relpath in existing:
            click.echo(f"  {relpath}", err=True)
        raise SystemExit(_EXIT_INVALID)

    created: list[str] = []
    for path, content in targets.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        created.append(str(path.relative_to(repo_root)))

    _print_json({"created": sorted(created)})
    raise SystemExit(_EXIT_OK)


def _compile_or_exit(repo_root: str, revision: str | None) -> Any:
    source = _open_source(repo_root, revision)
    try:
        return compile_bundle(source)
    except BundleValidationError as exc:
        _emit_diagnostics_and_exit(exc.diagnostics)
    except (GitSourceError, YamlParseError) as exc:
        click.echo(f"error: {exc}", err=True)
        raise SystemExit(_EXIT_ENVIRONMENT) from exc


@project.command("lint")
@click.option(
    "--server",
    "server_profile",
    default=None,
    help="Reserved for server-augmented checks (see #674); local structural checks only for now.",
)
@click.option(
    "--revision",
    default=None,
    help="Git revision to pin (default: the resolved trusted default branch).",
)
def project_lint(server_profile: str | None, revision: str | None) -> None:
    """Perform local structural compilation checks. Never mutates a server."""
    repo_root = _resolve_repo_root()
    compiled = _compile_or_exit(repo_root, revision)
    if server_profile is not None:
        click.echo(
            "note: --server checks (profile names, forge capabilities, server limits, "
            "registration policy) require a registered project (see #674); this run "
            "performed local structural checks only.",
            err=True,
        )
    _print_json(
        {
            "ok": True,
            "trusted_base_commit": compiled.trusted_base_commit.to_json(),
            "workflow_hash": compiled.workflow_hash,
            "files_checked": sorted(compiled.files),
        }
    )
    raise SystemExit(_EXIT_OK)


@project.command("explain")
@click.option(
    "--server",
    "server_profile",
    default=None,
    help="Reserved for server-augmented checks (see #674); local compilation only for now.",
)
@click.option(
    "--revision",
    default=None,
    help="Git revision to pin (default: the resolved trusted default branch).",
)
def project_explain(server_profile: str | None, revision: str | None) -> None:
    """Print the fully defaulted normalized policy, referenced files, and hashes."""
    repo_root = _resolve_repo_root()
    compiled = _compile_or_exit(repo_root, revision)
    referenced_profiles = sorted(
        {
            compiled.workflow["spec"]["implementation"]["profile"],
            *compiled.workflow["spec"]["implementation"]["alternateProfiles"],
            compiled.workflow["spec"]["verification"]["repair"]["profile"],
            *compiled.workflow["spec"]["verification"]["repair"]["alternateProfiles"],
            *(slot["profile"] for slot in compiled.workflow["spec"]["review"]["slots"]),
            *(
                alt
                for slot in compiled.workflow["spec"]["review"]["slots"]
                for alt in slot["alternates"]
            ),
            compiled.workflow["spec"]["review"]["adjudicator"]["profile"],
            *compiled.workflow["spec"]["review"]["adjudicator"]["alternates"],
        }
    )
    result = {
        "trusted_base_commit": compiled.trusted_base_commit.to_json(),
        "workflow_hash": compiled.workflow_hash,
        "project": compiled.project,
        "workflow": compiled.workflow,
        "files": {path: compiled.files[path].to_json() for path in sorted(compiled.files)},
        "referenced_execution_profiles": referenced_profiles,
        "server_constraints": {
            "available": False,
            "note": "server-resolved limits, capability validation, and policy_hash "
            "inputs require a registered project (see #674); not available from "
            "local compilation alone.",
        },
    }
    _print_json(result)
    raise SystemExit(_EXIT_OK)


@project.command("simulate")
@click.option(
    "--event",
    "event_fixture",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to a local, typed, non-secret event fixture (JSON).",
)
@click.option(
    "--revision",
    default=None,
    help="Git revision to pin (default: the resolved trusted default branch).",
)
def project_simulate(event_fixture: str, revision: str | None) -> None:
    """Compile the bundle and validate an event fixture. Never runs an agent or writes to a forge.

    Prints ``reducer.status: "deferred"`` until the pure reducer (#675) lands.
    """
    repo_root = _resolve_repo_root()
    compiled = _compile_or_exit(repo_root, revision)

    try:
        fixture_raw = Path(event_fixture).read_bytes()
    except OSError as exc:
        click.echo(f"error: could not read {event_fixture}: {exc}", err=True)
        raise SystemExit(_EXIT_ENVIRONMENT) from exc

    try:
        fixture = json.loads(fixture_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _emit_diagnostics_and_exit(
            [
                _FixtureDiagnostic(
                    "FIXTURE_NOT_JSON", f"{event_fixture} is not valid UTF-8 JSON: {exc}"
                )
            ]
        )

    if not isinstance(fixture, dict) or set(fixture) != {"trigger", "payload"}:
        _emit_diagnostics_and_exit(
            [
                _FixtureDiagnostic(
                    "FIXTURE_SHAPE_INVALID",
                    "fixture must be a JSON object with exactly {trigger, payload}",
                )
            ]
        )

    trigger = fixture["trigger"]
    payload = fixture["payload"]
    trigger_enum = enums.get_enum("transition.trigger_kind")
    if not isinstance(trigger, str) or trigger not in {member.value for member in trigger_enum}:
        _emit_diagnostics_and_exit(
            [
                _FixtureDiagnostic(
                    "FIXTURE_TRIGGER_INVALID",
                    f"trigger must be one of {sorted(m.value for m in trigger_enum)!r}",
                )
            ]
        )
    if not isinstance(payload, dict):
        _emit_diagnostics_and_exit(
            [_FixtureDiagnostic("FIXTURE_PAYLOAD_INVALID", "payload must be a JSON object")]
        )

    _print_json(
        {
            "trusted_base_commit": compiled.trusted_base_commit.to_json(),
            "workflow_hash": compiled.workflow_hash,
            "trigger": trigger,
            "payload": payload,
            "reducer": {
                "status": "deferred",
                "reason": "the pure reducer and transition ledger "
                "(workflow-control-v1-id V1-08, issue #675) is not implemented yet; "
                "simulate validates the fixture and compiles/pins the bundle, but "
                "cannot yet print transitions/Activities. No agent was executed and "
                "no forge write was performed.",
            },
        }
    )
    raise SystemExit(_EXIT_OK)


class _FixtureDiagnostic:
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message

    def to_json(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "file": "<fixture>", "path": "$"}
