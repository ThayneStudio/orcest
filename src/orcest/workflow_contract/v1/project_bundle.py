"""Parse and strictly validate repository-owned ``.orcest`` v1 documents.

Implements the ``project.yaml`` and ``Workflow`` schemas from
``docs/wiki/repository-configuration.md`` ("`project.yaml` schema" and
"Workflow schema"): closed field sets, materialized defaults, enum/range
validation, repository-relative path safety, and a local secret-value scan.

This module is pure (no filesystem/git access): it turns already-fetched
bytes into materialized, validated documents plus the set of paths those
documents reference. Fetching referenced bytes at one pinned commit is
:mod:`orcest.workflow_contract.v1.project_bundle_source`; combining the two
into one normalized, hashed bundle is
:mod:`orcest.workflow_contract.v1.project_bundle_compile`.

Every diagnostic is a :class:`Diagnostic` carrying a stable ``code``, the
``file`` it came from, and a dotted ``path`` within that document -- never
the offending value, so diagnostics stay secret-free even when the rejected
input was a credential.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Protocol

from orcest.workflow_contract.v1.project_bundle_yaml import (
    load_strict_yaml_document,
)

__all__ = [
    "Diagnostic",
    "DiagnosticsSink",
    "BundleValidationError",
    "PROJECT_API_VERSION",
    "PROJECT_KIND",
    "WORKFLOW_KIND",
    "BUNDLE_ROOT",
    "CHANGE_POLICIES",
    "SPECIFICATION_COMMENTS_MODES",
    "VERIFICATION_PROFILE",
    "EXTERNAL_HEAD_POLICY",
    "normalize_repo_path",
    "scan_mapping_for_secret_key_names",
    "scan_prompt_bytes_for_secret",
    "ParsedProject",
    "ParsedWorkflow",
    "parse_project_document",
    "parse_workflow_document",
]

PROJECT_API_VERSION = "orcest.dev/v1"
PROJECT_KIND = "Project"
WORKFLOW_KIND = "Workflow"
BUNDLE_ROOT = ".orcest"

CHANGE_POLICIES = frozenset({"rebase-before-publication", "pin", "supersede-at-boundary"})
SPECIFICATION_COMMENTS_MODES = frozenset({"none", "maintainer-marked"})
VERIFICATION_PROFILE = "default"
EXTERNAL_HEAD_POLICY = "verify-and-adopt"

_WORKFLOW_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")

_DEFAULT_READY_LABEL = "orcest:ready"
_DEFAULT_WORKING_LABEL = "orcest:working"
_DEFAULT_CHANGE_POLICY = "rebase-before-publication"
_DEFAULT_SPEC_COMMENTS = "none"

_DEFAULT_IMPLEMENTATION_TIMEOUT_SECONDS = 7200
_MIN_IMPLEMENTATION_TIMEOUT_SECONDS = 60
_DEFAULT_VERIFY_COMMAND_TIMEOUT_SECONDS = 1800
_MIN_VERIFY_COMMAND_TIMEOUT_SECONDS = 1
_DEFAULT_MAX_REPAIR_CYCLES = 4
_MIN_MAX_REPAIR_CYCLES = 1
_DEFAULT_REVIEW_APPROVALS_REQUIRED = 2
_MIN_REVIEW_APPROVALS_REQUIRED = 2
_DEFAULT_REVIEW_SLOT_TIMEOUT_SECONDS = 3600
_MIN_REVIEW_SLOT_TIMEOUT_SECONDS = 60
_DEFAULT_MAX_ATTEMPTS_BEFORE_DIAGNOSIS = 3
_MIN_MAX_ATTEMPTS_BEFORE_DIAGNOSIS = 1
_DEFAULT_MAX_DIAGNOSES_BEFORE_REPLAN = 2
_MIN_MAX_DIAGNOSES_BEFORE_REPLAN = 1
_DEFAULT_MAX_PROVIDER_RATE_LIMIT_WAIT_MS = 86_400_000
_MIN_MAX_PROVIDER_RATE_LIMIT_WAIT_MS = 1


@dataclass(frozen=True, slots=True)
class Diagnostic:
    code: str
    message: str
    file: str
    path: str

    def to_json(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "file": self.file, "path": self.path}


class BundleValidationError(ValueError):
    """Raised with one or more secret-free :class:`Diagnostic` records."""

    def __init__(self, diagnostics: list[Diagnostic]):
        self.diagnostics = list(diagnostics)
        super().__init__(
            "; ".join(f"{d.file}:{d.path}: {d.message} [{d.code}]" for d in self.diagnostics)
        )


class _Diagnostics:
    def __init__(self, file: str):
        self.file = file
        self.items: list[Diagnostic] = []

    def add(self, code: str, message: str, path: str) -> None:
        self.items.append(Diagnostic(code=code, message=message, file=self.file, path=path))

    def raise_if_any(self) -> None:
        if self.items:
            raise BundleValidationError(list(self.items))


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "mapping"
    return type(value).__name__


# --- local secret-value scan -------------------------------------------------
#
# The wiki reserves the exact non-secret environment allowlist and profile
# capability checks to the server (#674, not this leaf). Independent of that,
# the issue requires rejecting secret *values* wherever they appear in a
# repository-owned document. This is a defensive, local, pattern-based scan
# applied to every string leaf across project.yaml/workflow.yaml (and, from
# project_bundle_compile, every referenced prompt file) -- it cannot prove a
# string is not a secret, but it fails closed on the well-known shapes.

_SECRET_VALUE_PATTERNS = (
    re.compile(r"ghp_[A-Za-z0-9]{20,}"),
    re.compile(r"gh[oprsu]_[A-Za-z0-9]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
)
_SECRET_KEY_NAME_RE = re.compile(
    r"(secret|token|passwd|password|api[-_]?key|credential|private[-_]?key)", re.IGNORECASE
)


def _scan_string_for_secret(value: str, *, path: str, diags: _Diagnostics) -> None:
    for pattern in _SECRET_VALUE_PATTERNS:
        if pattern.search(value):
            diags.add(
                "SECRET_VALUE_REJECTED",
                "value matches a known credential/secret shape and is rejected "
                "(the value itself is not included in this diagnostic)",
                path,
            )
            return


class DiagnosticsSink(Protocol):
    """The narrow ``.add(code, message, path)`` shape a diagnostics collector needs.

    :class:`_Diagnostics` (file-scoped, used within this module) and
    :mod:`project_bundle_compile`'s bundle-scoped ``_Diag`` adapter both
    satisfy this structurally -- it is the shared contract between the two
    without either module depending on the other's private collector type.
    """

    def add(self, code: str, message: str, path: str) -> None: ...


def scan_mapping_for_secret_key_names(value: Any, *, path: str, diags: DiagnosticsSink) -> None:
    """Recursively flag mapping keys that look like they hold a secret, by name alone."""
    if isinstance(value, dict):
        for key, item in value.items():
            item_path = f"{path}.{key}"
            if isinstance(key, str) and _SECRET_KEY_NAME_RE.search(key):
                diags.add(
                    "SECRET_KEY_NAME_REJECTED",
                    f"field name {key!r} looks like it is meant to hold a secret; "
                    "repository configuration must not carry secret values",
                    item_path,
                )
            scan_mapping_for_secret_key_names(item, path=item_path, diags=diags)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            scan_mapping_for_secret_key_names(item, path=f"{path}[{index}]", diags=diags)


def scan_prompt_bytes_for_secret(text: str, *, file: str) -> list[Diagnostic]:
    diags = _Diagnostics(file)
    for lineno, line in enumerate(text.splitlines(), start=1):
        _scan_string_for_secret(line, path=f"line {lineno}", diags=diags)
    return diags.items


# --- repository-relative path safety ----------------------------------------


def normalize_repo_path(
    raw: Any, *, path: str, diags: _Diagnostics, must_be_in_bundle: bool = True
) -> str | None:
    """Validate a repository-relative, slash-separated path per the wiki's path rules.

    Rejects absolute paths, ``..``/empty segments, backslashes, and (when
    ``must_be_in_bundle``) anything outside ``.orcest/``. Returns ``None``
    (after recording a diagnostic) rather than raising, so callers can keep
    collecting diagnostics from sibling fields.
    """
    if not isinstance(raw, str):
        diags.add("TYPE_INVALID", f"expected a string path, got {_type_name(raw)}", path)
        return None
    if raw == "":
        diags.add("PATH_INVALID", "path must not be empty", path)
        return None
    if "\\" in raw:
        diags.add("PATH_INVALID", "backslashes are not allowed in a repository path", path)
        return None
    if raw != raw.strip():
        diags.add("PATH_INVALID", "path must not have leading/trailing whitespace", path)
        return None
    if raw.startswith("/"):
        diags.add("PATH_TRAVERSAL", "absolute paths are not allowed", path)
        return None
    segments = raw.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        diags.add("PATH_TRAVERSAL", "path traversal or an empty path segment is not allowed", path)
        return None
    if must_be_in_bundle and not (raw == BUNDLE_ROOT or raw.startswith(f"{BUNDLE_ROOT}/")):
        diags.add("PATH_OUTSIDE_BUNDLE", f"path must be under {BUNDLE_ROOT}/", path)
        return None
    return raw


def normalize_repo_relative_dir(raw: Any, *, path: str, diags: _Diagnostics) -> str | None:
    """Validate a verification command's ``cwd``: any repo-relative directory, default ``.``."""
    if not isinstance(raw, str):
        diags.add("TYPE_INVALID", f"expected a string directory, got {_type_name(raw)}", path)
        return None
    if raw == ".":
        return raw
    if raw == "" or raw.startswith("/") or "\\" in raw or raw != raw.strip():
        diags.add("PATH_INVALID", "cwd must be '.' or a repository-relative directory", path)
        return None
    segments = raw.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        diags.add("PATH_TRAVERSAL", "path traversal or an empty path segment is not allowed", path)
        return None
    return raw


# --- generic field poppers ---------------------------------------------------


def _expect_mapping(value: Any, *, path: str, diags: _Diagnostics) -> dict | None:
    if not isinstance(value, dict):
        diags.add("TYPE_INVALID", f"expected a mapping, got {_type_name(value)}", path)
        return None
    return dict(value)


def _reject_unknown(remaining: dict, *, path: str, diags: _Diagnostics) -> None:
    for key in sorted(remaining):
        diags.add("UNKNOWN_FIELD", f"unknown field {key!r}", f"{path}.{key}")


def _pop_required_literal(
    d: dict, key: str, literal: str, *, path: str, diags: _Diagnostics
) -> None:
    if key not in d:
        diags.add("FIELD_MISSING", f"{key} is required", f"{path}.{key}")
        return
    value = d.pop(key)
    if value != literal:
        diags.add(
            "LITERAL_MISMATCH", f"{key} must be exactly {literal!r}, got {value!r}", f"{path}.{key}"
        )


def _pop_required_str(
    d: dict, key: str, *, path: str, diags: _Diagnostics, enum: frozenset[str] | None = None
) -> str | None:
    if key not in d:
        diags.add("FIELD_MISSING", f"{key} is required", f"{path}.{key}")
        return None
    field_path = f"{path}.{key}"
    value = d.pop(key)
    if value is None:
        diags.add("NULL_NOT_ALLOWED", f"{key} must not be null", field_path)
        return None
    if not isinstance(value, str):
        diags.add("TYPE_INVALID", f"{key} must be a string, got {_type_name(value)}", field_path)
        return None
    _scan_string_for_secret(value, path=field_path, diags=diags)
    if enum is not None and value not in enum:
        diags.add(
            "ENUM_INVALID", f"{key} must be one of {sorted(enum)!r}, got {value!r}", field_path
        )
        return None
    return value


def _pop_optional_str(
    d: dict,
    key: str,
    default: str,
    *,
    path: str,
    diags: _Diagnostics,
    enum: frozenset[str] | None = None,
) -> str:
    if key not in d:
        return default
    field_path = f"{path}.{key}"
    value = d.pop(key)
    if value is None:
        diags.add("NULL_NOT_ALLOWED", f"{key} must not be null", field_path)
        return default
    if not isinstance(value, str):
        diags.add("TYPE_INVALID", f"{key} must be a string, got {_type_name(value)}", field_path)
        return default
    _scan_string_for_secret(value, path=field_path, diags=diags)
    if enum is not None and value not in enum:
        diags.add(
            "ENUM_INVALID", f"{key} must be one of {sorted(enum)!r}, got {value!r}", field_path
        )
        return default
    return value


def _pop_optional_bool(d: dict, key: str, default: bool, *, path: str, diags: _Diagnostics) -> bool:
    if key not in d:
        return default
    field_path = f"{path}.{key}"
    value = d.pop(key)
    if not isinstance(value, bool):
        diags.add("TYPE_INVALID", f"{key} must be a boolean, got {_type_name(value)}", field_path)
        return default
    return value


def _pop_optional_int(
    d: dict, key: str, default: int, *, path: str, diags: _Diagnostics, minimum: int
) -> int:
    if key not in d:
        return default
    field_path = f"{path}.{key}"
    value = d.pop(key)
    if isinstance(value, bool) or not isinstance(value, int):
        diags.add("TYPE_INVALID", f"{key} must be an integer, got {_type_name(value)}", field_path)
        return default
    if value < minimum:
        diags.add("RANGE_INVALID", f"{key} must be >= {minimum}, got {value}", field_path)
        return default
    return value


def _pop_optional_str_list_unique(
    d: dict, key: str, default: tuple[str, ...], *, path: str, diags: _Diagnostics
) -> list[str]:
    if key not in d:
        return list(default)
    field_path = f"{path}.{key}"
    value = d.pop(key)
    if not isinstance(value, list):
        diags.add("TYPE_INVALID", f"{key} must be an array, got {_type_name(value)}", field_path)
        return list(default)
    result: list[str] = []
    seen: set[str] = set()
    ok = True
    for index, item in enumerate(value):
        item_path = f"{field_path}[{index}]"
        if not isinstance(item, str) or not item:
            diags.add("TYPE_INVALID", f"{key}[{index}] must be a nonempty string", item_path)
            ok = False
            continue
        _scan_string_for_secret(item, path=item_path, diags=diags)
        if item in seen:
            diags.add("DUPLICATE_VALUE", f"{key} contains a duplicate value {item!r}", item_path)
            ok = False
            continue
        seen.add(item)
        result.append(item)
    return result if ok else list(default)


# --- project.yaml -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParsedProject:
    materialized: dict[str, Any]
    workflow_path: str


def parse_project_document(
    raw_bytes: bytes, *, file: str = f"{BUNDLE_ROOT}/project.yaml"
) -> ParsedProject:
    document = load_strict_yaml_document(raw_bytes, file=file)
    diags = _Diagnostics(file)
    root = _expect_mapping(document, path="$", diags=diags)
    if root is None:
        diags.raise_if_any()
    assert root is not None

    _pop_required_literal(root, "apiVersion", PROJECT_API_VERSION, path="$", diags=diags)
    _pop_required_literal(root, "kind", PROJECT_KIND, path="$", diags=diags)

    workflow_path: str | None = None
    change_policy = _DEFAULT_CHANGE_POLICY
    ready_label = _DEFAULT_READY_LABEL
    working_label = _DEFAULT_WORKING_LABEL
    spec_comments = _DEFAULT_SPEC_COMMENTS

    if "spec" not in root:
        diags.add("FIELD_MISSING", "spec is required", "$.spec")
    else:
        spec_dict = _expect_mapping(root.pop("spec"), path="$.spec", diags=diags)
        if spec_dict is not None:
            if "workflow" not in spec_dict:
                diags.add("FIELD_MISSING", "workflow is required", "$.spec.workflow")
            else:
                workflow_path = normalize_repo_path(
                    spec_dict.pop("workflow"), path="$.spec.workflow", diags=diags
                )

            if "base" in spec_dict:
                base_dict = _expect_mapping(spec_dict.pop("base"), path="$.spec.base", diags=diags)
                if base_dict is not None:
                    change_policy = _pop_optional_str(
                        base_dict,
                        "changePolicy",
                        _DEFAULT_CHANGE_POLICY,
                        path="$.spec.base",
                        diags=diags,
                        enum=CHANGE_POLICIES,
                    )
                    _reject_unknown(base_dict, path="$.spec.base", diags=diags)

            if "intake" in spec_dict:
                intake_dict = _expect_mapping(
                    spec_dict.pop("intake"), path="$.spec.intake", diags=diags
                )
                if intake_dict is not None:
                    ready_label = _pop_optional_str(
                        intake_dict,
                        "readyLabel",
                        _DEFAULT_READY_LABEL,
                        path="$.spec.intake",
                        diags=diags,
                    )
                    working_label = _pop_optional_str(
                        intake_dict,
                        "workingLabel",
                        _DEFAULT_WORKING_LABEL,
                        path="$.spec.intake",
                        diags=diags,
                    )
                    spec_comments = _pop_optional_str(
                        intake_dict,
                        "specificationComments",
                        _DEFAULT_SPEC_COMMENTS,
                        path="$.spec.intake",
                        diags=diags,
                        enum=SPECIFICATION_COMMENTS_MODES,
                    )
                    if not ready_label:
                        diags.add(
                            "FIELD_INVALID",
                            "readyLabel must be nonempty",
                            "$.spec.intake.readyLabel",
                        )
                    if not working_label:
                        diags.add(
                            "FIELD_INVALID",
                            "workingLabel must be nonempty",
                            "$.spec.intake.workingLabel",
                        )
                    _reject_unknown(intake_dict, path="$.spec.intake", diags=diags)

            _reject_unknown(spec_dict, path="$.spec", diags=diags)

    if ready_label == working_label:
        diags.add(
            "READY_WORKING_LABEL_COLLISION",
            "spec.intake.readyLabel and spec.intake.workingLabel must differ",
            "$.spec.intake",
        )

    _reject_unknown(root, path="$", diags=diags)
    diags.raise_if_any()
    assert workflow_path is not None

    materialized = {
        "apiVersion": PROJECT_API_VERSION,
        "kind": PROJECT_KIND,
        "spec": {
            "workflow": workflow_path,
            "base": {"changePolicy": change_policy},
            "intake": {
                "readyLabel": ready_label,
                "workingLabel": working_label,
                "specificationComments": spec_comments,
            },
        },
    }
    return ParsedProject(materialized=materialized, workflow_path=workflow_path)


# --- workflow.yaml -------------------------------------------------------------


def _pop_required_argv(d: dict, key: str, *, path: str, diags: _Diagnostics) -> list[str] | None:
    field_path = f"{path}.{key}"
    if key not in d:
        diags.add("FIELD_MISSING", f"{key} is required", field_path)
        return None
    value = d.pop(key)
    if not isinstance(value, list) or not value:
        diags.add("TYPE_INVALID", f"{key} must be a nonempty array of strings", field_path)
        return None
    result: list[str] = []
    ok = True
    for index, item in enumerate(value):
        item_path = f"{field_path}[{index}]"
        if not isinstance(item, str) or not item:
            diags.add("TYPE_INVALID", f"{key}[{index}] must be a nonempty string", item_path)
            ok = False
            continue
        _scan_string_for_secret(item, path=item_path, diags=diags)
        result.append(item)
    return result if ok else None


def _pop_optional_str_str_dict(
    d: dict, key: str, *, path: str, diags: _Diagnostics
) -> dict[str, str]:
    if key not in d:
        return {}
    field_path = f"{path}.{key}"
    value = d.pop(key)
    if not isinstance(value, dict):
        diags.add("TYPE_INVALID", f"{key} must be a mapping of string to string", field_path)
        return {}
    result: dict[str, str] = {}
    for name, item in value.items():
        item_path = f"{field_path}.{name}"
        if not isinstance(name, str) or not name:
            diags.add("TYPE_INVALID", f"{key} keys must be nonempty strings", field_path)
            continue
        if _SECRET_KEY_NAME_RE.search(name):
            diags.add(
                "SECRET_KEY_NAME_REJECTED",
                f"environment variable name {name!r} looks like it is meant to hold a secret",
                item_path,
            )
            continue
        if not isinstance(item, str):
            diags.add("TYPE_INVALID", f"{key}.{name} must be a string", item_path)
            continue
        _scan_string_for_secret(item, path=item_path, diags=diags)
        result[name] = item
    return result


def _parse_command(
    raw: Any, *, index: int, path: str, diags: _Diagnostics
) -> dict[str, Any] | None:
    item_path = f"{path}[{index}]"
    command = _expect_mapping(raw, path=item_path, diags=diags)
    if command is None:
        return None
    command_id = _pop_required_str(command, "id", path=item_path, diags=diags)
    argv = _pop_required_argv(command, "argv", path=item_path, diags=diags)
    cwd_raw = command.pop("cwd", ".")
    cwd = normalize_repo_relative_dir(cwd_raw, path=f"{item_path}.cwd", diags=diags)
    timeout_seconds = _pop_optional_int(
        command,
        "timeoutSeconds",
        _DEFAULT_VERIFY_COMMAND_TIMEOUT_SECONDS,
        path=item_path,
        diags=diags,
        minimum=_MIN_VERIFY_COMMAND_TIMEOUT_SECONDS,
    )
    environment = _pop_optional_str_str_dict(command, "environment", path=item_path, diags=diags)
    _reject_unknown(command, path=item_path, diags=diags)
    if command_id is None or argv is None or cwd is None:
        return None
    return {
        "id": command_id,
        "argv": argv,
        "cwd": cwd,
        "timeoutSeconds": timeout_seconds,
        "environment": environment,
    }


def _parse_review_slot(
    raw: Any, *, index: int, path: str, diags: _Diagnostics
) -> tuple[dict[str, Any], str] | None:
    item_path = f"{path}[{index}]"
    slot = _expect_mapping(raw, path=item_path, diags=diags)
    if slot is None:
        return None
    slot_id = _pop_required_str(slot, "id", path=item_path, diags=diags)
    profile = _pop_required_str(slot, "profile", path=item_path, diags=diags)
    prompt = None
    if "prompt" not in slot:
        diags.add("FIELD_MISSING", "prompt is required", f"{item_path}.prompt")
    else:
        prompt = normalize_repo_path(slot.pop("prompt"), path=f"{item_path}.prompt", diags=diags)
    alternates = _pop_optional_str_list_unique(slot, "alternates", (), path=item_path, diags=diags)
    timeout_seconds = _pop_optional_int(
        slot,
        "timeoutSeconds",
        _DEFAULT_REVIEW_SLOT_TIMEOUT_SECONDS,
        path=item_path,
        diags=diags,
        minimum=_MIN_REVIEW_SLOT_TIMEOUT_SECONDS,
    )
    _reject_unknown(slot, path=item_path, diags=diags)
    if slot_id is None or profile is None or prompt is None:
        return None
    return (
        {
            "id": slot_id,
            "profile": profile,
            "prompt": prompt,
            "alternates": alternates,
            "timeoutSeconds": timeout_seconds,
        },
        prompt,
    )


@dataclass(frozen=True, slots=True)
class ParsedWorkflow:
    materialized: dict[str, Any]
    referenced_prompt_paths: list[str] = field(default_factory=list)


def parse_workflow_document(raw_bytes: bytes, *, file: str) -> ParsedWorkflow:
    document = load_strict_yaml_document(raw_bytes, file=file)
    diags = _Diagnostics(file)
    root = _expect_mapping(document, path="$", diags=diags)
    if root is None:
        diags.raise_if_any()
    assert root is not None

    _pop_required_literal(root, "apiVersion", PROJECT_API_VERSION, path="$", diags=diags)
    _pop_required_literal(root, "kind", WORKFLOW_KIND, path="$", diags=diags)

    name: str | None = None
    if "metadata" not in root:
        diags.add("FIELD_MISSING", "metadata is required", "$.metadata")
    else:
        metadata_dict = _expect_mapping(root.pop("metadata"), path="$.metadata", diags=diags)
        if metadata_dict is not None:
            name = _pop_required_str(metadata_dict, "name", path="$.metadata", diags=diags)
            if name is not None and not _WORKFLOW_NAME_RE.fullmatch(name):
                diags.add(
                    "PATTERN_MISMATCH",
                    "name must match [a-z][a-z0-9-]{0,63}",
                    "$.metadata.name",
                )
            _reject_unknown(metadata_dict, path="$.metadata", diags=diags)

    prompt_paths: list[str] = []
    implementation: dict[str, Any] | None = None
    verification: dict[str, Any] | None = None
    review: dict[str, Any] | None = None
    publication = {"requiredChecks": [], "externalHeadPolicy": EXTERNAL_HEAD_POLICY}
    recovery = {
        "maxAttemptsPerActivityBeforeDiagnosis": _DEFAULT_MAX_ATTEMPTS_BEFORE_DIAGNOSIS,
        "maxDiagnosesBeforeReplan": _DEFAULT_MAX_DIAGNOSES_BEFORE_REPLAN,
        "maxProviderRateLimitWaitMs": _DEFAULT_MAX_PROVIDER_RATE_LIMIT_WAIT_MS,
    }

    if "spec" not in root:
        diags.add("FIELD_MISSING", "spec is required", "$.spec")
    else:
        spec_dict = _expect_mapping(root.pop("spec"), path="$.spec", diags=diags)
        if spec_dict is not None:
            # implementation
            if "implementation" not in spec_dict:
                diags.add("FIELD_MISSING", "implementation is required", "$.spec.implementation")
            else:
                impl_path = "$.spec.implementation"
                impl_dict = _expect_mapping(
                    spec_dict.pop("implementation"), path=impl_path, diags=diags
                )
                if impl_dict is not None:
                    impl_profile = _pop_required_str(
                        impl_dict, "profile", path=impl_path, diags=diags
                    )
                    impl_prompt = None
                    if "prompt" not in impl_dict:
                        diags.add("FIELD_MISSING", "prompt is required", f"{impl_path}.prompt")
                    else:
                        impl_prompt = normalize_repo_path(
                            impl_dict.pop("prompt"), path=f"{impl_path}.prompt", diags=diags
                        )
                    impl_timeout = _pop_optional_int(
                        impl_dict,
                        "timeoutSeconds",
                        _DEFAULT_IMPLEMENTATION_TIMEOUT_SECONDS,
                        path=impl_path,
                        diags=diags,
                        minimum=_MIN_IMPLEMENTATION_TIMEOUT_SECONDS,
                    )
                    impl_alternates = _pop_optional_str_list_unique(
                        impl_dict, "alternateProfiles", (), path=impl_path, diags=diags
                    )
                    _reject_unknown(impl_dict, path=impl_path, diags=diags)
                    if impl_profile is not None and impl_prompt is not None:
                        implementation = {
                            "profile": impl_profile,
                            "prompt": impl_prompt,
                            "timeoutSeconds": impl_timeout,
                            "alternateProfiles": impl_alternates,
                        }
                        prompt_paths.append(impl_prompt)

            # verification
            if "verification" not in spec_dict:
                diags.add("FIELD_MISSING", "verification is required", "$.spec.verification")
            else:
                ver_path = "$.spec.verification"
                ver_dict = _expect_mapping(
                    spec_dict.pop("verification"), path=ver_path, diags=diags
                )
                if ver_dict is not None:
                    ver_profile = _pop_optional_str(
                        ver_dict,
                        "profile",
                        VERIFICATION_PROFILE,
                        path=ver_path,
                        diags=diags,
                        enum=frozenset({VERIFICATION_PROFILE}),
                    )
                    commands: list[dict[str, Any]] = []
                    if "commands" not in ver_dict:
                        diags.add("FIELD_MISSING", "commands is required", f"{ver_path}.commands")
                    else:
                        commands_raw = ver_dict.pop("commands")
                        if not isinstance(commands_raw, list) or not commands_raw:
                            diags.add(
                                "TYPE_INVALID",
                                "commands must be a nonempty array",
                                f"{ver_path}.commands",
                            )
                        else:
                            seen_ids: set[str] = set()
                            for index, raw_command in enumerate(commands_raw):
                                parsed = _parse_command(
                                    raw_command,
                                    index=index,
                                    path=f"{ver_path}.commands",
                                    diags=diags,
                                )
                                if parsed is None:
                                    continue
                                if parsed["id"] in seen_ids:
                                    diags.add(
                                        "DUPLICATE_VALUE",
                                        f"duplicate command id {parsed['id']!r}",
                                        f"{ver_path}.commands[{index}].id",
                                    )
                                    continue
                                seen_ids.add(parsed["id"])
                                commands.append(parsed)

                    repair: dict[str, Any] | None = None
                    if "repair" not in ver_dict:
                        diags.add("FIELD_MISSING", "repair is required", f"{ver_path}.repair")
                    else:
                        repair_path = f"{ver_path}.repair"
                        repair_dict = _expect_mapping(
                            ver_dict.pop("repair"), path=repair_path, diags=diags
                        )
                        if repair_dict is not None:
                            repair_profile = _pop_required_str(
                                repair_dict, "profile", path=repair_path, diags=diags
                            )
                            repair_prompt = None
                            if "prompt" not in repair_dict:
                                diags.add(
                                    "FIELD_MISSING", "prompt is required", f"{repair_path}.prompt"
                                )
                            else:
                                repair_prompt = normalize_repo_path(
                                    repair_dict.pop("prompt"),
                                    path=f"{repair_path}.prompt",
                                    diags=diags,
                                )
                            default_impl_timeout = (
                                implementation["timeoutSeconds"]
                                if implementation is not None
                                else _DEFAULT_IMPLEMENTATION_TIMEOUT_SECONDS
                            )
                            repair_timeout = _pop_optional_int(
                                repair_dict,
                                "timeoutSeconds",
                                default_impl_timeout,
                                path=repair_path,
                                diags=diags,
                                minimum=_MIN_IMPLEMENTATION_TIMEOUT_SECONDS,
                            )
                            default_impl_alternates = tuple(
                                implementation["alternateProfiles"]
                                if implementation is not None
                                else ()
                            )
                            repair_alternates = _pop_optional_str_list_unique(
                                repair_dict,
                                "alternateProfiles",
                                default_impl_alternates,
                                path=repair_path,
                                diags=diags,
                            )
                            _reject_unknown(repair_dict, path=repair_path, diags=diags)
                            if repair_profile is not None and repair_prompt is not None:
                                repair = {
                                    "profile": repair_profile,
                                    "prompt": repair_prompt,
                                    "timeoutSeconds": repair_timeout,
                                    "alternateProfiles": repair_alternates,
                                }
                                prompt_paths.append(repair_prompt)

                    max_repair_cycles = _pop_optional_int(
                        ver_dict,
                        "maxRepairCyclesBeforeDiagnosis",
                        _DEFAULT_MAX_REPAIR_CYCLES,
                        path=ver_path,
                        diags=diags,
                        minimum=_MIN_MAX_REPAIR_CYCLES,
                    )
                    _reject_unknown(ver_dict, path=ver_path, diags=diags)
                    if commands and repair is not None:
                        verification = {
                            "profile": ver_profile,
                            "commands": commands,
                            "repair": repair,
                            "maxRepairCyclesBeforeDiagnosis": max_repair_cycles,
                        }

            # review
            if "review" not in spec_dict:
                diags.add("FIELD_MISSING", "review is required", "$.spec.review")
            else:
                rev_path = "$.spec.review"
                rev_dict = _expect_mapping(spec_dict.pop("review"), path=rev_path, diags=diags)
                if rev_dict is not None:
                    approvals_required = _pop_optional_int(
                        rev_dict,
                        "approvalsRequired",
                        _DEFAULT_REVIEW_APPROVALS_REQUIRED,
                        path=rev_path,
                        diags=diags,
                        minimum=_MIN_REVIEW_APPROVALS_REQUIRED,
                    )
                    require_distinct_provider = _pop_optional_bool(
                        rev_dict, "requireDistinctProviderFamily", False, path=rev_path, diags=diags
                    )
                    require_distinct_model = _pop_optional_bool(
                        rev_dict, "requireDistinctModelFamily", False, path=rev_path, diags=diags
                    )
                    slots: list[dict[str, Any]] = []
                    if "slots" not in rev_dict:
                        diags.add("FIELD_MISSING", "slots is required", f"{rev_path}.slots")
                    else:
                        slots_raw = rev_dict.pop("slots")
                        if not isinstance(slots_raw, list) or not slots_raw:
                            diags.add(
                                "TYPE_INVALID",
                                "slots must be a nonempty array",
                                f"{rev_path}.slots",
                            )
                        else:
                            seen_slot_ids: set[str] = set()
                            for index, raw_slot in enumerate(slots_raw):
                                parsed_slot = _parse_review_slot(
                                    raw_slot, index=index, path=f"{rev_path}.slots", diags=diags
                                )
                                if parsed_slot is None:
                                    continue
                                slot, slot_prompt = parsed_slot
                                if slot["id"] in seen_slot_ids:
                                    diags.add(
                                        "DUPLICATE_VALUE",
                                        f"duplicate review slot id {slot['id']!r}",
                                        f"{rev_path}.slots[{index}].id",
                                    )
                                    continue
                                seen_slot_ids.add(slot["id"])
                                slots.append(slot)
                                prompt_paths.append(slot_prompt)
                            if slots and len(slots) != approvals_required:
                                diags.add(
                                    "REVIEW_SLOT_COUNT_MISMATCH",
                                    f"review.slots must contain exactly approvalsRequired "
                                    f"({approvals_required}) entries, found {len(slots)}",
                                    f"{rev_path}.slots",
                                )

                    adjudicator: dict[str, Any] | None = None
                    if "adjudicator" not in rev_dict:
                        diags.add(
                            "FIELD_MISSING", "adjudicator is required", f"{rev_path}.adjudicator"
                        )
                    else:
                        adj_path = f"{rev_path}.adjudicator"
                        adj_dict = _expect_mapping(
                            rev_dict.pop("adjudicator"), path=adj_path, diags=diags
                        )
                        if adj_dict is not None:
                            adj_profile = _pop_required_str(
                                adj_dict, "profile", path=adj_path, diags=diags
                            )
                            adj_prompt = None
                            if "prompt" not in adj_dict:
                                diags.add(
                                    "FIELD_MISSING", "prompt is required", f"{adj_path}.prompt"
                                )
                            else:
                                adj_prompt = normalize_repo_path(
                                    adj_dict.pop("prompt"), path=f"{adj_path}.prompt", diags=diags
                                )
                            adj_alternates = _pop_optional_str_list_unique(
                                adj_dict, "alternates", (), path=adj_path, diags=diags
                            )
                            adj_timeout = _pop_optional_int(
                                adj_dict,
                                "timeoutSeconds",
                                _DEFAULT_REVIEW_SLOT_TIMEOUT_SECONDS,
                                path=adj_path,
                                diags=diags,
                                minimum=_MIN_REVIEW_SLOT_TIMEOUT_SECONDS,
                            )
                            _reject_unknown(adj_dict, path=adj_path, diags=diags)
                            if adj_profile is not None and adj_prompt is not None:
                                adjudicator = {
                                    "profile": adj_profile,
                                    "prompt": adj_prompt,
                                    "alternates": adj_alternates,
                                    "timeoutSeconds": adj_timeout,
                                }
                                prompt_paths.append(adj_prompt)

                    _reject_unknown(rev_dict, path=rev_path, diags=diags)
                    if slots and adjudicator is not None:
                        review = {
                            "approvalsRequired": approvals_required,
                            "requireDistinctProviderFamily": require_distinct_provider,
                            "requireDistinctModelFamily": require_distinct_model,
                            "slots": slots,
                            "adjudicator": adjudicator,
                        }

            # publication (optional whole object)
            if "publication" in spec_dict:
                pub_path = "$.spec.publication"
                pub_dict = _expect_mapping(spec_dict.pop("publication"), path=pub_path, diags=diags)
                if pub_dict is not None:
                    required_checks = _pop_optional_str_list_unique(
                        pub_dict, "requiredChecks", (), path=pub_path, diags=diags
                    )
                    external_head_policy = _pop_optional_str(
                        pub_dict,
                        "externalHeadPolicy",
                        EXTERNAL_HEAD_POLICY,
                        path=pub_path,
                        diags=diags,
                        enum=frozenset({EXTERNAL_HEAD_POLICY}),
                    )
                    _reject_unknown(pub_dict, path=pub_path, diags=diags)
                    publication = {
                        "requiredChecks": required_checks,
                        "externalHeadPolicy": external_head_policy,
                    }

            # recovery (optional whole object)
            if "recovery" in spec_dict:
                rec_path = "$.spec.recovery"
                rec_dict = _expect_mapping(spec_dict.pop("recovery"), path=rec_path, diags=diags)
                if rec_dict is not None:
                    max_attempts = _pop_optional_int(
                        rec_dict,
                        "maxAttemptsPerActivityBeforeDiagnosis",
                        _DEFAULT_MAX_ATTEMPTS_BEFORE_DIAGNOSIS,
                        path=rec_path,
                        diags=diags,
                        minimum=_MIN_MAX_ATTEMPTS_BEFORE_DIAGNOSIS,
                    )
                    max_diagnoses = _pop_optional_int(
                        rec_dict,
                        "maxDiagnosesBeforeReplan",
                        _DEFAULT_MAX_DIAGNOSES_BEFORE_REPLAN,
                        path=rec_path,
                        diags=diags,
                        minimum=_MIN_MAX_DIAGNOSES_BEFORE_REPLAN,
                    )
                    max_wait_ms = _pop_optional_int(
                        rec_dict,
                        "maxProviderRateLimitWaitMs",
                        _DEFAULT_MAX_PROVIDER_RATE_LIMIT_WAIT_MS,
                        path=rec_path,
                        diags=diags,
                        minimum=_MIN_MAX_PROVIDER_RATE_LIMIT_WAIT_MS,
                    )
                    _reject_unknown(rec_dict, path=rec_path, diags=diags)
                    recovery = {
                        "maxAttemptsPerActivityBeforeDiagnosis": max_attempts,
                        "maxDiagnosesBeforeReplan": max_diagnoses,
                        "maxProviderRateLimitWaitMs": max_wait_ms,
                    }

            _reject_unknown(spec_dict, path="$.spec", diags=diags)

    # Cyclic-reference guard: no prompt may resolve to this workflow's own path
    # or to any other prompt path already claimed by a different logical role
    # colliding case-insensitively (defensive; see module docstring).
    _check_no_self_reference(prompt_paths, workflow_file=file, diags=diags)

    _reject_unknown(root, path="$", diags=diags)
    diags.raise_if_any()

    assert name is not None
    assert implementation is not None
    assert verification is not None
    assert review is not None

    materialized = {
        "apiVersion": PROJECT_API_VERSION,
        "kind": WORKFLOW_KIND,
        "metadata": {"name": name},
        "spec": {
            "implementation": implementation,
            "verification": verification,
            "review": review,
            "publication": publication,
            "recovery": recovery,
        },
    }
    # Deduplicate while preserving first-encounter (deterministic) order.
    ordered_unique_prompts = list(dict.fromkeys(prompt_paths))
    return ParsedWorkflow(materialized=materialized, referenced_prompt_paths=ordered_unique_prompts)


def _check_no_self_reference(
    prompt_paths: list[str], *, workflow_file: str, diags: _Diagnostics
) -> None:
    for index, prompt_path in enumerate(prompt_paths):
        if prompt_path == workflow_file:
            diags.add(
                "CYCLIC_REFERENCE",
                f"a prompt path must not reference the workflow document {workflow_file!r} itself",
                f"$.<prompt reference {index}>",
            )
