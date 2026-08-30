"""Tests for orcest.workflow_contract.v1.project_bundle (schema parsing)."""

from __future__ import annotations

import pytest

from orcest.workflow_contract.v1.project_bundle import (
    BundleValidationError,
    parse_project_document,
    parse_workflow_document,
)
from orcest.workflow_contract.v1.project_bundle_yaml import YamlParseError

VALID_PROJECT = b"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
"""

VALID_WORKFLOW = b"""
apiVersion: orcest.dev/v1
kind: Workflow
metadata:
  name: implementation
spec:
  implementation:
    profile: codex-default
    prompt: .orcest/prompts/implement.md
  verification:
    commands:
      - id: unit
        argv: [make, test]
    repair:
      profile: codex-default
      prompt: .orcest/prompts/repair.md
  review:
    slots:
      - id: correctness
        profile: claude-review
        prompt: .orcest/prompts/review-correctness.md
      - id: security
        profile: codex-review
        prompt: .orcest/prompts/review-security.md
    adjudicator:
      profile: claude-review
      prompt: .orcest/prompts/adjudicate.md
"""


def _diag_codes(exc: BundleValidationError) -> set[str]:
    return {d.code for d in exc.diagnostics}


# --- project.yaml -------------------------------------------------------------


def test_project_materializes_defaults() -> None:
    parsed = parse_project_document(VALID_PROJECT)
    assert parsed.materialized == {
        "apiVersion": "orcest.dev/v1",
        "kind": "Project",
        "spec": {
            "workflow": ".orcest/workflows/implementation.yaml",
            "base": {"changePolicy": "rebase-before-publication"},
            "intake": {
                "readyLabel": "orcest:ready",
                "workingLabel": "orcest:working",
                "specificationComments": "none",
            },
        },
    }
    assert parsed.workflow_path == ".orcest/workflows/implementation.yaml"


def test_project_wrong_api_version_rejected() -> None:
    raw = VALID_PROJECT.replace(b"orcest.dev/v1", b"orcest.dev/v2")
    with pytest.raises(BundleValidationError) as excinfo:
        parse_project_document(raw)
    assert "LITERAL_MISMATCH" in _diag_codes(excinfo.value)


def test_project_unknown_field_rejected() -> None:
    raw = VALID_PROJECT + b"extra: true\n"
    with pytest.raises(BundleValidationError) as excinfo:
        parse_project_document(raw)
    assert "UNKNOWN_FIELD" in _diag_codes(excinfo.value)


def test_project_duplicate_key_rejected() -> None:
    raw = b"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
  workflow: .orcest/workflows/other.yaml
"""
    with pytest.raises(YamlParseError) as excinfo:
        parse_project_document(raw)
    assert excinfo.value.code == "YAML_INVALID"


def test_project_alias_rejected() -> None:
    raw = b"""
apiVersion: orcest.dev/v1
kind: Project
spec: &s
  workflow: .orcest/workflows/implementation.yaml
extra: *s
"""
    with pytest.raises(YamlParseError):
        parse_project_document(raw)


def test_project_merge_key_rejected() -> None:
    raw = b"""
base: &b
  changePolicy: pin
apiVersion: orcest.dev/v1
kind: Project
spec:
  <<: *b
  workflow: .orcest/workflows/implementation.yaml
"""
    with pytest.raises(YamlParseError):
        parse_project_document(raw)


def test_project_float_rejected() -> None:
    raw = b"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
  base:
    changePolicy: 1.5
"""
    with pytest.raises(YamlParseError):
        parse_project_document(raw)


def test_project_null_where_not_declared_rejected() -> None:
    raw = b"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: null
"""
    with pytest.raises(BundleValidationError) as excinfo:
        parse_project_document(raw)
    assert "TYPE_INVALID" in _diag_codes(excinfo.value) or "NULL_NOT_ALLOWED" in _diag_codes(
        excinfo.value
    )


@pytest.mark.parametrize(
    "bad_workflow_path",
    [
        "/absolute/path.yaml",
        "../escape.yaml",
        ".orcest/../escape.yaml",
        "outside/project.yaml",
        "",
    ],
)
def test_project_path_traversal_rejected(bad_workflow_path: str) -> None:
    raw = f"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: {bad_workflow_path!r}
""".encode()
    with pytest.raises((BundleValidationError, YamlParseError)):
        parse_project_document(raw)


def test_project_ready_working_label_collision_rejected() -> None:
    raw = b"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
  intake:
    readyLabel: same-label
    workingLabel: same-label
"""
    with pytest.raises(BundleValidationError) as excinfo:
        parse_project_document(raw)
    assert "READY_WORKING_LABEL_COLLISION" in _diag_codes(excinfo.value)


def test_project_secret_looking_value_rejected() -> None:
    raw = b"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
  intake:
    readyLabel: ghp_abcdefghijklmnopqrstuvwxyz012345
"""
    with pytest.raises(BundleValidationError) as excinfo:
        parse_project_document(raw)
    assert "SECRET_VALUE_REJECTED" in _diag_codes(excinfo.value)


def test_project_invalid_change_policy_rejected() -> None:
    raw = b"""
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
  base:
    changePolicy: nonsense
"""
    with pytest.raises(BundleValidationError) as excinfo:
        parse_project_document(raw)
    assert "ENUM_INVALID" in _diag_codes(excinfo.value)


# --- workflow.yaml -------------------------------------------------------------


def test_workflow_materializes_defaults() -> None:
    parsed = parse_workflow_document(VALID_WORKFLOW, file=".orcest/workflows/implementation.yaml")
    spec = parsed.materialized["spec"]
    assert spec["implementation"]["timeoutSeconds"] == 7200
    assert spec["implementation"]["alternateProfiles"] == []
    assert spec["verification"]["profile"] == "default"
    assert spec["verification"]["maxRepairCyclesBeforeDiagnosis"] == 4
    assert spec["verification"]["repair"]["timeoutSeconds"] == 7200
    assert spec["review"]["approvalsRequired"] == 2
    assert spec["review"]["requireDistinctProviderFamily"] is False
    assert spec["publication"] == {"requiredChecks": [], "externalHeadPolicy": "verify-and-adopt"}
    assert spec["recovery"] == {
        "maxAttemptsPerActivityBeforeDiagnosis": 3,
        "maxDiagnosesBeforeReplan": 2,
        "maxProviderRateLimitWaitMs": 86_400_000,
    }
    assert parsed.referenced_prompt_paths == [
        ".orcest/prompts/implement.md",
        ".orcest/prompts/repair.md",
        ".orcest/prompts/review-correctness.md",
        ".orcest/prompts/review-security.md",
        ".orcest/prompts/adjudicate.md",
    ]


def test_workflow_repair_defaults_to_implementation_values() -> None:
    raw = VALID_WORKFLOW.replace(
        b"    profile: codex-default\n    prompt: .orcest/prompts/implement.md\n",
        b"    profile: codex-default\n    prompt: .orcest/prompts/implement.md\n"
        b"    timeoutSeconds: 9000\n    alternateProfiles: [claude-default]\n",
    )
    parsed = parse_workflow_document(raw, file=".orcest/workflows/implementation.yaml")
    repair = parsed.materialized["spec"]["verification"]["repair"]
    assert repair["timeoutSeconds"] == 9000
    assert repair["alternateProfiles"] == ["claude-default"]


def test_workflow_name_pattern_rejected() -> None:
    raw = VALID_WORKFLOW.replace(b"name: implementation", b"name: Not_Valid")
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    assert "PATTERN_MISMATCH" in _diag_codes(excinfo.value)


def test_workflow_duplicate_command_id_rejected() -> None:
    raw = VALID_WORKFLOW.replace(
        b"    commands:\n      - id: unit\n        argv: [make, test]\n",
        b"    commands:\n      - id: unit\n        argv: [make, test]\n"
        b"      - id: unit\n        argv: [make, lint]\n",
    )
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    assert "DUPLICATE_VALUE" in _diag_codes(excinfo.value)


def test_workflow_slot_count_must_match_approvals_required() -> None:
    raw = VALID_WORKFLOW.replace(
        b"  review:\n    slots:",
        b"  review:\n    approvalsRequired: 3\n    slots:",
    )
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    assert "REVIEW_SLOT_COUNT_MISMATCH" in _diag_codes(excinfo.value)


def test_workflow_approvals_required_below_minimum_rejected() -> None:
    raw = VALID_WORKFLOW.replace(
        b"  review:\n    slots:",
        b"  review:\n    approvalsRequired: 1\n    slots:",
    )
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    # approvalsRequired falls back to the default (2) on RANGE_INVALID, which
    # then collides with the two configured slots being reported valid --
    # the range violation itself must still surface.
    assert "RANGE_INVALID" in _diag_codes(excinfo.value)


def test_workflow_verify_command_requires_nonempty_argv() -> None:
    raw = VALID_WORKFLOW.replace(b"argv: [make, test]", b"argv: []")
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    assert "TYPE_INVALID" in _diag_codes(excinfo.value)


def test_workflow_implementation_timeout_below_minimum_rejected() -> None:
    raw = VALID_WORKFLOW.replace(
        b"    prompt: .orcest/prompts/implement.md\n",
        b"    prompt: .orcest/prompts/implement.md\n    timeoutSeconds: 5\n",
    )
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    assert "RANGE_INVALID" in _diag_codes(excinfo.value)


def test_workflow_environment_secret_key_name_rejected() -> None:
    raw = VALID_WORKFLOW.replace(
        b"        argv: [make, test]\n",
        b"        argv: [make, test]\n        environment:\n          API_TOKEN: whatever\n",
    )
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    assert "SECRET_KEY_NAME_REJECTED" in _diag_codes(excinfo.value)


def test_workflow_external_head_policy_only_accepts_sole_value() -> None:
    raw = VALID_WORKFLOW + b"  publication:\n    externalHeadPolicy: force-overwrite\n"
    with pytest.raises(BundleValidationError) as excinfo:
        parse_workflow_document(raw, file="wf.yaml")
    assert "ENUM_INVALID" in _diag_codes(excinfo.value)


def test_workflow_publication_and_recovery_omittable() -> None:
    parsed = parse_workflow_document(VALID_WORKFLOW, file="wf.yaml")
    assert parsed.materialized["spec"]["publication"]["externalHeadPolicy"] == "verify-and-adopt"
    assert parsed.materialized["spec"]["recovery"]["maxDiagnosesBeforeReplan"] == 2
