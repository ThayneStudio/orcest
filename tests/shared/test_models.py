"""Unit tests for Task and TaskResult dataclasses."""

from orcest.shared.models import (
    ResultStatus,
    Task,
    TaskResult,
    TaskType,
)


def _make_task(**overrides) -> Task:
    """Helper to build a Task with sensible defaults."""
    defaults = {
        "task_type": TaskType.FIX_CI,
        "repo": "acme/widget",
        "token": "ghp_fake123",
        "resource_type": "pr",
        "resource_id": 42,
        "prompt": "Fix the failing lint check",
        "branch": "fix/lint",
    }
    defaults.update(overrides)
    return Task.create(**defaults)


def _make_task_result(**overrides) -> TaskResult:
    """Helper to build a TaskResult with sensible defaults."""
    defaults = {
        "task_id": "aaa-bbb-ccc",
        "worker_id": "worker-7",
        "status": ResultStatus.COMPLETED,
        "branch": "fix/lint",
        "summary": "Fixed 3 ruff errors",
        "duration_seconds": 120,
        "resource_type": "pr",
        "resource_id": 42,
    }
    defaults.update(overrides)
    return TaskResult(**defaults)


def test_task_create_generates_unique_ids():
    t1 = _make_task()
    t2 = _make_task()
    assert t1.id != t2.id


def test_task_to_dict_values_are_strings():
    task = _make_task()
    d = task.to_dict()
    for key, value in d.items():
        assert isinstance(value, str), f"to_dict()[{key!r}] is {type(value).__name__}, expected str"


def test_task_to_dict_from_dict_round_trip():
    original = _make_task()
    rebuilt = Task.from_dict(original.to_dict())

    assert rebuilt.id == original.id
    assert rebuilt.type == original.type
    assert rebuilt.repo == original.repo
    assert rebuilt.token == original.token
    assert rebuilt.resource_type == original.resource_type
    assert rebuilt.resource_id == original.resource_id
    assert rebuilt.prompt == original.prompt
    assert rebuilt.branch == original.branch
    # Datetime round-trip through isoformat loses sub-microsecond
    # precision but should be equal within a second.
    assert abs((rebuilt.created_at - original.created_at).total_seconds()) < 1


def test_task_none_branch_serializes_to_empty_string():
    task = _make_task(branch=None)
    d = task.to_dict()
    assert d["branch"] == ""

    rebuilt = Task.from_dict(d)
    # from_dict uses `data["branch"] or None`, so empty string -> None
    assert rebuilt.branch is None


def test_task_type_enum_round_trip():
    for member in TaskType:
        assert TaskType(member.value) is member


def test_task_result_to_dict_from_dict_round_trip():
    original = _make_task_result()
    rebuilt = TaskResult.from_dict(original.to_dict())

    assert rebuilt.task_id == original.task_id
    assert rebuilt.worker_id == original.worker_id
    assert rebuilt.status == original.status
    assert rebuilt.branch == original.branch
    assert rebuilt.summary == original.summary
    assert rebuilt.duration_seconds == original.duration_seconds
    assert rebuilt.resource_type == original.resource_type
    assert rebuilt.resource_id == original.resource_id


def test_result_status_enum_round_trip():
    for member in ResultStatus:
        assert ResultStatus(member.value) is member
