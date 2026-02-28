"""End-to-end task lifecycle tests using real Redis streams."""

from __future__ import annotations

import pytest

from orcest.shared.models import (
    ResultStatus,
    Task,
    TaskResult,
    TaskType,
)
from orcest.shared.redis_client import RedisClient


@pytest.mark.integration
class TestTaskFlow:
    """Verify the full publish-consume-ack cycle on real Redis."""

    def test_full_task_lifecycle(self, real_redis_client: RedisClient) -> None:
        """Round-trip: task → stream → consumer → result → stream → ack."""
        rc = real_redis_client

        # 1. Create consumer groups
        rc.ensure_consumer_group("tasks", "workers")
        rc.ensure_consumer_group("results", "orchestrator")

        # 2. Create and publish a task
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="fake",
            resource_type="pr",
            resource_id=1,
            prompt="fix",
            branch="main",
        )
        task_entry_id = rc.xadd("tasks", task.to_dict())
        assert task_entry_id  # non-empty string

        # 3. Consume the task
        entries = rc.xreadgroup(
            group="workers",
            consumer="worker-1",
            stream="tasks",
            count=1,
            block_ms=None,
        )
        assert len(entries) == 1
        entry_id, fields = entries[0]
        assert entry_id == task_entry_id

        # 4. Verify deserialized task fields
        consumed_task = Task.from_dict(fields)
        assert consumed_task.id == task.id
        assert consumed_task.type == TaskType.FIX_CI
        assert consumed_task.repo == "owner/repo"
        assert consumed_task.token == "fake"
        assert consumed_task.resource_type == "pr"
        assert consumed_task.resource_id == 1
        assert consumed_task.prompt == "fix"
        assert consumed_task.branch == "main"

        # 5. Create and publish a result
        result = TaskResult(
            task_id=task.id,
            worker_id="worker-1",
            status=ResultStatus.COMPLETED,
            resource_type="pr",
            resource_id=1,
            branch="main",
            summary="done",
            duration_seconds=5,
        )
        result_entry_id = rc.xadd("results", result.to_dict())
        assert result_entry_id

        # 6. Ack the task entry
        ack_count = rc.xack("tasks", "workers", entry_id)
        assert ack_count == 1

        # 7. Consume the result
        result_entries = rc.xreadgroup(
            group="orchestrator",
            consumer="orch-main",
            stream="results",
            count=1,
            block_ms=None,
        )
        assert len(result_entries) == 1
        r_entry_id, r_fields = result_entries[0]
        assert r_entry_id == result_entry_id

        # 8. Verify deserialized result fields
        consumed_result = TaskResult.from_dict(r_fields)
        assert consumed_result.task_id == task.id
        assert consumed_result.worker_id == "worker-1"
        assert consumed_result.status == ResultStatus.COMPLETED
        assert consumed_result.summary == "done"
        assert consumed_result.duration_seconds == 5
        assert consumed_result.resource_type == "pr"
        assert consumed_result.resource_id == 1
        assert consumed_result.branch == "main"

        # 9. Ack the result entry
        r_ack = rc.xack("results", "orchestrator", r_entry_id)
        assert r_ack == 1

    def test_multiple_tasks_fifo(self, real_redis_client: RedisClient) -> None:
        """Tasks consumed in the order they were published."""
        rc = real_redis_client
        rc.ensure_consumer_group("tasks", "workers")

        # Publish 5 tasks with sequential PR numbers
        published_ids: list[str] = []
        for i in range(1, 6):
            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/repo",
                token="fake",
                resource_type="pr",
                resource_id=i,
                prompt=f"fix pr {i}",
                branch="main",
            )
            entry_id = rc.xadd("tasks", task.to_dict())
            published_ids.append(entry_id)

        # Consume all 5
        entries = rc.xreadgroup(
            group="workers",
            consumer="worker-1",
            stream="tasks",
            count=5,
            block_ms=None,
        )
        assert len(entries) == 5

        consumed_ids = [eid for eid, _ in entries]
        # Stream IDs are monotonic; FIFO order must match publish order
        assert consumed_ids == published_ids

    def test_consumer_group_distributes(self, real_redis_client: RedisClient) -> None:
        """Two consumers in the same group split the work."""
        rc = real_redis_client
        rc.ensure_consumer_group("tasks", "workers")

        # Publish 10 tasks
        for i in range(10):
            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/repo",
                token="fake",
                resource_type="pr",
                resource_id=i,
                prompt=f"task {i}",
                branch="main",
            )
            rc.xadd("tasks", task.to_dict())

        # Consumer A reads 5
        entries_a = rc.xreadgroup(
            group="workers",
            consumer="consumer-a",
            stream="tasks",
            count=5,
            block_ms=None,
        )
        assert len(entries_a) == 5

        # Consumer B reads remaining (ask for up to 10)
        entries_b = rc.xreadgroup(
            group="workers",
            consumer="consumer-b",
            stream="tasks",
            count=10,
            block_ms=None,
        )
        assert len(entries_b) == 5

        # No overlap -- all entry IDs are distinct
        ids_a = {eid for eid, _ in entries_a}
        ids_b = {eid for eid, _ in entries_b}
        assert ids_a.isdisjoint(ids_b)

        # All 10 tasks accounted for
        assert len(ids_a | ids_b) == 10

    def test_unacked_task_in_pending(self, real_redis_client: RedisClient) -> None:
        """An unacknowledged entry stays in the pending list."""
        rc = real_redis_client
        rc.ensure_consumer_group("tasks", "workers")

        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="fake",
            resource_type="pr",
            resource_id=99,
            prompt="pending test",
            branch="main",
        )
        rc.xadd("tasks", task.to_dict())

        # Consume but do NOT ack
        entries = rc.xreadgroup(
            group="workers",
            consumer="worker-1",
            stream="tasks",
            count=1,
            block_ms=None,
        )
        assert len(entries) == 1

        # xpending returns a dict with 'pending' count (among others)
        pending_info = rc.client.xpending("tasks", "workers")
        assert pending_info["pending"] == 1
