"""Integration test: real worker loop with NoopRunner proves no overlap.

Uses the actual run_worker code with NoopRunner (runner.type="noop")
and backend="claude" to exercise the real lock -> heartbeat -> execute ->
release -> ack lifecycle.

Two test approaches are used:
1. _execute_task with real NoopRunner and real Redis locks (reliable, direct)
2. run_worker in threads with mocked signal/logging/workspace (full lifecycle)
"""

from __future__ import annotations

import json
import logging
import threading
import time
import unittest.mock
from pathlib import Path

import pytest

from orcest.shared.config import RedisConfig, RunnerConfig, WorkerConfig
from orcest.shared.coordination import RedisLock, make_pr_lock_key
from orcest.shared.credential_handoff import (
    _MIGRATE_V1_INTENT_LUA,
    ATOMIC_CREDENTIAL_TERMINAL_LUA,
    CredentialCheckpointStatus,
    CredentialRecoveryOutcome,
    credential_intent_key,
    handoff_marker_key,
    handoff_payload_fingerprint,
    load_credential_checkpoint,
    recover_credential_checkpoint,
    store_credential_checkpoint,
    version_credential_checkpoint,
)
from orcest.shared.models import ResultStatus, Task, TaskResult, TaskType
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat
from orcest.worker.loop import (
    CONSUMER_GROUP,
    RESULTS_STREAM,
    ResultPublishOutcome,
    _credential_checkpoint_key,
    _execute_task,
    _publish_result_with_retry,
    run_worker,
)
from orcest.worker.noop_runner import NoopRunner
from orcest.worker.workspace import Workspace


@pytest.fixture(autouse=True)
def _mock_worker_pr_snapshot_lookup(monkeypatch):
    """Keep integration isolation tests focused on locks, not live GitHub state."""
    monkeypatch.setattr(
        "orcest.worker.loop.gh.get_pr",
        lambda repo, pr_number, token: {"headRefOid": "abc123", "statusCheckRollup": []},
    )


@pytest.mark.integration
class TestWorkerIsolation:
    """Prove workers using the real loop can't overlap on the same PR."""

    # ------------------------------------------------------------------
    # Approach 1: Direct _execute_task with real locks and real NoopRunner
    # ------------------------------------------------------------------

    def test_credential_terminal_handoff_is_idempotent_after_lost_eval_response(
        self,
        real_redis_client: RedisClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A post-execution connection loss cannot append the secret twice."""
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        fq_tasks_stream = tasks_stream
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="github-token",
            resource_type="pr",
            resource_id=42,
            prompt="fix",
        )
        redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)
        entry_id = redis.xadd(tasks_stream, task.to_dict())
        claimed = redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer="credential-worker",
            stream=tasks_stream,
            count=1,
            block_ms=None,
        )
        assert claimed and claimed[0][0] == entry_id
        result = TaskResult(
            task_id=task.id,
            worker_id="credential-worker",
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="done",
            duration_seconds=1,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"secret"}',
            credential_update_minted_at=123,
        )
        original_eval = redis.client.eval
        lost = False

        def lose_first_terminal_response(script: str, *args: object) -> object:
            nonlocal lost
            response = original_eval(script, *args)
            if script == ATOMIC_CREDENTIAL_TERMINAL_LUA and not lost:
                lost = True
                raise ConnectionError("response lost after Redis executed EVAL")
            return response

        monkeypatch.setattr(redis.client, "eval", lose_first_terminal_response)

        outcome = _publish_result_with_retry(
            redis,
            result,
            task,
            logging.getLogger("test.credential-response-loss"),
            fq_tasks_stream,
            entry_id,
            abort_event=unittest.mock.MagicMock(wait=unittest.mock.MagicMock(return_value=False)),
        )

        assert outcome is ResultPublishOutcome.PUBLISHED
        rows = redis.client.xrevrange(RESULTS_STREAM, count=10)
        assert len(rows) == 1
        assert TaskResult.from_dict(rows[0][1]).credential_update == ('{"refresh_token":"secret"}')
        checkpoint_key = _credential_checkpoint_key(
            RESULTS_STREAM,
            fq_tasks_stream,
            entry_id,
            task.id,
        )
        assert redis.client.exists(checkpoint_key) == 0
        assert redis.client.xpending(fq_tasks_stream, CONSUMER_GROUP)["pending"] == 0
        terminal_receipt = redis.client.get(
            handoff_marker_key(RESULTS_STREAM, fq_tasks_stream, entry_id, task.id)
        )
        assert isinstance(terminal_receipt, str)
        assert terminal_receipt.startswith("terminal|")

    def test_missing_checkpoint_before_terminal_eval_fails_closed(
        self,
        real_redis_client: RedisClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """External checkpoint deletion is not positive publication proof."""
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="github-token",
            resource_type="pr",
            resource_id=43,
            prompt="fix",
        )
        redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)
        entry_id = redis.xadd(tasks_stream, task.to_dict())
        assert redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer="credential-worker",
            stream=tasks_stream,
            count=1,
            block_ms=None,
        )
        result = TaskResult(
            task_id=task.id,
            worker_id="credential-worker",
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="done",
            duration_seconds=1,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"secret"}',
            credential_update_minted_at=123,
        )
        original_eval = redis.client.eval
        deleted_checkpoint_key = ""

        def delete_checkpoint_before_terminal_eval(script: str, *args: object) -> object:
            nonlocal deleted_checkpoint_key
            if script == ATOMIC_CREDENTIAL_TERMINAL_LUA:
                deleted_checkpoint_key = str(args[4])
                redis.client.delete(deleted_checkpoint_key)
            return original_eval(script, *args)

        monkeypatch.setattr(redis.client, "eval", delete_checkpoint_before_terminal_eval)
        outcome = _publish_result_with_retry(
            redis,
            result,
            task,
            logging.getLogger("test.credential-external-delete"),
            tasks_stream,
            entry_id,
            abort_event=unittest.mock.MagicMock(wait=unittest.mock.MagicMock(return_value=False)),
        )

        assert outcome is ResultPublishOutcome.BLOCKED
        assert deleted_checkpoint_key
        assert redis.client.xlen(RESULTS_STREAM) == 0
        assert redis.client.xpending(tasks_stream, CONSUMER_GROUP)["pending"] == 1
        assert (
            redis.client.exists(
                credential_intent_key(RESULTS_STREAM, tasks_stream, entry_id, task.id)
            )
            == 1
        )

    def test_stale_version_actor_cannot_resurrect_or_overwrite_checkpoint(
        self,
        real_redis_client: RedisClient,
    ) -> None:
        """Version rewrites are exact CAS operations across worker/reaper actors."""
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        entry_id = "1-0"
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="github-token",
            resource_type="pr",
            resource_id=44,
            prompt="fix",
        )
        result = TaskResult(
            task_id=task.id,
            worker_id="credential-worker",
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="done",
            duration_seconds=1,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"secret"}',
            credential_update_minted_at=0,
        )
        initial = store_credential_checkpoint(
            redis,
            RESULTS_STREAM,
            tasks_stream,
            entry_id,
            task.id,
            result.to_dict(),
        )
        conflicting_fields = {**result.to_dict(), "summary": "stale actor overwrite"}
        with pytest.raises(RuntimeError, match="already exists"):
            store_credential_checkpoint(
                redis,
                RESULTS_STREAM,
                tasks_stream,
                entry_id,
                task.id,
                conflicting_fields,
            )
        assert redis.client.get(initial.key) == initial.serialized
        status, actor_a = load_credential_checkpoint(
            redis,
            RESULTS_STREAM,
            tasks_stream,
            entry_id,
            task,
            logging.getLogger("test.credential-cas"),
        )
        assert status is CredentialCheckpointStatus.VALID
        assert actor_a is not None
        actor_b = actor_a
        winner = version_credential_checkpoint(
            redis, actor_b, RESULTS_STREAM, tasks_stream, entry_id, task.id
        )

        with pytest.raises(RuntimeError, match="changed during versioning"):
            version_credential_checkpoint(
                redis, actor_a, RESULTS_STREAM, tasks_stream, entry_id, task.id
            )
        assert redis.client.get(winner.key) == winner.serialized

        redis.client.delete(winner.key)
        with pytest.raises(RuntimeError, match="changed during versioning"):
            version_credential_checkpoint(
                redis, actor_a, RESULTS_STREAM, tasks_stream, entry_id, task.id
            )
        assert redis.client.exists(winner.key) == 0

    @pytest.mark.parametrize("preexisting_intent", [False, True])
    def test_valid_v1_checkpoint_migration_persists_state_then_recovers(
        self,
        real_redis_client: RedisClient,
        preexisting_intent: bool,
    ) -> None:
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="github-token",
            resource_type="pr",
            resource_id=45,
            prompt="fix",
        )
        redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)
        entry_id = redis.xadd(tasks_stream, task.to_dict())
        assert redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer="legacy-worker",
            stream=tasks_stream,
            count=1,
            block_ms=None,
        )
        fields = TaskResult(
            task_id=task.id,
            worker_id="legacy-worker",
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="legacy rotation",
            duration_seconds=1,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"legacy-secret"}',
            credential_update_minted_at=123,
        ).to_dict()
        checkpoint_key = _credential_checkpoint_key(RESULTS_STREAM, tasks_stream, entry_id, task.id)
        legacy = json.dumps(
            {
                "version": 1,
                "target_stream": RESULTS_STREAM,
                "tasks_stream": tasks_stream,
                "entry_id": entry_id,
                "task_id": task.id,
                "result_fingerprint": handoff_payload_fingerprint(fields),
                "result": fields,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        redis.client.set(checkpoint_key, legacy, ex=60)
        intent_key = credential_intent_key(RESULTS_STREAM, tasks_stream, entry_id, task.id)
        if preexisting_intent:
            redis.client.set(intent_key, "1", ex=60)

        status, checkpoint = load_credential_checkpoint(
            redis,
            RESULTS_STREAM,
            tasks_stream,
            entry_id,
            task,
            logging.getLogger("test.credential-v1"),
        )

        assert status is CredentialCheckpointStatus.VALID
        assert checkpoint is not None and checkpoint.serialized == legacy
        assert redis.client.get(intent_key) == "1"
        assert redis.client.ttl(checkpoint_key) == -1
        assert redis.client.ttl(intent_key) == -1

        recovered = recover_credential_checkpoint(
            redis,
            task,
            RESULTS_STREAM,
            tasks_stream,
            entry_id,
            "dead-letter",
            logging.getLogger("test.credential-v1-recover"),
            maxlen=20000,
        )
        assert recovered is CredentialRecoveryOutcome.RECOVERED
        assert redis.client.exists(checkpoint_key) == 0
        assert redis.client.exists(intent_key) == 0
        assert redis.client.xpending(tasks_stream, CONSUMER_GROUP)["pending"] == 0
        assert redis.client.xlen(RESULTS_STREAM) == 1

    @pytest.mark.parametrize("mutation", ["missing", "changed", "invalid_intent"])
    def test_v1_intent_migration_fails_closed_on_concurrent_state_change(
        self,
        real_redis_client: RedisClient,
        monkeypatch: pytest.MonkeyPatch,
        mutation: str,
    ) -> None:
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        entry_id = "1-0"
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="github-token",
            resource_type="pr",
            resource_id=46,
            prompt="fix",
        )
        fields = TaskResult(
            task_id=task.id,
            worker_id="legacy-worker",
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="legacy rotation",
            duration_seconds=1,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"legacy-secret"}',
            credential_update_minted_at=123,
        ).to_dict()
        checkpoint_key = _credential_checkpoint_key(RESULTS_STREAM, tasks_stream, entry_id, task.id)
        legacy = json.dumps(
            {
                "version": 1,
                "target_stream": RESULTS_STREAM,
                "tasks_stream": tasks_stream,
                "entry_id": entry_id,
                "task_id": task.id,
                "result_fingerprint": handoff_payload_fingerprint(fields),
                "result": fields,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        redis.client.set(checkpoint_key, legacy)
        original_eval = redis.client.eval

        def mutate_before_migration(script: str, *args: object) -> object:
            if script == _MIGRATE_V1_INTENT_LUA:
                if mutation == "missing":
                    redis.client.delete(checkpoint_key)
                elif mutation == "changed":
                    redis.client.set(checkpoint_key, "concurrent replacement")
                else:
                    redis.client.set(
                        credential_intent_key(RESULTS_STREAM, tasks_stream, entry_id, task.id),
                        "invalid",
                    )
            return original_eval(script, *args)

        monkeypatch.setattr(redis.client, "eval", mutate_before_migration)

        status, checkpoint = load_credential_checkpoint(
            redis,
            RESULTS_STREAM,
            tasks_stream,
            entry_id,
            task,
            logging.getLogger("test.credential-v1-race"),
        )

        assert status is CredentialCheckpointStatus.BLOCKED
        assert checkpoint is None
        intent = redis.client.get(
            credential_intent_key(RESULTS_STREAM, tasks_stream, entry_id, task.id)
        )
        assert intent == ("invalid" if mutation == "invalid_intent" else None)

    def test_execute_task_no_overlap_same_pr(
        self,
        real_redis_client: RedisClient,
    ) -> None:
        """Multiple threads running _execute_task for the same PR are
        serialized by the Redis lock — no concurrent execution detected.

        This exercises the real NoopRunner, real RedisLock, real Heartbeat,
        and the real _execute_task function from the worker loop.
        """
        redis = real_redis_client

        # Concurrency tracking
        active_count = {"value": 0, "max": 0}
        count_lock = threading.Lock()

        # Create a NoopRunner with instrumented sleep to track concurrency
        noop_duration = 0.15
        runner = NoopRunner(duration=noop_duration)
        original_run = runner.run

        def instrumented_run(
            prompt,
            work_dir,
            token,
            timeout,
            logger=None,
            on_output=None,
            on_stderr=None,
            abort_event=None,
            claude_token="",
            provider="claude",
            credential="",
            model="",
        ):
            """Wrap NoopRunner.run to track concurrent executions."""
            with count_lock:
                active_count["value"] += 1
                active_count["max"] = max(active_count["max"], active_count["value"])
            try:
                return original_run(
                    prompt=prompt,
                    work_dir=work_dir,
                    token=token,
                    timeout=timeout,
                    logger=logger,
                    on_output=on_output,
                    on_stderr=on_stderr,
                    abort_event=abort_event,
                    claude_token=claude_token,
                    provider=provider,
                    credential=credential,
                    model=model,
                )
            finally:
                with count_lock:
                    active_count["value"] -= 1

        runner.run = instrumented_run  # type: ignore[assignment]

        config = WorkerConfig(
            worker_id="isolation-test",
            workspace_dir="/tmp/orcest-test-isolation",
            runner=RunnerConfig(type="noop", timeout=10, extra={"duration": "0.15"}),
        )
        test_logger = logging.getLogger("test.isolation.execute")

        num_tasks = 10
        num_threads = 5
        pr_number = 42  # All tasks target the same PR

        tasks = [
            Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                provider="noop",
                resource_type="pr",
                resource_id=pr_number,
                prompt=f"Task {i}",
                branch="fix-branch",
                snapshot_head_sha="abc123",
            )
            for i in range(num_tasks)
        ]

        results = []
        results_lock = threading.Lock()
        errors = []

        def worker_fn(worker_id: str, task: Task) -> None:
            """Acquire lock, run _execute_task, release lock."""
            # Each thread gets its own mock workspace to avoid thread-safety
            # issues with MagicMock (its internal call tracking is not
            # thread-safe).
            mock_workspace = unittest.mock.MagicMock(spec=Workspace)
            mock_workspace.setup.return_value = Path("/tmp/fake-workspace")
            mock_workspace.current_head_sha.return_value = "abc123"

            lock_key = make_pr_lock_key(task.repo, task.resource_id)
            lock = RedisLock(redis, lock_key, ttl=30, owner=worker_id)

            if not lock.acquire():
                # Another worker holds the lock — skip (expected behavior)
                return

            heartbeat = Heartbeat(lock, interval=5)
            heartbeat.start()
            try:
                result = _execute_task(task, config, runner, mock_workspace, redis, test_logger)
                with results_lock:
                    results.append((worker_id, task.id, result))
            except Exception as e:
                errors.append(f"{worker_id}: {e}")
            finally:
                heartbeat.stop()
                lock.release()

        # Run threads: each thread picks a task and tries to lock + execute
        barrier = threading.Barrier(num_threads)

        def contending_worker(worker_id: str, assigned_tasks: list[Task]) -> None:
            """Worker that processes its assigned tasks sequentially."""
            try:
                barrier.wait(timeout=10)
            except threading.BrokenBarrierError:
                errors.append(f"{worker_id}: barrier broken (thread start synchronization failed)")
                return
            for task in assigned_tasks:
                worker_fn(worker_id, task)

        # Distribute tasks round-robin to workers
        worker_tasks: dict[str, list[Task]] = {f"w-{i}": [] for i in range(num_threads)}
        for i, task in enumerate(tasks):
            worker_id = f"w-{i % num_threads}"
            worker_tasks[worker_id].append(task)

        threads = [
            threading.Thread(
                target=contending_worker,
                args=(wid, wtasks),
                name=wid,
            )
            for wid, wtasks in worker_tasks.items()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        hung = [t.name for t in threads if t.is_alive()]
        assert not hung, f"Threads still alive after join timeout: {hung}"

        # Assertions
        assert errors == [], f"Worker errors: {errors}"
        assert active_count["max"] <= 1, (
            f"Concurrent executions on same PR detected! max={active_count['max']}, "
            f"expected at most 1"
        )
        # At least some tasks should have been processed
        assert len(results) >= 1, "No tasks were processed"

    def test_execute_task_different_prs_can_overlap(
        self,
        real_redis_client: RedisClient,
    ) -> None:
        """Tasks for different PRs CAN execute concurrently.

        This is the complement of the isolation test — different PRs should
        NOT block each other.
        """
        redis = real_redis_client

        active_count = {"value": 0, "max": 0}
        count_lock = threading.Lock()

        noop_duration = 0.3
        runner = NoopRunner(duration=noop_duration)
        original_run = runner.run

        def instrumented_run(
            prompt,
            work_dir,
            token,
            timeout,
            logger=None,
            on_output=None,
            on_stderr=None,
            abort_event=None,
            claude_token="",
            provider="claude",
            credential="",
            model="",
        ):
            with count_lock:
                active_count["value"] += 1
                active_count["max"] = max(active_count["max"], active_count["value"])
            try:
                return original_run(
                    prompt=prompt,
                    work_dir=work_dir,
                    token=token,
                    timeout=timeout,
                    logger=logger,
                    on_output=on_output,
                    on_stderr=on_stderr,
                    abort_event=abort_event,
                    claude_token=claude_token,
                    provider=provider,
                    credential=credential,
                    model=model,
                )
            finally:
                with count_lock:
                    active_count["value"] -= 1

        runner.run = instrumented_run  # type: ignore[assignment]

        config = WorkerConfig(
            worker_id="isolation-test",
            workspace_dir="/tmp/orcest-test-isolation",
            runner=RunnerConfig(type="noop", timeout=10, extra={"duration": "0.3"}),
        )
        test_logger = logging.getLogger("test.isolation.overlap")

        num_threads = 3
        errors = []
        barrier = threading.Barrier(num_threads)

        def worker_fn(worker_id: str, pr_number: int) -> None:
            # Each thread gets its own mock workspace to avoid thread-safety
            # issues with MagicMock (its internal call tracking is not
            # thread-safe).
            mock_workspace = unittest.mock.MagicMock(spec=Workspace)
            mock_workspace.setup.return_value = Path("/tmp/fake-workspace")
            mock_workspace.current_head_sha.return_value = "abc123"

            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                provider="noop",
                resource_type="pr",
                resource_id=pr_number,  # Different PR per worker!
                prompt=f"Task for PR {pr_number}",
                branch=f"fix-pr-{pr_number}",
                snapshot_head_sha="abc123",
            )
            lock_key = make_pr_lock_key(task.repo, task.resource_id)
            lock = RedisLock(redis, lock_key, ttl=30, owner=worker_id)

            if not lock.acquire():
                errors.append(f"{worker_id}: couldn't acquire lock for PR {pr_number}")
                return

            heartbeat = Heartbeat(lock, interval=5)
            heartbeat.start()
            try:
                barrier.wait(timeout=5)  # Synchronize start
                _execute_task(task, config, runner, mock_workspace, redis, test_logger)
            except Exception as e:
                errors.append(f"{worker_id}: {e}")
            finally:
                heartbeat.stop()
                lock.release()

        threads = [
            threading.Thread(target=worker_fn, args=(f"w-{i}", i + 1), name=f"w-{i}")
            for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        hung = [t.name for t in threads if t.is_alive()]
        assert not hung, f"Threads still alive after join timeout: {hung}"

        assert errors == [], f"Worker errors: {errors}"
        # With different PRs, workers SHOULD be able to run concurrently
        assert active_count["max"] >= 2, (
            f"Expected concurrent execution on different PRs, but max={active_count['max']}"
        )

    # ------------------------------------------------------------------
    # Approach 2: Full run_worker with real Redis, NoopRunner, mocked I/O
    # ------------------------------------------------------------------

    def test_run_worker_no_concurrent_execution_same_pr(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """3 real workers via run_worker, 5 tasks for same PR — no overlap.

        Uses the actual run_worker function from loop.py with mocked
        Workspace, setup_logging, and signal.signal (which can't be set
        from non-main threads).
        """
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        results_stream = RESULTS_STREAM

        redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)
        redis.ensure_consumer_group(results_stream, "orchestrator")

        # Publish 5 tasks all targeting the same PR
        for i in range(5):
            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                provider="noop",
                resource_type="pr",
                resource_id=1,  # Same PR!
                prompt=f"Task {i}",
                branch="fix-branch",
                snapshot_head_sha="abc123",
            )
            redis.xadd(tasks_stream, task.to_dict())

        # Track concurrent NoopRunner executions.
        active_count = {"value": 0, "max": 0}
        count_lock = threading.Lock()
        original_sleep = time.sleep

        original_noop_run = NoopRunner.run

        def instrumented_noop_run(self, *args, **kwargs):
            with count_lock:
                active_count["value"] += 1
                active_count["max"] = max(active_count["max"], active_count["value"])
            try:
                return original_noop_run(self, *args, **kwargs)
            finally:
                with count_lock:
                    active_count["value"] -= 1

        # Build worker configs
        num_workers = 3
        configs = []
        for i in range(num_workers):
            rc = make_real_redis_client()
            conn_kwargs = rc.client.connection_pool.connection_kwargs
            parsed = {
                "host": conn_kwargs.get("host", "localhost"),
                "port": conn_kwargs.get("port", 6379),
                "db": conn_kwargs.get("db", 15),
                "password": conn_kwargs.get("password"),
            }
            cfg = WorkerConfig(
                redis=RedisConfig(
                    host=parsed["host"],
                    port=parsed["port"],
                    db=parsed["db"],
                    password=parsed["password"],
                    key_prefix="",
                ),
                worker_id=f"test-worker-{i}",
                workspace_dir="/tmp/orcest-test-isolation",
                backend="claude",
                runner=RunnerConfig(
                    type="noop",
                    timeout=10,
                    extra={"duration": "0.1"},
                ),
            )
            configs.append(cfg)

        # Patchers for things that don't work in threads or need mocking.
        # Use side_effect (not return_value) so each thread gets its own
        # MagicMock instance -- MagicMock's internal call tracking is not
        # thread-safe.
        workspace_patcher = unittest.mock.patch("orcest.worker.loop.Workspace")
        mock_ws_cls = workspace_patcher.start()

        def _make_mock_ws(*args, **kwargs):
            ws = unittest.mock.MagicMock(spec=Workspace)
            ws.setup.return_value = Path("/tmp/fake-workspace")
            ws.current_head_sha.return_value = "abc123"
            return ws

        mock_ws_cls.side_effect = _make_mock_ws

        logging_patcher = unittest.mock.patch("orcest.worker.loop.setup_logging")
        mock_logging = logging_patcher.start()
        mock_logging.return_value = logging.getLogger("test.isolation.runworker")

        signal_patcher = unittest.mock.patch("orcest.worker.loop.signal.signal")
        signal_patcher.start()

        noop_run_patcher = unittest.mock.patch(
            "orcest.worker.noop_runner.NoopRunner.run",
            new=instrumented_noop_run,
        )
        noop_run_patcher.start()

        stop_event = threading.Event()

        try:
            # Run workers in threads
            errors: list[str] = []

            def run_with_client(cfg: WorkerConfig) -> None:
                try:
                    run_worker(cfg, stop_event)
                except SystemExit:
                    # run_worker calls sys.exit(1) on Redis health check
                    # failure. SystemExit is a BaseException, not Exception,
                    # so catch it explicitly to avoid silent thread death.
                    errors.append(f"{cfg.worker_id}: sys.exit called (Redis health check failed?)")
                except Exception as e:
                    errors.append(f"{cfg.worker_id}: {e}")

            threads = []
            for cfg in configs:
                t = threading.Thread(
                    target=run_with_client,
                    args=(cfg,),
                    name=cfg.worker_id,
                )
                threads.append(t)

            for t in threads:
                t.start()

            # Wait for tasks to be consumed. Workers that can't acquire the
            # lock will skip (ACK without result), so we just need enough
            # time for all 5 tasks to be read and either processed or skipped.
            # With 0.1s noop sleep, this is very fast.
            #
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                results_count = redis.client.xlen(results_stream)
                if results_count >= 1:
                    # At least one task processed; give others time to skip.
                    original_sleep(1)
                    break
                original_sleep(0.2)
        finally:
            # Signal workers to stop and wait for them to exit so they
            # release Redis connections before the fixture calls flushdb().
            stop_event.set()
            hung_threads = []
            for t in threads:
                t.join(timeout=10)
                if t.is_alive():
                    hung_threads.append(t.name)
            # Cleanup patchers unconditionally before raising
            workspace_patcher.stop()
            logging_patcher.stop()
            signal_patcher.stop()
            noop_run_patcher.stop()
            if hung_threads:
                raise RuntimeError(f"Worker thread(s) {hung_threads!r} did not stop within 10 s")

        # Workers that can't acquire the PR lock correctly skip the task
        # (ACK without processing). This IS the locking mechanism working:
        # only one worker processes a task for a given PR at a time, and
        # redundant tasks for the same PR are discarded.
        results_count = redis.client.xlen(results_stream)
        assert results_count >= 1, f"Expected at least 1 result, got {results_count}"
        assert errors == [], f"Worker errors: {errors}"
        assert active_count["max"] <= 1, (
            f"Concurrent executions detected! max={active_count['max']}"
        )

    # ------------------------------------------------------------------
    # Approach 2b: run_worker reports lock contention without processing
    # ------------------------------------------------------------------

    def test_run_worker_reports_locked_tasks(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """A locked task gets a transient result without duplicate execution.

        The following unlocked task still executes normally, and both source
        entries are ACKed only after their respective results are durable.
        """
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        results_stream = RESULTS_STREAM

        redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)
        redis.ensure_consumer_group(results_stream, "orchestrator")

        pr_number = 77

        # Pre-acquire the lock for this PR (simulating another worker)
        lock_key = make_pr_lock_key("owner/testrepo", pr_number)
        blocker_lock = RedisLock(redis, lock_key, ttl=60, owner="blocker-worker")
        assert blocker_lock.acquire() is True

        # Publish a task for the locked PR
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/testrepo",
            token="fake",
            provider="noop",
            resource_type="pr",
            resource_id=pr_number,
            prompt="This should be skipped",
            branch="fix-branch",
            snapshot_head_sha="abc123",
        )
        redis.xadd(tasks_stream, task.to_dict())

        # Also publish a task for a different PR (should succeed)
        task2 = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/testrepo",
            token="fake",
            provider="noop",
            resource_type="pr",
            resource_id=pr_number + 1,  # Different PR
            prompt="This should succeed",
            branch="fix-branch-2",
            snapshot_head_sha="abc123",
        )
        redis.xadd(tasks_stream, task2.to_dict())

        # Build config for a single worker
        rc = make_real_redis_client()
        conn_kwargs = rc.client.connection_pool.connection_kwargs
        parsed = {
            "host": conn_kwargs.get("host", "localhost"),
            "port": conn_kwargs.get("port", 6379),
            "db": conn_kwargs.get("db", 15),
            "password": conn_kwargs.get("password"),
        }
        cfg = WorkerConfig(
            redis=RedisConfig(
                host=parsed["host"],
                port=parsed["port"],
                db=parsed["db"],
                password=parsed["password"],
                key_prefix="",
            ),
            worker_id="skip-test-worker",
            workspace_dir="/tmp/orcest-test-isolation",
            backend="claude",
            runner=RunnerConfig(
                type="noop",
                timeout=10,
                extra={"duration": "0.01"},
            ),
        )

        workspace_patcher = unittest.mock.patch("orcest.worker.loop.Workspace")
        mock_ws_cls = workspace_patcher.start()

        def _make_mock_ws(*args, **kwargs):
            ws = unittest.mock.MagicMock(spec=Workspace)
            ws.setup.return_value = Path("/tmp/fake-workspace")
            ws.current_head_sha.return_value = "abc123"
            return ws

        mock_ws_cls.side_effect = _make_mock_ws

        logging_patcher = unittest.mock.patch("orcest.worker.loop.setup_logging")
        mock_logging = logging_patcher.start()
        mock_logging.return_value = logging.getLogger("test.isolation.skip")

        signal_patcher = unittest.mock.patch("orcest.worker.loop.signal.signal")
        signal_patcher.start()

        runner_calls: list[str] = []
        original_noop_run = NoopRunner.run

        def counted_noop_run(self, prompt, *args, **kwargs):
            runner_calls.append(prompt)
            return original_noop_run(self, prompt, *args, **kwargs)

        noop_run_patcher = unittest.mock.patch.object(
            NoopRunner,
            "run",
            new=counted_noop_run,
        )
        noop_run_patcher.start()

        stop_event = threading.Event()

        try:
            errors: list[str] = []

            def run_worker_thread(config: WorkerConfig) -> None:
                try:
                    run_worker(config, stop_event)
                except SystemExit:
                    errors.append("sys.exit called (Redis health check failed?)")
                except Exception as e:
                    errors.append(str(e))

            t = threading.Thread(
                target=run_worker_thread,
                args=(cfg,),
                name="skip-test-worker",
            )
            t.start()

            # Wait for both the lock-contention and completed results.
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                results_count = redis.client.xlen(results_stream)
                if results_count >= 2:
                    break
                time.sleep(0.2)
        finally:
            # Signal the worker to stop and wait for it to exit so it releases
            # its Redis connection before the fixture calls flushdb().
            stop_event.set()
            t.join(timeout=10)
            thread_hung = t.is_alive()
            # Cleanup unconditionally before raising — release the blocker
            # lock first so the thread can unblock if it's waiting on it.
            blocker_lock.release()
            workspace_patcher.stop()
            logging_patcher.stop()
            signal_patcher.stop()
            noop_run_patcher.stop()
            if thread_hung:
                raise RuntimeError(f"Worker thread {t.name!r} did not stop within 10 s")

        # Both tasks have a durable result, but only the unlocked task ran.
        results_count = redis.client.xlen(results_stream)
        assert results_count == 2, f"Expected exactly 2 results, got {results_count}"
        results = [fields for _, fields in redis.client.xrange(results_stream)]
        by_task = {fields["task_id"]: fields for fields in results}
        assert by_task[task.id]["status"] == "failed"
        assert by_task[task.id]["summary"].startswith("[transient] ")
        assert by_task[task2.id]["status"] == "completed"
        assert runner_calls == [task2.prompt]

        # Both tasks should be ACKed (no pending entries)
        pending = redis.client.xpending(tasks_stream, CONSUMER_GROUP)
        assert pending["pending"] == 0, (
            f"Expected 0 pending tasks (both ACKed), got {pending['pending']}"
        )

        assert errors == [], f"Worker errors: {errors}"
