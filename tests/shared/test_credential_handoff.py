"""Focused fallback tests for private credential terminal handoff."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, call

import pytest

from orcest.shared.credential_handoff import (
    CREDENTIAL_CHECKPOINT_TTL_SECONDS,
    HANDOFF_FINGERPRINT_FIELD,
    CredentialCheckpoint,
    CredentialCheckpointStatus,
    CredentialRecoveryOutcome,
    CredentialTerminalOutcome,
    _migrate_v1_intent_once,
    credential_checkpoint_key,
    credential_intent_key,
    handoff_marker_key,
    handoff_payload_fingerprint,
    load_credential_checkpoint,
    recover_credential_checkpoint,
    store_credential_checkpoint,
    terminal_credential_handoff_once,
)
from orcest.shared.models import CONSUMER_GROUP, ResultStatus, Task, TaskResult, TaskType


def _task_and_fields() -> tuple[Task, dict[str, str]]:
    task = Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token="github-token",
        resource_type="pr",
        resource_id=42,
        prompt="fix",
    )
    result = TaskResult(
        task_id=task.id,
        worker_id="worker",
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
    return task, result.to_dict()


def test_checkpoint_fallback_treats_none_set_nx_as_collision() -> None:
    redis = MagicMock()
    redis.client.get.return_value = None
    redis.client.set.return_value = None
    task, fields = _task_and_fields()

    with pytest.raises(RuntimeError, match="creation raced"):
        store_credential_checkpoint(redis, "results", "tasks:claude", "1-0", task.id, fields)

    assert redis.client.set.call_count == 1
    redis.client.delete.assert_not_called()


def test_checkpoint_fallback_cleans_exact_checkpoint_when_intent_nx_fails() -> None:
    redis = MagicMock()
    values: dict[str, str] = {}
    set_count = 0

    def get_value(key: str):
        return values.get(key)

    def set_value(key: str, value: str, *, nx: bool = False, **_kwargs):
        nonlocal set_count
        set_count += 1
        if set_count == 2:
            return None
        if nx and key in values:
            return None
        values[key] = value
        return True

    def delete_value(key: str):
        values.pop(key, None)
        return 1

    redis.client.get.side_effect = get_value
    redis.client.set.side_effect = set_value
    redis.client.delete.side_effect = delete_value
    task, fields = _task_and_fields()

    with pytest.raises(RuntimeError, match="intent creation raced"):
        store_credential_checkpoint(redis, "results", "tasks:claude", "1-0", task.id, fields)

    assert values == {}
    redis.client.delete.assert_called_once()


@pytest.mark.parametrize(
    ("checkpoint_value", "intent_value", "expected"),
    [
        ("serialized", "1", True),
        (None, "1", False),
        ("changed", "1", False),
        ("serialized", "invalid", False),
    ],
)
def test_v1_migration_fallback_extends_ttl_only_for_exact_valid_state(
    checkpoint_value: str | None,
    intent_value: str,
    expected: bool,
) -> None:
    redis = MagicMock()
    values = {"checkpoint": checkpoint_value, "intent": intent_value}
    redis.client.get.side_effect = values.get

    migrated = _migrate_v1_intent_once(
        redis,
        "checkpoint",
        "intent",
        "serialized",
    )

    assert migrated is expected
    if expected:
        redis.client.expire.assert_has_calls(
            [
                call("checkpoint", CREDENTIAL_CHECKPOINT_TTL_SECONDS),
                call("intent", CREDENTIAL_CHECKPOINT_TTL_SECONDS),
            ]
        )
    else:
        redis.client.expire.assert_not_called()


def test_new_checkpoint_and_intent_get_a_bounded_ttl(fake_redis_client) -> None:
    """A freshly created checkpoint must not live forever.

    Regression: both keys were created with `SET NX` and no expiry, and the
    v1 migration actively `PERSIST`ed them. They hold the plaintext rotated
    OAuth blob, and the only code that deletes them runs off a live PEL entry
    -- so `XGROUP DELCONSUMER` (the documented operator fix for a stalled
    queue) discarded the PEL entry and stranded the secret in Redis forever.
    """
    redis = fake_redis_client
    task, fields = _task_and_fields()
    checkpoint = store_credential_checkpoint(
        redis, "test:results", "test:tasks:claude", "1-0", task.id, fields
    )
    intent_key = credential_intent_key("test:results", "test:tasks:claude", "1-0", task.id)

    for key in (checkpoint.key, intent_key):
        ttl = redis.client.ttl(key)
        assert ttl > 0, f"{key} must expire eventually, got {ttl}"
        assert ttl <= CREDENTIAL_CHECKPOINT_TTL_SECONDS
        # Must comfortably outlive any task the pool will let run.
        assert ttl > 24 * 3600, f"{key} must outlive any task, got {ttl}"


def test_absent_checkpoint_fallback_requires_exact_terminal_row_and_empty_pel() -> None:
    redis = MagicMock()
    task, fields = _task_and_fields()
    checkpoint = CredentialCheckpoint("checkpoint", "serialized", fields)
    fingerprint = handoff_payload_fingerprint(fields)
    marked = {**fields, HANDOFF_FINGERPRINT_FIELD: fingerprint}
    marker = handoff_marker_key("results", "tasks:claude", "1-0", task.id)
    intent = credential_intent_key("results", "tasks:claude", "1-0", task.id)
    values = {marker: f"terminal|2-0|{fingerprint}", intent: "1"}
    redis.client.get.side_effect = values.get
    redis.client.delete.side_effect = lambda key: int(values.pop(key, None) is not None)
    redis.client.xrange.return_value = [("2-0", marked)]
    redis.client.xpending_range.return_value = [{"message_id": "1-0"}]

    pending = terminal_credential_handoff_once(
        redis,
        checkpoint,
        "results",
        "tasks:claude",
        "1-0",
        task.id,
        maxlen=20000,
    )
    assert pending is CredentialTerminalOutcome.ABSENT
    assert values[intent] == "1"

    redis.client.xpending_range.return_value = []
    terminal = terminal_credential_handoff_once(
        redis,
        checkpoint,
        "results",
        "tasks:claude",
        "1-0",
        task.id,
        maxlen=20000,
    )
    assert terminal is CredentialTerminalOutcome.COMPLETE
    assert intent not in values


def test_fallback_retains_private_state_until_post_xack_pel_is_empty() -> None:
    redis = MagicMock()
    task, fields = _task_and_fields()
    checkpoint = CredentialCheckpoint("checkpoint", "serialized", fields)
    intent = credential_intent_key("results", "tasks:claude", "1-0", task.id)
    values = {checkpoint.key: checkpoint.serialized, intent: "1"}

    def get_value(key: str):
        return values.get(key)

    def set_value(key: str, value: str, **_kwargs):
        values[key] = value
        return True

    def delete_value(key: str):
        values.pop(key, None)
        return 1

    redis.client.get.side_effect = get_value
    redis.client.set.side_effect = set_value
    redis.client.delete.side_effect = delete_value
    redis.xadd_capped_raw.return_value = "2-0"
    redis.xack_raw.return_value = 1
    redis.client.xpending_range.return_value = [{"message_id": "1-0"}]

    incomplete = terminal_credential_handoff_once(
        redis,
        checkpoint,
        "results",
        "tasks:claude",
        "1-0",
        task.id,
        maxlen=20000,
    )
    assert incomplete is CredentialTerminalOutcome.MISMATCH
    assert values[checkpoint.key] == checkpoint.serialized
    assert values[intent] == "1"

    marked = redis.xadd_capped_raw.call_args.args[1]
    redis.client.xrange.return_value = [("2-0", marked)]
    redis.client.xpending_range.return_value = []
    complete = terminal_credential_handoff_once(
        redis,
        checkpoint,
        "results",
        "tasks:claude",
        "1-0",
        task.id,
        maxlen=20000,
    )
    assert complete is CredentialTerminalOutcome.COMPLETE
    assert checkpoint.key not in values
    assert intent not in values


@pytest.mark.parametrize("preexisting_intent", [False, True])
def test_fakeredis_v1_ttl_migration_persists_then_recovers(
    fake_redis_client,
    preexisting_intent: bool,
) -> None:
    redis = fake_redis_client
    task, fields = _task_and_fields()
    task.key_prefix = "test"
    logical_tasks = "tasks:claude"
    tasks_stream = "test:tasks:claude"
    target_stream = "test:results"
    redis.ensure_consumer_group(logical_tasks, CONSUMER_GROUP)
    entry_id = redis.xadd(logical_tasks, task.to_dict())
    assert redis.xreadgroup(
        group=CONSUMER_GROUP,
        consumer="legacy-worker",
        stream=logical_tasks,
        block_ms=None,
    )
    checkpoint_key = credential_checkpoint_key(target_stream, tasks_stream, entry_id, task.id)
    intent_key = credential_intent_key(target_stream, tasks_stream, entry_id, task.id)
    legacy = json.dumps(
        {
            "version": 1,
            "target_stream": target_stream,
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
    if preexisting_intent:
        redis.client.set(intent_key, "1", ex=60)

    status, checkpoint = load_credential_checkpoint(
        redis,
        target_stream,
        tasks_stream,
        entry_id,
        task,
        logging.getLogger("test.fake-v1"),
    )
    assert status is CredentialCheckpointStatus.VALID
    assert checkpoint is not None
    # The v1 key carried a 60s TTL, which could expire while the entry is still
    # pending. Migration lifts it to the checkpoint backstop -- far beyond any
    # task duration, but still bounded so an unreachable checkpoint (e.g. after
    # XGROUP DELCONSUMER discards the PEL entry) cannot retain a plaintext
    # OAuth blob forever.
    for key in (checkpoint_key, intent_key):
        ttl = redis.client.ttl(key)
        assert ttl > 60, f"{key} must outlive any task, got {ttl}"
        assert ttl <= CREDENTIAL_CHECKPOINT_TTL_SECONDS, f"{key} must stay bounded, got {ttl}"

    recovered = recover_credential_checkpoint(
        redis,
        task,
        target_stream,
        tasks_stream,
        entry_id,
        "test:dead-letter",
        logging.getLogger("test.fake-v1-recover"),
        maxlen=20000,
    )
    assert recovered is CredentialRecoveryOutcome.RECOVERED
    assert redis.client.exists(checkpoint_key) == 0
    assert redis.client.exists(intent_key) == 0
    assert redis.client.xpending(tasks_stream, CONSUMER_GROUP)["pending"] == 0
    assert redis.client.xlen(target_stream) == 1
