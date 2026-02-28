"""Tests for orcest.shared.redis_client using fakeredis."""

# Tests use the fake_redis_client fixture from conftest.py


def test_health_check_succeeds(fake_redis_client):
    """health_check returns True when Redis is reachable."""
    assert fake_redis_client.health_check() is True


def test_xadd_returns_entry_id(fake_redis_client):
    """xadd returns a non-empty string entry ID."""
    entry_id = fake_redis_client.xadd("test-stream", {"key": "value"})
    assert isinstance(entry_id, str)
    assert len(entry_id) > 0


def test_ensure_consumer_group_creates_group(fake_redis_client):
    """After ensure_consumer_group, xreadgroup works without error."""
    fake_redis_client.ensure_consumer_group("test-stream", "test-group")
    result = fake_redis_client.xreadgroup(
        group="test-group",
        consumer="c1",
        stream="test-stream",
        block_ms=None,
    )
    assert result == []


def test_ensure_consumer_group_idempotent(fake_redis_client):
    """Calling ensure_consumer_group twice raises no error."""
    fake_redis_client.ensure_consumer_group("test-stream", "test-group")
    fake_redis_client.ensure_consumer_group("test-stream", "test-group")


def test_xadd_then_xreadgroup_round_trip(fake_redis_client):
    """Fields written via xadd are returned by xreadgroup."""
    stream = "test-stream"
    group = "test-group"
    fields = {"repo": "owner/repo", "action": "review"}

    fake_redis_client.ensure_consumer_group(stream, group)
    fake_redis_client.xadd(stream, fields)

    entries = fake_redis_client.xreadgroup(
        group=group,
        consumer="c1",
        stream=stream,
        block_ms=None,
    )
    assert len(entries) == 1
    entry_id, entry_fields = entries[0]
    assert isinstance(entry_id, str)
    assert entry_fields["repo"] == "owner/repo"
    assert entry_fields["action"] == "review"


def test_xack_removes_from_pending(fake_redis_client):
    """xack brings the pending count back to zero."""
    stream = "test-stream"
    group = "test-group"

    fake_redis_client.xadd(stream, {"k": "v"})
    fake_redis_client.ensure_consumer_group(stream, group)

    entries = fake_redis_client.xreadgroup(
        group=group,
        consumer="c1",
        stream=stream,
        block_ms=None,
    )
    assert len(entries) == 1
    entry_id = entries[0][0]

    # Pending count should be > 0 before ack.
    pending_info = fake_redis_client.client.xpending(stream, group)
    assert pending_info["pending"] > 0

    fake_redis_client.xack(stream, group, entry_id)

    # Pending count should be 0 after ack.
    pending_info = fake_redis_client.client.xpending(stream, group)
    assert pending_info["pending"] == 0


def test_xreadgroup_empty_returns_empty_list(fake_redis_client):
    """xreadgroup on an empty stream returns an empty list."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)

    result = fake_redis_client.xreadgroup(
        group=group,
        consumer="c1",
        stream=stream,
        block_ms=None,
    )
    assert result == []


def test_close_is_idempotent(fake_redis_client):
    """Calling close() twice raises no error."""
    fake_redis_client.close()
    fake_redis_client.close()
