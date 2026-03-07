"""Tests for orcest.shared.redis_client using fakeredis."""

import pytest

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


# ---------------------------------------------------------------------------
# Tests for xadd_capped and xread_after helpers
# ---------------------------------------------------------------------------


def test_xadd_capped_basic(fake_redis_client):
    """xadd_capped adds entries that are readable."""
    stream = "output:worker-1"
    entry_id = fake_redis_client.xadd_capped(stream, {"line": "hello"}, maxlen=2000)
    assert isinstance(entry_id, str)
    assert len(entry_id) > 0

    # Entry should be in the stream
    length = fake_redis_client.client.xlen(stream)
    assert length == 1


def test_xadd_capped_trims(fake_redis_client):
    """xadd_capped trims the stream when it exceeds maxlen."""
    stream = "output:worker-1"
    maxlen = 10
    for i in range(30):
        fake_redis_client.xadd_capped(stream, {"line": f"line-{i}"}, maxlen=maxlen)

    length = fake_redis_client.client.xlen(stream)
    # With approximate trimming, length should be at or near maxlen.
    # Assert both an upper bound (trimming happened) and that the stream
    # is not empty (entries were added).
    assert length <= maxlen + 5, f"expected at most ~{maxlen} entries, got {length}"
    assert length >= 1, "stream should not be empty after 30 inserts"


def test_xadd_capped_rejects_zero_maxlen(fake_redis_client):
    """xadd_capped raises ValueError when maxlen is not positive."""
    with pytest.raises(ValueError, match="maxlen must be positive"):
        fake_redis_client.xadd_capped("output:worker-1", {"line": "x"}, maxlen=0)
    with pytest.raises(ValueError, match="maxlen must be positive"):
        fake_redis_client.xadd_capped("output:worker-1", {"line": "x"}, maxlen=-1)


def test_xadd_capped_rejects_empty_fields(fake_redis_client):
    """xadd_capped raises ValueError when fields is empty."""
    with pytest.raises(ValueError, match="fields must be a non-empty dict"):
        fake_redis_client.xadd_capped("output:worker-1", {}, maxlen=2000)


def test_xread_after_returns_new_entries(fake_redis_client):
    """xread_after returns entries after the given ID."""
    stream = "output:worker-1"

    # Add some entries
    id1 = fake_redis_client.xadd_capped(stream, {"line": "line-1"}, maxlen=2000)
    fake_redis_client.xadd_capped(stream, {"line": "line-2"}, maxlen=2000)
    id3 = fake_redis_client.xadd_capped(stream, {"line": "line-3"}, maxlen=2000)

    # Read all from beginning
    entries = fake_redis_client.xread_after(stream, "0-0")
    assert len(entries) == 3
    assert entries[0][1]["line"] == "line-1"
    assert entries[2][1]["line"] == "line-3"

    # Read only entries after id1
    entries = fake_redis_client.xread_after(stream, id1)
    assert len(entries) == 2
    assert entries[0][1]["line"] == "line-2"
    assert entries[1][1]["line"] == "line-3"

    # Read after id3 -> nothing new
    entries = fake_redis_client.xread_after(stream, id3)
    assert entries == []


def test_xread_after_empty_stream(fake_redis_client):
    """xread_after on nonexistent stream returns empty list."""
    entries = fake_redis_client.xread_after("nonexistent-stream", "0-0")
    assert entries == []


def test_xread_after_rejects_zero_count(fake_redis_client):
    """xread_after raises ValueError when count is not positive."""
    with pytest.raises(ValueError, match="count must be positive"):
        fake_redis_client.xread_after("output:worker-1", count=0)
    with pytest.raises(ValueError, match="count must be positive"):
        fake_redis_client.xread_after("output:worker-1", count=-1)


def test_xread_after_returns_empty_on_connection_error(fake_redis_client, mocker, caplog):
    """xread_after returns [] and logs a warning on ConnectionError."""
    import logging

    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "xread",
        side_effect=_redis.ConnectionError("connection lost"),
    )
    with caplog.at_level(logging.WARNING, logger="orcest.shared.redis_client"):
        entries = fake_redis_client.xread_after("output:worker-1", "0-0")
    assert entries == []
    assert any("xread_after failed" in record.message for record in caplog.records)


def test_xread_after_returns_empty_on_timeout_error(fake_redis_client, mocker, caplog):
    """xread_after returns [] and logs a warning on TimeoutError."""
    import logging

    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "xread",
        side_effect=_redis.TimeoutError("read timed out"),
    )
    with caplog.at_level(logging.WARNING, logger="orcest.shared.redis_client"):
        entries = fake_redis_client.xread_after("output:worker-1", "0-0")
    assert entries == []
    assert any("xread_after failed" in record.message for record in caplog.records)


def test_xread_after_returns_empty_on_response_error(fake_redis_client, mocker, caplog):
    """xread_after returns [] and logs a warning on ResponseError (e.g. WRONGTYPE)."""
    import logging

    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "xread",
        side_effect=_redis.ResponseError("WRONGTYPE Operation against a key"),
    )
    with caplog.at_level(logging.WARNING, logger="orcest.shared.redis_client"):
        entries = fake_redis_client.xread_after("output:worker-1", "0-0")
    assert entries == []
    assert any("xread_after failed" in record.message for record in caplog.records)


def test_xread_after_returns_empty_on_auth_error(fake_redis_client, mocker, caplog):
    """xread_after returns [] and logs a warning on AuthenticationError."""
    import logging

    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "xread",
        side_effect=_redis.AuthenticationError("invalid password"),
    )
    with caplog.at_level(logging.WARNING, logger="orcest.shared.redis_client"):
        entries = fake_redis_client.xread_after("output:worker-1", "0-0")
    assert entries == []
    assert any("xread_after failed" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# Tests for health_check error handling
# ---------------------------------------------------------------------------


def test_health_check_connection_error(fake_redis_client, mocker):
    """health_check returns False when ping() raises ConnectionError."""
    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "ping",
        side_effect=_redis.ConnectionError("refused"),
    )
    assert fake_redis_client.health_check() is False


def test_health_check_timeout_error(fake_redis_client, mocker):
    """health_check returns False when ping() raises TimeoutError."""
    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "ping",
        side_effect=_redis.TimeoutError("timed out"),
    )
    assert fake_redis_client.health_check() is False


def test_health_check_auth_error(fake_redis_client, mocker):
    """health_check returns False when ping() raises AuthenticationError."""
    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "ping",
        side_effect=_redis.AuthenticationError("invalid password"),
    )
    assert fake_redis_client.health_check() is False


def test_health_check_response_error(fake_redis_client, mocker):
    """health_check returns False when ping() raises ResponseError (e.g. NOPERM)."""
    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "ping",
        side_effect=_redis.ResponseError("NOPERM this user has no permissions"),
    )
    assert fake_redis_client.health_check() is False


# ---------------------------------------------------------------------------
# Tests for ensure_consumer_group error handling
# ---------------------------------------------------------------------------


def test_ensure_consumer_group_other_error_reraises(fake_redis_client, mocker):
    """Non-BUSYGROUP ResponseError from xgroup_create is re-raised."""
    import redis as _redis

    mocker.patch.object(
        fake_redis_client._client,
        "xgroup_create",
        side_effect=_redis.ResponseError("WRONGTYPE Operation against a key"),
    )
    with pytest.raises(_redis.ResponseError, match="WRONGTYPE"):
        fake_redis_client.ensure_consumer_group("test-stream", "test-group")


# ---------------------------------------------------------------------------
# Tests for xack edge cases
# ---------------------------------------------------------------------------


def test_xack_nonexistent_entry_returns_zero(fake_redis_client):
    """Acking an entry ID that doesn't exist returns 0."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)

    result = fake_redis_client.xack(stream, group, "9999999999999-0")
    assert result == 0


# ---------------------------------------------------------------------------
# Tests for xpending_count
# ---------------------------------------------------------------------------


def test_xpending_count_returns_one_after_first_delivery(fake_redis_client):
    """xpending_count returns 1 after a message is delivered once."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)
    fake_redis_client.xadd(stream, {"k": "v"})

    entries = fake_redis_client.xreadgroup(group=group, consumer="c1", stream=stream, block_ms=None)
    assert len(entries) == 1
    entry_id = entries[0][0]

    count = fake_redis_client.xpending_count(stream, group, entry_id)
    assert count == 1


def test_xpending_count_returns_zero_for_acked_entry(fake_redis_client):
    """xpending_count returns 0 after the entry has been ACKed."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)
    fake_redis_client.xadd(stream, {"k": "v"})

    entries = fake_redis_client.xreadgroup(group=group, consumer="c1", stream=stream, block_ms=None)
    entry_id = entries[0][0]
    fake_redis_client.xack(stream, group, entry_id)

    count = fake_redis_client.xpending_count(stream, group, entry_id)
    assert count == 0


def test_xpending_count_returns_zero_for_nonexistent_entry(fake_redis_client):
    """xpending_count returns 0 for an entry ID that was never delivered."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)

    count = fake_redis_client.xpending_count(stream, group, "9999999999999-0")
    assert count == 0


def test_xpending_count_returns_zero_on_error(fake_redis_client, mocker):
    """xpending_count returns 0 when xpending_range raises an exception."""
    mocker.patch.object(
        fake_redis_client._client,
        "xpending_range",
        side_effect=Exception("Redis error"),
    )
    count = fake_redis_client.xpending_count("test-stream", "test-group", "1-0")
    assert count == 0
