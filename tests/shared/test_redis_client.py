"""Tests for orcest.shared.redis_client using fakeredis."""

import logging

import pytest
import redis as _redis

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
    pending_info = fake_redis_client.client.xpending(fake_redis_client._prefixed(stream), group)
    assert pending_info["pending"] > 0

    fake_redis_client.xack(stream, group, entry_id)

    # Pending count should be 0 after ack.
    pending_info = fake_redis_client.client.xpending(fake_redis_client._prefixed(stream), group)
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
    length = fake_redis_client.xlen(stream)
    assert length == 1


def test_xadd_capped_trims(fake_redis_client):
    """xadd_capped trims the stream when it exceeds maxlen."""
    stream = "output:worker-1"
    maxlen = 10
    for i in range(30):
        fake_redis_client.xadd_capped(stream, {"line": f"line-{i}"}, maxlen=maxlen)

    length = fake_redis_client.xlen(stream)
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
    mocker.patch.object(
        fake_redis_client._client,
        "ping",
        side_effect=_redis.ConnectionError("refused"),
    )
    assert fake_redis_client.health_check() is False


def test_health_check_timeout_error(fake_redis_client, mocker):
    """health_check returns False when ping() raises TimeoutError."""
    mocker.patch.object(
        fake_redis_client._client,
        "ping",
        side_effect=_redis.TimeoutError("timed out"),
    )
    assert fake_redis_client.health_check() is False


def test_health_check_auth_error(fake_redis_client, mocker):
    """health_check returns False when ping() raises AuthenticationError."""
    mocker.patch.object(
        fake_redis_client._client,
        "ping",
        side_effect=_redis.AuthenticationError("invalid password"),
    )
    assert fake_redis_client.health_check() is False


def test_health_check_response_error(fake_redis_client, mocker):
    """health_check returns False when ping() raises ResponseError (e.g. NOPERM)."""
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
    mocker.patch.object(
        fake_redis_client._client,
        "xgroup_create",
        side_effect=_redis.ResponseError("WRONGTYPE Operation against a key"),
    )
    with pytest.raises(_redis.ResponseError, match="WRONGTYPE"):
        fake_redis_client.ensure_consumer_group("test-stream", "test-group")


# ---------------------------------------------------------------------------
# Tests for stream_queue_depth
# ---------------------------------------------------------------------------


def test_stream_queue_depth_warns_on_non_list(fake_redis_client, mocker, caplog):
    """stream_queue_depth returns 0 and logs a warning when xinfo_groups returns a non-list."""
    mocker.patch.object(
        fake_redis_client._client,
        "xinfo_groups",
        return_value="unexpected",
    )
    with caplog.at_level(logging.WARNING, logger="orcest.shared.redis_client"):
        result = fake_redis_client.stream_queue_depth("mystream", "mygroup")
    assert result == 0
    assert any("unexpected type" in record.message for record in caplog.records)


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


# ---------------------------------------------------------------------------
# Tests for set operations (sadd, srem, smembers, scard)
# ---------------------------------------------------------------------------


def test_sadd_adds_members(fake_redis_client):
    """sadd adds members to a set and returns the number added."""
    result = fake_redis_client.sadd("myset", "a", "b", "c")
    assert result == 3


def test_sadd_returns_zero_for_existing_members(fake_redis_client):
    """sadd returns 0 when all members already exist."""
    fake_redis_client.sadd("myset", "a", "b")
    result = fake_redis_client.sadd("myset", "a", "b")
    assert result == 0


def test_srem_removes_members(fake_redis_client):
    """srem removes members from a set and returns the number removed."""
    fake_redis_client.sadd("myset", "a", "b", "c")
    result = fake_redis_client.srem("myset", "a", "c")
    assert result == 2


def test_srem_returns_zero_for_missing_members(fake_redis_client):
    """srem returns 0 for members that don't exist."""
    fake_redis_client.sadd("myset", "a")
    result = fake_redis_client.srem("myset", "z")
    assert result == 0


def test_smembers_returns_set_of_strings(fake_redis_client):
    """smembers returns a set of strings, not bytes."""
    fake_redis_client.sadd("myset", "a", "b", "c")
    result = fake_redis_client.smembers("myset")
    assert isinstance(result, set)
    assert result == {"a", "b", "c"}
    for member in result:
        assert isinstance(member, str)


def test_smembers_empty_set(fake_redis_client):
    """smembers returns an empty set for a non-existent key."""
    result = fake_redis_client.smembers("nonexistent")
    assert result == set()


def test_scard_returns_count(fake_redis_client):
    """scard returns the number of members in the set."""
    fake_redis_client.sadd("myset", "a", "b", "c")
    assert fake_redis_client.scard("myset") == 3


def test_scard_empty_set(fake_redis_client):
    """scard returns 0 for a non-existent key."""
    assert fake_redis_client.scard("nonexistent") == 0


# ---------------------------------------------------------------------------
# Tests for hash operations (hlen, hdel)
# ---------------------------------------------------------------------------


def test_hlen_returns_field_count(fake_redis_client):
    """hlen returns the number of fields in a hash."""
    fake_redis_client.hset("myhash", "f1", "v1")
    fake_redis_client.hset("myhash", "f2", "v2")
    assert fake_redis_client.hlen("myhash") == 2


def test_hlen_empty_hash(fake_redis_client):
    """hlen returns 0 for a non-existent key."""
    assert fake_redis_client.hlen("nonexistent") == 0


def test_hdel_removes_fields(fake_redis_client):
    """hdel removes fields from a hash and returns the number removed."""
    fake_redis_client.hset("myhash", "f1", "v1")
    fake_redis_client.hset("myhash", "f2", "v2")
    fake_redis_client.hset("myhash", "f3", "v3")
    result = fake_redis_client.hdel("myhash", "f1", "f3")
    assert result == 2
    assert fake_redis_client.hlen("myhash") == 1


def test_hdel_returns_zero_for_missing_fields(fake_redis_client):
    """hdel returns 0 for fields that don't exist."""
    fake_redis_client.hset("myhash", "f1", "v1")
    result = fake_redis_client.hdel("myhash", "nonexistent")
    assert result == 0


# ---------------------------------------------------------------------------
# Tests for xinfo_consumers
# ---------------------------------------------------------------------------


def test_xinfo_consumers_returns_consumer_list(fake_redis_client):
    """xinfo_consumers returns consumer info after a consumer reads from a stream."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)
    fake_redis_client.xadd(stream, {"k": "v"})
    fake_redis_client.xreadgroup(group=group, consumer="c1", stream=stream, block_ms=None)

    consumers = fake_redis_client.xinfo_consumers(stream, group)
    assert isinstance(consumers, list)
    assert len(consumers) == 1
    assert consumers[0]["name"] == "c1"
    assert consumers[0]["pending"] == 1


def test_xinfo_consumers_empty_group(fake_redis_client):
    """xinfo_consumers returns empty list for a group with no consumers."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)

    consumers = fake_redis_client.xinfo_consumers(stream, group)
    assert consumers == []


# ---------------------------------------------------------------------------
# Tests for key prefixing on new methods
# ---------------------------------------------------------------------------


def test_set_operations_use_prefixed_keys(fake_redis_client):
    """Verify that set operations actually store under prefixed keys."""
    fake_redis_client.sadd("myset", "a")

    # The raw client should see the key under the prefix
    raw_keys = list(fake_redis_client.client.scan_iter(match="*myset*"))
    assert len(raw_keys) == 1
    assert raw_keys[0] == fake_redis_client._prefix + "myset"


def test_hash_operations_use_prefixed_keys(fake_redis_client):
    """Verify that hlen/hdel operate on prefixed keys."""
    fake_redis_client.hset("myhash", "f1", "v1")

    # Direct client check to verify prefix
    raw_val = fake_redis_client.client.hget(fake_redis_client._prefix + "myhash", "f1")
    assert raw_val == "v1"

    # hlen via wrapper
    assert fake_redis_client.hlen("myhash") == 1

    # hdel via wrapper
    fake_redis_client.hdel("myhash", "f1")
    assert fake_redis_client.hlen("myhash") == 0


# ---------------------------------------------------------------------------
# Tests for PrefixedPipeline
# ---------------------------------------------------------------------------


def test_pipeline_sadd_srem_execute(fake_redis_client):
    """Pipeline sadd and srem batch operations correctly."""
    fake_redis_client.sadd("myset", "a", "b", "c")

    pipe = fake_redis_client.pipeline()
    pipe.srem("myset", "a")
    pipe.sadd("myset", "d")
    results = pipe.execute()

    assert results[0] == 1  # srem removed 1
    assert results[1] == 1  # sadd added 1
    assert fake_redis_client.smembers("myset") == {"b", "c", "d"}


def test_pipeline_hdel_execute(fake_redis_client):
    """Pipeline hdel batches hash field deletions correctly."""
    fake_redis_client.hset("myhash", "f1", "v1")
    fake_redis_client.hset("myhash", "f2", "v2")

    pipe = fake_redis_client.pipeline()
    pipe.hdel("myhash", "f1")
    pipe.hset("myhash", "f3", "v3")
    results = pipe.execute()

    assert results[0] == 1  # hdel removed 1
    assert fake_redis_client.hlen("myhash") == 2
    assert fake_redis_client.hget("myhash", "f3") == "v3"


def test_pipeline_chaining(fake_redis_client):
    """Pipeline methods return self for fluent chaining."""
    pipe = fake_redis_client.pipeline()
    result = (
        pipe.sadd("myset", "a")
        .srem("myset", "b")
        .hdel("myhash", "f1")
        .hset("myhash", "f2", "v2")
        .incr("counter")
        .expire("counter", 60)
        .hincrby("myhash", "count", 1)
        .execute()
    )
    assert isinstance(result, list)


def test_pipeline_uses_prefixed_keys(fake_redis_client):
    """Pipeline operations actually use prefixed keys in Redis."""
    pipe = fake_redis_client.pipeline()
    pipe.sadd("myset", "a")
    pipe.execute()

    # Verify the key is stored with the prefix
    raw_members = fake_redis_client.client.smembers(fake_redis_client._prefix + "myset")
    assert "a" in raw_members


def test_pipeline_delete(fake_redis_client):
    """Pipeline delete removes keys using the prefix."""
    fake_redis_client.sadd("myset", "a")
    fake_redis_client.hset("myhash", "f1", "v1")

    pipe = fake_redis_client.pipeline()
    pipe.delete("myset", "myhash")
    results = pipe.execute()

    assert results[0] == 2  # deleted 2 keys
    assert fake_redis_client.smembers("myset") == set()
    assert fake_redis_client.hgetall("myhash") == {}


def test_pipeline_delete_chaining(fake_redis_client):
    """Pipeline delete returns self for fluent chaining."""
    fake_redis_client.sadd("myset", "a")

    result = fake_redis_client.pipeline().delete("myset").sadd("myset", "b").execute()
    assert isinstance(result, list)
    assert fake_redis_client.smembers("myset") == {"b"}


def test_pipeline_context_manager(fake_redis_client):
    """Pipeline supports context manager protocol."""
    fake_redis_client.sadd("myset", "a")

    with fake_redis_client.pipeline() as pipe:
        pipe.sadd("myset", "b")
        pipe.srem("myset", "a")
        results = pipe.execute()

    assert results[0] == 1  # sadd added 1
    assert results[1] == 1  # srem removed 1
    assert fake_redis_client.smembers("myset") == {"b"}


def test_pipeline_context_manager_resets_on_exception(fake_redis_client):
    """Pipeline resets on exception exit without raising a secondary error."""
    fake_redis_client.sadd("myset", "a")

    with pytest.raises(ValueError, match="test error"):
        with fake_redis_client.pipeline() as pipe:
            pipe.sadd("myset", "b")
            raise ValueError("test error")

    # The pipeline was not executed, so only "a" should remain
    assert fake_redis_client.smembers("myset") == {"a"}


# ---------------------------------------------------------------------------
# Tests for empty variadic argument guards
# ---------------------------------------------------------------------------


def test_delete_no_keys_returns_zero(fake_redis_client):
    """delete() with no keys returns 0 without hitting Redis."""
    assert fake_redis_client.delete() == 0


def test_xdel_no_entry_ids_returns_zero(fake_redis_client):
    """xdel() with no entry IDs returns 0 without hitting Redis."""
    fake_redis_client.xadd("test-stream", {"k": "v"})
    assert fake_redis_client.xdel("test-stream") == 0
    # The entry should still be in the stream
    assert fake_redis_client.xlen("test-stream") == 1


def test_sadd_no_members_returns_zero(fake_redis_client):
    """sadd() with no members returns 0 without hitting Redis."""
    assert fake_redis_client.sadd("myset") == 0


def test_srem_no_members_returns_zero(fake_redis_client):
    """srem() with no members returns 0 without hitting Redis."""
    fake_redis_client.sadd("myset", "a")
    assert fake_redis_client.srem("myset") == 0
    assert fake_redis_client.smembers("myset") == {"a"}


def test_hdel_no_fields_returns_zero(fake_redis_client):
    """hdel() with no fields returns 0 without hitting Redis."""
    fake_redis_client.hset("myhash", "f1", "v1")
    assert fake_redis_client.hdel("myhash") == 0
    assert fake_redis_client.hlen("myhash") == 1


def test_pipeline_delete_no_keys_is_noop(fake_redis_client):
    """Pipeline delete() with no keys is a no-op (does not queue a command)."""
    fake_redis_client.sadd("myset", "a")

    pipe = fake_redis_client.pipeline()
    pipe.delete()  # no keys -- should not queue anything
    pipe.sadd("myset", "b")
    results = pipe.execute()

    # Only the sadd should have been queued
    assert len(results) == 1
    assert results[0] == 1
    assert fake_redis_client.smembers("myset") == {"a", "b"}


def test_pipeline_sadd_no_members_is_noop(fake_redis_client):
    """Pipeline sadd() with no members is a no-op."""
    pipe = fake_redis_client.pipeline()
    pipe.sadd("myset")
    pipe.incr("counter")
    results = pipe.execute()

    # Only incr should have been queued
    assert len(results) == 1


def test_pipeline_srem_no_members_is_noop(fake_redis_client):
    """Pipeline srem() with no members is a no-op."""
    fake_redis_client.sadd("myset", "a")

    pipe = fake_redis_client.pipeline()
    pipe.srem("myset")
    pipe.incr("counter")
    results = pipe.execute()

    assert len(results) == 1
    assert fake_redis_client.smembers("myset") == {"a"}


def test_pipeline_hdel_no_fields_is_noop(fake_redis_client):
    """Pipeline hdel() with no fields is a no-op."""
    fake_redis_client.hset("myhash", "f1", "v1")

    pipe = fake_redis_client.pipeline()
    pipe.hdel("myhash")
    pipe.incr("counter")
    results = pipe.execute()

    assert len(results) == 1
    assert fake_redis_client.hlen("myhash") == 1


# ---------------------------------------------------------------------------
# Tests for xdel (with actual deletions)
# ---------------------------------------------------------------------------


def test_xdel_removes_entries_from_stream(fake_redis_client):
    """xdel removes entries by ID and returns the number deleted."""
    stream = "test-stream"
    id1 = fake_redis_client.xadd(stream, {"k": "v1"})
    id2 = fake_redis_client.xadd(stream, {"k": "v2"})
    fake_redis_client.xadd(stream, {"k": "v3"})

    deleted = fake_redis_client.xdel(stream, id1, id2)
    assert deleted == 2
    assert fake_redis_client.xlen(stream) == 1


def test_xdel_nonexistent_entry_returns_zero(fake_redis_client):
    """xdel returns 0 when the entry ID does not exist."""
    stream = "test-stream"
    fake_redis_client.xadd(stream, {"k": "v1"})

    deleted = fake_redis_client.xdel(stream, "9999999999999-0")
    assert deleted == 0


# ---------------------------------------------------------------------------
# Tests for xrevrange
# ---------------------------------------------------------------------------


def test_xrevrange_returns_entries_in_reverse_order(fake_redis_client):
    """xrevrange returns entries newest-first."""
    stream = "test-stream"
    fake_redis_client.xadd(stream, {"line": "first"})
    fake_redis_client.xadd(stream, {"line": "second"})
    fake_redis_client.xadd(stream, {"line": "third"})

    entries = fake_redis_client.xrevrange(stream, count=3)
    assert len(entries) == 3
    assert entries[0][1]["line"] == "third"
    assert entries[2][1]["line"] == "first"


def test_xrevrange_respects_count(fake_redis_client):
    """xrevrange limits the number of returned entries."""
    stream = "test-stream"
    for i in range(10):
        fake_redis_client.xadd(stream, {"line": f"line-{i}"})

    entries = fake_redis_client.xrevrange(stream, count=3)
    assert len(entries) == 3
    # Should be the 3 most recent entries
    assert entries[0][1]["line"] == "line-9"
    assert entries[2][1]["line"] == "line-7"


def test_xrevrange_empty_stream(fake_redis_client):
    """xrevrange on a nonexistent stream returns empty list."""
    entries = fake_redis_client.xrevrange("nonexistent", count=10)
    assert entries == []


# ---------------------------------------------------------------------------
# Tests for scan_iter
# ---------------------------------------------------------------------------


def test_scan_iter_returns_unprefixed_keys(fake_redis_client):
    """scan_iter strips the key prefix from returned keys."""
    fake_redis_client.sadd("set:a", "val")
    fake_redis_client.sadd("set:b", "val")
    fake_redis_client.hset("hash:c", "f", "v")

    keys = fake_redis_client.scan_iter("set:*")
    assert set(keys) == {"set:a", "set:b"}


def test_scan_iter_no_matches(fake_redis_client):
    """scan_iter returns empty list when no keys match."""
    fake_redis_client.sadd("myset", "val")
    keys = fake_redis_client.scan_iter("nonexistent:*")
    assert keys == []
