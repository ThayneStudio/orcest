"""Tests for orcest.shared.redis_client using fakeredis."""

import logging
from unittest.mock import MagicMock

import pytest
import redis as _redis

from orcest.shared.redis_client import ConsumerGroupInspection

# Tests use the fake_redis_client fixture from conftest.py


def _oom_error() -> _redis.ResponseError:
    oom_cls = getattr(_redis.exceptions, "OutOfMemoryError", _redis.ResponseError)
    return oom_cls("OOM command not allowed when used memory > maxmemory")


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


def test_inspect_consumer_group_returns_exists(fake_redis_client):
    """Existing groups are reported without creating anything."""
    fake_redis_client.client.xadd("test:test-stream", {"k": "v"})
    fake_redis_client.client.xgroup_create("test:test-stream", "test-group", id="0")

    result = fake_redis_client.inspect_consumer_group("test-stream", "test-group")

    assert result is ConsumerGroupInspection.EXISTS


def test_ensure_consumer_group_existing_group_does_not_write(fake_redis_client, mocker):
    """Existing groups remain usable when Redis rejects writes."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.client.xadd("test:test-stream", {"k": "v"})
    fake_redis_client.client.xgroup_create("test:test-stream", group, id="0")
    xinfo = mocker.spy(fake_redis_client._client, "xinfo_groups")
    create = mocker.patch.object(
        fake_redis_client._client,
        "xgroup_create",
        side_effect=_oom_error(),
    )

    fake_redis_client.ensure_consumer_group(stream, group)

    xinfo.assert_called_once_with("test:test-stream")
    create.assert_not_called()
    assert fake_redis_client.inspect_consumer_group(stream, group) is ConsumerGroupInspection.EXISTS


def test_inspect_consumer_group_absent_stream_maps_to_missing(fake_redis_client, mocker):
    """ERR no such key from XINFO GROUPS is the normal absent-stream result."""
    mocker.patch.object(
        fake_redis_client._client,
        "xinfo_groups",
        side_effect=_redis.ResponseError("ERR no such key"),
    )

    result = fake_redis_client.inspect_consumer_group("test-stream", "test-group")

    assert result is ConsumerGroupInspection.MISSING


def test_ensure_consumer_group_absent_stream_creates_with_mkstream(fake_redis_client, mocker):
    """Missing streams still use XGROUP CREATE ... MKSTREAM."""
    xinfo = mocker.patch.object(
        fake_redis_client._client,
        "xinfo_groups",
        side_effect=_redis.ResponseError("ERR no such key"),
    )
    create = mocker.patch.object(fake_redis_client._client, "xgroup_create")

    fake_redis_client.ensure_consumer_group("test-stream", "test-group")

    xinfo.assert_called_once_with("test:test-stream")
    create.assert_called_once_with(
        name="test:test-stream", groupname="test-group", id="0", mkstream=True
    )


def test_ensure_consumer_group_missing_group_creates(fake_redis_client, mocker):
    """Streams without the requested group inspect once, then create."""
    xinfo = mocker.patch.object(
        fake_redis_client._client,
        "xinfo_groups",
        return_value=[{"name": "other-group"}],
    )
    create = mocker.patch.object(fake_redis_client._client, "xgroup_create")

    fake_redis_client.ensure_consumer_group("test-stream", "test-group")

    xinfo.assert_called_once_with("test:test-stream")
    create.assert_called_once_with(
        name="test:test-stream", groupname="test-group", id="0", mkstream=True
    )


def test_ensure_consumer_group_busygroup_race_succeeds(fake_redis_client, mocker):
    """A missing-group create race remains successful via BUSYGROUP handling."""
    mocker.patch.object(fake_redis_client._client, "xinfo_groups", return_value=[])
    mocker.patch.object(
        fake_redis_client._client,
        "xgroup_create",
        side_effect=_redis.ResponseError("BUSYGROUP Consumer Group name already exists"),
    )

    fake_redis_client.ensure_consumer_group("test-stream", "test-group")


def test_ensure_consumer_group_create_oom_propagates(fake_redis_client, mocker):
    """Creation OOM is not converted into an existence result."""
    mocker.patch.object(fake_redis_client._client, "xinfo_groups", return_value=[])
    mocker.patch.object(fake_redis_client._client, "xgroup_create", side_effect=_oom_error())

    with pytest.raises(_redis.ResponseError, match="OOM"):
        fake_redis_client.ensure_consumer_group("test-stream", "test-group")


def test_inspect_consumer_group_wrongtype_remains_error(fake_redis_client):
    """WRONGTYPE from XINFO GROUPS is not hidden as missing."""
    fake_redis_client.set_value("test-stream", "not-a-stream")

    with pytest.raises(_redis.ResponseError, match="WRONGTYPE"):
        fake_redis_client.inspect_consumer_group("test-stream", "test-group")


def test_inspect_consumer_group_acl_error_remains_error(fake_redis_client, mocker):
    """ACL failures from XINFO GROUPS remain caller-visible errors."""
    mocker.patch.object(
        fake_redis_client._client,
        "xinfo_groups",
        side_effect=_redis.ResponseError("NOPERM this user has no permissions"),
    )

    with pytest.raises(_redis.ResponseError, match="NOPERM"):
        fake_redis_client.inspect_consumer_group("test-stream", "test-group")


def test_inspect_consumer_group_malformed_non_list_remains_error(fake_redis_client, mocker):
    """Malformed XINFO GROUPS response shapes are not treated as missing."""
    mocker.patch.object(fake_redis_client._client, "xinfo_groups", return_value="unexpected")

    with pytest.raises(TypeError, match="xinfo_groups returned str"):
        fake_redis_client.inspect_consumer_group("test-stream", "test-group")


def test_inspect_consumer_group_malformed_entry_remains_error(fake_redis_client, mocker):
    """Malformed group entries are not treated as missing."""
    mocker.patch.object(fake_redis_client._client, "xinfo_groups", return_value=["bad-entry"])

    with pytest.raises(TypeError, match="malformed group entry"):
        fake_redis_client.inspect_consumer_group("test-stream", "test-group")


def test_inspect_consumer_group_raw_uses_fully_qualified_stream(fake_redis_client, mocker):
    """Raw inspection never applies the client's key prefix."""
    xinfo = mocker.patch.object(
        fake_redis_client._client,
        "xinfo_groups",
        return_value=[{"name": "test-group"}],
    )

    result = fake_redis_client.inspect_consumer_group_raw("raw-stream", "test-group")

    assert result is ConsumerGroupInspection.EXISTS
    xinfo.assert_called_once_with("raw-stream")


def test_ensure_consumer_group_raw_existing_group_does_not_write(fake_redis_client, mocker):
    """Raw ensure uses the same read-first semantics as prefixed ensure."""
    fake_redis_client.client.xadd("raw-stream", {"k": "v"})
    fake_redis_client.client.xgroup_create("raw-stream", "test-group", id="0")
    create = mocker.patch.object(
        fake_redis_client._client,
        "xgroup_create",
        side_effect=_oom_error(),
    )

    fake_redis_client.ensure_consumer_group_raw("raw-stream", "test-group")

    create.assert_not_called()


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


def test_xreadgroup_pending_uses_explicit_start_id(fake_redis_client, mocker):
    """Pending reads can advance past an unACKed entry without losing it."""
    read = mocker.patch.object(fake_redis_client.client, "xreadgroup", return_value=[])

    result = fake_redis_client.xreadgroup(
        group="test-group",
        consumer="c1",
        stream="test-stream",
        count=10,
        block_ms=None,
        pending=True,
        pending_start_id="123-4",
    )

    assert result == []
    read.assert_called_once_with(
        groupname="test-group",
        consumername="c1",
        streams={fake_redis_client._prefixed("test-stream"): "123-4"},
        count=10,
        block=None,
    )


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


def test_xadd_capped_exact_trim(fake_redis_client):
    """approximate=False trims the stream to exactly maxlen."""
    stream = "output:worker-1"
    maxlen = 10
    for i in range(30):
        fake_redis_client.xadd_capped(
            stream, {"line": f"line-{i}"}, maxlen=maxlen, approximate=False
        )
    assert fake_redis_client.xlen(stream) == maxlen


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


# --- xadd_capped_expire (#604) ---


def test_xadd_capped_expire_sets_ttl_on_new_stream(fake_redis_client):
    """A successful first append always leaves a positive TTL."""
    stream = "output:worker-1"
    entry_id = fake_redis_client.xadd_capped_expire(
        stream, {"line": "hello"}, maxlen=2000, ttl_seconds=3600
    )
    assert isinstance(entry_id, str)
    assert len(entry_id) > 0
    assert fake_redis_client.xlen(stream) == 1
    ttl = fake_redis_client.ttl(stream)
    assert 0 < ttl <= 3600


def test_xadd_capped_expire_refreshes_ttl_on_existing_stream(fake_redis_client):
    """A second append to an already-TTL'd stream refreshes the TTL."""
    stream = "output:worker-1"
    fake_redis_client.xadd_capped_expire(stream, {"line": "one"}, maxlen=2000, ttl_seconds=3600)
    fake_redis_client.client.expire(fake_redis_client._prefixed(stream), 5)
    assert fake_redis_client.ttl(stream) <= 5

    fake_redis_client.xadd_capped_expire(stream, {"line": "two"}, maxlen=2000, ttl_seconds=3600)

    assert fake_redis_client.xlen(stream) == 2
    ttl = fake_redis_client.ttl(stream)
    assert 5 < ttl <= 3600


def test_xadd_capped_expire_raw_uses_fully_qualified_key(fake_redis_client):
    """xadd_capped_expire_raw writes and expires the exact key given, unprefixed."""
    fq_stream = "projectA:output:worker-1"
    entry_id = fake_redis_client.xadd_capped_expire_raw(
        fq_stream, {"line": "hello"}, maxlen=2000, ttl_seconds=3600
    )
    assert isinstance(entry_id, str)
    assert len(entry_id) > 0
    assert fake_redis_client.client.xlen(fq_stream) == 1
    ttl = fake_redis_client.client.ttl(fq_stream)
    assert 0 < ttl <= 3600
    # Must not land under the client's default prefix.
    assert fake_redis_client.xlen("output:worker-1") == 0


def test_xadd_capped_expire_exact_trim(fake_redis_client):
    """approximate=False trims the stream to exactly maxlen, same as xadd_capped."""
    stream = "output:worker-1"
    maxlen = 10
    for i in range(30):
        fake_redis_client.xadd_capped_expire(
            stream, {"line": f"line-{i}"}, maxlen=maxlen, ttl_seconds=3600, approximate=False
        )
    assert fake_redis_client.xlen(stream) == maxlen
    assert 0 < fake_redis_client.ttl(stream) <= 3600


def test_xadd_capped_expire_rejects_zero_maxlen(fake_redis_client):
    with pytest.raises(ValueError, match="maxlen must be positive"):
        fake_redis_client.xadd_capped_expire(
            "output:worker-1", {"line": "x"}, maxlen=0, ttl_seconds=3600
        )


def test_xadd_capped_expire_rejects_empty_fields(fake_redis_client):
    with pytest.raises(ValueError, match="fields must be a non-empty dict"):
        fake_redis_client.xadd_capped_expire("output:worker-1", {}, maxlen=2000, ttl_seconds=3600)


def test_xadd_capped_expire_rejects_non_positive_ttl(fake_redis_client):
    with pytest.raises(ValueError, match="ttl_seconds must be positive"):
        fake_redis_client.xadd_capped_expire(
            "output:worker-1", {"line": "x"}, maxlen=2000, ttl_seconds=0
        )


def test_xadd_capped_expire_is_one_round_trip(fake_redis_client, mocker):
    """The append+TTL happens as a single EVAL command, not separate XADD/EXPIRE calls."""
    spy = mocker.spy(fake_redis_client._client, "execute_command")

    fake_redis_client.xadd_capped_expire(
        "output:worker-1", {"line": "hello"}, maxlen=2000, ttl_seconds=3600
    )

    assert spy.call_count == 1
    assert spy.call_args[0][0] == "EVAL"


def test_xadd_capped_expire_chunked_lines_each_get_one_round_trip(fake_redis_client, mocker):
    """Multiple chunked entries each cost exactly one round trip."""
    spy = mocker.spy(fake_redis_client._client, "execute_command")
    stream = "output:worker-1"

    for i in range(3):
        fake_redis_client.xadd_capped_expire(
            stream,
            {"line": f"chunk-{i}", "part": str(i), "parts": "3"},
            maxlen=2000,
            ttl_seconds=3600,
        )

    assert spy.call_count == 3
    assert all(call.args[0] == "EVAL" for call in spy.call_args_list)
    assert fake_redis_client.xlen(stream) == 3


def test_xadd_capped_expire_failure_leaves_no_partial_state(fake_redis_client, mocker):
    """Append failure leaves neither a partial key nor a misleading success result."""
    stream = "output:worker-1"
    mocker.patch.object(fake_redis_client._client, "eval", side_effect=_oom_error())

    with pytest.raises(_redis.ResponseError, match="OOM"):
        fake_redis_client.xadd_capped_expire(
            stream, {"line": "hello"}, maxlen=2000, ttl_seconds=3600
        )

    assert fake_redis_client.xlen(stream) == 0
    assert fake_redis_client.ttl(stream) == -2  # key does not exist


def test_xadd_capped_expire_raw_failure_leaves_no_partial_state(fake_redis_client, mocker):
    """Same failure guarantee for the fully-qualified (raw) path."""
    fq_stream = "projectA:output:worker-1"
    mocker.patch.object(fake_redis_client._client, "eval", side_effect=_oom_error())

    with pytest.raises(_redis.ResponseError, match="OOM"):
        fake_redis_client.xadd_capped_expire_raw(
            fq_stream, {"line": "hello"}, maxlen=2000, ttl_seconds=3600
        )

    assert fake_redis_client.client.xlen(fq_stream) == 0


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
# Tests for stream_unread_count
# ---------------------------------------------------------------------------


def test_stream_unread_count_warns_on_non_list(fake_redis_client, mocker, caplog):
    """stream_unread_count returns 0 and logs a warning when xinfo_groups returns a non-list."""
    mocker.patch.object(
        fake_redis_client._client,
        "xinfo_groups",
        return_value="unexpected",
    )
    with caplog.at_level(logging.WARNING, logger="orcest.shared.redis_client"):
        result = fake_redis_client.stream_unread_count("mystream", "mygroup")
    assert result == 0
    assert any("unexpected type" in record.message for record in caplog.records)


def test_stream_unread_count_excludes_in_flight_entries(fake_redis_client):
    """Pending (claimed-but-not-ACKed) entries are in flight, not unread."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)
    fake_redis_client.xadd(stream, {"k": "v"})

    fake_redis_client.xreadgroup(group=group, consumer="c1", stream=stream, block_ms=None)

    assert fake_redis_client.stream_unread_count(stream, group) == 0


def test_stream_unread_count_returns_undelivered_lag(fake_redis_client):
    """Entries appended but not yet delivered to any consumer count as unread."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)
    # Prime the group by delivering and ACKing one entry so entries-read is
    # initialized; otherwise fakeredis reports lag off-by-one.
    primer_id = fake_redis_client.xadd(stream, {"k": "primer"})
    fake_redis_client.xreadgroup(group=group, consumer="c0", stream=stream, block_ms=None)
    fake_redis_client.xack(stream, group, primer_id)

    for _ in range(3):
        fake_redis_client.xadd(stream, {"k": "v"})

    assert fake_redis_client.stream_unread_count(stream, group) == 3


def test_stream_unread_count_with_mix_of_pending_and_lag(fake_redis_client):
    """When some entries are claimed and others queued, only the queued count."""
    stream = "test-stream"
    group = "test-group"
    fake_redis_client.ensure_consumer_group(stream, group)
    for _ in range(5):
        fake_redis_client.xadd(stream, {"k": "v"})

    fake_redis_client.xreadgroup(group=group, consumer="c1", stream=stream, block_ms=None, count=2)

    assert fake_redis_client.stream_unread_count(stream, group) == 3


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


# ---------------------------------------------------------------------------
# Tests for delconsumer_raw / xautoclaim_raw (H3-conc)
# ---------------------------------------------------------------------------


def test_delconsumer_raw_returns_pending_count(fake_redis_client):
    """H3-conc: delconsumer_raw removes a consumer and reports its PEL size."""
    rc = fake_redis_client
    fq = rc._prefixed("tasks:claude")
    rc.client.xadd(fq, {"k": "v1"})
    rc.client.xadd(fq, {"k": "v2"})
    rc.client.xgroup_create(fq, "workers", id="0")
    # Dead consumer claims both, never ACKs.
    rc.client.xreadgroup(
        groupname="workers", consumername="orcest-worker-305", streams={fq: ">"}, count=10
    )
    consumers = {c["name"]: c for c in rc.client.xinfo_consumers(fq, "workers")}
    assert consumers["orcest-worker-305"]["pending"] == 2

    reclaimed = rc.delconsumer_raw(fq, "workers", "orcest-worker-305")

    assert reclaimed == 2
    names = [c["name"] for c in rc.client.xinfo_consumers(fq, "workers")]
    assert "orcest-worker-305" not in names


def test_delconsumer_raw_missing_group_returns_zero(fake_redis_client):
    """delconsumer_raw on a nonexistent stream/group returns 0, no raise."""
    rc = fake_redis_client
    assert rc.delconsumer_raw(rc._prefixed("nope"), "workers", "orcest-worker-1") == 0


def test_xautoclaim_raw_reclaims_idle_entries(fake_redis_client):
    """xautoclaim_raw transfers idle PEL entries to a new consumer."""
    rc = fake_redis_client
    fq = rc._prefixed("tasks:claude")
    rc.client.xadd(fq, {"k": "v1"})
    rc.client.xadd(fq, {"k": "v2"})
    rc.client.xgroup_create(fq, "workers", id="0")
    rc.client.xreadgroup(groupname="workers", consumername="dead", streams={fq: ">"}, count=10)

    cursor, claimed = rc.xautoclaim_raw(
        fq, "workers", "pool-manager-sweeper", min_idle_ms=0, start_id="0-0", count=100
    )

    assert isinstance(cursor, str)
    assert len(claimed) == 2
    assert claimed[0][1]["k"] == "v1"
    # Ownership moved to the sweeper consumer.
    owners = {
        p["consumer"] for p in rc.client.xpending_range(fq, "workers", min="-", max="+", count=10)
    }
    assert owners == {"pool-manager-sweeper"}


def test_xautoclaim_raw_empty_pel_returns_empty(fake_redis_client):
    """xautoclaim_raw on an empty PEL returns ('0-0', [])."""
    rc = fake_redis_client
    fq = rc._prefixed("tasks:claude")
    rc.client.xadd(fq, {"k": "v"})
    rc.client.xgroup_create(fq, "workers", id="0")
    cursor, claimed = rc.xautoclaim_raw(fq, "workers", "sweeper", min_idle_ms=0)
    assert claimed == []


# Tests for xtrim_acked_entries (M1-conc)
# ---------------------------------------------------------------------------


def test_xtrim_acked_entries_keeps_unacked_and_reclaims_acked(fake_redis_client):
    """M1-conc: trims delivered+ACKed entries up to the lowest still-pending id,
    keeping the un-ACKed entry (and its credentials) plus anything after it."""
    stream = "tasks:claude"
    group = "workers"
    fake_redis_client.ensure_consumer_group(stream, group)
    id1 = fake_redis_client.xadd(stream, {"token": "ghp_secret1"})
    id2 = fake_redis_client.xadd(stream, {"token": "ghp_secret2"})
    id3 = fake_redis_client.xadd(stream, {"token": "ghp_secret3"})
    # Deliver all three to one consumer, then ACK only the first two.
    fake_redis_client.xreadgroup(group=group, consumer="c1", stream=stream, count=10, block_ms=None)
    fake_redis_client.xack(stream, group, id1)
    fake_redis_client.xack(stream, group, id2)

    removed = fake_redis_client.xtrim_acked_entries(stream, group)

    assert removed == 2  # id1, id2 reclaimed
    assert fake_redis_client.xlen(stream) == 1  # id3 (un-ACKed) survives
    # The surviving entry is exactly the still-pending one, creds intact.
    survivors = fake_redis_client.xrevrange(stream, count=10)
    assert [eid for eid, _ in survivors] == [id3]
    assert survivors[0][1]["token"] == "ghp_secret3"


def test_xtrim_acked_entries_does_not_trim_undelivered(fake_redis_client):
    """M1-conc: never drop work nobody has read yet (last-delivered-id is 0-0)."""
    stream = "tasks:claude"
    group = "workers"
    fake_redis_client.ensure_consumer_group(stream, group)
    fake_redis_client.xadd(stream, {"token": "ghp_a"})
    fake_redis_client.xadd(stream, {"token": "ghp_b"})
    # Nothing delivered, nothing pending.
    removed = fake_redis_client.xtrim_acked_entries(stream, group)
    assert removed == 0
    assert fake_redis_client.xlen(stream) == 2


def test_xtrim_acked_entries_reclaims_fully_drained_stream(fake_redis_client):
    """M1-conc: when every delivered entry is ACKed (empty PEL), reclaim via
    last-delivered-id so credentials don't linger forever."""
    stream = "tasks:claude"
    group = "workers"
    fake_redis_client.ensure_consumer_group(stream, group)
    fake_redis_client.xadd(stream, {"token": "ghp_a"})
    id2 = fake_redis_client.xadd(stream, {"token": "ghp_b"})
    fake_redis_client.xreadgroup(group=group, consumer="c1", stream=stream, count=10, block_ms=None)
    fake_redis_client.xack(stream, group, fake_redis_client.xrevrange(stream, count=10)[1][0])
    fake_redis_client.xack(stream, group, id2)
    removed = fake_redis_client.xtrim_acked_entries(stream, group)
    assert removed == 2
    assert fake_redis_client.xlen(stream) == 0


def test_xtrim_acked_entries_missing_stream_returns_zero(fake_redis_client):
    """M1-conc: no stream / no group -> safe no-op, never raises."""
    assert fake_redis_client.xtrim_acked_entries("tasks:claude", "workers") == 0


def test_xtrim_acked_entries_retries_when_claim_changes_watched_stream(fake_redis_client, mocker):
    first = MagicMock()
    first.xpending.return_value = {
        "pending": 0,
        "min": None,
        "max": None,
        "consumers": [],
    }
    first.xinfo_groups.return_value = [{"name": "workers", "last-delivered-id": "10-0"}]
    first.execute.side_effect = _redis.WatchError()

    second = MagicMock()
    second.xpending.return_value = {
        "pending": 1,
        "min": "10-0",
        "max": "10-0",
        "consumers": [],
    }
    second.execute.return_value = [0]
    mocker.patch.object(
        fake_redis_client.client,
        "pipeline",
        side_effect=[first, second],
    )

    assert fake_redis_client.xtrim_acked_entries("tasks:claude", "workers") == 0
    first.xtrim.assert_called_once()
    second.xtrim.assert_called_once_with("test:tasks:claude", minid="10-0", approximate=False)


def test_round_robin_turn_is_stable_then_advances(fake_redis_client):
    identities = ["alpha", "beta"]

    first = fake_redis_client.claim_round_robin_turn("turn", "sequence", identities, ttl_seconds=60)
    same = fake_redis_client.claim_round_robin_turn("turn", "sequence", identities, ttl_seconds=60)
    fake_redis_client.delete("turn")
    second = fake_redis_client.claim_round_robin_turn(
        "turn", "sequence", identities, ttl_seconds=60
    )

    assert first == same == "alpha"
    assert second == "beta"


def test_next_monotonic_version_uses_shared_redis_clock(fake_redis_client):
    first = fake_redis_client.next_monotonic_version("credential-version")
    second = fake_redis_client.next_monotonic_version("credential-version")

    assert first > 1_000_000_000_000_000
    assert second > first


# Tests for raw helper methods (B2)
# ---------------------------------------------------------------------------


def test_set_ex_raw_and_get_raw_round_trip(fake_redis_client):
    """set_ex_raw and get_raw round-trip with TTL visible via underlying client."""
    rc = fake_redis_client
    fq_key = "orcest:fleet:pressure"

    # Set via raw method
    rc.set_ex_raw(fq_key, "heavy", ttl=300)

    # Get via raw method
    val = rc.get_raw(fq_key)
    assert val == "heavy"

    # TTL visible via underlying client
    ttl = rc.client.ttl(fq_key)
    assert 0 < ttl <= 300


def test_incr_raw_twice_returns_two(fake_redis_client):
    """incr_raw called twice increments to 2."""
    rc = fake_redis_client
    fq_key = "orcest:fleet:kill_budget:202408171000"

    first = rc.incr_raw(fq_key)
    assert first == 1

    second = rc.incr_raw(fq_key)
    assert second == 2


def test_hgetall_raw_returns_hset_raw_writes(fake_redis_client):
    """hgetall_raw returns what hset_raw wrote."""
    rc = fake_redis_client
    fq_key = "workers:activity:orcest-worker-1"

    # Write via hset_raw
    rc.hset_raw(fq_key, "task_id", "task-123")
    rc.hset_raw(fq_key, "timestamp", "1234567890")

    # Read via hgetall_raw
    data = rc.hgetall_raw(fq_key)
    assert data["task_id"] == "task-123"
    assert data["timestamp"] == "1234567890"


def test_expire_raw_sets_ttl(fake_redis_client):
    """expire_raw sets TTL on a key."""
    rc = fake_redis_client
    fq_key = "orcest:fleet:pressure"

    # Set a key without expiry
    rc.client.set(fq_key, "value")

    # Verify no TTL initially
    initial_ttl = rc.client.ttl(fq_key)
    assert initial_ttl == -1

    # Set TTL via expire_raw
    rc.expire_raw(fq_key, 200)

    # Verify TTL is set
    ttl = rc.client.ttl(fq_key)
    assert 0 < ttl <= 200


def test_raw_methods_do_not_prefix_in_prefixed_client(fake_redis_client):
    """Raw methods never prefix keys, even in a prefixed-client instance."""
    rc = fake_redis_client

    # The fixture is a prefixed client with prefix "test:"
    # Write a literal key "k" via set_ex_raw
    rc.set_ex_raw("k", "v", ttl=300)

    # Read the literal key directly from underlying client
    val = rc.client.get("k")
    assert val == "v"

    # Verify that the prefixed key does not exist
    val_prefixed = rc.client.get(rc._prefix + "k")
    assert val_prefixed is None
