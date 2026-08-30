import pytest

from orcest.workflow_contract.v1.identity import (
    CommitId,
    ObjectFormat,
    is_lowercase_uuid,
    is_nonempty_opaque_string,
    require_lowercase_uuid,
    require_nonempty_opaque_string,
)


def test_lowercase_uuid_accepted() -> None:
    assert is_lowercase_uuid("11111111-1111-1111-1111-111111111111")


def test_uppercase_uuid_rejected() -> None:
    assert not is_lowercase_uuid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".upper())


def test_non_uuid_string_rejected() -> None:
    assert not is_lowercase_uuid("not-a-uuid")
    assert not is_lowercase_uuid(12345)  # type: ignore[arg-type]


def test_require_lowercase_uuid_raises() -> None:
    with pytest.raises(ValueError):
        require_lowercase_uuid("nope", field="attempt_id")


def test_nonempty_opaque_string() -> None:
    assert is_nonempty_opaque_string("issue-42")
    assert not is_nonempty_opaque_string("")
    assert not is_nonempty_opaque_string(42)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        require_nonempty_opaque_string("")


def test_commit_id_sha1_round_trip() -> None:
    oid = "a" * 40
    commit = CommitId(object_format=ObjectFormat.SHA1, oid=oid)
    assert commit.to_json() == {"object_format": "sha1", "oid": oid}
    assert CommitId.from_json(commit.to_json()) == commit


def test_commit_id_rejects_unsupported_object_format() -> None:
    with pytest.raises(ValueError):
        CommitId(object_format="sha256", oid="a" * 64)


def test_commit_id_rejects_malformed_sha1_oid() -> None:
    with pytest.raises(ValueError):
        CommitId(object_format=ObjectFormat.SHA1, oid="not-hex")
    with pytest.raises(ValueError):
        CommitId(object_format=ObjectFormat.SHA1, oid="a" * 39)


def test_commit_id_from_json_rejects_extra_or_missing_fields() -> None:
    with pytest.raises(ValueError):
        CommitId.from_json({"object_format": "sha1", "oid": "a" * 40, "extra": 1})
    with pytest.raises(ValueError):
        CommitId.from_json({"object_format": "sha1"})
