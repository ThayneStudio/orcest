"""Legacy/v1 stream and pool isolation."""

from __future__ import annotations

from orcest.workflow_store.v1.isolation import (
    is_legacy_pel_allowlisted,
    is_v1_protocol_stream,
    legacy_pel_denied,
    validate_capacity_pool_isolation,
)


def test_v1_offer_streams_are_not_legacy_pel_allowlisted() -> None:
    assert is_v1_protocol_stream("tasks:activity:v1:default")
    assert is_v1_protocol_stream("orcest:tasks:activity:v1:default")
    assert legacy_pel_denied("tasks:activity:v1:default")
    assert not is_legacy_pel_allowlisted("tasks:activity:v1:default")


def test_legacy_task_streams_are_allowlisted() -> None:
    assert is_legacy_pel_allowlisted("tasks:claude")
    assert is_legacy_pel_allowlisted("orcest:tasks:issue:grok")
    assert not legacy_pel_denied("prefix:tasks:claude")


def test_validate_rejects_dual_class_and_wrong_reaper() -> None:
    existing = {"redis_prefix:legacy:": ("legacy:", "LEGACY")}
    assert (
        validate_capacity_pool_isolation(
            template_class="V1_CLONE_FIXED",
            template_id="t",
            pool_manager_principal_id="p",
            redis_acl_identity="a",
            redis_prefix="legacy:",
            consumer_group="g",
            stream_namespace="tasks:activity:v1:x",
            reaper_authority="V1_AUTHENTICATED_LOSS",
            clone_credential_removal_attested=True,
            existing=existing,
        )
        == "DUAL_CLASS_IDENTITY"
    )
    assert (
        validate_capacity_pool_isolation(
            template_class="V1_CLONE_FIXED",
            template_id="t",
            pool_manager_principal_id="p",
            redis_acl_identity="a",
            redis_prefix="v1:",
            consumer_group="g",
            stream_namespace="tasks:activity:v1:x",
            reaper_authority="LEGACY_PEL_ALLOWLIST",
            clone_credential_removal_attested=True,
            existing={},
        )
        == "REAPER_AUTHORITY_MISMATCH"
    )
