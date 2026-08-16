"""Real-Redis coverage for dedicated mixed-provider worker streams."""

from __future__ import annotations

import pytest

from orcest.orchestrator.provider_pool import ProviderPool
from orcest.shared.models import CONSUMER_GROUP, Task, TaskType
from orcest.shared.providers import ProviderEntry
from orcest.shared.redis_client import RedisClient


@pytest.mark.integration
def test_mixed_provider_claim_ack_and_same_vmid_recovery(
    real_redis_client: RedisClient,
    make_real_redis_client,
) -> None:
    """Each backend claims only its stream and a replacement recovers its PEL."""
    rc = real_redis_client
    pool = ProviderPool(
        [
            ProviderEntry("clauder", "credential-clauder"),
            ProviderEntry("codex", "credential-codex"),
            ProviderEntry("grok", "credential-grok"),
        ]
    )
    selected = [pool.next_entry() for _ in range(3)]
    assert [entry.provider for entry in selected if entry is not None] == [
        "clauder",
        "codex",
        "grok",
    ]
    entries: dict[str, str] = {}
    for index, entry in enumerate(selected, start=1):
        assert entry is not None
        backend = entry.provider
        stream = f"tasks:{backend}"
        rc.ensure_consumer_group(stream, CONSUMER_GROUP)
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="fake",
            provider=backend,
            credential=pool.effective_credential(entry),
            resource_type="pr",
            resource_id=index,
            prompt=f"run with {backend}",
            branch="main",
        )
        entries[backend] = rc.xadd(stream, task.to_dict())

    worker_ids = {
        "clauder": "orcest-worker-300",
        "codex": "orcest-worker-301",
        "grok": "orcest-worker-302",
    }
    for backend in ("clauder", "codex", "grok"):
        stream = f"tasks:{backend}"
        worker_id = worker_ids[backend]
        claimed = rc.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=worker_id,
            stream=stream,
            count=1,
            block_ms=None,
        )
        assert len(claimed) == 1
        entry_id, fields = claimed[0]
        assert entry_id == entries[backend]
        assert Task.from_dict(fields).provider == backend
        if backend != "codex":
            assert rc.xack(stream, CONSUMER_GROUP, entry_id) == 1

    # A replacement reusing Codex's deterministic VMID/consumer name can read
    # and ACK the prior generation's PEL after the original process disappears.
    restarted = make_real_redis_client()
    recovered = restarted.xreadgroup(
        group=CONSUMER_GROUP,
        consumer="orcest-worker-301",
        stream="tasks:codex",
        count=1,
        block_ms=None,
        pending=True,
    )
    assert [entry_id for entry_id, _fields in recovered] == [entries["codex"]]
    assert restarted.xack("tasks:codex", CONSUMER_GROUP, entries["codex"]) == 1

    for backend in ("clauder", "codex", "grok"):
        pending = rc.client.xpending(f"tasks:{backend}", CONSUMER_GROUP)
        assert pending["pending"] == 0
