"""Concurrency and correctness tests for the hardened ProviderPool.

These tests are written first (TDD) to drive the implementation per Task 3.
They cover thread safety (RLock), stable non-secret identity() keys,
max-expiry on cooldowns, register/mark/task_completed contract, TOCTOU
safety, duplicate USAGE handling, restart semantics (new pool = clean state),
and mixed-provider round-robin + independent exhaustion.
"""

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from orcest.orchestrator.provider_pool import ProviderPool
from orcest.shared.providers import ProviderEntry


def _make_entries(n: int, prefix: str = "tok") -> List[ProviderEntry]:
    return [ProviderEntry(provider="claude", credential=f"{prefix}-{i}") for i in range(n)]


def test_from_claude_tokens_synthesis_and_lean_surface():
    """Migration path must synthesize with rich fields left as None/defaults."""
    pool = ProviderPool.from_claude_tokens(["secret-a", "secret-b"])
    assert pool.size == 2
    entry = pool.next_entry()
    assert entry is not None
    assert entry.provider == "claude"
    assert entry.credential == "secret-a"
    assert entry.model is None
    assert entry.cli_binary is None
    assert entry.env_var is None
    assert entry.extras == {}
    # identity must not contain raw secret
    ident = entry.identity()
    assert "secret-a" not in ident
    assert "secret" not in ident.lower()
    assert ident.startswith("claude::")


def test_basic_round_robin_and_exhaustion():
    entries = _make_entries(2, "t")
    pool = ProviderPool(entries)
    assert pool.size == 2
    assert pool.available_count == 2

    e0 = pool.next_entry()
    e1 = pool.next_entry()
    e2 = pool.next_entry()
    assert e0 is not None and e0.credential == "t-0"
    assert e1 is not None and e1.credential == "t-1"
    assert e2 is not None and e2.credential == "t-0"

    # Exhaust the first one
    task = "task-1"
    pool.register_task(task, e0)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    pool.mark_exhausted(task, resets_at=future)

    assert pool.available_count == 1
    # next should skip the exhausted one
    got = pool.next_entry()
    assert got is not None and got.credential == "t-1"


def test_max_existing_expiry_wins():
    """Later mark with sooner expiry must not shorten an existing longer cooldown."""
    pool = ProviderPool(_make_entries(1))
    e = pool.next_entry()
    assert e is not None
    pool.register_task("t1", e)

    far = datetime.now(timezone.utc) + timedelta(hours=2)
    soon = datetime.now(timezone.utc) + timedelta(minutes=5)

    pool.mark_exhausted("t1", resets_at=far)
    # now mark again with sooner time for same identity (via new task mapping)
    pool.register_task("t2", e)
    pool.mark_exhausted("t2", resets_at=soon)

    # The far one must still be in effect
    assert pool.available_count == 0
    got = pool.next_entry()
    assert got is None


def test_earlier_expiry_is_overwritten_by_later():
    pool = ProviderPool(_make_entries(1))
    e = pool.next_entry()
    pool.register_task("t1", e)
    soon = datetime.now(timezone.utc) + timedelta(minutes=5)
    pool.mark_exhausted("t1", resets_at=soon)

    # new mark with later
    pool.register_task("t2", e)
    far = datetime.now(timezone.utc) + timedelta(hours=1)
    pool.mark_exhausted("t2", resets_at=far)

    # still exhausted (far in future)
    assert pool.next_entry() is None


def test_duplicate_usage_exhausted_is_safe():
    """Calling mark_exhausted twice for same task_id (or after pop) must be no-op."""
    pool = ProviderPool(_make_entries(1))
    e = pool.next_entry()
    pool.register_task("task-dup", e)
    exp = datetime.now(timezone.utc) + timedelta(minutes=30)
    pool.mark_exhausted("task-dup", resets_at=exp)
    # second call for same task_id: already popped, safe
    pool.mark_exhausted("task-dup", resets_at=exp)
    # also safe to call for unknown task
    pool.mark_exhausted("never-registered", resets_at=exp)
    assert pool.available_count == 0


def test_task_completed_cleans_mapping():
    pool = ProviderPool(_make_entries(1))
    e = pool.next_entry()
    pool.register_task("tc", e)
    pool.task_completed("tc")
    # now marking the (now-unknown) task is safe and does not exhaust
    pool.mark_exhausted("tc")
    assert pool.available_count == 1


def test_register_before_publish_contract_and_cleanup_on_failure():
    """register immediately after next, then on publish failure call task_completed to release mapping."""
    pool = ProviderPool(_make_entries(1))
    e = pool.next_entry()
    assert e is not None
    tid = "fail-pub-1"
    pool.register_task(tid, e)
    # simulate publish failure path
    pool.task_completed(tid)
    # the entry must be immediately selectable again (not stuck in in-flight)
    e2 = pool.next_entry()
    assert e2 is not None and e2.identity() == e.identity()


def test_mixed_providers_independent_exhaustion():
    claude = ProviderEntry("claude", "c-secret")
    grok = ProviderEntry("grok", "g-secret", model="grok-3")
    pool = ProviderPool([claude, grok])
    assert pool.size == 2

    # round robin hits both
    seen = {pool.next_entry().provider for _ in range(4)}
    assert seen == {"claude", "grok"}

    # exhaust only claude
    c_task = "c1"
    pool.register_task(c_task, claude)
    pool.mark_exhausted(c_task, resets_at=datetime.now(timezone.utc) + timedelta(hours=1))

    # grok still available, claude not
    for _ in range(3):
        e = pool.next_entry()
        assert e is not None
        assert e.provider == "grok"

    assert "claude" not in {pool.next_entry().provider for _ in range(2)}


def test_concurrent_next_under_contention():
    """Many threads calling next_entry concurrently must not crash and must respect round-robin + cooldowns."""
    entries = _make_entries(3)
    pool = ProviderPool(entries)
    results = []
    errors = []
    lock = threading.Lock()

    def worker(n: int):
        try:
            for _ in range(20):
                e = pool.next_entry()
                if e is not None:
                    tid = f"t-{threading.get_ident()}-{n}"
                    pool.register_task(tid, e)
                    # small jitter
                    time.sleep(0.0005)
                    # randomly complete or exhaust (rare)
                    if n % 7 == 0:
                        pool.mark_exhausted(tid, resets_at=datetime.now(timezone.utc) + timedelta(seconds=1))
                    else:
                        pool.task_completed(tid)
                else:
                    time.sleep(0.001)
        except Exception as exc:
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not errors, f"Concurrent ops raised: {errors}"
    # After cooldowns expire (some workers set 1s) we should eventually recover.
    # Use a tolerant wait so the test is not flaky under load / slow CI.
    recovered = False
    for _ in range(30):
        if pool.next_entry() is not None:
            recovered = True
            break
        time.sleep(0.05)
    assert recovered, "Pool never recovered after concurrent cooldowns (all entries stayed exhausted)"


def test_stable_identity_keys_not_raw_secret():
    """All internal tracking uses identity(); raw credential never used as dict key."""
    e = ProviderEntry("claude", "super-secret-xyz")
    pool = ProviderPool([e])
    # We can't easily inspect private _cooldowns without poking, but we can assert
    # that after operations the behavior is correct and repr/identity safe.
    ident = e.identity()
    assert "secret" not in ident
    assert "xyz" not in ident

    pool.register_task("t-secret", e)
    pool.mark_exhausted("t-secret")
    # pool must still function
    assert pool.available_count == 0


def test_restart_loss_clears_cooldowns():
    """A fresh ProviderPool instance (simulating orchestrator restart) has no cooldown memory."""
    pool1 = ProviderPool(_make_entries(1))
    e = pool1.next_entry()
    pool1.register_task("r1", e)
    pool1.mark_exhausted("r1", resets_at=datetime.now(timezone.utc) + timedelta(hours=5))
    assert pool1.next_entry() is None

    # "restart"
    pool2 = ProviderPool(_make_entries(1))
    assert pool2.available_count == 1
    assert pool2.next_entry() is not None


def test_available_count_and_size():
    pool = ProviderPool(_make_entries(2))
    assert pool.size == 2
    assert pool.available_count == 2
    e = pool.next_entry()
    pool.register_task("ac1", e)
    pool.mark_exhausted("ac1")
    assert pool.available_count == 1
    assert pool.size == 2


def test_empty_pool_rejected():
    with pytest.raises(ValueError, match="at least one"):
        ProviderPool([])


def test_duplicate_identities_rejected():
    e1 = ProviderEntry("claude", "same")
    e2 = ProviderEntry("claude", "same")
    with pytest.raises(ValueError, match="Duplicate"):
        ProviderPool([e1, e2])


def test_legacy_shim_methods_exist_for_migration():
    """ProviderPool must provide next_token / register_task(str) / get_task_token / mark with cooldown_until
    so that existing call sites in loop.py continue to work during the phased port."""
    pool = ProviderPool.from_claude_tokens(["shim-tok"])
    # next_token shim
    tok = pool.next_token()
    assert tok == "shim-tok"
    # register with str (legacy)
    pool.register_task("shim-task", "shim-tok")
    # get_task_token
    assert pool.get_task_token("shim-task") == "shim-tok"
    # mark with old kwarg name
    pool.mark_exhausted("shim-task", cooldown_until=datetime.now(timezone.utc) + timedelta(minutes=10))
    assert pool.next_token() is None
