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
    assert entry.source == "legacy_claude_tokens"
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
    """register immediately after next, then on publish failure call task_completed to release."""
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

    # round robin hits both (defensive guard against None for robustness)
    seen: set[str] = set()
    for _ in range(4):
        e = pool.next_entry()
        if e is not None:
            seen.add(e.provider)
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

    # defensive: collect safely in case next_entry returns None under edge conditions
    remaining = set()
    for _ in range(2):
        e = pool.next_entry()
        if e is not None:
            remaining.add(e.provider)
    assert "claude" not in remaining


def test_exhaustion_cooldown_is_per_account_not_per_model():
    """H3-logic: rate limits are per-account, so benching an account benches every
    model-entry that shares its credential -- a second model pinned to the same
    account must NOT remain selectable and must NOT get an independent cooldown.
    """
    from datetime import datetime, timedelta, timezone

    shared = "acct-shared"
    opus = ProviderEntry("claude", shared, model="opus")
    sonnet = ProviderEntry("claude", shared, model="sonnet")
    other = ProviderEntry("claude", "acct-other")
    pool = ProviderPool([opus, sonnet, other])
    assert pool.size == 3
    assert pool.available_count == 3

    # Serve a task on the opus entry of the shared account, then mark exhausted.
    pool.register_task("task-opus", opus)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    pool.mark_exhausted("task-opus", resets_at=future)

    # The shared ACCOUNT is benched: BOTH opus and sonnet entries are gone,
    # leaving only the other account (1 entry available, not 2).
    assert pool.available_count == 1

    # next_entry must never hand back the benched account under any model;
    # only acct-other should ever come out while the account is cooled down.
    creds_seen = set()
    for _ in range(6):
        e = pool.next_entry()
        assert e is not None
        creds_seen.add(e.credential)
    assert creds_seen == {"acct-other"}


def test_exhaustion_cooldown_is_shared_across_claude_aliases():
    """The legacy and interactive Claude queue aliases rate-limit the same account."""
    shared = "claude-oauth-shared"
    legacy = ProviderEntry("claude", shared)
    interactive = ProviderEntry("clauder", shared)
    pool = ProviderPool([legacy, interactive])
    assert pool.size == 2
    assert pool.available_count == 2

    pool.register_task("legacy-task", legacy)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    pool.mark_exhausted("legacy-task", resets_at=future)

    assert pool.available_count == 0
    assert pool.next_entry() is None


def test_concurrent_next_under_contention():
    """Many threads calling next_entry concurrently must not crash and respect RR + cooldowns."""
    entries = _make_entries(3)
    pool = ProviderPool(entries)
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
                    # randomly complete or exhaust (rare) - short cooldown for test robustness
                    if n % 7 == 0:
                        resets = datetime.now(timezone.utc) + timedelta(seconds=0.2)
                        pool.mark_exhausted(tid, resets_at=resets)
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
    # After short artificial cooldowns expire we must recover.
    # Use a generous deadline (not a tight 1.5s fixed window) so the test
    # is robust under load, thread scheduling jitter, or slow CI.
    deadline = time.time() + 5.0
    recovered = False
    while time.time() < deadline:
        if pool.next_entry() is not None:
            recovered = True
            break
        time.sleep(0.05)
    assert recovered, "Pool never recovered after concurrent cooldowns"


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


def test_grok_oauth_json_without_refresh_token_rejected():
    bad = ProviderEntry("grok", '{"access_token":"access-only"}')
    with pytest.raises(ValueError, match="usable"):
        ProviderPool([bad])


def test_grok_oauth_json_without_refresh_token_filtered_from_selection():
    good = ProviderEntry("claude", "claude-secret")
    bad = ProviderEntry("grok", '{"access_token":"access-only"}')

    pool = ProviderPool([bad, good])

    assert pool.size == 1
    assert pool.next_entry() == good


def test_duplicate_identities_rejected():
    e1 = ProviderEntry("claude", "same")
    e2 = ProviderEntry("claude", "same")
    with pytest.raises(ValueError, match="Duplicate"):
        ProviderPool([e1, e2])


def test_legacy_shim_methods_exist_for_migration():
    """ProviderPool must provide next_token / register_task(str) / get_task_token / mark
    with cooldown_until so existing call sites in loop.py continue to work during the
    phased port."""
    pool = ProviderPool.from_claude_tokens(["shim-tok"])
    # next_token shim
    tok = pool.next_token()
    assert tok == "shim-tok"
    # register with str (legacy)
    pool.register_task("shim-task", "shim-tok")
    # get_task_token
    assert pool.get_task_token("shim-task") == "shim-tok"
    # mark with old kwarg name
    pool.mark_exhausted(
        "shim-task", cooldown_until=datetime.now(timezone.utc) + timedelta(minutes=10)
    )
    assert pool.next_token() is None


def test_high_concurrency_mixed_providers_with_exhaustion_and_redaction():
    """High-concurrency (16+ threads) stress with mixed providers (claude + grok),
    concurrent exhaustion simulation, round-robin under contention, duplicate-safe
    handling, and strict redaction invariants (no raw credentials in identity(),
    repr(), exceptions, or any observable state).

    Validates the Provider Registration & Invocation Boundary: only lean surface
    (provider/credential/model/identity()) is ever used internally; execution
    details stay worker-side.
    """
    from datetime import datetime, timedelta, timezone

    claude1 = ProviderEntry(provider="claude", credential="claude-secret-cc01")
    claude2 = ProviderEntry(provider="claude", credential="claude-secret-cc02")
    grok1 = ProviderEntry(provider="grok", credential="grok-secret-gg01", model="grok-3")
    entries = [claude1, grok1, claude2]
    pool = ProviderPool(entries)

    errors: list[str] = []
    seen: list[str] = []
    lock = threading.Lock()

    def worker(n: int) -> None:
        try:
            for i in range(25):
                e = pool.next_entry()
                if e is not None:
                    # Redaction: identity and repr must never contain raw secret
                    ident = e.identity()
                    r = repr(e)
                    assert "claude-secret" not in ident
                    assert "grok-secret" not in ident
                    assert "secret" not in ident.lower()
                    assert "claude-secret" not in r
                    assert "grok-secret" not in r

                    tid = f"mix-{threading.get_ident()}-{n}-{i}"
                    pool.register_task(tid, e)
                    time.sleep(0.0003)
                    # Mix of complete + occasional exhaustion to create contention
                    if (n + i) % 7 == 0:
                        resets = datetime.now(timezone.utc) + timedelta(seconds=0.15)
                        pool.mark_exhausted(tid, resets_at=resets)
                    else:
                        pool.task_completed(tid)
                    with lock:
                        seen.append(e.provider)
                else:
                    time.sleep(0.0005)
        except Exception as exc:
            with lock:
                err_str = str(exc)
                errors.append(err_str)
                # Even exception strings must not leak secrets (defensive)
                assert "claude-secret" not in err_str
                assert "grok-secret" not in err_str

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=8.0)

    assert not errors, f"Concurrent mixed-provider ops raised: {errors}"

    # Both providers must have been handed out under load
    seen_set = set(seen)
    assert "claude" in seen_set and "grok" in seen_set, f"Missing providers in mix: {seen_set}"

    # After short cooldowns expire, pool must become available again (recovery)
    deadline = time.time() + 3.0
    recovered = False
    while time.time() < deadline:
        if pool.next_entry() is not None:
            recovered = True
            break
        time.sleep(0.02)
    assert recovered, "Mixed pool did not recover after concurrent partial exhaustion"

    # Final sanity: no raw secrets in pool repr or any identity
    pool_repr = repr(pool)
    assert "claude-secret" not in pool_repr
    assert "grok-secret" not in pool_repr
    for e in entries:
        assert "secret" not in e.identity()


# ---------------------------------------------------------------------------
# Credential write-back (OAuth-blob providers: Grok/Codex)
# ---------------------------------------------------------------------------


def _grok_entry(blob: str = "orig-blob") -> ProviderEntry:
    return ProviderEntry(provider="grok", credential=blob)


def test_effective_credential_returns_original_without_override():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    assert pool.effective_credential(entry) == "orig"


def test_apply_credential_update_overrides_published_credential():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.register_task("t1", entry)

    account = pool.apply_credential_update("t1", "rotated-blob", minted_at=100.0)
    assert account == entry.account_key()
    # Published credential now reflects the rotated blob...
    assert pool.effective_credential(entry) == "rotated-blob"
    # ...but the entry identity (pool anchor) is unchanged.
    assert pool.next_entry().identity() == entry.identity()


def test_apply_credential_update_accepts_grok_json_with_refresh_token():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.register_task("t1", entry)
    blob = '{"access_token":"new-access","refresh_token":"new-refresh"}'

    account = pool.apply_credential_update("t1", blob, minted_at=100.0)

    assert account == entry.account_key()
    assert pool.effective_credential(entry) == blob


def test_apply_credential_update_rejects_grok_json_without_refresh_token():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.register_task("t1", entry)

    account = pool.apply_credential_update(
        "t1",
        '{"access_token":"new-access","expires_at":123}',
        minted_at=100.0,
    )

    assert account is None
    assert pool.effective_credential(entry) == "orig"


def test_apply_credential_update_ignores_stale_minted_at():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.register_task("t1", entry)
    assert pool.apply_credential_update("t1", "newer", minted_at=200.0) == entry.account_key()
    # An older update must not clobber the newer blob.
    pool.register_task("t2", entry)
    assert pool.apply_credential_update("t2", "older", minted_at=150.0) is None
    assert pool.effective_credential(entry) == "newer"


def test_apply_credential_update_unknown_task_is_noop():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    assert pool.apply_credential_update("never-registered", "blob", minted_at=1.0) is None
    assert pool.effective_credential(entry) == "orig"


def test_apply_credential_update_empty_blob_is_noop():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.register_task("t1", entry)
    assert pool.apply_credential_update("t1", "", minted_at=1.0) is None
    assert pool.effective_credential(entry) == "orig"


def test_apply_credential_update_for_account_handles_restarted_task_mapping():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    rotated = '{"access_token":"new-access","refresh_token":"new-refresh"}'

    account = pool.apply_credential_update_for_account(
        entry.account_key(),
        rotated,
        minted_at=100.0,
    )

    assert account == entry.account_key()
    assert pool.effective_credential(entry) == rotated


def test_apply_credential_update_for_account_rejects_unknown_account():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])

    assert pool.apply_credential_update_for_account(
        "grok:unknown-account",
        '{"access_token":"new-access","refresh_token":"new-refresh"}',
        minted_at=100.0,
    ) is None
    assert pool.effective_credential(entry) == "orig"


def test_mark_account_exhausted_handles_restarted_task_mapping():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])

    assert pool.mark_account_exhausted(
        entry.account_key(),
        resets_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    assert pool.available_count == 0
    assert pool.next_entry() is None


def test_mark_account_exhausted_rejects_unknown_account():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])

    assert not pool.mark_account_exhausted(
        "grok:unknown-account",
        resets_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    assert pool.available_count == 1


def test_seed_credential_override_restores_on_startup():
    """A legacy identity override is converted to the matching account."""
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.seed_credential_override(entry.identity(), "restored-blob", minted_at=50.0)
    assert pool.effective_credential(entry) == "restored-blob"


def test_seed_credential_override_restores_account_key_on_startup():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.seed_credential_override(entry.account_key(), "restored-blob", minted_at=50.0)
    assert pool.effective_credential(entry) == "restored-blob"


def test_account_scoped_override_applies_to_model_variants():
    opus = ProviderEntry(provider="grok", credential="orig", model="opus")
    sonnet = ProviderEntry(provider="grok", credential="orig", model="sonnet")
    pool = ProviderPool([opus, sonnet])
    pool.register_task("t1", opus)

    assert pool.apply_credential_update("t1", "rotated-blob", minted_at=100.0) == (
        opus.account_key()
    )

    assert pool.effective_credential(opus) == "rotated-blob"
    assert pool.effective_credential(sonnet) == "rotated-blob"


def test_seed_credential_override_ignores_unknown_key():
    entry = _grok_entry("orig")
    pool = ProviderPool([entry])
    pool.seed_credential_override("identity-from-removed-config", "x", minted_at=1.0)
    assert pool.effective_credential(entry) == "orig"
