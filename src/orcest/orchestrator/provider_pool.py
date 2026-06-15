"""Hardened ProviderPool: concurrency-safe round-robin with stable non-secret identities.

Generalizes the legacy TokenPool for multi-provider support (ProviderEntry).

CRITICAL BOUNDARY (non-negotiable):
- This class and all its callers in the orchestrator ONLY ever touch the lean
  surface: provider, credential, model, and identity().
- NEVER reads cli_binary, env_var, extras, or calls effective_* helpers.
- All tracking (cooldowns, in-flight, round-robin) uses the stable identity()
  string (hash of credential + provider + model) as the key. Never raw secrets.
- When created via from_claude_tokens, rich execution fields are left None.

Internal synchronization uses threading.RLock so concurrent next/register/mark
from any threads (test stress, future parallel poll, etc.) are safe.
max(existing, new) expiry prevents a late "sooner reset" from un-benching a
token that is still rate-limited according to a previous observation.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

from orcest.shared.providers import ProviderEntry

logger = logging.getLogger(__name__)


class ProviderPool:
    """Round-robin pool of ProviderEntry objects with per-identity exhaustion cooldowns.

    TOCTOU / NO-RESERVATION CONTRACT:
    next_entry() returns a *snapshot* of an available entry at the instant of
    the call. It performs no reservation or atomic claim. A concurrent caller
    may receive the same entry (or the entry may be marked exhausted by a
    racing mark_exhausted) before the original caller registers or publishes.
    Callers MUST tolerate None returns and MUST use the register-before-publish
    + task_completed rollback pattern to avoid leaking in-flight mappings on
    publish failure. See next_entry and the usage example below.

    Usage (new path)::

        pool = ProviderPool([ProviderEntry("claude", "tok1"), ProviderEntry("grok", "g1")])
        entry = pool.next_entry()
        if entry:
            pool.register_task(task.id, entry)  # before or after publish
            ... publish using entry.provider / entry.credential / entry.model ...
        ...
        pool.mark_exhausted(task.id, resets_at=reset_dt)  # or task_completed

    Legacy shims are provided so that the claude_tokens migration path in
    loop.py can swap the pool instance without changing every call site yet.
    """

    def __init__(self, entries: list[ProviderEntry]) -> None:
        if not entries:
            raise ValueError("ProviderPool requires at least one entry")
        # Validate uniqueness by stable identity (never by raw credential)
        seen: dict[str, ProviderEntry] = {}
        for e in entries:
            ident = e.identity()
            if ident in seen:
                raise ValueError(f"Duplicate provider identity in pool: {ident}")
            seen[ident] = e

        self._entries: list[ProviderEntry] = list(entries)  # order preserved for RR
        self._counter: int = 0
        # Cooldowns are keyed by ACCOUNT (provider + credential hash), NOT by
        # identity(): rate limits are per-account, so benching an account benches
        # every model-entry that shares its credential. See ProviderEntry.account_key.
        self._cooldowns: dict[str, datetime] = {}  # account_key -> UTC expiry
        self._task_identities: dict[str, str] = {}  # task_id -> identity
        self._identity_to_entry: dict[str, ProviderEntry] = {e.identity(): e for e in entries}
        self._identity_to_account: dict[str, str] = {
            e.identity(): e.account_key() for e in entries
        }
        # Credential write-back overrides (OAuth-blob providers like Grok/Codex).
        # account_key -> (latest_blob, minted_at). OAuth refresh tokens are
        # account-scoped, not model/project scoped, so the override must follow
        # the provider account rather than a model-inclusive identity().
        # effective_credential() consults this so publishes use the latest blob
        # without changing the entry's stable identity.
        # Persistence to Redis lives in the orchestrator, not here (this class
        # stays Redis-free per the pool boundary).
        self._credential_overrides: dict[str, tuple[str, float]] = {}
        self._lock = threading.RLock()

    @classmethod
    def from_claude_tokens(cls, tokens: list[str]) -> "ProviderPool":
        """Migration helper: synthesize lean ProviderEntry objects for legacy claude_tokens.

        Rich fields (cli_binary, env_var, extras) are deliberately left as
        None/defaults per the Provider Registration & Invocation Boundary.
        """
        entries = [
            ProviderEntry(
                provider="claude",
                credential=t,
                model=None,
                # execution recipe fields intentionally omitted (None / default)
            )
            for t in tokens
        ]
        return cls(entries)

    @property
    def size(self) -> int:
        """Total entries in the pool."""
        return len(self._entries)

    @property
    def available_count(self) -> int:
        """Number of entries whose ACCOUNT is not currently on cooldown.

        Counts entries (not accounts): a benched account removes every one of
        its model-entries from availability.
        """
        with self._lock:
            now = datetime.now(timezone.utc)
            benched = {k for k, exp in self._cooldowns.items() if exp > now}
            return sum(1 for e in self._entries if e.account_key() not in benched)

    @property
    def provider_names(self) -> list[str]:
        """Return the provider names (lean surface only: no cli_binary/env_var/extras).
        Used for per-provider observability counters (exhausted_skip, rebake failures).
        """
        with self._lock:
            # unique preserving order of first appearance
            seen: list[str] = []
            for e in self._entries:
                if e.provider not in seen:
                    seen.append(e.provider)
            return seen

    def _prune_cooldowns(self) -> None:
        """Remove expired cooldown entries. Must be called while holding self._lock."""
        now = datetime.now(timezone.utc)
        self._cooldowns = {i: exp for i, exp in self._cooldowns.items() if exp > now}

    def next_entry(self) -> ProviderEntry | None:
        """Return next available entry (round-robin, skipping exhausted).

        CRITICAL CONTRACT — TOCTOU, no reservation:
        The returned entry (if any) is only guaranteed available *at the moment
        of return*. There is no lock held across the call, no reservation made,
        and no prevention of the same identity being handed to another caller
        (or being benched) before the caller acts on it. This is intentional
        for lock-free concurrent use; the design relies on best-effort + the
        register/task_completed safety net.

        Call register_task (before or after publish) to associate the task_id
        for later mark_exhausted. Always handle the None case (all exhausted).
        Returns None if every entry is currently cooled down.
        """
        with self._lock:
            self._prune_cooldowns()

            n = len(self._entries)
            for _ in range(n):
                idx = self._counter % n
                self._counter += 1
                entry = self._entries[idx]
                if entry.account_key() not in self._cooldowns:
                    return entry
            return None

    # ------------------------------------------------------------------
    # Primary (lean) registration / exhaustion API
    # ------------------------------------------------------------------

    def register_task(self, task_id: str, entry: ProviderEntry | str) -> None:
        """Record the identity used for *task_id* (for later mark_exhausted).

        Accepts either a ProviderEntry (preferred, new path) or a raw credential
        string (legacy shim for claude_tokens migration).  The mapping uses the
        stable identity() string internally.
        """
        with self._lock:
            if isinstance(entry, str):
                # Legacy path (claude-only pools during transition): lookup by credential
                # (safe because legacy TokenPool uniqueness was on the token strings)
                for ident, e in self._identity_to_entry.items():
                    if e.credential == entry:
                        self._task_identities[task_id] = ident
                        return
                logger.warning(
                    "register_task: credential not found in pool for task %s (legacy path)",
                    task_id,
                )
                return

            # New path: entry object (lean surface only)
            ident = entry.identity()
            if ident in self._identity_to_entry:
                self._task_identities[task_id] = ident
            else:
                logger.warning(
                    "register_task: entry identity %s not in pool for task %s",
                    ident,
                    task_id,
                )

    def mark_exhausted(
        self,
        task_id: str,
        resets_at: datetime | None = None,
        cooldown_until: datetime | None = None,  # legacy kwarg alias
    ) -> None:
        """Mark the entry that served *task_id* exhausted until the given time.

        Uses max(existing_expiry, new_expiry) so that a later-observed longer
        cooldown always wins (prevents premature re-use after a partial view).
        Safe to call multiple times (duplicate USAGE results etc.).
        """
        with self._lock:
            ident = self._task_identities.pop(task_id, None)
            if ident is None:
                return

            # Bench by ACCOUNT, not identity: a rate-limited account is benched
            # regardless of which model entry served the task.
            account = self._identity_to_account.get(ident)
            if account is None:
                return

            default = datetime.now(timezone.utc) + timedelta(minutes=30)
            candidate = resets_at or cooldown_until or default

            existing = self._cooldowns.get(account)
            if existing is not None and existing > candidate:
                expiry = existing
            else:
                expiry = candidate

            self._cooldowns[account] = expiry

            entry = self._identity_to_entry.get(ident)
            prov = entry.provider if entry else "?"
            logger.info(
                "Provider %s (account=%s) benched until %s",
                prov,
                account,
                expiry.isoformat(),
            )

    def task_completed(self, task_id: str) -> None:
        """Release any in-flight mapping for a finished (non-exhausted) task."""
        with self._lock:
            self._task_identities.pop(task_id, None)

    def get_task_entry(self, task_id: str) -> ProviderEntry | None:
        """Return the ProviderEntry used by *task_id*, or None."""
        with self._lock:
            ident = self._task_identities.get(task_id)
            return self._identity_to_entry.get(ident) if ident else None

    # ------------------------------------------------------------------
    # Credential write-back (OAuth-blob providers: Grok, Codex)
    # ------------------------------------------------------------------

    def effective_credential(self, entry: ProviderEntry) -> str:
        """Credential to actually publish for *entry*: the latest rotated blob
        if one was written back, else the original config credential.

        The entry's identity() (and thus all pool tracking) stays anchored to
        the original config credential; only the published value differs.
        """
        with self._lock:
            override = self._credential_overrides.get(entry.account_key())
            return override[0] if override else entry.credential

    def apply_credential_update(self, task_id: str, blob: str, minted_at: float) -> str | None:
        """Record a rotated credential blob reported for *task_id*.

        Keyed by the task's provider account (provider + credential hash).
        Last-write-wins by ``minted_at`` (a stale, out-of-order update is
        ignored). Returns the account key if stored (so the caller can persist to
        Redis), else None (no mapping for the task, empty blob, or stale).
        """
        if not blob:
            return None
        with self._lock:
            ident = self._task_identities.get(task_id)
            if ident is None or ident not in self._identity_to_entry:
                return None
            account = self._identity_to_account.get(ident)
            if account is None:
                return None
            existing = self._credential_overrides.get(account)
            if existing is not None and existing[1] >= minted_at:
                return None  # stale / duplicate
            self._credential_overrides[account] = (blob, minted_at)
            return account

    def seed_credential_override(self, key: str, blob: str, minted_at: float) -> None:
        """Load a persisted credential override at startup.

        ``key`` may be the current account key (``provider:<credential-hash>``)
        or a legacy model-inclusive identity. Legacy keys are converted to the
        corresponding account key so existing persisted overrides continue to
        work after the account-scoped migration.
        """
        if not blob:
            return
        if key in self._identity_to_entry:
            account = self._identity_to_account.get(key)
        else:
            account_keys = set(self._identity_to_account.values())
            account = key if key in account_keys else None
        if account is None:
            return
        with self._lock:
            existing = self._credential_overrides.get(account)
            if existing is not None and existing[1] >= minted_at:
                return
            self._credential_overrides[account] = (blob, minted_at)

    # ------------------------------------------------------------------
    # Legacy shims (so claude_tokens call sites continue to work after swap-in)
    # These will be removed after full Task 5 wiring.
    # ------------------------------------------------------------------

    def next_token(self) -> str | None:
        """Legacy shim returning raw credential (for claude path only)."""
        entry = self.next_entry()
        return entry.credential if entry else None

    def get_task_token(self, task_id: str) -> str | None:
        """Legacy shim returning raw credential for the task."""
        entry = self.get_task_entry(task_id)
        return entry.credential if entry else None

    # mark_exhausted already accepts the old cooldown_until kwarg name.

    def __repr__(self) -> str:
        with self._lock:
            cooled = len([e for e in self._cooldowns.values() if e > datetime.now(timezone.utc)])
            return (
                f"ProviderPool(size={self.size}, available={self.available_count}, "
                f"cooled={cooled}, identities={[e.identity() for e in self._entries]})"
            )
