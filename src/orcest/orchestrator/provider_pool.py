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
        self._cooldowns: dict[str, datetime] = {}  # identity -> UTC expiry
        self._task_identities: dict[str, str] = {}  # task_id -> identity
        self._identity_to_entry: dict[str, ProviderEntry] = {e.identity(): e for e in entries}
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
        """Number of entries not currently on cooldown."""
        with self._lock:
            now = datetime.now(timezone.utc)
            active = sum(1 for exp in self._cooldowns.values() if exp > now)
            return self.size - active

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
                ident = entry.identity()
                if ident not in self._cooldowns:
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

            default = datetime.now(timezone.utc) + timedelta(minutes=30)
            candidate = resets_at or cooldown_until or default

            existing = self._cooldowns.get(ident)
            if existing is not None and existing > candidate:
                expiry = existing
            else:
                expiry = candidate

            self._cooldowns[ident] = expiry

            entry = self._identity_to_entry.get(ident)
            prov = entry.provider if entry else "?"
            logger.info(
                "Provider %s (id=%s) benched until %s",
                prov,
                ident,
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
