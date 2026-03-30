"""Round-robin Claude OAuth token pool with per-token exhaustion tracking.

Distributes tasks across multiple tokens and temporarily removes tokens
that hit usage limits until their reset window expires.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class TokenPool:
    """Manages a pool of Claude OAuth tokens with round-robin and cooldowns.

    Usage::

        pool = TokenPool(["token-a", "token-b"])
        token = pool.next_token()           # "token-a"
        # ... publish task, get back task.id ...
        pool.register_task(task.id, token)   # track which task uses which token
        # ... later, on USAGE_EXHAUSTED result ...
        pool.mark_exhausted(task.id, cooldown_until=reset_time)
    """

    def __init__(self, tokens: list[str]) -> None:
        if not tokens:
            raise ValueError("TokenPool requires at least one token")
        if len(set(tokens)) != len(tokens):
            raise ValueError("TokenPool tokens must be unique")
        self._tokens = list(tokens)
        self._counter = 0
        self._cooldowns: dict[int, datetime] = {}  # index -> expiry (UTC)
        self._task_tokens: dict[str, int] = {}  # task_id -> token index
        # Reverse map for register_task (token value -> index)
        self._token_index: dict[str, int] = {t: i for i, t in enumerate(self._tokens)}

    @property
    def size(self) -> int:
        """Total number of tokens in the pool."""
        return len(self._tokens)

    @property
    def available_count(self) -> int:
        """Number of tokens not currently on cooldown."""
        now = datetime.now(timezone.utc)
        active_cooldowns = sum(1 for exp in self._cooldowns.values() if exp > now)
        return self.size - active_cooldowns

    def next_token(self) -> str | None:
        """Return the next available token, skipping exhausted ones.

        Call :meth:`register_task` after publishing to track the task→token mapping.

        Returns ``None`` if all tokens are currently on cooldown.
        """
        now = datetime.now(timezone.utc)
        # Expire stale cooldowns
        self._cooldowns = {i: exp for i, exp in self._cooldowns.items() if exp > now}

        for _ in range(len(self._tokens)):
            idx = self._counter % len(self._tokens)
            self._counter += 1
            if idx not in self._cooldowns:
                return self._tokens[idx]

        return None  # All tokens exhausted

    def register_task(self, task_id: str, token: str) -> None:
        """Record which token was used for a task (for later exhaustion tracking)."""
        idx = self._token_index.get(token)
        if idx is not None:
            self._task_tokens[task_id] = idx
        else:
            logger.warning("register_task: token not in pool for task %s", task_id)

    def mark_exhausted(
        self,
        task_id: str,
        cooldown_until: datetime | None = None,
    ) -> None:
        """Mark the token used by *task_id* as exhausted.

        Args:
            task_id: The task that triggered the exhaustion.
            cooldown_until: UTC datetime when the token becomes available again.
                Defaults to 30 minutes from now if not specified.
        """
        idx = self._task_tokens.pop(task_id, None)
        if idx is None:
            return
        default = datetime.now(timezone.utc) + timedelta(minutes=30)
        expiry = cooldown_until or default
        self._cooldowns[idx] = expiry
        # Mask token for logging (show first 10 chars)
        masked = self._tokens[idx][:10] + "..."
        logger.info(
            "Token %s (#%d) benched until %s",
            masked,
            idx,
            expiry.isoformat(),
        )

    def get_task_token(self, task_id: str) -> str | None:
        """Return the token that was assigned to *task_id*, or None."""
        idx = self._task_tokens.get(task_id)
        return self._tokens[idx] if idx is not None else None

    def task_completed(self, task_id: str) -> None:
        """Clean up tracking state for a completed/failed task."""
        self._task_tokens.pop(task_id, None)
