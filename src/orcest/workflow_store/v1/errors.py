"""Fail-closed errors for durable object stores.

Messages MUST NOT include secret values, integrity tags, or raw object
payloads. Callers pass static, identity-only context (ids, versions, digests,
paths that do not contain secret bytes).
"""

from __future__ import annotations


class DurableStoreError(Exception):
    """Base error for Workflow Blob, Candidate, and Secret stores."""


class LayoutError(DurableStoreError):
    """Control-root layout, mode, or symlink invariant failed."""


class StorageLockError(DurableStoreError):
    """``storage.lock`` could not be acquired or released safely."""


class QuotaExceededError(DurableStoreError):
    """Object rejected before any durable bytes were accepted."""


class IntegrityConflictError(DurableStoreError):
    """No-clobber or keyed-integrity mismatch; existing bytes are unchanged."""


class ObjectNotFoundError(DurableStoreError):
    """Exact-object read/verify named an object that is not installed."""
