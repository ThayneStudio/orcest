"""Canonical JSON serialization for Workflow-Control v1.

Implements the "Representation conventions" section of
``docs/wiki/domain-model.md``: structured payloads that are hashed or
compared for byte-identity MUST be normalized as UTF-8 JSON with sorted
object keys, no insignificant whitespace, LF newlines, and Unicode
normalized to NFC before hashing.

Every digest helper in :mod:`orcest.workflow_contract.v1.digest` builds its
preimage from this module's output. Feature code MUST NOT hand-roll
``json.dumps`` for anything that is later hashed or compared for replay
identity; it must go through :func:`canonical_json_bytes`.
"""

from __future__ import annotations

import json
import unicodedata
from typing import Any

__all__ = ["normalize_value", "canonical_json_text", "canonical_json_bytes"]


def _normalize_string(value: str) -> str:
    # CRLF/CR line endings are normalized to LF before NFC normalization so
    # that byte-identical semantic content hashes identically regardless of
    # the platform/editor that produced it.
    return unicodedata.normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))


def normalize_value(value: Any) -> Any:
    """Recursively normalize strings (NFC, LF) within a JSON-compatible value.

    Dict key order is not touched here; ``json.dumps(..., sort_keys=True)``
    in :func:`canonical_json_text` performs the sort. This function only
    normalizes string content and validates that the value is JSON-safe
    (no floats' NaN/Infinity, no non-string keys).
    """
    if isinstance(value, str):
        return _normalize_string(value)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int,)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("canonical JSON forbids NaN/Infinity float values")
        return value
    if value is None:
        return None
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"canonical JSON object keys must be str, got {type(key)!r}")
            normalized[_normalize_string(key)] = normalize_value(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    raise TypeError(f"value of type {type(value)!r} is not canonical-JSON-safe")


def canonical_json_text(value: Any) -> str:
    """Return the canonical JSON text for ``value``.

    Sorted object keys, compact separators (no insignificant whitespace),
    ``ensure_ascii=False`` (UTF-8 output, not ``\\uXXXX`` escapes), and no
    NaN/Infinity.
    """
    normalized = normalize_value(value)
    return json.dumps(
        normalized,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_json_bytes(value: Any) -> bytes:
    """Return the canonical UTF-8 JSON bytes for ``value``, ready for hashing."""
    return canonical_json_text(value).encode("utf-8")
