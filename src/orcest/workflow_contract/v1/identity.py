"""Identity primitives from the "Representation conventions" section.

- Controller-generated opaque identifiers MUST be lowercase canonical UUIDv4
  strings.
- External identifiers MUST be stored as non-empty opaque strings.
- Commit identifiers MUST be represented by ``{object_format, oid}``. v1 MUST
  support ``sha1``; it MUST NOT assume every Git repository uses SHA-1.
- Counters/generations are unsigned integers starting at 1 unless stated
  otherwise, and only increase.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = [
    "LOWERCASE_UUID_RE",
    "is_lowercase_uuid",
    "require_lowercase_uuid",
    "is_nonempty_opaque_string",
    "require_nonempty_opaque_string",
    "ObjectFormat",
    "CommitId",
]

LOWERCASE_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def is_lowercase_uuid(value: object) -> bool:
    """Return True if ``value`` is a lowercase canonical UUID string.

    v1 does not require the UUID version nibble to be ``4``: it requires the
    *representation* (lowercase, hyphenated, canonical form) used by every
    controller-generated opaque identifier.
    """
    return isinstance(value, str) and bool(LOWERCASE_UUID_RE.fullmatch(value))


def require_lowercase_uuid(value: object, *, field: str = "id") -> str:
    if not is_lowercase_uuid(value):
        raise ValueError(f"{field} must be a lowercase canonical UUID string, got {value!r}")
    assert isinstance(value, str)
    return value


def is_nonempty_opaque_string(value: object) -> bool:
    """External identifiers MUST be non-empty opaque strings, never coerced to int."""
    return isinstance(value, str) and len(value) > 0


def require_nonempty_opaque_string(value: object, *, field: str = "id") -> str:
    if not is_nonempty_opaque_string(value):
        raise ValueError(f"{field} must be a non-empty opaque string, got {value!r}")
    assert isinstance(value, str)
    return value


# Code-owned closed set of supported Git object formats. v1 MUST support
# sha1 and MUST NOT assume every repository uses it -- adding a format here
# (e.g. sha256) is the sole authority for what a CommitId may declare.
class ObjectFormat:
    SHA1 = "sha1"

    ALL = frozenset({SHA1})


@dataclass(frozen=True, slots=True)
class CommitId:
    """A ``{object_format, oid}`` commit identity per the representation conventions."""

    object_format: str
    oid: str

    def __post_init__(self) -> None:
        if self.object_format not in ObjectFormat.ALL:
            raise ValueError(
                f"unsupported object_format {self.object_format!r}; "
                f"known formats are {sorted(ObjectFormat.ALL)!r}"
            )
        if self.object_format == ObjectFormat.SHA1:
            if not re.fullmatch(r"[0-9a-f]{40}", self.oid):
                raise ValueError(f"sha1 oid must be 40 lowercase hex chars, got {self.oid!r}")
        elif not isinstance(self.oid, str) or not self.oid:
            raise ValueError(f"oid must be a non-empty string, got {self.oid!r}")

    def to_json(self) -> dict:
        return {"object_format": self.object_format, "oid": self.oid}

    @classmethod
    def from_json(cls, value: dict) -> "CommitId":
        if not isinstance(value, dict) or set(value) != {"object_format", "oid"}:
            raise ValueError(f"CommitId must be exactly {{object_format, oid}}, got {value!r}")
        return cls(object_format=value["object_format"], oid=value["oid"])
