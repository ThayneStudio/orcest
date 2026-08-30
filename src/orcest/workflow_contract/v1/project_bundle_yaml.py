"""Strict YAML 1.2 scalar-rule parsing for repository-owned ``.orcest`` documents.

``docs/wiki/repository-configuration.md``, "Parsing and validation" requires
the v1 parser to:

- use safe YAML 1.2 scalar rules;
- reject duplicate mapping keys, merge keys, aliases/anchors, explicit tags,
  floats, null where not declared, and every unknown key;
- accept only UTF-8 without NUL bytes;
- enforce per-file and total-bundle size limits before parsing.

This module implements the byte-level and YAML-level half of that contract
(everything up to "a plain Python dict/list/str/int/bool/None tree"). Field
presence, closed-schema, and default materialization live in
:mod:`orcest.workflow_contract.v1.project_bundle`, which consumes this
module's output.
"""

from __future__ import annotations

from typing import Callable

import yaml
import yaml.composer
import yaml.constructor

__all__ = [
    "YamlParseError",
    "MAX_DOCUMENT_BYTES",
    "MAX_BUNDLE_TOTAL_BYTES",
    "load_strict_yaml_document",
]


class YamlParseError(ValueError):
    """Raised when a ``.orcest`` document fails strict byte/YAML-level parsing."""

    def __init__(self, code: str, message: str, *, file: str):
        self.code = code
        self.file = file
        super().__init__(message)


# Local pre-parse safety ceilings. These are this CLI's own defensive limits,
# not the server's authoritative per-file/total-bundle limits (which are
# resolved at project registration, #674) -- they exist so a pathological
# input fails fast and closed rather than exhausting memory during parsing.
MAX_DOCUMENT_BYTES = 1_000_000
MAX_BUNDLE_TOTAL_BYTES = 8_000_000


def _reject_tag(tag_name: str) -> "Callable[[yaml.SafeLoader, yaml.Node], None]":
    def _constructor(loader: yaml.SafeLoader, node: yaml.Node) -> None:
        raise yaml.constructor.ConstructorError(
            None,
            None,
            f"{tag_name} values are not allowed in a v1 bundle document",
            node.start_mark,
        )

    return _constructor


def _no_duplicate_no_merge_construct_mapping(
    loader: yaml.SafeLoader, node: yaml.Node, deep: bool = False
) -> dict:
    if not isinstance(node, yaml.MappingNode):
        raise yaml.constructor.ConstructorError(
            None, None, f"expected a mapping node, got {type(node).__name__}", node.start_mark
        )
    mapping: dict = {}
    for key_node, value_node in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "merge keys ('<<') are not allowed",
                key_node.start_mark,
            )
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"mapping keys must be strings, got {type(key).__name__}",
                key_node.start_mark,
            )
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"duplicate mapping key {key!r}",
                key_node.start_mark,
            )
        value = loader.construct_object(value_node, deep=deep)
        mapping[key] = value
    return mapping


def _no_alias_compose_node(
    self: yaml.composer.Composer, parent: yaml.Node | None, index: int
) -> yaml.Node | None:
    # The Loader class multiply-inherits Parser directly (there is no
    # separate `self.parser`); `check_event`/`peek_event` are self's own.
    if self.check_event(yaml.AliasEvent):  # type: ignore[attr-defined]
        event = self.peek_event()  # type: ignore[attr-defined]
        raise yaml.composer.ComposerError(
            None, None, "aliases/anchors are not allowed in a v1 bundle document", event.start_mark
        )
    return _ORIGINAL_COMPOSE_NODE(self, parent, index)


class _StrictLoader(yaml.SafeLoader):
    """A YAML 1.2-scalar-rule loader closed against the v1 parser's reject list."""


_ORIGINAL_COMPOSE_NODE = yaml.composer.Composer.compose_node
_StrictLoader.compose_node = _no_alias_compose_node  # type: ignore[method-assign,assignment]
_StrictLoader.add_constructor("tag:yaml.org,2002:map", _no_duplicate_no_merge_construct_mapping)
for _rejected_tag in (
    "tag:yaml.org,2002:float",
    "tag:yaml.org,2002:timestamp",
    "tag:yaml.org,2002:binary",
    "tag:yaml.org,2002:set",
    "tag:yaml.org,2002:omap",
    "tag:yaml.org,2002:pairs",
):
    _StrictLoader.add_constructor(_rejected_tag, _reject_tag(_rejected_tag.rsplit(":", 1)[-1]))


def load_strict_yaml_document(raw_bytes: bytes, *, file: str) -> object:
    """Parse exactly one YAML document from ``raw_bytes`` under the v1 reject list.

    Returns a plain ``dict``/``list``/``str``/``int``/``bool``/``None`` tree
    (or a bare scalar). Raises :class:`YamlParseError` with a stable ``code``
    and ``file`` on any violation -- never on ambiguity silently accepted.
    """
    if len(raw_bytes) > MAX_DOCUMENT_BYTES:
        raise YamlParseError(
            "DOCUMENT_TOO_LARGE",
            f"{file}: exceeds the {MAX_DOCUMENT_BYTES}-byte per-file limit",
            file=file,
        )
    if b"\x00" in raw_bytes:
        raise YamlParseError("NUL_BYTE_REJECTED", f"{file}: contains a NUL byte", file=file)
    try:
        text = raw_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise YamlParseError("NOT_UTF8", f"{file}: is not valid UTF-8 ({exc})", file=file) from exc

    try:
        documents = list(yaml.load_all(text, Loader=_StrictLoader))
    except yaml.YAMLError as exc:
        raise YamlParseError("YAML_INVALID", f"{file}: {exc}", file=file) from exc

    if len(documents) != 1:
        raise YamlParseError(
            "MULTI_DOCUMENT_REJECTED",
            f"{file}: must contain exactly one YAML document, found {len(documents)}",
            file=file,
        )
    return documents[0]
