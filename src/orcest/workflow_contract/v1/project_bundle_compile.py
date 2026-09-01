"""Compile a repository-owned ``.orcest`` bundle into one normalized, hashed object.

Ties together :mod:`orcest.workflow_contract.v1.project_bundle` (schema
parsing) and :mod:`orcest.workflow_contract.v1.project_bundle_source`
(pinned-commit blob access) into the "Normalization and bundle hash"
procedure from ``docs/wiki/repository-configuration.md``:

1. resolve the trusted commit once and read every referenced blob at it;
2. validate and materialize both documents' defaults;
3. normalize every referenced file's bytes into a Workflow Blob (media kind,
   byte length, domain-separated digest);
4. assemble the ``files`` map and hash the whole normalized object.

Everything server-owned (execution-profile resolution, launch-isolation
mapping, budget policy -- the rest of ``policy_hash``'s inputs) is out of
scope for this leaf; see :func:`compute_policy_hash` for the documented
extension point a later leaf (project registration, #674) composes with.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Any, Mapping

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.canonical import canonical_json_bytes
from orcest.workflow_contract.v1.digest import (
    config_bundle_hash,
    policy_digest,
    workflow_blob_digest,
)
from orcest.workflow_contract.v1.identity import CommitId
from orcest.workflow_contract.v1.project_bundle import (
    BUNDLE_ROOT,
    BundleValidationError,
    Diagnostic,
    ParsedProject,
    ParsedWorkflow,
    parse_project_document,
    parse_workflow_document,
    scan_mapping_for_secret_key_names,
    scan_prompt_bytes_for_secret,
)
from orcest.workflow_contract.v1.project_bundle_source import (
    GitBundleSource,
    GitSourceError,
    TreeEntry,
)
from orcest.workflow_contract.v1.project_bundle_yaml import (
    MAX_BUNDLE_TOTAL_BYTES,
    MAX_DOCUMENT_BYTES,
)

__all__ = [
    "DEFAULT_PROJECT_PATH",
    "CompiledFile",
    "CompiledBundle",
    "compile_bundle",
    "compute_policy_hash",
]

DEFAULT_PROJECT_PATH = f"{BUNDLE_ROOT}/project.yaml"


class _Diag(list):
    def add(self, code: str, message: str, *, file: str, path: str) -> None:
        self.append(Diagnostic(code=code, message=message, file=file, path=path))

    def raise_if_any(self) -> None:
        if self:
            raise BundleValidationError(list(self))


def _check_no_case_collisions(source: GitBundleSource, *, diags: _Diag) -> None:
    entries: list[TreeEntry] = source.list_tree_recursive(BUNDLE_ROOT)
    seen: dict[str, str] = {}
    for entry in entries:
        lowered = entry.path.lower()
        if lowered in seen and seen[lowered] != entry.path:
            diags.add(
                "CASE_COLLISION",
                f"{entry.path!r} collides case-insensitively with {seen[lowered]!r}",
                file=BUNDLE_ROOT,
                path=entry.path,
            )
        else:
            seen[lowered] = entry.path


def _normalize_prompt_text(raw_bytes: bytes, *, file: str, diags: _Diag) -> bytes | None:
    if len(raw_bytes) > MAX_DOCUMENT_BYTES:
        diags.add(
            "DOCUMENT_TOO_LARGE",
            f"exceeds the {MAX_DOCUMENT_BYTES}-byte per-file limit",
            file=file,
            path="$",
        )
        return None
    if b"\x00" in raw_bytes:
        diags.add("NUL_BYTE_REJECTED", "contains a NUL byte", file=file, path="$")
        return None
    try:
        text = raw_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        diags.add("NOT_UTF8", f"is not valid UTF-8 ({exc})", file=file, path="$")
        return None

    if text.startswith("﻿"):
        text = text[1:]
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = unicodedata.normalize("NFC", text)
    text = text.rstrip("\n") + "\n" if text.strip("\n") or text else "\n"

    secret_diags = scan_prompt_bytes_for_secret(text, file=file)
    diags.extend(secret_diags)
    if secret_diags:
        return None
    return text.encode("utf-8")


@dataclass(frozen=True, slots=True)
class CompiledFile:
    path: str
    blob_id: str
    media_kind: str
    byte_length: int
    digest: str

    def to_json(self) -> dict[str, Any]:
        return {
            "blob_id": self.blob_id,
            "media_kind": self.media_kind,
            "byte_length": self.byte_length,
            "digest": self.digest,
        }


@dataclass(frozen=True, slots=True)
class CompiledBundle:
    trusted_base_commit: CommitId
    project: dict[str, Any]
    workflow: dict[str, Any]
    files: dict[str, CompiledFile]
    normalized_bundle: dict[str, Any]
    workflow_hash: str


def _fetch_and_register_file(
    source: GitBundleSource,
    path: str,
    *,
    media_kind: str,
    normalized_bytes: bytes,
    files: dict[str, CompiledFile],
) -> None:
    entry = source.ls_tree_entry(path)
    assert entry is not None  # already successfully read via read_regular_blob
    files[path] = CompiledFile(
        path=path,
        blob_id=entry.oid,
        media_kind=media_kind,
        byte_length=len(normalized_bytes),
        digest=workflow_blob_digest(media_kind, normalized_bytes),
    )


def compile_bundle(
    source: GitBundleSource, *, project_path: str = DEFAULT_PROJECT_PATH
) -> CompiledBundle:
    diags = _Diag()

    _check_no_case_collisions(source, diags=diags)

    try:
        project_bytes = source.read_regular_blob(project_path, max_bytes=MAX_DOCUMENT_BYTES)
    except GitSourceError as exc:
        diags.add(exc.code, str(exc), file=project_path, path="$")
        diags.raise_if_any()
        raise AssertionError("unreachable")

    parsed_project: ParsedProject = parse_project_document(project_bytes, file=project_path)
    scan_mapping_for_secret_key_names(
        parsed_project.materialized, path="$", diags=_ProjectDiagsAdapter(diags, project_path)
    )

    workflow_path = parsed_project.workflow_path
    if workflow_path == project_path:
        diags.add(
            "CYCLIC_REFERENCE",
            "spec.workflow must not reference project.yaml itself",
            file=project_path,
            path="$.spec.workflow",
        )
        diags.raise_if_any()

    try:
        workflow_bytes = source.read_regular_blob(workflow_path, max_bytes=MAX_DOCUMENT_BYTES)
    except GitSourceError as exc:
        diags.add(exc.code, str(exc), file=workflow_path, path="$")
        diags.raise_if_any()
        raise AssertionError("unreachable")

    parsed_workflow: ParsedWorkflow = parse_workflow_document(workflow_bytes, file=workflow_path)
    scan_mapping_for_secret_key_names(
        parsed_workflow.materialized, path="$", diags=_ProjectDiagsAdapter(diags, workflow_path)
    )

    for prompt_path in parsed_workflow.referenced_prompt_paths:
        if prompt_path in (project_path, workflow_path):
            diags.add(
                "CYCLIC_REFERENCE",
                f"a prompt must not reference {prompt_path!r}, which is a config document",
                file=workflow_path,
                path="$",
            )
    diags.raise_if_any()

    files: dict[str, CompiledFile] = {}
    total_normalized_bytes = 0

    project_normalized = canonical_json_bytes(parsed_project.materialized)
    total_normalized_bytes += len(project_normalized)
    _fetch_and_register_file(
        source,
        project_path,
        media_kind=enums.WorkflowBlobMediaKind.CONFIG_JSON.value,
        normalized_bytes=project_normalized,
        files=files,
    )

    workflow_normalized = canonical_json_bytes(parsed_workflow.materialized)
    total_normalized_bytes += len(workflow_normalized)
    _fetch_and_register_file(
        source,
        workflow_path,
        media_kind=enums.WorkflowBlobMediaKind.CONFIG_JSON.value,
        normalized_bytes=workflow_normalized,
        files=files,
    )

    for prompt_path in parsed_workflow.referenced_prompt_paths:
        try:
            raw_bytes = source.read_regular_blob(prompt_path, max_bytes=MAX_DOCUMENT_BYTES)
        except GitSourceError as exc:
            diags.add(exc.code, str(exc), file=prompt_path, path="$")
            continue
        normalized = _normalize_prompt_text(raw_bytes, file=prompt_path, diags=diags)
        if normalized is None:
            continue
        total_normalized_bytes += len(normalized)
        _fetch_and_register_file(
            source,
            prompt_path,
            media_kind=enums.WorkflowBlobMediaKind.PROMPT_UTF8.value,
            normalized_bytes=normalized,
            files=files,
        )

    if total_normalized_bytes > MAX_BUNDLE_TOTAL_BYTES:
        diags.add(
            "BUNDLE_TOO_LARGE",
            f"total normalized bundle size {total_normalized_bytes} exceeds the "
            f"{MAX_BUNDLE_TOTAL_BYTES}-byte local limit",
            file=BUNDLE_ROOT,
            path="$",
        )

    diags.raise_if_any()

    normalized_bundle: dict[str, Any] = {
        "orcest-config-bundle": "v1",
        "trusted_base_commit": source.commit.to_json(),
        "project": parsed_project.materialized,
        "workflow": parsed_workflow.materialized,
        "files": {path: files[path].to_json() for path in sorted(files)},
    }
    workflow_hash = config_bundle_hash(normalized_bundle)

    return CompiledBundle(
        trusted_base_commit=source.commit,
        project=parsed_project.materialized,
        workflow=parsed_workflow.materialized,
        files=files,
        normalized_bundle=normalized_bundle,
        workflow_hash=workflow_hash,
    )


def compute_policy_hash(*, workflow_hash: str, server_policy: Mapping[str, Any]) -> str:
    """The effective ``policy_hash`` combining this leaf's ``workflow_hash`` with
    server-resolved policy (execution-profile resolutions, launch-isolation
    mapping, budget policy, etc.) that project registration (#674) supplies.

    This function is the one fixed digest boundary #674 must call rather than
    hand-rolling its own hash; the shape of ``server_policy`` itself is that
    leaf's responsibility to assemble from the wiki's "Normalization and
    bundle hash" `policy_hash` field list.
    """
    return policy_digest({"workflow_hash": workflow_hash, "server_policy": dict(server_policy)})


class _ProjectDiagsAdapter:
    """Adapt :mod:`project_bundle`'s per-file ``_Diagnostics`` protocol to this module's
    ``_Diag``.
    """

    def __init__(self, diags: _Diag, file: str):
        self._diags = diags
        self._file = file

    def add(self, code: str, message: str, path: str) -> None:
        self._diags.add(code, message, file=self._file, path=path)
