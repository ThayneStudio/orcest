"""Read-only GitHub issue delivery verification.

This module proves issue handoff from live GitHub state only.  Worker summaries
and PR body text are deliberately ignored; GitHub's canonical
``closingIssuesReferences`` relation is the authority.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from orcest.orchestrator import gh
from orcest.orchestrator.gh import GhCliError, GhNotInstalledError, GhRateLimitError

_DEFAULT_PAGE_CAP = 50
_PAGE_SIZE = 100
_OID_RE = re.compile(r"^[0-9a-fA-F]{40,64}$")


class DeliveryErrorKind(str, Enum):
    """Typed failure classes for read-only delivery verification."""

    NONE = "none"
    TRANSPORT = "transport"
    RATE_LIMIT = "rate_limit"
    AUTHENTICATION = "authentication"
    PERMISSION = "permission"
    SCHEMA = "schema"
    COMPLETENESS = "completeness"
    NOT_FOUND = "not_found"
    MISMATCH = "mismatch"


class DeliveryFailureReason(str, Enum):
    """Specific unverifiable outcomes."""

    VERIFIED = "verified"
    GH_NOT_INSTALLED = "gh_not_installed"
    GH_TRANSPORT_ERROR = "gh_transport_error"
    GH_RATE_LIMITED = "gh_rate_limited"
    AUTHENTICATION_FAILED = "authentication_failed"
    PERMISSION_DENIED = "permission_denied"
    GRAPHQL_ERRORS = "graphql_errors"
    MALFORMED_RESPONSE = "malformed_response"
    NULL_FIELD = "null_field"
    TRUNCATED_FIELD = "truncated_field"
    MISSING_CURSOR = "missing_cursor"
    PAGE_CAP_EXCEEDED = "page_cap_exceeded"
    REPOSITORY_NOT_FOUND = "repository_not_found"
    DEFAULT_BRANCH_MISMATCH = "default_branch_mismatch"
    HEAD_REF_NOT_FOUND = "head_ref_not_found"
    HEAD_OID_MISMATCH = "head_oid_mismatch"
    NO_CANDIDATE_PR = "no_candidate_pr"
    NO_CANONICAL_CLOSING_REFERENCE = "no_canonical_closing_reference"


@dataclass(frozen=True)
class ClosingIssueReference:
    repository: str
    number: int
    url: str


@dataclass(frozen=True)
class CandidatePullRequest:
    number: int
    url: str
    state: str
    is_draft: bool
    base_repository: str
    base_ref_name: str
    head_repository: str
    head_ref_name: str
    head_oid: str
    closing_issues_references: tuple[ClosingIssueReference, ...]


@dataclass(frozen=True)
class DeliveryVerification:
    verified: bool
    error_kind: DeliveryErrorKind
    reason: DeliveryFailureReason
    repo: str
    issue_number: int
    default_branch: str
    default_branch_oid: str
    expected_head_ref: str
    expected_head_oid: str
    live_head_oid: str
    candidate_prs: tuple[CandidatePullRequest, ...] = field(default_factory=tuple)
    pages_fetched: int = 0
    closing_pages_fetched: int = 0
    complete: bool = False
    message: str = ""

    def to_shadow_fields(self) -> dict[str, str]:
        """Return bounded, secret-free Redis fields."""
        matching = [
            pr
            for pr in self.candidate_prs
            if any(
                ref.repository == self.repo and ref.number == self.issue_number
                for ref in pr.closing_issues_references
            )
        ]
        urls = [pr.url for pr in self.candidate_prs[:10]]
        return {
            "verified": "1" if self.verified else "0",
            "error_kind": self.error_kind.value,
            "reason": self.reason.value,
            "repo": self.repo,
            "issue_number": str(self.issue_number),
            "default_branch": self.default_branch[:120],
            "default_branch_oid": self.default_branch_oid,
            "expected_head_ref": self.expected_head_ref[:240],
            "expected_head_oid": self.expected_head_oid,
            "live_head_oid": self.live_head_oid,
            "candidate_pr_count": str(len(self.candidate_prs)),
            "matching_pr_numbers": ",".join(str(pr.number) for pr in matching[:20]),
            "candidate_pr_urls": json.dumps(urls, separators=(",", ":")),
            "pages_fetched": str(self.pages_fetched),
            "closing_pages_fetched": str(self.closing_pages_fetched),
            "complete": "1" if self.complete else "0",
            "message": self.message[:500],
        }


class _VerificationAbort(Exception):
    def __init__(
        self,
        kind: DeliveryErrorKind,
        reason: DeliveryFailureReason,
        message: str,
    ):
        super().__init__(message)
        self.kind = kind
        self.reason = reason
        self.message = message


def _classify_gh_error(exc: GhCliError) -> tuple[DeliveryErrorKind, DeliveryFailureReason]:
    text = f"{exc.stderr} {exc}".lower()
    if isinstance(exc, GhRateLimitError):
        return DeliveryErrorKind.RATE_LIMIT, DeliveryFailureReason.GH_RATE_LIMITED
    if isinstance(exc, GhNotInstalledError):
        return DeliveryErrorKind.TRANSPORT, DeliveryFailureReason.GH_NOT_INSTALLED
    if any(term in text for term in ("bad credentials", "authentication", "unauthorized")):
        return DeliveryErrorKind.AUTHENTICATION, DeliveryFailureReason.AUTHENTICATION_FAILED
    if any(term in text for term in ("forbidden", "permission", "resource not accessible")):
        return DeliveryErrorKind.PERMISSION, DeliveryFailureReason.PERMISSION_DENIED
    return DeliveryErrorKind.TRANSPORT, DeliveryFailureReason.GH_TRANSPORT_ERROR


def _expect_object(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"{path} was not an object",
        )
    return value


def _expect_string(value: Any, path: str, *, allow_empty: bool = False) -> str:
    if value is None:
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.NULL_FIELD,
            f"{path} was null",
        )
    if not isinstance(value, str):
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"{path} was not a string",
        )
    if not allow_empty and not value:
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.NULL_FIELD,
            f"{path} was empty",
        )
    return value


def _expect_int(value: Any, path: str) -> int:
    if not isinstance(value, int):
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"{path} was not an integer",
        )
    return value


def _expect_bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"{path} was not a boolean",
        )
    return value


def _connection_nodes(connection: Any, path: str) -> tuple[list[Any], dict[str, Any]]:
    obj = _expect_object(connection, path)
    page_info = _expect_object(obj.get("pageInfo"), f"{path}.pageInfo")
    has_next = page_info.get("hasNextPage")
    if not isinstance(has_next, bool):
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"{path}.pageInfo.hasNextPage was not a boolean",
        )
    if "endCursor" not in page_info:
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"{path}.pageInfo.endCursor was missing",
        )
    nodes = obj.get("nodes")
    if not isinstance(nodes, list):
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"{path}.nodes was not a list",
        )
    return nodes, page_info


def _parse_payload(output: str) -> dict[str, Any]:
    if not output:
        raise _VerificationAbort(
            DeliveryErrorKind.TRANSPORT,
            DeliveryFailureReason.GH_TRANSPORT_ERROR,
            "gh returned empty output",
        )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.MALFORMED_RESPONSE,
            f"gh returned non-JSON output: {exc}",
        ) from exc
    root = _expect_object(payload, "response")
    if "errors" in root:
        errors = root.get("errors")
        if isinstance(errors, list):
            text = "; ".join(
                str(_expect_object(err, "errors[]").get("message", err)) for err in errors
            )
        else:
            text = str(errors)
        lower = text.lower()
        if "rate limit" in lower:
            raise _VerificationAbort(
                DeliveryErrorKind.RATE_LIMIT,
                DeliveryFailureReason.GH_RATE_LIMITED,
                text,
            )
        if "could not resolve to a repository" in lower:
            raise _VerificationAbort(
                DeliveryErrorKind.NOT_FOUND,
                DeliveryFailureReason.REPOSITORY_NOT_FOUND,
                text,
            )
        if any(term in lower for term in ("forbidden", "permission", "resource not accessible")):
            raise _VerificationAbort(
                DeliveryErrorKind.PERMISSION,
                DeliveryFailureReason.PERMISSION_DENIED,
                text,
            )
        raise _VerificationAbort(
            DeliveryErrorKind.SCHEMA,
            DeliveryFailureReason.GRAPHQL_ERRORS,
            text,
        )
    return _expect_object(root.get("data"), "data")


def _graphql(
    repo: str,
    token: str,
    query: str,
    variables: dict[str, str | int | None],
) -> dict[str, Any]:
    args = ["api", "graphql", "-f", f"query={query}"]
    for key, value in variables.items():
        if value is None:
            continue
        flag = "-F" if isinstance(value, int) else "-f"
        args.extend([flag, f"{key}={value}"])
    try:
        return _parse_payload(gh._run_gh(args, token))
    except GhCliError as exc:
        kind, reason = _classify_gh_error(exc)
        raise _VerificationAbort(kind, reason, str(exc)) from exc


def _repository_name(node: Any, path: str) -> str:
    repo = _expect_object(node, path)
    return _expect_string(repo.get("nameWithOwner"), f"{path}.nameWithOwner")


_REPOSITORY_QUERY = """
query($owner: String!, $repo: String!, $headRef: String!) {
  repository(owner: $owner, name: $repo) {
    nameWithOwner
    defaultBranchRef { name target { oid } }
    ref(qualifiedName: $headRef) { name target { oid } }
  }
}
"""

_PRS_QUERY = """
query($owner: String!, $repo: String!, $headRefName: String!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(
      headRefName: $headRefName
      first: 100
      after: $after
      orderBy: {field: CREATED_AT, direction: ASC}
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        url
        state
        isDraft
        baseRefName
        headRefName
        headRefOid
        baseRepository { nameWithOwner }
        headRepository { nameWithOwner }
        closingIssuesReferences(first: 100) {
          pageInfo { hasNextPage endCursor }
          nodes {
            number
            url
            repository { nameWithOwner }
          }
        }
      }
    }
  }
}
"""

_CLOSING_REFS_QUERY = """
query($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      closingIssuesReferences(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          number
          url
          repository { nameWithOwner }
        }
      }
    }
  }
}
"""


def _fetch_repository_boundary(
    repo: str,
    token: str,
    expected_head_ref: str,
) -> tuple[str, str, str]:
    owner, name = repo.split("/", 1)
    data = _graphql(
        repo,
        token,
        _REPOSITORY_QUERY,
        {"owner": owner, "repo": name, "headRef": expected_head_ref},
    )
    repository = data.get("repository")
    if repository is None:
        raise _VerificationAbort(
            DeliveryErrorKind.NOT_FOUND,
            DeliveryFailureReason.REPOSITORY_NOT_FOUND,
            f"repository {repo} was not found",
        )
    repository = _expect_object(repository, "data.repository")
    default_ref = _expect_object(repository.get("defaultBranchRef"), "repository.defaultBranchRef")
    default_branch = _expect_string(default_ref.get("name"), "repository.defaultBranchRef.name")
    default_oid = _expect_string(
        _expect_object(default_ref.get("target"), "repository.defaultBranchRef.target").get("oid"),
        "repository.defaultBranchRef.target.oid",
    )
    head_ref = repository.get("ref")
    if head_ref is None:
        raise _VerificationAbort(
            DeliveryErrorKind.NOT_FOUND,
            DeliveryFailureReason.HEAD_REF_NOT_FOUND,
            f"head ref {expected_head_ref} was not found in {repo}",
        )
    live_head_oid = _expect_string(
        _expect_object(
            _expect_object(head_ref, "repository.ref").get("target"),
            "repository.ref.target",
        ).get("oid"),
        "repository.ref.target.oid",
    )
    return default_branch, default_oid, live_head_oid


def _parse_closing_refs(
    connection: Any,
    path: str,
) -> tuple[list[ClosingIssueReference], dict[str, Any]]:
    nodes, page_info = _connection_nodes(connection, path)
    refs: list[ClosingIssueReference] = []
    for index, node in enumerate(nodes):
        issue = _expect_object(node, f"{path}.nodes[{index}]")
        refs.append(
            ClosingIssueReference(
                repository=_repository_name(
                    issue.get("repository"), f"{path}.nodes[{index}].repository"
                ),
                number=_expect_int(issue.get("number"), f"{path}.nodes[{index}].number"),
                url=_expect_string(issue.get("url"), f"{path}.nodes[{index}].url"),
            )
        )
    return refs, page_info


def _fetch_remaining_closing_refs(
    repo: str,
    token: str,
    pr_number: int,
    initial_refs: list[ClosingIssueReference],
    initial_page_info: dict[str, Any],
    page_cap: int,
) -> tuple[tuple[ClosingIssueReference, ...], int]:
    owner, name = repo.split("/", 1)
    refs = list(initial_refs)
    page_info = initial_page_info
    pages = 1
    while page_info.get("hasNextPage"):
        cursor = page_info.get("endCursor")
        if not cursor:
            raise _VerificationAbort(
                DeliveryErrorKind.COMPLETENESS,
                DeliveryFailureReason.MISSING_CURSOR,
                f"PR #{pr_number} closingIssuesReferences had no cursor",
            )
        if pages >= page_cap:
            raise _VerificationAbort(
                DeliveryErrorKind.COMPLETENESS,
                DeliveryFailureReason.PAGE_CAP_EXCEEDED,
                f"PR #{pr_number} closingIssuesReferences exceeded page cap {page_cap}",
            )
        data = _graphql(
            repo,
            token,
            _CLOSING_REFS_QUERY,
            {"owner": owner, "repo": name, "number": pr_number, "after": str(cursor)},
        )
        pr = (_expect_object(data.get("repository"), "data.repository")).get("pullRequest")
        if pr is None:
            raise _VerificationAbort(
                DeliveryErrorKind.SCHEMA,
                DeliveryFailureReason.NULL_FIELD,
                f"pullRequest was null while paginating PR #{pr_number}",
            )
        more_refs, page_info = _parse_closing_refs(
            _expect_object(pr, "repository.pullRequest").get("closingIssuesReferences"),
            f"pullRequest({pr_number}).closingIssuesReferences",
        )
        refs.extend(more_refs)
        pages += 1
    return tuple(refs), pages


def _fetch_candidate_prs(
    repo: str,
    token: str,
    expected_head_ref: str,
    page_cap: int,
) -> tuple[tuple[CandidatePullRequest, ...], int, int]:
    owner, name = repo.split("/", 1)
    candidates: list[CandidatePullRequest] = []
    cursor: str | None = None
    pages = 0
    closing_pages = 0
    head_ref_name = expected_head_ref.removeprefix("heads/")
    while True:
        if pages >= page_cap:
            raise _VerificationAbort(
                DeliveryErrorKind.COMPLETENESS,
                DeliveryFailureReason.PAGE_CAP_EXCEEDED,
                f"candidate PR retrieval exceeded page cap {page_cap}",
            )
        data = _graphql(
            repo,
            token,
            _PRS_QUERY,
            {
                "owner": owner,
                "repo": name,
                "headRefName": head_ref_name,
                "after": cursor,
            },
        )
        repository = data.get("repository")
        if repository is None:
            raise _VerificationAbort(
                DeliveryErrorKind.NOT_FOUND,
                DeliveryFailureReason.REPOSITORY_NOT_FOUND,
                f"repository {repo} was not found",
            )
        pull_requests = _expect_object(repository, "data.repository").get("pullRequests")
        pr_nodes, page_info = _connection_nodes(pull_requests, "repository.pullRequests")
        pages += 1
        for index, node in enumerate(pr_nodes):
            pr = _expect_object(node, f"pullRequests.nodes[{index}]")
            initial_refs, closing_page_info = _parse_closing_refs(
                pr.get("closingIssuesReferences"),
                f"pullRequests.nodes[{index}].closingIssuesReferences",
            )
            refs, pages_for_pr = _fetch_remaining_closing_refs(
                repo,
                token,
                _expect_int(pr.get("number"), f"pullRequests.nodes[{index}].number"),
                initial_refs,
                closing_page_info,
                page_cap,
            )
            closing_pages += pages_for_pr
            candidate = CandidatePullRequest(
                number=_expect_int(pr.get("number"), f"pullRequests.nodes[{index}].number"),
                url=_expect_string(pr.get("url"), f"pullRequests.nodes[{index}].url"),
                state=_expect_string(pr.get("state"), f"pullRequests.nodes[{index}].state"),
                is_draft=_expect_bool(pr.get("isDraft"), f"pullRequests.nodes[{index}].isDraft"),
                base_repository=_repository_name(
                    pr.get("baseRepository"), f"pullRequests.nodes[{index}].baseRepository"
                ),
                base_ref_name=_expect_string(
                    pr.get("baseRefName"), f"pullRequests.nodes[{index}].baseRefName"
                ),
                head_repository=_repository_name(
                    pr.get("headRepository"), f"pullRequests.nodes[{index}].headRepository"
                ),
                head_ref_name=_expect_string(
                    pr.get("headRefName"), f"pullRequests.nodes[{index}].headRefName"
                ),
                head_oid=_expect_string(
                    pr.get("headRefOid"), f"pullRequests.nodes[{index}].headRefOid"
                ),
                closing_issues_references=refs,
            )
            if candidate.head_repository == repo and candidate.head_ref_name == head_ref_name:
                candidates.append(candidate)
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            raise _VerificationAbort(
                DeliveryErrorKind.COMPLETENESS,
                DeliveryFailureReason.MISSING_CURSOR,
                "candidate PR page had hasNextPage=true and no endCursor",
            )
    return tuple(candidates), pages, closing_pages


def normalize_head_ref(ref: str) -> str:
    """Return a branch name without ``refs/`` or ``heads/`` prefixes."""
    value = (ref or "").strip()
    value = value.removeprefix("refs/")
    value = value.removeprefix("heads/")
    return value


def candidate_qualifies(
    pr: CandidatePullRequest,
    *,
    repo: str,
    issue_number: int,
    expected_head_owner: str,
    expected_head_ref: str,
    default_branch: str,
    live_head_oid: str,
) -> bool:
    """Return True when *pr* is a fully observed, usable issue handoff."""
    head_ref = normalize_head_ref(expected_head_ref)
    if not head_ref or not default_branch or not live_head_oid:
        return False
    if not _OID_RE.fullmatch(live_head_oid):
        return False
    if pr.state.upper() != "OPEN" or pr.is_draft:
        return False
    if pr.base_repository != repo or pr.head_repository != repo:
        return False
    owner = expected_head_owner or repo.split("/", 1)[0]
    head_owner = pr.head_repository.split("/", 1)[0]
    if head_owner != owner:
        return False
    if pr.head_ref_name != head_ref:
        return False
    if pr.base_ref_name != default_branch:
        return False
    if not pr.head_oid or not _OID_RE.fullmatch(pr.head_oid):
        return False
    if pr.head_oid != live_head_oid:
        return False
    return any(
        ref.repository == repo and ref.number == issue_number
        for ref in pr.closing_issues_references
    )


@dataclass(frozen=True)
class HandoffObservation:
    """Live-GitHub observation used by durable delivery verification."""

    verified: bool
    error_kind: DeliveryErrorKind
    reason: DeliveryFailureReason
    repo: str
    issue_number: int
    default_branch: str
    default_branch_oid: str
    expected_head_ref: str
    claimed_head_oid: str
    live_head_oid: str
    complete: bool
    echo_mismatch: bool = False
    ambiguous: bool = False
    selected_pr: CandidatePullRequest | None = None
    qualifying_prs: tuple[CandidatePullRequest, ...] = field(default_factory=tuple)
    candidate_prs: tuple[CandidatePullRequest, ...] = field(default_factory=tuple)
    pages_fetched: int = 0
    closing_pages_fetched: int = 0
    message: str = ""

    def to_shadow_fields(self) -> dict[str, str]:
        urls = [pr.url for pr in self.qualifying_prs[:10] or self.candidate_prs[:10]]
        selected = self.selected_pr
        return {
            "verified": "1" if self.verified else "0",
            "error_kind": self.error_kind.value,
            "reason": self.reason.value,
            "repo": self.repo,
            "issue_number": str(self.issue_number),
            "default_branch": self.default_branch[:120],
            "default_branch_oid": self.default_branch_oid,
            "expected_head_ref": self.expected_head_ref[:240],
            "claimed_head_oid": self.claimed_head_oid,
            "live_head_oid": self.live_head_oid,
            "candidate_pr_count": str(len(self.candidate_prs)),
            "qualifying_pr_count": str(len(self.qualifying_prs)),
            "selected_pr_number": "" if selected is None else str(selected.number),
            "matching_pr_numbers": ",".join(str(pr.number) for pr in self.qualifying_prs[:20]),
            "candidate_pr_urls": json.dumps(urls, separators=(",", ":")),
            "pages_fetched": str(self.pages_fetched),
            "closing_pages_fetched": str(self.closing_pages_fetched),
            "complete": "1" if self.complete else "0",
            "echo_mismatch": "1" if self.echo_mismatch else "0",
            "ambiguous": "1" if self.ambiguous else "0",
            "message": self.message[:500],
        }


def observe_issue_handoff(
    repo: str,
    issue_number: int,
    expected_head_ref: str,
    token: str,
    *,
    expected_head_owner: str = "",
    claimed_head_oid: str = "",
    claimed_branch: str = "",
    expected_default_branch: str | None = None,
    page_cap: int = _DEFAULT_PAGE_CAP,
    pre_schema: bool = False,
) -> HandoffObservation:
    """Observe live GitHub state for an issue implementation handoff.

    Provider echoes (``claimed_head_oid`` / ``claimed_branch``) are diagnostic
    only: a mismatch cannot verify or invalidate an otherwise qualifying PR.
    """
    gh._validate_repo(repo)
    head_ref = normalize_head_ref(expected_head_ref)
    if page_cap < 1:
        raise ValueError("page_cap must be positive")
    if pre_schema:
        return _observe_pre_schema_handoff(
            repo,
            issue_number,
            token,
            claimed_head_oid=claimed_head_oid,
            claimed_branch=claimed_branch,
            page_cap=page_cap,
        )
    if not head_ref:
        return _handoff_failure(
            repo,
            issue_number,
            "",
            claimed_head_oid,
            DeliveryErrorKind.MISMATCH,
            DeliveryFailureReason.HEAD_REF_NOT_FOUND,
            "expected head ref was empty",
            complete=True,
        )
    try:
        default_branch, default_oid, live_oid = _fetch_repository_boundary(repo, token, head_ref)
        echo_mismatch = _echo_mismatch(claimed_head_oid, claimed_branch, live_oid, head_ref)
        if expected_default_branch is not None and default_branch != expected_default_branch:
            return _handoff_failure(
                repo,
                issue_number,
                head_ref,
                claimed_head_oid,
                DeliveryErrorKind.MISMATCH,
                DeliveryFailureReason.DEFAULT_BRANCH_MISMATCH,
                f"default branch was {default_branch}, expected {expected_default_branch}",
                default_branch=default_branch,
                default_branch_oid=default_oid,
                live_head_oid=live_oid,
                complete=True,
                echo_mismatch=echo_mismatch,
            )
        candidates, pr_pages, closing_pages = _fetch_candidate_prs(repo, token, head_ref, page_cap)
        qualifying = tuple(
            pr
            for pr in candidates
            if candidate_qualifies(
                pr,
                repo=repo,
                issue_number=issue_number,
                expected_head_owner=expected_head_owner or repo.split("/", 1)[0],
                expected_head_ref=head_ref,
                default_branch=default_branch,
                live_head_oid=live_oid,
            )
        )
        if qualifying:
            selected = min(qualifying, key=lambda pr: pr.number)
            ambiguous = len(qualifying) > 1
            return HandoffObservation(
                verified=True,
                error_kind=DeliveryErrorKind.NONE,
                reason=DeliveryFailureReason.VERIFIED,
                repo=repo,
                issue_number=issue_number,
                default_branch=default_branch,
                default_branch_oid=default_oid,
                expected_head_ref=head_ref,
                claimed_head_oid=claimed_head_oid,
                live_head_oid=live_oid,
                complete=True,
                echo_mismatch=echo_mismatch,
                ambiguous=ambiguous,
                selected_pr=selected,
                qualifying_prs=tuple(sorted(qualifying, key=lambda pr: pr.number)),
                candidate_prs=candidates,
                pages_fetched=pr_pages,
                closing_pages_fetched=closing_pages,
                message="verified from canonical GitHub closingIssuesReferences",
            )
        reason, message = _mismatch_reason(candidates, repo, issue_number)
        return _handoff_failure(
            repo,
            issue_number,
            head_ref,
            claimed_head_oid,
            DeliveryErrorKind.MISMATCH,
            reason,
            message,
            default_branch=default_branch,
            default_branch_oid=default_oid,
            live_head_oid=live_oid,
            candidate_prs=candidates,
            pages_fetched=pr_pages,
            closing_pages_fetched=closing_pages,
            complete=True,
            echo_mismatch=echo_mismatch,
        )
    except _VerificationAbort as exc:
        return _handoff_failure(
            repo,
            issue_number,
            head_ref,
            claimed_head_oid,
            exc.kind,
            exc.reason,
            exc.message,
            echo_mismatch=_echo_mismatch(claimed_head_oid, claimed_branch, "", head_ref),
        )


def verify_issue_delivery(
    repo: str,
    issue_number: int,
    expected_head_ref: str,
    expected_head_oid: str,
    token: str,
    *,
    expected_default_branch: str | None = None,
    page_cap: int = _DEFAULT_PAGE_CAP,
) -> DeliveryVerification:
    """Verify a completed issue implementation from live GitHub evidence."""
    gh._validate_repo(repo)
    expected_head_ref = expected_head_ref.removeprefix("refs/")
    if not expected_head_ref:
        return _failure(
            repo,
            issue_number,
            "",
            expected_head_oid,
            DeliveryErrorKind.MISMATCH,
            DeliveryFailureReason.HEAD_REF_NOT_FOUND,
            "expected head ref was empty",
        )
    if page_cap < 1:
        raise ValueError("page_cap must be positive")
    if not expected_head_oid or not _OID_RE.fullmatch(expected_head_oid):
        return _failure(
            repo,
            issue_number,
            expected_head_ref,
            expected_head_oid,
            DeliveryErrorKind.MISMATCH,
            DeliveryFailureReason.HEAD_OID_MISMATCH,
            "expected head OID was missing or malformed",
        )
    try:
        default_branch, default_oid, live_oid = _fetch_repository_boundary(
            repo, token, expected_head_ref
        )
        if expected_default_branch is not None and default_branch != expected_default_branch:
            return _failure(
                repo,
                issue_number,
                expected_head_ref,
                expected_head_oid,
                DeliveryErrorKind.MISMATCH,
                DeliveryFailureReason.DEFAULT_BRANCH_MISMATCH,
                f"default branch was {default_branch}, expected {expected_default_branch}",
                default_branch=default_branch,
                default_branch_oid=default_oid,
                live_head_oid=live_oid,
                complete=True,
            )
        if live_oid != expected_head_oid:
            return _failure(
                repo,
                issue_number,
                expected_head_ref,
                expected_head_oid,
                DeliveryErrorKind.MISMATCH,
                DeliveryFailureReason.HEAD_OID_MISMATCH,
                f"live head OID {live_oid} did not match expected {expected_head_oid}",
                default_branch=default_branch,
                default_branch_oid=default_oid,
                live_head_oid=live_oid,
                complete=True,
            )
        candidates, pr_pages, closing_pages = _fetch_candidate_prs(
            repo, token, expected_head_ref, page_cap
        )
        if not candidates:
            return _failure(
                repo,
                issue_number,
                expected_head_ref,
                expected_head_oid,
                DeliveryErrorKind.MISMATCH,
                DeliveryFailureReason.NO_CANDIDATE_PR,
                "no same-repository PR found for expected head ref",
                default_branch=default_branch,
                default_branch_oid=default_oid,
                live_head_oid=live_oid,
                pages_fetched=pr_pages,
                closing_pages_fetched=closing_pages,
                complete=True,
            )
        verified = any(
            ref.repository == repo and ref.number == issue_number
            for pr in candidates
            for ref in pr.closing_issues_references
        )
        if not verified:
            return _failure(
                repo,
                issue_number,
                expected_head_ref,
                expected_head_oid,
                DeliveryErrorKind.MISMATCH,
                DeliveryFailureReason.NO_CANONICAL_CLOSING_REFERENCE,
                "candidate PRs did not include the canonical closing issue relation",
                default_branch=default_branch,
                default_branch_oid=default_oid,
                live_head_oid=live_oid,
                candidate_prs=candidates,
                pages_fetched=pr_pages,
                closing_pages_fetched=closing_pages,
                complete=True,
            )
        return DeliveryVerification(
            verified=True,
            error_kind=DeliveryErrorKind.NONE,
            reason=DeliveryFailureReason.VERIFIED,
            repo=repo,
            issue_number=issue_number,
            default_branch=default_branch,
            default_branch_oid=default_oid,
            expected_head_ref=expected_head_ref,
            expected_head_oid=expected_head_oid,
            live_head_oid=live_oid,
            candidate_prs=candidates,
            pages_fetched=pr_pages,
            closing_pages_fetched=closing_pages,
            complete=True,
            message="verified from canonical GitHub closingIssuesReferences",
        )
    except _VerificationAbort as exc:
        return _failure(
            repo,
            issue_number,
            expected_head_ref,
            expected_head_oid,
            exc.kind,
            exc.reason,
            exc.message,
        )


def _failure(
    repo: str,
    issue_number: int,
    expected_head_ref: str,
    expected_head_oid: str,
    kind: DeliveryErrorKind,
    reason: DeliveryFailureReason,
    message: str,
    *,
    default_branch: str = "",
    default_branch_oid: str = "",
    live_head_oid: str = "",
    candidate_prs: tuple[CandidatePullRequest, ...] = (),
    pages_fetched: int = 0,
    closing_pages_fetched: int = 0,
    complete: bool = False,
) -> DeliveryVerification:
    return DeliveryVerification(
        verified=False,
        error_kind=kind,
        reason=reason,
        repo=repo,
        issue_number=issue_number,
        default_branch=default_branch,
        default_branch_oid=default_branch_oid,
        expected_head_ref=expected_head_ref,
        expected_head_oid=expected_head_oid,
        live_head_oid=live_head_oid,
        candidate_prs=candidate_prs,
        pages_fetched=pages_fetched,
        closing_pages_fetched=closing_pages_fetched,
        complete=complete,
        message=message,
    )


def _echo_mismatch(
    claimed_head_oid: str,
    claimed_branch: str,
    live_head_oid: str,
    expected_head_ref: str,
) -> bool:
    claimed_oid = (claimed_head_oid or "").strip()
    claimed_ref = normalize_head_ref(claimed_branch)
    if claimed_oid and live_head_oid and claimed_oid != live_head_oid:
        return True
    if claimed_ref and expected_head_ref and claimed_ref != expected_head_ref:
        return True
    return False


def _mismatch_reason(
    candidates: tuple[CandidatePullRequest, ...],
    repo: str,
    issue_number: int,
) -> tuple[DeliveryFailureReason, str]:
    if not candidates:
        return (
            DeliveryFailureReason.NO_CANDIDATE_PR,
            "no same-repository PR found for expected head ref",
        )
    if any(
        any(
            ref.repository == repo and ref.number == issue_number
            for ref in pr.closing_issues_references
        )
        for pr in candidates
    ):
        return (
            DeliveryFailureReason.NO_CANDIDATE_PR,
            "candidate PRs existed but none were open, non-draft, matching, and OID-aligned",
        )
    return (
        DeliveryFailureReason.NO_CANONICAL_CLOSING_REFERENCE,
        "candidate PRs did not include the canonical closing issue relation",
    )


def _handoff_failure(
    repo: str,
    issue_number: int,
    expected_head_ref: str,
    claimed_head_oid: str,
    kind: DeliveryErrorKind,
    reason: DeliveryFailureReason,
    message: str,
    *,
    default_branch: str = "",
    default_branch_oid: str = "",
    live_head_oid: str = "",
    candidate_prs: tuple[CandidatePullRequest, ...] = (),
    pages_fetched: int = 0,
    closing_pages_fetched: int = 0,
    complete: bool = False,
    echo_mismatch: bool = False,
) -> HandoffObservation:
    return HandoffObservation(
        verified=False,
        error_kind=kind,
        reason=reason,
        repo=repo,
        issue_number=issue_number,
        default_branch=default_branch,
        default_branch_oid=default_branch_oid,
        expected_head_ref=expected_head_ref,
        claimed_head_oid=claimed_head_oid,
        live_head_oid=live_head_oid,
        complete=complete,
        echo_mismatch=echo_mismatch,
        candidate_prs=candidate_prs,
        pages_fetched=pages_fetched,
        closing_pages_fetched=closing_pages_fetched,
        message=message,
    )


_PRE_SCHEMA_TIMELINE_QUERY = """
query($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    nameWithOwner
    defaultBranchRef { name target { oid } }
    issue(number: $number) {
      timelineItems(
        itemTypes: [CROSS_REFERENCED_EVENT, CONNECTED_EVENT],
        first: 100,
        after: $after
      ) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on CrossReferencedEvent {
            source {
              __typename
              ... on PullRequest {
                number
                url
                state
                isDraft
                baseRefName
                headRefName
                headRefOid
                baseRepository { nameWithOwner }
                headRepository { nameWithOwner }
                closingIssuesReferences(first: 100) {
                  pageInfo { hasNextPage endCursor }
                  nodes {
                    number
                    url
                    repository { nameWithOwner }
                  }
                }
              }
            }
          }
          ... on ConnectedEvent {
            subject {
              __typename
              ... on PullRequest {
                number
                url
                state
                isDraft
                baseRefName
                headRefName
                headRefOid
                baseRepository { nameWithOwner }
                headRepository { nameWithOwner }
                closingIssuesReferences(first: 100) {
                  pageInfo { hasNextPage endCursor }
                  nodes {
                    number
                    url
                    repository { nameWithOwner }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


def _pr_from_timeline_node(
    node: Any,
    path: str,
    repo: str,
    token: str,
    page_cap: int,
) -> tuple[CandidatePullRequest | None, int]:
    if not isinstance(node, dict):
        return None, 0
    typename = node.get("__typename")
    payload: Any
    if typename == "CrossReferencedEvent":
        payload = node.get("source")
    elif typename == "ConnectedEvent":
        payload = node.get("subject")
    else:
        payload = node
    if not isinstance(payload, dict):
        return None, 0
    if payload.get("__typename") not in (None, "PullRequest"):
        return None, 0
    if "number" not in payload:
        return None, 0
    initial_refs, closing_page_info = _parse_closing_refs(
        payload.get("closingIssuesReferences"),
        f"{path}.closingIssuesReferences",
    )
    pr_number = _expect_int(payload.get("number"), f"{path}.number")
    refs, closing_pages = _fetch_remaining_closing_refs(
        repo, token, pr_number, initial_refs, closing_page_info, page_cap
    )
    return (
        CandidatePullRequest(
            number=pr_number,
            url=_expect_string(payload.get("url"), f"{path}.url"),
            state=_expect_string(payload.get("state"), f"{path}.state"),
            is_draft=_expect_bool(payload.get("isDraft"), f"{path}.isDraft"),
            base_repository=_repository_name(
                payload.get("baseRepository"), f"{path}.baseRepository"
            ),
            base_ref_name=_expect_string(payload.get("baseRefName"), f"{path}.baseRefName"),
            head_repository=_repository_name(
                payload.get("headRepository"), f"{path}.headRepository"
            ),
            head_ref_name=_expect_string(payload.get("headRefName"), f"{path}.headRefName"),
            head_oid=_expect_string(payload.get("headRefOid"), f"{path}.headRefOid"),
            closing_issues_references=refs,
        ),
        closing_pages,
    )


def _observe_pre_schema_handoff(
    repo: str,
    issue_number: int,
    token: str,
    *,
    claimed_head_oid: str,
    claimed_branch: str,
    page_cap: int,
) -> HandoffObservation:
    """Prove a handoff by canonical closing relationship without a branch slug."""
    owner, name = repo.split("/", 1)
    try:
        candidates: list[CandidatePullRequest] = []
        seen_set: set[int] = set()
        cursor: str | None = None
        pages = 0
        closing_pages = 0
        default_branch = ""
        default_oid = ""
        live_oids: dict[str, str] = {}
        while True:
            if pages >= page_cap:
                raise _VerificationAbort(
                    DeliveryErrorKind.COMPLETENESS,
                    DeliveryFailureReason.PAGE_CAP_EXCEEDED,
                    "pre-schema issue timeline exceeded page cap",
                )
            data = _graphql(
                repo,
                token,
                _PRE_SCHEMA_TIMELINE_QUERY,
                {"owner": owner, "repo": name, "number": issue_number, "after": cursor},
            )
            repository = data.get("repository")
            if repository is None:
                raise _VerificationAbort(
                    DeliveryErrorKind.NOT_FOUND,
                    DeliveryFailureReason.REPOSITORY_NOT_FOUND,
                    f"repository {repo} was not found",
                )
            repository = _expect_object(repository, "data.repository")
            default_ref = _expect_object(
                repository.get("defaultBranchRef"), "repository.defaultBranchRef"
            )
            default_branch = _expect_string(
                default_ref.get("name"), "repository.defaultBranchRef.name"
            )
            default_oid = _expect_string(
                _expect_object(default_ref.get("target"), "repository.defaultBranchRef.target").get(
                    "oid"
                ),
                "repository.defaultBranchRef.target.oid",
            )
            issue = repository.get("issue")
            if issue is None:
                raise _VerificationAbort(
                    DeliveryErrorKind.NOT_FOUND,
                    DeliveryFailureReason.REPOSITORY_NOT_FOUND,
                    f"issue #{issue_number} was not found in {repo}",
                )
            nodes, page_info = _connection_nodes(
                _expect_object(issue, "repository.issue").get("timelineItems"),
                "repository.issue.timelineItems",
            )
            pages += 1
            for index, node in enumerate(nodes):
                pr, extra_closing = _pr_from_timeline_node(
                    node, f"timelineItems.nodes[{index}]", repo, token, page_cap
                )
                closing_pages += extra_closing
                if pr is None or pr.number in seen_set:
                    continue
                seen_set.add(pr.number)
                candidates.append(pr)
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                raise _VerificationAbort(
                    DeliveryErrorKind.COMPLETENESS,
                    DeliveryFailureReason.MISSING_CURSOR,
                    "pre-schema timeline had hasNextPage=true and no endCursor",
                )
        qualifying: list[CandidatePullRequest] = []
        for pr in candidates:
            if pr.head_repository != repo:
                continue
            if pr.head_ref_name not in live_oids:
                try:
                    _default, _default_oid, live_oid = _fetch_repository_boundary(
                        repo, token, pr.head_ref_name
                    )
                except _VerificationAbort as exc:
                    if (
                        exc.kind is DeliveryErrorKind.NOT_FOUND
                        and exc.reason is DeliveryFailureReason.HEAD_REF_NOT_FOUND
                    ):
                        continue
                    raise
                live_oids[pr.head_ref_name] = live_oid
            live_oid = live_oids[pr.head_ref_name]
            if candidate_qualifies(
                pr,
                repo=repo,
                issue_number=issue_number,
                expected_head_owner=repo.split("/", 1)[0],
                expected_head_ref=pr.head_ref_name,
                default_branch=default_branch,
                live_head_oid=live_oid,
            ):
                qualifying.append(pr)
        selected_ref = qualifying[0].head_ref_name if qualifying else ""
        selected_live = live_oids.get(selected_ref, "")
        echo_mismatch = _echo_mismatch(
            claimed_head_oid, claimed_branch, selected_live, selected_ref
        )
        if qualifying:
            selected = min(qualifying, key=lambda pr: pr.number)
            return HandoffObservation(
                verified=True,
                error_kind=DeliveryErrorKind.NONE,
                reason=DeliveryFailureReason.VERIFIED,
                repo=repo,
                issue_number=issue_number,
                default_branch=default_branch,
                default_branch_oid=default_oid,
                expected_head_ref=selected.head_ref_name,
                claimed_head_oid=claimed_head_oid,
                live_head_oid=live_oids.get(selected.head_ref_name, selected.head_oid),
                complete=True,
                echo_mismatch=echo_mismatch,
                ambiguous=len(qualifying) > 1,
                selected_pr=selected,
                qualifying_prs=tuple(sorted(qualifying, key=lambda pr: pr.number)),
                candidate_prs=tuple(candidates),
                pages_fetched=pages,
                closing_pages_fetched=closing_pages,
                message="verified pre-schema handoff from canonical closing relationship",
            )
        return _handoff_failure(
            repo,
            issue_number,
            "",
            claimed_head_oid,
            DeliveryErrorKind.MISMATCH,
            DeliveryFailureReason.NO_CANONICAL_CLOSING_REFERENCE
            if candidates
            else DeliveryFailureReason.NO_CANDIDATE_PR,
            "pre-schema observation could not prove a canonical issue handoff",
            default_branch=default_branch,
            default_branch_oid=default_oid,
            candidate_prs=tuple(candidates),
            pages_fetched=pages,
            complete=True,
            echo_mismatch=echo_mismatch,
        )
    except _VerificationAbort as exc:
        return _handoff_failure(
            repo,
            issue_number,
            "",
            claimed_head_oid,
            exc.kind,
            exc.reason,
            exc.message,
        )
