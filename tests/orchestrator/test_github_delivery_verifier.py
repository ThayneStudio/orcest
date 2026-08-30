"""Tests for the read-only GitHub issue delivery verifier."""

from __future__ import annotations

import json

from orcest.orchestrator.gh import GhCliError, GhRateLimitError
from orcest.orchestrator.github_delivery_verifier import (
    DeliveryErrorKind,
    DeliveryFailureReason,
    verify_issue_delivery,
)

REPO = "acme/widgets"
TOKEN = "ghp-test-token"
ISSUE = 657
BRANCH = "issue-657-delivery-verifier"
OID = "a" * 40


def _repo_payload(*, live_oid: str = OID, default_branch: str = "master") -> str:
    return json.dumps(
        {
            "data": {
                "repository": {
                    "nameWithOwner": REPO,
                    "defaultBranchRef": {
                        "name": default_branch,
                        "target": {"oid": "b" * 40},
                    },
                    "ref": {
                        "name": BRANCH,
                        "target": {"oid": live_oid},
                    },
                }
            }
        }
    )


def _pr_node(
    number: int,
    *,
    body_only: bool = False,
    closing_refs: list[dict] | None = None,
    closing_has_next: bool = False,
    closing_cursor: str | None = None,
) -> dict:
    if closing_refs is None:
        closing_refs = [] if body_only else [_issue_ref(ISSUE, REPO)]
    return {
        "number": number,
        "url": f"https://github.com/{REPO}/pull/{number}",
        "state": "OPEN",
        "isDraft": False,
        "baseRefName": "master",
        "headRefName": BRANCH,
        "headRefOid": OID,
        "baseRepository": {"nameWithOwner": REPO},
        "headRepository": {"nameWithOwner": REPO},
        "closingIssuesReferences": {
            "pageInfo": {
                "hasNextPage": closing_has_next,
                "endCursor": closing_cursor,
            },
            "nodes": closing_refs,
        },
    }


def _issue_ref(number: int, repo: str) -> dict:
    return {
        "number": number,
        "url": f"https://github.com/{repo}/issues/{number}",
        "repository": {"nameWithOwner": repo},
    }


def _prs_payload(nodes: list[dict], *, has_next: bool = False, cursor: str | None = None) -> str:
    return json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequests": {
                        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                        "nodes": nodes,
                    }
                }
            }
        }
    )


def _closing_payload(
    nodes: list[dict],
    *,
    has_next: bool = False,
    cursor: str | None = None,
) -> str:
    return json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequest": {
                        "closingIssuesReferences": {
                            "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                            "nodes": nodes,
                        }
                    }
                }
            }
        }
    )


def test_verifies_complete_multi_page_candidates_and_closing_refs(mocker):
    """The verifier fully paginates candidate PRs and nested closing references."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        side_effect=[
            _repo_payload(),
            _prs_payload(
                [
                    _pr_node(
                        10,
                        closing_refs=[_issue_ref(1, REPO)],
                        closing_has_next=True,
                        closing_cursor="closing-1",
                    )
                ],
                has_next=True,
                cursor="prs-1",
            ),
            _closing_payload([_issue_ref(ISSUE, REPO)]),
            _prs_payload([_pr_node(11, closing_refs=[_issue_ref(2, REPO)])]),
        ],
    )

    result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN, page_cap=5)

    assert result.verified is True
    assert result.complete is True
    assert result.pages_fetched == 2
    assert result.closing_pages_fetched == 3
    assert [pr.number for pr in result.candidate_prs] == [10, 11]
    assert mock_run.call_count == 4


def test_page_cap_failure_cannot_verify(mocker):
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        side_effect=[
            _repo_payload(),
            _prs_payload([_pr_node(10)], has_next=True, cursor="next"),
        ],
    )

    result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN, page_cap=1)

    assert result.verified is False
    assert result.error_kind == DeliveryErrorKind.COMPLETENESS
    assert result.reason == DeliveryFailureReason.PAGE_CAP_EXCEEDED
    assert result.complete is False


def test_missing_cursor_failure_cannot_verify(mocker):
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        side_effect=[
            _repo_payload(),
            _prs_payload([_pr_node(10)], has_next=True, cursor=None),
        ],
    )

    result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN, page_cap=5)

    assert result.error_kind == DeliveryErrorKind.COMPLETENESS
    assert result.reason == DeliveryFailureReason.MISSING_CURSOR


def test_null_and_truncated_fields_are_schema_failures(mocker):
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        side_effect=[
            _repo_payload(),
            _prs_payload(
                [
                    {
                        **_pr_node(10),
                        "closingIssuesReferences": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [
                                {
                                    "number": ISSUE,
                                    "url": None,
                                    "repository": {"nameWithOwner": REPO},
                                }
                            ],
                        },
                    }
                ]
            ),
        ],
    )

    result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN)

    assert result.error_kind == DeliveryErrorKind.SCHEMA
    assert result.reason == DeliveryFailureReason.NULL_FIELD


def test_malformed_response_failure_cannot_verify(mocker):
    mocker.patch("orcest.orchestrator.gh._run_gh", return_value="{")

    result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN)

    assert result.error_kind == DeliveryErrorKind.SCHEMA
    assert result.reason == DeliveryFailureReason.MALFORMED_RESPONSE


def test_exact_live_oid_mismatch_cannot_verify(mocker):
    mocker.patch("orcest.orchestrator.gh._run_gh", return_value=_repo_payload(live_oid="c" * 40))

    result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN)

    assert result.error_kind == DeliveryErrorKind.MISMATCH
    assert result.reason == DeliveryFailureReason.HEAD_OID_MISMATCH
    assert result.live_head_oid == "c" * 40
    assert result.pages_fetched == 0


def test_default_branch_mismatch_is_typed(mocker):
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=_repo_payload(default_branch="main"),
    )

    result = verify_issue_delivery(
        REPO,
        ISSUE,
        BRANCH,
        OID,
        TOKEN,
        expected_default_branch="master",
    )

    assert result.error_kind == DeliveryErrorKind.MISMATCH
    assert result.reason == DeliveryFailureReason.DEFAULT_BRANCH_MISMATCH


def test_body_text_alone_does_not_verify(mocker):
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        side_effect=[
            _repo_payload(),
            _prs_payload([_pr_node(10, body_only=True)]),
        ],
    )

    result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN)

    assert result.verified is False
    assert result.reason == DeliveryFailureReason.NO_CANONICAL_CLOSING_REFERENCE
    assert result.candidate_prs[0].closing_issues_references == ()


def test_auth_permission_rate_limit_and_transport_are_distinguished(mocker):
    cases = [
        (GhCliError("failed", stderr="authentication required"), DeliveryErrorKind.AUTHENTICATION),
        (
            GhCliError("failed", stderr="Resource not accessible by integration"),
            DeliveryErrorKind.PERMISSION,
        ),
        (
            GhRateLimitError("rate limited", stderr="HTTP 429 rate limit exceeded"),
            DeliveryErrorKind.RATE_LIMIT,
        ),
        (GhCliError("failed", stderr="connection reset by peer"), DeliveryErrorKind.TRANSPORT),
    ]
    for exc, kind in cases:
        mocker.patch("orcest.orchestrator.gh._run_gh", side_effect=exc)
        result = verify_issue_delivery(REPO, ISSUE, BRANCH, OID, TOKEN)
        assert result.error_kind == kind
        mocker.stopall()


def test_shadow_fields_are_bounded_and_secret_free():
    result = verify_issue_delivery(REPO, ISSUE, BRANCH, "", TOKEN)

    fields = result.to_shadow_fields()

    assert TOKEN not in json.dumps(fields)
    assert fields["reason"] == DeliveryFailureReason.HEAD_OID_MISMATCH.value
    assert len(fields["message"]) <= 500
