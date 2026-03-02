"""Unit tests for the GitHub CLI wrapper (orchestrator/gh.py).

Every test mocks ``subprocess.run`` so that no real ``gh`` process is spawned.
The mock target is the *module-level* import inside ``gh.py``, i.e.
``orcest.orchestrator.gh.subprocess.run``.
"""

import json
import logging
import subprocess

import pytest

from orcest.orchestrator.gh import (
    GhCliError,
    GhNotInstalledError,
    add_label,
    get_check_run_logs,
    get_ci_status,
    get_failed_run_logs,
    get_pr,
    get_pr_diff,
    get_unresolved_review_threads,
    list_open_prs,
    merge_pr,
    post_comment,
    remove_label,
    resolve_review_thread,
)

REPO = "acme/widgets"
TOKEN = "test-token-abc123"


# ---------------------------------------------------------------------------
# list_open_prs
# ---------------------------------------------------------------------------


def test_list_open_prs_parses_json(mocker):
    """list_open_prs returns the parsed JSON array from stdout."""
    payload = [
        {"number": 1, "title": "First PR"},
        {"number": 2, "title": "Second PR"},
    ]
    mock_run = mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=json.dumps(payload), stderr=""
        ),
    )
    result = list_open_prs(REPO, TOKEN)
    assert result == payload
    mock_run.assert_called_once()


def test_list_open_prs_sets_token_env(mocker):
    """Both GITHUB_TOKEN and GH_TOKEN are forwarded to subprocess.run."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["gh"], returncode=0, stdout="[]", stderr=""),
    )
    list_open_prs(REPO, TOKEN)
    _, kwargs = mock_run.call_args
    env = kwargs["env"]
    assert env["GITHUB_TOKEN"] == TOKEN
    assert env["GH_TOKEN"] == TOKEN


def test_list_open_prs_empty_output(mocker):
    """Empty stdout or '[]' both produce an empty list."""
    mock_run = mocker.patch("orcest.orchestrator.gh.subprocess.run")

    # Case 1: completely empty string
    mock_run.return_value = subprocess.CompletedProcess(
        args=["gh"], returncode=0, stdout="", stderr=""
    )
    assert list_open_prs(REPO, TOKEN) == []

    # Case 2: empty JSON array
    mock_run.return_value = subprocess.CompletedProcess(
        args=["gh"], returncode=0, stdout="[]", stderr=""
    )
    assert list_open_prs(REPO, TOKEN) == []


# ---------------------------------------------------------------------------
# get_pr
# ---------------------------------------------------------------------------


def test_get_pr_parses_json(mocker):
    """get_pr returns the parsed JSON dict from stdout."""
    pr_data = {
        "number": 42,
        "title": "Add feature X",
        "body": "Description",
        "headRefName": "feat-x",
        "statusCheckRollup": [],
    }
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=json.dumps(pr_data), stderr=""
        ),
    )
    result = get_pr(REPO, 42, TOKEN)
    assert result == pr_data
    assert result["number"] == 42


# ---------------------------------------------------------------------------
# get_ci_status
# ---------------------------------------------------------------------------


def test_get_ci_status_returns_failed_checks(mocker):
    """get_ci_status extracts statusCheckRollup from the PR JSON."""
    checks = [
        {"name": "tests", "status": "COMPLETED", "conclusion": "FAILURE"},
        {"name": "lint", "status": "COMPLETED", "conclusion": "SUCCESS"},
    ]
    pr_data = {
        "number": 10,
        "title": "broken",
        "statusCheckRollup": checks,
    }
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=json.dumps(pr_data), stderr=""
        ),
    )
    result = get_ci_status(REPO, 10, TOKEN)
    assert result == checks
    failed = [c for c in result if c["conclusion"] == "FAILURE"]
    assert len(failed) == 1
    assert failed[0]["name"] == "tests"


# ---------------------------------------------------------------------------
# add_label
# ---------------------------------------------------------------------------


def test_add_label_calls_correct_args(mocker):
    """add_label passes --add-label with the correct gh pr edit args."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
    )
    add_label(REPO, 7, "orcest:queued", TOKEN)

    args_passed = mock_run.call_args[0][0]
    assert args_passed[0] == "gh"
    assert "pr" in args_passed
    assert "edit" in args_passed
    assert "--add-label" in args_passed
    label_idx = args_passed.index("--add-label")
    assert args_passed[label_idx + 1] == "orcest:queued"
    assert "--repo" in args_passed
    repo_idx = args_passed.index("--repo")
    assert args_passed[repo_idx + 1] == REPO


# ---------------------------------------------------------------------------
# remove_label
# ---------------------------------------------------------------------------


def test_remove_label_swallows_not_found(mocker):
    """remove_label silently swallows GhCliError when label not present."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=1,
            cmd=["gh", "pr", "edit"],
            stderr="label 'orcest:queued' not found",
        ),
    )
    # Should NOT raise
    remove_label(REPO, 5, "orcest:queued", TOKEN)


def test_remove_label_reraises_other_errors(mocker):
    """remove_label re-raises GhCliError for non-'not found' errors."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=1,
            cmd=["gh", "pr", "edit"],
            stderr="authentication required",
        ),
    )
    with pytest.raises(GhCliError):
        remove_label(REPO, 5, "orcest:queued", TOKEN)


# ---------------------------------------------------------------------------
# post_comment
# ---------------------------------------------------------------------------


def test_post_comment_uses_body_file(mocker):
    """post_comment writes body to a temp file and passes --body-file."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
    )
    post_comment(REPO, 3, "Hello, world!", TOKEN)

    args_passed = mock_run.call_args[0][0]
    assert "--body-file" in args_passed
    # The value after --body-file should be a file path (string)
    bf_idx = args_passed.index("--body-file")
    body_file_path = args_passed[bf_idx + 1]
    assert isinstance(body_file_path, str)
    assert len(body_file_path) > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_gh_not_installed_raises(mocker):
    """FileNotFoundError from subprocess.run -> GhNotInstalledError."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=FileNotFoundError("No such file: 'gh'"),
    )
    with pytest.raises(GhNotInstalledError, match="gh CLI not found"):
        list_open_prs(REPO, TOKEN)


def test_gh_error_raises_gh_cli_error(mocker):
    """CalledProcessError from subprocess.run -> GhCliError with details."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=2,
            cmd=["gh", "pr", "list"],
            stderr="authentication required",
        ),
    )
    with pytest.raises(GhCliError) as exc_info:
        list_open_prs(REPO, TOKEN)

    err = exc_info.value
    assert err.returncode == 2
    assert "authentication required" in err.stderr


# ---------------------------------------------------------------------------
# get_unresolved_review_threads
# ---------------------------------------------------------------------------


def test_get_unresolved_review_threads(mocker):
    """Only unresolved threads are returned, with correct structure."""
    graphql_response = json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {
                            "pageInfo": {"hasNextPage": False},
                            "nodes": [
                                {
                                    "id": "PRRT_resolved",
                                    "path": "src/old.py",
                                    "line": 10,
                                    "isResolved": True,
                                    "comments": {
                                        "nodes": [
                                            {"body": "Looks good now", "author": {"login": "alice"}}
                                        ]
                                    },
                                },
                                {
                                    "id": "PRRT_unresolved",
                                    "path": "src/foo.py",
                                    "line": 42,
                                    "isResolved": False,
                                    "comments": {
                                        "nodes": [
                                            {"body": "Fix this", "author": {"login": "reviewer1"}},
                                            {"body": "Agreed", "author": {"login": "reviewer2"}},
                                        ]
                                    },
                                },
                            ],
                        }
                    }
                }
            }
        }
    )
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=graphql_response,
    )

    result = get_unresolved_review_threads(REPO, 5, TOKEN)

    assert len(result) == 1
    thread = result[0]
    assert thread["id"] == "PRRT_unresolved"
    assert thread["path"] == "src/foo.py"
    assert thread["line"] == 42
    assert len(thread["comments"]) == 2
    assert thread["comments"][0] == {"author": "reviewer1", "body": "Fix this"}
    assert thread["comments"][1] == {"author": "reviewer2", "body": "Agreed"}


def test_get_unresolved_threads_all_resolved(mocker):
    """When all threads are resolved, an empty list is returned."""
    graphql_response = json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {
                            "pageInfo": {"hasNextPage": False},
                            "nodes": [
                                {
                                    "id": "PRRT_1",
                                    "path": "a.py",
                                    "line": 1,
                                    "isResolved": True,
                                    "comments": {
                                        "nodes": [{"body": "Done", "author": {"login": "bob"}}]
                                    },
                                },
                                {
                                    "id": "PRRT_2",
                                    "path": "b.py",
                                    "line": 5,
                                    "isResolved": True,
                                    "comments": {
                                        "nodes": [{"body": "Fixed", "author": {"login": "carol"}}]
                                    },
                                },
                            ],
                        }
                    }
                }
            }
        }
    )
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=graphql_response,
    )

    result = get_unresolved_review_threads(REPO, 10, TOKEN)

    assert result == []


# ---------------------------------------------------------------------------
# resolve_review_thread
# ---------------------------------------------------------------------------


def test_resolve_review_thread(mocker):
    """resolve_review_thread calls _run_gh with correct GraphQL mutation args."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=(
            '{"data": {"resolveReviewThread": {"thread": '
            '{"id": "thread-id-123", "isResolved": true}}}}'
        ),
    )

    resolve_review_thread("thread-id-123", TOKEN)

    mock_run.assert_called_once()
    call_args = mock_run.call_args[0][0]
    assert "api" in call_args
    assert "graphql" in call_args
    # Check the threadId is passed correctly
    assert "-f" in call_args
    # There may be multiple -f flags; find the one with threadId
    found_thread_id = False
    found_query = False
    for i, arg in enumerate(call_args):
        if arg == "-f" and i + 1 < len(call_args):
            val = call_args[i + 1]
            if val.startswith("threadId="):
                assert val == "threadId=thread-id-123"
                found_thread_id = True
            if val.startswith("query="):
                assert "resolveReviewThread" in val
                found_query = True
    assert found_thread_id, "threadId argument not found in _run_gh call"
    assert found_query, "mutation query not found in _run_gh call"
    # Check token was passed
    assert mock_run.call_args[0][1] == TOKEN


def test_resolve_review_thread_rejects_empty_id():
    """resolve_review_thread raises ValueError for empty thread_id."""
    with pytest.raises(ValueError, match="non-empty string"):
        resolve_review_thread("", TOKEN)


def test_resolve_review_thread_empty_output(mocker):
    """resolve_review_thread raises GhCliError on empty output."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value="",
    )
    with pytest.raises(GhCliError, match="empty response"):
        resolve_review_thread("thread-id-123", TOKEN)


def test_resolve_review_thread_graphql_error(mocker):
    """resolve_review_thread raises GhCliError when GraphQL returns errors."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps({"errors": [{"message": "Could not resolve to a node"}]}),
    )
    with pytest.raises(GhCliError, match="Could not resolve to a node"):
        resolve_review_thread("bad-id", TOKEN)


def test_get_unresolved_review_threads_null_repository(mocker):
    """Raises GhCliError when GraphQL returns null repository."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps({"data": {"repository": None}}),
    )
    with pytest.raises(GhCliError, match="null repository"):
        get_unresolved_review_threads(REPO, 5, TOKEN)


def test_get_unresolved_review_threads_null_pull_request(mocker):
    """Raises GhCliError when GraphQL returns null pullRequest."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps({"data": {"repository": {"pullRequest": None}}}),
    )
    with pytest.raises(GhCliError, match="null pullRequest"):
        get_unresolved_review_threads(REPO, 999, TOKEN)


def test_get_unresolved_review_threads_empty_output(mocker):
    """get_unresolved_review_threads raises GhCliError on empty output."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value="",
    )
    with pytest.raises(GhCliError, match="empty response"):
        get_unresolved_review_threads(REPO, 5, TOKEN)


def test_get_unresolved_review_threads_graphql_error(mocker):
    """get_unresolved_review_threads raises GhCliError on GraphQL errors."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps({"errors": [{"message": "rate limited"}]}),
    )
    with pytest.raises(GhCliError, match="rate limited"):
        get_unresolved_review_threads(REPO, 5, TOKEN)


# ---------------------------------------------------------------------------
# merge_pr
# ---------------------------------------------------------------------------


def test_merge_pr_calls_correct_args(mocker):
    """merge_pr passes --squash and --delete-branch by default."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value="",
    )
    merge_pr(REPO, 42, TOKEN)

    mock_run.assert_called_once()
    args_passed = mock_run.call_args[0][0]
    assert "pr" in args_passed
    assert "merge" in args_passed
    assert "42" in args_passed
    assert "--repo" in args_passed
    assert REPO in args_passed
    assert "--squash" in args_passed
    assert "--delete-branch" in args_passed
    assert mock_run.call_args[0][1] == TOKEN


def test_merge_pr_rejects_invalid_method():
    """merge_pr raises ValueError for unsupported merge methods."""
    with pytest.raises(ValueError, match="Invalid merge method"):
        merge_pr(REPO, 42, TOKEN, method="fast-forward")


# ---------------------------------------------------------------------------
# get_pr — empty / malformed output
# ---------------------------------------------------------------------------


def test_get_pr_empty_output_raises(mocker):
    """get_pr returns empty string -> raises GhCliError."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
    )
    with pytest.raises(GhCliError, match="empty output"):
        get_pr(REPO, 1, TOKEN)


def test_get_pr_malformed_json_raises(mocker):
    """get_pr returns non-JSON -> JSONDecodeError propagates."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="not json at all", stderr=""
        ),
    )
    with pytest.raises(json.JSONDecodeError):
        get_pr(REPO, 1, TOKEN)


# ---------------------------------------------------------------------------
# list_open_prs — malformed JSON
# ---------------------------------------------------------------------------


def test_list_open_prs_custom_limit(mocker):
    """list_open_prs passes the custom limit value to the gh CLI."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["gh"], returncode=0, stdout="[]", stderr=""),
    )
    list_open_prs(REPO, TOKEN, limit=250)

    args_passed = mock_run.call_args[0][0]
    assert "--limit" in args_passed
    limit_idx = args_passed.index("--limit")
    assert args_passed[limit_idx + 1] == "250"


def test_list_open_prs_default_limit(mocker):
    """list_open_prs defaults to limit 100 when not specified."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["gh"], returncode=0, stdout="[]", stderr=""),
    )
    list_open_prs(REPO, TOKEN)

    args_passed = mock_run.call_args[0][0]
    assert "--limit" in args_passed
    limit_idx = args_passed.index("--limit")
    assert args_passed[limit_idx + 1] == "100"


def test_list_open_prs_malformed_json(mocker):
    """list_open_prs with invalid JSON stdout -> JSONDecodeError."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="{not valid json", stderr=""
        ),
    )
    with pytest.raises(json.JSONDecodeError):
        list_open_prs(REPO, TOKEN)


# ---------------------------------------------------------------------------
# get_ci_status — missing rollup
# ---------------------------------------------------------------------------


def test_get_ci_status_missing_rollup(mocker):
    """statusCheckRollup absent/None -> returns []."""
    # Case 1: key absent entirely
    pr_data_no_key = {"number": 10, "title": "no checks"}
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=json.dumps(pr_data_no_key), stderr=""
        ),
    )
    assert get_ci_status(REPO, 10, TOKEN) == []

    # Case 2: key present but None
    pr_data_none = {"number": 10, "title": "null checks", "statusCheckRollup": None}
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=json.dumps(pr_data_none), stderr=""
        ),
    )
    assert get_ci_status(REPO, 10, TOKEN) == []


# ---------------------------------------------------------------------------
# get_pr_diff
# ---------------------------------------------------------------------------


def test_get_pr_diff_success(mocker):
    """get_pr_diff returns the diff text from stdout."""
    diff_text = "diff --git a/foo.py b/foo.py\n+new line"
    mock_run = mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=diff_text, stderr=""
        ),
    )
    result = get_pr_diff(REPO, 5, TOKEN)
    assert result == diff_text
    args_passed = mock_run.call_args[0][0]
    assert "pr" in args_passed
    assert "diff" in args_passed
    assert "5" in args_passed
    assert REPO in args_passed


def test_get_pr_diff_failure_raises(mocker):
    """CalledProcessError in get_pr_diff -> GhCliError."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=1,
            cmd=["gh", "pr", "diff"],
            stderr="not found",
        ),
    )
    with pytest.raises(GhCliError):
        get_pr_diff(REPO, 99, TOKEN)


# ---------------------------------------------------------------------------
# get_check_run_logs (_run_gh_bytes path)
# ---------------------------------------------------------------------------


def test_get_check_run_logs_success(mocker):
    """get_check_run_logs returns raw bytes from _run_gh_bytes."""
    zip_bytes = b"PK\x03\x04fake-zip-content"
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=zip_bytes, stderr=b""
        ),
    )
    result = get_check_run_logs(REPO, 12345, TOKEN)
    assert result == zip_bytes
    assert isinstance(result, bytes)


def test_get_check_run_logs_failure_raises(mocker):
    """CalledProcessError in _run_gh_bytes -> GhCliError."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=1,
            cmd=["gh", "api"],
            stderr=b"403 Forbidden",
        ),
    )
    with pytest.raises(GhCliError, match="403 Forbidden"):
        get_check_run_logs(REPO, 12345, TOKEN)


def test_get_check_run_logs_malformed_utf8_stderr(mocker):
    """_run_gh_bytes uses errors='replace' when decoding stderr bytes."""
    # Build stderr with invalid UTF-8 byte sequences
    bad_stderr = b"Error: \xff\xfe bad bytes"
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=1,
            cmd=["gh", "api"],
            stderr=bad_stderr,
        ),
    )
    with pytest.raises(GhCliError) as exc_info:
        get_check_run_logs(REPO, 12345, TOKEN)
    # The replacement character should appear instead of raising UnicodeDecodeError
    assert "\ufffd" in exc_info.value.stderr


# ---------------------------------------------------------------------------
# get_failed_run_logs
# ---------------------------------------------------------------------------


def test_get_failed_run_logs_success(mocker):
    """get_failed_run_logs returns the log text on success."""
    log_text = "Step 3: FAIL npm test\nError: tests failed"
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout=log_text, stderr=""
        ),
    )
    result = get_failed_run_logs(REPO, 9876, TOKEN)
    assert result == log_text


def test_get_failed_run_logs_exception_returns_empty(mocker):
    """get_failed_run_logs swallows exceptions and returns empty string."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=1,
            cmd=["gh", "run", "view"],
            stderr="run not found",
        ),
    )
    result = get_failed_run_logs(REPO, 9876, TOKEN)
    assert result == ""


# ---------------------------------------------------------------------------
# post_comment — body file content
# ---------------------------------------------------------------------------


def test_post_comment_body_file_contains_body(mocker):
    """Verify the temp file written by post_comment contains the body arg."""
    body_text = "## CI Fix\nApplied patch to `src/foo.py`."
    written_content = {}

    def capture_run(args, **kwargs):
        # Find the --body-file path and read it before it's deleted
        if "--body-file" in args:
            idx = args.index("--body-file")
            path = args[idx + 1]
            with open(path) as f:
                written_content["body"] = f.read()
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=capture_run,
    )
    post_comment(REPO, 3, body_text, TOKEN)
    assert written_content["body"] == body_text


# ---------------------------------------------------------------------------
# merge_pr — rebase / merge methods
# ---------------------------------------------------------------------------


def test_merge_pr_rebase_method(mocker):
    """merge_pr with method='rebase' passes --rebase flag."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value="",
    )
    merge_pr(REPO, 10, TOKEN, method="rebase")

    args_passed = mock_run.call_args[0][0]
    assert "--rebase" in args_passed
    assert "--squash" not in args_passed
    assert "--merge" not in args_passed
    assert "--delete-branch" in args_passed


def test_merge_pr_merge_method(mocker):
    """merge_pr with method='merge' passes --merge flag."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value="",
    )
    merge_pr(REPO, 10, TOKEN, method="merge")

    args_passed = mock_run.call_args[0][0]
    assert "--merge" in args_passed
    assert "--squash" not in args_passed
    assert "--rebase" not in args_passed
    assert "--delete-branch" in args_passed


def test_merge_pr_no_delete_branch(mocker):
    """merge_pr with delete_branch=False omits --delete-branch flag."""
    mock_run = mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value="",
    )
    merge_pr(REPO, 10, TOKEN, delete_branch=False)

    args_passed = mock_run.call_args[0][0]
    assert "--delete-branch" not in args_passed
    assert "--squash" in args_passed


# ---------------------------------------------------------------------------
# resolve_review_thread — non-string thread_id
# ---------------------------------------------------------------------------


def test_resolve_review_thread_non_string_id_raises():
    """resolve_review_thread raises ValueError for None and int thread_id."""
    with pytest.raises(ValueError, match="non-empty string"):
        resolve_review_thread(None, TOKEN)

    with pytest.raises(ValueError, match="non-empty string"):
        resolve_review_thread(42, TOKEN)


# ---------------------------------------------------------------------------
# get_unresolved_review_threads — null data / null reviewThreads / null author
# ---------------------------------------------------------------------------


def test_get_unresolved_threads_null_data(mocker):
    """{"data": None} -> GhCliError (null repository)."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps({"data": None}),
    )
    with pytest.raises(GhCliError, match="null repository"):
        get_unresolved_review_threads(REPO, 5, TOKEN)


def test_get_unresolved_threads_null_review_threads(mocker):
    """reviewThreads is None -> returns empty list (no crash)."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps(
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": None,
                        }
                    }
                }
            }
        ),
    )
    result = get_unresolved_review_threads(REPO, 5, TOKEN)
    assert result == []


def test_get_unresolved_threads_null_comment_author(mocker):
    """Comment with author=None -> author defaults to empty string."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps(
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "pageInfo": {"hasNextPage": False},
                                "nodes": [
                                    {
                                        "id": "PRRT_1",
                                        "path": "file.py",
                                        "line": 10,
                                        "isResolved": False,
                                        "comments": {
                                            "nodes": [
                                                {"body": "Ghost comment", "author": None},
                                            ]
                                        },
                                    },
                                ],
                            }
                        }
                    }
                }
            }
        ),
    )
    result = get_unresolved_review_threads(REPO, 5, TOKEN)
    assert len(result) == 1
    assert result[0]["comments"][0]["author"] == ""
    assert result[0]["comments"][0]["body"] == "Ghost comment"


def test_get_unresolved_threads_comment_pagination_warns(mocker, caplog):
    """Logs a warning when a review thread has more than 10 comments."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps(
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "pageInfo": {"hasNextPage": False},
                                "nodes": [
                                    {
                                        "id": "PRRT_many_comments",
                                        "path": "big.py",
                                        "line": 1,
                                        "isResolved": False,
                                        "comments": {
                                            "pageInfo": {"hasNextPage": True},
                                            "nodes": [
                                                {"body": f"Comment {i}", "author": {"login": "u"}}
                                                for i in range(10)
                                            ],
                                        },
                                    },
                                ],
                            }
                        }
                    }
                }
            }
        ),
    )

    with caplog.at_level(logging.WARNING, logger="orcest.orchestrator.gh"):
        result = get_unresolved_review_threads(REPO, 5, TOKEN)

    # Should still return the thread (with the 10 comments it got)
    assert len(result) == 1
    assert result[0]["id"] == "PRRT_many_comments"
    assert len(result[0]["comments"]) == 10
    # Should have logged a warning about truncated comments
    assert any("more than 10 comments" in msg for msg in caplog.messages)


def test_get_unresolved_threads_pagination_fetches_all(mocker, caplog):
    """Fetches all pages and returns threads from every page when hasNextPage is True."""
    page1 = json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {
                            "pageInfo": {"hasNextPage": True, "endCursor": "cursor_abc"},
                            "nodes": [
                                {
                                    "id": "PRRT_page1",
                                    "path": "a.py",
                                    "line": 1,
                                    "isResolved": False,
                                    "comments": {
                                        "pageInfo": {"hasNextPage": False},
                                        "nodes": [{"body": "Fix", "author": {"login": "alice"}}],
                                    },
                                },
                            ],
                        }
                    }
                }
            }
        }
    )
    page2 = json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [
                                {
                                    "id": "PRRT_page2",
                                    "path": "b.py",
                                    "line": 2,
                                    "isResolved": False,
                                    "comments": {
                                        "pageInfo": {"hasNextPage": False},
                                        "nodes": [{"body": "Also fix", "author": {"login": "bob"}}],
                                    },
                                },
                            ],
                        }
                    }
                }
            }
        }
    )
    mock_run = mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        side_effect=[page1, page2],
    )

    with caplog.at_level(logging.WARNING, logger="orcest.orchestrator.gh"):
        result = get_unresolved_review_threads(REPO, 5, TOKEN)

    # Threads from both pages are returned
    assert len(result) == 2
    assert result[0]["id"] == "PRRT_page1"
    assert result[1]["id"] == "PRRT_page2"
    # Warning logged about additional pages
    assert any("more than 100 review threads" in msg for msg in caplog.messages)
    # Second call includes the cursor
    second_args = mock_run.call_args_list[1][0][0]
    assert "after=cursor_abc" in " ".join(second_args)


def test_get_unresolved_threads_missing_cursor_stops_pagination(mocker, caplog):
    """Stops pagination and logs a warning when hasNextPage=True but endCursor is absent."""
    mocker.patch(
        "orcest.orchestrator.gh._run_gh",
        return_value=json.dumps(
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "pageInfo": {"hasNextPage": True},
                                "nodes": [
                                    {
                                        "id": "PRRT_1",
                                        "path": "a.py",
                                        "line": 1,
                                        "isResolved": False,
                                        "comments": {
                                            "pageInfo": {"hasNextPage": False},
                                            "nodes": [
                                                {"body": "Fix", "author": {"login": "alice"}}
                                            ],
                                        },
                                    },
                                ],
                            }
                        }
                    }
                }
            }
        ),
    )
    with caplog.at_level(logging.WARNING, logger="orcest.orchestrator.gh"):
        result = get_unresolved_review_threads(REPO, 5, TOKEN)

    # The fetched thread is still returned
    assert len(result) == 1
    assert result[0]["id"] == "PRRT_1"
    # Both the "more than 100" and "endCursor missing" warnings are logged
    assert any("more than 100 review threads" in msg for msg in caplog.messages)
    assert any("endCursor is missing" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Timeout handling
# ---------------------------------------------------------------------------


def test_run_gh_timeout_raises_gh_cli_error(mocker):
    """subprocess.TimeoutExpired in _run_gh -> GhCliError."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.TimeoutExpired(
            cmd=["gh", "pr", "list"],
            timeout=120,
        ),
    )
    with pytest.raises(GhCliError, match="timed out"):
        list_open_prs(REPO, TOKEN)


def test_run_gh_bytes_timeout_raises_gh_cli_error(mocker):
    """subprocess.TimeoutExpired in _run_gh_bytes -> GhCliError."""
    mocker.patch(
        "orcest.orchestrator.gh.subprocess.run",
        side_effect=subprocess.TimeoutExpired(
            cmd=["gh", "api"],
            timeout=120,
        ),
    )
    with pytest.raises(GhCliError, match="timed out"):
        get_check_run_logs(REPO, 12345, TOKEN)
