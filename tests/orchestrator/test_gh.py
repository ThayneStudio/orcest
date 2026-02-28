"""Unit tests for the GitHub CLI wrapper (orchestrator/gh.py).

Every test mocks ``subprocess.run`` so that no real ``gh`` process is spawned.
The mock target is the *module-level* import inside ``gh.py``, i.e.
``orcest.orchestrator.gh.subprocess.run``.
"""

import json
import subprocess

import pytest

from orcest.orchestrator.gh import (
    GhCliError,
    GhNotInstalledError,
    add_label,
    get_ci_status,
    get_pr,
    list_open_prs,
    post_comment,
    remove_label,
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
