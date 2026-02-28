"""Unit tests for workspace management (worker/workspace.py).

Every test mocks ``subprocess.run`` so that no real ``git`` process is spawned.
The mock target is ``orcest.worker.workspace.subprocess.run``.
"""

import subprocess

import pytest

from orcest.worker.workspace import Workspace, WorkspaceError

REPO = "acme/widgets"
TOKEN = "test-token-workspace-xyz789"
BRANCH = "feat"


# ---------------------------------------------------------------------------
# setup -- cloning
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_setup_clones_repo(mocker, tmp_path):
    """setup() invokes git clone for the given repo."""
    mock_run = mocker.patch(
        "orcest.worker.workspace.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr=""),
    )
    ws = Workspace(str(tmp_path))
    ws.setup(REPO, BRANCH, TOKEN)

    # First call is the clone, second is remote set-url
    clone_call = mock_run.call_args_list[0]
    clone_args = clone_call[0][0]
    assert clone_args[0] == "git"
    assert "clone" in clone_args
    # The clone URL embeds the token for auth
    clone_url = f"https://x-access-token:{TOKEN}@github.com/{REPO}.git"
    assert clone_url in clone_args


@pytest.mark.unit
def test_setup_shallow_clone_args(mocker, tmp_path):
    """setup() uses --depth 1 --single-branch for a shallow clone."""
    mock_run = mocker.patch(
        "orcest.worker.workspace.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr=""),
    )
    ws = Workspace(str(tmp_path))
    ws.setup(REPO, BRANCH, TOKEN)

    clone_args = mock_run.call_args_list[0][0][0]
    assert "--depth" in clone_args
    depth_idx = clone_args.index("--depth")
    assert clone_args[depth_idx + 1] == "1"
    assert "--single-branch" in clone_args


@pytest.mark.unit
def test_setup_with_branch(mocker, tmp_path):
    """When branch is provided, --branch <name> appears in clone args."""
    mock_run = mocker.patch(
        "orcest.worker.workspace.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr=""),
    )
    ws = Workspace(str(tmp_path))
    ws.setup(REPO, "feat", TOKEN)

    clone_args = mock_run.call_args_list[0][0][0]
    assert "--branch" in clone_args
    branch_idx = clone_args.index("--branch")
    assert clone_args[branch_idx + 1] == "feat"


@pytest.mark.unit
def test_setup_without_branch(mocker, tmp_path):
    """When branch is None, --branch does NOT appear in clone args."""
    mock_run = mocker.patch(
        "orcest.worker.workspace.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr=""),
    )
    ws = Workspace(str(tmp_path))
    ws.setup(REPO, None, TOKEN)

    clone_args = mock_run.call_args_list[0][0][0]
    assert "--branch" not in clone_args


# ---------------------------------------------------------------------------
# setup -- credential stripping
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_setup_strips_credentials(mocker, tmp_path):
    """After cloning, setup() calls git remote set-url to remove the token."""
    mock_run = mocker.patch(
        "orcest.worker.workspace.subprocess.run",
        return_value=subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr=""),
    )
    ws = Workspace(str(tmp_path))
    ws.setup(REPO, BRANCH, TOKEN)

    # Second subprocess.run call should be the remote set-url
    assert mock_run.call_count == 2
    seturl_args = mock_run.call_args_list[1][0][0]
    assert "remote" in seturl_args
    assert "set-url" in seturl_args
    assert "origin" in seturl_args
    # The sanitized URL should NOT contain the token
    clean_url = f"https://github.com/{REPO}.git"
    assert clean_url in seturl_args


# ---------------------------------------------------------------------------
# setup -- error handling
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_clone_failure_raises_workspace_error(mocker, tmp_path):
    """CalledProcessError during clone -> WorkspaceError."""
    mocker.patch(
        "orcest.worker.workspace.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=128,
            cmd=["git", "clone"],
            stderr="fatal: repository not found",
        ),
    )
    ws = Workspace(str(tmp_path))
    with pytest.raises(WorkspaceError, match="git clone failed"):
        ws.setup(REPO, None, TOKEN)


@pytest.mark.unit
def test_token_sanitized_in_error(mocker, tmp_path):
    """When clone fails, the token is replaced with '***' in the error message."""
    error_msg = f"fatal: could not read from https://x-access-token:{TOKEN}@github.com"
    mocker.patch(
        "orcest.worker.workspace.subprocess.run",
        side_effect=subprocess.CalledProcessError(
            returncode=128,
            cmd=["git", "clone"],
            stderr=error_msg,
        ),
    )
    ws = Workspace(str(tmp_path))
    with pytest.raises(WorkspaceError) as exc_info:
        ws.setup(REPO, None, TOKEN)

    error_text = str(exc_info.value)
    assert TOKEN not in error_text
    assert "***" in error_text


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cleanup_removes_directory(tmp_path):
    """cleanup() removes the workspace temp directory."""
    # Simulate what setup() does: create a temp dir under base_dir
    ws = Workspace(str(tmp_path))
    fake_temp = tmp_path / "orcest_tmp_abc"
    fake_temp.mkdir()
    fake_repo = fake_temp / "repo"
    fake_repo.mkdir()

    # Manually set internal state to point at our fake dirs
    ws._temp_dir = fake_temp
    ws._work_dir = fake_repo

    ws.cleanup()
    assert not fake_temp.exists()


@pytest.mark.unit
def test_cleanup_idempotent(tmp_path):
    """Calling cleanup() twice does not raise an error."""
    ws = Workspace(str(tmp_path))
    fake_temp = tmp_path / "orcest_tmp_def"
    fake_temp.mkdir()
    ws._temp_dir = fake_temp
    ws._work_dir = fake_temp / "repo"

    ws.cleanup()
    ws.cleanup()  # Should not raise
