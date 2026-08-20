"""Tests for the Claude Code PreToolUse merge-policy hook."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[1] / ".claude" / "hooks" / "block-direct-pr-merge.sh"
SETTINGS = Path(__file__).resolve().parents[1] / ".claude" / "settings.json"
CLAUDE_REVIEW_WORKFLOW = (
    Path(__file__).resolve().parents[1] / ".github" / "workflows" / "claude-review.yml"
)


def _run_hook(
    payload: dict[str, object] | str, *, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    data = payload if isinstance(payload, str) else json.dumps(payload)
    return subprocess.run(
        ["/bin/bash", str(HOOK)],
        input=data,
        capture_output=True,
        text=True,
        env=env if env is not None else os.environ.copy(),
        check=False,
    )


def _denied(result: subprocess.CompletedProcess[str], reason_substr: str) -> None:
    assert result.returncode == 0, result.stderr
    body = json.loads(result.stdout)
    output = body["hookSpecificOutput"]
    assert output["permissionDecision"] == "deny"
    assert reason_substr in output["permissionDecisionReason"]


def _allowed(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def _payload(command: str) -> dict[str, object]:
    return {"tool_name": "Bash", "tool_input": {"command": command}}


@pytest.mark.parametrize(
    "command",
    [
        "gh pr merge 123",
        "cd repo && gh pr merge 123 --squash",
        "gh  pr   merge 1",
        "gh pr \\\n  merge 123",
        "gh --repo o/r pr merge 9",
        "gh -R o/r pr merge 9 --squash",
        "gh api -X PUT repos/o/r/pulls/9/merge",
        "gh --repo o/r api -X PUT repos/o/r/pulls/9/merge",
        "gh api graphql -f query='mutation { mergePullRequest(input: {}) }'",
    ],
)
def test_denies_merge_commands(command: str) -> None:
    _denied(_run_hook(_payload(command)), "PR merges go through orcest")


@pytest.mark.parametrize(
    "command",
    [
        "gh pr view 123",
        "gh pr list",
        "gh pr ready",
        "gh pr diff 552",
        "gh pr review 552 --approve",
        "git merge main",
        "echo hello",
        "",
    ],
)
def test_allows_non_merge_commands(command: str) -> None:
    _allowed(_run_hook(_payload(command)))


def test_fail_closed_on_invalid_json() -> None:
    _denied(_run_hook("not json"), "could not parse")


def test_fail_closed_on_non_object() -> None:
    _denied(_run_hook("[]"), "could not parse")


def test_fail_closed_on_empty_stdin() -> None:
    _denied(_run_hook(""), "could not parse")


def test_missing_tool_input_is_not_a_merge() -> None:
    _allowed(_run_hook({"tool_name": "Bash"}))


def test_fail_closed_when_parsers_missing(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name in ("cat", "tr", "grep"):
        for prefix in (Path("/usr/bin"), Path("/bin")):
            src = prefix / name
            if src.exists():
                (bin_dir / name).symlink_to(src)
                break
        else:
            pytest.skip(f"{name} not found on this system")
    env = os.environ.copy()
    env["PATH"] = str(bin_dir)
    _denied(_run_hook(_payload("gh pr view 1"), env=env), "could not parse")


def test_hook_is_executable() -> None:
    assert HOOK.is_file()
    assert os.access(HOOK, os.X_OK)


def test_settings_point_at_hook() -> None:
    settings = json.loads(SETTINGS.read_text())
    hooks = settings["hooks"]["PreToolUse"]
    commands = [h["command"] for group in hooks for h in group["hooks"]]
    assert any(command.endswith("block-direct-pr-merge.sh") for command in commands)


def test_claude_review_workflow_pins_cli_and_keeps_known_good_args() -> None:
    """@v1 floated to CLI 2.1.236, which dies with is_error:true / $0.

    Pin the action SHA that still ships 2.1.235, and do not pass
    --setting-sources: on that CLI it produces the same immediate crash.
    The action already restores .claude from the base branch.
    """
    text = CLAUDE_REVIEW_WORKFLOW.read_text()
    assert "uses: anthropics/claude-code-action@d40ddef4c030e508327d6e35a9c45f3368482c50" in text
    assert "uses: anthropics/claude-code-action@v1" not in text
    args_lines = [
        line.strip() for line in text.splitlines() if line.strip().startswith("claude_args:")
    ]
    assert args_lines, "claude-review.yml must set claude_args"
    assert all("--setting-sources" not in line for line in args_lines)
    assert any("Bash(gh *)" in line for line in args_lines)
    assert "continue-on-error: true" in text
