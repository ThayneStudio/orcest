from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "claude-review-usage.yml"
REVIEW_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "claude-review.yml"
COMPOSE_FILES = (
    REPO_ROOT / "docker-compose.yml",
    REPO_ROOT / "src" / "orcest" / "fleet" / "deploy" / "docker-compose.yml",
)


def test_claude_review_usage_diagnostic_is_dispatch_only() -> None:
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)

    triggers = workflow["on"]
    assert "workflow_dispatch" in triggers
    assert "pull_request" not in triggers


def test_claude_review_usage_diagnostic_has_no_full_output_flag() -> None:
    assert "show_full_output" not in WORKFLOW.read_text()


def test_claude_review_usage_diagnostic_does_not_interpolate_secret_in_run_script() -> None:
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    steps = workflow["jobs"]["usage"]["steps"]
    run_scripts = [step["run"] for step in steps if "run" in step]

    assert run_scripts
    assert all("${{ secrets.CLAUDE_CODE_OAUTH_TOKEN }}" not in script for script in run_scripts)


def test_claude_review_workflows_use_actions_oauth_secret() -> None:
    """Actions review plane resolves CLAUDE_CODE_OAUTH_TOKEN from the repo secret."""
    review = REVIEW_WORKFLOW.read_text()
    usage = WORKFLOW.read_text()
    assert "claude_code_oauth_token: ${{ secrets.CLAUDE_CODE_OAUTH_TOKEN }}" in review
    assert "CLAUDE_CODE_OAUTH_TOKEN: ${{ secrets.CLAUDE_CODE_OAUTH_TOKEN }}" in usage
    assert "ANTHROPIC_API_KEY" not in review
    assert "ANTHROPIC_API_KEY" not in usage


def test_compose_does_not_forward_anthropic_api_key() -> None:
    """Runtime compose must not inherit or expose ANTHROPIC_API_KEY."""
    for path in COMPOSE_FILES:
        text = path.read_text()
        assert "CLAUDE_CODE_OAUTH_TOKEN" in text
        assert "ANTHROPIC_API_KEY" not in text
