"""Tests for orcest.fleet.orchestrator pure functions."""

import subprocess

import pytest
import yaml

from orcest.fleet.config import require_valid_project_name as _validate_project_name
from orcest.fleet.orchestrator import (
    generate_env_file,
    generate_orchestrator_config,
    image_exists,
)

pytestmark = pytest.mark.unit


class TestValidateProjectName:
    def test_valid_names(self):
        for name in ["alpha", "my-project", "v2.0", "test_repo", "A1"]:
            _validate_project_name(name)  # should not raise

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid project name"):
            _validate_project_name("")

    def test_rejects_shell_injection(self):
        with pytest.raises(ValueError):
            _validate_project_name('; rm -rf /')

    def test_rejects_spaces(self):
        with pytest.raises(ValueError):
            _validate_project_name("has space")

    def test_rejects_leading_hyphen(self):
        with pytest.raises(ValueError):
            _validate_project_name("-bad")

    def test_rejects_over_64_chars(self):
        with pytest.raises(ValueError):
            _validate_project_name("a" * 65)


class TestGenerateEnvFile:
    def test_contains_required_vars(self):
        env = generate_env_file(
            github_token="ghp_test",
            key_prefix="myproj",
            project_name="myproj",
        )
        assert "GITHUB_TOKEN='ghp_test'" in env
        assert "GH_TOKEN='ghp_test'" in env
        assert "ORCEST_REDIS_KEY_PREFIX='myproj'" in env
        assert "ORCEST_IMAGE='orcest:latest'" in env
        assert "ORCEST_CONFIG_DIR='/opt/orcest/projects/myproj/config'" in env

    def test_project_name_in_config_dir(self):
        env = generate_env_file(
            github_token="t", key_prefix="special-name",
            project_name="special-name",
        )
        assert "projects/special-name/config" in env

    def test_rejects_newline_in_key_prefix(self):
        with pytest.raises(ValueError, match="key_prefix"):
            generate_env_file(
                github_token="tok",
                key_prefix="bad\nINJECTED_VAR=evil",
                project_name="proj",
            )

    def test_rejects_newline_in_project_name(self):
        # project_name with newline is caught by _validate_project_name (called
        # before _validate_env_value), which raises "Invalid project name".
        with pytest.raises(ValueError, match="Invalid project name"):
            generate_env_file(
                github_token="tok",
                key_prefix="ok",
                project_name="bad\nINJECTED=evil",
            )

    def test_rejects_newline_in_github_token(self):
        with pytest.raises(ValueError, match="github_token"):
            generate_env_file(
                github_token="tok\nINJECTED=evil",
                key_prefix="ok",
                project_name="proj",
            )

    def test_rejects_single_quote_in_value(self):
        with pytest.raises(ValueError, match="single quotes"):
            generate_env_file(
                github_token="tok'evil",
                key_prefix="ok",
                project_name="proj",
            )

    def test_rejects_path_traversal_in_project_name(self):
        """project_name is embedded in a path; reject names that would traverse."""
        with pytest.raises(ValueError):
            generate_env_file(
                github_token="tok",
                key_prefix="ok",
                project_name="../../etc",
            )

    def test_values_are_single_quoted(self):
        """Values must be single-quoted to prevent $-expansion by Docker Compose."""
        env = generate_env_file(
            github_token="ghp_has$dollar",
            key_prefix="pfx",
            project_name="proj",
        )
        # The $dollar should be preserved literally inside single quotes
        assert "GITHUB_TOKEN='ghp_has$dollar'" in env


class TestGenerateOrchestratorConfig:
    def test_basic_structure(self):
        config_yaml = generate_orchestrator_config(
            repo="Org/repo", key_prefix="myproj",
        )
        data = yaml.safe_load(config_yaml)

        assert data["redis"]["host"] == "redis"
        assert data["redis"]["port"] == 6379
        assert data["redis"]["key_prefix"] == "myproj"
        assert data["github"]["repo"] == "Org/repo"

    def test_key_prefix_matches_project(self):
        """The key_prefix in the config matches what was passed."""
        data = yaml.safe_load(
            generate_orchestrator_config(repo="O/r", key_prefix="alpha")
        )
        assert data["redis"]["key_prefix"] == "alpha"


class TestImageExists:
    def test_returns_true_when_image_found(self, mocker):
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=0),
        )
        assert image_exists("user@host") is True

    def test_returns_false_when_image_missing(self, mocker):
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=1),
        )
        assert image_exists("user@host") is False

    def test_rejects_shell_injection_in_image_name(self):
        with pytest.raises(ValueError, match="Invalid Docker image"):
            image_exists("user@host", image="; rm -rf /")

    def test_rejects_backtick_injection(self):
        with pytest.raises(ValueError, match="Invalid Docker image"):
            image_exists("user@host", image="`whoami`")

    def test_accepts_valid_image_references(self, mocker):
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=0),
        )
        for img in [
            "orcest:latest",
            "registry.example.com/orcest:v1.0",
            "ghcr.io/org/image:sha-abc123",
            "ubuntu",
        ]:
            assert image_exists("user@host", image=img) is True
