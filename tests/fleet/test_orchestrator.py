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
            redis_port=6380,
            project_name="myproj",
        )
        assert "GITHUB_TOKEN=ghp_test" in env
        assert "GH_TOKEN=ghp_test" in env
        assert "REDIS_PORT=6380" in env
        assert "ORCEST_IMAGE=orcest:latest" in env
        assert "ORCEST_CONFIG_DIR=/opt/orcest/projects/myproj/config" in env

    def test_project_name_in_config_dir(self):
        env = generate_env_file(
            github_token="t", redis_port=6379, project_name="special-name",
        )
        assert "projects/special-name/config" in env


class TestGenerateOrchestratorConfig:
    def test_basic_structure(self):
        config_yaml = generate_orchestrator_config(repo="Org/repo", redis_port=6380)
        data = yaml.safe_load(config_yaml)

        assert data["redis"]["host"] == "redis"
        # Internal port is always 6379 regardless of redis_port arg
        assert data["redis"]["port"] == 6379
        assert data["github"]["repo"] == "Org/repo"

    def test_internal_port_always_6379(self):
        """The Redis port inside Docker is always 6379, regardless of the external mapping."""
        for port in [6379, 6380, 6399]:
            data = yaml.safe_load(generate_orchestrator_config(repo="O/r", redis_port=port))
            assert data["redis"]["port"] == 6379


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
