"""Tests for orcest.fleet.orchestrator pure functions."""

import subprocess

import pytest
import yaml

from orcest.fleet.config import require_valid_project_name as _validate_project_name
from orcest.fleet.orchestrator import (
    generate_env_file,
    generate_orchestrator_config,
    image_exists,
    upload_fleet_config,
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


class TestUploadFleetConfig:
    def _ok(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    def _fail(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="oops")

    def test_happy_path(self, mocker, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("test: true\n")
        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        scp = mocker.patch("orcest.fleet.orchestrator._scp", side_effect=self._ok)
        upload_fleet_config("user@host", str(cfg_file))
        # mkdir, mv+chmod
        assert ssh.call_count == 2
        assert scp.call_count == 1
        # Verify the SSH commands are correct
        ssh.assert_any_call("user@host", "sudo mkdir -p /etc/orcest")
        ssh.assert_any_call(
            "user@host",
            "sudo mv /tmp/.orcest-config.yaml.tmp /etc/orcest/config.yaml"
            " && sudo chmod 600 /etc/orcest/config.yaml",
        )
        # Verify SCP uploads the local file to the temp path on the remote
        scp.assert_called_once_with(
            str(cfg_file), "user@host", "/tmp/.orcest-config.yaml.tmp",
        )

    def test_missing_config_raises(self):
        with pytest.raises(FileNotFoundError, match="Fleet config not found"):
            upload_fleet_config("user@host", "/nonexistent/config.yaml")

    def test_mkdir_failure_raises(self, mocker, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("test: true\n")
        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._fail)
        with pytest.raises(RuntimeError, match="Failed to create /etc/orcest"):
            upload_fleet_config("user@host", str(cfg_file))

    def test_scp_failure_raises(self, mocker, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("test: true\n")
        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        mocker.patch("orcest.fleet.orchestrator._scp", side_effect=self._fail)
        with pytest.raises(RuntimeError, match="Failed to upload fleet config"):
            upload_fleet_config("user@host", str(cfg_file))
        # Only the mkdir call should have happened; mv+chmod must not run
        assert ssh.call_count == 1

    def test_mv_failure_cleans_up(self, mocker, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("test: true\n")
        call_count = 0

        def ssh_side_effect(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # mkdir
                return self._ok()
            elif call_count == 2:  # mv+chmod
                return self._fail()
            else:  # cleanup rm
                return self._ok()

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        mocker.patch("orcest.fleet.orchestrator._scp", side_effect=self._ok)
        with pytest.raises(RuntimeError, match="Failed to install fleet config"):
            upload_fleet_config("user@host", str(cfg_file))
        # mkdir + mv(fail) + rm cleanup
        assert ssh.call_count == 3
        # Verify the cleanup call removes the temp file
        cleanup_call = ssh.call_args_list[2]
        assert cleanup_call == mocker.call(
            "user@host", "rm -f /tmp/.orcest-config.yaml.tmp",
        )


class TestStopPoolManager:
    def _ok(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    def _fail(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="oops")

    def test_success(self, mocker):
        from orcest.fleet.orchestrator import stop_pool_manager
        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        stop_pool_manager("user@host")
        ssh.assert_called_once()
        assert "docker compose" in ssh.call_args[0][1]
        assert "down" in ssh.call_args[0][1]

    def test_failure_raises(self, mocker):
        from orcest.fleet.orchestrator import stop_pool_manager
        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._fail)
        with pytest.raises(RuntimeError, match="Failed to stop pool manager"):
            stop_pool_manager("user@host")


class TestGetPoolRedisMembers:
    def test_parses_idle_and_active(self, mocker):
        from orcest.fleet.orchestrator import get_pool_redis_members
        def ssh_side_effect(target, cmd):
            if "SMEMBERS" in cmd:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="300\n301\n", stderr="",
                )
            if "HGETALL" in cmd:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="302\n1000.0\n303\n2000.0\n", stderr="",
                )
            return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")
        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        idle, active = get_pool_redis_members("user@host")
        assert idle == {"300", "301"}
        assert active == {"302": "1000.0", "303": "2000.0"}

    def test_handles_empty(self, mocker):
        from orcest.fleet.orchestrator import get_pool_redis_members
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
        )
        idle, active = get_pool_redis_members("user@host")
        assert idle == set()
        assert active == {}


class TestCleanPoolRedis:
    def test_builds_correct_commands(self, mocker):
        from orcest.fleet.orchestrator import clean_pool_redis
        ssh = mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
        )
        clean_pool_redis("user@host", ["300", "301"])
        ssh.assert_called_once()
        cmd = ssh.call_args[0][1]
        assert "SREM orcest:pool:idle" in cmd
        assert "HDEL orcest:pool:active" in cmd
        assert "300" in cmd
        assert "301" in cmd

    def test_noop_for_empty_list(self, mocker):
        from orcest.fleet.orchestrator import clean_pool_redis
        ssh = mocker.patch("orcest.fleet.orchestrator._ssh")
        clean_pool_redis("user@host", [])
        ssh.assert_not_called()
