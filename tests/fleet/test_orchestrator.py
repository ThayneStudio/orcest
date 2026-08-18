"""Tests for orcest.fleet.orchestrator pure functions."""

import subprocess
from pathlib import Path

import pytest
import yaml

from orcest.fleet.config import require_valid_project_name as _validate_project_name
from orcest.fleet.orchestrator import (
    clean_pending_tasks,
    generate_env_file,
    generate_orchestrator_config,
    get_deployed_pool_backend,
    get_deployed_pool_vmid_range,
    get_worker_heartbeats,
    image_exists,
    upload_fleet_config,
)

pytestmark = pytest.mark.unit


def test_get_deployed_pool_backend_defaults_legacy_config_to_claude(mocker):
    mocker.patch(
        "orcest.fleet.orchestrator._ssh",
        return_value=subprocess.CompletedProcess([], 0, stdout="pool: {}\n", stderr=""),
    )

    # Legacy deployed configs predate worker_runner_mode; they were deployed
    # with the interactive PTY runner, so the signature reflects that default.
    assert get_deployed_pool_backend("user@host") == (
        "vm_id_start=0;vm_id_end=0;backend=claude;runner=claude;mode=interactive"
    )


def test_get_deployed_pool_backend_reads_configured_backend(mocker):
    mocker.patch(
        "orcest.fleet.orchestrator._ssh",
        return_value=subprocess.CompletedProcess(
            [], 0, stdout="pool:\n  worker_backend: clauder\n", stderr=""
        ),
    )

    assert get_deployed_pool_backend("user@host") == (
        "vm_id_start=0;vm_id_end=0;backend=clauder;runner=claude;mode=interactive"
    )


def test_get_deployed_pool_backend_returns_full_mixed_layout_signature(mocker):
    mocker.patch(
        "orcest.fleet.orchestrator._ssh",
        return_value=subprocess.CompletedProcess(
            [],
            0,
            stdout=(
                "pool:\n"
                "  vm_id_start: 10000\n"
                "  worker_profiles:\n"
                "    - backend: clauder\n"
                "    - backend: codex\n"
                "    - backend: grok\n"
            ),
            stderr="",
        ),
    )

    assert get_deployed_pool_backend("user@host") == (
        "vm_id_start=10000;vm_id_end=0;profiles=clauder:claude:interactive,codex:codex:,grok:grok:"
    )


def test_get_deployed_pool_vmid_range_reads_both_bounds(mocker):
    mocker.patch(
        "orcest.fleet.orchestrator._ssh",
        return_value=subprocess.CompletedProcess(
            [],
            0,
            stdout="pool:\n  vm_id_start: 10000\n  vm_id_end: 10099\n",
            stderr="",
        ),
    )

    assert get_deployed_pool_vmid_range("user@host") == (10000, 10099)


def test_get_worker_heartbeats_returns_backend_and_revision(mocker):
    key = "orcest:workers:heartbeat:orcest-worker-10001"
    ssh = mocker.patch(
        "orcest.fleet.orchestrator._ssh",
        side_effect=[
            subprocess.CompletedProcess([], 0, stdout=f"{key}\n", stderr=""),
            subprocess.CompletedProcess(
                [],
                0,
                stdout='{"backend":"codex","revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\n',
                stderr="",
            ),
        ],
    )

    assert get_worker_heartbeats("user@host") == {"orcest-worker-10001": ("codex", "a" * 40)}
    assert ssh.call_count == 2


class TestValidateProjectName:
    def test_valid_names(self):
        for name in ["alpha", "my-project", "v2.0", "test_repo", "A1"]:
            _validate_project_name(name)  # should not raise

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid project name"):
            _validate_project_name("")

    def test_rejects_shell_injection(self):
        with pytest.raises(ValueError):
            _validate_project_name("; rm -rf /")

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
            github_token="t",
            key_prefix="special-name",
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

    def test_generate_env_file_emits_redis_password(self):
        """C1: a supplied redis_password is written (single-quoted) into the .env
        so the orchestrator container can AUTH to the password-protected Redis."""
        env = generate_env_file(
            github_token="ghp_test",
            key_prefix="myproj",
            project_name="myproj",
            redis_password="s3cr3t-pw",
        )
        assert "ORCEST_REDIS_PASSWORD='s3cr3t-pw'" in env

    def test_generate_env_file_omits_redis_password_when_empty(self):
        """C1: backward compat -- no password arg means no ORCEST_REDIS_PASSWORD
        line (so existing single-token onboarding output is unchanged)."""
        env = generate_env_file(github_token="t", key_prefix="p", project_name="p")
        assert "ORCEST_REDIS_PASSWORD" not in env

    def test_generate_env_file_emits_monitor_write_token(self):
        env = generate_env_file(
            github_token="t",
            key_prefix="p",
            project_name="p",
            monitor_write_token="monitor-secret",
        )

        assert "MONITOR_WRITE_TOKEN='monitor-secret'" in env

    def test_generate_env_file_rejects_single_quote_in_redis_password(self):
        """C1: the password is single-quoted in .env, so a single quote must be
        rejected (mirrors the github_token/key_prefix injection guards)."""
        with pytest.raises(ValueError, match="redis_password"):
            generate_env_file(
                github_token="t",
                key_prefix="p",
                project_name="p",
                redis_password="pw'injected",
            )

    def test_generate_env_file_emits_clauder_api_key(self):
        env = generate_env_file(
            github_token="t",
            key_prefix="p",
            project_name="p",
            provider_credentials={"clauder": ["clauder-oauth-token"]},
        )

        assert "CLAUDER_API_KEY='clauder-oauth-token'" in env


class TestGenerateOrchestratorConfig:
    def test_basic_structure(self):
        config_yaml = generate_orchestrator_config(
            repo="Org/repo",
            key_prefix="myproj",
        )
        data = yaml.safe_load(config_yaml)

        assert data["redis"]["host"] == "redis"
        assert data["redis"]["port"] == 6379
        assert data["redis"]["key_prefix"] == "myproj"
        assert data["github"]["repo"] == "Org/repo"

    def test_key_prefix_matches_project(self):
        """The key_prefix in the config matches what was passed."""
        data = yaml.safe_load(generate_orchestrator_config(repo="O/r", key_prefix="alpha"))
        assert data["redis"]["key_prefix"] == "alpha"

    def test_emits_monitor_ingest_wiring(self):
        data = yaml.safe_load(
            generate_orchestrator_config(
                repo="O/r",
                key_prefix="p",
                monitor_ingest_url="http://monitor:9091/ingest/v1/events",
            )
        )

        assert data["monitor_ingest_url"] == "http://monitor:9091/ingest/v1/events"
        assert data["monitor_write_token_env"] == "MONITOR_WRITE_TOKEN"

    def test_no_providers_block_without_extra_providers(self):
        """Default: no providers: block (claude comes from legacy synthesis)."""
        data = yaml.safe_load(generate_orchestrator_config(repo="O/r", key_prefix="p"))
        assert "providers" not in data

    def test_emits_providers_block_for_grok(self):
        """A non-claude provider is emitted as a credential-empty providers entry
        (the orchestrator resolves the value from .env via XAI_API_KEY)."""
        data = yaml.safe_load(
            generate_orchestrator_config(repo="O/r", key_prefix="p", extra_providers=["grok"])
        )
        assert data["providers"] == [{"provider": "grok", "credential": "", "model": ""}]

    def test_claude_excluded_from_providers_block(self):
        """Claude is synthesized from CLAUDE_CODE_OAUTH_TOKENS, not listed here,
        so it is dropped from extra_providers to avoid a double entry."""
        data = yaml.safe_load(
            generate_orchestrator_config(
                repo="O/r", key_prefix="p", extra_providers=["claude", "grok"]
            )
        )
        provs = [p["provider"] for p in data["providers"]]
        assert provs == ["grok"]

    def test_clauder_becomes_generated_default_runner(self):
        """A clauder pool should publish Claude-backed work to tasks:clauder."""
        data = yaml.safe_load(
            generate_orchestrator_config(repo="O/r", key_prefix="p", extra_providers=["clauder"])
        )
        assert data["default_runner"] == "clauder"
        assert data["providers"] == [{"provider": "clauder", "credential": "", "model": ""}]

    def test_default_runner_can_follow_pool_backend_without_provider_credentials(self):
        data = yaml.safe_load(
            generate_orchestrator_config(repo="O/r", key_prefix="p", default_runner="clauder")
        )

        assert data["default_runner"] == "clauder"
        assert "providers" not in data


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
        remote_tmp = "/tmp/orcest-config.ABC123"

        def ssh_side_effect(_target, command):
            if "mktemp" in command:
                return subprocess.CompletedProcess([], 0, stdout=f"{remote_tmp}\n", stderr="")
            return self._ok()

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        scp = mocker.patch("orcest.fleet.orchestrator._scp", side_effect=self._ok)
        upload_fleet_config("user@host", str(cfg_file))
        # mkdir, private mktemp, install, cleanup
        assert ssh.call_count == 4
        assert scp.call_count == 1
        ssh.assert_any_call("user@host", "sudo mkdir -p /etc/orcest")
        ssh.assert_any_call(
            "user@host",
            f"sudo install -m 600 -o orcest -g orcest {remote_tmp} /etc/orcest/config.yaml",
        )
        ssh.assert_any_call("user@host", f"rm -f {remote_tmp}")
        scp.assert_called_once_with(str(cfg_file), "user@host", remote_tmp)

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
        remote_tmp = "/tmp/orcest-config.ABC123"

        def ssh_side_effect(_target, command):
            if "mktemp" in command:
                return subprocess.CompletedProcess([], 0, stdout=f"{remote_tmp}\n", stderr="")
            return self._ok()

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        mocker.patch("orcest.fleet.orchestrator._scp", side_effect=self._fail)
        with pytest.raises(RuntimeError, match="Failed to upload fleet config"):
            upload_fleet_config("user@host", str(cfg_file))
        assert ssh.call_count == 3  # mkdir, mktemp, cleanup; no install
        ssh.assert_any_call("user@host", f"rm -f {remote_tmp}")

    def test_mv_failure_cleans_up(self, mocker, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("test: true\n")
        call_count = 0

        def ssh_side_effect(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # mkdir
                return self._ok()
            elif call_count == 2:  # mktemp
                return subprocess.CompletedProcess(
                    [], 0, stdout="/tmp/orcest-config.ABC123\n", stderr=""
                )
            elif call_count == 3:  # install
                return self._fail()
            else:  # cleanup rm
                return self._ok()

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        mocker.patch("orcest.fleet.orchestrator._scp", side_effect=self._ok)
        with pytest.raises(RuntimeError, match="Failed to install fleet config"):
            upload_fleet_config("user@host", str(cfg_file))
        # mkdir + mktemp + install(fail) + rm cleanup
        assert ssh.call_count == 4
        # Verify the cleanup call removes the temp file
        cleanup_call = ssh.call_args_list[3]
        assert cleanup_call == mocker.call(
            "user@host",
            "rm -f /tmp/orcest-config.ABC123",
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
                    args=[],
                    returncode=0,
                    stdout="300\n301\n",
                    stderr="",
                )
            if "HGETALL" in cmd:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="302\n1000.0\n303\n2000.0\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")

        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        idle, active = get_pool_redis_members("user@host")
        assert idle == {"300", "301"}
        assert active == {"302": "1000.0", "303": "2000.0"}

    def test_rejects_odd_hgetall_output(self, mocker):
        from orcest.fleet.orchestrator import get_pool_redis_members

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            side_effect=[
                subprocess.CompletedProcess(args=[], returncode=0, stdout="300\n", stderr=""),
                subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="302\n1000.0\n303\n", stderr=""
                ),
            ],
        )

        with pytest.raises(RuntimeError, match="odd number"):
            get_pool_redis_members("user@host")

    def test_handles_empty(self, mocker):
        from orcest.fleet.orchestrator import get_pool_redis_members

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
        )
        idle, active = get_pool_redis_members("user@host")
        assert idle == set()
        assert active == {}

    def test_raises_when_idle_read_fails(self, mocker):
        from orcest.fleet.orchestrator import get_pool_redis_members

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="NOAUTH Authentication required.",
            ),
        )

        with pytest.raises(RuntimeError, match="Failed to read pool idle set"):
            get_pool_redis_members("user@host")

    @pytest.mark.parametrize(
        "diagnostic",
        [
            "NOAUTH Authentication required.",
            "(error) WRONGTYPE Operation against a key holding the wrong kind of value",
            "ERR invalid command",
        ],
    )
    def test_raises_on_redis_error_output_even_with_zero_exit(self, mocker, diagnostic):
        from orcest.fleet.orchestrator import get_pool_redis_members

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout=f"{diagnostic}\n", stderr=""
            ),
        )

        with pytest.raises(RuntimeError, match="Failed to read pool idle set"):
            get_pool_redis_members("user@host")

    def test_raises_when_active_read_fails(self, mocker):
        from orcest.fleet.orchestrator import get_pool_redis_members

        def ssh_side_effect(target, cmd):
            if "SMEMBERS" in cmd:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="300\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="redis unavailable",
            )

        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)

        with pytest.raises(RuntimeError, match="Failed to read pool active hash"):
            get_pool_redis_members("user@host")


class TestCleanPoolRedis:
    def test_builds_correct_commands(self, mocker):
        from orcest.fleet.orchestrator import clean_pool_redis

        def ssh_side_effect(_target, command):
            stdout = "0\n0\n0\n0\n0\n" if "EXISTS" in command else ""
            return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")

        ssh = mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            side_effect=ssh_side_effect,
        )
        clean_pool_redis("user@host", ["300", "301"])
        assert ssh.call_count == 3
        cmd = ssh.call_args_list[0][0][1]
        assert "SREM orcest:pool:idle" in cmd
        assert "HDEL orcest:pool:active" in cmd
        assert "SREM orcest:pool:draining orcest-worker-300" in cmd
        assert "SREM orcest:pool:provisioning 300" in cmd
        assert "SREM orcest:pool:ambiguous-clones 300" in cmd
        assert "DEL orcest:pool:done:orcest-worker-300" in cmd
        assert "DEL orcest:workers:heartbeat:orcest-worker-300" in cmd
        assert "300" in cmd
        assert "301" in cmd

    def test_noop_for_empty_list(self, mocker):
        from orcest.fleet.orchestrator import clean_pool_redis

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh")
        clean_pool_redis("user@host", [])
        ssh.assert_not_called()

    def test_failure_raises(self, mocker):
        from orcest.fleet.orchestrator import clean_pool_redis

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="redis unavailable",
            ),
        )

        with pytest.raises(RuntimeError, match="Failed to clean pool Redis state"):
            clean_pool_redis("user@host", ["300"])

    def test_marker_verification_failure_raises(self, mocker):
        from orcest.fleet.orchestrator import clean_pool_redis

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            side_effect=[
                subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="1\n0\n0\n0\n0\n", stderr=""
                ),
            ],
        )

        with pytest.raises(RuntimeError, match="expected lifecycle markers to be absent"):
            clean_pool_redis("user@host", ["300"])


class TestRedisCliRoutedThroughDockerExec:
    """Bug 3: redis-cli must run inside the orcest-redis-redis-1 container.

    C1: the prefix now also authenticates (reads ORCEST_REDIS_PASSWORD from the
    container env), so assertions check the docker-exec routing rather than the
    exact unauthenticated string.
    """

    def _ok(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    def test_set_current_template_vmid_uses_docker_exec(self, mocker):
        from orcest.fleet.orchestrator import set_current_template_vmid

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        set_current_template_vmid("user@host", 9001)
        cmd = ssh.call_args[0][1]
        assert "docker exec orcest-redis-redis-1" in cmd
        assert "SET orcest:pool:current_template_vmid" in cmd
        assert "9001" in cmd

    def test_get_current_template_vmid_uses_docker_exec(self, mocker):
        from orcest.fleet.orchestrator import get_current_template_vmid

        ssh = mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout="9001\n", stderr=""
            ),
        )
        result = get_current_template_vmid("user@host")
        assert result == 9001
        cmd = ssh.call_args[0][1]
        assert "docker exec orcest-redis-redis-1" in cmd
        assert "GET orcest:pool:current_template_vmid" in cmd

    def test_get_pool_redis_members_uses_docker_exec(self, mocker):
        from orcest.fleet.orchestrator import get_pool_redis_members

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        get_pool_redis_members("user@host")
        for call in ssh.call_args_list:
            assert "docker exec orcest-redis-redis-1" in call[0][1]

    def test_clean_pool_redis_uses_docker_exec(self, mocker):
        from orcest.fleet.orchestrator import clean_pool_redis

        def ssh_side_effect(_target, command):
            if "EXISTS" in command:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="0\n0\n0\n0\n0\n", stderr=""
                )
            return self._ok()

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        clean_pool_redis("user@host", ["300"])
        cleanup_cmd = ssh.call_args_list[0][0][1]
        verify_cmd = ssh.call_args_list[1][0][1]
        assert cleanup_cmd.count("docker exec orcest-redis-redis-1") == 7
        assert verify_cmd.count("docker exec orcest-redis-redis-1") == 5

    def test_clean_pending_tasks_uses_docker_exec(self, mocker):
        from orcest.fleet.orchestrator import clean_pending_tasks

        call_count = 0

        def ssh_side_effect(target, command):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="orcest:pending:foo\n",
                    stderr="",
                )
            return self._ok()

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        clean_pending_tasks("user@host")
        for call in ssh.call_args_list:
            assert "docker exec orcest-redis-redis-1" in call[0][1]

    def test_get_workers_with_pending_tasks_uses_docker_exec(self, mocker):
        from orcest.fleet.orchestrator import get_workers_with_pending_tasks

        def ssh_side_effect(target, command):
            if "--scan" in command:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="orcest:tasks:claude\norcest:tasks:issue:claude\n",
                    stderr="",
                )
            if " TYPE " in command:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="stream\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=(
                    "name\norcest-worker-300\npending\n1\nidle\n42\n"
                    "name\norcest-worker-301\npending\n0\nidle\n10\n"
                ),
                stderr="",
            )

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)

        result = get_workers_with_pending_tasks("user@host")

        assert result == {"orcest-worker-300"}
        for call in ssh.call_args_list:
            assert "docker exec orcest-redis-redis-1" in call[0][1]

    def test_get_workers_with_pending_tasks_scans_unprefixed_streams(self, mocker):
        from orcest.fleet.orchestrator import get_workers_with_pending_tasks

        def ssh_side_effect(target, command):
            if "--scan" in command and "'tasks:*'" in command:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="tasks:claude\ntasks:issue:claude\n",
                    stderr="",
                )
            if "--scan" in command and "'*:tasks:*'" in command:
                return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
            if " TYPE " in command:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="stream\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="name\norcest-worker-300\npending\n1\nidle\n42\n",
                stderr="",
            )

        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)

        assert get_workers_with_pending_tasks("user@host") == {"orcest-worker-300"}

    def test_get_workers_with_pending_tasks_scan_failure_raises(self, mocker):
        from orcest.fleet.orchestrator import get_workers_with_pending_tasks

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="redis unavailable",
            ),
        )

        with pytest.raises(RuntimeError, match="Failed to scan task streams"):
            get_workers_with_pending_tasks("user@host")

    def test_get_workers_with_pending_tasks_ignores_missing_group(self, mocker):
        from orcest.fleet.orchestrator import get_workers_with_pending_tasks

        def ssh_side_effect(target, command):
            if "--scan" in command and "'tasks:*'" in command:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="orcest:tasks:claude\n",
                    stderr="",
                )
            if "--scan" in command and "'*:tasks:*'" in command:
                return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
            if " TYPE " in command:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="stream\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="NOGROUP No such consumer group 'workers'\n",
                stderr="",
            )

        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)

        assert get_workers_with_pending_tasks("user@host") == set()

    def test_get_workers_skips_non_stream_task_metadata(self, mocker):
        from orcest.fleet.orchestrator import get_workers_with_pending_tasks

        def ssh_side_effect(_target, command):
            if "--scan" in command and "'tasks:*'" in command:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="tasks:metadata\ntasks:claude\n", stderr=""
                )
            if "--scan" in command:
                return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
            if "TYPE tasks:metadata" in command:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="hash\n", stderr=""
                )
            if " TYPE " in command:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="stream\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="name\norcest-worker-300\npending\n1\nidle\n2\n",
                stderr="",
            )

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)

        assert get_workers_with_pending_tasks("user@host") == {"orcest-worker-300"}
        assert not any(
            "XINFO CONSUMERS tasks:metadata" in call.args[1] for call in ssh.call_args_list
        )


class TestPendingConsumerParser:
    @pytest.mark.parametrize(
        "stdout",
        [
            "name\nworker\npending\nnot-an-int\n",
            "name\nworker\npending\n-1\n",
            "name\nworker\npending\n",
            "name\nworker\n",
            "pending\n1\n",
        ],
    )
    def test_malformed_pending_values_fail_closed(self, stdout):
        from orcest.fleet.orchestrator import _parse_xinfo_consumers_with_pending

        with pytest.raises(RuntimeError, match="Malformed XINFO"):
            _parse_xinfo_consumers_with_pending(stdout)


class TestUploadSource:
    """M1-infra: the deploy build context must stage requirements.lock at its
    root so the deploy Dockerfile's ``COPY requirements.lock .`` resolves.

    The Dockerfile (src/orcest/fleet/deploy/Dockerfile) does
    ``COPY requirements.lock .`` reading from the *context root*. upload_source
    assembles that context; if requirements.lock is not staged at the root the
    image build fails at the COPY. We also require the stale-file cleanup ``rm``
    to remove a previously-extracted requirements.lock so a re-deploy does not
    leave a stale lock behind.
    """

    def _ok(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    def _run_upload(self, mocker, source_root=None):
        """Drive upload_source with _ssh/_scp mocked; capture the tarball
        members (the tarball still exists when _scp is invoked) and every
        _ssh command string. Returns (tar_members, ssh_cmds, file_contents)."""
        import tarfile

        from orcest.fleet import orchestrator as orch

        ssh_cmds: list[str] = []
        captured: dict[str, object] = {}

        def ssh_side_effect(target, cmd):
            ssh_cmds.append(cmd)
            if cmd.startswith("umask 077 && mktemp"):
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="/tmp/orcest-source.ABC123\n",
                    stderr="",
                )
            return self._ok()

        def scp_side_effect(src, dest_target, dest_path):
            # The tarball is the build context; record its members.
            with tarfile.open(src, "r:gz") as tf:
                captured["members"] = tf.getnames()
                captured["files"] = {
                    member: tf.extractfile(member).read().decode(errors="replace")
                    for member in tf.getnames()
                    if tf.getmember(member).isfile()
                }
            return self._ok()

        mocker.patch.object(orch, "_ssh", side_effect=ssh_side_effect)
        mocker.patch.object(orch, "_scp", side_effect=scp_side_effect)
        orch.upload_source("user@host", source_root=source_root)
        return captured.get("members", []), ssh_cmds, captured.get("files", {})

    def test_requirements_lock_staged_at_context_root(self, mocker):
        members, _, _ = self._run_upload(mocker)
        # tar was created with cwd=staging, so root-level files appear bare.
        assert "requirements.lock" in members, (
            "requirements.lock must be at the deploy context root for the "
            f"Dockerfile COPY to resolve; got {members!r}"
        )

    def test_uses_unique_private_remote_archive_and_cleans_it(self, mocker):
        from orcest.fleet import orchestrator as orch

        destinations: list[str] = []

        def ssh_side_effect(_target, command):
            if command.startswith("umask 077 && mktemp"):
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="/tmp/orcest-source.XYZ789\n",
                    stderr="",
                )
            return self._ok()

        def scp_side_effect(_src, _dest_target, dest_path):
            destinations.append(dest_path)
            return self._ok()

        ssh = mocker.patch.object(orch, "_ssh", side_effect=ssh_side_effect)
        mocker.patch.object(orch, "_scp", side_effect=scp_side_effect)

        orch.upload_source("user@host")

        assert destinations == ["/tmp/orcest-source.XYZ789"]
        assert any(call.args[1] == "rm -f /tmp/orcest-source.XYZ789" for call in ssh.call_args_list)

    def test_exact_revision_is_baked_into_source_archive(self, mocker):
        revision = "a" * 40
        mocker.patch(
            "orcest.fleet.orchestrator._resolve_deploy_revision",
            return_value=revision,
        )

        members, _, files = self._run_upload(mocker)

        assert ".orcest-revision" in members
        assert files[".orcest-revision"] == f"{revision}\n"
        assert f"BUILD_REVISION = {revision!r}" in files["src/orcest/_build_revision.py"]

    def test_root_lock_not_only_the_source_tree_copy(self, mocker):
        """The lock the Dockerfile COPYs is the one at the context root, not the
        one nested under src/orcest/fleet/deploy/. Guard that the root copy is
        present even though a nested copy also rides along in src/."""
        members, _, _ = self._run_upload(mocker)
        nested = [
            m for m in members if m.endswith("requirements.lock") and m != "requirements.lock"
        ]
        # A nested copy under src/ may exist (package data) but is irrelevant to
        # the COPY; the root copy is what matters and must be present.
        assert "requirements.lock" in members
        # Sanity: the nested copies (if any) live under src/, never at the root.
        for m in nested:
            assert m.startswith("src/"), f"unexpected non-root lock copy: {m!r}"

    def test_cleanup_rm_removes_stale_requirements_lock(self, mocker):
        _, ssh_cmds, _ = self._run_upload(mocker)
        rm_cmds = [c for c in ssh_cmds if c.startswith("cd /opt/orcest && rm -rf")]
        assert rm_cmds, f"expected a stale-file cleanup rm; ssh cmds were {ssh_cmds!r}"
        assert any("requirements.lock" in c for c in rm_cmds), (
            "stale-file cleanup must remove a previously-deployed "
            f"requirements.lock; got {rm_cmds!r}"
        )
        cleanup = "\n".join(rm_cmds)
        assert "docker-compose*.yml" not in cleanup
        assert "docker-compose.dashboard.yml" not in cleanup
        for expected in (
            "docker-compose.yml",
            "docker-compose.redis.yml",
            "docker-compose.pool.yml",
            "Dockerfile.monitor",
            "docker-compose.monitor.yml",
            "config/monitor.example.yaml",
            "README.md",
        ):
            assert expected in cleanup
        assert ".orcest-revision" in cleanup
        # Never wipe the config/ dir wholesale: the VM's live monitor.yaml
        # (copied from the example) must survive a redeploy.
        assert "rm -rf config " not in cleanup and not any(
            c.rstrip().endswith("config") for c in rm_cmds
        )

    def test_forced_source_root_packages_that_source(self, mocker, tmp_path):
        source_root = tmp_path / "source"
        deploy_dir = source_root / "src" / "orcest" / "fleet" / "deploy"
        deploy_dir.mkdir(parents=True)
        (source_root / "src" / "orcest" / "__init__.py").write_text(
            'SENTINEL = "from-forced-source-root"\n'
        )
        for fname in (
            "Dockerfile",
            "docker-compose.yml",
            "docker-compose.redis.yml",
            "docker-compose.pool.yml",
        ):
            (deploy_dir / fname).write_text(f"{fname} from deploy dir\n")
        (source_root / "pyproject.toml").write_text("[project]\nname = 'forced-orcest'\n")
        (source_root / "requirements.lock").write_text("redis==5.0.0\n")
        # Monitor stack files ship from the checkout root (not the deploy dir).
        (source_root / "config").mkdir()
        for fname in (
            "Dockerfile.monitor",
            "docker-compose.monitor.yml",
            "config/monitor.example.yaml",
            "README.md",
        ):
            (source_root / fname).write_text(f"{fname} from repo root\n")

        members, _, files = self._run_upload(mocker, source_root=source_root)

        assert "src/orcest/__init__.py" in members
        assert files["src/orcest/__init__.py"] == 'SENTINEL = "from-forced-source-root"\n'
        assert files["pyproject.toml"] == "[project]\nname = 'forced-orcest'\n"
        assert files["requirements.lock"] == "redis==5.0.0\n"
        for fname in (
            "Dockerfile.monitor",
            "docker-compose.monitor.yml",
            "config/monitor.example.yaml",
            "README.md",
        ):
            assert files[fname] == f"{fname} from repo root\n"


def test_build_image_requires_and_propagates_exact_revision(mocker):
    from orcest.fleet.orchestrator import build_image

    ssh = mocker.patch(
        "orcest.fleet.orchestrator._ssh",
        return_value=subprocess.CompletedProcess([], 0, stdout="", stderr=""),
    )

    build_image("user@host")

    command = ssh.call_args.args[1]
    assert "cat .orcest-revision" in command
    assert "^[0-9a-f]{7,64}$" in command
    assert "id -u orcest" in command
    assert "id -g orcest" in command
    assert 'ORCEST_BUILD_REVISION="$revision" ORCEST_UID="$orcest_uid"' in command
    assert 'ORCEST_GID="$orcest_gid" docker compose build --no-cache' in command
    assert "docker run --rm --entrypoint id orcest:latest -u orcest" in command
    assert "docker run --rm --entrypoint id orcest:latest -g orcest" in command


def test_source_root_is_discovered_from_current_working_directory(monkeypatch, tmp_path):
    from orcest.fleet.orchestrator import _resolve_source_root

    root = tmp_path / "release"
    nested = root / "operator" / "work"
    deploy_dir = root / "src" / "orcest" / "fleet" / "deploy"
    deploy_dir.mkdir(parents=True)
    nested.mkdir(parents=True)
    (root / "pyproject.toml").write_text("[project]\nname = 'orcest'\n")
    (root / "requirements.lock").write_text("redis==5.0.0\n")
    (root / "src" / "orcest" / "__init__.py").write_text("")
    (deploy_dir / "Dockerfile").write_text("FROM scratch\n")
    monkeypatch.chdir(nested)

    assert _resolve_source_root() == root


class TestCleanPendingTasks:
    def _ok(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    def test_deletes_found_keys(self, mocker):
        call_count = 0

        def ssh_side_effect(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # SCAN
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="orcest:pending:issue:Org/repo:1\norcest:pending:issue:Org/repo:2\n",
                    stderr="",
                )
            return self._ok()  # DEL

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        count = clean_pending_tasks("user@host")
        assert count == 2
        assert ssh.call_count == 2
        del_cmd = ssh.call_args_list[1][0][1]
        assert "DEL" in del_cmd
        assert "orcest:pending:issue:Org/repo:1" in del_cmd
        assert "orcest:pending:issue:Org/repo:2" in del_cmd

    def test_noop_when_no_keys(self, mocker):
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="",
                stderr="",
            ),
        )
        count = clean_pending_tasks("user@host")
        assert count == 0

    def test_scan_failure_raises(self, mocker):
        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="redis unavailable",
            ),
        )

        with pytest.raises(RuntimeError, match="Failed to scan pending task markers"):
            clean_pending_tasks("user@host")

    def test_delete_failure_raises(self, mocker):
        call_count = 0

        def ssh_side_effect(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="orcest:pending:foo\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="redis unavailable",
            )

        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)

        with pytest.raises(RuntimeError, match="Failed to delete pending task markers"):
            clean_pending_tasks("user@host")


# ── C1: Redis password mint + wiring ────────────────────────


class TestEnsureRedisPassword:
    """C1: a strong ORCEST_REDIS_PASSWORD must be minted + persisted on the
    orchestrator VM (0600), idempotently reused across deploys (never rotated,
    which would orphan running workers/Redis data)."""

    def _ok(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    def test_mints_and_persists_when_absent(self, mocker):
        """When the env file is absent, a strong random password is generated,
        written 0600, and returned by reading it back."""
        from orcest.fleet.orchestrator import REDIS_ENV_PATH, ensure_redis_password

        commands: list[str] = []

        def ssh_side_effect(target, cmd):
            commands.append(cmd)
            # The read-back of the persisted file returns the minted value.
            if "cat" in cmd and REDIS_ENV_PATH in cmd:
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="ORCEST_REDIS_PASSWORD=minted-abc123\n", stderr=""
                )
            return self._ok()

        mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        pw = ensure_redis_password("user@host")
        assert pw == "minted-abc123"
        joined = "\n".join(commands)
        # Idempotent create: only writes if the file is missing or empty.
        assert REDIS_ENV_PATH in joined
        # CSPRNG source (openssl rand) used to mint when absent.
        assert "openssl rand" in joined
        # Persisted 0600 (secret at rest).
        assert "chmod 600" in joined or "0600" in joined

    def test_reuses_existing_password(self, mocker):
        """IDEMPOTENT: if a non-empty password file already exists, its value is
        reused and NOT regenerated (rotating would orphan workers/Redis data)."""
        from orcest.fleet.orchestrator import REDIS_ENV_PATH, ensure_redis_password

        def ssh_side_effect(target, cmd):
            if "cat" in cmd and REDIS_ENV_PATH in cmd:
                return subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="ORCEST_REDIS_PASSWORD=existing-pw-xyz\n",
                    stderr="",
                )
            return self._ok()

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        pw = ensure_redis_password("user@host")
        assert pw == "existing-pw-xyz"
        # The mint must be guarded so that openssl only runs when the file is
        # absent/empty -- the shell does this in-band, so the value read back
        # is the pre-existing one. (Guard expressed as a single idempotent
        # shell command; we assert the value round-trips unchanged.)
        all_cmds = "\n".join(c[0][1] for c in ssh.call_args_list)
        # The guard keeps the existing file (test -s / [ -s ] check present).
        assert "-s " in all_cmds

    def test_failure_to_read_back_raises(self, mocker):
        from orcest.fleet.orchestrator import ensure_redis_password

        mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="boom"
            ),
        )
        with pytest.raises(RuntimeError):
            ensure_redis_password("user@host")


class TestRedisStackEnvFile:
    """C1: every compose stack that interpolates ${ORCEST_REDIS_PASSWORD} must be
    started with --env-file pointing at the minted .redis.env -- otherwise Redis
    boots with an empty requirepass (FATAL) and the orchestrator/pool passthrough
    env never receive the value."""

    def _ok(self, *a, **kw):
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    def test_ensure_redis_stack_passes_env_file(self, mocker):
        from orcest.fleet.orchestrator import REDIS_ENV_PATH, ensure_redis_stack

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        ensure_redis_stack("user@host")
        cmd = ssh.call_args[0][1]
        assert f"--env-file {REDIS_ENV_PATH}" in cmd
        assert "docker-compose.redis.yml" in cmd

    def test_ensure_pool_manager_passes_env_file(self, mocker):
        from orcest.fleet.orchestrator import REDIS_ENV_PATH, ensure_pool_manager

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        ensure_pool_manager("user@host")
        cmd = ssh.call_args[0][1]
        assert f"--env-file {REDIS_ENV_PATH}" in cmd
        assert "docker-compose.pool.yml" in cmd
        assert "run --rm --no-deps --entrypoint sh pool-manager" in cmd
        assert "test -r /home/orcest/app/config/fleet.yaml" in cmd
        assert "test -r /home/orcest/.ssh" in cmd
        assert "test -x /home/orcest/.ssh" in cmd
        assert "test -f /home/orcest/.ssh/id_ed25519" in cmd
        assert "test -r /home/orcest/.ssh/id_ed25519" in cmd
        assert "up -d --force-recreate pool-manager" in cmd
        assert "RestartCount" in cmd

    def test_deploy_stack_passes_redis_env_file(self, mocker):
        """The per-project orchestrator stack needs the redis password too. Its
        project .env already carries ORCEST_REDIS_PASSWORD (written by
        generate_env_file), so --env-file <project .env> suffices; but the redis
        env file is layered first as a fallback / single source of truth."""
        from orcest.fleet.orchestrator import REDIS_ENV_PATH, deploy_stack

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=self._ok)
        deploy_stack("user@host", "myproj")
        cmd = ssh.call_args[0][1]
        # The project env (carrying the pw) and the redis env are both supplied.
        assert "projects/myproj/.env" in cmd
        assert REDIS_ENV_PATH in cmd

    def test_orchestrator_compose_passes_clauder_api_key(self):
        compose = Path("src/orcest/fleet/deploy/docker-compose.yml").read_text()
        assert "- CLAUDER_API_KEY" in compose

    @pytest.mark.parametrize(
        "compose_path",
        ["docker-compose.yml", "src/orcest/fleet/deploy/docker-compose.yml"],
    )
    def test_orchestrator_compose_passes_codex_credentials(self, compose_path):
        compose = Path(compose_path).read_text()

        assert "CODEX_API_KEY" in compose
        assert "OPENAI_API_KEY" in compose


class TestRedisCliAuthenticates:
    """C1: _REDIS_CLI_PREFIX must authenticate -- otherwise every fleet redis-cli
    op (pool SMEMBERS/HGETALL, clean_pending_tasks, template pointer) breaks with
    NOAUTH against the now-password-protected Redis."""

    def test_prefix_passes_auth_flag(self):
        from orcest.fleet.orchestrator import _REDIS_CLI_PREFIX

        # redis-cli must receive -a <password> and suppress the auth warning.
        assert "-a " in _REDIS_CLI_PREFIX
        assert "--no-auth-warning" in _REDIS_CLI_PREFIX
        assert " -e " in _REDIS_CLI_PREFIX

    def test_prefix_reads_password_from_container_env(self):
        """The password must come from the container's own env (delivered via
        --env-file), NOT be interpolated by the outer ssh shell (which would
        leak it into argv/process listings and ssh debug logs)."""
        from orcest.fleet.orchestrator import _REDIS_CLI_PREFIX

        # The container env var is referenced (so the value never appears on the
        # fleet host's argv). A literal password or outer-shell $(...) is wrong.
        assert "ORCEST_REDIS_PASSWORD" in _REDIS_CLI_PREFIX
        # Still routed through docker exec into the redis container.
        assert "docker exec orcest-redis-redis-1" in _REDIS_CLI_PREFIX

    def test_existing_callsites_still_route_through_docker_exec(self, mocker):
        """All existing redis-cli call sites keep working with the authenticated
        prefix (they append flat arg strings after the prefix)."""
        from orcest.fleet.orchestrator import (
            clean_pool_redis,
            get_pool_redis_members,
            set_current_template_vmid,
        )

        def ssh_side_effect(_target, command):
            stdout = "0\n0\n0\n0\n0\n" if "EXISTS" in command else ""
            return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")

        ssh = mocker.patch("orcest.fleet.orchestrator._ssh", side_effect=ssh_side_effect)
        set_current_template_vmid("user@host", 9001)
        assert "ORCEST_REDIS_PASSWORD" in ssh.call_args[0][1]
        assert "SET orcest:pool:current_template_vmid" in ssh.call_args[0][1]

        ssh.reset_mock()
        get_pool_redis_members("user@host")
        for call in ssh.call_args_list:
            assert "--no-auth-warning" in call[0][1]

        ssh.reset_mock()
        clean_pool_redis("user@host", ["300"])
        cleanup_cmd = ssh.call_args_list[0][0][1]
        verify_cmd = ssh.call_args_list[1][0][1]
        # Seven cleanup operations plus five verification reads are authenticated.
        assert cleanup_cmd.count("--no-auth-warning") == 7
        assert verify_cmd.count("--no-auth-warning") == 5

    def test_raw_flag_preserved(self, mocker):
        """--raw is still appended for line-per-value parsing."""
        from orcest.fleet.orchestrator import get_current_template_vmid

        ssh = mocker.patch(
            "orcest.fleet.orchestrator._ssh",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout="9001\n", stderr=""
            ),
        )
        get_current_template_vmid("user@host")
        assert "--raw" in ssh.call_args[0][1]
