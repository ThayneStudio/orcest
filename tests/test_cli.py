"""Tests for the orcest CLI entry points (src/orcest/cli.py)."""

import io
import re
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner
from rich.console import Console

from orcest.cli import _dead_letters_command, _status_once, _validate_ssh_input, main


@pytest.fixture
def runner():
    """CliRunner that always captures stdout and stderr as independent streams.

    Click 8.1.x defaults ``mix_stderr=True`` (merges stderr into stdout), so
    we pass ``mix_stderr=False`` explicitly.  Click 8.2 removed the parameter
    entirely and made stream separation unconditional, so we fall back to a
    plain ``CliRunner()`` when the keyword argument is rejected.

    Either way, ``result.stderr`` is populated only by text written to stderr
    and all assertions on it remain meaningful.
    ``test_runner_separates_stderr_from_stdout`` verifies this empirically.
    """
    try:
        return CliRunner(mix_stderr=False)
    except TypeError as exc:
        # Intentionally narrow: only suppress the TypeError caused by Click 8.2+
        # removing the mix_stderr parameter.  The check relies on CPython's error
        # message including the parameter name verbatim (e.g. "got an unexpected
        # keyword argument 'mix_stderr'").  Any other TypeError — a typo, a broken
        # plugin wrapping CliRunner.__init__, etc. — propagates so it is never
        # silently swallowed.
        if "mix_stderr" not in str(exc):
            raise
        # Click 8.2+ removed mix_stderr; streams are always separated.
        return CliRunner()


# ---------------------------------------------------------------------------
# Verify that the runner fixture separates stderr from stdout (Click 8.2+)
# ---------------------------------------------------------------------------


def test_runner_separates_stderr_from_stdout(runner):
    """CliRunner captures stderr and stdout as independent streams.

    Click 8.2 removed ``mix_stderr`` and made separation unconditional.
    This test guards against regressions where stderr leaks into stdout or
    ``result.stderr`` is empty, which would make all stderr assertions below
    meaningless.
    """

    @click.command()
    def _probe():
        click.echo("stdout-only")
        click.echo("stderr-only", err=True)

    result = runner.invoke(_probe)
    assert result.exit_code == 0
    assert "stdout-only" in result.stdout
    assert "stderr-only" not in result.stdout, (
        "stderr leaked into stdout — CliRunner is merging streams (mix_stderr=True behaviour)"
    )
    assert "stderr-only" in result.stderr, (
        "result.stderr is empty — Click may have reverted to mix_stderr=True default; "
        "all result.stderr assertions in this file would be meaningless"
    )
    assert "stdout-only" not in result.stderr


# ---------------------------------------------------------------------------
# _validate_ssh_input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "root",
        "myhost.example.com",
        "192.168.1.1",
        "host-name_1",
    ],
)
def test_validate_ssh_input_valid(value):
    """Valid SSH host/user values pass without raising."""
    _validate_ssh_input(value, "host")  # should not raise


@pytest.mark.parametrize(
    "value",
    [
        "host;rm -rf /",
        "user name",
        "host$(id)",
        "",
        "host\neval",
    ],
)
def test_validate_ssh_input_invalid(value):
    """Invalid SSH host/user values raise click.BadParameter."""
    with pytest.raises(click.BadParameter):
        _validate_ssh_input(value, "host")


# ---------------------------------------------------------------------------
# Help / basic invocation
# ---------------------------------------------------------------------------


def test_main_help(runner):
    """Main group --help exits 0 and lists subcommands."""
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "orchestrate" in result.stdout
    assert "work" in result.stdout
    assert "status" in result.stdout


def test_rollout_health_text_output_includes_metrics(runner, mocker):
    revision = "a" * 40
    client = MagicMock()
    mocker.patch("orcest.cli._resolve_redis_config", return_value=MagicMock())
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=client)
    mocker.patch(
        "orcest.rollout_health.collect_rollout_health",
        return_value={
            "ok": True,
            "revision": revision,
            "checks": [
                {
                    "name": "checker_revision",
                    "passed": True,
                    "actual": revision,
                    "expected": revision,
                }
            ],
            "metrics": {"queue_depth": 3, "pending": 1, "lag": 2},
        },
    )

    result = runner.invoke(
        main,
        ["rollout-health", "localhost", "--prefix", "demo", "--expected-revision", revision],
    )

    assert result.exit_code == 0, result.output
    assert "PASS checker_revision" in result.output
    assert 'METRICS {"lag": 2, "pending": 1, "queue_depth": 3}' in result.output
    client.close.assert_called_once()


def test_rollout_health_requires_prefix(runner):
    """Without --prefix the project gates would silently inspect 'orcest:*'."""
    result = runner.invoke(
        main,
        ["rollout-health", "localhost", "--expected-revision", "a" * 40],
    )

    assert result.exit_code != 0
    assert "Missing option '--prefix'" in result.output


def test_rollout_health_forwards_pool_prefix(runner, mocker):
    revision = "a" * 40
    mocker.patch("orcest.cli._resolve_redis_config", return_value=MagicMock())
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=MagicMock())
    collect = mocker.patch(
        "orcest.rollout_health.collect_rollout_health",
        return_value={"ok": True, "revision": revision, "checks": [], "metrics": {}},
    )

    result = runner.invoke(
        main,
        [
            "rollout-health",
            "localhost",
            "--prefix",
            "demo",
            "--task-prefix",
            "orcest",
            "--pool-prefix",
            "fleet",
            "--expected-revision",
            revision,
        ],
    )

    assert result.exit_code == 0, result.output
    assert collect.call_args.kwargs["pool_prefix"] == "fleet"


def test_rollout_health_expected_backend_requires_pool_size_and_vmid_start(runner):
    revision = "a" * 40

    missing_size = runner.invoke(
        main,
        [
            "rollout-health",
            "localhost",
            "--prefix",
            "demo",
            "--expected-revision",
            revision,
            "--expected-backend",
            "codex",
        ],
    )
    assert missing_size.exit_code != 0
    assert "requires --expected-pool-size" in missing_size.output

    missing_start = runner.invoke(
        main,
        [
            "rollout-health",
            "localhost",
            "--prefix",
            "demo",
            "--expected-revision",
            revision,
            "--expected-pool-size",
            "1",
            "--expected-backend",
            "codex",
        ],
    )
    assert missing_start.exit_code != 0
    assert "requires --expected-vmid-start" in missing_start.output


def test_rollout_health_expected_backend_count_must_match_pool_size(runner):
    revision = "a" * 40

    result = runner.invoke(
        main,
        [
            "rollout-health",
            "localhost",
            "--prefix",
            "demo",
            "--expected-revision",
            revision,
            "--expected-pool-size",
            "2",
            "--expected-vmid-start",
            "300",
            "--expected-backend",
            "codex",
        ],
    )

    assert result.exit_code != 0
    assert "exactly once per expected pool slot" in result.output


@pytest.mark.parametrize(
    ("extra_args", "expected_force"),
    [([], False), (["--force"], True)],
)
def test_task_streams_quarantine_forwards_force(runner, mocker, extra_args, expected_force):
    mocker.patch("orcest.cli._resolve_redis_config", return_value=MagicMock())
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=MagicMock())
    quarantine = mocker.patch(
        "orcest.task_stream_quarantine.quarantine_task_streams",
        return_value={"ok": True, "operation": "quarantine", "streams": []},
    )

    result = runner.invoke(
        main,
        [
            "task-streams",
            "quarantine",
            "localhost",
            "--quarantine-id",
            "release-1",
            *extra_args,
        ],
    )

    assert result.exit_code == 0, result.output
    assert quarantine.call_args.kwargs["force"] is expected_force


def test_task_streams_quarantine_reports_live_worker_refusal(runner, mocker):
    from orcest.task_stream_quarantine import TaskStreamQuarantineError

    mocker.patch("orcest.cli._resolve_redis_config", return_value=MagicMock())
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=MagicMock())
    mocker.patch(
        "orcest.task_stream_quarantine.quarantine_task_streams",
        side_effect=TaskStreamQuarantineError(
            "refusing to fence task streams while work is in flight"
        ),
    )

    result = runner.invoke(
        main,
        ["task-streams", "quarantine", "localhost", "--quarantine-id", "release-1"],
    )

    assert result.exit_code != 0
    assert "refusing to fence task streams while work is in flight" in result.output


def test_work_missing_required_id(runner):
    """work without --id exits non-zero (--id is a required option)."""
    result = runner.invoke(main, ["work"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# status command
# ---------------------------------------------------------------------------


def test_status_redis_connection_failure(mocker, runner):
    """When Redis cannot be reached, status exits 1 with an error message."""
    mock_redis = MagicMock()
    mock_redis.health_check.return_value = False
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=mock_redis)

    result = runner.invoke(main, ["status", "localhost", "--once"])

    assert result.exit_code == 1
    assert "Cannot connect to Redis" in result.stderr
    assert "Cannot connect to Redis" not in result.stdout


def test_status_zero_interval_exits_error(mocker, runner, fake_redis_client):
    """status --interval=0 exits 1 before launching the TUI."""
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=fake_redis_client)

    result = runner.invoke(main, ["status", "localhost", "--interval", "0"])

    assert result.exit_code == 1
    assert "interval must be positive" in result.stderr
    assert "interval must be positive" not in result.stdout


def test_status_once_with_redis_host(mocker, runner, fake_redis_client):
    """status <host> --once succeeds and prints the status table header.

    Rich's Console() inside _status_once writes to sys.stdout, which Click
    captures in result.stdout.  We assert on result.stdout (not just
    result.output) to confirm that Rich output is not leaking to stderr.
    """
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=fake_redis_client)

    result = runner.invoke(main, ["status", "localhost:6379", "--once"])

    assert result.exit_code == 0
    assert "Queue Depths" in result.stdout
    assert "Queue Depths" not in result.stderr


def test_status_host_without_port_defaults_6379(mocker, runner, fake_redis_client):
    """status <host> (no port suffix) passes port=6379 to RedisClient."""
    mock_redis_cls = MagicMock(return_value=fake_redis_client)
    mocker.patch("orcest.shared.redis_client.RedisClient", mock_redis_cls)

    runner.invoke(main, ["status", "myhost", "--once"])

    config_arg = mock_redis_cls.call_args[0][0]
    assert config_arg.host == "myhost"
    assert config_arg.port == 6379


def test_status_host_with_port(mocker, runner, fake_redis_client):
    """status <host:port> correctly parses a custom port."""
    mock_redis_cls = MagicMock(return_value=fake_redis_client)
    mocker.patch("orcest.shared.redis_client.RedisClient", mock_redis_cls)

    runner.invoke(main, ["status", "10.0.0.1:6380", "--once"])

    config_arg = mock_redis_cls.call_args[0][0]
    assert config_arg.host == "10.0.0.1"
    assert config_arg.port == 6380


def test_status_once_normal(fake_redis_client):
    """_status_once runs without error on an empty Redis."""
    _status_once(fake_redis_client)


def test_status_once_wrongtype_tasks_key_does_not_raise(fake_redis_client):
    """_status_once handles WRONGTYPE on a tasks:* key without crashing."""
    # A non-stream value at a tasks:* key triggers WRONGTYPE on xlen
    fake_redis_client.client.set("tasks:not-a-stream", "some-value")
    _status_once(fake_redis_client)


def test_status_once_wrongtype_results_key_does_not_raise(fake_redis_client):
    """_status_once handles WRONGTYPE on the results key without crashing."""
    # A non-stream value at results triggers WRONGTYPE on xlen
    fake_redis_client.client.set("results", "some-value")
    _status_once(fake_redis_client)


def test_status_once_wrongtype_both_does_not_raise(fake_redis_client):
    """_status_once handles WRONGTYPE on both tasks:* and results keys."""
    fake_redis_client.client.set("tasks:bad-key", "oops")
    fake_redis_client.client.set("results", "also-bad")
    _status_once(fake_redis_client)


def test_status_once_wrongtype_tasks_key_excluded_from_output(fake_redis_client):
    """A WRONGTYPE tasks:* key is silently excluded from queue depths output."""
    fake_redis_client.client.set("tasks:bad-key", "oops")
    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    # fetch_snapshot skips WRONGTYPE keys silently; bad key should not appear
    assert "tasks:bad-key" not in output
    assert "Orcest System Status" in output


def test_status_once_wrongtype_results_key_shows_zero(fake_redis_client):
    """A WRONGTYPE results key falls back to 0 in the queue depths table."""
    fake_redis_client.client.set("results", "some-value")
    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    # fetch_snapshot catches the ResponseError and returns results_depth=0
    # Assert the results row in the table specifically shows 0, not just that "0"
    # appears somewhere in the output.
    assert re.search(r"│\s*results\s*│\s*0\b", output), (
        "Expected 'results' table row to show depth 0, got:\n" + output
    )
    assert "Orcest System Status" in output


# ---------------------------------------------------------------------------
# orchestrate command
# ---------------------------------------------------------------------------


def test_orchestrate_invokes_run_orchestrator(mocker, runner):
    """orchestrate loads config and calls run_orchestrator with it."""
    mock_config = MagicMock()
    mocker.patch("orcest.shared.config.load_orchestrator_config", return_value=mock_config)
    mock_run = mocker.patch("orcest.orchestrator.loop.run_orchestrator")

    runner.invoke(main, ["orchestrate"])

    mock_run.assert_called_once_with(mock_config)


# ---------------------------------------------------------------------------
# work command
# ---------------------------------------------------------------------------


def test_work_invokes_run_worker(mocker, runner):
    """work sets worker_id on the loaded config then calls run_worker."""
    mock_config = MagicMock()
    mocker.patch("orcest.shared.config.load_worker_config", return_value=mock_config)
    mock_run = mocker.patch("orcest.worker.loop.run_worker")

    runner.invoke(main, ["work", "--id", "worker-42"])

    assert mock_config.worker_id == "worker-42"
    mock_run.assert_called_once_with(mock_config)


def test_work_runner_override(mocker, runner):
    """work --runner=noop overrides both cfg.runner.type and cfg.backend."""
    mock_config = MagicMock()
    mocker.patch("orcest.shared.config.load_worker_config", return_value=mock_config)
    mocker.patch("orcest.worker.loop.run_worker")

    runner.invoke(main, ["work", "--id", "worker-1", "--runner", "noop"])

    assert mock_config.runner.type == "noop"
    assert mock_config.backend == "noop"


def test_work_clauder_override_preserves_interactive_alias_semantics(mocker, runner):
    """work --runner=clauder uses the Claude CLI in interactive mode."""
    mock_config = MagicMock()
    mock_config.runner.extra = {}
    mocker.patch("orcest.shared.config.load_worker_config", return_value=mock_config)
    mocker.patch("orcest.worker.loop.run_worker")

    result = runner.invoke(main, ["work", "--id", "worker-1", "--runner", "clauder"])

    assert result.exit_code == 0, result.output
    assert mock_config.backend == "clauder"
    assert mock_config.runner.type == "claude"
    assert mock_config.runner.extra["mode"] == "interactive"


def test_pool_manage_accepts_template_range_without_legacy_id(mocker, runner):
    """Range-mode blue/green pools are valid pool-manager configurations."""
    from orcest.fleet.config import FleetConfig, PoolConfig, ProxmoxConfig

    cfg = FleetConfig(
        proxmox=ProxmoxConfig(api_token_id="id", api_token_secret="secret"),
        pool=PoolConfig(template_vm_id=0, template_vmid_range=[9000, 9009]),
    )
    mocker.patch("orcest.fleet.config.load_config", return_value=cfg)
    mocker.patch("orcest.fleet.proxmox_api.ProxmoxClient")
    redis_cls = mocker.patch("orcest.shared.redis_client.RedisClient")
    redis_cls.return_value.health_check.return_value = True
    manager_cls = mocker.patch("orcest.fleet.pool_manager.PoolManager")

    result = runner.invoke(main, ["pool-manage", "--interval", "0.1"])

    assert result.exit_code == 0, result.output
    manager_cls.return_value.run.assert_called_once_with(interval=0.1)


# ---------------------------------------------------------------------------
# _status_once dead-letter integration
# ---------------------------------------------------------------------------


def test_status_once_shows_dead_letter_row(fake_redis_client):
    """_status_once includes the orcest:dead-letter row in the Queue Depths table."""
    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    assert "dead-letter" in output


def test_status_once_shows_dead_letter_count(fake_redis_client):
    """_status_once reflects dead-letter entries in the count column."""
    fake_redis_client.xadd("dead-letter", {"id": "t1", "type": "fix_ci"})
    fake_redis_client.xadd("dead-letter", {"id": "t2", "type": "fix_ci"})

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    assert "dead-letter" in output
    # Count "2" should appear somewhere in the output
    assert "2" in output


# ---------------------------------------------------------------------------
# dead-letters command
# ---------------------------------------------------------------------------

_SAMPLE_DEAD_LETTER_FIELDS = {
    "id": "task-abc",
    "type": "fix_ci",
    "repo": "org/repo",
    "token": "tok",
    "resource_type": "pr",
    "resource_id": "42",
    "prompt": "fix it",
    "branch": "",
    "base_branch": "",
    "created_at": "2024-01-01T00:00:00+00:00",
    "dead_letter_reason": "Exceeded max delivery count (3)",
    "tasks_stream": "test:tasks:claude",
    "original_entry_id": "1234-0",
    "delivery_count": "3",
}


def test_dead_letters_command_empty(fake_redis_client):
    """_dead_letters_command prints 'no entries' message when stream is empty."""
    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _dead_letters_command(fake_redis_client, replay=False, count=100)

    assert "No dead-lettered tasks" in buf.getvalue()


def test_dead_letters_command_lists_tasks(fake_redis_client):
    """_dead_letters_command lists dead-lettered task metadata."""
    fake_redis_client.xadd("dead-letter", _SAMPLE_DEAD_LETTER_FIELDS)

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False, width=200)):
        _dead_letters_command(fake_redis_client, replay=False, count=100)

    output = buf.getvalue()
    assert "task-abc" in output
    assert "org/repo" in output
    # The sample uses a fully-qualified "test:tasks:claude" (to simulate prefixed original stream);
    # rich table may truncate the column, so assert a distinguishing prefix that is always present.
    assert "test:tas" in output


def test_dead_letters_command_replay(fake_redis_client):
    """_dead_letters_command --replay re-enqueues tasks and removes dead-letter entries."""
    fake_redis_client.xadd("dead-letter", _SAMPLE_DEAD_LETTER_FIELDS)

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _dead_letters_command(fake_redis_client, replay=True, count=100)

    output = buf.getvalue()
    assert "Replayed 1" in output

    # Task should now be in the original stream
    replayed_entries = fake_redis_client.xread_after("tasks:claude")
    assert len(replayed_entries) == 1
    assert replayed_entries[0][1]["id"] == "task-abc"

    # Dead-letter metadata fields must be stripped
    replayed_fields = replayed_entries[0][1]
    assert "dead_letter_reason" not in replayed_fields
    assert "tasks_stream" not in replayed_fields
    assert "original_entry_id" not in replayed_fields
    assert "delivery_count" not in replayed_fields

    # Dead-letter stream should be empty after replay
    dl_entries = fake_redis_client.xread_after("dead-letter")
    assert len(dl_entries) == 0


def test_dead_letters_command_replay_missing_tasks_stream(fake_redis_client):
    """_dead_letters_command skips entries without a tasks_stream field."""
    bad_fields = dict(_SAMPLE_DEAD_LETTER_FIELDS)
    del bad_fields["tasks_stream"]
    fake_redis_client.xadd("dead-letter", bad_fields)

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _dead_letters_command(fake_redis_client, replay=True, count=100)

    output = buf.getvalue()
    assert "skipping" in output
    assert "skipped (no tasks_stream field)" in output
    assert "error" not in output.lower()


def test_dead_letters_command_replay_refuses_redacted_entries(fake_redis_client):
    """--replay refuses (with clear guidance) when DL entry has redacted secrets.

    Regression test for the Critical replay-safety issue after Task 2 redaction layer.
    DL now stores only to_safe_dict() so replay must not blindly re-inject "[REDACTED]".
    """
    redacted = dict(_SAMPLE_DEAD_LETTER_FIELDS)
    redacted["credential"] = "[REDACTED]"
    redacted["claude_token"] = "[REDACTED]"
    redacted["token"] = "[REDACTED]"
    # Use sample's tasks_stream ("test:tasks:claude") -- raw replay path uses it directly.
    fake_redis_client.xadd("dead-letter", redacted)

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _dead_letters_command(fake_redis_client, replay=True, count=100)

    output = buf.getvalue()
    assert "Cannot replay" in output
    assert "redacted" in output.lower()
    assert "Replayed" not in output
    assert "error" in output.lower()  # we count it as error

    # No replayed task should have been xadd_raw'ed to the tasks stream
    # (xread_after on the logical name will look in the prefixed stream used by sample)
    replayed = fake_redis_client.xread_after("tasks:claude")
    assert len(replayed) == 0, "redacted replay must not have injected a task"

    # DL entry remains (we don't delete on refusal)
    dl = fake_redis_client.xread_after("dead-letter")
    assert len(dl) == 1


def test_dead_letters_cli_redis_connection_failure(mocker, runner):
    """dead-letters exits 1 when Redis is unreachable."""
    mock_redis = MagicMock()
    mock_redis.health_check.return_value = False
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=mock_redis)

    result = runner.invoke(main, ["dead-letters", "localhost"])

    assert result.exit_code == 1
    assert "Cannot connect to Redis" in result.stderr


def test_dead_letters_cli_zero_count_exits_error(runner):
    """dead-letters --count=0 exits 1 with a clear error before connecting to Redis."""
    result = runner.invoke(main, ["dead-letters", "localhost", "--count", "0"])

    assert result.exit_code == 1
    assert "count must be a positive integer" in result.stderr


def test_dead_letters_cli_negative_count_exits_error(runner):
    """dead-letters --count=-1 exits 1 with a clear error before connecting to Redis."""
    result = runner.invoke(main, ["dead-letters", "localhost", "--count", "-1"])

    assert result.exit_code == 1
    assert "count must be a positive integer" in result.stderr


def test_dead_letters_cli_lists_tasks(mocker, runner, fake_redis_client):
    """dead-letters command lists entries via the CLI runner."""
    fake_redis_client.xadd("dead-letter", _SAMPLE_DEAD_LETTER_FIELDS)
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=fake_redis_client)

    result = runner.invoke(main, ["dead-letters", "localhost"])

    assert result.exit_code == 0
    assert "task-abc" in result.stdout


# ── check commands ──────────────────────────────────────────


def test_check_github_token_success(runner, mocker):
    """check github-token exits 0 and prints username when gh api user succeeds."""
    mock_run = mocker.patch(
        "subprocess.run",
        return_value=MagicMock(
            returncode=0,
            stdout='{"login": "testuser", "id": 12345}',
            stderr="",
        ),
    )
    result = runner.invoke(main, ["check", "github-token"], input="ghp_test123\n")
    assert result.exit_code == 0
    assert "testuser" in result.output
    # Verify GH_TOKEN was set in the subprocess environment
    call_kwargs = mock_run.call_args
    env = call_kwargs.kwargs.get("env") or call_kwargs[1].get("env")
    assert env["GH_TOKEN"] == "ghp_test123"
    assert env["GITHUB_TOKEN"] == "ghp_test123"
    mock_run.assert_called_once()


def test_check_github_token_failure(runner, mocker):
    """check github-token exits 1 and prints error when gh api user fails."""
    mock_run = mocker.patch(
        "subprocess.run",
        return_value=MagicMock(returncode=1, stdout="", stderr="bad credentials"),
    )
    result = runner.invoke(main, ["check", "github-token"], input="ghp_bad\n")
    assert result.exit_code == 1
    mock_run.assert_called_once()


def test_check_github_token_no_input(runner):
    """check github-token exits 1 when no token is provided on stdin."""
    result = runner.invoke(main, ["check", "github-token"], input="")
    assert result.exit_code == 1
    assert "no token provided" in (result.output + (result.stderr or ""))


def test_check_github_token_whitespace_only(runner):
    """check github-token exits 1 when only whitespace is provided."""
    result = runner.invoke(main, ["check", "github-token"], input="   \n  ")
    assert result.exit_code == 1
    assert "no token provided" in (result.output + (result.stderr or ""))


def test_check_github_token_multiline_rejected(runner):
    """check github-token rejects input containing multiple lines."""
    result = runner.invoke(main, ["check", "github-token"], input="ghp_first\nghp_second\n")
    assert result.exit_code == 1
    assert "newlines" in (result.output + (result.stderr or ""))
