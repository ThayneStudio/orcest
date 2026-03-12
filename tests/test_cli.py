"""Tests for the orcest CLI entry points (src/orcest/cli.py)."""

import io
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


def test_status_once_wrongtype_tasks_key_shows_not_a_stream(fake_redis_client):
    """A WRONGTYPE tasks:* key is reported as '(not a stream)' in output."""
    fake_redis_client.client.set("tasks:bad-key", "oops")
    # _status_once uses Rich console which writes to stdout
    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    assert "(not a stream)" in output


def test_status_once_wrongtype_results_key_shows_not_a_stream(fake_redis_client):
    """A WRONGTYPE results key is reported as '(not a stream)' in the results row."""
    fake_redis_client.client.set("results", "some-value")
    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    assert "(not a stream)" in output


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


# ---------------------------------------------------------------------------
# _status_once dead-letter integration
# ---------------------------------------------------------------------------


def test_status_once_shows_dead_letter_row(fake_redis_client):
    """_status_once includes the orcest:dead-letter row in the Queue Depths table."""
    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    assert "orcest:dead-letter" in output


def test_status_once_shows_dead_letter_count(fake_redis_client):
    """_status_once reflects dead-letter entries in the count column."""
    fake_redis_client.xadd("orcest:dead-letter", {"id": "t1", "type": "fix_ci"})
    fake_redis_client.xadd("orcest:dead-letter", {"id": "t2", "type": "fix_ci"})

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _status_once(fake_redis_client)

    output = buf.getvalue()
    assert "orcest:dead-letter" in output
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
    "tasks_stream": "tasks:claude",
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
    fake_redis_client.xadd("orcest:dead-letter", _SAMPLE_DEAD_LETTER_FIELDS)

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False, width=200)):
        _dead_letters_command(fake_redis_client, replay=False, count=100)

    output = buf.getvalue()
    assert "task-abc" in output
    assert "org/repo" in output
    assert "tasks:claude" in output


def test_dead_letters_command_replay(fake_redis_client):
    """_dead_letters_command --replay re-enqueues tasks and removes dead-letter entries."""
    fake_redis_client.xadd("orcest:dead-letter", _SAMPLE_DEAD_LETTER_FIELDS)

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
    dl_entries = fake_redis_client.xread_after("orcest:dead-letter")
    assert len(dl_entries) == 0


def test_dead_letters_command_replay_missing_tasks_stream(fake_redis_client):
    """_dead_letters_command skips entries without a tasks_stream field."""
    bad_fields = dict(_SAMPLE_DEAD_LETTER_FIELDS)
    del bad_fields["tasks_stream"]
    fake_redis_client.xadd("orcest:dead-letter", bad_fields)

    buf = io.StringIO()
    with patch("orcest.cli.Console", return_value=Console(file=buf, highlight=False)):
        _dead_letters_command(fake_redis_client, replay=True, count=100)

    output = buf.getvalue()
    assert "skipping" in output
    assert "skipped (no tasks_stream field)" in output
    assert "error" not in output.lower()


def test_dead_letters_cli_redis_connection_failure(mocker, runner):
    """dead-letters exits 1 when Redis is unreachable."""
    mock_redis = MagicMock()
    mock_redis.health_check.return_value = False
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=mock_redis)

    result = runner.invoke(main, ["dead-letters", "localhost"])

    assert result.exit_code == 1
    assert "Cannot connect to Redis" in result.stderr


def test_dead_letters_cli_lists_tasks(mocker, runner, fake_redis_client):
    """dead-letters command lists entries via the CLI runner."""
    fake_redis_client.xadd("orcest:dead-letter", _SAMPLE_DEAD_LETTER_FIELDS)
    mocker.patch("orcest.shared.redis_client.RedisClient", return_value=fake_redis_client)

    result = runner.invoke(main, ["dead-letters", "localhost"])

    assert result.exit_code == 0
    assert "task-abc" in result.stdout
