"""Tests for the orcest CLI entry points (src/orcest/cli.py)."""

import io
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner
from rich.console import Console

from orcest.cli import _status_once, _validate_ssh_input, main


@pytest.fixture
def runner():
    """CliRunner with stderr and stdout separated (mix_stderr=False semantics).

    Root cause of the original CI failure: the ``_status_once`` Rich Console
    was constructed without an explicit ``file=`` argument.  Rich's default
    ``Console()`` detects that it is not attached to a real terminal and falls
    back to writing to the real ``sys.stderr`` file descriptor, which bypasses
    Click's stream capture entirely.  This meant ``result.stderr`` contained
    spurious Rich output that had nothing to do with ``click.echo(..., err=True)``.

    Fix applied in ``cli.py``: ``Console(file=sys.stdout)`` so that Rich table
    output is captured by Click on ``result.stdout``.

    Why ``CliRunner(mix_stderr=False)`` is not used: Click 8.3 removed the
    ``mix_stderr`` constructor parameter entirely.  Passing it raises::

        TypeError: CliRunner.__init__() got an unexpected keyword argument 'mix_stderr'

    In Click 8.3+, streams are **always** separated — ``result.stdout`` and
    ``result.stderr`` are independent regardless of constructor arguments.
    This gives the same guarantees as the old ``mix_stderr=False`` flag:

    * ``result.stdout`` — only what was written to ``sys.stdout``
    * ``result.stderr`` — only what was written to ``sys.stderr``

    All error-message assertions below use ``result.stderr`` to verify that
    ``click.echo(..., err=True)`` routes output to stderr, not stdout.

    ``test_runner_separates_stderr_from_stdout`` asserts these invariants
    empirically and will fail immediately if Click ever reverts behaviour.
    """
    return CliRunner()


# ---------------------------------------------------------------------------
# Verify that the runner fixture separates stderr from stdout (Click 8.2+)
# ---------------------------------------------------------------------------


def test_runner_separates_stderr_from_stdout(runner):
    """CliRunner captures stderr and stdout as independent streams.

    Click 8.3 removed ``mix_stderr`` and made separation unconditional.
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
