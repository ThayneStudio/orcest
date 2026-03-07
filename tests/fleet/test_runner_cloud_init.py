"""Tests for orcest.fleet.runner_cloud_init."""

import pytest
import yaml

from orcest.fleet.runner_cloud_init import render_runner_userdata

pytestmark = pytest.mark.unit


def _render(**overrides):
    defaults = {
        "org_url": "https://github.com/ThayneStudio",
        "runner_token": "ATOKEN123",
    }
    defaults.update(overrides)
    return render_runner_userdata(**defaults)


def test_output_starts_with_cloud_config_header():
    output = _render()
    assert output.startswith("#cloud-config\n")


def test_output_is_valid_yaml():
    output = _render()
    data = yaml.safe_load(output)
    assert isinstance(data, dict)


def test_runner_user_created():
    output = _render()
    data = yaml.safe_load(output)
    runner_user = next(u for u in data["users"] if isinstance(u, dict) and u["name"] == "runner")
    assert "docker" in runner_user["groups"]


def test_runcmd_installs_key_tools():
    output = _render()
    data = yaml.safe_load(output)
    runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
    assert "docker-ce" in runcmd
    assert "nodesource" in runcmd
    assert "playwright" in runcmd
    assert "supabase" in runcmd


def test_runcmd_configures_runner():
    output = _render(org_url="https://github.com/MyOrg", runner_token="MYTOKEN")
    data = yaml.safe_load(output)
    runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
    assert "https://github.com/MyOrg" in runcmd
    assert "MYTOKEN" in runcmd
    assert "./config.sh" in runcmd
    assert "svc.sh install" in runcmd


def test_runcmd_uses_custom_name():
    output = _render(runner_name="my-runner")
    data = yaml.safe_load(output)
    runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
    assert "--name my-runner" in runcmd


def test_runcmd_omits_name_flag_when_no_name():
    # When no runner_name is provided, --name is omitted and config.sh defaults to hostname
    output = _render()
    data = yaml.safe_load(output)
    runcmd = "\n".join(str(cmd) for cmd in data["runcmd"])
    assert "--name " not in runcmd
