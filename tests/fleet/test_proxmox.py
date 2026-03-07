"""Tests for orcest.fleet.proxmox."""

from unittest.mock import MagicMock

import pytest

from orcest.fleet.inventory import ProxmoxConfig
from orcest.fleet.proxmox import ProxmoxClient

pytestmark = pytest.mark.unit


@pytest.fixture
def px_config():
    return ProxmoxConfig(
        host="proxmox.example.com",
        node="pve",
        storage="local-lvm",
        token_id="root@pam!orcest",
        token_secret="fake-secret",
    )


@pytest.fixture
def mock_api():
    """Create a mock ProxmoxAPI with a node hierarchy."""
    api = MagicMock()
    # node.qemu(vm_id).status.current.get() etc.
    return api


@pytest.fixture
def client(px_config, mock_api):
    """ProxmoxClient with a pre-injected mock API."""
    c = ProxmoxClient(px_config)
    c._api = mock_api
    return c


def test_vm_exists_true(client, mock_api):
    """vm_exists returns True when the VM status query succeeds."""
    mock_api.nodes("pve").qemu(200).status.current.get.return_value = {"status": "running"}
    assert client.vm_exists(200) is True


def test_vm_exists_false(client, mock_api):
    """vm_exists returns False when the VM status query raises."""
    mock_api.nodes("pve").qemu(999).status.current.get.side_effect = Exception("not found")
    assert client.vm_exists(999) is False


def test_get_vm_status(client, mock_api):
    """get_vm_status returns the status string from the API."""
    mock_api.nodes("pve").qemu(200).status.current.get.return_value = {"status": "stopped"}
    assert client.get_vm_status(200) == "stopped"


def test_destroy_vm_stops_then_deletes(client, mock_api):
    """destroy_vm stops a running VM then deletes it."""
    node = mock_api.nodes("pve")
    node.qemu(200).status.current.get.side_effect = [
        {"status": "running"},  # initial check
        {"status": "stopped"},  # after stop
    ]

    client.destroy_vm(200)

    node.qemu(200).status.stop.post.assert_called_once()
    node.qemu(200).delete.assert_called_once_with(purge=1)


def test_destroy_vm_already_stopped(client, mock_api):
    """destroy_vm skips stop if VM is already stopped."""
    node = mock_api.nodes("pve")
    node.qemu(200).status.current.get.return_value = {"status": "stopped"}

    client.destroy_vm(200)

    node.qemu(200).status.stop.post.assert_not_called()
    node.qemu(200).delete.assert_called_once_with(purge=1)


def test_create_vm_raises_if_exists(client, mock_api):
    """create_vm raises ValueError if the VM already exists."""
    mock_api.nodes("pve").qemu(200).status.current.get.return_value = {"status": "running"}
    with pytest.raises(ValueError, match="VM 200 already exists"):
        client.create_vm(200, "test-worker")


def test_get_vm_ip_returns_ipv4(client, mock_api):
    """get_vm_ip returns a non-loopback IPv4 address."""
    mock_api.nodes("pve").qemu(200).agent("network-get-interfaces").get.return_value = {
        "result": [
            {
                "name": "lo",
                "ip-addresses": [
                    {"ip-address": "127.0.0.1", "ip-address-type": "ipv4"},
                ],
            },
            {
                "name": "eth0",
                "ip-addresses": [
                    {"ip-address": "10.20.0.50", "ip-address-type": "ipv4"},
                ],
            },
        ]
    }

    ip = client.get_vm_ip(200, timeout=1)
    assert ip == "10.20.0.50"


def test_get_vm_ip_returns_none_on_timeout(client, mock_api):
    """get_vm_ip returns None when guest agent doesn't respond."""
    mock_api.nodes("pve").qemu(200).agent("network-get-interfaces").get.side_effect = Exception(
        "agent not running"
    )
    ip = client.get_vm_ip(200, timeout=1)
    assert ip is None
