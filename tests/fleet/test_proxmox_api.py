"""Tests for orcest.fleet.proxmox_api."""

from unittest.mock import MagicMock, patch

import pytest

from orcest.fleet.proxmox_api import ProxmoxClient, mac_for_vm_id, parse_vm_ip

pytestmark = pytest.mark.unit


# -- mac_for_vm_id ------------------------------------------------------------


class TestMacForVmId:
    def test_format(self):
        mac = mac_for_vm_id(300)
        parts = mac.split(":")
        assert len(parts) == 6
        assert all(len(p) == 2 for p in parts)

    def test_locally_administered_prefix(self):
        mac = mac_for_vm_id(0)
        assert mac.startswith("02:4F:52:")

    def test_vm_id_zero(self):
        assert mac_for_vm_id(0) == "02:4F:52:00:00:00"

    def test_vm_id_300(self):
        assert mac_for_vm_id(300) == "02:4F:52:00:01:2C"

    def test_vm_id_65535(self):
        assert mac_for_vm_id(65535) == "02:4F:52:00:FF:FF"

    def test_uniqueness(self):
        macs = {mac_for_vm_id(i) for i in range(1000)}
        assert len(macs) == 1000

    def test_deterministic(self):
        assert mac_for_vm_id(42) == mac_for_vm_id(42)


# -- parse_vm_ip --------------------------------------------------------------


class TestParseVmIp:
    def test_single_interface_with_ipv4(self):
        interfaces = [
            {
                "name": "eth0",
                "ip-addresses": [
                    {"ip-address": "10.20.0.50", "ip-address-type": "ipv4"},
                ],
            },
        ]
        assert parse_vm_ip(interfaces) == "10.20.0.50"

    def test_skips_loopback_interface(self):
        interfaces = [
            {
                "name": "lo",
                "ip-addresses": [
                    {"ip-address": "127.0.0.1", "ip-address-type": "ipv4"},
                ],
            },
            {
                "name": "eth0",
                "ip-addresses": [
                    {"ip-address": "192.168.1.100", "ip-address-type": "ipv4"},
                ],
            },
        ]
        assert parse_vm_ip(interfaces) == "192.168.1.100"

    def test_loopback_only_returns_none(self):
        interfaces = [
            {
                "name": "lo",
                "ip-addresses": [
                    {"ip-address": "127.0.0.1", "ip-address-type": "ipv4"},
                ],
            },
        ]
        assert parse_vm_ip(interfaces) is None

    def test_skips_ipv6_returns_ipv4(self):
        interfaces = [
            {
                "name": "eth0",
                "ip-addresses": [
                    {"ip-address": "fe80::1", "ip-address-type": "ipv6"},
                    {"ip-address": "10.0.0.5", "ip-address-type": "ipv4"},
                ],
            },
        ]
        assert parse_vm_ip(interfaces) == "10.0.0.5"

    def test_ipv6_only_returns_none(self):
        interfaces = [
            {
                "name": "eth0",
                "ip-addresses": [
                    {"ip-address": "fe80::1", "ip-address-type": "ipv6"},
                ],
            },
        ]
        assert parse_vm_ip(interfaces) is None

    def test_empty_interfaces_returns_none(self):
        assert parse_vm_ip([]) is None

    def test_no_ip_addresses_field(self):
        interfaces = [{"name": "eth0"}]
        assert parse_vm_ip(interfaces) is None

    def test_multiple_interfaces_returns_first_ipv4(self):
        interfaces = [
            {
                "name": "eth0",
                "ip-addresses": [
                    {"ip-address": "10.0.0.1", "ip-address-type": "ipv4"},
                ],
            },
            {
                "name": "eth1",
                "ip-addresses": [
                    {"ip-address": "10.0.0.2", "ip-address-type": "ipv4"},
                ],
            },
        ]
        assert parse_vm_ip(interfaces) == "10.0.0.1"

    def test_skips_127_address_on_non_lo_interface(self):
        """Even if a non-loopback interface has 127.0.0.1, skip it."""
        interfaces = [
            {
                "name": "eth0",
                "ip-addresses": [
                    {"ip-address": "127.0.0.1", "ip-address-type": "ipv4"},
                ],
            },
            {
                "name": "eth1",
                "ip-addresses": [
                    {"ip-address": "10.0.0.5", "ip-address-type": "ipv4"},
                ],
            },
        ]
        assert parse_vm_ip(interfaces) == "10.0.0.5"


# -- Helper to build a mocked ProxmoxClient -----------------------------------


def _make_client() -> tuple[ProxmoxClient, MagicMock]:
    """Create a ProxmoxClient with a mocked ProxmoxAPI backend.

    Returns (client, mock_api) so tests can configure return values.
    """
    with patch("orcest.fleet.proxmox_api.ProxmoxAPI") as mock_cls:
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        client = ProxmoxClient(
            endpoint="https://10.20.0.1:8006",
            token_id="root@pam!orcest",
            token_secret="secret-token-value",
            node="pve",
            verify_ssl=False,
        )
    return client, mock_api


# -- ProxmoxClient tests ------------------------------------------------------


class TestProxmoxClientInit:
    def test_parses_endpoint_and_token(self):
        with patch("orcest.fleet.proxmox_api.ProxmoxAPI") as mock_cls:
            mock_cls.return_value = MagicMock()
            ProxmoxClient(
                endpoint="https://10.20.0.1:8006",
                token_id="root@pam!orcest",
                token_secret="secret123",
                node="pve",
            )
            mock_cls.assert_called_once_with(
                "10.20.0.1",
                port=8006,
                user="root@pam",
                token_name="orcest",
                token_value="secret123",
                verify_ssl=False,
                backend="https",
            )

    def test_rejects_token_id_without_separator(self):
        with pytest.raises(ValueError, match="expected 'user@realm!tokenname' format"):
            with patch("orcest.fleet.proxmox_api.ProxmoxAPI") as mock_cls:
                mock_cls.return_value = MagicMock()
                ProxmoxClient(
                    endpoint="https://10.20.0.1:8006",
                    token_id="root@pam",  # missing !tokenname
                    token_secret="secret123",
                    node="pve",
                )


class TestCloneVm:
    def test_calls_clone_endpoint(self):
        client, mock_api = _make_client()

        # Set up the chain: nodes("pve").qemu(100).clone.post(...)
        mock_clone = MagicMock()
        mock_clone.post.return_value = "UPID:pve:00001234:ABCDEF:clone"
        mock_qemu = MagicMock()
        mock_qemu.clone = mock_clone
        mock_api.nodes("pve").qemu.return_value = mock_qemu

        # Make wait_for_task succeed immediately
        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "stopped", "exitstatus": "OK"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        upid = client.clone_vm(
            template_id=100, new_id=200, name="worker-200",
        )

        assert upid == "UPID:pve:00001234:ABCDEF:clone"
        # Linked clone: storage must NOT be passed (Proxmox rejects it)
        mock_clone.post.assert_called_once_with(
            newid=200,
            name="worker-200",
            full=0,
        )
        # Verify wait_for_task was called (clone_vm blocks until task completes)
        mock_task_status.get.assert_called()

    def test_full_clone(self):
        client, mock_api = _make_client()

        mock_clone = MagicMock()
        mock_clone.post.return_value = "UPID:pve:00001234:ABCDEF:clone"
        mock_qemu = MagicMock()
        mock_qemu.clone = mock_clone
        mock_api.nodes("pve").qemu.return_value = mock_qemu

        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "stopped", "exitstatus": "OK"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        client.clone_vm(
            template_id=100, new_id=200, name="worker-200",
            storage="local-lvm", linked=False,
        )

        mock_clone.post.assert_called_once_with(
            newid=200,
            name="worker-200",
            storage="local-lvm",
            full=1,
        )


class TestStartVm:
    def test_calls_start_endpoint(self):
        client, mock_api = _make_client()
        client.start_vm(200)
        mock_api.nodes("pve").qemu(200).status.start.post.assert_called_once()


class TestStopVm:
    def test_calls_stop_endpoint(self):
        client, mock_api = _make_client()
        client.stop_vm(200)
        mock_api.nodes("pve").qemu(200).status.stop.post.assert_called_once()


class TestShutdownVm:
    @patch("orcest.fleet.proxmox_api.time")
    def test_graceful_shutdown_succeeds(self, mock_time):
        client, mock_api = _make_client()
        mock_time.monotonic.side_effect = [0, 1, 2]
        mock_api.nodes("pve").qemu(200).status.current.get.return_value = {
            "status": "stopped",
        }
        client.shutdown_vm(200, timeout=30)
        mock_api.nodes("pve").qemu(200).status.shutdown.post.assert_called_once()

    @patch("orcest.fleet.proxmox_api.time")
    def test_falls_back_to_hard_stop_on_timeout(self, mock_time):
        client, mock_api = _make_client()
        # monotonic: deadline, first check (in range), sleep, second check (past deadline)
        mock_time.monotonic.side_effect = [0, 1, 100]
        mock_api.nodes("pve").qemu(200).status.current.get.return_value = {
            "status": "running",
        }
        client.shutdown_vm(200, timeout=30)
        mock_api.nodes("pve").qemu(200).status.shutdown.post.assert_called_once()
        mock_api.nodes("pve").qemu(200).status.stop.post.assert_called_once()


class TestDestroyVm:
    def test_calls_delete_with_purge_and_disk_cleanup(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu(200).delete.return_value = None
        client.destroy_vm(200)
        mock_api.nodes("pve").qemu(200).delete.assert_called_once()
        call_kwargs = mock_api.nodes("pve").qemu(200).delete.call_args.kwargs
        assert call_kwargs["purge"] == 1
        # destroy-unreferenced-disks should be set when purging
        assert call_kwargs["destroy-unreferenced-disks"] == 1

    def test_calls_delete_without_purge(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu(200).delete.return_value = None
        client.destroy_vm(200, purge=False)
        mock_api.nodes("pve").qemu(200).delete.assert_called_once_with(purge=0)

    def test_waits_for_task_when_upid_returned(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu(200).delete.return_value = "UPID:pve:destroy"
        mock_api.nodes("pve").tasks("UPID:pve:destroy").status.get.return_value = {
            "status": "stopped", "exitstatus": "OK",
        }
        client.destroy_vm(200)
        mock_api.nodes("pve").tasks("UPID:pve:destroy").status.get.assert_called()


class TestGetVmStatus:
    def test_returns_status_field(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu(200).status.current.get.return_value = {
            "status": "running",
            "vmid": 200,
        }
        assert client.get_vm_status(200) == "running"


class TestGetVmIp:
    @patch("orcest.fleet.proxmox_api.time")
    def test_returns_ip_on_first_poll(self, mock_time):
        client, mock_api = _make_client()

        # Simulate monotonic clock that hasn't expired
        mock_time.monotonic.side_effect = [0, 1]

        mock_agent = MagicMock()
        mock_agent.get.return_value = {
            "result": [
                {
                    "name": "eth0",
                    "ip-addresses": [
                        {"ip-address": "10.20.0.50", "ip-address-type": "ipv4"},
                    ],
                },
            ],
        }
        mock_api.nodes("pve").qemu(200).agent.return_value = mock_agent

        ip = client.get_vm_ip(200, timeout=120)
        assert ip == "10.20.0.50"

    @patch("orcest.fleet.proxmox_api.time")
    def test_returns_none_on_timeout(self, mock_time):
        client, mock_api = _make_client()

        # First call sets deadline (0 + 5 = 5), second checks (10 > 5) -> exit
        mock_time.monotonic.side_effect = [0, 10]

        mock_api.nodes("pve").qemu(200).agent.side_effect = Exception("agent not ready")

        ip = client.get_vm_ip(200, timeout=5)
        assert ip is None

    @patch("orcest.fleet.proxmox_api.time")
    def test_retries_until_agent_ready(self, mock_time):
        client, mock_api = _make_client()

        # monotonic: set deadline, check (in range), sleep, check (in range), check (done)
        mock_time.monotonic.side_effect = [0, 1, 2, 3]

        mock_agent = MagicMock()
        # The agent() call should raise on first call, succeed on second
        mock_api.nodes("pve").qemu(200).agent.side_effect = [
            Exception("not ready"),
            mock_agent,
        ]
        mock_agent.get.return_value = {
            "result": [
                {
                    "name": "eth0",
                    "ip-addresses": [
                        {"ip-address": "10.20.0.50", "ip-address-type": "ipv4"},
                    ],
                },
            ],
        }

        ip = client.get_vm_ip(200, timeout=120)
        assert ip == "10.20.0.50"
        # Verify it actually retried (slept between attempts)
        mock_time.sleep.assert_called_with(2)


class TestNextFreeVmid:
    def test_returns_int(self):
        client, mock_api = _make_client()
        mock_api.cluster.nextid.get.return_value = "300"
        assert client.next_free_vmid() == 300

    def test_already_int(self):
        client, mock_api = _make_client()
        mock_api.cluster.nextid.get.return_value = 301
        assert client.next_free_vmid() == 301


class TestListVms:
    def test_returns_all_vms_without_prefix(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu.get.return_value = [
            {"vmid": 100, "name": "my-vm", "status": "running"},
            {"vmid": 200, "name": "orcest-worker-200", "status": "stopped"},
        ]
        result = client.list_vms()
        assert len(result) == 2

    def test_filters_by_name_prefix(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu.get.return_value = [
            {"vmid": 100, "name": "my-vm", "status": "running"},
            {"vmid": 200, "name": "orcest-worker-200", "status": "stopped"},
            {"vmid": 201, "name": "orcest-worker-201", "status": "running"},
        ]
        result = client.list_vms(name_prefix="orcest-worker-")
        assert len(result) == 2
        assert all(vm["name"].startswith("orcest-worker-") for vm in result)

    def test_empty_result(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu.get.return_value = []
        result = client.list_vms(name_prefix="orcest-worker-")
        assert result == []

    def test_no_match(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu.get.return_value = [
            {"vmid": 100, "name": "unrelated-vm", "status": "running"},
        ]
        result = client.list_vms(name_prefix="orcest-worker-")
        assert result == []


class TestSetCloudInitUserdata:
    def test_uploads_snippet_and_sets_cicustom(self):
        import io

        client, mock_api = _make_client()
        client.set_cloud_init_userdata(100, "#cloud-config\npackages: []")

        # Verify upload was called with a BytesIO file object
        upload_call = mock_api.nodes("pve").storage("local").upload.post
        upload_call.assert_called_once()
        call_kwargs = upload_call.call_args.kwargs
        assert call_kwargs["content"] == "snippets"
        assert call_kwargs["filename"] == "orcest-template-100-user.yaml"
        # The file should be a BytesIO (io.IOBase), not raw bytes
        assert isinstance(call_kwargs["file"], io.BytesIO)
        call_kwargs["file"].seek(0)
        assert call_kwargs["file"].read() == b"#cloud-config\npackages: []"

        # Verify cicustom was set on the VM config
        mock_api.nodes("pve").qemu(100).config.put.assert_called_once_with(
            cicustom="user=local:snippets/orcest-template-100-user.yaml",
        )

    def test_custom_storage(self):
        client, mock_api = _make_client()
        client.set_cloud_init_userdata(200, "data", storage="nfs-share")

        mock_api.nodes("pve").storage("nfs-share").upload.post.assert_called_once()
        mock_api.nodes("pve").qemu(200).config.put.assert_called_once_with(
            cicustom="user=nfs-share:snippets/orcest-template-200-user.yaml",
        )


class TestSetVmNetwork:
    def test_sets_net0_with_mac_and_bridge(self):
        client, mock_api = _make_client()
        client.set_vm_network(200, mac="02:4F:52:00:00:C8")
        mock_api.nodes("pve").qemu(200).config.put.assert_called_once_with(
            net0="virtio=02:4F:52:00:00:C8,bridge=vmbr0",
        )

    def test_custom_bridge(self):
        client, mock_api = _make_client()
        client.set_vm_network(200, mac="02:4F:52:00:00:C8", bridge="vmbr1")
        mock_api.nodes("pve").qemu(200).config.put.assert_called_once_with(
            net0="virtio=02:4F:52:00:00:C8,bridge=vmbr1",
        )


class TestDownloadImage:
    def test_downloads_and_waits_for_task(self):
        client, mock_api = _make_client()
        mock_storage = mock_api.nodes("pve").storage("local")
        mock_storage("download-url").post.return_value = "UPID:pve:download"

        # Make wait_for_task succeed
        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "stopped", "exitstatus": "OK"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        upid = client.download_image(
            "https://example.com/image.img", "image.img", storage="local",
        )
        assert upid == "UPID:pve:download"
        mock_storage("download-url").post.assert_called_once_with(
            content="iso", filename="image.img", url="https://example.com/image.img",
        )

    def test_custom_content_type(self):
        client, mock_api = _make_client()
        mock_storage = mock_api.nodes("pve").storage("nfs")
        mock_storage("download-url").post.return_value = "UPID:pve:dl2"

        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "stopped", "exitstatus": "OK"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        client.download_image(
            "https://example.com/disk.qcow2", "disk.qcow2",
            storage="nfs", content_type="images",
        )
        mock_storage("download-url").post.assert_called_once_with(
            content="images", filename="disk.qcow2", url="https://example.com/disk.qcow2",
        )

    @patch("orcest.fleet.proxmox_api.time")
    def test_propagates_task_failure(self, mock_time):
        client, mock_api = _make_client()
        mock_storage = mock_api.nodes("pve").storage("local")
        mock_storage("download-url").post.return_value = "UPID:pve:dl-fail"

        mock_time.monotonic.side_effect = [0, 1]
        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {
            "status": "stopped",
            "exitstatus": "download failed: connection refused",
        }
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        with pytest.raises(RuntimeError, match="download failed"):
            client.download_image("https://example.com/bad.img", "bad.img")


class TestCreateVm:
    def test_creates_vm_with_kwargs(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu.post.return_value = None

        client.create_vm(vm_id=200, name="test-vm", memory=2048, cores=2)
        mock_api.nodes("pve").qemu.post.assert_called_once_with(
            vmid=200, name="test-vm", memory=2048, cores=2,
        )

    def test_waits_for_task_when_upid_returned(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu.post.return_value = "UPID:pve:create"

        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "stopped", "exitstatus": "OK"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        client.create_vm(vm_id=200, name="test-vm")
        # Verify wait_for_task was actually invoked
        mock_task_status.get.assert_called()

    def test_forwards_import_from_kwargs(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu.post.return_value = None

        client.create_vm(
            vm_id=200, name="test-vm",
            scsihw="virtio-scsi-pci",
            scsi0="ssd-pool:0,import-from=local:iso/noble-server-cloudimg-amd64.img",
        )
        mock_api.nodes("pve").qemu.post.assert_called_once_with(
            vmid=200, name="test-vm",
            scsihw="virtio-scsi-pci",
            scsi0="ssd-pool:0,import-from=local:iso/noble-server-cloudimg-amd64.img",
        )


class TestResizeDisk:
    def test_calls_resize_endpoint(self):
        client, mock_api = _make_client()
        client.resize_disk(200, "scsi0", "30G")
        mock_api.nodes("pve").qemu(200).resize.put.assert_called_once_with(
            disk="scsi0", size="30G",
        )


class TestConvertToTemplate:
    def test_calls_template_endpoint(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu(100).template.post.return_value = None
        client.convert_to_template(100)
        mock_api.nodes("pve").qemu(100).template.post.assert_called_once()

    def test_waits_for_task_when_upid_returned(self):
        client, mock_api = _make_client()
        mock_api.nodes("pve").qemu(100).template.post.return_value = (
            "UPID:pve:00001234:ABCDEF:template"
        )

        # Make wait_for_task succeed
        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "stopped", "exitstatus": "OK"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        client.convert_to_template(100)


class TestWaitForTask:
    @patch("orcest.fleet.proxmox_api.time")
    def test_success(self, mock_time):
        client, mock_api = _make_client()

        mock_time.monotonic.side_effect = [0, 1]

        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "stopped", "exitstatus": "OK"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        result = client.wait_for_task("UPID:pve:00001234:ABCDEF:clone")
        assert result is True

    @patch("orcest.fleet.proxmox_api.time")
    def test_failure_raises(self, mock_time):
        client, mock_api = _make_client()

        mock_time.monotonic.side_effect = [0, 1]

        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {
            "status": "stopped",
            "exitstatus": "command 'qm clone' failed: error",
        }
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        with pytest.raises(RuntimeError, match="failed with exit status"):
            client.wait_for_task("UPID:pve:00001234:ABCDEF:clone")

    @patch("orcest.fleet.proxmox_api.time")
    def test_timeout_raises(self, mock_time):
        client, mock_api = _make_client()

        # First call sets deadline (0 + 10 = 10), second check exceeds it
        mock_time.monotonic.side_effect = [0, 20]

        mock_task_status = MagicMock()
        mock_task_status.get.return_value = {"status": "running"}
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        with pytest.raises(RuntimeError, match="timed out"):
            client.wait_for_task("UPID:pve:00001234:ABCDEF:clone", timeout=10)

    @patch("orcest.fleet.proxmox_api.time")
    def test_polls_until_done(self, mock_time):
        client, mock_api = _make_client()

        # monotonic: deadline, check(in range), sleep, check(in range), done
        mock_time.monotonic.side_effect = [0, 1, 2, 3]

        mock_task_status = MagicMock()
        mock_task_status.get.side_effect = [
            {"status": "running"},
            {"status": "stopped", "exitstatus": "OK"},
        ]
        mock_task = MagicMock()
        mock_task.status = mock_task_status
        mock_api.nodes("pve").tasks.return_value = mock_task

        result = client.wait_for_task("UPID:pve:00001234:ABCDEF:clone")
        assert result is True
        assert mock_task_status.get.call_count == 2
        mock_time.sleep.assert_called_with(1)
