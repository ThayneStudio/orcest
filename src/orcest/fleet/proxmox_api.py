"""Proxmox REST API client for VM lifecycle operations.

Thin wrapper around the ``proxmoxer`` library, focused on the subset of
Proxmox VE operations needed by the orcest pool manager: cloning templates,
starting/stopping/destroying VMs, and querying guest-agent networking.
"""

from __future__ import annotations

import io
import logging
import time

from proxmoxer import ProxmoxAPI

logger = logging.getLogger(__name__)


def mac_for_vm_id(vm_id: int) -> str:
    """Derive a deterministic MAC address from a VM ID.

    Uses the locally-administered unicast prefix ``02:4F:52`` (``OR`` in
    ASCII, for *orcest*), leaving 3 bytes for the VM ID — enough for
    16 million unique IDs.  The ``02`` first octet sets the U/L bit
    (locally administered) and clears the I/G bit (unicast).

    This avoids collision with Proxmox auto-generated MACs (OUI
    ``BC:24:11``) and with any real hardware vendor OUIs.
    """
    b1 = (vm_id >> 16) & 0xFF
    b2 = (vm_id >> 8) & 0xFF
    b3 = vm_id & 0xFF
    return f"02:4F:52:{b1:02X}:{b2:02X}:{b3:02X}"


def parse_vm_ip(interfaces: list[dict]) -> str | None:
    """Extract the first non-loopback IPv4 address from guest-agent interface data.

    The *interfaces* list comes from the Proxmox
    ``/agent/network-get-interfaces`` endpoint and looks like::

        [
            {"name": "lo", "ip-addresses": [{"ip-address": "127.0.0.1", ...}]},
            {"name": "eth0", "ip-addresses": [
                {"ip-address": "10.20.0.50", "ip-address-type": "ipv4", ...},
                {"ip-address": "fe80::1", "ip-address-type": "ipv6", ...},
            ]},
        ]

    Returns the first non-loopback IPv4 address found, or ``None``.
    """
    for iface in interfaces:
        if iface.get("name") == "lo":
            continue
        for addr in iface.get("ip-addresses", []):
            if addr.get("ip-address-type") == "ipv4":
                ip = addr.get("ip-address")
                if ip and ip != "127.0.0.1" and not ip.startswith("169.254."):
                    return ip
    return None


class ProxmoxClient:
    """Client for Proxmox VE REST API operations.

    Uses ``proxmoxer`` with token-based authentication. All VM operations
    target a single Proxmox node.

    Args:
        endpoint: Proxmox API URL (e.g. ``https://10.20.0.1:8006``).
        token_id: API token ID (e.g. ``root@pam!orcest``).
        token_secret: API token secret.
        node: Proxmox node name (e.g. ``pve``).
        verify_ssl: Whether to verify SSL certificates. ``False`` for self-signed certs.
    """

    def __init__(
        self,
        endpoint: str,
        token_id: str,
        token_secret: str,
        node: str,
        verify_ssl: bool = False,
    ) -> None:
        # Parse host and port from endpoint URL
        # e.g. "https://10.20.0.1:8006" -> host="10.20.0.1", port=8006
        from urllib.parse import urlparse

        parsed = urlparse(endpoint)
        host = parsed.hostname or endpoint
        port = parsed.port or 8006

        # Split token_id into user and token name
        # e.g. "root@pam!orcest" -> user="root@pam", token_name="orcest"
        if "!" not in token_id:
            raise ValueError(
                f"Invalid token_id {token_id!r}: expected 'user@realm!tokenname' format"
            )
        user, token_name = token_id.split("!", 1)

        self._api = ProxmoxAPI(
            host,
            port=port,
            user=user,
            token_name=token_name,
            token_value=token_secret,
            verify_ssl=verify_ssl,
            backend="https",
            timeout=30,
        )
        self._node = node
        self._host = host

    def clone_vm(
        self,
        template_id: int,
        new_id: int,
        name: str,
        storage: str = "",
        linked: bool = True,
    ) -> str:
        """Clone a VM from a template.

        Args:
            template_id: VM ID of the template to clone from.
            new_id: VM ID for the new clone.
            name: Name for the new VM.
            storage: Target storage for full clones. Ignored for linked clones
                (linked clones must use the same storage as the template).
            linked: If True, create a linked clone (faster, less disk). Otherwise full clone.

        Returns:
            The Proxmox task UPID string.
        """
        logger.info(
            "Cloning VM %d -> %d (name=%s, linked=%s)",
            template_id,
            new_id,
            name,
            linked,
        )
        params: dict[str, object] = {
            "newid": new_id,
            "name": name,
            "full": 0 if linked else 1,
        }
        # Proxmox rejects the 'storage' parameter for linked clones
        if not linked and storage:
            params["storage"] = storage
        upid = self._api.nodes(self._node).qemu(template_id).clone.post(**params)
        self.wait_for_task(upid)
        return upid

    def set_vm_network(
        self,
        vm_id: int,
        mac: str,
        bridge: str = "vmbr0",
    ) -> None:
        """Set the network adapter configuration on a VM.

        Typically called after cloning (before boot) to assign a
        deterministic MAC address via :func:`mac_for_vm_id`.

        Args:
            vm_id: The VM ID to configure.
            mac: MAC address (e.g. ``"02:4F:52:00:01:2C"``).
            bridge: Proxmox bridge name (default ``"vmbr0"``).
        """
        logger.info("Setting VM %d MAC to %s (bridge=%s)", vm_id, mac, bridge)
        self._api.nodes(self._node).qemu(vm_id).config.put(
            net0=f"virtio={mac},bridge={bridge}",
        )

    def start_vm(self, vm_id: int) -> None:
        """Start a VM.

        Args:
            vm_id: The VM ID to start.
        """
        logger.info("Starting VM %d", vm_id)
        self._api.nodes(self._node).qemu(vm_id).status.start.post()

    def stop_vm(self, vm_id: int) -> None:
        """Stop a VM (hard stop — like pulling the power cord).

        Args:
            vm_id: The VM ID to stop.
        """
        logger.info("Stopping VM %d", vm_id)
        self._api.nodes(self._node).qemu(vm_id).status.stop.post()

    def shutdown_vm(self, vm_id: int, timeout: int = 60) -> None:
        """Gracefully shut down a VM via ACPI and wait for it to stop.

        Sends an ACPI shutdown signal and polls until the VM reaches
        ``stopped`` state. Falls back to a hard stop on timeout.

        This is preferred over :meth:`stop_vm` when the guest filesystem
        must be cleanly flushed (e.g. before converting to a template).

        Args:
            vm_id: The VM ID to shut down.
            timeout: Seconds to wait before falling back to hard stop.
        """
        logger.info("Shutting down VM %d (timeout=%ds)", vm_id, timeout)
        self._api.nodes(self._node).qemu(vm_id).status.shutdown.post()

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.get_vm_status(vm_id) == "stopped":
                logger.info("VM %d shut down gracefully", vm_id)
                return
            time.sleep(2)

        logger.warning("VM %d did not shut down in %ds, forcing stop", vm_id, timeout)
        self.stop_vm(vm_id)

    def destroy_vm(self, vm_id: int, purge: bool = True) -> None:
        """Destroy a VM and optionally purge its disks.

        Args:
            vm_id: The VM ID to destroy.
            purge: If True, also remove disks and any related resources.
        """
        logger.info("Destroying VM %d (purge=%s)", vm_id, purge)
        params: dict[str, int] = {"purge": 1 if purge else 0}
        if purge:
            # Explicitly request disk cleanup to avoid orphaned disks across
            # Proxmox versions (some older releases don't default this on).
            params["destroy-unreferenced-disks"] = 1
        upid = self._api.nodes(self._node).qemu(vm_id).delete(**params)
        if upid:
            self.wait_for_task(upid)

    def get_vm_status(self, vm_id: int) -> str:
        """Get the current status of a VM.

        Args:
            vm_id: The VM ID to query.

        Returns:
            Status string (e.g. ``"running"``, ``"stopped"``).
        """
        result = self._api.nodes(self._node).qemu(vm_id).status.current.get()
        return result["status"]

    def get_vm_ip(self, vm_id: int, timeout: int = 120) -> str | None:
        """Get the VM's IP address via the QEMU guest agent.

        Polls the guest agent until a non-loopback IPv4 address is found
        or the timeout expires. The guest agent typically takes a few seconds
        to become available after VM boot.

        Args:
            vm_id: The VM ID to query.
            timeout: Maximum seconds to wait for an IP address.

        Returns:
            The first non-loopback IPv4 address, or ``None`` on timeout.
        """
        logger.info("Waiting for VM %d IP address (timeout=%ds)", vm_id, timeout)
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            try:
                result = (
                    self._api.nodes(self._node).qemu(vm_id).agent("network-get-interfaces").get()
                )
                interfaces = result.get("result", [])
                ip = parse_vm_ip(interfaces)
                if ip is not None:
                    logger.info("VM %d has IP %s", vm_id, ip)
                    return ip
            except Exception:
                # Guest agent not ready yet — keep polling
                pass

            time.sleep(2)

        logger.warning("Timed out waiting for VM %d IP address", vm_id)
        return None

    def next_free_vmid(self) -> int:
        """Get the next available VM ID from the cluster.

        Returns:
            The next free VM ID as an integer.
        """
        vmid = self._api.cluster.nextid.get()
        return int(vmid)

    def list_vms(self, name_prefix: str = "") -> list[dict]:
        """List VMs on the node, optionally filtering by name prefix.

        Args:
            name_prefix: If non-empty, only return VMs whose name starts
                with this string.

        Returns:
            List of VM info dicts from the Proxmox API, each containing
            at least ``vmid``, ``name``, and ``status`` keys.
        """
        vms: list[dict] = self._api.nodes(self._node).qemu.get()
        if name_prefix:
            vms = [vm for vm in vms if vm.get("name", "").startswith(name_prefix)]
        return vms

    def list_storage(self, content_type: str | None = None) -> list[dict]:
        """Query available storage pools on this node.

        Args:
            content_type: If given, filter to storages that support this
                content type (e.g. ``"images"``, ``"snippets"``).

        Returns:
            List of storage info dicts with keys: ``storage``, ``type``,
            ``content``, ``avail``, ``total``, ``enabled``.
        """
        storages: list[dict] = self._api.nodes(self._node).storage.get()
        # Only include enabled/active storages
        storages = [s for s in storages if s.get("enabled", 1) and s.get("active", 1)]
        if content_type:
            storages = [s for s in storages if content_type in s.get("content", "").split(",")]
        return storages

    def set_cloud_init_userdata(
        self,
        vm_id: int,
        userdata: str,
        storage: str = "local",
    ) -> None:
        """Set cloud-init user-data on a VM.

        Writes the snippet to the Proxmox host via SSH and attaches it to
        the VM's cloud-init drive via ``qm set --cicustom``.

        Falls back to the API upload endpoint if SSH fails (e.g. when
        running directly on the Proxmox host without SSH configured).

        Args:
            vm_id: The VM ID to configure.
            userdata: The cloud-init user-data YAML content.
            storage: Proxmox storage name for snippets (default ``"local"``).
        """
        snippet_name = f"orcest-template-{vm_id}-user.yaml"
        logger.info("Uploading cloud-init snippet %s for VM %d", snippet_name, vm_id)

        # Try SSH first (works from pool manager on orchestrator VM)
        host = self._host
        if self._write_snippet_ssh(host, snippet_name, userdata, storage, vm_id):
            return

        # Fallback: API upload (may fail on some PVE versions)
        logger.info("SSH failed, falling back to API upload for VM %d", vm_id)
        payload = io.BytesIO(userdata.encode())
        payload.name = snippet_name
        self._api.nodes(self._node).storage(storage).upload.post(
            content="snippets",
            filename=snippet_name,
            file=payload,
        )
        self._api.nodes(self._node).qemu(vm_id).config.put(
            cicustom=f"user={storage}:snippets/{snippet_name}",
        )

    def _write_snippet_ssh(
        self,
        host: str,
        snippet_name: str,
        userdata: str,
        storage: str,
        vm_id: int,
    ) -> bool:
        """Write a cloud-init snippet via SSH and set cicustom.

        Returns ``True`` on success, ``False`` if SSH is not available.
        Raises on SSH errors after connection succeeds.
        """
        import subprocess

        ssh_target = f"root@{host}"
        ssh_opts = [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
        ]

        # Write snippet content via stdin
        write_cmd = f"mkdir -p /var/lib/vz/snippets && cat > /var/lib/vz/snippets/{snippet_name}"
        result = subprocess.run(
            ["ssh", *ssh_opts, ssh_target, write_cmd],
            input=userdata,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            logger.warning("SSH snippet write failed: %s", result.stderr.strip())
            return False

        # Set cicustom on the VM
        cicustom_cmd = f"qm set {vm_id} --cicustom user={storage}:snippets/{snippet_name}"
        result = subprocess.run(
            ["ssh", *ssh_opts, ssh_target, cicustom_cmd],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            raise RuntimeError(f"qm set --cicustom failed: {result.stderr.strip()}")
        return True

    def download_image(
        self,
        url: str,
        filename: str,
        storage: str = "local",
        content_type: str = "iso",
        timeout: int = 600,
    ) -> str:
        """Download a file from a URL to Proxmox storage.

        Uses the ``/storage/{storage}/download-url`` endpoint to fetch
        a cloud image (or other file) directly onto the Proxmox node.

        Args:
            url: URL to download from.
            filename: Target filename on storage.
            storage: Proxmox storage name (default ``"local"``).
            content_type: Storage content type (default ``"iso"``).
            timeout: Seconds to wait for the download task to complete.

        Returns:
            The Proxmox task UPID string.
        """
        logger.info("Downloading %s -> %s:%s/%s", url, storage, content_type, filename)
        upid = (
            self._api.nodes(self._node)
            .storage(storage)("download-url")
            .post(
                content=content_type,
                filename=filename,
                url=url,
            )
        )
        self.wait_for_task(upid, timeout=timeout)
        return upid

    def create_vm(self, vm_id: int, name: str, **kwargs: object) -> None:
        """Create a new VM with the given configuration.

        When ``import-from`` is used in disk parameters, the API may return
        a task UPID for the async disk import. This method waits for it.

        Args:
            vm_id: The VM ID for the new VM.
            name: Name for the new VM.
            **kwargs: Additional Proxmox VM configuration parameters
                (e.g. ``memory``, ``cores``, ``scsihw``, ``net0``).
        """
        logger.info("Creating VM %d (name=%s)", vm_id, name)
        upid = self._api.nodes(self._node).qemu.post(vmid=vm_id, name=name, **kwargs)
        if upid:
            self.wait_for_task(upid)

    def resize_disk(self, vm_id: int, disk: str, size: str) -> None:
        """Resize a VM disk.

        Args:
            vm_id: The VM ID.
            disk: Disk identifier (e.g. ``"scsi0"``).
            size: New size (e.g. ``"30G"``).
        """
        logger.info("Resizing VM %d disk %s to %s", vm_id, disk, size)
        self._api.nodes(self._node).qemu(vm_id).resize.put(disk=disk, size=size)

    def convert_to_template(self, vm_id: int) -> None:
        """Convert a VM to a template.

        Args:
            vm_id: The VM ID to convert.
        """
        logger.info("Converting VM %d to template", vm_id)
        upid = self._api.nodes(self._node).qemu(vm_id).template.post()
        if upid:
            self.wait_for_task(upid)

    def wait_for_task(self, upid: str, timeout: int = 300) -> bool:
        """Poll a Proxmox task until it completes.

        Args:
            upid: The Proxmox task UPID string.
            timeout: Maximum seconds to wait for task completion.

        Returns:
            ``True`` if the task completed successfully.

        Raises:
            RuntimeError: If the task fails or times out.
        """
        logger.debug("Waiting for task %s (timeout=%ds)", upid, timeout)
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            task_status = self._api.nodes(self._node).tasks(upid).status.get()
            status = task_status.get("status")
            if status == "stopped":
                exitstatus = task_status.get("exitstatus", "")
                if exitstatus == "OK":
                    logger.debug("Task %s completed successfully", upid)
                    return True
                raise RuntimeError(f"Proxmox task {upid} failed with exit status: {exitstatus}")

            time.sleep(1)

        raise RuntimeError(f"Proxmox task {upid} timed out after {timeout}s")
