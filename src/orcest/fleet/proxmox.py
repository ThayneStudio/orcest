"""Proxmox VE API client for VM lifecycle management.

Wraps the ``proxmoxer`` library to provide high-level operations for
creating, destroying, and querying worker VMs. Replaces the shell-based
``create-vm.sh`` with API-driven VM management.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time
from typing import Any

from orcest.fleet.inventory import ProxmoxConfig

logger = logging.getLogger(__name__)

# Ubuntu 24.04 cloud image
CLOUD_IMG_URL = "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
CLOUD_IMG_NAME = "noble-server-cloudimg-amd64.img"

# Default VM specs for workers
DEFAULT_MEMORY = 16384  # 16 GB
DEFAULT_CORES = 4
DEFAULT_SOCKETS = 2
DEFAULT_DISK_SIZE = "30G"
DEFAULT_BRIDGE = "vmbr0"


class ProxmoxClient:
    """High-level Proxmox API client for orcest fleet management."""

    def __init__(self, config: ProxmoxConfig) -> None:
        self.config = config
        self._api = None

    @property
    def api(self) -> Any:
        """Lazy-connect to the Proxmox API."""
        if self._api is None:
            try:
                from proxmoxer import ProxmoxAPI
            except ImportError:
                raise ImportError(
                    "proxmoxer is required for fleet management. "
                    "Install it with: pip install proxmoxer requests"
                )
            self._api = ProxmoxAPI(
                self.config.host,
                user=self.config.token_id.split("!")[0],
                token_name=self.config.token_id.split("!")[-1],
                token_value=self.config.token_secret,
                verify_ssl=self.config.verify_ssl,
            )
        return self._api

    @property
    def node(self) -> Any:
        """Proxmox node API endpoint."""
        return self.api.nodes(self.config.node)

    def vm_exists(self, vm_id: int) -> bool:
        """Check if a VM with the given ID exists."""
        try:
            self.node.qemu(vm_id).status.current.get()
            return True
        except Exception:
            logger.debug("VM %d: existence check failed, treating as not found", vm_id, exc_info=True)
            return False

    def get_vm_status(self, vm_id: int) -> str:
        """Get VM status (running, stopped, etc.)."""
        result = self.node.qemu(vm_id).status.current.get()
        return result.get("status", "unknown")

    def get_vm_ip(self, vm_id: int, timeout: int = 120) -> str | None:
        """Get VM IP address via QEMU guest agent.

        Polls up to ``timeout`` seconds for the guest agent to report a
        non-loopback IPv4 address. Returns None if no IP is found.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                ifaces = self.node.qemu(vm_id).agent("network-get-interfaces").get()
                for iface in ifaces.get("result", []):
                    for addr in iface.get("ip-addresses", []):
                        ip = addr.get("ip-address", "")
                        if addr.get("ip-address-type") == "ipv4" and not ip.startswith("127."):
                            return ip
            except Exception:
                logger.debug(
                    "VM %d: guest agent not ready yet, retrying", vm_id, exc_info=True
                )
            time.sleep(5)
        return None

    def create_vm(
        self,
        vm_id: int,
        name: str,
        *,
        memory: int = DEFAULT_MEMORY,
        cores: int = DEFAULT_CORES,
        sockets: int = DEFAULT_SOCKETS,
        disk_size: str = DEFAULT_DISK_SIZE,
        bridge: str = DEFAULT_BRIDGE,
        cicustom: str = "",
        ci_user: str = "ubuntu",
        ssh_public_key: str = "",
    ) -> None:
        """Create a VM from a cloud image with cloud-init.

        This replicates the ``create-vm.sh`` workflow via the Proxmox API:
        1. Download cloud image (if not cached on the node)
        2. Create VM with specified resources
        3. Import cloud image as disk
        4. Configure cloud-init
        5. Start the VM

        Args:
            vm_id: Proxmox VM ID.
            name: VM display name.
            memory: RAM in MB.
            cores: CPU cores per socket.
            sockets: CPU sockets.
            disk_size: Disk size (e.g. "30G").
            bridge: Network bridge.
            cicustom: Cloud-init custom user-data snippet path
                      (e.g. "user=local:snippets/worker-200.yaml").
            ci_user: Cloud-init default user.
            ssh_public_key: SSH public key for cloud-init user.
        """
        storage = self.config.storage

        if self.vm_exists(vm_id):
            raise ValueError(f"VM {vm_id} already exists")

        logger.info("Creating VM %d (%s) on node %s", vm_id, name, self.config.node)

        # Create the VM
        self.node.qemu.create(
            vmid=vm_id,
            name=name,
            cpu="host",
            memory=memory,
            cores=cores,
            sockets=sockets,
            net0=f"virtio,bridge={bridge}",
            serial0="socket",
            vga="serial0",
            agent="enabled=1",
            scsihw="virtio-scsi-pci",
            ide2=f"{storage}:cloudinit",
            boot="order=scsi0",
            ipconfig0="ip=dhcp",
            ciuser=ci_user,
        )

        if ssh_public_key:
            self.node.qemu(vm_id).config.put(sshkeys=ssh_public_key)

        if cicustom:
            self.node.qemu(vm_id).config.put(cicustom=cicustom)

        # Download and import cloud image
        # The image needs to be on the Proxmox node. We use the download-url
        # API to fetch it to the node's storage if not already present.
        logger.info("Importing cloud image for VM %d", vm_id)
        self._import_cloud_image(vm_id, storage, disk_size)

        # Start the VM
        logger.info("Starting VM %d", vm_id)
        self.node.qemu(vm_id).status.start.post()

    def _import_cloud_image(self, vm_id: int, storage: str, disk_size: str) -> None:
        """Import the Ubuntu cloud image as the VM's boot disk.

        Uses the Proxmox download-url API to cache the image on the node,
        then imports it as a disk for the VM.
        """
        # Download cloud image to the node's ISO storage
        # Check if already downloaded
        try:
            volumes = self.node.storage("local").content.get()
            has_image = any(v.get("volid", "").endswith(CLOUD_IMG_NAME) for v in volumes)
        except Exception:
            logger.debug("Failed to check cloud image presence, assuming not downloaded", exc_info=True)
            has_image = False

        if not has_image:
            logger.info("Downloading cloud image to Proxmox node...")
            upid: str = self.node.storage("local").post(
                "download-url",
                content="iso",
                filename=CLOUD_IMG_NAME,
                url=CLOUD_IMG_URL,
            )
            # Wait for download to complete
            self._wait_for_task_completion(upid)

        # Import the disk via qm importdisk (requires SSH access to the Proxmox host).
        # The Proxmox REST API does not expose a direct importdisk endpoint.
        img_path = f"/var/lib/vz/template/iso/{CLOUD_IMG_NAME}"
        result = subprocess.run(
            ["ssh", f"root@{self.config.host}", f"qm importdisk {vm_id} {img_path} {storage}"],
            check=True,
            capture_output=True,
            text=True,
        )
        # qm importdisk prints: Successfully imported disk as 'storage:vm-NNN-disk-N'
        m = re.search(r"'([^']+)'", result.stdout)
        disk_volid = m.group(1) if m else f"{storage}:vm-{vm_id}-disk-0"
        self.node.qemu(vm_id).config.put(scsi0=f"{disk_volid},discard=on")

        # Resize disk
        self.node.qemu(vm_id).resize.put(disk="scsi0", size=disk_size)

    def _wait_for_task_completion(self, upid: str, timeout: int = 300) -> None:
        """Wait for a specific node task (identified by UPID) to complete."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = self.node.tasks(upid).status.get()
            status = result.get("status", "")
            if status == "OK":
                return
            if status == "ERROR":
                raise RuntimeError(f"Proxmox task {upid} failed")
            time.sleep(2)
        logger.warning("Task %s did not complete within %ds", upid, timeout)

    def destroy_vm(self, vm_id: int) -> None:
        """Stop and destroy a VM."""
        logger.info("Destroying VM %d", vm_id)
        try:
            status = self.get_vm_status(vm_id)
            if status == "running":
                self.node.qemu(vm_id).status.stop.post()
                # Wait for VM to stop
                deadline = time.time() + 60
                while time.time() < deadline:
                    if self.get_vm_status(vm_id) == "stopped":
                        break
                    time.sleep(2)
        except Exception:
            logger.warning("Could not stop VM %d before destroying", vm_id, exc_info=True)

        self.node.qemu(vm_id).delete(purge=1)

    def upload_snippet(self, filename: str, content: str, storage: str = "local") -> str:
        """Upload a cloud-init snippet to Proxmox node storage.

        Returns the snippet path suitable for ``--cicustom`` (e.g.
        ``local:snippets/worker-200.yaml``).
        """
        # The Proxmox API for snippet upload requires writing to the
        # snippets directory. We use the storage content API.
        self.node.storage(storage).upload.post(
            content="snippets",
            filename=filename,
            file=content.encode(),
        )
        return f"{storage}:snippets/{filename}"

    def delete_snippet(self, volid: str) -> None:
        """Delete a snippet from Proxmox storage."""
        storage, _, _ = volid.partition(":")
        self.node.storage(storage).content(volid).delete()
