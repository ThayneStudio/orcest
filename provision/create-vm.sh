#!/usr/bin/env bash
# Create an orcest worker VM on Proxmox using a Ubuntu cloud image.
#
# Usage:
#   ./provision/create-vm.sh <vm-id> <vm-name> [storage]
#
# Examples:
#   ./provision/create-vm.sh 200 orcest-worker-1
#   ./provision/create-vm.sh 201 orcest-worker-2 local-zfs
#
# Prerequisites:
#   - Run on the Proxmox host (needs qm command)
#   - Cloud image downloaded (script will download if missing)
#   - SSH public key at ~/.ssh/authorized_keys or ~/.ssh/id_ed25519.pub

set -euo pipefail

VMID="${1:?Usage: $0 <vm-id> <vm-name> [storage]}"
VMNAME="${2:?Usage: $0 <vm-id> <vm-name> [storage]}"
STORAGE="${3:-local-lvm}"

CLOUD_IMG="noble-server-cloudimg-amd64.img"
CLOUD_URL="https://cloud-images.ubuntu.com/noble/current/${CLOUD_IMG}"
IMG_CACHE="/var/cache/cloud-images"

MEMORY=16384    # 16 GB
CORES=4
SOCKETS=2
DISK_SIZE=10G
CI_USER=thayne
BRIDGE=vmbr0

# Find SSH public key
SSH_KEY=""
for candidate in ~/.ssh/authorized_keys ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub; do
    if [[ -f "$candidate" ]]; then
        SSH_KEY="$candidate"
        break
    fi
done
if [[ -z "$SSH_KEY" ]]; then
    echo "Error: No SSH public key found in ~/.ssh/"
    exit 1
fi

echo "=== Creating orcest worker VM ==="
echo "  VMID:    $VMID"
echo "  Name:    $VMNAME"
echo "  Memory:  $((MEMORY / 1024)) GB"
echo "  CPUs:    ${SOCKETS}s x ${CORES}c"
echo "  Disk:    $DISK_SIZE"
echo "  Storage: $STORAGE"
echo "  User:    $CI_USER"
echo "  SSH key: $SSH_KEY"
echo ""

# Download cloud image if not cached
mkdir -p "$IMG_CACHE"
if [[ ! -f "${IMG_CACHE}/${CLOUD_IMG}" ]]; then
    echo "Downloading cloud image to ${IMG_CACHE}/..."
    wget -q --show-progress -O "${IMG_CACHE}/${CLOUD_IMG}" "$CLOUD_URL"
else
    echo "Using cached image: ${IMG_CACHE}/${CLOUD_IMG}"
fi

# Create VM
echo "Creating VM ${VMID}..."
qm create "$VMID" \
    --name "$VMNAME" \
    --memory "$MEMORY" \
    --cores "$CORES" \
    --sockets "$SOCKETS" \
    --net0 "virtio,bridge=${BRIDGE}" \
    --agent enabled=1

# Import disk
echo "Importing disk..."
qm importdisk "$VMID" "${IMG_CACHE}/${CLOUD_IMG}" "$STORAGE" --format qcow2

# Attach disk + cloud-init drive
echo "Configuring VM..."
qm set "$VMID" --scsihw virtio-scsi-pci --scsi0 "${STORAGE}:vm-${VMID}-disk-0"
qm set "$VMID" --ide2 "${STORAGE}:cloudinit"
qm set "$VMID" --boot order=scsi0

# Resize disk
echo "Resizing disk to ${DISK_SIZE}..."
qm disk resize "$VMID" scsi0 "$DISK_SIZE"

# Cloud-init config
echo "Configuring cloud-init..."
qm set "$VMID" --ciuser "$CI_USER"
qm set "$VMID" --sshkeys "$SSH_KEY"
qm set "$VMID" --ipconfig0 ip=dhcp

# Start
echo "Starting VM..."
qm start "$VMID"

echo ""
echo "=== VM ${VMNAME} (${VMID}) created and starting ==="
echo ""
echo "Wait ~30s for boot, then:"
echo "  1. Get the IP:  qm guest exec ${VMID} -- ip -4 addr show"
echo "     Or check your DHCP leases / Proxmox summary page"
echo "  2. Provision:   orcest provision <ip> --user ${CI_USER}"
