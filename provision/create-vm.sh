#!/usr/bin/env bash
# Create an orcest worker VM on Proxmox using a Ubuntu cloud image.
#
# Usage:
#   create-vm.sh <vm-id> <vm-name> [storage]
#
# Examples:
#   create-vm.sh 200 orcest-worker-1
#   create-vm.sh 201 orcest-worker-2 local-zfs
#
# Environment overrides:
#   ORCEST_VM_USER=thayne    ORCEST_VM_MEMORY=16384
#   ORCEST_VM_BRIDGE=vmbr0   ORCEST_VM_CORES=4
#   ORCEST_VM_SOCKETS=2      ORCEST_VM_DISK=10G
#
# Prerequisites:
#   - Run on the Proxmox host (needs qm and wget)
#   - SSH public key at ~/.ssh/id_ed25519.pub or ~/.ssh/id_rsa.pub

set -euo pipefail

# --- Dependencies ---
for cmd in qm wget gpg; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: '${cmd}' not found. This script must be run on a Proxmox host."
        exit 1
    fi
done

# --- Arguments ---
VMID="${1:?Usage: $0 <vm-id> <vm-name> [storage]}"
VMNAME="${2:?Usage: $0 <vm-id> <vm-name> [storage]}"
STORAGE="${3:-local-lvm}"

# Validate inputs
if ! [[ "$VMID" =~ ^[0-9]+$ ]]; then
    echo "Error: VMID must be a positive integer" >&2
    exit 1
fi
if ! [[ "$VMNAME" =~ ^[a-zA-Z0-9._-]+$ ]]; then
    echo "Error: VMNAME contains invalid characters" >&2
    exit 1
fi
if ! [[ "$STORAGE" =~ ^[a-zA-Z0-9._-]+$ ]]; then
    echo "Error: STORAGE contains invalid characters" >&2
    exit 1
fi

# Check for existing VM
if qm status "$VMID" &>/dev/null; then
    echo "Error: VM ${VMID} already exists. Destroy it first with: qm destroy ${VMID} --purge"
    exit 1
fi

# --- Configuration ---
CLOUD_IMG="noble-server-cloudimg-amd64.img"
CLOUD_URL="https://cloud-images.ubuntu.com/noble/current/${CLOUD_IMG}"
IMG_CACHE="/var/cache/cloud-images"

MEMORY="${ORCEST_VM_MEMORY:-16384}"
CORES="${ORCEST_VM_CORES:-4}"
SOCKETS="${ORCEST_VM_SOCKETS:-2}"
DISK_SIZE="${ORCEST_VM_DISK:-10G}"
CI_USER="${ORCEST_VM_USER:-thayne}"
BRIDGE="${ORCEST_VM_BRIDGE:-vmbr0}"
VLAN="${ORCEST_VM_VLAN:-}"

# Find SSH public key (prefer .pub files over authorized_keys)
SSH_KEY=""
for candidate in ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys; do
    if [[ -f "$candidate" ]]; then
        SSH_KEY="$candidate"
        break
    fi
done
if [[ -z "$SSH_KEY" ]]; then
    echo "Error: No SSH public key found in ~/.ssh/"
    exit 1
fi

# --- Cleanup on failure ---
VM_CREATED=false
cleanup() {
    local rc=$?
    if [[ $rc -ne 0 && "$VM_CREATED" == true ]]; then
        echo ""
        echo "ERROR: Script failed. Cleaning up VM ${VMID}..."
        qm stop "$VMID" 2>/dev/null || true
        qm destroy "$VMID" --purge 2>/dev/null || true
    fi
}
trap cleanup EXIT

# --- Summary ---
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

# --- Download cloud image (atomic, with cache) ---
mkdir -p "$IMG_CACHE"
chmod 700 "$IMG_CACHE"
if [[ ! -f "${IMG_CACHE}/${CLOUD_IMG}" ]]; then
    echo "Downloading cloud image to ${IMG_CACHE}/..."
    wget -q --show-progress --max-redirect=0 \
        -O "${IMG_CACHE}/${CLOUD_IMG}.tmp" "$CLOUD_URL"
    mv "${IMG_CACHE}/${CLOUD_IMG}.tmp" "${IMG_CACHE}/${CLOUD_IMG}"
else
    echo "Using cached image: ${IMG_CACHE}/${CLOUD_IMG}"
fi

# --- Import Ubuntu cloud image signing key (if not already present) ---
UBUNTU_SIGNING_KEY="843938DF228D22F7B3742BC0D94AA3F0EFE21092"
if ! gpg --list-keys "$UBUNTU_SIGNING_KEY" &>/dev/null; then
    echo "Importing Ubuntu cloud image signing key ${UBUNTU_SIGNING_KEY}..."
    gpg --keyserver keyserver.ubuntu.com --recv-keys "$UBUNTU_SIGNING_KEY"
fi

# --- Verify cloud image checksum (with GPG signature check) ---
echo "Verifying image checksum..."
wget -q --max-redirect=0 \
    "https://cloud-images.ubuntu.com/noble/current/SHA256SUMS" \
    -O "${IMG_CACHE}/SHA256SUMS"
wget -q --max-redirect=0 \
    "https://cloud-images.ubuntu.com/noble/current/SHA256SUMS.gpg" \
    -O "${IMG_CACHE}/SHA256SUMS.gpg"
gpg --keyid-format long --verify "${IMG_CACHE}/SHA256SUMS.gpg" "${IMG_CACHE}/SHA256SUMS"
(cd "$IMG_CACHE" && sha256sum -c --ignore-missing SHA256SUMS)

# --- Create VM ---
echo "Creating VM ${VMID}..."
qm create "$VMID" \
    --name "$VMNAME" \
    --memory "$MEMORY" \
    --cores "$CORES" \
    --sockets "$SOCKETS" \
    --net0 "virtio,bridge=${BRIDGE}${VLAN:+,tag=${VLAN}}" \
    --serial0 socket \
    --vga serial0 \
    --agent enabled=1
VM_CREATED=true

# Import disk (no --format flag; Proxmox auto-selects raw for LVM, qcow2 for directory)
echo "Importing disk..."
qm importdisk "$VMID" "${IMG_CACHE}/${CLOUD_IMG}" "$STORAGE"
# The imported disk appears as unused0 in the VM config
DISK_REF=$(qm config "$VMID" | grep -oP 'unused0:\s*\K\S+')
echo "  Disk: ${DISK_REF}"

# Attach disk + cloud-init drive
echo "Configuring VM..."
qm set "$VMID" --scsihw virtio-scsi-pci --scsi0 "${DISK_REF}"
qm set "$VMID" --ide2 "${STORAGE}:cloudinit"
qm set "$VMID" --boot order=scsi0

# Resize disk
echo "Resizing disk to ${DISK_SIZE}..."
qm disk resize "$VMID" scsi0 "$DISK_SIZE"

# Cloud-init config
echo "Configuring cloud-init..."
qm set "$VMID" --ciuser "$CI_USER"
qm set "$VMID" --sshkey "$SSH_KEY"
qm set "$VMID" --ipconfig0 ip=dhcp

# Show MAC address for DHCP reservation
MAC=$(qm config "$VMID" | grep -oP 'virtio=\K[A-F0-9:]+' -i)
echo ""
echo "  MAC address: ${MAC}"
echo ""
echo "Set up a static DHCP reservation now if desired."
read -rp "Press Enter to boot the VM (or Ctrl-C to abort)..."

# Start
echo "Starting VM..."
qm start "$VMID"

# --- Wait for IP ---
echo ""
echo "Waiting for VM to get an IP (up to 120s)..."
IP=""
for _ in $(seq 1 24); do
    IP=$(qm guest cmd "$VMID" network-get-interfaces 2>/dev/null \
        | grep -oP '"ip-address"\s*:\s*"\K[0-9.]+' \
        | grep -v '^127\.' | head -1) && break
    sleep 5
done

echo ""
echo "=== VM ${VMNAME} (${VMID}) created ==="
echo ""
if [[ -n "${IP:-}" ]]; then
    echo "  IP: ${IP}"
    echo ""
    echo "From your dev machine, run:"
    echo "  orcest provision ${IP} --user ${CI_USER}"
else
    echo "  Could not detect IP (guest agent may not be installed yet)."
    echo "  Check the Proxmox web UI summary page for the IP, then from your dev machine:"
    echo "  orcest provision <ip> --user ${CI_USER}"
fi
