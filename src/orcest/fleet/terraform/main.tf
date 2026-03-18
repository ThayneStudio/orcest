# ── Cloud image ─────────────────────────────────────────────
#
# Download the Ubuntu 24.04 (Noble) cloud image once and reference it in
# all VM boot disks.  The bpg/proxmox provider manages the lifecycle of
# the downloaded file on the Proxmox storage.

resource "proxmox_virtual_environment_download_file" "ubuntu_cloud_image" {
  content_type = "iso"
  datastore_id = "local"
  node_name    = var.proxmox_node
  url          = "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
  file_name    = "noble-server-cloudimg-amd64.img"
}

# ── Cloud-init snippets ─────────────────────────────────────
#
# Each VM gets a cloud-init user-data snippet uploaded to the
# "snippets" content type on the target storage.

resource "proxmox_virtual_environment_file" "orchestrator_cloud_init" {
  content_type = "snippets"
  datastore_id = "local"
  node_name    = var.proxmox_node

  source_raw {
    data      = var.orchestrator.cloud_init_content
    file_name = "orcest-orchestrator-ci.yaml"
  }
}

# ── Orchestrator VM ─────────────────────────────────────────

resource "proxmox_virtual_environment_vm" "orchestrator" {
  name      = "orcest-orchestrator"
  node_name = var.proxmox_node
  vm_id     = var.orchestrator.vm_id

  agent {
    enabled = true
  }

  cpu {
    cores = var.orchestrator.cores
    type  = "host"
  }

  memory {
    dedicated = var.orchestrator.memory
  }

  # Boot disk cloned from the cloud image
  disk {
    datastore_id = var.proxmox_storage
    file_id      = proxmox_virtual_environment_download_file.ubuntu_cloud_image.id
    interface    = "scsi0"
    size         = var.orchestrator.disk_size
    discard      = "on"
    ssd          = true
  }

  network_device {
    model  = "virtio"
    bridge = "vmbr0"
  }

  initialization {
    datastore_id = var.proxmox_storage

    ip_config {
      ipv4 {
        address = "dhcp"
      }
    }

    user_data_file_id = proxmox_virtual_environment_file.orchestrator_cloud_init.id
  }

  # Start the VM on creation
  started = true
}

# Worker VMs are managed by the pool manager via Proxmox API,
# not by Terraform. See src/orcest/fleet/pool_manager.py.
