variable "proxmox_endpoint" {
  description = "Proxmox API endpoint URL (e.g. https://10.0.0.1:8006)."
  type        = string
}

variable "proxmox_api_token" {
  description = "Proxmox API token in 'ID=SECRET' format."
  type        = string
  sensitive   = true
}

variable "proxmox_node" {
  description = "Proxmox node name to create VMs on."
  type        = string
  default     = "pve"
}

variable "proxmox_storage" {
  description = "Proxmox storage pool for VM disks."
  type        = string
  default     = "local-lvm"
}

variable "orchestrator" {
  description = "Orchestrator VM configuration."
  type = object({
    vm_id              = number
    memory             = number
    cores              = number
    disk_size          = number # in GB
    cloud_init_content = string
  })
  # See comment on var.workers for why this is not marked sensitive.
}

# Workers are managed by the pool manager via Proxmox API, not Terraform.
# The variable is kept (empty) for backward compatibility with existing state.
variable "workers" {
  description = "Unused — kept for Terraform state compatibility."
  type        = map(any)
  default     = {}
}
