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
  sensitive = true
}

variable "workers" {
  description = "Map of worker VMs keyed by project-name + index."
  type = map(object({
    vm_id              = number
    project_name       = string
    memory             = number
    cores              = number
    disk_size          = number # in GB
    cloud_init_content = string
  }))
  default   = {}
  sensitive = true
}
