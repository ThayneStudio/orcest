output "orchestrator_ip" {
  description = "IP address of the orchestrator VM."
  # Index [1] = second NIC (first is loopback at [0]), [0] = first address on that NIC.
  # Standard ordering for the bpg/proxmox provider with a single virtio NIC.
  value = try(proxmox_virtual_environment_vm.orchestrator.ipv4_addresses[1][0], null)
}
