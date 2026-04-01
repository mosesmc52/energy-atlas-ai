output "droplet_ipv4" {
  description = "Public IPv4 address of the energy-atlas-ai droplet"
  value       = digitalocean_droplet.energy_atlas_ai.ipv4_address
}

output "ssh_command" {
  description = "SSH command to connect to the energy-atlas-ai droplet"
  value       = "ssh root@${digitalocean_droplet.energy_atlas_ai.ipv4_address}"
}
