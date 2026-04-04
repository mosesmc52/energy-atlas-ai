locals {
  # Keep only letters+digits, join with "-" for hostname-safe names
  droplet_slug = lower(join("-", regexall("[0-9A-Za-z]+", var.name)))

  # Keep only letters+digits, join with "" for volume-safe names (alphanumeric only)
  volume_slug = lower(join("", regexall("[0-9A-Za-z]+", var.name)))

  # Optional: cap lengths to be safe (DO is picky sometimes)
  droplet_name = substr("${local.droplet_slug}-energy-atlas-ai", 0, 63)
  volume_name  = substr("${local.volume_slug}artifacts", 0, 32)
}




resource "digitalocean_droplet" "energy_atlas_ai" {
  name   = local.droplet_name
  region = var.region
  size   = coalesce(var.size, var.droplet_size)
  image  = var.image
  ipv6   = var.enable_ipv6

  ssh_keys = [var.ssh_key_fingerprint]
  tags     = ["energy-atlas-ai", "ephemeral"]

  user_data = templatefile("${path.module}/cloud-init.yaml", {
    REPO_ROOT        = var.repo_root
    ADMIN_USER       = var.admin_user
    timezone         = var.timezone
    ZILLA_SSH_PUBKEY = chomp(file(pathexpand(var.zilla_pubkey_path)))
  })

}




# Firewall: SSH + HTTP + HTTPS
resource "digitalocean_firewall" "runner_fw" {
  name = "${local.droplet_slug}-fw"

  droplet_ids = [digitalocean_droplet.energy_atlas_ai.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}


# Optional block storage volume for artifacts
resource "digitalocean_volume" "artifacts" {
  count  = var.create_volume ? 1 : 0
  name   = local.volume_name
  region = var.region
  size   = var.volume_size_gb
}



resource "digitalocean_volume_attachment" "attach_artifacts" {
  count      = var.create_volume ? 1 : 0
  droplet_id = digitalocean_droplet.energy_atlas_ai.id
  volume_id  = digitalocean_volume.artifacts[0].id
}
