variable "size" {
  description = "Deprecated: use droplet_size. When set, overrides droplet_size."
  type        = string
  default     = null
}

variable "image" {
  description = "Droplet image slug"
  type        = string
  default     = "ubuntu-22-04-x64"
}

variable "do_token" {
  description = "DigitalOcean API token"
  type        = string
  sensitive   = true
}

variable "region" {
  description = "DigitalOcean region slug (e.g. nyc3)"
  type        = string
  default     = "nyc3"
}

variable "droplet_size" {
  description = "DigitalOcean droplet size slug (e.g. s-4vcpu-16gb)"
  type        = string
  default     = "s-1vcpu-2gb"
}

variable "name" {
  type        = string
  description = "Base name for energy-atlas-ai infrastructure"
}

variable "ssh_key_fingerprint" {
  type        = string
  description = "SSH key fingerprint for droplet access (e.g. 33:43:...)"
}


variable "enable_ipv6" {
  type    = bool
  default = true
}

variable "volume_size_gb" {
  type    = number
  default = 100
}

variable "timezone" {
  type    = string
  default = "America/New_York"
}


variable "repo_root" {
  type    = string
  default = "/opt"
}

variable "create_volume" {
  type    = bool
  default = true
}

variable "zilla_pubkey_path" {
  description = "Path to public SSH key (used for provisioners or metadata)"
  type        = string
}

variable "admin_user" {
  type    = string
  default = "zilla"
}
