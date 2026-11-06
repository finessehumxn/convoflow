variable "region" {
  type    = string
  default = "us-east-1"
}

variable "production" {
  description = "false is the small test footprint. true adds private task subnets with NAT, a standby database and larger limits."
  type        = bool
  default     = false
}

variable "image_tag" {
  description = "Image tag to run. The deploy workflow sets this to the commit SHA."
  type        = string
  default     = "bootstrap"
}

variable "alert_email" {
  type = string
}

variable "certificate_arn" {
  description = "ACM certificate for HTTPS. Leave empty only for the test footprint."
  type        = string
  default     = ""
}

variable "github_repo" {
  type    = string
  default = "finessehumxn/convoflow"
}

variable "worker_min" {
  type    = number
  default = 2
}

variable "worker_max" {
  type    = number
  default = 8
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}
