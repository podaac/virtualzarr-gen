variable "app_name" {
    default = "virtualzarr-gen"
    type = string
}

variable "app_version" {
    type = string
}

variable "default_tags" {
    type = map(string)
    default = {}
}

variable "stage" {
    type = string
}

variable "output_bucket" {
    type = list(string)
}

variable "region" {
    default = "us-west-2"
    type = string
}

variable "ami_id_ssm_name" {
    default = "/ngap/amis/image_id_ecs_al2023_x86"
    description = "Name of the SSM Parameter that contains the NGAP approved ECS AMI ID."
}

variable "image_name" {
  description = "ECS container image name"
  type        = string
  default     = "ghcr.io/podaac/virtualzarr-gen:main"
}

variable "append_lambda_max_concurrency" {
  description = "Max concurrent Lambda invocations for the append queue (one per collection)"
  type        = number
  default     = 10
}

variable "cnm_sns_topic_arn" {
  description = "ARN of the SNS topic that receives CNM-R messages. Leave empty to skip subscription."
  type        = string
  default     = ""
}

variable "cnm_collection_allowlist" {
  description = "Comma-separated list of collection short names to process. Empty = all collections."
  type        = string
  default     = ""
}

variable "ssm_edl_username_name" {
  description = "Name of the SSM parameter holding the Earthdata (EDL) username."
  type        = string
  default     = ""
}

variable "ssm_edl_password_name" {
  description = "Name of the SSM parameter holding the Earthdata (EDL) password."
  type        = string
  default     = ""
}

variable "ssm_edl_token_name" {
  description = "Name of the SSM parameter holding an Earthdata (EDL) token. Takes precedence over username/password if set."
  type        = string
  default     = ""
}

# Direct EDL credentials (temporary alternative to SSM). Set via tfvars or
# TF_VAR_edl_* env vars. If provided, the append Lambda uses these directly.
# NOTE: these end up in terraform state in plaintext; prefer SSM for anything
# long-lived.
variable "edl_username" {
  description = "Earthdata (EDL) username set directly on the append Lambda."
  type        = string
  default     = ""
  sensitive   = true
}

variable "edl_password" {
  description = "Earthdata (EDL) password set directly on the append Lambda."
  type        = string
  default     = ""
  sensitive   = true
}

variable "edl_token" {
  description = "Earthdata (EDL) token set directly on the append Lambda. Takes precedence over username/password."
  type        = string
  default     = ""
  sensitive   = true
}
