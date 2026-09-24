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
