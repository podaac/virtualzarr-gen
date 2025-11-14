
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
    type = string
}

variable "region" {
    default = "us-west-2"
    type = string
}

variable "ami_id_ssm_name" {
    default = "/ngap/amis/image_id_ecs_al2023_x86"
    description = "Name of the SSM Parameter that contains the NGAP approved ECS AMI ID."
}

/*
variable "container_image" {
    type = string
    default = "ghcr.io/podaac/swodlr-api"
}

variable "container_image_tag" {
    type = string
}
*/