locals {
  
  name    = var.app_name
  environment = var.stage

  resource_prefix = "service-${local.name}-${local.environment}"


  default_tags = length(var.default_tags) == 0 ? {
    team = "TVA"
    application = local.resource_prefix
    version = var.app_version
    Environment = local.environment
  } : var.default_tags
}