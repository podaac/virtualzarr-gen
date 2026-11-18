locals {
  
  name    = var.app_name
  environment = var.stage

  resource_prefix = "service-${local.name}-${local.environment}"

  # Bucket the icechunk stores are written to (under virtual_collections/).
  # uat/ops write straight into the Cumulus public bucket; any other stage
  # (e.g. sit) falls back to the first entry of var.output_bucket. Set
  # var.store_bucket to override the default with a custom bucket.
  store_bucket = var.store_bucket != "" ? var.store_bucket : lookup({
    uat = "podaac-uat-cumulus-public"
    ops = "podaac-ops-cumulus-public"
  }, var.stage, var.output_bucket[0])


  default_tags = length(var.default_tags) == 0 ? {
    team = "TVA"
    application = local.resource_prefix
    version = var.app_version
    Environment = local.environment
  } : var.default_tags
}