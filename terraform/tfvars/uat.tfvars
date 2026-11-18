app_name        = "virtualzarr-gen"
default_tags    = {}
stage   = "uat"
app_version = "1.0"
output_bucket = ["podaac-uat-services-cloud-optimizer", "simon-vds-staging", "podaac-uat-cumulus-public"]
image_name = "ghcr.io/podaac/virtualzarr-gen:uat"
ssm_cnm_publisher_principals_name = "/service/virtualzarr-gen/cnm-publisher-principals"
