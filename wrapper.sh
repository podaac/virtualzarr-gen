#!/bin/bash
set -euo pipefail  # Exit on error, undefined variables, pipe failures

# Environment Variables must be passed into the docker run command
[[ -z "${START_DATE}" ]] && startDate='' || startDate="${START_DATE}"
[[ -z "${END_DATE}" ]] && endDate='' || endDate="${END_DATE}"
[[ -z "${CPU_COUNT}" ]] && cpuCount=16 || cpuCount="${CPU_COUNT}"
[[ -z "${MEMORY_LIMIT}" ]] && memoryLimit="12GB" || memoryLimit="${MEMORY_LIMIT}"
[[ -z "${BATCH_SIZE}" ]] && batchSize=48 || batchSize="${BATCH_SIZE}"

# Retrieve EDL credentials from AWS SSM (boto3 from the venv; no aws cli / jq in the image)
get_ssm_param() {
  python -c 'import sys, boto3; print(boto3.client("ssm").get_parameter(Name=sys.argv[1], WithDecryption=True)["Parameter"]["Value"])' "$1"
}
edl_username=$(get_ssm_param "$SSM_EDL_USERNAME") || {
  echo "Failed to retrieve EDL username" >&2
  exit 1
}
edl_password=$(get_ssm_param "$SSM_EDL_PASSWORD") || {
  echo "Failed to retrieve EDL password" >&2
  exit 1
}

# Set Earthdata environment variables
export EARTHDATA_USERNAME="$edl_username"
export EARTHDATA_PASSWORD="$edl_password"

# Build command arguments
cmd_args=(
  --collection "$COLLECTION"
  --loadable-coord-vars "$LOADABLE_VARS"
  --cpu-count "$cpuCount"
  --memory-limit "$memoryLimit"
  --batch-size "$batchSize"
)

if [[ "$COLLECTION" == *"L2"* ]]; then
  cmd_args+=(--level-2-data)
fi

if [[ -n "$startDate" ]]; then
  cmd_args+=(--start-date "$startDate")
fi
if [[ -n "$endDate" ]]; then
  cmd_args+=(--end-date "$endDate")
fi

exec generate-vds-icechunk "${cmd_args[@]}" --output-bucket "$OUTPUT_BUCKET"
