#!/bin/bash
set -euo pipefail  # Exit on error, undefined variables, pipe failures

# Environment Variables must be passed into the docker run command
[[ -z "${START_DATE}" ]] && startDate='' || startDate="${START_DATE}"
[[ -z "${END_DATE}" ]] && endDate='' || endDate="${END_DATE}"
[[ -z "${CPU_COUNT}" ]] && cpuCount=16 || cpuCount="${CPU_COUNT}"
[[ -z "${MEMORY_LIMIT}" ]] && memoryLimit="12GB" || memoryLimit="${MEMORY_LIMIT}"
[[ -z "${BATCH_SIZE}" ]] && batchSize=48 || batchSize="${BATCH_SIZE}"

# Retrieve EDL credentials from AWS SSM
edl_username=$(aws ssm get-parameter --with-decryption --name "$SSM_EDL_USERNAME" | jq .Parameter.Value --raw-output) || {
  echo "Failed to retrieve EDL username" >&2
  exit 1
}
edl_password=$(aws ssm get-parameter --with-decryption --name "$SSM_EDL_PASSWORD" | jq .Parameter.Value --raw-output) || {
  echo "Failed to retrieve EDL password" >&2
  exit 1
}

# Set Earthdata environment variables
export EARTHDATA_USERNAME="$edl_username"
export EARTHDATA_PASSWORD="$edl_password"

if [[ "$COLLECTION" == *"L2"* ]]; then
  # Use papermill for L2 collections
  papermill vds_basic_L2_dummytime_prod.ipynb output.ipynb --log-output -p shortname "$COLLECTION"
else
  # Use generate-vds-s3 for other collections
  cmd_args=(
    --collection "$COLLECTION"
    --loadable-coord-vars "$LOADABLE_VARS"
    --cpu-count "$cpuCount"
    --memory-limit "$memoryLimit"
    --batch-size "$batchSize"
  )
  
  if [[ -n "$startDate" ]]; then
    cmd_args+=(--start-date "$startDate")
  fi
  
  if [[ -n "$endDate" ]]; then
    cmd_args+=(--end-date "$endDate")
  fi
  
  generate-vds-s3 "${cmd_args[@]}"

fi

# Upload output files to S3
aws s3 sync . "s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/" --exclude "*" --include "*virtual*.json"

# Translate all *_virtual_s3.json files to *_virtual_https.json using translate-s3-https
for s3file in *_virtual_s3.json; do
  if [[ -f "$s3file" ]]; then
    httpsfile="${s3file/_virtual_s3.json/_virtual_https.json}"
    translate-s3-https "$s3file" "$httpsfile"
  fi
done

# Upload output files to S3
aws s3 sync . "s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/" --exclude "*" --include "*virtual*.json"