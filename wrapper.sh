#!/bin/bash
set -euo pipefail  # Exit on error, undefined variables, pipe failures

# Helper function to sync output files to S3
sync_to_s3() {
  aws s3 sync . "s3://$OUTPUT_BUCKET/virtual_collections/$COLLECTION/" \
    --exclude "*" --include "*virtual*.json" || echo "Primary bucket not found, skipping..."

  if [[ -n "${STAGING_BUCKET}" ]]; then
    aws s3 sync . "s3://${STAGING_BUCKET}/virtual_collections/${COLLECTION}/" \
      --exclude "*" --include "*virtual*.json" || echo "Staging bucket not found, skipping..."
  fi
}

# Helper function to translate S3 JSON files to HTTPS JSON files
translate_all_s3_to_https() {
  for s3file in *_virtual_s3*.json; do
    if [[ -f "$s3file" ]]; then
      httpsfile="${s3file/_virtual_s3.json/_virtual_https.json}"
      translate-s3-https "$s3file" "$httpsfile"
    fi
  done
}

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

sync_icechunk_to_s3() {
  local found=0

  # Upload tar files
  while IFS= read -r tarfile; do
    found=1
    filename=$(basename "$tarfile")
    echo "Uploading tar: $tarfile"
    aws s3 cp "$tarfile" "s3://$OUTPUT_BUCKET/virtual_collections/$COLLECTION/$filename" \
      || echo "Primary bucket upload of $filename failed, skipping..."

    if [[ -n "${STAGING_BUCKET}" ]]; then
      aws s3 cp "$tarfile" "s3://${STAGING_BUCKET}/virtual_collections/${COLLECTION}/$filename" \
        || echo "Staging bucket upload of $filename failed, skipping..."
    fi
  done < <(find / -maxdepth 5 -name "*.icechunk_v2.*.tar" -type f 2>/dev/null)

  # Upload directories
  while IFS= read -r dir; do
    found=1
    dirname=$(basename "$dir")
    echo "Uploading directory: $dir"
    aws s3 sync "$dir" "s3://$OUTPUT_BUCKET/virtual_collections/$COLLECTION/$dirname/" \
      || echo "Primary bucket upload of $dirname failed, skipping..."

    if [[ -n "${STAGING_BUCKET}" ]]; then
      aws s3 sync "$dir" "s3://${STAGING_BUCKET}/virtual_collections/${COLLECTION}/$dirname/" \
        || echo "Staging bucket upload of $dirname failed, skipping..."
    fi
  done < <(find / -maxdepth 5 -type d \( -name "*.icechunk_v2.s3" -o -name "*.icechunk_v2.https" \) 2>/dev/null)

  if [[ "$found" -eq 0 ]]; then
    echo "WARNING: No icechunk tar files or directories found"
  fi
}

if [[ "${ENGINE:-kerchunk}" == "icechunk" ]]; then
  source /opt/venv-icechunk/bin/activate
  generate-vds-icechunk "${cmd_args[@]}" || true
  sync_icechunk_to_s3
else
  source /opt/venv-kerchunk/bin/activate
  generate-vds-s3 "${cmd_args[@]}"
  sync_to_s3
  translate_all_s3_to_https
  sync_to_s3
fi