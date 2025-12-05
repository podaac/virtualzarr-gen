#!/bin/bash
# Environment Variables must be passed into the docker run command

[[ -z "${START_DATE}" ]] && startDate='' || startDate="${START_DATE}"
[[ -z "${END_DATE}" ]] && endDate='' || endDate="${END_DATE}"
[[ -z "${CPU_COUNT}" ]] && cpuCount=16 || cpuCount="${CPU_COUNT}"
[[ -z "${MEMORY_LIMIT}" ]] && memoryLimit="12GB" || memoryLimit="${MEMORY_LIMIT}"
[[ -z "${BATCH_SIZE}" ]] && batchSize=48 || batchSize="${BATCH_SIZE}"

# Retrieve EDL credentials from AWS SSM
export edl_username=$(aws ssm get-parameter --with-decryption --name $SSM_EDL_USERNAME | jq .Parameter.Value --raw-output)
export edl_password=$(aws ssm get-parameter --with-decryption --name $SSM_EDL_PASSWORD | jq .Parameter.Value --raw-output)

# Set Earthdata environment variables
export EARTHDATA_USERNAME="$edl_username"
export EARTHDATA_PASSWORD="$edl_password"

cmd="generate-vds-s3 --collection $COLLECTION --loadable-coord-vars $LOADABLE_VARS --cpu-count $cpuCount --memory-limit $memoryLimit --batch-size $batchSize"
if [[ -n "$startDate" ]]; then
  cmd="$cmd --start-date $startDate"
fi
if [[ -n "$endDate" ]]; then
  cmd="$cmd --end-date $endDate"
fi

# Run the main CLI command
eval $cmd

# Replace s3:// with https://archive.podaac.earthdata.nasa.gov/ in any *_virtual_s3.json file and create *_virtual_https.json
for s3_file in *_virtual_s3.json; do
  https_file="${s3_file/_virtual_s3.json/_virtual_https.json}"
  sed 's#s3://#https://archive.podaac.earthdata.nasa.gov/#g' "$s3_file" > "$https_file"
done

# Upload output files to S3
aws s3 sync . s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/ --exclude "*" --include "*virtual_https.json"
aws s3 sync . s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/ --exclude "*" --include "*virtual_s3.json"
