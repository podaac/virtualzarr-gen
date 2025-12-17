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

if [[ "$COLLECTION" == *"L2"* ]]; then
  # Use papermill for L2 collections
  cmd="papermill vds_basic_L2_dummytime_prod.ipynb output.ipynb --log-output -p shortname $COLLECTION"
else
  # Use generate-vds-s3 for other collections
  cmd="generate-vds-s3 --collection $COLLECTION --loadable-coord-vars $LOADABLE_VARS --cpu-count $cpuCount --memory-limit $memoryLimit --batch-size $batchSize"
  if [[ -n "$startDate" ]]; then
    cmd="$cmd --start-date $startDate"
  fi
  if [[ -n "$endDate" ]]; then
    cmd="$cmd --end-date $endDate"
  fi
fi

# Run the main CLI command
eval $cmd

ls -al

# Replace s3:// with https://archive.podaac.earthdata.nasa.gov/ in any *virtual*.json file and create *_virtual_https.json
for s3_file in *virtual*.json; do
  https_file="${s3_file%.json}_https.json"
  sed 's#s3://#https://archive.podaac.earthdata.nasa.gov/#g' "$s3_file" > "$https_file"
done

ls -al

# Upload output files to S3
aws s3 sync . s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/ --exclude "*" --include "*virtual*.json"
