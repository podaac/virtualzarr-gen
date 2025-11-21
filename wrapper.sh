#!/bin/bash
# Environment Variables must be passed into the docker run command

[[ -z "${START_DATE}" ]] && startDate='' || startDate="${START_DATE}"
[[ -z "${END_DATE}" ]] && endDate='' || endDate="${END_DATE}"

# Retrieve EDL credentials from AWS SSM
export edl_username=$(aws ssm get-parameter --with-decryption --name $SSM_EDL_USERNAME | jq .Parameter.Value --raw-output)
export edl_password=$(aws ssm get-parameter --with-decryption --name $SSM_EDL_PASSWORD | jq .Parameter.Value --raw-output)

# Set Earthdata environment variables
export EARTHDATA_USERNAME="$edl_username"
export EARTHDATA_PASSWORD="$edl_password"

cmd="python3 generate_vds_s3.py --collection $COLLECTION --loadable-coord-vars $LOADABLE_VARS"
if [[ -n "$startDate" ]]; then
  cmd="$cmd --start-date $startDate"
fi
if [[ -n "$endDate" ]]; then
  cmd="$cmd --end-date $endDate"
fi

eval $cmd

# Upload output files to S3
aws s3 sync . s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/ --exclude "*" --include "*virtual_s3.json" --include "output.ipynb" --exclude ".ipynb_checkpoints"