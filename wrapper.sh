#!/bin/bash
# Environment Variables must be passed into the docker run command

[[ -z "${START_DATE}" ]] && startDate='' || startDate="${START_DATE}"
[[ -z "${END_DATE}" ]] && endDate='' || endDate="${END_DATE}"

# Retrieve EDL credentials from AWS SSM
export edl_username=$(aws ssm get-parameter --with-decryption --name $SSM_EDL_USERNAME | jq .Parameter.Value --raw-output)
export edl_password=$(aws ssm get-parameter --with-decryption --name $SSM_EDL_PASSWORD | jq .Parameter.Value --raw-output)

cat > ~/.netrc <<EOF
machine urs.earthdata.nasa.gov
  login $edl_username
  password $edl_password
EOF

cmd="python3 generate_vds_clean.py --collection $COLLECTION --loadable-coord-vars $LOADABLE_VARS"
if [[ -n "$startDate" ]]; then
  cmd="$cmd --start-date $startDate"
fi
if [[ -n "$endDate" ]]; then
  cmd="$cmd --end-date $endDate"
fi

eval $cmd

# Upload output files to S3
aws s3 sync . s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/ --exclude "*" --include "*virtual_https.json" --include "output.ipynb" --exclude ".ipynb_checkpoints"