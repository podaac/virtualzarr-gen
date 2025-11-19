#!/bin/bash
# Environment Variables must be passed into the docker run command

[[ -z "${START_DATE}" ]] && startDate='None' || startDate="${START_DATE}"
[[ -z "${END_DATE}" ]] && endDate='None' || endDate="${END_DATE}"

#need to update this 
export edl_username=`aws ssm get-parameter --with-decryption --name $SSM_EDL_USERNAME | jq .Parameter.Value --raw-output`
export edl_password=`aws ssm get-parameter --with-decryption --name $SSM_EDL_PASSWORD | jq .Parameter.Value --raw-output`

cat > ~/.netrc <<EOF
machine urs.earthdata.nasa.gov
  login $edl_username
  password $edl_password
EOF

# papermill generate_cloud_optimized_store_https.ipynb output.ipynb --log-output  -p collection $COLLECTION -p loadable_coord_vars $LOADABLE_VARS -p start_date $startDate -p end_date $endDate -p bucket $OUTPUT_BUCKET
python3 generate_vds_clean.py --collection $COLLECTION --loadable-coord-vars $LOADABLE_VARS --start-date $startDate --end-date $endDate

# This call will upload the output.ipynb and then virtual collection (regardless of name) to the output location
aws s3 sync . s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/ --exclude "*" --include "*virtual_https.json" --include "output.ipynb" --exclude ".ipynb_checkpoints"
