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

papermill generate_cloud_optimized_store_https.ipynb output.ipynb --log-output  -p collection $COLLECTION -p loadable_coord_vars $LOADABLE_VARS -p start_date $startDate -p end_date $endDate -p bucket $OUTPUT_BUCKET

# TODO move the copy of the outputfile from notebook to this wrapper.
# TODO, need to allow aws sync for icechunk, parquet directories
aws s3 cp output.ipynb s3://$OUTPUT_BUCKET/virtualcollection/$COLLECTION/output.ipynb
