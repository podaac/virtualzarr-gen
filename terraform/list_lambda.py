import boto3
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime, timezone
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment

s3 = boto3.client("s3")

# Defaults
DEFAULT_PREFIX = "virtual_collections/"
DEFAULT_OUTPUT_BUCKET = "podaac-ops-services-cloud-optimizer"
DEFAULT_OUTPUT_PREFIX = "virtual_collections_reports/"

def create_xlxs(results, file_name):
    # 1. Process Data
    rows = []
    for result in results:
        bucket = result['bucket']
        for folder in result['folders']:
            rows.append({'Collection': folder, 'Bucket': bucket, 'Status': '✅ Yes'})

    df = pd.DataFrame(rows)
    inventory = df.pivot(index='Collection', columns='Bucket', values='Status').fillna('❌ No')

    # 2. Create Excel with openpyxl
    writer = pd.ExcelWriter(file_name, engine='openpyxl')
    inventory.to_excel(writer, sheet_name='Cloud Inventory')

    # 3. Access the openpyxl worksheet object
    worksheet = writer.book['Cloud Inventory']

    # Auto-adjust the first column (The folder names - Column A)
    max_folder_len = inventory.index.astype(str).str.len().max()
    worksheet.column_dimensions['A'].width = max_folder_len + 5

    # Auto-adjust the bucket columns (Starting from Column B)
    for i, col in enumerate(inventory.columns, start=2):
        col_letter = get_column_letter(i)
        column_len = max(len(str(col)), 15)  # Ensure headers aren't cut off
        worksheet.column_dimensions[col_letter].width = column_len + 2
        
        # Optional: Center the ✅ and ❌ for better visuals
        for cell in worksheet[col_letter]:
            cell.alignment = Alignment(horizontal='center')

    writer.close()
    return file_name


def lambda_handler(event, context):
    """
    Lambda handler to list top-level folders under a prefix for one or more buckets.

    Event parameters:
    - buckets: (list[str]) Required. List of S3 bucket names to scan.
    - prefix: (str) Prefix to scan. Default: virtual_collections/
    - output_bucket: (str) Destination bucket for report. Default: podaac-ops-services-cloud-optimizer
    - output_prefix: (str) Prefix in output bucket. Default: virtual_collections_reports/
    - output_key: (str) Optional full object key. If set, overrides output_prefix.

    Returns statusCode and body with the uploaded S3 key.
    """
    buckets = [
        "podaac-ops-services-cloud-optimizer",
        "podaac-uat-cumulus-public",
        "podaac-ops-cumulus-public"
    ]    
    prefix = event.get("prefix", DEFAULT_PREFIX)
    output_bucket = event.get("output_bucket", DEFAULT_OUTPUT_BUCKET)
    output_prefix = event.get("output_prefix", DEFAULT_OUTPUT_PREFIX)
    output_key = event.get("output_key")

    if not buckets or not isinstance(buckets, list):
        return {"statusCode": 400, "body": "Missing or invalid 'buckets' list"}

    results = []
    errors = []

    for bucket in buckets:
        try:
            folders = _list_top_level_folders(bucket, prefix)
            results.append({"bucket": bucket, "folders": folders})
        except ClientError as e:
            errors.append({"bucket": bucket, "error": str(e)})

    if not output_key:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_key = f"{output_prefix.rstrip('/')}/virtual_collections_{ts}.xlsx"

    file_name = create_xlxs(results, "/tmp/podaac_inventory.xlsx")
    s3.upload_file(file_name, output_bucket, output_key)

    return {
        "statusCode": 200,
        "body": {
            "output_bucket": output_bucket,
            "output_key": output_key,
            "errors": errors,
        },
    }


def _list_top_level_folders(bucket, prefix):
    """Return top-level folder names directly under the prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    folders = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            full_prefix = common.get("Prefix", "")
            if not full_prefix.startswith(prefix):
                continue
            folder = full_prefix[len(prefix):].rstrip("/")
            if folder:
                folders.append(folder)

    return sorted(set(folders))
