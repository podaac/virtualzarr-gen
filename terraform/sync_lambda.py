import os
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

# Defaults (used if no input provided)
DEFAULT_SOURCE_BUCKET = "my-source-bucket"
DEFAULT_SOURCE_PREFIX = "data/incoming/"
DEFAULT_DEST_BUCKET = "my-dest-bucket"
DEFAULT_DEST_PREFIX = "data/synced/"

def handler(event, context):
    """
    Event example:
    {
        "source_bucket": "bucket1",
        "source_prefix": "some/path/",
        "dest_bucket": "bucket2",
        "dest_prefix": "another/path/"
    }
    """
    # Get values from event or use defaults
    source_bucket = event.get("source_bucket", DEFAULT_SOURCE_BUCKET)
    source_prefix = event.get("source_prefix", DEFAULT_SOURCE_PREFIX)
    dest_bucket   = event.get("dest_bucket", DEFAULT_DEST_BUCKET)
    dest_prefix   = event.get("dest_prefix", DEFAULT_DEST_PREFIX)

    paginator = s3.get_paginator("list_objects_v2")

    copied = 0
    skipped = 0

    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
        for obj in page.get("Contents", []):
            source_key = obj["Key"]

            # Skip "directory" keys
            if source_key.endswith("/"):
                continue

            relative_key = source_key[len(source_prefix):]
            dest_key = f"{dest_prefix}{relative_key}"

            if object_is_same(source_bucket, source_key, dest_bucket, dest_key):
                skipped += 1
                continue

            copy_object(source_bucket, source_key, dest_bucket, dest_key)
            copied += 1

    return {
        "statusCode": 200,
        "body": {
            "copied": copied,
            "skipped": skipped,
            "source_bucket": source_bucket,
            "source_prefix": source_prefix,
            "dest_bucket": dest_bucket,
            "dest_prefix": dest_prefix
        }
    }


def object_is_same(source_bucket, source_key, dest_bucket, dest_key):
    """Check if object already exists and is identical (by ETag)"""
    try:
        src = s3.head_object(Bucket=source_bucket, Key=source_key)
        dst = s3.head_object(Bucket=dest_bucket, Key=dest_key)
        return src["ETag"] == dst["ETag"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def copy_object(source_bucket, source_key, dest_bucket, dest_key):
    """Copy object from source to destination"""
    s3.copy_object(
        Bucket=dest_bucket,
        Key=dest_key,
        CopySource={"Bucket": source_bucket, "Key": source_key}
    )
