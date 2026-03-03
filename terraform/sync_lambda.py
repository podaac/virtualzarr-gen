import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

# Defaults
DEFAULT_SOURCE_BUCKET = "podaac-ops-services-cloud-optimizer"
DEFAULT_SOURCE_PREFIX =  "virtual_collections/"
DEFAULT_DEST_BUCKET = "podaac-uat-cumulus-public"
DEFAULT_DEST_PREFIX = "virtual_collections/"

def handler(event, context):
    """
    Lambda handler for S3 sync/copy/delete operations.

    Event parameters:
    - source_bucket: (str) Source S3 bucket name. Default: podaac-ops-services-cloud-optimizer
    - source_prefix: (str) Source S3 prefix (folder). Default: virtual_collections/
    - dest_bucket: (str) Destination S3 bucket name. Default: podaac-uat-cumulus-public
    - dest_prefix: (str) Destination S3 prefix (folder). Default: virtual_collections/
    - mode: (str) Operation mode. One of:
        * "copy" (default): Copy all files from source to destination, skipping files that are the same unless ignore_is_same is true.
        * "sync": Make destination exactly match source (copy missing/changed files, delete extras in destination).
        * "upload_folder": Only copy files from a specific folder (requires 'folder').
        * "delete_folder": Delete all files in a specific folder in destination (requires 'folder').
    - folder: (str) Folder name under virtual_collections/ for upload_folder or delete_folder modes.
    - ignore_is_same: (bool) If true, always copy files even if they are the same (only for copy/upload_folder modes).

    Returns a dict with statusCode and body containing operation details.
    """
    source_bucket = event.get("source_bucket", DEFAULT_SOURCE_BUCKET)
    source_prefix = event.get("source_prefix", DEFAULT_SOURCE_PREFIX)
    dest_bucket   = event.get("dest_bucket", DEFAULT_DEST_BUCKET)
    dest_prefix   = event.get("dest_prefix", DEFAULT_DEST_PREFIX)
    mode          = event.get("mode", "copy")
    folder        = event.get("folder")

    copied, skipped, deleted = 0, 0, 0

    # Adjust prefixes if a specific folder is targeted
    if mode in ["upload_folder", "delete_folder"]:
        if not folder:
            return {"statusCode": 400, "body": f"Missing 'folder' for {mode} mode"}
        
        # Safely append the folder to the prefixes
        source_prefix = f"{source_prefix.rstrip('/')}/{folder}/"
        dest_prefix = f"{dest_prefix.rstrip('/')}/{folder}/"

    if mode in ["copy", "upload_folder"]:
        ignore_is_same = event.get("ignore_is_same", False)
        copied, skipped, _ = _process_copy(source_bucket, source_prefix, dest_bucket, dest_prefix, ignore_is_same=ignore_is_same)
        
    elif mode == "sync":
        copied, skipped, src_keys = _process_copy(source_bucket, source_prefix, dest_bucket, dest_prefix)
        deleted = _sync_deletes(dest_bucket, dest_prefix, src_keys)
        
    elif mode == "delete_folder":
        deleted = _batch_delete(dest_bucket, dest_prefix)
        
    else:
        return {"statusCode": 400, "body": f"Unknown mode: {mode}"}

    return {
        "statusCode": 200,
        "body": {
            "copied": copied,
            "skipped": skipped,
            "deleted": deleted,
            "source_bucket": source_bucket,
            "source_prefix": source_prefix,
            "dest_bucket": dest_bucket,
            "dest_prefix": dest_prefix,
            "mode": mode,
            "folder": folder
        }
    }


def _process_copy(source_bucket, source_prefix, dest_bucket, dest_prefix, ignore_is_same=False):
    """Core logic to paginate, compare, and copy objects. Returns counts and expected destination keys."""
    copied = 0
    skipped = 0
    expected_dest_keys = set()
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
        for obj in page.get("Contents", []):
            source_key = obj["Key"]
            if source_key.endswith("/"):
                continue

            relative_key = source_key[len(source_prefix):]
            dest_key = f"{dest_prefix}{relative_key}"
            expected_dest_keys.add(dest_key)

            if not ignore_is_same and object_is_same(source_bucket, source_key, dest_bucket, dest_key):
                skipped += 1
                continue

            print(f"Copied s3://{source_bucket}/{source_key} -> s3://{dest_bucket}/{dest_key}")
            copy_object(source_bucket, source_key, dest_bucket, dest_key)
            copied += 1
            
    return copied, skipped, expected_dest_keys


def _sync_deletes(dest_bucket, dest_prefix, valid_keys):
    """Finds files in destination that aren't in the valid_keys set and deletes them in bulk."""
    keys_to_delete = []
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=dest_bucket, Prefix=dest_prefix):
        for obj in page.get("Contents", []):
            dest_key = obj["Key"]
            if dest_key.endswith("/") or dest_key in valid_keys:
                continue
            keys_to_delete.append({"Key": dest_key})

    return _execute_batch_delete(dest_bucket, keys_to_delete)


def _batch_delete(bucket, prefix):
    """Gathers all objects under a prefix and deletes them in bulk."""
    keys_to_delete = []
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):
                keys_to_delete.append({"Key": obj["Key"]})

    return _execute_batch_delete(bucket, keys_to_delete)


def _execute_batch_delete(bucket, objects_to_delete):
    """Helper to perform the actual S3 batch delete operation."""
    if not objects_to_delete:
        return 0
        
    # Note: S3 delete_objects has a limit of 1000 keys per request.
    # The current implementation assumes fewer than 1000 items to delete at a time.
    print(f"Deleting {len(objects_to_delete)} objects from {bucket}")
    s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": objects_to_delete, "Quiet": True}
    )
    return len(objects_to_delete)


def object_is_same(source_bucket, source_key, dest_bucket, dest_key):
    """Check if object already exists, has the same size, and same checksum (custom metadata or ETag)"""
    try:
        src = s3.head_object(Bucket=source_bucket, Key=source_key)
        dst = s3.head_object(Bucket=dest_bucket, Key=dest_key)

        if src["ContentLength"] != dst["ContentLength"]: 
            return False
            
        src_etag = src.get("ETag")
        dst_etag = dst.get("ETag")
        
        dst_meta_source_etag = dst.get("Metadata", {}).get("source-etag")
        
        if dst_meta_source_etag:
            if src_etag != dst_meta_source_etag: 
                return False
        else:
            if src_etag != dst_etag: 
                return False
                
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def copy_object(source_bucket, source_key, dest_bucket, dest_key):
    """Copy object from source to destination and stamp the source ETag into custom metadata"""
    source_head = s3.head_object(Bucket=source_bucket, Key=source_key)
    
    metadata = source_head.get("Metadata", {})
    metadata["source-etag"] = source_head.get("ETag")
    
    copy_args = {
        "Bucket": dest_bucket,
        "Key": dest_key,
        "CopySource": {"Bucket": source_bucket, "Key": source_key},
        "ACL": "bucket-owner-full-control",
        "MetadataDirective": "REPLACE", # Tells S3 we are writing new metadata
        "Metadata": metadata
    }
    
    if "ContentType" in source_head: copy_args["ContentType"] = source_head["ContentType"]
    if "CacheControl" in source_head: copy_args["CacheControl"] = source_head["CacheControl"]
    if "ContentDisposition" in source_head: copy_args["ContentDisposition"] = source_head["ContentDisposition"]
    if "ContentEncoding" in source_head: copy_args["ContentEncoding"] = source_head["ContentEncoding"]
    if "ContentLanguage" in source_head: copy_args["ContentLanguage"] = source_head["ContentLanguage"]
    
    s3.copy_object(**copy_args)
