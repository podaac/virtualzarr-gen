"""
Transform CNM-R (Cloud Notification Mechanism Response) messages from SNS
into granule append messages for the SQS FIFO queue.

CNM-R input (from Cumulus via SNS):
    {
        "collection": "MUR25-JPL-L4-GLOB-v04.2",
        "response": {"status": "SUCCESS", ...},
        "product": {
            "files": [
                {"type": "data", "uri": "s3://bucket/path/file.nc", ...},
                {"type": "metadata", "uri": "...", ...}
            ]
        }
    }

SQS output:
    {
        "collection": "MUR25-JPL-L4-GLOB-v04.2",
        "granules": ["s3://bucket/path/file.nc"]
    }
"""

import json
import logging
import os
import re
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")

QUEUE_URL = os.environ.get("APPEND_QUEUE_URL", "")

COLLECTION_ALLOWLIST = os.environ.get("COLLECTION_ALLOWLIST", "")


def is_collection_allowed(collection):
    if not COLLECTION_ALLOWLIST:
        return True
    allowed = [c.strip() for c in COLLECTION_ALLOWLIST.split(",") if c.strip()]
    return collection in allowed


def https_to_s3(uri):
    """Convert HTTPS API Gateway URI to S3 URI.

    Input:  https://host:port/STAGE/bucket-name/key/path/file.nc
    Output: s3://bucket-name/key/path/file.nc

    If the URI is already s3://, return as-is.
    """
    if uri.startswith("s3://"):
        return uri

    parsed = urlparse(uri)
    path_parts = parsed.path.strip("/").split("/")

    # Skip the API stage (e.g. "DEV", "UAT", "OPS")
    if len(path_parts) >= 2 and re.match(r"^[A-Z]+$", path_parts[0]):
        path_parts = path_parts[1:]

    if len(path_parts) < 2:
        logger.warning("Cannot parse URI to S3: %s", uri)
        return None

    bucket = path_parts[0]
    key = "/".join(path_parts[1:])
    return f"s3://{bucket}/{key}"


def extract_data_uris(cnm_message):
    """Extract S3 URIs for data files from a CNM-R message."""
    files = cnm_message.get("product", {}).get("files", [])
    s3_uris = []

    for f in files:
        if f.get("type") != "data":
            continue
        uri = f.get("uri", "")
        s3_uri = https_to_s3(uri)
        if s3_uri:
            s3_uris.append(s3_uri)

    return s3_uris


def handler(event, context):
    if not QUEUE_URL:
        raise ValueError("APPEND_QUEUE_URL environment variable is required")

    records = event.get("Records", [])
    logger.info("Received %d SNS record(s)", len(records))

    sent = 0
    skipped = 0

    for record in records:
        sns_message = record.get("Sns", {}).get("Message", "{}")

        try:
            cnm = json.loads(sns_message)
        except json.JSONDecodeError:
            logger.error("Failed to parse SNS message as JSON")
            skipped += 1
            continue

        collection = cnm.get("collection", "")
        status = cnm.get("response", {}).get("status", "")

        if status != "SUCCESS":
            logger.info("Skipping non-SUCCESS CNM-R: collection=%s status=%s",
                        collection, status)
            skipped += 1
            continue

        if not is_collection_allowed(collection):
            logger.info("Skipping collection not in allowlist: %s", collection)
            skipped += 1
            continue

        granule_uris = extract_data_uris(cnm)
        if not granule_uris:
            logger.warning("No data files found in CNM-R for %s", collection)
            skipped += 1
            continue

        msg_body = json.dumps({
            "collection": collection,
            "granules": granule_uris,
        })

        logger.info("Sending to SQS: collection=%s granules=%d uris=%s",
                     collection, len(granule_uris), granule_uris)

        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=msg_body,
            MessageGroupId=collection,
        )
        sent += 1

    logger.info("Done: sent=%d skipped=%d", sent, skipped)

    return {
        "statusCode": 200,
        "body": json.dumps({"sent": sent, "skipped": skipped}),
    }
