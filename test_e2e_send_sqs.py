#!/usr/bin/env python3
"""
Step 2: Send granule append messages to the SQS FIFO queue.

Can send granules one-at-a-time or as a batch. Discovers granules
via earthaccess search, skipping those already in the store.

Usage:
    # Send 3 granules one-per-message
    python test_e2e_send_sqs.py --queue-url $QUEUE_URL --count 3

    # Send 3 granules as one batch message
    python test_e2e_send_sqs.py --queue-url $QUEUE_URL --count 3 --batch

    # Send specific granule URLs
    python test_e2e_send_sqs.py --queue-url $QUEUE_URL \
        --granules s3://bucket/path/file1.nc s3://bucket/path/file2.nc
"""

import argparse
import json
import logging
import uuid

import boto3
import earthaccess
import icechunk
import xarray as xr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
STORE_PREFIX_S3 = "virtual_collections/_test_e2e/MUR25_test.icechunk_v2.s3/"


def get_existing_time_count(store_bucket):
    """Check how many time steps the store currently has."""
    storage = icechunk.s3_storage(
        bucket=store_bucket, prefix=STORE_PREFIX_S3, region="us-west-2",
    )
    config = icechunk.Repository.fetch_config(storage)
    repo = icechunk.Repository.open(storage, config=config)
    session = repo.readonly_session(branch="main")
    ds = xr.open_zarr(session.store, consolidated=False)
    count = ds.sizes["time"]
    ds.close()
    return count


def search_granules(count, skip):
    """Search for granules, returning those after the skip offset."""
    total = skip + count
    results = earthaccess.search_data(
        short_name=COLLECTION,
        provider="POCLOUD",
        cloud_hosted=True,
        count=total,
    )
    urls = [g.data_links(access="direct")[0] for g in results]
    return urls[skip:]


def send_messages(sqs, queue_url, granule_urls, batch_mode, store_prefix=None):
    """Send granule URLs to the SQS FIFO queue."""
    if batch_mode:
        msg = {"collection": COLLECTION, "granules": granule_urls}
        if store_prefix:
            msg["store_prefix"] = store_prefix
        resp = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(msg),
            MessageGroupId=COLLECTION,
            MessageDeduplicationId=str(uuid.uuid4()),
        )
        logging.info("Sent batch message (%d granules): MessageId=%s",
                      len(granule_urls), resp["MessageId"])
    else:
        for url in granule_urls:
            msg = {"collection": COLLECTION, "granules": [url]}
            if store_prefix:
                msg["store_prefix"] = store_prefix
            resp = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(msg),
                MessageGroupId=COLLECTION,
                MessageDeduplicationId=str(uuid.uuid4()),
            )
            logging.info("Sent message: %s → MessageId=%s", url.split("/")[-1], resp["MessageId"])


def main():
    parser = argparse.ArgumentParser(description="Send granule append messages to SQS")
    parser.add_argument("--queue-url", required=True, help="SQS FIFO queue URL")
    parser.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--count", type=int, default=3,
                        help="Number of new granules to send (ignored if --granules is set)")
    parser.add_argument("--granules", nargs="+", help="Specific granule S3 URLs to send")
    parser.add_argument("--batch", action="store_true",
                        help="Send all granules as one SQS message instead of one per granule")
    parser.add_argument("--store-prefix", default=STORE_PREFIX_S3,
                        help="Override store prefix in messages (default: test S3 path)")
    args = parser.parse_args()

    earthaccess.login(strategy="netrc")
    sqs = boto3.client("sqs", region_name="us-west-2")

    existing_count = get_existing_time_count(args.store_bucket)
    logging.info("Store currently has %d time steps", existing_count)

    if args.granules:
        granule_urls = args.granules
    else:
        logging.info("Searching for %d granules to append (skipping first %d)...",
                      args.count, existing_count)
        granule_urls = search_granules(args.count, skip=existing_count)

    if not granule_urls:
        logging.error("No granules found to send.")
        return

    logging.info("Sending %d granule(s) to queue...", len(granule_urls))
    for url in granule_urls:
        logging.info("  %s", url)

    send_messages(sqs, args.queue_url, granule_urls, args.batch,
                  store_prefix=args.store_prefix)

    logging.info("Done. Monitor with: python test_e2e_verify_store.py --watch")


if __name__ == "__main__":
    main()
