#!/usr/bin/env python3
"""
Poll an SQS queue for granule append messages and update icechunk stores.

Expected SQS message body (JSON):
    {
        "collection": "MUR25-JPL-L4-GLOB-v04.2",
        "granules": [
            "s3://podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/file1.nc",
            "s3://podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/file2.nc"
        ]
    }

Usage:
    python sqs_append_granules.py \
        --queue-url https://sqs.us-west-2.amazonaws.com/123456789/my-queue \
        --store-bucket podaac-sit-services-cloud-optimizer

    # Process up to 5 batches then exit
    python sqs_append_granules.py \
        --queue-url https://sqs.us-west-2.amazonaws.com/123456789/my-queue \
        --store-bucket podaac-sit-services-cloud-optimizer \
        --max-batches 5

    # Poll continuously
    python sqs_append_granules.py \
        --queue-url https://sqs.us-west-2.amazonaws.com/123456789/my-queue \
        --store-bucket podaac-sit-services-cloud-optimizer \
        --poll
"""

import argparse
import json
import logging
import sys
import time
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import earthaccess
import icechunk
import numpy as np
import xarray as xr
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser
import virtualizarr as vz

from icechunk_append import read_store_coordinate, build_write_plan, apply_write_plan
from source_url_coord import (
    url_map_from_granules,
    retire_moved_urls,
    reconcile_source_urls,
)

from podaac.collection_config import (
    BUCKET_TO_HOST,
    DEFAULT_HTTPS_HOST,
    get_collection_config,
    get_preprocess_fn,
    get_store_prefix_s3,
)


def open_repo_s3(bucket, prefix, vcc_bucket):
    storage = icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region="us-west-2",
    )
    config = icechunk.Repository.fetch_config(storage)
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=vcc_bucket + "/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )
    return icechunk.Repository.open(storage, config=config)


def build_vds(data_urls, auth, concat_dim="time", data_vars="minimal",
              coords="minimal", preprocess_fn=None):
    parsed_url = urlparse(data_urls[0])
    bucket = parsed_url.netloc
    # Credentials endpoint follows the archive host of the source bucket
    # (e.g. SWOT buckets use archive.swot.podaac.earthdata.nasa.gov).
    creds_host = BUCKET_TO_HOST.get(bucket, DEFAULT_HTTPS_HOST)
    credentials_endpoint = f"https://{creds_host}/s3credentials"

    s3_store = S3Store(
        bucket=bucket,
        region="us-west-2",
        credential_provider=NasaEarthdataCredentialProvider(
            credentials_endpoint, auth=auth.token["access_token"]
        ),
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )
    registry = ObjectStoreRegistry({f"s3://{bucket}": s3_store})

    mfdataset_kwargs = dict(
        urls=data_urls,
        registry=registry,
        parser=HDFParser(),
        decode_times=False,
        parallel="dask",
        combine="nested",
        concat_dim=concat_dim,
        data_vars=data_vars,
        coords=coords,
        compat="override",
        combine_attrs="override",
    )
    if preprocess_fn:
        mfdataset_kwargs["preprocess"] = preprocess_fn

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)
        vds = vz.open_virtual_mfdataset(**mfdataset_kwargs)

    return vds, bucket


def receive_messages(sqs, queue_url, max_messages=10, wait_time=20):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_time,
    )
    return response.get("Messages", [])


def parse_and_group_messages(messages):
    """Group SQS messages by collection. Returns {collection: [(granules, receipt_handle), ...]}."""
    grouped = defaultdict(list)
    errors = []

    for msg in messages:
        try:
            body = json.loads(msg["Body"])
            collection = body["collection"]
            granules = body["granules"]
            if not collection or not granules:
                raise ValueError("Missing collection or granules")
            grouped[collection].append({
                "granules": granules,
                "receipt_handle": msg["ReceiptHandle"],
                "message_id": msg["MessageId"],
            })
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.error("Bad message %s: %s", msg.get("MessageId", "?"), e)
            errors.append(msg)

    return grouped, errors


def _filter_new_along_dim(vds, existing_ds, concat_dim):
    """Return the subset of vds whose concat-dim coordinate values are NOT
    already present in existing_ds.

    Idempotency guard: SQS delivers at-least-once and failed batches are retried,
    so filtering already-present coordinate values makes every append a no-op if
    it has already been applied, preventing duplicate time steps.
    """
    if concat_dim not in vds.coords:
        logging.warning("Concat dim %r is not a coordinate; skipping idempotency filter.", concat_dim)
        return vds

    new_vals = np.asarray(vds[concat_dim].values)
    existing_vals = (
        np.asarray(existing_ds[concat_dim].values)
        if concat_dim in existing_ds.coords
        else np.array([], dtype=new_vals.dtype)
    )
    mask = ~np.isin(new_vals, existing_vals)
    if mask.all():
        return vds
    logging.info(
        "Idempotency filter: %d of %d incoming %s value(s) are new.",
        int(mask.sum()), len(new_vals), concat_dim,
    )
    return vds.isel({concat_dim: np.where(mask)[0]})


def append_to_collection(collection, granule_urls, store_bucket, auth):
    config = get_collection_config(collection)
    concat_dim = config["concat_dim"]
    data_vars = config["data_vars"]
    coords = config["coords"]
    sort = config.get("sort", False)
    preprocess_fn = get_preprocess_fn(config)

    logging.info("[%s] Building VDS for %d granule(s)...", collection, len(granule_urls))
    vds, source_bucket = build_vds(
        granule_urls, auth,
        concat_dim=concat_dim,
        data_vars=data_vars,
        coords=coords,
        preprocess_fn=preprocess_fn,
    )
    logging.info("[%s] New VDS shape: %s", collection, dict(vds.sizes))

    store_prefix = get_store_prefix_s3(collection)
    vcc_bucket = f"s3://{source_bucket}"

    repo = open_repo_s3(store_bucket, store_prefix, vcc_bucket)
    logging.info("[%s] Opened store: s3://%s/%s", collection, store_bucket, store_prefix)

    session = repo.writable_session("main")

    # Map each incoming granule URL to its (encoded) time value, reading the
    # granule with this module's own SWOT-aware build_vds (and the collection's
    # preprocess). The URL is the stable identity across reprocessing; time can
    # change.
    def _build_vds_fn(urls, a, **kw):
        return build_vds(urls, a, preprocess_fn=preprocess_fn, **kw)

    url_map = url_map_from_granules(
        granule_urls, auth, concat_dim=concat_dim,
        build_vds_fn=_build_vds_fn, data_vars=data_vars, coords=coords,
    )

    # If a granule was reprocessed with its time SHIFTED (same URL, new time),
    # drop the old step first so the write becomes a move, not a duplicate. This
    # only matches when the store already carries source_url; otherwise it is a
    # no-op. Runs before read_store_coordinate so the plan sees the removal.
    retired = retire_moved_urls(session, url_map, concat_dim=concat_dim)
    if retired["n_retired"]:
        logging.info("[%s] Retired %d moved granule step(s) at %s=%s",
                     collection, retired["n_retired"], concat_dim, retired["retired"])

    # read_store_coordinate opens the store with decode_times=False so the raw
    # encoded values compare like-for-like with the incoming vds.
    existing = read_store_coordinate(session.store, concat_dim)
    logging.info(
        "[%s] Existing '%s' extent: %d step(s)",
        collection, concat_dim, 0 if existing is None else existing.size,
    )

    plan = build_write_plan(vds, existing, dimension=concat_dim)
    logging.info(
        "[%s] Write plan [%s]: %d new, %d already-present granule(s)",
        collection, plan.mode, plan.n_new, plan.n_region,
    )

    if plan.is_empty and not retired["n_retired"]:
        logging.info("[%s] Nothing to write.", collection)
        return

    if not plan.is_empty:
        apply_write_plan(session, vds, plan)

    # Record/refresh the time -> URL link for everything just written.
    reconcile_source_urls(session, url_map, concat_dim=concat_dim)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    retired_note = f", {retired['n_retired']} moved" if retired["n_retired"] else ""
    commit_msg = (
        f"{plan.mode} {plan.n_new} new, {plan.n_region} in-place{retired_note} "
        f"granule(s) at {timestamp}"
    )
    session.commit(commit_msg)
    logging.info("[%s] Committed: %s", collection, commit_msg)

    verify_session = repo.readonly_session(branch="main")
    verify_ds = xr.open_zarr(verify_session.store, consolidated=False, decode_times=False)
    logging.info("[%s] Verified shape: %s", collection, dict(verify_ds.sizes))
    verify_ds.close()


def delete_messages(sqs, queue_url, receipt_handles):
    for rh in receipt_handles:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=rh)


def process_batch(sqs, queue_url, store_bucket, auth):
    messages = receive_messages(sqs, queue_url)
    if not messages:
        return 0

    logging.info("Received %d message(s) from SQS", len(messages))

    grouped, errors = parse_and_group_messages(messages)

    for collection, entries in grouped.items():
        all_granules = []
        receipt_handles = []
        for entry in entries:
            all_granules.extend(entry["granules"])
            receipt_handles.append(entry["receipt_handle"])

        logging.info("[%s] Processing %d granule(s) from %d message(s)",
                     collection, len(all_granules), len(entries))

        try:
            append_to_collection(collection, all_granules, store_bucket, auth)
            delete_messages(sqs, queue_url, receipt_handles)
            logging.info("[%s] Done — deleted %d message(s)", collection, len(receipt_handles))
        except Exception:
            logging.exception("[%s] Failed to append", collection)

    return len(messages)


def main():
    parser = argparse.ArgumentParser(
        description="Poll SQS for granule append messages and update icechunk stores",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--queue-url", required=True, help="SQS queue URL")
    parser.add_argument("--store-bucket", required=True, help="S3 bucket containing icechunk stores")
    parser.add_argument("--poll", action="store_true", help="Poll continuously (default: process one batch and exit)")
    parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between polls when queue is empty (default: 30)")
    parser.add_argument("--max-batches", type=int, default=None, help="Max number of batches to process then exit")
    parser.add_argument("--cpu-count", type=int, default=8, help="Number of Dask workers")
    parser.add_argument("--memory-limit", default="4GB", help="Memory limit per Dask worker")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    auth = earthaccess.login()
    sqs = boto3.client("sqs", region_name="us-west-2")

    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(
        n_workers=args.cpu_count,
        threads_per_worker=1,
        memory_limit=args.memory_limit,
        silence_logs=logging.ERROR,
    )
    client = Client(cluster)
    logging.info("Dask client: %s", client)

    def _silence(token):
        import warnings, logging, earthaccess as ea, os
        if token:
            os.environ["EARTHDATA_TOKEN"] = token
            ea.login(strategy="environment")
        warnings.filterwarnings("ignore")
        for name in ["distributed", "xarray", "py.warnings", "fsspec", "h5py"]:
            logging.getLogger(name).setLevel(logging.ERROR)

    client.run(_silence, auth.token["access_token"])

    try:
        batches_processed = 0
        while True:
            count = process_batch(sqs, args.queue_url, args.store_bucket, auth)
            batches_processed += 1

            if args.max_batches and batches_processed >= args.max_batches:
                logging.info("Reached max batches (%d), exiting.", args.max_batches)
                break

            if not args.poll:
                break

            if count == 0:
                logging.info("Queue empty, waiting %ds...", args.poll_interval)
                time.sleep(args.poll_interval)

    except KeyboardInterrupt:
        logging.info("Interrupted, shutting down...")
    finally:
        warnings.filterwarnings("ignore")
        logging.disable(logging.CRITICAL)
        try:
            client.shutdown(timeout=30)
        except Exception:
            pass
        try:
            cluster.close(timeout=30)
        except Exception:
            pass
        logging.disable(logging.NOTSET)

    logging.info("Done.")


if __name__ == "__main__":
    main()
