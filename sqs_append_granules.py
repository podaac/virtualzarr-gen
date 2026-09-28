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


BUCKET_TO_HOST = {
    "podaac-swot-ops-cumulus-protected": "archive.swot.podaac.earthdata.nasa.gov",
    "podaac-swot-ops-cumulus-public": "archive.swot.podaac.earthdata.nasa.gov",
}
DEFAULT_HTTPS_HOST = "archive.podaac.earthdata.nasa.gov"

_STANDARD_16_VAR = [
    "sss_smap", "sss_smap_unc", "sss_smap_40km", "sss_smap_40km_unc",
    "sss_smap_RF", "sss_smap_RF_unc", "sss_ref", "gland", "fland",
    "gice_est", "surtep", "winspd", "nobs", "nobs_40km", "nobs_RF",
    "sea_ice_zones",
]

COLLECTION_CONFIG = {
    "MUR25-JPL-L4-GLOB-v04.2": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6": {
        "concat_dim": "time",
        "data_vars": _STANDARD_16_VAR,
        "coords": "minimal",
        "preprocess": "expand-time-dim",
    },
    "NEUROST_SSH-SST_L4_V2024.0": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "all",
        "preprocess": "time-from-filename",
        "sort": True,
    },
    "OSTIA-UKMO-L4-GLOB-REP-v2.0": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    # Real short_name (was previously the wrong "CCMP_Wind_Analysis_V3.1_L4").
    "CCMP_WINDS_10M6HR_L4_V3.1": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "ECCO_L4_OBP_05DEG_DAILY_V4R4B": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "ECCO_L4_OCEAN_VEL_05DEG_DAILY_V4R4": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "ECCO_L4_SSH_05DEG_DAILY_V4R4B": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "ECCO_L4_TEMP_SALINITY_05DEG_DAILY_V4R4": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
}

def _preprocess_expand_time_dim(ds):
    return ds.expand_dims("time") if "time" not in ds.dims else ds


def _preprocess_time_from_filename(ds):
    import re
    source = ds.encoding.get("source", "") or ""
    match = re.search(r"NeurOST_SSH-SST_(\d{8})_", source)
    if match:
        date = np.datetime64(f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:8]}")
    else:
        date = ds["time"].values.flat[0] if "time" in ds.coords else np.datetime64("NaT")
    ds = ds.assign_coords(time=[date])
    return ds


PREPROCESS_FUNCTIONS = {
    "expand-time-dim": _preprocess_expand_time_dim,
    "time-from-filename": _preprocess_time_from_filename,
}


def get_collection_config(collection):
    if collection in COLLECTION_CONFIG:
        return COLLECTION_CONFIG[collection]
    return {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    }


def get_store_prefix(collection):
    return f"virtual_collections/{collection}/{collection}_icechunk_v2.s3/"


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
    preprocess_name = config.get("preprocess")
    preprocess_fn = PREPROCESS_FUNCTIONS.get(preprocess_name) if preprocess_name else None

    logging.info("[%s] Building VDS for %d granule(s)...", collection, len(granule_urls))
    vds, source_bucket = build_vds(
        granule_urls, auth,
        concat_dim=concat_dim,
        data_vars=data_vars,
        coords=coords,
        preprocess_fn=preprocess_fn,
    )
    logging.info("[%s] New VDS shape: %s", collection, dict(vds.sizes))

    store_prefix = get_store_prefix(collection)
    vcc_bucket = f"s3://{source_bucket}"

    repo = open_repo_s3(store_bucket, store_prefix, vcc_bucket)
    logging.info("[%s] Opened store: s3://%s/%s", collection, store_bucket, store_prefix)

    session = repo.writable_session("main")

    existing_ds = xr.open_zarr(session.store, consolidated=False)
    existing_max = None
    if concat_dim in existing_ds.coords and existing_ds.sizes.get(concat_dim, 0) > 0:
        existing_max = np.asarray(existing_ds[concat_dim].values).max()
    logging.info("[%s] Existing shape: %s", collection, dict(existing_ds.sizes))

    # Idempotency: drop any granules whose concat-dim value is already present.
    vds_new = _filter_new_along_dim(vds, existing_ds, concat_dim)
    existing_ds.close()

    n_new = int(vds_new.sizes.get(concat_dim, 0))
    if n_new == 0:
        logging.info("[%s] Nothing new to append (all values already present).", collection)
        return

    # Icechunk append only concatenates; it does not reorder. Warn if this would
    # break monotonicity along the append dimension.
    if existing_max is not None and concat_dim in vds_new.coords:
        incoming_min = np.asarray(vds_new[concat_dim].values).min()
        if incoming_min <= existing_max:
            logging.warning(
                "[%s] Incoming %s min (%s) <= existing max (%s); append will produce "
                "non-monotonic %s.", collection, concat_dim, incoming_min, existing_max, concat_dim,
            )

    if sort:
        vds_new = vds_new.sortby(concat_dim)

    vds_new.vz.to_icechunk(session.store, append_dim=concat_dim)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    commit_msg = f"Append {n_new} granule(s) at {timestamp}"
    session.commit(commit_msg)
    logging.info("[%s] Committed: %s", collection, commit_msg)

    verify_session = repo.readonly_session(branch="main")
    verify_ds = xr.open_zarr(verify_session.store, consolidated=False)
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
