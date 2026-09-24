#!/usr/bin/env python3
"""
AWS Lambda handler for appending granules to Icechunk v2 stores.

Triggered by an SQS FIFO queue. Each invocation receives a batch of messages
for a single collection (enforced by MessageGroupId) and appends all granules
in one Icechunk commit.

Expected SQS message body (JSON):
    {
        "collection": "MUR25-JPL-L4-GLOB-v04.2",
        "granules": ["s3://bucket/path/to/granule.nc"]
    }
"""

import json
import logging
import os
import warnings
from datetime import datetime, timezone
from urllib.parse import urlparse

import earthaccess
import icechunk
import numpy as np
import xarray as xr
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser
import virtualizarr as vz

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STORE_BUCKET = os.environ.get("STORE_BUCKET", "")

COLLECTION_CONFIG = {
    "MUR25-JPL-L4-GLOB-v04.2": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6": {
        "concat_dim": "time",
        "data_vars": [
            "sss_smap", "sss_smap_unc", "sss_smap_40km", "sss_smap_40km_unc",
            "sss_smap_RF", "sss_smap_RF_unc", "sss_ref", "gland", "fland",
            "gice_est", "surtep", "winspd", "nobs", "nobs_40km", "nobs_RF",
            "sea_ice_zones",
        ],
        "coords": "minimal",
        "preprocess": "expand-time-dim",
    },
    "NEUROST_SSH-SST_L4_V2024.0": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "all",
        "preprocess": "time-from-filename",
    },
    "OSTIA-UKMO-L4-GLOB-REP-v2.0": {
        "concat_dim": "time",
        "data_vars": "minimal",
        "coords": "minimal",
    },
    "CCMP_Wind_Analysis_V3.1_L4": {
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
    credentials_endpoint = "https://archive.podaac.earthdata.nasa.gov/s3credentials"

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


def append_to_collection(collection, granule_urls, store_bucket, auth):
    config = get_collection_config(collection)
    concat_dim = config["concat_dim"]
    data_vars = config["data_vars"]
    coords = config["coords"]
    preprocess_name = config.get("preprocess")
    preprocess_fn = PREPROCESS_FUNCTIONS.get(preprocess_name) if preprocess_name else None

    logger.info("[%s] Building VDS for %d granule(s)...", collection, len(granule_urls))
    vds, source_bucket = build_vds(
        granule_urls, auth,
        concat_dim=concat_dim,
        data_vars=data_vars,
        coords=coords,
        preprocess_fn=preprocess_fn,
    )
    logger.info("[%s] New VDS shape: %s", collection, dict(vds.sizes))

    store_prefix = get_store_prefix(collection)
    vcc_bucket = f"s3://{source_bucket}"

    repo = open_repo_s3(store_bucket, store_prefix, vcc_bucket)
    logger.info("[%s] Opened store: s3://%s/%s", collection, store_bucket, store_prefix)

    session = repo.writable_session("main")

    existing_ds = xr.open_zarr(session.store, consolidated=False)
    logger.info("[%s] Existing shape: %s", collection, dict(existing_ds.sizes))
    existing_ds.close()

    vds.vz.to_icechunk(session.store, append_dim=concat_dim)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    commit_msg = f"Append {len(granule_urls)} granule(s) at {timestamp}"
    session.commit(commit_msg)
    logger.info("[%s] Committed: %s", collection, commit_msg)

    verify_session = repo.readonly_session(branch="main")
    verify_ds = xr.open_zarr(verify_session.store, consolidated=False)
    logger.info("[%s] Verified shape: %s", collection, dict(verify_ds.sizes))
    verify_ds.close()


def handler(event, context):
    """Lambda entry point. Receives SQS event with batch of messages."""
    store_bucket = STORE_BUCKET
    if not store_bucket:
        raise ValueError("STORE_BUCKET environment variable is required")

    auth = earthaccess.login()

    records = event.get("Records", [])
    if not records:
        logger.info("No records in event, nothing to do.")
        return {"statusCode": 200, "body": "No records"}

    logger.info("Received %d SQS record(s)", len(records))

    all_granules = []
    collection = None

    for record in records:
        body = json.loads(record["body"])
        msg_collection = body["collection"]
        granules = body["granules"]

        if collection is None:
            collection = msg_collection
        elif collection != msg_collection:
            logger.warning(
                "Mixed collections in batch: %s vs %s. Processing %s only.",
                collection, msg_collection, collection,
            )
            continue

        all_granules.extend(granules)

    if not all_granules:
        logger.info("No granules to append.")
        return {"statusCode": 200, "body": "No granules"}

    logger.info("[%s] Appending %d granule(s)...", collection, len(all_granules))
    append_to_collection(collection, all_granules, store_bucket, auth)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "collection": collection,
            "granules_appended": len(all_granules),
        }),
    }
