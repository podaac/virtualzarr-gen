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

from podaac.collection_config import (
    BUCKET_TO_HOST,
    DEFAULT_HTTPS_HOST,
    get_collection_config,
    get_preprocess_fn,
    get_store_prefix_https,
    get_store_prefix_s3,
    s3_to_http_url,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STORE_BUCKET = os.environ.get("STORE_BUCKET", "")

# Names of the SSM parameters holding Earthdata credentials. Matches the ECS
# convention (see wrapper.sh): the env var holds the *parameter name*, which we
# resolve at cold start and inject into the environment for earthaccess.
SSM_EDL_USERNAME = os.environ.get("SSM_EDL_USERNAME", "")
SSM_EDL_PASSWORD = os.environ.get("SSM_EDL_PASSWORD", "")
SSM_EDL_TOKEN = os.environ.get("SSM_EDL_TOKEN", "")


_ssm_client = None


def _get_ssm_parameter(name):
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    resp = _ssm_client.get_parameter(Name=name, WithDecryption=True)
    return resp["Parameter"]["Value"]


def login_earthdata():
    """Authenticate to Earthdata.

    Prefers an explicit token from SSM, then username/password from SSM, and
    finally falls back to whatever is already in the environment. Injecting the
    SSM values into the environment lets earthaccess pick them up and also makes
    the token available to Dask/obstore credential providers.

    SSM lookups are best-effort: if a configured parameter is missing (e.g. the
    stage has no SSM params yet), we log and fall back to EARTHDATA_* env vars
    set directly on the Lambda rather than failing the whole invocation.
    """
    if SSM_EDL_TOKEN:
        try:
            os.environ["EARTHDATA_TOKEN"] = _get_ssm_parameter(SSM_EDL_TOKEN)
        except Exception as exc:
            logger.warning("Could not read SSM token param %r: %s; falling back to env creds.", SSM_EDL_TOKEN, exc)
    if SSM_EDL_USERNAME and SSM_EDL_PASSWORD:
        try:
            os.environ["EARTHDATA_USERNAME"] = _get_ssm_parameter(SSM_EDL_USERNAME)
            os.environ["EARTHDATA_PASSWORD"] = _get_ssm_parameter(SSM_EDL_PASSWORD)
        except Exception as exc:
            logger.warning("Could not read SSM EDL user/pass params: %s; falling back to env creds.", exc)

    if not (
        os.environ.get("EARTHDATA_TOKEN")
        or (os.environ.get("EARTHDATA_USERNAME") and os.environ.get("EARTHDATA_PASSWORD"))
    ):
        raise ValueError(
            "No Earthdata credentials available. Set SSM_EDL_TOKEN (or "
            "SSM_EDL_USERNAME/SSM_EDL_PASSWORD) to the SSM parameter name(s), or "
            "provide EARTHDATA_TOKEN / EARTHDATA_USERNAME+EARTHDATA_PASSWORD directly."
        )

    return earthaccess.login(strategy="environment")


def open_repo(bucket, prefix, vcc_url_prefix, vcc_store):
    logger.info("open_repo: bucket=%s prefix=%s vcc_url_prefix=%s", bucket, prefix, vcc_url_prefix)
    storage = icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region="us-west-2",
    )
    config = icechunk.Repository.fetch_config(storage)
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=vcc_url_prefix,
            store=vcc_store,
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


def _filter_new_along_dim(vds, existing_ds, concat_dim):
    """Return the subset of vds whose concat-dim coordinate values are NOT
    already present in existing_ds.

    This is the idempotency guard: SQS delivers at-least-once, retries re-run
    the whole batch, and the S3/HTTPS stores are committed separately (not
    atomically). Filtering already-present coordinate values makes every
    append a no-op if it has already been applied, so duplicates never appear.
    """
    if concat_dim not in vds.coords:
        # No coordinate to dedup on; caller must accept possible duplicates.
        logger.warning("Concat dim %r is not a coordinate; skipping idempotency filter.", concat_dim)
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
    kept = int(mask.sum())
    logger.info(
        "Idempotency filter: %d of %d incoming %s value(s) are new.",
        kept, len(new_vals), concat_dim,
    )
    return vds.isel({concat_dim: np.where(mask)[0]})


def _append_to_store(collection, vds, store_bucket, store_prefix, vcc_url_prefix,
                     vcc_store, concat_dim, store_type, sort=False):
    """Append a VDS to a single Icechunk store. Returns count appended."""
    logger.info("[%s][%s] Opening store: s3://%s/%s", collection, store_type, store_bucket, store_prefix)
    repo = open_repo(store_bucket, store_prefix, vcc_url_prefix, vcc_store)

    session = repo.writable_session("main")

    # Open undecoded: the incoming vds uses decode_times=False (raw encoded
    # ints), and the store holds the same raw values. Decoding the existing
    # side to datetime64 would make the idempotency filter and monotonicity
    # check compare int vs datetime64 and raise DTypePromotionError.
    existing_ds = xr.open_zarr(session.store, consolidated=False, decode_times=False)
    existing_max = None
    if concat_dim in existing_ds.coords and existing_ds.sizes.get(concat_dim, 0) > 0:
        existing_max = np.asarray(existing_ds[concat_dim].values).max()
    logger.info("[%s][%s] Existing shape: %s", collection, store_type, dict(existing_ds.sizes))

    # Idempotency: drop any granules whose concat-dim value is already present.
    vds_new = _filter_new_along_dim(vds, existing_ds, concat_dim)
    existing_ds.close()

    n_new = int(vds_new.sizes.get(concat_dim, 0))
    if n_new == 0:
        logger.info("[%s][%s] Nothing new to append (all values already present).", collection, store_type)
        return 0

    # Icechunk append only concatenates; it does not reorder. Warn loudly if the
    # incoming data would break monotonicity along the append dimension.
    if existing_max is not None and concat_dim in vds_new.coords:
        incoming_min = np.asarray(vds_new[concat_dim].values).min()
        if incoming_min <= existing_max:
            logger.warning(
                "[%s][%s] Incoming %s min (%s) <= existing max (%s); appending will "
                "produce non-monotonic %s. Consider a rewrite for this collection.",
                collection, store_type, concat_dim, incoming_min, existing_max, concat_dim,
            )

    if sort:
        vds_new = vds_new.sortby(concat_dim)

    vds_new.vz.to_icechunk(session.store, append_dim=concat_dim)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    commit_msg = f"Append {n_new} granule(s) at {timestamp}"
    session.commit(commit_msg)
    logger.info("[%s][%s] Committed: %s", collection, store_type, commit_msg)

    verify_session = repo.readonly_session(branch="main")
    verify_ds = xr.open_zarr(verify_session.store, consolidated=False, decode_times=False)
    logger.info("[%s][%s] Verified shape: %s", collection, store_type, dict(verify_ds.sizes))
    verify_ds.close()
    return n_new


def append_to_collection(collection, granule_urls, store_bucket, auth,
                         store_prefix_override=None):
    config = get_collection_config(collection)
    concat_dim = config["concat_dim"]
    data_vars = config["data_vars"]
    coords = config["coords"]
    sort = config.get("sort", False)
    preprocess_fn = get_preprocess_fn(config)

    logger.info("[%s] Building VDS for %d granule(s)...", collection, len(granule_urls))
    vds_s3, source_bucket = build_vds(
        granule_urls, auth,
        concat_dim=concat_dim,
        data_vars=data_vars,
        coords=coords,
        preprocess_fn=preprocess_fn,
    )
    logger.info("[%s] New VDS shape: %s", collection, dict(vds_s3.sizes))

    # S3 store
    s3_prefix = store_prefix_override or get_store_prefix_s3(collection)
    vcc_s3_prefix = f"s3://{source_bucket}/"
    _append_to_store(
        collection, vds_s3, store_bucket, s3_prefix,
        vcc_url_prefix=vcc_s3_prefix,
        vcc_store=icechunk.s3_store(region="us-west-2", anonymous=True),
        concat_dim=concat_dim,
        store_type="s3",
        sort=sort,
    )

    # HTTPS store
    https_prefix = store_prefix_override.replace(".s3/", ".https/") if store_prefix_override else get_store_prefix_https(collection)
    https_host = BUCKET_TO_HOST.get(source_bucket, DEFAULT_HTTPS_HOST)
    vcc_https_prefix = f"https://{https_host}/{source_bucket}/"

    vds_https = vds_s3.vz.rename_paths(s3_to_http_url)
    _append_to_store(
        collection, vds_https, store_bucket, https_prefix,
        vcc_url_prefix=vcc_https_prefix,
        vcc_store=icechunk.http_store(),
        concat_dim=concat_dim,
        store_type="https",
        sort=sort,
    )


def handler(event, context):
    """Lambda entry point. Receives an SQS event with a batch of messages.

    A single SQS FIFO poll can contain messages from multiple MessageGroupIds
    (collections), so we group by collection and process each group instead of
    dropping all-but-the-first (which would silently delete those messages).

    Returns partial batch failures so that only the messages belonging to a
    failing collection are retried; the rest are deleted by SQS. This requires
    the event source mapping to declare ReportBatchItemFailures.
    """
    store_bucket = STORE_BUCKET
    if not store_bucket:
        raise ValueError("STORE_BUCKET environment variable is required")

    auth = login_earthdata()

    records = event.get("Records", [])
    if not records:
        logger.info("No records in event, nothing to do.")
        return {"batchItemFailures": []}

    logger.info("Received %d SQS record(s)", len(records))

    # Group records by collection, preserving message ids for failure reporting.
    groups = {}  # collection -> {"granules": [...], "message_ids": [...], "store_prefix": str|None}
    parse_failures = []  # message ids we could not even parse

    for record in records:
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            msg_collection = body["collection"]
            granules = body["granules"]
        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("Malformed message %s: %s", message_id, exc)
            parse_failures.append(message_id)
            continue

        group = groups.setdefault(
            msg_collection,
            {"granules": [], "message_ids": [], "store_prefix": body.get("store_prefix")},
        )
        group["granules"].extend(granules)
        group["message_ids"].append(message_id)

    batch_item_failures = [{"itemIdentifier": mid} for mid in parse_failures]

    for collection, group in groups.items():
        granules = group["granules"]
        if not granules:
            continue
        logger.info("[%s] Appending %d granule(s)...", collection, len(granules))
        try:
            append_to_collection(
                collection, granules, store_bucket, auth,
                store_prefix_override=group["store_prefix"],
            )
        except Exception:
            # Fail only this collection's messages; the idempotency guard makes
            # the retry safe even if one of the two stores was already updated.
            logger.exception("[%s] Append failed; will retry these messages.", collection)
            batch_item_failures.extend(
                {"itemIdentifier": mid} for mid in group["message_ids"]
            )

    return {"batchItemFailures": batch_item_failures}
