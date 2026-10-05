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
import xarray as xr
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser
import virtualizarr as vz

from icechunk_pipeline.icechunk_append import read_store_coordinate, build_write_plan, apply_write_plan
from icechunk_pipeline.source_url_coord import (
    url_map_from_granules,
    retire_moved_urls,
    reconcile_source_urls,
)

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


def _append_to_store(collection, vds, store_bucket, store_prefix, vcc_url_prefix,
                     vcc_store, concat_dim, store_type, url_map=None):
    """Append a VDS to a single Icechunk store. Returns count appended.

    ``url_map`` maps each incoming concat-dim value to the source URL *as it
    appears in this store's manifest* (s3:// for the S3 store, https:// for the
    HTTPS store). When given, a granule reprocessed with a shifted time (same
    URL, new time) retires its stale step first, and the time->URL link is
    recorded in a ``source_url`` coordinate.
    """
    logger.info("[%s][%s] Opening store: s3://%s/%s", collection, store_type, store_bucket, store_prefix)
    repo = open_repo(store_bucket, store_prefix, vcc_url_prefix, vcc_store)

    session = repo.writable_session("main")

    # Retire stale steps for shifted-time reprocessing before planning, so the
    # plan reflects the removal. No-op unless the store already has source_url.
    retired = {"n_retired": 0, "retired": []}
    if url_map:
        retired = retire_moved_urls(session, url_map, concat_dim=concat_dim)
        if retired["n_retired"]:
            logger.info("[%s][%s] Retired %d moved granule step(s) at %s=%s",
                        collection, store_type, retired["n_retired"],
                        concat_dim, retired["retired"])

    # read_store_coordinate opens the store with decode_times=False so the raw
    # encoded values compare like-for-like with the incoming vds.
    existing = read_store_coordinate(session.store, concat_dim)
    logger.info(
        "[%s][%s] Existing '%s' extent: %d step(s)",
        collection, store_type, concat_dim,
        0 if existing is None else existing.size,
    )

    plan = build_write_plan(vds, existing, dimension=concat_dim)
    logger.info(
        "[%s][%s] Write plan [%s]: %d new, %d already-present granule(s)",
        collection, store_type, plan.mode, plan.n_new, plan.n_region,
    )

    if plan.is_empty and not retired["n_retired"]:
        logger.info("[%s][%s] Nothing to write.", collection, store_type)
        return 0

    if not plan.is_empty:
        apply_write_plan(session, vds, plan)
    n_new = plan.n_new

    if url_map:
        reconcile_source_urls(session, url_map, concat_dim=concat_dim)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    retired_note = f", {retired['n_retired']} moved" if retired["n_retired"] else ""
    commit_msg = (
        f"{plan.mode} {plan.n_new} new, {plan.n_region} in-place{retired_note} "
        f"granule(s) at {timestamp}"
    )
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

    # Map each incoming granule URL to its (encoded) concat-dim value, reading
    # the granule with this handler's own build_vds (SWOT-aware credentials
    # endpoint, same preprocess). The URL is the stable identity across
    # reprocessing; the time can change.
    def _build_vds_fn(urls, a, **kw):
        return build_vds(urls, a, preprocess_fn=preprocess_fn, **kw)

    url_map_s3 = url_map_from_granules(
        granule_urls, auth, concat_dim=concat_dim,
        build_vds_fn=_build_vds_fn, data_vars=data_vars, coords=coords,
    )

    # S3 store
    s3_prefix = store_prefix_override or get_store_prefix_s3(collection)
    vcc_s3_prefix = f"s3://{source_bucket}/"
    _append_to_store(
        collection, vds_s3, store_bucket, s3_prefix,
        vcc_url_prefix=vcc_s3_prefix,
        vcc_store=icechunk.s3_store(region="us-west-2", anonymous=True),
        concat_dim=concat_dim,
        store_type="s3",
        url_map=url_map_s3,
    )

    # HTTPS store
    https_prefix = store_prefix_override.replace(".s3/", ".https/") if store_prefix_override else get_store_prefix_https(collection)
    https_host = BUCKET_TO_HOST.get(source_bucket, DEFAULT_HTTPS_HOST)
    vcc_https_prefix = f"https://{https_host}/{source_bucket}/"

    vds_https = vds_s3.vz.rename_paths(s3_to_http_url)
    # Same map, but URLs rewritten to match the HTTPS store's manifest paths.
    url_map_https = {t: s3_to_http_url(u) for t, u in url_map_s3.items()}
    _append_to_store(
        collection, vds_https, store_bucket, https_prefix,
        vcc_url_prefix=vcc_https_prefix,
        vcc_store=icechunk.http_store(),
        concat_dim=concat_dim,
        store_type="https",
        url_map=url_map_https,
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
