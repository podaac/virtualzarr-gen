#!/usr/bin/env python3
"""
Single end-to-end scenario validator for the live SQS append pipeline.

Walks ONE fresh scratch Icechunk store (S3 + HTTPS) through every append-path
write mode, driving each append through the REAL SQS queue -> deployed
lambda/sqs handler -> both stores, and asserts the store state after each stage.
Prints PASS/FAIL/SKIP per stage and a final summary; exits non-zero on any FAIL.

Self-contained: this script only uses external libraries (icechunk, xarray,
virtualizarr, obstore, boto3, earthaccess) and inlines the few helpers it needs,
so it does not import the pipeline's own modules. It treats the deployed handler
as a black box reached only through SQS.

Stages (9 time-sorted granules, positions chosen to force each mode):

  0 CREATE       positions [0, 2, 4]   direct write  -> store has source_url
  1 TAIL-APPEND  positions [6, 8]      via SQS        -> handler mode 'append'
  2 IDEMPOTENT   position  [2] again   via SQS        -> 'region', no duplicate
  3 GAP-INSERT   positions [1,3,5,7]   via SQS        -> 'insert' (reindex)
  4 MOVE         --                    SKIPPED        -> see note below

The shifted-time MOVE (same URL re-delivered at a new time) is NOT exercised
here: a protected PO.DAAC granule cannot be made to re-decode to a different
time through the queue. It stays covered by the Tier-1 self-test:
    python source_url_coord.py
DELETE is also out of scope -- the handler only appends; use delete_granules.py.

After each growth stage, both stores are polled until the expected step count
appears (or --timeout elapses -> FAIL). Each check asserts, per store:
  * time strictly increasing
  * source_url present, one per step, no blanks, NO url at two times
and across stores: equal step counts and HTTPS source_url == rewrite(S3 map).

MUST run on an EC2 in us-west-2 (direct-S3 granule reads are region-scoped) with
the deployed handler consuming --queue-url.

Usage:
    python test_e2e_all_scenarios.py --queue-url "$QUEUE_URL"
    python test_e2e_all_scenarios.py --queue-url "$QUEUE_URL" --recreate
    python test_e2e_all_scenarios.py --plan-only          # dry layout, no SQS/S3
"""

import argparse
import json
import logging
import sys
import time
import uuid
import warnings
from urllib.parse import urlparse

import boto3
import earthaccess
import icechunk
import numpy as np
import xarray as xr
import zarr
import virtualizarr as vz
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("e2e")

DEFAULT_COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
DEFAULT_PREFIX_BASE = "virtual_collections/_test_e2e_all/"
N_GRANULES = 9

SOURCE_URL_VAR = "source_url"
_STRING_DTYPE = np.dtypes.StringDType()

# Archive host per source bucket (SWOT uses a different host); mirrors the
# pipeline's collection_config but kept inline so this test stays standalone.
BUCKET_TO_HOST = {
    "podaac-swot-ops-cumulus-protected": "archive.swot.podaac.earthdata.nasa.gov",
    "podaac-swot-ops-cumulus-public": "archive.swot.podaac.earthdata.nasa.gov",
}
DEFAULT_HTTPS_HOST = "archive.podaac.earthdata.nasa.gov"

# (stage label, positions into the time-sorted granule list, how delivered)
STAGES = [
    ("CREATE", [0, 2, 4], "direct"),
    ("TAIL-APPEND", [6, 8], "sqs"),
    ("IDEMPOTENT", [2], "sqs"),
    ("GAP-INSERT", [1, 3, 5, 7], "sqs"),
]


def https_host_for_bucket(bucket):
    return BUCKET_TO_HOST.get(bucket, DEFAULT_HTTPS_HOST)


def s3_to_https_url(s3_url):
    if not s3_url.startswith("s3://"):
        return s3_url
    raw_path = s3_url.replace("s3://", "")
    bucket = raw_path.split("/", 1)[0]
    return f"https://{https_host_for_bucket(bucket)}/{raw_path}"


# --------------------------------------------------------------------------- #
# Granule discovery
# --------------------------------------------------------------------------- #
def granule_start(g):
    """Beginning datetime string from a granule's UMM, for time ordering."""
    try:
        return g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
    except Exception:
        try:
            return g["umm"]["TemporalExtent"]["SingleDateTime"]
        except Exception:
            return ""


def discover_granules(collection, provider):
    results = earthaccess.search_data(
        short_name=collection, provider=provider, cloud_hosted=True,
    )
    if len(results) < N_GRANULES:
        raise SystemExit(f"Need at least {N_GRANULES} granules, found {len(results)}.")
    results = sorted(results, key=granule_start)[:N_GRANULES]
    urls = [g.data_links(access="direct")[0] for g in results]
    return urls


# --------------------------------------------------------------------------- #
# Virtual dataset build (inline; no dask -- a handful of granules at a time)
# --------------------------------------------------------------------------- #
def build_vds(urls, auth, concat_dim, data_vars, coords):
    bucket = urlparse(urls[0]).netloc
    cred_endpoint = f"https://{https_host_for_bucket(bucket)}/s3credentials"
    s3_store = S3Store(
        bucket=bucket, region="us-west-2",
        credential_provider=NasaEarthdataCredentialProvider(
            cred_endpoint, auth=auth.token["access_token"]),
        virtual_hosted_style_request=False, client_options={"allow_http": True},
    )
    registry = ObjectStoreRegistry({f"s3://{bucket}": s3_store})
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)
        vds = vz.open_virtual_mfdataset(
            urls=urls, registry=registry, parser=HDFParser(),
            decode_times=False, combine="nested", concat_dim=concat_dim,
            data_vars=data_vars, coords=coords, compat="override", combine_attrs="override",
        )
    return vds, bucket


# --------------------------------------------------------------------------- #
# source_url coordinate (inline read/write; mirrors source_url_coord)
# --------------------------------------------------------------------------- #
def url_map_from_vds(vds, concat_dim):
    """{time value: source url} derived from the VDS chunk manifest (one step
    per chunk along concat_dim)."""
    coord = np.asarray(vds[concat_dim].values)
    for _name, var in vds.variables.items():
        manifest = getattr(getattr(var, "data", None), "manifest", None)
        if manifest is None or concat_dim not in var.dims:
            continue
        axis = var.dims.index(concat_dim)
        mapping = {}
        for chunk_key, entry in manifest.dict().items():
            pos = int(chunk_key.split(".")[axis])
            mapping[np.asarray(coord[pos]).item()] = entry["path"]
        if len(mapping) == coord.size:
            return mapping
    raise ValueError(f"No manifest-backed variable maps one chunk per '{concat_dim}' step")


def read_source_url_map(store, concat_dim):
    """{time value: url} currently stored, paired positionally; {} if absent."""
    try:
        root = zarr.open_group(store, mode="r")
    except Exception:
        return {}
    names = [n for n, _ in root.arrays()]
    if SOURCE_URL_VAR not in names or concat_dim not in names:
        return {}
    axis = np.asarray(root[concat_dim][:])
    urls = np.asarray(root[SOURCE_URL_VAR][:])
    n = min(axis.size, urls.size)
    return {np.asarray(axis[i]).item(): str(urls[i]) for i in range(n)}


def write_source_url(session, url_map, concat_dim):
    """Seed the source_url coordinate for the CREATE store so later stages have
    a complete time->URL map (the handler maintains it from here on)."""
    root = zarr.open_group(session.store, mode="a")
    axis = np.asarray(root[concat_dim][:])
    merged = {np.asarray(k).item(): v for k, v in url_map.items()}
    urls = np.array([merged.get(np.asarray(t).item(), "") for t in axis], dtype=_STRING_DTYPE)
    names = [n for n, _ in root.arrays()]
    if SOURCE_URL_VAR in names:
        arr = root[SOURCE_URL_VAR]
        if arr.shape != (axis.size,):
            arr.resize((axis.size,))
    else:
        arr = root.create_array(
            SOURCE_URL_VAR, shape=(axis.size,), chunks=(1,),
            dtype=_STRING_DTYPE, dimension_names=(concat_dim,),
        )
    arr[:] = urls


# --------------------------------------------------------------------------- #
# Store prefixes / repos
# --------------------------------------------------------------------------- #
def test_prefixes(base, collection):
    """Scratch S3/HTTPS prefixes. The .s3/ -> .https/ naming matches the
    handler's store_prefix_override derivation (append_lambda_handler)."""
    stem = f"{base}{collection}.icechunk_v2"
    return f"{stem}.s3/", f"{stem}.https/"


def _delete_prefix(s3, bucket, prefix):
    """Empty a scratch prefix so a fresh store can be created (scratch-only)."""
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        to_delete.extend({"Key": o["Key"]} for o in page.get("Contents", []))
    for i in range(0, len(to_delete), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete[i:i + 1000]})
    if to_delete:
        logger.info("Cleared %d object(s) under s3://%s/%s", len(to_delete), bucket, prefix)


def _repo_create_s3(bucket, prefix, source_bucket):
    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-west-2")
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=f"s3://{source_bucket}/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )
    return icechunk.Repository.create(storage, config)


def _repo_create_https(bucket, prefix, source_bucket):
    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-west-2")
    config = icechunk.RepositoryConfig.default()
    vcc_https_prefix = f"https://{https_host_for_bucket(source_bucket)}/{source_bucket}/"
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(vcc_https_prefix, icechunk.http_store())
    )
    return icechunk.Repository.create(storage, config)


def _open_read(bucket, prefix):
    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-west-2")
    config = icechunk.Repository.fetch_config(storage)
    repo = icechunk.Repository.open(storage, config=config)
    return repo.readonly_session(branch="main").store


# --------------------------------------------------------------------------- #
# Reading / assertions
# --------------------------------------------------------------------------- #
def read_axis(store, concat_dim):
    ds = xr.open_zarr(store, consolidated=False, decode_times=False)
    axis = np.asarray(ds[concat_dim].values)
    ds.close()
    return axis


def assert_store_ok(bucket, prefix, concat_dim, label):
    """Per-store invariants. Raises AssertionError on violation."""
    store = _open_read(bucket, prefix)
    axis = read_axis(store, concat_dim)
    if axis.size > 1:
        assert bool(np.all(np.diff(axis) > 0)), f"{label}: {concat_dim} not strictly increasing: {axis.tolist()}"

    url_map = read_source_url_map(store, concat_dim)  # {time: url}
    assert len(url_map) == axis.size, (
        f"{label}: source_url has {len(url_map)} entries but {concat_dim} has {axis.size}")
    blanks = [t for t, u in url_map.items() if not u]
    assert not blanks, f"{label}: {len(blanks)} step(s) have a blank source_url: {blanks}"
    seen = {}
    for t, u in url_map.items():
        assert u not in seen, f"{label}: url {u} appears at {concat_dim}={seen[u]} and {t} (duplicate)"
        seen[u] = t
    return axis, url_map


def assert_parity(s3_url_map, https_url_map, label):
    assert len(s3_url_map) == len(https_url_map), (
        f"{label}: step count differs S3={len(s3_url_map)} HTTPS={len(https_url_map)}")
    expected_https = {t: s3_to_https_url(u) for t, u in s3_url_map.items()}
    assert expected_https == https_url_map, (
        f"{label}: HTTPS source_url map does not match rewrite(S3 map)")


def poll_until(bucket, prefix, concat_dim, expected_count, timeout, interval, label):
    """Poll a store until it reaches expected_count steps or timeout. Returns the
    final count; raises TimeoutError on timeout."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = read_axis(_open_read(bucket, prefix), concat_dim).size
        except Exception as exc:  # store mid-commit / transient
            logger.debug("[%s] poll read error: %s", label, exc)
        if last == expected_count:
            return last
        time.sleep(interval)
    raise TimeoutError(f"{label}: waited {timeout}s for {expected_count} steps, last saw {last}")


# --------------------------------------------------------------------------- #
# SQS
# --------------------------------------------------------------------------- #
def send_sqs(sqs, queue_url, collection, urls, store_prefix):
    body = {"collection": collection, "granules": urls, "store_prefix": store_prefix}
    resp = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(body),
        MessageGroupId=collection,
        MessageDeduplicationId=str(uuid.uuid4()),
    )
    logger.info("Sent %d granule(s) -> MessageId=%s", len(urls), resp["MessageId"])


# --------------------------------------------------------------------------- #
# CREATE (direct, not via SQS -- the handler opens existing stores only)
# --------------------------------------------------------------------------- #
def create_stores(bucket, s3_prefix, https_prefix, urls, auth, concat_dim, data_vars, coords):
    vds, source_bucket = build_vds(urls, auth, concat_dim, data_vars, coords)
    logger.info("CREATE VDS shape: %s", dict(vds.sizes))

    url_map_s3 = url_map_from_vds(vds, concat_dim)

    repo_s3 = _repo_create_s3(bucket, s3_prefix, source_bucket)
    s3_sess = repo_s3.writable_session("main")
    vds.virtualize.to_icechunk(s3_sess.store)
    write_source_url(s3_sess, url_map_s3, concat_dim)
    s3_sess.commit("CREATE: initial store with source_url")

    repo_https = _repo_create_https(bucket, https_prefix, source_bucket)
    vds_https = vds.vz.rename_paths(s3_to_https_url)
    https_sess = repo_https.writable_session("main")
    vds_https.virtualize.to_icechunk(https_sess.store)
    url_map_https = {t: s3_to_https_url(u) for t, u in url_map_s3.items()}
    write_source_url(https_sess, url_map_https, concat_dim)
    https_sess.commit("CREATE: initial store with source_url")


# --------------------------------------------------------------------------- #
# Stage runner
# --------------------------------------------------------------------------- #
def run_stage(label, positions, how, *, urls, bucket, s3_prefix, https_prefix,
              concat_dim, data_vars, coords, sqs, queue_url, collection, auth,
              timeout, interval, expected_total):
    stage_urls = [urls[p] for p in positions]
    print(f"\n{'=' * 72}\nSTAGE {label}  positions={positions}  via={how}\n{'=' * 72}")
    for p, u in zip(positions, stage_urls):
        print(f"  [{p}] {u}")

    if how == "direct":
        create_stores(bucket, s3_prefix, https_prefix, stage_urls, auth,
                      concat_dim, data_vars, coords)
    elif label == "IDEMPOTENT":
        # Re-deliver an already-present granule: expect NO change. Send, settle,
        # then assert the count/axis did not move.
        before = read_axis(_open_read(bucket, s3_prefix), concat_dim)
        send_sqs(sqs, queue_url, collection, stage_urls, s3_prefix)
        logger.info("Settling %ds to let the handler process the no-op...", timeout)
        time.sleep(timeout)
        after = read_axis(_open_read(bucket, s3_prefix), concat_dim)
        assert after.size == before.size and np.array_equal(after, before), (
            f"IDEMPOTENT: axis changed {before.tolist()} -> {after.tolist()}")
    else:
        send_sqs(sqs, queue_url, collection, stage_urls, s3_prefix)
        poll_until(bucket, s3_prefix, concat_dim, expected_total, timeout, interval, "S3")
        poll_until(bucket, https_prefix, concat_dim, expected_total, timeout, interval, "HTTPS")

    s3_axis, s3_map = assert_store_ok(bucket, s3_prefix, concat_dim, "S3")
    _, https_map = assert_store_ok(bucket, https_prefix, concat_dim, "HTTPS")
    assert_parity(s3_map, https_map, label)
    print(f"  -> {len(s3_axis)} step(s); axis sorted; source_url aligned; S3/HTTPS in parity")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--queue-url", help="SQS FIFO queue URL (required unless --plan-only)")
    ap.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--collection", default=DEFAULT_COLLECTION)
    ap.add_argument("--provider", default="POCLOUD")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--data-vars", default="minimal")
    ap.add_argument("--coords", default="minimal")
    ap.add_argument("--test-prefix-base", default=DEFAULT_PREFIX_BASE)
    ap.add_argument("--timeout", type=int, default=300, help="Per-stage poll/settle seconds")
    ap.add_argument("--interval", type=int, default=15, help="Poll interval seconds")
    ap.add_argument("--recreate", action="store_true",
                    help="Empty the scratch prefixes first if they exist")
    ap.add_argument("--keep", action="store_true",
                    help="Leave the scratch stores in S3 after the run")
    ap.add_argument("--plan-only", action="store_true",
                    help="Print the computed stage/URL layout and exit (no SQS/S3)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    concat_dim = args.concat_dim
    s3_prefix, https_prefix = test_prefixes(args.test_prefix_base, args.collection)

    auth = earthaccess.login()
    urls = discover_granules(args.collection, args.provider)

    print(f"\nCollection : {args.collection}")
    print(f"Store      : s3://{args.store_bucket}/{s3_prefix}")
    print(f"             s3://{args.store_bucket}/{https_prefix}")
    print(f"Granules   : {len(urls)} (time-sorted)")

    # expected cumulative step count after each stage
    totals, running = {}, 0
    for label, positions, _how in STAGES:
        if label == "IDEMPOTENT":
            totals[label] = running  # no growth
        else:
            running += len(positions)
            totals[label] = running

    if args.plan_only:
        print("\n--- PLAN ONLY (no SQS/S3) ---")
        for label, positions, how in STAGES:
            print(f"  {label:12s} positions={positions} via={how} -> {totals[label]} total step(s)")
            for p in positions:
                print(f"       [{p}] {urls[p]}")
        print(f"  {'MOVE':12s} SKIPPED (same URL cannot re-decode to a new time via the queue)")
        return

    if not args.queue_url:
        ap.error("--queue-url is required (or use --plan-only)")

    s3 = boto3.client("s3", region_name="us-west-2")
    if args.recreate:
        _delete_prefix(s3, args.store_bucket, s3_prefix)
        _delete_prefix(s3, args.store_bucket, https_prefix)

    sqs = boto3.client("sqs", region_name="us-west-2")

    results = {}
    for label, positions, how in STAGES:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                run_stage(
                    label, positions, how,
                    urls=urls, bucket=args.store_bucket,
                    s3_prefix=s3_prefix, https_prefix=https_prefix,
                    concat_dim=concat_dim, data_vars=args.data_vars, coords=args.coords,
                    sqs=sqs, queue_url=args.queue_url, collection=args.collection,
                    auth=auth, timeout=args.timeout, interval=args.interval,
                    expected_total=totals[label],
                )
            results[label] = "PASS"
            print(f"STAGE {label}: PASS")
        except Exception as exc:
            results[label] = f"FAIL ({exc})"
            print(f"STAGE {label}: FAIL -- {exc}")
            break  # later stages build on this one; stop
    results["MOVE"] = ("SKIP (same URL cannot re-decode to a new time via the queue; "
                       "covered by: python source_url_coord.py)")

    all_passed = all(v == "PASS" for k, v in results.items() if k != "MOVE")
    if not args.keep and all_passed:
        _delete_prefix(s3, args.store_bucket, s3_prefix)
        _delete_prefix(s3, args.store_bucket, https_prefix)
    elif not args.keep:
        print("\n(Left scratch stores in place for inspection after failure; "
              "delete manually or re-run with --recreate.)")

    print(f"\n{'=' * 72}\nSUMMARY\n{'=' * 72}")
    for label, _p, _h in STAGES:
        print(f"  {label:12s} {results.get(label, '-')}")
    print(f"  {'MOVE':12s} {results['MOVE']}")

    failed = [k for k, v in results.items() if v.startswith("FAIL")]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
