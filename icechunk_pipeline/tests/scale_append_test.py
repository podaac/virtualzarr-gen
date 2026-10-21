#!/usr/bin/env python3
"""
Scale / load test for the live SQS append pipeline.

Builds a LARGE fresh scratch Icechunk store (S3 + HTTPS) directly from the first
--initial-count time-sorted granules of a collection, then drives --append-count
more granules through the REAL SQS queue -> deployed lambda/sqs handler as many
batched messages, delivered OUT OF ORDER on purpose, and finally asserts the
resulting store is correct and the two stores are in parity.

This exercises the pipeline the way a bulk back-fill would: lots of messages
arriving in a jumbled order, so the handler must keep the axis sorted via a mix
of appends and out-of-order inserts (reindex) while never losing or duplicating a
granule, and must keep the source_url coordinate aligned the whole way.

Self-contained: only external libraries (icechunk, xarray, virtualizarr, obstore,
boto3, earthaccess, numpy, zarr). It does NOT import the pipeline's own modules --
the deployed handler is a black box reached only through SQS. The inlined helpers
mirror those in test_e2e_all_scenarios.py.

Flow:
  1. discover (initial + append) time-sorted granules
  2. CREATE  the store directly from the first `initial` granules (writes
     source_url, as the generate path does); the handler only appends.
  3. APPEND  the next `append` granules, SHUFFLED into batches of --batch-size,
     each batch sent as one SQS message with a store_prefix override pointing at
     the scratch store. All messages are sent up front; the FIFO queue serializes
     them per collection.
  4. POLL    both stores until they reach initial+append steps (or --timeout).
  5. ASSERT  final count == initial+append; time strictly increasing; source_url
     one-per-step, no blanks, no URL at two times; the exact set of source URLs
     equals every granule we fed in (no loss, no extras); and S3 == rewrite of
     the HTTPS store (parity). Prints append throughput.

MUST run on an EC2 in us-west-2 (direct-S3 granule reads are region-scoped) with
the deployed handler consuming --queue-url. The CREATE step builds a VDS from
`initial` granules serially, which can take a few minutes for 100 granules.

Usage:
    python -m icechunk_pipeline.tests.scale_append_test --queue-url "$QUEUE_URL"
    python -m icechunk_pipeline.tests.scale_append_test --queue-url "$QUEUE_URL" \
        --initial-count 100 --append-count 100 --batch-size 10 --recreate
    python -m icechunk_pipeline.tests.scale_append_test --plan-only   # dry layout
"""

import argparse
import json
import logging
import random
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
logger = logging.getLogger("scale")

DEFAULT_COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
DEFAULT_PREFIX_BASE = "virtual_collections/_test_scale/"
DEFAULT_INITIAL = 100
DEFAULT_APPEND = 100
DEFAULT_BATCH = 10

SOURCE_URL_VAR = "source_url"
_STRING_DTYPE = np.dtypes.StringDType()

# Archive host per source bucket (SWOT uses a different host); mirrors the
# pipeline's collection_config but kept inline so this test stays standalone.
BUCKET_TO_HOST = {
    "podaac-swot-ops-cumulus-protected": "archive.swot.podaac.earthdata.nasa.gov",
    "podaac-swot-ops-cumulus-public": "archive.swot.podaac.earthdata.nasa.gov",
}
DEFAULT_HTTPS_HOST = "archive.podaac.earthdata.nasa.gov"


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


def discover_granules(collection, provider, n_needed):
    results = earthaccess.search_data(
        short_name=collection, provider=provider, cloud_hosted=True,
    )
    if len(results) < n_needed:
        raise SystemExit(f"Need at least {n_needed} granules, found {len(results)}.")
    results = sorted(results, key=granule_start)[:n_needed]
    urls = [g.data_links(access="direct")[0] for g in results]
    return urls


# --------------------------------------------------------------------------- #
# Virtual dataset build (inline; no dask)
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
    """Per-store invariants. Returns (axis, {time: url}). Raises on violation."""
    store = _open_read(bucket, prefix)
    axis = read_axis(store, concat_dim)
    if axis.size > 1:
        assert bool(np.all(np.diff(axis) > 0)), (
            f"{label}: {concat_dim} not strictly increasing")

    url_map = read_source_url_map(store, concat_dim)  # {time: url}
    assert len(url_map) == axis.size, (
        f"{label}: source_url has {len(url_map)} entries but {concat_dim} has {axis.size}")
    blanks = [t for t, u in url_map.items() if not u]
    assert not blanks, f"{label}: {len(blanks)} step(s) have a blank source_url"
    seen = {}
    for t, u in url_map.items():
        assert u not in seen, (
            f"{label}: url {u} appears at {concat_dim}={seen[u]} and {t} (duplicate)")
        seen[u] = t
    return axis, url_map


def assert_parity(s3_url_map, https_url_map, label):
    assert len(s3_url_map) == len(https_url_map), (
        f"{label}: step count differs S3={len(s3_url_map)} HTTPS={len(https_url_map)}")
    expected_https = {t: s3_to_https_url(u) for t, u in s3_url_map.items()}
    assert expected_https == https_url_map, (
        f"{label}: HTTPS source_url map does not match rewrite(S3 map)")


def poll_until(bucket, prefix, concat_dim, expected_count, timeout, interval, label):
    """Poll a store until it reaches expected_count steps or timeout. Logs
    progress. Returns the final count; raises TimeoutError on timeout."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = read_axis(_open_read(bucket, prefix), concat_dim).size
        except Exception as exc:  # store mid-commit / transient
            logger.debug("[%s] poll read error: %s", label, exc)
        logger.info("[%s] %s/%s steps", label, last, expected_count)
        if last == expected_count:
            return last
        if last is not None and last > expected_count:
            raise AssertionError(
                f"{label}: store overshot to {last} steps (expected {expected_count}); "
                "a granule was likely duplicated.")
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
    return resp["MessageId"]


# --------------------------------------------------------------------------- #
# CREATE (direct, not via SQS -- the handler opens existing stores only)
# --------------------------------------------------------------------------- #
def create_stores(bucket, s3_prefix, https_prefix, urls, auth, concat_dim, data_vars, coords):
    logger.info("Building CREATE VDS from %d granule(s) (serial, may take minutes)...", len(urls))
    vds, source_bucket = build_vds(urls, auth, concat_dim, data_vars, coords)
    logger.info("CREATE VDS shape: %s", dict(vds.sizes))

    url_map_s3 = url_map_from_vds(vds, concat_dim)

    repo_s3 = _repo_create_s3(bucket, s3_prefix, source_bucket)
    s3_sess = repo_s3.writable_session("main")
    vds.virtualize.to_icechunk(s3_sess.store)
    write_source_url(s3_sess, url_map_s3, concat_dim)
    s3_sess.commit(f"CREATE: initial store ({len(urls)} steps) with source_url")

    repo_https = _repo_create_https(bucket, https_prefix, source_bucket)
    vds_https = vds.vz.rename_paths(s3_to_https_url)
    https_sess = repo_https.writable_session("main")
    vds_https.virtualize.to_icechunk(https_sess.store)
    url_map_https = {t: s3_to_https_url(u) for t, u in url_map_s3.items()}
    write_source_url(https_sess, url_map_https, concat_dim)
    https_sess.commit(f"CREATE: initial store ({len(urls)} steps) with source_url")
    return url_map_s3


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
    ap.add_argument("--initial-count", type=int, default=DEFAULT_INITIAL,
                    help="Granules in the initial directly-created store")
    ap.add_argument("--append-count", type=int, default=DEFAULT_APPEND,
                    help="Granules appended via SQS afterward")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH,
                    help="Granules per SQS message (1 = one message per granule)")
    ap.add_argument("--seed", type=int, default=0, help="Shuffle seed for append order")
    ap.add_argument("--timeout", type=int, default=1800,
                    help="Max seconds to wait for the full append to land")
    ap.add_argument("--interval", type=int, default=15, help="Poll interval seconds")
    ap.add_argument("--recreate", action="store_true",
                    help="Empty the scratch prefixes first if they exist")
    ap.add_argument("--keep", action="store_true",
                    help="Leave the scratch stores in S3 after the run")
    ap.add_argument("--plan-only", action="store_true",
                    help="Print the computed layout and exit (no SQS/S3)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    concat_dim = args.concat_dim
    initial, append, batch = args.initial_count, args.append_count, args.batch_size
    if initial < 1 or append < 1 or batch < 1:
        ap.error("--initial-count, --append-count and --batch-size must all be >= 1")
    total = initial + append
    s3_prefix, https_prefix = test_prefixes(args.test_prefix_base, args.collection)

    auth = earthaccess.login()
    urls = discover_granules(args.collection, args.provider, total)
    create_urls = urls[:initial]
    append_idx = list(range(initial, total))

    # Shuffle the append order, then chunk into batches -> out-of-order delivery.
    random.Random(args.seed).shuffle(append_idx)
    batches = [append_idx[i:i + batch] for i in range(0, len(append_idx), batch)]

    print(f"\nCollection : {args.collection}")
    print(f"Store      : s3://{args.store_bucket}/{s3_prefix}")
    print(f"             s3://{args.store_bucket}/{https_prefix}")
    print(f"Initial    : {initial} granule(s)  (direct CREATE)")
    print(f"Append     : {append} granule(s) via {len(batches)} SQS message(s) "
          f"of up to {batch}, shuffled (seed={args.seed})")
    print(f"Final steps: {total}")

    if args.plan_only:
        print("\n--- PLAN ONLY (no SQS/S3) ---")
        print(f"  CREATE positions 0..{initial - 1}")
        for bi, b in enumerate(batches):
            print(f"  MSG {bi:>3}: positions {b}")
        return

    if not args.queue_url:
        ap.error("--queue-url is required (or use --plan-only)")

    s3 = boto3.client("s3", region_name="us-west-2")
    if args.recreate:
        _delete_prefix(s3, args.store_bucket, s3_prefix)
        _delete_prefix(s3, args.store_bucket, https_prefix)

    sqs = boto3.client("sqs", region_name="us-west-2")

    passed = False
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # ----- CREATE -----
            print(f"\n{'=' * 72}\nCREATE  {initial} granule(s)\n{'=' * 72}")
            create_map = create_stores(
                args.store_bucket, s3_prefix, https_prefix, create_urls, auth,
                concat_dim, args.data_vars, args.coords)
            axis, _ = assert_store_ok(args.store_bucket, s3_prefix, concat_dim, "S3")
            assert axis.size == initial, f"CREATE made {axis.size} steps, expected {initial}"
            print(f"  -> {axis.size} step(s); axis sorted; source_url aligned")

            # ----- APPEND via many SQS messages -----
            print(f"\n{'=' * 72}\nAPPEND  {append} granule(s) via {len(batches)} message(s)\n{'=' * 72}")
            t0 = time.time()
            for bi, b in enumerate(batches):
                mid = send_sqs(sqs, args.queue_url, args.collection,
                               [urls[p] for p in b], s3_prefix)
                logger.info("sent msg %d/%d (%d granule(s)) -> %s",
                            bi + 1, len(batches), len(b), mid)
            logger.info("all %d message(s) sent in %.1fs; polling for %d steps...",
                        len(batches), time.time() - t0, total)

            poll_until(args.store_bucket, s3_prefix, concat_dim, total,
                       args.timeout, args.interval, "S3")
            poll_until(args.store_bucket, https_prefix, concat_dim, total,
                       args.timeout, args.interval, "HTTPS")
            elapsed = time.time() - t0
            print(f"  -> all {append} granule(s) landed in {elapsed:.1f}s "
                  f"({append / elapsed:.2f} granule/s)")

            # ----- FINAL validation -----
            print(f"\n{'=' * 72}\nVALIDATE\n{'=' * 72}")
            s3_axis, s3_map = assert_store_ok(args.store_bucket, s3_prefix, concat_dim, "S3")
            _, https_map = assert_store_ok(args.store_bucket, https_prefix, concat_dim, "HTTPS")
            assert s3_axis.size == total, f"final has {s3_axis.size} steps, expected {total}"
            assert_parity(s3_map, https_map, "FINAL")

            # Every granule we fed in is present exactly once, with no extras.
            expected_urls = set(urls)
            got_urls = set(s3_map.values())
            missing = expected_urls - got_urls
            extra = got_urls - expected_urls
            assert not missing, f"{len(missing)} fed granule(s) missing from store, e.g. {next(iter(missing))}"
            assert not extra, f"{len(extra)} unexpected url(s) in store, e.g. {next(iter(extra))}"

            print(f"  -> {s3_axis.size} step(s); axis strictly increasing")
            print(f"  -> source_url: one per step, no blanks, no duplicate URL")
            print(f"  -> all {total} fed granules present exactly once (no loss/extras)")
            print(f"  -> S3 and HTTPS stores in parity")
            print("\nPASS scale append")
            passed = True
    finally:
        if passed and not args.keep:
            _delete_prefix(s3, args.store_bucket, s3_prefix)
            _delete_prefix(s3, args.store_bucket, https_prefix)
        elif not args.keep:
            print("\n(Left scratch stores in place for inspection after failure; "
                  "re-run with --recreate to reset.)")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
