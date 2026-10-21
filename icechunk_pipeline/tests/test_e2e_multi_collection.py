#!/usr/bin/env python3
"""
Multi-collection append test for the live SQS -> deployed-handler pipeline.

Drives SEVERAL collections at once through the real SQS FIFO queue to prove the
deployed append handler fans out correctly across collections (each collection
is its own FIFO MessageGroupId, so they append independently and in order).

For every collection the script:
  1. reads the current step count of its live S3 store,
  2. discovers that many + N granules via earthaccess and sends the next N NEW
     ones to the queue (MessageGroupId=collection), and
  3. (unless --no-verify) polls the S3 and HTTPS stores until each has grown by
     N steps, then asserts both stores agree on the count.

It prints PASS / FAIL / SKIP per collection and a final summary; exits non-zero
if any collection failed.

Self-contained: uses only external libraries (icechunk, xarray, boto3,
earthaccess) and inlines the store-path helpers, mirroring the other e2e tests.
It treats the deployed handler as a black box reached only through SQS.

The target store must ALREADY EXIST for each collection (the handler opens, it
never creates). By default messages carry no store_prefix, so the handler writes
to the real per-collection store path
    virtual_collections/{collection}/{collection}.icechunk_v2.{s3,https}/
exactly as production CNM-R ingestion would. Use --store-prefix-base to target
scratch stores instead.

MUST run in us-west-2 (direct-S3 granule reads are region-scoped), against a
deployed handler consuming --queue-url.

Two modes:
  * default  -- append to each collection's EXISTING store (real paths unless
                --store-prefix-base is given). Mutates those stores.
  * --create -- self-bootstrap a fresh scratch store per collection (seed via
                direct write), append via SQS, verify, then clean up. Nothing
                pre-exists and the real production stores are never touched --
                the multi-collection analogue of test_e2e_all_scenarios.

Usage:
    # Self-contained: fresh scratch store per collection, seed 3 + append 2, cleanup
    python test_e2e_multi_collection.py --queue-url "$QUEUE_URL" --create \
        --collections MUR25-JPL-L4-GLOB-v04.2 OSTIA-UKMO-L4-GLOB-REP-v2.0 \
        --seed-count 3 --count 2

    # Append 2 new granules each to the EXISTING real stores, then verify
    python test_e2e_multi_collection.py --queue-url "$QUEUE_URL" \
        --collections MUR25-JPL-L4-GLOB-v04.2 OSTIA-UKMO-L4-GLOB-REP-v2.0 --count 2

    # One SQS message per granule instead of one batch per collection
    python test_e2e_multi_collection.py --queue-url "$QUEUE_URL" --no-batch

    # Just show what would be sent/seeded, no SQS/S3 writes
    python test_e2e_multi_collection.py --create --plan-only
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
import virtualizarr as vz
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("e2e-multi")

DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
DEFAULT_COLLECTIONS = [
    "MUR25-JPL-L4-GLOB-v04.2",
    "OSTIA-UKMO-L4-GLOB-REP-v2.0",
]
DEFAULT_SCRATCH_BASE = "virtual_collections/_test_multi/"

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
# Store paths (dot-form, matching collection_config.get_store_prefix_*)
# --------------------------------------------------------------------------- #
def store_prefix_s3(collection, base=None):
    if base:
        return f"{base}{collection}.icechunk_v2.s3/"
    return f"virtual_collections/{collection}/{collection}.icechunk_v2.s3/"


def store_prefix_https(collection, base=None):
    if base:
        return f"{base}{collection}.icechunk_v2.https/"
    return f"virtual_collections/{collection}/{collection}.icechunk_v2.https/"


# --------------------------------------------------------------------------- #
# Store reads
# --------------------------------------------------------------------------- #
def open_read(bucket, prefix):
    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-west-2")
    config = icechunk.Repository.fetch_config(storage)
    repo = icechunk.Repository.open(storage, config=config)
    return repo.readonly_session(branch="main").store


def count_steps(bucket, prefix, concat_dim):
    """Current step count along concat_dim, or raise if the store is missing."""
    ds = xr.open_zarr(open_read(bucket, prefix), consolidated=False, decode_times=False)
    try:
        if concat_dim not in ds.sizes:
            raise KeyError(f"store has no '{concat_dim}' dimension (dims={dict(ds.sizes)})")
        return int(ds.sizes[concat_dim])
    finally:
        ds.close()


def read_axis(bucket, prefix, concat_dim):
    """Return the concat_dim coordinate values (undecoded) as a numpy array."""
    ds = xr.open_zarr(open_read(bucket, prefix), consolidated=False, decode_times=False)
    try:
        return np.asarray(ds[concat_dim].values)
    finally:
        ds.close()


def store_exists(bucket, prefix):
    try:
        open_read(bucket, prefix)
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Scratch store bootstrap (direct write; no SQS -- the handler opens, never
# creates, so a scratch store must be seeded directly first). Mirrors
# test_e2e_all_scenarios.create_stores, kept inline for standalone use.
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


def delete_prefix(s3, bucket, prefix):
    """Empty a scratch prefix so a fresh store can be created (scratch-only)."""
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        to_delete.extend({"Key": o["Key"]} for o in page.get("Contents", []))
    for i in range(0, len(to_delete), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete[i:i + 1000]})
    if to_delete:
        logger.info("cleared %d object(s) under s3://%s/%s", len(to_delete), bucket, prefix)


def create_scratch_stores(bucket, s3_prefix, https_prefix, urls, auth,
                          concat_dim, data_vars, coords):
    """Seed fresh S3 + HTTPS scratch stores directly from `urls`."""
    vds, source_bucket = build_vds(urls, auth, concat_dim, data_vars, coords)
    logger.info("seed VDS shape: %s", dict(vds.sizes))

    storage_s3 = icechunk.s3_storage(bucket=bucket, prefix=s3_prefix, region="us-west-2")
    config_s3 = icechunk.RepositoryConfig.default()
    config_s3.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=f"s3://{source_bucket}/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )
    repo_s3 = icechunk.Repository.create(storage_s3, config_s3)
    sess = repo_s3.writable_session("main")
    vds.vz.to_icechunk(sess.store)
    sess.commit(f"seed: {vds.sizes[concat_dim]} granule(s)")

    storage_https = icechunk.s3_storage(bucket=bucket, prefix=https_prefix, region="us-west-2")
    config_https = icechunk.RepositoryConfig.default()
    config_https.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            f"https://{https_host_for_bucket(source_bucket)}/{source_bucket}/",
            icechunk.http_store(),
        )
    )
    repo_https = icechunk.Repository.create(storage_https, config_https)
    sess_h = repo_https.writable_session("main")
    vds.vz.rename_paths(s3_to_https_url).vz.to_icechunk(sess_h.store)
    sess_h.commit(f"seed: {vds.sizes[concat_dim]} granule(s)")
    return int(vds.sizes[concat_dim])


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


def search_new_granules(collection, provider, count, skip):
    """Return up to `count` granule S3 URLs after the `skip` offset (i.e. the
    ones not yet in a store that already holds `skip` steps).

    Results are time-sorted (by UMM BeginningDateTime) so granules are sent in
    chronological order. SQS FIFO preserves per-collection send order, and the
    handler appends in that order; sending unsorted would otherwise leave the
    store's time axis non-monotonic for sort=False collections.
    """
    results = earthaccess.search_data(
        short_name=collection,
        provider=provider,
        cloud_hosted=True,
        count=skip + count,
    )
    results = sorted(results, key=granule_start)
    urls = [g.data_links(access="direct")[0] for g in results]
    return urls[skip:]


# --------------------------------------------------------------------------- #
# SQS
# --------------------------------------------------------------------------- #
def send(sqs, queue_url, collection, urls, batch, store_prefix=None):
    """Send granule URLs for one collection. One batch message by default, or
    one message per granule when batch=False. MessageGroupId=collection keeps
    each collection's ordering independent in the FIFO queue."""
    sent = []
    payloads = [urls] if batch else [[u] for u in urls]
    for granules in payloads:
        body = {"collection": collection, "granules": granules}
        if store_prefix:
            body["store_prefix"] = store_prefix
        resp = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(body),
            MessageGroupId=collection,
            MessageDeduplicationId=str(uuid.uuid4()),
        )
        sent.append(resp["MessageId"])
        logger.info("[%s] sent %d granule(s) -> MessageId=%s",
                    collection, len(granules), resp["MessageId"])
    return sent


# --------------------------------------------------------------------------- #
# Verification
# --------------------------------------------------------------------------- #
def poll_until(bucket, prefix, concat_dim, expected, timeout, interval, label):
    """Poll a store until it reaches `expected` steps or timeout. Returns the
    final count; raises TimeoutError on timeout."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = count_steps(bucket, prefix, concat_dim)
        except Exception as exc:  # store mid-commit / transient
            logger.debug("[%s] poll read error: %s", label, exc)
        if last == expected:
            return last
        time.sleep(interval)
    raise TimeoutError(f"{label}: waited {timeout}s for {expected} steps, last saw {last}")


# --------------------------------------------------------------------------- #
# Per-collection driver
# --------------------------------------------------------------------------- #
def run_collection(collection, *, bucket, prefix_base, concat_dim, provider,
                   count, batch, sqs, queue_url, verify, timeout, interval,
                   send_store_prefix, create, seed_count, recreate, data_vars,
                   coords, auth, s3, shuffle):
    s3_prefix = store_prefix_s3(collection, prefix_base)
    https_prefix = store_prefix_https(collection, prefix_base)

    print(f"\n{'=' * 72}\nCOLLECTION {collection}\n{'=' * 72}")
    print(f"  store: s3://{bucket}/{s3_prefix}")

    if create:
        # Self-bootstrap a fresh scratch store, then append via SQS. Pull enough
        # granules for both the seed and the append in one search.
        all_urls = search_new_granules(collection, provider, seed_count + count, skip=0)
        if len(all_urls) < seed_count + count:
            raise RuntimeError(
                f"need {seed_count + count} granules, found {len(all_urls)}")
        seed_urls, urls = all_urls[:seed_count], all_urls[seed_count:]

        if recreate or store_exists(bucket, s3_prefix):
            delete_prefix(s3, bucket, s3_prefix)
            delete_prefix(s3, bucket, https_prefix)
        print(f"  seeding scratch store with {seed_count} granule(s)...")
        before_s3 = create_scratch_stores(bucket, s3_prefix, https_prefix, seed_urls,
                                          auth, concat_dim, data_vars, coords)
    else:
        if not store_exists(bucket, s3_prefix):
            raise RuntimeError(
                f"store does not exist (generate it first, or use --create): "
                f"s3://{bucket}/{s3_prefix}")
        before_s3 = count_steps(bucket, s3_prefix, concat_dim)
        urls = search_new_granules(collection, provider, count, skip=before_s3)

    before_https = count_steps(bucket, https_prefix, concat_dim) if store_exists(bucket, https_prefix) else None
    print(f"  before: S3={before_s3} steps, HTTPS={before_https} steps")

    if not urls:
        raise RuntimeError(f"no new granules found after offset {before_s3}")

    if shuffle and len(urls) > 1:
        # Deliberately send out of chronological order to exercise the handler's
        # warn-and-append path. The handler appends at the end without
        # reordering, so the store is EXPECTED to end up non-monotonic.
        ordered = list(urls)
        while urls == ordered:  # ensure the order actually changes
            random.shuffle(urls)
        print(f"  SHUFFLE: sending {len(urls)} granule(s) OUT OF ORDER:")
    else:
        print(f"  sending {len(urls)} new granule(s):")
    for u in urls:
        print(f"    {u}")

    # store_prefix in the message: None => handler uses the real default path.
    msg_prefix = s3_prefix if send_store_prefix else None
    send(sqs, queue_url, collection, urls, batch, store_prefix=msg_prefix)

    if not verify:
        print(f"  -> sent (verification skipped)")
        return {"before": before_s3, "sent": len(urls), "after_s3": None, "after_https": None}

    expected = before_s3 + len(urls)
    after_s3 = poll_until(bucket, s3_prefix, concat_dim, expected, timeout, interval, "S3")
    after_https = poll_until(bucket, https_prefix, concat_dim, expected, timeout, interval, "HTTPS")
    assert after_s3 == after_https, f"store mismatch: S3={after_s3} HTTPS={after_https}"

    axis = read_axis(bucket, s3_prefix, concat_dim)
    monotonic = axis.size <= 1 or bool(np.all(np.diff(axis) > 0))
    if shuffle:
        # Out-of-order send: all granules must have landed (count check above
        # already proved that), but ordering is informational — the handler
        # warns and appends without reordering, so expect non-monotonic.
        state = "monotonic" if monotonic else "NON-monotonic (handler did not reorder, as expected)"
        print(f"  -> {after_s3} steps (was {before_s3}, +{len(urls)}); "
              f"S3/HTTPS in parity; {concat_dim} {state}")
    else:
        # In-order send: the append dim MUST be strictly increasing.
        assert monotonic, f"{concat_dim} not strictly increasing after append: {axis.tolist()}"
        print(f"  -> {after_s3} steps (was {before_s3}, +{len(urls)}); "
              f"S3/HTTPS in parity; {concat_dim} monotonic")
    return {"before": before_s3, "sent": len(urls), "after_s3": after_s3, "after_https": after_https}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--queue-url", help="SQS FIFO queue URL (required unless --plan-only)")
    ap.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--collections", nargs="+", default=DEFAULT_COLLECTIONS,
                    help="Collection short names to drive (space-separated)")
    ap.add_argument("--count", type=int, default=2,
                    help="New granules to append per collection (default: 2)")
    ap.add_argument("--provider", default="POCLOUD")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--no-batch", dest="batch", action="store_false",
                    help="Send one SQS message per granule instead of one batch per collection")
    ap.add_argument("--create", action="store_true",
                    help="Self-bootstrap a fresh scratch store per collection (direct seed "
                         "write), append via SQS, then clean up. No pre-existing store needed "
                         "and the real production stores are never touched.")
    ap.add_argument("--seed-count", type=int, default=3,
                    help="Granules to seed each scratch store with in --create mode (default: 3)")
    ap.add_argument("--data-vars", default="minimal")
    ap.add_argument("--coords", default="minimal")
    ap.add_argument("--recreate", action="store_true",
                    help="In --create mode, empty the scratch prefixes first if present")
    ap.add_argument("--keep", action="store_true",
                    help="In --create mode, leave the scratch stores in S3 after the run")
    ap.add_argument("--store-prefix-base", default=None,
                    help="Prefix base for scratch stores, e.g. 'virtual_collections/_test_multi/'. "
                         "Default: real per-collection path, or the scratch base in --create mode.")
    ap.add_argument("--send-store-prefix", action="store_true",
                    help="Include store_prefix in the SQS message (default: omit, so the "
                         "handler derives the default path). Implied when --store-prefix-base is set.")
    ap.add_argument("--shuffle", action="store_true",
                    help="Deliberately send the append granules OUT OF chronological order to "
                         "exercise the handler's warn-and-append path. Ordering becomes "
                         "informational (expected non-monotonic); count + parity still asserted.")
    ap.add_argument("--random-seed", type=int, default=None,
                    help="Seed the RNG used by --shuffle for a reproducible order")
    ap.add_argument("--no-verify", dest="verify", action="store_false",
                    help="Send only; do not poll the stores afterward")
    ap.add_argument("--timeout", type=int, default=300, help="Per-store poll seconds (default: 300)")
    ap.add_argument("--interval", type=int, default=15, help="Poll interval seconds (default: 15)")
    ap.add_argument("--auth-strategy", default="netrc", choices=["netrc", "environment"])
    ap.add_argument("--plan-only", action="store_true",
                    help="Show what would be sent (no SQS/S3 writes)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    auth = earthaccess.login(strategy=args.auth_strategy)

    if args.random_seed is not None:
        random.seed(args.random_seed)

    # In --create mode default to the scratch base so we never touch real stores.
    if args.create and args.store_prefix_base is None:
        args.store_prefix_base = DEFAULT_SCRATCH_BASE

    # Scratch stores must be addressed explicitly in the message.
    send_store_prefix = args.send_store_prefix or (args.store_prefix_base is not None)

    print(f"\nQueue       : {args.queue_url or '(plan-only)'}")
    print(f"Bucket      : {args.store_bucket}")
    print(f"Collections : {', '.join(args.collections)}")
    print(f"Per-coll N  : {args.count}   batch={args.batch}   verify={args.verify}")

    if args.plan_only:
        print("\n--- PLAN ONLY (no SQS/S3) ---")
        for collection in args.collections:
            s3_prefix = store_prefix_s3(collection, args.store_prefix_base)
            print(f"\n  {collection}")
            print(f"    scratch: s3://{args.store_bucket}/{s3_prefix}")
            if args.create:
                all_urls = search_new_granules(collection, args.provider,
                                               args.seed_count + args.count, skip=0)
                seed, append = all_urls[:args.seed_count], all_urls[args.seed_count:]
                print(f"    would seed ({len(seed)}):")
                for u in seed:
                    print(f"      {u}")
                print(f"    would append via SQS ({len(append)}):")
                for u in append:
                    print(f"      {u}")
            else:
                exists = store_exists(args.store_bucket, s3_prefix)
                before = count_steps(args.store_bucket, s3_prefix, args.concat_dim) if exists else None
                print(f"    store exists: {exists}  before={before}")
                urls = search_new_granules(collection, args.provider, args.count,
                                           skip=before or 0) if exists else []
                for u in urls:
                    print(f"    would send: {u}")
                if not exists:
                    print("    SKIP (store missing; use --create to self-bootstrap)")
        return

    if not args.queue_url:
        ap.error("--queue-url is required (or use --plan-only)")

    sqs = boto3.client("sqs", region_name="us-west-2")
    s3 = boto3.client("s3", region_name="us-west-2")

    results = {}
    for collection in args.collections:
        try:
            results[collection] = ("PASS", run_collection(
                collection,
                bucket=args.store_bucket, prefix_base=args.store_prefix_base,
                concat_dim=args.concat_dim, provider=args.provider,
                count=args.count, batch=args.batch, sqs=sqs, queue_url=args.queue_url,
                verify=args.verify, timeout=args.timeout, interval=args.interval,
                send_store_prefix=send_store_prefix,
                create=args.create, seed_count=args.seed_count, recreate=args.recreate,
                data_vars=args.data_vars, coords=args.coords, auth=auth, s3=s3,
                shuffle=args.shuffle,
            ))
            print(f"COLLECTION {collection}: PASS")
        except Exception as exc:
            results[collection] = (f"FAIL ({exc})", None)
            print(f"COLLECTION {collection}: FAIL -- {exc}")
            # Independent collections: keep going so one bad store doesn't mask others.

    # Scratch cleanup: remove created stores unless --keep (leave failures for
    # inspection so a FAIL can be debugged).
    if args.create and not args.keep:
        for collection in args.collections:
            if results.get(collection, ("", None))[0] != "PASS":
                print(f"  (kept scratch store for {collection} after non-pass for inspection)")
                continue
            delete_prefix(s3, args.store_bucket, store_prefix_s3(collection, args.store_prefix_base))
            delete_prefix(s3, args.store_bucket, store_prefix_https(collection, args.store_prefix_base))

    print(f"\n{'=' * 72}\nSUMMARY\n{'=' * 72}")
    for collection in args.collections:
        status, info = results.get(collection, ("-", None))
        detail = ""
        if info and info.get("after_s3") is not None:
            detail = f"  {info['before']} -> {info['after_s3']} (+{info['sent']})"
        elif info:
            detail = f"  sent {info['sent']} (unverified)"
        print(f"  {collection:48s} {status}{detail}")

    failed = [c for c, (s, _i) in results.items() if s.startswith("FAIL")]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
