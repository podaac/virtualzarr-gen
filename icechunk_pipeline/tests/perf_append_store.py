#!/usr/bin/env python3
"""
Append granules to an existing Icechunk scratch store via the live SQS pipeline.

Reads the current step count of the store, discovers that many + N more granules,
skips the ones already seeded, then sends the rest through the REAL SQS FIFO
queue -> deployed Lambda, timing each commit. Reports per-batch latency and
overall append throughput.

Run perf_seed_store.py first to create the initial store with the same
--collection and --store-prefix-base values.

MUST run in us-west-2 (direct-S3 granule reads are region-scoped), against a
deployed handler consuming --queue-url.

Usage:
    # Append 500 more granules in batches of 50
    python perf_append_store.py --queue-url "$QUEUE_URL" \
        --collection MUR25-JPL-L4-GLOB-v04.2 --append-count 500

    # Append the entire rest of the collection in batches of 50
    python perf_append_store.py --queue-url "$QUEUE_URL" \
        --collection MUR25-JPL-L4-GLOB-v04.2 --append-all

    # CNM-R-like burst: fire all messages at once and measure drain time
    python perf_append_store.py --queue-url "$QUEUE_URL" \
        --collection MUR25-JPL-L4-GLOB-v04.2 --append-count 100 --burst
"""

import argparse
import json
import logging
import sys
import time
import uuid

import boto3
import earthaccess
import icechunk
import numpy as np
import xarray as xr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("perf-append")

DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
DEFAULT_COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_SCRATCH_BASE = "virtual_collections/_perf_append/"

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


def store_prefix_s3(collection, base):
    return f"{base}{collection}.icechunk_v2.s3/"


def store_prefix_https(collection, base):
    return f"{base}{collection}.icechunk_v2.https/"


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def open_read(bucket, prefix):
    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-west-2")
    config = icechunk.Repository.fetch_config(storage)
    repo = icechunk.Repository.open(storage, config=config)
    return repo.readonly_session(branch="main").store


def count_steps(bucket, prefix, concat_dim):
    ds = xr.open_zarr(open_read(bucket, prefix), consolidated=False, decode_times=False)
    try:
        return int(ds.sizes[concat_dim])
    finally:
        ds.close()


def read_axis(bucket, prefix, concat_dim):
    ds = xr.open_zarr(open_read(bucket, prefix), consolidated=False, decode_times=False)
    try:
        return np.asarray(ds[concat_dim].values)
    finally:
        ds.close()


def granule_start(g):
    try:
        return g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
    except Exception:
        try:
            return g["umm"]["TemporalExtent"]["SingleDateTime"]
        except Exception:
            return ""


def discover_granules(collection, provider, total=None):
    kwargs = dict(short_name=collection, provider=provider, cloud_hosted=True)
    if total is not None:
        kwargs["count"] = total
    results = earthaccess.search_data(**kwargs)
    results = sorted(results, key=granule_start)
    return [g.data_links(access="direct")[0] for g in results]


def _delete_prefix(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        to_delete.extend({"Key": o["Key"]} for o in page.get("Contents", []))
    for i in range(0, len(to_delete), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete[i:i + 1000]})
    if to_delete:
        logger.info("cleared %d object(s) under s3://%s/%s", len(to_delete), bucket, prefix)


def send(sqs, queue_url, collection, urls, store_prefix):
    body = {"collection": collection, "granules": urls, "store_prefix": store_prefix}
    resp = sqs.send_message(
        QueueUrl=queue_url, MessageBody=json.dumps(body),
        MessageGroupId=collection, MessageDeduplicationId=str(uuid.uuid4()),
    )
    return resp["MessageId"]


def poll_until(bucket, prefix, concat_dim, expected, timeout, interval):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = count_steps(bucket, prefix, concat_dim)
        except Exception as exc:
            logger.debug("poll read error: %s", exc)
        if last == expected:
            return last
        time.sleep(interval)
    raise TimeoutError(f"waited {timeout}s for {expected} steps, last saw {last}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--queue-url", required=True, help="SQS FIFO queue URL")
    ap.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--collection", default=DEFAULT_COLLECTION)
    ap.add_argument("--provider", default="POCLOUD")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--append-count", type=int, default=500,
                    help="Granules to append via SQS (default: 500). "
                         "Ignored when --append-all is set.")
    ap.add_argument("--append-all", action="store_true",
                    help="Append EVERY remaining granule in the collection "
                         "(discovers the whole collection; overrides --append-count).")
    ap.add_argument("--append-batch", type=int, default=50,
                    help="Granules per SQS message / commit (default: 50). "
                         "Use 1 for one-granule-per-message like CNM-R.")
    ap.add_argument("--burst", action="store_true",
                    help="CNM-R-like load: fire every granule as its own message up front "
                         "then poll until the store drains. Forces one granule per message.")
    ap.add_argument("--store-prefix-base", default=DEFAULT_SCRATCH_BASE)
    ap.add_argument("--timeout", type=int, default=900,
                    help="Per-batch poll timeout seconds (default: 900)")
    ap.add_argument("--interval", type=int, default=5,
                    help="Poll interval seconds (default: 5)")
    ap.add_argument("--keep", action="store_true",
                    help="Leave the scratch stores after the run")
    ap.add_argument("--auth-strategy", default="netrc", choices=["netrc", "environment"])
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    earthaccess.login(strategy=args.auth_strategy)

    s3_prefix = store_prefix_s3(args.collection, args.store_prefix_base)
    https_prefix = store_prefix_https(args.collection, args.store_prefix_base)

    # Read how many steps are already in the store — these are the seeded granules to skip
    logger.info("reading current store state ...")
    try:
        seed_steps = count_steps(args.store_bucket, s3_prefix, args.concat_dim)
    except Exception as exc:
        raise SystemExit(
            f"could not open store at s3://{args.store_bucket}/{s3_prefix} — "
            f"run perf_seed_store.py first.\n{exc}"
        )

    if args.append_all:
        logger.info("discovering the WHOLE collection %s ...", args.collection)
        urls = discover_granules(args.collection, args.provider, total=None)
    else:
        total_needed = seed_steps + args.append_count
        logger.info("discovering up to %d granules for %s ...", total_needed, args.collection)
        urls = discover_granules(args.collection, args.provider, total=total_needed)

    append_urls = urls[seed_steps:]
    if not args.append_all:
        append_urls = append_urls[:args.append_count]

    if not append_urls:
        raise SystemExit(
            f"no new granules to append — store already has {seed_steps} steps "
            f"and only {len(urls)} granules were discovered."
        )

    print(f"\nCollection : {args.collection}")
    print(f"Store      : s3://{args.store_bucket}/{s3_prefix}")
    print(f"Baseline   : {seed_steps} steps already in store")
    print(f"Append     : {len(append_urls)} granule(s) via SQS (batch {args.append_batch})")

    sqs = boto3.client("sqs", region_name="us-west-2")
    s3 = boto3.client("s3", region_name="us-west-2")
    batch_latencies = []
    running = seed_steps
    append_start = time.time()

    if args.burst:
        final_expected = seed_steps + len(append_urls)
        t_fire = time.time()
        for u in append_urls:
            send(sqs, args.queue_url, args.collection, [u], s3_prefix)
        fire_secs = time.time() - t_fire
        print(f"  burst: fired {len(append_urls)} one-granule message(s) in {fire_secs:.1f}s; "
              f"draining...")
        poll_until(args.store_bucket, s3_prefix, args.concat_dim, final_expected,
                   args.timeout, args.interval)
        poll_until(args.store_bucket, https_prefix, args.concat_dim, final_expected,
                   args.timeout, args.interval)
        running = final_expected
    else:
        batch_size = max(1, args.append_batch)
        for i, chunk in enumerate(chunks(append_urls, batch_size)):
            expected = running + len(chunk)
            t_send = time.time()
            msg_id = send(sqs, args.queue_url, args.collection, chunk, s3_prefix)
            logger.info("append batch %d: sent %d granule(s) MessageId=%s", i, len(chunk), msg_id)
            poll_until(args.store_bucket, s3_prefix, args.concat_dim, expected,
                       args.timeout, args.interval)
            poll_until(args.store_bucket, https_prefix, args.concat_dim, expected,
                       args.timeout, args.interval)
            latency = time.time() - t_send
            batch_latencies.append((len(chunk), latency))
            running = expected
            print(f"  batch {i:3d}: +{len(chunk):4d} -> {running:5d} steps in {latency:6.1f}s "
                  f"({len(chunk) / latency:5.1f} granules/s)")

    append_secs = time.time() - append_start
    appended = running - seed_steps

    axis = read_axis(args.store_bucket, s3_prefix, args.concat_dim)
    monotonic = axis.size <= 1 or bool(np.all(np.diff(axis) > 0))
    s3_final = count_steps(args.store_bucket, s3_prefix, args.concat_dim)
    https_final = count_steps(args.store_bucket, https_prefix, args.concat_dim)

    lat = [l for _n, l in batch_latencies]
    print(f"\n{'=' * 60}\nPERFORMANCE SUMMARY\n{'=' * 60}")
    print(f"  baseline:    {seed_steps} steps already in store")
    print(f"  appended:    {appended} granules in {append_secs:.1f}s "
          f"({appended / append_secs:.2f}/s overall)")
    if args.burst:
        print(f"  mode:        burst ({len(append_urls)} one-granule messages, drain-timed)")
    else:
        print(f"  batches:     {len(batch_latencies)} × {args.append_batch} granule(s)")
    if lat:
        print(f"  per-batch s: min={min(lat):.1f} mean={sum(lat) / len(lat):.1f} max={max(lat):.1f}")
    print(f"  final store: S3={s3_final} HTTPS={https_final} "
          f"({'parity OK' if s3_final == https_final else 'MISMATCH'})")
    print(f"  {args.concat_dim}:        {'monotonic' if monotonic else 'NON-monotonic'}")

    ok = (s3_final == https_final == seed_steps + appended) and monotonic

    if not args.keep and ok:
        _delete_prefix(s3, args.store_bucket, s3_prefix)
        _delete_prefix(s3, args.store_bucket, https_prefix)
    elif not ok:
        print("\n(kept scratch stores for inspection after non-clean result)")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
