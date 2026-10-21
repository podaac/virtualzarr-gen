#!/usr/bin/env python3
"""
Append throughput / latency performance test against the live SQS pipeline.

Seeds a fresh SCRATCH store for one collection with a large initial VDS
(--seed-count granules, written directly in batches the way the real generator
does), then appends the remaining granules through the REAL SQS FIFO queue ->
deployed Lambda, timing each commit. Reports per-batch latency and overall
append throughput.

This measures the END-TO-END append path (SQS visibility + Lambda cold/warm
start + virtual-reference build + two-store commit), not just raw compute.

Self-contained: external libraries only, inlines the helpers, treats the
deployed handler as a black box reached through SQS. The real production store
is never touched -- everything goes under --store-prefix-base.

MUST run in us-west-2 (direct-S3 granule reads are region-scoped), against a
deployed handler consuming --queue-url.

Usage:
    # Seed 1000 MUR25 granules, then append the next 500 in batches of 50
    python test_perf_append.py --queue-url "$QUEUE_URL" \
        --collection MUR25-JPL-L4-GLOB-v04.2 \
        --seed-count 1000 --append-count 500 --append-batch 50

    # Seed 1000, then append the ENTIRE rest of the collection in batches of 50
    python test_perf_append.py --queue-url "$QUEUE_URL" \
        --collection MUR25-JPL-L4-GLOB-v04.2 \
        --seed-count 1000 --append-all --append-batch 50

    # Seed only; no SQS
    python test_perf_append.py --seed-count 1000 --append-count 0 --keep
"""

import argparse
import json
import logging
import os
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

# Reuse the deployed handler's source_url maintenance so the seed store is shaped
# exactly like a generator-produced store (one source_url per step). Without it
# the handler warns "N step(s) have no known URL" for the seeded steps.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from source_url_coord import reconcile_source_urls, url_map_from_vds
    _HAVE_SOURCE_URL = True
except Exception:  # keep the perf test runnable even if the module moves
    _HAVE_SOURCE_URL = False

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


# --------------------------------------------------------------------------- #
# Store reads
# --------------------------------------------------------------------------- #
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


def store_exists(bucket, prefix):
    try:
        open_read(bucket, prefix)
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Granule discovery
# --------------------------------------------------------------------------- #
def granule_start(g):
    try:
        return g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
    except Exception:
        try:
            return g["umm"]["TemporalExtent"]["SingleDateTime"]
        except Exception:
            return ""


def discover_granules(collection, provider, total=None):
    """Return time-sorted granule S3 URLs. `total=None` fetches the whole
    collection (every granule); otherwise at most `total`."""
    kwargs = dict(short_name=collection, provider=provider, cloud_hosted=True)
    if total is not None:
        kwargs["count"] = total
    results = earthaccess.search_data(**kwargs)
    results = sorted(results, key=granule_start)
    return [g.data_links(access="direct")[0] for g in results]


# --------------------------------------------------------------------------- #
# VDS build + scratch seed (direct write, no SQS)
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
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        to_delete.extend({"Key": o["Key"]} for o in page.get("Contents", []))
    for i in range(0, len(to_delete), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete[i:i + 1000]})
    if to_delete:
        logger.info("cleared %d object(s) under s3://%s/%s", len(to_delete), bucket, prefix)


def _seed_source_url(session, vds, concat_dim, to_https):
    """Reconcile the source_url coordinate for a seed batch, matching the
    deployed handler. `to_https` rewrites the S3 urls to archive URLs for the
    HTTPS store. No-op if the source_url module isn't importable."""
    if not _HAVE_SOURCE_URL:
        return
    url_map = url_map_from_vds(vds, concat_dim=concat_dim)
    if to_https:
        url_map = {t: s3_to_https_url(u) for t, u in url_map.items()}
    reconcile_source_urls(session, url_map, concat_dim=concat_dim)


def seed_stores(bucket, s3_prefix, https_prefix, urls, auth, concat_dim,
                data_vars, coords, seed_batch):
    """Build the seed store directly, in sub-batches (mirrors the generator's
    batched build). First batch creates the repos; the rest append directly.
    Each batch also writes the source_url coordinate so the seed matches a
    generator-produced store. Returns (step_count, elapsed_seconds)."""
    t0 = time.time()
    repo_s3 = repo_https = None
    for i, chunk in enumerate(chunks(urls, seed_batch)):
        vds, source_bucket = build_vds(chunk, auth, concat_dim, data_vars, coords)
        vds_https = vds.vz.rename_paths(s3_to_https_url)
        logger.info("seed batch %d: %d granule(s)", i, len(chunk))

        if i == 0:
            storage_s3 = icechunk.s3_storage(bucket=bucket, prefix=s3_prefix, region="us-west-2")
            cfg_s3 = icechunk.RepositoryConfig.default()
            cfg_s3.set_virtual_chunk_container(icechunk.VirtualChunkContainer(
                url_prefix=f"s3://{source_bucket}/",
                store=icechunk.s3_store(region="us-west-2", anonymous=True)))
            repo_s3 = icechunk.Repository.create(storage_s3, cfg_s3)

            storage_https = icechunk.s3_storage(bucket=bucket, prefix=https_prefix, region="us-west-2")
            cfg_https = icechunk.RepositoryConfig.default()
            cfg_https.set_virtual_chunk_container(icechunk.VirtualChunkContainer(
                f"https://{https_host_for_bucket(source_bucket)}/{source_bucket}/",
                icechunk.http_store()))
            repo_https = icechunk.Repository.create(storage_https, cfg_https)

            s = repo_s3.writable_session("main")
            vds.vz.to_icechunk(s.store)
            _seed_source_url(s, vds, concat_dim, to_https=False)
            s.commit(f"seed batch 0: {len(chunk)}")
            h = repo_https.writable_session("main")
            vds_https.vz.to_icechunk(h.store)
            _seed_source_url(h, vds, concat_dim, to_https=True)
            h.commit(f"seed batch 0: {len(chunk)}")
        else:
            s = repo_s3.writable_session("main")
            vds.vz.to_icechunk(s.store, append_dim=concat_dim)
            _seed_source_url(s, vds, concat_dim, to_https=False)
            s.commit(f"seed batch {i}: {len(chunk)}")
            h = repo_https.writable_session("main")
            vds_https.vz.to_icechunk(h.store, append_dim=concat_dim)
            _seed_source_url(h, vds, concat_dim, to_https=True)
            h.commit(f"seed batch {i}: {len(chunk)}")

    elapsed = time.time() - t0
    return count_steps(bucket, s3_prefix, concat_dim), elapsed


# --------------------------------------------------------------------------- #
# SQS + polling
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--queue-url", help="SQS FIFO queue URL (required unless --append-count 0)")
    ap.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--collection", default=DEFAULT_COLLECTION)
    ap.add_argument("--provider", default="POCLOUD")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--data-vars", default="minimal")
    ap.add_argument("--coords", default="minimal")
    ap.add_argument("--seed-count", type=int, default=1000,
                    help="Granules in the initial (direct) seed store (default: 1000)")
    ap.add_argument("--seed-batch", type=int, default=200,
                    help="Granules per direct build/commit while seeding (default: 200)")
    ap.add_argument("--append-count", type=int, default=500,
                    help="Granules to append via SQS after seeding (default: 500; 0 = seed only). "
                         "Ignored when --append-all is set.")
    ap.add_argument("--append-all", action="store_true",
                    help="Append EVERY remaining granule in the collection after the seed "
                         "(discovers the whole collection; overrides --append-count).")
    ap.add_argument("--append-batch", type=int, default=50,
                    help="Granules per SQS message / commit during append (default: 50). "
                         "Use 1 for one-granule-per-message like CNM-R.")
    ap.add_argument("--burst", action="store_true",
                    help="CNM-R-like load: send every granule as its own message up front "
                         "(no waiting between), then poll until the store drains. Measures "
                         "end-to-end drain throughput instead of per-batch latency. "
                         "Forces one granule per message.")
    ap.add_argument("--store-prefix-base", default=DEFAULT_SCRATCH_BASE)
    ap.add_argument("--timeout", type=int, default=900, help="Per-batch poll seconds (default: 900)")
    ap.add_argument("--interval", type=int, default=5, help="Poll interval seconds (default: 5)")
    ap.add_argument("--recreate", action="store_true", help="Empty the scratch prefixes first")
    ap.add_argument("--keep", action="store_true", help="Leave the scratch stores after the run")
    ap.add_argument("--auth-strategy", default="netrc", choices=["netrc", "environment"])
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    if (args.append_all or args.append_count > 0) and not args.queue_url:
        ap.error("--queue-url is required unless appending nothing (--append-count 0)")

    auth = earthaccess.login(strategy=args.auth_strategy)

    s3_prefix = store_prefix_s3(args.collection, args.store_prefix_base)
    https_prefix = store_prefix_https(args.collection, args.store_prefix_base)

    if args.append_all:
        logger.info("discovering the WHOLE collection %s ...", args.collection)
        urls = discover_granules(args.collection, args.provider, total=None)
    else:
        total_needed = args.seed_count + args.append_count
        logger.info("discovering up to %d granules for %s ...", total_needed, args.collection)
        urls = discover_granules(args.collection, args.provider, total=total_needed)

    if len(urls) < args.seed_count:
        raise SystemExit(f"need >= {args.seed_count} granules to seed, found {len(urls)}")
    seed_urls = urls[:args.seed_count]
    if args.append_all:
        append_urls = urls[args.seed_count:]  # everything after the seed
    else:
        append_urls = urls[args.seed_count:args.seed_count + args.append_count]

    print(f"\nCollection : {args.collection}")
    print(f"Store      : s3://{args.store_bucket}/{s3_prefix}")
    print(f"Seed       : {len(seed_urls)} granule(s) (direct, batch {args.seed_batch})")
    print(f"Append     : {len(append_urls)} granule(s) via SQS (batch {args.append_batch})")

    s3 = boto3.client("s3", region_name="us-west-2")
    if args.recreate or store_exists(args.store_bucket, s3_prefix):
        delete_prefix(s3, args.store_bucket, s3_prefix)
        delete_prefix(s3, args.store_bucket, https_prefix)

    # --- Seed phase ---
    seed_steps, seed_secs = seed_stores(
        args.store_bucket, s3_prefix, https_prefix, seed_urls, auth,
        args.concat_dim, args.data_vars, args.coords, args.seed_batch)
    seed_rate = seed_steps / seed_secs if seed_secs else 0
    print(f"\n[SEED] {seed_steps} steps in {seed_secs:.1f}s  ({seed_rate:.1f} granules/s)")

    if not append_urls:
        print("\n(append-count 0: seed only)")
        if not args.keep:
            delete_prefix(s3, args.store_bucket, s3_prefix)
            delete_prefix(s3, args.store_bucket, https_prefix)
        return

    # --- Append phase (via live SQS -> Lambda) ---
    sqs = boto3.client("sqs", region_name="us-west-2")
    batch_latencies = []
    running = seed_steps
    append_start = time.time()

    if args.burst:
        # CNM-R-like: fire every granule as its own message with no waiting,
        # then poll until the store drains. Measures end-to-end drain, not
        # per-message latency.
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
        batch_size = 1 if args.append_batch < 1 else args.append_batch
        for i, chunk in enumerate(chunks(append_urls, batch_size)):
            expected = running + len(chunk)
            t_send = time.time()
            msg_id = send(sqs, args.queue_url, args.collection, chunk, s3_prefix)
            logger.info("append batch %d: sent %d granule(s) MessageId=%s", i, len(chunk), msg_id)
            poll_until(args.store_bucket, s3_prefix, args.concat_dim, expected, args.timeout, args.interval)
            poll_until(args.store_bucket, https_prefix, args.concat_dim, expected, args.timeout, args.interval)
            latency = time.time() - t_send
            batch_latencies.append((len(chunk), latency))
            running = expected
            print(f"  batch {i:3d}: +{len(chunk):4d} -> {running:5d} steps in {latency:6.1f}s "
                  f"({len(chunk) / latency:5.1f} granules/s)")

    append_secs = time.time() - append_start
    appended = running - seed_steps

    # --- Final assertions + report ---
    axis = read_axis(args.store_bucket, s3_prefix, args.concat_dim)
    monotonic = axis.size <= 1 or bool(np.all(np.diff(axis) > 0))
    s3_final = count_steps(args.store_bucket, s3_prefix, args.concat_dim)
    https_final = count_steps(args.store_bucket, https_prefix, args.concat_dim)

    lat = [l for _n, l in batch_latencies]
    print(f"\n{'=' * 60}\nPERFORMANCE SUMMARY\n{'=' * 60}")
    print(f"  seed:        {seed_steps} granules in {seed_secs:.1f}s ({seed_rate:.1f}/s)")
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
    if not args.keep:
        if ok:
            delete_prefix(s3, args.store_bucket, s3_prefix)
            delete_prefix(s3, args.store_bucket, https_prefix)
        else:
            print("\n(kept scratch stores for inspection after non-clean result)")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
