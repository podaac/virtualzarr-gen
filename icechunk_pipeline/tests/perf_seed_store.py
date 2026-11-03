#!/usr/bin/env python3
"""
Seed an Icechunk store with a large initial VDS (direct write, no SQS).

Creates a fresh scratch S3 + HTTPS store for one collection by writing
--seed-count granules in --seed-batch sub-batches, timing the whole build.

MUST run in us-west-2 (direct-S3 granule reads are region-scoped).

Usage:
    # Seed 1000 MUR25 granules
    python perf_seed_store.py --collection MUR25-JPL-L4-GLOB-v04.2 --seed-count 1000

    # Seed only 10, keep the store for inspection
    python perf_seed_store.py --seed-count 10 --keep

    # Wipe and re-seed an existing scratch store
    python perf_seed_store.py --seed-count 1000 --recreate

After seeding, run perf_append_store.py with the same --collection and
--store-prefix-base to append more granules through the live SQS pipeline.
"""

import argparse
import logging
import os
import sys
import time
import warnings
from urllib.parse import urlparse

import boto3
import earthaccess
import icechunk
import xarray as xr
import virtualizarr as vz
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from source_url_coord import reconcile_source_urls, url_map_from_vds
    _HAVE_SOURCE_URL = True
except Exception:
    _HAVE_SOURCE_URL = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("perf-seed")

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


def store_exists(bucket, prefix):
    try:
        open_read(bucket, prefix)
        return True
    except Exception:
        return False


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
    if not _HAVE_SOURCE_URL:
        return
    url_map = url_map_from_vds(vds, concat_dim=concat_dim)
    if to_https:
        url_map = {t: s3_to_https_url(u) for t, u in url_map.items()}
    reconcile_source_urls(session, url_map, concat_dim=concat_dim)


def seed_stores(bucket, s3_prefix, https_prefix, urls, auth, concat_dim,
                data_vars, coords, seed_batch):
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


def granule_start(g):
    try:
        return g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
    except Exception:
        try:
            return g["umm"]["TemporalExtent"]["SingleDateTime"]
        except Exception:
            return ""


def discover_granules(collection, provider, total):
    results = earthaccess.search_data(
        short_name=collection, provider=provider, cloud_hosted=True, count=total)
    results = sorted(results, key=granule_start)
    return [g.data_links(access="direct")[0] for g in results]


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--collection", default=DEFAULT_COLLECTION)
    ap.add_argument("--provider", default="POCLOUD")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--data-vars", default="minimal")
    ap.add_argument("--coords", default="minimal")
    ap.add_argument("--seed-count", type=int, default=1000,
                    help="Granules to write into the initial store (default: 1000)")
    ap.add_argument("--seed-batch", type=int, default=200,
                    help="Granules per direct build/commit (default: 200)")
    ap.add_argument("--store-prefix-base", default=DEFAULT_SCRATCH_BASE)
    ap.add_argument("--recreate", action="store_true",
                    help="Delete the scratch prefixes before seeding")
    ap.add_argument("--keep", action="store_true",
                    help="Leave the scratch stores even if seeding fails")
    ap.add_argument("--auth-strategy", default="netrc", choices=["netrc", "environment"])
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    auth = earthaccess.login(strategy=args.auth_strategy)

    s3_prefix = store_prefix_s3(args.collection, args.store_prefix_base)
    https_prefix = store_prefix_https(args.collection, args.store_prefix_base)

    logger.info("discovering %d granules for %s ...", args.seed_count, args.collection)
    urls = discover_granules(args.collection, args.provider, total=args.seed_count)
    if len(urls) < args.seed_count:
        raise SystemExit(f"need >= {args.seed_count} granules to seed, found {len(urls)}")

    print(f"\nCollection : {args.collection}")
    print(f"Store      : s3://{args.store_bucket}/{s3_prefix}")
    print(f"Seed       : {len(urls)} granule(s) (direct, batch {args.seed_batch})")

    s3 = boto3.client("s3", region_name="us-west-2")
    if args.recreate or store_exists(args.store_bucket, s3_prefix):
        delete_prefix(s3, args.store_bucket, s3_prefix)
        delete_prefix(s3, args.store_bucket, https_prefix)

    seed_steps, seed_secs = seed_stores(
        args.store_bucket, s3_prefix, https_prefix, urls, auth,
        args.concat_dim, args.data_vars, args.coords, args.seed_batch)
    seed_rate = seed_steps / seed_secs if seed_secs else 0

    print(f"\n[SEED] {seed_steps} steps in {seed_secs:.1f}s  ({seed_rate:.1f} granules/s)")
    print(f"\nTo append more granules via SQS, run:")
    print(f"  python perf_append_store.py --queue-url $QUEUE_URL "
          f"--collection {args.collection} "
          f"--store-prefix-base {args.store_prefix_base} "
          f"--append-count <N>")


if __name__ == "__main__":
    main()
