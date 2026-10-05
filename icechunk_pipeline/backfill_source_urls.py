#!/usr/bin/env python3
"""
One-time backfill of the ``source_url`` coordinate for an EXISTING Icechunk v2
store that was written before ``source_url_coord`` was wired in.

Without ``source_url`` the store cannot self-heal a shifted-time reprocess (a
granule re-delivered under a new time leaves an untraceable orphan). This script
reconstructs the ``time -> URL`` link for every step already in the store, so
later appends through the wired paths can retire moved granules correctly.

Where the URLs come from
------------------------
By default the store's own manifest is read (``all_virtual_chunk_locations``) to
discover the source URLs it references; each is opened once to read its
concat-dim value. Alternatively pass an explicit granule list with ``--granules``
/ ``--granule-file`` (e.g. the full collection from CMR). Steps for which no URL
is resolved are left as "" and reported.

MUST run where the source granules are readable: on an EC2 in us-west-2 for
PO.DAAC direct-S3 granules (the manifest URLs are ``s3://``).

Usage:
    python backfill_source_urls.py \
        --store-bucket podaac-sit-services-cloud-optimizer \
        --store-prefix virtual_collections/MUR25-.../MUR25.icechunk_v2.s3/ \
        --concat-dim time

    # show the resolved mapping, write nothing
    python backfill_source_urls.py ... --dry-run
"""

import argparse
import logging
import warnings
from datetime import datetime, timezone

import earthaccess
import icechunk

from icechunk_pipeline.sqs_append_granules import build_vds
from icechunk_pipeline.icechunk_append import read_store_coordinate
from icechunk_pipeline.source_url_coord import (
    url_map_from_granules,
    reconcile_source_urls,
    read_source_url_map,
)


def open_repo(store_bucket, store_prefix, vcc_bucket=None):
    """Open the store. The virtual chunk container is only needed to read chunk
    *data*; backfill reads materialized coordinates and manifest locations only,
    so a vcc is optional (set it if provided to be safe)."""
    storage = icechunk.s3_storage(bucket=store_bucket, prefix=store_prefix,
                                  region="us-west-2")
    config = icechunk.Repository.fetch_config(storage)
    if vcc_bucket:
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                url_prefix=vcc_bucket + "/",
                store=icechunk.s3_store(region="us-west-2", anonymous=True),
            )
        )
    return icechunk.Repository.open(storage, config=config)


def main():
    ap = argparse.ArgumentParser(
        description="Backfill source_url for an existing Icechunk v2 store",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    ap.add_argument("--store-bucket", required=True)
    ap.add_argument("--store-prefix", required=True)
    ap.add_argument("--concat-dim", default="time")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--granules", nargs="+", help="Explicit source URLs to map "
                   "(default: read the store's manifest)")
    g.add_argument("--granule-file", help="File of source URLs, one per line")
    ap.add_argument("--vcc-bucket", help="s3://<source-bucket> (optional)")
    ap.add_argument("--cpu-count", type=int, default=8)
    ap.add_argument("--memory-limit", default="4GB")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    auth = earthaccess.login()

    repo = open_repo(args.store_bucket, args.store_prefix, args.vcc_bucket)
    logging.info("Opened store: s3://%s/%s", args.store_bucket, args.store_prefix)

    axis = read_store_coordinate(repo.readonly_session("main").store, args.concat_dim)
    if axis is None or axis.size == 0:
        raise SystemExit(f"Store has no '{args.concat_dim}' coordinate; nothing to backfill.")
    logging.info("Store has %d step(s) along '%s'", axis.size, args.concat_dim)

    already = read_source_url_map(repo.readonly_session("main").store, args.concat_dim)
    if already:
        logging.info("Store already carries source_url for %d step(s); "
                     "backfill will refresh/extend it.", len(already))

    # Collect the source URLs to map.
    if args.granule_file:
        with open(args.granule_file) as f:
            urls = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    elif args.granules:
        urls = list(args.granules)
    else:
        urls = list(repo.readonly_session("main").all_virtual_chunk_locations())
        logging.info("Read %d referenced URL(s) from the store manifest.", len(urls))
        non_s3 = [u for u in urls if not u.startswith("s3://")]
        if non_s3:
            logging.warning("%d manifest URL(s) are not s3:// (e.g. %s). This "
                            "store is likely an HTTPS store; its granules cannot "
                            "be read directly here. Pass --granules with the s3 "
                            "URLs instead.", len(non_s3), non_s3[0])

    if not urls:
        raise SystemExit("No source URLs to map.")

    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=args.cpu_count, threads_per_worker=1,
                           memory_limit=args.memory_limit, silence_logs=logging.ERROR)
    client = Client(cluster)
    client.run(lambda t: (__import__("os").environ.__setitem__("EARTHDATA_TOKEN", t),
                          __import__("earthaccess").login(strategy="environment")),
               auth.token["access_token"])

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            url_map = url_map_from_granules(urls, auth, concat_dim=args.concat_dim)

        covered = sum(1 for t in axis.tolist() if t in url_map)
        logging.info("Resolved %d URL(s) -> %d coordinate value(s); %d of %d "
                     "store step(s) will be covered.",
                     len(urls), len(url_map), covered, axis.size)

        if args.dry_run:
            logging.info("Dry run — resolved mapping (first 10):")
            for u, t in list({v: k for k, v in url_map.items()}.items())[:10]:
                logging.info("  %s -> %s=%s", u, args.concat_dim, t)
            logging.info("Dry run — not writing.")
            return

        session = repo.writable_session("main")
        summary = reconcile_source_urls(session, url_map, concat_dim=args.concat_dim)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = (f"Backfill source_url: {summary['n_steps']} step(s), "
               f"{summary['n_missing']} without a URL, at {ts}")
        session.commit(msg)
        logging.info("Committed: %s", msg)
        if summary["n_missing"]:
            logging.warning("%d step(s) still have no URL. Supply more granules "
                            "with --granules to cover them.", summary["n_missing"])
    finally:
        warnings.filterwarnings("ignore")
        logging.disable(logging.CRITICAL)
        for closer in (lambda: client.shutdown(timeout=30), lambda: cluster.close(timeout=30)):
            try:
                closer()
            except Exception:
                pass
        logging.disable(logging.NOTSET)


if __name__ == "__main__":
    main()
