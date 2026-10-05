#!/usr/bin/env python3
"""
Delete granules from an existing Icechunk v2 virtual Zarr store on S3.

A granule is removed by its coordinate value along the concat dimension. You can
pass the coordinate values directly (--times) or let the tool read them from the
granules themselves by S3 URL (--granules): each granule's virtual dataset is
opened and its concat-dim value(s) looked up, so the match uses the exact
encoded value the store holds.

Deletion is metadata-only -- later chunks are shifted down and the arrays
shrunk; the source granule files in S3 are NOT touched.

Usage:
    # Delete by granule S3 URL (mapped to coordinate automatically)
    python delete_granules.py \
        --store-bucket my-bucket \
        --store-prefix virtual_collections/MUR25-.../MUR25.icechunk_v2.s3/ \
        --granules s3://.../granule1.nc s3://.../granule2.nc \
        --concat-dim time

    # Just show the URL -> coordinate mapping, delete nothing
    python delete_granules.py ... --granules s3://.../g.nc --dry-run

    # Delete by raw coordinate value directly
    python delete_granules.py ... --times 1072915200 1073001600
"""

import argparse
import logging
import sys
import warnings
from datetime import datetime, timezone

import earthaccess
import numpy as np

from icechunk_pipeline.append_granules import build_vds, open_repo_s3
from icechunk_pipeline.icechunk_append import read_store_coordinate, delete_coordinates


def map_urls_to_coordinates(urls, auth, concat_dim="time",
                            data_vars="minimal", coords="minimal"):
    """Return {url: [encoded coordinate value(s)]} by reading each granule's VDS.

    Built one granule at a time so the mapping is unambiguous even if a granule
    carries more than one step along ``concat_dim``.
    """
    mapping = {}
    for url in urls:
        vds, _ = build_vds([url], auth, concat_dim=concat_dim,
                           data_vars=data_vars, coords=coords)
        if concat_dim not in vds.coords:
            raise SystemExit(f"{url}: no '{concat_dim}' coordinate in granule")
        mapping[url] = np.asarray(vds[concat_dim].values).tolist()
    return mapping


def main():
    ap = argparse.ArgumentParser(
        description="Delete granules from an Icechunk v2 store on S3",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    ap.add_argument("--store-bucket", required=True)
    ap.add_argument("--store-prefix", required=True)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--granules", nargs="+", help="S3 URLs of granules to delete")
    g.add_argument("--times", nargs="+", help="Raw concat-dim coordinate values to delete")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--vcc-bucket", help="s3://<source-bucket> for the virtual chunk "
                    "container (defaults to the bucket of the first --granules URL)")
    ap.add_argument("--cpu-count", type=int, default=8)
    ap.add_argument("--memory-limit", default="4GB")
    ap.add_argument("--dry-run", action="store_true",
                    help="Resolve/print what would be deleted, but do not write")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    auth = earthaccess.login()

    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=args.cpu_count, threads_per_worker=1,
                           memory_limit=args.memory_limit, silence_logs=logging.ERROR)
    client = Client(cluster)
    client.run(lambda t: (__import__("os").environ.__setitem__("EARTHDATA_TOKEN", t),
                          __import__("earthaccess").login(strategy="environment")),
               auth.token["access_token"])

    try:
        # Resolve the coordinate values to delete.
        if args.granules:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                mapping = map_urls_to_coordinates(args.granules, auth, args.concat_dim)
            logging.info("Granule -> %s mapping:", args.concat_dim)
            for url, vals in mapping.items():
                logging.info("  %s -> %s", url, vals)
            target_values = [v for vals in mapping.values() for v in vals]
            source_bucket = args.granules[0].split("/")[2]
        else:
            target_values = [float(t) if "." in t else int(t) for t in args.times]
            source_bucket = None
            logging.info("Deleting %s values: %s", args.concat_dim, target_values)

        if not target_values:
            logging.info("No coordinate values resolved; nothing to delete.")
            return

        if args.dry_run:
            logging.info("Dry run — not opening the store or deleting anything.")
            return

        vcc_bucket = args.vcc_bucket or (f"s3://{source_bucket}" if source_bucket else None)
        if vcc_bucket is None:
            raise SystemExit("--times requires --vcc-bucket (s3://<source-bucket>) to "
                             "open the store's virtual chunk container.")

        repo = open_repo_s3(args.store_bucket, args.store_prefix, vcc_bucket)
        logging.info("Opened store: s3://%s/%s", args.store_bucket, args.store_prefix)

        session = repo.writable_session("main")
        before = read_store_coordinate(session.store, args.concat_dim)
        logging.info("Store '%s' before: %d step(s)",
                     args.concat_dim, 0 if before is None else before.size)

        summary = delete_coordinates(session, target_values, dimension=args.concat_dim)
        if summary["n_deleted"] == 0:
            logging.info("No matching coordinates in store; no commit made.")
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = f"Delete {summary['n_deleted']} granule(s) at {ts}"
        session.commit(msg)
        logging.info("Committed: %s (%d step(s) remain)", msg, summary["remaining"])

        after = read_store_coordinate(repo.readonly_session("main").store, args.concat_dim)
        logging.info("Store '%s' after: %d step(s)", args.concat_dim,
                     0 if after is None else after.size)
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
