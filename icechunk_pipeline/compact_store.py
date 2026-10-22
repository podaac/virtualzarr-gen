#!/usr/bin/env python3
"""
Compact an Icechunk store: expire old snapshots + garbage-collect unreferenced
objects, reclaiming space that accrues as appends pile up version history.

Each append commits a new immutable snapshot and (via reconcile_source_urls)
rewrites the whole source_url array; old snapshots/manifests/chunks are retained
for time-travel and never freed automatically. This drops history older than a
threshold and deletes the now-unreferenced objects.

Runs on the S3 and HTTPS stores for a collection. Reports object count + bytes
before/after so you can see what was reclaimed.

DANGER: expiring + GC is destructive to version HISTORY -- after this you can no
longer read the expired snapshots. The current tip (latest data) is always kept.
Use --dry-run first.

Usage:
    # Dry run: show what GC would delete on the perf scratch store
    python compact_store.py --collection MUR25-JPL-L4-GLOB-v04.2 --dry-run

    # Keep only the last 7 days of history, then reclaim
    python compact_store.py --collection MUR25-JPL-L4-GLOB-v04.2 --older-than-days 7

    # Aggressively drop ALL history but the current tip (default: 0 days)
    python compact_store.py --collection MUR25-JPL-L4-GLOB-v04.2

    # Real production store path
    python compact_store.py --collection MUR25-JPL-L4-GLOB-v04.2 \
        --store-prefix-base "virtual_collections/{c}/"
"""

import argparse
from datetime import datetime, timedelta, timezone

import boto3
import icechunk

DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
DEFAULT_COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_SCRATCH_BASE = "virtual_collections/_perf_append/"


def prefixes(collection, base):
    """`base` may contain '{c}' for the collection, else it is a flat base."""
    if "{c}" in base:
        stem = base.format(c=collection) + collection
    else:
        stem = f"{base}{collection}"
    return f"{stem}.icechunk_v2.s3/", f"{stem}.icechunk_v2.https/"


def prefix_size(s3, bucket, prefix):
    """(object_count, total_bytes) under a prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    n = total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            n += 1
            total += o["Size"]
    return n, total


def human(nbytes):
    """Format a byte count as a human-readable string (B .. TiB)."""
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f}{unit}"
        nbytes /= 1024
    return f"{nbytes:.1f}TiB"


def compact_one(s3, bucket, prefix, cutoff, dry_run):
    """Expire + GC one store. Returns (freed_objects, freed_bytes)."""
    print(f"\n--- s3://{bucket}/{prefix} ---")
    try:
        storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-west-2")
        config = icechunk.Repository.fetch_config(storage)
        repo = icechunk.Repository.open(storage, config=config)
    except Exception as exc:
        print(f"  (skip: cannot open store: {exc})")
        return 0, 0

    n_before, bytes_before = prefix_size(s3, bucket, prefix)
    print(f"  before: {n_before} objects, {human(bytes_before)}")
    print(f"  cutoff: {cutoff.isoformat()}  (snapshots/objects older than this)")

    expired = repo.expire_snapshots(older_than=cutoff)
    print(f"  expired {len(expired)} snapshot(s)")

    summary = repo.garbage_collect(delete_object_older_than=cutoff, dry_run=dry_run)
    verb = "would delete" if dry_run else "deleted"
    print(f"  GC {verb}: {summary}")

    if dry_run:
        return 0, 0

    n_after, bytes_after = prefix_size(s3, bucket, prefix)
    freed_n, freed_b = n_before - n_after, bytes_before - bytes_after
    print(f"  after:  {n_after} objects, {human(bytes_after)}  "
          f"(freed {freed_n} objects, {human(freed_b)})")
    return freed_n, freed_b


def main():
    """Compact one or more Icechunk stores from the command line."""
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--collection", default=DEFAULT_COLLECTION)
    ap.add_argument("--store-prefix-base", default=DEFAULT_SCRATCH_BASE,
                    help="Prefix base; use '{c}' for the real path, e.g. "
                         "'virtual_collections/{c}/'. Default: perf scratch base.")
    ap.add_argument("--older-than-days", type=float, default=0.0,
                    help="Expire snapshots / collect objects older than this many days "
                         "(default: 0 = keep only the current tip).")
    ap.add_argument("--s3-only", action="store_true", help="Only the .s3 store")
    ap.add_argument("--https-only", action="store_true", help="Only the .https store")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what GC would delete without deleting")
    args = ap.parse_args()

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.older_than_days)
    s3_prefix, https_prefix = prefixes(args.collection, args.store_prefix_base)

    targets = []
    if not args.https_only:
        targets.append(("S3", s3_prefix))
    if not args.s3_only:
        targets.append(("HTTPS", https_prefix))

    print(f"collection : {args.collection}")
    print(f"bucket     : {args.store_bucket}")
    print(f"mode       : {'DRY RUN' if args.dry_run else 'DELETE'}  "
          f"older-than={args.older_than_days}d")

    s3 = boto3.client("s3", region_name="us-west-2")
    total_n = total_b = 0
    for _label, prefix in targets:
        fn, fb = compact_one(s3, args.store_bucket, prefix, cutoff, args.dry_run)
        total_n += fn
        total_b += fb

    if not args.dry_run:
        print(f"\nTOTAL freed: {total_n} objects, {human(total_b)}")
    else:
        print("\n(dry run: nothing deleted; re-run without --dry-run to reclaim)")


if __name__ == "__main__":
    main()
