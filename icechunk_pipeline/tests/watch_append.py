#!/usr/bin/env python3
"""
Watch an Icechunk append store grow in real time.

Polls the S3 (and HTTPS) store for a collection and prints step count, time
range, and growth rate each time it changes -- handy while test_perf_append.py
or a live ingest is appending.

Defaults target the perf-test scratch store (virtual_collections/_perf_append/).
Point --store-prefix-base at the real path or another scratch base as needed.

Usage:
    # watch the perf scratch store for MUR25
    python watch_append.py --collection MUR25-JPL-L4-GLOB-v04.2

    # watch the real production store
    python watch_append.py --collection MUR25-JPL-L4-GLOB-v04.2 \
        --store-prefix-base "virtual_collections/{c}/"

    # one-shot snapshot, then exit
    python watch_append.py --collection MUR25-JPL-L4-GLOB-v04.2 --once
"""

import argparse
import time
from datetime import datetime, timezone

import icechunk
import numpy as np
import xarray as xr

DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
DEFAULT_COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_SCRATCH_BASE = "virtual_collections/_perf_append/"


def prefixes(collection, base):
    """`base` may contain '{c}' for the collection; otherwise it is a flat base
    like 'virtual_collections/_perf_append/'."""
    if "{c}" in base:
        stem = base.format(c=collection) + collection
    else:
        stem = f"{base}{collection}"
    return f"{stem}.icechunk_v2.s3/", f"{stem}.icechunk_v2.https/"


def read_state(bucket, prefix, concat_dim):
    """Return (count, first, last) for the store, or None if unreadable."""
    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-west-2")
    try:
        config = icechunk.Repository.fetch_config(storage)
        repo = icechunk.Repository.open(storage, config=config)
        store = repo.readonly_session(branch="main").store
        ds = xr.open_zarr(store, consolidated=False, decode_times=False)
    except Exception:
        return None
    try:
        if concat_dim not in ds.sizes:
            return None
        axis = np.asarray(ds[concat_dim].values)
        n = int(axis.size)
        return n, (axis[0].item() if n else None), (axis[-1].item() if n else None)
    finally:
        ds.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--collection", default=DEFAULT_COLLECTION)
    ap.add_argument("--store-prefix-base", default=DEFAULT_SCRATCH_BASE,
                    help="Prefix base; use '{c}' for the collection, e.g. "
                         "'virtual_collections/{c}/' for the real store path.")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--interval", type=int, default=10, help="Poll seconds (default: 10)")
    ap.add_argument("--once", action="store_true", help="Print one snapshot and exit")
    args = ap.parse_args()

    s3_prefix, https_prefix = prefixes(args.collection, args.store_prefix_base)
    stores = [("S3", s3_prefix), ("HTTPS", https_prefix)]

    print(f"collection : {args.collection}")
    print(f"bucket     : {args.store_bucket}")
    print(f"s3   prefix: {s3_prefix}")
    print(f"https prefix: {https_prefix}")
    if not args.once:
        print(f"watching every {args.interval}s (Ctrl+C to stop)\n")

    last = {"S3": None, "HTTPS": None}
    last_change_t = {"S3": time.time(), "HTTPS": time.time()}

    def snapshot():
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        changed = False
        for label, prefix in stores:
            st = read_state(args.store_bucket, prefix, args.concat_dim)
            if st is None:
                print(f"[{now}] [{label}] (store not readable yet)")
                continue
            n, first, lastv = st
            prev = last[label]
            if prev is None or n != prev:
                rate = ""
                if prev is not None and n > prev:
                    dt = time.time() - last_change_t[label]
                    rate = f"  (+{n - prev} in {dt:.0f}s = {(n - prev) / dt:.2f}/s)"
                    last_change_t[label] = time.time()
                print(f"[{now}] [{label:5s}] {n} steps  range=[{first} … {lastv}]{rate}")
                last[label] = n
                changed = True
        if not changed and not args.once:
            s3c = last["S3"]
            hc = last["HTTPS"]
            print(f"[{now}] no change (S3={s3c}, HTTPS={hc})", end="\r", flush=True)

    if args.once:
        snapshot()
        return

    try:
        while True:
            snapshot()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopped.")


if __name__ == "__main__":
    main()
