#!/usr/bin/env python3
"""
Step 3: Verify both S3 and HTTPS Icechunk stores — show current state or watch for updates.

Usage:
    # One-shot: show current state of both stores
    python test_e2e_verify_store.py

    # Watch mode: poll every 15s and show when time steps change
    python test_e2e_verify_store.py --watch

    # Watch with custom interval
    python test_e2e_verify_store.py --watch --interval 10

    # Show full dataset details
    python test_e2e_verify_store.py --verbose
"""

import argparse
import logging
import time
from datetime import datetime, timezone

import earthaccess
import icechunk
import xarray as xr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
STORE_PREFIX_S3 = "virtual_collections/_test_e2e/MUR25_test.icechunk_v2.s3/"
STORE_PREFIX_HTTPS = "virtual_collections/_test_e2e/MUR25_test.icechunk_v2.https/"


def open_store(store_bucket, prefix):
    storage = icechunk.s3_storage(
        bucket=store_bucket, prefix=prefix, region="us-west-2",
    )
    config = icechunk.Repository.fetch_config(storage)
    repo = icechunk.Repository.open(storage, config=config)
    session = repo.readonly_session(branch="main")
    return repo, xr.open_zarr(session.store, consolidated=False)


def print_store_status(ds, store_bucket, prefix, store_type, verbose=False):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    time_count = ds.sizes["time"]
    print(f"\n[{now}] [{store_type}] s3://{store_bucket}/{prefix}")
    print(f"  Time steps: {time_count}")
    print(f"  Dimensions: {dict(ds.sizes)}")
    print(f"  Variables:  {list(ds.data_vars)}")

    if "time" in ds.coords:
        time_vals = ds.coords["time"].values
        print(f"  Time range: {time_vals[0]} → {time_vals[-1]}")

    if verbose:
        print(f"\n  Full dataset:\n{ds}")


def print_commit_log(repo, store_type, limit=10):
    print(f"\n  [{store_type}] Recent commits (last {limit}):")
    try:
        for i, ancestor in enumerate(repo.ancestry(branch="main")):
            if i >= limit:
                break
            print(f"    [{ancestor.written_at}] {ancestor.message}")
    except Exception as e:
        print(f"    (Could not read ancestry: {e})")


def verify_both(store_bucket, verbose=False, commit_limit=10):
    results = {}
    for store_type, prefix in [("S3", STORE_PREFIX_S3), ("HTTPS", STORE_PREFIX_HTTPS)]:
        try:
            repo, ds = open_store(store_bucket, prefix)
            print_store_status(ds, store_bucket, prefix, store_type, verbose=verbose)
            print_commit_log(repo, store_type, limit=commit_limit)
            results[store_type] = ds.sizes["time"]
            ds.close()
        except Exception as e:
            logging.error("[%s] Error reading store: %s", store_type, e)
            results[store_type] = None

    s3_count = results.get("S3")
    https_count = results.get("HTTPS")
    if s3_count is not None and https_count is not None:
        if s3_count == https_count:
            print(f"\n  ✓ Both stores in sync: {s3_count} time steps")
        else:
            print(f"\n  ✗ MISMATCH: S3={s3_count} vs HTTPS={https_count}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Verify both S3 and HTTPS test Icechunk stores")
    parser.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--watch", action="store_true", help="Poll for changes")
    parser.add_argument("--interval", type=int, default=15, help="Seconds between polls (default: 15)")
    parser.add_argument("--verbose", action="store_true", help="Show full dataset info")
    args = parser.parse_args()

    earthaccess.login(strategy="netrc")

    if not args.watch:
        verify_both(args.store_bucket, verbose=args.verbose)
        return

    print(f"Watching both stores every {args.interval}s... (Ctrl+C to stop)")
    last_counts = {"S3": None, "HTTPS": None}

    try:
        while True:
            try:
                changed = False
                for store_type, prefix in [("S3", STORE_PREFIX_S3), ("HTTPS", STORE_PREFIX_HTTPS)]:
                    try:
                        repo, ds = open_store(args.store_bucket, prefix)
                        current_count = ds.sizes["time"]

                        if current_count != last_counts[store_type]:
                            if last_counts[store_type] is not None:
                                diff = current_count - last_counts[store_type]
                                print(f"\n{'=' * 50}")
                                print(f"  [{store_type}] CHANGE DETECTED: +{diff} time step(s)")
                                print(f"{'=' * 50}")

                            print_store_status(ds, args.store_bucket, prefix, store_type,
                                               verbose=args.verbose)
                            print_commit_log(repo, store_type, limit=5)
                            last_counts[store_type] = current_count
                            changed = True

                        ds.close()
                    except Exception as e:
                        logging.error("[%s] Error: %s", store_type, e)

                if not changed:
                    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    s3_c = last_counts["S3"] or "?"
                    https_c = last_counts["HTTPS"] or "?"
                    print(f"[{now}] No change (S3={s3_c}, HTTPS={https_c})", end="\r")

            except Exception as e:
                logging.error("Error: %s", e)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopped watching.")


if __name__ == "__main__":
    main()
