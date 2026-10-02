#!/usr/bin/env python3
"""
Staged append test against a REAL PO.DAAC collection on S3.

Creates a fresh throwaway Icechunk store from real granules, then walks it
through every write mode and prints the store's time axis after each commit:

  STAGE 1  generate    granules at sorted positions [0, 2, 4]  -> create
  STAGE 2  tail-append granules at [6, 8]                      -> append
  STAGE 3  re-deliver  granules at [2, 6] again                -> region (idempotent)
  STAGE 4  gap-insert  granules at [1, 3, 5, 7]                -> insert (reindex)

Final axis holds all 9 granules in strictly-increasing time order, with the
odd-position granules inserted between the originals.

MUST run on an EC2 in us-west-2: granules are read by direct S3 link and the
Earthdata S3 credentials are region-scoped.

Example:
    python real_collection_append_test.py \
        --collection MUR25-JPL-L4-GLOB-v04.2 \
        --store-bucket podaac-sit-services-cloud-optimizer \
        --store-prefix virtual_collections/_test_staged/MUR25_staged.icechunk_v2.s3/

The store prefix MUST NOT already exist -- pick a scratch path. Delete it from
S3 when you are done.
"""

import argparse
import logging
import sys
import warnings
from datetime import datetime, timezone

import earthaccess
import icechunk
import numpy as np
import xarray as xr

from append_granules import build_vds
from icechunk_append import read_store_coordinate, build_write_plan, apply_write_plan
from podaac.collection_config import get_collection_config, get_preprocess_fn

# positions (into the time-sorted granule list) written at each stage
STAGES = [
    ("generate", [0, 2, 4]),
    ("tail-append", [6, 8]),
    ("re-deliver", [2, 6]),
    ("gap-insert", [1, 3, 5, 7]),
]
N_GRANULES = 9


def granule_start(g):
    """Beginning datetime string from a granule's UMM, for time ordering."""
    try:
        rng = g["umm"]["TemporalExtent"]["RangeDateTime"]
        return rng["BeginningDateTime"]
    except Exception:
        # single date-time granules
        try:
            return g["umm"]["TemporalExtent"]["SingleDateTime"]
        except Exception:
            return ""


def create_fresh_repo(store_bucket, store_prefix, vcc_bucket):
    storage = icechunk.s3_storage(bucket=store_bucket, prefix=store_prefix, region="us-west-2")
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=vcc_bucket + "/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )
    try:
        return icechunk.Repository.create(storage, config)
    except Exception as e:
        raise SystemExit(
            f"Could not create a fresh store at s3://{store_bucket}/{store_prefix}: {e}\n"
            "Pick a store prefix that does not already exist (this test refuses to "
            "write into an existing store)."
        )


def show_store(repo, title, concat_dim):
    """Print the committed store's coordinate axis and commit history.

    Reads only the materialized concat-dim coordinate (native icechunk chunks),
    never the virtual data, so inspection needs no source-granule access.
    """
    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store, consolidated=False, decode_times=False)
    coord = np.asarray(ds[concat_dim].values)
    sizes = dict(ds.sizes)
    ds.close()

    sorted_ok = bool(np.all(np.diff(coord) > 0)) if coord.size > 1 else True
    history = [c.message for c in repo.ancestry(branch="main")]

    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
    print(f"  sizes          : {sizes}")
    print(f"  {concat_dim} count     : {coord.size}")
    print(f"  {concat_dim}[:5]       : {coord[:5].tolist()}")
    print(f"  {concat_dim}[-5:]      : {coord[-5:].tolist()}")
    print(f"  strictly ↑     : {sorted_ok}")
    print(f"  commits        : {history}")
    if not sorted_ok:
        raise SystemExit("Store axis is NOT strictly increasing — aborting.")
    return coord


def write_stage(repo, mode_label, urls, auth, cfg):
    vds, _ = build_vds(
        urls, auth,
        preprocess=cfg["preprocess"],
        concat_dim=cfg["concat_dim"],
        data_vars=cfg["data_vars"],
        coords=cfg["coords"],
    )
    session = repo.writable_session("main")
    existing = read_store_coordinate(session.store, cfg["concat_dim"])
    plan = build_write_plan(vds, existing, dimension=cfg["concat_dim"])
    logging.info("[%s] plan mode=%s new=%d present=%d",
                 mode_label, plan.mode, plan.n_new, plan.n_region)
    if plan.is_empty:
        logging.info("[%s] nothing to write", mode_label)
        return plan
    apply_write_plan(session, vds, plan)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    session.commit(f"[{mode_label}:{plan.mode}] {ts}")
    return plan


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--collection", required=True, help="CMR short_name")
    ap.add_argument("--store-bucket", required=True, help="S3 bucket for the new store")
    ap.add_argument("--store-prefix", required=True,
                    help="S3 prefix for the new store (MUST NOT exist; use a scratch path)")
    ap.add_argument("--provider", default="POCLOUD")
    ap.add_argument("--cpu-count", type=int, default=8)
    ap.add_argument("--memory-limit", default="4GB")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    config = get_collection_config(args.collection)
    cfg = {
        "concat_dim": config["concat_dim"],
        "data_vars": config["data_vars"],
        "coords": config["coords"],
        "preprocess": get_preprocess_fn(config),
    }
    logging.info("Collection config: %s", {k: v for k, v in cfg.items() if k != "preprocess"})

    auth = earthaccess.login()

    logging.info("Searching %s for granules...", args.collection)
    results = earthaccess.search_data(
        short_name=args.collection, provider=args.provider, cloud_hosted=True,
    )
    if len(results) < N_GRANULES:
        raise SystemExit(f"Need at least {N_GRANULES} granules, found {len(results)}.")

    results = sorted(results, key=granule_start)[:N_GRANULES]
    urls = [g.data_links(access="direct")[0] for g in results]
    logging.info("Using %d time-sorted granules:", N_GRANULES)
    for i, (g, u) in enumerate(zip(results, urls)):
        logging.info("  [%d] %s  %s", i, granule_start(g), u)

    source_bucket = urls[0].split("/")[2]
    vcc_bucket = f"s3://{source_bucket}"

    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=args.cpu_count, threads_per_worker=1,
                           memory_limit=args.memory_limit, silence_logs=logging.ERROR)
    client = Client(cluster)

    def _silence(token):
        import warnings, logging, os, earthaccess as ea
        if token:
            os.environ["EARTHDATA_TOKEN"] = token
            ea.login(strategy="environment")
        warnings.filterwarnings("ignore")
        for n in ["distributed", "xarray", "py.warnings", "fsspec", "h5py"]:
            logging.getLogger(n).setLevel(logging.ERROR)

    client.run(_silence, auth.token["access_token"])

    try:
        logging.info("Creating fresh store: s3://%s/%s", args.store_bucket, args.store_prefix)
        repo = create_fresh_repo(args.store_bucket, args.store_prefix, vcc_bucket)

        for mode_label, positions in STAGES:
            stage_urls = [urls[p] for p in positions]
            logging.info("--- STAGE %s: positions %s ---", mode_label, positions)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                plan = write_stage(repo, mode_label, stage_urls, auth, cfg)
            show_store(repo, f"STAGE  {mode_label}  positions={positions}  -> mode={plan.mode!r}",
                       cfg["concat_dim"])

        print(f"\nAll stages complete. Store at s3://{args.store_bucket}/{args.store_prefix}")
        print("Remember to delete this scratch prefix from S3 when done.")
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
