#!/usr/bin/env python3
"""
Tier-2 shifted-time MOVE test against REAL S3 virtual chunks.

Validates the one reprocess case the live SQS queue cannot drive: a granule that
keeps its source URL but is re-published with a DIFFERENT time value. Detecting
that requires the *same URL* to re-decode to a *new time*, which only works on a
file you control -- so this test stages its own granule and overwrites it in
place. It does NOT go through the deployed handler/queue (which can only read
PO.DAAC buckets with Earthdata-scoped credentials).

It drives the real planner directly --
    retire_moved_urls -> build_write_plan -> apply_write_plan -> reconcile_source_urls
-- against an ``s3://`` virtual-chunk store, so the move is exercised on actual S3
references, one level above the local-filesystem self-test in
``source_url_coord.py`` (``PASS moved-time``).

Flow (an anchor granule stays put so the move is a realistic multi-step edit):
  1. stage anchor g_anchor.nc (time=TA) and mover g_mover.nc (time=T0)
  2. create an icechunk store (VCC -> your bucket, authenticated) from both;
     reconcile source_url -> store = {TA: url_A, T0: url_M}
  3. overwrite the SAME mover key with a new file whose time=T1 (the reprocess)
  4. re-read url_M -> {T1: url_M}; retire_moved_urls deletes the stale T0 step;
     write T1; reconcile source_url
  5. assert axis == [TA, T1], source_url == {TA: url_A, T1: url_M}, T0 is gone,
     no URL maps to two times, and the T1 data chunk resolves through the VCC

MUST run on an EC2 in us-west-2 with AWS credentials that can read+write
<bucket>. Scratch prefixes must not already hold a store; use --recreate to
clear them.

Example:
    python -m icechunk_pipeline.tests.real_move_test \
        --bucket podaac-sit-services-cloud-optimizer
"""

import argparse
import logging
import sys
import tempfile
import warnings
from pathlib import Path

import boto3
import icechunk
import numpy as np
import xarray as xr
import virtualizarr as vz
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

from icechunk_pipeline.icechunk_append import (
    read_store_coordinate,
    build_write_plan,
    apply_write_plan,
)
from icechunk_pipeline.source_url_coord import (
    url_map_from_vds,
    reconcile_source_urls,
    read_source_url_map,
    retire_moved_urls,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("move-test")

REGION = "us-west-2"
CONCAT_DIM = "time"
Y, X = 16, 16

DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
DEFAULT_GRANULE_PREFIX = "virtual_collections/_test_move/granules/"
DEFAULT_STORE_PREFIX = "virtual_collections/_test_move/store.icechunk_v2.s3/"

# Encoded time values (raw ints; read back with decode_times=False). TA stays,
# the mover travels T0 -> T1. T1 is after both so the rewrite is a tail append.
TA = 1_000_000_000
T0 = 1_000_043_200
T1 = 1_000_172_800


# --------------------------------------------------------------------------- #
# Staging controllable granules
# --------------------------------------------------------------------------- #
def make_granule(path, time_value):
    """Write a tiny but real chunked HDF5/NetCDF granule with one time step.

    ``foo`` is filled with ``time_value`` so a read-back proves which file's
    bytes the store resolved. Chunked (1, Y, X) so VirtualiZarr references it as
    a manifest chunk rather than inlining it.
    """
    marker = np.full((1, Y, X), np.int32(time_value), dtype="int32")
    ds = xr.Dataset(
        {"foo": ((CONCAT_DIM, "y", "x"), marker)},
        coords={CONCAT_DIM: ((CONCAT_DIM,), np.array([time_value], dtype="int64"))},
    )
    # No CF units on time -> decode_times=False yields the raw int unchanged.
    ds.to_netcdf(path, engine="h5netcdf", encoding={"foo": {"chunksizes": (1, Y, X)}})
    ds.close()


def upload(s3, local_path, bucket, key):
    s3.upload_file(str(local_path), bucket, key)
    logger.info("staged s3://%s/%s", bucket, key)


# --------------------------------------------------------------------------- #
# S3 virtual dataset build (your bucket, default AWS credentials)
# --------------------------------------------------------------------------- #
def build_vds(urls, bucket):
    store = S3Store(
        bucket=bucket, region=REGION,
        virtual_hosted_style_request=False, client_options={"allow_http": True},
    )
    registry = ObjectStoreRegistry({f"s3://{bucket}": store})
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)
        vds = vz.open_virtual_mfdataset(
            urls=urls, registry=registry, parser=HDFParser(),
            decode_times=False, combine="nested", concat_dim=CONCAT_DIM,
            data_vars="minimal", coords="minimal",
            compat="override", combine_attrs="override",
        )
    return vds


# --------------------------------------------------------------------------- #
# Icechunk store (VCC -> your bucket, authenticated via the AWS env/instance role)
# --------------------------------------------------------------------------- #
def create_repo(store_bucket, store_prefix, src_bucket):
    storage = icechunk.s3_storage(bucket=store_bucket, prefix=store_prefix, region=REGION)
    config = icechunk.RepositoryConfig.default()
    vcc_prefix = f"s3://{src_bucket}/"
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=vcc_prefix,
            store=icechunk.s3_store(region=REGION),  # authenticated, not anonymous
        )
    )
    try:
        return icechunk.Repository.create(
            storage, config,
            authorize_virtual_chunk_access={
                vcc_prefix: icechunk.credentials.s3_from_env_credentials(),
            },
        )
    except Exception as e:
        raise SystemExit(
            f"Could not create a fresh store at s3://{store_bucket}/{store_prefix}: {e}\n"
            "Pick a scratch store prefix that does not already exist, or pass --recreate."
        )


def read_axis(repo):
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False, decode_times=False)
    axis = np.asarray(ds[CONCAT_DIM].values)
    ds.close()
    return axis


def read_foo_at(repo, index):
    """foo[index, 0, 0] -- forces resolution of a virtual chunk through the VCC."""
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False, decode_times=False)
    val = int(np.asarray(ds["foo"].values)[index, 0, 0])
    ds.close()
    return val


# --------------------------------------------------------------------------- #
# S3 cleanup
# --------------------------------------------------------------------------- #
def delete_prefix(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend({"Key": o["Key"]} for o in page.get("Contents", []))
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i:i + 1000]})
    if keys:
        logger.info("cleared %d object(s) under s3://%s/%s", len(keys), bucket, prefix)


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", default=DEFAULT_BUCKET,
                    help="S3 bucket you can read+write (holds both staged granules and the store)")
    ap.add_argument("--granule-prefix", default=DEFAULT_GRANULE_PREFIX)
    ap.add_argument("--store-prefix", default=DEFAULT_STORE_PREFIX)
    ap.add_argument("--recreate", action="store_true",
                    help="Clear the scratch granule + store prefixes first")
    ap.add_argument("--keep", action="store_true",
                    help="Leave the scratch store and granules in S3 after the run")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    s3 = boto3.client("s3", region_name=REGION)
    bucket = args.bucket
    anchor_key = f"{args.granule_prefix}g_anchor.nc"
    mover_key = f"{args.granule_prefix}g_mover.nc"
    url_anchor = f"s3://{bucket}/{anchor_key}"
    url_mover = f"s3://{bucket}/{mover_key}"

    if args.recreate:
        delete_prefix(s3, bucket, args.store_prefix)
        delete_prefix(s3, bucket, args.granule_prefix)

    passed = False
    try:
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            # ----- 1. stage anchor (TA) and mover (T0) -----
            make_granule(d / "anchor.nc", TA)
            make_granule(d / "mover.nc", T0)
            upload(s3, d / "anchor.nc", bucket, anchor_key)
            upload(s3, d / "mover.nc", bucket, mover_key)

            # ----- 2. create the store from both -----
            print(f"\n{'=' * 70}\nCREATE  (TA={TA}, T0={T0})\n{'=' * 70}")
            vds_init = build_vds([url_anchor, url_mover], bucket)
            url_map_init = url_map_from_vds(vds_init, CONCAT_DIM)
            logger.info("init url map: %s", url_map_init)

            repo = create_repo(bucket, args.store_prefix, bucket)
            session = repo.writable_session("main")
            plan = build_write_plan(vds_init, read_store_coordinate(session.store, CONCAT_DIM), CONCAT_DIM)
            apply_write_plan(session, vds_init, plan)
            reconcile_source_urls(session, url_map_init, concat_dim=CONCAT_DIM)
            session.commit("create: anchor + mover")

            axis = read_axis(repo)
            smap = read_source_url_map(repo.readonly_session("main").store, CONCAT_DIM)
            assert axis.tolist() == [TA, T0], f"create axis {axis.tolist()} != [{TA}, {T0}]"
            assert smap == {TA: url_anchor, T0: url_mover}, f"create source_url wrong: {smap}"
            print(f"  axis={axis.tolist()}  source_url ok")

            # ----- 3. reprocess: overwrite the SAME mover key with time=T1 -----
            print(f"\n{'=' * 70}\nMOVE  (mover T0={T0} -> T1={T1}, same URL)\n{'=' * 70}")
            make_granule(d / "mover_reproc.nc", T1)
            upload(s3, d / "mover_reproc.nc", bucket, mover_key)  # same key

            # ----- 4. drive the move: retire stale T0, write T1, reconcile -----
            vds_move = build_vds([url_mover], bucket)
            url_map_move = url_map_from_vds(vds_move, CONCAT_DIM)
            logger.info("move url map: %s", url_map_move)
            assert url_map_move == {T1: url_mover}, f"re-read map wrong: {url_map_move}"

            session = repo.writable_session("main")
            retired = retire_moved_urls(session, url_map_move, concat_dim=CONCAT_DIM)
            assert retired["retired"] == [T0], f"expected T0 retired, got {retired}"
            print(f"  retired stale step(s): {retired['retired']}")

            existing = read_store_coordinate(session.store, CONCAT_DIM)
            plan = build_write_plan(vds_move, existing, CONCAT_DIM)
            logger.info("move plan mode=%s new=%d present=%d", plan.mode, plan.n_new, plan.n_region)
            apply_write_plan(session, vds_move, plan)
            reconcile_source_urls(session, url_map_move, concat_dim=CONCAT_DIM)
            session.commit("move: mover T0 -> T1")

            # ----- 5. assert final state -----
            axis = read_axis(repo)
            smap = read_source_url_map(repo.readonly_session("main").store, CONCAT_DIM)
            assert axis.tolist() == [TA, T1], f"final axis {axis.tolist()} != [{TA}, {T1}]"
            assert T0 not in smap, f"stale time {T0} still present: {smap}"
            assert smap == {TA: url_anchor, T1: url_mover}, f"final source_url wrong: {smap}"
            seen = {}
            for t, u in smap.items():
                assert u not in seen, f"url {u} maps to {seen[u]} and {t} (duplicate)"
                seen[u] = t
            # data resolves through the s3 VCC, and reflects the overwritten file
            assert read_foo_at(repo, 0) == TA, "anchor data changed unexpectedly"
            assert read_foo_at(repo, 1) == T1, "mover data did not follow to T1"
            print(f"  axis={axis.tolist()}  source_url={smap}")
            print(f"  foo resolves via VCC: [TA]={TA}, [T1]={T1}")
            print("\nPASS move -> stale step retired, URL moved to new time, no duplicate, data resolves")
            passed = True
    finally:
        if passed and not args.keep:
            delete_prefix(s3, bucket, args.store_prefix)
            delete_prefix(s3, bucket, args.granule_prefix)
        elif not args.keep:
            print("\n(Left scratch store/granules in place for inspection after failure.)")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
