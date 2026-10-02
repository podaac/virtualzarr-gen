#!/usr/bin/env python3
"""
Walk an Icechunk v2 virtual store through every write mode and show the store
after each stage.

Stages:
  1. generate   -- create a store from granules [0, 2, 4]
  2. tail-append-- append [6, 8]            (after the current max)
  3. re-deliver -- write [2, 6] again       (idempotent, in place)
  4. gap-insert -- write [1, 3, 5, 7]        (out of order -> reindex)

Ends with a strictly-increasing axis [0..8] whose data is aligned to its
coordinate. Each granule's data is the constant value of its time coordinate,
so a correct store always has foo[t, 0, 0] == time[t].

Runnable with no cloud credentials:  python demo_append_stages.py
The store is written to a temp local-filesystem Icechunk repo and inspected
after every commit.
"""

import tempfile

import numpy as np
import xarray as xr
import icechunk
from virtualizarr.manifests import ChunkManifest, ManifestArray
from zarr.codecs import BytesCodec
from zarr.core.dtype import parse_data_type
from zarr.core.metadata import ArrayV3Metadata

from icechunk_append import read_store_coordinate, build_write_plan, apply_write_plan

Y, X = 2, 3


def make_vds(time_values, chunk_dir):
    """A virtual dataset: step t holds the constant value time_values[t]."""
    entries = {}
    for i, tv in enumerate(time_values):
        buf = np.full((1, Y, X), tv, dtype="int32").tobytes()
        path = f"{chunk_dir}/chunk_{tv}"
        with open(path, "wb") as f:
            f.write(buf)
        entries[f"{i}.0.0"] = {"path": f"file://{path}", "offset": 0, "length": len(buf)}

    manifest = ChunkManifest(entries)
    zdtype = parse_data_type(np.dtype("int32"), zarr_format=3)
    metadata = ArrayV3Metadata(
        shape=(len(time_values), Y, X),
        data_type=zdtype,
        chunk_grid={"name": "regular", "configuration": {"chunk_shape": (1, Y, X)}},
        chunk_key_encoding={"name": "default"},
        fill_value=zdtype.default_scalar(),
        codecs=[BytesCodec()],
        attributes={},
        dimension_names=("time", "y", "x"),
        storage_transformers=None,
    )
    ma = ManifestArray(chunkmanifest=manifest, metadata=metadata)
    return xr.Dataset(
        {"foo": xr.Variable(("time", "y", "x"), ma)},
        coords={"time": ("time", np.asarray(time_values, dtype="int64"))},
    )


def make_repo(chunk_dir, store_dir):
    prefix = f"file://{chunk_dir}/"
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(prefix, icechunk.local_filesystem_store(chunk_dir))
    )
    return icechunk.Repository.create(
        storage=icechunk.local_filesystem_storage(store_dir),
        config=config,
        authorize_virtual_chunk_access={
            prefix: icechunk.credentials.LocalFileSystemAccess
        },
    )


def show_store(repo, title):
    """Print the committed store: axis, data, alignment, and icechunk history."""
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False,
                      decode_times=False)
    times = np.asarray(ds["time"].values)
    vals = np.asarray(ds["foo"].values)[:, 0, 0]
    ds.close()

    sorted_ok = bool(np.all(np.diff(times) > 0)) if times.size > 1 else True
    aligned_ok = bool(np.array_equal(times.astype("int32"), vals))
    history = [c.message for c in repo.ancestry(branch="main")]

    print(f"\n{'=' * 68}\n{title}\n{'=' * 68}")
    print(f"  shape        : time={times.size}, y={Y}, x={X}")
    print(f"  time axis    : {times.tolist()}")
    print(f"  foo[:,0,0]   : {vals.tolist()}")
    print(f"  strictly ↑   : {sorted_ok}")
    print(f"  data aligned : {aligned_ok}   (foo[t,0,0] == time[t])")
    print(f"  commits      : {history}")
    assert sorted_ok and aligned_ok, "store is out of order or misaligned!"


def write(repo, time_values, chunk_dir):
    vds = make_vds(time_values, chunk_dir)
    session = repo.writable_session("main")
    existing = read_store_coordinate(session.store, "time")
    plan = build_write_plan(vds, existing, dimension="time")
    if plan.is_empty:
        print(f"  (input {time_values}: nothing to write)")
        return plan
    apply_write_plan(session, vds, plan)
    session.commit(f"[{plan.mode}] {time_values}")
    return plan


def main():
    with tempfile.TemporaryDirectory() as chunk_dir, \
         tempfile.TemporaryDirectory() as store_dir:
        print(f"icechunk store : {store_dir}")
        print(f"virtual chunks : {chunk_dir}")
        repo = make_repo(chunk_dir, store_dir)

        p = write(repo, [0, 2, 4], chunk_dir)
        show_store(repo, f"STAGE 1  generate [0, 2, 4]            -> mode={p.mode!r}")

        p = write(repo, [6, 8], chunk_dir)
        show_store(repo, f"STAGE 2  tail-append [6, 8]            -> mode={p.mode!r}")

        p = write(repo, [2, 6], chunk_dir)
        show_store(repo, f"STAGE 3  re-deliver [2, 6] (idempotent)-> mode={p.mode!r}")

        p = write(repo, [1, 3, 5, 7], chunk_dir)
        show_store(repo, f"STAGE 4  gap-insert [1, 3, 5, 7]       -> mode={p.mode!r}")

        print(f"\nFinal axis is [0..8], sorted and aligned. ✓")


if __name__ == "__main__":
    main()
