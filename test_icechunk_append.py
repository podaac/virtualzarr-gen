#!/usr/bin/env python3
"""
Tests for icechunk_append: region / append / insert placement against a real
in-memory Icechunk store backed by local virtual chunks.

Run directly:   python test_icechunk_append.py
Or with pytest: pytest test_icechunk_append.py
"""

import tempfile

import numpy as np
import xarray as xr
import zarr
import icechunk
from virtualizarr.manifests import ChunkManifest, ManifestArray
from zarr.codecs import BytesCodec
from zarr.core.dtype import parse_data_type
from zarr.core.metadata import ArrayV3Metadata

from icechunk_append import (
    read_store_coordinate,
    build_write_plan,
    apply_write_plan,
    delete_coordinates,
    NonRegularGridError,
)

Y, X = 2, 3


def _make_vds(time_values, chunk_dir):
    """A virtual dataset of len(time_values) steps; chunk t holds the constant
    value time_values[t] so placement can be verified by reading it back."""
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


def _repo(chunk_dir):
    prefix = f"file://{chunk_dir}/"
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(prefix, icechunk.local_filesystem_store(chunk_dir))
    )
    return icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=config,
        authorize_virtual_chunk_access={
            prefix: icechunk.credentials.LocalFileSystemAccess
        },
    )


def _write(repo, time_values, chunk_dir):
    """Build a VDS for time_values and update the store via the planner."""
    vds = _make_vds(time_values, chunk_dir)
    session = repo.writable_session("main")
    existing = read_store_coordinate(session.store, "time")
    plan = build_write_plan(vds, existing, dimension="time")
    if plan.is_empty:
        return plan
    apply_write_plan(session, vds, plan)
    session.commit(f"{plan.mode}: {time_values}")
    return plan


def _read(repo):
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False,
                      decode_times=False)
    times = np.asarray(ds["time"].values)
    # each cell equals its time value, so foo[:,0,0] mirrors the time axis
    vals = np.asarray(ds["foo"].values)[:, 0, 0]
    ds.close()
    return times, vals


def _check_sorted_and_aligned(times, vals):
    assert np.all(np.diff(times) > 0), f"axis not strictly increasing: {times}"
    assert np.array_equal(times.astype("int32"), vals), (
        f"data not aligned to coordinate: times={times} vals={vals}"
    )


def test_append_then_region_then_insert():
    with tempfile.TemporaryDirectory() as d:
        repo = _repo(d)

        # initial write (empty store -> append path)
        p = _write(repo, [0, 2, 4], d)
        assert p.mode == "append", p.mode
        times, vals = _read(repo)
        assert list(times) == [0, 2, 4]
        _check_sorted_and_aligned(times, vals)

        # tail append
        p = _write(repo, [6, 8], d)
        assert p.mode == "append", p.mode
        times, vals = _read(repo)
        assert list(times) == [0, 2, 4, 6, 8]
        _check_sorted_and_aligned(times, vals)

        # idempotent re-delivery: all already present -> region-only, no growth
        p = _write(repo, [2, 6], d)
        assert p.mode == "region-only", p.mode
        assert p.n_new == 0 and p.n_region == 2
        times, vals = _read(repo)
        assert list(times) == [0, 2, 4, 6, 8]
        _check_sorted_and_aligned(times, vals)

        # out-of-order insert: fill the gaps 1,3,5,7 (all before current max)
        p = _write(repo, [1, 3, 5, 7], d)
        assert p.mode == "insert", p.mode
        assert p.needs_reindex
        times, vals = _read(repo)
        assert list(times) == [0, 1, 2, 3, 4, 5, 6, 7, 8], list(times)
        _check_sorted_and_aligned(times, vals)


def test_insert_before_everything():
    with tempfile.TemporaryDirectory() as d:
        repo = _repo(d)
        _write(repo, [10, 11, 12], d)
        p = _write(repo, [5], d)  # earlier than the whole store
        assert p.mode == "insert", p.mode
        times, vals = _read(repo)
        assert list(times) == [5, 10, 11, 12], list(times)
        _check_sorted_and_aligned(times, vals)


def test_mixed_present_new_and_insert():
    with tempfile.TemporaryDirectory() as d:
        repo = _repo(d)
        _write(repo, [0, 5, 10], d)
        # 5 present (region), 2 is an insert, 15 is a tail -> reindex path
        p = _write(repo, [2, 5, 15], d)
        assert p.mode == "insert", p.mode
        assert p.n_region == 1 and p.n_new == 2
        times, vals = _read(repo)
        assert list(times) == [0, 2, 5, 10, 15], list(times)
        _check_sorted_and_aligned(times, vals)


def test_duplicate_incoming_rejected():
    with tempfile.TemporaryDirectory() as d:
        vds = _make_vds([3, 3], d)
        try:
            build_write_plan(vds, None, "time")
        except ValueError as e:
            assert "duplicate" in str(e)
        else:
            raise AssertionError("expected ValueError on duplicate coordinates")


def _delete(repo, values):
    session = repo.writable_session("main")
    summary = delete_coordinates(session, values, dimension="time")
    if summary["n_deleted"]:
        session.commit(f"delete {values}")
    return summary


def test_delete_middle_and_tail():
    with tempfile.TemporaryDirectory() as d:
        repo = _repo(d)
        _write(repo, [0, 1, 2, 3, 4], d)

        # delete a middle granule -> gap closed, data stays aligned
        s = _delete(repo, [2])
        assert s["n_deleted"] == 1 and s["remaining"] == 4
        times, vals = _read(repo)
        assert list(times) == [0, 1, 3, 4], list(times)
        _check_sorted_and_aligned(times, vals)

        # delete the newest granule -> pure resize
        s = _delete(repo, [4])
        assert s["n_deleted"] == 1
        times, vals = _read(repo)
        assert list(times) == [0, 1, 3], list(times)
        _check_sorted_and_aligned(times, vals)


def test_delete_multiple_and_missing():
    with tempfile.TemporaryDirectory() as d:
        repo = _repo(d)
        _write(repo, [0, 1, 2, 3, 4, 5], d)
        # 1 and 4 exist, 99 does not -> deletes 2, ignores missing
        s = _delete(repo, [1, 4, 99])
        assert s["n_deleted"] == 2 and s["remaining"] == 4
        times, vals = _read(repo)
        assert list(times) == [0, 2, 3, 5], list(times)
        _check_sorted_and_aligned(times, vals)


def test_delete_then_reinsert():
    with tempfile.TemporaryDirectory() as d:
        repo = _repo(d)
        _write(repo, [0, 1, 2, 3, 4], d)
        _delete(repo, [2])
        # re-add the deleted coordinate -> insert puts it back in order
        p = _write(repo, [2], d)
        assert p.mode == "insert", p.mode
        times, vals = _read(repo)
        assert list(times) == [0, 1, 2, 3, 4], list(times)
        _check_sorted_and_aligned(times, vals)


def _run_all():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {name}")
    print("ALL PASS")


if __name__ == "__main__":
    _run_all()
