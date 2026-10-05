#!/usr/bin/env python3
"""
Coordinate-aware write planning for updating an Icechunk v2 virtual Zarr store.

A naive append (``vds.vz.to_icechunk(store, append_dim=...)``) adds every
incoming granule to the end of the axis unconditionally. That is wrong whenever
the incoming granules are not a clean, in-order extension of the store:

* **Re-delivery / re-run** -- a granule whose coordinate is already in the store
  would get a *second* row with the same value.
* **Out-of-order / back-fill** -- a granule whose coordinate precedes the
  store's current end would still land at the end, breaking monotonicity.

This module classifies each incoming coordinate against the store's current axis
and writes it the cheapest correct way:

* **region** -- the coordinate already exists; write it in place
  (``region="auto"``), an idempotent overwrite.
* **append** -- the coordinate is strictly after the store's maximum; extend the
  axis (``append_dim``).
* **insert** -- the coordinate falls before the maximum (or into a gap); the
  store's existing data is shifted to open a slot at the sorted position and the
  granule is written there.

The insert path keeps a sorted, monotonic axis without ever pre-declaring the
store's full extent. It relies on Icechunk's manifest indirection: resizing an
array and relabelling its chunk positions (``Session.reindex_array``) is a
metadata-only operation -- no chunk payload is downloaded or rewritten -- so a
back-fill costs a manifest update regardless of store size. This requires
Icechunk >= 2.2 and a **regular chunk grid with chunk size 1 along the append
dimension** (one time step per chunk, which these per-granule stores use); an
array that does not satisfy that raises ``NonRegularGridError`` rather than
risking a corrupt store.

Coordinates are compared in their *encoded* (raw) form. Callers build the
incoming VDS with ``decode_times=False``; :func:`read_store_coordinate` opens
the store the same way so both sides compare like-for-like. This assumes the
granules share the store's time encoding (units/calendar), which holds for
granules of one collection.
"""

import logging
from dataclasses import dataclass, field

import numpy as np
import xarray as xr
import zarr
import virtualizarr  # noqa: F401  -- registers the `.vz` dataset accessor

logger = logging.getLogger(__name__)


class NonRegularGridError(ValueError):
    """An insert was required but an array's chunk grid cannot be shifted
    cheaply (not regular, or chunk size != 1 along the append dimension)."""


@dataclass
class WritePlan:
    """How an incoming VDS maps onto an existing store along ``dimension``.

    ``present_positions``/``new_positions`` index into the incoming VDS.
    ``target_values`` is the sorted union the axis becomes. ``needs_reindex`` is
    True when existing store data must move to keep the axis sorted (an insert);
    False when the new coordinates are a pure tail that can simply be appended.
    """

    dimension: str
    existing_values: np.ndarray | None
    target_values: np.ndarray
    present_positions: list = field(default_factory=list)
    new_positions: list = field(default_factory=list)
    needs_reindex: bool = False

    @property
    def n_region(self) -> int:
        return len(self.present_positions)

    @property
    def n_new(self) -> int:
        return len(self.new_positions)

    @property
    def is_empty(self) -> bool:
        return self.n_region == 0 and self.n_new == 0

    @property
    def mode(self) -> str:
        if self.n_new == 0:
            return "region-only"
        return "insert" if self.needs_reindex else "append"


def read_store_coordinate(store, dimension: str = "time") -> np.ndarray | None:
    """The encoded coordinate values the store already holds along ``dimension``.

    Returns ``None`` if the store has no group yet or no such coordinate. Opened
    with ``decode_times=False`` so values compare directly against an incoming
    VDS built the same way.
    """
    try:
        ds = xr.open_zarr(store, consolidated=False, decode_times=False)
    except Exception:
        return None
    try:
        if dimension not in ds.coords:
            return None
        return np.asarray(ds[dimension].values)
    finally:
        ds.close()


def build_write_plan(
    vds: xr.Dataset,
    existing: np.ndarray | None,
    dimension: str = "time",
) -> WritePlan:
    """Classify ``vds`` against the store's existing ``dimension`` axis.

    Raises ``ValueError`` if the incoming VDS carries duplicate coordinates.
    Decides, without touching the store, whether the write is a plain append or
    needs an insert (existing data must shift). The regular-grid requirement for
    an insert is only checked when the plan is applied.
    """
    if dimension not in vds.coords:
        raise ValueError(f"Incoming VDS has no '{dimension}' coordinate")

    incoming = np.asarray(vds[dimension].values)
    if incoming.ndim != 1:
        raise ValueError(f"'{dimension}' coordinate must be 1-D, got {incoming.ndim}-D")
    if np.unique(incoming).size != incoming.size:
        raise ValueError(
            f"Incoming granules contain duplicate '{dimension}' coordinates"
        )

    if existing is None or np.asarray(existing).size == 0:
        order = list(np.argsort(incoming))
        return WritePlan(
            dimension=dimension,
            existing_values=None,
            target_values=np.sort(incoming),
            new_positions=order,
            needs_reindex=False,
        )

    existing = np.asarray(existing)
    present_mask = np.isin(incoming, existing)
    present_positions = list(np.nonzero(present_mask)[0])
    new_positions = list(np.nonzero(~present_mask)[0])

    new_values = incoming[~present_mask]
    target_values = np.union1d(existing, new_values)  # sorted, unique

    # Existing data stays put iff the store is a prefix of the new axis, i.e.
    # every new coordinate is after the current maximum (a pure tail append).
    needs_reindex = not (
        new_values.size == 0
        or np.array_equal(target_values[: existing.size], existing)
    )

    # Keep the new coordinates in ascending order for a tidy append block.
    new_positions.sort(key=lambda p: incoming[p])

    return WritePlan(
        dimension=dimension,
        existing_values=existing,
        target_values=target_values,
        present_positions=present_positions,
        new_positions=new_positions,
        needs_reindex=needs_reindex,
    )


def _region_write(session, vds: xr.Dataset, dim: str, positions) -> None:
    """Write the given incoming positions one coordinate at a time, in place."""
    for p in positions:
        sub = vds.isel({dim: [int(p)]})
        logger.info("region write %s=%r", dim, np.asarray(sub[dim].values)[0])
        sub.vz.to_icechunk(session.store, region="auto")


def _time_axis_arrays(root, dim: str):
    """Yield (path, array, axis) for every array carrying ``dim``."""
    for name, arr in root.arrays():
        dims = arr.metadata.dimension_names
        if dims and dim in dims:
            yield f"/{name}", arr, dims.index(dim)


def _assert_shiftable(arr, axis: int, path: str) -> None:
    """An array can be shifted metadata-only only on a regular grid with chunk
    size 1 along the append axis (so an element insert is a whole-chunk move)."""
    grid = arr.metadata.chunk_grid
    chunk_shape = getattr(grid, "chunk_shape", None)
    if chunk_shape is None:
        raise NonRegularGridError(
            f"{path}: chunk grid is not regular; cannot insert out-of-order "
            f"without rewriting data."
        )
    if chunk_shape[axis] != 1:
        raise NonRegularGridError(
            f"{path}: chunk size along the append dimension is "
            f"{chunk_shape[axis]} (expected 1); an insert is not a whole-chunk "
            f"move and cannot be done metadata-only."
        )


def _apply_insert(session, vds: xr.Dataset, plan: WritePlan) -> None:
    """Shift existing data to open sorted slots, then write every granule.

    Resizes each time-dependent array to the final extent, relabels existing
    chunks to their new positions with ``reindex_array`` (metadata only), writes
    the full sorted coordinate, then region-writes every incoming granule into
    the slot its coordinate now owns.
    """
    dim = plan.dimension
    existing = plan.existing_values
    target = plan.target_values

    # old time index -> new time index (both element==chunk index, chunk size 1)
    pos_map = {i: int(np.searchsorted(target, existing[i])) for i in range(existing.size)}
    inv_map = {v: k for k, v in pos_map.items()}

    root = zarr.open_group(session.store, mode="a")

    if dim not in [name for name, _ in root.arrays()]:
        raise ValueError(
            f"Store has no materialized '{dim}' coordinate; cannot place writes "
            f"by coordinate."
        )

    # Validate every array up front so we fail before mutating anything.
    time_arrays = list(_time_axis_arrays(root, dim))
    for path, arr, axis in time_arrays:
        if path != f"/{dim}":
            _assert_shiftable(arr, axis, path)

    def make_fwd_bwd(axis):
        def fwd(idx):
            t = idx[axis]
            if t not in pos_map:
                return None
            out = list(idx)
            out[axis] = pos_map[t]
            return out

        def bwd(idx):
            t = idx[axis]
            if t not in inv_map:
                return None
            out = list(idx)
            out[axis] = inv_map[t]
            return out

        return fwd, bwd

    for path, arr, axis in time_arrays:
        new_shape = list(arr.shape)
        new_shape[axis] = int(target.size)
        arr.resize(tuple(new_shape))
        if path == f"/{dim}":
            continue  # the coordinate itself is set wholesale below
        fwd, bwd = make_fwd_bwd(axis)
        logger.info("reindex %s to open %d insert slot(s)", path, plan.n_new)
        session.reindex_array(path, forward=fwd, backward=bwd)

    # Write the full sorted coordinate so region="auto" can place data by value.
    root[dim][:] = np.asarray(target)

    _region_write(session, vds, dim, plan.present_positions + plan.new_positions)


def apply_write_plan(session, vds: xr.Dataset, plan: WritePlan) -> dict:
    """Execute ``plan`` against an Icechunk writable session (no commit).

    Returns a summary of what was written. Raises ``NonRegularGridError`` if an
    insert is needed but an array cannot be shifted cheaply; the session is left
    uncommitted so nothing is persisted on failure.
    """
    dim = plan.dimension

    if plan.needs_reindex:
        _apply_insert(session, vds, plan)
        return {"mode": "insert", "n_region": plan.n_region, "n_new": plan.n_new}

    # Pure tail append (+ idempotent in-place writes for already-present coords).
    _region_write(session, vds, dim, plan.present_positions)
    if plan.n_new:
        block = vds.isel({dim: [int(p) for p in plan.new_positions]})
        if plan.existing_values is None:
            logger.info("creating store with %d %s step(s)", plan.n_new, dim)
            block.vz.to_icechunk(session.store)
        else:
            logger.info("appending %d new %s step(s)", plan.n_new, dim)
            block.vz.to_icechunk(session.store, append_dim=dim)

    return {"mode": plan.mode, "n_region": plan.n_region, "n_new": plan.n_new}


def delete_coordinates(session, values, dimension: str = "time") -> dict:
    """Remove one or more coordinates (granules) from the store along ``dimension``.

    The gap each deletion leaves is closed by shifting the later chunks down
    (``reindex_array``) and shrinking the arrays (``resize``) -- both metadata
    only, so no chunk data is read or rewritten, and the referenced source files
    are untouched. Deleting the newest coordinate(s) needs no shift, just a
    resize. Leaves the session uncommitted; the caller commits.

    ``values`` are matched against the store's encoded coordinate (open the
    store/vds with ``decode_times=False`` to supply raw values). Values not
    present are ignored (with a warning). Raises ``NonRegularGridError`` if an
    array cannot be shifted cheaply.
    """
    requested = np.atleast_1d(np.asarray(values))
    root = zarr.open_group(session.store, mode="a")

    if dimension not in [name for name, _ in root.arrays()]:
        raise ValueError(f"Store has no materialized '{dimension}' coordinate")

    existing = np.asarray(root[dimension][:])
    delete_mask = np.isin(existing, requested)
    missing = np.setdiff1d(requested, existing)
    if missing.size:
        logger.warning("delete: %d value(s) not in store, ignored: %s",
                       missing.size, missing.tolist())
    n_delete = int(delete_mask.sum())
    if n_delete == 0:
        logger.info("delete: nothing to remove")
        return {"n_deleted": 0, "remaining": int(existing.size)}

    deleted_set = set(np.nonzero(delete_mask)[0].tolist())
    kept_values = existing[~delete_mask]
    new_len = int(kept_values.size)

    # old time index -> new (compacted) index, for kept indices only
    shift, nxt = {}, 0
    for i in range(existing.size):
        if i in deleted_set:
            continue
        shift[i] = nxt
        nxt += 1
    inv = {v: k for k, v in shift.items()}

    time_arrays = list(_time_axis_arrays(root, dimension))
    # Validate before mutating anything.
    for path, arr, axis in time_arrays:
        if path != f"/{dimension}":
            _assert_shiftable(arr, axis, path)

    def make_fwd_bwd(axis):
        def fwd(idx):
            t = idx[axis]
            if t not in shift:  # deleted or out of range -> drop
                return None
            out = list(idx)
            out[axis] = shift[t]
            return out

        def bwd(idx):
            t = idx[axis]
            if t not in inv:
                return None
            out = list(idx)
            out[axis] = inv[t]
            return out

        return fwd, bwd

    # Close the gaps first (on the full-length array), then shrink.
    for path, arr, axis in time_arrays:
        if path == f"/{dimension}":
            continue
        fwd, bwd = make_fwd_bwd(axis)
        logger.info("reindex %s to drop %d coordinate(s)", path, n_delete)
        session.reindex_array(path, forward=fwd, backward=bwd)

    for path, arr, axis in time_arrays:
        new_shape = list(arr.shape)
        new_shape[axis] = new_len
        arr.resize(tuple(new_shape))

    # Rewrite the compacted coordinate.
    root[dimension][:] = kept_values

    logger.info("deleted %d coordinate(s), %d remain", n_delete, new_len)
    return {"n_deleted": n_delete, "remaining": new_len}
