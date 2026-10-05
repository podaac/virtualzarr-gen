#!/usr/bin/env python3
"""
Optional ``source_url`` coordinate for Icechunk v2 virtual Zarr stores.

PROPOSAL / FOR REVIEW -- not wired into the append paths yet. See
"Integration" below for the two lines each path would add.

Why
---
In these per-granule stores a granule is identified only by its ``time``
coordinate value; the manifest links each data chunk to a source URL, but
``Session.all_virtual_chunk_locations()`` returns a flat, un-keyed set, so there
is no clean ``time -> URL`` lookup (see the reprocess discussion). This module
adds a small *materialized* string coordinate ``source_url`` indexed along the
concat dimension, giving an exact, queryable ``time <-> URL`` link that lives in
the store and survives reopen.

Design
------
``source_url`` is maintained *out of band* from the virtual data write, by
reconciliation rather than by threading it through the write planner:

1. The normal write (``apply_write_plan`` / ``delete_coordinates``) runs first
   and leaves the store's ``time`` axis in its final, sorted state.
2. :func:`reconcile_source_urls` then reads that final axis, merges the incoming
   ``{time: url}`` map over whatever ``source_url`` the store already held
   (incoming wins -- so a reprocessed granule updates its URL in place), and
   rewrites the whole ``source_url`` array to match the axis.

Because step 2 always rebuilds from the *current* axis, the same call is correct
for every write mode -- create, tail-append, out-of-order insert -- with no
special cases. Deletes need no reconcile at all: ``source_url`` carries the
concat dim, so :func:`icechunk_append.delete_coordinates` already shifts and
shrinks it along with the data (it is a regular grid, one step per chunk).

The array is written with ``VariableLengthUTF8`` (no fixed width limit) and
``chunks=(1,)`` along the concat dim -- one step per chunk, matching the data
arrays -- so an out-of-order *insert* on a store that already has ``source_url``
stays a metadata-only chunk relabel inside the planner and does not trip
``NonRegularGridError``.

Integration (what the append paths would add)
----------------------------------------------
In ``append_granules.main`` / ``append_lambda_handler._append_to_store`` /
``sqs_append_granules.append_to_collection``, after ``apply_write_plan`` and
before ``session.commit``::

    from source_url_coord import url_map_from_granules, reconcile_source_urls
    url_map = url_map_from_granules(granule_urls, auth, concat_dim=concat_dim)
    reconcile_source_urls(session, url_map, concat_dim=concat_dim)

For the generate path, pass the ``{time: url}`` map for the granules used to
create the store the same way.

Query::

    from source_url_coord import source_urls_for_values
    source_urls_for_values(repo.readonly_session("main").store, [1072915200])
    # {1072915200: 's3://.../granule.nc'}
"""

import logging

import numpy as np
import zarr

from icechunk_append import delete_coordinates

logger = logging.getLogger(__name__)

SOURCE_URL_VAR = "source_url"
_STRING_DTYPE = np.dtypes.StringDType()


def url_map_from_granules(urls, auth, concat_dim="time", build_vds_fn=None,
                          data_vars="minimal", coords="minimal"):
    """Resolve ``{encoded concat-dim value: url}`` by reading each granule.

    Each granule's virtual dataset is opened individually (so a granule that
    carries several steps maps every one of its coordinate values to the same
    URL) and its concat-dim values read in their *encoded* form -- build the VDS
    with ``decode_times=False`` so the values match what the store holds.

    ``build_vds_fn`` defaults to ``append_granules.build_vds``; it is injected to
    keep this module importable without the full S3 dependency stack.
    """
    if build_vds_fn is None:
        from append_granules import build_vds as build_vds_fn

    mapping = {}
    for url in urls:
        vds, _ = build_vds_fn([url], auth, concat_dim=concat_dim,
                              data_vars=data_vars, coords=coords)
        if concat_dim not in vds.coords:
            raise ValueError(f"{url}: no '{concat_dim}' coordinate in granule")
        for value in np.atleast_1d(np.asarray(vds[concat_dim].values)):
            key = _key(value)
            if key in mapping and mapping[key] != url:
                logger.warning("%s=%r maps to both %s and %s; keeping the latter",
                               concat_dim, key, mapping[key], url)
            mapping[key] = url
    return mapping


def _key(value):
    """A hashable, round-trippable key for an encoded coordinate value."""
    v = np.asarray(value).item()
    return v


def url_map_from_vds(vds, concat_dim="time"):
    """Derive ``{concat-dim value: url}`` from a VDS's chunk manifest.

    Unlike :func:`url_map_from_granules`, this reads nothing extra -- it recovers
    each step's source URL from the virtual references already in ``vds`` (so it
    is cheap at generation time, where the combined VDS is in hand). The step's
    chunk position maps to the ``concat_dim`` value at that position, and the
    manifest gives that chunk's source path.

    Requires a data variable backed by a VirtualiZarr ``ManifestArray`` spanning
    ``concat_dim`` with **chunk size 1 along that dimension** (one step per
    chunk, as these per-granule stores use). Raises ``ValueError`` if no such
    variable exists or the 1-chunk-per-step assumption does not hold (so the
    mapping would be ambiguous). Safe to call after ``sortby`` -- the manifest
    is reordered with the data.
    """
    if concat_dim in vds.coords:
        coord = np.asarray(vds[concat_dim].values)
    elif concat_dim in vds.dims:
        coord = None  # index dimension with no materialized values
    else:
        raise ValueError(f"VDS has no '{concat_dim}' dimension")

    for name, var in vds.variables.items():
        manifest = getattr(getattr(var, "data", None), "manifest", None)
        if manifest is None or concat_dim not in var.dims:
            continue
        axis = var.dims.index(concat_dim)
        mapping = {}
        for chunk_key, entry in manifest.dict().items():
            pos = int(chunk_key.split(".")[axis])  # chunk index == step (size-1 chunks)
            value = _key(coord[pos]) if coord is not None else pos
            mapping[value] = entry["path"]
        expected = int(coord.size) if coord is not None else var.sizes[concat_dim]
        if len(mapping) != expected:
            raise ValueError(
                f"'{name}' maps {len(mapping)} step(s) but '{concat_dim}' has "
                f"{expected}; chunk size along '{concat_dim}' is probably not 1, "
                f"so a per-step source_url is ambiguous."
            )
        return mapping

    raise ValueError(f"No manifest-backed variable spans '{concat_dim}'")


def read_source_url_map(store, concat_dim="time"):
    """Return ``{concat-dim value: url}`` currently stored, or ``{}`` if none.

    Reads the materialized ``source_url`` and ``time`` arrays directly with zarr
    (not ``xr.open_zarr``) and pairs them positionally. Reading with zarr matters
    because a tail append extends ``time`` before ``source_url`` is reconciled,
    leaving the store momentarily dim-inconsistent -- which ``xr.open_zarr``
    refuses to open. Append never reorders existing steps and the insert/delete
    paths relabel ``source_url`` in lock-step with the data, so position ``i`` of
    ``source_url`` always matches position ``i`` of ``time``.
    """
    try:
        root = zarr.open_group(store, mode="r")
    except Exception:
        return {}
    names = [name for name, _ in root.arrays()]
    if SOURCE_URL_VAR not in names or concat_dim not in names:
        return {}
    axis = np.asarray(root[concat_dim][:])
    urls = np.asarray(root[SOURCE_URL_VAR][:])
    n = min(axis.size, urls.size)
    return {_key(axis[i]): str(urls[i]) for i in range(n)}


def reconcile_source_urls(session, new_map, concat_dim="time"):
    """Rebuild the ``source_url`` coordinate to match the store's current axis.

    Call *after* the data write (``apply_write_plan``) and before ``commit``.
    Merges ``new_map`` ({concat-dim value: url}) over the store's existing
    ``source_url`` -- incoming URLs win, so a reprocessed granule overwrites the
    URL for its time step -- then writes one URL per step in the exact order of
    the store's final ``time`` axis.

    Steps on the axis with no known URL are written as "" and warned about.
    Returns a summary dict. Leaves the session uncommitted.
    """
    root = zarr.open_group(session.store, mode="a")
    array_names = [name for name, _ in root.arrays()]
    if concat_dim not in array_names:
        raise ValueError(f"Store has no materialized '{concat_dim}' coordinate")

    axis = np.asarray(root[concat_dim][:])
    n = int(axis.size)

    merged = read_source_url_map(session.store, concat_dim)
    for value, url in new_map.items():
        merged[_key(value)] = url

    urls = np.array([merged.get(_key(t), "") for t in axis], dtype=_STRING_DTYPE)
    n_missing = int((urls == "").sum())
    if n_missing:
        missing_vals = [axis[i].item() for i in np.nonzero(urls == "")[0]]
        logger.warning("source_url: %d step(s) have no known URL: %s",
                       n_missing, missing_vals)

    if SOURCE_URL_VAR in array_names:
        arr = root[SOURCE_URL_VAR]
        if arr.shape != (n,):
            arr.resize((n,))
    else:
        arr = root.create_array(
            SOURCE_URL_VAR, shape=(n,), chunks=(1,),
            dtype=_STRING_DTYPE, dimension_names=(concat_dim,),
        )
    arr[:] = urls

    logger.info("source_url reconciled: %d step(s), %d newly supplied, %d unknown",
                n, len(new_map), n_missing)
    return {"n_steps": n, "n_supplied": len(new_map), "n_missing": n_missing}


def source_urls_for_values(store, values, concat_dim="time"):
    """Query ``{value: url or None}`` for specific concat-dim values."""
    mapping = read_source_url_map(store, concat_dim)
    return {_key(v): mapping.get(_key(v)) for v in np.atleast_1d(np.asarray(values))}


def retire_moved_urls(session, new_map, concat_dim="time"):
    """Delete store steps whose URL is being re-delivered at a *different* time.

    Handles the one reprocess case the planner cannot: a granule keeping its
    **name/URL** but changing its ``time`` value. The planner matches on time, so
    it would add the new time and leave the old step as an untraceable orphan
    (same URL referenced twice). This closes that gap by using ``source_url`` as
    the stable identity:

      for each incoming URL, if the store already holds that URL at a time *not*
      in ``new_map`` for that URL, delete those old time(s) first -- turning the
      reprocess into a move (remove old, then the caller writes the new).

    Call *before* ``apply_write_plan`` + :func:`reconcile_source_urls`. Requires
    the store to already carry ``source_url`` (i.e. earlier writes used this
    module); on a store without it, nothing matches and this is a no-op.

    Does NOT touch a URL re-delivered at the *same* time -- that is a normal
    in-place overwrite the planner handles. Leaves the session uncommitted;
    returns ``{"retired": [old values deleted], "n_retired": int}``.
    """
    stored = read_source_url_map(session.store, concat_dim)  # {time: url}
    if not stored:
        return {"retired": [], "n_retired": 0}

    # URL -> the time(s) it is being (re)delivered at now
    incoming_times_by_url = {}
    for value, url in new_map.items():
        incoming_times_by_url.setdefault(url, set()).add(_key(value))

    # A stored step is stale iff its URL is being re-delivered, but NOT at this
    # stored step's own time (that same-time case is a plain overwrite).
    stale = []
    for time_value, url in stored.items():
        new_times = incoming_times_by_url.get(url)
        if new_times is not None and _key(time_value) not in new_times:
            stale.append(time_value)

    if not stale:
        return {"retired": [], "n_retired": 0}

    logger.info("retiring %d moved granule step(s) (same URL, new time): %s",
                len(stale), stale)
    delete_coordinates(session, stale, dimension=concat_dim)
    return {"retired": stale, "n_retired": len(stale)}


# --------------------------------------------------------------------------- #
# Self-test: drives the real icechunk_append planner through create / append /
# reprocess / insert / delete on a synthetic local store and checks that
# source_url stays aligned to the time axis at every stage.
#   python source_url_coord.py
# --------------------------------------------------------------------------- #
def _selftest():
    import tempfile

    from icechunk_append import (
        read_store_coordinate, build_write_plan, apply_write_plan,
        delete_coordinates,
    )
    # reuse the synthetic VDS/repo builders from the planner's test module
    from test_icechunk_append import _make_vds, _repo

    def url_for(tv):
        return f"s3://bucket/g{tv}.nc"

    def write(repo, d, time_values):
        vds = _make_vds(time_values, d)
        session = repo.writable_session("main")
        existing = read_store_coordinate(session.store, "time")
        plan = build_write_plan(vds, existing, "time")
        if not plan.is_empty:
            apply_write_plan(session, vds, plan)
        reconcile_source_urls(session, {tv: url_for(tv) for tv in time_values}, "time")
        session.commit(f"[{plan.mode}] {time_values}")
        return plan

    def axis_and_urls(repo):
        store = repo.readonly_session("main").store
        m = read_source_url_map(store, "time")
        axis = sorted(m)
        return axis, [m[t] for t in axis]

    def check(repo, expected_times, expected_url_of):
        axis, urls = axis_and_urls(repo)
        assert axis == expected_times, f"axis {axis} != {expected_times}"
        want = [expected_url_of(t) for t in axis]
        assert urls == want, f"urls {urls} != {want}"

    with tempfile.TemporaryDirectory() as d:
        repo = _repo(d)

        # create
        write(repo, d, [0, 2, 4])
        check(repo, [0, 2, 4], url_for)
        print("PASS create       -> source_url aligned")

        # tail append
        write(repo, d, [6, 8])
        check(repo, [0, 2, 4, 6, 8], url_for)
        print("PASS tail-append  -> source_url extended")

        # reprocess time=2 from a NEW url -> source_url for 2 updates, no dup
        session = repo.writable_session("main")
        vds = _make_vds([2], d)
        plan = build_write_plan(vds, read_store_coordinate(session.store, "time"), "time")
        apply_write_plan(session, vds, plan)
        reconcile_source_urls(session, {2: "s3://bucket/g2_REPROCESSED.nc"}, "time")
        session.commit("reprocess 2")
        check(repo, [0, 2, 4, 6, 8],
              lambda t: "s3://bucket/g2_REPROCESSED.nc" if t == 2 else url_for(t))
        print("PASS reprocess    -> source_url overwritten in place")

        # out-of-order insert (store already has source_url -> relabel stays cheap)
        write(repo, d, [1, 3, 5, 7])
        check(repo, [0, 1, 2, 3, 4, 5, 6, 7, 8],
              lambda t: "s3://bucket/g2_REPROCESSED.nc" if t == 2 else url_for(t))
        print("PASS insert       -> source_url stayed aligned after reindex")

        # delete -- planner shifts source_url automatically; no reconcile needed
        session = repo.writable_session("main")
        delete_coordinates(session, [3], "time")
        session.commit("delete 3")
        check(repo, [0, 1, 2, 4, 5, 6, 7, 8],
              lambda t: "s3://bucket/g2_REPROCESSED.nc" if t == 2 else url_for(t))
        print("PASS delete       -> source_url compacted by the planner")

        # query helper
        q = source_urls_for_values(repo.readonly_session("main").store, [2, 6, 999])
        assert q[2] == "s3://bucket/g2_REPROCESSED.nc"
        assert q[6] == url_for(6)
        assert q[999] is None
        print("PASS query        ->", q)

        # moved timestamp: granule g6.nc is reprocessed with its time shifted
        # 6 -> 10 (SAME url, new time). retire_moved_urls deletes the old step so
        # it becomes a move, not an orphan-leaving duplicate.
        moved_url = url_for(6)                      # "s3://bucket/g6.nc"
        moved_map = {10: moved_url}                 # same url, new time 10
        session = repo.writable_session("main")
        summary = retire_moved_urls(session, moved_map, "time")
        assert summary["retired"] == [6], summary   # old time 6 scheduled for removal
        vds = _make_vds([10], d)
        plan = build_write_plan(vds, read_store_coordinate(session.store, "time"), "time")
        apply_write_plan(session, vds, plan)
        reconcile_source_urls(session, moved_map, "time")
        session.commit("move 6->10")

        final = read_source_url_map(repo.readonly_session("main").store)
        assert sorted(final) == [0, 1, 2, 4, 5, 7, 8, 10], sorted(final)
        assert final[10] == moved_url and 6 not in final
        # no URL appears at more than one time -> no orphan duplicates
        seen = {}
        for t, u in final.items():
            assert u not in seen, f"duplicate url {u} at {seen.get(u)} and {t}"
            seen[u] = t
        print("PASS moved-time   -> old step retired, no duplicate URL")

    print("ALL PASS")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    _selftest()
