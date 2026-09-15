"""
Test locally generated Icechunk v2 stores for NEUROST_SSH-SST_L4_V2024.0.

Validates both the S3-reference and HTTP-reference stores produced by
vz2_icechunk2_NEUROST.py. Run the generation script first, then run
these tests against the local output.
"""

from pathlib import Path

import pytest
import icechunk
import xarray as xr
import numpy as np


SCRIPT_DIR = Path(__file__).parent
FNAME_STORE_S3 = SCRIPT_DIR / "NEUROST_SSH-SST_L4_V2024.0.icechunk_v2.s3"
FNAME_STORE_HTTP = SCRIPT_DIR / "NEUROST_SSH-SST_L4_V2024.0.icechunk_v2.https"

EXPECTED_DIMS = {"time", "latitude", "longitude"}
EXPECTED_COORDS = {"time", "latitude", "longitude", "Lambert_Azimuthal_Grid"}
EXPECTED_DATA_VARS = {
    "adt", "sla", "sn", "ss", "ugos", "ugosa", "vgos", "vgosa", "zeta",
}

NUM_GRANULES = 5459


def _open_local_store(store_path):
    storage = icechunk.local_filesystem_storage(path=str(store_path))
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session(branch="main")
    return xr.open_zarr(session.store, consolidated=False)


# ---------------------------------------------------------------------------
# S3 store tests
# ---------------------------------------------------------------------------

s3_exists = pytest.mark.skipif(
    not FNAME_STORE_S3.exists(),
    reason=f"Local S3 store not found at {FNAME_STORE_S3}. Run vz2_icechunk2_NEUROST.py first.",
)


@pytest.fixture(scope="module")
def ds_s3():
    if not FNAME_STORE_S3.exists():
        pytest.skip("S3 store not generated yet")
    return _open_local_store(FNAME_STORE_S3)


@s3_exists
def test_s3_dataset_loads(ds_s3):
    assert isinstance(ds_s3, xr.Dataset)
    print("\n=== S3 Store Dataset ===")
    print(ds_s3)
    print("\n=== Dimensions ===")
    print(dict(ds_s3.sizes))
    print("\n=== Variable dims & shapes ===")
    for name in sorted(ds_s3.data_vars):
        print(f"  {name}: dims={ds_s3[name].dims}  shape={ds_s3[name].shape}")
    print("\n=== Time (first 10) ===")
    print(ds_s3["time"].values[:10])
    print(f"\n=== Time (last 10) ===")
    print(ds_s3["time"].values[-10:])
    print(f"\nTotal time steps: {ds_s3.sizes['time']}")
    print(f"Unique time values: {len(np.unique(ds_s3['time'].values))}")
    print(f"\n=== Global Attrs ===")
    for k, v in list(ds_s3.attrs.items())[:10]:
        print(f"  {k}: {v}")


@s3_exists
def test_s3_dimensions(ds_s3):
    assert set(ds_s3.sizes.keys()) == EXPECTED_DIMS


@s3_exists
def test_s3_dimension_sizes(ds_s3):
    assert ds_s3.sizes["latitude"] == 1500
    assert ds_s3.sizes["longitude"] == 3600
    assert ds_s3.sizes["time"] == NUM_GRANULES


@s3_exists
def test_s3_coordinates(ds_s3):
    assert EXPECTED_COORDS.issubset(set(ds_s3.coords))


@s3_exists
def test_s3_data_vars(ds_s3):
    missing = EXPECTED_DATA_VARS - set(ds_s3.data_vars)
    assert not missing, f"Missing data variables: {missing}"


@s3_exists
def test_s3_no_unexpected_data_vars(ds_s3):
    extra = set(ds_s3.data_vars) - EXPECTED_DATA_VARS
    assert not extra, f"Unexpected data variables: {extra}"


@s3_exists
def test_s3_all_vars_have_time_dim(ds_s3):
    for name in EXPECTED_DATA_VARS:
        if name in ds_s3.data_vars:
            assert "time" in ds_s3[name].dims, (
                f"{name} missing time dimension — has {ds_s3[name].dims}"
            )


@s3_exists
def test_s3_var_shapes(ds_s3):
    for name in EXPECTED_DATA_VARS:
        if name in ds_s3.data_vars:
            var = ds_s3[name]
            assert var.shape == (1500, 3600, NUM_GRANULES), (
                f"{name} has unexpected shape {var.shape}"
            )


@s3_exists
def test_s3_time_has_correct_count(ds_s3):
    assert len(ds_s3["time"].values) == NUM_GRANULES


@s3_exists
def test_s3_time_values_are_unique(ds_s3):
    time_vals = ds_s3["time"].values
    assert len(np.unique(time_vals)) == len(time_vals), (
        f"Only {len(np.unique(time_vals))} unique values out of {len(time_vals)} time steps"
    )


@s3_exists
def test_s3_time_is_monotonically_increasing(ds_s3):
    time_vals = ds_s3["time"].values
    assert np.all(time_vals[:-1] < time_vals[1:]), "Time coordinate must be strictly increasing"


@s3_exists
def test_s3_time_spans_expected_range(ds_s3):
    time_vals = ds_s3["time"].values
    assert time_vals[0] >= np.datetime64("2010-01-01")
    assert time_vals[-1] >= np.datetime64("2024-12-01"), (
        f"Last time value is {time_vals[-1]}, expected >= 2024-12-01"
    )


@s3_exists
def test_s3_latitude_range(ds_s3):
    lat = ds_s3["latitude"].values
    assert lat.min() >= -90.0
    assert lat.max() <= 90.0


@s3_exists
def test_s3_longitude_range(ds_s3):
    lon = ds_s3["longitude"].values
    assert lon.min() >= -180.0
    assert lon.max() <= 360.0


@s3_exists
def test_s3_global_attrs(ds_s3):
    assert len(ds_s3.attrs) > 0
    assert "date_created" in ds_s3.attrs
    assert "history" in ds_s3.attrs


# ---------------------------------------------------------------------------
# HTTP store tests
# ---------------------------------------------------------------------------

http_exists = pytest.mark.skipif(
    not FNAME_STORE_HTTP.exists(),
    reason=f"Local HTTP store not found at {FNAME_STORE_HTTP}. Run vz2_icechunk2_NEUROST.py first.",
)


@pytest.fixture(scope="module")
def ds_http():
    if not FNAME_STORE_HTTP.exists():
        pytest.skip("HTTP store not generated yet")
    return _open_local_store(FNAME_STORE_HTTP)


@http_exists
def test_http_dataset_loads(ds_http):
    assert isinstance(ds_http, xr.Dataset)
    print("\n=== HTTP Store Dataset ===")
    print(ds_http)
    print("\n=== Time (first 10) ===")
    print(ds_http["time"].values[:10])
    print(f"\n=== Time (last 10) ===")
    print(ds_http["time"].values[-10:])
    print(f"\nTotal time steps: {ds_http.sizes['time']}")
    print(f"Unique time values: {len(np.unique(ds_http['time'].values))}")


@http_exists
def test_http_dimensions(ds_http):
    assert set(ds_http.sizes.keys()) == EXPECTED_DIMS


@http_exists
def test_http_dimension_sizes(ds_http):
    assert ds_http.sizes["latitude"] == 1500
    assert ds_http.sizes["longitude"] == 3600
    assert ds_http.sizes["time"] == NUM_GRANULES


@http_exists
def test_http_coordinates(ds_http):
    assert EXPECTED_COORDS.issubset(set(ds_http.coords))


@http_exists
def test_http_data_vars(ds_http):
    missing = EXPECTED_DATA_VARS - set(ds_http.data_vars)
    assert not missing, f"Missing data variables: {missing}"


@http_exists
def test_http_all_vars_have_time_dim(ds_http):
    for name in EXPECTED_DATA_VARS:
        if name in ds_http.data_vars:
            assert "time" in ds_http[name].dims, (
                f"{name} missing time dimension — has {ds_http[name].dims}"
            )


@http_exists
def test_http_time_values_are_unique(ds_http):
    time_vals = ds_http["time"].values
    assert len(np.unique(time_vals)) == len(time_vals), (
        f"Only {len(np.unique(time_vals))} unique values out of {len(time_vals)} time steps"
    )


@http_exists
def test_http_time_is_monotonically_increasing(ds_http):
    time_vals = ds_http["time"].values
    assert np.all(time_vals[:-1] < time_vals[1:]), "Time coordinate must be strictly increasing"


@http_exists
def test_http_time_matches_s3(ds_s3, ds_http):
    np.testing.assert_array_equal(ds_s3["time"].values, ds_http["time"].values)


@http_exists
def test_http_global_attrs(ds_http):
    assert len(ds_http.attrs) > 0
    assert "date_created" in ds_http.attrs
    assert "history" in ds_http.attrs


# ---------------------------------------------------------------------------
# Cross-store consistency
# ---------------------------------------------------------------------------

both_exist = pytest.mark.skipif(
    not (FNAME_STORE_S3.exists() and FNAME_STORE_HTTP.exists()),
    reason="Both local stores required for cross-store tests",
)


@both_exist
def test_both_stores_same_structure(ds_s3, ds_http):
    assert set(ds_s3.sizes.keys()) == set(ds_http.sizes.keys())
    assert dict(ds_s3.sizes) == dict(ds_http.sizes)
    assert set(ds_s3.data_vars) == set(ds_http.data_vars)
    assert set(ds_s3.coords) == set(ds_http.coords)


@both_exist
def test_both_stores_same_attrs(ds_s3, ds_http):
    for key in ["date_created", "history"]:
        assert ds_s3.attrs[key] == ds_http.attrs[key]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
