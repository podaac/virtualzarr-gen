"""
Test locally generated Icechunk v2 stores for SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6.

Validates both the S3-reference and HTTP-reference stores produced by
vz2_icechunk2_SMAP_SSS.py. Run the generation script first, then run
these tests against the local output.
"""

import os
from pathlib import Path

import pytest
import icechunk
import xarray as xr
import numpy as np


SCRIPT_DIR = Path(__file__).parent
FNAME_STORE_S3 = SCRIPT_DIR / "SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6.icechunk_v2.s3"
FNAME_STORE_HTTP = SCRIPT_DIR / "SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6.icechunk_v2.https"

EXPECTED_DIMS = {"time", "lat", "lon", "iceflag_components", "uncertainty_components"}
EXPECTED_COORDS = {"time", "lat", "lon"}
EXPECTED_DATA_VARS = {
    "sss_smap",
    "sss_smap_unc",
    "sss_smap_40km",
    "sss_smap_40km_unc",
    "sss_smap_RF",
    "sss_smap_RF_unc",
    "sss_ref",
    "gland",
    "fland",
    "gice_est",
    "surtep",
    "winspd",
    "nobs",
    "nobs_40km",
    "nobs_RF",
    "anc_sea_ice_flag",
    "sea_ice_zones",
    "sss_smap_unc_comp",
    "sss_smap_40km_unc_comp",
}

VARS_WITH_TIME = {
    "sss_smap", "sss_smap_unc", "sss_smap_40km", "sss_smap_40km_unc",
    "sss_smap_RF", "sss_smap_RF_unc", "sss_ref", "gland", "fland",
    "gice_est", "surtep", "winspd", "nobs", "nobs_40km", "nobs_RF",
    "sea_ice_zones",
}

# These have different chunk shapes and are not concatenated along time
VARS_WITHOUT_TIME = {
    "anc_sea_ice_flag", "sss_smap_unc_comp", "sss_smap_40km_unc_comp",
}

NUM_GRANULES = 4048


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
    reason=f"Local S3 store not found at {FNAME_STORE_S3}. Run vz2_icechunk2_SMAP_SSS.py first.",
)


@pytest.fixture(scope="module")
def ds_s3():
    if not FNAME_STORE_S3.exists():
        pytest.skip("S3 store not generated yet")
    return _open_local_store(FNAME_STORE_S3)


@s3_exists
def test_s3_dataset_loads(ds_s3):
    assert isinstance(ds_s3, xr.Dataset)


@s3_exists
def test_s3_dimensions(ds_s3):
    assert set(ds_s3.sizes.keys()) == EXPECTED_DIMS


@s3_exists
def test_s3_dimension_sizes(ds_s3):
    assert ds_s3.sizes["lat"] == 720
    assert ds_s3.sizes["lon"] == 1440
    assert ds_s3.sizes["time"] == NUM_GRANULES
    assert ds_s3.sizes["iceflag_components"] == 3
    assert ds_s3.sizes["uncertainty_components"] == 9


@s3_exists
def test_s3_coordinates(ds_s3):
    assert EXPECTED_COORDS.issubset(set(ds_s3.coords))


@s3_exists
def test_s3_data_vars(ds_s3):
    missing = EXPECTED_DATA_VARS - set(ds_s3.data_vars)
    assert not missing, f"Missing data variables: {missing}"


@s3_exists
def test_s3_time_dimension(ds_s3):
    time_vals = ds_s3["time"].values
    assert len(time_vals) == NUM_GRANULES
    assert np.issubdtype(time_vals.dtype, np.datetime64) or "time" in str(
        ds_s3["time"].encoding.get("units", "")
    )


@s3_exists
def test_s3_time_is_sorted(ds_s3):
    time_vals = ds_s3["time"].values
    assert np.all(time_vals[:-1] <= time_vals[1:]), "Time coordinate must be monotonically increasing"


@s3_exists
def test_s3_lat_lon_range(ds_s3):
    lat = ds_s3["lat"].values
    lon = ds_s3["lon"].values
    assert lat.min() >= -90.0
    assert lat.max() <= 90.0
    assert lon.min() >= -180.0
    assert lon.max() <= 360.0


@s3_exists
def test_s3_vars_have_time_dim(ds_s3):
    for name in VARS_WITH_TIME:
        if name in ds_s3.data_vars:
            assert "time" in ds_s3[name].dims, (
                f"{name} missing time dimension — has {ds_s3[name].dims}"
            )


@s3_exists
def test_s3_sss_smap_shape(ds_s3):
    var = ds_s3["sss_smap"]
    assert var.dims == ("time", "lat", "lon")
    assert var.shape == (NUM_GRANULES, 720, 1440)


@s3_exists
def test_s3_global_attrs(ds_s3):
    assert len(ds_s3.attrs) > 0
    assert "date_created" in ds_s3.attrs
    assert "history" in ds_s3.attrs


@s3_exists
def test_s3_virtual_refs_are_s3(ds_s3):
    store = ds_s3["sss_smap"].encoding.get("source", "")
    if store:
        assert "s3://" in store, f"Expected S3 paths, got: {store}"


# ---------------------------------------------------------------------------
# HTTP store tests
# ---------------------------------------------------------------------------

http_exists = pytest.mark.skipif(
    not FNAME_STORE_HTTP.exists(),
    reason=f"Local HTTP store not found at {FNAME_STORE_HTTP}. Run vz2_icechunk2_SMAP_SSS.py first.",
)


@pytest.fixture(scope="module")
def ds_http():
    if not FNAME_STORE_HTTP.exists():
        pytest.skip("HTTP store not generated yet")
    return _open_local_store(FNAME_STORE_HTTP)


@http_exists
def test_http_dataset_loads(ds_http):
    assert isinstance(ds_http, xr.Dataset)


@http_exists
def test_http_dimensions(ds_http):
    assert set(ds_http.sizes.keys()) == EXPECTED_DIMS


@http_exists
def test_http_dimension_sizes(ds_http):
    assert ds_http.sizes["lat"] == 720
    assert ds_http.sizes["lon"] == 1440
    assert ds_http.sizes["time"] == NUM_GRANULES
    assert ds_http.sizes["iceflag_components"] == 3
    assert ds_http.sizes["uncertainty_components"] == 9


@http_exists
def test_http_coordinates(ds_http):
    assert EXPECTED_COORDS.issubset(set(ds_http.coords))


@http_exists
def test_http_data_vars(ds_http):
    missing = EXPECTED_DATA_VARS - set(ds_http.data_vars)
    assert not missing, f"Missing data variables: {missing}"


@http_exists
def test_http_vars_have_time_dim(ds_http):
    for name in VARS_WITH_TIME:
        if name in ds_http.data_vars:
            assert "time" in ds_http[name].dims, (
                f"{name} missing time dimension — has {ds_http[name].dims}"
            )


@http_exists
def test_http_sss_smap_shape(ds_http):
    var = ds_http["sss_smap"]
    assert var.dims == ("time", "lat", "lon")
    assert var.shape == (NUM_GRANULES, 720, 1440)


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
