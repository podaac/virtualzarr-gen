"""
Test the SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6 Icechunk v2 store
hosted at UAT archive.podaac.

Opens the store read-only over HTTPS, loads it as an xarray Dataset,
and checks structure, dimensions, coordinates, and data accessibility.
"""

import pytest
import icechunk
import xarray as xr
import numpy as np


def _has_earthdata_auth():
    try:
        import earthaccess
        auth = earthaccess.login()
        return auth.authenticated
    except Exception:
        return False


STORE_URL = (
    "https://archive.podaac.uat.earthdata.nasa.gov/"
    "podaac-uat-cumulus-public/virtual_collections/"
    "SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6/"
    "SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6.icechunk_v2.https"
)

VCC_PREFIX = "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/"

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


def _open_repo(with_auth=False):
    storage = icechunk.http_storage(base_url=STORE_URL)

    if with_auth:
        import earthaccess
        earthaccess.login()
        edl_token = earthaccess.get_edl_token()

        config = icechunk.Repository.fetch_config(storage)
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                VCC_PREFIX,
                icechunk.http_store(
                    headers={"Authorization": f"Bearer {edl_token['access_token']}"}
                ),
            )
        )
        repo = icechunk.Repository.open(
            storage,
            config=config,
            authorize_virtual_chunk_access={VCC_PREFIX: icechunk.credentials.HttpAccess},
        )
    else:
        repo = icechunk.Repository.open(storage)

    return repo


@pytest.fixture(scope="module")
def ds():
    repo = _open_repo(with_auth=False)
    session = repo.readonly_session(branch="main")
    return xr.open_zarr(session.store, consolidated=False)


@pytest.fixture(scope="module")
def ds_with_auth():
    repo = _open_repo(with_auth=True)
    session = repo.readonly_session(branch="main")
    return xr.open_zarr(session.store, consolidated=False)


def test_dataset_loads(ds):
    assert ds is not None
    assert isinstance(ds, xr.Dataset)


def test_dimensions(ds):
    assert set(ds.sizes.keys()) == EXPECTED_DIMS


def test_dimension_sizes(ds):
    assert ds.sizes["lat"] == 720
    assert ds.sizes["lon"] == 1440
    assert ds.sizes["time"] > 0
    assert ds.sizes["iceflag_components"] == 3
    assert ds.sizes["uncertainty_components"] == 9


def test_coordinates_present(ds):
    assert EXPECTED_COORDS.issubset(set(ds.coords))


def test_expected_data_vars(ds):
    missing = EXPECTED_DATA_VARS - set(ds.data_vars)
    assert not missing, f"Missing data variables: {missing}"


def test_lat_lon_range(ds):
    lat = ds["lat"].values
    lon = ds["lon"].values
    assert lat.min() >= -90.0
    assert lat.max() <= 90.0
    assert lon.min() >= -180.0
    assert lon.max() <= 360.0


def test_time_dimension(ds):
    assert ds.sizes["time"] > 0
    time_vals = ds["time"].values
    assert np.issubdtype(time_vals.dtype, np.datetime64) or "time" in str(
        ds["time"].encoding.get("units", "")
    )


def test_sss_smap_has_time_dimension(ds):
    var = ds["sss_smap"]
    assert "time" in var.dims, "sss_smap must include time dimension for time series analysis"
    assert var.dims == ("time", "lat", "lon")
    assert var.shape[1] == 720
    assert var.shape[2] == 1440
    assert var.shape[0] == ds.sizes["time"]


@pytest.mark.skipif(
    not _has_earthdata_auth(),
    reason="Earthdata authentication required to read virtual chunks",
)
def test_can_read_sss_smap_slice(ds_with_auth):
    # Pick mid-ocean region to avoid NaN over land/ice
    sample = ds_with_auth["sss_smap"].isel(time=0, lat=slice(300, 400), lon=slice(600, 700)).values
    assert sample.shape == (100, 100)
    assert not np.all(np.isnan(sample)), "Expected some non-NaN data values in mid-ocean region"


def test_global_attrs(ds):
    assert len(ds.attrs) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
