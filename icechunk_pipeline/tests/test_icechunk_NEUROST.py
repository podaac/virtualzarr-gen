"""
Test the NEUROST_SSH-SST_L4_V2024.0 Icechunk v2 store
hosted at UAT archive.podaac.

Opens the store read-only over HTTPS, loads it as an xarray Dataset,
and checks structure, dimensions, coordinates, and data accessibility.

Known issue: the time coordinate is broken — most values are stuck at
2010-01-01 instead of increasing daily. Only 531 unique values exist
across 5459 time steps, and they are not monotonically sorted. This is
caused by the generation script using decode_times=False with
coords="minimal", which replicates the first granule's raw time value
instead of concatenating the actual time from each file.
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
    "NEUROST_SSH-SST_L4_V2024.0/"
    "NEUROST_SSH-SST_L4_V2024.0.icechunk_v2.https"
)

VCC_PREFIX = "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/"

EXPECTED_DIMS = {"time", "latitude", "longitude"}
EXPECTED_COORDS = {"time", "latitude", "longitude", "Lambert_Azimuthal_Grid"}
EXPECTED_DATA_VARS = {
    "adt", "sla", "sn", "ss", "ugos", "ugosa", "vgos", "vgosa", "zeta",
}

NUM_GRANULES = 5459


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


# ---------------------------------------------------------------------------
# Structure tests (no auth needed — metadata only)
# ---------------------------------------------------------------------------

def test_dataset_loads(ds):
    assert ds is not None
    assert isinstance(ds, xr.Dataset)


def test_dimensions(ds):
    assert set(ds.sizes.keys()) == EXPECTED_DIMS


def test_dimension_sizes(ds):
    assert ds.sizes["latitude"] == 1500
    assert ds.sizes["longitude"] == 3600
    assert ds.sizes["time"] == NUM_GRANULES


def test_coordinates_present(ds):
    assert EXPECTED_COORDS.issubset(set(ds.coords))


def test_expected_data_vars(ds):
    missing = EXPECTED_DATA_VARS - set(ds.data_vars)
    assert not missing, f"Missing data variables: {missing}"


def test_no_unexpected_data_vars(ds):
    extra = set(ds.data_vars) - EXPECTED_DATA_VARS
    assert not extra, f"Unexpected data variables: {extra}"


def test_all_vars_have_time_dim(ds):
    for name in EXPECTED_DATA_VARS:
        if name in ds.data_vars:
            assert "time" in ds[name].dims, (
                f"{name} missing time dimension — has {ds[name].dims}"
            )


def test_var_shapes(ds):
    for name in EXPECTED_DATA_VARS:
        if name in ds.data_vars:
            var = ds[name]
            assert var.shape == (1500, 3600, NUM_GRANULES), (
                f"{name} has unexpected shape {var.shape}"
            )


def test_latitude_range(ds):
    lat = ds["latitude"].values
    assert lat.min() >= -90.0
    assert lat.max() <= 90.0


def test_longitude_range(ds):
    lon = ds["longitude"].values
    assert lon.min() >= -180.0
    assert lon.max() <= 360.0


def test_global_attrs(ds):
    assert len(ds.attrs) > 0


# ---------------------------------------------------------------------------
# Time coordinate quality tests — these document the known bug
# ---------------------------------------------------------------------------

def test_time_has_correct_count(ds):
    assert ds.sizes["time"] == NUM_GRANULES


def test_time_values_are_unique(ds):
    time_vals = ds["time"].values
    assert len(np.unique(time_vals)) == len(time_vals), (
        f"Only {len(np.unique(time_vals))} unique values out of {len(time_vals)} time steps"
    )


def test_time_is_monotonically_increasing(ds):
    time_vals = ds["time"].values
    assert np.all(time_vals[:-1] < time_vals[1:])


def test_time_spans_expected_range(ds):
    time_vals = ds["time"].values
    first = np.datetime64("2010-01-01")
    last = np.datetime64("2024-12-01")
    assert time_vals[0] >= first
    assert time_vals[-1] >= last, (
        f"Last time value is {time_vals[-1]}, expected >= {last}"
    )


# ---------------------------------------------------------------------------
# Data read test (requires Earthdata auth)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not _has_earthdata_auth(),
    reason="Earthdata authentication required to read virtual chunks",
)
def test_can_read_sla_slice(ds_with_auth):
    # Pick a mid-grid region
    sample = ds_with_auth["sla"].isel(
        time=0, latitude=slice(700, 710), longitude=slice(1700, 1710)
    ).values
    assert sample.shape == (10, 10)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
