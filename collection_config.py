"""
Single source of truth for per-collection Icechunk v2 settings.

Both the initial store *generation* (generate_icechunk.py) and the *append*
pipeline (append_lambda_handler.py, sqs_append_granules.py, append_granules.py)
import from here so a collection's handling is defined in exactly one place.

Schema (all fields optional except concat_dim/data_vars/coords, which have
defaults via get_collection_config):

    concat_dim : str            dimension to append/concat along (default "time")
    data_vars  : "minimal" | [str, ...]   xarray data_vars strategy
    coords     : "minimal" | "all"        xarray coords strategy
    preprocess : str | None     key into PREPROCESS_FUNCTIONS (per-granule fn)
    sort       : bool           sortby(concat_dim) after combine/append
    attrs      : dict           extra dataset attributes to set at generation
    n_workers  : int            Dask workers for generation (default 32)
    memory_limit : str          Dask per-worker memory for generation
    batch_size : int            granules per mfdataset batch at generation
"""

import re

import numpy as np


# ---------------------------------------------------------------------------
# Host / URL helpers (shared by generation + append)
# ---------------------------------------------------------------------------

BUCKET_TO_HOST = {
    "podaac-swot-ops-cumulus-protected": "archive.swot.podaac.earthdata.nasa.gov",
    "podaac-swot-ops-cumulus-public": "archive.swot.podaac.earthdata.nasa.gov",
}
DEFAULT_HTTPS_HOST = "archive.podaac.earthdata.nasa.gov"


def https_host_for_bucket(bucket):
    return BUCKET_TO_HOST.get(bucket, DEFAULT_HTTPS_HOST)


def credentials_endpoint_for_bucket(bucket):
    return f"https://{https_host_for_bucket(bucket)}/s3credentials"


def s3_to_http_url(s3_url, bucket=None):
    """Convert s3://bucket/key to the matching https archive URL."""
    if not s3_url.startswith("s3://"):
        return s3_url
    raw_path = s3_url.replace("s3://", "")
    src_bucket = bucket or raw_path.split("/", 1)[0]
    return f"https://{https_host_for_bucket(src_bucket)}/{raw_path}"


def get_store_prefix_s3(collection):
    return f"virtual_collections/{collection}/{collection}.icechunk_v2.s3/"


def get_store_prefix_https(collection):
    return f"virtual_collections/{collection}/{collection}.icechunk_v2.https/"


# ---------------------------------------------------------------------------
# Per-granule preprocessing functions
# ---------------------------------------------------------------------------

def _preprocess_expand_time_dim(ds):
    """SMAP granules lack a time dim; add one so they concat along time."""
    return ds.expand_dims("time") if "time" not in ds.dims else ds


def _preprocess_time_from_filename(ds):
    """NEUROST granules store time=0; the real date is only in the filename:
    NeurOST_SSH-SST_YYYYMMDD_YYYYMMDD.nc
    """
    source = ds.encoding.get("source", "") or ""
    match = re.search(r"NeurOST_SSH-SST_(\d{8})_", source)
    if match:
        d = match.group(1)
        date = np.datetime64(f"{d[:4]}-{d[4:6]}-{d[6:8]}")
    else:
        date = ds["time"].values.flat[0] if "time" in ds.coords else np.datetime64("NaT")
    return ds.assign_coords(time=[date])


PREPROCESS_FUNCTIONS = {
    "expand-time-dim": _preprocess_expand_time_dim,
    "time-from-filename": _preprocess_time_from_filename,
}


# ---------------------------------------------------------------------------
# Collection configuration
# ---------------------------------------------------------------------------

_SMAP_16_VAR = [
    "sss_smap", "sss_smap_unc", "sss_smap_40km", "sss_smap_40km_unc",
    "sss_smap_RF", "sss_smap_RF_unc", "sss_ref", "gland", "fland",
    "gice_est", "surtep", "winspd", "nobs", "nobs_40km", "nobs_RF",
    "sea_ice_zones",
]

DEFAULT_CONFIG = {
    "concat_dim": "time",
    "data_vars": "minimal",
    "coords": "minimal",
    "preprocess": None,
    "sort": False,
    "attrs": {},
    "n_workers": 32,
    "memory_limit": "4GiB",
    "batch_size": 500,
}

COLLECTION_CONFIG = {
    "MUR25-JPL-L4-GLOB-v04.2": {
        "n_workers": 32,
        "attrs": {
            "identifier_product_doi": "https://doi.org/10.5067/GHM25-4FJ42",
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for MUR25",
        },
    },
    "OSTIA-UKMO-L4-GLOB-REP-v2.0": {
        "n_workers": 32,
        "attrs": {
            "time_coverage_start": "1990-01-01T00:00:00Z",
            "time_coverage_end": "1999-12-31T00:00:00Z",
            "identifier_product_doi": "https://doi.org/10.5067/GHOST-4RM02",
            "date_created": "2026-08-05T00:00:00Z",
            "history": "Icechunk v2  VDS for OSTIA",
        },
    },
    "CCMP_WINDS_10M6HR_L4_V3.1": {
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for CCMP Winds",
        },
    },
    "ECCO_L4_OBP_05DEG_DAILY_V4R4B": {
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for ECCO L4 OBP",
        },
    },
    "ECCO_L4_OCEAN_VEL_05DEG_DAILY_V4R4": {
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for ECCO L4 Ocean Velocity",
        },
    },
    "ECCO_L4_SSH_05DEG_DAILY_V4R4B": {
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for ECCO L4 SSH",
        },
    },
    "ECCO_L4_TEMP_SALINITY_05DEG_DAILY_V4R4": {
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for ECCO L4 Temp Salinity",
        },
    },
    "TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4": {
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for TELLUS GRACE/GRACE-FO Mascon",
        },
    },
    "SMAP_RSS_L3_SSS_SMI_8DAY-RUNNINGMEAN_V6": {
        "data_vars": _SMAP_16_VAR,
        "preprocess": "expand-time-dim",
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for SMAP RSS L3 SSS",
        },
    },
    "NEUROST_SSH-SST_L4_V2024.0": {
        "coords": "all",
        "preprocess": "time-from-filename",
        "sort": True,
        "n_workers": 64,
        "attrs": {
            "date_created": "2026-09-03T00:00:00Z",
            "history": "Icechunk v2 VDS for NEUROST SSH-SST",
        },
    },
}


def get_collection_config(collection):
    """Return a fully-populated config for a collection, applying defaults for
    any unspecified field. Unknown collections get all defaults."""
    merged = dict(DEFAULT_CONFIG)
    merged.update(COLLECTION_CONFIG.get(collection, {}))
    return merged


def get_preprocess_fn(config):
    """Resolve the preprocess function (or None) from a config dict."""
    name = config.get("preprocess")
    return PREPROCESS_FUNCTIONS.get(name) if name else None
