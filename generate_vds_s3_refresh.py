#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import time
import multiprocessing
import threading

import fsspec
import earthaccess
import xarray as xr
from virtualizarr import open_virtual_dataset
from dask import delayed
import dask.array as da
from dask.distributed import Client

fs = None
fs_last_refresh = 0
fs_lock = threading.Lock()
FS_REFRESH_INTERVAL = 50 * 60  # 50 minutes in seconds

def setup_logging(debug=False):
    log_format = "%(asctime)s %(levelname)s %(message)s"
    if debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
        log = logging.getLogger('urllib3')
        log.setLevel(logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)

def opends_withref(ref, fs_data):
    """Open dataset from a reference file using xarray."""
    storage_opts = {"fo": ref, "remote_protocol": "https", "remote_options": fs_data.storage_options}
    fs_ref = fsspec.filesystem('reference', **storage_opts)
    m = fs_ref.get_mapper('')
    data = xr.open_dataset(
        m, engine="zarr", chunks={}, decode_times=False,
        backend_kwargs={"consolidated": False}
    )
    return data

def get_fs():
    global fs, fs_last_refresh
    now = time.time()
    if fs is None or (now - fs_last_refresh) > FS_REFRESH_INTERVAL:
        with fs_lock:
            # Double-check after acquiring lock
            if fs is None or (time.time() - fs_last_refresh) > FS_REFRESH_INTERVAL:
                earthaccess.login()
                fs = earthaccess.get_s3_filesystem(daac="PODAAC")
                fs_last_refresh = time.time()
    return fs

# Usage in your Dask-delayed function:
@delayed
def open_vds_par(datalink, loadable_variables=None):
    fs_obj = get_fs()
    reader_options = {"storage_options": fs_obj.storage_options}
    return open_virtual_dataset(
        datalink, indexes={}, reader_options=reader_options,
        loadable_variables=loadable_variables, decode_times=False
    )


def main(
    collection,
    loadable_coord_vars,
    start_date,
    end_date,
    debug=False
):
    setup_logging(debug)
    logging.info(f"Collection: {collection}")
    logging.info(f"Vars: {loadable_coord_vars}")
    logging.info(f"start_date: {start_date}")
    logging.info(f"end_date: {end_date}")

    xr.set_options(
        display_expand_attrs=False,
        display_expand_coords=True,
        display_expand_data=True,
    )

    # Earthdata login
    earthaccess.login()

    # Get HTTPS session for fsspec
    fs = earthaccess.get_s3_filesystem(daac="PODAAC")
    print(fs)

    # Search for granules
    if start_date or end_date:
        granule_info = earthaccess.search_data(
            short_name=collection,
            temporal=(start_date, end_date)
        )
    else:
        logging.info("Getting all granules...")
        granule_info = earthaccess.search_data(short_name=collection)

    # Get S3 links
    logging.info(f"Found {len(granule_info)} granules.")
    data_s3links = [g.data_links(access="direct")[0] for g in granule_info]

    logging.info(f"Found {len(data_s3links)} data files.")
    coord_vars = loadable_coord_vars.split(",")
    reader_opts = {"storage_options": fs.storage_options}

    # Parallel reference creation for all files
    logging.info(f"CPU count = {multiprocessing.cpu_count()}")
    #client = Client(n_workers=multiprocessing.cpu_count(), threads_per_worker=1)
    client = Client(n_workers=16, threads_per_worker=1)

    logging.info("Generating references for all files...")

    tasks = [
        open_vds_par(p, indexes={}, reader_options=reader_opts, loadable_variables=coord_vars, decode_times=False) 
        for p in data_s3links
        ]
    virtual_ds_list = da.compute(tasks)[0]

    # Combine references
    logging.info("Combining references...")
    virtual_ds_combined = xr.combine_nested(
        virtual_ds_list, concat_dim='time', coords="minimal", compat="override", combine_attrs='drop_conflicts'
    )

    if not virtual_ds_combined.attrs:
        logging.info("Global Attributes not found for generated dataset.")
        sys.exit(1)

    # Filename for combined reference
    temporal = ""
    if start_date or end_date:
        start = start_date if start_date else "beginning"
        end = end_date if end_date else "present"
        temporal = f'{start}_to_{end}_'

    fname_combined_json = f'{collection}_{temporal}virtual_s3.json'
    virtual_ds_combined.virtualize.to_kerchunk(fname_combined_json, format='json')
    logging.info(f"Saved: {fname_combined_json}")

    # Test lazy loading of the combined reference file
    data_json = opends_withref(fname_combined_json, fs)
    logging.info(f"Test open with combined reference file: {data_json}")

    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Cloud Optimized Store Reference Files")
    parser.add_argument("--collection", type=str, required=True, help="Earthdata collection short name")
    parser.add_argument("--loadable-coord-vars", type=str, default="latitude,longitude,time", help="Comma-separated list of loadable coordinate variables")
    parser.add_argument("--start-date", type=str, default=None, help="Start date (e.g., 1-1-2022)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (e.g., 1-1-2025)")
    parser.add_argument("--debug", action="store_true", default=True, help="Enable debug logging")
    args = parser.parse_args()

    main(
        args.collection,
        args.loadable_coord_vars,
        args.start_date,
        args.end_date,
        args.debug
    )