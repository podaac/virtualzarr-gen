#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import time
import multiprocessing

import fsspec
import earthaccess
import xarray as xr
from virtualizarr import open_virtual_dataset
from dask import delayed
import dask.array as da
from dask.distributed import Client

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

@delayed
def open_vds_par(datalink, reader_options=None, loadable_variables=None):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
    print(f"{timestamp} {datalink}")
    for cnt in range(1, 5):
        try:
            if cnt != 1:
                logging.debug(f"Retrying ({cnt}) {datalink}")
            return open_virtual_dataset(
                datalink, indexes={}, reader_options=reader_options,
                loadable_variables=loadable_variables, decode_times=False
            )
        except Exception as e:
            logging.debug(e)
            time.sleep(cnt ** 2)
    raise Exception(f"Could not process file {datalink}")

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
    fs = earthaccess.get_fsspec_https_session()

    # Search for granules
    if start_date or end_date:
        granule_info = earthaccess.search_data(
            short_name=collection,
            temporal=(start_date, end_date)
        )
    else:
        logging.info("Getting all granules...")
        granule_info = earthaccess.search_data(short_name=collection)

    # Get HTTPS links
    logging.info(f"Found {len(granule_info)} granules.")
    data_https_links = [g.data_links(access="https")[0] for g in granule_info]
    if not data_https_links:
        logging.info("No data links found.")
        sys.exit(1)

    logging.info(f"Found {len(data_https_links)} data files.")
    coord_vars = loadable_coord_vars.split(",")
    reader_opts = {"storage_options": fs.storage_options}

    # Create reference for the first data file
    #logging.info("Generating reference for first file...")
    #virtual_ds_example = open_virtual_dataset(
    #    data_https_links[0], indexes={},
    #    reader_options=reader_opts, loadable_variables=coord_vars, decode_times=False
    #)
    #virtual_ds_example.virtualize.to_kerchunk('virtual_ds_example.json', format='json')
    #logging.info("Saved: virtual_ds_example.json")

    # Test opening with reference
    #data_example = opends_withref('virtual_ds_example.json', fs)
    #logging.info(f"Test open with reference file: {data_example}")

    # Parallel reference creation for all files
    logging.info(f"CPU count = {multiprocessing.cpu_count()}")
    #client = Client(n_workers=multiprocessing.cpu_count(), threads_per_worker=1)
    client = Client(n_workers=8, threads_per_worker=1)

    #logging.info(f"Dask dashboard: {client.dashboard_link}")

    logging.info("Generating references for all files...")
    tasks = [
        open_vds_par(p, reader_options=reader_opts, loadable_variables=coord_vars)
        for p in data_https_links
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

    fname_combined_json = f'{collection}_{temporal}virtual_https.json'
    virtual_ds_combined.virtualize.to_kerchunk(fname_combined_json, format='json')
    logging.info(f"Saved: {fname_combined_json}")

    # Test lazy loading of the combined reference file
    data_json = opends_withref(fname_combined_json, fs)
    logging.info(f"Test open with combined reference file: {data_json}")

    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Cloud Optimized Store Reference Files")
    parser.add_argument("--collection", type=str, default="OSCAR_L4_OC_FINAL_V2.0", help="Earthdata collection short name")
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