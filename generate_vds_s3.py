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

def process_in_batches(data_s3links, coord_vars, fs, batch_size=1000):
    """Process S3 links in batches, refreshing filesystem between batches."""

    earthaccess.login()

    virtual_ds_list = []
    
    for i in range(0, len(data_s3links), batch_size):
        batch = data_s3links[i:i + batch_size]
        
        # Get HTTPS session for fsspec
        fs = earthaccess.get_s3_filesystem(daac="PODAAC")

        reader_opts = {"storage_options": fs.storage_options}
        open_vds_par = delayed(open_virtual_dataset)
        
        tasks = [
            open_vds_par(
                p, 
                indexes={}, 
                reader_options=reader_opts, 
                loadable_variables=coord_vars, 
                decode_times=False
            ) 
            for p in batch
        ]
        
        batch_results = da.compute(tasks)[0]
        virtual_ds_list.extend(batch_results)
            
    return virtual_ds_list

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
    client = Client(n_workers=16, threads_per_worker=1, memory_limit='15GB')

    logging.info("Generating references for all files...")

    #open_vds_par = delayed(open_virtual_dataset)
    #tasks = [
    #    open_vds_par(p, indexes={}, reader_options=reader_opts, loadable_variables=coord_vars, decode_times=False) 
    #    for p in data_s3links
    #    ]
    #virtual_ds_list = da.compute(tasks)[0]

    # Usage
    virtual_ds_list = process_in_batches(data_s3links, coord_vars, fs)

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