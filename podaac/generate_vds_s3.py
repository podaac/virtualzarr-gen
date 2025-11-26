#!/usr/bin/env python3
"""
Generate Cloud Optimized Store Reference Files for Earthdata collections.

This module provides functionality to generate virtual Zarr datasets from Earthdata
collections stored in S3. It uses virtualizarr to create reference files that
enable efficient cloud-optimized access to remote datasets without downloading
the entire dataset locally.

The module processes Earthdata granules in batches, creates virtual datasets
for each granule, and combines them into a single reference file that can be
used for lazy loading of the combined dataset.
"""

import os
import sys
import argparse
import logging
import multiprocessing
import psutil

import fsspec
import earthaccess
import xarray as xr
from virtualizarr import open_virtual_dataset
from dask import delayed
import dask.array as da
from dask.distributed import Client


def print_memory_usage(note=""):
    """
    Log the current memory usage of the process.

    Args:
        note (str): Optional note to include in the log message for context.
    """
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    logging.info("[MEMORY] %s RSS: %.2f MB", note, mem_mb)


def setup_logging(debug=False):
    """
    Configure logging for the application.

    Args:
        debug (bool): If True, sets logging level to DEBUG and enables
            debug logging for urllib3. Otherwise, sets level to INFO.
    """
    log_format = "%(asctime)s %(levelname)s %(message)s"
    if debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
        log = logging.getLogger('urllib3')
        log.setLevel(logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)


def opends_withref(ref, fs_data):
    """
    Open dataset from a reference file using xarray.

    This function opens a kerchunk reference file and returns an xarray Dataset
    that can be used for lazy loading of the remote data.

    Args:
        ref (str): Path to the kerchunk reference JSON file.
        fs_data: Filesystem object with storage options for accessing remote data.

    Returns:
        xr.Dataset: An xarray Dataset opened from the reference file with
            zarr engine, configured for lazy loading without time decoding.
    """
    storage_opts = {"fo": ref, "remote_protocol": "https",
                    "remote_options": fs_data.storage_options}
    fs_ref = fsspec.filesystem('reference', **storage_opts)
    m = fs_ref.get_mapper('')
    data = xr.open_dataset(
        m, engine="zarr", chunks={}, decode_times=False,
        backend_kwargs={"consolidated": False}
    )
    return data


def process_in_batches(data_s3links, coord_vars, batch_size=36):
    """
    Process S3 links in batches, creating virtual datasets for each file.

    This function processes S3 links in batches to avoid overwhelming the
    system with too many concurrent operations. It refreshes the filesystem
    connection between batches and uses Dask for parallel processing.

    Args:
        data_s3links (list): List of S3 URLs to process.
        coord_vars (list): List of coordinate variable names to load into memory.
        batch_size (int): Number of files to process in each batch. Default is 36.

    Returns:
        list: List of virtual xarray Datasets, one for each input S3 link.
    """
    earthaccess.login()
    open_vds_par = delayed(open_virtual_dataset)

    virtual_ds_list = []
    total_batches = (len(data_s3links) + batch_size - 1) // batch_size

    for i in range(0, len(data_s3links), batch_size):
        batch_num = (i // batch_size) + 1
        batch = data_s3links[i:i + batch_size]

        logging.info("Processing batch %d of %d (%d files)",
                     batch_num, total_batches, len(batch))
        # Get HTTPS session for fsspec
        fs = earthaccess.get_s3_filesystem(daac="PODAAC")
        reader_opts = {"storage_options": fs.storage_options}
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

        batch_results = list(da.compute(*tasks))

        virtual_ds_list.extend(batch_results)

    return virtual_ds_list


def main(
    collection,
    loadable_coord_vars,
    start_date,
    end_date,
    debug=False
):
    """
    Main function to generate cloud-optimized store reference files.

    This function orchestrates the entire process:
    1. Authenticates with Earthdata
    2. Searches for granules in the specified collection and date range
    3. Processes granules in batches to create virtual datasets
    4. Combines all virtual datasets into a single reference file
    5. Saves the combined reference file and tests it

    Args:
        collection (str): Earthdata collection short name to process.
        loadable_coord_vars (str): Comma-separated list of coordinate variables
            to load into memory (e.g., "latitude,longitude,time").
        start_date (str, optional): Start date for temporal filtering
            (e.g., "1-1-2022"). If None, searches from beginning.
        end_date (str, optional): End date for temporal filtering
            (e.g., "1-1-2025"). If None, searches to present.
        debug (bool): Enable debug logging. Default is False.

    Raises:
        SystemExit: Exits with code 1 if the combined dataset has no attributes.
    """
    setup_logging(debug)
    logging.info("Collection: %s", collection)
    logging.info("Vars: %s", loadable_coord_vars)
    logging.info("start_date: %s", start_date)
    logging.info("end_date: %s", end_date)

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
    logging.info("Found %d granules.", len(granule_info))
    data_s3links = [g.data_links(access="direct")[0] for g in granule_info]

    logging.info("Found %d data files.", len(data_s3links))
    coord_vars = loadable_coord_vars.split(",")

    # Parallel reference creation for all files
    logging.info("CPU count = %d", multiprocessing.cpu_count())
    client = Client(n_workers=12, threads_per_worker=1, memory_limit='12GB')

    logging.info("Generating references for all files...")
    virtual_ds_list = process_in_batches(data_s3links, coord_vars)

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
    virtual_ds_combined.virtualize.to_kerchunk(
        fname_combined_json, format='json')
    logging.info("Saved: %s", fname_combined_json)

    # Test lazy loading of the combined reference file
    data_json = opends_withref(fname_combined_json, fs)
    logging.info("Test open with combined reference file: %s", data_json)

    client.close()


def cli():
    """
    Command-line interface for generating cloud-optimized store reference files.

    Parses command-line arguments and calls the main function with the
    provided parameters. This is the entry point when the script is run
    directly from the command line.
    """
    parser = argparse.ArgumentParser(
        description="Generate Cloud Optimized Store Reference Files")
    parser.add_argument("--collection", type=str, required=True,
                        help="Earthdata collection short name")
    parser.add_argument("--loadable-coord-vars", type=str, default="latitude,longitude,time",
                        help="Comma-separated list of loadable coordinate variables")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date (e.g., 1-1-2022)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date (e.g., 1-1-2025)")
    parser.add_argument("--debug", action="store_true",
                        default=True, help="Enable debug logging")
    args = parser.parse_args()

    main(
        args.collection,
        args.loadable_coord_vars,
        args.start_date,
        args.end_date,
        args.debug
    )


if __name__ == "__main__":
    cli()
