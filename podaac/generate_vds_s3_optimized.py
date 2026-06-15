#!/usr/bin/env python3
# pylint: disable=too-many-statements, too-many-branches
# pylint: disable=R0801
"""
Generate Cloud Optimized Store Reference Files for Earthdata collections.

This optimized variant keeps the original script behavior but reduces duplication,
clarifies flow, and fixes a few correctness edge cases.
"""

import argparse
import logging
import multiprocessing
import os
import sys
from pathlib import Path

import earthaccess
import fsspec
import numpy as np
import pandas as pd
import psutil
import xarray as xr
from dask.distributed import Client
from virtualizarr import open_virtual_dataset

SWOT_ENDPOINT = "https://archive.swot.podaac.earthdata.nasa.gov/s3credentials"
SPECIAL_COLLECTION_COORDS = {"SWOT_L2_LR_SSH_Basic_2.0", "SWOT_L2_LR_SSH_Basic_D"}

SPECIAL_COLLECTION_SEARCHES = {
    "SWOT_L2_LR_SSH_Basic_2.0": [
        {
            "short_name": "SWOT_L2_LR_SSH_Basic_2.0",
            "granule_name": "SWOT_L2_LR_SSH_Basic*PGC*.nc",
            "temporal": ("2023-07-26", "2024-01-24"),
        },
        {
            "short_name": "SWOT_L2_LR_SSH_Basic_2.0",
            "granule_name": "SWOT_L2_LR_SSH_Basic*PIC*.nc",
            "temporal": ("2024-01-25", "2025-05-03"),
        },
    ],
    "SWOT_L2_LR_SSH_Basic_D": [
        {
            "short_name": "SWOT_L2_LR_SSH_Basic_D",
            "granule_name": "SWOT_L2_LR_SSH_Basic*PGD*.nc",
            "temporal": ("2023-07-26", "2025-04-08"),
        },
        {
            "short_name": "SWOT_L2_LR_SSH_Basic_D",
            "granule_name": "SWOT_L2_LR_SSH_Basic*PID*.nc",
            "temporal": ("2025-05-06", "2027-01-01"),
        },
    ],
}


def get_daac_from_s3_link(s3_link):
    """Identify the DAAC/provider from an S3 URL."""
    return "SWOT" if "swot" in s3_link else "PODAAC"


def print_memory_usage(note=""):
    """Log current process memory usage in MB."""
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    logging.info("[MEMORY] %s RSS: %.2f MB", note, mem_mb)


def setup_logging(debug=False):
    """Configure logging level/format."""
    log_format = "%(asctime)s %(levelname)s %(message)s"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=log_format)
    if debug:
        logging.getLogger("urllib3").setLevel(logging.DEBUG)


def opends_withref(ref, fs_data):
    """Open a kerchunk reference JSON as an xarray Dataset via zarr engine."""
    storage_opts = {
        "fo": ref,
        "remote_protocol": "https",
        "remote_options": fs_data.storage_options,
    }
    fs_ref = fsspec.filesystem("reference", **storage_opts)
    mapper = fs_ref.get_mapper("")
    return xr.open_dataset(
        mapper,
        engine="zarr",
        chunks={},
        decode_times=False,
        backend_kwargs={"consolidated": False},
    )


def is_valid_date(value):
    """Return True when a date arg is present and not an empty sentinel."""
    return value not in (None, "None", "")


def parse_coord_vars(loadable_coord_vars, level_2_data, collection):
    """Resolve loadable coordinate variable list for virtualizarr reads."""
    if level_2_data:
        if collection in SPECIAL_COLLECTION_COORDS:
            return ["num_lines", "num_pixels"]
        return []
    return [value.strip() for value in loadable_coord_vars.split(",") if value.strip()]


def get_temporal_range(start_date, end_date):
    """Build optional earthaccess temporal tuple."""
    if is_valid_date(start_date) or is_valid_date(end_date):
        return (start_date, end_date)
    return None


def get_s3_filesystem_for_daac(daac_name):
    """Authenticate and return a fresh Earthaccess S3 filesystem."""
    earthaccess.login()
    if daac_name == "PODAAC":
        return earthaccess.get_s3_filesystem(daac="PODAAC")
    return earthaccess.get_s3_filesystem(endpoint=SWOT_ENDPOINT)


def search_granules(collection, temporal):
    """Search granules, including special split-time searches for SWOT collections."""
    if collection in SPECIAL_COLLECTION_SEARCHES:
        results = []
        for query in SPECIAL_COLLECTION_SEARCHES[collection]:
            results.extend(earthaccess.search_data(**query))
        return results

    if temporal:
        logging.info(
            "Searching granules with temporal filter - start_date: %s, end_date: %s",
            temporal[0],
            temporal[1],
        )
        return earthaccess.search_data(short_name=collection, temporal=temporal)

    logging.info("Getting all granules...")
    return earthaccess.search_data(short_name=collection)


def process_in_batches(data_s3links, coord_vars, client, batch_size=48):
    """Create virtual datasets in batches to reduce token/session expiration risk."""
    virtual_ds_list = []
    total_links = len(data_s3links)
    total_batches = (total_links + batch_size - 1) // batch_size

    for batch_start in range(0, total_links, batch_size):
        batch_num = (batch_start // batch_size) + 1
        batch = data_s3links[batch_start: batch_start + batch_size]
        logging.info(
            "Processing batch %d of %d (%d files)",
            batch_num,
            total_batches,
            len(batch),
        )

        daac_name = get_daac_from_s3_link(batch[0])
        fs = get_s3_filesystem_for_daac(daac_name)
        reader_opts = {"storage_options": fs.storage_options}

        futures = client.map(
            open_virtual_dataset,
            batch,
            indexes={},
            reader_options=reader_opts,
            loadable_variables=coord_vars,
            decode_times=False,
        )
        virtual_ds_list.extend(client.gather(futures))

    return virtual_ds_list


def combine_level_2(collection, granule_info, virtual_ds_list):
    """Combine level-2 virtual datasets using collection-specific coordinate logic."""
    if collection == "SWOT_L2_LR_SSH_Basic_2.0":
        orbit_start = [
            np.datetime64(g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"], "ns")
            for g in granule_info[: len(virtual_ds_list)]
        ]

        pairs = sorted(zip(orbit_start, virtual_ds_list), key=lambda item: item[0])
        orbit_start_sorted, virtual_ds_list_sorted = zip(*pairs)
        orbit_idx = pd.Index(orbit_start_sorted, name="orbit")

        return xr.concat(
            virtual_ds_list_sorted,
            orbit_idx,
            coords=["latitude", "longitude"],
            compat="override",
            combine_attrs="drop_conflicts",
        )

    if collection == "SWOT_L2_LR_SSH_Basic_D":
        granules = granule_info[: len(virtual_ds_list)]
        orbit_start = [
            np.datetime64(g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"], "ns")
            for g in granules
        ]
        cycle_num = [
            g["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Track"]["Cycle"]
            for g in granules
        ]
        pass_num = [
            g["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Track"]["Passes"][0]["Pass"]
            for g in granules
        ]
        file_list = [Path(g.data_links(access="https")[0]).name for g in granules]

        combined = xr.concat(
            virtual_ds_list,
            dim="granule",
            coords=["latitude", "longitude"],
            compat="override",
            combine_attrs="drop_conflicts",
        )
        granule_index = np.arange(len(virtual_ds_list))
        return combined.assign_coords(
            granule=("granule", granule_index),
            orbit=("granule", orbit_start),
            cycle=("granule", cycle_num),
            ppass=("granule", pass_num),
            filename=("granule", file_list),
        )

    basetime_str = "1970-01-01T00:00:00"
    raw_dates = [
        g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        for g in granule_info[: len(virtual_ds_list)]
    ]
    datetime_array = np.array(raw_dates, dtype="datetime64[s]")
    basetime_obj = np.datetime64(basetime_str, "s")
    orbit_starttime_array = (datetime_array - basetime_obj).astype(int)

    orbit_starttime_da = xr.DataArray(
        data=orbit_starttime_array,
        name="orbit_segment_start_time",
        dims=["orbit_segment_start_time"],
        attrs={
            "units": f"seconds since {basetime_str}",
            "calendar": "gregorian",
        },
    )

    return xr.concat(
        virtual_ds_list,
        orbit_starttime_da,
        coords=["lat", "lon"],
        compat="override",
        combine_attrs="drop_conflicts",
    )


def build_output_name(collection, start_date, end_date):
    """Build output filename, preserving original naming convention."""
    temporal_str = ""
    if is_valid_date(start_date) or is_valid_date(end_date):
        start = start_date if is_valid_date(start_date) else "beginning"
        end = end_date if is_valid_date(end_date) else "present"
        temporal_str = f"{start}_to_{end}_"
    return f"{collection}_{temporal_str}virtual_s3.json"


def main(
    collection,
    loadable_coord_vars,
    start_date,
    end_date,
    debug=False,
    level_2_data=False,
    cpu_count=16,
    memory_limit="12GB",
    batch_size=48,
):
    """Generate a combined virtual reference file for an Earthdata collection."""
    setup_logging(debug)
    logging.info("Collection: %s", collection)
    logging.info("Vars: %s", loadable_coord_vars)
    logging.info("start_date: %s", start_date)
    logging.info("end_date: %s", end_date)
    logging.info("cpu_count: %s", cpu_count)
    logging.info("memory_limit: %s", memory_limit)
    logging.info("batch_size: %s", batch_size)

    xr.set_options(
        display_expand_attrs=False,
        display_expand_coords=True,
        display_expand_data=True,
    )

    earthaccess.login()

    temporal = get_temporal_range(start_date, end_date)
    granule_info = search_granules(collection, temporal)
    if not granule_info:
        logging.warning("No granules found matching criteria. Exiting.")
        sys.exit(0)

    logging.info("Found %d granules.", len(granule_info))
    data_s3links = [g.data_links(access="direct")[0] for g in granule_info]
    logging.info("Found %d data files.", len(data_s3links))
    if not data_s3links:
        logging.warning("No direct-access S3 links found. Exiting.")
        sys.exit(0)

    coord_vars = parse_coord_vars(loadable_coord_vars, level_2_data, collection)

    logging.info("CPU count = %d", multiprocessing.cpu_count())
    client = Client(n_workers=cpu_count, threads_per_worker=1, memory_limit=memory_limit)

    try:
        logging.info("Generating references for all files...")
        virtual_ds_list = process_in_batches(
            data_s3links,
            coord_vars,
            client,
            batch_size=batch_size,
        )
        print_memory_usage("After per-granule virtualization")

        logging.info("Combining references...")
        if level_2_data:
            virtual_ds_combined = combine_level_2(collection, granule_info, virtual_ds_list)
        else:
            virtual_ds_combined = xr.combine_nested(
                virtual_ds_list,
                concat_dim="time",
                coords="minimal",
                compat="override",
                combine_attrs="drop_conflicts",
            )

        if not virtual_ds_combined.attrs:
            logging.info("Global Attributes not found for generated dataset.")
            sys.exit(1)

        output_file = build_output_name(collection, start_date, end_date)
        virtual_ds_combined.virtualize.to_kerchunk(output_file, format="json")
        logging.info("Saved: %s", output_file)

        daac_name = get_daac_from_s3_link(data_s3links[0])
        fs = get_s3_filesystem_for_daac(daac_name)
        data_json = opends_withref(output_file, fs)
        logging.info("Test open with combined reference file: %s", data_json)

    finally:
        logging.info("Shutting down Dask client and cluster...")
        try:
            client.close()
        except Exception:
            logging.info("Error closing Dask client")

        try:
            if client.cluster:
                client.cluster.close(timeout=15)
        except TimeoutError:
            logging.info(
                "Dask cluster shutdown timed out. Safely ignoring and continuing exit."
            )


def cli():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Generate Cloud Optimized Store Reference Files")
    parser.add_argument("--collection", type=str, required=True, help="Earthdata collection short name")
    parser.add_argument(
        "--loadable-coord-vars",
        type=str,
        default="latitude,longitude,time",
        help="Comma-separated list of loadable coordinate variables",
    )
    parser.add_argument("--start-date", type=str, default=None, help="Start date (e.g., 1-1-2022)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (e.g., 1-1-2025)")
    parser.add_argument("--debug", action="store_true", default=False, help="Enable debug logging")
    parser.add_argument(
        "--level-2-data",
        action="store_true",
        default=False,
        help="Indicate if processing level 2 data",
    )
    parser.add_argument("--cpu-count", type=int, default=16, help="Number of Dask workers (default: 16)")
    parser.add_argument(
        "--memory-limit",
        type=str,
        default="12GB",
        help="Memory limit per Dask worker (default: 12GB)",
    )
    parser.add_argument("--batch-size", type=int, default=48, help="Batch size for processing S3 links")
    args = parser.parse_args()

    main(
        args.collection,
        args.loadable_coord_vars,
        args.start_date,
        args.end_date,
        args.debug,
        args.level_2_data,
        args.cpu_count,
        args.memory_limit,
        args.batch_size,
    )


if __name__ == "__main__":
    cli()
