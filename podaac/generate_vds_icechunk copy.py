#!/usr/bin/env python3
"""
Generate Icechunk virtual Zarr stores for Earthdata collections.

Uses VirtualiZarr v2 to build virtual references from granules on Earthdata,
then writes them to local Icechunk repositories with both S3 and HTTP endpoints.
"""

import argparse
import logging
import multiprocessing
import os
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import earthaccess
import icechunk
import numpy as np
import xarray as xr
from dask.distributed import Client, LocalCluster
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser
import virtualizarr as vz


SWOT_ENDPOINT = "https://archive.swot.podaac.earthdata.nasa.gov/s3credentials"

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
    "SWOT_L2_LR_SSH_EXPERT_D": [
        {
            "short_name": "SWOT_L2_LR_SSH_EXPERT_D",
            "granule_name": "SWOT_L2_LR_SSH_EXPERT*PGD*.nc",
            "temporal": ("2023-07-26", "2025-04-08"),
        },
        {
            "short_name": "SWOT_L2_LR_SSH_EXPERT_D",
            "granule_name": "SWOT_L2_LR_SSH_EXPERT*PID*.nc",
            "temporal": ("2025-05-06", "2027-01-01"),
        },
    ],
}


def setup_logging(debug=False):
    log_format = "%(asctime)s %(levelname)s %(message)s"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=log_format)


def is_valid_date(value):
    return value not in (None, "None", "")


def get_temporal_range(start_date, end_date):
    if is_valid_date(start_date) or is_valid_date(end_date):
        return (start_date, end_date)
    return None


def s3_to_http_url(old_s3_path: str) -> str:
    https_base = "https://archive.podaac.earthdata.nasa.gov/"
    return str(https_base + old_s3_path.split("//")[-1])


def search_granules(collection, temporal):
    if collection in SPECIAL_COLLECTION_SEARCHES:
        results = []
        for query in SPECIAL_COLLECTION_SEARCHES[collection]:
            results.extend(earthaccess.search_data(**query))
        return results

    if temporal:
        logging.info(
            "Searching granules with temporal filter - start_date: %s, end_date: %s",
            temporal[0], temporal[1],
        )
        return earthaccess.search_data(short_name=collection, temporal=temporal)

    logging.info("Getting all granules...")
    return earthaccess.search_data(short_name=collection)


def silence_worker_warnings_and_auth(token):
    import warnings
    import logging
    import earthaccess as ea
    import os

    if token:
        os.environ["EARTHDATA_TOKEN"] = token
        ea.login(strategy="environment")

    warnings.filterwarnings("ignore")
    for name in ["distributed", "xarray", "py.warnings", "fsspec", "h5py"]:
        logging.getLogger(name).setLevel(logging.ERROR)


def create_local_icechunk_repo_s3access(repo_name: str, vcc_bucket: str):
    storage = icechunk.local_filesystem_storage(path=repo_name)
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=vcc_bucket + "/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )
    return icechunk.Repository.create(storage, config)


def create_local_icechunk_repo_httpaccess(repo_name: str, vcc_http_base: str):
    storage = icechunk.local_filesystem_storage(path=repo_name)
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            vcc_http_base,
            icechunk.http_store(),
        )
    )
    return icechunk.Repository.create(storage, config)


def build_output_names(collection, start_date, end_date):
    temporal_str = ""
    if is_valid_date(start_date) or is_valid_date(end_date):
        start = start_date if is_valid_date(start_date) else "beginning"
        end = end_date if is_valid_date(end_date) else "present"
        temporal_str = f"{start}_to_{end}_"
    base = f"{collection}_{temporal_str}"
    return f"{base}icechunk_v2.s3", f"{base}icechunk_v2.https"


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
    setup_logging(debug)
    logging.info("Collection: %s", collection)
    logging.info("Vars: %s", loadable_coord_vars)
    logging.info("start_date: %s", start_date)
    logging.info("end_date: %s", end_date)
    logging.info("cpu_count: %s", cpu_count)
    logging.info("memory_limit: %s", memory_limit)
    logging.info("batch_size: %s", batch_size)
    logging.info("CPU count = %d", multiprocessing.cpu_count())

    auth = earthaccess.login()

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

    # Setup obstore registry for S3 access
    credentials_endpoint = "https://archive.podaac.earthdata.nasa.gov/s3credentials"
    parsed_url = urlparse(data_s3links[0])
    bucket = parsed_url.netloc

    s3_store = S3Store(
        bucket=bucket,
        region="us-west-2",
        credential_provider=NasaEarthdataCredentialProvider(
            credentials_endpoint, auth=auth.token["access_token"]
        ),
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )
    s3_obstore_registry = ObjectStoreRegistry({f"s3://{bucket}": s3_store})

    # Create Dask cluster
    cluster = LocalCluster(
        n_workers=cpu_count,
        threads_per_worker=1,
        memory_limit=memory_limit,
        silence_logs=logging.ERROR,
    )
    client = Client(cluster)
    logging.info("Dask client: %s", client)
    client.run(silence_worker_warnings_and_auth, auth.token["access_token"])

    try:
        # Build VDS using virtualizarr v2
        xr_combine_kwargs = {
            "concat_dim": "time",
            "data_vars": "minimal",
            "coords": "minimal",
            "compat": "override",
            "combine_attrs": "override",
        }

        if level_2_data:
            xr_combine_kwargs["concat_dim"] = "granule"

        logging.info("Generating virtual references...")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)

            total_links = len(data_s3links)
            total_batches = (total_links + batch_size - 1) // batch_size
            vds_list = []

            for batch_start in range(0, total_links, batch_size):
                batch_num = (batch_start // batch_size) + 1
                batch = data_s3links[batch_start: batch_start + batch_size]
                logging.info(
                    "Processing batch %d of %d (%d files)",
                    batch_num, total_batches, len(batch),
                )

                # Re-auth for each batch to avoid token expiry
                auth = earthaccess.login()
                s3_store = S3Store(
                    bucket=bucket,
                    region="us-west-2",
                    credential_provider=NasaEarthdataCredentialProvider(
                        credentials_endpoint, auth=auth.token["access_token"]
                    ),
                    virtual_hosted_style_request=False,
                    client_options={"allow_http": True},
                )
                s3_obstore_registry = ObjectStoreRegistry({f"s3://{bucket}": s3_store})

                vds_batch = vz.open_virtual_mfdataset(
                    urls=batch,
                    registry=s3_obstore_registry,
                    parser=HDFParser(),
                    decode_times=False,
                    parallel="dask",
                    combine="nested",
                    **xr_combine_kwargs,
                )
                vds_list.append(vds_batch)

        # Combine batches
        if len(vds_list) == 1:
            vds_s3 = vds_list[0]
        else:
            logging.info("Combining %d batch VDS results...", len(vds_list))
            vds_s3 = xr.combine_nested(
                vds_list,
                concat_dim=xr_combine_kwargs["concat_dim"],
                data_vars="minimal",
                coords="minimal",
                compat="override",
                combine_attrs="override",
            )

        logging.info("Combined VDS: %s", vds_s3)

        if not vds_s3.attrs:
            logging.info("Global Attributes not found for generated dataset.")
            sys.exit(1)

        # Derive temporal range from granule metadata
        granule_starts = [
            g["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
            for g in granule_info
        ]
        granule_ends = [
            g["umm"]["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]
            for g in granule_info
        ]
        vds_s3.attrs['time_coverage_start'] = min(granule_starts)
        vds_s3.attrs['time_coverage_end'] = max(granule_ends)
        vds_s3.attrs['identifier_product_doi'] = "https://doi.org/10.5067/GHOST-4RM02"
        vds_s3.attrs['date_created'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        vds_s3.attrs['history'] = f"Icechunk v2 VDS for {collection}"

        # Create HTTP version
        vds_http = vds_s3.vz.rename_paths(s3_to_http_url)

        # Write to Icechunk stores
        fname_s3, fname_http = build_output_names(collection, start_date, end_date)

        logging.info("Creating S3 Icechunk store: %s", fname_s3)
        repo_s3 = create_local_icechunk_repo_s3access(fname_s3, "s3://" + bucket)
        session_s3 = repo_s3.writable_session("main")
        vds_s3.virtualize.to_icechunk(session_s3.store)
        session_s3.commit("Initial commit.")
        logging.info("S3 store committed.")

        logging.info("Creating HTTP Icechunk store: %s", fname_http)
        repo_http = create_local_icechunk_repo_httpaccess(
            fname_http,
            "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/",
        )
        session_http = repo_http.writable_session("main")
        vds_http.virtualize.to_icechunk(session_http.store)
        session_http.commit("Initial commit.")
        logging.info("HTTP store committed.")

        # Tar the stores
        subprocess.run(["tar", "-cvf", f"{fname_s3}.tar", fname_s3], check=True)
        subprocess.run(["tar", "-cvf", f"{fname_http}.tar", fname_http], check=True)
        logging.info("Tar files created.")

    finally:
        client.close()
        cluster.close()

    logging.info("Done.")


def cli():
    parser = argparse.ArgumentParser(description="Generate Icechunk virtual Zarr stores")
    parser.add_argument("--collection", type=str, required=True, help="Earthdata collection short name")
    parser.add_argument(
        "--loadable-coord-vars",
        type=str,
        default="latitude,longitude,time",
        help="Comma-separated list of loadable coordinate variables",
    )
    parser.add_argument("--start-date", type=str, default=None, help="Start date (e.g., 2022-01-01)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (e.g., 2025-01-01)")
    parser.add_argument("--debug", action="store_true", default=False, help="Enable debug logging")
    parser.add_argument(
        "--level-2-data",
        action="store_true",
        default=False,
        help="Indicate if processing level 2 data",
    )
    parser.add_argument("--cpu-count", type=int, default=16, help="Number of Dask workers")
    parser.add_argument("--memory-limit", type=str, default="12GB", help="Memory limit per Dask worker")
    parser.add_argument("--batch-size", type=int, default=48, help="Batch size for processing")
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
