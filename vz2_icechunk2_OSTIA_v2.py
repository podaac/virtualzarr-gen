"""
Create Icechunk v2 virtual Zarr store for the OSTIA-UKMO-L4-GLOB-REP-v2.0 dataset.

Uses VirtualiZarr to build virtual references from granules on Earthdata,
then writes them to a local Icechunk repository with both S3 and HTTP endpoints.
"""

import warnings
import logging
import multiprocessing
from urllib.parse import urlparse

import earthaccess as ea
import xarray as xr
import numpy as np
import dask
from dask.distributed import Client, LocalCluster
import icechunk
import obstore
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import HTTPStore, S3Store
from virtualizarr.parsers import HDFParser
from obspec_utils.registry import ObjectStoreRegistry
import virtualizarr as vz


# =====================================================================================
# Configuration
# =====================================================================================

SHORTNAMES = ["OSTIA-UKMO-L4-GLOB-REP-v2.0"]

FNAME_STORE_S3 = "OSTIA-UKMO-L4-GLOB-REP-v2.0.icechunk_v2.s3"
FNAME_STORE_HTTP = "OSTIA-UKMO-L4-GLOB-REP-v2.0.icechunk_v2.https"

ENVIRONMENT = "local"
N_WORKERS = 32
MEMORY_LIMIT = "4GiB"
BATCH_SIZE = 500


# =====================================================================================
# Helper functions
# =====================================================================================

def create_dask_cluster(environment="local", n_workers=8, memory_limit="4GiB", cloud_opts=None):
    if environment == "local":
        print("Creating new local Dask client")
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=1,
            memory_limit=memory_limit,
            silence_logs=logging.ERROR,
        )
    else:
        from coiled import Cluster as CoiledCluster
        print("Creating new Coiled Dask client")
        cluster = CoiledCluster(n_workers=n_workers, **(cloud_opts or {}))
    client = Client(cluster)
    return client, cluster


def silence_worker_warnings_and_auth(token):
    import warnings
    import logging
    import earthaccess as ea
    import os

    if token:
        os.environ["EARTHDATA_TOKEN"] = token
        ea.login(strategy="environment")

    warnings.filterwarnings("ignore")
    for name in ["distributed", "xarray", "py.warnings", "fsspec", "h5netcdf", "h5py"]:
        logging.getLogger(name).setLevel(logging.ERROR)


def s3_to_http_url(old_s3_path: str) -> str:
    https_base = "https://archive.podaac.earthdata.nasa.gov/"
    return str(https_base + old_s3_path.split("//")[-1])


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


def open_virtual_mfdataset_batched(urls, registry, batch_size, **kwargs):
    """Process granules in batches to avoid overwhelming the Dask scheduler."""
    vds_batches = []
    for i in range(0, len(urls), batch_size):
        batch = urls[i : i + batch_size]
        print(f"  Processing batch {i // batch_size + 1} ({len(batch)} granules)")
        vds = vz.open_virtual_mfdataset(
            urls=batch,
            registry=registry,
            **kwargs,
        )
        vds_batches.append(vds)

    if len(vds_batches) == 1:
        return vds_batches[0]

    return xr.combine_nested(
        vds_batches,
        concat_dim="time",
        data_vars="minimal",
        coords="minimal",
        compat="override",
        combine_attrs="override",
    )


# =====================================================================================
# Main
# =====================================================================================

def main():
    cpu_count = multiprocessing.cpu_count()
    print(f"CPU count = {cpu_count}")

    n_workers = min(N_WORKERS, cpu_count)
    print(f"Using {n_workers} workers with {MEMORY_LIMIT} memory limit each")

    # --- Authenticate ---
    auth = ea.login()

    cloud_opts = {
        "region": "us-west-2",
        "worker_vm_types": ["t3a.medium"],
        "spot_policy": "spot_with_fallback",
        "name": "test-vd",
        "environ": {"EARTHDATA_TOKEN": auth.token["access_token"]},
    }

    # --- Create Dask cluster ---
    client, cluster = create_dask_cluster(
        environment=ENVIRONMENT, n_workers=n_workers,
        memory_limit=MEMORY_LIMIT, cloud_opts=cloud_opts,
    )
    print(client)
    client.run(silence_worker_warnings_and_auth, auth.token["access_token"])

    # --- Build VDS for each collection ---
    vds_s3_list = []

    for sn in SHORTNAMES:
        print(f"\nProcessing: {sn}")
        auth = ea.login()

        # 1. Get granule metadata and S3 links
        results = ea.search_data(
            short_name=sn,
            provider="POCLOUD",
            cloud_hosted=True
            #temporal=("1990-01-01", "1999-12-31"),
        )
        granule_data_urls_s3 = [
            granule.data_links(access="direct")[0] for granule in results
        ]
        print(f"  Number of files found: {len(results)}")

        # 2. Setup store registry
        credentials_endpoint = "https://archive.podaac.earthdata.nasa.gov/s3credentials"
        parsed_url = urlparse(results[0].data_links(access="direct")[0])
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

        # 3. Create VDS reference in batches
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)
            vds_s3 = open_virtual_mfdataset_batched(
                urls=granule_data_urls_s3,
                registry=s3_obstore_registry,
                batch_size=BATCH_SIZE,
                parser=HDFParser(),
                decode_times=False,
                parallel="dask",
                combine="nested",
                concat_dim="time",
                preprocess=lambda ds: ds,
                data_vars="minimal",
                coords="minimal",
                compat="override",
                combine_attrs="override",
            )

        vds_s3_list.append(vds_s3)

    # --- Combine into single VDS ---
    vds_composite_s3 = xr.combine_nested(
        vds_s3_list, concat_dim=None,
        compat="override", combine_attrs="drop_conflicts",
    )

    # --- Add/modify attributes ---
    vds_composite_s3.attrs['time_coverage_start'] = '1990-01-01T00:00:00Z'
    vds_composite_s3.attrs['time_coverage_end'] = '1999-12-31T00:00:00Z'
    vds_composite_s3.attrs['identifier_product_doi'] = "https://doi.org/10.5067/GHOST-4RM02"
    vds_composite_s3.attrs['date_created'] = "2026-08-05T00:00:00Z"
    vds_composite_s3.attrs['history'] = "Icechunk v2  VDS for OSTIA"

    print(vds_composite_s3)

    # --- Create HTTP version ---
    vds_composite_http = vds_composite_s3.vz.rename_paths(s3_to_http_url)

    # --- Write to Icechunk stores ---
    # S3 endpoints
    print(f"\nCreating S3 Icechunk store: {FNAME_STORE_S3}")
    repo_s3 = create_local_icechunk_repo_s3access(FNAME_STORE_S3, "s3://" + bucket)
    session_s3 = repo_s3.writable_session("main")
    vds_composite_s3.virtualize.to_icechunk(session_s3.store)
    session_s3.commit("Initial commit.")
    print("  S3 store committed.")

    # HTTP endpoints
    print(f"\nCreating HTTP Icechunk store: {FNAME_STORE_HTTP}")
    repo_http = create_local_icechunk_repo_httpaccess(
        FNAME_STORE_HTTP,
        "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/",
    )
    session_http = repo_http.writable_session("main")
    vds_composite_http.virtualize.to_icechunk(session_http.store)
    session_http.commit("Initial commit.")
    print("  HTTP store committed.")

    # --- Tar the stores ---
    import subprocess
    subprocess.run(["tar", "-cvf", f"{FNAME_STORE_S3}.tar", FNAME_STORE_S3], check=True)
    subprocess.run(["tar", "-cvf", f"{FNAME_STORE_HTTP}.tar", FNAME_STORE_HTTP], check=True)

    # Cleanup
    client.close()
    cluster.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
