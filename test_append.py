#!/usr/bin/env python3
"""
End-to-end test: create a small icechunk store on S3, then append granules to it.

Step 1: Search for 6 MUR25 granules
Step 2: Create a new icechunk store from the first 5
Step 3: Append the 6th granule using append_granules.py
Step 4: Verify the store has all 6 time steps
"""

import logging
import subprocess
import sys
import warnings
from urllib.parse import urlparse

import earthaccess
import icechunk
import xarray as xr
import virtualizarr as vz
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OUTPUT_BUCKET = "podaac-sit-services-cloud-optimizer"
STORE_PREFIX = "virtual_collections/_test_append/MUR25_test.icechunk_v2.s3/"
COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"


def main():
    auth = earthaccess.login()

    # 1. Search for 6 granules
    logging.info("Searching for 6 %s granules...", COLLECTION)
    results = earthaccess.search_data(
        short_name=COLLECTION,
        provider="POCLOUD",
        cloud_hosted=True,
        count=6,
    )
    urls = [g.data_links(access="direct")[0] for g in results]
    logging.info("Found %d granules", len(urls))
    for u in urls:
        logging.info("  %s", u)

    initial_urls = urls[:5]
    append_url = urls[5]

    # 2. Build VDS from first 5 granules
    parsed = urlparse(urls[0])
    bucket = parsed.netloc
    cred_endpoint = "https://archive.podaac.earthdata.nasa.gov/s3credentials"

    s3_store = S3Store(
        bucket=bucket,
        region="us-west-2",
        credential_provider=NasaEarthdataCredentialProvider(
            cred_endpoint, auth=auth.token["access_token"]
        ),
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )
    registry = ObjectStoreRegistry({f"s3://{bucket}": s3_store})

    logging.info("Building VDS from first %d granules...", len(initial_urls))
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)
        vds = vz.open_virtual_mfdataset(
            urls=initial_urls,
            registry=registry,
            parser=HDFParser(),
            decode_times=False,
            combine="nested",
            concat_dim="time",
            data_vars="minimal",
            coords="minimal",
            compat="override",
            combine_attrs="override",
        )

    logging.info("Initial VDS shape: %s", dict(vds.sizes))

    # 3. Write to S3 icechunk store
    logging.info("Creating icechunk store at s3://%s/%s", OUTPUT_BUCKET, STORE_PREFIX)
    storage = icechunk.s3_storage(
        bucket=OUTPUT_BUCKET,
        prefix=STORE_PREFIX,
        region="us-west-2",
    )
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=f"s3://{bucket}/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )

    try:
        repo = icechunk.Repository.create(storage, config)
    except Exception:
        logging.info("Store already exists, deleting and recreating...")
        repo = icechunk.Repository.open(storage, config=config)
        repo = icechunk.Repository.create(storage, config)

    session = repo.writable_session("main")
    vds.vz.to_icechunk(session.store)
    session.commit("Initial commit: 5 granules")
    logging.info("Initial store committed with %d time steps", vds.sizes["time"])

    # 4. Verify initial store
    verify_session = repo.readonly_session(branch="main")
    ds = xr.open_zarr(verify_session.store, consolidated=False)
    initial_time_count = ds.sizes["time"]
    logging.info("Verified initial store: %d time steps", initial_time_count)
    ds.close()

    # 5. Run append_granules.py to append the 6th granule
    logging.info("Appending 6th granule via append_granules.py...")
    logging.info("  %s", append_url)

    cmd = [
        sys.executable, "append_granules.py",
        "--store-bucket", OUTPUT_BUCKET,
        "--store-prefix", STORE_PREFIX,
        "--granules", append_url,
        "--concat-dim", "time",
        "--cpu-count", "4",
    ]
    logging.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        logging.error("append_granules.py failed with return code %d", result.returncode)
        sys.exit(1)

    # 6. Verify the appended store
    logging.info("Verifying appended store...")
    repo = icechunk.Repository.open(storage, config=config)
    verify_session = repo.readonly_session(branch="main")
    ds = xr.open_zarr(verify_session.store, consolidated=False)
    final_time_count = ds.sizes["time"]
    logging.info("Final store: %d time steps (was %d)", final_time_count, initial_time_count)
    ds.close()

    if final_time_count == initial_time_count + 1:
        logging.info("SUCCESS — append added 1 time step as expected")
    else:
        logging.error(
            "UNEXPECTED — expected %d time steps, got %d",
            initial_time_count + 1,
            final_time_count,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
