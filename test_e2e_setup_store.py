#!/usr/bin/env python3
"""
Step 1: Create a test Icechunk store with 5 MUR25 granules.

Searches for 10 granules, uses the first 5 to create the store,
and prints the remaining 5 as SQS-ready JSON messages for appending.

Usage:
    python test_e2e_setup_store.py
    python test_e2e_setup_store.py --store-bucket my-bucket --count 10
"""

import argparse
import json
import logging
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

COLLECTION = "MUR25-JPL-L4-GLOB-v04.2"
DEFAULT_BUCKET = "podaac-sit-services-cloud-optimizer"
STORE_PREFIX_S3 = "virtual_collections/_test_e2e/MUR25_test.icechunk_v2.s3/"
STORE_PREFIX_HTTPS = "virtual_collections/_test_e2e/MUR25_test.icechunk_v2.https/"

BUCKET_TO_HOST = {
    "podaac-swot-ops-cumulus-protected": "archive.swot.podaac.earthdata.nasa.gov",
    "podaac-swot-ops-cumulus-public": "archive.swot.podaac.earthdata.nasa.gov",
}
DEFAULT_HTTPS_HOST = "archive.podaac.earthdata.nasa.gov"


def s3_to_https_url(s3_url):
    if not s3_url.startswith("s3://"):
        return s3_url
    raw_path = s3_url.replace("s3://", "")
    bucket_name = raw_path.split("/", 1)[0]
    host = BUCKET_TO_HOST.get(bucket_name, DEFAULT_HTTPS_HOST)
    return f"https://{host}/{raw_path}"


def main():
    parser = argparse.ArgumentParser(description="Create a test Icechunk store")
    parser.add_argument("--store-bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--count", type=int, default=10,
                        help="Total granules to search for (first 5 for store, rest for appending)")
    args = parser.parse_args()

    auth = earthaccess.login(strategy="netrc")

    logging.info("Searching for %d %s granules...", args.count, COLLECTION)
    results = earthaccess.search_data(
        short_name=COLLECTION,
        provider="POCLOUD",
        cloud_hosted=True,
        count=args.count,
    )
    urls = [g.data_links(access="direct")[0] for g in results]
    logging.info("Found %d granules", len(urls))

    initial_urls = urls[:5]
    append_urls = urls[5:]

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
    for u in initial_urls:
        logging.info("  %s", u)

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

    # --- S3 store ---
    logging.info("Creating S3 icechunk store at s3://%s/%s", args.store_bucket, STORE_PREFIX_S3)
    storage_s3 = icechunk.s3_storage(
        bucket=args.store_bucket,
        prefix=STORE_PREFIX_S3,
        region="us-west-2",
    )
    config_s3 = icechunk.RepositoryConfig.default()
    config_s3.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=f"s3://{bucket}/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )

    try:
        repo_s3 = icechunk.Repository.create(storage_s3, config_s3)
    except Exception:
        logging.info("S3 store already exists, recreating...")
        repo_s3 = icechunk.Repository.open(storage_s3, config=config_s3)
        repo_s3 = icechunk.Repository.create(storage_s3, config_s3)

    session_s3 = repo_s3.writable_session("main")
    vds.vz.to_icechunk(session_s3.store)
    session_s3.commit("Initial commit: 5 granules")
    logging.info("S3 store committed with %d time steps", vds.sizes["time"])

    verify_s3 = repo_s3.readonly_session(branch="main")
    ds_s3 = xr.open_zarr(verify_s3.store, consolidated=False)
    logging.info("S3 store verified: %s", dict(ds_s3.sizes))
    ds_s3.close()

    # --- HTTPS store ---
    https_host = BUCKET_TO_HOST.get(bucket, DEFAULT_HTTPS_HOST)
    vcc_https_prefix = f"https://{https_host}/{bucket}/"

    logging.info("Creating HTTPS icechunk store at s3://%s/%s", args.store_bucket, STORE_PREFIX_HTTPS)
    storage_https = icechunk.s3_storage(
        bucket=args.store_bucket,
        prefix=STORE_PREFIX_HTTPS,
        region="us-west-2",
    )
    config_https = icechunk.RepositoryConfig.default()
    config_https.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            vcc_https_prefix,
            icechunk.http_store(),
        )
    )

    try:
        repo_https = icechunk.Repository.create(storage_https, config_https)
    except Exception:
        logging.info("HTTPS store already exists, recreating...")
        repo_https = icechunk.Repository.open(storage_https, config=config_https)
        repo_https = icechunk.Repository.create(storage_https, config_https)

    vds_https = vds.vz.rename_paths(s3_to_https_url)
    session_https = repo_https.writable_session("main")
    vds_https.vz.to_icechunk(session_https.store)
    session_https.commit("Initial commit: 5 granules")
    logging.info("HTTPS store committed with %d time steps", vds.sizes["time"])

    verify_https = repo_https.readonly_session(branch="main")
    ds_https = xr.open_zarr(verify_https.store, consolidated=False)
    logging.info("HTTPS store verified: %s", dict(ds_https.sizes))
    ds_https.close()

    print("\n" + "=" * 70)
    print("STORES CREATED SUCCESSFULLY")
    print(f"  Bucket:      {args.store_bucket}")
    print(f"  S3 prefix:   {STORE_PREFIX_S3}")
    print(f"  HTTPS prefix: {STORE_PREFIX_HTTPS}")
    print(f"  Time steps:  {vds.sizes['time']}")
    print("=" * 70)

    if append_urls:
        print(f"\nGranules available for appending ({len(append_urls)}):")
        for u in append_urls:
            print(f"  {u}")

        print("\n--- SQS messages (one granule per message) ---\n")
        for i, url in enumerate(append_urls):
            msg = json.dumps({"collection": COLLECTION, "granules": [url]})
            print(f"# Message {i + 1}")
            print(f"aws sqs send-message \\")
            print(f"  --queue-url \"$QUEUE_URL\" \\")
            print(f"  --message-body '{msg}' \\")
            print(f"  --message-group-id \"{COLLECTION}\"\n")

        print("--- SQS message (all granules in one batch) ---\n")
        batch_msg = json.dumps({"collection": COLLECTION, "granules": append_urls})
        print(f"aws sqs send-message \\")
        print(f"  --queue-url \"$QUEUE_URL\" \\")
        print(f"  --message-body '{batch_msg}' \\")
        print(f"  --message-group-id \"{COLLECTION}\"")
    else:
        print("\nNo additional granules for appending. Increase --count.")


if __name__ == "__main__":
    main()
