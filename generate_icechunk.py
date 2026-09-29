#!/usr/bin/env python3
"""
Generate Icechunk v2 virtual Zarr stores for a collection, driven by
collection_config.COLLECTION_CONFIG.

This one script replaces the former per-collection generation scripts: all
per-collection differences (data_vars, coords, preprocess, sort, attrs,
worker/batch tuning) come from the shared config, so generation and the append
pipeline stay in lock-step.

Usage:
    python generate_icechunk.py \
        --collection MUR25-JPL-L4-GLOB-v04.2 \
        --output-bucket podaac-sit-services-cloud-optimizer

    # temporal subset (optional)
    python generate_icechunk.py --collection OSTIA-UKMO-L4-GLOB-REP-v2.0 \
        --output-bucket my-bucket --start 1990-01-01 --end 1999-12-31

Writes two stores:
    virtual_collections/{collection}/{collection}.icechunk_v2.s3/
    virtual_collections/{collection}/{collection}.icechunk_v2.https/
"""

import argparse
import logging
import multiprocessing
import os
import warnings
from urllib.parse import urlparse

import earthaccess as ea
import icechunk
import xarray as xr
from dask.distributed import Client, LocalCluster
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser
import virtualizarr as vz

from collection_config import (
    credentials_endpoint_for_bucket,
    get_collection_config,
    get_preprocess_fn,
    get_store_prefix_https,
    get_store_prefix_s3,
    s3_to_http_url,
)

HTTPS_VCC_BASE = "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/"


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


def create_s3_icechunk_repo(output_bucket, prefix, vcc_url_prefix, vcc_store):
    storage = icechunk.s3_storage(bucket=output_bucket, prefix=prefix, region="us-west-2")
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(url_prefix=vcc_url_prefix, store=vcc_store)
    )
    return icechunk.Repository.create(storage, config)


def open_virtual_mfdataset_batched(urls, registry, batch_size, concat_dim,
                                   data_vars, coords, **kwargs):
    """Build the VDS in batches to avoid overwhelming the Dask scheduler.

    A single-granule collection uses open_virtual_dataset directly.
    """
    if len(urls) == 1:
        return vz.open_virtual_dataset(
            url=urls[0], registry=registry, parser=HDFParser(), decode_times=False,
        )

    vds_batches = []
    for i in range(0, len(urls), batch_size):
        batch = urls[i:i + batch_size]
        print(f"  Processing batch {i // batch_size + 1} ({len(batch)} granules)")
        vds_batches.append(vz.open_virtual_mfdataset(
            urls=batch, registry=registry, concat_dim=concat_dim,
            data_vars=data_vars, coords=coords, **kwargs,
        ))

    if len(vds_batches) == 1:
        return vds_batches[0]

    return xr.combine_nested(
        vds_batches, concat_dim=concat_dim, data_vars=data_vars, coords=coords,
        compat="override", combine_attrs="override",
    )


def main(collection, output_bucket, start=None, end=None):
    config = get_collection_config(collection)
    concat_dim = config["concat_dim"]
    data_vars = config["data_vars"]
    coords = config["coords"]
    sort = config["sort"]
    attrs = config["attrs"]
    preprocess_fn = get_preprocess_fn(config)

    cpu_count = multiprocessing.cpu_count()
    n_workers = min(config["n_workers"], cpu_count)
    memory_limit = config["memory_limit"]
    batch_size = config["batch_size"]
    print(f"CPU count = {cpu_count}; using {n_workers} workers @ {memory_limit}")

    auth = ea.login()

    cluster = LocalCluster(
        n_workers=n_workers, threads_per_worker=1,
        memory_limit=memory_limit, silence_logs=logging.ERROR,
    )
    client = Client(cluster)
    print(client)
    client.run(silence_worker_warnings_and_auth, auth.token["access_token"])

    # --- Search granules ---
    print(f"\nProcessing: {collection}")
    search_kwargs = dict(short_name=collection, provider="POCLOUD", cloud_hosted=True)
    if start and end:
        search_kwargs["temporal"] = (start, end)
    results = ea.search_data(**search_kwargs)
    granule_urls = [g.data_links(access="direct")[0] for g in results]
    print(f"  Number of files found: {len(granule_urls)}")
    if not granule_urls:
        raise SystemExit(f"No granules found for {collection}")

    # --- Registry ---
    bucket = urlparse(granule_urls[0]).netloc
    s3_store = S3Store(
        bucket=bucket, region="us-west-2",
        credential_provider=NasaEarthdataCredentialProvider(
            credentials_endpoint_for_bucket(bucket), auth=auth.token["access_token"],
        ),
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )
    registry = ObjectStoreRegistry({f"s3://{bucket}": s3_store})

    # --- Build VDS ---
    mf_kwargs = dict(parser=HDFParser(), decode_times=False, parallel="dask",
                     combine="nested", compat="override", combine_attrs="override")
    if preprocess_fn:
        mf_kwargs["preprocess"] = preprocess_fn

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)
        vds_s3 = open_virtual_mfdataset_batched(
            granule_urls, registry, batch_size, concat_dim, data_vars, coords, **mf_kwargs,
        )

    # --- Attributes + optional sort ---
    for key, value in attrs.items():
        vds_s3.attrs[key] = value
    if sort:
        vds_s3 = vds_s3.sortby(concat_dim)
    print(vds_s3)

    vds_https = vds_s3.vz.rename_paths(s3_to_http_url)

    # --- Write S3-access store ---
    s3_prefix = get_store_prefix_s3(collection)
    print(f"\nCreating S3 Icechunk store: s3://{output_bucket}/{s3_prefix}")
    repo_s3 = create_s3_icechunk_repo(
        output_bucket, s3_prefix, f"s3://{bucket}/",
        icechunk.s3_store(region="us-west-2", anonymous=True),
    )
    session_s3 = repo_s3.writable_session("main")
    vds_s3.vz.to_icechunk(session_s3.store)
    session_s3.commit("Initial commit.")
    print("  S3 store committed.")

    # --- Write HTTPS-access store ---
    https_prefix = get_store_prefix_https(collection)
    print(f"\nCreating HTTPS Icechunk store: s3://{output_bucket}/{https_prefix}")
    repo_https = create_s3_icechunk_repo(
        output_bucket, https_prefix, HTTPS_VCC_BASE, icechunk.http_store(),
    )
    session_https = repo_https.writable_session("main")
    vds_https.vz.to_icechunk(session_https.store)
    session_https.commit("Initial commit.")
    print("  HTTPS store committed.")

    client.close()
    cluster.close()
    print("\nDone.")


def cli():
    parser = argparse.ArgumentParser(
        description="Generate Icechunk v2 stores for a collection (config-driven)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--collection", required=True, help="Collection short_name")
    parser.add_argument(
        "--output-bucket", default=os.environ.get("OUTPUT_BUCKET", ""),
        help="S3 bucket for icechunk stores (default: $OUTPUT_BUCKET)",
    )
    parser.add_argument("--start", help="Temporal range start (YYYY-MM-DD), optional")
    parser.add_argument("--end", help="Temporal range end (YYYY-MM-DD), optional")
    args = parser.parse_args()
    if not args.output_bucket:
        parser.error("--output-bucket is required (or set OUTPUT_BUCKET env var)")
    main(args.collection, args.output_bucket, start=args.start, end=args.end)


if __name__ == "__main__":
    cli()
