#!/usr/bin/env python3
"""
Append new granules to an existing Icechunk v2 virtual Zarr store on S3.

Usage:
    # Append granules by S3 URL (one per line in a file)
    python append_granules.py \
        --store-bucket podaac-sit-services-cloud-optimizer \
        --store-prefix virtual_collections/MUR25-JPL-L4-GLOB-v04.2/MUR25-JPL-L4-GLOB-v04.2_icechunk_v2.s3/ \
        --granule-file new_granules.txt \
        --concat-dim time

    # Append granules passed directly as arguments
    python append_granules.py \
        --store-bucket podaac-sit-services-cloud-optimizer \
        --store-prefix virtual_collections/MUR25-JPL-L4-GLOB-v04.2/MUR25-JPL-L4-GLOB-v04.2_icechunk_v2.s3/ \
        --granules s3://bucket/path/to/granule1.nc s3://bucket/path/to/granule2.nc \
        --concat-dim time
"""

import argparse
import logging
import sys
import warnings
from datetime import datetime, timezone
from urllib.parse import urlparse

import earthaccess
import icechunk
import xarray as xr
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser
import virtualizarr as vz


def setup_logging(debug=False):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_granule_urls(granule_file=None, granules=None):
    urls = []
    if granule_file:
        with open(granule_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
    if granules:
        urls.extend(granules)
    return urls


def open_repo_s3(bucket, prefix, vcc_bucket):
    storage = icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region="us-west-2",
    )
    config = icechunk.Repository.fetch_config(storage)
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=vcc_bucket + "/",
            store=icechunk.s3_store(region="us-west-2", anonymous=True),
        )
    )
    return icechunk.Repository.open(storage, config=config)


def build_vds(data_urls, auth, preprocess=None, concat_dim="time",
              data_vars="minimal", coords="minimal"):
    parsed_url = urlparse(data_urls[0])
    bucket = parsed_url.netloc
    credentials_endpoint = "https://archive.podaac.earthdata.nasa.gov/s3credentials"

    s3_store = S3Store(
        bucket=bucket,
        region="us-west-2",
        credential_provider=NasaEarthdataCredentialProvider(
            credentials_endpoint, auth=auth.token["access_token"]
        ),
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )
    registry = ObjectStoreRegistry({f"s3://{bucket}": s3_store})

    mfdataset_kwargs = dict(
        urls=data_urls,
        registry=registry,
        parser=HDFParser(),
        decode_times=False,
        parallel="dask",
        combine="nested",
        concat_dim=concat_dim,
        data_vars=data_vars,
        coords=coords,
        compat="override",
        combine_attrs="override",
    )
    if preprocess:
        mfdataset_kwargs["preprocess"] = preprocess

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Numcodecs codecs*", category=UserWarning)
        vds = vz.open_virtual_mfdataset(**mfdataset_kwargs)

    return vds, bucket


def main():
    parser = argparse.ArgumentParser(
        description="Append granules to an existing Icechunk v2 store on S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument("--store-bucket", type=str, required=True, help="S3 bucket containing the icechunk store")
    parser.add_argument("--store-prefix", type=str, required=True, help="S3 prefix for the icechunk store")

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--granule-file", type=str, help="File containing S3 URLs of granules to append (one per line)")
    input_group.add_argument("--granules", nargs="+", help="S3 URLs of granules to append")

    parser.add_argument("--concat-dim", default="time", help="Dimension to append along (default: time)")
    parser.add_argument("--data-vars", default="minimal", help="Data vars strategy (default: minimal)")
    parser.add_argument("--coords", default="minimal", choices=["minimal", "all"], help="Coords strategy")
    parser.add_argument("--cpu-count", type=int, default=8, help="Number of Dask workers")
    parser.add_argument("--memory-limit", default="4GB", help="Memory limit per Dask worker")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--dry-run", action="store_true", help="Build VDS but don't commit to the store")

    args = parser.parse_args()
    setup_logging(args.debug)

    granule_urls = load_granule_urls(args.granule_file, args.granules)
    if not granule_urls:
        logging.error("No granule URLs provided.")
        sys.exit(1)

    logging.info("Granules to append: %d", len(granule_urls))
    for url in granule_urls:
        logging.info("  %s", url)

    auth = earthaccess.login()

    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(
        n_workers=args.cpu_count,
        threads_per_worker=1,
        memory_limit=args.memory_limit,
        silence_logs=logging.ERROR,
    )
    client = Client(cluster)
    logging.info("Dask client: %s", client)

    def _silence(token):
        import warnings, logging, earthaccess as ea, os
        if token:
            os.environ["EARTHDATA_TOKEN"] = token
            ea.login(strategy="environment")
        warnings.filterwarnings("ignore")
        for name in ["distributed", "xarray", "py.warnings", "fsspec", "h5py"]:
            logging.getLogger(name).setLevel(logging.ERROR)

    client.run(_silence, auth.token["access_token"])

    try:
        logging.info("Building virtual dataset from new granules...")
        vds, source_bucket = build_vds(
            granule_urls,
            auth,
            concat_dim=args.concat_dim,
            data_vars=args.data_vars,
            coords=args.coords,
        )
        logging.info("New VDS shape: %s", dict(vds.sizes))
        logging.info("New VDS:\n%s", vds)

        if args.dry_run:
            logging.info("Dry run — skipping store write.")
            return

        vcc_bucket = f"s3://{source_bucket}"
        repo = open_repo_s3(args.store_bucket, args.store_prefix, vcc_bucket)
        logging.info("Opened S3 store: s3://%s/%s", args.store_bucket, args.store_prefix)

        session = repo.writable_session("main")

        existing_ds = xr.open_zarr(session.store, consolidated=False)
        logging.info("Existing store shape: %s", dict(existing_ds.sizes))
        existing_ds.close()

        logging.info("Appending along '%s'...", args.concat_dim)
        vds.vz.to_icechunk(session.store, append_dim=args.concat_dim)

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        commit_msg = f"Append {len(granule_urls)} granule(s) at {timestamp}"
        session.commit(commit_msg)
        logging.info("Committed: %s", commit_msg)

        verify_session = repo.readonly_session(branch="main")
        verify_ds = xr.open_zarr(verify_session.store, consolidated=False)
        logging.info("Verified store shape: %s", dict(verify_ds.sizes))
        verify_ds.close()

    finally:
        warnings.filterwarnings("ignore")
        logging.disable(logging.CRITICAL)
        try:
            client.shutdown(timeout=30)
        except Exception:
            pass
        try:
            cluster.close(timeout=30)
        except Exception:
            pass
        logging.disable(logging.NOTSET)

    logging.info("Done.")


if __name__ == "__main__":
    main()
