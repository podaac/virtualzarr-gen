#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import time
import multiprocessing
import ujson
from pathlib import Path

import fsspec
import earthaccess
import xarray as xr
from virtualizarr import open_virtual_dataset
from dask import delayed, compute
from dask.distributed import Client
from kerchunk.combine import MultiZarrToZarr

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

def create_single_reference(datalink, reader_options=None, loadable_variables=None):
    """Create a single Kerchunk reference and return as dict."""
    setup_logging(debug=False)
    logging.info(f"Processing: {datalink}")
    
    for cnt in range(1, 5):
        try:
            if cnt != 1:
                logging.debug(f"Retrying ({cnt}) {datalink}")
            
            # Open virtual dataset
            vds = open_virtual_dataset(
                datalink, 
                indexes={}, 
                reader_options=reader_options,
                loadable_variables=loadable_variables, 
                decode_times=False
            )
            
            # Convert to Kerchunk reference dict (small, just metadata)
            ref_dict = vds.virtualize.to_kerchunk(format='dict')
            
            # Clean up immediately
            vds.close()
            del vds
            
            return ref_dict
            
        except Exception as e:
            logging.debug(f"Error: {e}")
            time.sleep(cnt ** 2)
    
    raise Exception(f"Could not process file {datalink}")

def process_in_batches(data_links, reader_opts, coord_vars, batch_size=20, output_dir="kerchunk_refs"):
    """Process files in batches and save individual references."""
    Path(output_dir).mkdir(exist_ok=True)
    ref_files = []
    
    total_batches = (len(data_links) - 1) // batch_size + 1
    
    for i in range(0, len(data_links), batch_size):
        batch = data_links[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logging.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} files)")
        
        # Process batch in parallel
        tasks = [
            delayed(create_single_reference)(p, reader_options=reader_opts, loadable_variables=coord_vars)
            for p in batch
        ]
        batch_refs = compute(*tasks)
        
        # Save each reference to disk immediately
        for j, ref_dict in enumerate(batch_refs):
            ref_file = os.path.join(output_dir, f"ref_{i+j:05d}.json")
            with open(ref_file, 'w') as f:
                ujson.dump(ref_dict, f)
            ref_files.append(ref_file)
        
        # Clean up batch from memory
        del batch_refs, tasks
        
        # Periodic garbage collection
        if batch_num % 5 == 0:
            import gc
            gc.collect()
    
    return ref_files

def combine_references(ref_files, output_file, concat_dim='time'):
    """Combine individual Kerchunk references into a single file."""
    logging.info(f"Combining {len(ref_files)} reference files...")
    
    try:
        # Use MultiZarrToZarr for efficient combining
        mzz = MultiZarrToZarr(
            ref_files,
            concat_dims=[concat_dim],
            identical_dims=['latitude', 'longitude'],  # Adjust as needed
            coo_map={concat_dim: 'cf:time'}  # Adjust if needed
        )
        
        combined_ref = mzz.translate()
        
        # Save combined reference
        with open(output_file, 'w') as f:
            ujson.dump(combined_ref, f)
        
        logging.info(f"Successfully saved combined reference: {output_file}")
        return True
        
    except Exception as e:
        logging.error(f"MultiZarrToZarr failed: {e}")
        logging.info("Falling back to xarray combine method...")
        return False

def combine_with_xarray_batched(data_links, reader_opts, coord_vars, output_file, stage_size=50):
    """Fallback: combine using xarray in stages."""
    stage_datasets = []
    total_stages = (len(data_links) - 1) // stage_size + 1
    
    for i in range(0, len(data_links), stage_size):
        batch = data_links[i:i + stage_size]
        stage_num = i // stage_size + 1
        
        logging.info(f"Processing stage {stage_num}/{total_stages}")
        
        # Open batch
        tasks = [
            delayed(open_virtual_dataset)(
                p, indexes={}, reader_options=reader_opts,
                loadable_variables=coord_vars, decode_times=False
            )
            for p in batch
        ]
        batch_ds = compute(*tasks)
        
        # Combine this stage
        #stage_combined = xr.combine_nested(
        #    batch_ds,
        #    concat_dim='time',
        #    coords='minimal',
        #    compat='override',
        #    combine_attrs='drop_conflicts'
        #)
        
        stage_datasets.append(batch_ds)
        
        # Clean up
        del batch_ds
        
        if stage_num % 3 == 0:
            import gc
            gc.collect()
    
    # Final combine
    logging.info("Combining all stages...")
    virtual_ds_combined = xr.combine_nested(
        stage_datasets,
        concat_dim='time',
        coords='minimal',
        compat='override',
        combine_attrs='drop_conflicts'
    )
    
    # Save to Kerchunk
    logging.info("Generating Kerchunk reference...")
    virtual_ds_combined.virtualize.to_kerchunk(output_file, format='json')
    
    del stage_datasets, virtual_ds_combined
    
    return True

def main(
    collection,
    loadable_coord_vars,
    start_date,
    end_date,
    batch_size=20,
    stage_size=50,
    debug=False,
    use_batched_approach=True
):
    setup_logging(debug)
    logging.info(f"Collection: {collection}")
    logging.info(f"Vars: {loadable_coord_vars}")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"Batch size: {batch_size}")

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
    logging.info(f"Found {len(data_https_links)} data files.")

    coord_vars = loadable_coord_vars.split(",")
    reader_opts = {"storage_options": fs.storage_options}

    # Setup Dask client with conservative memory settings
    n_workers = min(8, multiprocessing.cpu_count() - 1)
    logging.info(f"Starting Dask client with {n_workers} workers")
    client = Client(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit='8GB'  # Adjust based on available memory
    )

    # Filename for combined reference
    temporal = ""
    if start_date or end_date:
        start = start_date if start_date else "beginning"
        end = end_date if end_date else "present"
        temporal = f'{start}_to_{end}_'

    fname_combined_json = f'{collection}_{temporal}virtual_https_batch.json'

    try:
        if use_batched_approach:
            # Method 1: Process in batches and combine with MultiZarrToZarr
            ref_files = process_in_batches(
                data_https_links, 
                reader_opts, 
                coord_vars, 
                batch_size=batch_size
            )
            
            success = False
            #success = combine_references(ref_files, fname_combined_json)
            
            if not success:
                # Fallback to xarray method
                logging.info("Using fallback xarray combine method...")
                combine_with_xarray_batched(
                    data_https_links,
                    reader_opts,
                    coord_vars,
                    fname_combined_json,
                    stage_size=stage_size
                )
            
            # Clean up individual reference files
            logging.info("Cleaning up temporary reference files...")
            for ref_file in ref_files:
                try:
                    os.remove(ref_file)
                except:
                    pass
            try:
                os.rmdir("kerchunk_refs")
            except:
                pass
        else:
            # Method 2: Original xarray approach with staging
            combine_with_xarray_batched(
                data_https_links,
                reader_opts,
                coord_vars,
                fname_combined_json,
                stage_size=stage_size
            )

        logging.info(f"Saved: {fname_combined_json}")

        # Test lazy loading of the combined reference file
        logging.info("Testing combined reference file...")
        data_json = opends_withref(fname_combined_json, fs)
        logging.info(f"Successfully opened combined reference")
        logging.info(f"Dataset shape: {data_json.dims}")

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        raise
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Cloud Optimized Store Reference Files")
    parser.add_argument("--collection", type=str, default="OSCAR_L4_OC_FINAL_V2.0", 
                       help="Earthdata collection short name")
    parser.add_argument("--loadable-coord-vars", type=str, default="latitude,longitude,time", 
                       help="Comma-separated list of loadable coordinate variables")
    parser.add_argument("--start-date", type=str, default=None, 
                       help="Start date (e.g., 1-1-2022)")
    parser.add_argument("--end-date", type=str, default=None, 
                       help="End date (e.g., 1-1-2025)")
    parser.add_argument("--batch-size", type=int, default=20, 
                       help="Number of files to process per batch")
    parser.add_argument("--stage-size", type=int, default=50, 
                       help="Files per stage for xarray fallback method")
    parser.add_argument("--use-batched", action="store_true", default=True,
                       help="Use batched approach with MultiZarrToZarr")
    parser.add_argument("--debug", action="store_true", default=False, 
                       help="Enable debug logging")
    
    args = parser.parse_args()

    main(
        args.collection,
        args.loadable_coord_vars,
        args.start_date,
        args.end_date,
        args.batch_size,
        args.stage_size,
        args.debug,
        args.use_batched
    )