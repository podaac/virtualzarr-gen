# PO.DAAC's Virtualzarr Generation

The purpose of this repository is to provide an operationalized mechanism for generating kerchunk virtual data stores for Level 3 and Level 4 data products. Level 2 products will be added in time as well. The resulting virtual dataset will offer chunked data access within the us-west-2 region ***AS WELL AS*** remote, non-cloud access. That is, users can run the same workflows within and outside of the cloud and reap the benefits of cloud optimized access.

# Quick Setup and Run

Ensure you have a `.netrc` file setup to utilize earthaccess access features.

```
conda create --name cloudgen
conda activate cloudgen
pip install -r requirements.txt 
papermill generate_cloud_optimized_store_https.ipynb output.ipynb --log-output  -p collection TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4 -p loadable_coord_vars 'lat,lon,time' 
```

If you get an error during build that looks like:
```
Collecting pyyaml>=5.3.1 (from dask==2024.5.2->dask[complete]==2024.5.2->-r requirements.txt (line 6))
  Using cached PyYAML-6.0.1.tar.gz (125 kB)
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Preparing metadata (pyproject.toml) ... done
  Using cached PyYAML-6.0.tar.gz (124 kB)
  Installing build dependencies ... done
  Getting requirements to build wheel ... error
  error: subprocess-exited-with-error
  
  × Getting requirements to build wheel did not run successfully.
  │ exit code: 1
  ╰─> [78 lines of output]
      /private/var/folders/db/wncsd6tj18dgzwxnzqgcxtq80000gq/T/pip-build-env-6_qz3z8j/overlay/lib/python3.13/site-packages/setuptools/dist.py:759: SetuptoolsDeprecationWarning: License classifiers are deprecated.
      !!

```

run:
```
pip install PyYAML==6.0.2
```
and try again.


# How it works
Fundamentally, the notebook within this repository will access all the files of a given collection (filterable by start and end dates if desired), and generate kerchunk datamaps for each file, and then aggregate them all into a single virtual datastore file. The notebook can be run manually, but has been converted into a papermill notebook that is run within the docker container generated from this repository. There are several environment variables required by the Docker container to run effectively.

| Environment Variable  | Definition |Example |  Used in |
| ------------- | ------------- | ------------- | ------------- |
|  LOADABLE_VARS |  The variables whos data values will be included in the virtual dataset files. These can be used without accessing actual data files. Often these are the dimmension/coordinate data within a netcdf file. | `lat,lon,time` - note, no spaces! | Notebook |
|  COLLECTION | the collection name to generate virtual datasets for. This is used in the Earthaccess search.  | `MUR-JPL-L4-GLOB-v4.1`| Notebook |
|  START_DATE | Start data of data range to process  | `1-1-2022` | Notebook |
|  END_DATE |  End date of data range to process |`12-31-2022` | Notebook |
|  OUTPUT_BUCKET | Output bucket to write virtual datastore files to, along with the resulting notebook run  | `podaac-virtualzarr-gen` | Notebook, container entrypoint |
|  SSM_EDL_USERNAME |  SSM Parameter that stores a valid EDL username | `virtualzarr-gen/edl_username` | container entrypoint |
|  SSM_EDL_PASSWORD |  SSM Parameter that stores a valid EDL password | `virtualzarr-gen/edl_password` | container entrypoint |

Note: we use the parameter store for specifying credentials so that it can be changed over time and so that the values are not visible when exploring job runs from the ECS cluster.

See 'wrapper.sh' to view how these environment variables are used.

## Deployment

Once you're ready to deploy and run this, you can deploy this container as an ECS process through terraform. This will create an ECS cluster, task definitions, launch config and autoscaling cluster to spin up an EC2 instance, do the processing, and spin down while adding logs to cloudwatch. There are a number of variables that can be configured.


## Limitations and Notes

* The CMR calls to discover data products utilize Earthaccess. Inputs for the collection, start and end dates are directly influenced by its requirements.
* While users are able to filter the time period on which they will generate virtual datasets, there is currently no mechanisms for appending a new data files kerchunk store to an existing virtual dataset. This is a desired function in the works.
