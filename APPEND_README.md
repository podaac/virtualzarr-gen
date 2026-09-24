# Granule Append Architecture

## Overview

Append new granules to existing Icechunk v2 virtual Zarr stores on S3, driven by an SQS FIFO queue. Supports 100+ collections concurrently while ensuring only one writer per collection at a time.

## Architecture

```
Event Source (CNM, Cumulus, EventBridge)
        │
        ▼
   SQS FIFO Queue
   (MessageGroupId = collection short_name)
        │
        ▼
   Lambda (one per collection, concurrent)
        │
        ▼
   Icechunk Store on S3 (one per collection)
```

## How It Works

### Single FIFO Queue, Partitioned by Collection

All collections share one SQS FIFO queue. Each message uses the collection short name as the `MessageGroupId`. This gives us:

- **Parallel across collections** — messages for different collections dispatch to separate Lambda invocations concurrently
- **Sequential within a collection** — messages for the same collection are delivered in order, one batch at a time, preventing concurrent writes to the same Icechunk store

### Batching

SQS batches messages before triggering Lambda using two settings:

| Setting | Value | Description |
|---------|-------|-------------|
| `BatchSize` | 10 | Max granules per Lambda invocation |
| `MaximumBatchingWindow` | 60s | Wait up to 60s to fill a batch before triggering |

If 50 granules arrive for MUR25, they process as 5 sequential batches of 10. Each batch appends all 10 granules in a single Icechunk commit.

### Message Format

```json
{
  "collection": "MUR25-JPL-L4-GLOB-v04.2",
  "granules": [
    "s3://podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/file1.nc",
    "s3://podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/file2.nc"
  ]
}
```

The `MessageGroupId` must be set to the `collection` value, and each message needs a unique `MessageDeduplicationId`.

### Lambda Processing

Each Lambda invocation:

1. Receives a batch of SQS messages (up to 10 granules)
2. Groups granules by collection (usually all the same within a batch)
3. Builds a virtual dataset using `virtualizarr`
4. Opens the existing Icechunk store on S3
5. Appends the virtual dataset along the concat dimension (typically `time`)
6. Commits with a timestamped message
7. Verifies the final store shape

On success, SQS automatically deletes the processed messages. On failure, messages return to the queue after the visibility timeout and retry.

## Example Flow

```
100 collections, each receiving granules throughout the day:

  MUR25 (50 granules queued)
  ├── Batch 1: 10 granules → Lambda → commit → done
  ├── Batch 2: 10 granules → Lambda → commit → done   (starts after batch 1)
  ├── Batch 3: 10 granules → Lambda → commit → done
  ├── Batch 4: 10 granules → Lambda → commit → done
  └── Batch 5: 10 granules → Lambda → commit → done

  SMAP (20 granules queued)              ← runs concurrently with MUR25
  ├── Batch 1: 10 granules → Lambda → commit → done
  └── Batch 2: 10 granules → Lambda → commit → done

  OSTIA (5 granules queued)              ← runs concurrently with both
  └── Batch 1: 5 granules → Lambda → commit → done
```

## Key Files

| File | Purpose |
|------|---------|
| `append_granules.py` | CLI tool to append granules to an Icechunk store. Used by Lambda or standalone. |
| `sqs_append_granules.py` | SQS polling version (alternative to Lambda-triggered approach). Runs as a long-lived process. |
| `test_append.py` | End-to-end test: creates a store, appends a granule, verifies the result. |

## Collection Configuration

Each collection can specify:

- `concat_dim` — dimension to append along (default: `time`)
- `data_vars` — xarray data_vars strategy (`minimal` or explicit list)
- `coords` — xarray coords strategy (`minimal` or `all`)
- `preprocess` — optional preprocessing function (e.g. expanding time dims, extracting time from filename)

## Constraints

- **Lambda timeout**: 15 minutes max. If appending 10 granules exceeds this, reduce `BatchSize` or switch to ECS.
- **Icechunk single-writer**: Only one writer per branch per store at a time. The FIFO MessageGroupId enforces this.
- **FIFO throughput**: 300 messages/sec per MessageGroupId, 3000 messages/sec per queue with high throughput mode. More than sufficient for granule ingestion rates.

## Infrastructure Requirements

- SQS FIFO queue with content-based deduplication or explicit `MessageDeduplicationId`
- Lambda function with IAM permissions for S3 (read source buckets, read/write store bucket) and SQS
- Earthdata authentication credentials (via environment variable or Secrets Manager)
- Python dependencies: `earthaccess`, `icechunk`, `virtualizarr`, `xarray`, `obstore`, `obspec_utils`
