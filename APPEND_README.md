# Granule Append Architecture

## Overview

Append new granules to existing Icechunk v2 virtual Zarr stores on S3, driven by an SQS FIFO queue. Supports 100+ collections concurrently while ensuring only one writer per collection at a time.

## Architecture

```
Cumulus Ingest
      │
      ▼
  CNM-R SNS Topic
      │
      ▼
  Transform Lambda (cnm_transform_lambda)
  ├── Filters: SUCCESS status only, collection allowlist
  ├── Extracts data file URIs (skips .md5, .cmr.json)
  ├── Converts HTTPS → S3 URIs
  └── Sends formatted message to SQS
      │
      ▼
  SQS FIFO Queue
  (MessageGroupId = collection short_name)
      │
      ▼
  Append Lambda (append_lambda_handler)
  ├── Parallel across collections
  ├── Sequential within a collection
  └── Batch size up to 10 messages
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
| `BatchSize` | 10 | Max messages per Lambda invocation |

FIFO queues do not support batching windows. Messages are delivered immediately as they arrive (up to batch size). If 50 granules arrive for MUR25, they process as 5 sequential batches of 10.

### CNM-R Input Format (from Cumulus via SNS)

The transform Lambda receives CNM-R messages like this and extracts the data file URIs:

```json
{
  "collection": "MUR25-JPL-L4-GLOB-v04.2",
  "response": {"status": "SUCCESS"},
  "product": {
    "files": [
      {"type": "data", "uri": "s3://podaac-ops-cumulus-protected/MUR25.../file.nc"},
      {"type": "metadata", "uri": "...file.nc.md5"}
    ]
  }
}
```

It filters for `status == "SUCCESS"`, keeps only `type == "data"` files, converts HTTPS URIs to S3, and forwards to the SQS FIFO queue.

### SQS Message Format (internal)

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
| `terraform/cnm_transform_lambda.py` | Transform Lambda: parses CNM-R from SNS, sends formatted messages to SQS FIFO. |
| `terraform/cnm_transform_lambda.tf` | Terraform: transform Lambda, SNS subscription, IAM. |
| `append_lambda_handler.py` | Append Lambda: receives SQS events, appends granules to Icechunk stores. |
| `terraform/append_lambda.tf` | Terraform: SQS FIFO queue, DLQ, container Lambda, IAM, event source mapping. |
| `append_granules.py` | CLI tool to append granules to an Icechunk store (standalone usage). |
| `test_e2e_setup_store.py` | Test: create a test Icechunk store with 5 granules. |
| `test_e2e_send_sqs.py` | Test: send granule append messages to the SQS FIFO queue. |
| `test_e2e_verify_store.py` | Test: verify/watch the store for updates. |

## Docker Build Targets

The Dockerfile uses multi-stage builds with two targets:

```bash
# ECS target (default — same as before)
docker build --target ecs -t virtualzarr-gen:ecs .

# Lambda target (append handler)
docker build --target lambda -t virtualzarr-gen:lambda .
```

The Lambda target uses the `venv-icechunk` environment with `awslambdaric` and sets the handler to `append_lambda_handler.handler`.

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

## Infrastructure (Terraform)

### CNM Transform (`terraform/cnm_transform_lambda.tf`)

| Resource | Description |
|----------|-------------|
| Lambda function | `service-virtualzarr-gen-{stage}-cnm-transform` — zip-based, Python 3.12, 60s timeout |
| SNS subscription | Subscribes to the CNM-R SNS topic (optional, controlled by `cnm_sns_topic_arn`) |
| IAM role | SNS invoke + SQS send permissions |

### Append Pipeline (`terraform/append_lambda.tf`)

| Resource | Description |
|----------|-------------|
| SQS FIFO queue | `service-virtualzarr-gen-{stage}-append-granule.fifo` — main queue |
| SQS DLQ | `service-virtualzarr-gen-{stage}-append-granule-dlq.fifo` — failed messages after 3 retries |
| ECR repository | Hosts the append Lambda container image |
| Lambda function | `service-virtualzarr-gen-{stage}-append-granule` — container-based, 15 min timeout, 3 GB memory, VPC-attached |
| Event source mapping | SQS → Lambda, batch size 10 |
| IAM role | S3 read/write, SQS consume, CloudWatch logs, SSM parameter access |

### Terraform Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `append_lambda_max_concurrency` | 10 | Max concurrent append Lambda invocations (increase for more collections) |
| `cnm_sns_topic_arn` | `""` | ARN of the CNM-R SNS topic. Leave empty to skip subscription. |
| `cnm_collection_allowlist` | `""` | Comma-separated collection short names to process. Empty = all. |

### Deploying the Lambda Image

```bash
# Build the Lambda image
docker build --target lambda -t append-lambda:latest .

# Tag and push to ECR
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin <account>.dkr.ecr.us-west-2.amazonaws.com
docker tag append-lambda:latest <ecr-repo-url>:<version>
docker push <ecr-repo-url>:<version>

# Deploy with Terraform
cd terraform
terraform apply -var-file=tfvars/sit.tfvars
```

### Sending a Test Message

```bash
aws sqs send-message \
  --queue-url <queue-url> \
  --message-body '{"collection":"MUR25-JPL-L4-GLOB-v04.2","granules":["s3://podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc"]}' \
  --message-group-id "MUR25-JPL-L4-GLOB-v04.2"
```
