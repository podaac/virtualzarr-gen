# Append Pipeline Architecture

How a published granule becomes an appended step in an Icechunk v2 virtual Zarr
store. Covers the AWS wiring, the per-invocation handler logic, and the
correctness properties the design relies on.

**Audience:** engineers maintaining the append + generation pipeline.

Related code:
- `append_lambda_handler.py` — the append Lambda
- `terraform/cnm_transform_lambda.tf` — owned SNS topic + transform Lambda
- `terraform/append_lambda.tf` — SQS FIFO, DLQ, append Lambda, ECR image
- `podaac/collection_config.py` — per-collection settings + store paths
- `icechunk_pipeline/tests/` — e2e tests driving the live pipeline

---

## 1. High-level AWS flow

Granules reach the queue two ways: production CNM-R from Cumulus (via the owned
SNS topic + transform Lambda), or direct SQS sends from tests/manual tools
(which skip SNS and the transform entirely).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INGESTION TRIGGER (two ways)                          │
└─────────────────────────────────────────────────────────────────────────────┘

  (A) PRODUCTION                                   (B) TEST / MANUAL
  ┌──────────────────┐                             ┌───────────────────────────┐
  │ Cumulus          │                             │ trigger_task / manual      │
  │ (publishes CNM-R)│                             │ sqs.send_message           │
  └────────┬─────────┘                             └──────────────┬────────────┘
           │ SNS:Publish                                          │ (skips SNS +
           ▼                                                      │  transform)
  ┌──────────────────────────────────┐                           │
  │ SNS topic (WE OWN)               │                            │
  │ service-virtualzarr-gen-sit-     │                            │
  │ cnm-response                     │                            │
  │  policy: AllowPublish            │                            │
  └────────┬─────────────────────────┘                           │
           │ protocol: lambda                                     │
           ▼                                                      │
  ┌──────────────────────────────────┐                           │
  │ cnm-transform Lambda (zip)       │                            │
  │  • drop non-SUCCESS              │                            │
  │  • allowlist filter              │                            │
  │  • CNM-R → {collection,granules} │                            │
  │  • MessageGroupId = collection   │                            │
  └────────┬─────────────────────────┘                           │
           │ sqs.send_message                                     │
           └───────────────┬──────────────────────────────────────┘
                           ▼
         ┌───────────────────────────────────────────┐
         │ SQS FIFO queue                            │
         │ ...-append-granule.fifo                   │
         │  • per-collection ordering (GroupId)      │
         │  • batch_size 10, maxConcurrency per coll │
         └────────┬──────────────────────────────────┘
                  │ event source mapping
                  │ (ReportBatchItemFailures)
                  ▼
         ┌───────────────────────────────────────────┐
         │ append Lambda (container image, ECR)      │  ◀── EARTHDATA_* env (EDL creds)
         │ append_lambda_handler.handler             │  ◀── STORE_BUCKET env
         └────────┬──────────────────────────────────┘
                  │
    ┌─────────────┴──────────────┐  partial failures
    │ success → SQS deletes msg  │──────────────┐
    └────────────────────────────┘              ▼
                  │                    ┌──────────────────────┐
                  │                    │ retry ≤3 → DLQ.fifo   │
                  ▼                    │ + CloudWatch alarm    │
      ┌─────────────────────────┐     └──────────────────────┘
      │ Icechunk stores (S3)    │
      │ {coll}.icechunk_v2.s3/  │
      │ {coll}.icechunk_v2.https/│
      └─────────────────────────┘
```

---

## 2. Inside the append Lambda (per invocation)

```
handler(event)
  │
  ├─ login_earthdata()                      SSM best-effort → fallback to EARTHDATA_* env
  │
  ├─ group SQS records by collection        (one poll may carry several GroupIds)
  │     malformed msg → batchItemFailures
  │
  └─ for each collection group:
        append_to_collection(collection, granules, store_prefix_override?)
          │
          ├─ get_collection_config(collection)   concat_dim / data_vars / coords / preprocess / sort
          │
          ├─ build_vds(granules)                 open_virtual_mfdataset (decode_times=False)
          │     └─ S3Store + NasaEarthdataCredentialProvider  (reads source granules)
          │
          ├─ S3 store   ─┐
          └─ HTTPS store ─┤   _append_to_store(...)  × 2   (rename_paths for HTTPS)
                          │
                          ▼
          _append_to_store:
            ├─ open_repo(bucket, prefix)            prefix = override OR get_store_prefix_s3/https(coll)
            ├─ writable_session("main")
            ├─ open_zarr(decode_times=False)        ← read existing time axis
            ├─ _filter_new_along_dim                ← idempotency: drop times already present
            │     all present → n_new=0 → NO commit (no-op)
            ├─ monotonicity check                   incoming_min ≤ existing_max → WARN (still appends)
            ├─ if sort: vds.sortby(concat_dim)       (block-level only)
            ├─ vds.vz.to_icechunk(append_dim)
            └─ session.commit(...)                   → verify readonly_session

        exception in a collection → that collection's message_ids → batchItemFailures (retry)
```

---

## 3. Correctness properties

| Concern | Where handled |
|---|---|
| **Per-collection ordering** | FIFO `MessageGroupId = collection` (transform Lambda sets it) |
| **Idempotency** (at-least-once retries) | `_filter_new_along_dim` dedups by `time` value |
| **Two stores, non-atomic** | S3 + HTTPS committed separately; idempotency makes re-run safe |
| **Partial failure isolation** | `ReportBatchItemFailures` → only failing collection's msgs retry |
| **Poison messages** | redrive to DLQ after 3 tries + CloudWatch alarm |
| **Store path** | `STORE_BUCKET` env + `store_prefix_override` or `get_store_prefix_*(collection)` |
| **Store must pre-exist** | handler `open_repo` only opens; generation is a separate ECS path |
| **Credentials** | `EARTHDATA_*` env (manual, SSM fallback); `ignore_changes` keeps them across applies |

### Dedup is by `time` value, not granule name
`_filter_new_along_dim` compares the incoming `time` coordinate against what the
store already holds. Both sides are read **undecoded** (`decode_times=False`) so
raw encoded ints are compared on both sides — decoding one side to `datetime64`
raises `DTypePromotionError`. Consequences:

- Re-sending the same granule → same `time` → skipped (no-op). Safe to replay.
- A granule **reprocessed with the same `time` but new data** → **skipped**, old
  data kept. By-time dedup cannot tell "already have it" from "same timestamp,
  new bytes". Fixing that needs a rewrite, not an append.

### Ordering: append is append-only
Icechunk `append_dim` only writes at the **end**; it cannot insert mid-array or
reorder committed steps.

- **In order** (feed chronologically — FIFO preserves send order): store stays
  monotonic. This is the normal path.
- **Out of order / backfill** (a late granule with an earlier `time`): it is
  appended at the end → non-monotonic `time` axis. The handler logs a WARNING
  but still appends. `sortby` only sorts the incoming block, never the store.
  A non-monotonic axis breaks consumer `.sel(time=slice(...))`, `.resample`,
  `.interp`. Getting a globally sorted store requires **regeneration**, not
  append.

---

## 4. Store paths

Resolved as `s3://{STORE_BUCKET}/{prefix}`:

```
virtual_collections/{collection}/{collection}.icechunk_v2.s3/
virtual_collections/{collection}/{collection}.icechunk_v2.https/
```

- `STORE_BUCKET` = `var.output_bucket[0]` (Lambda env).
- Prefix precedence: `store_prefix` in the SQS message (tests use this to target
  scratch stores) → else `get_store_prefix_s3/https(collection)`.
- The HTTPS prefix is derived from the S3 one by swapping `.s3/` → `.https/`.
- The **generator** (`podaac/generate_vds_icechunk.py`) uses the *same*
  `get_store_prefix_*` helpers, so generation and append agree on the path.

---

## 5. Boundaries / out of scope for append

- **Generation** — creating the initial store (search all granules → build →
  sort → write) is a separate ECS pipeline. The append Lambda only *opens* an
  existing store; appending to a collection with no store fails → DLQ.
- **Reprocessing / out-of-order reconciliation** — append is append-only and
  by-time; corrected-but-same-time data and backfills are not reconciled here.
  Handle by regenerating the collection's store.
- **Deletes** — the handler never removes steps.

---

## 6. Testing

`icechunk_pipeline/tests/test_icechunk_append.py` covers the write-mode planner
(region / append / insert placement) against a real in-memory Icechunk store
backed by local virtual chunks. It runs with `pytest` and needs no cloud
credentials.
