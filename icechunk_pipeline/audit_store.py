"""
Audit an Icechunk store against CMR.

Reports, keyed by granule filename (so s3:// and https:// stores both work):

  missing  - in CMR, not in the store (never appended / failed)
  updated  - in the store, but CMR revision-date is newer than when the store
             last wrote it (needs reprocessing)
  extra    - in the store, no longer in CMR (deleted/renamed upstream)
  no_url   - store steps with no source_url recorded (run backfill_source_urls.py)

"When the store wrote it" is, by default, the store's latest commit time
(--since overrides). With --walk-history it is the commit time of the OLDEST
snapshot containing that URL, which is exact per granule but reads source_url at
every snapshot (slow on stores with many commits).

Usage:
    python audit_store.py --store-bucket B --store-prefix P \
        --collection SHORT_NAME --provider POCLOUD --out audit.json
    python audit_store.py ... --walk-history --reprocess-list reprocess.txt
"""
import argparse
import json
import logging
import posixpath
from datetime import datetime, timezone

import earthaccess
import icechunk

from icechunk_pipeline.source_url_coord import read_source_url_map

logger = logging.getLogger("audit_store")


def name_of(url):
    return posixpath.basename(url.split("?", 1)[0])


def parse_dt(s):
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def open_repo(bucket, prefix, region):
    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region=region)
    config = icechunk.Repository.fetch_config(storage)
    return icechunk.Repository.open(storage, config=config)


def first_written(repo, branch):
    """{filename: commit time of the oldest snapshot containing it}."""
    first = {}
    for snap in repo.ancestry(branch=branch):  # newest -> oldest
        store = repo.readonly_session(snapshot_id=snap.id).store
        for url in read_source_url_map(store).values():
            first[name_of(url)] = snap.written_at
    return first


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store-bucket", required=True)
    ap.add_argument("--store-prefix", required=True)
    ap.add_argument("--collection", required=True, help="CMR short_name")
    ap.add_argument("--provider", default="POCLOUD")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--branch", default="main")
    ap.add_argument("--concat-dim", default="time")
    ap.add_argument("--since", help="ISO time; treat granules revised after this as updated "
                                    "(default: store's latest commit time)")
    ap.add_argument("--walk-history", action="store_true",
                    help="Exact per-granule write time from commit history (slow)")
    ap.add_argument("--out", help="Write full JSON report here")
    ap.add_argument("--reprocess-list",
                    help="Write CMR direct-S3 URLs of missing + updated granules, one per line")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    repo = open_repo(args.store_bucket, args.store_prefix, args.region)
    latest = next(iter(repo.ancestry(branch=args.branch))).written_at
    stored = read_source_url_map(repo.readonly_session(branch=args.branch).store,
                                 args.concat_dim)
    stored_by_name = {name_of(u): (t, u) for t, u in stored.items()}
    logger.info("store: %d step(s) with source_url, last commit %s", len(stored), latest)

    results = earthaccess.search_data(short_name=args.collection,
                                      provider=args.provider, cloud_hosted=True)
    cmr = {}
    for g in results:
        links = g.data_links(access="direct")
        if links:
            cmr[name_of(links[0])] = (links[0], parse_dt(g["meta"]["revision-date"]))
    logger.info("CMR: %d granule(s)", len(cmr))

    written = first_written(repo, args.branch) if args.walk_history else {}
    since = parse_dt(args.since) if args.since else latest

    missing = sorted(n for n in cmr if n not in stored_by_name)
    extra = sorted(n for n in stored_by_name if n not in cmr)
    updated = []
    for n, (url, rev) in cmr.items():
        if n not in stored_by_name:
            continue
        wrote = written.get(n, since)
        if rev > wrote:
            updated.append({"granule": n, "revision_date": rev.isoformat(),
                            "store_written": wrote.isoformat(),
                            "time": stored_by_name[n][0]})
    updated.sort(key=lambda d: d["revision_date"])

    report = {
        "collection": args.collection,
        "store": f"s3://{args.store_bucket}/{args.store_prefix}",
        "counts": {"cmr": len(cmr), "store": len(stored), "missing": len(missing),
                   "updated": len(updated), "extra": len(extra)},
        "missing": [{"granule": n, "url": cmr[n][0], "revision_date": cmr[n][1].isoformat()}
                    for n in missing],
        "updated": updated,
        "extra": extra,
    }

    print(json.dumps(report["counts"], indent=2))
    for label in ("missing", "updated", "extra"):
        for item in report[label][:10]:
            print(f"  {label}: {item['granule'] if isinstance(item, dict) else item}")
        if len(report[label]) > 10:
            print(f"  {label}: ... {len(report[label]) - 10} more")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("wrote %s", args.out)
    if args.reprocess_list:
        urls = [cmr[n][0] for n in missing] + [cmr[u["granule"]][0] for u in updated]
        with open(args.reprocess_list, "w") as f:
            f.write("\n".join(urls) + ("\n" if urls else ""))
        logger.info("wrote %d URL(s) to %s", len(urls), args.reprocess_list)


if __name__ == "__main__":
    main()
