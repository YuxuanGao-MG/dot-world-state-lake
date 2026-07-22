"""Print the S3 lake inventory: files + bytes + rough rows per domain/source.

Run locally (with AWS creds) or via the `status` GitHub Action to see current
size from anywhere (incl. phone: Actions -> status -> Run workflow -> read log).
"""
from __future__ import annotations

import collections
import boto3

from config import settings
from worldstate import provenance as P


def main():
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    bucket = settings.S3_BUCKET
    paginator = s3.get_paginator("list_objects_v2")
    by = collections.defaultdict(lambda: [0, 0])  # domain/source -> [bytes, files]
    tb = tf = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=settings.DATA_PREFIX + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            parts = key.split("/")
            grp = "/".join(parts[1:3]) if len(parts) > 2 else key
            by[grp][0] += obj["Size"]
            by[grp][1] += 1
            tb += obj["Size"]
            tf += 1
    def cls_of(k):
        parts = dict(seg.split("=", 1) for seg in k.split("/") if "=" in seg)
        return P.pit_class(parts.get("domain", ""), parts.get("source", ""))

    print(f"=== s3://{bucket}/{settings.DATA_PREFIX} ===")
    print(f"  {'source':36} {'files':>5} {'size':>12}  pit_class")
    for k in sorted(by):
        c = cls_of(k)
        flag = "" if c in P.HISTORICAL_SAFE else "  <-- forward-only/caution"
        print(f"  {k:36} {by[k][1]:>5} {by[k][0]/1e6:9.1f} MB  {c}{flag}")
    print(f"  {'TOTAL':36} {tf:>5} {tb/1e6:9.1f} MB  ({tb/1e9:.3f} GB)")


if __name__ == "__main__":
    main()
