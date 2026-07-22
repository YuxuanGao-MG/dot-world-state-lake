"""AWS S3 backend — the primary lake. No commit model, so writes are fast and
massively concurrent (unlike HF's rate-limited git commits). Same interface as
hfstore, selected via worldstate/store.py.
"""
from __future__ import annotations

import io
import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from functools import lru_cache

from config import settings


@lru_cache(maxsize=1)
def _client():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", settings.AWS_REGION),
        config=Config(retries={"max_attempts": 8, "mode": "adaptive"},
                      max_pool_connections=32),
    )


def _bucket() -> str:
    b = settings.S3_BUCKET or os.environ.get("S3_BUCKET", "")
    if not b:
        raise RuntimeError("S3_BUCKET not set")
    return b


def ensure_repo() -> str:
    """Bucket is created once at setup; nothing to do per job."""
    return _bucket()


def exists(path_in_repo: str) -> bool:
    try:
        _client().head_object(Bucket=_bucket(), Key=path_in_repo)
        return True
    except Exception:
        return False


def _table_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def upload_table(table: pa.Table, path_in_repo: str, *, overwrite: bool = False) -> bool:
    if not overwrite and exists(path_in_repo):
        return False
    _client().put_object(Bucket=_bucket(), Key=path_in_repo, Body=_table_bytes(table))
    return True


def upload_tables(pairs: list[tuple[pa.Table, str]], *, commit_message: str = "",
                  overwrite: bool = False) -> int:
    """S3 has no commit model — each put is independent, so just write them all."""
    n = 0
    for table, path in pairs:
        if upload_table(table, path, overwrite=overwrite):
            n += 1
    return n


def shard_path(domain: str, source: str, *parts: str, name: str) -> str:
    segs = [settings.DATA_PREFIX, f"domain={domain}", f"source={source}", *parts, name]
    return "/".join(segs)
