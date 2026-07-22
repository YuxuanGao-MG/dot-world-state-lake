"""As-of (point-in-time) query engine over the Parquet corpus via DuckDB.

The one rule that makes this corpus valuable: an as-of(t) query may only see rows
with knowledge_time <= t, and for revisable series (macro) it must pick, per
(entity, event_time), the LATEST vintage whose knowledge_time <= t. That is the
number an agent standing at time t would actually have seen — no lookahead.

Reads from the configured backend (S3 by default; HF mirror if STORAGE_BACKEND=hf).
"""
from __future__ import annotations

import os
import duckdb

from config import settings

_BACKEND = os.environ.get("STORAGE_BACKEND", settings.STORAGE_BACKEND)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if _BACKEND == "hf":
        tok = os.environ.get("HF_TOKEN")
        if tok:
            con.execute(f"CREATE SECRET hf (TYPE huggingface, TOKEN '{tok}');")
    else:
        kid = os.environ.get("AWS_ACCESS_KEY_ID", "")
        sec = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        reg = os.environ.get("AWS_REGION", settings.AWS_REGION)
        con.execute(
            f"CREATE SECRET s3 (TYPE s3, KEY_ID '{kid}', SECRET '{sec}', REGION '{reg}');")
    return con


def _glob(domain: str, source: str) -> str:
    p = f"{settings.DATA_PREFIX}/domain={domain}/source={source}/**/*.parquet"
    if _BACKEND == "hf":
        repo = settings.HF_DATASET_REPO or os.environ["HF_DATASET_REPO"]
        return f"hf://datasets/{repo}/{p}"
    bucket = settings.S3_BUCKET or os.environ["S3_BUCKET"]
    return f"s3://{bucket}/{p}"


def as_of_prices(con, as_of: str, source: str = "yahoo"):
    """Daily bars knowable as of `as_of` (ISO timestamp)."""
    return con.execute(
        f"""SELECT entity, event_time, close, volume
            FROM read_parquet('{_glob('market', source)}', hive_partitioning=1)
            WHERE knowledge_time <= TIMESTAMP '{as_of}'
            ORDER BY entity, event_time""").df()


def as_of_macro(con, as_of: str, source: str = "alfred"):
    """Macro values as they were FIRST KNOWN on/before `as_of` — vintage-correct."""
    return con.execute(
        f"""WITH pit AS (
              SELECT entity, event_time, value, knowledge_time,
                     row_number() OVER (PARTITION BY entity, event_time
                                        ORDER BY knowledge_time DESC) AS rn
              FROM read_parquet('{_glob('macro', source)}', hive_partitioning=1)
              WHERE knowledge_time <= TIMESTAMP '{as_of}')
            SELECT entity, event_time, value, knowledge_time AS vintage
            FROM pit WHERE rn = 1 ORDER BY entity, event_time""").df()
