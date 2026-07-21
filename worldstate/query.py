"""As-of (point-in-time) query engine over the HF Parquet corpus via DuckDB.

The one rule that makes this corpus valuable: an as-of(t) query may only see rows
with knowledge_time <= t, and for revisable series (macro) it must pick, per
(entity, event_time), the LATEST vintage whose knowledge_time <= t. That is the
number an agent standing at time t would actually have seen — no lookahead.
"""
from __future__ import annotations

import os
import duckdb

from config import settings


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    tok = os.environ.get("HF_TOKEN")
    if tok:
        con.execute(f"CREATE SECRET hf (TYPE huggingface, TOKEN '{tok}');")
    return con


def _glob(domain: str, source: str) -> str:
    repo = settings.HF_DATASET_REPO or os.environ["HF_DATASET_REPO"]
    return (f"hf://datasets/{repo}/{settings.DATA_PREFIX}/"
            f"domain={domain}/source={source}/**/*.parquet")


def as_of_prices(con, as_of: str, source: str = "stooq"):
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
