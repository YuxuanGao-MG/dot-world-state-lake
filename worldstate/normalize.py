"""Assemble domain payloads into the canonical bitemporal envelope table."""
from __future__ import annotations

import pandas as pd
import pyarrow as pa

from worldstate import schema


def to_table(
    *,
    domain: str,
    source: str,
    payload: pd.DataFrame,
    event_time,          # array-like of tz-aware UTC timestamps
    knowledge_time,      # array-like of tz-aware UTC timestamps
    entity,              # scalar or array-like
    source_url,          # scalar or array-like
    vintage_id="",       # scalar or array-like
) -> pa.Table:
    """Build a pyarrow Table = envelope columns + payload columns.

    `payload` holds only the domain-specific columns (e.g. open/high/low/close).
    Envelope columns are prepended and a per-row content_hash is computed over
    the payload + entity + event_time so identical facts hash identically.
    """
    n = len(payload)
    df = payload.reset_index(drop=True).copy()

    def col(v):
        return v if hasattr(v, "__len__") and not isinstance(v, str) else [v] * n

    env = pd.DataFrame({
        "event_time": pd.to_datetime(event_time, utc=True),
        "knowledge_time": pd.to_datetime(knowledge_time, utc=True),
        "domain": domain,
        "source": source,
        "entity": col(entity),
        "vintage_id": [str(x) for x in col(vintage_id)],
        "source_url": col(source_url),
    })
    env["content_hash"] = [
        schema.content_hash(env.entity[i], env.event_time[i],
                            *[df[c].iloc[i] for c in df.columns])
        for i in range(n)
    ]

    out = pd.concat([env[schema.ENVELOPE_NAMES], df], axis=1)
    # Let pyarrow infer payload types; envelope types are pinned via cast below.
    table = pa.Table.from_pandas(out, preserve_index=False)
    return table
