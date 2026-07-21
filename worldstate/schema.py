"""Canonical bitemporal record envelope shared by every domain.

The whole value of this corpus is point-in-time (PIT) correctness. Every row
carries BOTH:
  - event_time     : when the fact is *about* / was observed        (valid time)
  - knowledge_time : when the fact became *knowable* to the world   (transaction time)

An as-of(t) query returns only rows with knowledge_time <= t, and for revisable
series picks the latest vintage whose knowledge_time <= t. That makes lookahead
bias structurally impossible instead of something we merely hope we avoided.
"""
from __future__ import annotations

import hashlib
import pyarrow as pa

# Columns present in EVERY normalized table, in this order, before payload cols.
ENVELOPE_FIELDS: list[tuple[str, pa.DataType]] = [
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("knowledge_time", pa.timestamp("us", tz="UTC")),
    ("domain", pa.string()),      # macro | market | news | fundamentals | events
    ("source", pa.string()),      # stooq | alfred | edgar | gdelt ...
    ("entity", pa.string()),      # ticker / series_id / cik / topic — the row's subject
    ("vintage_id", pa.string()),  # release/revision id for revisable series ("" if immutable)
    ("source_url", pa.string()),
    ("content_hash", pa.string()),  # sha256 of the payload that produced this row
]
ENVELOPE_NAMES = [name for name, _ in ENVELOPE_FIELDS]


def build_schema(payload_fields: list[tuple[str, pa.DataType]]) -> pa.Schema:
    """Envelope + domain-specific payload -> a full pyarrow schema."""
    return pa.schema(ENVELOPE_FIELDS + payload_fields)


def content_hash(*parts) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()
