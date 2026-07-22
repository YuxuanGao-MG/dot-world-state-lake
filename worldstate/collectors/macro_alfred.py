"""US macro from FRED/ALFRED with full vintage history (needs FRED_API_KEY).

This is the crown jewel for PIT correctness. The ALFRED observations endpoint,
queried across the full realtime range, returns one row per (observation_date,
vintage). We keep every vintage:
  event_time     = observation date  (what period the number describes)
  knowledge_time = realtime_start     (when that value was first released/knowable)
  vintage_id     = realtime_start
So GDP for Q1 that was 2.1% at first print and revised to 1.9% appears as two
rows with different knowledge_times, and as-of queries see the right one.
"""
from __future__ import annotations

import os
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

ALFRED_URL = "https://api.stlouisfed.org/fred/series/observations"


class AlfredVintages(Collector):
    domain = "macro"
    source = "alfred"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=8.0)
        self.key = os.environ.get("FRED_API_KEY", "")

    def chunks(self) -> list[str]:
        return list(settings.MACRO_SERIES)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        series = chunk
        path = hfstore.shard_path(self.domain, self.source, f"series={series}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"series": series, "skipped": True}
        if not self.key:
            # Missing optional key must not red-X the daily cron; skip gracefully.
            return {"series": series, "skipped_no_key": True}

        self.rl.wait()
        params = {
            "series_id": series, "api_key": self.key, "file_type": "json",
            "realtime_start": "1776-07-04", "realtime_end": "9999-12-31",
            "observation_start": settings.BACKFILL_START,
        }
        r = self.session.get(ALFRED_URL, params=params, timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        rows = [o for o in obs if o.get("value", ".") != "."]
        if not rows:
            return {"series": series, "rows": 0, "empty": True}

        df = pd.DataFrame(rows)
        payload = pd.DataFrame({"value": pd.to_numeric(df["value"], errors="coerce")})
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=pd.to_datetime(df["date"], utc=True),
            knowledge_time=pd.to_datetime(df["realtime_start"], utc=True),
            entity=series,
            source_url=f"{ALFRED_URL}?series_id={series}",
            vintage_id=df["realtime_start"].astype(str).values,
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"series": series, "rows": table.num_rows, "path": path}
