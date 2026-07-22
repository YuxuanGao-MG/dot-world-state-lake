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

        def _fetch(vintage: bool):
            p = {"series_id": series, "api_key": self.key, "file_type": "json",
                 "observation_start": settings.BACKFILL_START}
            if vintage:
                p["realtime_start"], p["realtime_end"] = "1776-07-04", "9999-12-31"
            self.rl.wait()
            return self.session.get(ALFRED_URL, params=p, timeout=settings.HTTP_TIMEOUT)

        # Try full vintage history; some (mostly daily) series 400 on the wide
        # realtime range, so fall back to latest values. Never crash the job.
        try:
            r = _fetch(vintage=True)
            vintage = r.status_code == 200
            if not vintage:
                r = _fetch(vintage=False)
            if r.status_code != 200:
                return {"series": series, "skipped_error": r.status_code}
            obs = r.json().get("observations", [])
        except Exception as e:
            return {"series": series, "skipped_error": type(e).__name__}

        rows = [o for o in obs if o.get("value", ".") != "."]
        if not rows:
            return {"series": series, "rows": 0, "empty": True}

        df = pd.DataFrame(rows)
        event = pd.to_datetime(df["date"], utc=True)
        if vintage:
            know = pd.to_datetime(df["realtime_start"], utc=True)
            vid = df["realtime_start"].astype(str).values
        else:
            know = event + pd.Timedelta(days=1)  # no vintage; known ~next day
            vid = ""
        payload = pd.DataFrame({"value": pd.to_numeric(df["value"], errors="coerce")})
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=event, knowledge_time=know, entity=series,
            source_url=f"{ALFRED_URL}?series_id={series}", vintage_id=vid,
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"series": series, "rows": table.num_rows, "vintage": vintage, "path": path}
