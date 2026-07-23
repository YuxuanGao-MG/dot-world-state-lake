"""Reusable FRED/ALFRED vintage collector base.

Subclass it with a SERIES list + domain + source to get a whole new vintage-clean
domain for free. Captures true release vintages (knowledge_time = first-release
realtime_start); falls back to latest values if a series 400s on the wide realtime
range; never crashes. Needs FRED_API_KEY.
"""
from __future__ import annotations

import os
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

ALFRED_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredVintageBase(Collector):
    domain = "macro"
    source = "fred"
    SERIES: list[str] = []

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=8.0)
        self.key = os.environ.get("FRED_API_KEY", "")

    def chunks(self) -> list[str]:
        return list(self.SERIES)

    def run_chunk(self, series: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, f"series={series}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"series": series, "skipped": True}
        if not self.key:
            return {"series": series, "skipped_no_key": True}

        def _fetch(vintage: bool):
            p = {"series_id": series, "api_key": self.key, "file_type": "json",
                 "observation_start": settings.BACKFILL_START}
            if vintage:
                p["realtime_start"], p["realtime_end"] = "1776-07-04", "9999-12-31"
            self.rl.wait()
            return self.session.get(ALFRED_URL, params=p, timeout=settings.HTTP_TIMEOUT)

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
            know = event + pd.Timedelta(days=1)
            vid = ""
        payload = pd.DataFrame({"value": pd.to_numeric(df["value"], errors="coerce")})
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=event, knowledge_time=know, entity=series,
            source_url=f"{ALFRED_URL}?series_id={series}", vintage_id=vid,
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"series": series, "rows": table.num_rows, "vintage": vintage, "path": path}
