"""Significant earthquakes from USGS (keyless).

Append-only event catalog -> clean PIT: a quake is knowable at its origin time
(we add a small reporting lag). event_time = knowledge_time = origin time.
entity = "EARTHQUAKE". One shard per year, magnitude >= 4.5.
"""
from __future__ import annotations

import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MIN_MAG = 4.5


class UsgsQuakes(Collector):
    domain = "events"
    source = "usgs"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=1.0)

    def chunks(self) -> list[str]:
        return [str(y) for y in range(int(settings.BACKFILL_START[:4]), date.today().year + 1)]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year = chunk
        path = hfstore.shard_path(self.domain, self.source, f"year={year}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"year": year, "skipped": True}

        self.rl.wait()
        r = self.session.get(URL, params={
            "format": "geojson", "starttime": f"{year}-01-01", "endtime": f"{year}-12-31",
            "minmagnitude": MIN_MAG, "orderby": "time-asc"}, timeout=60)
        r.raise_for_status()
        feats = r.json().get("features", [])
        if not feats:
            return {"year": year, "rows": 0, "empty": True}

        recs = []
        for f in feats:
            p = f.get("properties", {})
            g = (f.get("geometry") or {}).get("coordinates") or [None, None, None]
            if p.get("time") is None:
                continue
            recs.append({
                "time": int(p["time"]), "magnitude": p.get("mag"),
                "place": str(p.get("place", "")), "longitude": g[0], "latitude": g[1],
                "depth_km": g[2], "url": str(p.get("url", "")),
            })
        df = pd.DataFrame(recs)
        ev = pd.to_datetime(df["time"], unit="ms", utc=True)
        payload = pd.DataFrame({
            "magnitude": pd.to_numeric(df["magnitude"], errors="coerce"),
            "place": df["place"], "longitude": pd.to_numeric(df["longitude"], errors="coerce"),
            "latitude": pd.to_numeric(df["latitude"], errors="coerce"),
            "depth_km": pd.to_numeric(df["depth_km"], errors="coerce"), "url": df["url"],
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(minutes=20)).values,
            entity="EARTHQUAKE", source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"year": year, "rows": table.num_rows, "path": path}
