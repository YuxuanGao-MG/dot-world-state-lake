"""Natural events (wildfires, storms, volcanoes, floods) from NASA EONET (keyless).

Append-only event feed -> clean PIT: event_time = the observation date on each
geometry point; knowledge_time = +1 day. entity = category. One shard per year.
"""
from __future__ import annotations

import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://eonet.gsfc.nasa.gov/api/v3/events"


class NasaEvents(Collector):
    domain = "events"
    source = "nasa_eonet"

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
        r = self.session.get(URL, params={"start": f"{year}-01-01", "end": f"{year}-12-31",
                                          "status": "all", "limit": 5000}, timeout=60)
        r.raise_for_status()
        events = r.json().get("events", [])
        recs = []
        for e in events:
            cats = e.get("categories") or [{}]
            cat = cats[0].get("id", "")
            title = str(e.get("title", ""))
            for gm in (e.get("geometry") or []):
                d = gm.get("date")
                coords = gm.get("coordinates") or [None, None]
                if not d or not isinstance(coords, list) or len(coords) < 2:
                    continue
                recs.append({
                    "event_time": d, "category": cat, "title": title[:200],
                    "longitude": coords[0], "latitude": coords[1],
                    "magnitude_value": gm.get("magnitudeValue"),
                    "magnitude_unit": str(gm.get("magnitudeUnit", "") or ""),
                    "event_id": str(e.get("id", "")),
                })
        if not recs:
            return {"year": year, "rows": 0, "empty": True}

        df = pd.DataFrame(recs)
        ev = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
        keep = ev.notna()
        df, ev = df[keep].reset_index(drop=True), ev[keep].reset_index(drop=True)
        payload = pd.DataFrame({
            "category": df["category"], "title": df["title"],
            "longitude": pd.to_numeric(df["longitude"], errors="coerce"),
            "latitude": pd.to_numeric(df["latitude"], errors="coerce"),
            "magnitude_value": pd.to_numeric(df["magnitude_value"], errors="coerce"),
            "magnitude_unit": df["magnitude_unit"], "event_id": df["event_id"],
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(days=1)).values,
            entity=df["category"].values, source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"year": year, "rows": table.num_rows, "path": path}
