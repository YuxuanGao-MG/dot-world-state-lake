"""US energy fundamentals from EIA v2 (needs a free EIA_API_KEY).

Weekly petroleum/gas balance — the real-economy supply side for energy markets.
Released a few days after the reference week, so knowledge_time = period + 4 days.
entity = series name. One shard per series. Skips gracefully without a key.
"""
from __future__ import annotations

import os
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

V2 = "https://api.eia.gov/v2/{route}/data/"
# (name, route, series_id, frequency)
SERIES = [
    ("crude_stocks_excl_spr", "petroleum/stoc/wstk", "WCESTUS1", "weekly"),
    ("gasoline_stocks", "petroleum/stoc/wstk", "WGTSTUS1", "weekly"),
    ("distillate_stocks", "petroleum/stoc/wstk", "WDISTUS1", "weekly"),
    ("crude_production", "petroleum/sum/sndw", "WCRFPUS2", "weekly"),
    ("refiner_inputs", "petroleum/sum/sndw", "WCRRIUS2", "weekly"),
    ("natgas_storage", "natural-gas/stor/wkly", "NW2_EPG0_SWO_R48_BCF", "weekly"),
]


class EiaEnergy(Collector):
    domain = "commodity"
    source = "eia"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=2.0)
        self.key = os.environ.get("EIA_API_KEY", "")

    def chunks(self) -> list[str]:
        return [s[0] for s in SERIES]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        spec = next((s for s in SERIES if s[0] == chunk), None)
        if spec is None:
            return {"series": chunk, "unknown": True}
        name, route, series_id, freq = spec
        path = hfstore.shard_path(self.domain, self.source, f"series={name}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"series": name, "skipped": True}
        if not self.key:
            return {"series": name, "skipped_no_key": True}

        self.rl.wait()
        params = {
            "api_key": self.key, "frequency": freq, "data[0]": "value",
            "facets[series][]": series_id, "start": settings.BACKFILL_START,
            "sort[0][column]": "period", "sort[0][direction]": "asc", "length": 5000,
        }
        r = self.session.get(V2.format(route=route), params=params, timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json().get("response", {}).get("data", [])
        rows = [d for d in data if d.get("value") is not None and d.get("period")]
        if not rows:
            return {"series": name, "rows": 0, "empty": True}

        df = pd.DataFrame(rows)
        ev = pd.to_datetime(df["period"], utc=True, errors="coerce")
        payload = pd.DataFrame({"series": name,
                                "value": pd.to_numeric(df["value"], errors="coerce")})
        keep = ev.notna()
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload[keep].reset_index(drop=True),
            event_time=ev[keep].values, knowledge_time=(ev[keep] + pd.Timedelta(days=4)).values,
            entity=name, source_url=V2.format(route=route), vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"series": name, "rows": table.num_rows, "path": path}
