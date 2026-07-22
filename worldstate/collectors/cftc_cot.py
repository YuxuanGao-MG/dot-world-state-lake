"""CFTC Commitments of Traders — weekly futures positioning (Socrata, keyless).

Legacy futures-only report: per market, positions held by non-commercials
(speculators), commercials (hedgers), and non-reportables. The Tuesday snapshot
is released the following Friday, so knowledge_time = report_date + 3 days.
entity = contract market name. One shard per year.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter
from datetime import date

COT = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
COLS = {
    "open_interest_all": "open_interest",
    "noncomm_positions_long_all": "noncomm_long",
    "noncomm_positions_short_all": "noncomm_short",
    "comm_positions_long_all": "comm_long",
    "comm_positions_short_all": "comm_short",
    "nonrept_positions_long_all": "nonrept_long",
    "nonrept_positions_short_all": "nonrept_short",
}


class CftcCot(Collector):
    domain = "positioning"
    source = "cftc"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=1.0)

    def chunks(self) -> list[str]:
        start = int(settings.BACKFILL_START[:4])
        return [str(y) for y in range(start, date.today().year + 1)]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year = chunk
        path = hfstore.shard_path(self.domain, self.source, f"year={year}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"year": year, "skipped": True}

        self.rl.wait()
        params = {
            "$where": f"report_date_as_yyyy_mm_dd >= '{year}-01-01T00:00:00.000' "
                      f"and report_date_as_yyyy_mm_dd <= '{year}-12-31T23:59:59.000'",
            "$limit": 50000, "$order": "report_date_as_yyyy_mm_dd",
        }
        r = self.session.get(COT, params=params, timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            return {"year": year, "rows": 0, "empty": True}

        df = pd.DataFrame(rows)
        ev = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], utc=True, errors="coerce")
        payload = pd.DataFrame({v: pd.to_numeric(df.get(k), errors="coerce")
                                for k, v in COLS.items()})
        keep = ev.notna()
        df, ev, payload = df[keep], ev[keep], payload[keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(days=3)).values,
            entity=df["contract_market_name"].astype(str).values,
            source_url=COT, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"year": year, "rows": table.num_rows, "path": path}
