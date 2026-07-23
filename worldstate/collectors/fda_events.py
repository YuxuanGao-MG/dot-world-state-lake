"""Drug recall / enforcement events from openFDA (keyless).

Biotech/pharma catalysts. Dated -> clean PIT: event_time = knowledge_time =
report date. entity = recalling firm. One shard per year.
"""
from __future__ import annotations

import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://api.fda.gov/drug/enforcement.json"


class FdaEvents(Collector):
    domain = "events"
    source = "openfda"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=3.0)

    def chunks(self) -> list[str]:
        return [str(y) for y in range(int(settings.BACKFILL_START[:4]), date.today().year + 1)]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year = chunk
        path = hfstore.shard_path(self.domain, self.source, f"year={year}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"year": year, "skipped": True}

        rows, skip = [], 0
        while skip < 25000:
            self.rl.wait()
            params = {"search": f"report_date:[{year}0101 TO {year}1231]",
                      "limit": 1000, "skip": skip}
            try:
                r = self.session.get(URL, params=params, timeout=60)
            except Exception:
                break
            if r.status_code != 200:
                break
            res = r.json().get("results", [])
            if not res:
                break
            rows += res
            if len(res) < 1000:
                break
            skip += 1000

        if not rows:
            return {"year": year, "rows": 0, "empty": True}
        df = pd.DataFrame(rows)
        ev = pd.to_datetime(df["report_date"], format="%Y%m%d", utc=True, errors="coerce")
        keep = ev.notna()
        payload = pd.DataFrame({
            "classification": df.get("classification", "").astype(str),
            "recalling_firm": df.get("recalling_firm", "").astype(str),
            "product_description": df.get("product_description", "").astype(str).str[:500],
            "reason": df.get("reason_for_recall", "").astype(str).str[:500],
            "status": df.get("status", "").astype(str),
        })[keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev[keep].values, knowledge_time=ev[keep].values,
            entity=payload["recalling_firm"].values, source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"year": year, "rows": table.num_rows, "path": path}
