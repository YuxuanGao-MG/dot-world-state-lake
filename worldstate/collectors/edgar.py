"""SEC EDGAR filing event stream (keyless). One shard per (year, quarter).

Every SEC filing is a world-state event. The quarterly master index lists all
filings compactly: CIK|Company|Form|Date Filed|Filename. Small volume, high
value, PIT-perfect: a filing is public on its filing date.
  event_time = knowledge_time = filing date (UTC)   entity = CIK
Full-text extraction is a later, heavier layer; this indexes what/when/who.
"""
from __future__ import annotations

import io
import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

MASTER = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/master.idx"


class EdgarIndex(Collector):
    domain = "events"
    source = "edgar"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=settings.SEC_RATE_LIMIT_HZ)

    def chunks(self) -> list[str]:
        start_year = int(settings.BACKFILL_START[:4])
        today = date.today()
        out = []
        for y in range(start_year, today.year + 1):
            for q in range(1, 5):
                if date(y, (q - 1) * 3 + 1, 1) <= today:
                    out.append(f"{y}Q{q}")
        return out

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year, q = int(chunk[:4]), int(chunk[5])
        path = hfstore.shard_path(self.domain, self.source,
                                  f"year={year}", f"quarter={q}", name="part.parquet")
        if not force and hfstore.exists(path):
            return {"chunk": chunk, "skipped": True}

        self.rl.wait()
        url = MASTER.format(year=year, q=q)
        r = self.session.get(url, timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200:
            return {"chunk": chunk, "rows": 0, "status": r.status_code}
        # master.idx: header lines then CIK|Company|Form Type|Date Filed|Filename
        text = r.text.splitlines()
        data = [ln for ln in text if ln.count("|") == 4 and not ln.startswith("CIK")]
        df = pd.read_csv(io.StringIO("\n".join(data)), sep="|",
                         names=["cik", "company", "form", "date_filed", "filename"])
        df = df[pd.to_datetime(df["date_filed"], errors="coerce").notna()]
        if df.empty:
            return {"chunk": chunk, "rows": 0, "empty": True}
        ev = pd.to_datetime(df["date_filed"], utc=True)
        payload = pd.DataFrame({
            "company": df["company"].astype(str),
            "form": df["form"].astype(str),
            "filename": df["filename"].astype(str),
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=ev.values,
            entity=df["cik"].astype(str).values,
            source_url=("https://www.sec.gov/Archives/" + df["filename"].astype(str)).values,
            vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"chunk": chunk, "rows": table.num_rows, "path": path}
