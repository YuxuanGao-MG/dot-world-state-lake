"""FINRA daily consolidated short-sale volume (keyless CDN flat files).

One file per trading day lists short/total volume per symbol across FINRA venues.
Published after market close, so knowledge_time = date + 1 day. entity = symbol.
One shard per month (a job fetches that month's daily files and concatenates).
"""
from __future__ import annotations

import io
import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

FILE = "http://cdn.finra.org/equity/regsho/daily/CNMSshvol{ymd}.txt"


class FinraShort(Collector):
    domain = "positioning"
    source = "finra_short"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=6.0)

    def chunks(self) -> list[str]:
        y0, m0 = int(settings.BACKFILL_START[:4]), int(settings.BACKFILL_START[5:7])
        today = date.today()
        out = []
        y, m = y0, m0
        while (y, m) <= (today.year, today.month):
            out.append(f"{y}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year, month = int(chunk[:4]), int(chunk[5:7])
        path = hfstore.shard_path(self.domain, self.source, f"year={year}",
                                  f"month={month:02d}", name="part.parquet")
        if not force and hfstore.exists(path):
            return {"month": chunk, "skipped": True}

        frames = []
        month_end = (pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1))
        for ts in pd.date_range(start=pd.Timestamp(year=year, month=month, day=1),
                                end=month_end, freq="B"):  # business days only
            self.rl.wait()
            try:
                r = self.session.get(FILE.format(ymd=ts.strftime("%Y%m%d")),
                                     timeout=settings.HTTP_TIMEOUT)
                if r.status_code == 200 and r.text.startswith("Date|"):
                    df = pd.read_csv(io.StringIO(r.text), sep="|")
                    frames.append(df[df["Symbol"].notna()])
            except Exception:
                pass
        if not frames:
            return {"month": chunk, "rows": 0, "empty": True}

        alld = pd.concat(frames, ignore_index=True)
        ev = pd.to_datetime(alld["Date"].astype(str), format="%Y%m%d", utc=True)
        payload = pd.DataFrame({
            "short_volume": pd.to_numeric(alld["ShortVolume"], errors="coerce"),
            "short_exempt_volume": pd.to_numeric(alld["ShortExemptVolume"], errors="coerce"),
            "total_volume": pd.to_numeric(alld["TotalVolume"], errors="coerce"),
        })
        payload["short_ratio"] = payload["short_volume"] / payload["total_volume"].replace(0, pd.NA)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(days=1)).values,
            entity=alld["Symbol"].astype(str).values,
            source_url="http://cdn.finra.org/equity/regsho/daily/", vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"month": chunk, "rows": table.num_rows, "path": path}
