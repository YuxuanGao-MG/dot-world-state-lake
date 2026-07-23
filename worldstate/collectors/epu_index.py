"""US Economic Policy Uncertainty index (policyuncertainty.com, keyless).

Monthly index of policy-related uncertainty (a geopolitical/policy-risk gauge).
Published with ~1-month lag -> knowledge_time = month + 1 month. entity = "US".
"""
from __future__ import annotations

import io
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://www.policyuncertainty.com/media/US_Policy_Uncertainty_Data.csv"


class EpuIndex(Collector):
    domain = "sentiment"
    source = "epu"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=1.0)

    def chunks(self) -> list[str]:
        return ["us"]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, "region=us",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"skipped": True}

        self.rl.wait()
        r = self.session.get(URL, timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        df = df[pd.to_numeric(df.get("Year"), errors="coerce").notna()]
        df = df[pd.to_numeric(df.get("Month"), errors="coerce").notna()]
        if df.empty:
            return {"rows": 0, "empty": True}
        ev = pd.to_datetime(dict(year=df["Year"].astype(int), month=df["Month"].astype(int),
                                 day=1), utc=True)
        keep = ev >= pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        three = [c for c in df.columns if "Three_Component" in c]
        news = [c for c in df.columns if "News_Based" in c]
        payload = pd.DataFrame({
            "epu_3component": pd.to_numeric(df[three[0]], errors="coerce") if three else pd.NA,
            "epu_news_based": pd.to_numeric(df[news[0]], errors="coerce") if news else pd.NA,
        })[keep].reset_index(drop=True)
        ev = ev[keep]
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.DateOffset(months=1)).values,
            entity="US", source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"rows": table.num_rows, "path": path}
