"""DeFi economic activity from DefiLlama (keyless): DEX volume, fees, revenue.

Daily aggregates observed on-chain (not revised) -> clean PIT: knowledge_time =
date + 1 day. entity = metric. One shard per metric.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

ENDPOINTS = {
    "dex_volume": ("https://api.llama.fi/overview/dexs", "dailyVolume"),
    "fees": ("https://api.llama.fi/overview/fees", "dailyFees"),
    "revenue": ("https://api.llama.fi/overview/fees", "dailyRevenue"),
}


class DefiLlamaFlows(Collector):
    domain = "crypto_defi"
    source = "defillama_flows"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=2.0)

    def chunks(self) -> list[str]:
        return list(ENDPOINTS)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        url, dtype = ENDPOINTS[chunk]
        path = hfstore.shard_path(self.domain, self.source, f"metric={chunk}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"metric": chunk, "skipped": True}

        self.rl.wait()
        r = self.session.get(url, params={"excludeTotalDataChart": "false",
                                          "excludeTotalDataChartBreakdown": "true",
                                          "dataType": dtype}, timeout=60)
        r.raise_for_status()
        chart = r.json().get("totalDataChart", [])
        rows = [(int(ts), float(v)) for ts, v in chart if ts is not None and v is not None]
        if not rows:
            return {"metric": chunk, "rows": 0, "empty": True}

        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["event_time"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        df = df[df["event_time"] >= pd.Timestamp(settings.BACKFILL_START, tz="UTC")]
        if df.empty:
            return {"metric": chunk, "rows": 0, "empty": True}
        payload = pd.DataFrame({"metric": chunk, "value_usd": df["value"].values})
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=df["event_time"].values,
            knowledge_time=(df["event_time"] + pd.Timedelta(days=1)).values,
            entity=chunk, source_url=url, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"metric": chunk, "rows": table.num_rows, "path": path}
