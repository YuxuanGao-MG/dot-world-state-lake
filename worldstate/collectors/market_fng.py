"""CNN Fear & Greed index for the US STOCK MARKET (keyless).

The popular equity sentiment gauge (0=extreme fear, 100=extreme greed) plus its
7 sub-components (momentum, breadth, put/call, VIX, safe-haven, junk-bond). API
serves ~1yr of daily history -> forward_limited (accrues forward). Published EOD,
knowable next day. entity = index/component. One shard.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
SERIES = {
    "fear_and_greed_historical": "FNG",
    "market_momentum_sp500": "momentum",
    "stock_price_strength": "price_strength",
    "stock_price_breadth": "price_breadth",
    "put_call_options": "put_call",
    "market_volatility_vix": "volatility",
    "safe_haven_demand": "safe_haven",
    "junk_bond_demand": "junk_bond",
}


class MarketFearGreed(Collector):
    domain = "sentiment"
    source = "cnn_fng"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=1.0)

    def chunks(self) -> list[str]:
        return ["all"]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, name="part.parquet")
        if not force and hfstore.exists(path):
            return {"skipped": True}

        self.rl.wait()
        r = self.session.get(URL, headers={"User-Agent": "Mozilla/5.0 " + settings.USER_AGENT},
                             timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        frames = []
        for key, label in SERIES.items():
            data = (j.get(key) or {}).get("data", [])
            if not data:
                continue
            df = pd.DataFrame(data)
            df["event_time"] = pd.to_datetime(df["x"], unit="ms", utc=True).dt.floor("D")
            frames.append(pd.DataFrame({
                "event_time": df["event_time"].values,
                "entity": label,
                "score": pd.to_numeric(df["y"], errors="coerce").values,
                "rating": df.get("rating", "").astype(str).values,
            }))
        if not frames:
            return {"rows": 0, "empty": True}
        allm = pd.concat(frames, ignore_index=True).drop_duplicates(["entity", "event_time"])
        allm["event_time"] = pd.to_datetime(allm["event_time"], utc=True)
        allm = allm[allm["event_time"] >= pd.Timestamp(settings.BACKFILL_START, tz="UTC")].reset_index(drop=True)
        if allm.empty:
            return {"rows": 0, "empty": True}
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=allm[["score", "rating"]],
            event_time=allm["event_time"],
            knowledge_time=allm["event_time"] + pd.Timedelta(days=1),
            entity=allm["entity"].values, source_url=URL, vintage_id="")
        hfstore.upload_table(table, path, overwrite=force)
        return {"rows": table.num_rows, "components": int(allm["entity"].nunique()), "path": path}
