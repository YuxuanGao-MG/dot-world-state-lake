"""Crypto Fear & Greed index from alternative.me (keyless).

Daily market-sentiment gauge (0=extreme fear, 100=extreme greed). Published
daily, knowable next day -> clean PIT. entity = "crypto_market". One shard.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://api.alternative.me/fng/"


class CryptoFearGreed(Collector):
    domain = "sentiment"
    source = "alt_fng"

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
        r = self.session.get(URL, params={"limit": 0, "format": "json"},
                             timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            return {"rows": 0, "empty": True}

        df = pd.DataFrame(data)
        ev = pd.to_datetime(df["timestamp"].astype("int64"), unit="s", utc=True)
        keep = ev >= pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        payload = pd.DataFrame({
            "fng_value": pd.to_numeric(df["value"], errors="coerce"),
            "classification": df["value_classification"].astype(str),
        })[keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev[keep].values, knowledge_time=(ev[keep] + pd.Timedelta(days=1)).values,
            entity="crypto_market", source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"rows": table.num_rows, "path": path}
