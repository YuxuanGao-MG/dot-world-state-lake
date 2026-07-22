"""Bitcoin on-chain network metrics (Blockchain.com charts API, keyless).

A new "real-activity" dimension for crypto: addresses, transactions, fees, hash
rate, supply, mempool. Daily series; a day is knowable the next day, so
knowledge_time = date + 1 day. entity = "BTC", one shard per metric.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

CHART = "https://api.blockchain.info/charts/{metric}"
METRICS = [
    "n-unique-addresses", "n-transactions", "estimated-transaction-volume-usd",
    "transaction-fees-usd", "hash-rate", "difficulty", "miners-revenue",
    "mempool-count", "avg-block-size", "total-bitcoins", "market-cap",
    "n-transactions-per-block",
]


class CryptoOnchain(Collector):
    domain = "onchain"
    source = "blockchain"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=1.0)

    def chunks(self) -> list[str]:
        return list(METRICS)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        metric = chunk
        path = hfstore.shard_path(self.domain, self.source, f"metric={metric}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"metric": metric, "skipped": True}

        self.rl.wait()
        r = self.session.get(CHART.format(metric=metric),
                             params={"timespan": "all", "format": "json", "sampled": "false"},
                             timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        vals = r.json().get("values", [])
        if not vals:
            return {"metric": metric, "rows": 0, "empty": True}

        df = pd.DataFrame(vals)
        df["event_time"] = pd.to_datetime(df["x"], unit="s", utc=True)
        df = df[df["event_time"] >= pd.Timestamp(settings.BACKFILL_START, tz="UTC")]
        if df.empty:
            return {"metric": metric, "rows": 0, "empty": True}
        payload = pd.DataFrame({"metric": metric,
                                "value": pd.to_numeric(df["y"], errors="coerce")}).reset_index(drop=True)
        ev = df["event_time"].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(days=1)).values,
            entity="BTC", source_url=CHART.format(metric=metric), vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"metric": metric, "rows": table.num_rows, "path": path}
