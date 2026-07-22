"""DeFi TVL + stablecoin supply from DefiLlama (keyless).

On-chain, observed (not revised) daily snapshots -> clean PIT. A day's value is
knowable the next day, so knowledge_time = date + 1 day. entity = chain/metric.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

CHAINS = ["Ethereum", "Solana", "Tron", "BSC", "Arbitrum", "Base", "Polygon"]


class DefiLlama(Collector):
    domain = "crypto_defi"
    source = "defillama"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=2.0)

    def chunks(self) -> list[str]:
        return ["total"] + CHAINS + ["stablecoins"]

    def _fetch(self, chunk: str):
        if chunk == "total":
            url = "https://api.llama.fi/v2/historicalChainTvl"
        elif chunk == "stablecoins":
            url = "https://stablecoins.llama.fi/stablecoincharts/all"
        else:
            url = f"https://api.llama.fi/v2/historicalChainTvl/{chunk}"
        self.rl.wait()
        r = self.session.get(url, timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, f"series={chunk}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"series": chunk, "skipped": True}

        data = self._fetch(chunk)
        rows = []
        for d in data:
            ts = d.get("date")
            if chunk == "stablecoins":
                val = (d.get("totalCirculatingUSD") or {})
                val = val.get("peggedUSD") if isinstance(val, dict) else val
            else:
                val = d.get("tvl")
            if ts is None or val is None:
                continue
            rows.append((int(ts), float(val)))
        if not rows:
            return {"series": chunk, "rows": 0, "empty": True}

        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["event_time"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        df = df[df["event_time"] >= pd.Timestamp(settings.BACKFILL_START, tz="UTC")]
        if df.empty:
            return {"series": chunk, "rows": 0, "empty": True}
        payload = pd.DataFrame({"metric": "tvl_usd" if chunk != "stablecoins" else "stablecoin_mcap_usd",
                                "value": df["value"].values})
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=df["event_time"].values,
            knowledge_time=(df["event_time"] + pd.Timedelta(days=1)).values,
            entity=chunk, source_url="https://defillama.com", vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"series": chunk, "rows": table.num_rows, "path": path}
