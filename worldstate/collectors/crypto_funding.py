"""Perpetual-swap funding rates from OKX (keyless, US-accessible).

Funding rate = the cost of holding a perp long/short; a core institutional
positioning/basis signal. Settled at fixed intervals and never revised -> clean
PIT: knowledge_time = event_time = funding settlement time. entity = instrument.
One shard per instrument.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://www.okx.com/api/v5/public/funding-rate-history"
INSTRUMENTS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP",
               "BTC-USD-SWAP", "ETH-USD-SWAP"]
MAX_PAGES = 80


class CryptoFunding(Collector):
    domain = "crypto_deriv"
    source = "okx"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=4.0)

    def chunks(self) -> list[str]:
        return list(INSTRUMENTS)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        inst = chunk
        path = hfstore.shard_path(self.domain, self.source, f"instrument={inst}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"instrument": inst, "skipped": True}

        rows, after = [], None
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        for _ in range(MAX_PAGES):
            self.rl.wait()
            params = {"instId": inst, "limit": 100}
            if after:
                params["after"] = after
            r = self.session.get(URL, params=params, timeout=settings.HTTP_TIMEOUT)
            if r.status_code != 200:
                break
            data = r.json().get("data", [])
            if not data:
                break
            rows.extend(data)
            after = data[-1]["fundingTime"]  # page older
            if pd.to_datetime(int(after), unit="ms", utc=True) < start:
                break
            if len(data) < 100:
                break

        if not rows:
            return {"instrument": inst, "rows": 0, "empty": True}
        df = pd.DataFrame(rows)
        ev = pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms", utc=True)
        keep = ev >= start
        payload = pd.DataFrame({
            "funding_rate": pd.to_numeric(df["fundingRate"], errors="coerce"),
            "realized_rate": pd.to_numeric(df.get("realizedRate"), errors="coerce"),
        })[keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev[keep].values, knowledge_time=ev[keep].values,
            entity=inst, source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"instrument": inst, "rows": table.num_rows, "path": path}
