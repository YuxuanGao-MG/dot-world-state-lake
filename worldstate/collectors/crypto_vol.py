"""Crypto implied-volatility index (DVOL) from Deribit (keyless).

DVOL is the crypto "VIX" — the market's forward vol expectation, a key options
signal for institutions. Daily index, not revised -> clean PIT. entity =
currency. One shard per currency.
"""
from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
CURRENCIES = ["BTC", "ETH"]


class CryptoVol(Collector):
    domain = "crypto_deriv"
    source = "deribit"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=2.0)

    def chunks(self) -> list[str]:
        return list(CURRENCIES)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        cur = chunk
        path = hfstore.shard_path(self.domain, self.source, f"currency={cur}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"currency": cur, "skipped": True}

        start_ms = int(pd.Timestamp(settings.BACKFILL_START, tz="UTC").timestamp() * 1000)
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.rl.wait()
        r = self.session.get(URL, params={"currency": cur, "start_timestamp": start_ms,
                                          "end_timestamp": end_ms, "resolution": "1D"},
                             timeout=60)
        r.raise_for_status()
        data = r.json().get("result", {}).get("data", [])
        if not data:
            return {"currency": cur, "rows": 0, "empty": True}

        df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close"])
        ev = pd.to_datetime(df["ts"].astype("int64"), unit="ms", utc=True)
        payload = pd.DataFrame({"dvol_open": pd.to_numeric(df["open"], errors="coerce"),
                                "dvol_high": pd.to_numeric(df["high"], errors="coerce"),
                                "dvol_low": pd.to_numeric(df["low"], errors="coerce"),
                                "dvol_close": pd.to_numeric(df["close"], errors="coerce")})
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(days=1)).values,
            entity=cur, source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"currency": cur, "rows": table.num_rows, "path": path}
