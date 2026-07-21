"""Daily crypto OHLCV via yfinance (keyless, US-accessible, reliable).

Reuses the proven Yahoo path (CoinGecko now requires a key; Binance 451s from US
IPs). Crypto trades 24/7, so day D is knowable at the next UTC midnight:
event_time = D, knowledge_time = D + 1 day. entity = symbol (e.g. BTC).
"""
from __future__ import annotations

import pandas as pd
import yfinance as yf

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector

FIELDS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


class CryptoYahoo(Collector):
    domain = "crypto"
    source = "yahoo"

    def chunks(self) -> list[str]:
        return list(settings.CRYPTO_IDS.values())  # BTC, ETH, ...

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        sym = chunk
        ticker = f"{sym}-USD"
        path = hfstore.shard_path(self.domain, self.source, f"asset={sym}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"asset": sym, "skipped": True}

        data = yf.download(ticker, start=settings.BACKFILL_START, auto_adjust=False,
                           progress=False, threads=False)
        if data is None or data.empty:
            return {"asset": sym, "rows": 0, "empty": True}
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        sub = data[[c for c in FIELDS if c in data.columns]].dropna(how="all")
        if sub.empty:
            return {"asset": sym, "rows": 0, "empty": True}

        ev = pd.to_datetime(sub.index, utc=True)
        payload = pd.DataFrame({
            "open": sub["Open"].astype("float64").values,
            "high": sub["High"].astype("float64").values,
            "low": sub["Low"].astype("float64").values,
            "close": sub["Close"].astype("float64").values,
            "adj_close": sub["Adj Close"].astype("float64").values,
            "volume": sub["Volume"].astype("float64").values,
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(days=1)).values,
            entity=sym, source_url=f"https://finance.yahoo.com/quote/{ticker}/history",
            vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"asset": sym, "rows": table.num_rows, "path": path}
