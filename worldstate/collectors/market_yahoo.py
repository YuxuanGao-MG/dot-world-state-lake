"""US daily OHLCV from Yahoo Finance via yfinance (keyless, cloud-tolerant).

Replaces Stooq, which now gates every request behind a JS proof-of-work anti-bot
challenge (unusable from HTTP/CI). yfinance handles Yahoo's crumb/cookie and
batches a whole universe slice in one call.

PIT stamping: a daily bar for date D is knowable after the US close, so
event_time = D 00:00 UTC and knowledge_time = D 21:00 UTC (~US close). Prices are
not revised (we keep Adj Close alongside raw Close), so vintage_id is empty.
"""
from __future__ import annotations

import os
import pandas as pd
import yfinance as yf

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector

BATCH = 40
FIELDS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


def _universe() -> list[str]:
    path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "universe_us.txt")
    if os.path.exists(path):
        with open(path) as f:
            u = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]
        if u:
            return u
    return settings.SEED_UNIVERSE


class YahooDaily(Collector):
    domain = "market"
    source = "yahoo"

    def __init__(self):
        super().__init__()
        self.uni = _universe()

    def chunks(self) -> list[str]:
        n = (len(self.uni) + BATCH - 1) // BATCH
        return [f"{i:04d}" for i in range(n)]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        idx = int(chunk)
        tickers = self.uni[idx * BATCH:(idx + 1) * BATCH]
        path = hfstore.shard_path(self.domain, self.source, f"batch={chunk}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"chunk": chunk, "skipped": True, "tickers": len(tickers)}

        data = yf.download(
            tickers, start=settings.BACKFILL_START, auto_adjust=False,
            group_by="ticker", progress=False, threads=True,
        )
        if data is None or data.empty:
            return {"chunk": chunk, "rows": 0, "tickers": len(tickers), "empty": True}

        frames = []
        for t in tickers:
            try:
                sub = data if len(tickers) == 1 else data[t]
            except Exception:
                continue
            sub = sub[[c for c in FIELDS if c in sub.columns]].dropna(how="all")
            if sub.empty:
                continue
            ev = pd.to_datetime(sub.index, utc=True)
            payload = pd.DataFrame({
                "open": sub["Open"].astype("float64").values,
                "high": sub["High"].astype("float64").values,
                "low": sub["Low"].astype("float64").values,
                "close": sub["Close"].astype("float64").values,
                "adj_close": sub["Adj Close"].astype("float64").values,
                "volume": sub["Volume"].astype("float64").values,
            })
            tbl = normalize.to_table(
                domain=self.domain, source=self.source, payload=payload,
                event_time=ev.values,
                knowledge_time=(ev + pd.Timedelta(hours=21)).values,
                entity=t,
                source_url=f"https://finance.yahoo.com/quote/{t}/history",
                vintage_id="",
            )
            frames.append(tbl)

        if not frames:
            return {"chunk": chunk, "rows": 0, "tickers": len(tickers), "empty": True}
        import pyarrow as pa
        table = pa.concat_tables(frames, promote_options="default")
        hfstore.upload_table(table, path, overwrite=force)
        return {"chunk": chunk, "rows": table.num_rows, "tickers": len(tickers), "path": path}
