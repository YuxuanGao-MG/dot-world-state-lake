"""US daily OHLCV from Stooq (keyless). One shard per universe batch.

PIT stamping: a daily bar for date D is knowable after the US close, so
event_time = D 00:00 UTC (the bar's day) and knowledge_time = D 21:00 UTC
(~US market close). Prices are not revised, so vintage_id is empty.
"""
from __future__ import annotations

import io
import os
import pandas as pd

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

BATCH = 40


def _universe() -> list[str]:
    path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "universe_us.txt")
    if os.path.exists(path):
        with open(path) as f:
            u = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]
        if u:
            return u
    return settings.SEED_UNIVERSE


def _stooq_symbol(t: str) -> str:
    return t.lower() + ".us"


class StooqDaily(Collector):
    domain = "market"
    source = "stooq"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=3.0)
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

        frames = []
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        for t in tickers:
            self.rl.wait()
            url = f"https://stooq.com/q/d/l/?s={_stooq_symbol(t)}&i=d"
            try:
                r = self.session.get(url, timeout=settings.HTTP_TIMEOUT)
                if r.status_code != 200 or r.text.startswith("<") or "No data" in r.text[:32]:
                    continue
                df = pd.read_csv(io.StringIO(r.text))
                if df.empty or "Close" not in df.columns:
                    continue
                df = df.rename(columns=str.lower)
                df["event_time"] = pd.to_datetime(df["date"], utc=True)
                df = df[df["event_time"] >= start]
                if df.empty:
                    continue
                payload = df[["open", "high", "low", "close", "volume"]].astype(
                    {"open": "float64", "high": "float64", "low": "float64",
                     "close": "float64", "volume": "float64"})
                tbl = normalize.to_table(
                    domain=self.domain, source=self.source, payload=payload,
                    event_time=df["event_time"].values,
                    knowledge_time=(df["event_time"] + pd.Timedelta(hours=21)).values,
                    entity=t, source_url=url, vintage_id="",
                )
                frames.append(tbl)
            except Exception:
                continue

        if not frames:
            return {"chunk": chunk, "rows": 0, "tickers": len(tickers), "empty": True}

        import pyarrow as pa
        table = pa.concat_tables(frames, promote_options="default")
        hfstore.upload_table(table, path, overwrite=force)
        return {"chunk": chunk, "rows": table.num_rows, "tickers": len(tickers), "path": path}
