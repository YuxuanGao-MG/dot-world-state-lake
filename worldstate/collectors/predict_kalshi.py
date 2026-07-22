"""Regulated event-contract probabilities from Kalshi (keyless public market data).

Kalshi's read API is public. For LIQUID finance/econ markets we pull each market's
daily candlesticks over its trading window; the traded price (`_dollars`, already
0-1) = implied probability. Immutable, forward-looking -> clean PIT.
knowledge_time = event_time = day. entity = market ticker. One shard per category.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

BASE = "https://api.elections.kalshi.com/trade-api/v2"
CATEGORIES = ["Financials", "Economics"]
KEYWORDS = ("FED", "CPI", "GDP", "RATE", "RECESSION", "NASDAQ", "SP500", "SPX",
            "INFLATION", "JOBS", "PAYROLL", "UNRATE", "BTC", "ETH", "YIELD",
            "TREASURY", "STOCK", "DOW", "PPI")
MAX_SERIES = 60
MAX_MARKETS = 8


def _ts(iso):
    t = pd.to_datetime(iso, utc=True, errors="coerce")
    return None if pd.isna(t) else int(t.timestamp())


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _candle_prob(c):
    """Implied probability (0-1) from a Kalshi candle. Fields are _dollars strings."""
    p = c.get("price") or {}
    for k in ("close_dollars", "mean_dollars", "previous_dollars"):
        v = _f(p.get(k))
        if v is not None:
            return v
    ya = _f((c.get("yes_ask") or {}).get("close_dollars"))
    yb = _f((c.get("yes_bid") or {}).get("close_dollars"))
    if ya and yb:
        return (ya + yb) / 2
    return ya or yb


class PredictKalshi(Collector):
    domain = "predictions"
    source = "kalshi"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=6.0)

    def chunks(self) -> list[str]:
        return list(CATEGORIES)

    def _series(self, category: str) -> list[str]:
        self.rl.wait()
        r = self.session.get(f"{BASE}/series", timeout=60)
        r.raise_for_status()
        out = [s["ticker"] for s in r.json().get("series", [])
               if s.get("category") == category and s.get("ticker")
               and any(k in s["ticker"].upper() for k in KEYWORDS)]
        return out[:MAX_SERIES]

    def _markets(self, series: str) -> list[dict]:
        out = []
        for status in ("settled", "closed", "open"):
            self.rl.wait()
            r = self.session.get(f"{BASE}/markets", params={
                "series_ticker": series, "status": status, "limit": 100}, timeout=30)
            if r.status_code == 200:
                out += [m for m in r.json().get("markets", [])
                        if (_f(m.get("volume_fp")) or 0) > 0]
        out.sort(key=lambda m: -(_f(m.get("volume_fp")) or 0))
        return out[:MAX_MARKETS]

    def _candles(self, series: str, m: dict) -> pd.DataFrame:
        start = _ts(m.get("open_time") or m.get("created_time"))
        end = _ts(m.get("close_time") or m.get("expiration_time")) or int(
            pd.Timestamp.utcnow().timestamp())
        if not start:
            return pd.DataFrame()
        self.rl.wait()
        r = self.session.get(
            f"{BASE}/series/{series}/markets/{m['ticker']}/candlesticks",
            params={"start_ts": start, "end_ts": end, "period_interval": 1440}, timeout=30)
        if r.status_code != 200:
            return pd.DataFrame()
        rows = []
        for c in r.json().get("candlesticks", []):
            p = _candle_prob(c)
            ts = c.get("end_period_ts")
            if p is None or ts is None:
                continue
            rows.append((int(ts), p))
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts", "prob"])
        df["day"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.floor("D")
        return df.groupby("day", as_index=False)["prob"].last()

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        category = chunk
        path = hfstore.shard_path(self.domain, self.source, f"category={category}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"category": category, "skipped": True}

        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        frames, n = [], 0
        for series in self._series(category):
            for m in self._markets(series):
                daily = self._candles(series, m)
                if daily.empty:
                    continue
                daily = daily[daily["day"] >= start]
                if daily.empty:
                    continue
                n += 1
                frames.append(pd.DataFrame({
                    "event_time": daily["day"].values,
                    "entity": str(m["ticker"]),
                    "title": str(m.get("title", ""))[:300],
                    "probability": daily["prob"].astype("float64").values,
                    "series": series,
                }))
        if not frames:
            return {"category": category, "rows": 0, "empty": True}

        allm = pd.concat(frames, ignore_index=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source,
            payload=allm[["title", "probability", "series"]],
            event_time=allm["event_time"], knowledge_time=allm["event_time"],
            entity=allm["entity"].values, source_url=BASE, vintage_id="")
        hfstore.upload_table(table, path, overwrite=force)
        return {"category": category, "markets": n, "rows": table.num_rows, "path": path}
