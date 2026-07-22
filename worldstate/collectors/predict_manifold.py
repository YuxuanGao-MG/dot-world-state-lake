"""Prediction-market probabilities from Manifold (keyless).

The purest PIT signal in the lake: each bet stamps the market's implied
probability at an instant, and it's never revised. We reconstruct a daily
probability trajectory per market -> "what the crowd believed about a future
event, as-of each day". knowledge_time = event_time = that day. entity = market.
One shard per topic.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

SEARCH = "https://api.manifold.markets/v0/search-markets"
BETS = "https://api.manifold.markets/v0/bets"
TOPICS = ["recession", "inflation", "federal reserve", "interest rates",
          "stock market", "bitcoin", "ethereum", "election", "gdp",
          "unemployment", "artificial intelligence", "oil price"]
MARKETS_PER_TOPIC = 20


class PredictManifold(Collector):
    domain = "predictions"
    source = "manifold"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=4.0)

    def chunks(self) -> list[str]:
        return [t.replace(" ", "_") for t in TOPICS]

    def _bets_daily(self, market_id: str) -> pd.DataFrame:
        """Up to ~2000 bets -> daily last implied probability."""
        allb, before = [], None
        for _ in range(2):
            self.rl.wait()
            params = {"contractId": market_id, "limit": 1000}
            if before:
                params["before"] = before
            r = self.session.get(BETS, params=params, timeout=settings.HTTP_TIMEOUT)
            if r.status_code != 200:
                break
            b = r.json()
            if not b:
                break
            allb.extend(b)
            before = b[-1]["id"]
            if len(b) < 1000:
                break
        rows = [(x.get("createdTime"), x.get("probAfter")) for x in allb
                if x.get("probAfter") is not None and x.get("createdTime")]
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts", "prob"])
        df["day"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.floor("D")
        df = df.sort_values("ts").groupby("day", as_index=False).last()
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        return df[df["day"] >= start]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        topic = chunk.replace("_", " ")
        path = hfstore.shard_path(self.domain, self.source, f"topic={chunk}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"topic": chunk, "skipped": True}

        self.rl.wait()
        r = self.session.get(SEARCH, params={"term": topic, "limit": MARKETS_PER_TOPIC,
                                             "sort": "most-popular", "contractType": "BINARY"},
                             timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        markets = [m for m in r.json() if m.get("outcomeType") == "BINARY"]

        frames, n_markets = [], 0
        for m in markets:
            daily = self._bets_daily(m["id"])
            if daily.empty:
                continue
            n_markets += 1
            frames.append(pd.DataFrame({
                "event_time": daily["day"].values,
                "entity": str(m.get("slug", m["id"])),
                "question": str(m.get("question", ""))[:300],
                "probability": daily["prob"].astype("float64").values,
                "volume": float(m.get("volume", 0) or 0),
                "url": m.get("url", SEARCH),
            }))
        if not frames:
            return {"topic": chunk, "rows": 0, "empty": True}

        allm = pd.concat(frames, ignore_index=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source,
            payload=allm[["question", "probability", "volume"]],
            event_time=allm["event_time"], knowledge_time=allm["event_time"],
            entity=allm["entity"].values, source_url=allm["url"].iloc[0], vintage_id="")
        hfstore.upload_table(table, path, overwrite=force)
        return {"topic": chunk, "markets": n_markets, "rows": table.num_rows, "path": path}
