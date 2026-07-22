"""Real-money prediction-market probabilities from Polymarket (keyless).

Two-step: Gamma lists high-volume markets (with CLOB token ids); the CLOB
prices-history endpoint gives each token's daily price = implied probability.
Immutable, forward-looking -> clean PIT. knowledge_time = event_time = day.
entity = market slug. One shard per page of top-volume markets.
"""
from __future__ import annotations

import json
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

GAMMA = "https://gamma-api.polymarket.com/markets"
HIST = "https://clob.polymarket.com/prices-history"
PAGE = 100
PAGES = 4
MIN_VOL = 50000


class PredictPolymarket(Collector):
    domain = "predictions"
    source = "polymarket"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=3.0)

    def chunks(self) -> list[str]:
        return [str(i) for i in range(PAGES)]

    def _history(self, token: str) -> pd.DataFrame:
        self.rl.wait()
        r = self.session.get(HIST, params={"market": token, "interval": "max",
                                           "fidelity": "1440"}, timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200:
            return pd.DataFrame()
        h = r.json().get("history", [])
        if not h:
            return pd.DataFrame()
        df = pd.DataFrame(h)
        df["day"] = pd.to_datetime(df["t"], unit="s", utc=True).dt.floor("D")
        return df.groupby("day", as_index=False)["p"].last()

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        page = int(chunk)
        path = hfstore.shard_path(self.domain, self.source, f"page={chunk}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"page": chunk, "skipped": True}

        self.rl.wait()
        r = self.session.get(GAMMA, params={"limit": PAGE, "offset": page * PAGE,
                                            "volume_num_min": MIN_VOL, "order": "volumeNum",
                                            "ascending": "false"},
                             timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        markets = r.json()
        if not markets:
            return {"page": chunk, "rows": 0, "empty": True}

        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        frames, n = [], 0
        for m in markets:
            toks = m.get("clobTokenIds")
            if isinstance(toks, str):
                try:
                    toks = json.loads(toks)
                except Exception:
                    toks = None
            outs = m.get("outcomes")
            if isinstance(outs, str):
                try:
                    outs = json.loads(outs)
                except Exception:
                    outs = ["Yes", "No"]
            if not toks:
                continue
            hist = self._history(toks[0])
            hist = hist[hist["day"] >= start] if not hist.empty else hist
            if hist.empty:
                continue
            n += 1
            frames.append(pd.DataFrame({
                "event_time": hist["day"].values,
                "entity": str(m.get("slug", m.get("id", ""))),
                "question": str(m.get("question", ""))[:300],
                "probability": hist["p"].astype("float64").values,
                "outcome": (outs or ["Yes"])[0],
                "volume": float(m.get("volume", 0) or 0),
            }))
        if not frames:
            return {"page": chunk, "rows": 0, "empty": True}

        allm = pd.concat(frames, ignore_index=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source,
            payload=allm[["question", "probability", "outcome", "volume"]],
            event_time=allm["event_time"], knowledge_time=allm["event_time"],
            entity=allm["entity"].values, source_url=GAMMA, vintage_id="")
        hfstore.upload_table(table, path, overwrite=force)
        return {"page": chunk, "markets": n, "rows": table.num_rows, "path": path}
