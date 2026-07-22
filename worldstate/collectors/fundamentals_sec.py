"""As-reported company fundamentals from SEC XBRL companyfacts (keyless).

Genuinely point-in-time: every datapoint carries the SEC `filed` date, so
knowledge_time = filed and restatements appear as new rows (vintage_id = accn),
never overwriting the original print. event_time = period end. entity = ticker.

Batched by universe slice (matrix stays <=256 jobs); each job walks its tickers
sequentially and uploads one shard per ticker.
"""
from __future__ import annotations

import os
import pandas as pd

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
BATCH = 40


def _universe() -> list[str]:
    path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "universe_us.txt")
    if os.path.exists(path):
        with open(path) as f:
            u = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]
        if u:
            return u
    return settings.SEED_UNIVERSE


class SecFundamentals(Collector):
    domain = "fundamentals"
    source = "sec_xbrl"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=settings.SEC_RATE_LIMIT_HZ)
        self.uni = _universe()
        self.cik = self._ticker_cik_map()

    def _ticker_cik_map(self) -> dict:
        try:
            r = self.session.get(TICKERS_URL, timeout=settings.HTTP_TIMEOUT)
            r.raise_for_status()
            return {str(v["ticker"]).upper(): int(v["cik_str"]) for v in r.json().values()}
        except Exception:
            return {}

    def chunks(self) -> list[str]:
        n = (len(self.uni) + BATCH - 1) // BATCH
        return [f"{i:04d}" for i in range(n)]

    def _one_ticker(self, ticker: str, force: bool) -> dict:
        cik = self.cik.get(ticker)
        if not cik:
            return {"ticker": ticker, "no_cik": True}
        path = hfstore.shard_path(self.domain, self.source, f"ticker={ticker}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"ticker": ticker, "skipped": True}

        self.rl.wait()
        r = self.session.get(FACTS_URL.format(cik=cik), timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200:
            return {"ticker": ticker, "status": r.status_code}
        facts = r.json().get("facts", {}).get("us-gaap", {})
        recs = []
        for concept, body in facts.items():
            for unit, points in body.get("units", {}).items():
                for p in points:
                    if p.get("val") is None or not p.get("end") or not p.get("filed"):
                        continue
                    recs.append((concept, unit, p["end"], p["filed"], float(p["val"]),
                                 p.get("form", ""), p.get("fy"), p.get("fp", ""),
                                 p.get("accn", ""), p.get("start", "")))
        if not recs:
            return {"ticker": ticker, "rows": 0, "empty": True}
        df = pd.DataFrame(recs, columns=["concept", "unit", "end", "filed", "val",
                                         "form", "fy", "fp", "accn", "start"])
        payload = pd.DataFrame({
            "concept": df["concept"], "unit": df["unit"], "value": df["val"],
            "form": df["form"], "fiscal_year": df["fy"].astype("Int64").astype(str),
            "fiscal_period": df["fp"], "period_start": df["start"],
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=pd.to_datetime(df["end"], utc=True),
            knowledge_time=pd.to_datetime(df["filed"], utc=True),
            entity=ticker,
            source_url=FACTS_URL.format(cik=cik),
            vintage_id=df["accn"].values,
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"ticker": ticker, "rows": table.num_rows}

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        idx = int(chunk)
        tickers = self.uni[idx * BATCH:(idx + 1) * BATCH]
        done, total = 0, 0
        for t in tickers:
            try:
                res = self._one_ticker(t, force)
                total += res.get("rows", 0)
                done += 1
            except Exception:
                continue
        return {"chunk": chunk, "tickers": len(tickers), "processed": done, "rows": total}
