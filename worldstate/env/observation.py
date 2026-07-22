"""Builds the point-in-time observation an agent sees at the current cursor.

Every channel is filtered by knowledge_time <= cursor, so the observation is
exactly the slice of the world knowable at that instant. Returns both a
structured dict and a natural-language rendering (LLM-agent friendly). Channels
degrade gracefully — a source not yet in the lake just yields an empty section.
"""
from __future__ import annotations

import pandas as pd
from worldstate import query


class ObservationBuilder:
    def __init__(self, con=None, watchlist=None):
        self.con = con or query.connect()
        self.watchlist = watchlist or ["SPY", "QQQ", "AAPL", "NVDA", "MSFT",
                                       "TLT", "GLD", "BTC"]

    def _df(self, sql: str) -> pd.DataFrame:
        try:
            return self.con.execute(sql).df()
        except Exception:
            return pd.DataFrame()

    def _prices(self, asof: str) -> pd.DataFrame:
        wl = ",".join(f"'{t}'" for t in self.watchlist)
        g = query._glob("market", "yahoo")
        return self._df(f"""
            WITH v AS (SELECT entity, event_time, close,
                       row_number() OVER (PARTITION BY entity ORDER BY event_time DESC) rn
                       FROM read_parquet('{g}', hive_partitioning=1)
                       WHERE knowledge_time <= TIMESTAMP '{asof}' AND entity IN ({wl}))
            SELECT entity, event_time, close FROM v WHERE rn=1 ORDER BY entity""")

    def _macro(self, asof: str) -> pd.DataFrame:
        g = query._glob("macro", "alfred")
        return self._df(f"""
            WITH pit AS (
              SELECT entity, event_time, value, knowledge_time,
                     row_number() OVER (PARTITION BY entity ORDER BY knowledge_time DESC, event_time DESC) rn
              FROM read_parquet('{g}', hive_partitioning=1)
              WHERE knowledge_time <= TIMESTAMP '{asof}')
            SELECT entity, event_time, value FROM pit WHERE rn=1
            AND entity IN ('CPIAUCSL','UNRATE','FEDFUNDS','DGS10','GDPC1','VIXCLS')
            ORDER BY entity""")

    def _news(self, asof: str) -> pd.DataFrame:
        g = query._glob("news", "hackernews")
        return self._df(f"""
            SELECT title, entity AS topic, points, event_time
            FROM read_parquet('{g}', hive_partitioning=1)
            WHERE knowledge_time <= TIMESTAMP '{asof}'
              AND knowledge_time > TIMESTAMP '{asof}' - INTERVAL 7 DAY
            ORDER BY points DESC LIMIT 8""")

    def _filings(self, asof: str) -> pd.DataFrame:
        g = query._glob("events", "edgar")
        return self._df(f"""
            SELECT entity AS cik, form, company, event_time
            FROM read_parquet('{g}', hive_partitioning=1)
            WHERE knowledge_time <= TIMESTAMP '{asof}'
              AND knowledge_time > TIMESTAMP '{asof}' - INTERVAL 3 DAY
              AND form IN ('8-K','10-K','10-Q') LIMIT 10""")

    def build(self, asof: str) -> dict:
        prices = self._prices(asof)
        macro = self._macro(asof)
        news = self._news(asof)
        filings = self._filings(asof)
        return {
            "as_of": asof,
            "prices": prices.to_dict("records"),
            "macro": macro.to_dict("records"),
            "news": news.to_dict("records"),
            "filings": filings.to_dict("records"),
            "text": self._render(asof, prices, macro, news, filings),
        }

    @staticmethod
    def _render(asof, prices, macro, news, filings) -> str:
        L = [f"=== World state as of {asof} (only info knowable by now) ==="]
        if len(prices):
            L.append("Prices: " + ", ".join(
                f"{r.entity} {r.close:.2f}" for r in prices.itertuples()))
        if len(macro):
            L.append("Macro (latest vintage): " + ", ".join(
                f"{r.entity}={r.value:.2f}" for r in macro.itertuples()))
        if len(news):
            L.append("Top headlines:")
            L += [f"  - [{int(r.points)}] {r.title}" for r in news.itertuples()]
        if len(filings):
            L.append("Recent filings: " + ", ".join(
                f"{r.form}:{str(r.company)[:24]}" for r in filings.itertuples()))
        return "\n".join(L)
