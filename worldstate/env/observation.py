"""Builds the point-in-time observation an agent sees at the current cursor.

Comprehensive world snapshot across the lake's domains — prices, rates, credit,
commodities, crypto, macro, sentiment, prediction-market beliefs, real news
headlines, filings, and recent events — every channel filtered by
knowledge_time <= cursor (zero lookahead). Returns a structured dict + a
natural-language rendering (LLM-agent friendly). Channels degrade gracefully.
"""
from __future__ import annotations

import pandas as pd
from worldstate import query

MACRO = ["CPIAUCSL", "UNRATE", "FEDFUNDS", "GDPC1", "PAYEMS"]
RATES = ["DGS2", "DGS10", "T10Y2Y", "MORTGAGE30US", "VIXCLS"]
CREDIT = ["BAMLH0A0HYM2", "BAMLC0A0CM", "NFCI", "STLFSI4"]
COMMOD = ["DCOILWTICO", "GOLDAMGBD228NLBM", "DHHNGSP"]


class ObservationBuilder:
    def __init__(self, con=None, watchlist=None):
        self.con = con or query.connect()
        self.watchlist = watchlist or ["SPY", "QQQ", "IWM", "AAPL", "NVDA", "MSFT",
                                       "TLT", "GLD", "HYG"]

    def _df(self, sql: str) -> pd.DataFrame:
        try:
            return self.con.execute(sql).df()
        except Exception:
            return pd.DataFrame()

    def _latest(self, domain, source, entities, asof, col="value") -> pd.DataFrame:
        g = query._glob(domain, source)
        ins = ",".join(f"'{e}'" for e in entities)
        return self._df(f"""
            WITH v AS (SELECT entity, {col} AS value, knowledge_time, event_time,
                       row_number() OVER (PARTITION BY entity
                         ORDER BY knowledge_time DESC, event_time DESC) rn
                       FROM read_parquet('{g}', hive_partitioning=1)
                       WHERE knowledge_time <= TIMESTAMP '{asof}' AND entity IN ({ins}))
            SELECT entity, value FROM v WHERE rn=1""")

    def _prices(self, asof):
        wl = ",".join(f"'{t}'" for t in self.watchlist)
        g = query._glob("market", "yahoo")
        return self._df(f"""
            WITH v AS (SELECT entity, close, row_number() OVER
                       (PARTITION BY entity ORDER BY event_time DESC) rn
                       FROM read_parquet('{g}', hive_partitioning=1)
                       WHERE knowledge_time <= TIMESTAMP '{asof}' AND entity IN ({wl}))
            SELECT entity, close FROM v WHERE rn=1 ORDER BY entity""")

    def _crypto(self, asof):
        out = {}
        b = self._latest("crypto", "yahoo", ["BTC", "ETH"], asof, "close")
        for r in b.itertuples():
            out[r.entity] = round(r.value, 1)
        fng = self._latest("sentiment", "alt_fng", ["crypto_market"], asof, "fng_value")
        if len(fng):
            out["fear_greed"] = int(fng["value"].iloc[0])
        dv = self._latest("crypto_deriv", "deribit", ["BTC"], asof, "dvol_close")
        if len(dv):
            out["BTC_implied_vol"] = round(dv["value"].iloc[0], 1)
        return out

    def _news(self, asof):
        g = query._glob("news", "gdelt_gkg")
        df = self._df(f"""
            SELECT title, domain, tone FROM read_parquet('{g}', hive_partitioning=1)
            WHERE knowledge_time <= TIMESTAMP '{asof}'
              AND knowledge_time > TIMESTAMP '{asof}' - INTERVAL 2 DAY
              AND length(title) > 20 ORDER BY abs(tone) DESC LIMIT 8""")
        if df.empty:  # fall back to HN headlines while GKG backfills
            h = query._glob("news", "hackernews")
            df = self._df(f"""SELECT title, entity AS domain, points AS tone
                FROM read_parquet('{h}', hive_partitioning=1)
                WHERE knowledge_time <= TIMESTAMP '{asof}'
                  AND knowledge_time > TIMESTAMP '{asof}' - INTERVAL 7 DAY
                ORDER BY points DESC LIMIT 8""")
        return df

    def _predictions(self, asof):
        g = query._glob("predictions", "manifold")
        return self._df(f"""
            WITH v AS (SELECT question, probability, volume,
                       row_number() OVER (PARTITION BY entity ORDER BY event_time DESC) rn
                       FROM read_parquet('{g}', hive_partitioning=1)
                       WHERE knowledge_time <= TIMESTAMP '{asof}')
            SELECT question, probability FROM v WHERE rn=1 ORDER BY volume DESC LIMIT 5""")

    def _events(self, asof):
        g = query._glob("events", "usgs")
        return self._df(f"""
            SELECT 'quake M'||round(magnitude,1)||' '||place AS event, event_time
            FROM read_parquet('{g}', hive_partitioning=1)
            WHERE knowledge_time <= TIMESTAMP '{asof}'
              AND knowledge_time > TIMESTAMP '{asof}' - INTERVAL 14 DAY
              AND magnitude >= 6 ORDER BY magnitude DESC LIMIT 3""")

    def _regime(self, asof):
        g = query._glob("features", "derived")
        return self._df(f"""
            WITH v AS (SELECT *, row_number() OVER (ORDER BY event_time DESC) rn
                       FROM read_parquet('{g}', hive_partitioning=1, union_by_name=1)
                       WHERE kind='regime' AND knowledge_time <= TIMESTAMP '{asof}')
            SELECT regime, vix_pct_252, curve_inverted, risk_appetite FROM v WHERE rn=1""")

    def build(self, asof: str) -> dict:
        ch = {
            "regime": self._regime(asof),
            "prices": self._prices(asof),
            "rates": self._latest("macro", "alfred", RATES, asof),
            "macro": self._latest("macro", "alfred", MACRO, asof),
            "credit": self._latest("credit", "fred", CREDIT, asof),
            "commodities": self._latest("commodity", "fred", COMMOD, asof),
            "predictions": self._predictions(asof),
            "news": self._news(asof),
            "events": self._events(asof),
        }
        crypto = self._crypto(asof)
        return {
            "as_of": asof,
            "regime": ch["regime"].to_dict("records"),
            "prices": ch["prices"].to_dict("records"),
            "rates": ch["rates"].to_dict("records"),
            "macro": ch["macro"].to_dict("records"),
            "credit": ch["credit"].to_dict("records"),
            "commodities": ch["commodities"].to_dict("records"),
            "crypto": crypto,
            "predictions": ch["predictions"].to_dict("records"),
            "news": ch["news"].to_dict("records"),
            "events": ch["events"].to_dict("records"),
            "text": self._render(asof, ch, crypto),
        }

    @staticmethod
    def _kv(df):
        return ", ".join(f"{r.entity}={r.value:.2f}" for r in df.itertuples()) if len(df) else "—"

    def _render(self, asof, ch, crypto) -> str:
        L = [f"=== World state as of {asof} (only info knowable by now) ==="]
        if len(ch["regime"]):
            r = ch["regime"].iloc[0]
            L.append(f"Regime: {r['regime']} (VIX pct {r['vix_pct_252']:.0%}, "
                     f"curve {'inverted' if r['curve_inverted'] else 'normal'}, "
                     f"risk-appetite {r['risk_appetite']:+.2f})")
        if len(ch["prices"]):
            L.append("Equities: " + ", ".join(f"{r.entity} {r.close:.2f}" for r in ch["prices"].itertuples()))
        L.append("Rates: " + self._kv(ch["rates"]))
        L.append("Macro: " + self._kv(ch["macro"]))
        L.append("Credit/conditions: " + self._kv(ch["credit"]))
        L.append("Commodities: " + self._kv(ch["commodities"]))
        if crypto:
            L.append("Crypto: " + ", ".join(f"{k}={v}" for k, v in crypto.items()))
        if len(ch["predictions"]):
            L.append("Crowd beliefs: " + "; ".join(
                f"{r.probability:.0%} {str(r.question)[:60]}" for r in ch["predictions"].itertuples()))
        if len(ch["events"]):
            L.append("Recent shocks: " + "; ".join(str(r.event)[:50] for r in ch["events"].itertuples()))
        if len(ch["news"]):
            L.append("Top headlines:")
            L += [f"  - ({r.domain}) {str(r.title)[:90]}" for r in ch["news"].itertuples()]
        return "\n".join(L)
