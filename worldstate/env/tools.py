"""Tools the agent can call to fetch more of the world — gated by access tier.

This is what makes a trajectory multi-step and realistic: the base observation is
cheap/basic, but richer signals (fundamentals, positioning, filing text, on-chain)
sit behind a `pro` tier and cost budget to pull. So an episode naturally contains
the "what's my access / which tool do I call / then decide" steps.

Every tool query is as-of the clock cursor (knowledge_time <= cursor) — tools can
never leak the future either.
"""
from __future__ import annotations

from dataclasses import dataclass
from worldstate import query

TIERS = {"basic": 0, "pro": 1}


@dataclass
class Tool:
    name: str
    tier: str          # min access tier required
    cost: int          # budget units
    desc: str


class ToolRegistry:
    def __init__(self):
        self.tools = {t.name: t for t in [
            Tool("price_history", "basic", 1, "Last N daily closes for a ticker. args: {ticker, n?}"),
            Tool("fundamentals", "pro", 1, "Latest key financials (XBRL) for a ticker. args: {ticker}"),
            Tool("positioning", "pro", 1, "Latest short-sale volume for a ticker. args: {ticker}"),
            Tool("filing_text", "pro", 2, "Most recent 8-K/10-K text snippet. args: {ticker}"),
            Tool("onchain", "pro", 1, "Latest BTC on-chain metric. args: {metric}"),
            Tool("prediction_market", "pro", 1, "Crowd probability of future events matching a query. args: {query}"),
            Tool("defi_tvl", "pro", 1, "Latest DeFi TVL for a chain (or 'total'/'stablecoins'). args: {series}"),
            Tool("recent_shocks", "pro", 1, "Recent significant earthquakes (last 30d). args: {min_mag?}"),
        ]}

    def available(self, tier: str) -> list[dict]:
        lvl = TIERS.get(tier, 0)
        return [{"name": t.name, "cost": t.cost, "desc": t.desc}
                for t in self.tools.values() if TIERS.get(t.tier, 9) <= lvl]

    def get(self, name: str) -> Tool | None:
        return self.tools.get(name)

    # --- executors (as-of env.clock.cursor) ----------------------------------
    def run(self, env, name: str, args: dict) -> dict:
        asof = env.clock.iso()
        try:
            fn = getattr(self, f"_{name}")
        except Exception:
            return {"error": f"unknown tool {name}"}
        try:
            return fn(env, asof, args or {})
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}

    def _q(self, env, sql):
        return env.con.execute(sql).df().to_dict("records")

    def _price_history(self, env, asof, args):
        t = str(args.get("ticker", "SPY")).upper()
        n = int(args.get("n", 10))
        g = query._glob("market", "yahoo")
        return {"ticker": t, "history": self._q(env, f"""
            SELECT event_time, close FROM read_parquet('{g}', hive_partitioning=1)
            WHERE entity='{t}' AND knowledge_time <= TIMESTAMP '{asof}'
            ORDER BY event_time DESC LIMIT {n}""")}

    def _fundamentals(self, env, asof, args):
        t = str(args.get("ticker", "AAPL")).upper()
        g = query._glob("fundamentals", "sec_xbrl")
        return {"ticker": t, "facts": self._q(env, f"""
            WITH r AS (SELECT concept, value, event_time, knowledge_time,
                        row_number() OVER (PARTITION BY concept ORDER BY knowledge_time DESC) rn
                        FROM read_parquet('{g}', hive_partitioning=1)
                        WHERE entity='{t}' AND knowledge_time <= TIMESTAMP '{asof}'
                          AND concept IN ('Revenues','NetIncomeLoss','Assets','Liabilities','StockholdersEquity'))
            SELECT concept, value, event_time FROM r WHERE rn=1""")}

    def _positioning(self, env, asof, args):
        t = str(args.get("ticker", "AAPL")).upper()
        g = query._glob("positioning", "finra_short")
        return {"ticker": t, "short": self._q(env, f"""
            SELECT event_time, short_volume, total_volume, short_ratio
            FROM read_parquet('{g}', hive_partitioning=1)
            WHERE entity='{t}' AND knowledge_time <= TIMESTAMP '{asof}'
            ORDER BY event_time DESC LIMIT 1""")}

    def _filing_text(self, env, asof, args):
        t = str(args.get("ticker", "AAPL")).upper()
        g = query._glob("filings_text", "edgar")
        rows = self._q(env, f"""
            SELECT form, event_time, substr(text, 1, 500) AS snippet
            FROM read_parquet('{g}', hive_partitioning=1)
            WHERE entity='{t}' AND knowledge_time <= TIMESTAMP '{asof}'
            ORDER BY knowledge_time DESC LIMIT 1""")
        return {"ticker": t, "filing": rows}

    def _onchain(self, env, asof, args):
        m = str(args.get("metric", "n-unique-addresses"))
        g = query._glob("onchain", "blockchain")
        return {"metric": m, "series": self._q(env, f"""
            SELECT event_time, value FROM read_parquet('{g}', hive_partitioning=1)
            WHERE metric='{m}' AND knowledge_time <= TIMESTAMP '{asof}'
            ORDER BY event_time DESC LIMIT 1""")}

    def _prediction_market(self, env, asof, args):
        """Latest crowd probability across all 3 venues (Kalshi/Polymarket/Manifold)."""
        q = str(args.get("query", "")).lower().replace("'", "")
        rows = []
        # (source, question-column, volume-expr) — kalshi uses `title` and has no volume
        for src, qcol, vexpr in (("kalshi", "title", "0.0"),
                                 ("polymarket", "question", "volume"),
                                 ("manifold", "question", "volume")):
            g = query._glob("predictions", src)
            rows += self._q(env, f"""
                WITH v AS (SELECT {qcol} AS question, probability, {vexpr} AS volume, source,
                            row_number() OVER (PARTITION BY entity ORDER BY event_time DESC) rn
                            FROM read_parquet('{g}', hive_partitioning=1)
                            WHERE knowledge_time <= TIMESTAMP '{asof}'
                              AND lower({qcol}) LIKE '%{q}%')
                SELECT source, question, probability, volume FROM v WHERE rn=1
                ORDER BY volume DESC LIMIT 6""")
        return {"query": q, "venues": ["kalshi", "polymarket", "manifold"], "markets": rows}

    def _defi_tvl(self, env, asof, args):
        s = str(args.get("series", "total"))
        g = query._glob("crypto_defi", "defillama")
        return {"series": s, "latest": self._q(env, f"""
            SELECT event_time, value, metric FROM read_parquet('{g}', hive_partitioning=1)
            WHERE entity='{s}' AND knowledge_time <= TIMESTAMP '{asof}'
            ORDER BY event_time DESC LIMIT 1""")}

    def _recent_shocks(self, env, asof, args):
        mag = float(args.get("min_mag", 6.0))
        g = query._glob("events", "usgs")
        return {"min_mag": mag, "quakes": self._q(env, f"""
            SELECT event_time, magnitude, place FROM read_parquet('{g}', hive_partitioning=1)
            WHERE knowledge_time <= TIMESTAMP '{asof}'
              AND knowledge_time > TIMESTAMP '{asof}' - INTERVAL 30 DAY
              AND magnitude >= {mag} ORDER BY magnitude DESC LIMIT 5""")}
