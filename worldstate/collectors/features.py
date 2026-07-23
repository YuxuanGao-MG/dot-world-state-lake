"""Engineered features — DERIVED from the lake (DuckDB over S3), ready-made
signals so agents don't re-derive them. PIT-correct: each feature carries the
knowledge_time of its latest input, and windows only use past rows.

Chunks:
  technical           per-ticker: returns, SMAs, RSI, vol, drawdown, trend flags
  regime              market-wide daily: curve/inversion, VIX percentile, HY
                      spread, risk-appetite score, risk-on/off label
  insider_flow        per-ticker monthly: net insider buy/sell shares + counts
  prediction_momentum per prediction-market: probability + 30d change
"""
from __future__ import annotations

import pandas as pd

from worldstate import query, store as hfstore, normalize
from worldstate.collectors.base import Collector


class Features(Collector):
    domain = "features"
    source = "derived"

    def chunks(self) -> list[str]:
        return ["technical", "regime", "insider_flow", "prediction_momentum"]

    def _write(self, kind, df, payload_cols, force):
        path = hfstore.shard_path(self.domain, self.source, f"kind={kind}",
                                  name="part.parquet")
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=df[payload_cols],
            event_time=pd.to_datetime(df["event_time"], utc=True),
            knowledge_time=pd.to_datetime(df["knowledge_time"], utc=True),
            entity=df["entity"].astype(str).values, source_url=f"derived:features/{kind}",
            vintage_id="")
        hfstore.upload_table(table, path, overwrite=force)
        return {"kind": kind, "rows": table.num_rows, "path": path}

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, f"kind={chunk}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"kind": chunk, "skipped": True}
        try:
            con = query.connect()
            return getattr(self, f"_{chunk}")(con, force)
        except Exception as e:
            return {"kind": chunk, "skipped_error": type(e).__name__, "detail": str(e)[:150]}

    # --- technical (per ticker) ---------------------------------------------
    def _technical(self, con, force):
        g = query._glob("market", "yahoo")
        df = con.execute(f"""
            WITH p AS (SELECT entity, event_time, knowledge_time, close
                       FROM read_parquet('{g}', hive_partitioning=1)),
            r AS (SELECT *, close / lag(close) OVER w - 1 AS ret_1d
                  FROM p WINDOW w AS (PARTITION BY entity ORDER BY event_time))
            SELECT entity, event_time, knowledge_time, close, ret_1d,
              close / lag(close, 21) OVER w - 1 AS ret_1m,
              close / lag(close, 252) OVER w - 1 AS ret_12m,
              avg(close) OVER (w ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma50,
              avg(close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200,
              stddev_samp(ret_1d) OVER (w ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) * sqrt(252) AS vol_21d,
              close / max(close) OVER (w ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) - 1 AS dist_52w_high,
              avg(greatest(ret_1d, 0)) OVER (w ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS ag,
              avg(greatest(-ret_1d, 0)) OVER (w ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS al
            FROM r WINDOW w AS (PARTITION BY entity ORDER BY event_time)
        """).df()
        if df.empty:
            return {"kind": "technical", "rows": 0, "empty": True}
        df["rsi14"] = 100 - 100 / (1 + df["ag"] / df["al"].replace(0, pd.NA))
        df["above_sma50"] = (df["close"] > df["sma50"]).astype("int8")
        df["above_sma200"] = (df["close"] > df["sma200"]).astype("int8")
        cols = ["close", "ret_1d", "ret_1m", "ret_12m", "sma50", "sma200",
                "vol_21d", "dist_52w_high", "rsi14", "above_sma50", "above_sma200"]
        for c in cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return self._write("technical", df, cols, force)

    # --- regime (market-wide) -----------------------------------------------
    def _regime(self, con, force):
        gm = query._glob("macro", "alfred")
        gc = query._glob("credit", "fred")

        def series(glob, sid):
            return f"""(SELECT event_time, any_value(value) AS v FROM read_parquet('{glob}', hive_partitioning=1)
                       WHERE entity='{sid}' GROUP BY event_time)"""
        df = con.execute(f"""
            WITH vix AS {series(gm, 'VIXCLS')}, curve AS {series(gm, 'T10Y2Y')},
                 hy AS {series(gc, 'BAMLH0A0HYM2')}
            SELECT v.event_time, v.v AS vix, c.v AS curve_2s10s, h.v AS hy_spread
            FROM vix v JOIN curve c USING(event_time) JOIN hy h USING(event_time)
            ORDER BY v.event_time
        """).df()
        if df.empty:
            return {"kind": "regime", "rows": 0, "empty": True}
        df = df.sort_values("event_time")
        df["vix_pct_252"] = df["vix"].rolling(252, min_periods=30).apply(
            lambda s: (s.rank(pct=True).iloc[-1]), raw=False)
        df["hy_chg_21"] = df["hy_spread"] - df["hy_spread"].shift(21)
        z = lambda s: (s - s.rolling(252, min_periods=30).mean()) / s.rolling(252, min_periods=30).std()
        df["risk_appetite"] = (-z(df["vix"]) - z(df["hy_spread"])).round(3)
        df["curve_inverted"] = (df["curve_2s10s"] < 0).astype("int8")
        df["regime"] = "neutral"
        df.loc[(df["vix_pct_252"] > 0.8) | (df["hy_chg_21"] > 0.5), "regime"] = "risk_off"
        df.loc[(df["vix_pct_252"] < 0.4) & (df["hy_chg_21"] < 0), "regime"] = "risk_on"
        df["entity"] = "MARKET"
        df["knowledge_time"] = pd.to_datetime(df["event_time"], utc=True) + pd.Timedelta(days=1)
        cols = ["vix", "vix_pct_252", "curve_2s10s", "curve_inverted", "hy_spread",
                "hy_chg_21", "risk_appetite", "regime"]
        return self._write("regime", df, cols, force)

    # --- insider flow (per ticker, monthly) ---------------------------------
    def _insider_flow(self, con, force):
        g = query._glob("positioning", "sec_form4")
        df = con.execute(f"""
            SELECT entity, date_trunc('month', event_time) AS m,
              sum(CASE WHEN acq_disp='A' THEN coalesce(shares,0) ELSE -coalesce(shares,0) END) AS net_shares,
              sum(CASE WHEN acq_disp='A' THEN 1 ELSE 0 END) AS n_buys,
              sum(CASE WHEN acq_disp='D' THEN 1 ELSE 0 END) AS n_sells,
              max(knowledge_time) AS kt
            FROM read_parquet('{g}', hive_partitioning=1)
            WHERE txn_code IN ('P','S') GROUP BY 1,2""").df()
        if df.empty:
            return {"kind": "insider_flow", "rows": 0, "empty": True}
        df["net_score"] = (df["n_buys"] - df["n_sells"]) / (df["n_buys"] + df["n_sells"]).replace(0, pd.NA)
        df["event_time"] = pd.to_datetime(df["m"], utc=True)
        df["knowledge_time"] = pd.to_datetime(df["kt"], utc=True)
        cols = ["net_shares", "n_buys", "n_sells", "net_score"]
        return self._write("insider_flow", df, cols, force)

    # --- prediction momentum -------------------------------------------------
    def _prediction_momentum(self, con, force):
        g = query._glob("predictions", "manifold")
        df = con.execute(f"""
            SELECT entity, event_time, knowledge_time, any_value(question) AS question,
                   avg(probability) AS probability
            FROM read_parquet('{g}', hive_partitioning=1)
            GROUP BY entity, event_time, knowledge_time""").df()
        if df.empty:
            return {"kind": "prediction_momentum", "rows": 0, "empty": True}
        df = df.sort_values(["entity", "event_time"])
        df["prob_chg_30d"] = df.groupby("entity")["probability"].diff(30)
        cols = ["question", "probability", "prob_chg_30d"]
        return self._write("prediction_momentum", df, cols, force)
