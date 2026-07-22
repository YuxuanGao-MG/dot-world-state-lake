"""Economic surprise index — DERIVED from our own macro first-release vintages.

True consensus forecasts are paywalled, so we approximate surprise as the
first-released actual minus a naive random-walk forecast (previous value),
z-scored over a trailing window. This reads macro/alfred back out of the lake
(DuckDB over S3) and writes a derived series.

PIT-clean: a surprise is knowable exactly when its actual was first released, so
knowledge_time = the first-release knowledge_time. entity = series.
"""
from __future__ import annotations

import pandas as pd

from worldstate import query, store as hfstore, normalize
from worldstate.collectors.base import Collector

WINDOW = 20


class SurpriseIndex(Collector):
    domain = "derived"
    source = "surprise"

    def chunks(self) -> list[str]:
        return ["all"]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, name="part.parquet")
        if not force and hfstore.exists(path):
            return {"skipped": True}

        con = query.connect()
        glob = query._glob("macro", "alfred")
        df = con.execute(f"""
            WITH fr AS (
              SELECT entity, event_time, value, knowledge_time,
                     row_number() OVER (PARTITION BY entity, event_time
                                        ORDER BY knowledge_time ASC) rn
              FROM read_parquet('{glob}', hive_partitioning=1))
            SELECT entity, event_time, value AS actual, knowledge_time
            FROM fr WHERE rn = 1 ORDER BY entity, event_time
        """).df()
        if df.empty:
            return {"rows": 0, "empty": True}

        parts = []
        for ent, g in df.groupby("entity"):
            g = g.sort_values("event_time").copy()
            g["forecast"] = g["actual"].shift(1)
            g["surprise"] = g["actual"] - g["forecast"]
            m = g["surprise"].rolling(WINDOW, min_periods=5).mean()
            sd = g["surprise"].rolling(WINDOW, min_periods=5).std()
            g["surprise_z"] = (g["surprise"] - m) / sd
            parts.append(g)
        allg = pd.concat(parts).dropna(subset=["surprise"]).reset_index(drop=True)

        payload = allg[["actual", "forecast", "surprise", "surprise_z"]].astype("float64")
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=pd.to_datetime(allg["event_time"], utc=True),
            knowledge_time=pd.to_datetime(allg["knowledge_time"], utc=True),
            entity=allg["entity"].values,
            source_url="derived:macro/alfred first-release vs random-walk", vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"rows": table.num_rows, "series": int(allg["entity"].nunique()), "path": path}
