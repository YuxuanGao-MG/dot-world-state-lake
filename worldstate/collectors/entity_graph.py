"""Entity / knowledge graph — DERIVED from the lake (DuckDB over S3).

Turns siloed tables into a traversable graph an agent can walk:
  nodes       canonical companies (from security_master)
  ownership   13F manager --owns--> issuer            (knowledge_time = filing)
  insider     insider person --insider_of--> ticker   (knowledge_time = acceptance)
  co_mention  org <--co_mention--> org, per quarter    (from news GKG, weighted)

PIT-clean: every edge carries the knowledge_time of its evidence, so the graph
can be queried as-of any date. entity = source node. One shard per edge kind.
"""
from __future__ import annotations

import pandas as pd

from worldstate import query, store as hfstore, normalize
from worldstate.collectors.base import Collector


class EntityGraph(Collector):
    domain = "graph"
    source = "derived"

    def chunks(self) -> list[str]:
        return ["nodes", "ownership", "insider", "co_mention"]

    def _write(self, kind, df, event_time, knowledge_time, entity, payload_cols, force):
        path = hfstore.shard_path(self.domain, self.source, f"kind={kind}",
                                  name="part.parquet")
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=df[payload_cols],
            event_time=event_time, knowledge_time=knowledge_time,
            entity=entity, source_url=f"derived:{kind}", vintage_id="")
        hfstore.upload_table(table, path, overwrite=force)
        return {"kind": kind, "rows": table.num_rows, "path": path}

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, f"kind={chunk}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"kind": chunk, "skipped": True}
        try:
            con = query.connect()
            return self._build(con, chunk, force)
        except Exception as e:  # missing source files / transient — never crash the run
            return {"kind": chunk, "skipped_error": type(e).__name__, "detail": str(e)[:150]}

    def _build(self, con, chunk, force):
        if chunk == "nodes":
            g = query._glob("reference", "master")
            df = con.execute(f"""
                SELECT entity AS ticker, cik, name, exchange
                FROM read_parquet('{g}', hive_partitioning=1, union_by_name=1)
                WHERE record_type='identity'""").df()
            if df.empty:
                return {"kind": chunk, "rows": 0, "empty": True}
            df["node_type"] = "company"
            now = pd.Timestamp.utcnow()
            return self._write(chunk, df, now, now, df["ticker"].values,
                               ["cik", "name", "exchange", "node_type"], force)

        if chunk == "ownership":
            g = query._glob("ownership", "sec_13f")
            df = con.execute(f"""
                SELECT entity AS manager, issuer AS dst, value_usd_thousands AS value,
                       shares, event_time, knowledge_time
                FROM read_parquet('{g}', hive_partitioning=1)
                WHERE issuer IS NOT NULL AND length(issuer)>0""").df()
            if df.empty:
                return {"kind": chunk, "rows": 0, "empty": True}
            df["rel"] = "owns"
            return self._write(chunk, df, df["event_time"], df["knowledge_time"],
                               df["manager"].values, ["rel", "dst", "value", "shares"], force)

        if chunk == "insider":
            g = query._glob("positioning", "sec_form4")
            df = con.execute(f"""
                SELECT owner_name AS src, entity AS dst, shares, txn_code, acq_disp,
                       event_time, knowledge_time
                FROM read_parquet('{g}', hive_partitioning=1)
                WHERE owner_name IS NOT NULL AND length(owner_name)>0""").df()
            if df.empty:
                return {"kind": chunk, "rows": 0, "empty": True}
            df["rel"] = "insider_of"
            return self._write(chunk, df, df["event_time"], df["knowledge_time"],
                               df["src"].values, ["rel", "dst", "shares", "txn_code", "acq_disp"], force)

        # co_mention: org<->org from news GKG, aggregated per quarter (bounded to top orgs)
        g = query._glob("news", "gdelt_gkg")
        df = con.execute(f"""
            WITH raw AS (
              SELECT url, date_trunc('quarter', event_time) AS q,
                     unnest(string_split(organizations, ', ')) AS org0
              FROM read_parquet('{g}', hive_partitioning=1)
              WHERE organizations IS NOT NULL AND length(organizations)>0),
            orgs AS (SELECT url, q, trim(org0) AS org FROM raw WHERE length(trim(org0)) > 2),
            cnt AS (SELECT org, count(*) c FROM orgs GROUP BY org),
            top AS (SELECT org FROM cnt WHERE c >= 100 ORDER BY c DESC LIMIT 3000),
            f AS (SELECT o.url, o.q, o.org FROM orgs o JOIN top t ON o.org=t.org)
            SELECT a.org AS src, b.org AS dst, a.q AS q, count(*) AS weight
            FROM f a JOIN f b ON a.url=b.url AND a.q=b.q AND a.org < b.org
            GROUP BY 1,2,3 HAVING count(*) >= 3""").df()
        if df.empty:
            return {"kind": chunk, "rows": 0, "empty": True}
        df["rel"] = "co_mention"
        ev = pd.to_datetime(df["q"], utc=True)
        return self._write(chunk, df, ev, ev, df["src"].values,
                           ["rel", "dst", "weight"], force)
