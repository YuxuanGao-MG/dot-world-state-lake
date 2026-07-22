"""Finance/econ research papers from arXiv (keyless).

Dated, immutable publications -> clean PIT: event_time = knowledge_time =
submission date. entity = category. One shard per category.
"""
from __future__ import annotations

import time
import lxml.etree as ET
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

API = "http://export.arxiv.org/api/query"
CATS = ["q-fin.TR", "q-fin.PM", "q-fin.RM", "q-fin.ST", "q-fin.CP", "q-fin.GN",
        "q-fin.MF", "q-fin.EC", "econ.EM", "econ.GN", "econ.TH"]
NS = {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
PAGE = 200
MAX = 4000


class ArxivPapers(Collector):
    domain = "research"
    source = "arxiv"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=0.33)  # arXiv asks ~1 request / 3s

    def chunks(self) -> list[str]:
        return list(CATS)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        cat = chunk
        path = hfstore.shard_path(self.domain, self.source, f"category={cat}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"category": cat, "skipped": True}

        y0 = settings.BACKFILL_START[:4]
        q = f"cat:{cat} AND submittedDate:[{y0}01010000 TO 20301231235959]"
        rows, start = [], 0
        while start < MAX:
            self.rl.wait()
            r = self.session.get(API, params={"search_query": q, "start": start,
                                              "max_results": PAGE, "sortBy": "submittedDate",
                                              "sortOrder": "ascending"}, timeout=60)
            if r.status_code != 200:
                break
            root = ET.fromstring(r.content)
            entries = root.findall("a:entry", NS)
            if not entries:
                break
            for e in entries:
                pub = e.findtext("a:published", default="", namespaces=NS)
                authors = ", ".join(a.findtext("a:name", default="", namespaces=NS)
                                    for a in e.findall("a:author", NS))[:500]
                prim = e.find("arxiv:primary_category", NS)
                rows.append({
                    "arxiv_id": e.findtext("a:id", default="", namespaces=NS),
                    "title": " ".join((e.findtext("a:title", default="", namespaces=NS)).split()),
                    "abstract": " ".join((e.findtext("a:summary", default="", namespaces=NS)).split())[:3000],
                    "authors": authors,
                    "primary_category": prim.get("term") if prim is not None else cat,
                    "published": pub,
                })
            start += PAGE
            if len(entries) < PAGE:
                break

        if not rows:
            return {"category": cat, "rows": 0, "empty": True}
        df = pd.DataFrame(rows)
        ev = pd.to_datetime(df["published"], utc=True, errors="coerce")
        keep = ev.notna()
        payload = df[["arxiv_id", "title", "abstract", "authors", "primary_category"]][keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev[keep].values, knowledge_time=ev[keep].values,
            entity=cat, source_url=f"{API}?search_query=cat:{cat}", vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"category": cat, "rows": table.num_rows, "path": path}
