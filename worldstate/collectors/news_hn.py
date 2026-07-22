"""Headlines + discussion from Hacker News via the Algolia API (keyless).

Reliable, full-history, no rate-limit fragility. For each finance query we page
through monthly windows and collect stories (title, url, points, comments). A
story is knowable when posted: event_time = knowledge_time = created_at.
entity = query label.
"""
from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

SEARCH = "http://hn.algolia.com/api/v1/search_by_date"


def _month_edges(start: str):
    y, m = int(start[:4]), int(start[5:7])
    now = datetime.now(timezone.utc)
    while (y, m) <= (now.year, now.month):
        lo = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp())
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        hi = int(datetime(ny, nm, 1, tzinfo=timezone.utc).timestamp())
        yield lo, hi
        y, m = ny, nm


class HackerNews(Collector):
    domain = "news"
    source = "hackernews"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=6.0)

    def chunks(self) -> list[str]:
        return list(settings.HN_QUERIES.keys())

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        label = chunk
        query = settings.HN_QUERIES[label]
        path = hfstore.shard_path(self.domain, self.source, f"query={label}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"query": label, "skipped": True}

        rows = []
        for lo, hi in _month_edges(settings.BACKFILL_START):
            self.rl.wait()
            params = {"tags": "story", "query": query,
                      "numericFilters": f"created_at_i>={lo},created_at_i<{hi}",
                      "hitsPerPage": 100}
            try:
                r = self.session.get(SEARCH, params=params, timeout=settings.HTTP_TIMEOUT)
                if r.status_code != 200:
                    continue
                for h in r.json().get("hits", []):
                    if not h.get("created_at_i"):
                        continue
                    rows.append(h)
            except Exception:
                continue

        if not rows:
            return {"query": label, "rows": 0, "empty": True}
        df = pd.DataFrame(rows).drop_duplicates("objectID")
        ev = pd.to_datetime(df["created_at_i"], unit="s", utc=True)
        payload = pd.DataFrame({
            "title": df.get("title", pd.Series([""] * len(df))).fillna("").astype(str),
            "url": df.get("url", pd.Series([""] * len(df))).fillna("").astype(str),
            "author": df.get("author", pd.Series([""] * len(df))).fillna("").astype(str),
            "points": pd.to_numeric(df.get("points"), errors="coerce").fillna(0).astype("int64"),
            "num_comments": pd.to_numeric(df.get("num_comments"), errors="coerce").fillna(0).astype("int64"),
            "story_id": df["objectID"].astype(str),
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=ev.values,
            entity=label, source_url=f"{SEARCH}?query={query}", vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"query": label, "rows": table.num_rows, "path": path}
