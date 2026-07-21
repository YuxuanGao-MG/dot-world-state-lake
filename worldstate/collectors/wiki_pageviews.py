"""Daily Wikipedia pageviews per topic (Wikimedia REST API, keyless).

A public-attention signal. Views for day D are reported after D, so
event_time = D, knowledge_time = D + 1 day. entity = article title.
"""
from __future__ import annotations

import pandas as pd
from datetime import date
from urllib.parse import quote

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

BASE = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        "en.wikipedia/all-access/all-agents/{article}/daily/{start}/{end}")


def _slug(article: str) -> str:
    return article.replace("/", "_").replace(" ", "_")


class WikiPageviews(Collector):
    domain = "attention"
    source = "wikipedia"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=4.0)

    def chunks(self) -> list[str]:
        return list(settings.WIKI_ARTICLES)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        article = chunk
        path = hfstore.shard_path(self.domain, self.source,
                                  f"article={_slug(article)}", name="part.parquet")
        if not force and hfstore.exists(path):
            return {"article": article, "skipped": True}

        self.rl.wait()
        start = settings.BACKFILL_START.replace("-", "") + "00"
        end = date.today().strftime("%Y%m%d") + "00"
        url = BASE.format(article=quote(article, safe=""), start=start, end=end)
        r = self.session.get(url, timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200:
            return {"article": article, "rows": 0, "status": r.status_code}
        items = r.json().get("items", [])
        if not items:
            return {"article": article, "rows": 0, "empty": True}

        df = pd.DataFrame(items)
        ev = pd.to_datetime(df["timestamp"].str[:8], format="%Y%m%d", utc=True)
        payload = pd.DataFrame({"views": df["views"].astype("float64")})
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=(ev + pd.Timedelta(days=1)).values,
            entity=article,
            source_url=f"https://en.wikipedia.org/wiki/{_slug(article)}",
            vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"article": article, "rows": table.num_rows, "path": path}
