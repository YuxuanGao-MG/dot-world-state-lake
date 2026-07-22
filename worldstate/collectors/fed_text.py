"""FOMC policy text — statements + minutes (federalreserve.gov, keyless).

Collects the actual policy communications (the highest-signal macro text there
is). Statement URLs are monetaryYYYYMMDDa.htm; minutes are fomcminutesYYYYMMDD.htm.
Statements are public on the meeting day (knowledge_time = event); minutes are
released ~3 weeks later (knowledge_time = event + 21d). event_time = meeting date.
entity = "FOMC". Single sequential job -> one shard.
"""
from __future__ import annotations

import re
import lxml.html
import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

INDEX_PAGES = ["https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"] + \
    [f"https://www.federalreserve.gov/monetarypolicy/fomchistorical{y}.htm"
     for y in range(2020, date.today().year + 1)]
BASE = "https://www.federalreserve.gov"


class FomcText(Collector):
    domain = "policy"
    source = "fomc"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=3.0)

    def chunks(self) -> list[str]:
        return ["all"]

    def _discover(self) -> dict:
        """Return {url: (doc_type, meeting_date)} for statements + minutes."""
        found = {}
        for page in INDEX_PAGES:
            self.rl.wait()
            try:
                r = self.session.get(page, timeout=settings.HTTP_TIMEOUT)
                if r.status_code != 200:
                    continue
            except Exception:
                continue
            for m in re.finditer(r'/newsevents/pressreleases/monetary(\d{8})[a-z]\.htm', r.text):
                found[f"{BASE}{m.group(0)}"] = ("statement", m.group(1))
            for m in re.finditer(r'/monetarypolicy/fomcminutes(\d{8})\.htm', r.text):
                found[f"{BASE}{m.group(0)}"] = ("minutes", m.group(1))
        return found

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        path = hfstore.shard_path(self.domain, self.source, name="part.parquet")
        if not force and hfstore.exists(path):
            return {"skipped": True}

        docs = self._discover()
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        rows = []
        for url, (dtype, ymd) in docs.items():
            ev = pd.to_datetime(ymd, format="%Y%m%d", utc=True, errors="coerce")
            if pd.isna(ev) or ev < start:
                continue
            self.rl.wait()
            try:
                r = self.session.get(url, timeout=settings.HTTP_TIMEOUT)
                if r.status_code != 200:
                    continue
                text = " ".join(lxml.html.fromstring(r.content).text_content().split())
            except Exception:
                continue
            ktime = ev + pd.Timedelta(days=21 if dtype == "minutes" else 0)
            rows.append({"doc_type": dtype, "url": url, "text": text[:400_000],
                         "char_len": len(text), "event_time": ev, "knowledge_time": ktime})
        if not rows:
            return {"rows": 0, "empty": True}

        df = pd.DataFrame(rows).sort_values("event_time")
        payload = df[["doc_type", "url", "text", "char_len"]]
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=df["event_time"], knowledge_time=df["knowledge_time"],
            entity="FOMC", source_url=INDEX_PAGES[0], vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"docs": len(rows), "rows": table.num_rows, "path": path}
