"""Article-level news from GDELT GKG bulk files (keyless, reliable downloads).

The GKG 2.1 15-minute files give, per article: title, URL, source domain, topic
themes, mentioned organizations/persons, and tone — English-source news across
ALL sectors (finance/econ/tech/health/consumer/world). Bulk files are static
downloads (not the rate-limited DOC API). News is naturally PIT: event_time =
knowledge_time = publication time. entity = source domain. One shard per month.

We sample a few 15-min files per day (config HOURS) to bound volume; the daily
incremental keeps it current. Resilient: a failed file is skipped.
"""
from __future__ import annotations

import io
import re
import zipfile
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

FILE = "http://data.gdeltproject.org/gdeltv2/{stamp}.gkg.csv.zip"
HOURS = list(range(24))  # hourly snapshots/day (denser coverage)
TITLE_RE = re.compile(r"<PAGE_TITLE>(.*?)</PAGE_TITLE>")


def _clean(s: str, n: int) -> str:
    return (s or "").replace(";", ", ")[:n]


class NewsGkg(Collector):
    domain = "news"
    source = "gdelt_gkg"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=4.0)

    def chunks(self) -> list[str]:
        y0, m0 = int(settings.BACKFILL_START[:4]), int(settings.BACKFILL_START[5:7])
        today = pd.Timestamp.utcnow()
        out, y, m = [], y0, m0
        while (y, m) <= (today.year, today.month):
            out.append(f"{y}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out

    def _parse_file(self, raw: str) -> list[dict]:
        rows = []
        for line in raw.split("\n"):
            f = line.split("\t")
            if len(f) < 27 or not f[4]:
                continue
            tm = TITLE_RE.search(f[26])
            title = tm.group(1) if tm else ""
            tone = (f[15] or "").split(",")
            def tf(i):
                try:
                    return float(tone[i])
                except (IndexError, ValueError):
                    return None
            rows.append({
                "date": f[1], "domain": f[3], "url": f[4],
                "title": title[:300], "themes": _clean(f[7], 400),
                "organizations": _clean(f[13], 300), "persons": _clean(f[11], 200),
                "tone": tf(0), "positive": tf(1), "negative": tf(2), "polarity": tf(3),
            })
        return rows

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year, month = int(chunk[:4]), int(chunk[5:7])
        path = hfstore.shard_path(self.domain, self.source, f"year={year}",
                                  f"month={month:02d}", name="part.parquet")
        if not force and hfstore.exists(path):
            return {"month": chunk, "skipped": True}

        recs, files = [], 0
        month_end = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)
        for day in pd.date_range(pd.Timestamp(year=year, month=month, day=1), month_end, freq="D"):
            for h in HOURS:
                stamp = f"{day.strftime('%Y%m%d')}{h:02d}0000"
                self.rl.wait()
                try:
                    r = self.session.get(FILE.format(stamp=stamp), timeout=90)
                    if r.status_code != 200:
                        continue
                    z = zipfile.ZipFile(io.BytesIO(r.content))
                    raw = z.read(z.namelist()[0]).decode("utf-8", "ignore")
                    recs.extend(self._parse_file(raw))
                    files += 1
                except Exception:
                    continue

        if not recs:
            return {"month": chunk, "rows": 0, "empty": True}
        df = pd.DataFrame(recs).drop_duplicates("url")
        ev = pd.to_datetime(df["date"], format="%Y%m%d%H%M%S", utc=True, errors="coerce")
        keep = ev.notna()
        payload = df[["title", "url", "domain", "themes", "organizations", "persons",
                      "tone", "positive", "negative", "polarity"]][keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev[keep].values, knowledge_time=ev[keep].values,
            entity=df["domain"][keep].astype(str).values,
            source_url="http://data.gdeltproject.org/gdeltv2/", vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"month": chunk, "files": files, "rows": table.num_rows, "path": path}
