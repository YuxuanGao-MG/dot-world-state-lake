"""Daily news volume + tone per theme from GDELT DOC 2.0 (keyless).

GDELT rate-limits hard (429) under concurrency, so this runs as a SINGLE
sequential job (chunks() -> ["all"]) that walks the themes politely and is
graceful: a theme that fails after retries is skipped, never crashing the run.
Per-theme shards upload as we go (idempotent), so partial progress persists and
the daily cron fills any gaps.

News about day D is knowable on D, so event_time = knowledge_time = D.
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

DOC = "https://api.gdeltproject.org/api/v2/doc/doc"


class GdeltThemes(Collector):
    domain = "news"
    source = "gdelt"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=0.4)  # ~2.5s between calls — GDELT is touchy

    def chunks(self) -> list[str]:
        return ["all"]  # single sequential job to avoid concurrent 429 storms

    def _timeline(self, query: str, mode: str) -> pd.DataFrame:
        self.rl.wait()
        start = settings.BACKFILL_START.replace("-", "") + "000000"
        params = {"query": query, "mode": mode, "format": "json",
                  "startdatetime": start, "enddatetime": "20991231000000",
                  "timelinesmooth": "0"}
        try:
            r = self.session.get(DOC, params=params, timeout=settings.HTTP_TIMEOUT)
        except Exception:
            return pd.DataFrame()
        if r.status_code != 200 or not r.text.strip().startswith("{"):
            return pd.DataFrame()
        tl = r.json().get("timeline", [])
        if not tl or not tl[0].get("data"):
            return pd.DataFrame()
        df = pd.DataFrame(tl[0]["data"])
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
        return df.dropna(subset=["date"])[["date", "value"]]

    def _one_theme(self, theme: str, query: str, force: bool) -> dict:
        path = hfstore.shard_path(self.domain, self.source, f"theme={theme}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"theme": theme, "skipped": True}
        vol = self._timeline(query, "timelinevol").rename(columns={"value": "volume_pct"})
        tone = self._timeline(query, "timelinetone").rename(columns={"value": "avg_tone"})
        if vol.empty and tone.empty:
            return {"theme": theme, "rows": 0, "empty": True}
        df = pd.merge(vol, tone, on="date", how="outer").sort_values("date")
        payload = df[["volume_pct", "avg_tone"]].astype("float64").reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=df["date"].values, knowledge_time=df["date"].values,
            entity=theme, source_url=f"{DOC}?query={query}&mode=timelinevol",
            vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"theme": theme, "rows": table.num_rows}

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        done, failed = [], []
        for theme, query in settings.GDELT_THEMES.items():
            try:
                res = self._one_theme(theme, query, force)
                done.append(res)
            except Exception as e:  # never crash the whole run over one theme
                failed.append({"theme": theme, "error": type(e).__name__})
        return {"done": len(done), "failed": failed, "themes": done}
