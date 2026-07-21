"""Daily news volume + tone per theme from GDELT DOC 2.0 (keyless).

For each theme we pull two daily timelines — coverage volume intensity and
average tone — and merge them. News about day D is knowable on D, so
event_time = knowledge_time = D. entity = theme.
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
        self.rl = RateLimiter(hz=1.0)

    def chunks(self) -> list[str]:
        return list(settings.GDELT_THEMES.keys())

    def _timeline(self, query: str, mode: str) -> pd.DataFrame:
        self.rl.wait()
        start = settings.BACKFILL_START.replace("-", "") + "000000"
        params = {"query": query, "mode": mode, "format": "json",
                  "startdatetime": start, "enddatetime": "20991231000000",
                  "timelinesmooth": "0"}
        r = self.session.get(DOC, params=params, timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200 or not r.text.strip().startswith("{"):
            return pd.DataFrame()
        tl = r.json().get("timeline", [])
        if not tl:
            return pd.DataFrame()
        data = tl[0].get("data", [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
        return df.dropna(subset=["date"])[["date", "value"]]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        theme = chunk
        query = settings.GDELT_THEMES[theme]
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
            entity=theme,
            source_url=f"{DOC}?query={query}&mode=timelinevol",
            vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"theme": theme, "rows": table.num_rows, "path": path}
