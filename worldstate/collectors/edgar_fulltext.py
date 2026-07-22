"""SEC EDGAR filing FULL TEXT — the disclosure "body" (keyless).

For each universe company we read the SEC submissions index, take core forms
(8-K material events, 10-K/10-Q disclosures) filed since BACKFILL_START, fetch
each primary document and extract plain text. Precisely PIT: knowledge_time =
acceptanceDateTime (the exact instant the filing became public).

One shard per ticker; the whole batch is committed once (spares HF's API).
Text is capped at settings.EDGAR_TEXT_MAXLEN to bound shard size.
"""
from __future__ import annotations

import os
import lxml.html
import pandas as pd

from config import settings
from worldstate import hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

SUBS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
DOC_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accn}/{doc}"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
BATCH = 40


def _universe() -> list[str]:
    path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "universe_us.txt")
    if os.path.exists(path):
        with open(path) as f:
            u = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]
        if u:
            return u
    return settings.SEED_UNIVERSE


def _extract_text(raw: bytes) -> str:
    try:
        txt = lxml.html.fromstring(raw).text_content()
    except Exception:
        txt = raw.decode("utf-8", "ignore")
    txt = " ".join(txt.split())
    return txt[:settings.EDGAR_TEXT_MAXLEN]


class EdgarFulltext(Collector):
    domain = "filings_text"
    source = "edgar"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=settings.SEC_RATE_LIMIT_HZ)
        self.uni = _universe()
        self.cik = self._ticker_cik_map()

    def _ticker_cik_map(self) -> dict:
        try:
            r = self.session.get(TICKERS_URL, timeout=settings.HTTP_TIMEOUT)
            r.raise_for_status()
            return {str(v["ticker"]).upper(): int(v["cik_str"]) for v in r.json().values()}
        except Exception:
            return {}

    def chunks(self) -> list[str]:
        n = (len(self.uni) + BATCH - 1) // BATCH
        return [f"{i:04d}" for i in range(n)]

    def _one_ticker(self, ticker: str, force: bool):
        cik = self.cik.get(ticker)
        if not cik:
            return None
        path = hfstore.shard_path(self.domain, self.source, f"ticker={ticker}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return None

        self.rl.wait()
        r = self.session.get(SUBS_URL.format(cik=cik), timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        rec = r.json().get("filings", {}).get("recent", {})
        forms = rec.get("form", [])
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        rows = []
        for i, form in enumerate(forms):
            if form not in settings.EDGAR_FULLTEXT_FORMS:
                continue
            fdate = pd.to_datetime(rec["filingDate"][i], utc=True, errors="coerce")
            if pd.isna(fdate) or fdate < start:
                continue
            accn = rec["accessionNumber"][i]
            doc = rec["primaryDocument"][i]
            if not doc:
                continue
            url = DOC_URL.format(cik=cik, accn=accn.replace("-", ""), doc=doc)
            self.rl.wait()
            try:
                d = self.session.get(url, timeout=settings.HTTP_TIMEOUT)
                if d.status_code != 200:
                    continue
                text = _extract_text(d.content)
            except Exception:
                continue
            accepted = pd.to_datetime(rec.get("acceptanceDateTime", [None] * len(forms))[i],
                                      utc=True, errors="coerce")
            rows.append({
                "form": form, "accession": accn, "primary_doc": doc, "url": url,
                "text": text, "char_len": len(text),
                "event_time": fdate,
                "knowledge_time": accepted if pd.notna(accepted) else fdate,
            })
        if not rows:
            return None
        df = pd.DataFrame(rows)
        payload = df[["form", "accession", "primary_doc", "url", "text", "char_len"]]
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=df["event_time"], knowledge_time=df["knowledge_time"],
            entity=ticker, source_url=SUBS_URL.format(cik=cik),
            vintage_id=df["accession"].values,
        )
        return table, path, table.num_rows

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        idx = int(chunk)
        tickers = self.uni[idx * BATCH:(idx + 1) * BATCH]
        pairs, total, done = [], 0, 0
        for t in tickers:
            try:
                res = self._one_ticker(t, force)
            except Exception:
                continue
            if res is None:
                continue
            table, path, rows = res
            pairs.append((table, path))
            total += rows
            done += 1
        written = hfstore.upload_tables(
            pairs, commit_message=f"edgar fulltext batch {chunk} ({done} tickers)",
            overwrite=force) if pairs else 0
        return {"chunk": chunk, "tickers": len(tickers), "written": written, "filings": total}
