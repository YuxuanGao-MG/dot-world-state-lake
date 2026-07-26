"""SEC Form 4 insider transactions — parsed from the ownership XML (keyless).

Per universe company: every Form 4 since BACKFILL_START, each transaction a row
(who, code, shares, price, acquired/disposed, holdings after). Precisely PIT:
knowledge_time = filing acceptance datetime; event_time = transaction date.
entity = ticker. One shard per ticker; batch committed together.

The shard is a ticker's WHOLE history, so the daily refresh can neither skip it
(new filings would never land) nor rebuild it (~16h of SEC fetches for the full
universe — past the 6h job cap). In `--incremental` mode it instead reads the
existing shard, fetches only filings that became knowable after its last
knowledge_time, and appends them.
"""
from __future__ import annotations

import os
import lxml.etree as ET
import pandas as pd
import pyarrow as pa

from config import settings
from worldstate import store as hfstore, normalize
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


def _txt(node, path):
    el = node.find(path)
    return el.text.strip() if el is not None and el.text else ""


def _parse_form4(raw: bytes, ticker: str) -> list[dict]:
    try:
        root = ET.fromstring(raw)
    except Exception:
        return []
    owner = root.find(".//reportingOwner/reportingOwnerId/rptOwnerName")
    owner_name = owner.text.strip() if owner is not None and owner.text else ""
    rel = root.find(".//reportingOwner/reportingOwnerRelationship")
    is_dir = _txt(rel, "isDirector") if rel is not None else ""
    is_off = _txt(rel, "isOfficer") if rel is not None else ""
    title = _txt(rel, "officerTitle") if rel is not None else ""
    rows = []
    for deriv, tag in ((False, "nonDerivativeTransaction"), (True, "derivativeTransaction")):
        for t in root.iter(tag):
            rows.append({
                "owner_name": owner_name, "is_director": is_dir, "is_officer": is_off,
                "officer_title": title, "is_derivative": deriv,
                "security_title": _txt(t, "securityTitle/value"),
                "txn_date": _txt(t, "transactionDate/value"),
                "txn_code": _txt(t, "transactionCoding/transactionCode"),
                "shares": _txt(t, "transactionAmounts/transactionShares/value"),
                "price": _txt(t, "transactionAmounts/transactionPricePerShare/value"),
                "acq_disp": _txt(t, "transactionAmounts/transactionAcquiredDisposedCode/value"),
                "shares_after": _txt(t, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
            })
    return rows


def _merge(prev: pa.Table, new: pa.Table) -> pa.Table:
    """Append newly-knowable rows to an existing whole-history shard.

    content_hash covers entity + event_time + payload, so a filing re-read inside
    the overlap window collapses onto the row already stored — the merge is
    idempotent and adds no lookahead (every row keeps its own knowledge_time).
    """
    df = pd.concat([prev.to_pandas(), new.to_pandas()], ignore_index=True)
    df = (df.drop_duplicates("content_hash")
            .sort_values(["knowledge_time", "event_time"])
            .reset_index(drop=True))
    try:
        return pa.Table.from_pandas(df, schema=new.schema, preserve_index=False)
    except Exception:
        return pa.Table.from_pandas(df, preserve_index=False)


class InsiderForm4(Collector):
    domain = "positioning"
    source = "sec_form4"

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
        prev = hfstore.read_table(path) if self.incremental else None
        if prev is None and not force and hfstore.exists(path):
            return None

        # Only fetch filings newer than what the shard already holds. The 2-day
        # overlap re-reads the boundary (amendments, same-timestamp filings);
        # content_hash dedupe in _merge drops anything we already had.
        since = None
        if prev is not None and prev.num_rows:
            kt = pd.to_datetime(prev.column("knowledge_time").to_pandas(), utc=True)
            since = kt.max() - pd.Timedelta(days=2)

        self.rl.wait()
        r = self.session.get(SUBS_URL.format(cik=cik), timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        rec = r.json().get("filings", {}).get("recent", {})
        forms = rec.get("form", [])
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        all_rows = []
        for i, form in enumerate(forms):
            if form != "4":
                continue
            fdate = pd.to_datetime(rec["filingDate"][i], utc=True, errors="coerce")
            if pd.isna(fdate) or fdate < start:
                continue
            accepted = pd.to_datetime(rec.get("acceptanceDateTime", [None] * len(forms))[i],
                                      utc=True, errors="coerce")
            ktime = accepted if pd.notna(accepted) else fdate
            # Decided BEFORE the document fetch — this is what keeps the daily
            # refresh to a handful of requests per ticker instead of hundreds.
            if since is not None and ktime <= since:
                continue
            doc = rec["primaryDocument"][i]
            if not doc:
                continue
            accn = rec["accessionNumber"][i].replace("-", "")
            # primaryDocument may be an xsl-rendered path; strip to the raw xml
            raw_doc = doc.split("/")[-1] if doc.endswith(".xml") else doc
            if not raw_doc.endswith(".xml"):
                continue
            self.rl.wait()
            try:
                d = self.session.get(DOC_URL.format(cik=cik, accn=accn, doc=raw_doc),
                                     timeout=settings.HTTP_TIMEOUT)
                if d.status_code != 200:
                    continue
                txns = _parse_form4(d.content, ticker)
            except Exception:
                continue
            for row in txns:
                row["_ktime"] = ktime
                all_rows.append(row)
        if not all_rows:
            return None

        df = pd.DataFrame(all_rows)
        ev = pd.to_datetime(df["txn_date"], utc=True, errors="coerce").fillna(df["_ktime"])
        payload = pd.DataFrame({
            "owner_name": df["owner_name"], "officer_title": df["officer_title"],
            "is_director": df["is_director"], "is_officer": df["is_officer"],
            "is_derivative": df["is_derivative"], "security_title": df["security_title"],
            "txn_code": df["txn_code"],
            "shares": pd.to_numeric(df["shares"], errors="coerce"),
            "price": pd.to_numeric(df["price"], errors="coerce"),
            "acq_disp": df["acq_disp"],
            "shares_after": pd.to_numeric(df["shares_after"], errors="coerce"),
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev, knowledge_time=df["_ktime"],
            entity=ticker, source_url=SUBS_URL.format(cik=cik), vintage_id="",
        )
        added = table.num_rows
        if prev is not None:
            table = _merge(prev, table)
        return table, path, table.num_rows, added

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        idx = int(chunk)
        tickers = self.uni[idx * BATCH:(idx + 1) * BATCH]
        pairs, total, new, done = [], 0, 0, 0
        for t in tickers:
            try:
                res = self._one_ticker(t, force)
            except Exception:
                continue
            if res is None:
                continue
            table, path, rows, added = res
            pairs.append((table, path))
            total += rows
            new += added
            done += 1
        # Incremental rewrites an existing shard in place, so it must overwrite.
        written = hfstore.upload_tables(
            pairs, commit_message=f"form4 batch {chunk} ({done} tickers)",
            overwrite=force or self.incremental) if pairs else 0
        return {"chunk": chunk, "tickers": len(tickers), "written": written,
                "txns": total, "new_txns": new}
