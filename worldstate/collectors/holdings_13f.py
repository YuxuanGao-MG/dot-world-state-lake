"""SEC 13F institutional holdings — what major managers own (keyless).

For a curated list of notable managers: resolve name -> CIK via browse-edgar,
read their 13F-HR filings since BACKFILL_START, parse the INFORMATION TABLE XML
(one row per holding). PIT: knowledge_time = filing acceptance; event_time =
report period (quarter end). entity = manager. One shard per manager.
"""
from __future__ import annotations

import re
import lxml.etree as ET
import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

BROWSE = "https://www.sec.gov/cgi-bin/browse-edgar"
SUBS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
INDEX = "https://www.sec.gov/Archives/edgar/data/{cik}/{accn}/index.json"
FILE = "https://www.sec.gov/Archives/edgar/data/{cik}/{accn}/{doc}"


def _slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")


class Holdings13F(Collector):
    domain = "ownership"
    source = "sec_13f"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=settings.SEC_RATE_LIMIT_HZ)

    def chunks(self) -> list[str]:
        return list(settings.MANAGERS_13F)

    def _resolve_cik(self, name: str):
        self.rl.wait()
        try:
            r = self.session.get(BROWSE, params={"action": "getcompany", "company": name,
                                                 "type": "13F-HR", "output": "atom", "count": "5"},
                                 timeout=settings.HTTP_TIMEOUT)
            m = re.search(r"<cik>(\d+)</cik>", r.text)
            return int(m.group(1)) if m else None
        except Exception:
            return None

    def _info_table_rows(self, cik: int, accn: str) -> list[dict]:
        self.rl.wait()
        ij = self.session.get(INDEX.format(cik=cik, accn=accn), timeout=settings.HTTP_TIMEOUT)
        if ij.status_code != 200:
            return []
        xmls = [x["name"] for x in ij.json().get("directory", {}).get("item", [])
                if x["name"].endswith(".xml") and "primary_doc" not in x["name"]]
        rows = []
        for doc in xmls:
            self.rl.wait()
            try:
                r = self.session.get(FILE.format(cik=cik, accn=accn, doc=doc),
                                     timeout=settings.HTTP_TIMEOUT)
                root = ET.fromstring(r.content)
            except Exception:
                continue
            for it in root.findall(".//{*}infoTable"):
                rows.append({
                    "issuer": (it.findtext("{*}nameOfIssuer") or "").strip(),
                    "cusip": (it.findtext("{*}cusip") or "").strip(),
                    "value": it.findtext("{*}value") or "",
                    "shares": it.findtext(".//{*}sshPrnamt") or "",
                    "put_call": (it.findtext("{*}putCall") or "").strip(),
                })
        return rows

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        name = chunk
        path = hfstore.shard_path(self.domain, self.source, f"manager={_slug(name)}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"manager": name, "skipped": True}
        cik = self._resolve_cik(name)
        if not cik:
            return {"manager": name, "no_cik": True}

        self.rl.wait()
        r = self.session.get(SUBS.format(cik=cik), timeout=settings.HTTP_TIMEOUT)
        if r.status_code != 200:
            return {"manager": name, "status": r.status_code}
        rec = r.json().get("filings", {}).get("recent", {})
        forms = rec.get("form", [])
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        all_rows = []
        for i, form in enumerate(forms):
            if form != "13F-HR":
                continue
            fdate = pd.to_datetime(rec["filingDate"][i], utc=True, errors="coerce")
            if pd.isna(fdate) or fdate < start:
                continue
            accn = rec["accessionNumber"][i].replace("-", "")
            period = pd.to_datetime(rec.get("reportDate", [None] * len(forms))[i],
                                    utc=True, errors="coerce")
            accepted = pd.to_datetime(rec.get("acceptanceDateTime", [None] * len(forms))[i],
                                      utc=True, errors="coerce")
            ktime = accepted if pd.notna(accepted) else fdate
            evt = period if pd.notna(period) else fdate
            for row in self._info_table_rows(cik, accn):
                row["_ev"], row["_k"] = evt, ktime
                all_rows.append(row)
        if not all_rows:
            return {"manager": name, "rows": 0, "empty": True}

        df = pd.DataFrame(all_rows)
        payload = pd.DataFrame({
            "issuer": df["issuer"], "cusip": df["cusip"],
            "value_usd_thousands": pd.to_numeric(df["value"], errors="coerce"),
            "shares": pd.to_numeric(df["shares"], errors="coerce"),
            "put_call": df["put_call"],
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=df["_ev"], knowledge_time=df["_k"],
            entity=name, source_url=SUBS.format(cik=cik), vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"manager": name, "cik": cik, "rows": table.num_rows, "path": path}
