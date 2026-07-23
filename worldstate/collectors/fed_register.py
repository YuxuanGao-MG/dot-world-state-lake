"""US financial-regulatory actions from the Federal Register (keyless).

Rules/proposed-rules/notices from finance agencies (SEC/Fed/CFTC/OCC/FDIC/CFPB/
Treasury/IRS). Dated publications -> clean PIT: event_time = knowledge_time =
publication date. entity = document type. One shard per year.
"""
from __future__ import annotations

import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://www.federalregister.gov/api/v1/documents.json"
AGENCIES = ["securities-and-exchange-commission", "federal-reserve-system",
            "commodity-futures-trading-commission", "comptroller-of-the-currency",
            "federal-deposit-insurance-corporation", "consumer-financial-protection-bureau",
            "treasury-department", "internal-revenue-service"]


class FederalRegister(Collector):
    domain = "policy"
    source = "federal_register"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=3.0)

    def chunks(self) -> list[str]:
        return [str(y) for y in range(int(settings.BACKFILL_START[:4]), date.today().year + 1)]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year = chunk
        path = hfstore.shard_path(self.domain, self.source, f"year={year}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"year": year, "skipped": True}

        rows, page = [], 1
        while page <= 20:
            self.rl.wait()
            params = [("per_page", 1000), ("page", page),
                      ("conditions[publication_date][gte]", f"{year}-01-01"),
                      ("conditions[publication_date][lte]", f"{year}-12-31"),
                      ("fields[]", "title"), ("fields[]", "type"),
                      ("fields[]", "abstract"), ("fields[]", "publication_date"),
                      ("fields[]", "document_number"), ("fields[]", "html_url"),
                      ("fields[]", "agencies")]
            params += [("conditions[agencies][]", a) for a in AGENCIES]
            try:
                r = self.session.get(URL, params=params, timeout=60)
            except Exception:
                break
            if r.status_code != 200:
                break
            res = r.json().get("results", [])
            if not res:
                break
            rows += res
            if len(res) < 1000:
                break
            page += 1

        if not rows:
            return {"year": year, "rows": 0, "empty": True}
        df = pd.DataFrame(rows)
        ev = pd.to_datetime(df["publication_date"], utc=True, errors="coerce")
        keep = ev.notna()
        payload = pd.DataFrame({
            "title": df["title"].astype(str).str[:300],
            "type": df.get("type", "").astype(str),
            "abstract": df.get("abstract", "").astype(str).str[:1000],
            "document_number": df.get("document_number", "").astype(str),
            "agencies": df["agencies"].apply(
                lambda a: ", ".join(x.get("name", "") for x in a) if isinstance(a, list) else ""),
            "url": df.get("html_url", "").astype(str),
        })[keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev[keep].values, knowledge_time=ev[keep].values,
            entity=payload["type"].values, source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"year": year, "rows": table.num_rows, "path": path}
