"""US Treasury auctions / issuance (TreasuryDirect API, keyless).

Supply side of the govt-debt market: every auctioned security with its yield,
bid-to-cover, and size. One shard per security type. event_time = knowledge_time
= auction date. entity = security type (Bill/Note/Bond/TIPS/FRN/CMB).
"""
from __future__ import annotations

import pandas as pd

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

TYPES = ["Bill", "Note", "Bond", "TIPS", "FRN", "CMB"]
URL = "https://www.treasurydirect.gov/TA_WS/securities/{type}"
NUM = ["interestRate", "highYield", "highDiscountRate", "bidToCoverRatio",
       "offeringAmount", "totalAccepted", "totalTendered"]


class TreasuryAuctions(Collector):
    domain = "macro"
    source = "treasury"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=2.0)

    def chunks(self) -> list[str]:
        return list(TYPES)

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        sec_type = chunk
        path = hfstore.shard_path(self.domain, self.source, f"type={sec_type}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"type": sec_type, "skipped": True}

        self.rl.wait()
        r = self.session.get(URL.format(type=sec_type), params={"format": "json"},
                             timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            return {"type": sec_type, "rows": 0, "empty": True}

        df = pd.DataFrame(rows)
        df = df[df.get("auctionDate").notna()]
        ev = pd.to_datetime(df["auctionDate"], utc=True, errors="coerce")
        start = pd.Timestamp(settings.BACKFILL_START, tz="UTC")
        keep = ev.notna() & (ev >= start)
        df, ev = df[keep].reset_index(drop=True), ev[keep].reset_index(drop=True)
        if df.empty:
            return {"type": sec_type, "rows": 0, "empty": True}

        payload = pd.DataFrame({"cusip": df.get("cusip", "").astype(str),
                                "security_term": df.get("securityTerm", "").astype(str),
                                "maturity_date": df.get("maturityDate", "").astype(str)})
        for c in NUM:
            payload[c] = pd.to_numeric(df.get(c), errors="coerce")
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev.values, knowledge_time=ev.values,
            entity=sec_type, source_url=URL.format(type=sec_type), vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"type": sec_type, "rows": table.num_rows, "path": path}
