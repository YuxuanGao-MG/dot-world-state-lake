"""Security master — the reference backbone (keyless).

Two record kinds:
  * identity : ticker <-> CIK <-> name <-> exchange (SEC), the canonical entity map
    everything else links to.
  * sp500    : S&P 500 membership as point-in-time — current members PLUS the
    dated add/remove changes (Wikipedia), so an as_of(t) query can reconstruct
    index membership on any date (kills survivorship bias).

entity = ticker. Identity is a current snapshot; membership changes are stamped
with their effective date.
"""
from __future__ import annotations

import io
import pandas as pd
from datetime import datetime, timezone

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

IDENTITY_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


class SecurityMaster(Collector):
    domain = "reference"
    source = "master"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=2.0)

    def chunks(self) -> list[str]:
        return ["identity", "sp500"]

    def _now(self):
        return pd.Timestamp(datetime.now(timezone.utc))

    def _identity(self, force: bool) -> dict:
        path = hfstore.shard_path(self.domain, self.source, "kind=identity",
                                  name="part.parquet")
        self.rl.wait()
        r = self.session.get(IDENTITY_URL, timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        df = pd.DataFrame(j["data"], columns=j["fields"])  # cik, name, ticker, exchange
        now = self._now()
        payload = pd.DataFrame({
            "cik": df["cik"].astype("int64").astype(str),
            "name": df["name"].astype(str),
            "exchange": df["exchange"].astype(str),
            "record_type": "identity",
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=now, knowledge_time=now,
            entity=df["ticker"].astype(str).values, source_url=IDENTITY_URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=True)  # snapshot: always refresh
        return {"kind": "identity", "rows": table.num_rows, "path": path}

    def _sp500(self, force: bool) -> dict:
        path = hfstore.shard_path(self.domain, self.source, "kind=sp500",
                                  name="part.parquet")
        self.rl.wait()
        r = self.session.get(SP500_URL, timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        tables = pd.read_html(io.StringIO(r.text))
        now = self._now()
        recs = []  # (ticker, record_type, action, security, sector, reason, event_time, know)

        cur = tables[0]
        for _, row in cur.iterrows():
            recs.append((str(row.get("Symbol", "")), "current_member", "member",
                         str(row.get("Security", "")), str(row.get("GICS Sector", "")),
                         "", now, now))

        chg = tables[1]
        chg.columns = ["_".join([str(c) for c in col]).strip("_") if isinstance(col, tuple)
                       else str(col) for col in chg.columns]
        for _, row in chg.iterrows():
            eff = pd.to_datetime(row.get(chg.columns[0]), utc=True, errors="coerce")
            if pd.isna(eff):
                continue
            reason = str(row.get("Reason", "") or "")
            add_t = str(row.get("Added_Ticker", "") or "")
            rem_t = str(row.get("Removed_Ticker", "") or "")
            if add_t and add_t != "nan":
                recs.append((add_t, "change", "added", str(row.get("Added_Security", "")),
                             "", reason, eff, eff))
            if rem_t and rem_t != "nan":
                recs.append((rem_t, "change", "removed", str(row.get("Removed_Security", "")),
                             "", reason, eff, eff))

        df = pd.DataFrame(recs, columns=["ticker", "record_type", "action", "security",
                                         "sector", "reason", "event_time", "know"])
        payload = pd.DataFrame({
            "index_name": "SP500", "record_type": df["record_type"], "action": df["action"],
            "security": df["security"], "sector": df["sector"], "reason": df["reason"],
        })
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=df["event_time"], knowledge_time=df["know"],
            entity=df["ticker"].values, source_url=SP500_URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=True)
        return {"kind": "sp500", "rows": table.num_rows, "path": path}

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        return self._identity(force) if chunk == "identity" else self._sp500(force)
