"""Clinical-trial registrations from ClinicalTrials.gov v2 (keyless).

Biotech/pharma pipeline signal. A trial's first-posted date is immutable ->
clean PIT: event_time = knowledge_time = study first-post date. entity = lead
sponsor. One shard per year (by first-post date).
"""
from __future__ import annotations

import pandas as pd
from datetime import date

from config import settings
from worldstate import store as hfstore, normalize
from worldstate.collectors.base import Collector, RateLimiter

URL = "https://clinicaltrials.gov/api/v2/studies"
FIELDS = ("NCTId,BriefTitle,OverallStatus,Phase,LeadSponsorName,LeadSponsorClass,"
          "StudyFirstPostDate,StudyType,Condition")
MAX_PAGES = 10


class ClinicalTrials(Collector):
    domain = "research"
    source = "clinicaltrials"

    def __init__(self):
        super().__init__()
        self.rl = RateLimiter(hz=4.0)

    def chunks(self) -> list[str]:
        return [str(y) for y in range(int(settings.BACKFILL_START[:4]), date.today().year + 1)]

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        year = chunk
        path = hfstore.shard_path(self.domain, self.source, f"year={year}",
                                  name="part.parquet")
        if not force and hfstore.exists(path):
            return {"year": year, "skipped": True}

        rows, token, pages = [], None, 0
        adv = f"AREA[StudyFirstPostDate]RANGE[{year}-01-01,{year}-12-31] AND AREA[StudyType]Interventional"
        while pages < MAX_PAGES:
            self.rl.wait()
            params = {"pageSize": 1000, "filter.advanced": adv, "fields": FIELDS}
            if token:
                params["pageToken"] = token
            try:
                r = self.session.get(URL, params=params, timeout=60)
            except Exception:
                break
            if r.status_code != 200:
                break
            j = r.json()
            for s in j.get("studies", []):
                ps = s.get("protocolSection", {})
                idm = ps.get("identificationModule", {})
                stm = ps.get("statusModule", {})
                dm = ps.get("designModule", {})
                sm = ps.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
                cm = ps.get("conditionsModule", {})
                rows.append({
                    "nct_id": idm.get("nctId", ""),
                    "title": (idm.get("briefTitle", "") or "")[:300],
                    "status": stm.get("overallStatus", ""),
                    "first_post": (stm.get("studyFirstPostDateStruct", {}) or {}).get("date", ""),
                    "phase": ", ".join(dm.get("phases", []) or []),
                    "sponsor": sm.get("name", ""),
                    "sponsor_class": sm.get("class", ""),
                    "conditions": ", ".join((cm.get("conditions", []) or [])[:5]),
                })
            token = j.get("nextPageToken")
            pages += 1
            if not token:
                break

        if not rows:
            return {"year": year, "rows": 0, "empty": True}
        df = pd.DataFrame(rows)
        ev = pd.to_datetime(df["first_post"], utc=True, errors="coerce")
        keep = ev.notna()
        payload = df[["nct_id", "title", "status", "phase", "sponsor",
                      "sponsor_class", "conditions"]][keep].reset_index(drop=True)
        table = normalize.to_table(
            domain=self.domain, source=self.source, payload=payload,
            event_time=ev[keep].values, knowledge_time=ev[keep].values,
            entity=df["sponsor"][keep].values, source_url=URL, vintage_id="",
        )
        hfstore.upload_table(table, path, overwrite=force)
        return {"year": year, "rows": table.num_rows, "path": path}
