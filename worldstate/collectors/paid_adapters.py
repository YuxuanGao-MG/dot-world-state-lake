"""Paid-source connection points — wired but blank until a key is added.

Each adapter documents its provider, the auth env var (a GitHub Actions secret),
the endpoint, and where to sign up. With no key it skips gracefully (green run);
once the secret is set, fill in `_fetch()` and it goes live. This keeps the
premium categories (options/IV, transcripts, estimates, alt-data) plugged into
the same collector framework, ready to activate.
"""
from __future__ import annotations

import os

from worldstate.collectors.base import Collector


class PaidAdapter(Collector):
    provider = ""       # vendor name
    env_key = ""        # GitHub Actions secret to set
    endpoint = ""       # base API endpoint
    signup = ""         # where to get a key
    what = ""           # what data it provides
    domain = "premium"
    source = ""

    def chunks(self) -> list[str]:
        return ["_pending_key"]

    def _fetch(self, chunk, key, force):
        # TODO: implement using self.endpoint + key -> normalize.to_table -> hfstore.upload_table
        return {"not_implemented": True, "provider": self.provider,
                "hint": "key present; implement _fetch() in this adapter"}

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        key = os.environ.get(self.env_key)
        if not key:
            return {"skipped_no_key": True, "provider": self.provider,
                    "env_key": self.env_key, "signup": self.signup, "what": self.what}
        return self._fetch(chunk, key, force)


class OptionsPolygon(PaidAdapter):
    """Equity options chains + implied vol + greeks."""
    provider = "Polygon.io"; env_key = "POLYGON_API_KEY"
    endpoint = "https://api.polygon.io/v3/snapshot/options/{ticker}"
    signup = "https://polygon.io/dashboard/keys"; what = "options chains / IV / greeks"
    domain = "options"; source = "polygon"


class IntradayPolygon(PaidAdapter):
    """Minute-level intraday bars for equities/crypto."""
    provider = "Polygon.io"; env_key = "POLYGON_API_KEY"
    endpoint = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}"
    signup = "https://polygon.io/dashboard/keys"; what = "intraday minute bars"
    domain = "market"; source = "polygon_intraday"


class TranscriptsFMP(PaidAdapter):
    """Earnings-call transcripts."""
    provider = "Financial Modeling Prep"; env_key = "FMP_API_KEY"
    endpoint = "https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}"
    signup = "https://site.financialmodelingprep.com/developer/docs"
    what = "earnings-call transcripts"
    domain = "fundamentals"; source = "fmp_transcripts"


class EstimatesFMP(PaidAdapter):
    """Analyst estimates & revisions."""
    provider = "Financial Modeling Prep"; env_key = "FMP_API_KEY"
    endpoint = "https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}"
    signup = "https://site.financialmodelingprep.com/developer/docs"
    what = "analyst estimates / revisions / price targets"
    domain = "fundamentals"; source = "fmp_estimates"


class NewsTiingo(PaidAdapter):
    """Premium financial news with entity tagging."""
    provider = "Tiingo"; env_key = "TIINGO_API_KEY"
    endpoint = "https://api.tiingo.com/tiingo/news"
    signup = "https://www.tiingo.com/account/api/token"
    what = "premium financial news (tagged)"
    domain = "news"; source = "tiingo"


class ShippingDatalastic(PaidAdapter):
    """Vessel positions / AIS (alt-data: trade & supply-chain flows)."""
    provider = "Datalastic"; env_key = "DATALASTIC_API_KEY"
    endpoint = "https://api.datalastic.com/api/v0/vessel_history"
    signup = "https://datalastic.com/"; what = "AIS vessel tracking (shipping/trade flows)"
    domain = "alt"; source = "shipping_ais"


PAID_ADAPTERS = {
    "options_polygon": OptionsPolygon,
    "intraday_polygon": IntradayPolygon,
    "transcripts_fmp": TranscriptsFMP,
    "estimates_fmp": EstimatesFMP,
    "news_tiingo": NewsTiingo,
    "shipping_ais": ShippingDatalastic,
}
