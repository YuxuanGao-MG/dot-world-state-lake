"""PIT provenance registry — the single source of truth for how far each source
can be TRUSTED historically, so the environment is never contaminated.

Classes (weakest guarantee last):
  immutable       Timestamped events/observations, never revised. Full historical
                  backfill is PIT-correct; knowledge_time = the fact's true public time.
  vintage         Revisable, but we capture true release vintages (e.g. ALFRED
                  first-release). PIT-correct with revision history.
  derived         Computed from other lake data; PIT-correct by construction
                  (inherits the knowledge_time of its inputs).
  forward_limited Immutable, but the source API only serves a shallow window; older
                  history is missing and accrues forward. What exists IS PIT.
  revised_soft    Backfilled, but the provider may RESTATE historical values (e.g.
                  DefiLlama re-computing past TVL). Values are best-current-estimate,
                  NOT what was shown at the time. Strict users: treat as snapshot_forward.
  snapshot_forward Current-state snapshot only. knowledge_time = COLLECTION time, so an
                  as_of(past) query correctly returns nothing before we started
                  collecting. No pre-collection history exists.
  excluded_hazard Deliberately NOT collected: revised with no vintages / recomputed
                  after the fact. Backfilling would smuggle lookahead into the lake.

THE DISCIPLINE: snapshot_forward / revised_soft sources MUST stamp
knowledge_time = collection time (never the nominal data date). The bitemporal
envelope then makes contamination impossible — a past-dated query can't see them.
"""
from __future__ import annotations

# (domain, source) -> (pit_class, knowledge_time, note)
SOURCES: dict[tuple[str, str], tuple[str, str, str]] = {
    ("market", "yahoo"):        ("immutable", "trade date + ~close", "daily OHLCV, not revised"),
    ("crypto", "yahoo"):        ("immutable", "day + 1d", "daily OHLCV"),
    ("onchain", "blockchain"):  ("immutable", "day + 1d", "BTC on-chain (block data fixed)"),
    ("macro", "alfred"):        ("vintage", "realtime_start (first release)", "FRED/ALFRED vintages; daily rate series effectively immutable"),
    ("macro", "treasury"):      ("immutable", "auction date", "Treasury issuance"),
    ("events", "edgar"):        ("immutable", "filing date", "SEC filing index"),
    ("filings_text", "edgar"):  ("immutable", "acceptanceDateTime", "SEC filing bodies"),
    ("fundamentals", "sec_xbrl"): ("vintage", "filed date (restatements = new rows)", "as-reported XBRL"),
    ("positioning", "cftc"):    ("immutable", "report date + 3d release", "CFTC COT"),
    ("positioning", "finra_short"): ("immutable", "date + 1d", "FINRA short volume"),
    ("positioning", "sec_form4"): ("immutable", "acceptanceDateTime", "insider transactions"),
    ("ownership", "sec_13f"):   ("vintage", "acceptance (restatements = new)", "13F holdings"),
    ("policy", "fomc"):         ("immutable", "publish date (minutes +21d)", "FOMC text"),
    ("news", "gdelt"):          ("immutable", "publish day", "news tone/volume"),
    ("news", "hackernews"):     ("immutable", "post time", "headlines"),
    ("attention", "wikipedia"): ("immutable", "view day + 1d", "pageviews"),
    ("research", "arxiv"):      ("immutable", "submission date", "papers"),
    ("events", "usgs"):         ("immutable", "origin time", "earthquakes (minor mag revisions)"),
    ("events", "nasa_eonet"):   ("immutable", "observation date", "natural disasters"),
    ("predictions", "manifold"): ("immutable", "bet day", "play-money market probs"),
    ("predictions", "polymarket"): ("immutable", "CLOB day", "real-money market probs"),
    ("predictions", "kalshi"):  ("immutable", "candle day", "regulated event contracts"),
    ("sentiment", "alt_fng"):   ("immutable", "day + 1d", "crypto Fear & Greed"),
    ("crypto_deriv", "okx"):    ("forward_limited", "funding settlement", "~3mo API depth; accrues forward"),
    ("crypto_deriv", "deribit"): ("forward_limited", "day + 1d", "DVOL ~1000d API cap; accrues forward"),
    ("derived", "surprise"):    ("derived", "first-release knowledge_time", "econ surprise from ALFRED"),
    ("trajectories", "env"):    ("derived", "sim cursor", "agent episodes, not world data"),
    # revised / snapshot — historical NOT fully trustworthy
    ("crypto_defi", "defillama"): ("revised_soft", "day + 1d", "DefiLlama may RESTATE past TVL; treat as snapshot_forward if strict"),
    ("crypto_defi", "defillama_flows"): ("revised_soft", "day + 1d", "DEX volume/fees may be restated"),
    ("commodity", "eia"):      ("revised_soft", "period + 4d release", "EIA weekly petroleum/gas estimates get revised"),
    ("policy", "federal_register"): ("immutable", "publication date", "US financial-regulatory documents"),
    ("events", "openfda"):      ("immutable", "report date", "FDA drug recalls/enforcement"),
    ("sentiment", "epu"):       ("revised_soft", "month + 1mo lag", "Economic Policy Uncertainty index (methodology revisions)"),
    ("credit", "fred"):         ("vintage", "realtime_start (first release)", "credit spreads / financial-conditions indices (ALFRED vintages)"),
    ("real_estate", "fred"):    ("vintage", "realtime_start (first release)", "home prices / housing activity (ALFRED vintages)"),
    ("commodity", "fred"):      ("vintage", "realtime_start (first release)", "energy/metals/ag prices (ALFRED vintages)"),
    ("reference", "master"):    ("snapshot_forward", "collection time (identity kind); effective date (sp500 kind)", "SEC identity = current snapshot; S&P500 changes = immutable"),
}

# Sources deliberately excluded (would contaminate). Documented, never collected as-is.
EXCLUDED = {
    "World Bank / IMF / OECD / OWID": "latest-only, revised, no vintage API -> use vintage endpoint or snapshot_forward",
    "Weather reanalysis (ERA5/Open-Meteo historical)": "recomputed after the fact -> use archived forecasts + as-reported obs instead",
    "Wikidata (current)": "state snapshot -> only PIT via revision history / snapshot_forward",
}

# Which classes are safe to feed a HISTORICAL (pre-collection) training episode.
HISTORICAL_SAFE = {"immutable", "vintage", "derived", "forward_limited"}


def pit_class(domain: str, source: str) -> str:
    return SOURCES.get((domain, source), ("unknown", "", ""))[0]


def is_historical_safe(domain: str, source: str) -> bool:
    return pit_class(domain, source) in HISTORICAL_SAFE
