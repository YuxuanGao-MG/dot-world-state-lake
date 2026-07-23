# PIT Provenance — how far each source can be trusted historically

**Auto-generated from `worldstate/provenance.py`. Do not edit by hand.**

PIT provenance registry — the single source of truth for how far each source
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

## Sources by class

### `immutable` — ✅ historical-safe

| source | knowledge_time | note |
|---|---|---|
| `alt/shipping_ais` | ping time | PAID (Datalastic): AIS vessel tracking |
| `attention/wikipedia` | view day + 1d | pageviews |
| `crypto/yahoo` | day + 1d | daily OHLCV |
| `events/edgar` | filing date | SEC filing index |
| `events/nasa_eonet` | observation date | natural disasters |
| `events/openfda` | report date | FDA drug recalls/enforcement |
| `events/usgs` | origin time | earthquakes (minor mag revisions) |
| `filings_text/edgar` | acceptanceDateTime | SEC filing bodies |
| `fundamentals/fmp_estimates` | estimate date | PAID (FMP): analyst estimates/revisions |
| `fundamentals/fmp_transcripts` | call date | PAID (FMP): earnings-call transcripts |
| `macro/treasury` | auction date | Treasury issuance |
| `market/polygon_intraday` | bar time | PAID (Polygon): intraday minute bars |
| `market/yahoo` | trade date + ~close | daily OHLCV, not revised |
| `news/gdelt` | publish day | news tone/volume |
| `news/hackernews` | post time | headlines |
| `news/tiingo` | publish time | PAID (Tiingo): premium news |
| `onchain/blockchain` | day + 1d | BTC on-chain (block data fixed) |
| `options/polygon` | quote/trade time | PAID (Polygon): options chains/IV/greeks |
| `policy/federal_register` | publication date | US financial-regulatory documents |
| `policy/fomc` | publish date (minutes +21d) | FOMC text |
| `positioning/cftc` | report date + 3d release | CFTC COT |
| `positioning/finra_short` | date + 1d | FINRA short volume |
| `positioning/sec_form4` | acceptanceDateTime | insider transactions |
| `predictions/kalshi` | candle day | regulated event contracts |
| `predictions/manifold` | bet day | play-money market probs |
| `predictions/polymarket` | CLOB day | real-money market probs |
| `research/arxiv` | submission date | papers |
| `research/clinicaltrials` | study first-post date | ClinicalTrials.gov registrations |
| `sentiment/alt_fng` | day + 1d | crypto Fear & Greed |

### `vintage` — ✅ historical-safe

| source | knowledge_time | note |
|---|---|---|
| `commodity/fred` | realtime_start (first release) | energy/metals/ag prices (ALFRED vintages) |
| `credit/fred` | realtime_start (first release) | credit spreads / financial-conditions indices (ALFRED vintages) |
| `fundamentals/sec_xbrl` | filed date (restatements = new rows) | as-reported XBRL |
| `macro/alfred` | realtime_start (first release) | FRED/ALFRED vintages; daily rate series effectively immutable |
| `ownership/sec_13f` | acceptance (restatements = new) | 13F holdings |
| `real_estate/fred` | realtime_start (first release) | home prices / housing activity (ALFRED vintages) |

### `derived` — ✅ historical-safe

| source | knowledge_time | note |
|---|---|---|
| `derived/surprise` | first-release knowledge_time | econ surprise from ALFRED |
| `trajectories/env` | sim cursor | agent episodes, not world data |

### `forward_limited` — ✅ historical-safe

| source | knowledge_time | note |
|---|---|---|
| `crypto_deriv/deribit` | day + 1d | DVOL ~1000d API cap; accrues forward |
| `crypto_deriv/okx` | funding settlement | ~3mo API depth; accrues forward |

### `revised_soft` — ⚠️ forward-only / caution

| source | knowledge_time | note |
|---|---|---|
| `commodity/eia` | period + 4d release | EIA weekly petroleum/gas estimates get revised |
| `crypto_defi/defillama` | day + 1d | DefiLlama may RESTATE past TVL; treat as snapshot_forward if strict |
| `crypto_defi/defillama_flows` | day + 1d | DEX volume/fees may be restated |
| `sentiment/epu` | month + 1mo lag | Economic Policy Uncertainty index (methodology revisions) |

### `snapshot_forward` — ⚠️ forward-only / caution

| source | knowledge_time | note |
|---|---|---|
| `reference/master` | collection time (identity kind); effective date (sp500 kind) | SEC identity = current snapshot; S&P500 changes = immutable |

## Deliberately excluded (would contaminate)

| source | why |
|---|---|
| World Bank / IMF / OECD / OWID | latest-only, revised, no vintage API -> use vintage endpoint or snapshot_forward |
| Weather reanalysis (ERA5/Open-Meteo historical) | recomputed after the fact -> use archived forecasts + as-reported obs instead |
| Wikidata (current) | state snapshot -> only PIT via revision history / snapshot_forward |

## Rule for consumers / the env

Historical (pre-collection) training episodes should draw observations ONLY from classes: **derived, forward_limited, immutable, vintage**. `snapshot_forward` / `revised_soft` sources are trustworthy only from their collection date onward; the bitemporal `knowledge_time` enforces this automatically as long as those sources stamp knowledge_time = collection time.
