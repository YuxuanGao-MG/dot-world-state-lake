# DoT — Financial World-State Lake

A **point-in-time (bitemporal) financial world-state corpus** for agent RL, part of
the *Drift of Thought* research program. The goal is not a static dataset but an
**environment an agent can explore**: at any timestamp it can ask *what was the
state of the world as it was actually knowable then* — prices, macro, filings,
news — with **zero lookahead**.

**Compute = GitHub Actions. Storage = a private Hugging Face dataset. Nothing runs locally.**

## Why point-in-time

Agent-RL data is *trajectories*, and a trajectory needs the world to have a state
at each step. That state must reflect only what was knowable at the time — macro
numbers get revised, filings and news have release lags. So every row carries two
timestamps:

| column | meaning |
|---|---|
| `event_time` | when the fact is *about* / was observed (valid time) |
| `knowledge_time` | when it became *knowable* to the world (transaction time) |

An `as_of(t)` query returns only rows with `knowledge_time <= t`, and for revisable
series picks the latest vintage whose `knowledge_time <= t`. Lookahead becomes
structurally impossible. See `worldstate/query.py`.

## Layout

```
worldstate/
  schema.py        # the bitemporal envelope every row shares
  normalize.py     # payload -> envelope table
  hfstore.py       # idempotent Parquet shard upload to HF (deterministic paths)
  query.py         # as_of(t) engine over the HF corpus via DuckDB
  collectors/
    market_yahoo.py   # US equity/ETF daily OHLCV via yfinance (keyless)
    crypto_yahoo.py   # crypto daily OHLCV via yfinance (keyless)
    macro_alfred.py   # FRED/ALFRED macro WITH vintage history (needs FRED_API_KEY)
    edgar.py          # SEC filing event stream (keyless)
    news_gdelt.py     # daily news volume + tone per theme, GDELT DOC 2.0 (keyless)
    wiki_pageviews.py # daily Wikipedia pageviews per topic — attention (keyless)
scripts/
  run_collector.py  # CLI used by the workflows: list | run --chunk | run-all
  build_universe.py # expand US universe from SEC company_tickers.json
  asof_demo.py      # prove PIT: same series, two as-of dates, different vintages
.github/workflows/
  backfill.yml      # manual, per-collector, dynamic matrix (one job per chunk)
  incremental.yml   # daily cron refresh
```

Data on HF is Hive-partitioned: `data/domain=<d>/source=<s>/.../part.parquet`.

## Setup (one time)

In this repo's **Settings → Secrets and variables → Actions**:

- Variable **`HF_DATASET_REPO`** — e.g. `your-hf-username/dot-world-state-lake`
- Secret **`HF_TOKEN`** — a Hugging Face **write** token
- Secret **`FRED_API_KEY`** — free key from https://fred.stlouisfed.org (macro only)

## Run

- **Backfill**: Actions → *backfill* → pick a collector (`macro_alfred`, `edgar`,
  `market_yahoo`) → Run. Re-running resumes (existing shards skipped).
- **Daily**: the *incremental* workflow runs on cron automatically.

## Scope (first sample)

US-focused, **daily** granularity, **2020→present**. Free sources first; paid
adapters (intraday, premium news) slot in for the hundreds-of-GB expansion.
