# DoT World-State Lake — Handoff / Continuity Doc

Read this to pick up the project from anywhere (phone, new Claude session, new machine).
Repo: **github.com/YuxuanGao-MG/dot-world-state-lake** (public code) · Data: **private S3 `dot-financial-world-env`** (us-east-2).

---

## What this is
A **point-in-time (bitemporal) financial world-state corpus** for training agents with RL, under the *Drift of Thought* research. The goal is an *environment an agent can explore*: at any timestamp query "what was knowable then" — prices, macro, filings, news, positioning — with **zero lookahead**.

**Every row carries two timestamps:** `event_time` (when the fact is about) and `knowledge_time` (when it became public). An `as_of(t)` query returns only rows with `knowledge_time <= t`, picking the latest vintage for revisable series. That's the whole value — lookahead is structurally impossible.

## Architecture (one paragraph)
**GitHub Actions = compute. AWS S3 = storage. Nothing runs locally.** Each collector is a small Python class that fetches → normalizes to the bitemporal envelope → writes a Parquet shard to S3. Writes are idempotent (skip if the object exists) = the checkpoint. A `backfill` workflow fans out one job per chunk (matrix, parallel 16); an `incremental` cron refreshes daily. DuckDB queries the Parquet directly over `s3://`.

## Key facts / credentials (already configured)
- **S3 bucket:** `dot-financial-world-env` (region `us-east-2`, private, SSE-S3). IAM user `ClaudeCode`.
- **GitHub Actions secrets:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `FRED_API_KEY`, `HF_TOKEN` (HF is now just an optional mirror).
- **GitHub Actions variables:** `S3_BUCKET`, `AWS_REGION`, `HF_DATASET_REPO`.
- **Storage backend** is a dispatcher (`worldstate/store.py`): `STORAGE_BACKEND=s3` (default) or `hf`.

## Live collectors (backend-agnostic, in `worldstate/collectors/`)
market_yahoo · crypto_yahoo · macro_alfred (FRED vintages) · edgar (filing index) · edgar_fulltext (filing bodies) · fundamentals_sec (XBRL) · news_gdelt · news_hn · wiki_pageviews

---

## How to operate it FROM YOUR PHONE
Use the **GitHub mobile app / github.com** — no terminal needed:

1. **See current data size:** repo → **Actions** tab → **status** workflow → **Run workflow** → open the run → read the log (prints MB per source + total).
2. **Backfill a source:** Actions → **backfill** → **Run workflow** → pick a `collector` from the dropdown → Run. Re-running is safe (idempotent; existing shards skip). Add `force=true` to overwrite.
3. **Check progress:** Actions tab shows every run green/red. Red = open it, read the failing job log.
4. **Daily refresh** runs automatically (incremental cron, 08:00 UTC).

## How to resume with Claude Code (new session)
Point Claude at the repo and say: *"Continue the DoT world-state lake — read HANDOFF.md and ROADMAP below, then build the next unchecked tier."* Claude has memory of this project too (`project_dot_world_state_lake`).

Common commands (terminal):
```
gh workflow run backfill.yml -R YuxuanGao-MG/dot-world-state-lake -f collector=<name> -f universe_top=1500
gh workflow run status.yml   -R YuxuanGao-MG/dot-world-state-lake
gh run list -R YuxuanGao-MG/dot-world-state-lake -L 10
```
Add a collector: create `worldstate/collectors/<x>.py` (subclass `Collector`, impl `chunks()` + `run_chunk()`), register in `scripts/run_collector.py`, add to both workflows' collector lists. Validate the source API before firing (lesson: Stooq is JS-walled, CoinGecko needs a key, GDELT 429s — always probe first).

---

## ROADMAP — expansion tiers (check off as built)

### Tier 1 — free, PIT-clean, high signal (positioning / ownership / policy)
- [x] `insider_form4` — SEC Form 4 insider buys/sells (EDGAR XML) ✅ built+firing
- [x] `holdings_13f` — SEC 13F institutional holdings (EDGAR) ✅ built+firing
- [ ] `stakes_13dg` — SEC 13D/13G activist / >5% stakes (EDGAR)
- [x] `short_finra` — FINRA daily short volume (keyless CDN files) ✅ built+firing
- [x] `cftc_cot` — CFTC Commitments of Traders, weekly positioning (Socrata API) ✅ built+firing
- [x] `fed_text` — FOMC statements+minutes (federalreserve.gov) ✅ built+firing
- [x] `treasury_auctions` — Treasury issuance/auctions (TreasuryDirect API) ✅ built+firing

### Tier 2 — free-with-key / light engineering (real-economy + derivatives)
- [~] `eia_energy` — oil/gas inventories & production (EIA API) — built, NEEDS free EIA_API_KEY secret
- [!] `options_cboe` — free historical options is hard; yfinance gives only CURRENT chains (forward-accruing only; deferred)
- [!] `earnings_calls` — transcripts: no clean free historical source (needs paid API or heavy scrape; deferred)
- [~] `google_trends` — pytrends works but fragile/rate-limited; low marginal value vs Wikipedia (optional)
- [!] `congress_trades` — STOCK Act — easy public datasets now 403; needs official PDF parsing (deferred)
- [x] `crypto_onchain` — BTC on-chain metrics (Blockchain.com, keyless) ✅ built+firing

### Tier 3 — engineering bets (biggest long-term payoff)
- [!] `gdelt_gkg` article-level = GDELT 429-blocked; full press BODIES at scale = Common Crawl News (heavy, future)
- [x] `security_master` — SEC identity + S&P500 PIT membership (kills survivorship bias) ✅ built+firing
- [ ] `entity_graph` — link filings/news/prices to canonical entities; extract supply-chain/customer/competitor from 10-K text (NLP)
- [x] `surprise_index` — derived econ-surprise from our macro first-release (reads S3) ✅ built+firing
- [ ] `feature_layer` — rolling vol/corr, factor exposures, event windows, sentiment aggregates

### After data: the agent layer
- [x] financial-agent GYM: worldstate/env/ (WorldStateEnv reset/step + FastAPI server + DataApprovalTask) — see ENV.md ✅ v0
- [x] tiered access + tools (basic/pro, budget) — worldstate/env/tools.py ✅
- [x] trajectory logging -> domain=trajectories/source=env (RL training data) ✅
- [x] richer tasks: TradingTask (PnL), ForecastTask ✅
- [x] LLM agent policy via OpenMesh/OpenRouter (worldstate/env/llm_agent.py) ✅ — real LLM trajectories
- [ ] **data-approval** case study (the original use case)

## Status log (update as we go)
- 2026-07-22: Migrated storage HF → S3. Rebuilt 9 collectors to S3 at parallel-16. FRED key added (macro live). Starting Tier 1.

### World-expansion (PIT-clean, beyond strictly financial)
- [x] `defillama` — DeFi TVL + stablecoin supply (keyless; daily on-chain snapshots)
- [x] `usgs_quakes` — significant earthquakes (keyless; event origin time)
- [x] `arxiv_papers` — finance/econ research papers (keyless; submission date)
- [x] `predict_manifold` — prediction-market probability trajectories (keyless; the purest PIT — crowd beliefs about the future)
- [x] `predict_polymarket` — real-money market probabilities (keyless CLOB history)
- [x] `nasa_events` — NASA EONET natural-disaster events (keyless)
- [ ] next: PubMed/USPTO, GitHub activity, Wikidata graph, global macro (vintage-aware only)

### PIT governance (avoid env contamination)
- **`PIT.md`** classifies every source by trust: immutable/vintage/derived (historical-safe) vs forward_limited/revised_soft/snapshot_forward (forward-only/caution) vs excluded_hazard. Auto-generated from `worldstate/provenance.py` via `scripts.gen_pit_doc`.
- The `status` workflow now prints each source's `pit_class`. Historical training episodes must use only historical-safe classes; the bitemporal knowledge_time enforces it.
- [x] `defillama_flows` (DEX volume/fees/revenue; revised_soft) ; [~] `predict_kalshi` (keyless public API works, candle windows need debug)

### Deep crypto + prediction markets (institutional interest)
- [x] `crypto_funding` — OKX perp funding rates (positioning/basis; ~3mo depth, accrues forward)
- [x] `crypto_vol` — Deribit DVOL implied-vol index (crypto VIX; BTC/ETH)
- [x] `crypto_fng` — crypto Fear & Greed sentiment (full history)
- [x] deepened predict_manifold (17 topics) + predict_polymarket (8 pages)
- [ ] next: DefiLlama DEX-volume/fees, Metaculus (403 — needs headers), Kalshi (auth via existing creds)
