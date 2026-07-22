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
- [ ] `holdings_13f` — SEC 13F institutional holdings (EDGAR)
- [ ] `stakes_13dg` — SEC 13D/13G activist / >5% stakes (EDGAR)
- [x] `short_finra` — FINRA daily short volume (keyless CDN files) ✅ built+firing
- [x] `cftc_cot` — CFTC Commitments of Traders, weekly positioning (Socrata API) ✅ built+firing
- [ ] `fed_text` — FOMC statements/minutes/speeches/Beige Book (federalreserve.gov)
- [x] `treasury_auctions` — Treasury issuance/auctions (TreasuryDirect API) ✅ built+firing

### Tier 2 — free-with-key / light engineering (real-economy + derivatives)
- [ ] `eia_energy` — oil/gas inventories & production (EIA API, free key)
- [ ] `options_cboe` — put/call ratio, chain snapshots, forward IV surface
- [ ] `earnings_calls` — earnings-call transcripts (FMP/scrape)
- [ ] `google_trends` — search interest (pytrends)
- [ ] `congress_trades` — STOCK Act disclosures
- [ ] `crypto_onchain` — exchange flows, active addrs, stablecoin supply, gas

### Tier 3 — engineering bets (biggest long-term payoff)
- [ ] `news_ccnews` / `gdelt_gkg` — full press bodies at web scale (Common Crawl News / GDELT GKG)
- [ ] `security_master` — PIT index membership + delistings + ticker↔CIK↔CUSIP (kills survivorship bias)
- [ ] `entity_graph` — link filings/news/prices to canonical entities; extract supply-chain/customer/competitor from 10-K text (NLP)
- [ ] `surprise_index` — our own econ-surprise (FRED first-release vs naive forecast)
- [ ] `feature_layer` — rolling vol/corr, factor exposures, event windows, sentiment aggregates

### After data: the agent layer
- [ ] `as_of` tool API + tiered access model (the RL environment surface)
- [ ] **data-approval** case study (the original use case)

## Status log (update as we go)
- 2026-07-22: Migrated storage HF → S3. Rebuilt 9 collectors to S3 at parallel-16. FRED key added (macro live). Starting Tier 1.
