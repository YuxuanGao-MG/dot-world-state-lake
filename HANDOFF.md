# DoT World-State Lake — Handoff / Continuity Doc

Single entry point to pick up this project from anywhere (phone, new Claude session, new machine).
**Repo:** github.com/YuxuanGao-MG/dot-world-state-lake (public code) · **Data:** private S3 `dot-financial-world-env` (us-east-2).
Companion docs: **`PIT.md`** (data trust classification) · **`ENV.md`** (the agent gym) · **`README.md`**.

---

## What this is
A **point-in-time (bitemporal) financial world-state corpus** for training agents with RL, under the *Drift of Thought* research. Every row carries `event_time` (when the fact is about) and `knowledge_time` (when it became public). An `as_of(t)` query returns only rows with `knowledge_time <= t` — so an agent can explore the world *as it was knowable then*, with **zero lookahead**. On top of the data sits a **gym** (an environment agents play in) and a **trajectory pipeline** that turns episodes into RL training data.

## Architecture — cloud-native & autonomous
**GitHub Actions = compute. AWS S3 = storage. Nothing runs locally, and no session needs to stay open.**
Each collector fetches a source → normalizes to the bitemporal envelope → writes a Parquet shard to S3 (idempotent = the checkpoint). The **`incremental` cron runs daily at 08:00 UTC** and refreshes all sources by itself. Close your laptop; it keeps running. DuckDB queries the Parquet directly over `s3://`.

## Configured (nothing to set up)
- **S3:** bucket `dot-financial-world-env` (us-east-2, private, SSE-S3), IAM user `ClaudeCode`.
- **Actions secrets:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `FRED_API_KEY`, `EIA_API_KEY`, `OPENROUTER_API_KEY` (OpenMesh LLM API), `HF_TOKEN` (optional mirror).
- **Actions variables:** `S3_BUCKET`, `AWS_REGION`, `HF_DATASET_REPO`.

---

## Operate it FROM YOUR PHONE (GitHub app → Actions tab)
- **`status`** → Run → read the log: S3 size + `pit_class` per source (flags forward-only sources).
- **`backfill`** → Run → pick a `collector` → runs one source (idempotent; `force=true` overwrites).
- **`collect-trajectories`** → Run → pick task/agent/model → writes RL trajectories to S3. `agent=llm` + a `model` uses a real LLM.
- **`env-demo`** → Run → watch a baseline agent play the gym.
- Red run? Open it, read the failing job log. Daily `incremental` refresh is automatic.

## Resume with Claude Code (new session)
Say: *"Continue the DoT world-state lake — read HANDOFF.md, PIT.md, ENV.md, then pick up the roadmap."* Claude also has memory (`project_dot_world_state_lake`). Terminal:
```
gh workflow run backfill.yml -R YuxuanGao-MG/dot-world-state-lake -f collector=<name> [-f universe_top=1500]
gh workflow run status.yml   -R YuxuanGao-MG/dot-world-state-lake
gh run list -R YuxuanGao-MG/dot-world-state-lake -L 10
```
Add a collector: `worldstate/collectors/<x>.py` (subclass `Collector`, `chunks()`+`run_chunk()`), register in `scripts/run_collector.py`, add to both workflows, **classify it in `worldstate/provenance.py`**, then `python -m scripts.gen_pit_doc`. Always probe the source API before firing.

---

## The 30 collectors (by dimension)
- **Prices:** market_yahoo (US equities/ETFs), crypto_yahoo
- **Crypto (deep):** crypto_onchain (BTC on-chain), defillama + defillama_flows (DeFi TVL/DEX/fees), crypto_funding (OKX funding), crypto_vol (Deribit DVOL implied vol), crypto_fng (Fear & Greed)
- **Macro:** macro_alfred (FRED vintages), surprise_index (derived), treasury_auctions, eia_energy
- **SEC:** edgar (index), edgar_fulltext (bodies), fundamentals_sec (XBRL), insider_form4, holdings_13f
- **Positioning / policy:** cftc_cot, short_finra, fed_text (FOMC)
- **Prediction markets:** predict_kalshi (regulated), predict_polymarket (real-money), predict_manifold (crowd)
- **News / attention / sentiment:** news_gdelt, news_hn, wiki_pageviews
- **Events / research:** usgs_quakes, nasa_events, arxiv_papers
- **Reference:** security_master (identity + S&P 500 PIT membership)

## PIT governance (see `PIT.md`)
Every source is classified in `worldstate/provenance.py`: **immutable / vintage / derived** (historical-safe) vs **forward_limited / revised_soft / snapshot_forward** (forward-only, caution) vs **excluded_hazard** (never collected — World Bank/IMF/OWID, weather reanalysis). Historical training episodes should use only historical-safe classes; the bitemporal `knowledge_time` enforces it automatically. The `status` workflow prints each source's class.

## Agent gym (see `ENV.md`)
`worldstate/env/`: `WorldStateEnv` (reset/step, PIT observations), tasks (`DataApprovalTask`, `TradingTask`, `ForecastTask`), tiered tools (basic/pro + budget — tool-calls are intermediate steps → multi-step trajectories), `TrajectoryLogger` → `domain=trajectories/source=env`, FastAPI `server.py`, and `llm_agent.py` (real LLM policy via OpenMesh/OpenRouter: gemini-3-flash, deepseek-v4-flash, claude-sonnet-4.6, kimi-k2.6, gpt-5.4, …). The `prediction_market` pro-tool spans all 3 venues.

## Roadmap / next ideas
- Snapshot-forward reusable pattern (World Bank etc. — PIT-safe from now on).
- Prediction-market **resolutions** (ground truth) → a calibration training task.
- More crypto depth (per-protocol TVL, options IV surface, exchange flows).
- Wire the LLM agent loop at scale → large multi-model trajectory corpora → drift-of-thought analysis on the traces.

## Status log
- 2026-07-22: Storage on S3; **30 collectors** live (incl. deep crypto + Kalshi/Polymarket/Manifold prediction markets); **PIT provenance registry + PIT.md**; gym + tiered tools + LLM-agent (OpenMesh) + trajectory pipeline. Daily cron autonomous. ~3+ GB and growing.
