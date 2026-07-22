"""Central configuration. Values that differ between environments come from env
vars so the same code runs identically on a GitHub Actions runner and anywhere
else. Nothing here is a secret — secrets (HF_TOKEN, FRED_API_KEY) are read from
the environment at call sites only.
"""
from __future__ import annotations

import os

# --- Hugging Face dataset repo (the canonical store) ------------------------
# Set as a GitHub Actions repo *variable* HF_DATASET_REPO, e.g. "user/world-state-lake".
HF_DATASET_REPO = os.environ.get("HF_DATASET_REPO", "")
HF_REPO_TYPE = "dataset"

# Layout inside the HF repo. Hive-style partitioning so DuckDB / datasets can
# partition-prune on domain, source and date.
DATA_PREFIX = "data"  # data/domain=.../source=.../year=.../part-*.parquet

# --- Corpus scope (first sample) --------------------------------------------
BACKFILL_START = os.environ.get("BACKFILL_START", "2020-01-01")

# US market universe. The full list is fetched from SEC company_tickers.json by
# scripts/build_universe.py; this seed keeps smoke tests fast and offline-ish.
SEED_UNIVERSE = [
    # broad-market / sector ETFs
    "SPY", "QQQ", "IWM", "DIA", "VTI", "XLF", "XLK", "XLE", "XLV", "XLY",
    "TLT", "IEF", "HYG", "LQD", "GLD", "SLV", "USO", "UUP", "VXX",
    # mega-cap single names
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B",
    "JPM", "V", "UNH", "XOM", "JNJ", "WMT", "PG", "HD", "BAC", "KO",
]

# --- Macro series (FRED/ALFRED vintages) ------------------------------------
# Broad US macro. ALFRED gives the real vintage history so knowledge_time is the
# first-release date, not the latest revision. Daily series (rates/FX/commod/vol)
# have effectively one vintage; monthly/quarterly ones carry true revisions.
MACRO_SERIES = [
    # activity & prices
    "GDPC1", "GDP", "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE",
    "UNRATE", "PAYEMS", "INDPRO", "HOUST", "PERMIT", "RSAFS", "UMCSENT",
    "M2SL", "ICSA", "PPIACO", "RRPONTSYD",
    # rates & curve
    "DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS5", "DGS7",
    "DGS10", "DGS20", "DGS30", "T10Y2Y", "T10Y3M", "DFF", "FEDFUNDS",
    "SOFR", "MORTGAGE30US",
    # inflation expectations
    "T10YIE", "T5YIFR",
    # FX
    "DEXUSEU", "DEXJPUS", "DEXCHUS", "DEXUSUK", "DTWEXBGS",
    # commodities
    "DCOILWTICO", "DCOILBRENTEU", "GOLDAMGBD228NLBM", "DHHNGSP",
    # credit & volatility
    "VIXCLS", "BAMLH0A0HYM2", "BAMLC0A0CM",
]

# --- Crypto (CoinGecko, keyless, US-accessible; full daily history per call) --
CRYPTO_IDS = {
    "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "binancecoin": "BNB",
    "ripple": "XRP", "cardano": "ADA", "dogecoin": "DOGE", "avalanche-2": "AVAX",
    "chainlink": "LINK", "polkadot": "DOT", "tron": "TRX", "litecoin": "LTC",
}

# --- News themes (GDELT DOC 2.0 timeline API, keyless): daily volume + tone ----
GDELT_THEMES = {
    "recession": "recession",
    "inflation": "inflation",
    "federal_reserve": "\"federal reserve\"",
    "interest_rates": "\"interest rates\"",
    "stock_market": "\"stock market\"",
    "layoffs": "layoffs",
    "banking_crisis": "\"banking crisis\"",
    "oil_prices": "\"oil prices\"",
    "supply_chain": "\"supply chain\"",
    "unemployment": "unemployment",
    "ai_technology": "\"artificial intelligence\"",
    "geopolitics_war": "war",
    "tariffs_trade": "tariffs",
    "housing_market": "\"housing market\"",
}

# --- Headlines / discussion (Hacker News Algolia, keyless, full history) -------
HN_QUERIES = {
    "federal_reserve": "Federal Reserve",
    "inflation": "inflation",
    "recession": "recession",
    "stock_market": "stock market",
    "interest_rates": "interest rates",
    "bitcoin": "bitcoin",
    "ethereum": "ethereum",
    "layoffs": "layoffs",
    "banking_crisis": "bank collapse",
    "oil_energy": "oil prices",
    "ai": "artificial intelligence",
    "housing": "housing market",
    "tariffs": "tariffs",
    "unemployment": "unemployment",
}

# --- Attention (Wikipedia daily pageviews, keyless): interest per topic --------
WIKI_ARTICLES = [
    "Inflation", "Recession", "Federal_Reserve", "S&P_500", "Nasdaq",
    "Bitcoin", "Ethereum", "Stock_market_crash", "Yield_curve",
    "Quantitative_easing", "Unemployment", "Consumer_price_index",
    "Interest_rate", "Bear_market", "Bull_market", "Cryptocurrency",
]

# --- HTTP politeness ---------------------------------------------------------
# SEC requires a descriptive UA with contact; be a good citizen everywhere.
USER_AGENT = os.environ.get(
    "WORLDSTATE_UA", "world-state-lake research (yuxuangao826@hotmail.com)"
)
SEC_RATE_LIMIT_HZ = 8.0   # SEC allows 10 req/s; stay under.
HTTP_TIMEOUT = 30

# --- EDGAR full-text (filing bodies) ----------------------------------------
EDGAR_FULLTEXT_FORMS = ["8-K", "10-K", "10-Q"]   # material events + core disclosures
EDGAR_TEXT_MAXLEN = 800_000                       # cap per-doc text to bound shard size
