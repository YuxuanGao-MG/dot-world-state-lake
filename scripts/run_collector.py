"""CLI used by GitHub Actions (and for smoke tests).

  python -m scripts.run_collector <name> list                 # JSON chunk ids
  python -m scripts.run_collector <name> run --chunk <id> [--force]
  python -m scripts.run_collector <name> run-all [--force]     # loop every chunk

`list` feeds a dynamic Actions matrix; each matrix job runs one `run --chunk`.
"""
from __future__ import annotations

import argparse
import json
import sys

from worldstate import store as hfstore
from worldstate.collectors.market_yahoo import YahooDaily
from worldstate.collectors.macro_alfred import AlfredVintages
from worldstate.collectors.edgar import EdgarIndex
from worldstate.collectors.crypto_yahoo import CryptoYahoo
from worldstate.collectors.news_gdelt import GdeltThemes
from worldstate.collectors.news_hn import HackerNews
from worldstate.collectors.wiki_pageviews import WikiPageviews
from worldstate.collectors.fundamentals_sec import SecFundamentals
from worldstate.collectors.edgar_fulltext import EdgarFulltext
from worldstate.collectors.cftc_cot import CftcCot
from worldstate.collectors.short_finra import FinraShort
from worldstate.collectors.treasury_auctions import TreasuryAuctions
from worldstate.collectors.insider_form4 import InsiderForm4
from worldstate.collectors.fed_text import FomcText
from worldstate.collectors.holdings_13f import Holdings13F
from worldstate.collectors.crypto_onchain import CryptoOnchain
from worldstate.collectors.eia_energy import EiaEnergy
from worldstate.collectors.security_master import SecurityMaster
from worldstate.collectors.surprise_index import SurpriseIndex
from worldstate.collectors.defillama import DefiLlama
from worldstate.collectors.usgs_quakes import UsgsQuakes
from worldstate.collectors.arxiv_papers import ArxivPapers
from worldstate.collectors.predict_manifold import PredictManifold
from worldstate.collectors.nasa_events import NasaEvents
from worldstate.collectors.predict_polymarket import PredictPolymarket
from worldstate.collectors.crypto_funding import CryptoFunding
from worldstate.collectors.crypto_vol import CryptoVol
from worldstate.collectors.crypto_fng import CryptoFearGreed

REGISTRY = {
    "market_yahoo": YahooDaily,
    "macro_alfred": AlfredVintages,
    "edgar": EdgarIndex,
    "edgar_fulltext": EdgarFulltext,
    "crypto_yahoo": CryptoYahoo,
    "news_gdelt": GdeltThemes,
    "news_hn": HackerNews,
    "wiki_pageviews": WikiPageviews,
    "fundamentals_sec": SecFundamentals,
    "cftc_cot": CftcCot,
    "short_finra": FinraShort,
    "treasury_auctions": TreasuryAuctions,
    "insider_form4": InsiderForm4,
    "fed_text": FomcText,
    "holdings_13f": Holdings13F,
    "crypto_onchain": CryptoOnchain,
    "eia_energy": EiaEnergy,
    "security_master": SecurityMaster,
    "surprise_index": SurpriseIndex,
    "defillama": DefiLlama,
    "usgs_quakes": UsgsQuakes,
    "arxiv_papers": ArxivPapers,
    "predict_manifold": PredictManifold,
    "nasa_events": NasaEvents,
    "predict_polymarket": PredictPolymarket,
    "crypto_funding": CryptoFunding,
    "crypto_vol": CryptoVol,
    "crypto_fng": CryptoFearGreed,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name", choices=sorted(REGISTRY))
    ap.add_argument("action", choices=["list", "run", "run-all"])
    ap.add_argument("--chunk")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    collector = REGISTRY[args.name]()

    if args.action == "list":
        print(json.dumps(collector.chunks()))
        return

    hfstore.ensure_repo()
    if args.action == "run":
        if not args.chunk:
            ap.error("--chunk required for run")
        print(json.dumps(collector.run_chunk(args.chunk, force=args.force)))
    else:  # run-all
        for c in collector.chunks():
            res = collector.run_chunk(c, force=args.force)
            print(json.dumps(res), flush=True)


if __name__ == "__main__":
    sys.exit(main())
