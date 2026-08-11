"""Expand the US equity universe from SEC's company_tickers.json (keyless).

Writes config/universe_us.txt (ticker per line). Without this file the
collectors fall back to config.SEED_UNIVERSE. Use --top to cap by filing
frequency proxy (here simply first N as SEC orders by size-ish); default all.
"""
from __future__ import annotations

import argparse
import os
import requests

from config import settings

URL = "https://www.sec.gov/files/company_tickers.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=0, help="cap number of tickers (0 = all)")
    args = ap.parse_args()

    r = requests.get(URL, headers={"User-Agent": settings.USER_AGENT}, timeout=60)
    r.raise_for_status()
    data = r.json()

    # SEED_UNIVERSE always goes in first. company_tickers.json is operating
    # companies: not one of the 19 seed ETFs is in the top 1500, and TLT, IEF,
    # HYG, LQD, IWM, VTI and every XL* sector ETF are absent from the file
    # entirely. Since the collectors only fall back to SEED_UNIVERSE when this
    # file is MISSING -- and the workflow regenerates it every run -- writing
    # SEC tickers alone silently dropped every ETF from the lake. That left
    # features/cross_asset (needs SPY/TLT/GLD) returning zero rows and the gym's
    # price watchlist resolving only its three single names.
    seed = []
    seen = set()
    for t in settings.SEED_UNIVERSE:
        t = t.upper().strip()
        if t and t not in seen:
            seen.add(t)
            seed.append(t)

    sec = []
    for row in data.values():
        t = str(row["ticker"]).upper().strip()
        if t and t not in seen:
            seen.add(t)
            sec.append(t)
    # --top caps the SEC expansion; the seed set is small and always kept.
    if args.top:
        sec = sec[:args.top]
    tickers = seed + sec

    out = os.path.join(os.path.dirname(__file__), "..", "config", "universe_us.txt")
    with open(out, "w") as f:
        f.write("# US equity universe from SEC company_tickers.json\n")
        f.write("\n".join(tickers) + "\n")
    print(f"wrote {len(tickers)} tickers -> {out}")


if __name__ == "__main__":
    main()
