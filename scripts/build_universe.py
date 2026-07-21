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
    tickers = []
    seen = set()
    for row in data.values():
        t = str(row["ticker"]).upper().strip()
        if t and t not in seen:
            seen.add(t)
            tickers.append(t)
    if args.top:
        tickers = tickers[:args.top]

    out = os.path.join(os.path.dirname(__file__), "..", "config", "universe_us.txt")
    with open(out, "w") as f:
        f.write("# US equity universe from SEC company_tickers.json\n")
        f.write("\n".join(tickers) + "\n")
    print(f"wrote {len(tickers)} tickers -> {out}")


if __name__ == "__main__":
    main()
