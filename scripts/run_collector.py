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

from worldstate import hfstore
from worldstate.collectors.market_yahoo import YahooDaily
from worldstate.collectors.macro_alfred import AlfredVintages
from worldstate.collectors.edgar import EdgarIndex

REGISTRY = {
    "market_yahoo": YahooDaily,
    "macro_alfred": AlfredVintages,
    "edgar": EdgarIndex,
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
