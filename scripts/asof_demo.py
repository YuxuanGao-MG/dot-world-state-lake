"""Prove PIT correctness: the same macro series queried as-of two different dates
returns different (vintage-correct) numbers — no lookahead.

  python -m scripts.asof_demo GDPC1 2021-06-01 2023-06-01
"""
from __future__ import annotations

import sys
from worldstate import query


def main():
    series = sys.argv[1] if len(sys.argv) > 1 else "GDPC1"
    dates = sys.argv[2:] or ["2021-06-01", "2023-06-01"]
    con = query.connect()
    for d in dates:
        df = query.as_of_macro(con, d)
        df = df[df.entity == series].tail(3)
        print(f"\n=== {series} as known on {d} (last 3 observations) ===")
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
