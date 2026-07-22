"""Generate PIT.md from worldstate/provenance.py so the doc never drifts."""
from __future__ import annotations

import os
from worldstate import provenance as P

ORDER = ["immutable", "vintage", "derived", "forward_limited",
         "revised_soft", "snapshot_forward", "excluded_hazard", "unknown"]


def main():
    lines = ["# PIT Provenance — how far each source can be trusted historically",
             "",
             "**Auto-generated from `worldstate/provenance.py`. Do not edit by hand.**",
             "",
             P.__doc__.strip(), "",
             "## Sources by class", ""]
    by = {}
    for (dom, src), (cls, kt, note) in P.SOURCES.items():
        by.setdefault(cls, []).append((f"{dom}/{src}", kt, note))
    for cls in ORDER:
        if cls not in by:
            continue
        safe = "✅ historical-safe" if cls in P.HISTORICAL_SAFE else "⚠️ forward-only / caution"
        lines.append(f"### `{cls}` — {safe}")
        lines.append("")
        lines.append("| source | knowledge_time | note |")
        lines.append("|---|---|---|")
        for name, kt, note in sorted(by[cls]):
            lines.append(f"| `{name}` | {kt} | {note} |")
        lines.append("")
    lines.append("## Deliberately excluded (would contaminate)")
    lines.append("")
    lines.append("| source | why |")
    lines.append("|---|---|")
    for name, why in P.EXCLUDED.items():
        lines.append(f"| {name} | {why} |")
    lines.append("")
    lines.append("## Rule for consumers / the env")
    lines.append("")
    lines.append("Historical (pre-collection) training episodes should draw observations "
                 "ONLY from classes: **" + ", ".join(sorted(P.HISTORICAL_SAFE)) + "**. "
                 "`snapshot_forward` / `revised_soft` sources are trustworthy only from "
                 "their collection date onward; the bitemporal `knowledge_time` enforces "
                 "this automatically as long as those sources stamp knowledge_time = "
                 "collection time.")
    out = os.path.join(os.path.dirname(__file__), "..", "PIT.md")
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {out} ({len(P.SOURCES)} sources)")


if __name__ == "__main__":
    main()
