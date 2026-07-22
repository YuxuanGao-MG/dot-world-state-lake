"""Drive the WorldStateEnv with a simple rule-based agent to demo the loop.

Shows the reset -> observe -> act -> reward cycle over the point-in-time lake.
Needs AWS creds in env (it reads the S3 lake). Usage:
  python -m scripts.run_env_demo --steps 20 --task data_approval
"""
from __future__ import annotations

import argparse
from worldstate.env.env import WorldStateEnv
from worldstate.env.tasks import DataApprovalTask, ForecastTask


def rule_agent(packet) -> str:
    """Baseline: reject records whose value looks implausible vs the world state."""
    item = (packet.get("prompt") or {}).get("item", {})
    if packet["task"] == "data_approval":
        prices = {r["entity"]: r["close"] for r in packet["observation"]["prices"]}
        v, ent = item.get("value"), item.get("entity")
        ref = prices.get(ent)
        if v is None or v <= 0:
            return "reject"
        if str(item.get("as_of", "")).startswith("2099"):
            return "reject"
        if ref and (v > ref * 2.5 or v < ref * 0.4):
            return "reject"
        return "approve"
    return "up"  # trivial forecast baseline


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, default=20)
    ap.add_argument("--task", default="data_approval", choices=["data_approval", "forecast"])
    ap.add_argument("--step-days", type=int, default=3)
    args = ap.parse_args()

    task = DataApprovalTask() if args.task == "data_approval" else ForecastTask()
    env = WorldStateEnv(task=task, step_days=args.step_days)
    pkt = env.reset()
    print(pkt["text"][:600], "\n")

    total, n = 0.0, 0
    for _ in range(args.steps):
        action = rule_agent(pkt)
        pkt = env.step(action)
        if pkt["reward"] is not None:
            total += pkt["reward"]
            n += 1
            print(f"t={pkt['t']:>3} as_of={pkt['as_of'][:10]} action={action:<8} "
                  f"reward={pkt['reward']:+.0f} info={pkt['info']}")
        if pkt["done"]:
            break
    print(f"\n=== {n} decisions, total reward {total:+.0f}, "
          f"avg {total/max(n,1):+.3f} ===")


if __name__ == "__main__":
    main()
