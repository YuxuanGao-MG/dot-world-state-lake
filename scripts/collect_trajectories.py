"""Run agent episodes over the gym and log trajectories to the lake.

This is the point of the whole project: it turns the point-in-time world model
into RL training data — full observation -> (tool calls) -> decision -> reward
trajectories, written to s3://.../domain=trajectories/source=env.

  python -m scripts.collect_trajectories --episodes 5 --task data_approval \
      --agent tooluser --access-tier pro --steps 12
"""
from __future__ import annotations

import argparse
import random
import pandas as pd

from worldstate.env.env import WorldStateEnv
from worldstate.env.tasks import DataApprovalTask, ForecastTask, TradingTask
from worldstate.env.trajectory import TrajectoryLogger

TASKS = {"data_approval": DataApprovalTask, "forecast": ForecastTask, "trading": TradingTask}


def _decide(pkt) -> str:
    item = (pkt.get("prompt") or {}).get("item", {})
    if pkt["task"] == "data_approval":
        prices = {r["entity"]: r["close"] for r in pkt["observation"]["prices"]}
        v, ent = item.get("value"), item.get("entity")
        ref = prices.get(ent)
        if v is None or v <= 0 or str(item.get("as_of", "")).startswith("2099"):
            return "reject"
        if ref and (v > ref * 2.5 or v < ref * 0.4):
            return "reject"
        return "approve"
    if pkt["task"] == "trading":
        return random.choice(["long", "flat", "short"])
    return "up"


def _agent(name):
    def baseline(pkt):
        return _decide(pkt)

    def rand(pkt):
        if pkt["task"] == "data_approval":
            return random.choice(["approve", "reject"])
        if pkt["task"] == "trading":
            return random.choice(["long", "flat", "short"])
        return random.choice(["up", "down"])

    def tooluser(pkt):
        # pro agent: consult a premium tool once before deciding (multi-step trajectory)
        if pkt["access_tier"] == "pro" and pkt["tool_budget_left"] > 0 and not pkt["tool_results"]:
            item = (pkt.get("prompt") or {}).get("item", {})
            ent = item.get("entity") or item.get("target") or "AAPL"
            return {"tool": "positioning", "args": {"ticker": ent}}
        return _decide(pkt)

    return {"baseline": baseline, "random": rand, "tooluser": tooluser}[name]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", type=int, default=5)
    ap.add_argument("--task", default="data_approval", choices=list(TASKS))
    ap.add_argument("--agent", default="baseline", choices=["baseline", "random", "tooluser"])
    ap.add_argument("--steps", type=int, default=12)
    ap.add_argument("--step-days", type=int, default=5)
    ap.add_argument("--episode-days", type=int, default=120)
    ap.add_argument("--access-tier", default="basic", choices=["basic", "pro"])
    ap.add_argument("--tool-budget", type=int, default=3)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    random.seed(args.seed)
    agent = _agent(args.agent)
    win_lo = pd.Timestamp("2021-01-04", tz="UTC")
    win_hi = pd.Timestamp("2024-06-30", tz="UTC")
    total_rows = 0

    for ep in range(args.episodes):
        offset = random.randint(0, (win_hi - win_lo).days)
        start = (win_lo + pd.Timedelta(days=offset)).strftime("%Y-%m-%d")
        end = (win_lo + pd.Timedelta(days=offset + args.episode_days)).strftime("%Y-%m-%d")
        env = WorldStateEnv(task=TASKS[args.task](), start=start, end=end,
                            step_days=args.step_days, access_tier=args.access_tier,
                            tool_budget=args.tool_budget)
        logger = TrajectoryLogger(args.task, agent=args.agent)
        pkt = env.reset()
        for _ in range(args.steps * 3):  # room for tool sub-steps
            action = agent(pkt)
            nxt = env.step(action)
            logger.record(pkt, action, nxt)
            pkt = nxt
            if pkt["done"]:
                break
        res = logger.flush()
        total_rows += res.get("rows", 0)
        print(f"ep {ep+1}/{args.episodes} start={start} rows={res.get('rows')} "
              f"cum_reward={res.get('cum_reward'):.2f} -> {res.get('path')}")

    print(f"\n=== {args.episodes} episodes, {total_rows} trajectory rows written "
          f"to domain=trajectories/source=env ===")


if __name__ == "__main__":
    main()
