"""Tasks an agent is scored on inside the environment.

A Task turns the moving world state into a decision problem: it emits a prompt
(+ a hidden ground truth) each step and grades the agent's action. Tasks are
pluggable so the same world/env supports data-approval, forecasting, trading, …
"""
from __future__ import annotations

import random
from abc import ABC, abstractmethod


class Task(ABC):
    name = "base"

    def reset(self, env):
        pass

    @abstractmethod
    def prompt(self, env, obs: dict) -> dict:
        """Return {instruction, item, action_help}; stash ground truth on self."""

    @abstractmethod
    def score(self, env, action) -> tuple[float, dict]:
        """Grade the action for the most recent prompt -> (reward, info)."""


class DataApprovalTask(Task):
    """The case study: an incoming data record arrives; approve it or reject it.

    With probability `anomaly_rate` the record is corrupted (implausible value,
    or a stale/future timestamp) — the agent must catch it using the world
    context. Ground-truthed, so reward is exact: +1 correct, -1 wrong.
    """
    name = "data_approval"

    def __init__(self, anomaly_rate: float = 0.4, seed: int = 0):
        self.anomaly_rate = anomaly_rate
        self.rng = random.Random(seed)
        self._truth = None

    def prompt(self, env, obs: dict) -> dict:
        prices = obs.get("prices") or []
        if prices:
            base = self.rng.choice(prices)
            entity, value = base["entity"], float(base["close"])
        else:
            entity, value = "AAPL", 200.0
        record = {"record_type": "market_daily_bar", "entity": entity,
                  "field": "close", "value": round(value, 2), "as_of": obs["as_of"]}

        anomaly = self.rng.random() < self.anomaly_rate
        if anomaly:
            kind = self.rng.choice(["value_spike", "negative", "stale_date", "zero"])
            if kind == "value_spike":
                record["value"] = round(value * self.rng.uniform(3, 12), 2)
            elif kind == "negative":
                record["value"] = -abs(round(value, 2))
            elif kind == "zero":
                record["value"] = 0.0
            else:  # stale/future date
                record["as_of"] = "2099-01-01 00:00:00"
            record["_anomaly_kind"] = kind
        self._truth = "reject" if anomaly else "approve"

        return {
            "instruction": ("An incoming data record has arrived for ingestion. "
                            "Using the world state above, decide whether to APPROVE "
                            "(looks valid) or REJECT (looks anomalous/corrupted)."),
            "item": {k: v for k, v in record.items() if not k.startswith("_")},
            "action_help": "Respond with 'approve' or 'reject'.",
        }

    def score(self, env, action) -> tuple[float, dict]:
        a = str(action).strip().lower()
        decision = "reject" if "reject" in a else ("approve" if "approve" in a else "invalid")
        correct = decision == self._truth
        return (1.0 if correct else -1.0), {"truth": self._truth, "decision": decision}


class ForecastTask(Task):
    """Predict the direction of a target's price over the next step. Immediate
    reward via an oracle look at the realized value (agent never sees it)."""
    name = "forecast"

    def __init__(self, target: str = "SPY", horizon_days: int = 5, seed: int = 0):
        self.target = target
        self.horizon_days = horizon_days
        self._t0_price = None

    def prompt(self, env, obs: dict) -> dict:
        px = {r["entity"]: r["close"] for r in (obs.get("prices") or [])}
        self._t0_price = px.get(self.target)
        return {
            "instruction": (f"Predict whether {self.target} will be UP or DOWN "
                            f"over the next {self.horizon_days} days."),
            "item": {"target": self.target, "current_price": self._t0_price},
            "action_help": "Respond with 'up' or 'down'.",
        }

    def score(self, env, action) -> tuple[float, dict]:
        future = env.oracle_price(self.target,
                                  env.clock.cursor + __import__("pandas").Timedelta(
                                      days=self.horizon_days))
        if self._t0_price is None or future is None:
            return 0.0, {"skipped": "no price"}
        realized_up = future >= self._t0_price
        pred_up = "up" in str(action).strip().lower()
        return (1.0 if pred_up == realized_up else -1.0), {
            "t0": self._t0_price, "future": future, "realized_up": bool(realized_up)}
