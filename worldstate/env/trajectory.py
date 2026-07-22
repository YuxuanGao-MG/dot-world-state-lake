"""Trajectory logging — turn gym episodes into RL training data.

This is the point of the whole lake: an agent-RL datapoint is a *trajectory* (many
steps of observation -> decision -> outcome). Each episode is recorded step by step
and flushed to the lake at domain=trajectories/source=env, ready to train on.
"""
from __future__ import annotations

import json
import uuid
import pyarrow as pa

from worldstate import store as hfstore


def _action_type(action) -> str:
    if isinstance(action, dict):
        return "tool" if action.get("tool") else "decide"
    s = str(action).strip()
    if s.startswith("{"):
        try:
            return "tool" if json.loads(s).get("tool") else "decide"
        except Exception:
            return "decide"
    return "decide"

SCHEMA = pa.schema([
    ("episode_id", pa.string()), ("agent", pa.string()), ("task", pa.string()),
    ("step", pa.int32()), ("as_of", pa.string()), ("access_tier", pa.string()),
    ("action_type", pa.string()),   # "tool" or "decide"
    ("action", pa.string()), ("reward", pa.float64()), ("cum_reward", pa.float64()),
    ("scored", pa.bool_()), ("done", pa.bool_()),
    ("observation_json", pa.string()), ("prompt_json", pa.string()),
    ("tool_results_json", pa.string()), ("info_json", pa.string()),
])


class TrajectoryLogger:
    def __init__(self, task: str, agent: str = "baseline", episode_id: str = None):
        self.episode_id = episode_id or uuid.uuid4().hex[:16]
        self.agent = agent
        self.task = task
        self.rows: list[dict] = []
        self._cum = 0.0

    def record(self, state: dict, action, result: dict) -> None:
        """Log one transition: the state the agent saw, the action it took, and the
        resulting reward/info. Works for tool sub-steps (reward 0, scored False)
        and decision steps alike."""
        r = result.get("reward")
        scored = bool(result.get("scored", True)) and r is not None
        if r is not None:
            self._cum += r
        self.rows.append({
            "episode_id": self.episode_id, "agent": self.agent, "task": self.task,
            "step": int(state.get("t", 0)), "as_of": state.get("as_of", ""),
            "access_tier": state.get("access_tier", ""),
            "action_type": _action_type(action),
            "action": json.dumps(action, default=str) if isinstance(action, dict) else str(action),
            "reward": float(r) if r is not None else 0.0, "cum_reward": self._cum,
            "scored": scored, "done": bool(result.get("done", False)),
            "observation_json": json.dumps(state.get("observation", {}), default=str),
            "prompt_json": json.dumps(state.get("prompt", {}), default=str),
            "tool_results_json": json.dumps(result.get("tool_results", []), default=str),
            "info_json": json.dumps(result.get("info", {}), default=str),
        })

    def table(self) -> pa.Table:
        cols = {f.name: [row[f.name] for row in self.rows] for f in SCHEMA}
        return pa.table(cols, schema=SCHEMA)

    def flush(self) -> dict:
        if not self.rows:
            return {"episode_id": self.episode_id, "rows": 0}
        path = hfstore.shard_path("trajectories", "env", f"task={self.task}",
                                  f"episode={self.episode_id}", name="part.parquet")
        hfstore.upload_table(self.table(), path, overwrite=True)
        return {"episode_id": self.episode_id, "rows": len(self.rows),
                "cum_reward": self._cum, "path": path}
