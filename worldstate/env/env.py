"""WorldStateEnv — a gym for financial agents over the point-in-time lake.

Loop:  reset() -> packet(observation + decision prompt)
       step(action) -> packet(reward for last prompt, next observation + prompt)

The observation is always as-of the clock cursor (knowledge_time <= cursor), so
the agent literally experiences the world revealing information over time. Tasks
are pluggable; scoring may consult an oracle (future data) that the agent never
sees.
"""
from __future__ import annotations

import json
import pandas as pd

from worldstate import query
from worldstate.env.clock import SimClock
from worldstate.env.observation import ObservationBuilder
from worldstate.env.tasks import Task, DataApprovalTask


class WorldStateEnv:
    def __init__(self, task: Task = None, start="2021-01-04", end="2024-12-31",
                 step_days=1, watchlist=None, con=None):
        self.con = con or query.connect()
        self.clock = SimClock(start, end, step_days)
        self.obs_builder = ObservationBuilder(self.con, watchlist)
        self.task = task or DataApprovalTask()
        self.t = 0
        self._obs = None
        self._prompt = None

    def reset(self) -> dict:
        self.clock.cursor = self.clock.start
        self.t = 0
        self.task.reset(self)
        self._obs = self.obs_builder.build(self.clock.iso())
        self._prompt = self.task.prompt(self, self._obs)
        return self._packet(None, False, {})

    def step(self, action) -> dict:
        reward, info = self.task.score(self, action)
        self.clock.advance()
        self.t += 1
        done = self.clock.done
        if not done:
            self._obs = self.obs_builder.build(self.clock.iso())
            self._prompt = self.task.prompt(self, self._obs)
        return self._packet(reward, done, info)

    def observe(self) -> dict:
        return self._packet(None, self.clock.done, {})

    def _packet(self, reward, done, info) -> dict:
        p = self._prompt or {}
        text = (f"{self._obs['text']}\n\n--- TASK ({self.task.name}) ---\n"
                f"{p.get('instruction','')}\n"
                f"RECORD: {json.dumps(p.get('item', {}))}\n"
                f"{p.get('action_help','')}")
        return {"t": self.t, "as_of": self.clock.iso(), "task": self.task.name,
                "observation": self._obs, "prompt": self._prompt,
                "text": text, "reward": reward, "done": done, "info": info}

    # --- oracle (for reward only; never exposed in observations) --------------
    def oracle_price(self, entity: str, when: pd.Timestamp):
        g = query._glob("market", "yahoo")
        df = self.con.execute(
            f"""SELECT close FROM read_parquet('{g}', hive_partitioning=1)
                WHERE entity='{entity}' AND event_time >= TIMESTAMP '{when.strftime('%Y-%m-%d')}'
                ORDER BY event_time ASC LIMIT 1""").df()
        return float(df["close"].iloc[0]) if len(df) else None
