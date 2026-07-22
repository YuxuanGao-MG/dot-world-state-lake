"""WorldStateEnv — a gym for financial agents over the point-in-time lake.

Loop:  reset() -> packet(observation + decision prompt + available tools)
       step(action):
         - a TOOL action  -> runs the tool (gated by access tier + budget), appends
           its result to the current step, does NOT score or advance the clock.
         - a DECISION      -> scored by the task; clock advances; budget resets.

So one decision-step can contain several tool sub-steps: the trajectory naturally
includes "what's my access / which tool do I call / then decide". Observations and
tools are all as-of the cursor (knowledge_time <= cursor) — zero lookahead. Scoring
may consult an oracle (future data) the agent never sees.
"""
from __future__ import annotations

import json
import pandas as pd

from worldstate import query
from worldstate.env.clock import SimClock
from worldstate.env.observation import ObservationBuilder
from worldstate.env.tasks import Task, DataApprovalTask
from worldstate.env.tools import ToolRegistry


def _parse_action(action):
    """Normalize an action into ('tool', name, args) or ('decide', value)."""
    a = action
    if isinstance(a, str):
        s = a.strip()
        if s.startswith("{"):
            try:
                a = json.loads(s)
            except Exception:
                return ("decide", s)
        else:
            return ("decide", s)
    if isinstance(a, dict):
        if a.get("tool"):
            return ("tool", a["tool"], a.get("args", {}))
        return ("decide", a.get("action", a.get("decide", "")))
    return ("decide", str(a))


class WorldStateEnv:
    def __init__(self, task: Task = None, start="2021-01-04", end="2024-12-31",
                 step_days=1, watchlist=None, con=None,
                 access_tier="basic", tool_budget=3):
        self.con = con or query.connect()
        self.clock = SimClock(start, end, step_days)
        self.obs_builder = ObservationBuilder(self.con, watchlist)
        self.task = task or DataApprovalTask()
        self.registry = ToolRegistry()
        self.access_tier = access_tier
        self.tool_budget = tool_budget
        self.t = 0
        self._obs = None
        self._prompt = None
        self._tool_results = []
        self._budget_left = tool_budget

    def reset(self) -> dict:
        self.clock.cursor = self.clock.start
        self.t = 0
        self._tool_results = []
        self._budget_left = self.tool_budget
        self.task.reset(self)
        self._obs = self.obs_builder.build(self.clock.iso())
        self._prompt = self.task.prompt(self, self._obs)
        return self._packet(None, False, {})

    def step(self, action) -> dict:
        parsed = _parse_action(action)
        if parsed[0] == "tool":
            return self._tool_step(parsed[1], parsed[2])
        return self._decide_step(parsed[1])

    def _tool_step(self, name, args) -> dict:
        spec = self.registry.get(name)
        allowed = spec is not None and any(
            t["name"] == name for t in self.registry.available(self.access_tier))
        if not allowed:
            result = {"error": f"tool '{name}' not available at tier '{self.access_tier}'"}
        elif self._budget_left < (spec.cost if spec else 1):
            result = {"error": "tool budget exhausted for this decision step"}
        else:
            result = self.registry.run(self, name, args)
            self._budget_left -= spec.cost
        self._tool_results.append({"tool": name, "args": args, "result": result})
        return self._packet(None, False, {"tool_call": name}, scored=False)

    def _decide_step(self, decision) -> dict:
        reward, info = self.task.score(self, decision)
        self.clock.advance()
        self.t += 1
        done = self.clock.done
        self._tool_results = []
        self._budget_left = self.tool_budget
        if not done:
            self._obs = self.obs_builder.build(self.clock.iso())
            self._prompt = self.task.prompt(self, self._obs)
        return self._packet(reward, done, info, scored=True)

    def observe(self) -> dict:
        return self._packet(None, self.clock.done, {})

    def _packet(self, reward, done, info, scored=True) -> dict:
        p = self._prompt or {}
        tools = self.registry.available(self.access_tier)
        tool_help = "; ".join(f"{t['name']}(cost {t['cost']})" for t in tools)
        text = (f"{self._obs['text']}\n\n"
                f"ACCESS: tier={self.access_tier}, tool_budget_left={self._budget_left}\n"
                f"TOOLS (call as {{'tool':NAME,'args':{{...}}}}): {tool_help}\n"
                f"--- TASK ({self.task.name}) ---\n{p.get('instruction','')}\n"
                f"RECORD: {json.dumps(p.get('item', {}))}\n{p.get('action_help','')}")
        if self._tool_results:
            text += "\n--- TOOL RESULTS THIS STEP ---\n" + json.dumps(
                self._tool_results, default=str)[:2000]
        return {"t": self.t, "as_of": self.clock.iso(), "task": self.task.name,
                "access_tier": self.access_tier, "tool_budget_left": self._budget_left,
                "available_tools": tools, "tool_results": list(self._tool_results),
                "observation": self._obs, "prompt": self._prompt, "text": text,
                "reward": reward, "done": done, "scored": scored, "info": info}

    # --- oracle (reward only; never in observations/tools) --------------------
    def oracle_price(self, entity: str, when: pd.Timestamp):
        g = query._glob("market", "yahoo")
        df = self.con.execute(
            f"""SELECT close FROM read_parquet('{g}', hive_partitioning=1)
                WHERE entity='{entity}' AND event_time >= TIMESTAMP '{when.strftime('%Y-%m-%d')}'
                ORDER BY event_time ASC LIMIT 1""").df()
        return float(df["close"].iloc[0]) if len(df) else None
