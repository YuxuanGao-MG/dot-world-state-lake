"""FastAPI server — agents enter the gym over HTTP.

  POST /sessions            -> {session_id, packet}   (reset; first observation+prompt)
  POST /sessions/{id}/act   -> {packet}                (submit decision; get reward + next)
  GET  /sessions/{id}       -> {packet}                (re-observe current step)

Run:  uvicorn worldstate.env.server:app  (needs AWS creds in env for the lake)
"""
from __future__ import annotations

import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from worldstate.env.env import WorldStateEnv
from worldstate.env.tasks import DataApprovalTask, ForecastTask, TradingTask

app = FastAPI(title="DoT Financial-Agent Gym")
_SESSIONS: dict[str, WorldStateEnv] = {}
_TASKS = {"data_approval": DataApprovalTask, "forecast": ForecastTask,
          "trading": TradingTask}


class NewSession(BaseModel):
    task: str = "data_approval"
    start: str = "2021-01-04"
    end: str = "2024-12-31"
    step_days: int = 1
    access_tier: str = "basic"
    tool_budget: int = 3


class Action(BaseModel):
    action: str


@app.get("/health")
def health():
    return {"ok": True, "sessions": len(_SESSIONS), "tasks": list(_TASKS)}


@app.post("/sessions")
def create(req: NewSession):
    if req.task not in _TASKS:
        raise HTTPException(400, f"unknown task; choose {list(_TASKS)}")
    env = WorldStateEnv(task=_TASKS[req.task](), start=req.start, end=req.end,
                        step_days=req.step_days, access_tier=req.access_tier,
                        tool_budget=req.tool_budget)
    packet = env.reset()
    sid = uuid.uuid4().hex[:12]
    _SESSIONS[sid] = env
    return {"session_id": sid, "packet": packet}


@app.post("/sessions/{sid}/act")
def act(sid: str, body: Action):
    env = _SESSIONS.get(sid)
    if env is None:
        raise HTTPException(404, "session not found")
    packet = env.step(body.action)
    if packet["done"]:
        _SESSIONS.pop(sid, None)
    return {"packet": packet}


@app.get("/sessions/{sid}")
def observe(sid: str):
    env = _SESSIONS.get(sid)
    if env is None:
        raise HTTPException(404, "session not found")
    return {"packet": env.observe()}
