"""FastAPI server — agents enter the gym over HTTP.

  GET  /health              -> {ok, tasks}                (open, no key)
  GET  /whoami              -> {caller, sessions}         (checks your key works)
  POST /sessions            -> {session_id, packet}       (reset; first observation+prompt)
  POST /sessions/{id}/act   -> {packet}                   (submit decision; get reward + next)
  GET  /sessions/{id}       -> {packet}                   (re-observe current step)

ACCESS CONTROL -- the whole whitelist is one env var:

    GYM_API_KEYS="alice:s3cret-one,bob:s3cret-two"

Comma-separated `name:key` pairs. Callers send their key as `X-API-Key` (or
`Authorization: Bearer <key>`). Adding an outsider = append a pair and restart;
revoking = delete the pair and restart. No database, no user table.

Fails CLOSED: if GYM_API_KEYS is unset the server refuses every authed request
rather than serving the lake to the internet. For local work set a throwaway
pair, e.g. GYM_API_KEYS=dev:dev.

This matters because the process holds AWS credentials for a private bucket --
an unauthenticated caller would get unmetered read access to the whole corpus,
billed as S3 egress to us. Sessions are scoped to the caller who created them,
and each caller is capped at GYM_MAX_SESSIONS (default 3) concurrent envs, since
every env holds a DuckDB connection and drives queries against S3.
"""
from __future__ import annotations

import os
import secrets
import uuid

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from worldstate.env.env import WorldStateEnv
from worldstate.env.tasks import DataApprovalTask, ForecastTask, TradingTask

app = FastAPI(title="DoT Financial-Agent Gym")

# session_id -> (caller, env)
_SESSIONS: dict[str, tuple[str, WorldStateEnv]] = {}
_TASKS = {"data_approval": DataApprovalTask, "forecast": ForecastTask,
          "trading": TradingTask}

MAX_SESSIONS = int(os.environ.get("GYM_MAX_SESSIONS", "3"))


def _whitelist() -> dict[str, str]:
    """Parse GYM_API_KEYS into {key: caller_name}. Read per-request so the
    whitelist can be updated by restarting with a new value, nothing else."""
    out = {}
    for pair in os.environ.get("GYM_API_KEYS", "").split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        name, _, key = pair.partition(":")
        name, key = name.strip(), key.strip()
        if name and key:
            out[key] = name
    return out


def caller(x_api_key: str = Header(default=""),
           authorization: str = Header(default="")) -> str:
    """Resolve the caller from their API key, or 401."""
    wl = _whitelist()
    if not wl:
        raise HTTPException(
            503, "gym access is not configured: GYM_API_KEYS is unset")

    presented = x_api_key.strip()
    if not presented and authorization.lower().startswith("bearer "):
        presented = authorization[7:].strip()
    if not presented:
        raise HTTPException(401, "missing API key: send X-API-Key")

    # Constant-time compare against every key so a wrong key can't be recovered
    # by timing the response.
    for key, name in wl.items():
        if secrets.compare_digest(presented, key):
            return name
    raise HTTPException(401, "invalid API key")


def _own(sid: str, who: str) -> WorldStateEnv:
    """Fetch a session, but only if this caller created it. Returns 404 rather
    than 403 for someone else's session so IDs can't be probed for existence."""
    entry = _SESSIONS.get(sid)
    if entry is None or entry[0] != who:
        raise HTTPException(404, "session not found")
    return entry[1]


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
    """Open on purpose: liveness checks shouldn't need a key. Deliberately
    reports no session details."""
    return {"ok": True, "tasks": list(_TASKS), "configured": bool(_whitelist())}


@app.get("/whoami")
def whoami(who: str = Depends(caller)):
    mine = [s for s, (c, _) in _SESSIONS.items() if c == who]
    return {"caller": who, "sessions": mine, "max_sessions": MAX_SESSIONS}


@app.post("/sessions")
def create(req: NewSession, who: str = Depends(caller)):
    if req.task not in _TASKS:
        raise HTTPException(400, f"unknown task; choose {list(_TASKS)}")

    mine = [s for s, (c, _) in _SESSIONS.items() if c == who]
    if len(mine) >= MAX_SESSIONS:
        raise HTTPException(
            429, f"session limit reached ({MAX_SESSIONS}); finish or drop one "
                 f"with DELETE /sessions/{{id}}")

    env = WorldStateEnv(task=_TASKS[req.task](), start=req.start, end=req.end,
                        step_days=req.step_days, access_tier=req.access_tier,
                        tool_budget=req.tool_budget)
    packet = env.reset()
    sid = uuid.uuid4().hex[:12]
    _SESSIONS[sid] = (who, env)
    return {"session_id": sid, "packet": packet}


@app.post("/sessions/{sid}/act")
def act(sid: str, body: Action, who: str = Depends(caller)):
    env = _own(sid, who)
    packet = env.step(body.action)
    if packet["done"]:
        _SESSIONS.pop(sid, None)
    return {"packet": packet}


@app.get("/sessions/{sid}")
def observe(sid: str, who: str = Depends(caller)):
    return {"packet": _own(sid, who).observe()}


@app.delete("/sessions/{sid}")
def drop(sid: str, who: str = Depends(caller)):
    _own(sid, who)
    _SESSIONS.pop(sid, None)
    return {"dropped": sid}
