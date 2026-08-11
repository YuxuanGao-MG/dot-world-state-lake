"""FastAPI server — agents enter the gym over HTTP.

  GET    /                  -> {service, docs}                (open)
  GET    /health            -> {ok, lake, tasks}              (open, no key)
  GET    /whoami            -> {caller, sessions}             (checks your key works)
  POST   /sessions          -> {session_id, packet}           (reset; first obs + prompt)
  POST   /sessions/{id}/act -> {packet}                       (submit decision; reward + next)
  GET    /sessions/{id}     -> {packet}                       (re-observe current step)
  DELETE /sessions/{id}     -> {dropped}                      (end early, free a slot)

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
billed as S3 egress to us.

FAILS LOUD ON AN UNREADABLE LAKE. Every query path in this codebase degrades
quietly, so a server with no/bad AWS credentials would otherwise answer with a
perfectly well-formed episode about an empty world. /sessions probes the lake
first and 503s instead. See health.py.

Env vars:
    GYM_API_KEYS              whitelist (required; unset = refuse everything)
    GYM_MAX_SESSIONS          concurrent sessions per caller        (default 3)
    GYM_RATE_LIMIT            requests per window per caller        (default 60)
    GYM_RATE_WINDOW           rate-limit window, seconds            (default 60)
    GYM_SESSION_TTL_MIN       idle session expiry, minutes          (default 30)
    GYM_HEALTH_TTL            lake-probe cache, seconds             (default 60)
    GYM_ALLOW_UNVERIFIED_LAKE set to 1 to skip the lake probe -- TESTING ONLY;
                              with this on the server will happily serve empty
                              worlds, which is the exact bug the probe exists to
                              prevent. /health reports it so it can't hide.
"""
from __future__ import annotations

import os
import secrets
import threading
import time
import uuid

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from worldstate.env.env import WorldStateEnv
from worldstate.env.health import lake_status
from worldstate.env.tasks import DataApprovalTask, ForecastTask, TradingTask

app = FastAPI(title="DoT Financial-Agent Gym")

_TASKS = {"data_approval": DataApprovalTask, "forecast": ForecastTask,
          "trading": TradingTask}

_lock = threading.Lock()
# session_id -> {caller, env, last_seen}
_SESSIONS: dict[str, dict] = {}
# caller -> [request timestamps]
_HITS: dict[str, list[float]] = {}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


MAX_SESSIONS = _int_env("GYM_MAX_SESSIONS", 3)
RATE_LIMIT = _int_env("GYM_RATE_LIMIT", 60)
RATE_WINDOW = _int_env("GYM_RATE_WINDOW", 60)
SESSION_TTL = _int_env("GYM_SESSION_TTL_MIN", 30) * 60


def _skip_lake_check() -> bool:
    return os.environ.get("GYM_ALLOW_UNVERIFIED_LAKE", "") == "1"


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


def _sweep(now: float) -> None:
    """Drop sessions nobody has touched in SESSION_TTL. An abandoned episode
    otherwise pins a DuckDB connection for the life of the process."""
    dead = [s for s, e in _SESSIONS.items() if now - e["last_seen"] > SESSION_TTL]
    for s in dead:
        _SESSIONS.pop(s, None)


def caller(x_api_key: str = Header(default=""),
           authorization: str = Header(default="")) -> str:
    """Resolve the caller from their API key, rate-limit them, or raise."""
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
    who = None
    for key, name in wl.items():
        if secrets.compare_digest(presented, key):
            who = name
    if who is None:
        raise HTTPException(401, "invalid API key")

    now = time.monotonic()
    with _lock:
        _sweep(now)
        hits = [t for t in _HITS.get(who, []) if now - t < RATE_WINDOW]
        if len(hits) >= RATE_LIMIT:
            retry = int(RATE_WINDOW - (now - hits[0])) + 1
            raise HTTPException(429, f"rate limit: {RATE_LIMIT} requests per "
                                     f"{RATE_WINDOW}s; retry in {retry}s")
        hits.append(now)
        _HITS[who] = hits
    return who


def _own(sid: str, who: str) -> WorldStateEnv:
    """Fetch a session, but only if this caller created it. Returns 404 rather
    than 403 for someone else's session so IDs can't be probed for existence."""
    with _lock:
        entry = _SESSIONS.get(sid)
        if entry is None or entry["caller"] != who:
            raise HTTPException(404, "session not found")
        entry["last_seen"] = time.monotonic()
        return entry["env"]


class NewSession(BaseModel):
    task: str = "data_approval"
    start: str = "2021-01-04"
    end: str = "2024-12-31"
    step_days: int = 1
    access_tier: str = "basic"
    tool_budget: int = 3


class Action(BaseModel):
    action: str


@app.get("/")
def root():
    return {"service": "DoT Financial-Agent Gym",
            "docs": "/docs", "health": "/health",
            "auth": "send your key as X-API-Key"}


@app.get("/health")
def health():
    """Open on purpose: liveness checks shouldn't need a key. Deliberately
    reports no session detail. Always 200 so a platform health check doesn't
    restart-loop a server whose only problem is unreadable storage -- read the
    `lake` field to know whether it can actually serve."""
    lake = {"ok": True, "detail": "probe skipped"} if _skip_lake_check() \
        else lake_status()
    return {
        "ok": True,
        "lake": lake,
        "tasks": list(_TASKS),
        "configured": bool(_whitelist()),
        "unverified_lake_override": _skip_lake_check(),
    }


@app.get("/whoami")
def whoami(who: str = Depends(caller)):
    with _lock:
        mine = [s for s, e in _SESSIONS.items() if e["caller"] == who]
    return {"caller": who, "sessions": mine, "max_sessions": MAX_SESSIONS}


@app.post("/sessions")
def create(req: NewSession, who: str = Depends(caller)):
    if req.task not in _TASKS:
        raise HTTPException(400, f"unknown task; choose {list(_TASKS)}")

    # Probe BEFORE building an env. Without this the server hands back a
    # well-formed episode whose every observation channel is empty.
    if not _skip_lake_check():
        lake = lake_status()
        if not lake["ok"]:
            raise HTTPException(
                503, f"lake is not readable, refusing to serve an empty world: "
                     f"{lake['detail']}")

    with _lock:
        _sweep(time.monotonic())
        mine = [s for s, e in _SESSIONS.items() if e["caller"] == who]
        if len(mine) >= MAX_SESSIONS:
            raise HTTPException(
                429, f"session limit reached ({MAX_SESSIONS}); finish one or "
                     f"DELETE /sessions/{{id}}")

    env = WorldStateEnv(task=_TASKS[req.task](), start=req.start, end=req.end,
                        step_days=req.step_days, access_tier=req.access_tier,
                        tool_budget=req.tool_budget)
    packet = env.reset()
    sid = uuid.uuid4().hex[:12]
    with _lock:
        _SESSIONS[sid] = {"caller": who, "env": env,
                          "last_seen": time.monotonic()}
    return {"session_id": sid, "packet": packet}


@app.post("/sessions/{sid}/act")
def act(sid: str, body: Action, who: str = Depends(caller)):
    env = _own(sid, who)
    packet = env.step(body.action)
    if packet["done"]:
        with _lock:
            _SESSIONS.pop(sid, None)
    return {"packet": packet}


@app.get("/sessions/{sid}")
def observe(sid: str, who: str = Depends(caller)):
    return {"packet": _own(sid, who).observe()}


@app.delete("/sessions/{sid}")
def drop(sid: str, who: str = Depends(caller)):
    _own(sid, who)
    with _lock:
        _SESSIONS.pop(sid, None)
    return {"dropped": sid}
