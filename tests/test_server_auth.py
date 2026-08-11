"""Exercise the gym whitelist, lake gate, rate limit and session TTL.

Stubs WorldStateEnv so none of this needs AWS credentials or the real lake.
Run: python tests/test_server_auth.py
"""
import os, sys, types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub the heavy modules server.py imports, so no duckdb/boto3/S3 is needed.
for name in ["worldstate.env.env", "worldstate.env.tasks"]:
    sys.modules.setdefault(name, types.ModuleType(name))


class FakeEnv:
    def __init__(self, **kw): self.kw = kw; self.n = 0
    def reset(self): return {"text": "obs", "done": False}
    def step(self, a): self.n += 1; return {"text": "obs", "done": self.n >= 2, "reward": 1}
    def observe(self): return {"text": "obs", "done": False}


sys.modules["worldstate.env.env"].WorldStateEnv = FakeEnv
for t in ["Task", "DataApprovalTask", "ForecastTask", "TradingTask"]:
    setattr(sys.modules["worldstate.env.tasks"], t, type(t, (), {}))

os.environ["GYM_API_KEYS"] = "alice:key-alice,bob:key-bob"
os.environ["GYM_MAX_SESSIONS"] = "2"
os.environ["GYM_HEALTH_TTL"] = "0"          # never cache the probe in tests
os.environ["GYM_ALLOW_UNVERIFIED_LAKE"] = "1"

from fastapi.testclient import TestClient
from worldstate.env import server

c = TestClient(server.app)
A = {"X-API-Key": "key-alice"}
B = {"X-API-Key": "key-bob"}
fails = []
def ok(label, cond):
    print(("PASS  " if cond else "FAIL  ") + label)
    if not cond: fails.append(label)

# --- open endpoints ---------------------------------------------------------
r = c.get("/health"); ok("health open, no key needed", r.status_code == 200)
ok("health reports configured=True", r.json()["configured"] is True)
ok("health flags lake override", r.json()["unverified_lake_override"] is True)
ok("root endpoint open", c.get("/").status_code == 200)

# --- auth gate --------------------------------------------------------------
ok("no key            -> 401", c.post("/sessions", json={}).status_code == 401)
ok("wrong key         -> 401", c.post("/sessions", json={}, headers={"X-API-Key": "nope"}).status_code == 401)
ok("valid key         -> 200", c.post("/sessions", json={}, headers=A).status_code == 200)
ok("bearer form works", c.get("/whoami", headers={"Authorization": "Bearer key-bob"}).json()["caller"] == "bob")

# --- session ownership ------------------------------------------------------
sid = c.post("/sessions", json={}, headers=A).json()["session_id"]
ok("owner can observe  -> 200", c.get(f"/sessions/{sid}", headers=A).status_code == 200)
ok("other caller       -> 404", c.get(f"/sessions/{sid}", headers=B).status_code == 404)
ok("other cannot act   -> 404", c.post(f"/sessions/{sid}/act", json={"action": "approve"}, headers=B).status_code == 404)
ok("other cannot drop  -> 404", c.delete(f"/sessions/{sid}", headers=B).status_code == 404)

# --- per-caller session cap -------------------------------------------------
ok("3rd session        -> 429", c.post("/sessions", json={}, headers=A).status_code == 429)
ok("bob unaffected     -> 200", c.post("/sessions", json={}, headers=B).status_code == 200)
c.delete(f"/sessions/{sid}", headers=A)
ok("after delete       -> 200", c.post("/sessions", json={}, headers=A).status_code == 200)

# --- misc -------------------------------------------------------------------
ok("unknown task       -> 400", c.post("/sessions", json={"task": "zzz"}, headers=A).status_code == 400)

s2 = c.post("/sessions", json={}, headers=B).json()["session_id"]
c.post(f"/sessions/{s2}/act", json={"action": "approve"}, headers=B)
done = c.post(f"/sessions/{s2}/act", json={"action": "approve"}, headers=B).json()["packet"]["done"]
ok("done episode evicted", done and c.get(f"/sessions/{s2}", headers=B).status_code == 404)

# --- idle session expiry ----------------------------------------------------
s3 = c.post("/sessions", json={}, headers=B).json()["session_id"]
_ttl, server.SESSION_TTL = server.SESSION_TTL, -1     # everything is stale
ok("idle session swept -> 404", c.get(f"/sessions/{s3}", headers=B).status_code == 404)
server.SESSION_TTL = _ttl

# --- rate limit -------------------------------------------------------------
_rl, server.RATE_LIMIT = server.RATE_LIMIT, 3
server._HITS.clear()
codes = [c.get("/whoami", headers=A).status_code for _ in range(5)]
ok("rate limit kicks in", codes.count(200) == 3 and codes[-1] == 429)
ok("other caller unthrottled", c.get("/whoami", headers=B).status_code == 200)
server.RATE_LIMIT = _rl
server._HITS.clear()

# --- the lake gate: the whole point ----------------------------------------
# With the override off and no AWS credentials, the probe must fail and the
# server must REFUSE rather than serve an empty world.
os.environ["GYM_ALLOW_UNVERIFIED_LAKE"] = "0"
os.environ.pop("S3_BUCKET", None)
r = c.post("/sessions", json={}, headers=A)
ok("unreadable lake    -> 503", r.status_code == 503)
ok("503 explains why", "empty world" in r.json().get("detail", ""))
h = c.get("/health").json()
ok("health stays 200 when lake down", h["ok"] is True)
ok("health reports lake not ok", h["lake"]["ok"] is False)
os.environ["GYM_ALLOW_UNVERIFIED_LAKE"] = "1"

# --- fail closed when unconfigured -----------------------------------------
os.environ["GYM_API_KEYS"] = ""
ok("unset keys         -> 503", c.post("/sessions", json={}, headers=A).status_code == 503)
ok("health still 200",  c.get("/health").status_code == 200)

print()
print(f"{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
