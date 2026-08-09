"""Exercise the gym whitelist without touching AWS: stub WorldStateEnv."""
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

from fastapi.testclient import TestClient
from worldstate.env import server

c = TestClient(server.app)
A = {"X-API-Key": "key-alice"}
B = {"X-API-Key": "key-bob"}
ok = lambda label, cond: print(("PASS  " if cond else "FAIL  ") + label)

# health is open
r = c.get("/health"); ok("health open, no key needed", r.status_code == 200)
ok("health reports configured=True", r.json()["configured"] is True)

# auth gate
ok("no key            -> 401", c.post("/sessions", json={}).status_code == 401)
ok("wrong key         -> 401", c.post("/sessions", json={}, headers={"X-API-Key": "nope"}).status_code == 401)
ok("valid key         -> 200", c.post("/sessions", json={}, headers=A).status_code == 200)
ok("bearer form works", c.get("/whoami", headers={"Authorization": "Bearer key-bob"}).json()["caller"] == "bob")

# session ownership
sid = c.post("/sessions", json={}, headers=A).json()["session_id"]
ok("owner can observe  -> 200", c.get(f"/sessions/{sid}", headers=A).status_code == 200)
ok("other caller       -> 404", c.get(f"/sessions/{sid}", headers=B).status_code == 404)
ok("other cannot act   -> 404", c.post(f"/sessions/{sid}/act", json={"action": "approve"}, headers=B).status_code == 404)
ok("other cannot drop  -> 404", c.delete(f"/sessions/{sid}", headers=B).status_code == 404)

# per-caller cap (alice already holds 2 from above)
ok("3rd session        -> 429", c.post("/sessions", json={}, headers=A).status_code == 429)
ok("bob unaffected     -> 200", c.post("/sessions", json={}, headers=B).status_code == 200)

# release frees a slot
c.delete(f"/sessions/{sid}", headers=A)
ok("after delete       -> 200", c.post("/sessions", json={}, headers=A).status_code == 200)

# bad task
ok("unknown task       -> 400", c.post("/sessions", json={"task": "zzz"}, headers=A).status_code == 400)

# episode completion evicts the session
s2 = c.post("/sessions", json={}, headers=B).json()["session_id"]
c.post(f"/sessions/{s2}/act", json={"action": "approve"}, headers=B)
done = c.post(f"/sessions/{s2}/act", json={"action": "approve"}, headers=B).json()["packet"]["done"]
ok("done episode evicted", done and c.get(f"/sessions/{s2}", headers=B).status_code == 404)

# fail-closed when unconfigured
os.environ["GYM_API_KEYS"] = ""
ok("unset keys         -> 503", c.post("/sessions", json={}, headers=A).status_code == 503)
ok("health still 200",  c.get("/health").status_code == 200)
