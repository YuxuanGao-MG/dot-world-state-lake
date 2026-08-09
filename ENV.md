# The Financial-Agent Gym (`worldstate/env/`)

A **Gymnasium-style environment** where an agent is dropped into the world at a
point in time and steps forward through the **information universe**, receiving
point-in-time observations (zero lookahead) and decision prompts, scored by a
pluggable **Task**. This is the RL surface over the DoT world-state lake — and
the home of the **data-approval** case study.

## The loop
```
reset()            -> packet: observation (world as-of cursor) + task prompt
step(action)       -> packet: reward for last prompt, then advance the clock and
                              return the next observation + prompt
```
`packet.text` is a ready-to-send LLM prompt (world summary + task instruction +
the record to judge). Everything in the observation satisfies
`knowledge_time <= cursor`, so the agent can never see the future. Scoring may
use an **oracle** (future data) that the agent never receives.

## Pieces
| Module | Role |
|---|---|
| `clock.py` | `SimClock` — the moving "now"; advancing it reveals information |
| `observation.py` | `ObservationBuilder` — queries the lake as-of the cursor (prices, macro, news, filings), returns structured + text |
| `tasks.py` | `Task` ABC + `DataApprovalTask` (approve/reject an incoming record, anomalies injected for ground-truthed reward) + `ForecastTask` |
| `env.py` | `WorldStateEnv` — reset/step/observe + oracle |
| `server.py` | FastAPI: agents enter over HTTP |

## Run it
**In-process (Python):**
```python
from worldstate.env import WorldStateEnv, DataApprovalTask
env = WorldStateEnv(task=DataApprovalTask(), start="2021-01-04", end="2024-12-31", step_days=3)
pkt = env.reset()
while not pkt["done"]:
    action = my_agent(pkt["text"])      # "approve" / "reject"
    pkt = env.step(action)
```

**As an API** (agent enters over HTTP):
```
pip install -r requirements-env.txt
export GYM_API_KEYS="alice:$(python -c 'import secrets;print(secrets.token_urlsafe(24))')"
uvicorn worldstate.env.server:app       # needs AWS creds in env
# POST /sessions -> {session_id, packet};  POST /sessions/{id}/act {"action":"reject"}
```

## Letting an outsider in (whitelist)

The server holds AWS credentials for a **private** bucket, so it is never open.
Access is one env var — comma-separated `name:key` pairs:

```
GYM_API_KEYS="alice:s3cret-one,bob:s3cret-two"
```

Add someone → append a pair and restart. Revoke → delete the pair and restart.
No database, no user table. Callers send `X-API-Key: <key>` (or
`Authorization: Bearer <key>`).

Mint a key with `python -c "import secrets; print(secrets.token_urlsafe(24))"`.

| Behaviour | |
|---|---|
| `GYM_API_KEYS` unset | **fails closed** — every authed route 503s, so a misconfigured deploy can't leak the lake |
| `/health` | open (liveness needs no key), reports no session detail |
| Sessions | scoped to their creator; someone else's id returns **404**, not 403, so ids can't be probed |
| `GYM_MAX_SESSIONS` | per-caller concurrent cap, default 3 — each env holds a DuckDB connection and drives S3 egress |
| Key comparison | constant-time (`secrets.compare_digest`) |

Client sketch:
```bash
curl -sX POST $GYM/sessions -H "X-API-Key: $KEY" \
     -H 'content-type: application/json' -d '{"task":"data_approval"}'
curl -sX POST $GYM/sessions/$SID/act -H "X-API-Key: $KEY" \
     -H 'content-type: application/json' -d '{"action":"reject"}'
```

Verify with `python tests/test_server_auth.py` (stubs the env; no AWS needed).

Still worth knowing before you scale it: sessions live in a process-local dict,
so this is **single-replica** — a restart drops in-flight episodes, and you can't
run two instances behind a load balancer without moving session state out. There
is no per-request rate limit either; the session cap is the only throttle.

**From your phone:** Actions → **env-demo** → Run → read the reset/act/reward loop
in the log (a baseline rule agent plays it).

## Roadmap
- More observation channels (positioning, surprise index, on-chain, sector maps).
- Richer tasks: trading (PnL reward), forecasting with calibration, anomaly triage.
- (done) Access tiers + tools (`tools.py`): basic/pro, budget; tool-calls are
  intermediate steps so trajectories include the access/tool decisions.
- (done) Trajectory logging (`trajectory.py`) -> domain=trajectories/source=env.
- (done) LLM agent (`llm_agent.py`) via OpenMesh/OpenRouter — models: gemini-3-flash,
  deepseek-v4-flash, claude-sonnet-4.6, kimi-k2.6, gpt-5.4, … (`collect-trajectories --agent llm --model X`).
- Next: reward shaping; large multi-model trajectory corpora; drift-of-thought analysis on traces.
