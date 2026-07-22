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
uvicorn worldstate.env.server:app       # needs AWS creds in env
# POST /sessions -> {session_id, packet};  POST /sessions/{id}/act {"action":"reject"}
```

**From your phone:** Actions → **env-demo** → Run → read the reset/act/reward loop
in the log (a baseline rule agent plays it).

## Roadmap
- More observation channels (positioning, surprise index, on-chain, sector maps).
- Richer tasks: trading (PnL reward), forecasting with calibration, anomaly triage.
- Access tiers (free vs premium data) so trajectories include the "what's my
  access / which tool" step — the original RL-trajectory vision.
- Episode logging → trajectory datasets for training.
