"""Is the lake actually readable?

This exists because of a specific, repeated failure mode: everything in this
stack degrades quietly. ObservationBuilder._df() swallows query exceptions and
returns an empty DataFrame, so a server with no AWS credentials still answers
POST /sessions with a well-formed 200 whose every channel is empty -- an agent
gets a coherent-looking episode about a world containing nothing, and reports a
reward, and nobody notices. The daily cron had the same shape: green for weeks
while the derived feature layer was dead.

So we probe before serving, and we fail loudly.

The probe deliberately goes through DuckDB rather than boto3. store.exists()
would confirm credentials and bucket reachability, but observations are served
by DuckDB's httpfs reader with its own S3 secret -- a different code path that
can fail on its own (missing secret, httpfs not installed, bad region). Probing
the path we don't serve from would be theatre.
"""
from __future__ import annotations

import os
import threading
import time

# Cheap: DuckDB reads one file's Parquet footer, not the data.
_PROBE_DOMAIN = ("macro", "alfred")
_TTL_SECONDS = float(os.environ.get("GYM_HEALTH_TTL", "60"))

_lock = threading.Lock()
_cache: dict = {"at": 0.0, "ok": False, "detail": "not probed yet"}


def _probe() -> tuple[bool, str]:
    try:
        from worldstate import query
    except Exception as e:  # pragma: no cover - import-time environment problem
        return False, f"cannot import query engine: {type(e).__name__}: {e}"

    try:
        con = query.connect()
    except Exception as e:
        return False, f"cannot connect to storage backend: {type(e).__name__}: {e}"

    try:
        glob = query._glob(*_PROBE_DOMAIN)
        con.execute(
            f"SELECT 1 FROM read_parquet('{glob}', hive_partitioning=1) LIMIT 1"
        ).fetchall()
        return True, "ok"
    except Exception as e:
        msg = str(e).split("\n")[0][:200]
        return False, f"{type(e).__name__}: {msg}"
    finally:
        try:
            con.close()
        except Exception:
            pass


def lake_status(force: bool = False) -> dict:
    """Cached readability check. Cached because /health may be polled by an
    uptime monitor and each probe is a network round trip."""
    now = time.monotonic()
    with _lock:
        fresh = (now - _cache["at"]) < _TTL_SECONDS
        if fresh and not force:
            return {"ok": _cache["ok"], "detail": _cache["detail"], "cached": True}

    ok, detail = _probe()
    with _lock:
        _cache.update({"at": time.monotonic(), "ok": ok, "detail": detail})
    return {"ok": ok, "detail": detail, "cached": False}
