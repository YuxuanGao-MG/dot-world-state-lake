"""Thin layer over the Hugging Face dataset repo — the canonical store.

Design for parallel, resumable GitHub Actions jobs:
  * Every shard is written to a DETERMINISTIC path derived from its content
    scope (domain/source/partition/name). Re-running a chunk overwrites the same
    path, so the pipeline is idempotent and safe to retry.
  * `exists()` lets a job cheaply skip work already on HF — that is our
    checkpoint. No central mutable state, so parallel jobs never contend.
  * The catalog is DERIVED by listing repo files (scripts/build_catalog.py),
    never mutated concurrently.
"""
from __future__ import annotations

import io
import os
import time
import pyarrow as pa
import pyarrow.parquet as pq
from functools import lru_cache
from huggingface_hub import HfApi, CommitOperationAdd

from config import settings


def _with_retry(fn, *, tries: int = 8, base: float = 5.0):
    """Retry an HF API call, honoring Retry-After and backing off long enough to
    ride out HF's account-level 'api' rate limit under many concurrent jobs."""
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # HfHubHTTPError / connection errors
            last = e
            if i == tries - 1:
                raise
            wait = base * (2 ** i)
            resp = getattr(e, "response", None)
            ra = getattr(resp, "headers", {}).get("Retry-After") if resp is not None else None
            if ra:
                try:
                    wait = max(wait, float(ra))
                except (TypeError, ValueError):
                    pass
            time.sleep(min(wait, 120))
    raise last  # pragma: no cover


def _token() -> str:
    tok = os.environ.get("HF_TOKEN")
    if not tok:
        raise RuntimeError("HF_TOKEN not set in environment")
    return tok


def _repo() -> str:
    repo = settings.HF_DATASET_REPO or os.environ.get("HF_DATASET_REPO", "")
    if not repo:
        raise RuntimeError("HF_DATASET_REPO not set (e.g. 'user/world-state-lake')")
    return repo


@lru_cache(maxsize=1)
def api() -> HfApi:
    return HfApi(token=_token())


def ensure_repo() -> str:
    """Return the dataset repo id. The repo is created once at bootstrap; every
    job calling create_repo just burns HF's 'api' rate budget, so this is a no-op
    unless WORLDSTATE_INIT_REPO is set (used only for first-time setup)."""
    repo = _repo()
    if os.environ.get("WORLDSTATE_INIT_REPO"):
        try:
            api().create_repo(repo, repo_type=settings.HF_REPO_TYPE, private=True, exist_ok=True)
        except Exception:
            pass
    return repo


@lru_cache(maxsize=1)
def _listing() -> set[str]:
    try:
        return set(api().list_repo_files(_repo(), repo_type=settings.HF_REPO_TYPE))
    except Exception:
        return set()


def exists(path_in_repo: str) -> bool:
    return path_in_repo in _listing()


def _table_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def upload_table(table: pa.Table, path_in_repo: str, *, overwrite: bool = False) -> bool:
    """Serialize a pyarrow Table to Parquet and upload (with retry/backoff).
    Returns True if uploaded, False if skipped because it already existed."""
    if not overwrite and exists(path_in_repo):
        return False
    data = _table_bytes(table)
    _with_retry(lambda: api().upload_file(
        path_or_fileobj=data, path_in_repo=path_in_repo, repo_id=_repo(),
        repo_type=settings.HF_REPO_TYPE, commit_message=f"add {path_in_repo}"))
    _listing.cache_clear()
    return True


def upload_tables(pairs: list[tuple[pa.Table, str]], *, commit_message: str,
                  overwrite: bool = False) -> int:
    """Upload many (table, path) shards in ONE commit — drastically fewer HF API
    calls than per-shard commits, which avoids rate-limiting when a job produces
    many shards. Returns the number of shards written."""
    ops = []
    for table, path in pairs:
        if not overwrite and exists(path):
            continue
        ops.append(CommitOperationAdd(path_in_repo=path, path_or_fileobj=_table_bytes(table)))
    if not ops:
        return 0
    _with_retry(lambda: api().create_commit(
        repo_id=_repo(), repo_type=settings.HF_REPO_TYPE,
        operations=ops, commit_message=commit_message))
    _listing.cache_clear()
    return len(ops)


def shard_path(domain: str, source: str, *parts: str, name: str) -> str:
    """Hive-partitioned path: data/domain=market/source=stooq/year=2021/part-x.parquet"""
    segs = [settings.DATA_PREFIX, f"domain={domain}", f"source={source}", *parts, name]
    return "/".join(segs)
