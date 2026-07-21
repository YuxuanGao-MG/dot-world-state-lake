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
import pyarrow as pa
import pyarrow.parquet as pq
from functools import lru_cache
from huggingface_hub import HfApi

from config import settings


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
    """Create the private dataset repo if missing; return its id."""
    repo = _repo()
    api().create_repo(repo, repo_type=settings.HF_REPO_TYPE, private=True, exist_ok=True)
    return repo


@lru_cache(maxsize=1)
def _listing() -> set[str]:
    try:
        return set(api().list_repo_files(_repo(), repo_type=settings.HF_REPO_TYPE))
    except Exception:
        return set()


def exists(path_in_repo: str) -> bool:
    return path_in_repo in _listing()


def upload_table(table: pa.Table, path_in_repo: str, *, overwrite: bool = False) -> bool:
    """Serialize a pyarrow Table to Parquet and upload. Returns True if uploaded,
    False if skipped because it already existed."""
    if not overwrite and exists(path_in_repo):
        return False
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    api().upload_file(
        path_or_fileobj=buf,
        path_in_repo=path_in_repo,
        repo_id=_repo(),
        repo_type=settings.HF_REPO_TYPE,
        commit_message=f"add {path_in_repo}",
    )
    _listing.cache_clear()
    return True


def shard_path(domain: str, source: str, *parts: str, name: str) -> str:
    """Hive-partitioned path: data/domain=market/source=stooq/year=2021/part-x.parquet"""
    segs = [settings.DATA_PREFIX, f"domain={domain}", f"source={source}", *parts, name]
    return "/".join(segs)
