"""Storage backend dispatcher. Collectors import this (as `hfstore`) so the
same code writes to S3 (default, fast) or HF (mirror) based on STORAGE_BACKEND.
"""
from __future__ import annotations

import os
from config import settings

_backend = os.environ.get("STORAGE_BACKEND", settings.STORAGE_BACKEND)

if _backend == "hf":
    from worldstate import hfstore as _impl
else:
    from worldstate import s3store as _impl

exists = _impl.exists
upload_table = _impl.upload_table
upload_tables = _impl.upload_tables
shard_path = _impl.shard_path
ensure_repo = _impl.ensure_repo
backend = _backend
