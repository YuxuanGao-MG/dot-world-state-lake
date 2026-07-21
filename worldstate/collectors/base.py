"""Collector base class + shared HTTP plumbing.

A Collector knows how to (1) enumerate its backfill work as a list of opaque
`chunk` ids and (2) run a single chunk: fetch -> normalize to the bitemporal
envelope -> upload a Parquet shard to HF, skipping shards already present.
"""
from __future__ import annotations

import time
import requests
from requests.adapters import HTTPAdapter, Retry

from config import settings


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": settings.USER_AGENT})
    retry = Retry(
        total=5, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


class RateLimiter:
    def __init__(self, hz: float):
        self.min_interval = 1.0 / hz if hz > 0 else 0.0
        self._last = 0.0

    def wait(self):
        dt = time.monotonic() - self._last
        if dt < self.min_interval:
            time.sleep(self.min_interval - dt)
        self._last = time.monotonic()


class Collector:
    domain: str = "base"
    source: str = "base"

    def __init__(self):
        self.session = make_session()

    def chunks(self) -> list[str]:
        """Opaque work-unit ids covering the full backfill for this collector."""
        raise NotImplementedError

    def run_chunk(self, chunk: str, force: bool = False) -> dict:
        """Fetch+normalize+upload one chunk. Return a small stats dict."""
        raise NotImplementedError
