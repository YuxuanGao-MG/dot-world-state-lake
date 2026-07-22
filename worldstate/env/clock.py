"""Simulation clock — the moving 'now' that makes observations point-in-time.

The agent lives at `cursor`. Everything it can see is filtered by
knowledge_time <= cursor, so advancing the clock is literally the world
revealing new information. No lookahead is possible by construction.
"""
from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass
class SimClock:
    start: pd.Timestamp
    end: pd.Timestamp
    step_days: int = 1
    cursor: pd.Timestamp = None

    def __post_init__(self):
        self.start = pd.Timestamp(self.start, tz="UTC")
        self.end = pd.Timestamp(self.end, tz="UTC")
        if self.cursor is None:
            self.cursor = self.start

    def advance(self) -> pd.Timestamp:
        self.cursor = self.cursor + pd.Timedelta(days=self.step_days)
        return self.cursor

    @property
    def done(self) -> bool:
        return self.cursor >= self.end

    def iso(self) -> str:
        return self.cursor.strftime("%Y-%m-%d %H:%M:%S")
