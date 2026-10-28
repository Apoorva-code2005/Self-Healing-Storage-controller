from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


class ScenarioClock:
    """Controllable and deterministic scenario clock with fixed tick (default 1s).

    Operates purely via manual advance with zero real sleeping, per PRD 7.5.
    """

    def __init__(
        self,
        tick_size_s: float = 1.0,
        start_time_utc: Optional[datetime] = None,
    ):
        self.tick_size_s = float(tick_size_s)
        self.start_time_utc = start_time_utc or datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)
        self.current_time_s = 0.0

    @property
    def current_tick(self) -> int:
        """Returns the current tick index (0-based integer)."""
        if self.tick_size_s <= 0:
            return 0
        return int(round(self.current_time_s / self.tick_size_s))

    def now_utc(self) -> datetime:
        """Returns current simulated UTC datetime."""
        return self.start_time_utc + timedelta(seconds=self.current_time_s)

    def now_iso(self) -> str:
        """Returns current simulated UTC datetime in ISO-8601 string format."""
        return self.now_utc().isoformat()

    def tick(self, count: int = 1) -> float:
        """Advances clock forward by count ticks. Returns new current_time_s."""
        self.current_time_s += count * self.tick_size_s
        return self.current_time_s

    def advance(self, seconds: float) -> float:
        """Advances clock forward by specified seconds. Returns new current_time_s."""
        self.current_time_s += float(seconds)
        return self.current_time_s

    def reset(self, start_time_utc: Optional[datetime] = None) -> None:
        """Resets simulated elapsed seconds back to 0.0."""
        self.current_time_s = 0.0
        if start_time_utc:
            self.start_time_utc = start_time_utc
