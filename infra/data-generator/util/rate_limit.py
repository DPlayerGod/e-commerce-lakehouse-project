"""Token Bucket Rate Limiting."""
from __future__ import annotations

import math
from time import monotonic
from typing import Callable


def diurnal_multiplier() -> float:
    """Return 0.4..1.4 multiplier based on local hour."""
    from datetime import datetime

    hour = datetime.now().hour
    return 0.9 + 0.5 * math.sin((hour - 3) / 24 * 2 * math.pi)


class TokenBucket:
    """Rate limiting with diurnal multiplier."""

    def __init__(self, rate: float, curve: Callable[[], float] = diurnal_multiplier) -> None:
        self.rate = rate
        self.curve = curve
        self._tokens = 0.0
        self._last = monotonic()

    def refill(self) -> None:
        """Add tokens based on elapsed time."""
        now = monotonic()
        self._tokens += self.rate * self.curve() * (now - self._last)
        self._last = now

    def try_consume(self, n: float = 1.0) -> bool:
        """Try to consume n tokens."""
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False
