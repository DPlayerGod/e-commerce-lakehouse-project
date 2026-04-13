"""Common utilities - IDs, timestamps, cache."""
from __future__ import annotations

import random
import string
from collections import deque
from datetime import datetime, timezone
from typing import Deque


def now_ms() -> int:
    """Return current timestamp in milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def rid(prefix: str, n: int = 10) -> str:
    """Generate random ID with prefix."""
    return prefix + "_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


class HotCache:
    """Simple LRU cache for frequently accessed entities."""

    def __init__(self) -> None:
        self.users: Deque[str] = deque(maxlen=1000)
        self.products: Deque[str] = deque(maxlen=1000)
        self.product_prices: dict = {}  # product_id -> price mapping
        self.orders: Deque[str] = deque(maxlen=3000)

    def pick_user(self) -> str:
        """Pick a user from seeded cache."""
        if not self.users:
            return None
        return random.choice(list(self.users))

    def pick_product(self) -> str:
        """Pick a product from seeded cache."""
        if not self.products:
            return None
        return random.choice(list(self.products))

    def get_product_price(self, product_id: str) -> float | None:
        """Get price for product."""
        return self.product_prices.get(product_id)


def ensure_str(v, fallback: str) -> str:
    """Ensure value is string."""
    return v if isinstance(v, str) and v != "" else fallback


def ensure_float(v, fallback: float) -> float:
    """Ensure value is float."""
    try:
        return float(v)
    except Exception:
        return float(fallback)


def ensure_int(v, fallback: int) -> int:
    """Ensure value is int."""
    try:
        return int(v)
    except Exception:
        return int(fallback)
