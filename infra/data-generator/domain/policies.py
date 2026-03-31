"""Policies - Business rules and behaviors."""
from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class FaultPolicy:
    """Fault injection for data quality testing."""

    p_bad_record: float = 0.0

    def apply(self, obj: dict) -> dict:
        """Randomly corrupt a record for testing."""
        if self.p_bad_record <= 0:
            return obj
        if random.random() >= self.p_bad_record:
            return obj
        
        choice = random.choice(["drop_amount", "null_order"])
        out = dict(obj)
        if choice == "drop_amount" and "amount" in out:
            out["amount"] = None
        elif choice == "null_order" and "order_id" in out:
            out["order_id"] = None
        return out
