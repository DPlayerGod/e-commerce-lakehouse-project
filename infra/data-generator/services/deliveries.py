"""Delivery Service - Generate delivery status events after shipment eta_days."""
from __future__ import annotations

import random
from config import Config
from domain.enums import DELIVERY_STATUS, DELIVERY_REASONS
from ports.event_publisher import EventPublisher
from services.common import now_ms, rid


class DeliveryService:
    """Generate delivery status events for shipments."""

    def __init__(self, cfg: Config, publisher: EventPublisher, delivery_encoder) -> None:
        self.cfg = cfg
        self.publisher = publisher
        self.delivery_encoder = delivery_encoder

    def emit_delivery(self, shipment_id: str, order_id: str, shipment_ts: int, eta_days: int) -> None:
        """
        Generate delivery status event.
        
        Args:
            shipment_id: ID of the shipment
            order_id: ID of the order
            shipment_ts: Timestamp when shipment was created (ms)
            eta_days: Expected delivery days (1-7)
        
        Timing: delivery_ts = shipment_ts + (eta_days * 86400000) + random(0-30h)
        """
        # Calculate when delivery should occur
        # 86400000 ms = 1 day
        delivery_window_start_ms = shipment_ts + (eta_days * 86400000)
        
        # Add random delay: 0-30 hours after deadline
        random_delay_ms = random.randint(0, 30 * 3600 * 1000)
        delivery_ts = delivery_window_start_ms + random_delay_ms
        
        # Pick status (80% DELIVERED, 15% FAILED, 5% RETURNED)
        status = random.choices(
            DELIVERY_STATUS,
            weights=[80, 15, 5]
        )[0]
        
        # Pick reason based on status
        reason = random.choice(DELIVERY_REASONS[status])
        
        # Emit delivery event
        event = {
            "delivery_id": rid("del"),
            "shipment_id": shipment_id,
            "order_id": order_id,
            "status": status,
            "reason": reason,
            "ts": delivery_ts
        }
        
        self.publisher.publish(
            self.cfg.topic_deliveries,
            key=shipment_id,
            value=self.delivery_encoder.encode(event)
        )
