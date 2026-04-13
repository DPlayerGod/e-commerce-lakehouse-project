#!/usr/bin/env python3
"""Local test script - Generate sample events."""
from __future__ import annotations

import json
import random
from config import Config
from services.common import HotCache, now_ms, rid
from services.orders import OrderService
from services.deliveries import DeliveryService
from domain.enums import CURRENCIES, PAYMENT_METHODS, PAYMENT_STATUS, CARRIERS


class MockEncoder:
    """Mock encoder that converts data to JSON bytes for testing."""

    def __init__(self, topic: str):
        self.topic = topic

    def encode(self, data: dict) -> bytes:
        """Encode data as JSON bytes."""
        return bytes(json.dumps(data), 'utf-8')


class MockPublisher:
    """Mock publisher that prints events instead of sending to Kafka."""

    def __init__(self):
        self.events = []

    def publish(self, topic: str, key: str, value: bytes, headers=None) -> None:
        """Print event instead of publishing."""
        try:
            # Decode bytes to JSON string and parse
            value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
            event = json.loads(value_str)
            self.events.append({
                "topic": topic,
                "key": key,
                "value": event,
                "headers": headers,
            })
            print(f"\n📤 [{topic}] {key}")
            print(f"   {json.dumps(event, indent=2)}")
        except Exception as e:
            print(f"❌ Error parsing: {e}")

    def poll(self) -> None:
        pass

    def flush(self, timeout: int = 10) -> None:
        pass


def test_order_generation():
    """Test order generation."""
    print("=" * 80)
    print("🧪 E-commerce Data Generator - Local Test")
    print("=" * 80)

    cfg = Config()
    cache = HotCache()
    publisher = MockPublisher()

    # Create mock encoders
    order_encoder = MockEncoder(cfg.topic_orders)
    payment_encoder = MockEncoder(cfg.topic_payments)
    shipment_encoder = MockEncoder(cfg.topic_shipments)
    delivery_encoder = MockEncoder(cfg.topic_deliveries)

    # Seed cache
    print("\n📋 Seeding cache...", flush=True)
    cache.users.extend([rid("usr", 8) for _ in range(10)])
    for _ in range(10):
        prd_id = rid("prd", 8)
        cache.products.append(prd_id)
        # Set random price for this product
        cache.product_prices[prd_id] = round(random.uniform(10.0, 500.0), 2)
    print(f"   ✅ {len(cache.users)} users")
    print(f"   ✅ {len(cache.products)} products")

    # Create order service
    service = OrderService(cfg, cache, publisher, order_encoder, payment_encoder, shipment_encoder)
    delivery_service = DeliveryService(cfg, publisher, delivery_encoder)

    # Generate test events
    print("\n🚀 Generating 3 test transactions...\n")
    for i in range(3):
        print(f"\n{'=' * 80}")
        print(f"Transaction #{i+1}")
        print(f"{'=' * 80}")
        shipment_data = service.emit()
        
        # If shipment was created, emit delivery event
        if shipment_data:
            delivery_service.emit_delivery(
                shipment_data["shipment_id"],
                shipment_data["order_id"],
                shipment_data["shipment_ts"],
                shipment_data["eta_days"],
            )

    # Summary
    print(f"\n\n{'=' * 80}")
    print("📊 Summary")
    print(f"{'=' * 80}")
    
    topics_count = {}
    for event in publisher.events:
        topic = event["topic"]
        topics_count[topic] = topics_count.get(topic, 0) + 1
    
    print(f"Total events generated: {len(publisher.events)}")
    print(f"\nEvents by topic:")
    for topic, count in sorted(topics_count.items()):
        print(f"  • {topic}: {count}")

    print(f"\n✅ Test completed!")


if __name__ == "__main__":
    test_order_generation()
