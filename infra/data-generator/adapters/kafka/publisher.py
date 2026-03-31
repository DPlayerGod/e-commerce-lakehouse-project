"""Kafka Publisher implementation."""
from __future__ import annotations

from confluent_kafka import SerializingProducer

from ports.event_publisher import EventPublisher


class KafkaPublisher(EventPublisher):
    """Publish events to Kafka."""

    def __init__(self, producer: SerializingProducer) -> None:
        self.producer = producer
        self.publish_count = {}

    def publish(self, topic: str, key: str, value: bytes, headers=None) -> None:
        """Publish a message to Kafka."""
        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
            )
            # Track successful publishes
            if topic not in self.publish_count:
                self.publish_count[topic] = 0
            self.publish_count[topic] += 1
            
            # Log every 100 publishes
            if self.publish_count[topic] % 100 == 1:
                print(f"[kafka] Published {self.publish_count[topic]} to {topic}")
        except Exception as e:
            print(f"❌ Error publishing to {topic}: {e}")

    def poll(self) -> None:
        """Poll for delivery callbacks."""
        self.producer.poll(0)

    def flush(self, timeout: int = 10) -> None:
        """Flush pending messages."""
        self.producer.flush(timeout)
