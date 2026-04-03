"""Kafka Factory - Build producer."""
from __future__ import annotations

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from config import Config
from adapters.kafka.publisher import KafkaPublisher
from adapters.kafka.encoder import AvroEncoder, AvroSerializerImpl
from adapters.kafka.schemas import (
    ORDERS_SCHEMA,
    PAYMENTS_SCHEMA,
    SHIPMENTS_SCHEMA,
    DELIVERIES_SCHEMA,
)


def build_kafka(config: Config) -> tuple[KafkaPublisher, dict]:
    """Build and configure Kafka producer with Avro encoders.
    
    Returns:
        Tuple of (publisher, encoders_dict)
    """
    
    producer = SerializingProducer({
        "bootstrap.servers": config.bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 25,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "key.serializer": StringSerializer("utf_8"),
        "queue.buffering.max.messages": 200000,
    })
    
    # Initialize Schema Registry client
    sr_client = SchemaRegistryClient({"url": config.schema_registry})
    
    # Create encoders for each topic with dedicated serializers
    # ✅ Clean: each serializer created once, reused forever
    # ✅ No double registration: AvroSerializer handles schema registration internally
    encoders = {
        config.topic_orders: AvroEncoder(
            AvroSerializerImpl(sr_client, ORDERS_SCHEMA, config.topic_orders)
        ),
        config.topic_payments: AvroEncoder(
            AvroSerializerImpl(sr_client, PAYMENTS_SCHEMA, config.topic_payments)
        ),
        config.topic_shipments: AvroEncoder(
            AvroSerializerImpl(sr_client, SHIPMENTS_SCHEMA, config.topic_shipments)
        ),
        config.topic_deliveries: AvroEncoder(
            AvroSerializerImpl(sr_client, DELIVERIES_SCHEMA, config.topic_deliveries)
        ),
    }
    
    return KafkaPublisher(producer), encoders
