"""Avro Schema Encoder - Handle Avro serialization."""
from __future__ import annotations

import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.schema_registry_client import Schema
from io import BytesIO


class AvroEncoder:
    """Encode data to Avro format using Schema Registry."""

    def __init__(self, schema_registry_client: SchemaRegistryClient, topic: str, schema_dict: dict) -> None:
        self.sr_client = schema_registry_client
        self.topic = topic
        self.schema_dict = schema_dict
        self.schema_id = None
        self._register_schema()

    def _register_schema(self) -> None:
        """Register schema with Schema Registry."""
        schema_str = json.dumps(self.schema_dict)
        subject = f"{self.topic}-value"
        try:
            schema = Schema(schema_str, schema_type="AVRO")
            self.schema_id = self.sr_client.register_schema(subject, schema)
        except Exception as e:
            print(f"[avro] ⚠️ Schema registration failed: {e}")

    def encode(self, data: dict) -> bytes:
        """Encode data to Avro bytes."""
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import SerializationContext, MessageField

        serializer = AvroSerializer(
            self.sr_client,
            json.dumps(self.schema_dict),
        )

        ctx = SerializationContext(self.topic, MessageField.VALUE)
        return serializer(data, ctx)
