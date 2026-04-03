"""Avro Schema Encoder - Handle Avro serialization."""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


class Serializer(ABC):
    """Abstract interface for data serialization."""

    @abstractmethod
    def serialize(self, data: dict) -> bytes:
        """Serialize data to bytes."""
        pass


class AvroSerializerImpl(Serializer):
    """Avro serializer implementation - handles schema registration & serialization."""

    def __init__(self, sr_client: SchemaRegistryClient, schema_dict: dict, topic: str) -> None:
        """
        Initialize Avro serializer with schema.
        
        Args:
            sr_client: Schema Registry client (manages registration internally)
            schema_dict: Schema definition as dictionary
            topic: Kafka topic name
        """
        self._serializer = AvroSerializer(
            sr_client,
            json.dumps(schema_dict),
        )
        self._ctx = SerializationContext(topic, MessageField.VALUE)

    def serialize(self, data: dict) -> bytes:
        """Serialize data to Avro bytes.
        
        Note: AvroSerializer handles schema registration automatically on first use.
        """
        return self._serializer(data, self._ctx)


class AvroEncoder:
    """Encode data to Avro format (wrapper around Serializer)."""

    def __init__(self, serializer: Serializer) -> None:
        """
        Initialize encoder with a serializer.
        
        Args:
            serializer: Serializer implementation (typically AvroSerializerImpl)
        """
        self.serializer = serializer

    def encode(self, data: dict) -> bytes:
        """Encode data using the injected serializer."""
        return self.serializer.serialize(data)
