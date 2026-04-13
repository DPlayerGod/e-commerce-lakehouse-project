"""Re-export configuration from parent module for backward compatibility."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    KAFKA_BOOTSTRAP,
    SCHEMA_REGISTRY_URL,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SPARK_CONF,
    BRONZE_TOPICS,
    BRONZE_TOPIC_TO_TABLE,
    BRONZE_TOPIC_TO_CHECKPOINT,
)

__all__ = [
    "KAFKA_BOOTSTRAP",
    "SCHEMA_REGISTRY_URL",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "SPARK_CONF",
    "BRONZE_TOPICS",
    "BRONZE_TOPIC_TO_TABLE",
    "BRONZE_TOPIC_TO_CHECKPOINT",
]
