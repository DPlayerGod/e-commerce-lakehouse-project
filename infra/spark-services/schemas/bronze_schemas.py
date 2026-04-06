"""Schema definitions for Bronze layer (Pure Raw - Avro bytes only)."""

from __future__ import annotations

# Pure Raw: Store Avro bytes as-is, no deserialization
BRONZE_RAW_EVENTS_COLUMNS_SQL = """
    event_source STRING,
    event_time TIMESTAMP,
    partition INT,
    offset BIGINT,
    raw_value BINARY,
    processed_at TIMESTAMP
"""

# Topic classification
CDC_TOPICS = ["demo.public.users", "demo.public.products"]
STREAMING_TOPICS = ["orders.v1", "payments.v1", "shipments.v1", "delivery-status.v1"]
