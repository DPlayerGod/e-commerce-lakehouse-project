"""Schema definitions for Bronze layer."""

from __future__ import annotations

BRONZE_RAW_EVENTS_COLUMNS_SQL = """
    event_source STRING,
    event_time TIMESTAMP,
    schema_id INT,
    payload_size INT,
    json_payload STRING,
    partition INT,
    offset BIGINT
"""

# Dead Letter Queue (DLQ) schema - for failed/corrupt records
BRONZE_DLQ_COLUMNS_SQL = """
    event_source STRING,
    event_time TIMESTAMP,
    partition INT,
    offset BIGINT,
    raw_value BINARY,
    error_reason STRING,
    error_timestamp TIMESTAMP
"""

# Topic classification
CDC_TOPICS = ["demo.public.users", "demo.public.products"]
STREAMING_TOPICS = ["orders.v1", "payments.v1", "shipments.v1", "delivery-status.v1"]
