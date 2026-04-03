#!/usr/bin/env python3
"""Bronze ingestion: Kafka (Avro) -> Iceberg on MinIO.

Pipeline: Load schemas -> Deserialize Avro -> Split valid/error -> Stream to Iceberg
"""
from __future__ import annotations

import argparse
import io
import json
import logging
from typing import Optional

import fastavro
import requests
from pyspark.sql import SparkSession, functions as F

from adapters.iceberg import ensure_table, write_stream
from adapters.kafka import read_stream
from adapters.minio import apply_minio_s3a_config, ensure_bucket_exists
from config import (
    BRONZE_TOPICS,
    BRONZE_TABLE,
    BRONZE_CHECKPOINT,
    KAFKA_BOOTSTRAP,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SCHEMA_REGISTRY_URL,
    SPARK_CONF,
)
from schemas.bronze_schemas import (
    BRONZE_RAW_EVENTS_COLUMNS_SQL,
    BRONZE_DLQ_COLUMNS_SQL,
)

APP_NAME = "bronze_ingestion"
LOG = logging.getLogger(__name__)

DLQ_TABLE = "iceberg.bronze.dlq_raw_events"
DLQ_CHECKPOINT = "s3a://data-lake/checkpoints/bronze/dlq_raw_events"


# =========================
# Schema Registry
# =========================
def fetch_schema(topic: str, sr_url: str) -> Optional[str]:
    """Fetch Avro schema from Schema Registry."""
    subject = f"{topic}-value"
    try:
        response = requests.get(
            f"{sr_url}/subjects/{subject}/versions/latest",
            timeout=5
        )
        if response.status_code == 200:
            schema_json = response.json().get("schema")
            LOG.debug(f"✅ Schema fetched: {subject}")
            return schema_json
        LOG.warning(f"⚠️ Schema not found: {subject} (HTTP {response.status_code})")
    except requests.Timeout:
        LOG.error(f"❌ Timeout fetching {subject}")
    except Exception as e:
        LOG.error(f"❌ Schema fetch failed {subject}: {e}")
    return None


def load_schemas(sr_url: str, topics: list[str]) -> dict[str, str]:
    """Load schemas from Schema Registry with validation."""
    LOG.info(f"📋 Loading {len(topics)} schemas from {sr_url}...")
    schemas = {}
    for topic in topics:
        schema = fetch_schema(topic, sr_url)
        schemas[topic] = schema

    loaded = sum(1 for s in schemas.values() if s)
    if loaded == 0:
        raise ValueError(f"❌ No schemas loaded from {sr_url}")

    failed = len(topics) - loaded
    if failed > 0:
        LOG.warning(f"⚠️ Failed to load {failed}/{len(topics)} schemas")
    
    LOG.info(f"✅ Schemas loaded: {loaded}/{len(topics)}")
    return schemas


# =========================
# Utils
# =========================
def extract_schema_id(value_bytes: F.Column) -> F.Column:
    """Extract Schema Registry ID from Avro binary.
    
    Avro format: [magic_byte:1] [schema_id:4] [data:...]
    Returns a constant for now since we deserialize with fastavro.
    """
    return F.when(
        F.length(value_bytes) > 5,
        F.lit(0)  # Placeholder ID
    ).otherwise(F.lit(-1))


# =========================
# Core processing
# =========================
def deserialize_avro(raw_value: bytes, schema_json: str) -> Optional[str]:
    """Deserialize Avro bytes using fastavro. Returns JSON string or None on error.
    
    Kafka format: [magic:1] [schema_id:4] [avro_datum:...]
    We skip the first 5 bytes to get the actual Avro datum, then use schemaless_reader.
    """
    if not raw_value or not schema_json:
        return None
    
    try:
        # Skip Schema Registry header (magic byte + 4-byte schema ID)
        if len(raw_value) < 5:
            LOG.debug(f"Message too short: {len(raw_value)} bytes, need at least 5")
            return None
        
        avro_payload = raw_value[5:]
        if not avro_payload:
            LOG.debug("No Avro payload after header")
            return None
        
        schema = json.loads(schema_json)
        record = fastavro.schemaless_reader(io.BytesIO(avro_payload), schema)
        return json.dumps(record)
    except (ValueError, StopIteration, fastavro.schema.UnknownType, Exception) as e:
        LOG.debug(f"Avro deserialization error: {type(e).__name__}: {str(e)[:100]}")
        return None


def build_avro_case(schemas: dict[str, str]) -> F.Column:
    """Build CASE WHEN expression for Avro deserialization by topic."""
    valid_schemas = {t: s for t, s in schemas.items() if s}
    valid_topics = list(valid_schemas.keys())
    
    if not valid_schemas:
        raise ValueError("No valid schemas found for CASE expression")
    
    # Create a UDF for Avro deserialization
    deserialize_udf = F.udf(
        lambda raw_val, schema_str: deserialize_avro(raw_val, schema_str),
        returnType="string"
    )
    
    # Build first when clause
    topics_list = list(valid_schemas.items())
    case_expr = F.when(
        F.col("event_source") == topics_list[0][0],
        deserialize_udf(F.col("raw_value"), F.lit(topics_list[0][1]))
    )
    
    # Add remaining when clauses
    for topic, schema in topics_list[1:]:
        case_expr = case_expr.when(
            F.col("event_source") == topic,
            deserialize_udf(F.col("raw_value"), F.lit(schema))
        )
    
    LOG.debug(f"CASE expr for {len(valid_topics)} topics: {valid_topics}")
    return case_expr.otherwise(None)


def process_stream(df: object, schemas: dict[str, str]) -> tuple:
    """Process Kafka stream: deserialize Avro, split valid/error."""
    LOG.info(f"🔄 Processing {len(schemas)} topics")
    
    # Standardize columns
    base = df.select(
        F.col("topic").alias("event_source"),
        F.col("timestamp").alias("event_time"),
        "partition",
        "offset",
        F.col("value").alias("raw_value"),
    )
    LOG.debug("✅ Columns standardized")

    # Use CASE WHEN for efficient Avro deserialization
    case_expr = build_avro_case(schemas)
    
    df_processed = base.withColumn(
        "json_payload",
        case_expr
    ).withColumn(
        "schema_id",
        extract_schema_id(F.col("raw_value"))
    ).withColumn(
        "payload_size",
        F.when(F.col("json_payload").isNotNull(), F.length("json_payload")).otherwise(0)
    ).withColumn(
        "error_reason",
        F.when(
            F.col("json_payload").isNull(),
            F.lit("Avro deserialization failed - check schema registry"))
    )
    LOG.debug("✅ Avro deserialization applied")

    # Split into valid and error records
    valid_df = df_processed.filter("json_payload IS NOT NULL").select(
        "event_source", "event_time", "schema_id",
        "payload_size", "json_payload", "partition", "offset"
    )

    error_df = df_processed.filter("json_payload IS NULL").select(
        "event_source", "event_time", "partition", "offset",
        "raw_value", "error_reason",
        F.current_timestamp().alias("error_timestamp")
    )
    
    LOG.info("✅ Records split: valid + error partitions")
    return valid_df, error_df


# =========================
# Main
# =========================
def main() -> None:
    """Main pipeline orchestration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Bronze ingestion pipeline")
    parser.add_argument("--topics", default=",".join(BRONZE_TOPICS))
    parser.add_argument("--table", default=BRONZE_TABLE, help="Iceberg table for valid records")
    parser.add_argument("--checkpoint", default=BRONZE_CHECKPOINT, help="Checkpoint directory for valid stream")
    parser.add_argument("--dlq-table", default=DLQ_TABLE, help="Iceberg table for error records")
    parser.add_argument("--dlq-checkpoint", default=DLQ_CHECKPOINT, help="Checkpoint directory for error stream")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    args = parser.parse_args()

    topics = args.topics.split(",")
    LOG.info(f"📌 Topics: {topics}")

    query_valid = None
    query_dlq = None
    
    try:
        # Build Spark session
        LOG.info("🔧 Building Spark session...")
        builder = SparkSession.builder.appName(APP_NAME)
        for key, value in SPARK_CONF.items():
            builder = builder.config(key, value)
        spark = apply_minio_s3a_config(
            builder,
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        ).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        LOG.info("✅ Spark session created")

        # Ensure MinIO bucket exists
        LOG.info("🪣 Ensuring data-lake bucket exists...")
        ensure_bucket_exists(
            "data-lake",
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        )
        LOG.info("✅ data-lake bucket ready")

        # Ensure tables exist
        LOG.info("📊 Ensuring tables exist...")
        ensure_table(spark, args.table, BRONZE_RAW_EVENTS_COLUMNS_SQL)
        ensure_table(spark, args.dlq_table, BRONZE_DLQ_COLUMNS_SQL)
        LOG.info(f"✅ Tables ready: {args.table}, {args.dlq_table}")

        # Load schemas
        schemas = load_schemas(SCHEMA_REGISTRY_URL, topics)

        # Read Kafka stream
        LOG.info("🔌 Connecting to Kafka...")
        source = read_stream(
            spark,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topics_csv=args.topics,
            starting_offsets=args.starting_offsets,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
        LOG.info("✅ Kafka reader configured")

        # Process stream
        valid_df, error_df = process_stream(source, schemas)

        # Write streams
        LOG.info("📝 Starting stream writes...")
        query_valid = write_stream(
            valid_df,
            table=args.table,
            checkpoint=args.checkpoint,
            trigger_seconds=args.trigger_seconds
        )
        query_dlq = write_stream(
            error_df,
            table=args.dlq_table,
            checkpoint=args.dlq_checkpoint,
            trigger_seconds=args.trigger_seconds
        )
        LOG.info(f"✅ Valid -> {args.table}")
        LOG.info(f"✅ Errors -> {args.dlq_table}")

        LOG.info("🚀 Pipeline started")
        LOG.info(f"   Trigger: {args.trigger_seconds}s")
        LOG.info(f"   Max offsets/trigger: {args.max_offsets_per_trigger}")

        # Wait for streams
        query_valid.awaitTermination()

    except KeyboardInterrupt:
        LOG.info("⏹️ Pipeline interrupted by user")
        if query_valid:
            query_valid.stop()
        if query_dlq:
            query_dlq.stop()
    except Exception as e:
        LOG.error(f"❌ Pipeline error: {e}", exc_info=True)
        if query_valid:
            query_valid.stop()
        if query_dlq:
            query_dlq.stop()
        raise
    finally:
        LOG.info("🏁 Pipeline shutdown")


if __name__ == "__main__":
    main()