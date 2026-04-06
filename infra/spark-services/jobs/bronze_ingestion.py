#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession, functions as F

from adapters.iceberg import ensure_table, write_stream
from adapters.kafka import read_stream
from adapters.minio import apply_minio_s3a_config, ensure_bucket_exists
from config import (
    BRONZE_TOPICS,
    BRONZE_TOPIC_TO_TABLE,
    BRONZE_TOPIC_TO_CHECKPOINT,
    KAFKA_BOOTSTRAP,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SPARK_CONF,
)
from schemas.bronze_schemas import BRONZE_RAW_EVENTS_COLUMNS_SQL
from schemas.iceberg_config import BRONZE_CONFIG

APP_NAME = "bronze_ingestion"
LOG = logging.getLogger(__name__)


def process_stream(df: object) -> object:
    """Transform Kafka stream to Bronze schema (Pure Raw Avro bytes)."""
    LOG.debug("🔄 Processing stream (Pure Raw - no deserialization)")
    
    df_processed = df.select(
        F.col("topic").alias("event_source"),
        F.col("timestamp").alias("event_time"),
        "partition",
        "offset",
        F.col("value").alias("raw_value"),
        F.current_timestamp().alias("processed_at"),
    )
    
    return df_processed


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Bronze ingestion (Pure Raw - Multi-topic)")
    parser.add_argument("--topics", default=",".join(BRONZE_TOPICS), help="Comma-separated Kafka topics")
    parser.add_argument("--starting-offsets", default="earliest", help="Kafka offset: earliest/latest")
    parser.add_argument("--trigger-seconds", type=int, default=10, help="Micro-batch trigger interval")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000, help="Max offsets per trigger")
    args = parser.parse_args()

    topics = args.topics.split(",")
    LOG.info(f"🚀 Bronze Ingestion - Multi-Topic Mode")
    LOG.info(f"📌 Topics ({len(topics)}): {topics}")
    LOG.info(f"📌 Starting offsets: {args.starting_offsets}")
    LOG.info(f"📌 Trigger interval: {args.trigger_seconds}s")
    LOG.info(f"📌 Max offsets/trigger: {args.max_offsets_per_trigger}")

    query = None
    
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

        # Ensure all Bronze tables exist (one per topic)
        LOG.info(f"📊 Ensuring {len(topics)} Bronze tables exist...")
        for topic in topics:
            if topic in BRONZE_TOPIC_TO_TABLE:
                table_name = BRONZE_TOPIC_TO_TABLE[topic]
                ensure_table(spark, table_name, BRONZE_RAW_EVENTS_COLUMNS_SQL, BRONZE_CONFIG)
                LOG.info(f"  ✅ {topic:30} → {table_name}")
            else:
                LOG.warning(f"  ⚠️  Topic not in mapping: {topic}")

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

        # Process stream (Pure Raw)
        df_processed = process_stream(source)

        # Register temp view for splitting by topic
        df_processed.createOrReplaceTempView("raw_events")

        # Write each topic to its own table
        LOG.info("📝 Starting multi-topic stream writes...")
        queries = []
        for topic in topics:
            if topic not in BRONZE_TOPIC_TO_TABLE:
                LOG.warning(f"⚠️  Skipping topic (not in mapping): {topic}")
                continue
            
            table_name = BRONZE_TOPIC_TO_TABLE[topic]
            checkpoint_path = BRONZE_TOPIC_TO_CHECKPOINT[topic]
            
            # Filter data for this topic
            df_topic = df_processed.filter(F.col("event_source") == topic)
            
            # Write to topic-specific table
            query = write_stream(
                df_topic,
                table=table_name,
                checkpoint=checkpoint_path,
                trigger_seconds=args.trigger_seconds,
                config=BRONZE_CONFIG,
            )
            queries.append(query)
            LOG.info(f"  ✅ {topic:30} → {table_name} (checkpoint: {checkpoint_path})")

        LOG.info(f"🚀 Pipeline started with {len(queries)} streams")
        LOG.info(f"   Trigger: {args.trigger_seconds}s")
        LOG.info(f"   Max offsets/trigger: {args.max_offsets_per_trigger}")

        # Wait for any stream to terminate
        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        LOG.info("⏹️ Pipeline interrupted by user")
    except Exception as e:
        LOG.error(f"❌ Pipeline error: {e}", exc_info=True)
        raise
    finally:
        LOG.info("🏁 Pipeline shutdown")


if __name__ == "__main__":
    main()