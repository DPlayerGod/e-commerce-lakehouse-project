"""Kafka adapter for Spark Structured Streaming."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_stream(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topics_csv: str,
    starting_offsets: str,
    max_offsets_per_trigger: int,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topics_csv)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        .option("failOnDataLoss", "false")
        .load()
    )
