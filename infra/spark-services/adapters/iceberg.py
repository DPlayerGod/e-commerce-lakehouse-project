"""Iceberg adapter for table/bootstrap and stream writes."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def ensure_table(spark: SparkSession, table: str, columns_sql: str) -> None:
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError("table must be catalog.schema.table")

    catalog, schema, _ = parts
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {columns_sql}
        )
        USING iceberg
        PARTITIONED BY (days(event_time), event_source)
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet'
        )
        """
    )


def write_stream(df: DataFrame, *, table: str, checkpoint: str, trigger_seconds: int):
    return (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .toTable(table)
    )
