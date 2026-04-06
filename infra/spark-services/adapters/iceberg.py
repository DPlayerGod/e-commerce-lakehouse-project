"""Iceberg adapter for table/bootstrap and stream writes."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from schemas.iceberg_config import IcebergConfig


def ensure_table(
    spark: SparkSession,
    table: str,
    columns_sql: str,
    config: IcebergConfig,
) -> None:
    """Create Iceberg table with given config.
    
    Args:
        spark: SparkSession
        table: "catalog.schema.table" format
        columns_sql: column definition (e.g., "id INT, name STRING")
        config: IcebergConfig for this layer
    """
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError("table must be catalog.schema.table")

    catalog, schema, _ = parts
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Build TBLPROPERTIES from config
    props_dict = config.schema_properties()
    props_sql = ", ".join([f"'{k}'='{v}'" for k, v in props_dict.items()])
    
    # Build Z-order clause (optional)
    z_order_clause = config.z_order_clause()
    z_order_sql = f"\n    {z_order_clause}" if z_order_clause else ""
    
    # Build CREATE TABLE statement
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {columns_sql}
        )
        USING iceberg
        {config.partition_clause()}{z_order_sql}
        TBLPROPERTIES (
            {props_sql}
        )
    """
    
    spark.sql(create_sql)


def write_stream(
    df: DataFrame,
    *,
    table: str,
    checkpoint: str,
    trigger_seconds: int,
    config: IcebergConfig | None = None,
) -> object:
    """Write DataFrame to Iceberg table as stream.
    
    Args:
        df: DataFrame to write
        table: "catalog.schema.table" format
        checkpoint: checkpoint directory path
        trigger_seconds: trigger interval in seconds
        config: Optional IcebergConfig (for future per-layer tuning)
    """
    return (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .toTable(table)
    )
