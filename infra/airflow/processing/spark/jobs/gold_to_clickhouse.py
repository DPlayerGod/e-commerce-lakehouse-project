#!/usr/bin/env python3
"""Push Gold Marts to ClickHouse.

Simplified version for entry-level DE.
Reads data from Iceberg Gold layer and pushes it to ClickHouse via JDBC.
"""
import os
import sys
import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

sys.path.insert(0, "/opt/spark-services/jobs")
from silver_transform_utils import get_spark_session

LOG = logging.getLogger(__name__)

CH_HOST = os.getenv("CLICKHOUSE_HOST", "ecommerce-clickhouse")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")  
CH_DB = os.getenv("CLICKHOUSE_DB", "analytics")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

def push_table_to_clickhouse(spark: SparkSession, table_name: str, partition_date: str = None):
    """
    Reads a gold table from Iceberg and writes it to ClickHouse.
    
    Strategy:
    - Partitioned tables (mart_sales_overview): Append mode (incremental by date)
    - Non-partitioned tables (mart_customer_lifetime_value): Truncate + Insert (full refresh)
    """
    iceberg_table = f"iceberg.gold.{table_name}"
    ch_table = f"{CH_DB}.{table_name}"
    jdbc_url = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"
    
    LOG.info(f"Processing table: {table_name}")
    
    # 1. Read from Iceberg Gold layer
    df = spark.table(iceberg_table)
    
    # 2. Filter by partition date if provided (for partitioned tables only)
    write_mode = "append"  # Default is append for daily partitions
    if partition_date:
        if "order_date" in df.columns:
            LOG.info(f"Filtering by order_date = {partition_date}")
            df = df.filter(F.col("order_date") == partition_date)
        elif "shipment_date" in df.columns:
            LOG.info(f"Filtering by shipment_date = {partition_date}")
            df = df.filter(F.col("shipment_date") == partition_date)
        else:
            # Table like mart_customer_lifetime_value should be full refresh
            LOG.info(f"Table {table_name} - Full refresh needed (no partition key found)")
            write_mode = "overwrite"
    else:
        # If no partition date, non-partitioned tables are full refresh
        if not any(col in df.columns for col in ["order_date", "shipment_date"]):
            write_mode = "overwrite"
            LOG.info(f"Non-partitioned table {table_name} - Full refresh mode (overwrite)")
            
    # 3. Add a timestamp for when the data was synced
    df = df.withColumn("_synced_at", F.current_timestamp())
    
    # 4. Count rows for logging
    row_count = df.count()
    LOG.info(f"Writing {row_count} rows to ClickHouse table: {ch_table} (mode={write_mode})")
    
    # 5. Write mode logic: For full refresh, we truncate manually via JDBC first
    # then use append mode in Spark to preserve the ClickHouse schema (ORDER BY clause)
    if write_mode == "overwrite":
        LOG.info(f"Manually truncating ClickHouse table: {ch_table}")
        try:
            # Use PySpark's JVM gateway to run a direct TRUNCATE command via JDBC
            from py4j.java_gateway import java_import
            java_import(spark._jvm, 'java.util.Properties')
            java_import(spark._jvm, 'java.sql.DriverManager')
            
            props = spark._jvm.Properties()
            props.setProperty("user", CH_USER)
            props.setProperty("password", CH_PASSWORD)
            
            conn = spark._jvm.DriverManager.getConnection(jdbc_url, props)
            stmt = conn.createStatement()
            stmt.execute(f"TRUNCATE TABLE {ch_table}")
            stmt.close()
            conn.close()
            LOG.info(f"Truncate Successful: {ch_table}")
        except Exception as te:
            LOG.warning(f"Manual truncate failed for {ch_table}: {te}. Proceeding with potential duplicates (ReplacingMergeTree will handle them).")

    # 6. Write to ClickHouse via JDBC (Always use append to avoid Spark's DROP/CREATE)
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", ch_table) \
        .option("user", CH_USER) \
        .option("password", CH_PASSWORD) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("batchsize", "50000") \
        .mode("append") \
        .save()
        
    LOG.info(f"Successfully pushed {row_count} rows for {table_name}")

def main():
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Parse arguments
    parser = argparse.ArgumentParser(description="Gold to Clickhouse Push (Simplified)")
    parser.add_argument("--tables", required=True, help="Comma-separated list of gold tables")
    parser.add_argument("--partition-date", default=None, help="Process specific date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = None
    try:
        # Get Spark session using shared utility
        spark = get_spark_session()
        spark.conf.set("spark.app.name", "GoldToClickhouseSimplified")
        
        # Process each table provided in the arguments
        tables_to_process = [t.strip() for t in args.tables.split(",")]
        failed_tables = []
        for table in tables_to_process:
            try:
                push_table_to_clickhouse(spark, table, args.partition_date)
            except Exception as e:
                LOG.error(f"Error processing table {table}: {e}")
                failed_tables.append(table)
        
        if failed_tables:
            LOG.error(f"The following tables failed to sink to ClickHouse: {', '.join(failed_tables)}")
            sys.exit(1)

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
