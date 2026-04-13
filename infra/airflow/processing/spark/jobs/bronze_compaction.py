#!/usr/bin/env python3
"""
Bronze Compaction + Cleanup Job

Purpose: Compact + maintain Bronze layer (10-second micro-batches)

Pipeline:
1. **Compaction**: 51,840 files/day → 30 files/table (BINPACK strategy)
2. **Expire Snapshots**: Remove snapshots older than 7 days (keep last 5 for debugging)
3. **Remove Orphans**: Cleanup unreferenced data files (safe, doesn't affect queries)

Before: 6 tables × 8,640 files/day = 51,840 files/day
After:  6 tables × 30 files/day = 180 files/day (+ cleanup)

Why cleanup?
- Expire snapshots: Free metadata storage, keep query history (7 days)
- Remove orphans: Free storage from deleted/replaced files (safe operation)
- Iceberg tracks snapshots → cleanup won't break time-travel queries
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

from pyspark.sql import SparkSession

# Make shared modules (`adapters`, `schemas`) importable when running via spark-submit.
sys.path.insert(0, "/opt/spark-services")

# Import Iceberg Bronze config
from schemas.iceberg_config import BRONZE_CONFIG


LOG = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Build Spark session, resolving connection settings from environment variables."""
    from adapters.config import SPARK_CONF
    from adapters.minio import apply_minio_s3a_config

    # Read from env vars — set by Docker Compose for dev, by secrets backend for prod.
    endpoint   = os.getenv("MINIO_ENDPOINT",   "minio:9000").removeprefix("http://")
    access_key = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY",  "minioadmin")

    builder = SparkSession.builder.appName("bronze_compaction")
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)

    spark = apply_minio_s3a_config(
        builder,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
    ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def compact_table(
    spark: SparkSession,
    table_name: str,
    partition_date: str | None = None,
) -> None:
    """Compact & maintain Iceberg Bronze table per BRONZE_CONFIG.
    
    Stages:
    1. Rewrite data files (BINPACK) - uses target_file_size_bytes from config
    2. Expire snapshots - removes snapshots older than metadata_max_age_days
    3. Remove orphaned files - cleanup unreferenced data files
    
    Config values used (from BRONZE_CONFIG):
    - target_file_size_bytes: 134217728 (128 MB) - Iceberg BINPACK target
    - metadata_max_age_days: 7 - How long to keep snapshots
    - metadata_previous_versions_max: 100 - Max metadata versions to track
    
    Args:
        spark: Spark session
        table_name: Fully qualified table name (e.g., 'iceberg.bronze.bronze_orders')
        partition_date: ISO date string 'YYYY-MM-DD' to limit compaction to a single
            day partition. If None, compacts the entire table (expensive for large datasets).
    """
    
    LOG.info(f"🗜️  Compacting {table_name} per BRONZE_CONFIG")
    
    # Extract config values
    target_file_size = BRONZE_CONFIG.target_file_size_bytes
    max_age_days = BRONZE_CONFIG.metadata_max_age_days
    max_metadata_versions = BRONZE_CONFIG.metadata_previous_versions_max
    
    LOG.info(f"Config: target_file_size={target_file_size//1024//1024}MB, "
             f"max_age={max_age_days}d, max_metadata={max_metadata_versions}")
    
    try:
        # ========================================
        # STAGE 1: Rewrite Data Files (BINPACK)
        # ========================================
        
        LOG.info(f"STAGE 1/3: Rewriting data files (BINPACK) for {table_name}")
        LOG.info(f"  Target file size: {target_file_size//1024//1024} MB (from BRONZE_CONFIG)")
        if partition_date:
            LOG.info(f"  Partition filter: ingestion_date = '{partition_date}'")
        else:
            LOG.warning(f"  No partition_date → compacting ENTIRE table (slow for large datasets!)")
        
        try:
            # Build optional WHERE clause to restrict to a single day partition.
            # Use to_date(event_time) to match the ingestion_date partitioning correctly.
            where_clause = (
                f"where => \"to_date(event_time) = date('{partition_date}')\""
                if partition_date
                else ""
            )
            
            # STAGE 1: Rewrite Data Files (BINPACK)
            # Use min-input-files=2 to be more aggressive with small streaming files
            rewrite_query = f"""
                CALL system.rewrite_data_files(
                    table => '{table_name}',
                    strategy => 'binpack',
                    options => map('min-input-files', '2')
                    {(", " + where_clause) if where_clause else ""}
                )
            """
            
            rewrite_results = spark.sql(rewrite_query).collect()
            
            # Extract results
            if rewrite_results:
                res = rewrite_results[0]
                rewritten_count = res.rewritten_data_files_count
                added_count     = res.added_data_files_count
                LOG.info(f"STAGE 1 complete: Rewritten {rewritten_count} files into {added_count} files")
            else:
                LOG.info(f"STAGE 1 complete: No files required rewriting")
        except Exception as e:
            LOG.warning(f"STAGE 1 warning: {e}")
            # Continue - compaction may not always be needed
        
        # ========================================
        # STAGE 2: Expire Old Snapshots
        # ========================================
        
        expire_days = BRONZE_CONFIG.metadata_max_age_days
        retain_snapshots = max(5, min(10, max_metadata_versions // 10))  # Conservative: 5-10
        
        LOG.info(f"STAGE 2/3: Expiring old snapshots (> {expire_days} days) for {table_name}")
        LOG.info(f"  Retain at least: {retain_snapshots} recent snapshots")
        
        try:
            # Expire snapshots older than max_age_days
            spark.sql(f"""
                CALL system.expire_snapshots(
                    table => '{table_name}',
                    older_than => current_timestamp() - INTERVAL {expire_days} DAYS,
                    retain_last => {retain_snapshots}
                )
            """)
            LOG.info(f"STAGE 2 complete: Expired snapshots older than {expire_days} days")
        except Exception as e:
            LOG.warning(f"STAGE 2 warning (non-fatal): {e}")
            # Continue - expiration is optional
        
        # ========================================
        # STAGE 3: Remove Orphaned Files
        # ========================================
        
        LOG.info(f"STAGE 3/3: Removing orphaned files for {table_name}")
        
        try:
            # Remove files that are not referenced by any snapshot.
            # IMPORTANT: Use older_than >= 24h to avoid race condition with active writers
            # (e.g., Kafka ingestion writing new files that haven't been committed yet).
            spark.sql(f"""
                CALL system.remove_orphan_files(
                    table => '{table_name}',
                    older_than => current_timestamp() - INTERVAL 1 DAY
                )
            """)
            LOG.info(f"STAGE 3 complete: Removed orphaned files for {table_name}")
        except Exception as e:
            LOG.warning(f"STAGE 3 warning (non-fatal): {e}")
            # Continue - removal is optional
        
        LOG.info(f" {table_name} compaction + cleanup complete (per BRONZE_CONFIG)")
        
    except Exception as e:
        LOG.error(f" Error during compaction of {table_name}: {e}", exc_info=True)
        raise


def main() -> None:
    """Main: Compact all Bronze tables using BRONZE_CONFIG."""
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    parser = argparse.ArgumentParser(
        description="Bronze compaction + maintenance (per BRONZE_CONFIG)"
    )
    parser.add_argument(
        "--tables",
        default="bronze_orders,bronze_payments,bronze_shipments,bronze_delivery_status,bronze_users,bronze_products",
        help="Comma-separated table names (without 'iceberg.bronze.' prefix)"
    )
    parser.add_argument(
        "--partition-date",
        default=None,
        help="ISO date 'YYYY-MM-DD' of the partition to compact (default: all partitions)"
    )
    
    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",")]
    partition_date: str | None = args.partition_date
    
    LOG.info(f"Bronze Compaction + Maintenance Job (per BRONZE_CONFIG)")
    LOG.info(f"Tables ({len(tables)}): {tables}")
    LOG.info(f"Partition date: {partition_date or 'ALL (no filter)'}")
    LOG.info(f"Config from BRONZE_CONFIG:")
    LOG.info(f"  - target_file_size_bytes: {BRONZE_CONFIG.target_file_size_bytes//1024//1024} MB")
    LOG.info(f"  - metadata_max_age_days: {BRONZE_CONFIG.metadata_max_age_days} days")
    LOG.info(f"  - metadata_previous_versions_max: {BRONZE_CONFIG.metadata_previous_versions_max}")
    LOG.info(f"  - partition_spec: {BRONZE_CONFIG.partition_spec}")
    LOG.info(f"  - distribution_mode: {BRONZE_CONFIG.distribution_mode}")
    LOG.info(f"Cleanup: Expire snapshots + Remove orphans")
    
    spark = None
    try:
        # Build Spark session
        spark = get_spark_session()
        LOG.info("Spark session created")
        
        # Compact + cleanup each table
        for table in tables:
            full_table_name = f"iceberg.bronze.{table}"
            try:
                LOG.info(f"")  # Blank line for readability
                compact_table(spark, full_table_name, partition_date=partition_date)
            except Exception as e:
                LOG.error(f"Failed to compact {full_table_name}: {e}")
                # Continue with next table
        
        LOG.info(f"")
        LOG.info(f"Bronze compaction + cleanup complete for {len(tables)} tables")
        
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOG.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
