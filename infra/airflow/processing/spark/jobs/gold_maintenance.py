#!/usr/bin/env python3
"""
Gold Maintenance Job

Purpose: Maintain Gold Iceberg tables (Marts) after daily updates.
Operations:
1. rewrite_data_files (Z-Order) — optimize query performance for common filters.
2. expire_snapshots — cleanup old history.
3. remove_orphan_files — cleanup storage.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import sys

from pyspark.sql import SparkSession

# Ensure we can import shared modules
sys.path.insert(0, "/opt/spark-services/jobs")
from silver_transform_utils import get_spark_session


LOG = logging.getLogger(__name__)

# Optimization mapping for Gold tables
ZORDER_MAP: dict[str, str] = {
    "mart_sales_overview": "zorder(customer_id, country, category, product_name)",
    "mart_customer_lifetime_value": "zorder(customer_id, first_order, last_order)",
}


def maintain_table(
    spark: SparkSession,
    table_name: str,
    expire_days: int = 14,
    retain_snapshots: int = 3,
) -> None:
    """Run full Iceberg maintenance on one Gold table."""
    full_name = f"iceberg.gold.{table_name}"
    snapshot_cutoff = (datetime.now(timezone.utc) - timedelta(days=expire_days)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_cutoff = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    
    LOG.info("━━━ Maintaining %s", full_name)

    # Stage 1: Z-order optimize (fallback to BINPACK)
    try:
        zorder = ZORDER_MAP.get(table_name)
        if zorder:
            spark.sql(f"""
                CALL iceberg.system.rewrite_data_files(
                    table      => '{full_name}',
                    strategy   => 'sort',
                    sort_order => '{zorder}'
                )
            """)
            LOG.info("  Stage 1 optimize — applied %s", zorder)
        else:
            spark.sql(f"""
                CALL iceberg.system.rewrite_data_files(
                    table    => '{full_name}',
                    strategy => 'binpack'
                )
            """)
            LOG.info("  Stage 1 compact (binpack) complete")
    except Exception as exc:
        LOG.warning("  Stage 1 optimize failed/skipped: %s", exc)

    # Stage 2: Expire old snapshots
    try:
        spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table       => '{full_name}',
                older_than  => TIMESTAMP '{snapshot_cutoff}',
                retain_last => {retain_snapshots}
            )
        """)
        LOG.info("  Stage 2 expire_snapshots complete")
    except Exception as exc:
        LOG.warning("  Stage 2 expire_snapshots skipped: %s", exc)

    # Stage 3: Remove orphaned files
    try:
        spark.sql(f"""
            CALL iceberg.system.remove_orphan_files(
                table      => '{full_name}',
                older_than => TIMESTAMP '{orphan_cutoff}'
            )
        """)
        LOG.info("  Stage 3 remove_orphan_files complete")
    except Exception as exc:
        LOG.warning("  Stage 3 remove_orphan_files skipped: %s", exc)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Gold table maintenance")
    parser.add_argument(
        "--tables",
        default="mart_sales_overview,mart_customer_lifetime_value",
        help="Comma-separated Gold table names",
    )
    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",")]

    spark: SparkSession | None = None
    try:
        spark = get_spark_session()
        LOG.info("Spark session created")

        for table in tables:
            maintain_table(spark, table)

        LOG.info("Gold maintenance complete")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
