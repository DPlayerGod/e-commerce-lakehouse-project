#!/usr/bin/env python3
"""
Silver Maintenance Job

Purpose: Maintain Silver Iceberg tables after the daily transform.

Operations per table (all non-fatal — failure of one stage does not abort others):
1. rewrite_data_files (BINPACK) — compact small files produced by daily MERGE writes
2. expire_snapshots             — remove snapshots older than 14 days, keep ≥ 3
3. remove_orphan_files          — clean unreferenced files older than 24 h
                                  (24 h window avoids race condition with active writers)
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import sys

from pyspark.sql import SparkSession


LOG = logging.getLogger(__name__)

ZORDER_MAP: dict[str, str] = {
    "dim_customers": "zorder(user_id, customer_sk, valid_from)",
    "dim_products": "zorder(product_id, product_sk, valid_from)",
    "fact_orders": "zorder(order_id, customer_sk, product_sk)",
    "fact_payments": "zorder(payment_id, order_id)",
    "fact_shipments": "zorder(shipment_id, order_id)",
}


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark_session() -> SparkSession:
    """Build Spark session, resolving connection settings from environment variables."""
    sys.path.insert(0, "/opt/spark-services")

    from adapters.config import SPARK_CONF
    from adapters.minio import apply_minio_s3a_config

    endpoint   = os.getenv("MINIO_ENDPOINT",  "minio:9000").removeprefix("http://")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    builder = SparkSession.builder.appName("silver_maintenance")
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


# ---------------------------------------------------------------------------
# Maintenance logic
# ---------------------------------------------------------------------------

def maintain_table(
    spark: SparkSession,
    table_name: str,
    expire_days: int = 14,
    retain_snapshots: int = 3,
) -> None:
    """Run full Iceberg maintenance on one Silver table.

    Args:
        spark:             active SparkSession
        table_name:        bare table name, e.g. 'dim_customers'
        expire_days:       drop snapshots older than this many days
        retain_snapshots:  minimum number of snapshots to keep regardless of age
    """
    full_name = f"iceberg.silver.{table_name}"
    snapshot_cutoff = (datetime.now(timezone.utc) - timedelta(days=expire_days)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_cutoff = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    LOG.info("━━━ Maintaining %s", full_name)

    # ── Stage 1: Z-order optimize (fallback to BINPACK if unsupported) ───────
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

        file_count = (
            spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}.files")
            .collect()[0]
            .n
        )
        LOG.info("  Stage 1 compact — %d files remaining", file_count)
    except Exception as exc:
        LOG.warning("  Stage 1 sort optimize skipped (%s), fallback binpack", exc)
        try:
            spark.sql(f"""
                CALL iceberg.system.rewrite_data_files(
                    table    => '{full_name}',
                    strategy => 'binpack'
                )
            """)
            LOG.info("  Stage 1 fallback compact (binpack) complete")
        except Exception as fallback_exc:
            LOG.warning("  Stage 1 fallback binpack skipped: %s", fallback_exc)

    # ── Stage 2: Expire old snapshots ───────────────────────────────────────
    try:
        spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table       => '{full_name}',
                older_than  => TIMESTAMP '{snapshot_cutoff}',
                retain_last => {retain_snapshots}
            )
        """)
        LOG.info(
            "  Stage 2 expire_snapshots — older than %d days, kept >= %d",
            expire_days,
            retain_snapshots,
        )
    except Exception as exc:
        LOG.warning("  Stage 2 expire_snapshots skipped: %s", exc)

    # ── Stage 3: Remove orphaned files (>= 24 h old) ────────────────────────
    # The 24-hour window is intentional: Iceberg recommends >= 24 h to avoid
    # deleting files that concurrent writers have just created but not yet committed.
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

    LOG.info("  %s maintenance done.", full_name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Silver table maintenance (Iceberg)")
    parser.add_argument(
        "--tables",
        default="dim_customers,dim_products,fact_orders,fact_payments,fact_shipments",
        help="Comma-separated Silver table names to maintain",
    )
    parser.add_argument(
        "--expire-days",
        type=int,
        default=14,
        help="Drop snapshots older than this many days (default: 14)",
    )
    parser.add_argument(
        "--retain-snapshots",
        type=int,
        default=3,
        help="Minimum number of snapshots to keep regardless of age (default: 3)",
    )
    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",")]

    LOG.info(
        "Silver Maintenance — tables=%s  expire_days=%d  retain_snapshots=%d",
        tables,
        args.expire_days,
        args.retain_snapshots,
    )

    spark: SparkSession | None = None
    try:
        spark = get_spark_session()
        LOG.info("Spark session created")

        for table in tables:
            try:
                maintain_table(
                    spark,
                    table,
                    expire_days=args.expire_days,
                    retain_snapshots=args.retain_snapshots,
                )
            except Exception as exc:
                # Log but continue — one table failing shouldn't abort the rest
                LOG.error("Failed to maintain %s: %s", table, exc, exc_info=True)

        LOG.info("Silver maintenance complete")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOG.critical("Fatal error: %s", e, exc_info=True)
        sys.exit(1)
