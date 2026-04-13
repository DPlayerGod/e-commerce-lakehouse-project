#!/usr/bin/env python3
"""Silver transform entrypoint.

This module orchestrates Silver layer build steps and keeps business logic in
smaller, focused modules for easier debugging and maintenance.
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession

from silver_transform_builders import (
    create_dim_customers_scd2,
    create_dim_products_scd2,
    create_fact_orders,
    create_fact_payments,
    create_fact_shipments,
)
from silver_transform_utils import ensure_silver_tables, get_spark_session


LOG = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Silver transform with SCD")
    parser.add_argument(
        "--mode",
        default="incremental",
        choices=["full", "incremental"],
        help=(
            "'incremental' = process one partition date only (use --partition-date); "
            "'full' = process all Bronze data with no date filter (full rebuild)"
        ),
    )
    parser.add_argument(
        "--partition-date",
        default=None,
        help="ISO date YYYY-MM-DD; required for incremental mode",
    )
    args = parser.parse_args()

    partition_date: str | None = None if args.mode == "full" else args.partition_date

    if args.mode == "incremental" and partition_date is None:
        LOG.warning(
            "Incremental mode called without --partition-date; "
            "processing ALL partitions (same as full mode)."
        )

    LOG.info("Silver Transform - mode=%s  partition_date=%s", args.mode, partition_date or "ALL")

    spark: SparkSession | None = None
    try:
        spark = get_spark_session()
        LOG.info("Spark session created")

        ensure_silver_tables(spark)

        create_dim_customers_scd2(spark, partition_date)
        create_dim_products_scd2(spark, partition_date)
        create_fact_orders(spark, partition_date)
        create_fact_payments(spark, partition_date)
        create_fact_shipments(spark, partition_date)

        LOG.info("Silver transform complete")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        LOG.critical("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
