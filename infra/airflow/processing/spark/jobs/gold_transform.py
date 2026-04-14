#!/usr/bin/env python3
"""Gold transform entrypoint.

This module orchestrates Gold layer (Marts) build steps.
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark-services/jobs")

from builders.gold_marts import (
    create_mart_sales_overview,
    create_mart_customer_lifetime_value,
)
from silver_transform_utils import get_spark_session


LOG = logging.getLogger(__name__)


def ensure_gold_tables(spark: SparkSession) -> None:
    """Create Gold namespace/tables if missing."""
    LOG.info("Ensuring Iceberg Gold namespace/tables exist")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.mart_sales_overview (
            order_id        STRING,
            order_date      DATE,
            customer_id     STRING,
            country         STRING,
            product_name    STRING,
            category        STRING,
            quantity        INT,
            amount_usd      DOUBLE,
            payment_status  STRING
        )
        USING iceberg
        PARTITIONED BY (order_date)
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='268435456'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.mart_customer_lifetime_value (
            customer_id      STRING,
            total_orders     BIGINT,
            total_spent_usd  DOUBLE,
            first_order      DATE,
            last_order       DATE
        )
        USING iceberg
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='268435456'
        )
        """
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Gold Layer Transform")
    parser.add_argument(
        "--partition-date",
        default=None,
        help="ISO date YYYY-MM-DD for incremental processing",
    )
    args = parser.parse_args()

    LOG.info("Starting Gold Transform Job | partition_date=%s", args.partition_date or "ALL")

    spark: SparkSession | None = None
    try:
        spark = get_spark_session()
        LOG.info("Spark session created")

        ensure_gold_tables(spark)

        create_mart_sales_overview(spark, args.partition_date)
        create_mart_customer_lifetime_value(spark, args.partition_date)

        LOG.info("Gold transform complete")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        LOG.critical("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
