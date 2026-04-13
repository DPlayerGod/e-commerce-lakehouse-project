"""Fact payments builder."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from builders.common import get_exchange_rates_df
from silver_transform_schemas import AVRO_PAYMENTS, AVRO_ORDERS
from silver_transform_utils import read_bronze

LOG = logging.getLogger(__name__)


def create_fact_payments(spark: SparkSession, partition_date: str | None) -> None:
    """Build fact_payments with order and currency conversion."""
    LOG.info("Building fact_payments | partition_date=%s", partition_date or "ALL")

    payments = read_bronze(spark, "bronze_payments", AVRO_PAYMENTS, partition_date)
    orders = read_bronze(spark, "bronze_orders", AVRO_ORDERS, partition_date)

    if payments.rdd.isEmpty():
        LOG.warning("fact_payments: no payment data - skipping.")
        return

    # Read exchange rates for currency conversion (broadcast array)
    exchange_rates = get_exchange_rates_df(spark)

    payments_with_orders = (
        payments.join(orders.select("order_id", "amount", "currency"), on="order_id", how="left")
    )

    # Join with exchange rates for currency conversion (simple lookup by currency)
    payment_date_col = F.from_unixtime(F.col("ts") / 1000).cast("timestamp")
    payments_with_orders = payments_with_orders.join(
        exchange_rates.alias("xr"),
        on=F.col("currency") == F.col("xr.source_currency"),
        how="left",
    ).withColumn(
        "_exchange_rate",
        F.coalesce(F.col("xr.exchange_rate"), F.lit(1.0))
    )

    (
        payments_with_orders
        .select(
            F.col("payment_id"),
            F.col("order_id"),
            F.col("method").alias("payment_method"),
            F.col("status").alias("payment_status"),
            payment_date_col.alias("payment_date"),
            F.coalesce(F.col("amount"), F.lit(0.0)).cast(DoubleType()).alias("amount"),
            F.col("currency"),
            (F.coalesce(F.col("amount"), F.lit(0.0)) * F.col("_exchange_rate")).cast(DoubleType()).alias("amount_usd"),
            payment_date_col.alias("source_ts"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .distinct()
        .createOrReplaceTempView("_fact_payments_delta")
    )

    spark.sql(
        """
        MERGE INTO iceberg.silver.fact_payments AS t
        USING _fact_payments_delta AS s
        ON    t.payment_id = s.payment_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    LOG.info("fact_payments complete.")
