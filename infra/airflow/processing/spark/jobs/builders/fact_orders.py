"""Fact orders builder."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import DoubleType

from builders.common import get_exchange_rates_df
from silver_transform_schemas import AVRO_ORDERS, AVRO_PAYMENTS
from silver_transform_utils import read_bronze

LOG = logging.getLogger(__name__)


def create_fact_orders(spark: SparkSession, partition_date: str | None) -> None:
    """Build fact_orders with customer and product dimensions."""
    LOG.info("Building fact_orders | partition_date=%s", partition_date or "ALL")

    orders = read_bronze(spark, "bronze_orders", AVRO_ORDERS, partition_date)
    payments = read_bronze(spark, "bronze_payments", AVRO_PAYMENTS, partition_date)

    if orders.rdd.isEmpty():
        LOG.warning("fact_orders: no order data - skipping.")
        return

    orders_with_ts = orders.withColumn("order_ts", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))

    customer_versions = (
        spark.read.table("iceberg.silver.dim_customers")
        .select(
            F.col("user_id").alias("_user_id"),
            F.col("customer_sk"),
            F.col("valid_from").alias("_c_valid_from"),
            F.col("valid_to").alias("_c_valid_to"),
        )
    )

    product_versions = (
        spark.read.table("iceberg.silver.dim_products")
        .select(
            F.col("product_id").alias("_product_id"),
            F.col("product_sk"),
            F.col("valid_from").alias("_p_valid_from"),
            F.col("valid_to").alias("_p_valid_to"),
        )
    )

    # Read exchange rates for currency conversion (broadcast array)
    exchange_rates = get_exchange_rates_df(spark)

    fact_orders_joined = (
        orders_with_ts.join(payments.select("order_id", "payment_id", "method", "status"), on="order_id", how="left")
        .join(
            customer_versions,
            on=(
                (F.col("user_id") == F.col("_user_id"))
                & (F.col("order_ts") >= F.col("_c_valid_from"))
                & (F.col("_c_valid_to").isNull() | (F.col("order_ts") < F.col("_c_valid_to")))
            ),
            how="left",
        )
        .join(
            product_versions,
            on=(
                (F.col("product_id") == F.col("_product_id"))
                & (F.col("order_ts") >= F.col("_p_valid_from"))
                & (F.col("_p_valid_to").isNull() | (F.col("order_ts") < F.col("_p_valid_to")))
            ),
            how="left",
        )
    )

    # Join with exchange rates for currency conversion (simple lookup by currency)
    order_date_col = F.from_unixtime(F.col("ts") / 1000).cast("timestamp")
    fact_orders_joined = fact_orders_joined.join(
        exchange_rates.alias("xr"),
        on=F.col("currency") == F.col("xr.source_currency"),
        how="left",
    ).withColumn(
        "_exchange_rate",
        F.coalesce(F.col("xr.exchange_rate"), F.lit(1.0))
    )

    pit_window = Window.partitionBy("order_id").orderBy(
        F.col("_c_valid_from").desc_nulls_last(),
        F.col("_p_valid_from").desc_nulls_last(),
    )

    (
        fact_orders_joined
        .withColumn("_rn", F.row_number().over(pit_window))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("order_id"),
            F.col("customer_sk"),
            F.col("user_id").alias("customer_id"),
            F.col("product_sk"),
            F.col("product_id"),
            F.col("quantity"),
            order_date_col.alias("order_date"),
            F.col("amount"),
            F.col("currency"),
            (F.col("amount") * F.col("_exchange_rate")).cast(DoubleType()).alias("amount_usd"),
            F.col("payment_id"),
            F.col("method").alias("payment_method"),
            F.col("status").alias("payment_status"),
            F.col("order_ts").alias("source_ts"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .createOrReplaceTempView("_fact_orders_delta")
    )

    spark.sql(
        """
        MERGE INTO iceberg.silver.fact_orders AS t
        USING _fact_orders_delta AS s
        ON    t.order_id = s.order_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    LOG.info("fact_orders complete.")
