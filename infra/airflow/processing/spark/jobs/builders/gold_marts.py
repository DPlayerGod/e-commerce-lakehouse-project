"""Gold Marts builders."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, functions as F

LOG = logging.getLogger(__name__)


def create_mart_sales_overview(spark: SparkSession, partition_date: str | None = None) -> None:
    """Build mart_sales_overview - a denormalized view of orders."""
    LOG.info("Building mart_sales_overview | partition_date=%s", partition_date or "ALL")

    fact_orders = spark.read.table("iceberg.silver.fact_orders")
    if partition_date:
        fact_orders = fact_orders.filter(F.col("order_date") == partition_date)

    if fact_orders.rdd.isEmpty():
        LOG.warning("No data found in fact_orders for partition %s", partition_date or "ALL")
        return

    dim_customers = spark.read.table("iceberg.silver.dim_customers")
    dim_products = spark.read.table("iceberg.silver.dim_products")

    # Join fact with dimensions
    mart_sales = (
        fact_orders.alias("f")
        .join(
            dim_customers.alias("c"),
            on=F.col("f.customer_sk") == F.col("c.customer_sk"),
            how="left"
        )
        .join(
            dim_products.alias("p"),
            on=F.col("f.product_sk") == F.col("p.product_sk"),
            how="left"
        )
        .select(
            F.col("f.order_id"),
            F.col("f.order_date"),
            F.col("f.customer_id"),
            F.col("c.country"),
            F.col("p.title").alias("product_name"),
            F.col("p.category"),
            F.col("f.quantity"),
            F.col("f.amount_usd"),
            F.coalesce(F.col("f.payment_status"), F.lit("Unknown")).alias("payment_status")
        )
    )

    mart_sales.createOrReplaceTempView("_mart_sales_overview_delta")

    spark.sql(
        """
        MERGE INTO iceberg.gold.mart_sales_overview AS t
        USING _mart_sales_overview_delta AS s
        ON    t.order_id = s.order_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    LOG.info("mart_sales_overview complete.")


def create_mart_customer_lifetime_value(spark: SparkSession, partition_date: str | None = None) -> None:
    """Build mart_customer_lifetime_value - customer behavioral profile."""
    LOG.info("Building mart_customer_lifetime_value | partition_date=%s", partition_date or "ALL")

    # Use MART_SALES_OVERVIEW as source
    sales_overview = spark.read.table("iceberg.gold.mart_sales_overview")

    # In case of incremental, we only need to re-aggregate customers who appear in the new batch
    # But for simplicity and correctness in LTV, we can update them based on the whole history of impacted customers.
    # A more advanced logic would be to filter sales_overview for impacted customer_ids only.

    impacted_customer_ids = None
    if partition_date:
        impacted_customer_ids = (
            sales_overview.filter(F.col("order_date") == partition_date)
            .select("customer_id")
            .distinct()
        )

    source_df = sales_overview
    if impacted_customer_ids:
        source_df = sales_overview.join(impacted_customer_ids, on="customer_id", how="inner")

    clv = (
        source_df.groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount_usd").alias("total_spent_usd"),
            F.min("order_date").alias("first_order"),
            F.max("order_date").alias("last_order")
        )
    )

    clv.createOrReplaceTempView("_clv_delta")

    spark.sql(
        """
        MERGE INTO iceberg.gold.mart_customer_lifetime_value AS t
        USING _clv_delta AS s
        ON    t.customer_id = s.customer_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    LOG.info("mart_customer_lifetime_value complete.")
