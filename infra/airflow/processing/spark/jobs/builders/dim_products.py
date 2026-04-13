"""Dimension products builder (SCD2)."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, Window, functions as F

from builders.common import micros_to_timestamp, surrogate_key
from silver_transform_schemas import AVRO_PRODUCTS_ENV
from silver_transform_utils import read_bronze

LOG = logging.getLogger(__name__)


def create_dim_products_scd2(spark: SparkSession, partition_date: str | None) -> None:
    """Build dim_products with SCD2 logic (track history)."""
    LOG.info("Building dim_products (SCD2) | partition_date=%s", partition_date or "ALL")

    incoming = read_bronze(spark, "bronze_products", AVRO_PRODUCTS_ENV, partition_date, is_cdc=True)
    if incoming.rdd.isEmpty():
        LOG.warning("dim_products: no incoming data - skipping.")
        return

    incoming_prepared = (
        incoming.select(
            "product_id",
            "title",
            "category",
            "price",
            F.coalesce(
                micros_to_timestamp("updated_at"),
                micros_to_timestamp("created_at"),
                F.current_timestamp(),
            ).alias("source_ts"),
        )
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id", "title", "category", "price", "source_ts"])
    )

    current = (
        spark.read.table("iceberg.silver.dim_products")
        .filter(F.col("is_current") == True)
        .select("product_id", "title", "category", "price", "valid_from")
    )

    incoming_prepared = (
        incoming_prepared.alias("i")
        .join(
            current.select("product_id", F.col("valid_from").alias("_current_valid_from")).alias("c"),
            on="product_id",
            how="left",
        )
        .filter(F.col("_current_valid_from").isNull() | (F.col("source_ts") >= F.col("_current_valid_from")))
        .drop("_current_valid_from")
    )

    timeline = current.select(
        "product_id",
        "title",
        "category",
        "price",
        F.col("valid_from").alias("source_ts"),
    ).withColumn("_from_current", F.lit(1)).unionByName(
        incoming_prepared.withColumn("_from_current", F.lit(0))
    )

    win_timeline = Window.partitionBy("product_id").orderBy(F.col("source_ts").asc(), F.col("_from_current").desc())
    product_changes = (
        timeline.withColumn("_prev_product_id", F.lag("product_id").over(win_timeline))
        .withColumn("_prev_title", F.lag("title").over(win_timeline))
        .withColumn("_prev_category", F.lag("category").over(win_timeline))
        .withColumn("_prev_price", F.lag("price").over(win_timeline))
        .filter(F.col("_from_current") == 0)
        .filter(
            F.col("_prev_product_id").isNull()
            | (~F.col("title").eqNullSafe(F.col("_prev_title")))
            | (~F.col("category").eqNullSafe(F.col("_prev_category")))
            | (~F.col("price").eqNullSafe(F.col("_prev_price")))
        )
        .select("product_id", "title", "category", "price", "source_ts")
    )

    if product_changes.rdd.isEmpty():
        LOG.info("dim_products: no effective SCD2 changes.")
        return

    product_changes.createOrReplaceTempView("_scd2_products_changes")

    spark.sql(
        """
        MERGE INTO iceberg.silver.dim_products AS t
        USING (
            SELECT c.product_id, MIN(c.source_ts) AS first_change_ts
            FROM _scd2_products_changes c
            JOIN iceberg.silver.dim_products d
              ON c.product_id = d.product_id
             AND d.is_current = true
            GROUP BY c.product_id
        ) AS changed
        ON t.product_id = changed.product_id AND t.is_current = true
        WHEN MATCHED THEN
          UPDATE SET
            t.valid_to = changed.first_change_ts,
            t.is_current = false,
            t.updated_at = current_timestamp()
        """
    )

    # Identify which product_ids are completely new to the dimension
    existing_product_ids = spark.read.table("iceberg.silver.dim_products").select("product_id").distinct().alias("e")

    win_insert = Window.partitionBy("product_id").orderBy(F.col("source_ts").asc())
    to_insert = (
        product_changes
        .alias("new")
        .join(existing_product_ids, on="product_id", how="left")
        .withColumn("_rn", F.row_number().over(win_insert))
        .withColumn(
            "valid_from",
            F.when(
                (F.col("e.product_id").isNull()) & (F.col("_rn") == 1),
                F.to_timestamp(F.lit("1970-01-01 00:00:00"))
            ).otherwise(F.col("source_ts"))
        )
        .withColumn("valid_to", F.lead("source_ts").over(win_insert))
        .withColumn("is_current", F.lead("source_ts").over(win_insert).isNull())
        .withColumn(
            "product_sk",
            surrogate_key(
                F.col("product_id"),
                F.col("source_ts"),
                F.col("title"),
                F.col("category"),
                F.col("price"),
            ),
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "product_sk",
            "product_id",
            "title",
            "category",
            "price",
            "valid_from",
            "valid_to",
            "is_current",
            "source_ts",
            "updated_at",
        )
    )
    to_insert.createOrReplaceTempView("_scd2_products_to_insert")

    spark.sql(
        """
        INSERT INTO iceberg.silver.dim_products
            (product_sk, product_id, title, category, price, valid_from, valid_to, is_current, source_ts, updated_at)
        SELECT
            n.product_sk,
            n.product_id,
            n.title,
            n.category,
            n.price,
            n.valid_from,
            n.valid_to,
            n.is_current,
            n.source_ts,
            n.updated_at
        FROM _scd2_products_to_insert n
        LEFT JOIN iceberg.silver.dim_products t
          ON n.product_sk = t.product_sk
        WHERE t.product_sk IS NULL
        """
    )

    LOG.info("dim_products SCD2 complete.")
