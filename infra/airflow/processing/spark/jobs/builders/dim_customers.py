"""Dimension customers builder (SCD2)."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, Window, functions as F

from builders.common import micros_to_timestamp, surrogate_key
from silver_transform_schemas import AVRO_USERS_ENV
from silver_transform_utils import read_bronze

LOG = logging.getLogger(__name__)


def create_dim_customers_scd2(spark: SparkSession, partition_date: str | None) -> None:
    """Build dim_customers with SCD2 logic (track history)."""
    LOG.info("Building dim_customers (SCD2) | partition_date=%s", partition_date or "ALL")
    incoming = read_bronze(spark, "bronze_users", AVRO_USERS_ENV, partition_date, is_cdc=True)
    if incoming.rdd.isEmpty():
        LOG.warning("dim_customers: no incoming data - skipping.")
        return

    incoming_prepared = (
        incoming.select(
            "user_id",
            "email",
            "country",
            F.coalesce(
                micros_to_timestamp("updated_at"),
                micros_to_timestamp("created_at"),
                F.current_timestamp(),
            ).alias("source_ts"),
        )
        .filter(F.col("user_id").isNotNull())
        .dropDuplicates(["user_id", "email", "country", "source_ts"])
    )

    current = (
        spark.read.table("iceberg.silver.dim_customers")
        .filter(F.col("is_current") == True)
        .select("user_id", "email", "country", "valid_from")
    )

    incoming_prepared = (
        incoming_prepared.alias("i")
        .join(
            current.select("user_id", F.col("valid_from").alias("_current_valid_from")).alias("c"),
            on="user_id",
            how="left",
        )
        .filter(F.col("_current_valid_from").isNull() | (F.col("source_ts") >= F.col("_current_valid_from")))
        .drop("_current_valid_from")
    )

    timeline = current.select(
        "user_id",
        "email",
        "country",
        F.col("valid_from").alias("source_ts"),
    ).withColumn("_from_current", F.lit(1)).unionByName(
        incoming_prepared.withColumn("_from_current", F.lit(0))
    )

    win_timeline = Window.partitionBy("user_id").orderBy(F.col("source_ts").asc(), F.col("_from_current").desc())
    customer_changes = (
        timeline.withColumn("_prev_user_id", F.lag("user_id").over(win_timeline))
        .withColumn("_prev_email", F.lag("email").over(win_timeline))
        .withColumn("_prev_country", F.lag("country").over(win_timeline))
        .filter(F.col("_from_current") == 0)
        .filter(
            F.col("_prev_user_id").isNull()
            | (~F.col("email").eqNullSafe(F.col("_prev_email")))
            | (~F.col("country").eqNullSafe(F.col("_prev_country")))
        )
        .select("user_id", "email", "country", "source_ts")
    )

    if customer_changes.rdd.isEmpty():
        LOG.info("dim_customers: no effective SCD2 changes.")
        return

    customer_changes.createOrReplaceTempView("_scd2_customers_changes")

    spark.sql(
        """
        MERGE INTO iceberg.silver.dim_customers AS t
        USING (
            SELECT c.user_id, MIN(c.source_ts) AS first_change_ts
            FROM _scd2_customers_changes c
            JOIN iceberg.silver.dim_customers d
              ON c.user_id = d.user_id
             AND d.is_current = true
            GROUP BY c.user_id
        ) AS changed
        ON t.user_id = changed.user_id AND t.is_current = true
        WHEN MATCHED THEN
          UPDATE SET
            t.valid_to = changed.first_change_ts,
            t.is_current = false,
            t.updated_at = current_timestamp()
        """
    )

    # Identify which user_ids are completely new to the dimension
    existing_user_ids = spark.read.table("iceberg.silver.dim_customers").select("user_id").distinct().alias("e")
    
    win_insert = Window.partitionBy("user_id").orderBy(F.col("source_ts").asc())
    to_insert = (
        customer_changes
        .alias("new")
        .join(existing_user_ids, on="user_id", how="left")
        .withColumn("_rn", F.row_number().over(win_insert))
        .withColumn(
            "valid_from",
            F.when(
                (F.col("e.user_id").isNull()) & (F.col("_rn") == 1),
                F.to_timestamp(F.lit("1970-01-01 00:00:00"))
            ).otherwise(F.col("source_ts"))
        )
        .withColumn("valid_to", F.lead("source_ts").over(win_insert))
        .withColumn("is_current", F.lead("source_ts").over(win_insert).isNull())
        .withColumn(
            "customer_sk",
            surrogate_key(F.col("user_id"), F.col("source_ts"), F.col("email"), F.col("country")),
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "customer_sk",
            "user_id",
            "email",
            "country",
            "valid_from",
            "valid_to",
            "is_current",
            "source_ts",
            "updated_at",
        )
    )
    to_insert.createOrReplaceTempView("_scd2_customers_to_insert")

    spark.sql(
        """
        INSERT INTO iceberg.silver.dim_customers
            (customer_sk, user_id, email, country, valid_from, valid_to, is_current, source_ts, updated_at)
        SELECT
            n.customer_sk,
            n.user_id,
            n.email,
            n.country,
            n.valid_from,
            n.valid_to,
            n.is_current,
            n.source_ts,
            n.updated_at
        FROM _scd2_customers_to_insert n
        LEFT JOIN iceberg.silver.dim_customers t
          ON n.customer_sk = t.customer_sk
        WHERE t.customer_sk IS NULL
        """
    )

    LOG.info("dim_customers SCD2 complete.")
