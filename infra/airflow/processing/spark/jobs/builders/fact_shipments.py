"""Fact shipments builder."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, functions as F

from silver_transform_schemas import AVRO_SHIPMENTS, AVRO_DELIVERIES
from silver_transform_utils import read_bronze

LOG = logging.getLogger(__name__)


def create_fact_shipments(spark: SparkSession, partition_date: str | None) -> None:
    """Build fact_shipments with delivery status and reason."""
    LOG.info("Building fact_shipments | partition_date=%s", partition_date or "ALL")

    shipments = read_bronze(spark, "bronze_shipments", AVRO_SHIPMENTS, partition_date)
    deliveries = read_bronze(spark, "bronze_delivery_status", AVRO_DELIVERIES, partition_date)

    if shipments.rdd.isEmpty():
        LOG.warning("fact_shipments: no shipment data - skipping.")
        return

    (
        shipments.join(
            deliveries.select(
                "shipment_id",
                "status",
                "reason",
                F.col("ts").alias("delivery_ts"),
            ),
            on="shipment_id",
            how="left",
        )
        .select(
            F.col("shipment_id"),
            F.col("order_id"),
            F.from_unixtime(F.col("ts") / 1000).cast("timestamp").alias("shipment_date"),
            F.date_add(F.from_unixtime(F.col("ts") / 1000).cast("timestamp"), F.col("eta_days")).alias("estimated_delivery"),
            F.col("eta_days"),
            F.from_unixtime(F.col("delivery_ts") / 1000).cast("timestamp").alias("actual_delivery"),
            F.coalesce(F.col("status"), F.lit("PENDING")).alias("delivery_status"),
            F.col("reason").alias("delivery_reason"),
            F.from_unixtime(F.col("ts") / 1000).cast("timestamp").alias("source_ts"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .distinct()
        .createOrReplaceTempView("_fact_shipments_delta")
    )

    spark.sql(
        """
        MERGE INTO iceberg.silver.fact_shipments AS t
        USING _fact_shipments_delta AS s
        ON    t.shipment_id = s.shipment_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    LOG.info("fact_shipments complete.")
