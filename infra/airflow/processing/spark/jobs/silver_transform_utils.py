"""Utility helpers for silver transform job."""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

# Make shared modules importable when running via spark-submit.
sys.path.insert(0, "/opt/spark-services")

LOG = logging.getLogger(__name__)


# Exchange rates reference data (hardcoded - simple lookup by currency)
EXCHANGE_RATES = [
    ("USD", "USD", 1.0),
    ("GBP", "USD", 1.28),
    ("EUR", "USD", 1.11),
]


def _safe_sql(spark: SparkSession, statement: str, context: str) -> None:
    """Execute SQL statement and log warning instead of hard-failing."""
    try:
        spark.sql(statement)
    except Exception as exc:
        LOG.warning("%s skipped: %s", context, exc)


def get_spark_session() -> SparkSession:
    """Build Spark session and wire S3/MinIO settings from environment."""
    from adapters.config import SPARK_CONF
    from adapters.minio import apply_minio_s3a_config

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").removeprefix("http://")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    builder = SparkSession.builder.appName("silver_transform_scd")
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


def read_bronze(
    spark: SparkSession,
    table: str,
    avro_schema: str,
    partition_date: str | None,
    is_cdc: bool = False,
) -> DataFrame:
    """Read Bronze Iceberg table and decode Avro payload from raw_value bytes."""
    from pyspark.sql.avro.functions import from_avro

    df = spark.read.table(f"iceberg.bronze.{table}")
    if partition_date:
        df = df.filter(F.to_date(F.col("event_time")) == partition_date)

    # Strip Confluent wire-format header (magic byte + schema id)
    df = df.withColumn("avro_bytes", F.expr("substring(raw_value, 6, length(raw_value))"))

    if is_cdc:
        parsed = df.select(from_avro(F.col("avro_bytes"), avro_schema).alias("env")).select("env.after.*")
    else:
        parsed = df.select(from_avro(F.col("avro_bytes"), avro_schema).alias("d")).select("d.*")

    first_key = parsed.columns[0]
    parsed = parsed.filter(F.col(first_key).isNotNull())

    row_count = parsed.count()
    LOG.info(
        "  %-35s %6d rows  (partition_date=%s, is_cdc=%s)",
        table,
        row_count,
        partition_date or "ALL",
        is_cdc,
    )
    return parsed


def dedup_latest(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Keep latest row per key by ordering descending on order_col."""
    window = Window.partitionBy(key).orderBy(F.desc(order_col))
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def ensure_silver_tables(spark: SparkSession) -> None:
    """Create Silver namespace/tables if missing so first run can MERGE safely."""
    LOG.info("Ensuring Iceberg Silver namespace/tables exist")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.dim_customers (
            customer_sk STRING,
            user_id     STRING,
            email       STRING,
            country     STRING,
            valid_from  TIMESTAMP,
            valid_to    TIMESTAMP,
            is_current  BOOLEAN,
            source_ts   TIMESTAMP,
            updated_at  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(valid_from))
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='268435456'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.dim_products (
            product_sk STRING,
            product_id STRING,
            title      STRING,
            category   STRING,
            price      DOUBLE,
            valid_from TIMESTAMP,
            valid_to   TIMESTAMP,
            is_current BOOLEAN,
            source_ts  TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(valid_from))
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='268435456'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.fact_orders (
            order_id        STRING,
            customer_sk     STRING,
            customer_id     STRING,
            product_sk      STRING,
            product_id      STRING,
            quantity        INT,
            order_date      DATE,
            amount          DOUBLE,
            currency        STRING,
            amount_usd      DOUBLE,
            payment_id      STRING,
            payment_method  STRING,
            payment_status  STRING,
            source_ts       TIMESTAMP,
            created_at      TIMESTAMP,
            updated_at      TIMESTAMP
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
        CREATE TABLE IF NOT EXISTS iceberg.silver.fact_payments (
            payment_id      STRING,
            order_id        STRING,
            payment_method  STRING,
            payment_status  STRING,
            payment_date    TIMESTAMP,
            amount          DOUBLE,
            currency        STRING,
            amount_usd      DOUBLE,
            source_ts       TIMESTAMP,
            created_at      TIMESTAMP,
            updated_at      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(payment_date))
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='268435456'
        )
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.fact_shipments (
            shipment_id         STRING,
            order_id            STRING,
            shipment_date       DATE,
            estimated_delivery  DATE,
            eta_days            BIGINT,
            actual_delivery     DATE,
            delivery_status     STRING,
            delivery_reason     STRING,
            source_ts           TIMESTAMP,
            created_at          TIMESTAMP,
            updated_at          TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (shipment_date)
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='268435456'
        )
        """
    )

    # Schema evolution for previously created Silver tables.
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_customers ADD COLUMNS (customer_sk STRING)",
        "dim_customers add customer_sk",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_customers ADD COLUMNS (source_ts TIMESTAMP)",
        "dim_customers add source_ts",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_customers ADD COLUMNS (updated_at TIMESTAMP)",
        "dim_customers add updated_at",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_customers ALTER COLUMN valid_from TYPE TIMESTAMP",
        "dim_customers alter valid_from to TIMESTAMP",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_customers ALTER COLUMN valid_to TYPE TIMESTAMP",
        "dim_customers alter valid_to to TIMESTAMP",
    )

    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_products ADD COLUMNS (product_sk STRING)",
        "dim_products add product_sk",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_products ADD COLUMNS (valid_from TIMESTAMP)",
        "dim_products add valid_from",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_products ADD COLUMNS (valid_to TIMESTAMP)",
        "dim_products add valid_to",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_products ADD COLUMNS (is_current BOOLEAN)",
        "dim_products add is_current",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.dim_products ADD COLUMNS (source_ts TIMESTAMP)",
        "dim_products add source_ts",
    )

    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_orders ADD COLUMNS (customer_sk STRING)",
        "fact_orders add customer_sk",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_orders ADD COLUMNS (product_sk STRING)",
        "fact_orders add product_sk",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_orders ADD COLUMNS (quantity INT)",
        "fact_orders add quantity",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_orders ADD COLUMNS (source_ts TIMESTAMP)",
        "fact_orders add source_ts",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_orders ADD COLUMNS (updated_at TIMESTAMP)",
        "fact_orders add updated_at",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_orders ADD COLUMNS (amount_usd DOUBLE)",
        "fact_orders add amount_usd",
    )

    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_payments ADD COLUMNS (source_ts TIMESTAMP)",
        "fact_payments add source_ts",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_payments ADD COLUMNS (updated_at TIMESTAMP)",
        "fact_payments add updated_at",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_payments ADD COLUMNS (amount_usd DOUBLE)",
        "fact_payments add amount_usd",
    )

    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_shipments ADD COLUMNS (source_ts TIMESTAMP)",
        "fact_shipments add source_ts",
    )
    _safe_sql(
        spark,
        "ALTER TABLE iceberg.silver.fact_shipments ADD COLUMNS (updated_at TIMESTAMP)",
        "fact_shipments add updated_at",
    )
