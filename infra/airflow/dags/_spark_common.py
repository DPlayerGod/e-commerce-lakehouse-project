"""Shared utilities for Airflow DAGs that submit Spark jobs."""

from __future__ import annotations

import os
from typing import Iterable

from airflow.datasets import Dataset

# ---------------------------------------------------------------------------
# Connection settings — read from environment variables.
# For local dev, Docker Compose sets these via the `environment:` block.
# For production, inject via your secrets backend (Vault, AWS SSM, etc.).
# ---------------------------------------------------------------------------
_MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
_MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")  # dev default only
_MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")  # dev default only
_AWS_REGION       = os.getenv("AWS_REGION",         "ap-southeast-1")
_SPARK_MASTER     = os.getenv("SPARK_MASTER",       "spark://spark-master:7077")
_WAREHOUSE_PATH   = os.getenv("ICEBERG_WAREHOUSE",  "s3a://data-lake/warehouse")


def _build_base_conf() -> dict[str, str]:
    """Build Spark base config, resolving connection settings from env vars."""
    return {
        # Hive Metastore
        "hive.metastore.uris": "thrift://hive-metastore:9083",
        "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",

        # Spark Master
        "spark.master": _SPARK_MASTER,
        "spark.submit.deployMode": "client",

        # Resource Configuration
        "spark.executor.instances": "2",
        "spark.executor.cores": "1",
        "spark.executor.memory": "1G",
        "spark.executor.memoryFraction": "0.8",
        "spark.driver.memory": "1G",
        "spark.driver.maxResultSize": "512m",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",

        # Storage: Iceberg + MinIO/S3
        "spark.sql.warehouse.dir": _WAREHOUSE_PATH,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "iceberg",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "hadoop",
        "spark.sql.catalog.iceberg.warehouse": _WAREHOUSE_PATH,
        "spark.sql.catalog.iceberg.s3.endpoint": _MINIO_ENDPOINT,
        "spark.sql.catalog.iceberg.s3.path-style-access": "true",
        "spark.sql.catalog.iceberg.s3.region": _AWS_REGION,

        # Hadoop S3A
        "spark.hadoop.fs.s3a.endpoint": _MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": _MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": _MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

        # Propagate credentials to executors (required for Iceberg compaction workers)
        "spark.executorEnv.AWS_REGION": _AWS_REGION,
        "spark.executorEnv.AWS_ACCESS_KEY_ID": _MINIO_ACCESS_KEY,
        "spark.executorEnv.AWS_SECRET_ACCESS_KEY": _MINIO_SECRET_KEY,
        "spark.driverEnv.AWS_REGION": _AWS_REGION,
        "spark.driverEnv.AWS_ACCESS_KEY_ID": _MINIO_ACCESS_KEY,
        "spark.driverEnv.AWS_SECRET_ACCESS_KEY": _MINIO_SECRET_KEY,

        # Optimizations
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.kryoserializer.buffer.max": "256m",

        # Iceberg concurrency: snapshot isolation + optimistic locking
        # Handles concurrent streaming micro-batches + daily compaction
        "spark.sql.iceberg.commit.conflict.mode": "retry",
        "spark.sql.iceberg.commit.total-retries": "10",
        "spark.sql.iceberg.optimistic-locking.enabled": "true",
        "spark.sql.iceberg.fast-append.enabled": "true",
    }


_BASE_CONF: dict[str, str] = _build_base_conf()

_ENV_VARS: dict[str, str] = {
    "AWS_REGION": _AWS_REGION,
    "AWS_DEFAULT_REGION": _AWS_REGION,
    "AWS_ACCESS_KEY_ID": _MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": _MINIO_SECRET_KEY,
    "MINIO_ENDPOINT": _MINIO_ENDPOINT,
}


def spark_base_conf() -> dict[str, str]:
    """Return baseline Spark configuration shared by DAGs."""
    return dict(_BASE_CONF)


def spark_env_vars() -> dict[str, str]:
    """Return environment variables for Spark."""
    return dict(_ENV_VARS)


def iceberg_dataset(table_identifier: str) -> Dataset:
    """Create an Airflow Dataset for an Iceberg table (for DAG dependencies)."""
    return Dataset(f"iceberg://{table_identifier}")


def iceberg_maintenance(
    table: str,
    expire_days: int = 14,
    retain_snapshots: int = 3,
    **context,
) -> None:
    """Perform Iceberg maintenance on a table: compact + expire snapshots + remove orphans.

    Args:
        table:             fully-qualified table name, e.g. 'iceberg.silver.dim_customers'
        expire_days:       drop snapshots older than this many days (default 14)
        retain_snapshots:  minimum snapshots to keep regardless of age (default 3)
    """
    from pyspark.sql import SparkSession

    try:
        builder = SparkSession.builder.appName(f"iceberg_maintenance_{table}")
        for key, value in spark_base_conf().items():
            builder = builder.config(key, value)
        for key, value in spark_env_vars().items():
            builder = builder.config(f"spark.driverEnv.{key}", value)
            builder = builder.config(f"spark.executorEnv.{key}", value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        print(f"🧹 Iceberg Maintenance: {table}")

        # Stage 1: Compact small files (BINPACK)
        print(f"📦  rewrite_data_files {table}")
        spark.sql(f"""
            CALL system.rewrite_data_files(
                table    => '{table}',
                strategy => 'binpack'
            )
        """)

        # Stage 2: Expire old snapshots
        print(f"🗑️   expire_snapshots {table} (older than {expire_days} days, keep >= {retain_snapshots})")
        spark.sql(f"""
            CALL system.expire_snapshots(
                table       => '{table}',
                older_than  => current_timestamp() - INTERVAL {expire_days} DAYS,
                retain_last => {retain_snapshots}
            )
        """)

        # Stage 3: Remove orphaned files (>= 24 h old to avoid writer race conditions)
        print(f"🧹  remove_orphan_files {table}")
        spark.sql(f"""
            CALL system.remove_orphan_files(
                table      => '{table}',
                older_than => current_timestamp() - INTERVAL 1 DAY
            )
        """)

        print(f"✅ Maintenance complete: {table}")
        spark.stop()

    except Exception as e:
        print(f"⚠️  Maintenance error for {table}: {e}")
        # Non-fatal — do not fail the calling DAG task


__all__ = [
    "spark_base_conf",
    "spark_env_vars",
    "iceberg_dataset",
    "iceberg_maintenance",
]
