"""MinIO setup helpers for Spark jobs."""

from __future__ import annotations

from pyspark.sql import SparkSession


def apply_minio_s3a_config(builder: SparkSession.Builder, *, endpoint: str, access_key: str, secret_key: str) -> SparkSession.Builder:
    """Attach MinIO S3A configuration to a Spark session builder."""

    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
