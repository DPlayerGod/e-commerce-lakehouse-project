"""MinIO setup helpers for Spark jobs."""

from __future__ import annotations

import logging
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession


LOG = logging.getLogger(__name__)


def apply_minio_s3a_config(builder: SparkSession.Builder, *, endpoint: str, access_key: str, secret_key: str) -> SparkSession.Builder:
    """Attach MinIO S3A configuration to a Spark session builder."""

    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )


def ensure_bucket_exists(bucket_name: str, *, endpoint: str, access_key: str, secret_key: str) -> None:
    """Ensure S3/MinIO bucket exists, create if missing."""
    # Normalize endpoint URL
    # Input: "http://minio:9000" or "minio:9000"
    # Output: "http://minio:9000" (ready for boto3)
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint_url = f"http://{endpoint}"
    else:
        endpoint_url = endpoint
    
    LOG.info(f"Connecting to MinIO at {endpoint_url}")
    
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",  # MinIO default region
    )
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        LOG.info(f"Bucket exists: {bucket_name}")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            # Bucket doesn't exist, create it
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                LOG.info(f"Created bucket: {bucket_name}")
            except ClientError as create_err:
                LOG.error(f"Failed to create bucket {bucket_name}: {create_err}")
                raise
        else:
            LOG.error(f"Error checking bucket {bucket_name}: {e}")
            raise

