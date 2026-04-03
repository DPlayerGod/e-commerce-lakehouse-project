"""Central configuration for Spark jobs."""

from __future__ import annotations

KAFKA_BOOTSTRAP = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BRONZE_TOPICS = [
    "orders.v1",
    "payments.v1",
    "shipments.v1",
    "delivery-status.v1",
    "demo.public.users",
    "demo.public.products",
]

BRONZE_TABLE = "iceberg.bronze.raw_events"
BRONZE_CHECKPOINT = "s3a://data-lake/checkpoints/bronze/raw_events"

SPARK_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "s3a://data-lake/warehouse",
    "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg.s3.endpoint": MINIO_ENDPOINT,
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    "spark.sql.catalog.iceberg.s3.region": "ap-southeast-1",
    "spark.sql.catalog.iceberg.s3.access-key-id": MINIO_ACCESS_KEY,
    "spark.sql.catalog.iceberg.s3.secret-access-key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.endpoint.region": "ap-southeast-1",
}
