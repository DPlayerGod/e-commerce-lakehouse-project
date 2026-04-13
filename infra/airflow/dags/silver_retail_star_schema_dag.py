"""
Silver Layer DAG - Bronze Compaction + Silver Transform with SCD

Production-grade DAG for daily Silver layer transformations:
 1. Bronze Compaction (01:00): Compact 51,840 files → 180 files (BINPACK) + cleanup
 2. Silver Transform (01:15): Deserialize Avro + SCD2 + Dimension/Fact joins
 3. Silver Maintenance (02:15): OPTIMIZE tables for query performance

Architecture:
- Bronze: Raw Avro bytes (raw_value BINARY) from Kafka/CDC
- Silver: Structured tables with SCD logic (dim_customers SCD2, dim_products SCD2)
- Connection settings are read from environment variables (see _spark_common.py).
  For local dev these are set by Docker Compose; for production inject via your
  secrets backend (Vault, AWS SSM, etc.) — no code change needed.
"""

import os
import sys
from datetime import datetime, timedelta

# Ensure DAGs folder is in Python path for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from _spark_common import spark_base_conf, spark_env_vars


# --- Configuration (single source of truth: _spark_common.py) ---
SPARK_CONN_ID = "spark_default"
SPARK_JOB_BASE = "/opt/spark-services/jobs"
COMMON_CONF = spark_base_conf()
ENV_VARS = spark_env_vars()

# Centralised resource allocation (tune per environment via env vars)
SPARK_RESOURCES = {
    "driver_memory":  os.getenv("SPARK_DRIVER_MEMORY",   "1G"),
    "executor_memory": os.getenv("SPARK_EXECUTOR_MEMORY", "1G"),
    "executor_cores":  int(os.getenv("SPARK_EXECUTOR_CORES", "1")),
    "num_executors":   int(os.getenv("SPARK_NUM_EXECUTORS",  "1")),
}

default_args = {
    "owner": "ecommerce-lakehouse",
    "depends_on_past": True,  # Ensure SCD2 consistency
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id="silver_retail_star_schema",
    description="Daily Silver layer: Bronze compaction + cleanup + SCD transform",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * *",  # Daily at 01:00 UTC
    catchup=False,
    default_args=default_args,
    tags=["silver", "lakehouse", "production"],
) as dag:

    # ========================================
    # STAGE 1: Bronze Compaction + Cleanup
    # ========================================
    bronze_compaction = SparkSubmitOperator(
        task_id="bronze_compaction",
        conn_id=SPARK_CONN_ID,
        application=os.path.join(SPARK_JOB_BASE, "bronze_compaction.py"),
        application_args=[
            "--tables",
            "bronze_orders,bronze_payments,bronze_shipments,bronze_delivery_status,bronze_users,bronze_products",
            "--partition-date",
            # Use ds (YYYY-MM-DD format, works for both scheduled + manual trigger)
            "{{ ds }}",
        ],
        conf=COMMON_CONF,
        env_vars=ENV_VARS,
        **SPARK_RESOURCES,
        verbose=True,
    )

    # ========================================
    # STAGE 2: Silver Transform with SCD
    # ========================================
    silver_transform = SparkSubmitOperator(
        task_id="silver_transform_scd",
        conn_id=SPARK_CONN_ID,
        application=os.path.join(SPARK_JOB_BASE, "silver_transform.py"),
        application_args=[
            "--mode",
            "incremental",
            "--partition-date",
            "{{ ds }}",
        ],
        conf=COMMON_CONF,
        env_vars=ENV_VARS,
        packages="org.apache.spark:spark-avro_2.12:3.5.6",
        **SPARK_RESOURCES,
        verbose=True,
    )

    # ========================================
    # STAGE 3: Maintenance (Spark-based)
    # ========================================
    maintenance_silver = SparkSubmitOperator(
        task_id="maintenance_silver_tables",
        conn_id=SPARK_CONN_ID,
        application=os.path.join(SPARK_JOB_BASE, "silver_maintenance.py"),
        application_args=[
            "--tables",
            "dim_customers,dim_products,fact_orders,fact_payments,fact_shipments",
        ],
        conf=COMMON_CONF,
        env_vars=ENV_VARS,
        **SPARK_RESOURCES,
        verbose=True,
    )

    # ========================================
    # DAG Flow
    # ========================================
    bronze_compaction >> silver_transform >> maintenance_silver
