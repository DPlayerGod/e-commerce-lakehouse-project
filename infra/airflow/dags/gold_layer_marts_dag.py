"""
Gold Layer DAG - Analytical Marts

This DAG processes data from the Silver layer into business-ready Gold layer marts.
Scheduled at 02:30 UTC Daily, after Silver transformations are complete.
"""

import os
import sys
from datetime import datetime, timedelta

# Ensure DAGs folder is in Python path for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from _spark_common import spark_base_conf, spark_env_vars


# --- Configuration ---
SPARK_CONN_ID = "spark_default"
SPARK_JOB_BASE = "/opt/spark-services/jobs"
COMMON_CONF = spark_base_conf()
ENV_VARS = spark_env_vars()

# Resource allocation
SPARK_RESOURCES = {
    "driver_memory":  os.getenv("SPARK_DRIVER_MEMORY",   "1G"),
    "executor_memory": os.getenv("SPARK_EXECUTOR_MEMORY", "1G"),
    "executor_cores":  int(os.getenv("SPARK_EXECUTOR_CORES", "1")),
    "num_executors":   int(os.getenv("SPARK_NUM_EXECUTORS",  "1")),
}

default_args = {
    "owner": "ecommerce-lakehouse",
    "depends_on_past": True,  # Consistency for analytical history
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


with DAG(
    dag_id="gold_layer_marts",
    description="Analytical Gold layer: Sales overview and Customer LTV",
    start_date=datetime(2024, 1, 1),
    schedule="30 2 * * *",  # Daily at 02:30 UTC
    catchup=False,
    default_args=default_args,
    tags=["gold", "lakehouse", "marts"],
) as dag:

    # ========================================
    # STAGE 1: Gold Transform (Incremental)
    # ========================================
    gold_transform = SparkSubmitOperator(
        task_id="gold_transform_marts",
        conn_id=SPARK_CONN_ID,
        application=os.path.join(SPARK_JOB_BASE, "gold_transform.py"),
        application_args=[
            "--partition-date",
            "{{ ds }}",
        ],
        conf=COMMON_CONF,
        env_vars=ENV_VARS,
        **SPARK_RESOURCES,
        verbose=True,
    )

    # ========================================
    # STAGE 2: Gold Maintenance (Optimization)
    # ========================================
    gold_maintenance = SparkSubmitOperator(
        task_id="maintenance_gold_tables",
        conn_id=SPARK_CONN_ID,
        application=os.path.join(SPARK_JOB_BASE, "gold_maintenance.py"),
        application_args=[
            "--tables",
            "mart_sales_overview,mart_customer_lifetime_value",
        ],
        conf=COMMON_CONF,
        env_vars=ENV_VARS,
        **SPARK_RESOURCES,
        verbose=True,
    )

    # ========================================
    # STAGE 3: Push to ClickHouse (BI Layer)
    # ========================================
    push_to_clickhouse = SparkSubmitOperator(
        task_id="push_marts_to_clickhouse",
        conn_id=SPARK_CONN_ID,
        application=os.path.join(SPARK_JOB_BASE, "gold_to_clickhouse.py"),
        application_args=[
            "--tables",
            "mart_sales_overview,mart_customer_lifetime_value",
            "--partition-date",
            "{{ ds }}",
        ],
        conf=COMMON_CONF,
        env_vars=ENV_VARS,
        **SPARK_RESOURCES,
        verbose=True,
        do_xcom_push=True,
        jars="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.4/clickhouse-jdbc-0.6.4-all.jar",
    )
    
    # DAG Flow
    gold_transform >> gold_maintenance >> push_to_clickhouse
