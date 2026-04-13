# 🧩 Apache Airflow - E-commerce Lakehouse

## Overview

- **Purpose**: Orchestrate batch data pipeline jobs (Bronze compaction, Silver transform with SCD)
- **Executor**: LocalExecutor (simple, lightweight)
- **Database**: PostgreSQL (Airflow metadata)
- **Profile**: `airflow`

## 🚀 Getting Started

### 1. Start Airflow Stack

```bash
docker-compose --profile airflow up -d
```

This starts 2 services:
- `airflow-scheduler` - Runs DAGs on schedule (background)
- `airflow-webserver` - UI at http://localhost:8085

### 2. Admin Credentials

- **Username**: `airflow`
- **Password**: `airflow`
- **URL**: http://localhost:8085

### 3. Check DAG Status

Visit http://localhost:8085

Expected DAGs:
- `silver_retail_star_schema` - Daily at 01:00 UTC

## 📁 Directory Structure

```
infra/airflow/
├── Dockerfile                    # Airflow image (Spark + JARs)
├── README.md                     # This file
├── dags/                         # DAG definitions
│   ├── _spark_common.py         # Shared Spark config + utilities
│   └── silver_retail_star_schema_dag.py  # Main DAG
├── logs/                         # Airflow task logs (auto-created)
├── plugins/                      # Custom Airflow plugins (reserved)
└── processing/
    └── spark/
        └── jobs/
            ├── bronze_compaction.py     # Compact Bronze (10s batches → 30 files)
            └── silver_transform.py       # Apply SCD + Star schema to Silver
```

## 🔄 DAG: silver_retail_star_schema

**Schedule**: Daily at 01:00 UTC

**Stages**:

### Stage 1: Bronze Compaction (01:00-01:15)
- Input: 51,840 files/day (10-sec micro-batches)
- Output: ~30 files/table (180 total)
- Strategy: BINPACK (size-balanced)
- Tables: bronze_orders, bronze_payments, bronze_shipments, bronze_delivery_status, bronze_users, bronze_products

### Stage 2: Silver Transform + SCD (01:15-02:15)
- **Input**: Bronze raw_value (binary Avro)
- **Transform**:
  1. Deserialize Avro → struct columns
  2. Deduplication (latest per key)
  3. Apply SCD logic:
     - **dim_customers** (SCD2): Track history (address changes)
      - **dim_products** (SCD2): Track product history (price/title/category changes)
  4. Create fact tables with joins
    5. Z-order optimization by business/surrogate keys

- **Output Tables**:
  - `iceberg.silver.dim_customers` (SCD2)
  - `iceberg.silver.dim_products` (SCD2)
  - `iceberg.silver.fact_orders`
  - `iceberg.silver.fact_payments`
  - `iceberg.silver.fact_shipments`

### Stage 3: Maintenance (02:15-02:30)
- OPTIMIZE (compact small files)
- EXPIRE_SNAPSHOTS (cleanup old versions)

## 🔧 Spark Configuration

All jobs use shared config from `_spark_common.py`:

```python
PACKAGES = spark_packages()  # None  (JARs added via Dockerfile)
BASE_CONF = spark_base_conf()  # Iceberg + MinIO + Hive Metastore
ENV_VARS = spark_env_vars()  # AWS credentials
```

### Connection

- **Spark Master**: `spark://spark-master:7077`
- **Hive Metastore**: `thrift://hive-metastore:9083`
- **MinIO Endpoint**: `http://minio:9000`
- **Region**: `ap-southeast-1`

## 📊 Monitoring

### View DAG Runs
```
http://localhost:8085/dags/silver_retail_star_schema/grid
```

### View Task Logs
```
http://localhost:8085/home (click DAG → click task → logs)
```

### Check Airflow Logs
```bash
docker-compose --profile airflow logs -f airflow-scheduler
```

### Check Spark Job Logs
```bash
docker logs ecommerce-spark-job-ingest
```

## 🐛 Troubleshooting

### Airflow won't start

```bash
# Check init logs
docker-compose --profile airflow logs ecommerce-airflow-init

# Common issues:
# - PostgreSQL not ready: Wait for postgres health check
# - Port 8085 in use: Change port in docker-compose.yml
```

### DAG not appearing

```bash
# DAGs stored in /opt/airflow/dags
# Check volume mount in docker-compose.yml

# Refresh DAG parser:
docker-compose --profile airflow exec airflow-webserver airflow dags list
```

### Spark job fails

```bash
# Check Spark logs
docker-compose --profile airflow logs airflow-worker

# Common issues:
# - Spark Master not ready: Check spark-master health
# - MinIO not ready: Check minio health + credentials
# - Iceberg catalog not found: Check Hive Metastore
```

### Task stuck in "running"

```bash
# Kill stuck task
docker-compose --profile airflow exec airflow-scheduler airflow tasks kill silver_retail_star_schema task_id run_id
```

## 🚀 Next Steps

### 1. Verify Bronze Tables Exist
```bash
docker-compose --profile airflow exec airflow-webserver spark-sql -e "
  SHOW TABLES IN iceberg.bronze;
"
```

### 2. Manual Test Bronze Compaction
```bash
docker-compose --profile airflow exec airflow-webserver spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-services/jobs/bronze_compaction.py \
  --tables bronze_orders \
  --target-files-per-table 30
```

### 3. Manual Test Silver Transform
```bash
docker-compose --profile airflow exec airflow-webserver spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-services/jobs/silver_transform.py \
  --mode full
```

### 4. Trigger DAG Manually
```
http://localhost:8085/dags/silver_retail_star_schema/trigger
```

## ⚙️ Configuration Files

- `.env` - Environment variables (optional, for Airflow overrides)
- `docker-compose.yml` - Container orchestration
- `infra/airflow/dags/_spark_common.py` - Shared Spark config
- `infra/spark-services/config.py` - Spark job config (topics, tables, etc.)

## 📝 Notes

### Why SequentialExecutor?
- Simple: No Redis/Celery needed
- Suitable for development/testing
- Production: Switch to CeleryExecutor or KubernetesExecutor

### Why Bronze Compaction?
- 10-sec micro-batches create 8,640 files/day per table
- Compaction → 30 files/table (99.3% reduction)
- Faster metadata operations, better query performance

### Why SCD Type 2 for Customers?
- Track historical address/city changes
- Support analytical queries: "Customer X lived in Y from date A to date B"
- Supports slowly changing dimensions in star schema

### Why SCD Type 1 for Products?
- Only care about current price
- Simpler logic, smaller table size
- No need to track price history

## 📚 Resources

- [Airflow Docs](https://airflow.apache.org/docs/)
- [Iceberg Documentation](https://iceberg.apache.org/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html)
- [SCD Types](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/slowly-changing-dimensions/)
