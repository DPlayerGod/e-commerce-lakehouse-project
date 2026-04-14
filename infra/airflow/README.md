# 🧩 Apache Airflow DAG Orchestration

## ⚡ Quick Start

```bash
# Start Airflow
docker-compose --profile airflow up -d

# Open UI
http://localhost:8085
# Credentials: airflow / airflow
```

## 📅 DAGs

### 🔄 silver_retail_star_schema (01:00 UTC)

| Stage | Time | Task | Output |
|-------|------|------|--------|
| 1️⃣ **Bronze Compaction** | 01:00-01:15 | Compact 51,840 files → 30/table | 180 files total |
| 2️⃣ **Silver Transform** | 01:15-02:15 | Deserialize Avro + SCD + Star schema | 5 silver tables |
| 3️⃣ **Maintenance** | 02:15-02:30 | OPTIMIZE + EXPIRE_SNAPSHOTS | Cleanup |

### 📊 gold_layer_marts (02:30 UTC)

| Stage | Task | Output |
|-------|------|--------|
| 1️⃣ **Gold Transform** | Aggregate Silver → Business metrics | mart_sales_overview, mart_customer_lifetime_value |
| 2️⃣ **Maintenance** | OPTIMIZE + EXPIRE_SNAPSHOTS | Cleanup |

### Silver Tables (Input for Gold DAG)
- `dim_customers` (SCD2 - track address history)
- `dim_products` (SCD2 - track product changes)
- `fact_orders`, `fact_payments`, `fact_shipments`

## 📊 Monitoring

| Task | Command |
|------|---------|
| View DAGs | http://localhost:8085/dags |
| View Logs | http://localhost:8085/home → DAG → task → logs |
| Check Airflow | `docker logs airflow-scheduler` |

## 🐛 Quick Fixes

**DAG not showing?**
```bash
docker-compose --profile airflow exec airflow-webserver airflow dags list
```

**Spark job failed?**
```bash
# Check Spark Master, MinIO health, Hive Metastore connection
docker-compose ps
```

## 📁 Files

- `dags/silver_retail_star_schema_dag.py` - Silver DAG definition
- `dags/gold_layer_marts_dag.py` - Gold DAG definition
- `dags/_spark_common.py` - Shared Spark config
- `processing/spark/jobs/` - Bronze compaction & Silver transform jobs
- `processing/spark/jobs/builder` - Spark job builder scripts to build dim and fact tables in the silver layer
- `.env` - Configuration overrides
- `docker-compose.yml` - Airflow services setup
