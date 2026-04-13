#!/bin/bash
set -e

# Upgrade database schema (airflow 3.x uses 'migrate' instead of 'upgrade')
airflow db migrate

# Create default admin user if it doesn't exist
airflow users create \
  --username airflow \
  --password airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com 2>/dev/null || echo "User may already exist"

# Create Spark connection for SparkSubmitOperator
# Points to the Spark standalone master running in the spark-ingest profile
airflow connections add spark_default \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077 \
  2>/dev/null || echo "Spark connection already exists"

# Run the command passed as arguments
exec "$@"
