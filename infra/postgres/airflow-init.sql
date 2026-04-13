-- Create Airflow database and user
-- This script is run as postgres superuser during container startup

-- Create airflow user
CREATE USER airflow WITH PASSWORD 'airflow';

-- Create airflow database
CREATE DATABASE airflow OWNER airflow;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER ROLE airflow CREATEDB;

-- Create Debezium storage topic schemas (for Kafka Connect)
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
