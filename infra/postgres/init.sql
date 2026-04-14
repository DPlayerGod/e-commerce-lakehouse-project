-- PostgreSQL initialization script
-- This runs automatically when PostgreSQL starts

-- Enable logical replication for CDC
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;

-- Note: 
-- - demo DB is created by POSTGRES_DB=demo in docker-compose.yml
-- - Tables are created by data-generator/seed.py (not here)
-- - This script only sets up CDC replication parameters
-- - superset DB is created by 03-superset.sql

-- No tables created here - seed.py handles that with proper schema!

