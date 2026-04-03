-- PostgreSQL initialization script
-- This runs automatically when PostgreSQL starts

-- Enable logical replication for CDC
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;

-- Create demo database if not exists
CREATE DATABASE IF NOT EXISTS demo;

-- For each table in demo, set REPLICA IDENTITY FULL
-- This will be picked up by Debezium CDC

-- Create function to set REPLICA IDENTITY FULL for tables in demo DB
-- (Note: This script runs in postgres DB, so we'll set it per-table in seed.py instead)
