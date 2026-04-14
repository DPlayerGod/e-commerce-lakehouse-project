-- Create Superset Database and User
CREATE DATABASE superset OWNER admin;

-- Connect to superset database and create required extensions
\c superset admin;

-- Create required extensions (if needed)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
