-- Create warehouse database
CREATE DATABASE taxi_warehouse;

-- Create metabase database
CREATE DATABASE metabase;

-- Create warehouse user
CREATE USER dataeng WITH PASSWORD 'dataeng123';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE taxi_warehouse TO dataeng;

-- Connect to warehouse database and create schema
\c taxi_warehouse

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

GRANT ALL ON SCHEMA staging TO dataeng;
GRANT ALL ON SCHEMA core TO dataeng;
GRANT ALL ON SCHEMA analytics TO dataeng;

-- Grant default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO dataeng;