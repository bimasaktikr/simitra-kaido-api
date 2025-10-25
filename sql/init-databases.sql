-- ========================================
-- PostgreSQL Initialization Script
-- ========================================
-- This script automatically creates required databases
-- when PostgreSQL container starts for the first time.
--
-- Databases created:
-- 1. airflow_metadata - Airflow internal database (stores DAG runs, task logs, users, etc.)
-- 2. mitra_kaido      - ML results database (stores PSO optimization & recommendations)

-- Create airflow_metadata database if not exists
SELECT 'CREATE DATABASE airflow_metadata'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_metadata')\gexec

-- Create mitra_kaido database if not exists
SELECT 'CREATE DATABASE mitra_kaido'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mitra_kaido')\gexec

-- ========================================
-- Initialize ML Training Tables
-- ========================================
\echo 'Setting up ML training tables...';
\i /docker-entrypoint-initdb.d/init-ml-tables.sql

-- Grant all privileges (already granted to postgres user by default, but explicit is better)
\c airflow_metadata
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;

\c mitra_kaido
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;

-- Log success message
\echo '✅ PostgreSQL databases created successfully!'
\echo '   - airflow_metadata (Airflow internal)'
\echo '   - mitra_kaido (ML results)'
