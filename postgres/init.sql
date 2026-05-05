-- ============================================================================
-- NYC Taxi Batch Pipeline — Gold Layer Schema
-- ============================================================================
-- This script runs ONCE, on Postgres container's first start.
-- After that, the data directory persists across container restarts.
--
-- Schema layout:
--   gold        — analytical tables produced by Spark Silver→Gold job
--   public      — Airflow's metadata tables (managed by Airflow itself)
--
-- All gold.* tables include audit columns (created_at, source_run_id) so
-- every row is traceable back to the Spark job that produced it.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------------------------
-- Dimension: pickup/drop-off location lookup
-- Sourced from the TLC Taxi Zone Lookup CSV (~265 zones, refreshed yearly)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_id        SMALLINT PRIMARY KEY,
    borough            VARCHAR(50)  NOT NULL,
    zone               VARCHAR(100) NOT NULL,
    service_zone       VARCHAR(50),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_location_borough
    ON gold.dim_location (borough);

-- ---------------------------------------------------------------------------
-- Fact: hourly trip aggregates per pickup zone, vendor, and trip type
-- This is the ML feature table consumed by the downstream model.
-- Grain: one row per (pickup_hour, pu_location_id, vendor_id, trip_type)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_trips_hourly (
    pickup_hour          TIMESTAMPTZ NOT NULL,
    pu_location_id       SMALLINT    NOT NULL,
    vendor_id            SMALLINT    NOT NULL,
    trip_type            VARCHAR(10) NOT NULL,    -- 'yellow' or 'green'
    trip_count           INTEGER     NOT NULL CHECK (trip_count >= 0),
    total_passengers     INTEGER     NOT NULL CHECK (total_passengers >= 0),
    sum_fare_amount      NUMERIC(12,2) NOT NULL,
    sum_tip_amount       NUMERIC(12,2) NOT NULL,
    sum_trip_distance    NUMERIC(14,2) NOT NULL,
    avg_trip_duration_s  NUMERIC(10,2) NOT NULL CHECK (avg_trip_duration_s >= 0),
    source_run_id        VARCHAR(64)  NOT NULL,    -- Airflow run_id of the producing DAG
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pickup_hour, pu_location_id, vendor_id, trip_type),
    FOREIGN KEY (pu_location_id) REFERENCES gold.dim_location(location_id)
);

-- Time-range queries are the dominant access pattern (e.g., "last 90 days")
CREATE INDEX IF NOT EXISTS idx_fact_trips_hourly_time
    ON gold.fact_trips_hourly (pickup_hour DESC);

-- Zone-level lookups (e.g., "all hours for Times Square in Q1")
CREATE INDEX IF NOT EXISTS idx_fact_trips_hourly_zone
    ON gold.fact_trips_hourly (pu_location_id, pickup_hour DESC);

-- ---------------------------------------------------------------------------
-- Lineage: every Spark job execution leaves a row here
-- Powers the "Governance & Protection" requirement from Phase 1.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.pipeline_run (
    run_id           VARCHAR(64)  PRIMARY KEY,
    dag_id           VARCHAR(100) NOT NULL,
    task_id          VARCHAR(100) NOT NULL,
    job_name         VARCHAR(100) NOT NULL,
    started_at       TIMESTAMPTZ  NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           VARCHAR(20)  NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_in          BIGINT       CHECK (rows_in  >= 0),
    rows_out         BIGINT       CHECK (rows_out >= 0),
    error_message    TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_dag_time
    ON gold.pipeline_run (dag_id, started_at DESC);

-- ---------------------------------------------------------------------------
-- Confirmation logging — visible in Postgres container startup logs
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '======================================================';
    RAISE NOTICE ' Gold schema initialised';
    RAISE NOTICE '   - gold.dim_location';
    RAISE NOTICE '   - gold.fact_trips_hourly';
    RAISE NOTICE '   - gold.pipeline_run';
    RAISE NOTICE '======================================================';
END $$;
