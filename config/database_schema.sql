-- =============================================================================
-- database_schema.sql
-- =============================================================================
-- Initialisation script for the IoT Sensors data warehouse (PostgreSQL 13+).
-- This file is automatically executed by the data-warehouse-db container on
-- first startup via the /docker-entrypoint-initdb.d/ mechanism.
--
-- Tables
-- ------
--   raw_sensor_data       : Stores every raw sensor reading as received from
--                           the CSV ingestion task.  A unique constraint on
--                           (device_id, timestamp) enables idempotent UPSERTs.
--
--   processed_sensor_data : Stores daily, per-device aggregated readings
--                           produced by the transformation & loading tasks.
--                           A unique constraint on (device_id, reading_date)
--                           supports ON CONFLICT DO UPDATE (UPSERT) semantics.
--
--   quarantined_sensor_data : Rows that failed data-quality checks are logged
--                             here for later review / reprocessing.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Extension
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------------
-- raw_sensor_data
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id               BIGSERIAL PRIMARY KEY,
    device_id        VARCHAR(50)  NOT NULL,
    timestamp        TIMESTAMPTZ  NOT NULL,
    temperature      NUMERIC(8, 4),
    humidity         NUMERIC(8, 4),
    pressure         NUMERIC(10, 4),
    processed_flag   BOOLEAN      NOT NULL DEFAULT FALSE,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_raw_device_timestamp UNIQUE (device_id, timestamp)
);

-- Index for the transformation task's time-window query
CREATE INDEX IF NOT EXISTS idx_raw_timestamp
    ON raw_sensor_data (timestamp);

-- Index to quickly find un-processed rows
CREATE INDEX IF NOT EXISTS idx_raw_processed_flag
    ON raw_sensor_data (processed_flag)
    WHERE processed_flag = FALSE;

COMMENT ON TABLE  raw_sensor_data IS
    'Stores every raw IoT sensor reading ingested from CSV files.';
COMMENT ON COLUMN raw_sensor_data.processed_flag IS
    'Set to TRUE once the row has been successfully transformed and loaded.';

-- ---------------------------------------------------------------------------
-- processed_sensor_data
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_sensor_data (
    id                  BIGSERIAL PRIMARY KEY,
    device_id           VARCHAR(50)  NOT NULL,
    reading_date        DATE         NOT NULL,
    avg_temperature     NUMERIC(8, 4),
    avg_humidity        NUMERIC(8, 4),
    avg_pressure        NUMERIC(10, 4),
    reading_count       INTEGER      NOT NULL DEFAULT 0,
    data_quality_flags  JSONB        NOT NULL DEFAULT '{}',
    processed_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_processed_device_date UNIQUE (device_id, reading_date)
);

-- Index for fast per-device queries
CREATE INDEX IF NOT EXISTS idx_processed_device_id
    ON processed_sensor_data (device_id);

-- Index for date-range analytical queries
CREATE INDEX IF NOT EXISTS idx_processed_reading_date
    ON processed_sensor_data (reading_date);

-- GIN index for querying JSONB quality flags
CREATE INDEX IF NOT EXISTS idx_processed_dq_flags
    ON processed_sensor_data USING GIN (data_quality_flags);

COMMENT ON TABLE  processed_sensor_data IS
    'Daily per-device aggregated sensor readings after transformation and quality checks.';
COMMENT ON COLUMN processed_sensor_data.data_quality_flags IS
    'JSONB blob capturing data-quality check results for this record.';
COMMENT ON COLUMN processed_sensor_data.reading_count IS
    'Number of raw readings aggregated into this daily record.';

-- ---------------------------------------------------------------------------
-- quarantined_sensor_data
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantined_sensor_data (
    id              BIGSERIAL PRIMARY KEY,
    device_id       VARCHAR(50),
    timestamp       TIMESTAMPTZ,
    temperature     NUMERIC(8, 4),
    humidity        NUMERIC(8, 4),
    pressure        NUMERIC(10, 4),
    failure_reason  TEXT         NOT NULL,
    quarantined_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantine_device_id
    ON quarantined_sensor_data (device_id);

COMMENT ON TABLE quarantined_sensor_data IS
    'Sensor readings that failed data-quality checks and are pending manual review.';

-- ---------------------------------------------------------------------------
-- Verify schema
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE 'Schema initialisation complete: raw_sensor_data, processed_sensor_data, quarantined_sensor_data';
END $$;
