-- ============================================================================
-- 03. Bronze Layer (raw)
--
-- Design principle: BRONZE = raw + immutable. Preserve the exact source shape so
-- you can always re-derive Silver/Gold, and use Snowflake Time Travel to rewind.
--
-- Snowflake advantage: JSON goes into a VARIANT column and remains *queryable*
-- without a Glue Crawler or pre-defined schema. This is ELT, not strict ETL.
-- ============================================================================

USE SCHEMA AQ_WAREHOUSE.BRONZE;

-- ----------------------------------------------------------------------------
-- Raw WAQI API payloads (one row per JSON file / per API response)
-- The whole response is a VARIANT so nothing is lost.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.API_RAW (
    queried_city  STRING   NOT NULL COMMENT 'City we asked the API for',
    raw_payload   VARIANT  NOT NULL COMMENT 'Full, untouched WAQI JSON response',
    ingested_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Load time',
    source_file   STRING COMMENT 'Original file name (auditability)'
)
COMMENT = 'Bronze: raw WAQI API responses as VARIANT (ELT-style raw preservation)';

-- ----------------------------------------------------------------------------
-- Raw Kaggle CSV (2021 historical) — the exact source columns, all STRING.
-- This mirrors the AWS project's `historical_air_quality_2021` table.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.HISTORICAL_CSV (
    station_id           STRING,
    aqi_index            STRING,
    location             STRING,
    station_name         STRING,
    url                  STRING,
    dominant_pollutant   STRING,
    co                   STRING,
    dew                  STRING,
    humidity             STRING,
    no2                  STRING,
    o3                   STRING,
    pressure             STRING,
    pm10                 STRING,
    pm25                 STRING,
    so2                  STRING,
    temperature          STRING,
    wind                 STRING,
    data_time_s          STRING,
    data_time_tz         STRING,
    status               STRING,
    alert_level          STRING,
    ingested_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Bronze: raw 2021 Kaggle historical CSV, types preserved as STRING';

-- ----------------------------------------------------------------------------
-- Loading helper: COPY INTO reads directly from the internal stage.
-- The Snowpark loader (python/load/bronze.py) issues these statements; for the
-- pure-SQL path, uncomment and run:
-- ----------------------------------------------------------------------------
-- COPY INTO BRONZE.API_RAW (queried_city, raw_payload, source_file)
--   FROM (SELECT $1:data:_queried_city::STRING, $1, metadata$filename FROM @AQ_INTERNAL_STAGE)
--   PATTERN = '.*json.*';
