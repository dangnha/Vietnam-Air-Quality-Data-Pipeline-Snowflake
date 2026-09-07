-- ============================================================================
-- 04. Silver Layer (star schema)
--
-- SILVER = cleaned, typed, deduplicated. This is a classic star schema:
--     dim_station  (dimension)
--     fact_aqi     (fact, grain = one measurement at one station/time)
--
-- Snowflake advantage: clustering keys on the fact table make range scans on
-- queried_city/date cheap, and the automatic micro-partitioning removes the need
-- for manual Parquet partitioning (contrast with the AWS S3 path partitioning).
-- ============================================================================

USE SCHEMA AQ_WAREHOUSE.SILVER;

-- ----------------------------------------------------------------------------
-- Dimension: stations
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DIM_STATION (
    waqi_idx       INT           NOT NULL,
    station_name   STRING        NOT NULL,
    queried_city   STRING        NOT NULL,
    lat            DOUBLE,
    lon            DOUBLE,
    url            STRING,
    source         STRING        NOT NULL COMMENT 'kaggle | api',
    loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (waqi_idx, queried_city) NOT ENFORCED
)
COMMENT = 'Silver: conformed station dimension (one row per station per city)';

-- ----------------------------------------------------------------------------
-- Fact: air-quality measurements
-- Grain = (waqi_idx, measured_at). Same business shape as the AWS `fact_aqi`.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.FACT_AQI (
    waqi_idx            INT           NOT NULL,
    measured_at         TIMESTAMP_NTZ NOT NULL,
    aqi                 DOUBLE,
    dominant_pollutant  STRING,
    pm25                DOUBLE,
    pm10                DOUBLE,
    co                  DOUBLE,
    no2                 DOUBLE,
    o3                  DOUBLE,
    so2                 DOUBLE,
    humidity            DOUBLE,
    temperature         DOUBLE,
    pressure            DOUBLE,
    wind                DOUBLE,
    source              STRING        NOT NULL COMMENT 'kaggle | api',
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    queried_city        STRING        NOT NULL,
    year                INT,
    month               INT
)
CLUSTER BY (queried_city, year, month, measured_at)
COMMENT = 'Silver: one row per station measurement (star-schema fact)';

-- ----------------------------------------------------------------------------
-- The dimension is populated by:
--   * python/orchestrate/native_pipeline.py  -> CALL SILVER.LOAD_FROM_BRONZE()
--   * the Streams/Tasks ELT path             -> SILVER.LOAD_SILVER_TASK
-- Both use the same stored procedure, so the two engines stay in lock-step.
-- ----------------------------------------------------------------------------
