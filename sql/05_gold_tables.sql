-- ============================================================================
-- 05. Gold Layer (analytics-ready)
--
-- GOLD = business aggregates. Each table answers one specific question and is
-- safe to expose to BI tools (Athena/QuickSight in AWS; any SQL BI on Snowflake).
--
-- Snowflake advantage: these are normal tables, queryable instantly with
-- virtual-warehouse compute. No separate query service. The aggregates are
-- populated by the GOLD.LOAD_* stored procedures in sql/06_streams_tasks.sql.
-- ============================================================================

USE SCHEMA AQ_WAREHOUSE.GOLD;

-- ----------------------------------------------------------------------------
-- Daily summary (one row per city per day)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.AQI_DAILY_SUMMARY (
    queried_city        STRING,
    date                DATE,
    avg_aqi             DOUBLE,
    max_aqi             DOUBLE,
    min_aqi             DOUBLE,
    avg_pm25            DOUBLE,
    avg_pm10            DOUBLE,
    avg_humidity        DOUBLE,
    avg_temperature     DOUBLE,
    station_count       INT,
    record_count        INT,
    dominant_pollutant  STRING,
    pollution_level     STRING,
    aggregated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (date)
COMMENT = 'Gold: daily air-quality summary per city (analytics-ready)';

-- ----------------------------------------------------------------------------
-- City ranking (one row per city per month)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.AQI_CITY_RANKING (
    queried_city        STRING,
    year                INT,
    month               INT,
    avg_aqi             DOUBLE,
    max_aqi             DOUBLE,
    min_aqi             DOUBLE,
    avg_pm25            DOUBLE,
    avg_pm10            DOUBLE,
    record_count        INT,
    days_good           INT,
    days_moderate       INT,
    days_sensitive      INT,
    days_unhealthy      INT,
    days_very_unhealthy INT,
    days_hazardous      INT,
    dominant_pollutant  STRING,
    pollution_level     STRING,
    aqi_rank            INT,
    pm25_rank           INT,
    aggregated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Gold: monthly city ranking with pollution-day distribution';

-- ----------------------------------------------------------------------------
-- Station summary (one row per station per month)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.STATION_SUMMARY (
    waqi_idx            INT,
    station_name        STRING,
    queried_city        STRING,
    lat                 DOUBLE,
    lon                 DOUBLE,
    year                INT,
    month               INT,
    avg_aqi             DOUBLE,
    max_aqi             DOUBLE,
    min_aqi             DOUBLE,
    avg_pm25            DOUBLE,
    avg_pm10            DOUBLE,
    avg_humidity        DOUBLE,
    avg_temperature     DOUBLE,
    record_count        INT,
    dominant_pollutant  STRING,
    pollution_level     STRING,
    rank_in_city        INT,
    aggregated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (queried_city, year, month)
COMMENT = 'Gold: monthly station summary with within-city ranking';
