-- ============================================================================
-- 07. Snowflake Feature Demos (why Snowflake as a warehouse)
--
-- Each block demonstrates a capability the AWS (S3 + Glue + Athena) version
-- does not give you out of the box. Run these interactively to *feel* the
-- difference. These are educational demos, not production code.
-- ============================================================================

USE WAREHOUSE AQ_WH;
USE DATABASE AQ_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- 1. VARIANT + FLATTEN: query raw JSON with SQL, no schema, no crawler.
--    In AWS you'd need a Glue Crawler + Athena; here it's one query.
-- ----------------------------------------------------------------------------
SELECT
    queried_city,
    raw_payload:data:aqi::DOUBLE          AS aqi,
    raw_payload:data:city:name::STRING    AS station
FROM BRONZE.API_RAW
LIMIT 10;

-- Flatten a nested array (e.g. multiple pollutants) without pre-defining columns:
SELECT
    queried_city,
    f.value:key::STRING   AS pollutant,
    f.value:v::DOUBLE     AS value
FROM BRONZE.API_RAW,
     LATERAL FLATTEN(input => raw_payload:data:iaqi) AS f
LIMIT 10;

-- ----------------------------------------------------------------------------
-- 2. TIME TRAVEL: rewind a table to any point in the last 90 days (Enterprise).
--    "What did Silver look like before this morning's bad batch?"
-- ----------------------------------------------------------------------------
SELECT COUNT(*) AS now_count
FROM SILVER.FACT_AQI;

SELECT COUNT(*) AS one_hour_ago_count
FROM SILVER.FACT_AQI AT (OFFSET => -60 * 60);

-- Undo a bad load (restore Silver to an earlier state):
-- CREATE OR REPLACE TABLE SILVER.FACT_AQI
-- AS SELECT * FROM SILVER.FACT_AQI BEFORE (TIMESTAMP => '2026-01-01 08:00:00');

-- ----------------------------------------------------------------------------
-- 3. ZERO-COPY CLONE: create a dev/test copy instantly, no storage duplication.
--    In AWS you'd copy every Parquet object in S3.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE DATABASE AQ_WAREHOUSE_DEV CLONE AQ_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- 4. MICRO-PARTITIONS: inspect how Snowflake organizes data automatically.
--    (Contrast: manual S3 `year=2026/month=04/` folder partitions.)
-- ----------------------------------------------------------------------------
SELECT SYSTEM$CLUSTERING_INFORMATION('SILVER.FACT_AQI', '(queried_city, year, month, measured_at)');

-- ----------------------------------------------------------------------------
-- 5. ZERO-MANAGEMENT SCALE: spin up different-sized virtual warehouses on demand.
--    A small one for ETL, an XL one for analytics — pay per second, no clusters
--    to provision. (Execute only if you need a second warehouse.)
-- ----------------------------------------------------------------------------
-- CREATE WAREHOUSE AQ_WH_ANALYTICS
--   WITH WAREHOUSE_SIZE = 'XL'
--   AUTO_SUSPEND = 60
--   INITIALLY_SUSPENDED = TRUE;
