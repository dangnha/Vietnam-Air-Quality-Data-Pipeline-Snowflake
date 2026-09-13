-- ============================================================================
-- 04. Silver Layer (full Kimball star schema)
--
-- SILVER = cleaned, typed, deduplicated, and modeled into a proper star schema:
--
--     DIM_DATE     -> conformed calendar dimension (smart key = YYYYMMDD)
--     DIM_TIME     -> conformed time-of-day dimension (smart key = HHMM)
--     DIM_CITY     -> conformed city dimension (surrogate key)
--     DIM_STATION  -> conformed station dimension (surrogate key)
--     FACT_AQI     -> measurement fact referencing the four dimensions
--
-- Why surrogate keys? Natural keys (WAQI idx, city slug) are stable now, but a
-- surrogate key decouples the warehouse from source changes, makes fact joins
-- smaller/faster, and lets us support slowly-changing dimension behavior later.
--
-- The degenerate dimensions (pollution_level, dominant_pollutant, source) stay
-- on the fact table because they have no meaningful dimension of their own.
-- ============================================================================

USE SCHEMA AQ_WAREHOUSE.SILVER;

-- ----------------------------------------------------------------------------
-- Surrogate-key sequences. Values are meaningless by design — they exist only
-- to guarantee a unique, stable identifier for each dimension row.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE SEQUENCE SILVER.SEQ_CITY_KEY
  START = 1
  INCREMENT = 1
  COMMENT = 'Surrogate keys for SILVER.DIM_CITY';

CREATE OR REPLACE SEQUENCE SILVER.SEQ_STATION_KEY
  START = 1
  INCREMENT = 1
  COMMENT = 'Surrogate keys for SILVER.DIM_STATION';

-- ----------------------------------------------------------------------------
-- Reusable AQI -> human-readable category. One source of truth used by both
-- Silver (fact degenerate dimension) and Gold (aggregate summaries).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SILVER.AQI_CATEGORY(aqi DOUBLE)
RETURNS STRING
AS
$$
CASE
  WHEN aqi <= 50  THEN 'Good'
  WHEN aqi <= 100 THEN 'Moderate'
  WHEN aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
  WHEN aqi <= 200 THEN 'Unhealthy'
  WHEN aqi <= 300 THEN 'Very Unhealthy'
  ELSE 'Hazardous'
END
$$;

-- ----------------------------------------------------------------------------
-- Dimension: calendar. A date spine lets analysts slice by quarter, weekday,
-- season, etc. without repeating date logic in every query.
-- date_key is a "smart" YYYYMMDD integer (common, human-readable date key).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DIM_DATE (
    date_key      INT           NOT NULL,
    full_date     DATE          NOT NULL,
    year          INT,
    month         INT,
    month_name    STRING,
    day_of_month  INT,
    day_of_week   INT,
    day_name      STRING,
    quarter       INT,
    week_of_year  INT,
    is_weekend    BOOLEAN,
    season        STRING,
    PRIMARY KEY (date_key) NOT ENFORCED
)
COMMENT = 'Silver: conformed calendar dimension (date_key = YYYYMMDD)';

INSERT OVERWRITE INTO SILVER.DIM_DATE
SELECT
    TO_NUMBER(TO_VARCHAR(d.value, 'YYYYMMDD')) AS date_key,
    d.value                                    AS full_date,
    YEAR(d.value)                              AS year,
    MONTH(d.value)                             AS month,
    MONTHNAME(d.value)                         AS month_name,
    DAY(d.value)                               AS day_of_month,
    DAYOFWEEKISO(d.value)                      AS day_of_week,
    DAYNAME(d.value)                           AS day_name,
    QUARTER(d.value)                           AS quarter,
    WEEKOFYEAR(d.value)                        AS week_of_year,
    DAYOFWEEKISO(d.value) IN (6, 7)            AS is_weekend,
    CASE
      WHEN MONTH(d.value) BETWEEN 11 AND 12 THEN 'Dry'
      WHEN MONTH(d.value) BETWEEN 1 AND 4   THEN 'Dry'
      ELSE 'Rainy'
    END                                        AS season
FROM (
    SELECT DATEADD(DAY, seq4(), '2019-01-01'::DATE) AS value
    FROM TABLE(GENERATOR(ROWCOUNT => 4383))          -- 2019-01-01 .. 2030-12-31
) d;

-- ----------------------------------------------------------------------------
-- Dimension: time-of-day. Measurements arrive at 08:00, 14:00 and 20:00, so a
-- compact time dimension exposes meaningful "Morning / Afternoon / Evening"
-- slices without deriving the same CASE in every downstream query.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DIM_TIME (
    time_key     INT           NOT NULL,
    hour         INT           NOT NULL,
    minute       INT           NOT NULL,
    time_of_day  STRING        NOT NULL,
    PRIMARY KEY (time_key) NOT ENFORCED
)
COMMENT = 'Silver: conformed time-of-day dimension (time_key = HHMM)';

INSERT OVERWRITE INTO SILVER.DIM_TIME
SELECT
    t.value * 100  AS time_key,
    t.value        AS hour,
    0              AS minute,
    CASE
      WHEN t.value BETWEEN 5 AND 11  THEN 'Morning'
      WHEN t.value BETWEEN 12 AND 17 THEN 'Afternoon'
      WHEN t.value BETWEEN 18 AND 22 THEN 'Evening'
      ELSE 'Night'
    END            AS time_of_day
FROM (
    SELECT seq4() AS value
    FROM TABLE(GENERATOR(ROWCOUNT => 24))   -- one row per hour
) t;

-- ----------------------------------------------------------------------------
-- Dimension: city. Built from CONTROL.CITY_REFERENCE so the raw payload stays
-- free of editorial attributes (region, display name, center coordinates).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DIM_CITY (
    city_key   INT           NOT NULL,
    city_slug  STRING        NOT NULL,
    city_name  STRING        NOT NULL,
    region     STRING        NOT NULL,
    lat        DOUBLE,
    lon        DOUBLE,
    PRIMARY KEY (city_key) NOT ENFORCED,
    UNIQUE (city_slug) NOT ENFORCED
)
COMMENT = 'Silver: conformed city dimension (surrogate key + WAQI slug natural key)';

MERGE INTO SILVER.DIM_CITY AS t
USING CONTROL.CITY_REFERENCE AS s
ON t.city_slug = s.city_slug
WHEN MATCHED THEN UPDATE SET
    city_name = s.city_name,
    region    = s.region,
    lat       = s.lat,
    lon       = s.lon
WHEN NOT MATCHED THEN INSERT
    (city_key, city_slug, city_name, region, lat, lon)
  VALUES
    (SILVER.SEQ_CITY_KEY.NEXTVAL, s.city_slug, s.city_name, s.region, s.lat, s.lon);

-- ----------------------------------------------------------------------------
-- Dimension: station. Natural key is (waqi_idx, city_key); station_key is the
-- surrogate. Type-1 history (latest snapshot) is enough for this dataset, but
-- first_seen/last_seen already record the observation window for later SCD work.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DIM_STATION (
    station_key  INT           NOT NULL,
    waqi_idx     INT           NOT NULL,
    city_key     INT           NOT NULL,
    station_name STRING        NOT NULL,
    lat          DOUBLE,
    lon          DOUBLE,
    url          STRING,
    source       STRING        NOT NULL,
    is_active    BOOLEAN       DEFAULT TRUE,
    first_seen   TIMESTAMP_NTZ,
    last_seen    TIMESTAMP_NTZ,
    loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (station_key) NOT ENFORCED,
    UNIQUE (waqi_idx, city_key) NOT ENFORCED
)
COMMENT = 'Silver: conformed station dimension (surrogate key + natural waqi_idx/city)';

-- ----------------------------------------------------------------------------
-- Fact: one row per measurement. Grain = (station_key, measured_at).
-- The four dimension keys replace the old natural keys; the measurable columns
-- and degenerate dimensions stay on the fact row.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.FACT_AQI (
    date_key           INT           NOT NULL,
    time_key           INT           NOT NULL,
    city_key           INT           NOT NULL,
    station_key        INT           NOT NULL,
    measured_at        TIMESTAMP_NTZ NOT NULL,
    aqi                DOUBLE,
    pollution_level    STRING,
    dominant_pollutant STRING,
    pm25               DOUBLE,
    pm10               DOUBLE,
    co                 DOUBLE,
    no2                DOUBLE,
    o3                 DOUBLE,
    so2                DOUBLE,
    humidity           DOUBLE,
    temperature        DOUBLE,
    pressure           DOUBLE,
    wind               DOUBLE,
    source             STRING        NOT NULL,
    ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (city_key, date_key)
COMMENT = 'Silver: one row per measurement, referencing date/time/city/station dims';

-- ----------------------------------------------------------------------------
-- The dimension and fact rows are populated by:
--   * python/orchestrate/native_pipeline.py  -> CALL SILVER.LOAD_FROM_BRONZE()
--   * the Streams/Tasks ELT path             -> CONTROL.LOAD_SILVER_TASK
-- Both use the same stored procedure in sql/06_streams_tasks.sql, so the two
-- engines stay in lock-step.
-- ----------------------------------------------------------------------------
