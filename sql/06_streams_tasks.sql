-- ============================================================================
-- 06. Streams + Tasks (Snowflake-native incremental ELT)
--
-- This is THE Snowflake advantage over the AWS version. Instead of:
--   Glue job bookmarks + Lambda + Step Functions + EventBridge schedules,
-- Snowflake gives you:
--   * STREAM  -> a change-data-capture view over a table (new/changed rows only)
--   * TASK    -> a scheduled SQL job that runs on warehouse compute
--   * STORED PROCEDURE -> reusable transform logic that both Tasks and a local
--                         Snowpark client can call
--
-- Together they build an incremental, fully-internal pipeline with zero external
-- orchestration. Load raw data into BRONZE and the rest happens automatically.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Streams: track inserts on Bronze tables. Only NEW rows are returned on read.
-- APPEND_ONLY is safe here because Bronze is immutable (insert-only).
-- ----------------------------------------------------------------------------
USE SCHEMA AQ_WAREHOUSE.BRONZE;

CREATE OR REPLACE STREAM BRONZE.API_RAW_STREAM
  ON TABLE BRONZE.API_RAW
  APPEND_ONLY = TRUE
  COMMENT = 'CDC stream over raw WAQI API rows';

CREATE OR REPLACE STREAM BRONZE.HISTORICAL_CSV_STREAM
  ON TABLE BRONZE.HISTORICAL_CSV
  APPEND_ONLY = TRUE
  COMMENT = 'CDC stream over raw historical CSV rows';

-- ============================================================================
-- SILVER.LOAD_FROM_BRONZE()
-- Reads both streams, conforms cities, and upserts the star schema.
-- ============================================================================
USE SCHEMA AQ_WAREHOUSE.SILVER;

CREATE OR REPLACE PROCEDURE SILVER.LOAD_FROM_BRONZE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- --------------------------------------------------------------------------
  -- Conformed dimension (idempotent MERGE over both sources).
  -- --------------------------------------------------------------------------
  MERGE INTO SILVER.DIM_STATION AS t
  USING (
      SELECT * FROM (
          -- API stations
          SELECT
            r.raw_payload:data:idx::INT                      AS waqi_idx,
            r.raw_payload:data:city:name::STRING             AS station_name,
            r.queried_city                                   AS queried_city,
            r.raw_payload:data:city:geo[0]::DOUBLE           AS lat,
            r.raw_payload:data:city:geo[1]::DOUBLE           AS lon,
            r.raw_payload:data:city:url::STRING              AS url,
            'api'                                            AS source
          FROM BRONZE.API_RAW_STREAM r
          WHERE r.raw_payload:data:idx IS NOT NULL

          UNION ALL

          -- CSV stations (conform the human-readable location to a slug)
          SELECT
            TRY_TO_NUMBER(c.station_id)                      AS waqi_idx,
            c.station_name                                   AS station_name,
            COALESCE(m.conformed_city, LOWER(TRIM(c.location))) AS queried_city,
            NULL::DOUBLE                                     AS lat,
            NULL::DOUBLE                                     AS lon,
            c.url                                            AS url,
            'kaggle'                                         AS source
          FROM BRONZE.HISTORICAL_CSV_STREAM c
          LEFT JOIN CONTROL.CITY_MAP m ON m.source_city = c.location
          WHERE TRY_TO_NUMBER(c.station_id) IS NOT NULL
      ) raw
      QUALIFY ROW_NUMBER() OVER (
          PARTITION BY waqi_idx, queried_city
          ORDER BY CASE source WHEN 'api' THEN 1 ELSE 2 END
      ) = 1
  ) AS s
  ON t.waqi_idx = s.waqi_idx AND t.queried_city = s.queried_city
  WHEN MATCHED THEN UPDATE SET
      station_name = s.station_name,
      lat          = COALESCE(s.lat, t.lat),
      lon          = COALESCE(s.lon, t.lon),
      url          = COALESCE(s.url, t.url),
      loaded_at    = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
      (waqi_idx, station_name, queried_city, lat, lon, url, source, loaded_at)
    VALUES (s.waqi_idx, s.station_name, s.queried_city, s.lat, s.lon, s.url, s.source, CURRENT_TIMESTAMP());

  -- --------------------------------------------------------------------------
  -- Fact: API rows. VARIANT flattened into typed columns in a CTE so measured_at
  -- can be reused to derive year/month.
  -- --------------------------------------------------------------------------
  INSERT INTO SILVER.FACT_AQI (
      waqi_idx, measured_at, aqi, dominant_pollutant,
      pm25, pm10, co, no2, o3, so2, humidity, temperature, pressure, wind,
      source, ingested_at, queried_city, year, month
  )
  WITH api AS (
      SELECT
        r.raw_payload:data:idx::INT                          AS waqi_idx,
        TO_TIMESTAMP_NTZ(r.raw_payload:data:time:iso::STRING) AS measured_at,
        r.raw_payload:data:aqi::DOUBLE                       AS aqi,
        NULLIF(r.raw_payload:data:dominentpol::STRING, 'aqi') AS dominant_pollutant,
        r.raw_payload:data:iaqi:pm25:v::DOUBLE               AS pm25,
        r.raw_payload:data:iaqi:pm10:v::DOUBLE               AS pm10,
        r.raw_payload:data:iaqi:co:v::DOUBLE                 AS co,
        r.raw_payload:data:iaqi:no2:v::DOUBLE                AS no2,
        r.raw_payload:data:iaqi:o3:v::DOUBLE                 AS o3,
        r.raw_payload:data:iaqi:so2:v::DOUBLE                AS so2,
        r.raw_payload:data:iaqi:h:v::DOUBLE                  AS humidity,
        r.raw_payload:data:iaqi:t:v::DOUBLE                  AS temperature,
        r.raw_payload:data:iaqi:p:v::DOUBLE                  AS pressure,
        r.raw_payload:data:iaqi:w:v::DOUBLE                  AS wind,
        r.queried_city                                        AS queried_city,
        'api'                                                 AS source,
        r.ingested_at                                         AS ingested_at
      FROM BRONZE.API_RAW_STREAM r
      WHERE r.raw_payload:data:aqi IS NOT NULL
  )
  SELECT
      waqi_idx,
      measured_at,
      aqi,
      dominant_pollutant,
      pm25, pm10, co, no2, o3, so2,
      humidity, temperature, pressure, wind,
      source,
      ingested_at,
      queried_city,
      YEAR(measured_at),
      MONTH(measured_at)
  FROM api;

  -- --------------------------------------------------------------------------
  -- Fact: CSV rows. STRING -> typed, with city conformance and the known
  -- `dominant_pollutant = 'aqi'` artifact removed.
  -- --------------------------------------------------------------------------
  INSERT INTO SILVER.FACT_AQI (
      waqi_idx, measured_at, aqi, dominant_pollutant,
      pm25, pm10, co, no2, o3, so2, humidity, temperature, pressure, wind,
      source, ingested_at, queried_city, year, month
  )
  WITH csv AS (
      SELECT
        TRY_TO_NUMBER(c.station_id)                          AS waqi_idx,
        TRY_TO_TIMESTAMP(c.data_time_s)                      AS measured_at,
        TRY_TO_DOUBLE(c.aqi_index)                           AS aqi,
        NULLIF(c.dominant_pollutant, 'aqi')                  AS dominant_pollutant,
        TRY_TO_DOUBLE(c.pm25)                                AS pm25,
        TRY_TO_DOUBLE(c.pm10)                                AS pm10,
        TRY_TO_DOUBLE(c.co)                                  AS co,
        TRY_TO_DOUBLE(c.no2)                                 AS no2,
        TRY_TO_DOUBLE(c.o3)                                  AS o3,
        TRY_TO_DOUBLE(c.so2)                                 AS so2,
        TRY_TO_DOUBLE(c.humidity)                            AS humidity,
        TRY_TO_DOUBLE(c.temperature)                         AS temperature,
        TRY_TO_DOUBLE(c.pressure)                            AS pressure,
        TRY_TO_DOUBLE(c.wind)                                AS wind,
        COALESCE(m.conformed_city, LOWER(TRIM(c.location)))  AS queried_city,
        'kaggle'                                             AS source,
        c.ingested_at                                        AS ingested_at
      FROM BRONZE.HISTORICAL_CSV_STREAM c
      LEFT JOIN CONTROL.CITY_MAP m ON m.source_city = c.location
      WHERE TRY_TO_NUMBER(c.station_id) IS NOT NULL
  )
  SELECT
      waqi_idx,
      measured_at,
      aqi,
      dominant_pollutant,
      pm25, pm10, co, no2, o3, so2,
      humidity, temperature, pressure, wind,
      source,
      ingested_at,
      queried_city,
      YEAR(measured_at),
      MONTH(measured_at)
  FROM csv;

  RETURN 'silver loaded from api + csv streams';
END;
$$;

-- ============================================================================
-- GOLD procedures (all three aggregates, wrapped by GOLD.LOAD_ALL)
-- ============================================================================
USE SCHEMA AQ_WAREHOUSE.GOLD;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_DAILY_SUMMARY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT OVERWRITE INTO GOLD.AQI_DAILY_SUMMARY
  SELECT
      queried_city,
      measured_at::DATE                                   AS date,
      AVG(aqi)                                            AS avg_aqi,
      MAX(aqi)                                            AS max_aqi,
      MIN(aqi)                                            AS min_aqi,
      AVG(pm25)                                           AS avg_pm25,
      AVG(pm10)                                           AS avg_pm10,
      AVG(humidity)                                       AS avg_humidity,
      AVG(temperature)                                    AS avg_temperature,
      COUNT(DISTINCT waqi_idx)                            AS station_count,
      COUNT(*)                                            AS record_count,
      MODE(dominant_pollutant)                            AS dominant_pollutant,
      CASE
        WHEN MAX(aqi) <= 50  THEN 'Good'
        WHEN MAX(aqi) <= 100 THEN 'Moderate'
        WHEN MAX(aqi) <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN MAX(aqi) <= 200 THEN 'Unhealthy'
        WHEN MAX(aqi) <= 300 THEN 'Very Unhealthy'
        ELSE 'Hazardous'
      END                                                 AS pollution_level,
      CURRENT_TIMESTAMP()                                 AS aggregated_at
  FROM SILVER.FACT_AQI
  GROUP BY queried_city, date;

  RETURN 'gold daily summary loaded';
END;
$$;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_CITY_RANKING()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT OVERWRITE INTO GOLD.AQI_CITY_RANKING
  WITH daily AS (
      SELECT
        queried_city,
        measured_at::DATE AS date,
        AVG(aqi)          AS avg_aqi,
        MAX(aqi)          AS max_aqi,
        MIN(aqi)          AS min_aqi,
        AVG(pm25)         AS avg_pm25,
        AVG(pm10)         AS avg_pm10,
        MODE(dominant_pollutant) AS dominant_pollutant,
        CASE
          WHEN MAX(aqi) <= 50  THEN 'Good'
          WHEN MAX(aqi) <= 100 THEN 'Moderate'
          WHEN MAX(aqi) <= 150 THEN 'Unhealthy for Sensitive Groups'
          WHEN MAX(aqi) <= 200 THEN 'Unhealthy'
          WHEN MAX(aqi) <= 300 THEN 'Very Unhealthy'
          ELSE 'Hazardous'
        END AS pollution_level
      FROM SILVER.FACT_AQI
      GROUP BY queried_city, date
  ),
  monthly AS (
      SELECT
        queried_city,
        YEAR(date)  AS year,
        MONTH(date) AS month,
        AVG(avg_aqi) AS avg_aqi,
        MAX(max_aqi) AS max_aqi,
        MIN(min_aqi) AS min_aqi,
        AVG(avg_pm25) AS avg_pm25,
        AVG(avg_pm10) AS avg_pm10,
        COUNT(*)      AS record_count,
        SUM(IFF(pollution_level = 'Good', 1, 0))                        AS days_good,
        SUM(IFF(pollution_level = 'Moderate', 1, 0))                    AS days_moderate,
        SUM(IFF(pollution_level = 'Unhealthy for Sensitive Groups', 1, 0)) AS days_sensitive,
        SUM(IFF(pollution_level = 'Unhealthy', 1, 0))                   AS days_unhealthy,
        SUM(IFF(pollution_level = 'Very Unhealthy', 1, 0))              AS days_very_unhealthy,
        SUM(IFF(pollution_level = 'Hazardous', 1, 0))                   AS days_hazardous,
        MODE(dominant_pollutant) AS dominant_pollutant,
        CASE
          WHEN AVG(avg_aqi) <= 50  THEN 'Good'
          WHEN AVG(avg_aqi) <= 100 THEN 'Moderate'
          WHEN AVG(avg_aqi) <= 150 THEN 'Unhealthy for Sensitive Groups'
          WHEN AVG(avg_aqi) <= 200 THEN 'Unhealthy'
          WHEN AVG(avg_aqi) <= 300 THEN 'Very Unhealthy'
          ELSE 'Hazardous'
        END AS pollution_level
      FROM daily
      GROUP BY queried_city, year, month
  )
  SELECT
      queried_city,
      year,
      month,
      avg_aqi,
      max_aqi,
      min_aqi,
      avg_pm25,
      avg_pm10,
      record_count,
      days_good,
      days_moderate,
      days_sensitive,
      days_unhealthy,
      days_very_unhealthy,
      days_hazardous,
      dominant_pollutant,
      pollution_level,
      DENSE_RANK() OVER (ORDER BY avg_aqi DESC)  AS aqi_rank,
      DENSE_RANK() OVER (ORDER BY avg_pm25 DESC) AS pm25_rank,
      CURRENT_TIMESTAMP()                        AS aggregated_at
  FROM monthly;

  RETURN 'gold city ranking loaded';
END;
$$;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_STATION_SUMMARY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT OVERWRITE INTO GOLD.STATION_SUMMARY
  WITH station AS (
      SELECT
        f.waqi_idx,
        d.station_name,
        f.queried_city,
        d.lat,
        d.lon,
        YEAR(f.measured_at)  AS year,
        MONTH(f.measured_at) AS month,
        AVG(f.aqi)  AS avg_aqi,
        MAX(f.aqi)  AS max_aqi,
        MIN(f.aqi)  AS min_aqi,
        AVG(f.pm25) AS avg_pm25,
        AVG(f.pm10) AS avg_pm10,
        AVG(f.humidity) AS avg_humidity,
        AVG(f.temperature) AS avg_temperature,
        COUNT(*) AS record_count,
        MODE(f.dominant_pollutant) AS dominant_pollutant
      FROM SILVER.FACT_AQI f
      LEFT JOIN SILVER.DIM_STATION d
        ON d.waqi_idx = f.waqi_idx AND d.queried_city = f.queried_city
      GROUP BY f.waqi_idx, d.station_name, f.queried_city, d.lat, d.lon, year, month
  )
  SELECT
      waqi_idx,
      station_name,
      queried_city,
      lat,
      lon,
      year,
      month,
      avg_aqi,
      max_aqi,
      min_aqi,
      avg_pm25,
      avg_pm10,
      avg_humidity,
      avg_temperature,
      record_count,
      dominant_pollutant,
      CASE
        WHEN avg_aqi <= 50  THEN 'Good'
        WHEN avg_aqi <= 100 THEN 'Moderate'
        WHEN avg_aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN avg_aqi <= 200 THEN 'Unhealthy'
        WHEN avg_aqi <= 300 THEN 'Very Unhealthy'
        ELSE 'Hazardous'
      END AS pollution_level,
      DENSE_RANK() OVER (PARTITION BY queried_city, year, month ORDER BY avg_aqi DESC) AS rank_in_city,
      CURRENT_TIMESTAMP() AS aggregated_at
  FROM station;

  RETURN 'gold station summary loaded';
END;
$$;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_ALL()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  r1 STRING;
  r2 STRING;
  r3 STRING;
BEGIN
  CALL GOLD.LOAD_DAILY_SUMMARY() INTO :r1;
  CALL GOLD.LOAD_CITY_RANKING() INTO :r2;
  CALL GOLD.LOAD_STATION_SUMMARY() INTO :r3;
  RETURN 'gold loaded: ' || r1 || '; ' || r2 || '; ' || r3;
END;
$$;

-- ============================================================================
-- CONTROL.RUN_DQ_GATE()
-- The Snowflake-native data-quality gate. Writes a row per check into
-- CONTROL.DQ_RESULTS and RAISES on any failure so the downstream Gold task
-- does NOT run — the same "stop the pipeline" behavior as the AWS SNS gate.
-- ============================================================================
USE SCHEMA AQ_WAREHOUSE.CONTROL;

CREATE OR REPLACE PROCEDURE CONTROL.RUN_DQ_GATE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  run_id STRING := UUID_STRING();
  v_row_count INT;
  v_null_pct DOUBLE;
  v_range_violations INT;
  v_missing_cities INT;
  v_invalid_source INT;
  v_fresh_rows INT;
  v_failures INT := 0;
  DQ_FAILURE EXCEPTION (-20001, 'Silver data-quality gate failed');
BEGIN
  SELECT COUNT(*) INTO :v_row_count FROM SILVER.FACT_AQI;
  IF (v_row_count < 10) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'row_count', FALSE, 'rows=' || :v_row_count || ' (min 10)');
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'row_count', TRUE, 'rows=' || :v_row_count);
  END IF;

  -- Null % across critical columns.
  SELECT
    GREATEST(
      COUNT_IF(aqi IS NULL),
      COUNT_IF(pm25 IS NULL),
      COUNT_IF(pm10 IS NULL),
      COUNT_IF(measured_at IS NULL),
      COUNT_IF(queried_city IS NULL)
    ) / NULLIF(GREATEST(COUNT(*), 1), 0) * 100
  INTO :v_null_pct
  FROM SILVER.FACT_AQI;

  IF (v_null_pct > 5.0) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'null_pct', FALSE, 'worst null%=' || ROUND(:v_null_pct, 2) || ' (max 5%)');
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'null_pct', TRUE, 'worst null%=' || ROUND(:v_null_pct, 2));
  END IF;

  -- AQI range [0, 500].
  SELECT COUNT(*) INTO :v_range_violations
  FROM SILVER.FACT_AQI WHERE aqi < 0 OR aqi > 500;

  IF (v_range_violations > 0) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'aqi_range', FALSE, 'violations=' || :v_range_violations);
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'aqi_range', TRUE, 'violations=0');
  END IF;

  -- All 5 conformed cities present.
  SELECT COUNT(*) INTO :v_missing_cities
  FROM (
      SELECT 'ha-noi' AS c UNION ALL SELECT 'ho-chi-minh-city' UNION ALL
      SELECT 'da-nang' UNION ALL SELECT 'gia-lai' UNION ALL SELECT 'cao-bang'
  ) expected
  WHERE expected.c NOT IN (SELECT DISTINCT queried_city FROM SILVER.FACT_AQI);

  IF (v_missing_cities > 0) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'city_coverage', FALSE, 'missing=' || :v_missing_cities);
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'city_coverage', TRUE, 'all 5 cities present');
  END IF;

  -- Source validity.
  SELECT COUNT(*) INTO :v_invalid_source
  FROM SILVER.FACT_AQI WHERE source NOT IN ('kaggle', 'api');

  IF (v_invalid_source > 0) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'source_validity', FALSE, 'invalid=' || :v_invalid_source);
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'source_validity', TRUE, 'invalid=0');
  END IF;

  -- Freshness: at least one row ingested within 48 hours (uses load time, not
  -- event time, so historical seed data does not falsely fail the gate).
  SELECT COUNT(*) INTO :v_fresh_rows
  FROM SILVER.FACT_AQI
  WHERE ingested_at >= DATEADD(HOUR, -48, CURRENT_TIMESTAMP());

  IF (v_fresh_rows = 0) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'freshness', FALSE, 'no rows measured within 48h');
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'freshness', TRUE, 'fresh rows=' || :v_fresh_rows);
  END IF;

  IF (v_failures > 0) THEN
      RAISE DQ_FAILURE;
  END IF;

  RETURN 'DQ PASSED';
END;
$$;

-- ============================================================================
-- Task DAG (self-orchestration, no external scheduler)
--
--   LOAD_SILVER_TASK  ->  RUN_DQ_TASK  ->  LOAD_GOLD_TASK
--
-- If the DQ gate RAISES, the RUN_DQ_TASK fails and LOAD_GOLD_TASK (AFTER) is
-- skipped — the native "stop the pipeline" behavior.
-- ============================================================================
USE SCHEMA AQ_WAREHOUSE.SILVER;

CREATE OR REPLACE TASK SILVER.LOAD_SILVER_TASK
  WAREHOUSE = AQ_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.API_RAW_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('BRONZE.HISTORICAL_CSV_STREAM')
AS
  CALL SILVER.LOAD_FROM_BRONZE();

USE SCHEMA AQ_WAREHOUSE.CONTROL;

CREATE OR REPLACE TASK CONTROL.RUN_DQ_TASK
  WAREHOUSE = AQ_WH
  AFTER SILVER.LOAD_SILVER_TASK
AS
  CALL CONTROL.RUN_DQ_GATE();

USE SCHEMA AQ_WAREHOUSE.GOLD;

CREATE OR REPLACE TASK GOLD.LOAD_GOLD_TASK
  WAREHOUSE = AQ_WH
  AFTER CONTROL.RUN_DQ_TASK
AS
  CALL GOLD.LOAD_ALL();

-- Tasks start suspended by default. Enable the whole DAG:
-- ALTER TASK SILVER.LOAD_SILVER_TASK RESUME;
-- ALTER TASK CONTROL.RUN_DQ_TASK RESUME;
-- ALTER TASK GOLD.LOAD_GOLD_TASK RESUME;
