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
-- Snapshots both streams once, then builds the full star schema:
--   DIM_CITY (sync) -> DIM_STATION (merge) -> FACT_AQI (insert)
-- ============================================================================
USE SCHEMA AQ_WAREHOUSE.SILVER;

CREATE OR REPLACE PROCEDURE SILVER.LOAD_FROM_BRONZE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- --------------------------------------------------------------------------
  -- 1. Snapshot the two streams into ONE session-scoped staging table.
  --    Each stream is read exactly once: a second read of the same stream would
  --    see nothing because the first committed read already advanced the offset.
  -- --------------------------------------------------------------------------
  CREATE OR REPLACE TEMPORARY TABLE _SRC_MEASUREMENT AS
  SELECT * FROM (
      -- API measurements (VARIANT -> typed columns)
      SELECT
        r.raw_payload:data:idx::INT                                       AS waqi_idx,
        r.raw_payload:data:city:name::STRING                              AS station_name,
        r.raw_payload:data:city:geo[0]::DOUBLE                            AS lat,
        r.raw_payload:data:city:geo[1]::DOUBLE                            AS lon,
        r.raw_payload:data:city:url::STRING                               AS url,
        r.queried_city                                                    AS city_slug,
        'api'                                                             AS source,
        TRY_TO_TIMESTAMP_NTZ(LEFT(r.raw_payload:data:time:iso::STRING, 19)) AS measured_at,
        r.raw_payload:data:aqi::DOUBLE                                    AS aqi,
        NULLIF(r.raw_payload:data:dominentpol::STRING, 'aqi')             AS dominant_pollutant,
        r.raw_payload:data:iaqi:pm25:v::DOUBLE                            AS pm25,
        r.raw_payload:data:iaqi:pm10:v::DOUBLE                            AS pm10,
        r.raw_payload:data:iaqi:co:v::DOUBLE                              AS co,
        r.raw_payload:data:iaqi:no2:v::DOUBLE                             AS no2,
        r.raw_payload:data:iaqi:o3:v::DOUBLE                              AS o3,
        r.raw_payload:data:iaqi:so2:v::DOUBLE                             AS so2,
        r.raw_payload:data:iaqi:h:v::DOUBLE                               AS humidity,
        r.raw_payload:data:iaqi:t:v::DOUBLE                               AS temperature,
        r.raw_payload:data:iaqi:p:v::DOUBLE                               AS pressure,
        r.raw_payload:data:iaqi:w:v::DOUBLE                               AS wind,
        r.ingested_at                                                    AS ingested_at
      FROM BRONZE.API_RAW_STREAM r
      WHERE r.raw_payload:data:aqi IS NOT NULL

      UNION ALL

      -- Historical CSV measurements (STRING -> typed, city conformed)
      SELECT
        TRY_TO_NUMBER(c.station_id)                                      AS waqi_idx,
        c.station_name                                                   AS station_name,
        NULL::DOUBLE                                                     AS lat,
        NULL::DOUBLE                                                     AS lon,
        c.url                                                            AS url,
        COALESCE(m.conformed_city, LOWER(TRIM(c.location)))              AS city_slug,
        'kaggle'                                                         AS source,
        TRY_TO_TIMESTAMP(c.data_time_s)                                  AS measured_at,
        TRY_TO_DOUBLE(c.aqi_index)                                       AS aqi,
        NULLIF(c.dominant_pollutant, 'aqi')                              AS dominant_pollutant,
        TRY_TO_DOUBLE(c.pm25)                                            AS pm25,
        TRY_TO_DOUBLE(c.pm10)                                            AS pm10,
        TRY_TO_DOUBLE(c.co)                                              AS co,
        TRY_TO_DOUBLE(c.no2)                                             AS no2,
        TRY_TO_DOUBLE(c.o3)                                              AS o3,
        TRY_TO_DOUBLE(c.so2)                                             AS so2,
        TRY_TO_DOUBLE(c.humidity)                                        AS humidity,
        TRY_TO_DOUBLE(c.temperature)                                     AS temperature,
        TRY_TO_DOUBLE(c.pressure)                                        AS pressure,
        TRY_TO_DOUBLE(c.wind)                                            AS wind,
        c.ingested_at                                                    AS ingested_at
      FROM BRONZE.HISTORICAL_CSV_STREAM c
      LEFT JOIN CONTROL.CITY_MAP m ON m.source_city = c.location
      WHERE TRY_TO_NUMBER(c.station_id) IS NOT NULL
  ) src;

  -- --------------------------------------------------------------------------
  -- 2. Keep DIM_CITY in sync with CONTROL.CITY_REFERENCE (idempotent MERGE).
  -- --------------------------------------------------------------------------
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

  -- --------------------------------------------------------------------------
  -- 3. Upsert DIM_STATION. The natural key is (waqi_idx, city_key); when a
  --    station appears in both sources the API row wins (richer coordinates).
  -- --------------------------------------------------------------------------
  MERGE INTO SILVER.DIM_STATION AS t
  USING (
      WITH src AS (
        SELECT
          s.waqi_idx,
          c.city_key,
          s.station_name,
          s.lat,
          s.lon,
          s.url,
          s.source,
          s.measured_at
        FROM _SRC_MEASUREMENT s
        JOIN SILVER.DIM_CITY c ON c.city_slug = s.city_slug
      ),
      best AS (
        SELECT *
        FROM src
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY waqi_idx, city_key
          ORDER BY CASE source WHEN 'api' THEN 1 ELSE 2 END, measured_at DESC
        ) = 1
      )
      SELECT
        b.waqi_idx,
        b.city_key,
        b.station_name,
        b.lat,
        b.lon,
        b.url,
        b.source,
        agg.first_seen,
        agg.last_seen
      FROM best b
      JOIN (
        SELECT
          waqi_idx,
          city_key,
          MIN(measured_at) AS first_seen,
          MAX(measured_at) AS last_seen
        FROM src
        GROUP BY waqi_idx, city_key
      ) agg ON agg.waqi_idx = b.waqi_idx AND agg.city_key = b.city_key
  ) AS s
  ON t.waqi_idx = s.waqi_idx AND t.city_key = s.city_key
  WHEN MATCHED THEN UPDATE SET
      station_name = s.station_name,
      lat          = COALESCE(s.lat, t.lat),
      lon          = COALESCE(s.lon, t.lon),
      url          = COALESCE(s.url, t.url),
      source       = s.source,
      is_active    = TRUE,
      last_seen    = COALESCE(s.last_seen, t.last_seen),
      loaded_at    = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
      (station_key, waqi_idx, city_key, station_name, lat, lon, url, source,
       is_active, first_seen, last_seen, loaded_at)
    VALUES
      (SILVER.SEQ_STATION_KEY.NEXTVAL, s.waqi_idx, s.city_key, s.station_name,
       s.lat, s.lon, s.url, s.source, TRUE, s.first_seen, s.last_seen, CURRENT_TIMESTAMP());

  -- --------------------------------------------------------------------------
  -- 4. Insert FACT_AQI by resolving all four surrogate keys. Rows with an
  --    unparseable timestamp are skipped (the DQ gate would otherwise flag them).
  -- --------------------------------------------------------------------------
  INSERT INTO SILVER.FACT_AQI (
      date_key, time_key, city_key, station_key, measured_at, aqi, pollution_level,
      dominant_pollutant, pm25, pm10, co, no2, o3, so2, humidity, temperature,
      pressure, wind, source, ingested_at
  )
  SELECT
      d.date_key,
      tm.time_key,
      c.city_key,
      st.station_key,
      s.measured_at,
      s.aqi,
      SILVER.AQI_CATEGORY(s.aqi) AS pollution_level,
      s.dominant_pollutant,
      s.pm25, s.pm10, s.co, s.no2, s.o3, s.so2,
      s.humidity, s.temperature, s.pressure, s.wind,
      s.source,
      s.ingested_at
  FROM _SRC_MEASUREMENT s
  JOIN SILVER.DIM_CITY c      ON c.city_slug = s.city_slug
  JOIN SILVER.DIM_STATION st  ON st.waqi_idx = s.waqi_idx AND st.city_key = c.city_key
  JOIN SILVER.DIM_DATE d      ON d.full_date = s.measured_at::DATE
  JOIN SILVER.DIM_TIME tm     ON tm.hour = HOUR(s.measured_at) AND tm.minute = MINUTE(s.measured_at)
  WHERE s.measured_at IS NOT NULL;

  DROP TABLE IF EXISTS _SRC_MEASUREMENT;

  RETURN 'silver loaded from api + csv streams';
END;
$$;

-- ============================================================================
-- GOLD procedures (aggregate marts, wrapped by GOLD.LOAD_ALL)
-- ============================================================================
USE SCHEMA AQ_WAREHOUSE.GOLD;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_DAILY_SUMMARY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT OVERWRITE INTO GOLD.AQI_DAILY_SUMMARY (
      queried_city, city_name, region, date, avg_aqi, max_aqi, min_aqi,
      avg_pm25, avg_pm10, avg_humidity, avg_temperature, station_count,
      record_count, dominant_pollutant, pollution_level, aggregated_at
  )
  SELECT
      c.city_slug,
      c.city_name,
      c.region,
      d.full_date AS date,
      AVG(f.aqi),
      MAX(f.aqi),
      MIN(f.aqi),
      AVG(f.pm25),
      AVG(f.pm10),
      AVG(f.humidity),
      AVG(f.temperature),
      COUNT(DISTINCT f.station_key),
      COUNT(*),
      MODE(f.dominant_pollutant),
      SILVER.AQI_CATEGORY(MAX(f.aqi)),
      CURRENT_TIMESTAMP()
  FROM SILVER.FACT_AQI f
  JOIN SILVER.DIM_CITY c ON c.city_key = f.city_key
  JOIN SILVER.DIM_DATE d ON d.date_key = f.date_key
  GROUP BY c.city_slug, c.city_name, c.region, d.full_date;

  RETURN 'gold daily summary loaded';
END;
$$;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_CITY_RANKING()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT OVERWRITE INTO GOLD.AQI_CITY_RANKING (
      queried_city, city_name, region, year, month, avg_aqi, max_aqi, min_aqi,
      avg_pm25, avg_pm10, record_count, days_good, days_moderate, days_sensitive,
      days_unhealthy, days_very_unhealthy, days_hazardous, dominant_pollutant,
      pollution_level, aqi_rank, pm25_rank, aggregated_at
  )
  WITH daily AS (
      SELECT
        c.city_slug,
        c.city_name,
        c.region,
        d.full_date AS date,
        AVG(f.aqi) AS avg_aqi,
        MAX(f.aqi) AS max_aqi,
        MIN(f.aqi) AS min_aqi,
        AVG(f.pm25) AS avg_pm25,
        AVG(f.pm10) AS avg_pm10,
        MODE(f.dominant_pollutant) AS dominant_pollutant,
        SILVER.AQI_CATEGORY(MAX(f.aqi)) AS pollution_level
      FROM SILVER.FACT_AQI f
      JOIN SILVER.DIM_CITY c ON c.city_key = f.city_key
      JOIN SILVER.DIM_DATE d ON d.date_key = f.date_key
      GROUP BY c.city_slug, c.city_name, c.region, d.full_date
  ),
  monthly AS (
      SELECT
        city_slug,
        city_name,
        region,
        YEAR(date) AS year,
        MONTH(date) AS month,
        AVG(avg_aqi) AS avg_aqi,
        MAX(max_aqi) AS max_aqi,
        MIN(min_aqi) AS min_aqi,
        AVG(avg_pm25) AS avg_pm25,
        AVG(avg_pm10) AS avg_pm10,
        COUNT(*) AS record_count,
        SUM(IFF(pollution_level = 'Good', 1, 0)) AS days_good,
        SUM(IFF(pollution_level = 'Moderate', 1, 0)) AS days_moderate,
        SUM(IFF(pollution_level = 'Unhealthy for Sensitive Groups', 1, 0)) AS days_sensitive,
        SUM(IFF(pollution_level = 'Unhealthy', 1, 0)) AS days_unhealthy,
        SUM(IFF(pollution_level = 'Very Unhealthy', 1, 0)) AS days_very_unhealthy,
        SUM(IFF(pollution_level = 'Hazardous', 1, 0)) AS days_hazardous,
        MODE(dominant_pollutant) AS dominant_pollutant,
        SILVER.AQI_CATEGORY(AVG(avg_aqi)) AS pollution_level
      FROM daily
      GROUP BY city_slug, city_name, region, year, month
  )
  SELECT
      city_slug,
      city_name,
      region,
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
      DENSE_RANK() OVER (ORDER BY avg_aqi DESC) AS aqi_rank,
      DENSE_RANK() OVER (ORDER BY avg_pm25 DESC) AS pm25_rank,
      CURRENT_TIMESTAMP()
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
  INSERT OVERWRITE INTO GOLD.STATION_SUMMARY (
      waqi_idx, station_name, queried_city, city_name, region, lat, lon, year, month,
      avg_aqi, max_aqi, min_aqi, avg_pm25, avg_pm10, avg_humidity, avg_temperature,
      record_count, dominant_pollutant, pollution_level, rank_in_city, aggregated_at
  )
  WITH station AS (
      SELECT
        st.waqi_idx,
        st.station_name,
        c.city_slug,
        c.city_name,
        c.region,
        st.lat,
        st.lon,
        d.year AS year,
        d.month AS month,
        AVG(f.aqi) AS avg_aqi,
        MAX(f.aqi) AS max_aqi,
        MIN(f.aqi) AS min_aqi,
        AVG(f.pm25) AS avg_pm25,
        AVG(f.pm10) AS avg_pm10,
        AVG(f.humidity) AS avg_humidity,
        AVG(f.temperature) AS avg_temperature,
        COUNT(*) AS record_count,
        MODE(f.dominant_pollutant) AS dominant_pollutant
      FROM SILVER.FACT_AQI f
      JOIN SILVER.DIM_STATION st ON st.station_key = f.station_key
      JOIN SILVER.DIM_CITY c      ON c.city_key = f.city_key
      JOIN SILVER.DIM_DATE d      ON d.date_key = f.date_key
      GROUP BY st.waqi_idx, st.station_name, c.city_slug, c.city_name, c.region,
               st.lat, st.lon, d.year, d.month
  )
  SELECT
      waqi_idx,
      station_name,
      city_slug,
      city_name,
      region,
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
      SILVER.AQI_CATEGORY(avg_aqi) AS pollution_level,
      DENSE_RANK() OVER (PARTITION BY city_slug, year, month ORDER BY avg_aqi DESC) AS rank_in_city,
      CURRENT_TIMESTAMP()
  FROM station;

  RETURN 'gold station summary loaded';
END;
$$;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_HOURLY_PATTERN()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT OVERWRITE INTO GOLD.AQI_HOURLY_PATTERN (
      queried_city, city_name, region, hour, time_of_day, avg_aqi, avg_pm25,
      avg_pm10, record_count, aggregated_at
  )
  SELECT
      c.city_slug,
      c.city_name,
      c.region,
      t.hour,
      t.time_of_day,
      AVG(f.aqi),
      AVG(f.pm25),
      AVG(f.pm10),
      COUNT(*),
      CURRENT_TIMESTAMP()
  FROM SILVER.FACT_AQI f
  JOIN SILVER.DIM_CITY c ON c.city_key = f.city_key
  JOIN SILVER.DIM_TIME t ON t.time_key = f.time_key
  GROUP BY c.city_slug, c.city_name, c.region, t.hour, t.time_of_day;

  RETURN 'gold hourly pattern loaded';
END;
$$;

CREATE OR REPLACE PROCEDURE GOLD.LOAD_SEASONAL_SUMMARY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT OVERWRITE INTO GOLD.AQI_SEASONAL_SUMMARY (
      queried_city, city_name, region, year, season, avg_aqi, max_aqi, min_aqi,
      avg_pm25, avg_pm10, record_count, dominant_pollutant, aggregated_at
  )
  SELECT
      c.city_slug,
      c.city_name,
      c.region,
      d.year,
      d.season,
      AVG(f.aqi),
      MAX(f.aqi),
      MIN(f.aqi),
      AVG(f.pm25),
      AVG(f.pm10),
      COUNT(*),
      MODE(f.dominant_pollutant),
      CURRENT_TIMESTAMP()
  FROM SILVER.FACT_AQI f
  JOIN SILVER.DIM_CITY c ON c.city_key = f.city_key
  JOIN SILVER.DIM_DATE d ON d.date_key = f.date_key
  GROUP BY c.city_slug, c.city_name, c.region, d.year, d.season;

  RETURN 'gold seasonal summary loaded';
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
  r4 STRING;
  r5 STRING;
BEGIN
  CALL GOLD.LOAD_DAILY_SUMMARY() INTO :r1;
  CALL GOLD.LOAD_CITY_RANKING() INTO :r2;
  CALL GOLD.LOAD_STATION_SUMMARY() INTO :r3;
  CALL GOLD.LOAD_HOURLY_PATTERN() INTO :r4;
  CALL GOLD.LOAD_SEASONAL_SUMMARY() INTO :r5;
  RETURN 'gold loaded: ' || :r1 || '; ' || :r2 || '; ' || :r3 || '; ' || :r4 || '; ' || :r5;
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
  v_orphan_rows INT;
  v_failures INT := 0;
  DQ_FAILURE EXCEPTION (-20001, 'Silver data-quality gate failed');
BEGIN
  -- row_count
  SELECT COUNT(*) INTO :v_row_count FROM SILVER.FACT_AQI;
  IF (v_row_count < 10) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'row_count', FALSE, 'rows=' || :v_row_count || ' (min 10)');
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'row_count', TRUE, 'rows=' || :v_row_count);
  END IF;

  -- Null % across critical columns (including the surrogate keys).
  SELECT
    GREATEST(
      COUNT_IF(aqi IS NULL),
      COUNT_IF(pm25 IS NULL),
      COUNT_IF(pm10 IS NULL),
      COUNT_IF(measured_at IS NULL),
      COUNT_IF(date_key IS NULL),
      COUNT_IF(city_key IS NULL),
      COUNT_IF(station_key IS NULL),
      COUNT_IF(time_key IS NULL)
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

  -- All conformed reference cities present in the fact table.
  SELECT COUNT(*) INTO :v_missing_cities
  FROM CONTROL.CITY_REFERENCE r
  WHERE NOT EXISTS (
      SELECT 1
      FROM SILVER.FACT_AQI f
      JOIN SILVER.DIM_CITY c ON c.city_key = f.city_key
      WHERE c.city_slug = r.city_slug
  );

  IF (v_missing_cities > 0) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'city_coverage', FALSE, 'missing=' || :v_missing_cities);
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'city_coverage', TRUE, 'all reference cities present');
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

  -- Freshness: at least one row loaded within 48 hours (uses load time, not
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

  -- Foreign-key integrity: every fact must resolve to all four dimensions.
  SELECT COUNT(*) INTO :v_orphan_rows
  FROM SILVER.FACT_AQI f
  LEFT JOIN SILVER.DIM_DATE d    ON d.date_key = f.date_key
  LEFT JOIN SILVER.DIM_TIME t    ON t.time_key = f.time_key
  LEFT JOIN SILVER.DIM_CITY c    ON c.city_key = f.city_key
  LEFT JOIN SILVER.DIM_STATION s ON s.station_key = f.station_key
  WHERE d.date_key IS NULL OR t.time_key IS NULL
     OR c.city_key IS NULL OR s.station_key IS NULL;

  IF (v_orphan_rows > 0) THEN
      v_failures := v_failures + 1;
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'fk_integrity', FALSE, 'orphan fact rows=' || :v_orphan_rows);
  ELSE
      INSERT INTO CONTROL.DQ_RESULTS (run_id, check_name, passed, detail)
        VALUES (:run_id, 'fk_integrity', TRUE, 'orphan rows=0');
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
-- Snowflake requires all tasks in a single AFTER chain to live in the SAME
-- schema, so we place the orchestration tasks in CONTROL even though they call
-- procedures in SILVER/CONTROL/GOLD.
--
-- If the DQ gate RAISES, the RUN_DQ_TASK fails and LOAD_GOLD_TASK (AFTER) is
-- skipped — the native "stop the pipeline" behavior.
-- ============================================================================
USE SCHEMA AQ_WAREHOUSE.CONTROL;

CREATE OR REPLACE TASK CONTROL.LOAD_SILVER_TASK
  WAREHOUSE = AQ_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.API_RAW_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('BRONZE.HISTORICAL_CSV_STREAM')
AS
  CALL SILVER.LOAD_FROM_BRONZE();

CREATE OR REPLACE TASK CONTROL.RUN_DQ_TASK
  WAREHOUSE = AQ_WH
  AFTER CONTROL.LOAD_SILVER_TASK
AS
  CALL CONTROL.RUN_DQ_GATE();

CREATE OR REPLACE TASK CONTROL.LOAD_GOLD_TASK
  WAREHOUSE = AQ_WH
  AFTER CONTROL.RUN_DQ_TASK
AS
  CALL GOLD.LOAD_ALL();

-- Tasks start suspended by default. Enable the whole DAG:
-- ALTER TASK CONTROL.LOAD_SILVER_TASK RESUME;
-- ALTER TASK CONTROL.RUN_DQ_TASK RESUME;
-- ALTER TASK CONTROL.LOAD_GOLD_TASK RESUME;
