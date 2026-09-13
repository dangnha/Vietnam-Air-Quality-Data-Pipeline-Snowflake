-- ============================================================================
-- 01. Databases & Schemas
-- Medallion architecture: BRONZE -> SILVER -> GOLD as separate schemas.
--
-- Why separate schemas (not separate databases)?
--   * Zero-copy cloning and Time Travel work across schemas cheaply.
--   * A single database keeps governance simple while still isolating layers.
--   * You can grant access per schema (raw vs curated) using one DB object.
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE AQ_WH;

CREATE DATABASE IF NOT EXISTS AQ_WAREHOUSE
  COMMENT = 'Vietnam air-quality data warehouse (Snowflake Medallion architecture)';

USE DATABASE AQ_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- Layer schemas
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS BRONZE
  COMMENT = 'Raw, immutable source data. JSON stored as VARIANT, CSV as-is.';

CREATE SCHEMA IF NOT EXISTS SILVER
  COMMENT = 'Cleaned, typed, deduplicated star-schema layer (dim + fact).';

CREATE SCHEMA IF NOT EXISTS GOLD
  COMMENT = 'Analytics-ready aggregates (daily / city / station summaries).';

CREATE SCHEMA IF NOT EXISTS CONTROL
  COMMENT = 'Audit, quality-gate, and pipeline-control tables (not business data).';

-- ----------------------------------------------------------------------------
-- Control tables that the pipeline relies on.
-- These are deliberately in SQL file 01 so every later file can reference them.
-- ----------------------------------------------------------------------------
USE SCHEMA AQ_WAREHOUSE.CONTROL;

-- Maps human-readable CSV city names to the WAQI slug used as the conformed key.
CREATE OR REPLACE TABLE CONTROL.CITY_MAP (
    source_city   STRING NOT NULL,
    conformed_city STRING NOT NULL,
    PRIMARY KEY (source_city) NOT ENFORCED
)
COMMENT = 'Conformance map: CSV location -> WAQI queried_city slug';

INSERT OVERWRITE INTO CONTROL.CITY_MAP VALUES
    ('Ha Noi',           'ha-noi'),
    ('Hanoi',            'ha-noi'),
    ('Ho Chi Minh City', 'ho-chi-minh-city'),
    ('Ho Chi Minh',      'ho-chi-minh-city'),
    ('Da Nang',          'da-nang'),
    ('Gia Lai',          'gia-lai'),
    ('Cao Bang',         'cao-bang');

-- Conformed city reference: the WAQI slug is the natural key, enriched with the
-- display name, Vietnam region, and city-center coordinates. DIM_CITY is built
-- from this reference, so the dimension carries business attributes the raw
-- payload does not contain.
CREATE OR REPLACE TABLE CONTROL.CITY_REFERENCE (
    city_slug  STRING NOT NULL,
    city_name  STRING NOT NULL,
    region     STRING NOT NULL,
    lat        DOUBLE,
    lon        DOUBLE,
    PRIMARY KEY (city_slug) NOT ENFORCED
)
COMMENT = 'Conformed city reference: WAQI slug -> name, region, center coords';

INSERT OVERWRITE INTO CONTROL.CITY_REFERENCE VALUES
    ('ha-noi',           'Ha Noi',           'North',             21.0285, 105.8542),
    ('ho-chi-minh-city', 'Ho Chi Minh City', 'South',             10.8231, 106.6297),
    ('da-nang',          'Da Nang',          'Central',           16.0544, 108.2022),
    ('gia-lai',          'Gia Lai',          'Central Highlands', 13.9718, 108.0151),
    ('cao-bang',         'Cao Bang',         'North',             22.6657, 106.2579);

-- One row per quality check per run. Populated by CONTROL.RUN_DQ_GATE().
CREATE OR REPLACE TABLE CONTROL.DQ_RESULTS (
    run_id        STRING,
    check_name    STRING,
    passed        BOOLEAN,
    detail        STRING,
    checked_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Audit log for the Silver data-quality gate';

-- Idempotency guard for the Snowpark loader: files already staged/loaded.
CREATE OR REPLACE TABLE CONTROL.LOADED_FILES (
    file_name     STRING,
    layer         STRING,
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (file_name, layer) NOT ENFORCED
)
COMMENT = 'Tracks files already COPY INTO Bronze so reloads are no-ops';
