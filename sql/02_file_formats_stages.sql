-- ============================================================================
-- 02. File Formats & External Stages
--
-- Snowflake advantage: you don't need S3 *and* a catalog *and* a query engine.
-- A stage points at object storage (or ships files in the sample dir), and Snowflake
-- reads it directly. VARIANT keeps raw JSON queryable without pre-defining columns.
-- ============================================================================

USE SCHEMA AQ_WAREHOUSE.BRONZE;

-- JSON from the WAQI API, one object per file. Strip nulls keeps the raw payload
-- compact while preserving every non-null field.
CREATE OR REPLACE FILE FORMAT AQ_JSON_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = TRUE
  ALLOW_DUPLICATE = FALSE;

-- Kaggle CSV uses a comma delimiter and the first row is a header.
CREATE OR REPLACE FILE FORMAT AQ_CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null', 'NA', 'N/A')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ----------------------------------------------------------------------------
-- Internal stage is free and perfect for the educational/offline path. The
-- Snowpark loader PUTs local files here, then COPY INTO reads them.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STAGE AQ_INTERNAL_STAGE
  FILE_FORMAT = AQ_JSON_FORMAT
  COMMENT = 'Local Snowflake stage for uploading raw WAQI JSON / CSV files.';

-- ----------------------------------------------------------------------------
-- (Optional) external stage pointing at a cloud bucket. Uncomment and fill in
-- a real URL + storage integration if you want S3 as the landing zone instead
-- of the internal stage.
-- ----------------------------------------------------------------------------
-- CREATE OR REPLACE STAGE AQ_EXTERNAL_STAGE
--   URL = 's3://<your-bucket>/air-quality/'
--   STORAGE_INTEGRATION = <your_storage_integration>
--   FILE_FORMAT = AQ_JSON_FORMAT
--   COMMENT = 'Optional S3 landing zone.';
