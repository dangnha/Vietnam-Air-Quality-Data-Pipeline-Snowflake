-- ============================================================================
-- 00. Account Setup: Warehouse + Cost Guard
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- 1. Virtual warehouse
-- ----------------------------------------------------------------------------

CREATE WAREHOUSE IF NOT EXISTS AQ_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Education air-quality warehouse.';

-- ----------------------------------------------------------------------------
-- 2. Resource monitor
-- ----------------------------------------------------------------------------

CREATE RESOURCE MONITOR IF NOT EXISTS AQ_BUDGET_MONITOR
  WITH
    CREDIT_QUOTA = 400
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 50 PERCENT DO NOTIFY
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND_IMMEDIATE
;

-- ----------------------------------------------------------------------------
-- 3. Attach resource monitor to warehouse
-- ----------------------------------------------------------------------------

ALTER WAREHOUSE AQ_WH
  SET RESOURCE_MONITOR = AQ_BUDGET_MONITOR;

-- ----------------------------------------------------------------------------
-- 4. Give SYSADMIN access to warehouse
-- ----------------------------------------------------------------------------

GRANT USAGE ON WAREHOUSE AQ_WH TO ROLE SYSADMIN;
GRANT MODIFY ON WAREHOUSE AQ_WH TO ROLE SYSADMIN;
GRANT MONITOR ON WAREHOUSE AQ_WH TO ROLE SYSADMIN;

USE ROLE SYSADMIN;
USE WAREHOUSE AQ_WH;

