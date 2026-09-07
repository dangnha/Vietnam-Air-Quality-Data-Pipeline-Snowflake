-- ============================================================================
-- 00. Account Setup: Warehouse + Cost Guard
--
-- Run this file FIRST, as ACCOUNTADMIN. It creates the virtual warehouse that
-- every later file uses, plus a resource monitor that hard-caps spending.
--
-- Why these settings:
--   * XSMALL            -> smallest warehouse, plenty for this educational data
--   * AUTO_SUSPEND = 60 -> warehouse bills $0 while idle (pay per second)
--   * AUTO_RESUME = TRUE-> warehouse starts automatically when a query needs it
--   * MAX_CLUSTER_COUNT = 1 -> never scale out to multiple clusters
--   * 400-credit monitor -> explicit budget ceiling requested for this project
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- 1. Virtual warehouse
-- ----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS AQ_WH
  WITH
    WAREHOUSE_SIZE      = XSMALL
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    MAX_CLUSTER_COUNT   = 1
    MIN_CLUSTER_COUNT   = 1
    SCALING_POLICY      = ECONOMY
    COMMENT = 'Education air-quality warehouse. XSMALL + auto-suspend to keep costs near zero.';

-- ----------------------------------------------------------------------------
-- 2. Resource monitor (the $400 ceiling)
--
-- CREDIT_QUOTA is per calendar month. We notify early and SUSPEND_IMMEDIATE at
-- 100% so the account cannot silently overshoot the budget.
--
-- NOTE: the SUSPEND_IMMEDIATE action applies to warehouses owned by the monitor.
-- We associate AQ_WH below so this monitor is the enforcement point.
-- ============================================================================
CREATE RESOURCE MONITOR IF NOT EXISTS AQ_BUDGET_MONITOR
  WITH
    CREDIT_QUOTA = 400
    FREQUENCY    = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = (SYSADMIN)
    TRIGGERS
      ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO SUSPEND_IMMEDIATE
  COMMENT = 'Hard cap: 400 Snowflake credits per month for the air-quality project.';

-- Attach the warehouse to the monitor so 100% triggers suspend it.
ALTER WAREHOUSE AQ_WH SET RESOURCE_MONITOR = AQ_BUDGET_MONITOR;

-- ----------------------------------------------------------------------------
-- 3. Hand the warehouse to SYSADMIN so the rest of the SQL can run without
--    staying in ACCOUNTADMIN.
-- ----------------------------------------------------------------------------
GRANT USAGE ON WAREHOUSE AQ_WH TO ROLE SYSADMIN;
GRANT MODIFY ON WAREHOUSE AQ_WH TO ROLE SYSADMIN;
GRANT MONITOR ON WAREHOUSE AQ_WH TO ROLE SYSADMIN;

USE ROLE SYSADMIN;
USE WAREHOUSE AQ_WH;
