-- Run once as ACCOUNTADMIN. Creates the objects the platform needs.
-- Every statement here is deliberate; see docs/01-tools-explained.md.

USE ROLE ACCOUNTADMIN;

-- A dedicated role, not ACCOUNTADMIN, for day-to-day work.
CREATE ROLE IF NOT EXISTS NYC_PLATFORM_ROLE;

-- XSMALL is genuinely enough for this volume, and it auto-suspends after
-- 60 seconds so you are not billed for an idle warehouse. Credit control
-- is a data engineering responsibility, not an afterthought.
CREATE WAREHOUSE IF NOT EXISTS NYC_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS NYC_PLATFORM;

CREATE SCHEMA IF NOT EXISTS NYC_PLATFORM.RAW;        -- landed from S3
CREATE SCHEMA IF NOT EXISTS NYC_PLATFORM.STAGING;    -- dbt staging models
CREATE SCHEMA IF NOT EXISTS NYC_PLATFORM.MARTS;      -- dbt star schema

GRANT USAGE ON WAREHOUSE NYC_WH TO ROLE NYC_PLATFORM_ROLE;
GRANT ALL ON DATABASE NYC_PLATFORM TO ROLE NYC_PLATFORM_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE NYC_PLATFORM TO ROLE NYC_PLATFORM_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE NYC_PLATFORM TO ROLE NYC_PLATFORM_ROLE;

-- Replace with your Snowflake login name.
GRANT ROLE NYC_PLATFORM_ROLE TO USER <YOUR_SNOWFLAKE_USER>;
