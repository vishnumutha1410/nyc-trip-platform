-- Connect Snowflake to your S3 bucket WITHOUT putting AWS keys in Snowflake.
--
-- This is the part worth understanding properly, because it is real cloud
-- engineering and it comes up in interviews. Snowflake assumes an IAM role
-- in YOUR account. No access keys are ever stored in Snowflake.
--
-- The chicken-and-egg is intentional: you create the integration with a
-- placeholder role ARN, ask Snowflake for the IAM user it will use, then
-- edit the AWS trust policy to allow exactly that principal.

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS NYC_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/snowflake-nyc-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET>/curated/');

-- Step 2: run this, then copy STORAGE_AWS_IAM_USER_ARN and
-- STORAGE_AWS_EXTERNAL_ID into the trust policy of that IAM role in AWS.
DESC INTEGRATION NYC_S3_INT;

GRANT USAGE ON INTEGRATION NYC_S3_INT TO ROLE NYC_PLATFORM_ROLE;

-- Step 3: once the trust policy is updated, create the stage.
USE ROLE NYC_PLATFORM_ROLE;
USE DATABASE NYC_PLATFORM;
USE SCHEMA RAW;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FMT TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS CURATED_STAGE
  STORAGE_INTEGRATION = NYC_S3_INT
  URL = 's3://<YOUR_BUCKET>/curated/trips/'
  FILE_FORMAT = PARQUET_FMT;

-- Sanity check: should list your parquet files.
LIST @CURATED_STAGE;
