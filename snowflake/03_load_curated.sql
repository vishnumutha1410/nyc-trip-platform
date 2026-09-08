-- Load curated Parquet from S3 into the RAW schema.
--
-- MATCH_BY_COLUMN_NAME means Snowflake maps parquet fields to table columns
-- by name rather than position, so a new column appearing upstream does not
-- silently shift every value one column to the left.
--
-- COPY INTO is idempotent by default: Snowflake tracks which files it has
-- already loaded and skips them. Re-running this is safe.

USE ROLE NYC_PLATFORM_ROLE;
USE WAREHOUSE NYC_WH;
USE DATABASE NYC_PLATFORM;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_TRIPS (
    vendor_id              NUMBER,
    pickup_datetime        TIMESTAMP_NTZ,
    dropoff_datetime       TIMESTAMP_NTZ,
    passenger_count        NUMBER,
    trip_distance          FLOAT,
    rate_code_id           NUMBER,
    store_and_fwd_flag     VARCHAR,
    pickup_location_id     NUMBER,
    dropoff_location_id    NUMBER,
    payment_type           NUMBER,
    fare_amount            FLOAT,
    extra                  FLOAT,
    mta_tax                FLOAT,
    tip_amount             FLOAT,
    tolls_amount           FLOAT,
    improvement_surcharge  FLOAT,
    total_amount           FLOAT,
    congestion_surcharge   FLOAT,
    airport_fee            FLOAT,
    cbd_congestion_fee     FLOAT,
    pickup_date            DATE,
    trip_duration_seconds  NUMBER,
    pickup_year            NUMBER,
    pickup_month           NUMBER,
    _loaded_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO RAW_TRIPS (
    vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    rate_code_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    cbd_congestion_fee, pickup_date, trip_duration_seconds, pickup_year, pickup_month
)
FROM @CURATED_STAGE
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

SELECT count(*) AS rows_loaded,
       min(pickup_datetime) AS earliest,
       max(pickup_datetime) AS latest
FROM RAW_TRIPS;
